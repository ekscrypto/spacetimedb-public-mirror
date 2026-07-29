//! Host-side helpers for `--public-mirror-v1`.
//!
//! Applies externally observed row ops into an in-memory [`RelationalDB`] and
//! broadcasts [`ModuleEvent`]s that preserve upstream reducer provenance.

use super::module_host::{
    create_table_from_view_def, DatabaseUpdate, EventStatus, ModuleEvent, ModuleFunctionCall,
};
use super::ArgsTuple;
use crate::db::relational_db::RelationalDB;
use crate::error::DBError;
use crate::subscription::module_subscription_actor::{commit_and_broadcast_event, ModuleSubscriptions};
use bytes::Bytes;
use spacetimedb_client_api_messages::energy::FunctionBudget;
use spacetimedb_datastore::execution_context::Workload;
use spacetimedb_datastore::system_tables::ModuleKind;
use spacetimedb_datastore::traits::{IsolationLevel, Program};
use spacetimedb_execution::dml::MutDatastore;
use spacetimedb_lib::{ConnectionId, Identity, ProductValue, Timestamp};
use spacetimedb_primitives::TableId;
use spacetimedb_schema::def::ModuleDef;
use spacetimedb_schema::identifier::Identifier;
use spacetimedb_schema::reducer_name::ReducerName;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Upstream reducer call-stack / provenance to attach to a mirrored update.
pub struct ExternalProvenance {
    pub reducer_name: String,
    pub caller_identity: Identity,
    pub caller_connection_id: ConnectionId,
    pub timestamp: Timestamp,
    pub request_id: u32,
    pub args: Bytes, // upstream reducer args blob
}

/// Row ops for a single table within an external update.
pub struct TableOps {
    pub table_id: TableId,
    pub deletes: Vec<ProductValue>,
    pub inserts: Vec<Bytes>, // BSATN row bytes for `RelationalDB::insert`
}

/// One externally observed update ready to apply into the mirror.
///
/// A batch of these is applied in one executor job (each update in its own
/// transaction + broadcast) by [`super::module_host::ModuleHost::apply_mirrored_updates`].
pub struct MirroredUpdate {
    pub provenance: Option<ExternalProvenance>,
    pub ops: Vec<TableOps>,
    pub is_seed: bool,
}

/// Shared counters updated during a large seed insert (for `/v1/mirrors`).
#[derive(Clone)]
pub struct SeedApplyProgress {
    pub rows_applied: Arc<AtomicU64>,
    pub last_apply_unix_ms: Arc<AtomicU64>,
}

impl SeedApplyProgress {
    fn record(&self, total_applied: u64) {
        self.rows_applied.store(total_applied, Ordering::Relaxed);
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        self.last_apply_unix_ms.store(ms, Ordering::Relaxed);
    }
}

/// Apply row ops in one mut tx and broadcast with upstream provenance.
///
/// When `is_seed` is set, each table is cleared before its snapshot rows are
/// inserted. Reconnect re-seeds otherwise collide with rows left over from the
/// previous session (unique constraint violation → endless reconnect loop).
pub fn apply_external_update(
    subs: &ModuleSubscriptions,
    provenance: Option<ExternalProvenance>,
    ops: impl IntoIterator<Item = TableOps>,
    progress: Option<SeedApplyProgress>,
    is_seed: bool,
) -> Result<(), DBError> {
    let stdb = subs.relational_db();
    let mut tx = stdb.begin_mut_tx(IsolationLevel::Serializable, Workload::Update);

    // Yield / report periodically so a multi-million-row seed insert yields the
    // dedicated JobCores thread under CPU contention, and so `/v1/mirrors`
    // can show forward progress during `applying_seed`. (Other mirrors' live
    // WS tasks run on the shared Tokio runtime + their own JobCores threads;
    // they are not scheduled on this thread.)
    const PROGRESS_EVERY: u64 = 50_000;
    let mut total_applied = 0u64;
    let mut since_tick = 0u64;

    for table_ops in ops {
        if is_seed {
            // Truncate-then-insert makes re-seeds idempotent. In the same mut-tx,
            // unchanged rows cancel out (delete + identical insert), so downstream
            // subscribers only see the actual diff against the previous session.
            let removed = tx.clear_table(table_ops.table_id)?;
            if removed > 0 {
                log::info!(
                    "public-mirror: cleared {removed} stale rows from table {} before re-seed",
                    table_ops.table_id
                );
            }
        }
        for row in &table_ops.deletes {
            tx.delete_product_value(table_ops.table_id, row)?;
            total_applied += 1;
            since_tick += 1;
            if since_tick >= PROGRESS_EVERY {
                if let Some(p) = &progress {
                    p.record(total_applied);
                }
                std::thread::yield_now();
                since_tick = 0;
            }
        }
        for row_bytes in &table_ops.inserts {
            stdb.insert(&mut tx, table_ops.table_id, row_bytes)?;
            total_applied += 1;
            since_tick += 1;
            if since_tick >= PROGRESS_EVERY {
                if let Some(p) = &progress {
                    p.record(total_applied);
                }
                std::thread::yield_now();
                since_tick = 0;
            }
        }
    }
    if let Some(p) = &progress {
        p.record(total_applied);
    }

    let (timestamp, caller_identity, caller_connection_id, request_id, function_call) = match provenance {
        Some(p) => {
            let reducer = match Identifier::new(p.reducer_name.clone().into()) {
                Ok(id) => ReducerName::new(id),
                Err(_) => ReducerName::new(Identifier::new_assume_valid(p.reducer_name.into())),
            };
            (
                p.timestamp,
                p.caller_identity,
                Some(p.caller_connection_id),
                Some(p.request_id),
                ModuleFunctionCall {
                    reducer: Some(reducer),
                    reducer_id: u32::MAX.into(),
                    args: ArgsTuple::from_bsatn_unchecked(p.args),
                },
            )
        }
        None => (
            Timestamp::now(),
            Identity::ZERO,
            None,
            None,
            ModuleFunctionCall::update(),
        ),
    };

    let event = ModuleEvent {
        timestamp,
        caller_identity,
        caller_connection_id,
        function_call,
        // Filled from committed writes inside `commit_and_broadcast_event`.
        status: EventStatus::Committed(DatabaseUpdate::default()),
        reducer_return_value: None,
        execution_budget_used: FunctionBudget::ZERO,
        host_execution_duration: Duration::ZERO,
        request_id,
        timer: None,
    };

    let _ = commit_and_broadcast_event(subs, None, event, tx);
    Ok(())
}

/// Bootstrap user tables (and views) from a [`ModuleDef`] without running an init reducer.
pub fn create_tables_from_module_def(stdb: &RelationalDB, module_def: &ModuleDef) -> anyhow::Result<()> {
    let tx = stdb.begin_mut_tx(IsolationLevel::Serializable, Workload::Internal);
    let (tx, ()) = stdb.with_auto_rollback(tx, |tx| {
        let mut table_defs: Vec<_> = module_def.tables().collect();
        table_defs.sort_by_key(|x| &x.name);
        for def in table_defs {
            spacetimedb_engine::update::create_table_from_def(stdb, tx, module_def, def)?;
        }

        let mut view_defs: Vec<_> = module_def.views().collect();
        view_defs.sort_by_key(|x| &x.name);
        for def in view_defs {
            create_table_from_view_def(stdb, tx, module_def, def)?;
        }

        let program = Program::empty(ModuleKind::MIRROR);
        stdb.set_initialized(tx, program)?;
        anyhow::Ok(())
    })?;

    if let Some((_tx_offset, tx_data, tx_metrics, reducer)) = stdb.commit_tx(tx)? {
        stdb.report_mut_tx_metrics(reducer, tx_metrics, Some(tx_data));
    }
    Ok(())
}
