//! Host-side helpers for `--public-mirror-v1`.
//!
//! Applies externally observed row ops into an in-memory [`RelationalDB`] and
//! broadcasts [`ModuleEvent`]s that preserve upstream reducer provenance.

use super::module_host::{
    create_table_from_def, create_table_from_view_def, DatabaseUpdate, EventStatus, ModuleEvent, ModuleFunctionCall,
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
use std::time::Duration;

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

/// Apply row ops in one mut tx and broadcast with upstream provenance.
pub fn apply_external_update(
    subs: &ModuleSubscriptions,
    provenance: Option<ExternalProvenance>,
    ops: impl IntoIterator<Item = TableOps>,
) -> Result<(), DBError> {
    let stdb = subs.relational_db();
    let mut tx = stdb.begin_mut_tx(IsolationLevel::Serializable, Workload::Update);

    for table_ops in ops {
        for row in &table_ops.deletes {
            tx.delete_product_value(table_ops.table_id, row)?;
        }
        for row_bytes in &table_ops.inserts {
            stdb.insert(&mut tx, table_ops.table_id, row_bytes)?;
        }
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
            create_table_from_def(stdb, tx, module_def, def)?;
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
