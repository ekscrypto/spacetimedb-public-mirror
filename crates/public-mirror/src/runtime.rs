//! Public-mirror apply loop: upstream → `apply_external_update`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use spacetimedb::db::relational_db::RelationalDB;
use spacetimedb::host::module_host::ModuleHost;
use spacetimedb::host::public_mirror::{apply_external_update, ExternalProvenance, TableOps};
use spacetimedb_datastore::execution_context::Workload;
use spacetimedb_primitives::TableId;
use spacetimedb_schema::def::ModuleDef;
use url::Url;

use crate::schema::public_user_table_names;
use crate::upstream::{self, UpstreamConfig, UpstreamUpdate};

/// Configuration for the public-mirror upstream loop.
#[derive(Debug, Clone)]
pub struct PublicMirrorConfig {
    pub upstream: Url,
    pub database: String,
    pub auth_token: Option<String>,
    /// When `None`, subscribe to all public user tables from the module def.
    pub tables: Option<Vec<String>>,
    pub connect_timeout: Duration,
}

/// Resolve table name → [`TableId`] via the local relational DB.
fn resolve_table_ids(stdb: &RelationalDB, names: &[String]) -> anyhow::Result<HashMap<String, TableId>> {
    let tx = stdb.begin_tx(Workload::Internal);
    let mut map = HashMap::with_capacity(names.len());
    for name in names {
        let Some(id) = stdb.table_id_from_name(&tx, name)? else {
            anyhow::bail!("local mirror has no table `{name}`");
        };
        map.insert(name.clone(), id);
    }
    // Tx is dropped without commit — read-only.
    drop(tx);
    Ok(map)
}

fn update_to_table_ops(
    update: UpstreamUpdate,
    table_ids: &HashMap<String, TableId>,
) -> anyhow::Result<(Option<ExternalProvenance>, Vec<TableOps>)> {
    let provenance = update.provenance.map(|p| ExternalProvenance {
        reducer_name: p.reducer_name,
        caller_identity: p.caller_identity,
        caller_connection_id: p.caller_connection_id,
        timestamp: p.timestamp,
        request_id: p.request_id,
        args: p.args,
    });
    let mut ops = Vec::with_capacity(update.tables.len());
    for t in update.tables {
        let Some(&table_id) = table_ids.get(&t.table_name) else {
            log::warn!(
                "public-mirror: skipping ops for unknown local table `{}`",
                t.table_name
            );
            continue;
        };
        ops.push(TableOps {
            table_id,
            deletes: t.deletes,
            inserts: t.inserts,
        });
    }
    Ok((provenance, ops))
}

/// Connect upstream, sequential-subscribe public tables, apply seed + live updates into `module_host`.
pub async fn run_public_mirror_loop(
    module_host: ModuleHost,
    config: PublicMirrorConfig,
    module_def: ModuleDef,
) -> anyhow::Result<()> {
    let tables = match config.tables {
        Some(t) if !t.is_empty() => t,
        _ => public_user_table_names(&module_def),
    };
    if tables.is_empty() {
        anyhow::bail!("no public user tables to mirror");
    }
    log::info!(
        "public-mirror: mirroring {} tables from {} database={}",
        tables.len(),
        config.upstream,
        config.database
    );

    let stdb = module_host.relational_db().clone();
    let table_ids = resolve_table_ids(&stdb, &tables)?;
    let subs = module_host.subscriptions().clone();

    let on_update = Arc::new(move |update: UpstreamUpdate| -> Result<(), anyhow::Error> {
        let (provenance, ops) = update_to_table_ops(update, &table_ids)?;
        if ops.is_empty() {
            return Ok(());
        }
        let n_ins: usize = ops.iter().map(|o| o.inserts.len()).sum();
        let n_del: usize = ops.iter().map(|o| o.deletes.len()).sum();
        apply_external_update(&subs, provenance, ops)?;
        log::debug!("public-mirror: applied update (+{n_ins} -{n_del})");
        Ok(())
    });

    let upstream_cfg = UpstreamConfig {
        host: config.upstream,
        database: config.database,
        auth_token: config.auth_token,
        connect_timeout: config.connect_timeout,
    };

    // Reconnect loop.
    loop {
        match upstream::connect_and_mirror(upstream_cfg.clone(), &module_def, &tables, on_update.clone()).await {
            Ok(()) => {
                log::warn!("public-mirror: upstream loop exited cleanly; reconnecting in 5s");
            }
            Err(e) => {
                log::error!("public-mirror: upstream error: {e:#}; reconnecting in 5s");
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Convenience: hash schema bytes into a SpacetimeDB [`spacetimedb_lib::Hash`].
pub fn schema_program_hash(schema_bytes: &[u8]) -> spacetimedb_lib::Hash {
    spacetimedb_sats::hash::hash_bytes(schema_bytes)
}
