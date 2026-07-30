use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use crate::NodeDelegate;

/// Connectivity phase for one upstream mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorConnectivity {
    /// Parked on the in-process subscribe gate, waiting for a free slot.
    Waiting,
    Connecting,
    Subscribing,
    Live,
    Disconnected,
}

/// Sub-phase while `connectivity == subscribing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscribePhase {
    /// Waiting for `SubscribeMultiApplied` (seed may still be arriving on the wire).
    AwaitingSeed,
    /// Seed message received; applying rows into the local mirror DB.
    ApplyingSeed,
}

#[derive(Debug, Clone, Serialize)]
pub struct MirrorStatusSnapshot {
    pub host: String,
    pub database: String,
    pub connectivity: MirrorConnectivity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disconnected_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_attempt_eta_secs: Option<u64>,
    pub tables_live: u32,
    pub tables_total: u32,
    pub transactions_processed: u64,
    /// Table whose subscribe is currently in progress (mainly while `subscribing`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table_started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table_phase: Option<SubscribePhase>,
    /// Socket bytes read since this table's subscribe was sent (framing + interleaved TUs).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table_bytes_received: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_byte_at: Option<String>,
    /// Row count from `SubscribeMultiApplied`, set while `applying_seed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table_seed_rows: Option<u64>,
    /// Inserts committed into the local mut-tx so far (ticks during `applying_seed`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_table_seed_rows_applied: Option<u64>,
    /// Wall clock of the last seed-insert progress tick (hang detector during apply).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seed_apply_at: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MirrorsResponse {
    pub mirrors: Vec<MirrorStatusSnapshot>,
}

pub async fn mirrors<S: NodeDelegate>(State(ctx): State<S>) -> impl IntoResponse {
    Json(ctx.mirror_statuses())
}

/// Whether downstream clients may connect to this node.
///
/// Non-mirror deployments (empty `/v1/mirrors` registry) always accept clients.
/// In `--public-mirror-v1` mode, clients are rejected until every mirrored
/// database reports [`MirrorConnectivity::Live`] — so seed apply can skip
/// subscription eval / broadcast with no risk of missing updates. Status
/// polling via `GET /v1/mirrors` is unaffected.
pub fn public_mirror_accepts_clients(statuses: &MirrorsResponse) -> bool {
    let mirrors = &statuses.mirrors;
    mirrors.is_empty() || mirrors.iter().all(|m| m.connectivity == MirrorConnectivity::Live)
}

pub fn router<S>() -> axum::Router<S>
where
    S: NodeDelegate + Clone + 'static,
{
    use axum::routing::get;
    axum::Router::new().route("/", get(mirrors::<S>))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(connectivity: MirrorConnectivity) -> MirrorStatusSnapshot {
        MirrorStatusSnapshot {
            host: "h".into(),
            database: "db".into(),
            connectivity,
            connected_since: None,
            disconnected_since: None,
            next_attempt_at: None,
            next_attempt_eta_secs: None,
            tables_live: 0,
            tables_total: 1,
            transactions_processed: 0,
            current_table: None,
            current_table_started_at: None,
            current_table_phase: None,
            current_table_bytes_received: None,
            last_byte_at: None,
            current_table_seed_rows: None,
            current_table_seed_rows_applied: None,
            last_seed_apply_at: None,
        }
    }

    #[test]
    fn accepts_clients_when_not_mirroring() {
        assert!(public_mirror_accepts_clients(&MirrorsResponse::default()));
    }

    #[test]
    fn rejects_clients_until_all_mirrors_live() {
        let statuses = MirrorsResponse {
            mirrors: vec![
                snap(MirrorConnectivity::Live),
                snap(MirrorConnectivity::Subscribing),
            ],
        };
        assert!(!public_mirror_accepts_clients(&statuses));

        let statuses = MirrorsResponse {
            mirrors: vec![snap(MirrorConnectivity::Live), snap(MirrorConnectivity::Live)],
        };
        assert!(public_mirror_accepts_clients(&statuses));
    }
}
