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
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct MirrorsResponse {
    pub mirrors: Vec<MirrorStatusSnapshot>,
}

pub async fn mirrors<S: NodeDelegate>(State(ctx): State<S>) -> impl IntoResponse {
    Json(ctx.mirror_statuses())
}

pub fn router<S>() -> axum::Router<S>
where
    S: NodeDelegate + Clone + 'static,
{
    use axum::routing::get;
    axum::Router::new().route("/", get(mirrors::<S>))
}
