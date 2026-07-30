//! Isolated `GET /v1/mirrors` listener for public-mirror mode.
//!
//! Runs on a dedicated OS thread with its own single-threaded Tokio runtime so
//! mirror readiness stays responsive while the main HTTP server is busy seeding.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use anyhow::Context;
use axum::routing::get;
use axum::{Json, Router};
use spacetimedb_public_mirror_client::MirrorStatusRegistry;
use tokio::net::TcpListener;

/// Default sidecar bind: loopback, main listen port + 1.
pub fn default_listen_addr(main_listen: &str) -> anyhow::Result<String> {
    let (_host, port_str) = main_listen
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("listen-addr must be host:port, got `{main_listen}`"))?;
    let port: u16 = port_str
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid port in listen-addr `{main_listen}`"))?;
    let status_port = port
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("mirror status port overflow (main port 65535)"))?;
    Ok(format!("127.0.0.1:{status_port}"))
}

/// Spawn the sidecar HTTP server. Returns immediately; the thread runs until process exit.
pub fn spawn(registry: Arc<MirrorStatusRegistry>, listen_addr: String) -> JoinHandle<()> {
    thread::Builder::new()
        .name("mirror-status-http".into())
        .spawn(move || {
            if let Err(e) = run(listen_addr, registry) {
                log::error!("mirror status HTTP server exited: {e:#}");
            }
        })
        .expect("spawn mirror-status-http thread")
}

fn run(listen_addr: String, registry: Arc<MirrorStatusRegistry>) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build mirror status tokio runtime")?;

    rt.block_on(async move {
        let app = Router::new()
            .route(
                "/v1/mirrors",
                get({
                    let registry = Arc::clone(&registry);
                    move || async move { Json(registry.snapshot()) }
                }),
            )
            .route("/health", get(|| async { "ok" }));

        let listener = TcpListener::bind(&listen_addr)
            .await
            .with_context(|| format!("bind mirror status listener on `{listen_addr}`"))?;
        log::info!(
            "mirror status HTTP listening on {} (isolated GET /v1/mirrors)",
            listener.local_addr()?
        );
        axum::serve(listener, app)
            .await
            .context("mirror status axum serve")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_listen_addr_is_loopback_port_plus_one() {
        assert_eq!(
            default_listen_addr("127.0.0.1:3030").unwrap(),
            "127.0.0.1:3031"
        );
        assert_eq!(
            default_listen_addr("0.0.0.0:3000").unwrap(),
            "127.0.0.1:3001"
        );
    }
}
