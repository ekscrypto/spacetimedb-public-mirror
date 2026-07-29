// SPDX-License-Identifier: MIT

//! Optional cross-process subscribe gate via relay-coordinator Unix socket.
//!
//! When `--coordinator-socket` is set, each mirror acquires a reconnect
//! permit before upstream subscribe setup and holds it until live —
//! serialising initial sync across per-instance processes the same way
//! the old relay fleet did.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const GRANT_TIMEOUT: Duration = Duration::from_secs(3600);

#[derive(Clone, Debug)]
pub struct CoordinatorClient {
    socket_path: PathBuf,
    relay_id: String,
}

impl CoordinatorClient {
    pub fn new(socket_path: impl Into<PathBuf>, relay_id: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            relay_id: relay_id.into(),
        }
    }

    /// Blocks until granted. Returns `None` when the coordinator is absent
    /// (mirror proceeds uncoordinated — same graceful degradation as relay).
    pub async fn acquire(&self) -> Option<CoordinatorPermit> {
        match self.try_acquire().await {
            Ok(permit) => {
                log::info!(
                    "public-mirror: coordinator permit granted for `{}`",
                    self.relay_id
                );
                Some(permit)
            }
            Err(e) => {
                log::warn!(
                    "public-mirror: coordinator unreachable for `{}` — proceeding without permit: {e:#}",
                    self.relay_id
                );
                None
            }
        }
    }

    async fn try_acquire(&self) -> Result<CoordinatorPermit> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, UnixStream::connect(&self.socket_path))
            .await
            .map_err(|_| anyhow::anyhow!("coordinator connect timeout"))??;

        let (reader, mut writer) = stream.into_split();
        let msg = format!("{{\"relay_id\":{}}}\n", serde_json::json!(self.relay_id));
        writer.write_all(msg.as_bytes()).await?;

        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        tokio::time::timeout(GRANT_TIMEOUT, reader.read_line(&mut line))
            .await
            .map_err(|_| anyhow::anyhow!("coordinator grant timeout"))??;

        if line.trim().is_empty() {
            anyhow::bail!("coordinator closed before granting");
        }

        Ok(CoordinatorPermit { _writer: writer })
    }
}

pub fn default_socket_path() -> PathBuf {
    PathBuf::from("/run/relay/coordinator.sock")
}

pub fn socket_exists(path: &Path) -> bool {
    path.exists()
}

/// RAII coordinator slot — drop releases the permit for the next mirror.
pub struct CoordinatorPermit {
    _writer: tokio::net::unix::OwnedWriteHalf,
}

impl Drop for CoordinatorPermit {
    fn drop(&mut self) {
        log::info!("public-mirror: coordinator permit released");
    }
}
