//! Per-mirror connectivity status for `GET /v1/mirrors`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use url::Url;

pub use spacetimedb_client_api::routes::mirrors::{
    MirrorConnectivity, MirrorStatusSnapshot, MirrorsResponse, SubscribePhase,
};

/// Shared socket byte counter attached for one upstream WebSocket session.
#[derive(Debug, Clone)]
pub struct ByteCounter {
    /// Cumulative bytes observed by [`crate::byte_count::ByteCountStream`].
    pub bytes_total: Arc<AtomicU64>,
    /// Unix millis of last non-zero `poll_read`, or 0 if none yet.
    pub last_byte_unix_ms: Arc<AtomicU64>,
}

impl ByteCounter {
    pub fn new() -> Self {
        Self {
            bytes_total: Arc::new(AtomicU64::new(0)),
            last_byte_unix_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn bytes_total(&self) -> u64 {
        self.bytes_total.load(Ordering::Relaxed)
    }

    pub fn last_byte_at(&self) -> Option<SystemTime> {
        let ms = self.last_byte_unix_ms.load(Ordering::Relaxed);
        if ms == 0 {
            None
        } else {
            Some(UNIX_EPOCH + Duration::from_millis(ms))
        }
    }

    pub fn record_read(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.bytes_total.fetch_add(n, Ordering::Relaxed);
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        self.last_byte_unix_ms.store(ms, Ordering::Relaxed);
    }
}

impl Default for ByteCounter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct MirrorStatusInner {
    host: String,
    database: String,
    connectivity: MirrorConnectivity,
    connected_since: Option<SystemTime>,
    disconnected_since: Option<SystemTime>,
    next_attempt_at: Option<SystemTime>,
    tables_live: u32,
    tables_total: u32,
    transactions_processed: u64,
    current_table: Option<String>,
    current_table_started_at: Option<SystemTime>,
    current_table_phase: Option<SubscribePhase>,
    /// Bytes_total snapshot when the current table subscribe was sent.
    current_table_bytes_baseline: u64,
    current_table_seed_rows: Option<u64>,
    byte_counter: Option<ByteCounter>,
}

impl MirrorStatusInner {
    fn clear_current_table(&mut self) {
        self.current_table = None;
        self.current_table_started_at = None;
        self.current_table_phase = None;
        self.current_table_bytes_baseline = 0;
        self.current_table_seed_rows = None;
    }

    fn snapshot(&self, now: SystemTime) -> MirrorStatusSnapshot {
        let (next_attempt_at, next_attempt_eta_secs) = match self.connectivity {
            MirrorConnectivity::Disconnected => {
                let at = self.next_attempt_at;
                let eta = at.map(|t| t.duration_since(now).unwrap_or(Duration::ZERO).as_secs());
                (at.map(format_rfc3339), eta)
            }
            _ => (None, None),
        };
        let (connected_since, disconnected_since) = match self.connectivity {
            MirrorConnectivity::Disconnected => (None, self.disconnected_since.map(format_rfc3339)),
            MirrorConnectivity::Waiting => (None, None),
            MirrorConnectivity::Connecting => (self.connected_since.map(format_rfc3339), None),
            MirrorConnectivity::Subscribing | MirrorConnectivity::Live => {
                (self.connected_since.map(format_rfc3339), None)
            }
        };

        let (current_table_bytes_received, last_byte_at) = if self.current_table.is_some() {
            let bytes = self.byte_counter.as_ref().map(|c| {
                c.bytes_total()
                    .saturating_sub(self.current_table_bytes_baseline)
            });
            let last = self
                .byte_counter
                .as_ref()
                .and_then(|c| c.last_byte_at())
                .map(format_rfc3339);
            (bytes, last)
        } else {
            (None, None)
        };

        MirrorStatusSnapshot {
            host: self.host.clone(),
            database: self.database.clone(),
            connectivity: self.connectivity,
            connected_since,
            disconnected_since,
            next_attempt_at,
            next_attempt_eta_secs,
            tables_live: self.tables_live,
            tables_total: self.tables_total,
            transactions_processed: self.transactions_processed,
            current_table: self.current_table.clone(),
            current_table_started_at: self.current_table_started_at.map(format_rfc3339),
            current_table_phase: self.current_table_phase,
            current_table_bytes_received,
            last_byte_at,
            current_table_seed_rows: self.current_table_seed_rows,
        }
    }
}

/// Shared registry of all mirrors in this process.
#[derive(Debug, Default)]
pub struct MirrorStatusRegistry {
    mirrors: Mutex<Vec<Arc<Mutex<MirrorStatusInner>>>>,
}

impl MirrorStatusRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a mirror and return a handle for the upstream loop to update.
    pub fn register(&self, host: &Url, database: impl Into<String>, tables_total: u32) -> MirrorStatusHandle {
        let database = database.into();
        let inner = Arc::new(Mutex::new(MirrorStatusInner {
            host: host_origin(host),
            database,
            connectivity: MirrorConnectivity::Disconnected,
            connected_since: None,
            disconnected_since: Some(SystemTime::now()),
            next_attempt_at: Some(SystemTime::now()),
            tables_live: 0,
            tables_total,
            transactions_processed: 0,
            current_table: None,
            current_table_started_at: None,
            current_table_phase: None,
            current_table_bytes_baseline: 0,
            current_table_seed_rows: None,
            byte_counter: None,
        }));
        self.mirrors
            .lock()
            .expect("mirror status registry poisoned")
            .push(Arc::clone(&inner));
        MirrorStatusHandle { inner }
    }

    pub fn snapshot(&self) -> MirrorsResponse {
        let now = SystemTime::now();
        let mirrors = self
            .mirrors
            .lock()
            .expect("mirror status registry poisoned")
            .iter()
            .map(|m| m.lock().expect("mirror status poisoned").snapshot(now))
            .collect();
        MirrorsResponse { mirrors }
    }
}

/// Per-mirror updater used by the upstream apply loop.
#[derive(Debug, Clone)]
pub struct MirrorStatusHandle {
    inner: Arc<Mutex<MirrorStatusInner>>,
}

impl MirrorStatusHandle {
    fn with_mut<R>(&self, f: impl FnOnce(&mut MirrorStatusInner) -> R) -> R {
        f(&mut self.inner.lock().expect("mirror status poisoned"))
    }

    /// Attach (or replace) the per-session socket byte counter.
    pub fn attach_byte_counter(&self, counter: ByteCounter) {
        self.with_mut(|s| {
            s.byte_counter = Some(counter);
        });
    }

    /// Queued behind the subscribe gate (another mirror is still seeding).
    pub fn set_waiting(&self) {
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Waiting;
            s.connected_since = None;
            s.disconnected_since = None;
            s.next_attempt_at = None;
            s.tables_live = 0;
            s.clear_current_table();
        });
    }

    /// About to open (or re-open) the upstream WebSocket.
    pub fn set_connecting(&self) {
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Connecting;
            s.connected_since = None;
            s.disconnected_since = None;
            s.next_attempt_at = None;
            s.tables_live = 0;
            s.clear_current_table();
        });
    }

    /// Upstream WebSocket is established (IdentityToken received or shortly after connect).
    pub fn set_connected(&self) {
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Connecting;
            if s.connected_since.is_none() {
                s.connected_since = Some(SystemTime::now());
            }
            s.disconnected_since = None;
            s.next_attempt_at = None;
        });
    }

    /// Begin awaiting seed for `table` (baselines byte counter).
    pub fn set_subscribing_table(&self, table: impl Into<String>) {
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Subscribing;
            s.disconnected_since = None;
            s.next_attempt_at = None;
            let baseline = s.byte_counter.as_ref().map(|c| c.bytes_total()).unwrap_or(0);
            s.current_table = Some(table.into());
            s.current_table_started_at = Some(SystemTime::now());
            s.current_table_phase = Some(SubscribePhase::AwaitingSeed);
            s.current_table_bytes_baseline = baseline;
            s.current_table_seed_rows = None;
        });
    }

    /// Seed message decoded; applying into the local DB.
    pub fn set_applying_seed(&self, seed_rows: u64) {
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Subscribing;
            s.current_table_phase = Some(SubscribePhase::ApplyingSeed);
            s.current_table_seed_rows = Some(seed_rows);
        });
    }

    /// `tables_live` tables have completed SubscribeMultiApplied (1..=tables_total).
    pub fn set_table_live(&self, tables_live: u32) {
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Subscribing;
            s.tables_live = tables_live;
            s.disconnected_since = None;
            s.next_attempt_at = None;
            s.clear_current_table();
        });
    }

    /// All tables subscribed; live update loop running.
    pub fn set_live(&self) {
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Live;
            s.tables_live = s.tables_total;
            s.disconnected_since = None;
            s.next_attempt_at = None;
            s.clear_current_table();
        });
    }

    /// Session ended; sleeping until `next_attempt_at`.
    pub fn set_disconnected(&self, next_attempt_at: SystemTime) {
        let now = SystemTime::now();
        self.with_mut(|s| {
            s.connectivity = MirrorConnectivity::Disconnected;
            s.connected_since = None;
            s.disconnected_since = Some(now);
            s.next_attempt_at = Some(next_attempt_at);
            s.clear_current_table();
            s.byte_counter = None;
        });
    }

    /// Lifetime counter: one committed upstream TransactionUpdate successfully applied.
    pub fn inc_transactions(&self) {
        self.with_mut(|s| {
            s.transactions_processed = s.transactions_processed.saturating_add(1);
        });
    }

    /// Update tables_total after the subscribe set is resolved (e.g. all public tables).
    pub fn set_tables_total(&self, tables_total: u32) {
        self.with_mut(|s| {
            s.tables_total = tables_total;
        });
    }
}

/// Scheme + host[:port] from an upstream URL (no path / database name).
pub fn host_origin(url: &Url) -> String {
    let mut out = String::new();
    out.push_str(url.scheme());
    out.push_str("://");
    match url.host_str() {
        Some(h) => out.push_str(h),
        None => out.push_str("localhost"),
    }
    if let Some(port) = url.port() {
        out.push(':');
        out.push_str(&port.to_string());
    }
    out
}

fn format_rfc3339(t: SystemTime) -> String {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = dur.as_secs() as i64;
    let millis = dur.subsec_millis();
    // Manual RFC3339 UTC — avoids a chrono dependency in this crate.
    let (year, month, day, hour, min, sec) = civil_from_days(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z")
}

/// Convert Unix seconds to (year, month, day, hour, min, sec) UTC.
fn civil_from_days(unix_secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = unix_secs.div_euclid(86_400);
    let tod = unix_secs.rem_euclid(86_400) as u32;
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;

    // Howard Hinnant civil_from_days algorithm.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (y, m, d, hour, min, sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_origin_strips_path() {
        let u = Url::parse("wss://ea.example/bitcraft-live-1").unwrap();
        assert_eq!(host_origin(&u), "wss://ea.example");
        // url::Url omits default HTTPS port 443 from .port(); use an explicit non-default port.
        let u = Url::parse("https://other.host:8443/prefix/db").unwrap();
        assert_eq!(host_origin(&u), "https://other.host:8443");
    }

    #[test]
    fn register_two_mirrors_and_drive_phases() {
        let reg = MirrorStatusRegistry::new();
        let host = Url::parse("wss://ea.example").unwrap();
        let a = reg.register(&host, "bitcraft-live-1", 2);
        let b = reg.register(&host, "bitcraft-live-global", 12);

        a.set_connecting();
        a.set_connected();
        a.set_table_live(1);
        a.set_table_live(2);
        a.set_live();
        a.inc_transactions();
        a.inc_transactions();

        let next = SystemTime::now() + Duration::from_secs(8);
        b.set_disconnected(next);

        let snap = reg.snapshot();
        assert_eq!(snap.mirrors.len(), 2);

        let m0 = &snap.mirrors[0];
        assert_eq!(m0.database, "bitcraft-live-1");
        assert_eq!(m0.connectivity, MirrorConnectivity::Live);
        assert!(m0.connected_since.is_some());
        assert!(m0.disconnected_since.is_none());
        assert!(m0.next_attempt_at.is_none());
        assert_eq!(m0.tables_live, 2);
        assert_eq!(m0.tables_total, 2);
        assert_eq!(m0.transactions_processed, 2);
        assert!(m0.current_table.is_none());

        let m1 = &snap.mirrors[1];
        assert_eq!(m1.connectivity, MirrorConnectivity::Disconnected);
        assert!(m1.disconnected_since.is_some());
        assert!(m1.next_attempt_at.is_some());
        assert!(m1.next_attempt_eta_secs.is_some());
        assert!(m1.next_attempt_eta_secs.unwrap() <= 8);
        assert!(m1.connected_since.is_none());
        assert_eq!(m1.tables_total, 12);
        assert_eq!(m1.transactions_processed, 0);

        let json = serde_json::to_value(&snap).unwrap();
        assert!(json["mirrors"][0]["connected_since"].is_string());
        assert!(json["mirrors"][1]["next_attempt_eta_secs"].is_number());
    }

    #[test]
    fn waiting_clears_on_connect() {
        let reg = MirrorStatusRegistry::new();
        let host = Url::parse("wss://ea.example").unwrap();
        let h = reg.register(&host, "db", 3);
        h.set_waiting();
        assert_eq!(reg.snapshot().mirrors[0].connectivity, MirrorConnectivity::Waiting);
        assert_eq!(reg.snapshot().mirrors[0].tables_live, 0);
        h.set_connecting();
        assert_eq!(reg.snapshot().mirrors[0].connectivity, MirrorConnectivity::Connecting);
    }

    #[test]
    fn transactions_accumulate_across_reconnect() {
        let reg = MirrorStatusRegistry::new();
        let host = Url::parse("wss://ea.example").unwrap();
        let h = reg.register(&host, "db", 1);
        h.set_live();
        h.inc_transactions();
        h.set_disconnected(SystemTime::now() + Duration::from_secs(1));
        h.set_connecting();
        h.set_connected();
        h.set_live();
        h.inc_transactions();
        assert_eq!(reg.snapshot().mirrors[0].transactions_processed, 2);
    }

    #[test]
    fn empty_registry() {
        let reg = MirrorStatusRegistry::new();
        let snap = reg.snapshot();
        assert!(snap.mirrors.is_empty());
        assert_eq!(serde_json::to_string(&snap).unwrap(), r#"{"mirrors":[]}"#);
    }

    #[test]
    fn subscribe_progress_exposes_bytes_and_phase() {
        let reg = MirrorStatusRegistry::new();
        let host = Url::parse("wss://ea.example").unwrap();
        let h = reg.register(&host, "db", 10);
        let counter = ByteCounter::new();
        h.attach_byte_counter(counter.clone());
        h.set_connecting();
        h.set_connected();
        h.set_subscribing_table("building_state");
        counter.record_read(1000);
        counter.record_read(500);

        let m = &reg.snapshot().mirrors[0];
        assert_eq!(m.connectivity, MirrorConnectivity::Subscribing);
        assert_eq!(m.current_table.as_deref(), Some("building_state"));
        assert_eq!(m.current_table_phase, Some(SubscribePhase::AwaitingSeed));
        assert_eq!(m.current_table_bytes_received, Some(1500));
        assert!(m.current_table_started_at.is_some());
        assert!(m.last_byte_at.is_some());
        assert!(m.current_table_seed_rows.is_none());

        h.set_applying_seed(42_000);
        let m = &reg.snapshot().mirrors[0];
        assert_eq!(m.current_table_phase, Some(SubscribePhase::ApplyingSeed));
        assert_eq!(m.current_table_seed_rows, Some(42_000));
        // Bytes keep reporting while applying.
        assert_eq!(m.current_table_bytes_received, Some(1500));

        h.set_table_live(1);
        let m = &reg.snapshot().mirrors[0];
        assert_eq!(m.tables_live, 1);
        assert!(m.current_table.is_none());
        assert!(m.current_table_bytes_received.is_none());

        let json = serde_json::to_value(reg.snapshot()).unwrap();
        // After clear, progress fields are omitted.
        assert!(json["mirrors"][0].get("current_table").is_none());
    }
}
