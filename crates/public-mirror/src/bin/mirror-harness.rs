//! Compatibility harness: compare TransactionUpdate provenance between upstream and local mirror.
//!
//! Usage:
//! ```text
//! mirror-harness \
//!   --upstream wss://bitcraft-early-access.spacetimedb.com \
//!   --database bitcraft-live-1 \
//!   --token "$BITCRAFT_TOKEN" \
//!   --mirror-url ws://127.0.0.1:3001 \
//!   --table player_username_state \
//!   --seconds 30
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use spacetimedb_client_api_messages::websocket::common::{QuerySetId, RowListLen};
use spacetimedb_client_api_messages::websocket::v1::{
    BsatnFormat, ClientMessage, CompressableQueryUpdate, DatabaseUpdate, ServerMessage, SubscribeMulti, UpdateStatus,
};
use spacetimedb_lib::bsatn;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

const SUBPROTOCOL_V1: &str = "v1.bsatn.spacetimedb";

#[derive(Parser, Debug)]
#[command(name = "mirror-harness", about = "Compare upstream vs local mirror TransactionUpdates")]
struct Args {
    /// Upstream SpacetimeDB host (ws/wss/http/https).
    #[arg(long)]
    upstream: String,

    /// Upstream database name.
    #[arg(long)]
    database: String,

    /// Auth bearer token (also BITCRAFT_TOKEN / MIRROR_TOKEN).
    #[arg(long, env = "BITCRAFT_TOKEN")]
    token: Option<String>,

    /// Local mirror listen URL (ws://127.0.0.1:3001).
    #[arg(long, default_value = "ws://127.0.0.1:3001")]
    mirror_url: String,

    /// Local mirror database name (defaults to --database).
    #[arg(long)]
    mirror_database: Option<String>,

    /// Tables to subscribe (repeatable). Defaults to a small set.
    #[arg(long = "table")]
    tables: Vec<String>,

    /// How long to collect updates (seconds).
    #[arg(long, default_value_t = 30)]
    seconds: u64,

    /// Only connect to the local mirror (skip upstream comparison). Useful to dump reducer provenance.
    #[arg(long, default_value_t = false)]
    mirror_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TuKey {
    reducer_name: String,
    request_id: u32,
    caller: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TuSample {
    insert_rows: usize,
    delete_rows: usize,
}

#[derive(Default)]
struct Collector {
    tus: HashMap<TuKey, TuSample>,
    tu_count: usize,
}

impl Collector {
    fn record(&mut self, key: TuKey, sample: TuSample) {
        self.tu_count += 1;
        let entry = self.tus.entry(key).or_default();
        entry.insert_rows = entry.insert_rows.saturating_add(sample.insert_rows);
        entry.delete_rows = entry.delete_rows.saturating_add(sample.delete_rows);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = Args::parse();
    if args.token.is_none() {
        args.token = std::env::var("MIRROR_TOKEN").ok();
    }
    if args.tables.is_empty() {
        args.tables = vec![
            "player_username_state".to_string(),
            "player_state".to_string(),
        ];
    }
    let mirror_db = args
        .mirror_database
        .clone()
        .unwrap_or_else(|| args.database.clone());

    let upstream_host: Url = args.upstream.parse()?;
    let mirror_host: Url = args.mirror_url.parse()?;

    let upstream = Arc::new(Mutex::new(Collector::default()));
    let mirror = Arc::new(Mutex::new(Collector::default()));

    if args.mirror_only {
        let collector = mirror.clone();
        let tables = args.tables.clone();
        let secs = args.seconds;
        run_client("mirror", mirror_host, &mirror_db, None, &tables, collector, secs).await?;

        let mir = mirror.lock().unwrap();
        println!("=== mirror-only provenance dump ===");
        println!("mirror TUs: {}", mir.tu_count);
        let mut samples: Vec<_> = mir.tus.keys().collect();
        samples.sort_by(|a, b| a.reducer_name.cmp(&b.reducer_name));
        println!("reducer provenance samples (up to 20):");
        for key in samples.into_iter().take(20) {
            let s = &mir.tus[key];
            println!(
                "  reducer={} request_id={} caller={} (+{}/-{})",
                key.reducer_name, key.request_id, key.caller, s.insert_rows, s.delete_rows
            );
        }
        if mir.tu_count == 0 {
            anyhow::bail!("no TransactionUpdates observed on mirror in {}s", args.seconds);
        }
        // Confirm non-empty reducer names (not anonymous / light updates only).
        let named = mir.tus.keys().filter(|k| !k.reducer_name.is_empty()).count();
        println!("named_reducers={named} distinct_keys={}", mir.tus.len());
        if named == 0 {
            anyhow::bail!("TransactionUpdates lacked reducer_name provenance");
        }
        println!("PASS (local mirror exposes reducer provenance)");
        return Ok(());
    }

    let up_task = {
        let collector = upstream.clone();
        let tables = args.tables.clone();
        let token = args.token.clone();
        let database = args.database.clone();
        let host = upstream_host;
        let secs = args.seconds;
        tokio::spawn(async move {
            run_client("upstream", host, &database, token.as_deref(), &tables, collector, secs).await
        })
    };
    let mir_task = {
        let collector = mirror.clone();
        let tables = args.tables.clone();
        // Local mirror issues its own identity; do not send the upstream BitCraft JWT
        // (signed for a different issuer / keypair → 401 Unauthorized).
        let secs = args.seconds;
        tokio::spawn(async move {
            run_client("mirror", mirror_host, &mirror_db, None, &tables, collector, secs).await
        })
    };

    let (up_res, mir_res) = tokio::join!(up_task, mir_task);
    up_res??;
    mir_res??;

    let up = upstream.lock().unwrap();
    let mir = mirror.lock().unwrap();

    println!("=== mirror-harness summary ===");
    println!("upstream TUs: {}", up.tu_count);
    println!("mirror   TUs: {}", mir.tu_count);

    // Show that local clients receive upstream reducer provenance on the mirror.
    let mut samples: Vec<_> = mir.tus.keys().collect();
    samples.sort_by(|a, b| a.reducer_name.cmp(&b.reducer_name));
    println!("mirror reducer provenance samples (up to 12):");
    for key in samples.into_iter().take(12) {
        let s = &mir.tus[key];
        println!(
            "  reducer={} request_id={} caller={} (+{}/-{})",
            key.reducer_name, key.request_id, key.caller, s.insert_rows, s.delete_rows
        );
    }

    let mut matched = 0usize;
    let mut mismatched = 0usize;
    let mut missing_on_mirror = 0usize;
    let mut extra_on_mirror = 0usize;

    for (key, up_sample) in &up.tus {
        match mir.tus.get(key) {
            Some(mir_sample) if mir_sample == up_sample => matched += 1,
            Some(mir_sample) => {
                mismatched += 1;
                println!(
                    "MISMATCH reducer={} request_id={} caller={}: upstream +{}/-{} vs mirror +{}/-{}",
                    key.reducer_name,
                    key.request_id,
                    key.caller,
                    up_sample.insert_rows,
                    up_sample.delete_rows,
                    mir_sample.insert_rows,
                    mir_sample.delete_rows
                );
            }
            None => {
                missing_on_mirror += 1;
                println!(
                    "MISSING on mirror: reducer={} request_id={} caller={} (+{}/-{})",
                    key.reducer_name, key.request_id, key.caller, up_sample.insert_rows, up_sample.delete_rows
                );
            }
        }
    }
    for key in mir.tus.keys() {
        if !up.tus.contains_key(key) {
            extra_on_mirror += 1;
            let s = &mir.tus[key];
            println!(
                "EXTRA on mirror: reducer={} request_id={} caller={} (+{}/-{})",
                key.reducer_name, key.request_id, key.caller, s.insert_rows, s.delete_rows
            );
        }
    }

    println!(
        "matched={matched} mismatched={mismatched} missing_on_mirror={missing_on_mirror} extra_on_mirror={extra_on_mirror}"
    );

    if mismatched == 0 && missing_on_mirror == 0 && extra_on_mirror == 0 && up.tu_count > 0 {
        println!("PASS");
        Ok(())
    } else if up.tu_count == 0 && mir.tu_count == 0 {
        println!("PASS (no TransactionUpdates observed in window)");
        Ok(())
    } else {
        println!("FAIL");
        anyhow::bail!("provenance / row-count comparison failed");
    }
}

async fn run_client(
    label: &str,
    host: Url,
    database: &str,
    token: Option<&str>,
    tables: &[String],
    collector: Arc<Mutex<Collector>>,
    seconds: u64,
) -> anyhow::Result<()> {
    let request = build_request(&host, database, token)?;
    eprintln!("{label}: connecting to {}", request.uri());
    let (mut sock, _) = tokio_tungstenite::connect_async(request).await?;

    // Drain IdentityToken.
    loop {
        let msg = next_binary(&mut sock).await?;
        let server = decode(&msg)?;
        if matches!(server, ServerMessage::IdentityToken(_)) {
            break;
        }
    }

    for (i, table) in tables.iter().enumerate() {
        let request_id = (i as u32) + 1;
        let query = format!("SELECT * FROM {table}");
        let frame = encode_subscribe(request_id, request_id, &query)?;
        sock.send(Message::Binary(frame.into())).await?;
        // Wait for SubscribeMultiApplied (ignore TUs during seed).
        loop {
            let msg = next_binary(&mut sock).await?;
            let server = decode(&msg)?;
            match server {
                ServerMessage::SubscribeMultiApplied(sma) if sma.query_id.id == request_id => break,
                ServerMessage::SubscriptionError(e) => anyhow::bail!("{label}: subscribe error: {}", e.error),
                ServerMessage::TransactionUpdate(tu) => {
                    if let UpdateStatus::Committed(db) = tu.status {
                        record_tu(&collector, &tu.reducer_call.reducer_name, tu.reducer_call.request_id, &tu.caller_identity.to_string(), &db);
                    }
                }
                _ => {}
            }
        }
        eprintln!("{label}: subscribed {table}");
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let msg = match tokio::time::timeout(remaining, next_binary(&mut sock)).await {
            Ok(Ok(m)) => m,
            Ok(Err(e)) => return Err(e),
            Err(_) => break,
        };
        let server = decode(&msg)?;
        if let ServerMessage::TransactionUpdate(tu) = server {
            if let UpdateStatus::Committed(db) = tu.status {
                record_tu(
                    &collector,
                    &tu.reducer_call.reducer_name,
                    tu.reducer_call.request_id,
                    &tu.caller_identity.to_string(),
                    &db,
                );
            }
        }
    }
    eprintln!("{label}: collection window ended");
    Ok(())
}

fn record_tu(
    collector: &Mutex<Collector>,
    reducer_name: &str,
    request_id: u32,
    caller: &str,
    db: &DatabaseUpdate<BsatnFormat>,
) {
    let mut insert_rows = 0usize;
    let mut delete_rows = 0usize;
    for t in &db.tables {
        for u in &t.updates {
            if let CompressableQueryUpdate::Uncompressed(qu) = u {
                insert_rows += qu.inserts.len();
                delete_rows += qu.deletes.len();
            }
        }
    }
    collector.lock().unwrap().record(
        TuKey {
            reducer_name: reducer_name.to_string(),
            request_id,
            caller: caller.to_string(),
        },
        TuSample {
            insert_rows,
            delete_rows,
        },
    );
}

fn encode_subscribe(request_id: u32, query_id: u32, query: &str) -> anyhow::Result<Vec<u8>> {
    let msg = ClientMessage::<Box<[u8]>>::SubscribeMulti(SubscribeMulti {
        query_strings: vec![query.to_string().into_boxed_str()].into_boxed_slice(),
        request_id,
        query_id: QuerySetId::new(query_id),
    });
    Ok(bsatn::to_vec(&msg)?)
}

fn decode(data: &[u8]) -> anyhow::Result<ServerMessage<BsatnFormat>> {
    anyhow::ensure!(!data.is_empty(), "empty frame");
    anyhow::ensure!(data[0] == 0, "unexpected compression tag {}", data[0]);
    Ok(bsatn::from_slice(&data[1..])?)
}

async fn next_binary(
    sock: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) -> anyhow::Result<Vec<u8>> {
    loop {
        let Some(msg) = sock.next().await else {
            anyhow::bail!("websocket closed");
        };
        match msg? {
            Message::Binary(b) => return Ok(b.to_vec()),
            Message::Close(_) => anyhow::bail!("websocket closed"),
            _ => {}
        }
    }
}

fn build_request(
    host: &Url,
    database: &str,
    token: Option<&str>,
) -> anyhow::Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let mut url = host.clone();
    match url.scheme() {
        "ws" | "wss" => {}
        "http" => {
            url.set_scheme("ws").map_err(|_| anyhow::anyhow!("scheme rewrite"))?;
        }
        "https" => {
            url.set_scheme("wss").map_err(|_| anyhow::anyhow!("scheme rewrite"))?;
        }
        other => anyhow::bail!("unsupported scheme {other}"),
    }
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/v1/database/");
    path.push_str(database);
    path.push_str("/subscribe");
    url.set_path(&path);
    url.query_pairs_mut().clear().append_pair("compression", "None");

    let mut request = url.as_str().into_client_request()?;
    request
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, SUBPROTOCOL_V1.parse()?);
    if let Some(token) = token {
        request
            .headers_mut()
            .insert(AUTHORIZATION, format!("Bearer {token}").parse()?);
    }
    Ok(request)
}
