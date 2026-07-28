//! v1.bsatn.spacetimedb WebSocket upstream client.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};
use spacetimedb_client_api_messages::websocket::common::QuerySetId;
use spacetimedb_client_api_messages::websocket::v1::{
    BsatnFormat, ClientMessage, CompressableQueryUpdate, DatabaseUpdate, QueryUpdate, ServerMessage, SubscribeMulti,
    UpdateStatus,
};
use spacetimedb_lib::{bsatn, ConnectionId, Identity, ProductValue, Timestamp};
use spacetimedb_sats::{ProductType, WithTypespace};
use spacetimedb_schema::def::ModuleDef;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{client_async_tls_with_config, WebSocketStream};
use url::Url;

use crate::byte_count::ByteCountStream;
use crate::status::{ByteCounter, MirrorStatusHandle};

const SUBPROTOCOL_V1: &str = "v1.bsatn.spacetimedb";

/// Client-initiated WS Ping interval. Matches relay-upstream / relay-cache wire:
/// keep the write path polled so tungstenite auto-Pongs flush during multi-GiB
/// fragmented-message reassembly (un-split socket — never `futures::split`).
const CLIENT_PING_INTERVAL: Duration = Duration::from_secs(10);

/// Per-table wait for `SubscribeMultiApplied`. Large BitCraft tables
/// (`location_state` ~1 GiB+) can take many minutes to finish on the wire.
const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Seed frames at/above this size are BSATN-decoded on the blocking pool so a
/// multi-second CPU decode cannot stall other mirrors' live WebSocket tasks on
/// the shared Tokio runtime. Live transaction updates stay well under this.
const OFFLOAD_SEED_DECODE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    pub host: Url,
    pub database: String,
    pub auth_token: Option<String>,
    pub connect_timeout: Duration,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            host: Url::parse("wss://localhost").expect("valid url"),
            database: String::new(),
            auth_token: None,
            connect_timeout: Duration::from_secs(60),
        }
    }
}

/// Upstream reducer provenance from a committed v1 `TransactionUpdate`.
#[derive(Debug, Clone)]
pub struct UpstreamProvenance {
    pub reducer_name: String,
    pub caller_identity: Identity,
    pub caller_connection_id: ConnectionId,
    pub timestamp: Timestamp,
    pub request_id: u32,
    pub args: Bytes,
}

/// Decoded row ops for one table.
#[derive(Debug, Clone)]
pub struct UpstreamTableOps {
    pub table_name: String,
    pub deletes: Vec<ProductValue>,
    pub inserts: Vec<Bytes>,
}

/// One applyable update from upstream (seed snapshot or live TU).
#[derive(Debug, Clone)]
pub struct UpstreamUpdate {
    /// `None` for subscribe-applied seed rows; `Some` for committed transaction updates.
    pub provenance: Option<UpstreamProvenance>,
    pub tables: Vec<UpstreamTableOps>,
}

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("invalid upstream url: {0}")]
    Url(String),
    #[error("connection failed: {0}")]
    Connect(String),
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("BSATN decode failed: {0}")]
    Decode(String),
    #[error("BSATN encode failed: {0}")]
    Encode(String),
    #[error("frame too short ({0} bytes)")]
    FrameTooShort(usize),
    #[error("unknown compression tag {0} (request compression=None)")]
    UnknownCompression(u8),
    #[error("compressed query update not supported (request compression=None)")]
    CompressedUpdate,
    #[error("subscription error: {0}")]
    Subscription(String),
    #[error("unknown table `{0}` in module def")]
    UnknownTable(String),
    #[error("row type for table `{0}` is not a product")]
    NotProduct(String),
    #[error("timed out waiting for SubscribeMultiApplied for `{0}`")]
    SubscribeTimeout(String),
    #[error("upstream closed: {0}")]
    Closed(String),
}

type ApplyFn = Arc<
    dyn Fn(
            UpstreamUpdate,
            Option<crate::status::SeedApplyProgress>,
        ) -> BoxFuture<'static, Result<(), anyhow::Error>>
        + Send
        + Sync,
>;

/// Connect to upstream v1, sequentially subscribe to each table, apply seeds and live updates.
///
/// When the live update loop begins, `live_started` is set to `Instant::now()` so the
/// caller can measure how long the session was actually live (for reconnect backoff).
/// It is left `None` if connect/subscribe fails before the live loop.
///
/// `subscribe_gate` limits concurrent **wire** seeds (and the initial connect). The permit
/// is released before each local seed apply so a multi-minute `location_state` insert
/// cannot block other mirrors from reconnecting. On failure the held permit (if any)
/// is dropped by RAII when this function returns.
///
/// **Live invariant:** once this session reaches `status.set_live()`, it never
/// reacquires the gate until disconnect. Another mirror's subscribe/seed must not
/// stall live applies — those run on this DB's JobCores thread, and large seed
/// decode is offloaded off the shared Tokio runtime.
pub async fn connect_and_mirror(
    config: UpstreamConfig,
    module_def: &ModuleDef,
    tables: &[String],
    on_update: ApplyFn,
    live_started: &mut Option<tokio::time::Instant>,
    status: MirrorStatusHandle,
    subscribe_gate: Arc<Semaphore>,
    mut subscribe_permit: Option<OwnedSemaphorePermit>,
) -> Result<(), UpstreamError> {
    *live_started = None;
    let row_types = build_row_types(module_def)?;
    let request = build_connect_request(&config)?;
    log::info!(
        "public-mirror: connecting to {} (database={}, auth_bytes={})",
        request.uri(),
        config.database,
        config.auth_token.as_ref().map(|t| t.len()).unwrap_or(0)
    );

    let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);

    let counter = ByteCounter::new();
    status.attach_byte_counter(counter.clone());

    let connect_fut = async {
        let host = request
            .uri()
            .host()
            .ok_or_else(|| UpstreamError::Connect("request URI missing host".into()))?;
        let port = request.uri().port_u16().unwrap_or_else(|| match request.uri().scheme_str() {
            Some("wss") | Some("https") => 443,
            _ => 80,
        });
        let addr = format!("{host}:{port}");
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| UpstreamError::Connect(format!("tcp connect {addr}: {e}")))?;
        let counted = ByteCountStream::new(tcp, counter.clone());
        client_async_tls_with_config(request, counted, Some(ws_config), None)
            .await
            .map_err(|e| UpstreamError::Connect(e.to_string()))
    };
    let (mut sock, _response) = tokio::time::timeout(config.connect_timeout, connect_fut)
        .await
        .map_err(|_| UpstreamError::Connect("connect timeout".into()))??;

    // Keep `sock` un-split for the whole session (same invariant as
    // spacetimedb-relay `relay-upstream` / bitcraft-relay `wire.rs`).
    // Splitting read/write means auto-Pongs queued during a multi-GiB
    // fragmented Binary never flush until the write half is polled —
    // upstream's ~30s ping timeout then RSTs the connection mid-seed.
    log::info!("public-mirror: upstream websocket established");
    status.set_connected();

    let mut ping_interval = tokio::time::interval(CLIENT_PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick.
    ping_interval.tick().await;

    // Wait for IdentityToken before subscribing.
    loop {
        tokio::select! {
            msg = next_binary(&mut sock) => {
                let msg = msg?;
                let server = decode_server_message(&msg)?;
                match server {
                    ServerMessage::IdentityToken(it) => {
                        log::info!(
                            "public-mirror: identity token received (identity={})",
                            it.identity
                        );
                        break;
                    }
                    other => {
                        log::debug!(
                            "public-mirror: ignoring pre-subscribe message: {}",
                            variant_name(&other)
                        );
                    }
                }
            }
            _ = ping_interval.tick() => {
                send_client_ping(&mut sock).await?;
            }
        }
    }

    for (idx, table) in tables.iter().enumerate() {
        // Hold the gate only for the wire seed. Release before local apply so a
        // multi-minute insert cannot park every reconnecting mirror behind us.
        // While queued, keep applying live TUs for tables already subscribed —
        // parking on acquire alone would freeze mid-subscribe progress.
        if subscribe_permit.is_none() {
            if subscribe_gate.available_permits() == 0 {
                log::info!(
                    "public-mirror: `{}` queued for subscribe slot before [{}/{}] {table} \
                     ({} tables already live)",
                    config.database,
                    idx + 1,
                    tables.len(),
                    idx
                );
                status.set_queued_for_subscribe_slot();
            }
            subscribe_permit = Some(
                acquire_subscribe_slot_keeping_alive(
                    &mut sock,
                    &subscribe_gate,
                    &row_types,
                    &on_update,
                    &status,
                    &mut ping_interval,
                )
                .await?,
            );
        }

        let request_id = (idx as u32).saturating_add(1);
        let query_id = request_id;
        let query = format!("SELECT * FROM {table}");
        let frame = encode_subscribe_multi(request_id, query_id, &query)?;
        log::info!(
            "public-mirror: subscribe [{}/{}] {table} (request_id={request_id})",
            idx + 1,
            tables.len()
        );
        status.set_subscribing_table(table.clone());
        sock.send(Message::Binary(frame.into())).await?;

        // Wait for SubscribeMultiApplied — may reassemble a multi-GiB Binary.
        // Keep client Pings flowing the whole time (un-split write flush).
        let deadline = tokio::time::Instant::now() + SUBSCRIBE_TIMEOUT;
        let seed = loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(UpstreamError::SubscribeTimeout(table.clone()));
            }
            tokio::select! {
                msg = next_binary(&mut sock) => {
                    let msg = msg?;
                    let outcome = decode_subscribe_wait_message(
                        msg,
                        query_id,
                        &row_types,
                        &mut sock,
                        &on_update,
                        &status,
                        &mut ping_interval,
                    )
                    .await?;
                    match outcome {
                        SubscribeWaitOutcome::Seed { tables_ops, n_rows, wire_bytes } => {
                            log::info!(
                                "public-mirror: SubscribeMultiApplied for {table} \
                                 ({n_rows} seed rows, {wire_bytes} wire bytes)"
                            );
                            break (tables_ops, n_rows);
                        }
                        SubscribeWaitOutcome::Handled => {}
                    }
                }
                _ = ping_interval.tick() => {
                    send_client_ping(&mut sock).await?;
                }
            }
        };

        // Free the slot before local apply — reconnecting mirrors can proceed.
        // Live mirrors never hold this gate; do not reacquire after set_live().
        drop(subscribe_permit.take());

        let (tables_ops, n_rows) = seed;
        let progress = status.set_applying_seed(n_rows as u64);
        if !tables_ops.is_empty() {
            // Apply can take a long time for huge seeds. Keep reading + pinging
            // so the upstream ping timeout does not RST mid-apply; queue any
            // interleaved live TUs until the seed commit finishes.
            let apply = on_update(
                UpstreamUpdate {
                    provenance: None,
                    tables: tables_ops,
                },
                Some(progress),
            );
            apply_seed_keeping_alive(&mut sock, apply, &row_types, &on_update, &status, &mut ping_interval)
                .await
                .map_err(|e| UpstreamError::Decode(format!("apply seed failed: {e:#}")))?;
        }
        status.set_table_live((idx as u32).saturating_add(1));
    }

    log::info!(
        "public-mirror: all {} tables subscribed; entering live update loop",
        tables.len()
    );
    // Live invariant: permit must be gone; this session will not touch the gate
    // again until disconnect → reconnect (which may wait disconnected).
    debug_assert!(
        subscribe_permit.is_none(),
        "subscribe gate must be released before live loop"
    );
    drop(subscribe_permit.take());
    status.set_live();
    *live_started = Some(tokio::time::Instant::now());

    // Live loop — same un-split + client Ping pattern. No subscribe_gate.
    loop {
        tokio::select! {
            msg = sock.next() => {
                let Some(msg) = msg else {
                    return Err(UpstreamError::Closed("stream ended".into()));
                };
                match msg? {
                    Message::Binary(data) => {
                        let server = decode_server_message(&data)?;
                        handle_live_update(server, &row_types, &on_update, &status).await?;
                    }
                    Message::Close(frame) => {
                        let reason = frame
                            .map(|f| format!("{}: {}", f.code, f.reason))
                            .unwrap_or_else(|| "no close frame".into());
                        return Err(UpstreamError::Closed(reason));
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Text(t) => {
                        log::warn!("public-mirror: unexpected text frame ({} bytes)", t.len());
                    }
                    Message::Frame(_) => {}
                }
            }
            _ = ping_interval.tick() => {
                send_client_ping(&mut sock).await?;
            }
        }
    }
}

async fn send_client_ping<S>(sock: &mut WebSocketStream<S>) -> Result<(), UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    sock.send(Message::Ping(Bytes::new()))
        .await
        .map_err(UpstreamError::from)
}

/// Wait for a subscribe-gate permit without freezing already-subscribed tables.
///
/// Mid-subscribe mirrors may already have `tables_live > 0`; parking solely on
/// `acquire` would stop applying interleaved live TUs and stall client pings.
async fn acquire_subscribe_slot_keeping_alive<S>(
    sock: &mut WebSocketStream<S>,
    gate: &Arc<Semaphore>,
    row_types: &HashMap<String, ProductType>,
    on_update: &ApplyFn,
    status: &MirrorStatusHandle,
    ping_interval: &mut tokio::time::Interval,
) -> Result<OwnedSemaphorePermit, UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let acquire = Arc::clone(gate).acquire_owned();
    futures::pin_mut!(acquire);
    loop {
        tokio::select! {
            biased;
            permit = &mut acquire => {
                return permit.map_err(|_| UpstreamError::Closed("subscribe gate closed".into()));
            }
            msg = next_binary(sock) => {
                let msg = msg?;
                let server = decode_server_message(&msg)?;
                match server {
                    ServerMessage::TransactionUpdate(_) | ServerMessage::TransactionUpdateLight(_) => {
                        handle_live_update(server, row_types, on_update, status).await?;
                    }
                    ServerMessage::SubscriptionError(err) => {
                        return Err(UpstreamError::Subscription(err.error.to_string()));
                    }
                    other => {
                        log::debug!(
                            "public-mirror: ignoring while queued for subscribe slot: {}",
                            variant_name(&other)
                        );
                    }
                }
            }
            _ = ping_interval.tick() => {
                send_client_ping(sock).await?;
            }
        }
    }
}

enum SubscribeWaitOutcome {
    Seed {
        tables_ops: Vec<UpstreamTableOps>,
        n_rows: usize,
        wire_bytes: usize,
    },
    /// Non-seed message already handled (live TU applied, ignored, or wrong query_id).
    Handled,
}

/// Decode one binary frame received while awaiting `SubscribeMultiApplied`.
///
/// Large frames are decoded on the blocking pool so multi-second BSATN work
/// cannot monopolize a shared Tokio worker (which would delay *other* mirrors
/// that are already `live`).
async fn decode_subscribe_wait_message<S>(
    msg: Vec<u8>,
    query_id: u32,
    row_types: &HashMap<String, ProductType>,
    sock: &mut WebSocketStream<S>,
    on_update: &ApplyFn,
    status: &MirrorStatusHandle,
    ping_interval: &mut tokio::time::Interval,
) -> Result<SubscribeWaitOutcome, UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let wire_bytes = msg.len();
    let row_types_for_decode = row_types.clone();
    let decode = move || -> Result<SubscribeWaitDecode, UpstreamError> {
        let server = decode_server_message(&msg)?;
        match server {
            ServerMessage::SubscribeMultiApplied(sma) => {
                if sma.query_id.id != query_id {
                    return Ok(SubscribeWaitDecode::WrongQueryId {
                        got: sma.query_id.id,
                        want: query_id,
                    });
                }
                let tables_ops = database_update_to_ops(&sma.update, &row_types_for_decode, /*seed*/ true)?;
                let n_rows = tables_ops.iter().map(|t| t.inserts.len()).sum();
                Ok(SubscribeWaitDecode::Seed {
                    tables_ops,
                    n_rows,
                    wire_bytes,
                })
            }
            other => Ok(SubscribeWaitDecode::Other(other)),
        }
    };

    let decoded = if wire_bytes >= OFFLOAD_SEED_DECODE_BYTES {
        let join = tokio::task::spawn_blocking(decode);
        await_blocking_keeping_alive(sock, join, row_types, on_update, status, ping_interval).await?
    } else {
        decode()?
    };

    match decoded {
        SubscribeWaitDecode::Seed {
            tables_ops,
            n_rows,
            wire_bytes,
        } => Ok(SubscribeWaitOutcome::Seed {
            tables_ops,
            n_rows,
            wire_bytes,
        }),
        SubscribeWaitDecode::WrongQueryId { got, want } => {
            log::warn!("public-mirror: unexpected SubscribeMultiApplied query_id={got} (want {want})");
            Ok(SubscribeWaitOutcome::Handled)
        }
        SubscribeWaitDecode::Other(server) => {
            match server {
                ServerMessage::SubscriptionError(err) => {
                    return Err(UpstreamError::Subscription(err.error.to_string()));
                }
                ServerMessage::TransactionUpdate(_) | ServerMessage::TransactionUpdateLight(_) => {
                    handle_live_update(server, row_types, on_update, status).await?;
                }
                ServerMessage::IdentityToken(_) => {}
                other => {
                    log::debug!(
                        "public-mirror: ignoring while awaiting subscribe: {}",
                        variant_name(&other)
                    );
                }
            }
            Ok(SubscribeWaitOutcome::Handled)
        }
    }
}

enum SubscribeWaitDecode {
    Seed {
        tables_ops: Vec<UpstreamTableOps>,
        n_rows: usize,
        wire_bytes: usize,
    },
    WrongQueryId {
        got: u32,
        want: u32,
    },
    Other(ServerMessage<BsatnFormat>),
}

/// Await a `spawn_blocking` join while keeping the un-split WebSocket alive.
async fn await_blocking_keeping_alive<S, T>(
    sock: &mut WebSocketStream<S>,
    join: tokio::task::JoinHandle<Result<T, UpstreamError>>,
    row_types: &HashMap<String, ProductType>,
    on_update: &ApplyFn,
    status: &MirrorStatusHandle,
    ping_interval: &mut tokio::time::Interval,
) -> Result<T, UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: Send + 'static,
{
    futures::pin_mut!(join);
    let mut queued: Vec<ServerMessage<BsatnFormat>> = Vec::new();
    let result = loop {
        tokio::select! {
            biased;
            result = &mut join => {
                break result.map_err(|e| UpstreamError::Decode(format!("seed decode join failed: {e}")))?;
            }
            msg = next_binary(sock) => {
                let msg = msg?;
                let server = decode_server_message(&msg)?;
                match server {
                    ServerMessage::TransactionUpdate(_) | ServerMessage::TransactionUpdateLight(_) => {
                        queued.push(server);
                    }
                    ServerMessage::SubscriptionError(err) => {
                        return Err(UpstreamError::Subscription(err.error.to_string()));
                    }
                    other => {
                        log::debug!(
                            "public-mirror: ignoring during seed decode: {}",
                            variant_name(&other)
                        );
                    }
                }
            }
            _ = ping_interval.tick() => {
                send_client_ping(sock).await?;
            }
        }
    };
    let value = result?;
    for server in queued {
        handle_live_update(server, row_types, on_update, status).await?;
    }
    Ok(value)
}

/// Await a seed apply while continuing to poll the un-split WebSocket.
///
/// Queues interleaved live transaction updates until the seed commit finishes,
/// then applies them in order. Client Pings keep auto-Pongs flushing.
async fn apply_seed_keeping_alive<S>(
    sock: &mut WebSocketStream<S>,
    apply: BoxFuture<'static, Result<(), anyhow::Error>>,
    row_types: &HashMap<String, ProductType>,
    on_update: &ApplyFn,
    status: &MirrorStatusHandle,
    ping_interval: &mut tokio::time::Interval,
) -> Result<(), anyhow::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    futures::pin_mut!(apply);
    let mut queued: Vec<ServerMessage<BsatnFormat>> = Vec::new();
    loop {
        tokio::select! {
            biased;
            result = &mut apply => {
                result?;
                break;
            }
            msg = next_binary(sock) => {
                let msg = msg.map_err(|e| anyhow::anyhow!("{e:#}"))?;
                let server = decode_server_message(&msg).map_err(|e| anyhow::anyhow!("{e:#}"))?;
                match server {
                    ServerMessage::TransactionUpdate(_) | ServerMessage::TransactionUpdateLight(_) => {
                        queued.push(server);
                    }
                    ServerMessage::SubscriptionError(err) => {
                        return Err(anyhow::anyhow!("subscription error during seed apply: {}", err.error));
                    }
                    other => {
                        log::debug!(
                            "public-mirror: ignoring during seed apply: {}",
                            variant_name(&other)
                        );
                    }
                }
            }
            _ = ping_interval.tick() => {
                send_client_ping(sock).await.map_err(|e| anyhow::anyhow!("{e:#}"))?;
            }
        }
    }
    for server in queued {
        handle_live_update(server, row_types, on_update, status).await?;
    }
    Ok(())
}

async fn handle_live_update(
    server: ServerMessage<BsatnFormat>,
    row_types: &HashMap<String, ProductType>,
    on_update: &ApplyFn,
    status: &MirrorStatusHandle,
) -> Result<(), UpstreamError> {
    match server {
        ServerMessage::TransactionUpdate(tu) => {
            let UpdateStatus::Committed(db) = tu.status else {
                return Ok(());
            };
            let provenance = Some(UpstreamProvenance {
                reducer_name: tu.reducer_call.reducer_name.to_string(),
                caller_identity: tu.caller_identity,
                caller_connection_id: tu.caller_connection_id,
                timestamp: tu.timestamp,
                request_id: tu.reducer_call.request_id,
                args: Bytes::copy_from_slice(tu.reducer_call.args.as_ref()),
            });
            let tables = database_update_to_ops(&db, row_types, false)?;
            if tables.is_empty() {
                return Ok(());
            }
            on_update(UpstreamUpdate { provenance, tables }, None)
                .await
                .map_err(|e| UpstreamError::Decode(format!("apply update failed: {e:#}")))?;
            status.inc_transactions();
        }
        ServerMessage::TransactionUpdateLight(tul) => {
            let tables = database_update_to_ops(&tul.update, row_types, false)?;
            if tables.is_empty() {
                return Ok(());
            }
            on_update(
                UpstreamUpdate {
                    provenance: None,
                    tables,
                },
                None,
            )
            .await
            .map_err(|e| UpstreamError::Decode(format!("apply light update failed: {e:#}")))?;
        }
        ServerMessage::SubscriptionError(err) => {
            return Err(UpstreamError::Subscription(err.error.to_string()));
        }
        ServerMessage::SubscribeMultiApplied(_)
        | ServerMessage::IdentityToken(_)
        | ServerMessage::InitialSubscription(_)
        | ServerMessage::SubscribeApplied(_)
        | ServerMessage::UnsubscribeApplied(_)
        | ServerMessage::UnsubscribeMultiApplied(_)
        | ServerMessage::OneOffQueryResponse(_)
        | ServerMessage::ProcedureResult(_) => {}
    }
    Ok(())
}

fn build_row_types(module_def: &ModuleDef) -> Result<HashMap<String, ProductType>, UpstreamError> {
    let mut map = HashMap::new();
    let typespace = module_def.typespace();
    for table in module_def.tables() {
        let name = table.name.to_string();
        let alg = typespace
            .get(table.product_type_ref)
            .ok_or_else(|| UpstreamError::UnknownTable(name.clone()))?;
        // Inline AlgebraicType::Ref… so ProductValue::decode (empty typespace) works.
        // BitCraft row types nest refs (e.g. timestamps / enums); leaving them unresolved
        // panics on the first live delete with "len is 0 but the index is N".
        let resolved = WithTypespace::new(typespace, alg).resolve_refs().map_err(|e| {
            UpstreamError::Decode(format!("resolve row type for {name}: {e}"))
        })?;
        let product = resolved
            .as_product()
            .ok_or_else(|| UpstreamError::NotProduct(name.clone()))?
            .clone();
        map.insert(name, product);
    }
    Ok(map)
}

fn database_update_to_ops(
    db: &DatabaseUpdate<BsatnFormat>,
    row_types: &HashMap<String, ProductType>,
    seed: bool,
) -> Result<Vec<UpstreamTableOps>, UpstreamError> {
    let mut out = Vec::with_capacity(db.tables.len());
    for table in &db.tables {
        let table_name = table.table_name.to_string();
        let row_ty = row_types
            .get(&table_name)
            .ok_or_else(|| UpstreamError::UnknownTable(table_name.clone()))?;

        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        for update in &table.updates {
            let qu = uncompressed(update)?;
            for row in &qu.inserts {
                inserts.push(Bytes::copy_from_slice(row.as_ref()));
            }
            if !seed {
                for row in &qu.deletes {
                    let mut bytes: &[u8] = row.as_ref();
                    let pv = ProductValue::decode(row_ty, &mut bytes)
                        .map_err(|e| UpstreamError::Decode(format!("delete row for {table_name}: {e}")))?;
                    deletes.push(pv);
                }
            }
        }
        if inserts.is_empty() && deletes.is_empty() {
            continue;
        }
        out.push(UpstreamTableOps {
            table_name,
            deletes,
            inserts,
        });
    }
    Ok(out)
}

fn uncompressed(u: &CompressableQueryUpdate<BsatnFormat>) -> Result<&QueryUpdate<BsatnFormat>, UpstreamError> {
    match u {
        CompressableQueryUpdate::Uncompressed(qu) => Ok(qu),
        CompressableQueryUpdate::Brotli(_) | CompressableQueryUpdate::Gzip(_) => Err(UpstreamError::CompressedUpdate),
    }
}

fn encode_subscribe_multi(request_id: u32, query_id: u32, query: &str) -> Result<Vec<u8>, UpstreamError> {
    let msg = ClientMessage::<Box<[u8]>>::SubscribeMulti(SubscribeMulti {
        query_strings: vec![query.to_string().into_boxed_str()].into_boxed_slice(),
        request_id,
        query_id: QuerySetId::new(query_id),
    });
    bsatn::to_vec(&msg).map_err(|e| UpstreamError::Encode(e.to_string()))
}

fn decode_server_message(data: &[u8]) -> Result<ServerMessage<BsatnFormat>, UpstreamError> {
    if data.is_empty() {
        return Err(UpstreamError::FrameTooShort(0));
    }
    let tag = data[0];
    // Compression tag: 0 = None.
    if tag != 0 {
        return Err(UpstreamError::UnknownCompression(tag));
    }
    if data.len() < 2 {
        return Err(UpstreamError::FrameTooShort(data.len()));
    }
    bsatn::from_slice::<ServerMessage<BsatnFormat>>(&data[1..])
        .map_err(|e| UpstreamError::Decode(e.to_string()))
}

async fn next_binary<S>(sock: &mut WebSocketStream<S>) -> Result<Vec<u8>, UpstreamError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let Some(msg) = sock.next().await else {
            return Err(UpstreamError::Closed("stream ended".into()));
        };
        match msg? {
            Message::Binary(data) => return Ok(data.to_vec()),
            Message::Close(frame) => {
                let reason = frame
                    .map(|f| format!("{}: {}", f.code, f.reason))
                    .unwrap_or_else(|| "no close frame".into());
                return Err(UpstreamError::Closed(reason));
            }
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Text(t) => {
                log::warn!("public-mirror: unexpected text frame ({} bytes)", t.len());
            }
            Message::Frame(_) => {}
        }
    }
}

fn build_connect_request(
    config: &UpstreamConfig,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, UpstreamError> {
    let mut url = config.host.clone();
    match url.scheme() {
        "ws" | "wss" => {}
        "http" => url
            .set_scheme("ws")
            .map_err(|_| UpstreamError::Url("scheme rewrite failed".into()))?,
        "https" => url
            .set_scheme("wss")
            .map_err(|_| UpstreamError::Url("scheme rewrite failed".into()))?,
        other => return Err(UpstreamError::Url(format!("unsupported scheme: {other}"))),
    }
    {
        let mut path = url.path().trim_end_matches('/').to_string();
        path.push_str("/v1/database/");
        path.push_str(&config.database);
        path.push_str("/subscribe");
        url.set_path(&path);
    }
    url.query_pairs_mut().clear().append_pair("compression", "None");

    let mut request = url
        .to_string()
        .into_client_request()
        .map_err(|e| UpstreamError::Url(e.to_string()))?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        SUBPROTOCOL_V1
            .parse()
            .map_err(|_| UpstreamError::Url("invalid subprotocol header".into()))?,
    );
    if let Some(token) = &config.auth_token {
        let value = format!("Bearer {token}");
        request.headers_mut().insert(
            AUTHORIZATION,
            value
                .parse()
                .map_err(|_| UpstreamError::Url("invalid auth header".into()))?,
        );
    }
    Ok(request)
}

fn variant_name(msg: &ServerMessage<BsatnFormat>) -> &'static str {
    match msg {
        ServerMessage::InitialSubscription(_) => "InitialSubscription",
        ServerMessage::TransactionUpdate(_) => "TransactionUpdate",
        ServerMessage::TransactionUpdateLight(_) => "TransactionUpdateLight",
        ServerMessage::IdentityToken(_) => "IdentityToken",
        ServerMessage::OneOffQueryResponse(_) => "OneOffQueryResponse",
        ServerMessage::SubscribeApplied(_) => "SubscribeApplied",
        ServerMessage::UnsubscribeApplied(_) => "UnsubscribeApplied",
        ServerMessage::SubscriptionError(_) => "SubscriptionError",
        ServerMessage::SubscribeMultiApplied(_) => "SubscribeMultiApplied",
        ServerMessage::UnsubscribeMultiApplied(_) => "UnsubscribeMultiApplied",
        ServerMessage::ProcedureResult(_) => "ProcedureResult",
    }
}
