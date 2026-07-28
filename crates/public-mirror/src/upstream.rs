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
use tokio::sync::OwnedSemaphorePermit;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use url::Url;

use crate::status::MirrorStatusHandle;

const SUBPROTOCOL_V1: &str = "v1.bsatn.spacetimedb";

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

type ApplyFn = Arc<dyn Fn(UpstreamUpdate) -> BoxFuture<'static, Result<(), anyhow::Error>> + Send + Sync>;

/// Connect to upstream v1, sequentially subscribe to each table, apply seeds and live updates.
///
/// When the live update loop begins, `live_started` is set to `Instant::now()` so the
/// caller can measure how long the session was actually live (for reconnect backoff).
/// It is left `None` if connect/subscribe fails before the live loop.
///
/// `subscribe_permit`, when present, is dropped as soon as all tables are subscribed
/// (entering live) so another mirror can begin its seed. On connect/subscribe failure
/// the permit is dropped by RAII when this function returns.
pub async fn connect_and_mirror(
    config: UpstreamConfig,
    module_def: &ModuleDef,
    tables: &[String],
    on_update: ApplyFn,
    live_started: &mut Option<tokio::time::Instant>,
    status: MirrorStatusHandle,
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
    let connect_fut = tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false);
    let (mut sock, _response) = tokio::time::timeout(config.connect_timeout, connect_fut)
        .await
        .map_err(|_| UpstreamError::Connect("connect timeout".into()))?
        .map_err(|e| UpstreamError::Connect(e.to_string()))?;

    log::info!("public-mirror: upstream websocket established");
    status.set_connected();

    // Wait for IdentityToken before subscribing.
    loop {
        let msg = next_binary(&mut sock).await?;
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
                log::debug!("public-mirror: ignoring pre-subscribe message: {}", variant_name(&other));
            }
        }
    }

    for (idx, table) in tables.iter().enumerate() {
        let request_id = (idx as u32).saturating_add(1);
        let query_id = request_id;
        let query = format!("SELECT * FROM {table}");
        let frame = encode_subscribe_multi(request_id, query_id, &query)?;
        log::info!(
            "public-mirror: subscribe [{}/{}] {table} (request_id={request_id})",
            idx + 1,
            tables.len()
        );
        sock.send(Message::Binary(frame.into())).await?;

        // Wait for SubscribeMultiApplied for this query.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(UpstreamError::SubscribeTimeout(table.clone()));
            }
            let msg = tokio::time::timeout(remaining, next_binary(&mut sock))
                .await
                .map_err(|_| UpstreamError::SubscribeTimeout(table.clone()))??;
            let server = decode_server_message(&msg)?;
            match server {
                ServerMessage::SubscribeMultiApplied(sma) => {
                    if sma.query_id.id != query_id {
                        log::warn!(
                            "public-mirror: unexpected SubscribeMultiApplied query_id={} (want {query_id})",
                            sma.query_id.id
                        );
                        continue;
                    }
                    let tables_ops = database_update_to_ops(&sma.update, &row_types, /*seed*/ true)?;
                    let n_rows: usize = tables_ops.iter().map(|t| t.inserts.len()).sum();
                    log::info!(
                        "public-mirror: SubscribeMultiApplied for {table} ({n_rows} seed rows)"
                    );
                    if !tables_ops.is_empty() {
                        on_update(UpstreamUpdate {
                            provenance: None,
                            tables: tables_ops,
                        })
                        .await
                        .map_err(|e| UpstreamError::Decode(format!("apply seed failed: {e:#}")))?;
                    }
                    status.set_table_live((idx as u32).saturating_add(1));
                    break;
                }
                ServerMessage::SubscriptionError(err) => {
                    return Err(UpstreamError::Subscription(err.error.to_string()));
                }
                ServerMessage::TransactionUpdate(_) | ServerMessage::TransactionUpdateLight(_) => {
                    // Live updates can arrive interleaved once some tables are subscribed.
                    handle_live_update(server, &row_types, &on_update, &status).await?;
                }
                ServerMessage::IdentityToken(_) => {}
                other => {
                    log::debug!(
                        "public-mirror: ignoring while awaiting subscribe: {}",
                        variant_name(&other)
                    );
                }
            }
        }
    }

    log::info!(
        "public-mirror: all {} tables subscribed; entering live update loop",
        tables.len()
    );
    // Release the subscribe gate so the next queued mirror can start seeding.
    drop(subscribe_permit.take());
    status.set_live();
    *live_started = Some(tokio::time::Instant::now());

    // Live loop.
    let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
                if let Err(e) = sock.send(Message::Ping(Bytes::new())).await {
                    return Err(e.into());
                }
            }
        }
    }
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
            on_update(UpstreamUpdate { provenance, tables })
                .await
                .map_err(|e| UpstreamError::Decode(format!("apply update failed: {e:#}")))?;
            status.inc_transactions();
        }
        ServerMessage::TransactionUpdateLight(tul) => {
            let tables = database_update_to_ops(&tul.update, row_types, false)?;
            if tables.is_empty() {
                return Ok(());
            }
            on_update(UpstreamUpdate {
                provenance: None,
                tables,
            })
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

async fn next_binary(
    sock: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) -> Result<Vec<u8>, UpstreamError> {
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
