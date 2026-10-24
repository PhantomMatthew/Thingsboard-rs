//! tb-transport-http：设备 HTTP 接入。
//! 上行：遥测/属性/RPC 经 transport-api 或 tb_rule_engine.Main；
//! 同步能力：GET attributes 走 transport-api 请求/响应；GET rpc 长轮询
//! （复用长轮询会话 → subscribe_rpc → 等本节点 notifications consumer 推 to_device_rpc；
//! 未取走的 RPC 留给后续轮询，会话空闲超时后清理）。

pub mod downlink;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use prost::Message as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tb_common::{KvEntry, KvValue, TbError, TsKvEntry};
use tb_proto::core as core_pb;
use tb_proto::transport as pb;
use tb_queue::{KafkaQueueProducer, QueueError, QueueProducer as _, TbTopic};
use tb_transport_api::TransportApiClient;
use tokio::sync::{Mutex, mpsc, watch};
use uuid::Uuid;

const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);
const RPC_SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const RPC_SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

pub struct RpcSession {
    pub device: pb::DeviceInfoProto,
    pub info: core_pb::SessionInfoProto,
    pub tx: mpsc::Sender<core_pb::ToDeviceRpcRequestMsg>,
    pub rx: Mutex<mpsc::Receiver<core_pb::ToDeviceRpcRequestMsg>>,
    pub last_activity_ms: AtomicI64,
    close_tx: watch::Sender<bool>,
    close_rx: watch::Receiver<bool>,
}

impl RpcSession {
    fn close(&self) {
        let _ = self.close_tx.send(true);
    }
}

/// device_id → 持久长轮询会话；接收队列跨 GET rpc 请求保留。
pub type RpcSessions = Arc<DashMap<String, Arc<RpcSession>>>;
/// session_id → device_id，供下行通知按会话路由。
pub type RpcSessionIds = Arc<DashMap<String, String>>;

pub struct RpcReplyRoute {
    pub session_id: String,
    pub reply_node_id: String,
    pub expiration_ts: i64,
}

pub type RpcReplyRoutes = Arc<DashMap<(String, i32), RpcReplyRoute>>;

#[derive(Clone)]
pub struct TransportState {
    pub transport_api: TransportApiClient,
    pub producer: KafkaQueueProducer,
    pub node_id: String,
    pub brokers: String,
    pub rpc_sessions: RpcSessions,
    pub rpc_session_ids: RpcSessionIds,
    pub rpc_reply_routes: RpcReplyRoutes,
    /// GET rpc 长轮询时长（测试可调小）。
    pub long_poll_timeout: Duration,
}

impl TransportState {
    pub fn new(brokers: &str, node_id: &str) -> Result<Self, QueueError> {
        Ok(Self {
            transport_api: TransportApiClient::new(brokers, node_id)?,
            producer: KafkaQueueProducer::new(brokers, &format!("tb-transport-http-{node_id}"))?,
            node_id: node_id.to_string(),
            brokers: brokers.to_string(),
            rpc_sessions: Arc::new(DashMap::new()),
            rpc_session_ids: Arc::new(DashMap::new()),
            rpc_reply_routes: Arc::new(DashMap::new()),
            long_poll_timeout: LONG_POLL_TIMEOUT,
        })
    }
}

pub fn build_router(state: TransportState) -> Router {
    tokio::spawn(sweep_rpc_sessions(state.clone()));
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/{token}/telemetry", post(post_telemetry))
        .route(
            "/api/v1/{token}/attributes",
            post(post_attributes).get(get_attributes),
        )
        .route("/api/v1/{token}/claim", post(claim))
        .route("/api/v1/{token}/rpc", post(post_rpc).get(get_rpc))
        .route("/api/v1/{token}/rpc/{requestId}", post(post_rpc_response))
        .route("/api/v1/{token}/firmware", get(get_firmware))
        .route("/api/v1/provision", post(provision))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.0, Json(json!({"message": self.1}))).into_response()
    }
}

impl From<TbError> for ApiError {
    fn from(e: TbError) -> Self {
        let status =
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        ApiError(status, e.to_string())
    }
}

/// 校验 token，失败映射 401。
async fn validate(
    state: &TransportState,
    token: &str,
) -> Result<tb_proto::transport::DeviceInfoProto, ApiError> {
    let resp = state.transport_api.validate_token(token).await?;
    if !resp.valid {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid access token".into(),
        ));
    }
    resp.device.ok_or_else(|| {
        ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "missing device info".into(),
        )
    })
}

fn session_info(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    session_id: &str,
) -> core_pb::SessionInfoProto {
    core_pb::SessionInfoProto {
        node_id: state.node_id.clone(),
        session_id: session_id.to_string(),
        tenant_id: device.tenant_id.clone(),
        device_id: device.device_id.clone(),
        device_profile_id: device.device_profile_id.clone(),
        device_name: device.device_name.clone(),
        device_type: device.device_type.clone(),
        is_gateway: false,
        gateway_session_id: String::new(),
    }
}

async fn send_core(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    msg: core_pb::to_core_msg::Msg,
) -> Result<(), ApiError> {
    let m = core_pb::ToCoreMsg {
        tenant_id: device.tenant_id.clone(),
        entity_id: device.device_id.clone(),
        msg: Some(msg),
    };
    state
        .producer
        .send(
            TbTopic::CORE,
            &device.device_id,
            Bytes::from(m.encode_to_vec()),
        )
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

fn get_or_create_rpc_session(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
) -> (Arc<RpcSession>, bool) {
    match state.rpc_sessions.entry(device.device_id.clone()) {
        Entry::Occupied(entry) => (entry.get().clone(), false),
        Entry::Vacant(entry) => {
            let session_id = Uuid::now_v7().to_string();
            let (tx, rx) = mpsc::channel(32);
            let (close_tx, close_rx) = watch::channel(false);
            let session = Arc::new(RpcSession {
                device: device.clone(),
                info: session_info(state, device, &session_id),
                tx,
                rx: Mutex::new(rx),
                last_activity_ms: AtomicI64::new(now_ms()),
                close_tx,
                close_rx,
            });
            entry.insert(session.clone());
            state
                .rpc_session_ids
                .insert(session_id, device.device_id.clone());
            (session, true)
        }
    }
}

pub(crate) fn remove_rpc_session(
    state: &TransportState,
    session_id: &str,
) -> Option<Arc<RpcSession>> {
    let device_id = state
        .rpc_session_ids
        .remove(session_id)
        .map(|(_, device_id)| device_id)?;
    let session = state
        .rpc_sessions
        .remove(&device_id)
        .map(|(_, session)| session);
    if let Some(session) = &session {
        session.close();
    }
    state
        .rpc_reply_routes
        .retain(|_, route| route.session_id != session_id);
    session
}

fn remove_rpc_session_if_same(
    state: &TransportState,
    device_id: &str,
    expected: &Arc<RpcSession>,
) -> Option<Arc<RpcSession>> {
    let removed = match state.rpc_sessions.entry(device_id.to_string()) {
        Entry::Occupied(entry) if Arc::ptr_eq(entry.get(), expected) => Some(entry.remove()),
        _ => None,
    };
    if let Some(session) = &removed {
        session.close();
        state.rpc_session_ids.remove(&expected.info.session_id);
        state
            .rpc_reply_routes
            .retain(|_, route| route.session_id != expected.info.session_id);
    }
    removed
}

async fn close_expired_rpc_sessions(state: &TransportState) {
    let now = now_ms();
    let expired: Vec<_> = state
        .rpc_sessions
        .iter()
        .filter(|entry| {
            now - entry.last_activity_ms.load(Ordering::Relaxed)
                > RPC_SESSION_IDLE_TIMEOUT.as_millis() as i64
        })
        .map(|entry| (entry.key().clone(), entry.value().clone()))
        .collect();
    for (device_id, session) in expired {
        let Some(session) = remove_rpc_session_if_same(state, &device_id, &session) else {
            continue;
        };
        if let Err(e) = send_core(
            state,
            &session.device,
            core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
                session: Some(session.info.clone()),
                reason: "http RPC session idle timeout".into(),
            }),
        )
        .await
        {
            tracing::warn!("failed to close idle HTTP RPC session: {}", e.1);
        }
    }
}

async fn sweep_rpc_sessions(state: TransportState) {
    loop {
        tokio::time::sleep(RPC_SESSION_SWEEP_INTERVAL).await;
        close_expired_rpc_sessions(&state).await;
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TelemetryPayload {
    Points(Vec<TsValues>),
    WithTs(TsValues),
    Map(HashMap<String, KvValue>),
}

#[derive(Debug, Deserialize)]
struct TsValues {
    ts: Option<i64>,
    values: HashMap<String, KvValue>,
}

impl TelemetryPayload {
    fn into_entries(self, now: i64) -> Vec<TsKvEntry> {
        match self {
            Self::Points(points) => points
                .into_iter()
                .flat_map(|p| {
                    let ts = p.ts.unwrap_or(now);
                    p.values
                        .into_iter()
                        .map(move |(key, value)| TsKvEntry { ts, key, value })
                })
                .collect(),
            Self::WithTs(p) => {
                let ts = p.ts.unwrap_or(now);
                p.values
                    .into_iter()
                    .map(|(key, value)| TsKvEntry { ts, key, value })
                    .collect()
            }
            Self::Map(values) => values
                .into_iter()
                .map(|(key, value)| TsKvEntry {
                    ts: now,
                    key,
                    value,
                })
                .collect(),
        }
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

async fn post_telemetry(
    State(state): State<TransportState>,
    Path(token): Path<String>,
    Json(payload): Json<TelemetryPayload>,
) -> Result<StatusCode, ApiError> {
    let device = validate(&state, &token).await?;
    state
        .transport_api
        .publish_telemetry(&device, payload.into_entries(now_ms()))
        .await?;
    Ok(StatusCode::OK)
}

async fn post_attributes(
    State(state): State<TransportState>,
    Path(token): Path<String>,
    Json(values): Json<HashMap<String, KvValue>>,
) -> Result<StatusCode, ApiError> {
    let device = validate(&state, &token).await?;
    let entries: Vec<KvEntry> = values
        .into_iter()
        .map(|(key, value)| KvEntry { key, value })
        .collect();
    state
        .transport_api
        .publish_attributes(&device, entries)
        .await?;
    Ok(StatusCode::OK)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttributesQuery {
    client_keys: Option<String>,
    shared_keys: Option<String>,
}

fn split_keys(raw: &Option<String>) -> Vec<String> {
    raw.as_deref()
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|k| !k.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn kvs_to_json(kvs: &[tb_proto::common::KeyValueProto]) -> Value {
    let mut map = serde_json::Map::new();
    for kv in kvs {
        if let Some((key, value)) = tb_proto::convert::kv_from_proto(kv, kv.key.clone()) {
            map.insert(key, serde_json::to_value(&value).unwrap_or_default());
        }
    }
    Value::Object(map)
}

/// GET attributes：经 transport-api 同步查询 tb-core（CLIENT_SCOPE + SHARED_SCOPE）。
async fn get_attributes(
    State(state): State<TransportState>,
    Path(token): Path<String>,
    Query(query): Query<AttributesQuery>,
) -> Result<Json<Value>, ApiError> {
    let device = validate(&state, &token).await?;
    let resp = state
        .transport_api
        .get_attributes(
            &device.device_id,
            split_keys(&query.client_keys),
            split_keys(&query.shared_keys),
        )
        .await?;
    Ok(Json(json!({
        "client": kvs_to_json(&resp.client_attributes),
        "shared": kvs_to_json(&resp.shared_attributes),
    })))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaimPayload {
    #[serde(default)]
    secret_key: String,
    #[serde(default)]
    duration_ms: i64,
}

async fn claim(
    State(state): State<TransportState>,
    Path(token): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    let device = validate(&state, &token).await?;
    let payload = if body.iter().all(u8::is_ascii_whitespace) {
        ClaimPayload::default()
    } else {
        let map: serde_json::Map<String, Value> = serde_json::from_slice(&body)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid claim payload".into()))?;
        serde_json::from_value(Value::Object(map))
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "invalid claim payload".into()))?
    };
    let session = session_info(&state, &device, &Uuid::now_v7().to_string());
    send_core(
        &state,
        &device,
        core_pb::to_core_msg::Msg::ClaimDevice(core_pb::ClaimDeviceMsg {
            session: Some(session),
            secret_key: payload.secret_key,
            duration_ms: payload.duration_ms,
        }),
    )
    .await?;
    Ok(StatusCode::OK)
}

#[derive(Debug, Deserialize)]
struct RpcPayload {
    method: String,
    #[serde(default)]
    params: Value,
    #[serde(default)]
    timeout: Option<i64>,
}

async fn post_rpc(
    State(state): State<TransportState>,
    Path(token): Path<String>,
    Json(payload): Json<RpcPayload>,
) -> Result<StatusCode, ApiError> {
    let device = validate(&state, &token).await?;
    state
        .transport_api
        .publish_to_server_rpc(
            &device,
            &payload.method,
            payload.params,
            payload.timeout.unwrap_or(0),
        )
        .await?;
    Ok(StatusCode::ACCEPTED)
}

/// GET rpc 长轮询：复用设备会话并保留未取走的 RPC；空闲超时或 core 踢线时才关闭。
async fn get_rpc(
    State(state): State<TransportState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let device = validate(&state, &token).await?;
    let (session, created) = get_or_create_rpc_session(&state, &device);
    session.last_activity_ms.store(now_ms(), Ordering::Relaxed);
    if created
        && let Err(error) = send_core(
            &state,
            &device,
            core_pb::to_core_msg::Msg::SubscribeRpc(core_pb::SubscribeRpcMsg {
                session: Some(session.info.clone()),
                subscribe: true,
            }),
        )
        .await
    {
        remove_rpc_session(&state, &session.info.session_id);
        return Err(error);
    }
    let received = {
        let mut rx = session.rx.lock().await;
        let mut closed = session.close_rx.clone();
        tokio::time::timeout(state.long_poll_timeout, async move {
            if *closed.borrow() {
                return None;
            }
            tokio::select! {
                rpc = rx.recv() => rpc,
                _ = closed.changed() => None,
            }
        })
        .await
    };
    session.last_activity_ms.store(now_ms(), Ordering::Relaxed);
    let result = match received {
        Ok(Some(rpc)) => {
            state
                .rpc_reply_routes
                .retain(|_, route| route.expiration_ts == 0 || route.expiration_ts >= now_ms());
            state.rpc_reply_routes.insert(
                (device.device_id.clone(), rpc.request_id),
                RpcReplyRoute {
                    session_id: session.info.session_id.clone(),
                    reply_node_id: rpc.reply_node_id.clone(),
                    expiration_ts: rpc.expiration_ts,
                },
            );
            let params: Value = serde_json::from_str(&rpc.params_json).unwrap_or(Value::Null);
            json!({"id": rpc.request_id, "method": rpc.method, "params": params})
        }
        _ => json!({}),
    };
    Ok(Json(result))
}

/// POST rpc/{requestId}：设备回服务端 RPC 响应 → tb_core rpc_response 完成 pending。
async fn post_rpc_response(
    State(state): State<TransportState>,
    Path((token, request_id)): Path<(String, i32)>,
    Json(payload): Json<Value>,
) -> Result<StatusCode, ApiError> {
    let device = validate(&state, &token).await?;
    let route = state
        .rpc_reply_routes
        .remove(&(device.device_id.clone(), request_id))
        .map(|(_, route)| route);
    let session_id = route
        .as_ref()
        .map(|route| route.session_id.as_str())
        .unwrap_or("");
    let info = session_info(&state, &device, session_id);
    send_core(
        &state,
        &device,
        core_pb::to_core_msg::Msg::RpcResponse(core_pb::RpcResponseMsg {
            session: Some(info),
            request_id,
            payload_json: payload.to_string(),
            error: String::new(),
            reply_node_id: route.map(|route| route.reply_node_id).unwrap_or_default(),
        }),
    )
    .await?;
    Ok(StatusCode::OK)
}

const DEFAULT_CHUNK_SIZE: i32 = 16 * 1024;
const MAX_CHUNK_SIZE: i32 = 1024 * 1024;

fn firmware_chunk_size(requested: Option<i32>) -> i32 {
    requested
        .filter(|size| *size > 0)
        .unwrap_or(DEFAULT_CHUNK_SIZE)
        .min(MAX_CHUNK_SIZE)
}

#[derive(Debug, Deserialize)]
struct FirmwareQuery {
    #[serde(default)]
    title: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    chunk: Option<i32>,
    #[serde(default)]
    size: Option<i32>,
}

/// GET firmware：按设备当前 title/version 查可更新包，返回指定分块二进制。
async fn get_firmware(
    State(state): State<TransportState>,
    Path(token): Path<String>,
    Query(query): Query<FirmwareQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let device = validate(&state, &token).await?;
    let package = state
        .transport_api
        .get_ota_package(
            &device.tenant_id,
            &device.device_profile_id,
            tb_proto::common::OtaPackageType::Firmware,
            &query.title,
            &query.version,
        )
        .await?
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "no firmware update".into()))?;
    let chunk = state
        .transport_api
        .get_ota_chunk(
            &package.ota_package_id,
            query.chunk.unwrap_or(0).max(0),
            firmware_chunk_size(query.size),
        )
        .await?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        chunk,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProvisionRequest {
    device_name: String,
    provision_device_key: String,
    provision_device_secret: String,
}

/// POST /api/v1/provision：无 token，profile provisionConfiguration 校验。
async fn provision(
    State(state): State<TransportState>,
    Json(req): Json<ProvisionRequest>,
) -> Result<Json<Value>, ApiError> {
    let resp = state
        .transport_api
        .provision_device(
            "",
            &req.device_name,
            &req.provision_device_key,
            &req.provision_device_secret,
        )
        .await?;
    if resp.status == "SUCCESS" {
        Ok(Json(json!({
            "status": "SUCCESS",
            "credentialsType": resp.credentials_type,
            "credentialsValue": resp.credentials_value,
            "deviceName": resp.device_name,
        })))
    } else {
        Ok(Json(
            json!({"status": "FAILURE", "errorMsg": resp.error_msg}),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn firmware_chunk_size_is_positive_and_capped() {
        assert_eq!(firmware_chunk_size(Some(100)), 100);
        assert_eq!(firmware_chunk_size(Some(0)), DEFAULT_CHUNK_SIZE);
        assert_eq!(firmware_chunk_size(None), DEFAULT_CHUNK_SIZE);
        assert_eq!(
            firmware_chunk_size(Some(MAX_CHUNK_SIZE + 1)),
            MAX_CHUNK_SIZE
        );
    }

    #[tokio::test]
    async fn queued_rpcs_survive_multiple_poll_windows() {
        let state =
            TransportState::new("localhost:29092", &format!("http-unit-{}", Uuid::now_v7()))
                .unwrap();
        let device = pb::DeviceInfoProto {
            device_id: "device-a".into(),
            ..Default::default()
        };
        let (session, created) = get_or_create_rpc_session(&state, &device);
        assert!(created);
        let (same_session, created) = get_or_create_rpc_session(&state, &device);
        assert!(!created);
        assert!(Arc::ptr_eq(&session, &same_session));

        for request_id in [1, 2] {
            session
                .tx
                .send(core_pb::ToDeviceRpcRequestMsg {
                    request_id,
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let first = session.rx.lock().await.recv().await.unwrap();
        assert_eq!(first.request_id, 1);
        assert_eq!(state.rpc_sessions.len(), 1);
        let second = session.rx.lock().await.recv().await.unwrap();
        assert_eq!(second.request_id, 2);

        remove_rpc_session(&state, &session.info.session_id).unwrap();
        assert!(state.rpc_sessions.is_empty());
        assert!(state.rpc_session_ids.is_empty());
        assert!(*session.close_rx.borrow());
    }
}
