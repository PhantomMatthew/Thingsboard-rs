//! tb-transport-http：设备 HTTP 接入。
//! 上行：遥测/属性/RPC 经 transport-api 或 tb_rule_engine.Main；
//! 同步能力：GET attributes 走 transport-api 请求/响应；GET rpc 长轮询
//! （注册临时会话 → subscribe_rpc → 等本节点 notifications consumer 推 to_device_rpc）。

pub mod downlink;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use dashmap::DashMap;
use prost::Message as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tb_common::{KvEntry, KvValue, TbError, TsKvEntry};
use tb_proto::core as core_pb;
use tb_proto::transport as pb;
use tb_queue::{KafkaQueueProducer, QueueError, QueueProducer as _, TbTopic};
use tb_transport_api::TransportApiClient;
use tokio::sync::mpsc;
use uuid::Uuid;

const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// 长轮询会话：session_id → to_device_rpc 投递通道。
pub type RpcSessions = Arc<DashMap<String, mpsc::Sender<core_pb::ToDeviceRpcRequestMsg>>>;

#[derive(Clone)]
pub struct TransportState {
    pub transport_api: TransportApiClient,
    pub producer: KafkaQueueProducer,
    pub node_id: String,
    pub brokers: String,
    pub rpc_sessions: RpcSessions,
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
            long_poll_timeout: LONG_POLL_TIMEOUT,
        })
    }
}

pub fn build_router(state: TransportState) -> Router {
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

async fn claim() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
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

/// GET rpc 长轮询：注册临时会话并 subscribe_rpc，等 notifications consumer 推 to_device_rpc；
/// 超时返回空 200。结束时发 session_close 清理 core 侧注册表。
async fn get_rpc(
    State(state): State<TransportState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let device = validate(&state, &token).await?;
    let session_id = Uuid::now_v7().to_string();
    let (tx, mut rx) = mpsc::channel(4);
    state.rpc_sessions.insert(session_id.clone(), tx);
    let info = session_info(&state, &device, &session_id);
    send_core(
        &state,
        &device,
        core_pb::to_core_msg::Msg::SubscribeRpc(core_pb::SubscribeRpcMsg {
            session: Some(info.clone()),
            subscribe: true,
        }),
    )
    .await?;
    let result = match tokio::time::timeout(state.long_poll_timeout, rx.recv()).await {
        Ok(Some(rpc)) => {
            let params: Value = serde_json::from_str(&rpc.params_json).unwrap_or(Value::Null);
            json!({"id": rpc.request_id, "method": rpc.method, "params": params})
        }
        _ => json!({}),
    };
    state.rpc_sessions.remove(&session_id);
    send_core(
        &state,
        &device,
        core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
            session: Some(info),
            reason: "http long-poll ended".into(),
        }),
    )
    .await?;
    Ok(Json(result))
}

/// POST rpc/{requestId}：设备回服务端 RPC 响应 → tb_core rpc_response 完成 pending。
async fn post_rpc_response(
    State(state): State<TransportState>,
    Path((token, request_id)): Path<(String, i32)>,
    Json(payload): Json<Value>,
) -> Result<StatusCode, ApiError> {
    let device = validate(&state, &token).await?;
    let info = session_info(&state, &device, "");
    send_core(
        &state,
        &device,
        core_pb::to_core_msg::Msg::RpcResponse(core_pb::RpcResponseMsg {
            session: Some(info),
            request_id,
            payload_json: payload.to_string(),
            error: String::new(),
        }),
    )
    .await?;
    Ok(StatusCode::OK)
}

const DEFAULT_CHUNK_SIZE: i32 = 16 * 1024;

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
            query.size.filter(|s| *s > 0).unwrap_or(DEFAULT_CHUNK_SIZE),
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
