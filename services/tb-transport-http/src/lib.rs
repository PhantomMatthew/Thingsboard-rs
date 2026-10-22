//! tb-transport-http：设备 HTTP 接入（无状态）。
//! 凭证校验走 transport-api（Kafka 请求/响应，带 60s 缓存），
//! 遥测/属性/RPC 封装为 ToRuleEngineMsg 发到 tb_rule_engine.Main。

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tb_common::{KvEntry, KvValue, TbError, TsKvEntry};
use tb_transport_api::TransportApiClient;

#[derive(Clone)]
pub struct TransportState {
    pub transport_api: TransportApiClient,
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
        .route("/api/v1/{token}/rpc", post(post_rpc))
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
        let status = StatusCode::from_u16(e.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
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
    resp.device
        .ok_or_else(|| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "missing device info".into()))
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
    #[allow(dead_code)]
    client_keys: Option<String>,
    #[allow(dead_code)]
    shared_keys: Option<String>,
}

/// Phase 3a 简化：同步会话查询在 Phase 5 实现，这里返回空集。
async fn get_attributes(
    State(state): State<TransportState>,
    Path(token): Path<String>,
    Query(_query): Query<AttributesQuery>,
) -> Result<Json<Value>, ApiError> {
    validate(&state, &token).await?;
    Ok(Json(json!({})))
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
