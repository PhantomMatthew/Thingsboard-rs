//! /api/rpc/{oneway,twoway}/{deviceId}：服务端 → 设备 RPC 下行。
//! twoway 经 tb_transport.notifications.<node_id> 下发后等 pending oneshot（tb_core 回流完成）。

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use prost::Message as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::{ApiError, AuthUser};
use tb_common::{TbError, TbResult};
use tb_dao::models::Authority;
use tb_proto::core as core_pb;
use tb_queue::{QueueProducer as _, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::{check_tenant_scope, require_authority};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TIMEOUT: Duration = Duration::from_secs(60);

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/rpc/twoway/{deviceId}", post(rpc_twoway))
        .route("/api/rpc/oneway/{deviceId}", post(rpc_oneway))
}

#[derive(Debug, Deserialize)]
struct RpcRequestBody {
    method: String,
    #[serde(default)]
    params: Value,
    /// 毫秒；缺省 10s，上限 60s。
    timeout: Option<i64>,
}

/// 设备存在性 + 租户/客户归属校验。
async fn check_device_access(state: &AppState, user: &AuthUser, device_id: Uuid) -> TbResult<()> {
    let device = state
        .device_dao
        .find_by_id(device_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device {device_id}")))?;
    match user.0.authority {
        Authority::TenantAdmin => check_tenant_scope(user, device.tenant_id),
        Authority::CustomerUser
            if user.0.customer_id.is_some() && user.0.customer_id == device.customer_id =>
        {
            Ok(())
        }
        _ => Err(TbError::PermissionDenied(
            "not allowed to access this device".into(),
        )),
    }
}

fn timeout_of(body: &RpcRequestBody) -> Duration {
    body.timeout
        .filter(|t| *t > 0)
        .map(|t| Duration::from_millis((t as u64).min(MAX_TIMEOUT.as_millis() as u64)))
        .unwrap_or(DEFAULT_TIMEOUT)
}

async fn send_rpc(
    state: &AppState,
    device_id: Uuid,
    session: &crate::device_session::SessionEntry,
    request_id: i32,
    body: &RpcRequestBody,
    timeout: Duration,
) -> TbResult<()> {
    let msg = core_pb::ToTransportMsg {
        session_id: session.session_id.clone(),
        entity_id: device_id.to_string(),
        msg: Some(core_pb::to_transport_msg::Msg::ToDeviceRpc(
            core_pb::ToDeviceRpcRequestMsg {
                request_id,
                method: body.method.clone(),
                params_json: body.params.to_string(),
                expiration_ts: chrono::Utc::now().timestamp_millis() + timeout.as_millis() as i64,
                persisted: false,
            },
        )),
    };
    let topic = TbTopic::transport_notifications(&session.node_id);
    state
        .queue_producer()
        .await?
        .send(&topic, &session.session_id, Bytes::from(msg.encode_to_vec()))
        .await
        .map_err(TbError::internal)
}

async fn rpc_twoway(
    State(state): State<AppState>,
    user: AuthUser,
    Path(device_id): Path<Uuid>,
    Json(body): Json<RpcRequestBody>,
) -> Result<Response, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    check_device_access(&state, &user, device_id).await?;
    let timeout = timeout_of(&body);
    let session = state.device_sessions.rpc_session(device_id).ok_or_else(|| {
        TbError::NotFound("device is offline or not subscribed to RPC".into())
    })?;
    let request_id = state.device_sessions.next_rpc_request_id();
    let rx = state.device_sessions.register_pending_rpc(device_id, request_id);
    send_rpc(&state, device_id, &session, request_id, &body, timeout).await?;
    // 等设备响应；超时作废 pending 槽位。
    match tokio::time::timeout(timeout + Duration::from_millis(500), rx).await {
        Ok(Ok(Ok(payload))) => {
            let value: Value =
                serde_json::from_str(&payload).unwrap_or(Value::String(payload));
            Ok(Json(value).into_response())
        }
        Ok(Ok(Err(e))) => Ok((
            StatusCode::BAD_GATEWAY,
            Json(json!({"message": format!("device rpc error: {e}")})),
        )
            .into_response()),
        Ok(Err(_)) => Ok((
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({"message": "rpc response channel closed"})),
        )
            .into_response()),
        Err(_) => {
            state.device_sessions.drop_pending_rpc(device_id, request_id);
            Ok((
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({"message": "rpc request timeout"})),
            )
                .into_response())
        }
    }
}

async fn rpc_oneway(
    State(state): State<AppState>,
    user: AuthUser,
    Path(device_id): Path<Uuid>,
    Json(body): Json<RpcRequestBody>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    check_device_access(&state, &user, device_id).await?;
    let timeout = timeout_of(&body);
    let session = state.device_sessions.rpc_session(device_id).ok_or_else(|| {
        TbError::NotFound("device is offline or not subscribed to RPC".into())
    })?;
    let request_id = state.device_sessions.next_rpc_request_id();
    send_rpc(&state, device_id, &session, request_id, &body, timeout).await?;
    Ok(StatusCode::OK)
}
