//! /api/auth REST 路由。库与独立 bin 共用。

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;
use tb_common::TbError;
use tb_dao::audit_log::AuditLog;
use tokio::sync::mpsc;

use crate::extract::AuthUser;
use crate::service::AuthService;
use crate::token::{JwtTokenFactory, TokenType};

#[derive(Clone)]
pub struct AuthState {
    pub auth_service: Arc<AuthService>,
    pub jwt_factory: Arc<JwtTokenFactory>,
    /// 可选审计通道（tb-core 注入，receiver 负责落库）；None 时不审计。
    pub audit_tx: Option<mpsc::Sender<AuditLog>>,
}

impl AuthState {
    pub fn new(auth_service: Arc<AuthService>, jwt_factory: Arc<JwtTokenFactory>) -> Self {
        Self {
            auth_service,
            jwt_factory,
            audit_tx: None,
        }
    }

    pub fn with_audit_tx(mut self, audit_tx: mpsc::Sender<AuditLog>) -> Self {
        self.audit_tx = Some(audit_tx);
        self
    }

    /// 尽力发送审计事件；通道满/关闭仅告警，不影响主流程。
    fn audit(&self, entry: AuditLog) {
        if let Some(tx) = &self.audit_tx
            && let Err(e) = tx.try_send(entry)
        {
            tracing::warn!("audit event dropped: {e}");
        }
    }
}

impl axum::extract::FromRef<AuthState> for Arc<JwtTokenFactory> {
    fn from_ref(state: &AuthState) -> Self {
        state.jwt_factory.clone()
    }
}

/// 统一错误响应：{status, message} + TbError::http_status()。
pub fn error_response(err: &TbError) -> Response {
    tracing::error!(error = %err, "request failed");
    let code = err.http_status();
    let status = StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (
        status,
        Json(json!({ "status": code, "message": err.client_message() })),
    )
        .into_response()
}

/// 供 handler 使用 `?` 直接返回错误 JSON。
pub struct ApiError(pub TbError);

impl From<TbError> for ApiError {
    fn from(err: TbError) -> Self {
        Self(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        error_response(&self.0)
    }
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

/// 认证路由（不含 /api/auth 前缀，由外层 nest）。
pub fn router(state: AuthState) -> Router {
    Router::new()
        .route("/login", post(login))
        .route("/token/refresh", post(refresh))
        .route("/changePassword", post(change_password))
        .route("/user", get(current_user))
        .with_state(state)
}

async fn login(
    State(state): State<AuthState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<crate::service::TokenPair>, ApiError> {
    match state.auth_service.login(&req.username, &req.password).await {
        Ok(pair) => {
            if state.audit_tx.is_some()
                && let Ok(claims) = state
                    .jwt_factory
                    .parse_typed(&pair.token, TokenType::Access)
                && let Ok(user) = claims.to_security_user()
            {
                state.audit(AuditLog::new(
                    user.tenant_id,
                    Some((user.id, "USER".into(), user.email.clone())),
                    Some(user.id),
                    Some(user.email),
                    "LOGIN",
                    None,
                    "SUCCESS",
                ));
            }
            Ok(Json(pair))
        }
        Err(e) => {
            state.audit(AuditLog::new(
                uuid::Uuid::nil(),
                None,
                None,
                Some(req.username.clone()),
                "LOGIN",
                Some(json!({"error": e.to_string()})),
                "FAILURE",
            ));
            Err(e.into())
        }
    }
}

async fn refresh(
    State(state): State<AuthState>,
    Json(req): Json<RefreshRequest>,
) -> Result<Json<crate::service::TokenPair>, ApiError> {
    let pair = state.auth_service.refresh(&req.refresh_token).await?;
    Ok(Json(pair))
}

async fn change_password(
    State(state): State<AuthState>,
    user: AuthUser,
    Json(req): Json<ChangePasswordRequest>,
) -> Result<StatusCode, ApiError> {
    state
        .auth_service
        .change_password(user.0.id, &req.current_password, &req.new_password)
        .await?;
    Ok(StatusCode::OK)
}

async fn current_user(
    State(state): State<AuthState>,
    user: AuthUser,
) -> Result<Json<tb_dao::models::User>, ApiError> {
    let user = state.auth_service.get_user(user.0.id).await?;
    Ok(Json(user))
}
