//! axum extractor：从 `Authorization: Bearer` 解析出已认证用户。
//! 仅接受 access 类型 token；刷新 token 一律拒绝。

use std::sync::Arc;

use axum::extract::{FromRef, FromRequestParts};
use axum::http::request::Parts;
use tb_common::{TbError, TbResult};

use crate::routes::error_response;
use crate::service::AuthService;

#[derive(Debug, Clone)]
pub struct AuthUser(pub crate::token::SecurityUser);

impl AuthUser {
    pub fn user(&self) -> &crate::token::SecurityUser {
        &self.0
    }
}

impl<S> FromRequestParts<S> for AuthUser
where
    Arc<AuthService>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        extract_user(parts, state)
            .await
            .map_err(|e| error_response(&e))
    }
}

async fn extract_user<S>(parts: &Parts, state: &S) -> TbResult<AuthUser>
where
    Arc<AuthService>: FromRef<S>,
    S: Send + Sync,
{
    let header = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| TbError::Unauthenticated("missing Authorization header".into()))?;
    let token = header
        .strip_prefix("Bearer ")
        .ok_or_else(|| TbError::Unauthenticated("expected Bearer token".into()))?;
    let service = Arc::<AuthService>::from_ref(state);
    Ok(AuthUser(service.get_user_from_token(token).await?))
}
