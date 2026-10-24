//! /api/oauth2/*：公开端点（clients 登录页列表 / code 登录）+ 管理端 CRUD。
//! GET /api/oauth2/clients 双语义：带有效 SYS_ADMIN/TENANT_ADMIN token 时返回分页管理
//! 视图（tenant admin 仅本租户），否则返回公开启用列表（id/title/clientId/authorizationUrl）。

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::oauth2::Oauth2LoginRequest;
use tb_auth::{ApiError, AuthUser, SecurityUser, TokenPair, TokenType};
use tb_common::TbError;
use tb_dao::models::{Authority, Oauth2Client};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::require_authority;
use crate::routes::PageQuery;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/oauth2/clients", get(list_clients))
        .route("/api/oauth2/login", post(oauth2_login))
        .route("/api/oauth2/client", post(save_client))
        .route(
            "/api/oauth2/client/{id}",
            get(get_client).delete(delete_client),
        )
}

fn try_admin(state: &AppState, headers: &HeaderMap) -> Option<SecurityUser> {
    let token = headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    let claims = state
        .jwt_factory
        .parse_typed(token, TokenType::Access)
        .ok()?;
    let user = claims.to_security_user().ok()?;
    matches!(user.authority, Authority::SysAdmin | Authority::TenantAdmin).then_some(user)
}

async fn list_clients(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageQuery>,
) -> Result<Json<Value>, ApiError> {
    match try_admin(&state, &headers) {
        Some(user) => {
            let tenant_id = match user.authority {
                Authority::SysAdmin => None,
                _ => Some(user.tenant_id),
            };
            let page_data = state
                .oauth2_client_dao
                .find_paged(tenant_id, &page.into())
                .await?;
            Ok(Json(json!(page_data)))
        }
        None => Ok(Json(json!(
            state.auth_service.oauth2_enabled_clients().await?
        ))),
    }
}

async fn oauth2_login(
    State(state): State<AppState>,
    Json(req): Json<Oauth2LoginRequest>,
) -> Result<Json<TokenPair>, ApiError> {
    Ok(Json(state.auth_service.oauth2_login(&req).await?))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveOauth2ClientRequest {
    pub id: Option<Uuid>,
    /// 仅 SYS_ADMIN 可指定（空 = 平台级）。
    pub tenant_id: Option<Uuid>,
    pub title: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub authorization_uri: String,
    pub token_uri: String,
    pub user_info_uri: String,
    pub redirect_uri_template: Option<String>,
    pub scope: Option<String>,
    pub enabled: Option<bool>,
}

fn validate_http_uri(name: &str, value: &str) -> Result<(), TbError> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| TbError::InvalidInput(format!("{name} must be an absolute HTTP(S) URL")))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(TbError::InvalidInput(format!(
            "{name} must be an absolute HTTP(S) URL"
        )));
    }
    Ok(())
}

async fn save_client(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveOauth2ClientRequest>,
) -> Result<Json<Oauth2Client>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin])?;
    validate_http_uri("authorizationUri", &req.authorization_uri)?;
    validate_http_uri("tokenUri", &req.token_uri)?;
    validate_http_uri("userInfoUri", &req.user_info_uri)?;
    if let Some(uri) = &req.redirect_uri_template {
        validate_http_uri("redirectUriTemplate", uri)?;
    }
    let tenant_id = req.tenant_id;
    let (id, created_time) = match req.id {
        Some(id) => {
            let existing = load_and_check(&state, &user, id).await?;
            (id, existing.created_time)
        }
        None => (Uuid::now_v7(), 0),
    };
    let client = Oauth2Client {
        id,
        created_time,
        tenant_id,
        title: req.title,
        client_id: req.client_id,
        client_secret: req.client_secret,
        authorization_uri: req.authorization_uri,
        token_uri: req.token_uri,
        user_info_uri: req.user_info_uri,
        redirect_uri_template: req.redirect_uri_template,
        scope: req.scope,
        enabled: req.enabled.unwrap_or(true),
    };
    Ok(Json(state.oauth2_client_dao.save(&client).await?))
}

async fn get_client(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<Oauth2Client>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin, Authority::TenantAdmin])?;
    Ok(Json(load_and_check(&state, &user, id).await?))
}

async fn delete_client(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::SysAdmin])?;
    load_and_check(&state, &user, id).await?;
    state.oauth2_client_dao.delete_by_id(id).await?;
    Ok(StatusCode::OK)
}

/// SYS_ADMIN 可见全部；TENANT_ADMIN 仅本租户客户端。
async fn load_and_check(
    state: &AppState,
    user: &AuthUser,
    id: Uuid,
) -> Result<Oauth2Client, TbError> {
    let client = state
        .oauth2_client_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("oauth2 client {id}")))?;
    if user.0.authority != Authority::SysAdmin && client.tenant_id != Some(user.0.tenant_id) {
        return Err(TbError::PermissionDenied(
            "oauth2 client belongs to another tenant".into(),
        ));
    }
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::validate_http_uri;

    #[test]
    fn oauth2_client_uris_require_http_or_https() {
        for uri in ["https://idp.example.com/token", "http://127.0.0.1/token"] {
            validate_http_uri("tokenUri", uri).unwrap();
        }
        for uri in ["file:///etc/passwd", "ftp://example.com/token", "/relative"] {
            assert!(validate_http_uri("tokenUri", uri).is_err(), "{uri}");
        }
    }
}
