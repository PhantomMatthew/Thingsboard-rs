//! /api/user(s)：SYS_ADMIN 可建任意用户；TENANT_ADMIN 管本租户用户（不可建 SYS_ADMIN）。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError};
use tb_dao::models::{Authority, User, UserCredentials};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;
use crate::routes::customer::load_customer;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/user", post(save_user))
        .route("/api/user/{id}", get(get_user).delete(delete_user))
        .route("/api/users", get(list_users))
        .route("/api/customer/{customerId}/users", get(list_customer_users))
}

/// 建用户请求；带 password 即等价于 sendActivationMail=false 的简化直连路径。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveUserRequest {
    pub id: Option<Uuid>,
    pub tenant_id: Option<Uuid>,
    pub customer_id: Option<Uuid>,
    pub authority: Authority,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone: Option<String>,
    pub additional_info: Option<Value>,
    pub password: Option<String>,
}

async fn save_user(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveUserRequest>,
) -> Result<Json<User>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin, Authority::TenantAdmin])?;
    let caller = &user.0;
    let tenant_id = match caller.authority {
        Authority::SysAdmin => req
            .tenant_id
            .ok_or_else(|| TbError::InvalidInput("tenantId is required".into()))?,
        _ => {
            if req.authority == Authority::SysAdmin {
                return Err(TbError::PermissionDenied(
                    "tenant admin cannot create SYS_ADMIN".into(),
                )
                .into());
            }
            caller.tenant_id
        }
    };
    if let Some(customer_id) = req.customer_id {
        let customer = load_customer(&state, customer_id).await?;
        if customer.tenant_id != tenant_id {
            return Err(TbError::InvalidInput("customer belongs to another tenant".into()).into());
        }
    }

    let is_new = req.id.is_none();
    let (id, created_time) = match req.id {
        Some(id) => {
            let existing = state
                .user_dao
                .find_by_id(id)
                .await?
                .ok_or_else(|| TbError::NotFound(format!("user {id}")))?;
            check_tenant_scope(&user, existing.tenant_id)?;
            (id, existing.created_time)
        }
        None => (Uuid::now_v7(), 0),
    };
    let new_user = User {
        id,
        created_time,
        tenant_id,
        customer_id: req.customer_id,
        authority: req.authority,
        email: req.email,
        first_name: req.first_name,
        last_name: req.last_name,
        phone: req.phone,
        additional_info: req.additional_info,
    };
    let saved = state.user_dao.save(&new_user).await?;

    match (&req.password, created_time == 0) {
        // 简化路径：直接设置密码并激活。
        (Some(password), _) => {
            let password = tb_auth::password::hash_password(password)?;
            let changed_ts = chrono::Utc::now().timestamp_millis();
            state
                .user_credentials_dao
                .upsert_password(saved.id, &password, changed_ts)
                .await?;
        }
        // 新用户未带密码：生成未激活凭证（激活邮件流程此处省略）。
        (None, true) => {
            let creds = UserCredentials {
                id: Uuid::now_v7(),
                created_time: 0,
                user_id: saved.id,
                enabled: false,
                password: None,
                activate_token: Some(Uuid::now_v7().to_string()),
                reset_token: None,
                failed_login_attempts: 0,
                last_login_ts: None,
                password_changed_ts: None,
            };
            state.user_credentials_dao.save(&creds).await?;
        }
        (None, false) => {}
    }
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::User, saved.email.clone())),
        if is_new { "CREATED" } else { "UPDATED" },
        None,
        "SUCCESS",
    );
    Ok(Json(saved))
}

async fn get_user(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<User>, ApiError> {
    let target = load_user(&state, id).await?;
    match user.0.authority {
        Authority::SysAdmin => {}
        Authority::TenantAdmin => check_tenant_scope(&user, target.tenant_id)?,
        Authority::CustomerUser if target.id == user.0.id => {}
        _ => {
            return Err(TbError::PermissionDenied("not allowed to read this user".into()).into());
        }
    }
    Ok(Json(target))
}

async fn delete_user(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::SysAdmin, Authority::TenantAdmin])?;
    let target = load_user(&state, id).await?;
    check_tenant_scope(&user, target.tenant_id)?;
    state.user_dao.delete_by_id(id).await?;
    audit(
        &state,
        &user,
        Some((target.id, EntityType::User, target.email)),
        "DELETED",
        None,
        "SUCCESS",
    );
    Ok(StatusCode::OK)
}

async fn list_users(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<User>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .user_dao
            .find_by_tenant_id(user.0.tenant_id, &page_link)
            .await?,
    ))
}

async fn list_customer_users(
    State(state): State<AppState>,
    user: AuthUser,
    Path(customer_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<User>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let customer = load_customer(&state, customer_id).await?;
    check_tenant_scope(&user, customer.tenant_id)?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .user_dao
            .find_by_customer_id(customer.tenant_id, customer_id, &page_link)
            .await?,
    ))
}

pub async fn load_user(state: &AppState, id: Uuid) -> tb_common::TbResult<User> {
    state
        .user_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("user {id}")))
}
