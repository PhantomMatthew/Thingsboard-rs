//! /api/tenant(s)：仅 SYS_ADMIN。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tb_auth::{ApiError, AuthUser};
use tb_common::TbError;
use tb_dao::models::{Authority, Tenant};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::require_authority;
use crate::routes::PageQuery;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/tenant", post(save_tenant))
        .route("/api/tenant/{id}", get(get_tenant).delete(delete_tenant))
        .route("/api/tenants", get(list_tenants))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveTenantRequest {
    pub id: Option<Uuid>,
    pub title: String,
    pub region: Option<String>,
    pub country: Option<String>,
    pub state: Option<String>,
    pub city: Option<String>,
    pub address: Option<String>,
    pub address2: Option<String>,
    pub zip: Option<String>,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub additional_info: Option<Value>,
    pub tenant_profile_id: Option<Uuid>,
}

async fn save_tenant(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveTenantRequest>,
) -> Result<Json<Tenant>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin])?;
    let (id, created_time) = match req.id {
        Some(id) => {
            let existing = state
                .tenant_dao
                .find_by_id(id)
                .await?
                .ok_or_else(|| TbError::NotFound(format!("tenant {id}")))?;
            (id, existing.created_time)
        }
        None => (Uuid::now_v7(), 0),
    };
    let tenant_profile_id = match req.tenant_profile_id {
        Some(p) => Some(p),
        None => state.tenant_profile_dao.find_default().await?.map(|p| p.id),
    };
    let tenant = Tenant {
        id,
        created_time,
        title: req.title,
        region: req.region,
        country: req.country,
        state: req.state,
        city: req.city,
        address: req.address,
        address2: req.address2,
        zip: req.zip,
        phone: req.phone,
        email: req.email,
        additional_info: req.additional_info,
        tenant_profile_id,
    };
    Ok(Json(state.tenant_dao.save(&tenant).await?))
}

async fn get_tenant(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<Tenant>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin])?;
    let tenant = state
        .tenant_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("tenant {id}")))?;
    Ok(Json(tenant))
}

async fn delete_tenant(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::SysAdmin])?;
    if !state.tenant_dao.delete_by_id(id).await? {
        return Err(TbError::NotFound(format!("tenant {id}")).into());
    }
    Ok(StatusCode::OK)
}

async fn list_tenants(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<Tenant>>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(state.tenant_dao.find_all(&page_link).await?))
}
