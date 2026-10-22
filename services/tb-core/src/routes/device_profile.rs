//! /api/deviceProfile(s)：设备档案管理（TENANT_ADMIN）。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::{ApiError, AuthUser};
use tb_common::TbError;
use tb_dao::models::{Authority, DeviceProfile};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/deviceProfile", post(save_profile))
        .route("/api/deviceProfile/default", get(get_default))
        .route(
            "/api/deviceProfile/{id}",
            get(get_profile).delete(delete_profile),
        )
        .route("/api/deviceProfiles", get(list_profiles))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveDeviceProfileRequest {
    pub id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    pub is_default: Option<bool>,
    pub transport_type: Option<String>,
    pub provision_type: Option<String>,
    pub profile_data: Option<Value>,
    pub firmware_id: Option<Uuid>,
    pub software_id: Option<Uuid>,
}

async fn save_profile(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveDeviceProfileRequest>,
) -> Result<Json<DeviceProfile>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let (id, created_time) = match req.id {
        Some(id) => {
            let existing = load_profile(&state, id).await?;
            check_tenant_scope(&user, existing.tenant_id)?;
            (id, existing.created_time)
        }
        None => (Uuid::now_v7(), 0),
    };
    let profile = DeviceProfile {
        id,
        created_time,
        tenant_id: user.0.tenant_id,
        name: req.name,
        description: req.description,
        is_default: req.is_default.unwrap_or(false),
        transport_type: req.transport_type.unwrap_or_else(|| "DEFAULT".into()),
        provision_type: req.provision_type.unwrap_or_else(|| "DISABLED".into()),
        profile_data: req.profile_data.unwrap_or_else(|| json!({})),
        firmware_id: req.firmware_id,
        software_id: req.software_id,
    };
    Ok(Json(state.device_profile_dao.save(&profile).await?))
}

async fn get_profile(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<DeviceProfile>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let profile = load_profile(&state, id).await?;
    check_tenant_scope(&user, profile.tenant_id)?;
    Ok(Json(profile))
}

async fn get_default(
    State(state): State<AppState>,
    user: AuthUser,
) -> Result<Json<DeviceProfile>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let profile = state
        .device_profile_dao
        .find_default(user.0.tenant_id)
        .await?
        .ok_or_else(|| TbError::NotFound("default device profile".into()))?;
    Ok(Json(profile))
}

async fn delete_profile(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let profile = load_profile(&state, id).await?;
    check_tenant_scope(&user, profile.tenant_id)?;
    state.device_profile_dao.delete_by_id(id).await?;
    Ok(StatusCode::OK)
}

async fn list_profiles(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<DeviceProfile>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .device_profile_dao
            .find_by_tenant_id(user.0.tenant_id, &page_link)
            .await?,
    ))
}

async fn load_profile(state: &AppState, id: Uuid) -> tb_common::TbResult<DeviceProfile> {
    state
        .device_profile_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device profile {id}")))
}
