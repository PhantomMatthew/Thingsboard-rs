//! /api/deviceProfile(s)：设备档案管理（TENANT_ADMIN）。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError};
use tb_dao::models::{Authority, DeviceProfile};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::ota::OtaKind;
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
    pub certificate: Option<String>,
    pub firmware_id: Option<Uuid>,
    pub software_id: Option<Uuid>,
}

async fn save_profile(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveDeviceProfileRequest>,
) -> Result<Json<DeviceProfile>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let is_new = req.id.is_none();
    let (id, created_time, previous) = match req.id {
        Some(id) => {
            let existing = load_profile(&state, id).await?;
            check_tenant_scope(&user, existing.tenant_id)?;
            (id, existing.created_time, Some(existing))
        }
        None => (Uuid::now_v7(), 0, None),
    };
    for (kind, ota_id) in [
        (OtaKind::Firmware, req.firmware_id),
        (OtaKind::Software, req.software_id),
    ] {
        let Some(ota_id) = ota_id else {
            continue;
        };
        let package = state
            .ota_package_dao
            .find_meta_by_id(ota_id)
            .await?
            .ok_or_else(|| TbError::NotFound(format!("ota package {ota_id}")))?;
        check_tenant_scope(&user, package.tenant_id)?;
        crate::ota::validate_binding(&package, kind, user.0.tenant_id, id)?;
    }
    let mut profile_data = req.profile_data.unwrap_or_else(|| json!({}));
    if let Some(certificate) = req.certificate {
        let data = profile_data.as_object_mut().ok_or_else(|| {
            TbError::InvalidInput("profileData must be an object when certificate is set".into())
        })?;
        data.insert("certificate".into(), Value::String(certificate));
    }
    let provision_type = req.provision_type.unwrap_or_else(|| "DISABLED".into());
    tb_common::x509::normalize_x509_chain_profile(&provision_type, &mut profile_data)?;
    let profile = DeviceProfile {
        id,
        created_time,
        tenant_id: user.0.tenant_id,
        name: req.name,
        description: req.description,
        is_default: req.is_default.unwrap_or(false),
        transport_type: req.transport_type.unwrap_or_else(|| "DEFAULT".into()),
        provision_type,
        profile_data,
        firmware_id: req.firmware_id,
        software_id: req.software_id,
    };
    if matches!(
        profile.provision_type.as_str(),
        "ALLOW_CREATE_NEW_DEVICES" | "CHECK_PRE_PROVISIONED_DEVICES"
    ) {
        let key = profile
            .profile_data
            .get("provisionConfiguration")
            .and_then(|config| config.get("provisionDeviceKey"))
            .and_then(|value| value.as_str())
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                TbError::InvalidInput("provisionDeviceKey is required for provisioning".into())
            })?;
        if state
            .device_profile_dao
            .find_by_provision_key(None, key)
            .await?
            .is_some_and(|existing| existing.id != profile.id)
        {
            return Err(TbError::Conflict("provisionDeviceKey already exists".into()).into());
        }
    }
    let saved = state.device_profile_dao.save(&profile).await?;
    if let Some(previous) = &previous {
        for kind in OtaKind::ALL {
            let before = kind.profile_binding(previous);
            if before != kind.profile_binding(&saved) {
                crate::ota::spawn_profile_sync(&state, saved.id, kind, before);
            }
        }
    }
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::DeviceProfile, saved.name.clone())),
        if is_new { "CREATED" } else { "UPDATED" },
        None,
        "SUCCESS",
    );
    Ok(Json(saved))
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
    audit(
        &state,
        &user,
        Some((profile.id, EntityType::DeviceProfile, profile.name)),
        "DELETED",
        None,
        "SUCCESS",
    );
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
