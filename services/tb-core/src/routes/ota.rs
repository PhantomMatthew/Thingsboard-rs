//! /api/otaPackage(s)：OTA 包管理（multipart 上传/下载/分页）与设备、档案级绑定/解绑。
//! 绑定校验与状态回写见 crate::ota；被绑定的包不可删除。

use axum::extract::{DefaultBodyLimit, Multipart, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use sha2::Digest as _;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::{Authority, OtaPackage};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::ota::OtaKind;

pub(crate) const MAX_OTA_PACKAGE_SIZE: usize = 256 * 1024 * 1024;

fn validate_ota_package_size(size: usize) -> TbResult<()> {
    if size > MAX_OTA_PACKAGE_SIZE {
        return Err(TbError::InvalidInput(
            "ota package file exceeds 256 MiB".into(),
        ));
    }
    Ok(())
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/otaPackage",
            post(save_ota_package).layer(DefaultBodyLimit::max(MAX_OTA_PACKAGE_SIZE)),
        )
        .route(
            "/api/otaPackage/{id}",
            get(get_ota_package).delete(delete_ota_package),
        )
        .route("/api/otaPackage/{id}/download", get(download_ota_package))
        .route("/api/otaPackages", get(list_ota_packages))
        .route(
            "/api/device/{deviceId}/firmware/{otaId}",
            post(assign_device_firmware),
        )
        .route(
            "/api/device/{deviceId}/software/{otaId}",
            post(assign_device_software),
        )
        .route(
            "/api/device/{deviceId}/firmware",
            delete(unassign_device_firmware),
        )
        .route(
            "/api/device/{deviceId}/software",
            delete(unassign_device_software),
        )
        .route(
            "/api/deviceProfile/{profileId}/firmware/{otaId}",
            post(assign_profile_firmware),
        )
        .route(
            "/api/deviceProfile/{profileId}/software/{otaId}",
            post(assign_profile_software),
        )
        .route(
            "/api/deviceProfile/{profileId}/firmware",
            delete(unassign_profile_firmware),
        )
        .route(
            "/api/deviceProfile/{profileId}/software",
            delete(unassign_profile_software),
        )
}

async fn load_ota(state: &AppState, id: Uuid) -> TbResult<OtaPackage> {
    state
        .ota_package_dao
        .find_meta_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("ota package {id}")))
}

async fn load_ota_with_data(state: &AppState, id: Uuid) -> TbResult<OtaPackage> {
    state
        .ota_package_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("ota package {id}")))
}

/// multipart 字段：title/version/type(FIRMWARE|SOFTWARE)/deviceProfileId/tag? + file。
async fn save_ota_package(
    State(state): State<AppState>,
    user: AuthUser,
    mut multipart: Multipart,
) -> Result<Json<OtaPackage>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let mut title = None;
    let mut version = None;
    let mut ota_type = None;
    let mut profile_id = None;
    let mut tag = None;
    let mut file_name = None;
    let mut content_type = None;
    let mut data = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| TbError::InvalidInput(format!("bad multipart: {e}")))?
    {
        match field.name() {
            Some("title") => title = Some(field.text().await.map_err(TbError::internal)?),
            Some("version") => version = Some(field.text().await.map_err(TbError::internal)?),
            Some("type") => ota_type = Some(field.text().await.map_err(TbError::internal)?),
            Some("deviceProfileId") => {
                profile_id = Some(field.text().await.map_err(TbError::internal)?)
            }
            Some("tag") => tag = Some(field.text().await.map_err(TbError::internal)?),
            Some("file") => {
                file_name = field.file_name().map(str::to_string);
                content_type = field.content_type().map(str::to_string);
                let bytes = field.bytes().await.map_err(TbError::internal)?;
                validate_ota_package_size(bytes.len())?;
                data = Some(bytes);
            }
            _ => {}
        }
    }
    let (Some(title), Some(version), Some(ota_type), Some(profile_id), Some(data)) =
        (title, version, ota_type, profile_id, data)
    else {
        return Err(TbError::InvalidInput(
            "title/version/type/deviceProfileId/file are required".into(),
        )
        .into());
    };
    match ota_type.as_str() {
        "FIRMWARE" | "SOFTWARE" => {}
        other => return Err(TbError::InvalidInput(format!("bad type: {other}")).into()),
    }
    let device_profile_id = Uuid::parse_str(&profile_id)
        .map_err(|_| TbError::InvalidInput("bad deviceProfileId".into()))?;
    let profile = state
        .device_profile_dao
        .find_by_id(device_profile_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device profile {device_profile_id}")))?;
    check_tenant_scope(&user, profile.tenant_id)?;
    let checksum = hex::encode(sha2::Sha256::digest(&data));
    let package = OtaPackage {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id: user.0.tenant_id,
        device_profile_id,
        r#type: ota_type,
        title,
        version,
        tag,
        file_name,
        content_type,
        checksum_algorithm: Some("SHA256".into()),
        checksum: Some(checksum),
        size: Some(data.len() as i64),
        data: Some(data.to_vec()),
        additional_info: None,
    };
    let saved = state.ota_package_dao.save(&package).await?;
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::OtaPackage, saved.title.clone())),
        "CREATED",
        None,
        "SUCCESS",
    );
    Ok(Json(saved))
}

async fn get_ota_package(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<OtaPackage>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let package = load_ota(&state, id).await?;
    check_tenant_scope(&user, package.tenant_id)?;
    Ok(Json(package))
}

async fn delete_ota_package(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let package = load_ota(&state, id).await?;
    check_tenant_scope(&user, package.tenant_id)?;
    if state.ota_package_dao.is_referenced(id).await? {
        return Err(TbError::Conflict(
            "ota package is assigned to a device or device profile".into(),
        )
        .into());
    }
    state.ota_package_dao.delete_by_id(id).await?;
    audit(
        &state,
        &user,
        Some((package.id, EntityType::OtaPackage, package.title)),
        "DELETED",
        None,
        "SUCCESS",
    );
    Ok(StatusCode::OK)
}

async fn download_ota_package(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let package = load_ota_with_data(&state, id).await?;
    check_tenant_scope(&user, package.tenant_id)?;
    let data = package
        .data
        .ok_or_else(|| TbError::NotFound("ota package data".into()))?;
    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], data).into_response())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OtaPageQuery {
    page: Option<i64>,
    page_size: Option<i64>,
    text_search: Option<String>,
    sort_property: Option<String>,
    sort_order: Option<String>,
    device_profile_id: Option<Uuid>,
    r#type: Option<String>,
}

async fn list_ota_packages(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<OtaPageQuery>,
) -> Result<Json<PageData<OtaPackage>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link = PageLink {
        page: query.page.unwrap_or(0),
        page_size: query.page_size.unwrap_or(10),
        text_search: query.text_search,
        sort_property: query.sort_property,
        sort_order: query.sort_order,
    };
    Ok(Json(
        state
            .ota_package_dao
            .find_meta_by_tenant_filtered(
                user.0.tenant_id,
                query.device_profile_id,
                query.r#type.as_deref(),
                &page_link,
            )
            .await?,
    ))
}

/// 校验并加载待绑定的包：同租户（403）、类型/档案一致且有数据（400）。
async fn load_bindable(
    state: &AppState,
    user: &AuthUser,
    ota_id: Uuid,
    kind: OtaKind,
    profile_id: Uuid,
) -> TbResult<OtaPackage> {
    let package = load_ota(state, ota_id).await?;
    check_tenant_scope(user, package.tenant_id)?;
    crate::ota::validate_binding(&package, kind, user.0.tenant_id, profile_id)?;
    Ok(package)
}

/// 设备级绑定（None = 解绑，回落到档案绑定）；生效包变化时同步回写状态。
async fn bind_device_ota(
    state: &AppState,
    user: &AuthUser,
    device_id: Uuid,
    ota_id: Option<Uuid>,
    kind: OtaKind,
) -> TbResult<()> {
    let mut device = crate::routes::device::load_device(state, device_id).await?;
    check_tenant_scope(user, device.tenant_id)?;
    if let Some(ota_id) = ota_id {
        load_bindable(state, user, ota_id, kind, device.device_profile_id).await?;
    }
    let profile = state
        .device_profile_dao
        .find_by_id(device.device_profile_id)
        .await?;
    let previous = crate::ota::effective_binding(kind, &device, profile.as_ref());
    kind.set_device_binding(&mut device, ota_id);
    let saved = state.device_dao.save(&device).await?;
    crate::ota::sync_device(state, &saved, kind, previous).await
}

async fn assign_device_firmware(
    State(state): State<AppState>,
    user: AuthUser,
    Path((device_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_device_ota(&state, &user, device_id, Some(ota_id), OtaKind::Firmware).await?;
    Ok(StatusCode::OK)
}

async fn assign_device_software(
    State(state): State<AppState>,
    user: AuthUser,
    Path((device_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_device_ota(&state, &user, device_id, Some(ota_id), OtaKind::Software).await?;
    Ok(StatusCode::OK)
}

async fn unassign_device_firmware(
    State(state): State<AppState>,
    user: AuthUser,
    Path(device_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_device_ota(&state, &user, device_id, None, OtaKind::Firmware).await?;
    Ok(StatusCode::OK)
}

async fn unassign_device_software(
    State(state): State<AppState>,
    user: AuthUser,
    Path(device_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_device_ota(&state, &user, device_id, None, OtaKind::Software).await?;
    Ok(StatusCode::OK)
}

/// 档案级绑定（None = 解绑）；继承该绑定的设备在后台同步回写状态。
async fn bind_profile_ota(
    state: &AppState,
    user: &AuthUser,
    profile_id: Uuid,
    ota_id: Option<Uuid>,
    kind: OtaKind,
) -> TbResult<()> {
    let mut profile = state
        .device_profile_dao
        .find_by_id(profile_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device profile {profile_id}")))?;
    check_tenant_scope(user, profile.tenant_id)?;
    if let Some(ota_id) = ota_id {
        load_bindable(state, user, ota_id, kind, profile_id).await?;
    }
    let previous = kind.profile_binding(&profile);
    kind.set_profile_binding(&mut profile, ota_id);
    state.device_profile_dao.save(&profile).await?;
    if previous != ota_id {
        crate::ota::spawn_profile_sync(state, profile_id, kind, previous);
    }
    Ok(())
}

async fn assign_profile_firmware(
    State(state): State<AppState>,
    user: AuthUser,
    Path((profile_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_profile_ota(&state, &user, profile_id, Some(ota_id), OtaKind::Firmware).await?;
    Ok(StatusCode::OK)
}

async fn assign_profile_software(
    State(state): State<AppState>,
    user: AuthUser,
    Path((profile_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_profile_ota(&state, &user, profile_id, Some(ota_id), OtaKind::Software).await?;
    Ok(StatusCode::OK)
}

async fn unassign_profile_firmware(
    State(state): State<AppState>,
    user: AuthUser,
    Path(profile_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_profile_ota(&state, &user, profile_id, None, OtaKind::Firmware).await?;
    Ok(StatusCode::OK)
}

async fn unassign_profile_software(
    State(state): State<AppState>,
    user: AuthUser,
    Path(profile_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    bind_profile_ota(&state, &user, profile_id, None, OtaKind::Software).await?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ota_package_size_limit_is_explicit() {
        assert!(validate_ota_package_size(MAX_OTA_PACKAGE_SIZE).is_ok());
        assert!(validate_ota_package_size(MAX_OTA_PACKAGE_SIZE + 1).is_err());
    }
}
