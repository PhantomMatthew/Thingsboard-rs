//! /api/otaPackage(s)：OTA 包管理（multipart 上传/下载/分页）与设备、档案级绑定。
//! 绑定后设备在线时经 tb_transport.notifications.<node_id> 推 ota_update。

use axum::extract::{Multipart, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use prost::Message as _;
use serde::Deserialize;
use sha2::Digest as _;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::{Authority, OtaPackage};
use tb_dao::{PageData, PageLink};
use tb_proto::common::OtaPackageType;
use tb_proto::core as core_pb;
use tb_queue::{QueueProducer as _, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/otaPackage", post(save_ota_package))
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
            "/api/deviceProfile/{profileId}/firmware/{otaId}",
            post(assign_profile_firmware),
        )
        .route(
            "/api/deviceProfile/{profileId}/software/{otaId}",
            post(assign_profile_software),
        )
}

async fn load_ota(state: &AppState, id: Uuid) -> TbResult<OtaPackage> {
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
                data = Some(field.bytes().await.map_err(TbError::internal)?);
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
    let package = load_ota(&state, id).await?;
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
            .find_by_tenant_id_filtered(
                user.0.tenant_id,
                query.device_profile_id,
                query.r#type.as_deref(),
                &page_link,
            )
            .await?,
    ))
}

/// 向设备全部在线会话推 ota_update（跨节点逐 session 发送）。
async fn notify_ota_update(
    state: &AppState,
    device_id: Uuid,
    ota_type: OtaPackageType,
    package: Option<&OtaPackage>,
) -> TbResult<()> {
    let sessions = state.device_sessions.any_sessions(device_id).await?;
    if sessions.is_empty() {
        return Ok(());
    }
    let producer = state.queue_producer().await?;
    for session in sessions {
        let msg = core_pb::ToTransportMsg {
            session_id: session.session_id.clone(),
            entity_id: device_id.to_string(),
            msg: Some(core_pb::to_transport_msg::Msg::OtaUpdate(
                core_pb::OtaUpdateNotification {
                    ota_type: ota_type as i32,
                    ota_package: package.map(crate::transport_api::ota_proto),
                },
            )),
        };
        let topic = TbTopic::transport_notifications(&session.node_id);
        producer
            .send(
                &topic,
                &session.session_id,
                Bytes::from(msg.encode_to_vec()),
            )
            .await
            .map_err(TbError::internal)?;
    }
    Ok(())
}

async fn assign_device_ota(
    state: &AppState,
    user: &AuthUser,
    device_id: Uuid,
    ota_id: Uuid,
    ota_type: OtaPackageType,
) -> TbResult<()> {
    let mut device = crate::routes::device::load_device(state, device_id).await?;
    check_tenant_scope(user, device.tenant_id)?;
    let package = load_ota(state, ota_id).await?;
    check_tenant_scope(user, package.tenant_id)?;
    match ota_type {
        OtaPackageType::Firmware => device.firmware_id = Some(ota_id),
        OtaPackageType::Software => device.software_id = Some(ota_id),
        _ => return Err(TbError::InvalidInput("bad ota type".into())),
    }
    state.device_dao.save(&device).await?;
    notify_ota_update(state, device_id, ota_type, Some(&package)).await
}

async fn assign_device_firmware(
    State(state): State<AppState>,
    user: AuthUser,
    Path((device_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    assign_device_ota(&state, &user, device_id, ota_id, OtaPackageType::Firmware).await?;
    Ok(StatusCode::OK)
}

async fn assign_device_software(
    State(state): State<AppState>,
    user: AuthUser,
    Path((device_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    assign_device_ota(&state, &user, device_id, ota_id, OtaPackageType::Software).await?;
    Ok(StatusCode::OK)
}

async fn assign_profile_ota(
    state: &AppState,
    user: &AuthUser,
    profile_id: Uuid,
    ota_id: Uuid,
    ota_type: OtaPackageType,
) -> TbResult<()> {
    let mut profile = state
        .device_profile_dao
        .find_by_id(profile_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device profile {profile_id}")))?;
    check_tenant_scope(user, profile.tenant_id)?;
    let package = load_ota(state, ota_id).await?;
    check_tenant_scope(user, package.tenant_id)?;
    match ota_type {
        OtaPackageType::Firmware => profile.firmware_id = Some(ota_id),
        OtaPackageType::Software => profile.software_id = Some(ota_id),
        _ => return Err(TbError::InvalidInput("bad ota type".into())),
    }
    state.device_profile_dao.save(&profile).await?;
    for device_id in state.device_dao.find_ids_by_profile_id(profile_id).await? {
        notify_ota_update(state, device_id, ota_type, Some(&package)).await?;
    }
    Ok(())
}

async fn assign_profile_firmware(
    State(state): State<AppState>,
    user: AuthUser,
    Path((profile_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    assign_profile_ota(&state, &user, profile_id, ota_id, OtaPackageType::Firmware).await?;
    Ok(StatusCode::OK)
}

async fn assign_profile_software(
    State(state): State<AppState>,
    user: AuthUser,
    Path((profile_id, ota_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    assign_profile_ota(&state, &user, profile_id, ota_id, OtaPackageType::Software).await?;
    Ok(StatusCode::OK)
}
