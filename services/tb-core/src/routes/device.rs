//! /api/device(s)：设备与设备凭证管理。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use prost::Message as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::{Authority, CredentialsType, Device, DeviceCredentials, DeviceProfile};
use tb_dao::{PageData, PageLink};
use tb_proto::core as core_pb;
use tb_queue::{QueueProducer as _, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;
use crate::routes::customer::load_customer;

async fn close_device_sessions(state: &AppState, device: &Device, reason: &str) -> TbResult<()> {
    let sessions = state.device_sessions.any_sessions(device.id).await?;
    if sessions.is_empty() {
        return Ok(());
    }
    let producer = state.queue_producer().await?;
    for session in &sessions {
        let msg = core_pb::ToTransportMsg {
            session_id: session.session_id.clone(),
            entity_id: device.id.to_string(),
            msg: Some(core_pb::to_transport_msg::Msg::SessionCloseNotification(
                core_pb::SessionCloseNotification {
                    reason: reason.into(),
                },
            )),
        };
        producer
            .send(
                &TbTopic::transport_notifications(&session.node_id),
                &session.session_id,
                Bytes::from(msg.encode_to_vec()),
            )
            .await
            .map_err(TbError::internal)?;
    }
    state.device_sessions.take_sessions(device.id).await?;
    Ok(())
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/device", post(save_device))
        .route("/api/device/types", get(device_types))
        .route("/api/device/credentials", post(save_device_credentials))
        .route("/api/device/{id}", get(get_device).delete(delete_device))
        .route("/api/device/{id}/credentials", get(get_device_credentials))
        .route("/api/devices", get(list_devices))
        .route(
            "/api/customer/{customerId}/devices",
            get(list_customer_devices),
        )
        .route(
            "/api/device/{deviceId}/assign/{customerId}",
            post(assign_device).delete(unassign_device),
        )
        .route(
            "/api/customer/device/{deviceName}/claim",
            post(claim_device),
        )
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaimDeviceRequest {
    #[serde(default)]
    secret_key: String,
}

async fn claim_device(
    State(state): State<AppState>,
    user: AuthUser,
    Path(device_name): Path<String>,
    Json(request): Json<ClaimDeviceRequest>,
) -> Result<Response, ApiError> {
    require_authority(&user, &[Authority::CustomerUser])?;
    let customer_id = user
        .0
        .customer_id
        .ok_or_else(|| TbError::PermissionDenied("customer user has no customer".into()))?;
    let customer = load_customer(&state, customer_id).await?;
    check_tenant_scope(&user, customer.tenant_id)?;
    let device = state
        .device_dao
        .find_by_name(user.0.tenant_id, &device_name)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device {device_name}")))?;
    let result = crate::claim::claim(&state, &device, customer_id, &request.secret_key).await?;
    let response = match result {
        crate::claim::ClaimResult::Success(device) => {
            publish_claimed_device(&state, &user, &device);
            return Ok(Json(json!({"device": device, "response": "SUCCESS"})).into_response());
        }
        crate::claim::ClaimResult::Failure => "FAILURE",
        crate::claim::ClaimResult::Claimed => {
            let device = load_device(&state, device.id).await?;
            publish_claimed_device(&state, &user, &device);
            "CLAIMED"
        }
    };
    Ok((StatusCode::BAD_REQUEST, response).into_response())
}

fn publish_claimed_device(state: &AppState, user: &AuthUser, device: &Device) {
    crate::edqs::publish_entity_upsert(
        state,
        device.tenant_id,
        device.id,
        EntityType::Device,
        device.customer_id,
        &device.name,
        device.r#type.as_deref().unwrap_or_default(),
        device.label.as_deref(),
        device.created_time,
    );
    audit(
        state,
        user,
        Some((device.id, EntityType::Device, device.name.clone())),
        "ASSIGNED_TO_CUSTOMER",
        device
            .customer_id
            .map(|customer_id| json!({"customerId": customer_id})),
        "SUCCESS",
    );
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveDeviceRequest {
    pub id: Option<Uuid>,
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub label: Option<String>,
    pub device_profile_id: Option<Uuid>,
    pub customer_id: Option<Uuid>,
    /// 可选：同时创建 ACCESS_TOKEN 类型凭证。
    pub access_token: Option<String>,
    pub additional_info: Option<Value>,
}

async fn save_device(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveDeviceRequest>,
) -> Result<Json<Device>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let tenant_id = user.0.tenant_id;
    let existing = match req.id {
        Some(id) => {
            let existing = load_device(&state, id).await?;
            check_tenant_scope(&user, existing.tenant_id)?;
            Some(existing)
        }
        None => None,
    };
    let is_new = existing.is_none();
    let profile_id = match req.device_profile_id {
        Some(id) => id,
        None => match existing.as_ref() {
            Some(existing) => existing.device_profile_id,
            None => ensure_default_device_profile(&state, tenant_id).await?.id,
        },
    };
    let profile = state
        .device_profile_dao
        .find_by_id(profile_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device profile {profile_id}")))?;
    check_tenant_scope(&user, profile.tenant_id)?;
    let customer_id = req
        .customer_id
        .or_else(|| existing.as_ref().and_then(|device| device.customer_id));
    if let Some(customer_id) = customer_id {
        let customer = load_customer(&state, customer_id).await?;
        check_tenant_scope(&user, customer.tenant_id)?;
    }

    let mut device = existing.unwrap_or(Device {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        customer_id: None,
        device_profile_id: profile_id,
        name: String::new(),
        r#type: None,
        label: None,
        external_id: None,
        firmware_id: None,
        software_id: None,
        additional_info: None,
        device_data: json!({}),
    });
    device.name = req.name;
    device.device_profile_id = profile_id;
    device.customer_id = customer_id;
    if req.r#type.is_some() {
        device.r#type = req.r#type;
    }
    if req.label.is_some() {
        device.label = req.label;
    }
    if req.additional_info.is_some() {
        device.additional_info = req.additional_info;
    }

    // 新设备总是带凭证；更新仅在显式给 accessToken 时轮换。
    let rotate_credentials = is_new || req.access_token.is_some();
    let token = req
        .access_token
        .unwrap_or_else(|| Uuid::now_v7().to_string());
    let saved = if rotate_credentials {
        let credentials = DeviceCredentials {
            id: Uuid::now_v7(),
            created_time: 0,
            device_id: device.id,
            credentials_type: CredentialsType::AccessToken,
            credentials_id: Some(token),
            credentials_value: None,
        };
        state
            .device_credentials_dao
            .save_device_with_credentials(&device, &credentials)
            .await?
            .0
    } else {
        state.device_dao.save(&device).await?
    };
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::Device, saved.name.clone())),
        if is_new { "CREATED" } else { "UPDATED" },
        None,
        "SUCCESS",
    );
    crate::edqs::publish_entity_upsert(
        &state,
        saved.tenant_id,
        saved.id,
        EntityType::Device,
        saved.customer_id,
        &saved.name,
        saved.r#type.as_deref().unwrap_or_default(),
        saved.label.as_deref(),
        saved.created_time,
    );
    if !is_new && rotate_credentials {
        close_device_sessions(&state, &saved, "device credentials updated").await?;
    }
    Ok(Json(saved))
}

async fn get_device(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<Device>, ApiError> {
    let device = load_device(&state, id).await?;
    check_device_read(&user, &device)?;
    Ok(Json(device))
}

async fn delete_device(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let device = load_device(&state, id).await?;
    check_tenant_scope(&user, device.tenant_id)?;
    let mut tx = state.pool.begin().await.map_err(TbError::db)?;
    state.timeseries_dao.delete_by_entity(&mut tx, id).await?;
    state.attributes_dao.delete_by_entity(&mut tx, id).await?;
    state.alarm_dao.delete_by_entity(&mut tx, id).await?;
    state
        .calculated_field_dao
        .delete_by_entity(&mut tx, id)
        .await?;
    state.device_dao.delete_by_id_in(&mut tx, id).await?;
    tx.commit().await.map_err(TbError::db)?;
    crate::telemetry_cache::invalidate_latest(&state, id).await;
    close_device_sessions(&state, &device, "device deleted").await?;
    audit(
        &state,
        &user,
        Some((device.id, EntityType::Device, device.name)),
        "DELETED",
        None,
        "SUCCESS",
    );
    crate::edqs::publish_entity_delete(&state, device.tenant_id, id, EntityType::Device);
    Ok(StatusCode::OK)
}

async fn list_devices(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<Device>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .device_dao
            .find_by_tenant_id(user.0.tenant_id, &page_link)
            .await?,
    ))
}

async fn list_customer_devices(
    State(state): State<AppState>,
    user: AuthUser,
    Path(customer_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<Device>>, ApiError> {
    let customer = load_customer(&state, customer_id).await?;
    match user.0.authority {
        Authority::TenantAdmin => check_tenant_scope(&user, customer.tenant_id)?,
        Authority::CustomerUser if user.0.customer_id == Some(customer_id) => {}
        _ => {
            return Err(
                TbError::PermissionDenied("not allowed to read these devices".into()).into(),
            );
        }
    }
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .device_dao
            .find_by_customer_id(customer.tenant_id, customer_id, &page_link)
            .await?,
    ))
}

async fn assign_device(
    State(state): State<AppState>,
    user: AuthUser,
    Path((device_id, customer_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Device>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let mut device = load_device(&state, device_id).await?;
    check_tenant_scope(&user, device.tenant_id)?;
    let customer = load_customer(&state, customer_id).await?;
    check_tenant_scope(&user, customer.tenant_id)?;
    device.customer_id = Some(customer_id);
    let saved = state.device_dao.save(&device).await?;
    crate::edqs::publish_entity_upsert(
        &state,
        saved.tenant_id,
        saved.id,
        EntityType::Device,
        saved.customer_id,
        &saved.name,
        saved.r#type.as_deref().unwrap_or_default(),
        saved.label.as_deref(),
        saved.created_time,
    );
    Ok(Json(saved))
}

async fn unassign_device(
    State(state): State<AppState>,
    user: AuthUser,
    Path((device_id, _customer_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Device>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let mut device = load_device(&state, device_id).await?;
    check_tenant_scope(&user, device.tenant_id)?;
    device.customer_id = None;
    let saved = state.device_dao.save(&device).await?;
    crate::edqs::publish_entity_upsert(
        &state,
        saved.tenant_id,
        saved.id,
        EntityType::Device,
        saved.customer_id,
        &saved.name,
        saved.r#type.as_deref().unwrap_or_default(),
        saved.label.as_deref(),
        saved.created_time,
    );
    Ok(Json(saved))
}

async fn get_device_credentials(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<DeviceCredentials>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let device = load_device(&state, id).await?;
    check_tenant_scope(&user, device.tenant_id)?;
    let credentials = state
        .device_credentials_dao
        .find_by_device_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("credentials for device {id}")))?;
    Ok(Json(credentials))
}

async fn save_device_credentials(
    State(state): State<AppState>,
    user: AuthUser,
    Json(mut credentials): Json<DeviceCredentials>,
) -> Result<Json<DeviceCredentials>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let device = load_device(&state, credentials.device_id).await?;
    check_tenant_scope(&user, device.tenant_id)?;
    if credentials.credentials_type == CredentialsType::X509Certificate {
        let certificate = tb_common::x509::parse_x509_credential(
            credentials.credentials_value.as_deref().unwrap_or(""),
        )?;
        credentials.credentials_id = Some(certificate.credentials_id);
        credentials.credentials_value = Some(certificate.certificate_base64);
    }
    if credentials.id.is_nil() {
        credentials.id = Uuid::now_v7();
    }
    let saved = state.device_credentials_dao.save(&credentials).await?;
    close_device_sessions(&state, &device, "device credentials updated").await?;
    Ok(Json(saved))
}

async fn device_types(
    State(state): State<AppState>,
    user: AuthUser,
) -> Result<Json<Vec<String>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let types: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT type FROM device WHERE tenant_id = $1 AND type IS NOT NULL ORDER BY type",
    )
    .bind(user.0.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(TbError::db)?;
    Ok(Json(types))
}

fn check_device_read(user: &AuthUser, device: &Device) -> TbResult<()> {
    match user.0.authority {
        Authority::TenantAdmin => check_tenant_scope(user, device.tenant_id),
        Authority::CustomerUser
            if user.0.customer_id.is_some() && user.0.customer_id == device.customer_id =>
        {
            Ok(())
        }
        _ => Err(TbError::PermissionDenied(
            "not allowed to read this device".into(),
        )),
    }
}

pub async fn load_device(state: &AppState, id: Uuid) -> TbResult<Device> {
    state
        .device_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("device {id}")))
}

pub async fn ensure_default_device_profile(
    state: &AppState,
    tenant_id: Uuid,
) -> TbResult<DeviceProfile> {
    if let Some(profile) = state.device_profile_dao.find_default(tenant_id).await? {
        return Ok(profile);
    }
    let profile = DeviceProfile {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        name: "default".into(),
        description: None,
        is_default: true,
        transport_type: "DEFAULT".into(),
        provision_type: "DISABLED".into(),
        profile_data: json!({}),
        firmware_id: None,
        software_id: None,
    };
    // 并发首建可能撞上 (tenant_id, name) 唯一约束，重读一次兜底。
    match state.device_profile_dao.save(&profile).await {
        Ok(p) => Ok(p),
        Err(_) => state
            .device_profile_dao
            .find_default(tenant_id)
            .await?
            .ok_or_else(|| TbError::Internal("default device profile lost".into())),
    }
}
