//! /api/asset(s)：资产管理（与 device 同构，无凭证）。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::{Asset, AssetProfile, Authority};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;
use crate::routes::customer::load_customer;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/asset", post(save_asset))
        .route("/api/asset/{id}", get(get_asset).delete(delete_asset))
        .route("/api/assets", get(list_assets))
        .route(
            "/api/customer/{customerId}/assets",
            get(list_customer_assets),
        )
        .route(
            "/api/asset/{assetId}/assign/{customerId}",
            post(assign_asset).delete(unassign_asset),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveAssetRequest {
    pub id: Option<Uuid>,
    pub name: String,
    pub label: Option<String>,
    pub asset_profile_id: Option<Uuid>,
    pub customer_id: Option<Uuid>,
    pub additional_info: Option<Value>,
}

async fn save_asset(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveAssetRequest>,
) -> Result<Json<Asset>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let tenant_id = user.0.tenant_id;
    let existing = match req.id {
        Some(id) => {
            let existing = load_asset(&state, id).await?;
            check_tenant_scope(&user, existing.tenant_id)?;
            Some(existing)
        }
        None => None,
    };
    let is_new = existing.is_none();
    let profile_id = match req.asset_profile_id {
        Some(id) => id,
        None => match existing.as_ref() {
            Some(existing) => existing.asset_profile_id,
            None => ensure_default_asset_profile(&state, tenant_id).await?.id,
        },
    };
    let profile = state
        .asset_profile_dao
        .find_by_id(profile_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("asset profile {profile_id}")))?;
    check_tenant_scope(&user, profile.tenant_id)?;
    let customer_id = req
        .customer_id
        .or_else(|| existing.as_ref().and_then(|asset| asset.customer_id));
    if let Some(customer_id) = customer_id {
        let customer = load_customer(&state, customer_id).await?;
        check_tenant_scope(&user, customer.tenant_id)?;
    }
    let mut asset = existing.unwrap_or(Asset {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        customer_id: None,
        asset_profile_id: profile_id,
        name: String::new(),
        label: None,
        external_id: None,
        additional_info: None,
    });
    asset.name = req.name;
    asset.asset_profile_id = profile_id;
    asset.customer_id = customer_id;
    if req.label.is_some() {
        asset.label = req.label;
    }
    if req.additional_info.is_some() {
        asset.additional_info = req.additional_info;
    }
    let saved = state.asset_dao.save(&asset).await?;
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::Asset, saved.name.clone())),
        if is_new { "CREATED" } else { "UPDATED" },
        None,
        "SUCCESS",
    );
    crate::edqs::publish_entity_upsert(
        &state,
        saved.tenant_id,
        saved.id,
        EntityType::Asset,
        saved.customer_id,
        &saved.name,
        "",
        saved.label.as_deref(),
        saved.created_time,
    );
    Ok(Json(saved))
}

async fn get_asset(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<Asset>, ApiError> {
    let asset = load_asset(&state, id).await?;
    match user.0.authority {
        Authority::TenantAdmin => check_tenant_scope(&user, asset.tenant_id)?,
        Authority::CustomerUser
            if user.0.customer_id.is_some() && user.0.customer_id == asset.customer_id => {}
        _ => {
            return Err(TbError::PermissionDenied("not allowed to read this asset".into()).into());
        }
    }
    Ok(Json(asset))
}

async fn delete_asset(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let asset = load_asset(&state, id).await?;
    check_tenant_scope(&user, asset.tenant_id)?;
    let mut tx = state.pool.begin().await.map_err(TbError::db)?;
    tb_dao::entity_cleanup::purge_deleted_entities(&mut tx, asset.tenant_id, &[id]).await?;
    state.asset_dao.delete_by_id_in(&mut tx, id).await?;
    tx.commit().await.map_err(TbError::db)?;
    crate::telemetry_cache::invalidate_latest(&state, id).await;
    audit(
        &state,
        &user,
        Some((asset.id, EntityType::Asset, asset.name)),
        "DELETED",
        None,
        "SUCCESS",
    );
    crate::edqs::publish_entity_delete(&state, asset.tenant_id, id, EntityType::Asset);
    Ok(StatusCode::OK)
}

async fn list_assets(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<Asset>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .asset_dao
            .find_by_tenant_id(user.0.tenant_id, &page_link)
            .await?,
    ))
}

async fn list_customer_assets(
    State(state): State<AppState>,
    user: AuthUser,
    Path(customer_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<Asset>>, ApiError> {
    let customer = load_customer(&state, customer_id).await?;
    match user.0.authority {
        Authority::TenantAdmin => check_tenant_scope(&user, customer.tenant_id)?,
        Authority::CustomerUser if user.0.customer_id == Some(customer_id) => {}
        _ => {
            return Err(
                TbError::PermissionDenied("not allowed to read these assets".into()).into(),
            );
        }
    }
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .asset_dao
            .find_by_customer_id(customer.tenant_id, customer_id, &page_link)
            .await?,
    ))
}

async fn assign_asset(
    State(state): State<AppState>,
    user: AuthUser,
    Path((asset_id, customer_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Asset>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let mut asset = load_asset(&state, asset_id).await?;
    check_tenant_scope(&user, asset.tenant_id)?;
    let customer = load_customer(&state, customer_id).await?;
    check_tenant_scope(&user, customer.tenant_id)?;
    asset.customer_id = Some(customer_id);
    let saved = state.asset_dao.save(&asset).await?;
    crate::edqs::publish_entity_upsert(
        &state,
        saved.tenant_id,
        saved.id,
        EntityType::Asset,
        saved.customer_id,
        &saved.name,
        "",
        saved.label.as_deref(),
        saved.created_time,
    );
    Ok(Json(saved))
}

async fn unassign_asset(
    State(state): State<AppState>,
    user: AuthUser,
    Path((asset_id, _customer_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Asset>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let mut asset = load_asset(&state, asset_id).await?;
    check_tenant_scope(&user, asset.tenant_id)?;
    asset.customer_id = None;
    let saved = state.asset_dao.save(&asset).await?;
    crate::edqs::publish_entity_upsert(
        &state,
        saved.tenant_id,
        saved.id,
        EntityType::Asset,
        saved.customer_id,
        &saved.name,
        "",
        saved.label.as_deref(),
        saved.created_time,
    );
    Ok(Json(saved))
}

pub async fn load_asset(state: &AppState, id: Uuid) -> TbResult<Asset> {
    state
        .asset_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("asset {id}")))
}

pub async fn ensure_default_asset_profile(
    state: &AppState,
    tenant_id: Uuid,
) -> TbResult<AssetProfile> {
    if let Some(profile) = state.asset_profile_dao.find_default(tenant_id).await? {
        return Ok(profile);
    }
    let profile = AssetProfile {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        name: "default".into(),
        description: None,
        is_default: true,
        profile_data: serde_json::json!({}),
    };
    // 并发首建可能撞上 (tenant_id, name) 唯一约束，重读一次兜底。
    match state.asset_profile_dao.save(&profile).await {
        Ok(p) => Ok(p),
        Err(_) => state
            .asset_profile_dao
            .find_default(tenant_id)
            .await?
            .ok_or_else(|| TbError::Internal("default asset profile lost".into())),
    }
}
