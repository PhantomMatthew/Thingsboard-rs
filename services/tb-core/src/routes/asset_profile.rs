//! /api/assetProfile(s)：资产档案管理（TENANT_ADMIN）。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError};
use tb_dao::models::{AssetProfile, Authority};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/assetProfile", post(save_profile))
        .route("/api/assetProfile/default", get(get_default))
        .route(
            "/api/assetProfile/{id}",
            get(get_profile).delete(delete_profile),
        )
        .route("/api/assetProfiles", get(list_profiles))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveAssetProfileRequest {
    pub id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    pub is_default: Option<bool>,
    pub profile_data: Option<Value>,
}

async fn save_profile(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveAssetProfileRequest>,
) -> Result<Json<AssetProfile>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let is_new = req.id.is_none();
    let (id, created_time) = match req.id {
        Some(id) => {
            let existing = load_profile(&state, id).await?;
            check_tenant_scope(&user, existing.tenant_id)?;
            (id, existing.created_time)
        }
        None => (Uuid::now_v7(), 0),
    };
    let profile = AssetProfile {
        id,
        created_time,
        tenant_id: user.0.tenant_id,
        name: req.name,
        description: req.description,
        is_default: req.is_default.unwrap_or(false),
        profile_data: req.profile_data.unwrap_or_else(|| json!({})),
    };
    let saved = state.asset_profile_dao.save(&profile).await?;
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::AssetProfile, saved.name.clone())),
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
) -> Result<Json<AssetProfile>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let profile = load_profile(&state, id).await?;
    check_tenant_scope(&user, profile.tenant_id)?;
    Ok(Json(profile))
}

async fn get_default(
    State(state): State<AppState>,
    user: AuthUser,
) -> Result<Json<AssetProfile>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let profile = state
        .asset_profile_dao
        .find_default(user.0.tenant_id)
        .await?
        .ok_or_else(|| TbError::NotFound("default asset profile".into()))?;
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
    state.asset_profile_dao.delete_by_id(id).await?;
    audit(
        &state,
        &user,
        Some((profile.id, EntityType::AssetProfile, profile.name)),
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
) -> Result<Json<PageData<AssetProfile>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .asset_profile_dao
            .find_by_tenant_id(user.0.tenant_id, &page_link)
            .await?,
    ))
}

async fn load_profile(state: &AppState, id: Uuid) -> tb_common::TbResult<AssetProfile> {
    state
        .asset_profile_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("asset profile {id}")))
}
