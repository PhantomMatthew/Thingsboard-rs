//! /api/tenant(s)：仅 SYS_ADMIN。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError};
use tb_dao::entity_cleanup::TenantEntities;
use tb_dao::models::{Authority, Tenant};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::require_authority;
use crate::routes::PageQuery;
use crate::routes::device::close_device_sessions;

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
    let is_new = req.id.is_none();
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
    let saved = state.tenant_dao.save(&tenant).await?;
    if is_new {
        crate::routes::rule_chain::seed_default_root_chain(&state, saved.id).await?;
    }
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::Tenant, saved.title.clone())),
        if is_new { "CREATED" } else { "UPDATED" },
        None,
        "SUCCESS",
    );
    Ok(Json(saved))
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
    let existing = state
        .tenant_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("tenant {id}")))?;
    let mut tx = state.pool.begin().await.map_err(TbError::db)?;
    let entities = tb_dao::entity_cleanup::purge_tenant_data(&mut tx, id).await?;
    if !state.tenant_dao.delete_by_id_in(&mut tx, id).await? {
        return Err(TbError::NotFound(format!("tenant {id}")).into());
    }
    tx.commit().await.map_err(TbError::db)?;
    spawn_runtime_cleanup(state.clone(), entities);
    crate::edqs::publish_tenant_delete(&state, id);
    audit(
        &state,
        &user,
        Some((existing.id, EntityType::Tenant, existing.title)),
        "DELETED",
        None,
        "SUCCESS",
    );
    Ok(StatusCode::OK)
}

/// 提交后关闭租户设备会话并清 Redis latest 缓存；设备可能很多，不阻塞请求。
fn spawn_runtime_cleanup(state: AppState, entities: TenantEntities) {
    tokio::spawn(async move {
        for device_id in entities.device_ids {
            if let Err(e) = close_device_sessions(&state, device_id, "tenant deleted").await {
                tracing::warn!(%device_id, "close sessions after tenant delete failed: {e}");
            }
        }
        for entity_id in entities.entity_ids {
            crate::telemetry_cache::invalidate_latest(&state, entity_id).await;
        }
    });
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
