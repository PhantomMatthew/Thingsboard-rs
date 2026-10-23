//! /api/calculatedField(s)：TENANT_ADMIN 管理计算字段；按实体查询配置。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError};
use tb_dao::models::{Authority, CalculatedField};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/calculatedField", post(save_calculated_field))
        .route(
            "/api/calculatedField/{id}",
            get(get_calculated_field).delete(delete_calculated_field),
        )
        .route("/api/calculatedFields", get(list_calculated_fields))
        .route(
            "/api/entity/{entityType}/{entityId}/calculatedFields",
            get(list_by_entity),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveCalculatedFieldRequest {
    pub id: Option<Uuid>,
    pub entity_id: Uuid,
    pub name: String,
    pub configuration: Value,
}

async fn save_calculated_field(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveCalculatedFieldRequest>,
) -> Result<Json<CalculatedField>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    if tb_calculated::CalculatedFieldConfig::parse(&req.configuration).is_none() {
        return Err(TbError::InvalidInput(
            "configuration is not a valid calculated field config".into(),
        )
        .into());
    }
    let (id, created_time) = match req.id {
        Some(id) => {
            let existing = load_and_check(&state, &user, id).await?;
            (id, existing.created_time)
        }
        None => (Uuid::now_v7(), 0),
    };
    let field = CalculatedField {
        id,
        created_time,
        tenant_id: user.0.tenant_id,
        entity_id: req.entity_id,
        name: req.name,
        configuration: req.configuration,
    };
    Ok(Json(state.calculated_field_dao.save(&field).await?))
}

async fn get_calculated_field(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<CalculatedField>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    Ok(Json(load_and_check(&state, &user, id).await?))
}

async fn delete_calculated_field(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    load_and_check(&state, &user, id).await?;
    state.calculated_field_dao.delete_by_id(id).await?;
    Ok(StatusCode::OK)
}

async fn list_calculated_fields(
    State(state): State<AppState>,
    user: AuthUser,
    Query(page): Query<PageQuery>,
) -> Result<Json<PageData<CalculatedField>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = page.into();
    Ok(Json(
        state
            .calculated_field_dao
            .find_by_tenant(user.0.tenant_id, &page_link)
            .await?,
    ))
}

async fn list_by_entity(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
) -> Result<Json<Vec<CalculatedField>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let entity_type: EntityType = entity_type.parse()?;
    crate::routes::telemetry::check_entity_access(&state, &user, entity_type, entity_id).await?;
    Ok(Json(
        state.calculated_field_dao.find_by_entity(entity_id).await?,
    ))
}

async fn load_and_check(
    state: &AppState,
    user: &AuthUser,
    id: Uuid,
) -> Result<CalculatedField, TbError> {
    let field = state
        .calculated_field_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("calculated field {id}")))?;
    check_tenant_scope(user, field.tenant_id)?;
    Ok(field)
}
