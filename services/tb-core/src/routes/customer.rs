//! /api/customer(s)：TENANT_ADMIN 管理本租户客户。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError};
use tb_dao::models::{Authority, Customer};
use tb_dao::{PageData, PageLink, entity_cleanup};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/customer", post(save_customer))
        .route(
            "/api/customer/{id}",
            get(get_customer).delete(delete_customer),
        )
        .route("/api/customers", get(list_customers))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveCustomerRequest {
    pub id: Option<Uuid>,
    pub title: String,
    pub country: Option<String>,
    pub state: Option<String>,
    pub city: Option<String>,
    pub address: Option<String>,
    pub address2: Option<String>,
    pub zip: Option<String>,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub additional_info: Option<Value>,
}

async fn save_customer(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveCustomerRequest>,
) -> Result<Json<Customer>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let is_new = req.id.is_none();
    let (id, created_time) = match req.id {
        Some(id) => {
            let existing = state
                .customer_dao
                .find_by_id(id)
                .await?
                .ok_or_else(|| TbError::NotFound(format!("customer {id}")))?;
            check_tenant_scope(&user, existing.tenant_id)?;
            (id, existing.created_time)
        }
        None => (Uuid::now_v7(), 0),
    };
    let customer = Customer {
        id,
        created_time,
        tenant_id: user.0.tenant_id,
        title: req.title,
        country: req.country,
        state: req.state,
        city: req.city,
        address: req.address,
        address2: req.address2,
        zip: req.zip,
        phone: req.phone,
        email: req.email,
        additional_info: req.additional_info,
    };
    let saved = state.customer_dao.save(&customer).await?;
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::Customer, saved.title.clone())),
        if is_new { "CREATED" } else { "UPDATED" },
        None,
        "SUCCESS",
    );
    Ok(Json(saved))
}

async fn get_customer(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<Customer>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let customer = load_customer(&state, id).await?;
    check_tenant_scope(&user, customer.tenant_id)?;
    Ok(Json(customer))
}

async fn delete_customer(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let customer = load_customer(&state, id).await?;
    check_tenant_scope(&user, customer.tenant_id)?;
    let mut tx = state.pool.begin().await.map_err(TbError::db)?;
    let entity_ids = entity_cleanup::customer_entity_ids(&mut tx, id).await?;
    entity_cleanup::purge_deleted_entities(&mut tx, customer.tenant_id, &entity_ids).await?;
    entity_cleanup::detach_customer(&mut tx, id).await?;
    state.customer_dao.delete_by_id_in(&mut tx, id).await?;
    tx.commit().await.map_err(TbError::db)?;
    for entity_id in entity_ids {
        crate::telemetry_cache::invalidate_latest(&state, entity_id).await;
    }
    crate::edqs::publish_tenant_resync(&state, customer.tenant_id);
    audit(
        &state,
        &user,
        Some((customer.id, EntityType::Customer, customer.title)),
        "DELETED",
        None,
        "SUCCESS",
    );
    Ok(StatusCode::OK)
}

async fn list_customers(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<Customer>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .customer_dao
            .find_by_tenant_id(user.0.tenant_id, &page_link)
            .await?,
    ))
}

pub async fn load_customer(state: &AppState, id: Uuid) -> tb_common::TbResult<Customer> {
    state
        .customer_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("customer {id}")))
}
