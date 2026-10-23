//! HTTP 查询 API。本期不做认证：tb-edqs 只在内网部署，鉴权由 tb-core 代理端点把关
//! （/api/edqs/query/entityData，强制 tenantId = JWT tenant）。

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::query::{
    EntityDataQuery, EntityFilter, PageDataResult, PageLinkReq, SortBy, SortDirection,
};
use crate::state::EdqsState;

pub fn router() -> Router<EdqsState> {
    Router::new()
        .route("/api/query/entityData", post(query_entity_data))
        .route("/api/query/entityData", get(query_entity_data_get))
        .route("/health", get(health))
}

async fn health() -> &'static str {
    "ok"
}

async fn query_entity_data(
    State(state): State<EdqsState>,
    Json(query): Json<EntityDataQuery>,
) -> Result<Json<PageDataResult>, StatusCode> {
    let Some(repo) = state.tenants.get(&query.tenant_id) else {
        return Ok(Json(PageDataResult {
            data: vec![],
            total_elements: 0,
            total_pages: 0,
            has_next: false,
        }));
    };
    Ok(Json(crate::query::execute_query(&repo, &query)))
}

/// GET 简易变体：仅实体过滤 + 分页排序，无 keyFilters/fields。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetQueryParams {
    tenant_id: Uuid,
    entity_type: Option<String>,
    name_contains: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
    sort_by: Option<String>,
    sort_order: Option<String>,
}

async fn query_entity_data_get(
    State(state): State<EdqsState>,
    Query(p): Query<GetQueryParams>,
) -> Result<Json<PageDataResult>, (StatusCode, Json<Value>)> {
    let entity_filter = match (&p.entity_type, &p.name_contains) {
        (Some(t), None) => EntityFilter::EntityType {
            entity_type: t.clone(),
        },
        (_, Some(n)) => EntityFilter::NameContains {
            name_contains: n.clone(),
            entity_type: p.entity_type.clone(),
        },
        (None, None) => EntityFilter::All,
    };
    let query = EntityDataQuery {
        tenant_id: p.tenant_id,
        entity_filter,
        key_filters: vec![],
        page_link: PageLinkReq {
            page: p.page.unwrap_or(0),
            page_size: p.page_size.unwrap_or(50),
            sort_by: p.sort_by.map(|key| SortBy {
                key,
                source: crate::query::KeySource::EntityField,
                direction: match p.sort_order.as_deref() {
                    Some(o) if o.eq_ignore_ascii_case("desc") => SortDirection::Desc,
                    _ => SortDirection::Asc,
                },
            }),
        },
        fields: vec![],
    };
    let Some(repo) = state.tenants.get(&p.tenant_id) else {
        return Ok(Json(PageDataResult {
            data: vec![],
            total_elements: 0,
            total_pages: 0,
            has_next: false,
        }));
    };
    Ok(Json(crate::query::execute_query(&repo, &query)))
}
