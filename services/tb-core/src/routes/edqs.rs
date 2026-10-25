//! /api/edqs/...：EDQS 查询代理。认证后强制 tenantId = JWT tenant（SYS_ADMIN 可指定），
//! CUSTOMER_USER 强制 customerId = JWT customer；转发与 PostgreSQL fallback 复用相同隔离语义。

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::{ApiError, AuthUser};
use tb_common::{TbError, TbResult};
use tb_dao::models::Authority;
use uuid::Uuid;

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route(
        "/api/edqs/query/entityData",
        post(query_entity_data).get(query_entity_data_get),
    )
}

/// TENANT_ADMIN / CUSTOMER_USER 强制覆盖为自身租户；SYS_ADMIN 可指定 tenantId。
fn resolve_tenant(user: &AuthUser, body: &mut Value) -> TbResult<Uuid> {
    let tenant_id = match user.0.authority {
        Authority::SysAdmin => {
            let raw = body["tenantId"].as_str().unwrap_or_default();
            Uuid::parse_str(raw)
                .map_err(|_| TbError::InvalidInput("tenantId is required for SYS_ADMIN".into()))?
        }
        _ => {
            body["tenantId"] = json!(user.0.tenant_id.to_string());
            user.0.tenant_id
        }
    };
    body["customerId"] = match user.0.authority {
        Authority::CustomerUser => json!(user.0.customer_id.ok_or_else(|| {
            TbError::PermissionDenied("customer user has no customer assignment".into())
        })?),
        _ => Value::Null,
    };
    Ok(tenant_id)
}

fn with_api_key(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match std::env::var("TB_EDQS_API_KEY") {
        Ok(key) if !key.is_empty() => request.header("X-EDQS-KEY", key),
        _ => request,
    }
}

fn is_unavailable(status: reqwest::StatusCode) -> bool {
    matches!(
        status,
        reqwest::StatusCode::BAD_GATEWAY
            | reqwest::StatusCode::SERVICE_UNAVAILABLE
            | reqwest::StatusCode::GATEWAY_TIMEOUT
    )
}

async fn postgres_fallback(
    state: &AppState,
    query: tb_edqs::EntityDataQuery,
) -> Result<Response, ApiError> {
    let repo = crate::edqs::load_tenant_repo(&state.pool, query.tenant_id).await?;
    Ok(Json(tb_edqs::query::execute_query(&repo, &query)).into_response())
}

async fn forward(resp: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let body = resp.text().await.unwrap_or_default();
    (
        status,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}

async fn query_entity_data(
    State(state): State<AppState>,
    user: AuthUser,
    Json(mut body): Json<Value>,
) -> Result<Response, ApiError> {
    let tenant_id = resolve_tenant(&user, &mut body)?;
    let request = state
        .http_client
        .post(format!("{}/api/query/entityData", state.edqs_url))
        .json(&body);
    match with_api_key(request).send().await {
        Ok(r) if !is_unavailable(r.status()) => Ok(forward(r).await),
        Ok(r) => {
            tracing::warn!(status = %r.status(), "edqs unavailable; using PostgreSQL fallback");
            let mut query: tb_edqs::EntityDataQuery =
                serde_json::from_value(body).map_err(|e| TbError::InvalidInput(e.to_string()))?;
            query.tenant_id = tenant_id;
            postgres_fallback(&state, query).await
        }
        Err(e) => {
            tracing::warn!("edqs request failed: {e}; using PostgreSQL fallback");
            let mut query: tb_edqs::EntityDataQuery =
                serde_json::from_value(body).map_err(|e| TbError::InvalidInput(e.to_string()))?;
            query.tenant_id = tenant_id;
            postgres_fallback(&state, query).await
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetParams {
    tenant_id: Option<String>,
    customer_id: Option<String>,
    entity_type: Option<String>,
    name_contains: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
    sort_by: Option<String>,
    sort_order: Option<String>,
}

fn get_query(params: GetParams) -> TbResult<tb_edqs::EntityDataQuery> {
    let tenant_id = params
        .tenant_id
        .as_deref()
        .ok_or_else(|| TbError::InvalidInput("tenantId is required for SYS_ADMIN".into()))
        .and_then(|id| {
            Uuid::parse_str(id).map_err(|_| TbError::InvalidInput("invalid tenantId".into()))
        })?;
    let customer_id = params
        .customer_id
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .map_err(|_| TbError::InvalidInput("invalid customerId".into()))?;
    Ok(tb_edqs::api::entity_data_query(
        tb_edqs::api::GetQueryParams {
            tenant_id,
            customer_id,
            entity_type: params.entity_type,
            name_contains: params.name_contains,
            page: params.page,
            page_size: params.page_size,
            sort_by: params.sort_by,
            sort_order: params.sort_order,
        },
    ))
}

async fn query_entity_data_get(
    State(state): State<AppState>,
    user: AuthUser,
    Query(mut params): Query<GetParams>,
) -> Result<Response, ApiError> {
    if user.0.authority != Authority::SysAdmin {
        params.tenant_id = Some(user.0.tenant_id.to_string());
    }
    params.customer_id = match user.0.authority {
        Authority::CustomerUser => Some(
            user.0
                .customer_id
                .ok_or_else(|| {
                    TbError::PermissionDenied("customer user has no customer assignment".into())
                })?
                .to_string(),
        ),
        _ => None,
    };
    let fallback_params = params.clone();
    let mut qs: Vec<(&str, String)> = vec![];
    let mut push = |k: &'static str, v: Option<String>| {
        if let Some(v) = v {
            qs.push((k, v));
        }
    };
    push("tenantId", params.tenant_id);
    push("customerId", params.customer_id);
    push("entityType", params.entity_type);
    push("nameContains", params.name_contains);
    push("page", params.page.map(|v| v.to_string()));
    push("pageSize", params.page_size.map(|v| v.to_string()));
    push("sortBy", params.sort_by);
    push("sortOrder", params.sort_order);
    let request = state
        .http_client
        .get(format!("{}/api/query/entityData", state.edqs_url))
        .query(&qs);
    match with_api_key(request).send().await {
        Ok(r) if !is_unavailable(r.status()) => Ok(forward(r).await),
        Ok(r) => {
            tracing::warn!(status = %r.status(), "edqs unavailable; using PostgreSQL fallback");
            postgres_fallback(&state, get_query(fallback_params)?).await
        }
        Err(e) => {
            tracing::warn!("edqs request failed: {e}; using PostgreSQL fallback");
            postgres_fallback(&state, get_query(fallback_params)?).await
        }
    }
}
