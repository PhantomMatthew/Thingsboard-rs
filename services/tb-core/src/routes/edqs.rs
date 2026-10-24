//! /api/edqs/...：EDQS 查询代理。认证后强制 tenantId = JWT tenant（SYS_ADMIN 可指定），
//! 转发给内网 tb-edqs；edqs 不可达返回 502（本期不做 DB 回退，提示调用方改用直接查询端点）。

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
    match user.0.authority {
        Authority::SysAdmin => {
            let raw = body["tenantId"].as_str().unwrap_or_default();
            Uuid::parse_str(raw)
                .map_err(|_| TbError::InvalidInput("tenantId is required for SYS_ADMIN".into()))
        }
        _ => {
            body["tenantId"] = json!(user.0.tenant_id.to_string());
            Ok(user.0.tenant_id)
        }
    }
}

fn with_api_key(request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    match std::env::var("TB_EDQS_API_KEY") {
        Ok(key) if !key.is_empty() => request.header("X-EDQS-KEY", key),
        _ => request,
    }
}

fn bad_gateway(e: impl std::fmt::Display) -> Response {
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "status": 502,
            "message": format!("edqs unavailable: {e}; fallback to direct DB query endpoints is not implemented yet"),
        })),
    )
        .into_response()
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
    resolve_tenant(&user, &mut body)?;
    let request = state
        .http_client
        .post(format!("{}/api/query/entityData", state.edqs_url))
        .json(&body);
    match with_api_key(request).send().await {
        Ok(r) => Ok(forward(r).await),
        Err(e) => Ok(bad_gateway(e)),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetParams {
    tenant_id: Option<String>,
    entity_type: Option<String>,
    name_contains: Option<String>,
    page: Option<i64>,
    page_size: Option<i64>,
    sort_by: Option<String>,
    sort_order: Option<String>,
}

async fn query_entity_data_get(
    State(state): State<AppState>,
    user: AuthUser,
    Query(mut params): Query<GetParams>,
) -> Result<Response, ApiError> {
    if user.0.authority != Authority::SysAdmin {
        params.tenant_id = Some(user.0.tenant_id.to_string());
    }
    let mut qs: Vec<(&str, String)> = vec![];
    let mut push = |k: &'static str, v: Option<String>| {
        if let Some(v) = v {
            qs.push((k, v));
        }
    };
    push("tenantId", params.tenant_id);
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
        Ok(r) => Ok(forward(r).await),
        Err(e) => Ok(bad_gateway(e)),
    }
}
