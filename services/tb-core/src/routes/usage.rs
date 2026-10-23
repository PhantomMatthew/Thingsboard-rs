//! /api/usage/{tenantId}：API 用量查询（SYS_ADMIN 任意租户；TENANT_ADMIN 本租户）。

use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use tb_auth::{ApiError, AuthUser};
use tb_common::TbError;
use tb_dao::models::Authority;
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::usage::UsageReport;

pub fn router() -> Router<AppState> {
    Router::new().route("/api/usage/{tenantId}", get(get_usage))
}

async fn get_usage(
    State(state): State<AppState>,
    user: AuthUser,
    Path(tenant_id): Path<Uuid>,
) -> Result<Json<UsageReport>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin, Authority::TenantAdmin])?;
    if user.0.authority != Authority::SysAdmin {
        check_tenant_scope(&user, tenant_id)?;
    }
    if state.tenant_dao.find_by_id(tenant_id).await?.is_none() {
        return Err(TbError::NotFound(format!("tenant {tenant_id}")).into());
    }
    Ok(Json(state.usage.report(&state, tenant_id).await?))
}
