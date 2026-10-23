//! /api/audit/logs：审计日志分页查询（TENANT_ADMIN 本租户；SYS_ADMIN 全部）。

use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use tb_auth::{ApiError, AuthUser};
use tb_dao::audit_log::{AuditLog, AuditLogFilter};
use tb_dao::models::Authority;
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::require_authority;

pub fn router() -> Router<AppState> {
    Router::new().route("/api/audit/logs", get(list_audit_logs))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditLogsQuery {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
    pub entity_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    /// 逗号分隔，如 "CREATED,DELETED"。
    pub action_types: Option<String>,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
}

async fn list_audit_logs(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<AuditLogsQuery>,
) -> Result<Json<PageData<AuditLog>>, ApiError> {
    require_authority(&user, &[Authority::SysAdmin, Authority::TenantAdmin])?;
    let tenant_id = match user.0.authority {
        Authority::SysAdmin => None,
        _ => Some(user.0.tenant_id),
    };
    let filter = AuditLogFilter {
        entity_id: query.entity_id,
        user_id: query.user_id,
        action_types: query.action_types.map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .map(str::to_string)
                .collect()
        }),
        start_ts: query.start_ts,
        end_ts: query.end_ts,
    };
    let page_link = PageLink::new(query.page.unwrap_or(0), query.page_size.unwrap_or(10));
    Ok(Json(
        state
            .audit_log_dao
            .find_by_tenant(tenant_id, &filter, &page_link)
            .await?,
    ))
}
