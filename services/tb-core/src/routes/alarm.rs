//! /api/alarm(s)/...：告警查询与 ack/clear。

use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::{Alarm, AlarmStatus, Authority};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::telemetry::check_entity_access;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/alarm/{alarmId}", get(get_alarm))
        .route("/api/alarm/{alarmId}/ack", post(ack_alarm))
        .route("/api/alarm/{alarmId}/clear", post(clear_alarm))
        .route("/api/alarms/{entityType}/{entityId}", get(get_entity_alarms))
        .route("/api/alarms", get(get_tenant_alarms))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlarmQuery {
    page: Option<i64>,
    page_size: Option<i64>,
    text_search: Option<String>,
    sort_property: Option<String>,
    sort_order: Option<String>,
    status: Option<String>,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
}

impl AlarmQuery {
    fn parts(&self) -> TbResult<(PageLink, Option<AlarmStatus>)> {
        let status = self
            .status
            .as_deref()
            .map(AlarmStatus::from_str)
            .transpose()?;
        let page_link = PageLink {
            page: self.page.unwrap_or(0),
            page_size: self.page_size.unwrap_or(10),
            text_search: self.text_search.clone(),
            sort_property: self.sort_property.clone(),
            sort_order: self.sort_order.clone(),
        };
        Ok((page_link, status))
    }
}

/// 告警归属校验：tenant admin 同租户；customer user 需 alarm.customer_id 匹配。
fn check_alarm_access(user: &AuthUser, alarm: &Alarm) -> TbResult<()> {
    match user.0.authority {
        Authority::TenantAdmin => check_tenant_scope(user, alarm.tenant_id),
        Authority::CustomerUser
            if user.0.customer_id.is_some() && user.0.customer_id == alarm.customer_id =>
        {
            Ok(())
        }
        _ => Err(TbError::PermissionDenied(
            "not allowed to access this alarm".into(),
        )),
    }
}

async fn load_alarm(state: &AppState, user: &AuthUser, alarm_id: Uuid) -> TbResult<Alarm> {
    let alarm = state
        .alarm_dao
        .find_by_id(alarm_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("alarm {alarm_id}")))?;
    check_alarm_access(user, &alarm)?;
    Ok(alarm)
}

async fn get_alarm(
    State(state): State<AppState>,
    user: AuthUser,
    Path(alarm_id): Path<Uuid>,
) -> Result<Json<Alarm>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    Ok(Json(load_alarm(&state, &user, alarm_id).await?))
}

async fn ack_alarm(
    State(state): State<AppState>,
    user: AuthUser,
    Path(alarm_id): Path<Uuid>,
) -> Result<Json<Alarm>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    load_alarm(&state, &user, alarm_id).await?;
    Ok(Json(state.alarm_dao.ack(alarm_id).await?))
}

async fn clear_alarm(
    State(state): State<AppState>,
    user: AuthUser,
    Path(alarm_id): Path<Uuid>,
) -> Result<Json<Alarm>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    load_alarm(&state, &user, alarm_id).await?;
    Ok(Json(state.alarm_dao.clear(alarm_id).await?))
}

async fn get_entity_alarms(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
    Query(query): Query<AlarmQuery>,
) -> Result<Json<PageData<Alarm>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let (page_link, status) = query.parts()?;
    let alarms = state
        .alarm_dao
        .find_by_tenant(
            user.0.tenant_id,
            Some(entity_id),
            status,
            query.start_ts,
            query.end_ts,
            &page_link,
        )
        .await?;
    Ok(Json(alarms))
}

async fn get_tenant_alarms(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<AlarmQuery>,
) -> Result<Json<PageData<Alarm>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let (page_link, status) = query.parts()?;
    let alarms = state
        .alarm_dao
        .find_by_tenant(
            user.0.tenant_id,
            None,
            status,
            query.start_ts,
            query.end_ts,
            &page_link,
        )
        .await?;
    Ok(Json(alarms))
}
