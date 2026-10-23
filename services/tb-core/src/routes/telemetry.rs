//! /api/telemetry/{entityType}/{entityId}/...：遥测与属性读写（重新设计的端点，非兼容 Java 版）。

use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use tb_auth::{ApiError, AuthUser};
use tb_common::{AttributeScope, EntityType, KvEntry, KvValue, TbError, TbResult, TsKvEntry};
use tb_dao::Aggregation;
use tb_dao::models::Authority;
use tb_proto::common as common_pb;
use tb_proto::core as core_pb;
use tb_queue::{QueueProducer as _, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::telemetry_cache;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/api/telemetry/{entityType}/{entityId}/timeseries",
            post(save_timeseries).delete(delete_timeseries),
        )
        .route(
            "/api/telemetry/{entityType}/{entityId}/values/timeseries",
            get(get_timeseries),
        )
        .route(
            "/api/telemetry/{entityType}/{entityId}/keys/timeseries",
            get(get_timeseries_keys),
        )
        .route(
            "/api/telemetry/{entityType}/{entityId}/attributes/{scope}",
            post(save_attributes).delete(delete_attributes),
        )
        .route(
            "/api/telemetry/{entityType}/{entityId}/values/attributes/{scope}",
            get(get_attributes),
        )
        .route(
            "/api/telemetry/{entityType}/{entityId}/values/attributes",
            get(get_attributes_all),
        )
}

/// 写入体：带 ts 的数组或 {k: v} 映射（ts 取当前时间）。
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TimeseriesPayload {
    Points(Vec<TsValues>),
    Map(HashMap<String, KvValue>),
}

#[derive(Debug, Deserialize)]
struct TsValues {
    ts: Option<i64>,
    values: HashMap<String, KvValue>,
}

impl TimeseriesPayload {
    fn into_entries(self, now: i64) -> Vec<TsKvEntry> {
        match self {
            Self::Points(points) => points
                .into_iter()
                .flat_map(|p| {
                    let ts = p.ts.unwrap_or(now);
                    p.values
                        .into_iter()
                        .map(move |(key, value)| TsKvEntry { ts, key, value })
                })
                .collect(),
            Self::Map(values) => values
                .into_iter()
                .map(|(key, value)| TsKvEntry {
                    ts: now,
                    key,
                    value,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimeseriesQuery {
    keys: Option<String>,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
    limit: Option<i64>,
    interval: Option<i64>,
    agg: Option<String>,
    order_by: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KeysQuery {
    keys: Option<String>,
    start_ts: Option<i64>,
    end_ts: Option<i64>,
}

fn parse_keys(raw: &Option<String>) -> Option<Vec<String>> {
    raw.as_ref().map(|s| {
        s.split(',')
            .map(str::trim)
            .filter(|k| !k.is_empty())
            .map(str::to_string)
            .collect()
    })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// SHARED_SCOPE 变更下行：推给该设备所有订阅了属性更新的在线会话（按各自 node 发 Kafka）。
/// value 为 None 表示删除通知。
async fn notify_shared_attribute_update(
    state: &AppState,
    entity_type: EntityType,
    entity_id: Uuid,
    attributes: Vec<common_pb::KeyValueProto>,
) {
    if entity_type != EntityType::Device || attributes.is_empty() {
        return;
    }
    let sessions = state.device_sessions.attribute_sessions(entity_id);
    if sessions.is_empty() {
        return;
    }
    let producer = match state.queue_producer().await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("queue producer unavailable, attribute update dropped: {e}");
            return;
        }
    };
    use prost::Message as _;
    for session in sessions {
        let msg = core_pb::ToTransportMsg {
            session_id: session.session_id.clone(),
            entity_id: entity_id.to_string(),
            msg: Some(core_pb::to_transport_msg::Msg::AttributeUpdate(
                core_pb::AttributeUpdateNotification {
                    scope: common_pb::AttributeScope::SharedScope as i32,
                    attributes: attributes.clone(),
                },
            )),
        };
        let topic = TbTopic::transport_notifications(&session.node_id);
        if let Err(e) = producer
            .send(
                &topic,
                &session.session_id,
                bytes::Bytes::from(msg.encode_to_vec()),
            )
            .await
        {
            tracing::warn!(
                session_id = session.session_id,
                "attribute update send failed: {e}"
            );
        }
    }
}

/// 实体存在性 + 租户/客户归属校验；仅支持 DEVICE/ASSET。
pub(crate) async fn check_entity_access(
    state: &AppState,
    user: &AuthUser,
    entity_type: EntityType,
    entity_id: Uuid,
) -> TbResult<()> {
    let (tenant_id, customer_id) = match entity_type {
        EntityType::Device => {
            let device = state
                .device_dao
                .find_by_id(entity_id)
                .await?
                .ok_or_else(|| TbError::NotFound(format!("device {entity_id}")))?;
            (device.tenant_id, device.customer_id)
        }
        EntityType::Asset => {
            let asset = state
                .asset_dao
                .find_by_id(entity_id)
                .await?
                .ok_or_else(|| TbError::NotFound(format!("asset {entity_id}")))?;
            (asset.tenant_id, asset.customer_id)
        }
        other => {
            return Err(TbError::InvalidInput(format!(
                "telemetry not supported for entity type: {other}"
            )));
        }
    };
    match user.0.authority {
        Authority::TenantAdmin => check_tenant_scope(user, tenant_id),
        Authority::CustomerUser
            if user.0.customer_id.is_some() && user.0.customer_id == customer_id =>
        {
            Ok(())
        }
        _ => Err(TbError::PermissionDenied(
            "not allowed to access this entity".into(),
        )),
    }
}

fn entries_to_json(entries: Vec<TsKvEntry>) -> Value {
    let mut map = serde_json::Map::new();
    for e in entries {
        map.entry(e.key)
            .or_insert_with(|| json!([]))
            .as_array_mut()
            .expect("array")
            .push(json!({"ts": e.ts, "value": e.value}));
    }
    Value::Object(map)
}

async fn save_timeseries(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
    Json(payload): Json<TimeseriesPayload>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let entries = payload.into_entries(now_ms());
    telemetry_cache::save_with_latest(&state, user.0.tenant_id, entity_id, &entries).await?;
    crate::edqs::publish_latest(&state, user.0.tenant_id, entity_id, &entries);
    Ok(StatusCode::OK)
}

async fn get_timeseries(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
    Query(query): Query<TimeseriesQuery>,
) -> Result<Json<Value>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let keys = parse_keys(&query.keys);

    match query.start_ts {
        // 无 startTs：返回 latest（全量时优先 Redis 缓存）
        None => {
            let entries = match &keys {
                Some(keys) => {
                    state
                        .timeseries_dao
                        .find_latest(entity_id, Some(keys))
                        .await?
                }
                None => match telemetry_cache::get_latest_all(&state, entity_id).await {
                    Some(cached) => cached,
                    None => {
                        let entries = state.timeseries_dao.find_latest(entity_id, None).await?;
                        telemetry_cache::put_latest_all(&state, entity_id, &entries).await;
                        entries
                    }
                },
            };
            Ok(Json(entries_to_json(entries)))
        }
        // 带 startTs：历史（agg=NONE 原始 / 否则分桶聚合）
        Some(start_ts) => {
            let keys = keys.ok_or_else(|| {
                TbError::InvalidInput("keys are required for history query".into())
            })?;
            let end_ts = query.end_ts.unwrap_or_else(now_ms);
            let agg = match &query.agg {
                Some(s) => Aggregation::parse(s)?,
                None => Aggregation::None,
            };
            let mut entries = Vec::new();
            for key in &keys {
                let mut part = if agg == Aggregation::None {
                    state
                        .timeseries_dao
                        .find_history(
                            entity_id,
                            key,
                            start_ts,
                            end_ts,
                            query.limit.unwrap_or(100),
                            query.order_by.as_deref() == Some("ASC"),
                        )
                        .await?
                } else {
                    state
                        .timeseries_dao
                        .find_aggregated(
                            entity_id,
                            key,
                            start_ts,
                            end_ts,
                            query.interval.unwrap_or(0),
                            agg,
                        )
                        .await?
                };
                entries.append(&mut part);
            }
            Ok(Json(entries_to_json(entries)))
        }
    }
}

async fn get_timeseries_keys(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
) -> Result<Json<Vec<String>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    Ok(Json(state.timeseries_dao.get_all_keys(entity_id).await?))
}

async fn delete_timeseries(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
    Query(query): Query<KeysQuery>,
) -> Result<Json<Value>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let keys = parse_keys(&query.keys)
        .filter(|k| !k.is_empty())
        .ok_or_else(|| TbError::InvalidInput("keys are required".into()))?;
    let range = match (query.start_ts, query.end_ts) {
        (Some(s), Some(e)) => Some((s, e)),
        (None, None) => None,
        _ => {
            return Err(TbError::InvalidInput(
                "startTs and endTs must be provided together".into(),
            )
            .into());
        }
    };
    let deleted = state.timeseries_dao.delete(entity_id, &keys, range).await?;
    if range.is_none() {
        telemetry_cache::delete_latest(&state, entity_id, &keys).await;
    }
    Ok(Json(json!({"deleted": deleted})))
}

/// 写侧 scope 仅允许 SERVER_SCOPE/SHARED_SCOPE（CLIENT 由设备侧写入）。
fn parse_write_scope(s: &str) -> TbResult<AttributeScope> {
    match s {
        "SERVER_SCOPE" => Ok(AttributeScope::ServerScope),
        "SHARED_SCOPE" => Ok(AttributeScope::SharedScope),
        other => Err(TbError::InvalidInput(format!(
            "attribute scope not allowed here: {other}"
        ))),
    }
}

fn parse_read_scope(s: &str) -> TbResult<AttributeScope> {
    match s {
        "CLIENT_SCOPE" => Ok(AttributeScope::ClientScope),
        "SERVER_SCOPE" => Ok(AttributeScope::ServerScope),
        "SHARED_SCOPE" => Ok(AttributeScope::SharedScope),
        other => Err(TbError::InvalidInput(format!(
            "unknown attribute scope: {other}"
        ))),
    }
}

async fn save_attributes(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id, scope)): Path<(String, Uuid, String)>,
    Json(values): Json<HashMap<String, KvValue>>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    let scope = parse_write_scope(&scope)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let entries: Vec<KvEntry> = values
        .into_iter()
        .map(|(key, value)| KvEntry { key, value })
        .collect();
    state
        .attributes_dao
        .save(entity_id, scope, &entries)
        .await?;
    crate::calculated::spawn_evaluate(&state, user.0.tenant_id, entity_id, entries.clone());
    crate::edqs::publish_attribute_update(
        &state,
        user.0.tenant_id,
        entity_id,
        scope,
        &entries,
        vec![],
    );
    if scope == AttributeScope::SharedScope {
        let mut kvs = Vec::with_capacity(entries.len());
        for e in &entries {
            let key_id = state.key_dictionary_dao.get_or_create_id(&e.key).await?;
            kvs.push(tb_proto::convert::kv_to_proto_named(
                key_id, &e.key, &e.value,
            ));
        }
        notify_shared_attribute_update(&state, entity_type, entity_id, kvs).await;
    }
    Ok(StatusCode::OK)
}

async fn get_attributes(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id, scope)): Path<(String, Uuid, String)>,
    Query(query): Query<KeysQuery>,
) -> Result<Json<Value>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    let scope = parse_read_scope(&scope)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let keys = parse_keys(&query.keys);
    let entries = state
        .attributes_dao
        .find(entity_id, scope, keys.as_deref())
        .await?;
    let list: Vec<Value> = entries
        .into_iter()
        .map(|e| json!({"key": e.key, "value": e.value}))
        .collect();
    Ok(Json(Value::Array(list)))
}

async fn get_attributes_all(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
) -> Result<Json<Value>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let all = state.attributes_dao.find_all(entity_id).await?;
    let mut grouped = json!({"client": {}, "server": {}, "shared": {}});
    for (scope, entry) in all {
        let group = match scope {
            AttributeScope::ClientScope => "client",
            AttributeScope::ServerScope => "server",
            AttributeScope::SharedScope => "shared",
        };
        grouped[group][entry.key] = json!(entry.value);
    }
    Ok(Json(grouped))
}

async fn delete_attributes(
    State(state): State<AppState>,
    user: AuthUser,
    Path((entity_type, entity_id, scope)): Path<(String, Uuid, String)>,
    Query(query): Query<KeysQuery>,
) -> Result<Json<Value>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin, Authority::CustomerUser])?;
    let entity_type = EntityType::from_str(&entity_type)?;
    let scope = parse_write_scope(&scope)?;
    check_entity_access(&state, &user, entity_type, entity_id).await?;
    let keys = parse_keys(&query.keys)
        .filter(|k| !k.is_empty())
        .ok_or_else(|| TbError::InvalidInput("keys are required".into()))?;
    let deleted = state.attributes_dao.delete(entity_id, scope, &keys).await?;
    if deleted > 0 {
        crate::edqs::publish_attribute_update(
            &state,
            user.0.tenant_id,
            entity_id,
            scope,
            &[],
            keys.clone(),
        );
    }
    if deleted > 0 && scope == AttributeScope::SharedScope {
        let mut kvs = Vec::with_capacity(keys.len());
        for key in &keys {
            let key_id = state.key_dictionary_dao.get_or_create_id(key).await?;
            // value 缺省 = 删除通知。
            kvs.push(common_pb::KeyValueProto {
                key_id,
                key: key.clone(),
                value: None,
            });
        }
        notify_shared_attribute_update(&state, entity_type, entity_id, kvs).await;
    }
    Ok(Json(json!({"deleted": deleted})))
}
