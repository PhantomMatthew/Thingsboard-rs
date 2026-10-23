//! EDQS 事件发布：实体/属性/latest 遥测变更同步到读模型。
//! fire-and-forget + 超时保护，失败仅 warn —— edqs 是读模型，不阻塞主链路。

use std::time::Duration;

use tb_common::{AttributeScope, EntityType, KvEntry, KvValue, TsKvEntry};
use tb_proto::common as common_pb;
use tb_proto::edqs as pb;
use uuid::Uuid;

use crate::AppState;

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

fn kv_proto(key: &str, value: &KvValue) -> common_pb::KeyValueProto {
    // key_id 置 0：edqs 侧以 key 字符串为准。
    tb_proto::convert::kv_to_proto_named(0, key, value)
}

pub fn publish(state: &AppState, tenant_id: Uuid, event: pb::EdqsEventMsg) {
    let state = state.clone();
    tokio::spawn(async move {
        let producer = match state.queue_producer().await {
            Ok(p) => p.clone(),
            Err(e) => {
                tracing::warn!("edqs publish skipped, producer unavailable: {e}");
                return;
            }
        };
        match tokio::time::timeout(
            PUBLISH_TIMEOUT,
            tb_queue::edqs::publish_event(&producer, tenant_id, &event),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!("edqs event publish failed: {e}"),
            Err(_) => tracing::warn!("edqs event publish timed out"),
        }
    });
}

pub fn publish_latest(state: &AppState, tenant_id: Uuid, entity_id: Uuid, entries: &[TsKvEntry]) {
    if entries.is_empty() {
        return;
    }
    publish(
        state,
        tenant_id,
        pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::LatestTelemetry(
                pb::LatestTelemetryMsg {
                    entity_id: entity_id.to_string(),
                    values: entries.iter().map(|e| kv_proto(&e.key, &e.value)).collect(),
                    ts: entries.iter().map(|e| e.ts).collect(),
                },
            )),
        },
    );
}

pub fn publish_attribute_update(
    state: &AppState,
    tenant_id: Uuid,
    entity_id: Uuid,
    scope: AttributeScope,
    updated: &[KvEntry],
    deleted_keys: Vec<String>,
) {
    if updated.is_empty() && deleted_keys.is_empty() {
        return;
    }
    publish(
        state,
        tenant_id,
        pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::AttributeUpdate(
                pb::AttributeUpdateMsg {
                    entity_id: entity_id.to_string(),
                    scope: common_pb::AttributeScope::from(scope) as i32,
                    updated: updated.iter().map(|e| kv_proto(&e.key, &e.value)).collect(),
                    deleted_keys,
                },
            )),
        },
    );
}

#[allow(clippy::too_many_arguments)]
pub fn publish_entity_upsert(
    state: &AppState,
    tenant_id: Uuid,
    entity_id: Uuid,
    entity_type: EntityType,
    customer_id: Option<Uuid>,
    name: &str,
    type_or_profile: &str,
    label: Option<&str>,
    created_time: i64,
) {
    publish(
        state,
        tenant_id,
        pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::EntityUpsert(
                pb::EntityUpsertMsg {
                    entity_id: entity_id.to_string(),
                    entity_type: common_pb::EntityType::from(entity_type) as i32,
                    customer_id: customer_id.map(|c| c.to_string()).unwrap_or_default(),
                    name: name.to_string(),
                    type_or_profile: type_or_profile.to_string(),
                    label: label.unwrap_or_default().to_string(),
                    created_time,
                },
            )),
        },
    );
}

pub fn publish_entity_delete(
    state: &AppState,
    tenant_id: Uuid,
    entity_id: Uuid,
    entity_type: EntityType,
) {
    publish(
        state,
        tenant_id,
        pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::EntityDelete(
                pb::EntityDeleteMsg {
                    entity_id: entity_id.to_string(),
                    entity_type: common_pb::EntityType::from(entity_type) as i32,
                },
            )),
        },
    );
}
