//! EDQS 事件发布：实体/属性/latest 遥测变更同步到读模型。
//! 单个有界通道 + 专用任务串行发送；失败或队列满仅丢弃并 warn，不阻塞主链路。

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

use sqlx::PgPool;
use tb_common::{AttributeScope, EntityType, KvEntry, KvValue, TbResult, TsKvEntry};
use tb_dao::{AssetDao, AttributesDao, DeviceDao, KeyDictionaryDao, TimeseriesDao};
use tb_edqs::state::{EntityRecord, TenantRepo};
use tb_proto::common as common_pb;
use tb_proto::edqs as pb;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::AppState;

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_QUEUE_CAPACITY: usize = 100_000;

struct QueuedEvent {
    state: AppState,
    tenant_id: Uuid,
    event: pb::EdqsEventMsg,
}

static EVENT_QUEUE: OnceLock<mpsc::Sender<QueuedEvent>> = OnceLock::new();

fn spawn_serial_sender<T, F, Fut>(capacity: usize, mut send: F) -> mpsc::Sender<T>
where
    T: Send + 'static,
    F: FnMut(T) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel(capacity);
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            send(event).await;
        }
    });
    tx
}

fn event_sender() -> &'static mpsc::Sender<QueuedEvent> {
    EVENT_QUEUE.get_or_init(|| spawn_serial_sender(EVENT_QUEUE_CAPACITY, send_event))
}

async fn send_event(queued: QueuedEvent) {
    let QueuedEvent {
        state,
        tenant_id,
        event,
    } = queued;
    let producer = match state.queue_producer().await {
        Ok(p) => p.clone(),
        Err(e) => {
            tracing::warn!(%tenant_id, "edqs publish skipped, producer unavailable: {e}");
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
        Ok(Err(e)) => tracing::warn!(%tenant_id, "edqs event publish failed: {e}"),
        Err(_) => tracing::warn!(%tenant_id, "edqs event publish timed out"),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn kv_proto(key: &str, value: &KvValue) -> common_pb::KeyValueProto {
    // key_id 置 0：edqs 侧以 key 字符串为准。
    tb_proto::convert::kv_to_proto_named(0, key, value)
}

// EDQS 是可重建的读模型：反压时允许丢失增量，不能反阻塞认证/遥测等主链路。
fn enqueue<T>(sender: &mpsc::Sender<T>, event: T, tenant_id: Uuid) -> bool {
    match sender.try_send(event) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(%tenant_id, "edqs event queue full; dropping event");
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!(%tenant_id, "edqs event queue closed; dropping event");
            false
        }
    }
}

pub fn publish(state: &AppState, tenant_id: Uuid, event: pb::EdqsEventMsg) {
    let queued = QueuedEvent {
        state: state.clone(),
        tenant_id,
        event,
    };
    enqueue(event_sender(), queued, tenant_id);
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
                    update_ts: now_ms(),
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

fn tenant_delete_event(tenant_id: Uuid) -> pb::EdqsEventMsg {
    pb::EdqsEventMsg {
        tenant_id: tenant_id.to_string(),
        event: Some(pb::edqs_event_msg::Event::TenantDelete(
            pb::TenantDeleteMsg {
                tenant_id: tenant_id.to_string(),
            },
        )),
    }
}

pub fn publish_tenant_delete(state: &AppState, tenant_id: Uuid) {
    publish(state, tenant_id, tenant_delete_event(tenant_id));
}

pub async fn load_tenant_repo(pool: &PgPool, tenant_id: Uuid) -> TbResult<TenantRepo> {
    let keys = KeyDictionaryDao::new(pool.clone());
    let device_dao = DeviceDao::new(pool.clone());
    let asset_dao = AssetDao::new(pool.clone());
    let attributes_dao = AttributesDao::new(pool.clone(), keys.clone());
    let timeseries_dao = TimeseriesDao::new(pool.clone(), keys);

    let (devices, assets) = tokio::try_join!(
        device_dao.find_all_by_tenant(tenant_id),
        asset_dao.find_all_by_tenant(tenant_id)
    )?;
    let mut repo = TenantRepo::default();
    for device in devices {
        repo.upsert_entity(EntityRecord {
            id: device.id,
            entity_type: EntityType::Device,
            customer_id: device.customer_id,
            name: device.name,
            type_or_profile: device.r#type.unwrap_or_default(),
            label: device.label,
            created_time: device.created_time,
        });
    }
    for asset in assets {
        repo.upsert_entity(EntityRecord {
            id: asset.id,
            entity_type: EntityType::Asset,
            customer_id: asset.customer_id,
            name: asset.name,
            type_or_profile: String::new(),
            label: asset.label,
            created_time: asset.created_time,
        });
    }

    let entity_ids: Vec<_> = repo.entities.keys().copied().collect();
    let (attributes, latest) = tokio::try_join!(
        attributes_dao.find_by_entity_ids(&entity_ids),
        timeseries_dao.find_latest_by_entity_ids(&entity_ids)
    )?;
    for (entity_id, scope, entry) in attributes {
        repo.apply_attribute_update(entity_id, scope, vec![(entry.key, entry.value)], &[], 0);
    }
    for (entity_id, entry) in latest {
        repo.apply_latest(entity_id, vec![(entry.key, entry.ts, entry.value)]);
    }
    Ok(repo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn serial_sender_preserves_enqueue_order() {
        let (delivered_tx, mut delivered_rx) = mpsc::unbounded_channel();
        let tx = spawn_serial_sender(8, move |value: u64| {
            let delivered = delivered_tx.clone();
            async move {
                tokio::task::yield_now().await;
                delivered.send(value).unwrap();
            }
        });
        for value in 0..32 {
            tx.send(value).await.unwrap();
        }
        drop(tx);
        for value in 0..32 {
            assert_eq!(delivered_rx.recv().await, Some(value));
        }
    }

    #[tokio::test]
    async fn bounded_queue_drops_when_full() {
        let (tx, mut rx) = mpsc::channel(1);
        let tenant_id = Uuid::now_v7();
        assert!(enqueue(&tx, 1, tenant_id));
        assert!(!enqueue(&tx, 2, tenant_id));
        assert_eq!(rx.recv().await, Some(1));
        assert!(enqueue(&tx, 3, tenant_id));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[test]
    fn tenant_delete_event_contains_tenant_in_envelope_and_payload() {
        let tenant_id = Uuid::now_v7();
        let event = tenant_delete_event(tenant_id);
        assert_eq!(event.tenant_id, tenant_id.to_string());
        match event.event {
            Some(pb::edqs_event_msg::Event::TenantDelete(delete)) => {
                assert_eq!(delete.tenant_id, tenant_id.to_string());
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
