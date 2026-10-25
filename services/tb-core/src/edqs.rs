//! EDQS 事件发布：实体/属性/latest 遥测变更同步到读模型。
//! 单个有界通道 + 专用任务串行发送，不阻塞主链路。发送失败或队列满时增量丢弃，
//! 租户记为待重同步，由发送任务补发重同步标记让 EDQS 从数据库重载该租户。

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sqlx::PgPool;
use tb_common::{AttributeScope, EntityType, KvEntry, KvValue, TbResult, TsKvEntry};
use tb_dao::TsDeleteResult;
use tb_edqs::state::TenantRepo;
use tb_proto::common as common_pb;
use tb_proto::edqs as pb;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::AppState;

const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_QUEUE_CAPACITY: usize = 100_000;

/// 事件出口；AppState 走 Kafka，测试用内存实现。
trait EventSink: Clone + Send + Sync + 'static {
    fn send(
        &self,
        tenant_id: Uuid,
        event: &pb::EdqsEventMsg,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

impl EventSink for AppState {
    async fn send(&self, tenant_id: Uuid, event: &pb::EdqsEventMsg) -> Result<(), String> {
        let producer = self
            .queue_producer()
            .await
            .map_err(|e| format!("producer unavailable: {e}"))?
            .clone();
        match tokio::time::timeout(
            PUBLISH_TIMEOUT,
            tb_queue::edqs::publish_event(&producer, tenant_id, event),
        )
        .await
        {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(_) => Err("timed out".into()),
        }
    }
}

#[derive(Clone, Copy)]
struct PipelineConfig {
    capacity: usize,
    send_attempts: u32,
    retry_base_delay: Duration,
    resync_interval: Duration,
}

const PIPELINE_CONFIG: PipelineConfig = PipelineConfig {
    capacity: EVENT_QUEUE_CAPACITY,
    send_attempts: 3,
    retry_base_delay: Duration::from_millis(200),
    resync_interval: Duration::from_secs(2),
};

type Queued<K> = (K, Uuid, pb::EdqsEventMsg);

struct Pipeline<K: EventSink> {
    config: PipelineConfig,
    tx: Mutex<Option<mpsc::Sender<Queued<K>>>>,
    /// 待重同步租户 → (出口, 代数)；补发期间再次标记会推进代数，避免误删新标记。
    dirty: Mutex<HashMap<Uuid, (K, u64)>>,
    generation: std::sync::atomic::AtomicU64,
}

impl<K: EventSink> Pipeline<K> {
    fn new(config: PipelineConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            tx: Mutex::new(None),
            dirty: Mutex::new(HashMap::new()),
            generation: Default::default(),
        })
    }

    fn publish(self: &Arc<Self>, sink: K, tenant_id: Uuid, event: pb::EdqsEventMsg) {
        let mut item = (sink, tenant_id, event);
        // 发送任务所在 runtime 已退出时通道关闭，在当前 runtime 重建一次。
        for _ in 0..2 {
            let sender = self.sender();
            match sender.try_send(item) {
                Ok(()) => return,
                Err(mpsc::error::TrySendError::Full((sink, ..))) => {
                    tracing::warn!(%tenant_id, "edqs event queue full; tenant marked for resync");
                    self.mark_dirty(sink, tenant_id);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => {
                    let mut tx = self.tx.lock().unwrap();
                    if tx.as_ref().is_some_and(|tx| tx.same_channel(&sender)) {
                        *tx = None;
                    }
                    item = returned;
                }
            }
        }
        tracing::warn!(%tenant_id, "edqs event queue unavailable; tenant marked for resync");
        self.mark_dirty(item.0, tenant_id);
    }

    fn sender(self: &Arc<Self>) -> mpsc::Sender<Queued<K>> {
        let mut tx = self.tx.lock().unwrap();
        if let Some(sender) = tx.as_ref() {
            return sender.clone();
        }
        let (sender, rx) = mpsc::channel(self.config.capacity);
        tokio::spawn(Arc::clone(self).run(rx));
        *tx = Some(sender.clone());
        sender
    }

    fn mark_dirty(&self, sink: K, tenant_id: Uuid) {
        let generation = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.dirty
            .lock()
            .unwrap()
            .insert(tenant_id, (sink, generation));
    }

    async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<Queued<K>>) {
        let mut resync = tokio::time::interval(self.config.resync_interval);
        resync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                item = rx.recv() => match item {
                    Some((sink, tenant_id, event)) => self.deliver(sink, tenant_id, event).await,
                    None => return,
                },
                _ = resync.tick() => self.flush_resyncs().await,
            }
        }
    }

    async fn deliver(&self, sink: K, tenant_id: Uuid, event: pb::EdqsEventMsg) {
        for attempt in 0..self.config.send_attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_base_delay * (1 << (attempt - 1))).await;
            }
            match sink.send(tenant_id, &event).await {
                Ok(()) => return,
                Err(e) => tracing::warn!(%tenant_id, attempt, "edqs event publish failed: {e}"),
            }
        }
        tracing::warn!(%tenant_id, "edqs event dropped after retries; tenant marked for resync");
        self.mark_dirty(sink, tenant_id);
    }

    async fn flush_resyncs(&self) {
        let pending: Vec<_> = self
            .dirty
            .lock()
            .unwrap()
            .iter()
            .map(|(tenant_id, (sink, generation))| (*tenant_id, sink.clone(), *generation))
            .collect();
        for (tenant_id, sink, generation) in pending {
            let event = tb_queue::edqs::tenant_resync_event(tenant_id);
            if let Err(e) = sink.send(tenant_id, &event).await {
                tracing::warn!(%tenant_id, "edqs tenant resync publish failed: {e}");
                return;
            }
            let mut dirty = self.dirty.lock().unwrap();
            if dirty.get(&tenant_id).is_some_and(|(_, g)| *g == generation) {
                dirty.remove(&tenant_id);
            }
        }
    }
}

static PIPELINE: OnceLock<Arc<Pipeline<AppState>>> = OnceLock::new();

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

pub fn publish(state: &AppState, tenant_id: Uuid, event: pb::EdqsEventMsg) {
    PIPELINE
        .get_or_init(|| Pipeline::new(PIPELINE_CONFIG))
        .publish(state.clone(), tenant_id, event);
}

/// 遥测删除后的 latest 变化；range 为 None 表示全量删除。
pub fn publish_latest_delete(
    state: &AppState,
    tenant_id: Uuid,
    entity_id: Uuid,
    keys: &[String],
    range: Option<(i64, i64)>,
    result: &TsDeleteResult,
) {
    if !result.latest_changed() {
        return;
    }
    publish(
        state,
        tenant_id,
        latest_delete_event(tenant_id, entity_id, keys, range, result),
    );
}

fn latest_delete_event(
    tenant_id: Uuid,
    entity_id: Uuid,
    keys: &[String],
    range: Option<(i64, i64)>,
    result: &TsDeleteResult,
) -> pb::EdqsEventMsg {
    let (start_ts, end_ts) = range.unwrap_or((i64::MIN, i64::MAX));
    pb::EdqsEventMsg {
        tenant_id: tenant_id.to_string(),
        event: Some(pb::edqs_event_msg::Event::LatestDelete(
            pb::LatestDeleteMsg {
                entity_id: entity_id.to_string(),
                keys: keys.to_vec(),
                start_ts,
                end_ts,
                replacements: result
                    .rewritten_latest
                    .iter()
                    .map(|e| kv_proto(&e.key, &e.value))
                    .collect(),
                replacement_ts: result.rewritten_latest.iter().map(|e| e.ts).collect(),
            },
        )),
    }
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
    tb_edqs::snapshot::load_tenant_repo(pool, tenant_id).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 可切换故障的内存出口；`gate` 可阻塞发送以制造队列满。
    #[derive(Clone)]
    struct MemorySink {
        failing: Arc<std::sync::atomic::AtomicBool>,
        gate: Arc<tokio::sync::Semaphore>,
        sent: Arc<Mutex<Vec<pb::EdqsEventMsg>>>,
    }

    impl MemorySink {
        fn closed() -> Self {
            Self {
                failing: Default::default(),
                gate: Arc::new(tokio::sync::Semaphore::new(0)),
                sent: Default::default(),
            }
        }

        fn open() -> Self {
            let sink = Self::closed();
            sink.gate.add_permits(tokio::sync::Semaphore::MAX_PERMITS);
            sink
        }

        fn sent(&self) -> Vec<pb::EdqsEventMsg> {
            self.sent.lock().unwrap().clone()
        }
    }

    impl EventSink for MemorySink {
        async fn send(&self, _tenant_id: Uuid, event: &pb::EdqsEventMsg) -> Result<(), String> {
            let _permit = self.gate.acquire().await.map_err(|e| e.to_string())?;
            if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
                return Err("kafka down".into());
            }
            self.sent.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    const TEST_CONFIG: PipelineConfig = PipelineConfig {
        capacity: 1,
        send_attempts: 2,
        retry_base_delay: Duration::from_millis(1),
        resync_interval: Duration::from_millis(20),
    };

    async fn wait_for(mut check: impl FnMut() -> bool) {
        for _ in 0..200 {
            if check() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    fn is_resync(event: &pb::EdqsEventMsg, tenant_id: Uuid) -> bool {
        *event == tb_queue::edqs::tenant_resync_event(tenant_id)
    }

    #[tokio::test]
    async fn pipeline_preserves_enqueue_order() {
        let sink = MemorySink::open();
        let pipeline = Pipeline::new(PipelineConfig {
            capacity: 64,
            ..TEST_CONFIG
        });
        let tenant_id = Uuid::now_v7();
        let events: Vec<_> = (0..32)
            .map(|_| tenant_delete_event(Uuid::now_v7()))
            .collect();
        for event in &events {
            pipeline.publish(sink.clone(), tenant_id, event.clone());
        }
        wait_for(|| sink.sent().len() == events.len()).await;
        assert_eq!(sink.sent(), events);
    }

    #[tokio::test]
    async fn failed_send_marks_tenant_and_resync_is_published_after_recovery() {
        let sink = MemorySink::open();
        sink.failing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let pipeline = Pipeline::new(TEST_CONFIG);
        let tenant_id = Uuid::now_v7();
        pipeline.publish(sink.clone(), tenant_id, tenant_delete_event(tenant_id));
        wait_for(|| pipeline.dirty.lock().unwrap().contains_key(&tenant_id)).await;
        assert!(sink.sent().is_empty());

        sink.failing
            .store(false, std::sync::atomic::Ordering::SeqCst);
        wait_for(|| sink.sent().iter().any(|e| is_resync(e, tenant_id))).await;
        wait_for(|| pipeline.dirty.lock().unwrap().is_empty()).await;
    }

    #[tokio::test]
    async fn full_queue_marks_tenant_for_resync() {
        let sink = MemorySink::closed();
        let pipeline = Pipeline::new(TEST_CONFIG);
        let tenant_id = Uuid::now_v7();
        // 第一条被发送任务取走并阻塞在 gate，第二条占满容量 1 的队列，第三条溢出。
        for _ in 0..3 {
            pipeline.publish(sink.clone(), tenant_id, tenant_delete_event(tenant_id));
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(pipeline.dirty.lock().unwrap().contains_key(&tenant_id));

        sink.gate.add_permits(tokio::sync::Semaphore::MAX_PERMITS);
        wait_for(|| sink.sent().iter().any(|e| is_resync(e, tenant_id))).await;
        assert_eq!(
            sink.sent()
                .iter()
                .filter(|e| !is_resync(e, tenant_id))
                .count(),
            2
        );
    }

    #[test]
    fn closed_queue_is_respawned_on_the_current_runtime() {
        let sink = MemorySink::open();
        let pipeline = Pipeline::new(TEST_CONFIG);
        let tenant_id = Uuid::now_v7();
        let runtime = || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
        };
        runtime().block_on(async {
            pipeline.publish(sink.clone(), tenant_id, tenant_delete_event(tenant_id));
            wait_for(|| sink.sent().len() == 1).await;
        });
        runtime().block_on(async {
            pipeline.publish(sink.clone(), tenant_id, tenant_delete_event(tenant_id));
            wait_for(|| sink.sent().len() == 2).await;
        });
        assert!(pipeline.dirty.lock().unwrap().is_empty());
    }

    #[test]
    fn latest_delete_event_carries_range_and_replacements() {
        let tenant_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        let result = TsDeleteResult {
            deleted: 2,
            removed_latest: vec!["humidity".into()],
            rewritten_latest: vec![TsKvEntry {
                ts: 200,
                key: "temperature".into(),
                value: KvValue::Long(20),
            }],
        };
        let keys = vec!["temperature".to_string(), "humidity".to_string()];
        let event = latest_delete_event(tenant_id, entity_id, &keys, Some((250, 400)), &result);
        let Some(pb::edqs_event_msg::Event::LatestDelete(delete)) = event.event else {
            panic!("unexpected event");
        };
        assert_eq!(delete.keys, keys);
        assert_eq!((delete.start_ts, delete.end_ts), (250, 400));
        assert_eq!(delete.replacement_ts, vec![200]);
        assert_eq!(delete.replacements[0].key, "temperature");

        let event = latest_delete_event(tenant_id, entity_id, &keys, None, &result);
        let Some(pb::edqs_event_msg::Event::LatestDelete(delete)) = event.event else {
            panic!("unexpected event");
        };
        assert_eq!((delete.start_ts, delete.end_ts), (i64::MIN, i64::MAX));
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
