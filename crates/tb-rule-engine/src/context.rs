//! 节点执行上下文：DAO、脚本引擎、队列、Redis latest 与跨链缓存重建的共享状态。

use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::Expiry;
use sqlx::PgPool;
use tb_dao::{
    AlarmDao, AssetDao, AttributesDao, CustomerDao, DeviceDao, KeyDictionaryDao, RelationDao,
    RuleNodeDebugEventDao, TenantDao, TimeseriesDao,
};
use tb_queue::{KafkaQueueProducer, QueueProducer};
use tb_script::ScriptEngine;
use uuid::Uuid;

const MAX_DELAY_TASKS: usize = 10_000;
const MAX_DEDUP_ENTRIES: u64 = 1_000_000;

type DedupKey = (Uuid, Uuid, Uuid, u64);

#[derive(Clone)]
struct DedupEntry {
    window: Duration,
    marker: Arc<()>,
}

struct DedupExpiry;

impl Expiry<DedupKey, DedupEntry> for DedupExpiry {
    fn expire_after_create(
        &self,
        _key: &DedupKey,
        value: &DedupEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.window)
    }
}

pub struct NodeContext {
    pub pool: PgPool,
    pub timeseries_dao: TimeseriesDao,
    pub attributes_dao: AttributesDao,
    pub alarm_dao: AlarmDao,
    pub relation_dao: RelationDao,
    pub device_dao: DeviceDao,
    pub asset_dao: AssetDao,
    pub customer_dao: CustomerDao,
    pub tenant_dao: TenantDao,
    pub debug_event_dao: RuleNodeDebugEventDao,
    pub script_engine: Arc<ScriptEngine>,
    pub kafka_producer: Arc<dyn QueueProducer>,
    /// EDQS 读模型事件发布（复用 kafka_producer）；失败仅 warn，不阻塞规则链。
    pub edqs_producer: Option<KafkaQueueProducer>,
    pub redis_manager: Option<redis::aio::ConnectionManager>,
    pub(crate) delay_semaphore: Arc<tokio::sync::Semaphore>,
    dedup_cache: moka::future::Cache<DedupKey, DedupEntry>,
}

impl NodeContext {
    pub fn new(pool: PgPool, kafka_producer: KafkaQueueProducer) -> Self {
        let edqs_producer = Some(kafka_producer.clone());
        Self::build(pool, Arc::new(kafka_producer), edqs_producer)
    }

    pub fn with_queue_producer(pool: PgPool, producer: Arc<dyn QueueProducer>) -> Self {
        Self::build(pool, producer, None)
    }

    fn build(
        pool: PgPool,
        producer: Arc<dyn QueueProducer>,
        edqs_producer: Option<KafkaQueueProducer>,
    ) -> Self {
        let keys = KeyDictionaryDao::new(pool.clone());
        Self {
            timeseries_dao: TimeseriesDao::new(pool.clone(), keys.clone()),
            attributes_dao: AttributesDao::new(pool.clone(), keys),
            alarm_dao: AlarmDao::new(pool.clone()),
            relation_dao: RelationDao::new(pool.clone()),
            device_dao: DeviceDao::new(pool.clone()),
            asset_dao: AssetDao::new(pool.clone()),
            customer_dao: CustomerDao::new(pool.clone()),
            tenant_dao: TenantDao::new(pool.clone()),
            debug_event_dao: RuleNodeDebugEventDao::new(pool.clone()),
            script_engine: Arc::new(ScriptEngine::new()),
            kafka_producer: producer,
            edqs_producer,
            redis_manager: None,
            delay_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_DELAY_TASKS)),
            dedup_cache: moka::future::Cache::builder()
                .max_capacity(MAX_DEDUP_ENTRIES)
                .expire_after(DedupExpiry)
                .build(),
            pool,
        }
    }

    pub fn with_redis_manager(mut self, manager: redis::aio::ConnectionManager) -> Self {
        self.redis_manager = Some(manager);
        self
    }

    /// 同一 key 在窗口内首次调用返回 false，后续并发调用返回 true。
    pub async fn is_duplicate(
        &self,
        chain_id: Uuid,
        node_id: Uuid,
        originator: Uuid,
        content_hash: u64,
        window: Duration,
    ) -> bool {
        let marker = Arc::new(());
        let inserted_marker = marker.clone();
        let entry = self
            .dedup_cache
            .get_with((chain_id, node_id, originator, content_hash), async move {
                DedupEntry {
                    window,
                    marker: inserted_marker,
                }
            })
            .await;
        !Arc::ptr_eq(&marker, &entry.marker)
    }
}
