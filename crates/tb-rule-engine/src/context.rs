//! 节点执行上下文：DAO、脚本引擎、队列、Redis latest 与跨链缓存重建的共享状态。

use std::sync::Arc;

use sqlx::PgPool;
use tb_dao::{
    AlarmDao, AssetDao, AttributesDao, CustomerDao, DeviceDao, KeyDictionaryDao, RelationDao,
    RuleNodeDebugEventDao, RuleNodeDedupDao, RuleNodeDelayDao, TenantDao, TimeseriesDao,
};
use tb_queue::{KafkaQueueProducer, QueueProducer};
use tb_script::ScriptEngine;

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
    pub dedup_dao: RuleNodeDedupDao,
    pub delay_dao: RuleNodeDelayDao,
    pub script_engine: Arc<ScriptEngine>,
    pub kafka_producer: Arc<dyn QueueProducer>,
    /// EDQS 读模型事件发布（复用 kafka_producer）；失败仅 warn，不阻塞规则链。
    pub edqs_producer: Option<KafkaQueueProducer>,
    pub redis_manager: Option<redis::aio::ConnectionManager>,
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
            dedup_dao: RuleNodeDedupDao::new(pool.clone()),
            delay_dao: RuleNodeDelayDao::new(pool.clone()),
            script_engine: Arc::new(ScriptEngine::new()),
            kafka_producer: producer,
            edqs_producer,
            redis_manager: None,
            pool,
        }
    }

    pub fn with_redis_manager(mut self, manager: redis::aio::ConnectionManager) -> Self {
        self.redis_manager = Some(manager);
        self
    }
}
