//! 节点执行上下文：DAO 集合 + 脚本引擎 + Kafka producer（checkpoint/DLQ/rpc_oneway 用）。

use std::sync::Arc;

use sqlx::PgPool;
use tb_dao::{
    AlarmDao, AssetDao, AttributesDao, CustomerDao, DeviceDao, KeyDictionaryDao, RelationDao,
    RuleNodeDebugEventDao, TenantDao, TimeseriesDao,
};
use tb_queue::KafkaQueueProducer;
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
    pub script_engine: Arc<ScriptEngine>,
    pub kafka_producer: KafkaQueueProducer,
    /// EDQS 读模型事件发布（复用 kafka_producer）；失败仅 warn，不阻塞规则链。
    pub edqs_producer: Option<KafkaQueueProducer>,
}

impl NodeContext {
    pub fn new(pool: PgPool, kafka_producer: KafkaQueueProducer) -> Self {
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
            edqs_producer: Some(kafka_producer.clone()),
            kafka_producer,
            pool,
        }
    }
}
