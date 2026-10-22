//! 节点执行上下文：DAO 集合 + 脚本引擎 + Kafka producer（checkpoint 用，Phase 7）。

use std::sync::Arc;

use sqlx::PgPool;
use tb_dao::{AlarmDao, AttributesDao, KeyDictionaryDao, TimeseriesDao};
use tb_queue::KafkaQueueProducer;
use tb_script::ScriptEngine;

pub struct NodeContext {
    pub pool: PgPool,
    pub timeseries_dao: TimeseriesDao,
    pub attributes_dao: AttributesDao,
    pub alarm_dao: AlarmDao,
    pub script_engine: Arc<ScriptEngine>,
    pub kafka_producer: KafkaQueueProducer,
}

impl NodeContext {
    pub fn new(pool: PgPool, kafka_producer: KafkaQueueProducer) -> Self {
        let keys = KeyDictionaryDao::new(pool.clone());
        Self {
            timeseries_dao: TimeseriesDao::new(pool.clone(), keys.clone()),
            attributes_dao: AttributesDao::new(pool.clone(), keys),
            alarm_dao: AlarmDao::new(pool.clone()),
            script_engine: Arc::new(ScriptEngine::new()),
            kafka_producer,
            pool,
        }
    }
}
