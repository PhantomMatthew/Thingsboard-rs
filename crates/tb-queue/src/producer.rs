use async_trait::async_trait;
use bytes::Bytes;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

use crate::QueueError;

#[async_trait]
pub trait QueueProducer: Send + Sync {
    /// 发送消息。`key` 用于分区（Kafka 默认 murmur2 partitioner）。
    async fn send(&self, topic: &str, key: &str, payload: Bytes) -> Result<(), QueueError>;
}

#[derive(Clone)]
pub struct KafkaQueueProducer {
    inner: FutureProducer,
}

impl KafkaQueueProducer {
    pub fn new(brokers: &str, client_id: &str) -> Result<Self, QueueError> {
        let inner = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("client.id", client_id)
            .set("message.timeout.ms", "10000")
            // 遥测需由全部同步副本确认，避免 leader 故障时丢失已确认写入。
            .set("acks", "all")
            .create()?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl QueueProducer for KafkaQueueProducer {
    async fn send(&self, topic: &str, key: &str, payload: Bytes) -> Result<(), QueueError> {
        self.inner
            .send(
                FutureRecord::to(topic).key(key).payload(payload.as_ref()),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| QueueError::Kafka(e))?;
        Ok(())
    }
}
