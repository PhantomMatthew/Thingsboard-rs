use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

use crate::QueueError;

pub struct ConsumedMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Bytes>,
    pub payload: Bytes,
}

#[async_trait]
pub trait QueueConsumer: Send + Sync {
    async fn recv(&mut self) -> Result<ConsumedMessage, QueueError>;
}

pub struct StreamQueueConsumer {
    inner: StreamConsumer,
}

impl StreamQueueConsumer {
    pub fn new(brokers: &str, group_id: &str, topics: &[&str]) -> Result<Self, QueueError> {
        Self::with_offset_reset(brokers, group_id, topics, "latest")
    }

    /// 从头消费：新 group 启动时重放历史消息（EDQS 快照加载期间的事件靠它兜底）。
    pub fn new_from_beginning(
        brokers: &str,
        group_id: &str,
        topics: &[&str],
    ) -> Result<Self, QueueError> {
        Self::with_offset_reset(brokers, group_id, topics, "earliest")
    }

    fn with_offset_reset(
        brokers: &str,
        group_id: &str,
        topics: &[&str],
        offset_reset: &str,
    ) -> Result<Self, QueueError> {
        let inner: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "1000")
            .set("auto.offset.reset", offset_reset)
            .set("session.timeout.ms", "30000")
            .create()?;
        inner.subscribe(topics)?;
        Ok(Self { inner })
    }
}

#[async_trait]
impl QueueConsumer for StreamQueueConsumer {
    async fn recv(&mut self) -> Result<ConsumedMessage, QueueError> {
        let msg = self
            .inner
            .stream()
            .next()
            .await
            .ok_or_else(|| QueueError::Serialization("consumer stream ended".into()))??;
        Ok(ConsumedMessage {
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            key: msg.key().map(Bytes::copy_from_slice),
            payload: Bytes::copy_from_slice(msg.payload().unwrap_or(&[])),
        })
    }
}
