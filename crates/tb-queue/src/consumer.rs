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
    pub fn new(
        brokers: &str,
        group_id: &str,
        topics: &[&str],
    ) -> Result<Self, QueueError> {
        let inner: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "1000")
            .set("auto.offset.reset", "latest")
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
