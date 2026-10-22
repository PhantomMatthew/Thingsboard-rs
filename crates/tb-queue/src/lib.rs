//! tb-queue：Kafka 消息总线抽象。
//!
//! 主题命名约定：
//! - `tb_transport.api.requests` / `tb_transport.api.responses.<node_id>`
//! - `tb_core` / `tb_core.notifications`
//! - `tb_rule_engine.<queue_name>`（默认 Main）/ `tb_rule_engine.notifications`
//! - `tb_transport.notifications.<node_id>`
//! - `tb_housekeeper`

pub mod admin;
pub mod consumer;
pub mod partition;
pub mod producer;
pub mod topics;

pub use consumer::{QueueConsumer, StreamQueueConsumer};
pub use producer::{KafkaQueueProducer, QueueProducer};
pub use topics::TbTopic;

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("timeout waiting for response")]
    Timeout,
}
