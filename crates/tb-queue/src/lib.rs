//! tb-queue：Kafka 消息总线抽象。
//!
//! 主题命名约定：
//! - `tb_transport.api.requests` / `tb_transport.api.responses.<node_id>`
//! - `tb_core` / `tb_core.notifications`
//! - `tb_rule_engine.<queue_name>`（默认 Main）/ `tb_rule_engine.notifications`
//! - `tb_transport.notifications.<node_id>`
//! - `tb_housekeeper`
//!
//! 消费提交过渡设计：`StreamQueueConsumer::new` 与 `new_from_beginning`
//! 暂时保留自动提交，避免既有消费循环在改为显式提交前重放消息。
//! 新消费循环应使用 `new_manual_commit` 或 `new_from_beginning_manual`，
//! 并在消息处理成功后调用 `QueueConsumer::commit`；后续各服务将逐个切换。

pub mod admin;
pub mod consumer;
pub mod edqs;
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
