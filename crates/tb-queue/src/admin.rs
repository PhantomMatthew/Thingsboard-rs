use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;

use crate::QueueError;

/// 创建 topic（幂等，已存在则忽略）。
pub async fn ensure_topic(brokers: &str, topic: &str, partitions: i32) -> Result<(), QueueError> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()?;
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([new_topic].iter(), &AdminOptions::new())
        .await?;
    for r in results {
        if let Err((_, code)) = r
            && code != rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists
        {
            return Err(QueueError::Kafka(rdkafka::error::KafkaError::AdminOp(code)));
        }
    }
    Ok(())
}
