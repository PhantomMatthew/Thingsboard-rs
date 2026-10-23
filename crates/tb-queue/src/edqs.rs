//! EDQS 事件发布：编码 EdqsEventMsg 发往 `tb_edqs.events`，key = tenant_id。

use bytes::Bytes;
use prost::Message as _;
use tb_proto::edqs::EdqsEventMsg;
use uuid::Uuid;

use crate::producer::{KafkaQueueProducer, QueueProducer};
use crate::{QueueError, TbTopic};

pub async fn publish_event(
    producer: &KafkaQueueProducer,
    tenant_id: Uuid,
    event: &EdqsEventMsg,
) -> Result<(), QueueError> {
    producer
        .send(
            TbTopic::EDQS_EVENTS,
            &tenant_id.to_string(),
            Bytes::from(event.encode_to_vec()),
        )
        .await
}
