use std::collections::HashMap;

use bytes::Bytes;
use prost::Message as _;
use tb_common::{EntityId, EntityType, TbError, TbResult};
use tb_proto::core::SessionInfoProto;
use tb_proto::ruleengine::ToRuleEngineMsg;
use tb_queue::{KafkaQueueProducer, QueueProducer as _, TbTopic};
use uuid::Uuid;

pub async fn publish_event(
    producer: &KafkaQueueProducer,
    tenant_id: Uuid,
    entity_type: EntityType,
    entity_id: Uuid,
    msg_type: &str,
    data: serde_json::Value,
    metadata: HashMap<String, String>,
) -> TbResult<()> {
    let tb_msg = tb_common::msg::TbMsg::new(
        msg_type,
        EntityId {
            entity_type,
            id: entity_id,
        },
        tenant_id,
        data,
        metadata,
    );
    let msg = ToRuleEngineMsg {
        tenant_id: tenant_id.to_string(),
        tb_msg: Some(tb_proto::convert::tb_msg_to_proto(&tb_msg)),
    };
    producer
        .send(
            &TbTopic::rule_engine("Main"),
            &entity_id.to_string(),
            Bytes::from(msg.encode_to_vec()),
        )
        .await
        .map_err(TbError::internal)
}

pub async fn publish_session_event(
    producer: &KafkaQueueProducer,
    session: &SessionInfoProto,
    msg_type: &str,
) -> TbResult<()> {
    let tenant_id = Uuid::parse_str(&session.tenant_id)
        .map_err(|_| TbError::InvalidInput(format!("bad tenant_id: {}", session.tenant_id)))?;
    let device_id = Uuid::parse_str(&session.device_id)
        .map_err(|_| TbError::InvalidInput(format!("bad device_id: {}", session.device_id)))?;
    let metadata = HashMap::from([
        ("deviceName".into(), session.device_name.clone()),
        ("deviceType".into(), session.device_type.clone()),
    ]);
    publish_event(
        producer,
        tenant_id,
        EntityType::Device,
        device_id,
        msg_type,
        serde_json::json!({}),
        metadata,
    )
    .await
}
