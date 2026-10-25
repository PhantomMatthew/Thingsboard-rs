//! tb_core 毒消息：写入 tb_core.dlq 后提交，同分区后续消息继续处理。

mod common;

use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use serde_json::Value;
use tb_common::AttributeScope;
use tb_proto::core as pb;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer, StreamQueueConsumer, TbTopic};
use uuid::Uuid;

use common::{INTEGRATION_LOCK, start_server_with_state};

#[tokio::test]
async fn poison_core_message_is_dead_lettered_and_partition_keeps_flowing() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let (_base, _pool, state) = start_server_with_state().await;
    let group = format!("tb-core-main-{}", Uuid::now_v7().simple());
    let _core = tb_core::core_consumer::start_core_consumer_for_group(state.clone(), &group).await;

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let producer = KafkaQueueProducer::new(&brokers, "tb-core-dlq-test").unwrap();
    let mut dlq = StreamQueueConsumer::new_from_beginning(
        &brokers,
        &format!("tb-core-dlq-test-{}", Uuid::now_v7().simple()),
        &[TbTopic::CORE_DLQ],
    )
    .unwrap();

    let device_id = Uuid::now_v7();
    let key = device_id.to_string();
    let open = pb::ToCoreMsg {
        tenant_id: Uuid::now_v7().to_string(),
        entity_id: key.clone(),
        msg: Some(pb::to_core_msg::Msg::SessionOpen(pb::SessionOpenMsg {
            session: Some(pb::SessionInfoProto {
                node_id: "transport-test".into(),
                session_id: Uuid::now_v7().to_string(),
                tenant_id: Uuid::now_v7().to_string(),
                device_id: key.clone(),
                ..Default::default()
            }),
        })),
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let mut letter: Option<Value> = None;
    let mut active = false;
    while tokio::time::Instant::now() < deadline && (letter.is_none() || !active) {
        producer
            .send(TbTopic::CORE, &key, Bytes::from_static(&[0xff, 0xff]))
            .await
            .unwrap();
        producer
            .send(TbTopic::CORE, &key, Bytes::from(open.encode_to_vec()))
            .await
            .unwrap();

        let round_end = tokio::time::Instant::now() + Duration::from_secs(5);
        while letter.is_none() {
            let Ok(Ok(msg)) = tokio::time::timeout_at(round_end, dlq.recv()).await else {
                break;
            };
            if msg.key.as_deref() == Some(key.as_bytes()) {
                letter = Some(serde_json::from_slice(&msg.payload).unwrap());
            }
        }
        active = state
            .attributes_dao
            .find(
                device_id,
                AttributeScope::ServerScope,
                Some(&["active".to_string()]),
            )
            .await
            .unwrap()
            .iter()
            .any(|kv| kv.value == tb_common::KvValue::Bool(true));
    }

    let letter = letter.expect("poison message reached tb_core.dlq");
    assert_eq!(letter["sourceTopic"], TbTopic::CORE);
    assert_eq!(letter["key"], key.as_str());
    assert_eq!(letter["payloadHex"], "ffff");
    assert_eq!(letter["redacted"], false);
    assert!(letter["offset"].as_i64().unwrap() >= 0);
    assert!(
        letter["error"]
            .as_str()
            .unwrap()
            .contains("undecodable ToCoreMsg")
    );
    assert!(
        active,
        "session open after the poison message was processed"
    );
}
