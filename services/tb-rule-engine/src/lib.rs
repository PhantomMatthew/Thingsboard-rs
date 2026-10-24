//! tb-rule-engine 服务：手动提交规则队列 offset，引擎错误与毒消息先进入 DLQ。

use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use sqlx::PgPool;
use tb_common::{TbResult, msg::TbMsg};
use tb_proto::ruleengine::ToRuleEngineMsg;
use tb_queue::topics::TbTopic;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer, StreamQueueConsumer};
use tb_rule_engine::RuleEngineService;

pub const DEFAULT_GROUP: &str = "tb-rule-engine";
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);

pub async fn start_rule_engine(
    brokers: &str,
    pool: PgPool,
    queue_name: &str,
    redis_manager: redis::aio::ConnectionManager,
) -> TbResult<tokio::task::JoinHandle<()>> {
    start_rule_engine_inner(
        brokers,
        pool,
        queue_name,
        DEFAULT_GROUP,
        DEFAULT_CACHE_TTL,
        Some(redis_manager),
    )
    .await
}

/// group_id / chain_cache_ttl 可定制：集成测试用独立 group 与短缓存 TTL。
pub async fn start_rule_engine_for_group(
    brokers: &str,
    pool: PgPool,
    queue_name: &str,
    group_id: &str,
    chain_cache_ttl: Duration,
) -> TbResult<tokio::task::JoinHandle<()>> {
    start_rule_engine_inner(brokers, pool, queue_name, group_id, chain_cache_ttl, None).await
}

async fn start_rule_engine_inner(
    brokers: &str,
    pool: PgPool,
    queue_name: &str,
    group_id: &str,
    chain_cache_ttl: Duration,
    redis_manager: Option<redis::aio::ConnectionManager>,
) -> TbResult<tokio::task::JoinHandle<()>> {
    let topic = TbTopic::rule_engine(queue_name);
    if let Err(e) = tb_queue::admin::ensure_topic(brokers, &topic, 10).await {
        tracing::warn!("ensure rule engine topic failed: {e}");
    }
    if let Err(e) = tb_queue::admin::ensure_topic(brokers, TbTopic::RULE_ENGINE_DLQ, 10).await {
        tracing::warn!("ensure rule engine DLQ topic failed: {e}");
    }
    let producer =
        KafkaQueueProducer::new(brokers, "tb-rule-engine").map_err(tb_common::TbError::internal)?;
    let engine = match redis_manager {
        Some(redis) => RuleEngineService::with_cache_ttl_and_redis(
            pool,
            producer.clone(),
            chain_cache_ttl,
            redis,
        ),
        None => RuleEngineService::with_cache_ttl(pool, producer.clone(), chain_cache_ttl),
    };
    let mut consumer = StreamQueueConsumer::new_manual_commit(brokers, group_id, &[&topic])
        .map_err(tb_common::TbError::internal)?;
    let group_id = group_id.to_string();
    Ok(tokio::spawn(async move {
        tracing::info!(topic = %topic, group = %group_id, "rule engine started");
        let scanner_owner = format!("{group_id}-{}", uuid::Uuid::now_v7().simple());
        let mut scan_interval = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                received = consumer.recv() => {
                    match received {
                        Ok(msg) => {
                            tracing::debug!(topic = %msg.topic, offset = msg.offset, "rule engine message received");
                            let key = msg
                                .key
                                .as_ref()
                                .map(|key| String::from_utf8_lossy(key).into_owned())
                                .unwrap_or_else(|| "rule-engine-dlq".into());
                            loop {
                                match process_payload(&engine, &producer, msg.payload.clone(), &key).await {
                                    Ok(()) => break,
                                    Err(e) => {
                                        tracing::error!(
                                            topic = %msg.topic,
                                            offset = msg.offset,
                                            "DLQ send failed; offset not committed: {e}"
                                        );
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                    }
                                }
                            }
                            loop {
                                match consumer.commit(&msg).await {
                                    Ok(()) => break,
                                    Err(e) => {
                                        tracing::error!("rule engine commit failed: {e}");
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("rule engine consumer error: {e}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
                _ = scan_interval.tick() => {
                    if let Err(e) = engine.process_due_windows(&scanner_owner, 100).await {
                        tracing::warn!("deduplication window scan failed: {e}");
                    }
                }
            }
        }
    }))
}

async fn send_dlq_envelope<P: QueueProducer>(
    producer: &P,
    key: &str,
    envelope: ToRuleEngineMsg,
) -> Result<(), tb_queue::QueueError> {
    producer
        .send(
            TbTopic::RULE_ENGINE_DLQ,
            key,
            Bytes::from(envelope.encode_to_vec()),
        )
        .await
}

async fn send_raw_dlq<P: QueueProducer>(
    producer: &P,
    key: &str,
    payload: Bytes,
) -> Result<(), tb_queue::QueueError> {
    producer.send(TbTopic::RULE_ENGINE_DLQ, key, payload).await
}

fn dlq_envelope(mut msg: TbMsg, error: String) -> ToRuleEngineMsg {
    msg.metadata.insert("error".into(), error);
    ToRuleEngineMsg {
        tenant_id: msg.tenant_id.to_string(),
        tb_msg: Some(tb_proto::convert::tb_msg_to_proto(&msg)),
    }
}

/// Ok 表示可以提交原 offset（包括 DLQ 已写成功）；Err 表示必须保留 offset。
async fn process_payload<P: QueueProducer>(
    engine: &RuleEngineService,
    producer: &P,
    payload: Bytes,
    key: &str,
) -> Result<(), tb_queue::QueueError> {
    let to_re = match ToRuleEngineMsg::decode(payload.clone()) {
        Ok(msg) => msg,
        Err(e) => {
            tracing::warn!("undecodable ToRuleEngineMsg, sending raw payload to DLQ: {e}");
            return send_raw_dlq(producer, key, payload).await;
        }
    };
    let ToRuleEngineMsg { tenant_id, tb_msg } = to_re;
    let Some(tb_msg) = tb_msg else {
        tracing::warn!("ToRuleEngineMsg without TbMsg, sending raw payload to DLQ");
        return send_raw_dlq(producer, key, payload).await;
    };
    match tb_proto::convert::tb_msg_from_proto(&tb_msg) {
        Ok(msg) => {
            let msg_id = msg.id;
            let msg_type = msg.msg_type.clone();
            let resume = msg.rule_chain_id.zip(msg.rule_node_id);
            let original = msg.clone();
            let result = match resume {
                Some((chain_id, node_id)) => engine.handle_at(msg, chain_id, node_id).await,
                None => engine.handle(msg).await,
            };
            match result {
                Ok(()) => {
                    tracing::debug!(%msg_id, %msg_type, "rule engine handled");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!(%msg_id, %msg_type, "rule engine handle failed: {e}");
                    let envelope =
                        dlq_envelope(original, format!("rule engine handle failed: {e}"));
                    send_dlq_envelope(producer, key, envelope).await
                }
            }
        }
        Err(e) => {
            tracing::warn!("bad TbMsgProto, sending to DLQ: {e}");
            let mut tb_msg = tb_msg;
            tb_msg
                .metadata
                .insert("error".into(), format!("bad TbMsgProto: {e}"));
            send_dlq_envelope(
                producer,
                key,
                ToRuleEngineMsg {
                    tenant_id,
                    tb_msg: Some(tb_msg),
                },
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use sqlx::postgres::PgPoolOptions;
    use tb_common::{EntityId, EntityType};
    use tb_queue::QueueError;
    use uuid::Uuid;

    use super::*;

    type SentMessages = Arc<Mutex<Vec<(String, Bytes)>>>;

    struct TestProducer {
        sent: SentMessages,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl QueueProducer for TestProducer {
        async fn send(&self, topic: &str, _key: &str, payload: Bytes) -> Result<(), QueueError> {
            if self.fail {
                return Err(QueueError::Serialization("injected DLQ failure".into()));
            }
            self.sent.lock().unwrap().push((topic.to_string(), payload));
            Ok(())
        }
    }

    fn producer(fail: bool) -> (TestProducer, SentMessages) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        (
            TestProducer {
                sent: sent.clone(),
                fail,
            },
            sent,
        )
    }

    fn failing_engine() -> RuleEngineService {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(200))
            .connect_lazy("postgres://thingsboard:thingsboard@127.0.0.1:1/thingsboard")
            .unwrap();
        let kafka = KafkaQueueProducer::new("127.0.0.1:1", "rule-engine-test").unwrap();
        RuleEngineService::with_cache_ttl(pool, kafka, Duration::from_secs(1))
    }

    fn message() -> TbMsg {
        TbMsg::new(
            "POST_TELEMETRY_REQUEST",
            EntityId::new(EntityType::Device),
            Uuid::now_v7(),
            serde_json::json!({"temperature": 20}),
            HashMap::new(),
        )
    }

    fn payload(msg: &TbMsg) -> Bytes {
        Bytes::from(
            ToRuleEngineMsg {
                tenant_id: msg.tenant_id.to_string(),
                tb_msg: Some(tb_proto::convert::tb_msg_to_proto(msg)),
            }
            .encode_to_vec(),
        )
    }

    #[tokio::test]
    async fn engine_error_sends_original_message_to_dlq() {
        let msg = message();
        let msg_id = msg.id.to_string();
        let (producer, sent) = producer(false);
        process_payload(&failing_engine(), &producer, payload(&msg), "key")
            .await
            .unwrap();

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, TbTopic::RULE_ENGINE_DLQ);
        let envelope = ToRuleEngineMsg::decode(sent[0].1.clone()).unwrap();
        let dlq_msg = envelope.tb_msg.unwrap();
        assert_eq!(dlq_msg.id, msg_id);
        assert!(
            dlq_msg
                .metadata
                .get("error")
                .is_some_and(|error| error.contains("rule engine handle failed"))
        );
    }

    #[tokio::test]
    async fn undecodable_proto_sends_raw_bytes_to_dlq() {
        let poison = Bytes::from_static(&[0xff, 0xff, 0xff]);
        let (producer, sent) = producer(false);
        process_payload(&failing_engine(), &producer, poison.clone(), "key")
            .await
            .unwrap();

        let sent = sent.lock().unwrap();
        assert_eq!(
            sent.as_slice(),
            &[(TbTopic::RULE_ENGINE_DLQ.to_string(), poison)]
        );
    }

    #[tokio::test]
    async fn dlq_failure_prevents_successful_processing_result() {
        let (producer, _) = producer(true);
        let result =
            process_payload(&failing_engine(), &producer, payload(&message()), "key").await;
        assert!(result.is_err());
    }
}
