//! tb-rule-engine 服务：消费 `tb_rule_engine.{queue}` 的 ToRuleEngineMsg，交给 RuleEngineService。

use std::time::Duration;

use prost::Message as _;
use sqlx::PgPool;
use tb_proto::ruleengine::ToRuleEngineMsg;
use tb_queue::topics::TbTopic;
use tb_queue::{KafkaQueueProducer, QueueConsumer, StreamQueueConsumer};
use tb_rule_engine::RuleEngineService;

pub const DEFAULT_GROUP: &str = "tb-rule-engine";
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);

fn parse_uuid(s: &str) -> Option<uuid::Uuid> {
    if s.is_empty() {
        return None;
    }
    uuid::Uuid::parse_str(s).ok()
}

pub async fn start_rule_engine(brokers: &str, pool: PgPool, queue_name: &str) -> tokio::task::JoinHandle<()> {
    start_rule_engine_for_group(brokers, pool, queue_name, DEFAULT_GROUP, DEFAULT_CACHE_TTL).await
}

/// group_id / chain_cache_ttl 可定制：集成测试用独立 group 与短缓存 TTL。
pub async fn start_rule_engine_for_group(
    brokers: &str,
    pool: PgPool,
    queue_name: &str,
    group_id: &str,
    chain_cache_ttl: Duration,
) -> tokio::task::JoinHandle<()> {
    let topic = TbTopic::rule_engine(queue_name);
    if let Err(e) = tb_queue::admin::ensure_topic(brokers, &topic, 10).await {
        tracing::warn!("ensure rule engine topic failed: {e}");
    }
    if let Err(e) = tb_queue::admin::ensure_topic(brokers, TbTopic::RULE_ENGINE_DLQ, 10).await {
        tracing::warn!("ensure rule engine DLQ topic failed: {e}");
    }
    let producer = KafkaQueueProducer::new(brokers, "tb-rule-engine")
        .expect("kafka producer init failed");
    let engine = RuleEngineService::with_cache_ttl(pool, producer, chain_cache_ttl);
    let brokers = brokers.to_string();
    let group_id = group_id.to_string();
    tokio::spawn(async move {
        let mut consumer = match StreamQueueConsumer::new(&brokers, &group_id, &[&topic]) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("rule engine consumer init failed: {e}");
                return;
            }
        };
        tracing::info!(topic = %topic, group = %group_id, "rule engine started");
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    tracing::debug!(topic = %msg.topic, offset = msg.offset, "rule engine message received");
                    let Ok(to_re) = ToRuleEngineMsg::decode(msg.payload) else {
                        tracing::warn!("undecodable ToRuleEngineMsg");
                        continue;
                    };
                    let Some(tb_msg) = to_re.tb_msg else {
                        continue;
                    };
                    // checkpoint 恢复：rule_chain_id + rule_node_id 非空时从指定节点继续。
                    let resume = parse_uuid(&tb_msg.rule_chain_id)
                        .zip(parse_uuid(&tb_msg.rule_node_id));
                    match tb_proto::convert::tb_msg_from_proto(&tb_msg) {
                        Ok(m) => {
                            let msg_type = m.msg_type.clone();
                            let msg_id = m.id;
                            let result = match resume {
                                Some((chain_id, node_id)) => {
                                    engine.handle_at(m, chain_id, node_id).await
                                }
                                None => engine.handle(m).await,
                            };
                            match result {
                                Ok(()) => {
                                    tracing::debug!(%msg_id, %msg_type, "rule engine handled")
                                }
                                Err(e) => {
                                    tracing::error!(%msg_id, %msg_type, "rule engine handle failed: {e}")
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("bad TbMsgProto: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("rule engine consumer error: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    })
}
