//! 遥测/属性 ingest：消费 `tb_rule_engine.Main`（Phase 3a 直通，Phase 4 由规则引擎接管），
//! POST_TELEMETRY_REQUEST 落 ts_kv + Redis latest write-through，
//! POST_ATTRIBUTES_REQUEST 落 attribute_kv（CLIENT_SCOPE）。

use prost::Message as _;
use tb_common::msg::msg_type;
use tb_common::{AttributeScope, KvEntry, KvValue, TsKvEntry};
use tb_proto::ruleengine::{TbMsgProto, ToRuleEngineMsg};
use tb_queue::topics::TbTopic;
use tb_queue::{QueueConsumer, StreamQueueConsumer};
use uuid::Uuid;

use crate::AppState;
use crate::telemetry_cache;

pub async fn start_telemetry_ingest(state: AppState) -> tokio::task::JoinHandle<()> {
    start_telemetry_ingest_for_group(state, "tb-core-ingest").await
}

/// group_id 可定制：集成测试用独立 group 避免幽灵成员拖慢 rebalance。
pub async fn start_telemetry_ingest_for_group(
    state: AppState,
    group_id: &str,
) -> tokio::task::JoinHandle<()> {
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let topic = TbTopic::rule_engine("Main");
    if let Err(e) = tb_queue::admin::ensure_topic(&brokers, &topic, 10).await {
        tracing::warn!("ensure rule engine topic failed: {e}");
    }
    let group_id = group_id.to_string();
    tokio::spawn(async move {
        let mut consumer = match StreamQueueConsumer::new(&brokers, &group_id, &[&topic]) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("ingest consumer init failed: {e}");
                return;
            }
        };
        tracing::info!("telemetry ingest started");
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let Ok(to_re) = ToRuleEngineMsg::decode(msg.payload) else {
                        tracing::warn!("undecodable ToRuleEngineMsg");
                        continue;
                    };
                    let Some(tb_msg) = to_re.tb_msg else {
                        continue;
                    };
                    if let Err(e) = handle_msg(&state, &tb_msg).await {
                        tracing::error!(msg_type = tb_msg.r#type, "ingest failed: {e}");
                    }
                }
                Err(e) => {
                    tracing::error!("ingest consumer error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    })
}

async fn handle_msg(state: &AppState, msg: &TbMsgProto) -> tb_common::TbResult<()> {
    match msg.r#type.as_str() {
        msg_type::POST_TELEMETRY_REQUEST => {
            let entity_id = Uuid::parse_str(&msg.originator_id).map_err(|_| {
                tb_common::TbError::InvalidInput(format!(
                    "bad originator_id: {}",
                    msg.originator_id
                ))
            })?;
            let data: serde_json::Value = serde_json::from_str(&msg.data_json)
                .map_err(|e| tb_common::TbError::InvalidInput(format!("bad data_json: {e}")))?;
            let ts = data["ts"].as_i64().unwrap_or(msg.ts);
            let values = data["values"].as_object().cloned().unwrap_or_default();
            let entries: Vec<TsKvEntry> = values
                .into_iter()
                .map(|(key, v)| TsKvEntry {
                    ts,
                    key,
                    value: json_to_kv(v),
                })
                .collect();
            telemetry_cache::save_with_latest(state, entity_id, &entries).await?;
        }
        msg_type::POST_ATTRIBUTES_REQUEST => {
            let entity_id = Uuid::parse_str(&msg.originator_id).map_err(|_| {
                tb_common::TbError::InvalidInput(format!(
                    "bad originator_id: {}",
                    msg.originator_id
                ))
            })?;
            let data: serde_json::Value = serde_json::from_str(&msg.data_json)
                .map_err(|e| tb_common::TbError::InvalidInput(format!("bad data_json: {e}")))?;
            let scope = match data["scope"].as_str().unwrap_or("CLIENT_SCOPE") {
                "CLIENT_SCOPE" => AttributeScope::ClientScope,
                "SERVER_SCOPE" => AttributeScope::ServerScope,
                "SHARED_SCOPE" => AttributeScope::SharedScope,
                other => {
                    tracing::warn!(scope = other, "unknown attribute scope, skipped");
                    return Ok(());
                }
            };
            let kv = data["kv"].as_object().cloned().unwrap_or_default();
            let entries: Vec<KvEntry> = kv
                .into_iter()
                .map(|(key, v)| KvEntry {
                    key,
                    value: json_to_kv(v),
                })
                .collect();
            state.attributes_dao.save(entity_id, scope, &entries).await?;
        }
        other => {
            tracing::debug!(msg_type = other, "ingest skipped");
        }
    }
    Ok(())
}

fn json_to_kv(v: serde_json::Value) -> KvValue {
    match v {
        serde_json::Value::Bool(b) => KvValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(l) = n.as_i64() {
                KvValue::Long(l)
            } else {
                KvValue::Double(n.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(s) => KvValue::Str(s),
        other => KvValue::Json(other),
    }
}
