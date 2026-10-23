//! tb-calculated：计算字段（calculated field）评估与输出落库。
//! configuration JSON 约定：
//! ```json
//! {"arguments": {"tempC": {"source": "TELEMETRY", "key": "temperature"}},
//!  "expression": "tempC * 9 / 5 + 32",
//!  "output": {"type": "TELEMETRY", "key": "tempF"}}
//! ```
//! 触发条件：本次保存的遥测/属性包含任一 argument key；随后取全参数（trigger 值优先，
//! 缺失的从对应来源最新值补齐；仍缺失则不计算），Rhai 求值后按 output 落库。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tb_common::{AttributeScope, KvEntry, KvValue, TsKvEntry};
use tb_dao::{AttributesDao, CalculatedFieldDao, KeyDictionaryDao, TimeseriesDao};
use tb_proto::common as common_pb;
use tb_proto::edqs as edqs_pb;
use tb_queue::KafkaQueueProducer;
use tb_script::ScriptEngine;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ArgumentSource {
    Telemetry,
    ClientAttr,
    ServerAttr,
    SharedAttr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CfArgument {
    pub source: ArgumentSource,
    pub key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CfOutputType {
    Telemetry,
    ServerAttr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CfOutput {
    #[serde(rename = "type")]
    pub r#type: CfOutputType,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalculatedFieldConfig {
    pub arguments: HashMap<String, CfArgument>,
    pub expression: String,
    pub output: CfOutput,
}

impl CalculatedFieldConfig {
    pub fn parse(configuration: &serde_json::Value) -> Option<Self> {
        serde_json::from_value(configuration.clone()).ok()
    }
}

pub struct CfOutputValue {
    pub output: CfOutput,
    pub value: KvValue,
}

#[derive(Default)]
pub struct CalculatedFieldEvaluator {
    script_engine: ScriptEngine,
}

impl CalculatedFieldEvaluator {
    pub fn new() -> Self {
        Self {
            script_engine: ScriptEngine::new(),
        }
    }

    /// 任一 argument key 出现在 trigger 中才计算；参数值 trigger 优先，缺失走 fetcher；
    /// 任一参数取不到或脚本失败返回 None。
    pub fn evaluate(
        &self,
        config: &CalculatedFieldConfig,
        trigger: &[KvEntry],
        fetcher: &dyn Fn(ArgumentSource) -> Vec<KvEntry>,
    ) -> Option<CfOutputValue> {
        if !config
            .arguments
            .values()
            .any(|arg| trigger.iter().any(|t| t.key == arg.key))
        {
            return None;
        }
        let mut vars = HashMap::with_capacity(config.arguments.len());
        for (name, arg) in &config.arguments {
            let value = trigger
                .iter()
                .find(|t| t.key == arg.key)
                .map(|t| t.value.clone())
                .or_else(|| {
                    fetcher(arg.source)
                        .into_iter()
                        .find(|e| e.key == arg.key)
                        .map(|e| e.value)
                })?;
            vars.insert(name.clone(), serde_json::to_value(&value).ok()?);
        }
        let result = self
            .script_engine
            .eval_value(&config.expression, &vars)
            .map_err(|e| {
                tracing::warn!("calculated field expression eval failed: {e}");
                e
            })
            .ok()?;
        Some(CfOutputValue {
            output: config.output.clone(),
            value: json_to_kv(result),
        })
    }
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

/// 一次成功落库的计算输出。
pub struct SavedOutput {
    pub output_type: CfOutputType,
    pub key: String,
    pub value: KvValue,
    pub ts: i64,
}

/// 共用入口：查实体全部计算字段 → 评估 → 输出落 ts_kv/ts_kv_latest 或 attribute_kv
/// （SERVER_SCOPE）→ 有 producer 时同步 EDQS 事件。任何失败仅 warn，不阻塞调用方；
/// 返回成功保存的输出（调用方可自行同步缓存等）。
pub async fn evaluate_and_save(
    pool: &PgPool,
    producer: Option<&KafkaQueueProducer>,
    tenant_id: Uuid,
    entity_id: Uuid,
    trigger: &[KvEntry],
) -> Vec<SavedOutput> {
    if trigger.is_empty() {
        return Vec::new();
    }
    let cf_dao = CalculatedFieldDao::new(pool.clone());
    let fields = match cf_dao.find_by_entity(entity_id).await {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("calculated fields lookup failed: {e}");
            return Vec::new();
        }
    };
    if fields.is_empty() {
        return Vec::new();
    }
    let keys = KeyDictionaryDao::new(pool.clone());
    let ts_dao = TimeseriesDao::new(pool.clone(), keys.clone());
    let attr_dao = AttributesDao::new(pool.clone(), keys);

    let latest: Vec<KvEntry> = ts_dao
        .find_latest(entity_id, None)
        .await
        .map(|entries| {
            entries
                .into_iter()
                .map(|e| KvEntry {
                    key: e.key,
                    value: e.value,
                })
                .collect()
        })
        .unwrap_or_default();
    let attrs = attr_dao.find_all(entity_id).await.unwrap_or_default();
    let fetcher = |source: ArgumentSource| -> Vec<KvEntry> {
        match source {
            ArgumentSource::Telemetry => latest.clone(),
            ArgumentSource::ClientAttr => attrs
                .iter()
                .filter(|(s, _)| *s == AttributeScope::ClientScope)
                .map(|(_, e)| e.clone())
                .collect(),
            ArgumentSource::ServerAttr => attrs
                .iter()
                .filter(|(s, _)| *s == AttributeScope::ServerScope)
                .map(|(_, e)| e.clone())
                .collect(),
            ArgumentSource::SharedAttr => attrs
                .iter()
                .filter(|(s, _)| *s == AttributeScope::SharedScope)
                .map(|(_, e)| e.clone())
                .collect(),
        }
    };

    let evaluator = CalculatedFieldEvaluator::new();
    let mut saved = Vec::new();
    for cf in &fields {
        let Some(config) = CalculatedFieldConfig::parse(&cf.configuration) else {
            tracing::warn!(cf_id = %cf.id, "bad calculated field configuration, skipped");
            continue;
        };
        let Some(out) = evaluator.evaluate(&config, trigger, &fetcher) else {
            continue;
        };
        let ts = chrono::Utc::now().timestamp_millis();
        let entry = KvEntry {
            key: out.output.key.clone(),
            value: out.value.clone(),
        };
        let result = match out.output.r#type {
            CfOutputType::Telemetry => {
                ts_dao
                    .save(
                        entity_id,
                        &[TsKvEntry {
                            ts,
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                        }],
                    )
                    .await
            }
            CfOutputType::ServerAttr => {
                attr_dao
                    .save(entity_id, AttributeScope::ServerScope, std::slice::from_ref(&entry))
                    .await
            }
        };
        if let Err(e) = result {
            tracing::warn!(cf_id = %cf.id, "calculated field output save failed: {e}");
            continue;
        }
        publish_edqs(producer, tenant_id, entity_id, out.output.r#type, &entry, ts);
        saved.push(SavedOutput {
            output_type: out.output.r#type,
            key: entry.key,
            value: entry.value,
            ts,
        });
    }
    saved
}

fn publish_edqs(
    producer: Option<&KafkaQueueProducer>,
    tenant_id: Uuid,
    entity_id: Uuid,
    output_type: CfOutputType,
    entry: &KvEntry,
    ts: i64,
) {
    let Some(producer) = producer.cloned() else {
        return;
    };
    let kv = tb_proto::convert::kv_to_proto_named(0, &entry.key, &entry.value);
    let event = match output_type {
        CfOutputType::Telemetry => edqs_pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(edqs_pb::edqs_event_msg::Event::LatestTelemetry(
                edqs_pb::LatestTelemetryMsg {
                    entity_id: entity_id.to_string(),
                    values: vec![kv],
                    ts: vec![ts],
                },
            )),
        },
        CfOutputType::ServerAttr => edqs_pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(edqs_pb::edqs_event_msg::Event::AttributeUpdate(
                edqs_pb::AttributeUpdateMsg {
                    entity_id: entity_id.to_string(),
                    scope: common_pb::AttributeScope::ServerScope as i32,
                    updated: vec![kv],
                    deleted_keys: vec![],
                },
            )),
        },
    };
    tokio::spawn(async move {
        if let Err(e) = tb_queue::edqs::publish_event(&producer, tenant_id, &event).await {
            tracing::warn!("edqs event publish failed: {e}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> CalculatedFieldConfig {
        CalculatedFieldConfig::parse(&json!({
            "arguments": {"tempC": {"source": "TELEMETRY", "key": "temperature"}},
            "expression": "tempC * 9 / 5 + 32",
            "output": {"type": "TELEMETRY", "key": "tempF"},
        }))
        .unwrap()
    }

    fn entry(key: &str, value: i64) -> KvEntry {
        KvEntry {
            key: key.into(),
            value: KvValue::Long(value),
        }
    }

    #[test]
    fn evaluates_when_trigger_matches() {
        let e = CalculatedFieldEvaluator::new();
        let out = e
            .evaluate(&config(), &[entry("temperature", 25)], &|_| vec![])
            .unwrap();
        assert_eq!(out.output.key, "tempF");
        assert_eq!(out.value, KvValue::Long(77));
    }

    #[test]
    fn skips_when_trigger_has_no_argument_key() {
        let e = CalculatedFieldEvaluator::new();
        assert!(
            e.evaluate(&config(), &[entry("humidity", 50)], &|_| vec![])
                .is_none()
        );
    }

    #[test]
    fn missing_argument_aborts() {
        let e = CalculatedFieldEvaluator::new();
        let cfg = CalculatedFieldConfig::parse(&json!({
            "arguments": {
                "a": {"source": "TELEMETRY", "key": "temperature"},
                "b": {"source": "SERVER_ATTR", "key": "offset"}
            },
            "expression": "a + b",
            "output": {"type": "TELEMETRY", "key": "sum"},
        }))
        .unwrap();
        // trigger 命中但 offset 取不到 → 不算
        assert!(
            e.evaluate(&cfg, &[entry("temperature", 1)], &|_| vec![])
                .is_none()
        );
        // fetcher 补齐 offset → 算
        let out = e
            .evaluate(&cfg, &[entry("temperature", 1)], &|source| {
                match source {
                    ArgumentSource::ServerAttr => vec![entry("offset", 10)],
                    _ => vec![],
                }
            })
            .unwrap();
        assert_eq!(out.value, KvValue::Long(11));
    }

    #[test]
    fn expression_error_aborts() {
        let e = CalculatedFieldEvaluator::new();
        let cfg = CalculatedFieldConfig::parse(&json!({
            "arguments": {"a": {"source": "TELEMETRY", "key": "temperature"}},
            "expression": "a +",
            "output": {"type": "SERVER_ATTR", "key": "x"},
        }))
        .unwrap();
        assert!(
            e.evaluate(&cfg, &[entry("temperature", 1)], &|_| vec![])
                .is_none()
        );
    }
}
