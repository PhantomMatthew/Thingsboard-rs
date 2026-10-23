//! action.*：副作用节点（告警清除/属性删除/延迟/去重/计算/下行 RPC）。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use prost::Message as _;
use serde::Deserialize;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{EntityId, EntityType, TbError, TbResult};
use tb_dao::models::{Alarm, AlarmStatus};
use tb_proto::core as core_pb;
use tb_queue::{QueueProducer, TbTopic};
use uuid::Uuid;

use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::{parse_config, parse_scope};

/// action.clear_alarm：originator+type 的最新 ACTIVE 告警 → clear，产出 ALARM 消息（"Cleared"）；
/// 无 ACTIVE 告警 → "False"；错误 → "Failure"。
pub struct ClearAlarmNode {
    alarm_type: String,
    details_script: Option<String>,
}

impl ClearAlarmNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            alarm_type: String,
            details_script: Option<String>,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            alarm_type: c.alarm_type,
            details_script: c.details_script,
        })
    }

    async fn process(&self, ctx: &NodeContext, msg: &TbMsg) -> TbResult<Option<Alarm>> {
        let latest = ctx
            .alarm_dao
            .find_latest_by_originator_and_type(msg.originator.id, &self.alarm_type)
            .await?;
        let Some(alarm) = latest else { return Ok(None) };
        if !matches!(
            alarm.status,
            AlarmStatus::ActiveUnack | AlarmStatus::ActiveAck
        ) {
            return Ok(None);
        }
        if let Some(script) = &self.details_script {
            let details = ctx
                .script_engine
                .eval_map(script, &msg.data, &msg.metadata, &msg.msg_type)
                .map_err(|e| TbError::InvalidInput(format!("details script: {e}")))?;
            let mut updated = alarm.clone();
            updated.details = Some(details);
            ctx.alarm_dao.save(&updated).await?;
        }
        ctx.alarm_dao.clear(alarm.id).await.map(Some)
    }
}

#[async_trait]
impl Node for ClearAlarmNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        match self.process(ctx, &msg).await {
            Ok(Some(alarm)) => {
                let data = serde_json::to_value(&alarm).unwrap_or_default();
                let alarm_msg = TbMsg {
                    id: Uuid::now_v7(),
                    queue_name: msg.queue_name.clone(),
                    msg_type: msg_type::ALARM.to_string(),
                    originator: EntityId {
                        entity_type: EntityType::Alarm,
                        id: alarm.id,
                    },
                    tenant_id: msg.tenant_id,
                    customer_id: msg.customer_id,
                    data,
                    metadata: msg.metadata.clone(),
                    ts: chrono::Utc::now().timestamp_millis(),
                };
                vec![("Cleared".into(), alarm_msg)]
            }
            Ok(None) => vec![("False".into(), msg)],
            Err(e) => {
                tracing::warn!("clear alarm failed: {e}");
                vec![("Failure".into(), msg)]
            }
        }
    }
}

/// action.delete_attributes：删除 originator 指定 scope 的属性。
pub struct DeleteAttributesNode {
    scope: String,
    keys: Vec<String>,
}

impl DeleteAttributesNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            scope: String,
            keys: Vec<String>,
        }
        let c: Config = parse_config(config)?;
        parse_scope(&c.scope)?;
        Ok(Self {
            scope: c.scope,
            keys: c.keys,
        })
    }
}

#[async_trait]
impl Node for DeleteAttributesNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let scope = parse_scope(&self.scope).expect("validated in new");
        let rel = match ctx
            .attributes_dao
            .delete(msg.originator.id, scope, &self.keys)
            .await
        {
            Ok(_) => "Success",
            Err(e) => {
                tracing::warn!("delete attributes failed: {e}");
                "Failure"
            }
        };
        vec![(rel.into(), msg)]
    }
}

/// action.delay：sleep delay_sec（上限 300）后 "Success"。
pub struct DelayNode {
    delay: Duration,
}

const MAX_DELAY_SEC: u64 = 300;

impl DelayNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            delay_sec: u64,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            delay: Duration::from_secs(c.delay_sec.min(MAX_DELAY_SEC)),
        })
    }
}

#[async_trait]
impl Node for DelayNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        tokio::time::sleep(self.delay).await;
        vec![("Success".into(), msg)]
    }
}

/// action.deduplication：per originator+内容 hash 窗口去重；首次 "Success"，重复 "Deduplicated"。
/// key_fields 为空时对 msg.data 全量 hash（serde_json Map 默认 BTree，序列化结果稳定）。
pub struct DeduplicationNode {
    window: Duration,
    key_fields: Vec<String>,
    seen: DashMap<(Uuid, u64), Instant>,
}

/// 触发惰性清理的容量阈值。
const DEDUP_SWEEP_THRESHOLD: usize = 10_000;

impl DeduplicationNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            window_sec: u64,
            #[serde(default)]
            key_fields: Vec<String>,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            window: Duration::from_secs(c.window_sec.max(1)),
            key_fields: c.key_fields,
            seen: DashMap::new(),
        })
    }

    fn content_hash(&self, msg: &TbMsg) -> u64 {
        let mut hasher = DefaultHasher::new();
        if self.key_fields.is_empty() {
            msg.data.to_string().hash(&mut hasher);
        } else {
            for f in &self.key_fields {
                msg.data.get(f).cloned().unwrap_or(serde_json::Value::Null).to_string().hash(&mut hasher);
            }
        }
        hasher.finish()
    }
}

#[async_trait]
impl Node for DeduplicationNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let key = (msg.originator.id, self.content_hash(&msg));
        // 惰性过期清理：超阈值时扫一遍删掉过期项。
        if self.seen.len() > DEDUP_SWEEP_THRESHOLD {
            let window = self.window;
            self.seen.retain(|_, t| t.elapsed() < window);
        }
        let duplicate = self
            .seen
            .get(&key)
            .is_some_and(|t| t.elapsed() < self.window);
        if duplicate {
            vec![("Deduplicated".into(), msg)]
        } else {
            self.seen.insert(key, Instant::now());
            vec![("Success".into(), msg)]
        }
    }
}

/// action.math：对 msg.data 中 fields 做数值运算，结果写回 result_key。
pub struct MathNode {
    operation: MathOp,
    fields: Vec<String>,
    result_key: String,
    round: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum MathOp {
    Add,
    Sub,
    Mult,
    Div,
    Min,
    Max,
}

impl MathNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            operation: MathOp,
            fields: Vec<String>,
            result_key: String,
            round: Option<u32>,
        }
        let c: Config = parse_config(config)?;
        if c.fields.is_empty() {
            return Err(TbError::InvalidInput("math node requires fields[]".into()));
        }
        Ok(Self {
            operation: c.operation,
            fields: c.fields,
            result_key: c.result_key,
            round: c.round,
        })
    }

    fn compute(&self, msg: &TbMsg) -> Result<f64, String> {
        let mut values = self.fields.iter().map(|f| {
            msg.data
                .get(f)
                .and_then(|v| v.as_f64())
                .ok_or_else(|| format!("field {f} is missing or not numeric"))
        });
        let first = values.next().expect("fields non-empty")?;
        let mut acc = first;
        for v in values {
            let v = v?;
            acc = match self.operation {
                MathOp::Add => acc + v,
                MathOp::Sub => acc - v,
                MathOp::Mult => acc * v,
                MathOp::Div => {
                    if v == 0.0 {
                        return Err("division by zero".into());
                    }
                    acc / v
                }
                MathOp::Min => acc.min(v),
                MathOp::Max => acc.max(v),
            };
        }
        if let Some(digits) = self.round {
            let factor = 10f64.powi(digits as i32);
            acc = (acc * factor).round() / factor;
        }
        Ok(acc)
    }
}

#[async_trait]
impl Node for MathNode {
    async fn on_msg(&self, _ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        match self.compute(&msg) {
            Ok(value) => {
                let Some(obj) = msg.data.as_object_mut() else {
                    tracing::warn!("math node requires msg.data to be a JSON object");
                    return vec![("Failure".into(), msg)];
                };
                let value = if value.fract() == 0.0 && value.abs() < 9.0e15 {
                    serde_json::json!(value as i64)
                } else {
                    serde_json::Number::from_f64(value)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                };
                obj.insert(self.result_key.clone(), value);
                vec![("Success".into(), msg)]
            }
            Err(e) => {
                tracing::warn!("math node failed: {e}");
                vec![("Failure".into(), msg)]
            }
        }
    }
}

/// action.rpc_oneway：向 originator 设备发下行 RPC（经 tb_core 由 core 路由，oneway 不等响应）。
pub struct RpcOnewayNode {
    method: String,
    params_script: Option<String>,
}

impl RpcOnewayNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            method: String,
            params_script: Option<String>,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            method: c.method,
            params_script: c.params_script,
        })
    }
}

#[async_trait]
impl Node for RpcOnewayNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rel = match self.send(ctx, &msg).await {
            Ok(()) => "Success",
            Err(e) => {
                tracing::warn!("rpc_oneway failed: {e}");
                "Failure"
            }
        };
        vec![(rel.into(), msg)]
    }
}

impl RpcOnewayNode {
    async fn send(&self, ctx: &NodeContext, msg: &TbMsg) -> TbResult<()> {
        if msg.originator.entity_type != EntityType::Device {
            return Err(TbError::InvalidInput(format!(
                "rpc_oneway requires DEVICE originator, got {}",
                msg.originator.entity_type
            )));
        }
        let params = match &self.params_script {
            Some(script) => ctx
                .script_engine
                .eval_map(script, &msg.data, &msg.metadata, &msg.msg_type)
                .map_err(|e| TbError::InvalidInput(format!("params script: {e}")))?,
            None => msg.data.clone(),
        };
        let device_id = msg.originator.id.to_string();
        let to_core = core_pb::ToCoreMsg {
            tenant_id: msg.tenant_id.to_string(),
            entity_id: device_id.clone(),
            msg: Some(core_pb::to_core_msg::Msg::RpcRequest(core_pb::RpcRequestMsg {
                device_id: device_id.clone(),
                method: self.method.clone(),
                params_json: params.to_string(),
                request_id: String::new(),
            })),
        };
        ctx.kafka_producer
            .send(
                TbTopic::CORE,
                &device_id,
                Bytes::from(to_core.encode_to_vec()),
            )
            .await
            .map_err(|e| TbError::Internal(format!("kafka send: {e}")))
    }
}
