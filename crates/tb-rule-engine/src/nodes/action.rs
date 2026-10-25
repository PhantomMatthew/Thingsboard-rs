//! action.*：副作用节点（告警清除/属性删除/延迟/去重/计算/下行 RPC）。

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message as _;
use serde::Deserialize;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{EntityId, EntityType, TbError, TbResult};
use tb_dao::models::{Alarm, AlarmStatus};
use tb_proto::common as common_pb;
use tb_proto::core as core_pb;
use tb_proto::edqs as edqs_pb;
use tb_queue::TbTopic;
use uuid::Uuid;

use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::legacy::publish_edqs;
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
                    rule_chain_id: None,
                    rule_node_id: None,
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
            Ok(deleted) => {
                if deleted > 0 {
                    publish_edqs(
                        ctx,
                        msg.tenant_id,
                        edqs_pb::EdqsEventMsg {
                            tenant_id: msg.tenant_id.to_string(),
                            event: Some(edqs_pb::edqs_event_msg::Event::AttributeUpdate(
                                edqs_pb::AttributeUpdateMsg {
                                    entity_id: msg.originator.id.to_string(),
                                    scope: common_pb::AttributeScope::from(scope) as i32,
                                    updated: vec![],
                                    deleted_keys: self.keys.clone(),
                                    update_ts: chrono::Utc::now().timestamp_millis(),
                                },
                            )),
                        },
                    );
                }
                "Success"
            }
            Err(e) => {
                tracing::warn!("delete attributes failed: {e}");
                "Failure"
            }
        };
        vec![(rel.into(), msg)]
    }
}

/// action.delay：立即产出 Success，由 chain 将下一跳安排为独立延时重投任务。
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

    pub(crate) fn delay(&self) -> Duration {
        self.delay
    }
}

#[async_trait]
impl Node for DelayNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        vec![("Success".into(), msg)]
    }
}

/// action.deduplication：按 originator 持久化窗口消息，到期后由共享扫描器输出 FIRST/LAST/ALL。
pub struct DeduplicationNode {
    interval: Duration,
    strategy: tb_dao::DedupStrategy,
    out_msg_type: Option<String>,
    max_pending_msgs: i32,
    max_retries: i32,
}

impl DeduplicationNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Config {
            #[serde(default = "default_interval", alias = "window_sec")]
            interval: u64,
            #[serde(default)]
            strategy: Option<tb_dao::DedupStrategy>,
            #[serde(default)]
            out_msg_type: Option<String>,
            #[serde(default = "default_max_pending")]
            max_pending_msgs: i32,
            #[serde(default = "default_max_retries")]
            max_retries: i32,
        }
        fn default_interval() -> u64 {
            60
        }
        fn default_max_pending() -> i32 {
            100
        }
        fn default_max_retries() -> i32 {
            3
        }
        let c: Config = parse_config(config)?;
        if c.interval == 0 || c.max_pending_msgs <= 0 || c.max_retries < 0 {
            return Err(TbError::InvalidInput(
                "deduplication interval, maxPendingMsgs and maxRetries are out of range".into(),
            ));
        }
        let strategy = c.strategy.unwrap_or(tb_dao::DedupStrategy::First);
        if strategy == tb_dao::DedupStrategy::All
            && c.out_msg_type.as_deref().is_none_or(str::is_empty)
        {
            return Err(TbError::InvalidInput(
                "outMsgType is required for ALL deduplication".into(),
            ));
        }
        Ok(Self {
            interval: Duration::from_secs(c.interval),
            strategy,
            out_msg_type: c.out_msg_type,
            max_pending_msgs: c.max_pending_msgs,
            max_retries: c.max_retries,
        })
    }
}

#[async_trait]
impl Node for DeduplicationNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        match ctx
            .dedup_dao
            .append(
                &msg,
                self.interval.as_millis() as i64,
                self.strategy,
                self.out_msg_type.as_deref().unwrap_or(""),
                i64::from(self.max_pending_msgs),
                self.max_retries,
            )
            .await
        {
            Ok(tb_dao::AppendResult::Buffered | tb_dao::AppendResult::Duplicate) => Vec::new(),
            Ok(tb_dao::AppendResult::LimitExceeded) => {
                vec![("Failure".into(), msg)]
            }
            Err(e) => {
                tracing::warn!(error = %e, "persist deduplication message failed");
                vec![("Failure".into(), msg)]
            }
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
            msg: Some(core_pb::to_core_msg::Msg::RpcRequest(
                core_pb::RpcRequestMsg {
                    device_id: device_id.clone(),
                    method: self.method.clone(),
                    params_json: params.to_string(),
                    request_id: String::new(),
                },
            )),
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
