//! 首期 8 个节点（msg_type_switch/script/save/alarm/log/checkpoint）。

use async_trait::async_trait;
use serde::Deserialize;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{AttributeScope, EntityId, EntityType, KvEntry, KvValue, TbError, TbResult, TsKvEntry};
use tb_dao::models::{Alarm, AlarmSeverity, AlarmStatus};
use uuid::Uuid;

use crate::context::NodeContext;
use crate::node::Node;

pub(crate) fn json_to_kv(v: serde_json::Value) -> KvValue {
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

pub(crate) fn parse_scope(s: &str) -> TbResult<AttributeScope> {
    match s {
        "CLIENT_SCOPE" => Ok(AttributeScope::ClientScope),
        "SERVER_SCOPE" => Ok(AttributeScope::ServerScope),
        "SHARED_SCOPE" => Ok(AttributeScope::SharedScope),
        other => Err(TbError::InvalidInput(format!(
            "unknown attribute scope: {other}"
        ))),
    }
}

pub(crate) fn parse_config<T: for<'de> Deserialize<'de>>(config: &serde_json::Value) -> TbResult<T> {
    serde_json::from_value(config.clone())
        .map_err(|e| TbError::InvalidInput(format!("bad node configuration: {e}")))
}

/// filter.msg_type_switch：按消息类型路由，relation label = msg 类型字符串。
pub struct MsgTypeSwitchNode;

#[async_trait]
impl Node for MsgTypeSwitchNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        vec![(msg.msg_type.clone(), msg)]
    }
}

/// filter.script：Rhai 返回 true→"True"，false→"False"，错误→"Failure"。
pub struct ScriptFilterNode {
    script: String,
}

impl ScriptFilterNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            script: String,
        }
        Ok(Self {
            script: parse_config::<Config>(config)?.script,
        })
    }
}

#[async_trait]
impl Node for ScriptFilterNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rel = match ctx
            .script_engine
            .eval_filter(&self.script, &msg.data, &msg.metadata, &msg.msg_type)
        {
            Ok(true) => "True",
            Ok(false) => "False",
            Err(e) => {
                tracing::warn!(script_error = %e, "script filter eval failed");
                "Failure"
            }
        };
        vec![(rel.to_string(), msg)]
    }
}

/// transform.script：改写 msg/metadata/msgType，成功→"Success"，错误→"Failure"。
pub struct ScriptTransformNode {
    script: String,
}

impl ScriptTransformNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            script: String,
        }
        Ok(Self {
            script: parse_config::<Config>(config)?.script,
        })
    }
}

#[async_trait]
impl Node for ScriptTransformNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        match ctx
            .script_engine
            .eval_transform(&self.script, &msg.data, &msg.metadata, &msg.msg_type)
        {
            Ok(out) => {
                let mut msg = msg;
                msg.data = out.msg;
                msg.metadata = out.metadata;
                msg.msg_type = out.msg_type;
                vec![("Success".to_string(), msg)]
            }
            Err(e) => {
                tracing::warn!(script_error = %e, "script transform eval failed");
                vec![("Failure".to_string(), msg)]
            }
        }
    }
}

/// action.save_timeseries：msg.data 为 {ts, values:{k:v}} 或扁平 {k:v}（ts 取 msg.ts）。
/// 写 ts_kv + ts_kv_latest。default_ttl 仅为配置兼容保留（本库 ts_kv 无 TTL 列）。
#[derive(Default)]
pub struct SaveTimeseriesNode {
    #[allow(dead_code)]
    default_ttl: Option<i64>,
}

impl SaveTimeseriesNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize, Default)]
        struct Config {
            default_ttl: Option<i64>,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            default_ttl: c.default_ttl,
        })
    }
}

#[async_trait]
impl Node for SaveTimeseriesNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rel = match save_timeseries(ctx, &msg).await {
            Ok(()) => "Success",
            Err(e) => {
                tracing::warn!("save timeseries failed: {e}");
                "Failure"
            }
        };
        vec![(rel.to_string(), msg)]
    }
}

async fn save_timeseries(ctx: &NodeContext, msg: &TbMsg) -> TbResult<()> {
    let (ts, values) = match &msg.data {
        serde_json::Value::Object(map) if map.contains_key("values") => {
            let ts = msg.data["ts"].as_i64().unwrap_or(msg.ts);
            let values = msg.data["values"].as_object().cloned().unwrap_or_default();
            (ts, values)
        }
        serde_json::Value::Object(map) => (msg.ts, map.clone()),
        other => {
            return Err(TbError::InvalidInput(format!(
                "timeseries data must be a JSON object, got {other}"
            )));
        }
    };
    let entries: Vec<TsKvEntry> = values
        .into_iter()
        .map(|(key, v)| TsKvEntry {
            ts,
            key,
            value: json_to_kv(v),
        })
        .collect();
    ctx.timeseries_dao.save(msg.originator.id, &entries).await
}

/// action.save_attributes：config {scope: 默认 CLIENT_SCOPE}；
/// msg.data 为 {k:v} 或 {scope, kv:{k:v}}（后者 scope 以 data 为准）。写 attribute_kv。
pub struct SaveAttributesNode {
    scope: String,
}

impl Default for SaveAttributesNode {
    fn default() -> Self {
        Self {
            scope: "CLIENT_SCOPE".into(),
        }
    }
}

impl SaveAttributesNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize, Default)]
        struct Config {
            scope: Option<String>,
        }
        let c: Config = parse_config(config)?;
        let scope = c.scope.unwrap_or_else(|| "CLIENT_SCOPE".into());
        parse_scope(&scope)?;
        Ok(Self { scope })
    }
}

#[async_trait]
impl Node for SaveAttributesNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rel = match self.save(ctx, &msg).await {
            Ok(()) => "Success",
            Err(e) => {
                tracing::warn!("save attributes failed: {e}");
                "Failure"
            }
        };
        vec![(rel.to_string(), msg)]
    }
}

impl SaveAttributesNode {
    async fn save(&self, ctx: &NodeContext, msg: &TbMsg) -> TbResult<()> {
        let (scope, kv) = match &msg.data {
            serde_json::Value::Object(map) if map.contains_key("kv") => {
                let scope = match map.get("scope").and_then(|s| s.as_str()) {
                    Some(s) => parse_scope(s)?,
                    None => parse_scope(&self.scope)?,
                };
                let kv = msg.data["kv"].as_object().cloned().unwrap_or_default();
                (scope, kv)
            }
            serde_json::Value::Object(map) => (parse_scope(&self.scope)?, map.clone()),
            other => {
                return Err(TbError::InvalidInput(format!(
                    "attributes data must be a JSON object, got {other}"
                )));
            }
        };
        let entries: Vec<KvEntry> = kv
            .into_iter()
            .map(|(key, v)| KvEntry {
                key,
                value: json_to_kv(v),
            })
            .collect();
        ctx.attributes_dao
            .save(msg.originator.id, scope, &entries)
            .await
    }
}

/// action.create_alarm：originator+type 的最新告警仍 ACTIVE 则更新 end_ts/details（"Updated"），
/// 否则新建 ACTIVE_UNACK 告警（"Created"）。产出 ALARM 类型新消息，data 为告警 JSON。
pub struct CreateAlarmNode {
    alarm_type: String,
    severity: AlarmSeverity,
    propagate: bool,
    details_script: Option<String>,
}

impl CreateAlarmNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            alarm_type: String,
            severity: AlarmSeverity,
            #[serde(default)]
            propagate: bool,
            details_script: Option<String>,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            alarm_type: c.alarm_type,
            severity: c.severity,
            propagate: c.propagate,
            details_script: c.details_script,
        })
    }
}

#[async_trait]
impl Node for CreateAlarmNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        match self.process(ctx, &msg).await {
            Ok((rel, alarm)) => {
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
                vec![(rel, alarm_msg)]
            }
            Err(e) => {
                tracing::warn!("create alarm failed: {e}");
                vec![("Failure".to_string(), msg)]
            }
        }
    }
}

impl CreateAlarmNode {
    async fn process(&self, ctx: &NodeContext, msg: &TbMsg) -> TbResult<(String, Alarm)> {
        let details = match &self.details_script {
            Some(script) => Some(
                ctx.script_engine
                    .eval_map(script, &msg.data, &msg.metadata, &msg.msg_type)
                    .map_err(|e| TbError::InvalidInput(format!("details script: {e}")))?,
            ),
            None => None,
        };
        let now = chrono::Utc::now().timestamp_millis();
        let existing = ctx
            .alarm_dao
            .find_latest_by_originator_and_type(msg.originator.id, &self.alarm_type)
            .await?;
        let active = existing.as_ref().is_some_and(|a| {
            matches!(
                a.status,
                AlarmStatus::ActiveUnack | AlarmStatus::ActiveAck
            )
        });
        if active {
            let mut alarm = existing.expect("checked above");
            alarm.end_ts = now;
            alarm.severity = self.severity;
            alarm.propagate = self.propagate;
            if let Some(d) = details {
                alarm.details = Some(d);
            }
            let alarm = ctx.alarm_dao.save(&alarm).await?;
            Ok(("Updated".to_string(), alarm))
        } else {
            let alarm = Alarm {
                id: Uuid::now_v7(),
                created_time: 0,
                tenant_id: msg.tenant_id,
                originator_id: msg.originator.id,
                originator_type: msg.originator.entity_type,
                r#type: self.alarm_type.clone(),
                severity: self.severity,
                status: AlarmStatus::ActiveUnack,
                start_ts: now,
                end_ts: now,
                ack_ts: None,
                clear_ts: None,
                details,
                propagate: self.propagate,
                propagate_to_owner: false,
                propagate_to_tenant: false,
                propagate_relation_types: None,
                customer_id: msg.customer_id,
                assignee_id: None,
                assign_ts: None,
            };
            let alarm = ctx.alarm_dao.save(&alarm).await?;
            Ok(("Created".to_string(), alarm))
        }
    }
}

/// action.log：tracing::info 输出消息内容，透传→"Success"。
pub struct LogNode {
    prefix: String,
}

impl LogNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize, Default)]
        struct Config {
            #[serde(default)]
            prefix: String,
        }
        Ok(Self {
            prefix: parse_config::<Config>(config)?.prefix,
        })
    }
}

#[async_trait]
impl Node for LogNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        tracing::info!(
            prefix = %self.prefix,
            msg_id = %msg.id,
            msg_type = %msg.msg_type,
            originator = %msg.originator,
            data = %msg.data,
            "rule chain log"
        );
        vec![("Success".to_string(), msg)]
    }
}

/// flow.checkpoint：节点本身透传 "Success"；requeue 语义由 chain::execute 特判实现
/// （消息携带 rule_chain_id + 下一跳 rule_node_id 发回 kafka 队列，不再本地转发）。
pub struct CheckpointNode;

#[async_trait]
impl Node for CheckpointNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        vec![("Success".to_string(), msg)]
    }
}
