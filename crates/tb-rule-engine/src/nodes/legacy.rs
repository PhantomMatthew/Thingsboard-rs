//! 首期 8 个节点（msg_type_switch/script/save/alarm/log/checkpoint）。

use async_trait::async_trait;
use prost::Message as _;
use serde::Deserialize;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{
    AttributeScope, EntityId, EntityType, KvEntry, KvValue, TbError, TbResult, TsKvEntry,
};
use tb_dao::models::{Alarm, AlarmSeverity, AlarmStatus};
use tb_proto::common as common_pb;
use tb_proto::edqs as edqs_pb;
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

pub(crate) fn parse_config<T: for<'de> Deserialize<'de>>(
    config: &serde_json::Value,
) -> TbResult<T> {
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
        let rel = match ctx.script_engine.eval_filter(
            &self.script,
            &msg.data,
            &msg.metadata,
            &msg.msg_type,
        ) {
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
        match ctx.script_engine.eval_transform(
            &self.script,
            &msg.data,
            &msg.metadata,
            &msg.msg_type,
        ) {
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

/// EDQS 读模型事件：fire-and-forget，失败仅 warn。
fn publish_edqs(ctx: &NodeContext, tenant_id: Uuid, event: edqs_pb::EdqsEventMsg) {
    let Some(producer) = ctx.edqs_producer.clone() else {
        return;
    };
    tokio::spawn(async move {
        if let Err(e) = tb_queue::edqs::publish_event(&producer, tenant_id, &event).await {
            tracing::warn!("edqs event publish failed: {e}");
        }
    });
}

/// 计算字段异步评估（输出落库 + EDQS 由 tb_calculated 负责）；失败仅 warn，不阻塞规则链。
fn spawn_calculated(ctx: &NodeContext, tenant_id: Uuid, entity_id: Uuid, trigger: Vec<KvEntry>) {
    if trigger.is_empty() {
        return;
    }
    let pool = ctx.pool.clone();
    let producer = ctx.edqs_producer.clone();
    tokio::spawn(async move {
        tb_calculated::evaluate_and_save(&pool, producer.as_ref(), tenant_id, entity_id, &trigger)
            .await;
    });
}

fn kv_proto(key: &str, value: &KvValue) -> common_pb::KeyValueProto {
    tb_proto::convert::kv_to_proto_named(0, key, value)
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

pub(crate) async fn save_timeseries(ctx: &NodeContext, msg: &TbMsg) -> TbResult<()> {
    let (ts, values) = match &msg.data {
        serde_json::Value::Object(map)
            if map.get("values").is_some_and(serde_json::Value::is_object) =>
        {
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
    ctx.timeseries_dao.save(msg.originator.id, &entries).await?;
    if !entries.is_empty() {
        let usage = tb_proto::core::UsageStatsMsg {
            tenant_id: msg.tenant_id.to_string(),
            transport_msgs: entries.len() as i64,
            re_msgs: 0,
        };
        if let Err(e) = ctx
            .kafka_producer
            .send(
                tb_queue::TbTopic::USAGE_STATS,
                &msg.tenant_id.to_string(),
                bytes::Bytes::from(usage.encode_to_vec()),
            )
            .await
        {
            tracing::warn!(tenant_id = %msg.tenant_id, "usage stats publish failed: {e}");
        }
    }
    crate::telemetry_cache::update_latest(ctx, msg.originator.id, &entries).await;
    spawn_calculated(
        ctx,
        msg.tenant_id,
        msg.originator.id,
        entries
            .iter()
            .map(|e| KvEntry {
                key: e.key.clone(),
                value: e.value.clone(),
            })
            .collect(),
    );
    publish_edqs(
        ctx,
        msg.tenant_id,
        edqs_pb::EdqsEventMsg {
            tenant_id: msg.tenant_id.to_string(),
            event: Some(edqs_pb::edqs_event_msg::Event::LatestTelemetry(
                edqs_pb::LatestTelemetryMsg {
                    entity_id: msg.originator.id.to_string(),
                    values: entries.iter().map(|e| kv_proto(&e.key, &e.value)).collect(),
                    ts: entries.iter().map(|e| e.ts).collect(),
                },
            )),
        },
    );
    Ok(())
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
    pub(crate) async fn save(&self, ctx: &NodeContext, msg: &TbMsg) -> TbResult<()> {
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
            .await?;
        spawn_calculated(ctx, msg.tenant_id, msg.originator.id, entries.clone());
        publish_edqs(
            ctx,
            msg.tenant_id,
            edqs_pb::EdqsEventMsg {
                tenant_id: msg.tenant_id.to_string(),
                event: Some(edqs_pb::edqs_event_msg::Event::AttributeUpdate(
                    edqs_pb::AttributeUpdateMsg {
                        entity_id: msg.originator.id.to_string(),
                        scope: common_pb::AttributeScope::from(scope) as i32,
                        updated: entries.iter().map(|e| kv_proto(&e.key, &e.value)).collect(),
                        deleted_keys: vec![],
                        update_ts: chrono::Utc::now().timestamp_millis(),
                    },
                )),
            },
        );
        Ok(())
    }
}

/// action.create_alarm：ACTIVE 告警原子更新，否则创建 ACTIVE_UNACK。
/// 部分唯一索引上的 ON CONFLICT 避免并发检查后再写入产生两条 ACTIVE。
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
            #[serde(alias = "alarmType")]
            alarm_type: String,
            severity: AlarmSeverity,
            #[serde(default)]
            propagate: bool,
            #[serde(alias = "detailsScript")]
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
                    rule_chain_id: None,
                    rule_node_id: None,
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
        let new_id = Uuid::now_v7();
        let alarm = Alarm {
            id: new_id,
            created_time: now,
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
        let saved = sqlx::query_as::<_, Alarm>(
            "INSERT INTO alarm (id, created_time, tenant_id, originator_id, originator_type, \
             type, severity, status, start_ts, end_ts, ack_ts, clear_ts, details, propagate, \
             propagate_to_owner, propagate_to_tenant, propagate_relation_types, customer_id, \
             assignee_id, assign_ts) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) \
             ON CONFLICT (originator_id, type) \
             WHERE status IN ('ACTIVE_UNACK','ACTIVE_ACK') DO UPDATE SET \
             end_ts=EXCLUDED.end_ts, severity=EXCLUDED.severity, \
             details=COALESCE(EXCLUDED.details, alarm.details), propagate=EXCLUDED.propagate \
             RETURNING alarm.*",
        )
        .bind(alarm.id)
        .bind(alarm.created_time)
        .bind(alarm.tenant_id)
        .bind(alarm.originator_id)
        .bind(alarm.originator_type.as_str())
        .bind(&alarm.r#type)
        .bind(alarm.severity)
        .bind(alarm.status)
        .bind(alarm.start_ts)
        .bind(alarm.end_ts)
        .bind(alarm.ack_ts)
        .bind(alarm.clear_ts)
        .bind(&alarm.details)
        .bind(alarm.propagate)
        .bind(alarm.propagate_to_owner)
        .bind(alarm.propagate_to_tenant)
        .bind(&alarm.propagate_relation_types)
        .bind(alarm.customer_id)
        .bind(alarm.assignee_id)
        .bind(alarm.assign_ts)
        .fetch_one(&ctx.pool)
        .await
        .map_err(TbError::db)?;
        let relation = if saved.id == new_id {
            "Created"
        } else {
            "Updated"
        };
        Ok((relation.to_string(), saved))
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
