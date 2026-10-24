//! 规则链状态构建与执行：BFS 遍历、跨 Kafka 重投的全局跳数限制与 DLQ。
//! debug 节点记录 IN/OUT；Failure 无边、checkpoint 无 Success 边及环路截断均进入 DLQ。

use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use prost::Message as _;
use sqlx::PgPool;
use tb_common::msg::TbMsg;
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::RuleNode;
use tb_dao::{RelationDao, RuleChainDao, RuleNodeDao, RuleNodeDebugEvent};
use tb_proto::convert::tb_msg_to_proto;
use tb_proto::ruleengine::ToRuleEngineMsg;
use tb_queue::{QueueProducer, TbTopic};
use uuid::Uuid;

use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::DelayNode;
use crate::registry;

const MAX_HOPS: usize = 100;
const HOPS_KEY: &str = "_hops";

pub struct RuleChainState {
    pub chain_id: Uuid,
    pub nodes: HashMap<Uuid, (RuleNode, Arc<dyn Node>)>,
    /// from_node_id → [(relation_type, to_node_id)]
    pub edges: HashMap<Uuid, Vec<(String, Uuid)>>,
    pub first_node_id: Option<Uuid>,
}

struct ErrorPlaceholderNode;

#[async_trait]
impl Node for ErrorPlaceholderNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        vec![("Failure".into(), msg)]
    }
}

pub struct ChainLoader {
    pool: PgPool,
}

impl ChainLoader {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 读 rule_chain + rule_node + relation（from_type=RULE_NODE, group=RULE）构建执行状态。
    pub async fn load(&self, chain_id: Uuid) -> TbResult<Option<RuleChainState>> {
        let chain_dao = RuleChainDao::new(self.pool.clone());
        let Some(chain) = chain_dao.find_by_id(chain_id).await? else {
            return Ok(None);
        };
        let node_dao = RuleNodeDao::new(self.pool.clone());
        let relation_dao = RelationDao::new(self.pool.clone());
        let rule_nodes = node_dao.find_by_rule_chain_id(chain_id).await?;
        let mut nodes = HashMap::with_capacity(rule_nodes.len());
        let mut edges: HashMap<Uuid, Vec<(String, Uuid)>> = HashMap::new();
        for rn in rule_nodes {
            let config = rn.configuration.clone().unwrap_or(serde_json::json!({}));
            let node = match registry::create_node(&rn.r#type, &config) {
                Ok(node) => node,
                Err(e) => {
                    tracing::error!(
                        node_id = %rn.id,
                        node = %rn.name,
                        node_type = %rn.r#type,
                        error = %e,
                        "rule node initialization failed; using failure placeholder"
                    );
                    Arc::new(ErrorPlaceholderNode)
                }
            };
            for rel in relation_dao
                .find_by_from(rn.id, EntityType::RuleNode)
                .await?
            {
                if rel.relation_type_group == "RULE" {
                    edges
                        .entry(rn.id)
                        .or_default()
                        .push((rel.relation_type, rel.to_id));
                }
            }
            nodes.insert(rn.id, (rn, node));
        }
        Ok(Some(RuleChainState {
            chain_id,
            nodes,
            edges,
            first_node_id: chain.first_rule_node_id,
        }))
    }
}

async fn save_debug_event(
    ctx: &NodeContext,
    state: &RuleChainState,
    node_id: Uuid,
    msg: &TbMsg,
    event_type: &str,
    error: Option<String>,
) {
    let event = RuleNodeDebugEvent::new(
        msg.tenant_id,
        state.chain_id,
        node_id,
        msg.id,
        event_type,
        Some(msg.data.clone()),
        Some(serde_json::to_value(&msg.metadata).unwrap_or_default()),
        error,
    );
    if let Err(e) = ctx.debug_event_dao.save(&event).await {
        tracing::warn!("save debug event failed: {e}");
    }
}

pub(crate) async fn report_execution(ctx: &NodeContext, msg: &TbMsg) {
    let usage = tb_proto::core::UsageStatsMsg {
        tenant_id: msg.tenant_id.to_string(),
        transport_msgs: 0,
        re_msgs: 1,
    };
    if let Err(e) = ctx
        .kafka_producer
        .send(
            TbTopic::USAGE_STATS,
            &msg.tenant_id.to_string(),
            Bytes::from(usage.encode_to_vec()),
        )
        .await
    {
        tracing::warn!(tenant_id = %msg.tenant_id, "rule execution usage publish failed: {e}");
    }
}

pub(crate) async fn send_dlq(
    producer: &Arc<dyn QueueProducer>,
    chain_id: Uuid,
    from_node_id: Uuid,
    mut msg: TbMsg,
    error: String,
) -> TbResult<()> {
    msg.metadata.insert("error".into(), error);
    msg.metadata
        .insert("fromNodeId".into(), from_node_id.to_string());
    msg.metadata.insert("chainId".into(), chain_id.to_string());
    let envelope = ToRuleEngineMsg {
        tenant_id: msg.tenant_id.to_string(),
        tb_msg: Some(tb_msg_to_proto(&msg)),
    };
    let key = msg.originator.id.to_string();
    producer
        .send(
            TbTopic::RULE_ENGINE_DLQ,
            &key,
            Bytes::from(envelope.encode_to_vec()),
        )
        .await
        .map_err(|e| {
            tracing::error!("send to DLQ failed: {e}");
            TbError::internal(e)
        })
}

/// 经 TbMsg 自身的 checkpoint 字段重投到原规则队列。
pub(crate) async fn requeue(
    producer: &Arc<dyn QueueProducer>,
    chain_id: Uuid,
    next_node: Uuid,
    mut msg: TbMsg,
) -> TbResult<()> {
    msg.rule_chain_id = Some(chain_id);
    msg.rule_node_id = Some(next_node);
    let envelope = ToRuleEngineMsg {
        tenant_id: msg.tenant_id.to_string(),
        tb_msg: Some(tb_msg_to_proto(&msg)),
    };
    let topic = TbTopic::rule_engine(&msg.queue_name);
    let key = msg.originator.id.to_string();
    producer
        .send(&topic, &key, Bytes::from(envelope.encode_to_vec()))
        .await
        .map_err(|e| {
            tracing::error!("rule engine requeue failed: {e}");
            TbError::internal(e)
        })
}

async fn schedule_requeue(
    ctx: &NodeContext,
    next_node: Uuid,
    msg: TbMsg,
    delay: Duration,
) -> TbResult<()> {
    ctx.delay_dao.enqueue(&msg, next_node, delay, 3).await?;
    Ok(())
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".into()
    }
}

async fn route_outputs(
    ctx: &NodeContext,
    state: &RuleChainState,
    rule_node: &RuleNode,
    outs: Vec<(String, TbMsg)>,
    queue: &mut VecDeque<(Uuid, TbMsg)>,
) -> TbResult<()> {
    let node_id = rule_node.id;
    let is_checkpoint = rule_node.r#type == registry::CHECKPOINT;
    let delay = if rule_node.r#type == registry::DELAY {
        let config = rule_node
            .configuration
            .clone()
            .unwrap_or(serde_json::json!({}));
        DelayNode::new(&config).ok().map(|node| node.delay())
    } else {
        None
    };
    for (rel, m) in outs {
        let matched = state
            .edges
            .get(&node_id)
            .map(|edges| {
                edges
                    .iter()
                    .filter(|(rel_type, _)| rel_type == &rel)
                    .map(|(_, to)| *to)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if matched.is_empty() {
            if rel == "Failure" || (is_checkpoint && rel == "Success") {
                let error = format!(
                    "no outgoing edge for relation '{rel}' at node '{}'",
                    rule_node.name
                );
                send_dlq(&ctx.kafka_producer, state.chain_id, rule_node.id, m, error).await?;
            } else {
                tracing::debug!(
                    node = %rule_node.name,
                    relation = %rel,
                    "no outgoing edge, message dropped"
                );
            }
            continue;
        }
        for to in matched {
            if is_checkpoint && rel == "Success" {
                requeue(&ctx.kafka_producer, state.chain_id, to, m.clone()).await?;
            } else if rel == "Success"
                && let Some(delay) = delay
            {
                schedule_requeue(ctx, to, m.clone(), delay).await?;
            } else {
                queue.push_back((to, m.clone()));
            }
        }
    }
    Ok(())
}

/// 从已处理节点的指定关系继续路由，供持久化去重窗口恢复输出。
pub async fn execute_from_relation(
    ctx: &NodeContext,
    state: &RuleChainState,
    from_node: Uuid,
    relation: &str,
    msg: TbMsg,
) -> TbResult<()> {
    let Some((rule_node, _)) = state.nodes.get(&from_node) else {
        tracing::debug!(%from_node, "output source node not found, dropped");
        return Ok(());
    };
    let mut queue = VecDeque::new();
    route_outputs(
        ctx,
        state,
        rule_node,
        vec![(relation.to_string(), msg)],
        &mut queue,
    )
    .await?;
    while let Some((node_id, msg)) = queue.pop_front() {
        execute(ctx, state, node_id, msg).await?;
    }
    Ok(())
}

/// 从 start_node 起 BFS 执行；`_hops` 随 checkpoint/delay 重投跨消费累计。
pub async fn execute(
    ctx: &NodeContext,
    state: &RuleChainState,
    start_node: Uuid,
    msg: TbMsg,
) -> TbResult<()> {
    let mut hops = msg
        .metadata
        .get(HOPS_KEY)
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut queue = VecDeque::from([(start_node, msg)]);
    while let Some((node_id, mut msg)) = queue.pop_front() {
        let Some((rule_node, node)) = state.nodes.get(&node_id) else {
            tracing::debug!(%node_id, "edge points to unknown node, dropped");
            continue;
        };
        if hops >= MAX_HOPS {
            hops = hops.saturating_add(1);
            msg.metadata.insert(HOPS_KEY.into(), hops.to_string());
            let error = format!("maximum rule chain hops ({MAX_HOPS}) exceeded; possible loop");
            tracing::warn!(node = %rule_node.name, %msg.id, "{error}");
            return send_dlq(
                &ctx.kafka_producer,
                state.chain_id,
                rule_node.id,
                msg,
                error,
            )
            .await;
        }
        hops += 1;
        msg.metadata.insert(HOPS_KEY.into(), hops.to_string());
        msg.rule_chain_id = Some(state.chain_id);
        msg.rule_node_id = Some(node_id);
        let debug = rule_node.debug_mode;
        if debug {
            save_debug_event(ctx, state, node_id, &msg, "IN", None).await;
        }
        report_execution(ctx, &msg).await;
        let result = AssertUnwindSafe(node.on_msg(ctx, msg.clone()))
            .catch_unwind()
            .await;
        let outs = match result {
            Ok(outs) => {
                if debug {
                    let out_msg = outs.first().map(|(_, m)| m).unwrap_or(&msg);
                    save_debug_event(ctx, state, node_id, out_msg, "OUT", None).await;
                }
                outs
            }
            Err(payload) => {
                let error = format!("node panicked: {}", panic_message(&*payload));
                tracing::warn!(node = %rule_node.name, "{error}");
                if debug {
                    save_debug_event(ctx, state, node_id, &msg, "OUT", Some(error)).await;
                }
                vec![("Failure".to_string(), msg)]
            }
        };
        route_outputs(ctx, state, rule_node, outs, &mut queue).await?;
    }
    Ok(())
}
