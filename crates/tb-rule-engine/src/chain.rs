//! 规则链状态构建与执行：BFS 遍历，relation 边路由，100 跳环路保护。
//! 附加语义：
//! - debug_mode 节点：on_msg 前后各写一条 rule_node_debug_event（IN/OUT，panic 记 error 列）。
//! - DLQ："Failure" relation 无匹配边（含 on_msg panic 合成的 Failure）时发 `tb_rule_engine.dlq`，
//!   metadata 加 error/fromNodeId/chainId。其余 relation 无边匹配维持原有丢弃语义
//!   （否则默认链终端节点的 "Success" 会刷屏 DLQ）。
//! - flow.checkpoint：节点的 "Success" 输出不在本地转发，而是携带 rule_chain_id/下一跳
//!   rule_node_id requeue 回 `tb_rule_engine.<queue>`，由消费循环从该节点恢复执行。

use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use futures::FutureExt;
use prost::Message as _;
use sqlx::PgPool;
use tb_common::msg::TbMsg;
use tb_common::{EntityType, TbResult};
use tb_dao::models::RuleNode;
use tb_dao::{RelationDao, RuleChainDao, RuleNodeDao, RuleNodeDebugEvent};
use tb_proto::convert::tb_msg_to_proto;
use tb_proto::ruleengine::ToRuleEngineMsg;
use tb_queue::{QueueProducer, TbTopic};
use uuid::Uuid;

use crate::context::NodeContext;
use crate::node::Node;
use crate::registry;

const MAX_HOPS: usize = 100;

pub struct RuleChainState {
    pub chain_id: Uuid,
    pub nodes: HashMap<Uuid, (RuleNode, Arc<dyn Node>)>,
    /// from_node_id → [(relation_type, to_node_id)]
    pub edges: HashMap<Uuid, Vec<(String, Uuid)>>,
    pub first_node_id: Option<Uuid>,
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
            let node = registry::create_node(&rn.r#type, &config)?;
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

/// 发 DLQ：metadata 追加 error/fromNodeId/chainId。
async fn send_to_dlq(
    ctx: &NodeContext,
    state: &RuleChainState,
    node: &RuleNode,
    mut msg: TbMsg,
    error: String,
) {
    msg.metadata.insert("error".into(), error);
    msg.metadata.insert("fromNodeId".into(), node.id.to_string());
    msg.metadata.insert("chainId".into(), state.chain_id.to_string());
    let envelope = ToRuleEngineMsg {
        tenant_id: msg.tenant_id.to_string(),
        tb_msg: Some(tb_msg_to_proto(&msg)),
    };
    let key = msg.originator.id.to_string();
    if let Err(e) = ctx
        .kafka_producer
        .send(
            TbTopic::RULE_ENGINE_DLQ,
            &key,
            Bytes::from(envelope.encode_to_vec()),
        )
        .await
    {
        tracing::error!("send to DLQ failed: {e}");
    }
}

/// checkpoint requeue：携带 rule_chain_id + 下一跳 rule_node_id 发回原队列。
async fn requeue(ctx: &NodeContext, state: &RuleChainState, next_node: Uuid, msg: TbMsg) {
    let mut proto = tb_msg_to_proto(&msg);
    proto.rule_chain_id = state.chain_id.to_string();
    proto.rule_node_id = next_node.to_string();
    let envelope = ToRuleEngineMsg {
        tenant_id: msg.tenant_id.to_string(),
        tb_msg: Some(proto),
    };
    let topic = TbTopic::rule_engine(&msg.queue_name);
    let key = msg.originator.id.to_string();
    if let Err(e) = ctx
        .kafka_producer
        .send(&topic, &key, Bytes::from(envelope.encode_to_vec()))
        .await
    {
        tracing::error!("checkpoint requeue failed: {e}");
    }
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

/// 从 start_node 起 BFS 执行：节点产出按 relation 匹配边转发。
pub async fn execute(ctx: &NodeContext, state: &RuleChainState, start_node: Uuid, msg: TbMsg) {
    let mut queue = VecDeque::from([(start_node, msg)]);
    let mut hops = 0usize;
    while let Some((node_id, msg)) = queue.pop_front() {
        hops += 1;
        if hops > MAX_HOPS {
            tracing::warn!("rule chain exceeded {MAX_HOPS} hops, message dropped (loop?)");
            break;
        }
        let Some((rule_node, node)) = state.nodes.get(&node_id) else {
            tracing::debug!(%node_id, "edge points to unknown node, dropped");
            continue;
        };
        let debug = rule_node.debug_mode;
        if debug {
            save_debug_event(ctx, state, node_id, &msg, "IN", None).await;
        }
        let result = AssertUnwindSafe(node.on_msg(ctx, msg.clone()))
            .catch_unwind()
            .await;
        let outs = match result {
            Ok(outs) => {
                if debug {
                    // OUT 事件记录首个输出（无输出时记录原消息）。
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
        let is_checkpoint = rule_node.r#type == registry::CHECKPOINT;
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
                if rel == "Failure" {
                    let error = format!("no outgoing edge for relation 'Failure' at node '{}'", rule_node.name);
                    send_to_dlq(ctx, state, rule_node, m, error).await;
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
                    requeue(ctx, state, to, m.clone()).await;
                } else {
                    queue.push_back((to, m.clone()));
                }
            }
        }
    }
}
