//! 规则链状态构建与执行：BFS 遍历，relation 边路由，100 跳环路保护。

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use sqlx::PgPool;
use tb_common::msg::TbMsg;
use tb_common::{EntityType, TbResult};
use tb_dao::models::RuleNode;
use tb_dao::{RelationDao, RuleChainDao, RuleNodeDao};
use uuid::Uuid;

use crate::context::NodeContext;
use crate::node::Node;
use crate::registry;

const MAX_HOPS: usize = 100;

pub struct RuleChainState {
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
            nodes,
            edges,
            first_node_id: chain.first_rule_node_id,
        }))
    }
}

/// 从 start_node 起 BFS 执行：节点产出按 relation 匹配边转发，无匹配边丢弃。
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
        let outs = node.on_msg(ctx, msg).await;
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
                tracing::debug!(
                    node = %rule_node.name,
                    relation = %rel,
                    "no outgoing edge, message dropped"
                );
            }
            for to in matched {
                queue.push_back((to, m.clone()));
            }
        }
    }
}
