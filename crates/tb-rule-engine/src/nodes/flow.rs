//! flow.*：链级流转节点。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use tb_common::TbResult;
use tb_common::msg::TbMsg;
use uuid::Uuid;

use crate::chain::{self, ChainLoader, RuleChainState};
use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::parse_config;

/// 链调用深度上限（flow.rule_chain 嵌套防环）。
pub const MAX_CHAIN_DEPTH: usize = 8;

/// metadata 中携带链调用深度的 key（flow.rule_chain 内部使用，随目标链消息传播）。
pub const CHAIN_DEPTH_KEY: &str = "_chainDepth";

/// flow.rule_chain：加载目标链从其 first_node 执行，消息被目标链吸收（不再转发）。
/// 目标链不存在/无 first_node → "Success" 边（用于错误处理）；
/// 深度超限/加载错误 → "Failure"。
/// 注：这与 Java 版 output 语义不同——Java 会把目标链输出回流到本节点下游，这里简化掉了。
pub struct RuleChainNode {
    rule_chain_id: Uuid,
    cache: moka::future::Cache<Uuid, Option<Arc<RuleChainState>>>,
}

impl RuleChainNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            rule_chain_id: Uuid,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            rule_chain_id: c.rule_chain_id,
            cache: moka::future::Cache::builder()
                .time_to_live(Duration::from_secs(30))
                .build(),
        })
    }
}

#[async_trait]
impl Node for RuleChainNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        let depth: usize = msg
            .metadata
            .get(CHAIN_DEPTH_KEY)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if depth >= MAX_CHAIN_DEPTH {
            tracing::warn!(
                chain_id = %self.rule_chain_id,
                "rule chain call depth exceeded {MAX_CHAIN_DEPTH}, message dropped to Failure"
            );
            return vec![("Failure".into(), msg)];
        }
        let state = if let Some(cached) = self.cache.get(&self.rule_chain_id).await {
            cached
        } else {
            let loaded = match ChainLoader::new(ctx.pool.clone())
                .load(self.rule_chain_id)
                .await
            {
                Ok(s) => s.map(Arc::new),
                Err(e) => {
                    tracing::warn!("rule chain load failed: {e}");
                    return vec![("Failure".into(), msg)];
                }
            };
            self.cache.insert(self.rule_chain_id, loaded.clone()).await;
            loaded
        };
        let Some(state) = state else {
            tracing::warn!(chain_id = %self.rule_chain_id, "target rule chain not found");
            return vec![("Success".into(), msg)];
        };
        let Some(first) = state.first_node_id else {
            tracing::warn!(chain_id = %self.rule_chain_id, "target rule chain has no first node");
            return vec![("Success".into(), msg)];
        };
        msg.metadata
            .insert(CHAIN_DEPTH_KEY.into(), (depth + 1).to_string());
        let failure_msg = msg.clone();
        if let Err(e) = chain::execute(ctx, &state, first, msg).await {
            tracing::warn!(chain_id = %self.rule_chain_id, "target rule chain execution failed: {e}");
            return vec![("Failure".into(), failure_msg)];
        }
        vec![]
    }
}
