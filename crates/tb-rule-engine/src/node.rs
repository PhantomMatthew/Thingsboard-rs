//! 规则节点抽象。节点不返回错误：失败语义通过 "Failure"/"False" 等关系边表达。

use async_trait::async_trait;
use tb_common::msg::TbMsg;

use crate::context::NodeContext;

/// 处理一条消息，返回 (relation_type, msg) 列表驱动下游路由。
#[async_trait]
pub trait Node: Send + Sync {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)>;
}
