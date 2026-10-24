//! tb-rule-engine：规则引擎核心库。
//! 节点（Node）消费 TbMsg 产出 (relation_type, TbMsg) 列表，chain 按边路由，engine 负责
//! 租户根链解析与缓存。

pub mod chain;
pub mod context;
pub mod engine;
pub mod node;
pub mod nodes;
pub mod registry;
pub mod seed;
mod telemetry_cache;

pub use chain::{ChainLoader, RuleChainState};
pub use context::NodeContext;
pub use engine::RuleEngineService;
pub use node::Node;
pub use registry::known_node_types;
