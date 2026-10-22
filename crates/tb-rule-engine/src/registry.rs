//! 节点类型注册表：类型字符串 → 工厂。

use std::sync::Arc;

use tb_common::{TbError, TbResult};

use crate::node::Node;
use crate::nodes::{
    CheckpointNode, CreateAlarmNode, LogNode, MsgTypeSwitchNode, SaveAttributesNode,
    SaveTimeseriesNode, ScriptFilterNode, ScriptTransformNode,
};

pub const MSG_TYPE_SWITCH: &str = "filter.msg_type_switch";
pub const SCRIPT_FILTER: &str = "filter.script";
pub const SCRIPT_TRANSFORM: &str = "transform.script";
pub const SAVE_TIMESERIES: &str = "action.save_timeseries";
pub const SAVE_ATTRIBUTES: &str = "action.save_attributes";
pub const CREATE_ALARM: &str = "action.create_alarm";
pub const LOG: &str = "action.log";
pub const CHECKPOINT: &str = "flow.checkpoint";

/// REST 层校验用的已知节点类型清单。
pub fn known_node_types() -> &'static [&'static str] {
    &[
        MSG_TYPE_SWITCH,
        SCRIPT_FILTER,
        SCRIPT_TRANSFORM,
        SAVE_TIMESERIES,
        SAVE_ATTRIBUTES,
        CREATE_ALARM,
        LOG,
        CHECKPOINT,
    ]
}

pub fn create_node(node_type: &str, config: &serde_json::Value) -> TbResult<Arc<dyn Node>> {
    let node: Arc<dyn Node> = match node_type {
        MSG_TYPE_SWITCH => Arc::new(MsgTypeSwitchNode),
        SCRIPT_FILTER => Arc::new(ScriptFilterNode::new(config)?),
        SCRIPT_TRANSFORM => Arc::new(ScriptTransformNode::new(config)?),
        SAVE_TIMESERIES => Arc::new(SaveTimeseriesNode::new(config)?),
        SAVE_ATTRIBUTES => Arc::new(SaveAttributesNode::new(config)?),
        CREATE_ALARM => Arc::new(CreateAlarmNode::new(config)?),
        LOG => Arc::new(LogNode::new(config)?),
        CHECKPOINT => Arc::new(CheckpointNode),
        other => {
            return Err(TbError::InvalidInput(format!(
                "unknown rule node type: {other}"
            )));
        }
    };
    Ok(node)
}
