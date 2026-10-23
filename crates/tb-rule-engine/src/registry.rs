//! 节点类型注册表：类型字符串 → 工厂。

use std::sync::Arc;

use tb_common::{TbError, TbResult};

use crate::node::Node;
use crate::nodes::{
    ChangeOriginatorNode, CheckpointNode, ClearAlarmNode, CreateAlarmNode, DeduplicationNode,
    DelayNode, DeleteAttributesNode, DeleteKeysNode, GeoFilterNode, KafkaNode, KeyExistsFilterNode,
    LogNode, MathNode, MqttNode, MsgTypeFilterNode, MsgTypeSwitchNode, OriginatorAttributesNode,
    OriginatorFieldsNode, OriginatorTypeFilterNode, RelatedAttributesNode, RelationFilterNode,
    RenameKeysNode, RestCallNode, RpcOnewayNode, RuleChainNode, SaveAttributesNode,
    SaveTimeseriesNode, ScriptFilterNode, ScriptTransformNode, TenantCustomerFieldsNode,
};

pub const MSG_TYPE_SWITCH: &str = "filter.msg_type_switch";
pub const SCRIPT_FILTER: &str = "filter.script";
pub const SCRIPT_TRANSFORM: &str = "transform.script";
pub const SAVE_TIMESERIES: &str = "action.save_timeseries";
pub const SAVE_ATTRIBUTES: &str = "action.save_attributes";
pub const CREATE_ALARM: &str = "action.create_alarm";
pub const LOG: &str = "action.log";
pub const CHECKPOINT: &str = "flow.checkpoint";
pub const ORIGINATOR_ATTRIBUTES: &str = "enrichment.originator_attributes";
pub const ORIGINATOR_FIELDS: &str = "enrichment.originator_fields";
pub const RELATED_ATTRIBUTES: &str = "enrichment.related_attributes";
pub const TENANT_CUSTOMER_FIELDS: &str = "enrichment.tenant_customer_fields";
pub const MSG_TYPE_FILTER: &str = "filter.msg_type";
pub const ORIGINATOR_TYPE_FILTER: &str = "filter.originator_type";
pub const KEY_EXISTS_FILTER: &str = "filter.key_exists";
pub const RELATION_FILTER: &str = "filter.relation";
pub const GEO_FILTER: &str = "filter.geo";
pub const RENAME_KEYS: &str = "transform.rename_keys";
pub const DELETE_KEYS: &str = "transform.delete_keys";
pub const CHANGE_ORIGINATOR: &str = "transform.change_originator";
pub const CLEAR_ALARM: &str = "action.clear_alarm";
pub const DELETE_ATTRIBUTES: &str = "action.delete_attributes";
pub const DELAY: &str = "action.delay";
pub const DEDUPLICATION: &str = "action.deduplication";
pub const MATH: &str = "action.math";
pub const RPC_ONEWAY: &str = "action.rpc_oneway";
pub const REST_CALL: &str = "external.rest_call";
pub const KAFKA: &str = "external.kafka";
pub const MQTT: &str = "external.mqtt";
pub const RULE_CHAIN: &str = "flow.rule_chain";

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
        ORIGINATOR_ATTRIBUTES,
        ORIGINATOR_FIELDS,
        RELATED_ATTRIBUTES,
        TENANT_CUSTOMER_FIELDS,
        MSG_TYPE_FILTER,
        ORIGINATOR_TYPE_FILTER,
        KEY_EXISTS_FILTER,
        RELATION_FILTER,
        GEO_FILTER,
        RENAME_KEYS,
        DELETE_KEYS,
        CHANGE_ORIGINATOR,
        CLEAR_ALARM,
        DELETE_ATTRIBUTES,
        DELAY,
        DEDUPLICATION,
        MATH,
        RPC_ONEWAY,
        REST_CALL,
        KAFKA,
        MQTT,
        RULE_CHAIN,
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
        ORIGINATOR_ATTRIBUTES => Arc::new(OriginatorAttributesNode::new(config)?),
        ORIGINATOR_FIELDS => Arc::new(OriginatorFieldsNode::new(config)?),
        RELATED_ATTRIBUTES => Arc::new(RelatedAttributesNode::new(config)?),
        TENANT_CUSTOMER_FIELDS => Arc::new(TenantCustomerFieldsNode::new(config)?),
        MSG_TYPE_FILTER => Arc::new(MsgTypeFilterNode::new(config)?),
        ORIGINATOR_TYPE_FILTER => Arc::new(OriginatorTypeFilterNode::new(config)?),
        KEY_EXISTS_FILTER => Arc::new(KeyExistsFilterNode::new(config)?),
        RELATION_FILTER => Arc::new(RelationFilterNode::new(config)?),
        GEO_FILTER => Arc::new(GeoFilterNode::new(config)?),
        RENAME_KEYS => Arc::new(RenameKeysNode::new(config)?),
        DELETE_KEYS => Arc::new(DeleteKeysNode::new(config)?),
        CHANGE_ORIGINATOR => Arc::new(ChangeOriginatorNode::new(config)?),
        CLEAR_ALARM => Arc::new(ClearAlarmNode::new(config)?),
        DELETE_ATTRIBUTES => Arc::new(DeleteAttributesNode::new(config)?),
        DELAY => Arc::new(DelayNode::new(config)?),
        DEDUPLICATION => Arc::new(DeduplicationNode::new(config)?),
        MATH => Arc::new(MathNode::new(config)?),
        RPC_ONEWAY => Arc::new(RpcOnewayNode::new(config)?),
        REST_CALL => Arc::new(RestCallNode::new(config)?),
        KAFKA => Arc::new(KafkaNode::new(config)?),
        MQTT => Arc::new(MqttNode::new(config)?),
        RULE_CHAIN => Arc::new(RuleChainNode::new(config)?),
        other => {
            return Err(TbError::InvalidInput(format!(
                "unknown rule node type: {other}"
            )));
        }
    };
    Ok(node)
}
