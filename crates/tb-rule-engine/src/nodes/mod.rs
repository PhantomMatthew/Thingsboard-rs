//! 内置规则节点。config 均从 rule_node.configuration JSON 反序列化。
//! relation 约定：成功 "Success"/失败 "Failure"，filter 类 "True"/"False"。

mod action;
mod enrichment;
mod external;
mod filter;
mod flow;
mod legacy;
mod transform;

pub use action::{
    ClearAlarmNode, DeduplicationNode, DelayNode, DeleteAttributesNode, MathNode, RpcOnewayNode,
};
pub use enrichment::{
    OriginatorAttributesNode, OriginatorFieldsNode, RelatedAttributesNode, TenantCustomerFieldsNode,
};
pub use external::{KafkaNode, MqttNode, RestCallNode};
pub use filter::{
    GeoFilterNode, KeyExistsFilterNode, MsgTypeFilterNode, OriginatorTypeFilterNode,
    RelationFilterNode,
};
pub use flow::RuleChainNode;
pub use legacy::{
    CheckpointNode, CreateAlarmNode, LogNode, MsgTypeSwitchNode, SaveAttributesNode,
    SaveTimeseriesNode, ScriptFilterNode, ScriptTransformNode,
};
pub use transform::{ChangeOriginatorNode, DeleteKeysNode, RenameKeysNode};

pub(crate) use legacy::{parse_config, parse_scope};

use tb_common::KvValue;

/// KvValue → JSON（属性查询结果合并进 msg.data/metadata 用）。
pub(crate) fn kv_to_json(v: &KvValue) -> serde_json::Value {
    match v {
        KvValue::Bool(b) => serde_json::Value::Bool(*b),
        KvValue::Long(l) => serde_json::json!(*l),
        KvValue::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        KvValue::Str(s) => serde_json::Value::String(s.clone()),
        KvValue::Json(j) => j.clone(),
    }
}

/// 把属性合并进 msg：target="msg" 时要求 data 为 object；target="metadata" 时值转字符串。
pub(crate) fn merge_attrs(
    msg: &mut tb_common::msg::TbMsg,
    target: &str,
    prefix: Option<&str>,
    attrs: Vec<(String, serde_json::Value)>,
) -> Result<(), String> {
    let prefix = prefix.unwrap_or_default();
    match target {
        "msg" => {
            let Some(obj) = msg.data.as_object_mut() else {
                return Err("target=msg requires msg.data to be a JSON object".into());
            };
            for (k, v) in attrs {
                obj.insert(format!("{prefix}{k}"), v);
            }
            Ok(())
        }
        "metadata" => {
            for (k, v) in attrs {
                let s = match &v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                msg.metadata.insert(format!("{prefix}{k}"), s);
            }
            Ok(())
        }
        other => Err(format!("unknown target: {other}")),
    }
}
