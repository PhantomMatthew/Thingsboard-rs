//! tb-common 类型 ↔ proto 类型的转换。

use crate::common as pb;
use tb_common::kv::{AttributeScope, KvValue};

impl From<AttributeScope> for pb::AttributeScope {
    fn from(s: AttributeScope) -> Self {
        match s {
            AttributeScope::ClientScope => pb::AttributeScope::ClientScope,
            AttributeScope::ServerScope => pb::AttributeScope::ServerScope,
            AttributeScope::SharedScope => pb::AttributeScope::SharedScope,
        }
    }
}

impl TryFrom<pb::AttributeScope> for AttributeScope {
    type Error = ();

    fn try_from(s: pb::AttributeScope) -> Result<Self, Self::Error> {
        match s {
            pb::AttributeScope::ClientScope => Ok(AttributeScope::ClientScope),
            pb::AttributeScope::ServerScope => Ok(AttributeScope::ServerScope),
            pb::AttributeScope::SharedScope => Ok(AttributeScope::SharedScope),
            pb::AttributeScope::ScopeUnspecified => Err(()),
        }
    }
}

pub fn kv_to_proto(key_id: i32, value: &KvValue) -> pb::KeyValueProto {
    kv_to_proto_named(key_id, "", value)
}

/// 带 key 字符串版本：下行通知/查询响应需要设备可读 key。
pub fn kv_to_proto_named(key_id: i32, key: &str, value: &KvValue) -> pb::KeyValueProto {
    use pb::key_value_proto::Value;
    let v = match value {
        KvValue::Bool(b) => Value::BoolV(*b),
        KvValue::Str(s) => Value::StrV(s.clone()),
        KvValue::Long(l) => Value::LongV(*l),
        KvValue::Double(d) => Value::DblV(*d),
        KvValue::Json(j) => Value::JsonV(j.to_string()),
    };
    pb::KeyValueProto {
        key_id,
        key: key.to_string(),
        value: Some(v),
    }
}

pub fn kv_from_proto(kv: &pb::KeyValueProto, key: String) -> Option<(String, KvValue)> {
    use pb::key_value_proto::Value;
    let value = match kv.value.as_ref()? {
        Value::BoolV(b) => KvValue::Bool(*b),
        Value::StrV(s) => KvValue::Str(s.clone()),
        Value::LongV(l) => KvValue::Long(*l),
        Value::DblV(d) => KvValue::Double(*d),
        Value::JsonV(j) => KvValue::Json(serde_json::from_str(j).ok()?),
    };
    Some((key, value))
}

impl From<tb_common::EntityType> for pb::EntityType {
    fn from(t: tb_common::EntityType) -> Self {
        use tb_common::EntityType as T;
        match t {
            T::Tenant => Self::Tenant,
            T::Customer => Self::Customer,
            T::User => Self::User,
            T::Device => Self::Device,
            T::DeviceProfile => Self::DeviceProfile,
            T::Asset => Self::Asset,
            T::AssetProfile => Self::AssetProfile,
            T::Dashboard => Self::Dashboard,
            T::RuleChain => Self::RuleChain,
            T::RuleNode => Self::RuleNode,
            T::OtaPackage => Self::OtaPackage,
            T::Alarm => Self::Alarm,
            T::Edge => Self::Edge,
        }
    }
}

impl TryFrom<pb::EntityType> for tb_common::EntityType {
    type Error = tb_common::TbError;

    fn try_from(t: pb::EntityType) -> Result<Self, Self::Error> {
        use tb_common::EntityType as T;
        match t {
            pb::EntityType::Tenant => Ok(T::Tenant),
            pb::EntityType::Customer => Ok(T::Customer),
            pb::EntityType::User => Ok(T::User),
            pb::EntityType::Device => Ok(T::Device),
            pb::EntityType::DeviceProfile => Ok(T::DeviceProfile),
            pb::EntityType::Asset => Ok(T::Asset),
            pb::EntityType::AssetProfile => Ok(T::AssetProfile),
            pb::EntityType::Dashboard => Ok(T::Dashboard),
            pb::EntityType::RuleChain => Ok(T::RuleChain),
            pb::EntityType::RuleNode => Ok(T::RuleNode),
            pb::EntityType::OtaPackage => Ok(T::OtaPackage),
            pb::EntityType::Alarm => Ok(T::Alarm),
            pb::EntityType::Edge => Ok(T::Edge),
            pb::EntityType::Unspecified => Err(tb_common::TbError::InvalidInput(
                "unspecified entity type".into(),
            )),
        }
    }
}

fn parse_uuid(field: &str, s: &str) -> tb_common::TbResult<uuid::Uuid> {
    uuid::Uuid::parse_str(s)
        .map_err(|_| tb_common::TbError::InvalidInput(format!("bad {field}: {s}")))
}

/// proto → tb-common 消息；空 customer_id 视为 None。
pub fn tb_msg_from_proto(
    p: &crate::ruleengine::TbMsgProto,
) -> tb_common::TbResult<tb_common::msg::TbMsg> {
    let originator_type = pb::EntityType::try_from(p.originator_type)
        .map_err(|_| {
            tb_common::TbError::InvalidInput(format!("bad originator_type: {}", p.originator_type))
        })?
        .try_into()?;
    let customer_id = if p.customer_id.is_empty() {
        None
    } else {
        Some(parse_uuid("customer_id", &p.customer_id)?)
    };
    Ok(tb_common::msg::TbMsg {
        id: parse_uuid("msg id", &p.id)?,
        queue_name: p.queue_name.clone(),
        msg_type: p.r#type.clone(),
        originator: tb_common::EntityId {
            entity_type: originator_type,
            id: parse_uuid("originator_id", &p.originator_id)?,
        },
        tenant_id: parse_uuid("tenant_id", &p.tenant_id)?,
        customer_id,
        data: serde_json::from_str(&p.data_json)
            .map_err(|e| tb_common::TbError::InvalidInput(format!("bad data_json: {e}")))?,
        metadata: p.metadata.clone(),
        ts: p.ts,
    })
}

pub fn tb_msg_to_proto(msg: &tb_common::msg::TbMsg) -> crate::ruleengine::TbMsgProto {
    crate::ruleengine::TbMsgProto {
        id: msg.id.to_string(),
        queue_name: msg.queue_name.clone(),
        r#type: msg.msg_type.clone(),
        originator_id: msg.originator.id.to_string(),
        originator_type: pb::EntityType::from(msg.originator.entity_type) as i32,
        tenant_id: msg.tenant_id.to_string(),
        customer_id: msg.customer_id.map(|c| c.to_string()).unwrap_or_default(),
        data_json: msg.data.to_string(),
        metadata: msg.metadata.clone(),
        rule_chain_id: String::new(),
        rule_node_id: String::new(),
        ts: msg.ts,
    }
}
