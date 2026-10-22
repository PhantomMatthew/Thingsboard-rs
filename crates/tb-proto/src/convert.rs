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
