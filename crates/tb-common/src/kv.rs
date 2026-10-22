use serde::{Deserialize, Serialize};

/// 遥测/属性的通用值类型（对应 ts_kv/attribute_kv 的 *_v 列）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KvValue {
    Bool(bool),
    Long(i64),
    Double(f64),
    Str(String),
    Json(serde_json::Value),
}

impl KvValue {
    pub fn is_numeric(&self) -> bool {
        matches!(self, Self::Long(_) | Self::Double(_))
    }
}

/// 一条带 key 的值。key_id 为 key_dictionary interning 结果；
/// 写入前、读出后两个方向都会用到（DAO 负责 interning/反查）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvEntry {
    pub key: String,
    pub value: KvValue,
}

/// 带时间戳的遥测点。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TsKvEntry {
    pub ts: i64, // epoch millis
    pub key: String,
    pub value: KvValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AttributeScope {
    ClientScope,
    ServerScope,
    SharedScope,
}

impl AttributeScope {
    pub fn as_i16(&self) -> i16 {
        match self {
            Self::ClientScope => 0,
            Self::ServerScope => 1,
            Self::SharedScope => 2,
        }
    }

    pub fn from_i16(v: i16) -> Option<Self> {
        match v {
            0 => Some(Self::ClientScope),
            1 => Some(Self::ServerScope),
            2 => Some(Self::SharedScope),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_roundtrip() {
        for s in [
            AttributeScope::ClientScope,
            AttributeScope::ServerScope,
            AttributeScope::SharedScope,
        ] {
            assert_eq!(AttributeScope::from_i16(s.as_i16()), Some(s));
        }
    }

    #[test]
    fn kv_value_json_roundtrip() {
        let v = KvValue::Long(42);
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, "42");
        let back: KvValue = serde_json::from_str(&s).unwrap();
        assert_eq!(back, v);
    }
}
