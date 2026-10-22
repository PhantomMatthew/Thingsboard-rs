//! ts_kv/attribute_kv 值列（bool_v/str_v/long_v/dbl_v/json_v）与 KvValue 的互转。

use tb_common::KvValue;

/// (bool_v, str_v, long_v, dbl_v, json_v)，恰好一个为 Some。
#[derive(Debug, Clone, Default)]
pub struct ValueColumns {
    pub bool_v: Option<bool>,
    pub str_v: Option<String>,
    pub long_v: Option<i64>,
    pub dbl_v: Option<f64>,
    pub json_v: Option<serde_json::Value>,
}

pub fn split_value(value: &KvValue) -> ValueColumns {
    let mut cols = ValueColumns::default();
    match value {
        KvValue::Bool(v) => cols.bool_v = Some(*v),
        KvValue::Str(v) => cols.str_v = Some(v.clone()),
        KvValue::Long(v) => cols.long_v = Some(*v),
        KvValue::Double(v) => cols.dbl_v = Some(*v),
        KvValue::Json(v) => cols.json_v = Some(v.clone()),
    }
    cols
}

/// 五列恰好一个非空时还原 KvValue；全空返回 None。
pub fn join_value(cols: &ValueColumns) -> Option<KvValue> {
    if let Some(v) = cols.bool_v {
        return Some(KvValue::Bool(v));
    }
    if let Some(v) = cols.long_v {
        return Some(KvValue::Long(v));
    }
    if let Some(v) = cols.dbl_v {
        return Some(KvValue::Double(v));
    }
    if let Some(v) = &cols.str_v {
        return Some(KvValue::Str(v.clone()));
    }
    if let Some(v) = &cols.json_v {
        return Some(KvValue::Json(v.clone()));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn value_columns_roundtrip() {
        for v in [
            KvValue::Bool(true),
            KvValue::Long(-7),
            KvValue::Double(1.5),
            KvValue::Str("s".into()),
            KvValue::Json(json!({"a": 1})),
        ] {
            assert_eq!(join_value(&split_value(&v)), Some(v));
        }
        assert_eq!(join_value(&ValueColumns::default()), None);
    }
}
