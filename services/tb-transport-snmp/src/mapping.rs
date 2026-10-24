use snmp2::Value;
use tb_common::KvValue;

use crate::DataType;

pub(crate) fn convert(value: Value<'_>, data_type: DataType) -> Result<Option<KvValue>, String> {
    if matches!(
        value,
        Value::NoSuchObject | Value::NoSuchInstance | Value::EndOfMibView | Value::Null
    ) {
        return Ok(None);
    }
    let converted = match data_type {
        DataType::Long => KvValue::Long(as_i64(&value)?),
        DataType::Double => KvValue::Double(as_f64(&value)?),
        DataType::Boolean => KvValue::Bool(as_bool(&value)?),
        DataType::String => KvValue::Str(as_string(&value)?),
        DataType::Json => KvValue::Json(as_json(&value)?),
    };
    Ok(Some(converted))
}

fn as_i64(value: &Value<'_>) -> Result<i64, String> {
    match value {
        Value::Integer(value) => Ok(*value),
        Value::Counter32(value) | Value::Unsigned32(value) | Value::Timeticks(value) => {
            Ok(i64::from(*value))
        }
        Value::Counter64(value) => {
            i64::try_from(*value).map_err(|_| format!("Counter64 value {value} does not fit LONG"))
        }
        Value::Boolean(value) => Ok(i64::from(*value)),
        _ => Err(type_error(value, "LONG")),
    }
}

fn as_f64(value: &Value<'_>) -> Result<f64, String> {
    let converted = match value {
        Value::Integer(value) => *value as f64,
        Value::Counter32(value) | Value::Unsigned32(value) | Value::Timeticks(value) => {
            f64::from(*value)
        }
        Value::Counter64(value) => *value as f64,
        Value::Boolean(value) => f64::from(u8::from(*value)),
        _ => return Err(type_error(value, "DOUBLE")),
    };
    if converted.is_finite() {
        Ok(converted)
    } else {
        Err("non-finite DOUBLE value".into())
    }
}

fn as_bool(value: &Value<'_>) -> Result<bool, String> {
    match value {
        Value::Boolean(value) => Ok(*value),
        Value::Integer(0) => Ok(false),
        Value::Integer(1) => Ok(true),
        Value::OctetString(value) => match std::str::from_utf8(value) {
            Ok("true" | "1") => Ok(true),
            Ok("false" | "0") => Ok(false),
            _ => Err(type_error(value, "BOOLEAN")),
        },
        _ => Err(type_error(value, "BOOLEAN")),
    }
}

fn as_string(value: &Value<'_>) -> Result<String, String> {
    match value {
        Value::OctetString(value) | Value::Opaque(value) => std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| "octet string is not valid UTF-8".into()),
        Value::ObjectIdentifier(value) => Ok(value.to_string()),
        Value::IpAddress(value) => Ok(value.map(|octet| octet.to_string()).join(".")),
        Value::Integer(value) => Ok(value.to_string()),
        Value::Counter32(value) | Value::Unsigned32(value) | Value::Timeticks(value) => {
            Ok(value.to_string())
        }
        Value::Counter64(value) => Ok(value.to_string()),
        Value::Boolean(value) => Ok(value.to_string()),
        _ => Err(type_error(value, "STRING")),
    }
}

fn as_json(value: &Value<'_>) -> Result<serde_json::Value, String> {
    match value {
        Value::OctetString(value) => {
            let text =
                std::str::from_utf8(value).map_err(|_| "JSON octet string is not valid UTF-8")?;
            serde_json::from_str(text).map_err(|error| format!("invalid JSON value: {error}"))
        }
        Value::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        Value::Integer(value) => Ok(serde_json::Value::from(*value)),
        Value::Counter32(value) | Value::Unsigned32(value) | Value::Timeticks(value) => {
            Ok(serde_json::Value::from(*value))
        }
        Value::Counter64(value) => Ok(serde_json::Value::from(*value)),
        _ => Err(type_error(value, "JSON")),
    }
}

fn type_error(value: &impl std::fmt::Debug, target: &str) -> String {
    format!("cannot convert {value:?} to {target}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn converts_all_supported_data_types() {
        assert_eq!(
            convert(Value::Integer(42), DataType::Long).unwrap(),
            Some(KvValue::Long(42))
        );
        assert_eq!(
            convert(Value::Unsigned32(7), DataType::Double).unwrap(),
            Some(KvValue::Double(7.0))
        );
        assert_eq!(
            convert(Value::Integer(1), DataType::Boolean).unwrap(),
            Some(KvValue::Bool(true))
        );
        assert_eq!(
            convert(Value::OctetString(b"sensor"), DataType::String).unwrap(),
            Some(KvValue::Str("sensor".into()))
        );
        assert_eq!(
            convert(Value::OctetString(br#"{"ok":true}"#), DataType::Json).unwrap(),
            Some(KvValue::Json(json!({"ok": true})))
        );
    }

    #[test]
    fn missing_and_incompatible_values_do_not_become_data() {
        assert_eq!(convert(Value::NoSuchObject, DataType::Long).unwrap(), None);
        assert!(convert(Value::OctetString(b"not-json"), DataType::Json).is_err());
        assert!(convert(Value::Integer(2), DataType::Boolean).is_err());
        assert!(convert(Value::Counter64(u64::MAX), DataType::Long).is_err());
    }
}
