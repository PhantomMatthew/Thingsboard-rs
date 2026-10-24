use base64::Engine as _;
use tb_common::{KvValue, TbError, TbResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceType {
    Boolean,
    Integer,
    Float,
    String,
    Opaque,
    ObjectLink,
    Time,
}

pub fn decode_value(resource_type: ResourceType, raw: &[u8]) -> TbResult<KvValue> {
    match resource_type {
        ResourceType::Boolean => match raw {
            [0] | b"0" | b"false" => Ok(KvValue::Bool(false)),
            [1] | b"1" | b"true" => Ok(KvValue::Bool(true)),
            _ => Err(invalid("invalid LwM2M boolean")),
        },
        ResourceType::Integer => {
            let value = match raw.len() {
                1 => i64::from(raw[0] as i8),
                2 => i64::from(i16::from_be_bytes(raw.try_into().unwrap())),
                4 => i64::from(i32::from_be_bytes(raw.try_into().unwrap())),
                8 => i64::from_be_bytes(raw.try_into().unwrap()),
                _ => return Err(invalid("LwM2M integer must use 1, 2, 4 or 8 bytes")),
            };
            Ok(KvValue::Long(value))
        }
        ResourceType::Float => match raw.len() {
            4 => {
                let value = f32::from_be_bytes(raw.try_into().unwrap());
                finite_double(f64::from(value))
            }
            8 => finite_double(f64::from_be_bytes(raw.try_into().unwrap())),
            _ => Err(invalid("LwM2M float must use 4 or 8 bytes")),
        },
        ResourceType::String => Ok(KvValue::Str(
            std::str::from_utf8(raw)
                .map_err(|_| invalid("LwM2M string is not UTF-8"))?
                .to_string(),
        )),
        ResourceType::Opaque => Ok(KvValue::Str(
            base64::engine::general_purpose::STANDARD.encode(raw),
        )),
        ResourceType::ObjectLink => {
            if raw.len() != 4 {
                return Err(invalid("LwM2M object link must use 4 bytes"));
            }
            let object = u16::from_be_bytes([raw[0], raw[1]]);
            let instance = u16::from_be_bytes([raw[2], raw[3]]);
            Ok(KvValue::Str(format!("{object}:{instance}")))
        }
        ResourceType::Time => match decode_value(ResourceType::Integer, raw)? {
            KvValue::Long(seconds) => seconds
                .checked_mul(1000)
                .map(KvValue::Long)
                .ok_or_else(|| invalid("LwM2M time overflows milliseconds")),
            _ => unreachable!(),
        },
    }
}

fn finite_double(value: f64) -> TbResult<KvValue> {
    if value.is_finite() {
        Ok(KvValue::Double(value))
    } else {
        Err(invalid("LwM2M float is not finite"))
    }
}

fn invalid(message: impl Into<String>) -> TbError {
    TbError::InvalidInput(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_types_decode() {
        assert_eq!(
            decode_value(ResourceType::Boolean, b"1").unwrap(),
            KvValue::Bool(true)
        );
        assert_eq!(
            decode_value(ResourceType::Integer, &(-2_i16).to_be_bytes()).unwrap(),
            KvValue::Long(-2)
        );
        assert_eq!(
            decode_value(ResourceType::Float, &1.5_f32.to_be_bytes()).unwrap(),
            KvValue::Double(1.5)
        );
        assert_eq!(
            decode_value(ResourceType::Time, &2_u64.to_be_bytes()).unwrap(),
            KvValue::Long(2000)
        );
        assert_eq!(
            decode_value(ResourceType::ObjectLink, &[0, 3, 0, 7]).unwrap(),
            KvValue::Str("3:7".into())
        );
    }

    #[test]
    fn invalid_values_are_rejected() {
        assert!(decode_value(ResourceType::Boolean, b"yes").is_err());
        assert!(decode_value(ResourceType::Integer, &[1, 2, 3]).is_err());
        assert!(decode_value(ResourceType::Float, &f32::NAN.to_be_bytes()).is_err());
        assert!(decode_value(ResourceType::ObjectLink, &[0]).is_err());
    }
}
