use std::collections::HashSet;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use snmp2::Oid;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid transport configuration JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing transportConfiguration")]
    MissingTransportConfiguration,
    #[error("invalid field `{field}`: {message}")]
    InvalidField { field: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceTransportConfiguration {
    pub host: String,
    pub port: u16,
    pub protocol_version: String,
    pub community: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawDeviceTransportConfiguration {
    host: String,
    port: u16,
    protocol_version: String,
    community: String,
}

impl DeviceTransportConfiguration {
    pub fn from_device_data(device_data: &Value) -> Result<Self, ConfigError> {
        let value = transport_configuration(device_data)?;
        Self::from_transport_configuration(value)
    }

    pub fn from_transport_configuration(value: &Value) -> Result<Self, ConfigError> {
        let raw: RawDeviceTransportConfiguration = serde_json::from_value(value.clone())?;
        if raw.host.trim().is_empty() {
            return Err(invalid("host", "must not be empty"));
        }
        if raw.port == 0 {
            return Err(invalid("port", "must be greater than zero"));
        }
        if raw.protocol_version != "V2C" {
            return Err(invalid("protocolVersion", "only V2C is supported"));
        }
        if raw.community.is_empty() {
            return Err(invalid("community", "must not be empty"));
        }
        Ok(Self {
            host: raw.host,
            port: raw.port,
            protocol_version: raw.protocol_version,
            community: raw.community,
        })
    }

    pub fn address(&self) -> String {
        if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuerySpec {
    Telemetry,
    ClientAttributes,
}

impl QuerySpec {
    fn parse(value: &str) -> Result<Self, ConfigError> {
        match value {
            "TELEMETRY_QUERYING" => Ok(Self::Telemetry),
            "CLIENT_ATTRIBUTES_QUERYING" => Ok(Self::ClientAttributes),
            _ => Err(invalid(
                "communicationConfigs.spec",
                "expected TELEMETRY_QUERYING or CLIENT_ATTRIBUTES_QUERYING",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Long,
    Double,
    Boolean,
    String,
    Json,
}

impl DataType {
    fn parse(value: &str) -> Result<Self, ConfigError> {
        match value {
            "LONG" => Ok(Self::Long),
            "DOUBLE" => Ok(Self::Double),
            "BOOLEAN" => Ok(Self::Boolean),
            "STRING" => Ok(Self::String),
            "JSON" => Ok(Self::Json),
            _ => Err(invalid(
                "communicationConfigs.mappings.dataType",
                "expected LONG, DOUBLE, BOOLEAN, STRING, or JSON",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Mapping {
    pub oid: String,
    pub key: String,
    pub data_type: DataType,
    pub(crate) parsed_oid: Oid<'static>,
}

#[derive(Debug, Clone)]
pub struct CommunicationConfig {
    pub spec: QuerySpec,
    pub querying_frequency: Duration,
    pub mappings: Vec<Mapping>,
}

#[derive(Debug, Clone)]
pub struct ProfileTransportConfiguration {
    pub timeout: Duration,
    pub retries: u32,
    pub communication_configs: Vec<CommunicationConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawProfileTransportConfiguration {
    timeout_ms: u64,
    retries: u32,
    communication_configs: Vec<RawCommunicationConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawCommunicationConfig {
    spec: String,
    querying_frequency_ms: u64,
    mappings: Vec<RawMapping>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMapping {
    oid: String,
    key: String,
    data_type: String,
}

impl ProfileTransportConfiguration {
    pub fn from_profile_data(profile_data: &Value) -> Result<Self, ConfigError> {
        let value = transport_configuration(profile_data)?;
        Self::from_transport_configuration(value)
    }

    pub fn from_transport_configuration(value: &Value) -> Result<Self, ConfigError> {
        let raw: RawProfileTransportConfiguration = serde_json::from_value(value.clone())?;
        if raw.timeout_ms == 0 {
            return Err(invalid("timeoutMs", "must be greater than zero"));
        }
        if raw.communication_configs.is_empty() {
            return Err(invalid("communicationConfigs", "must not be empty"));
        }

        let mut communication_configs = Vec::with_capacity(raw.communication_configs.len());
        for communication in raw.communication_configs {
            if communication.querying_frequency_ms == 0 {
                return Err(invalid(
                    "communicationConfigs.queryingFrequencyMs",
                    "must be greater than zero",
                ));
            }
            if communication.mappings.is_empty() {
                return Err(invalid(
                    "communicationConfigs.mappings",
                    "must not be empty",
                ));
            }
            let spec = QuerySpec::parse(&communication.spec)?;
            let mut oids = HashSet::new();
            let mut keys = HashSet::new();
            let mut mappings = Vec::with_capacity(communication.mappings.len());
            for mapping in communication.mappings {
                if mapping.key.trim().is_empty() {
                    return Err(invalid(
                        "communicationConfigs.mappings.key",
                        "must not be empty",
                    ));
                }
                let (oid, parsed_oid) = parse_oid(&mapping.oid)?;
                if !oids.insert(oid.clone()) {
                    return Err(invalid(
                        "communicationConfigs.mappings.oid",
                        format!("duplicate OID {oid}"),
                    ));
                }
                if !keys.insert(mapping.key.clone()) {
                    return Err(invalid(
                        "communicationConfigs.mappings.key",
                        format!("duplicate key {}", mapping.key),
                    ));
                }
                mappings.push(Mapping {
                    oid,
                    key: mapping.key,
                    data_type: DataType::parse(&mapping.data_type)?,
                    parsed_oid,
                });
            }
            communication_configs.push(CommunicationConfig {
                spec,
                querying_frequency: Duration::from_millis(communication.querying_frequency_ms),
                mappings,
            });
        }

        Ok(Self {
            timeout: Duration::from_millis(raw.timeout_ms),
            retries: raw.retries,
            communication_configs,
        })
    }
}

fn transport_configuration(value: &Value) -> Result<&Value, ConfigError> {
    value
        .get("transportConfiguration")
        .ok_or(ConfigError::MissingTransportConfiguration)
}

fn invalid(field: &str, message: impl Into<String>) -> ConfigError {
    ConfigError::InvalidField {
        field: field.into(),
        message: message.into(),
    }
}

fn parse_oid(input: &str) -> Result<(String, Oid<'static>), ConfigError> {
    if input.is_empty() || input.trim() != input {
        return Err(invalid("communicationConfigs.mappings.oid", "invalid OID"));
    }
    let arcs = input
        .split('.')
        .map(|arc| {
            if arc.is_empty() || !arc.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("communicationConfigs.mappings.oid", "invalid OID"));
            }
            arc.parse::<u64>().map_err(|_| {
                invalid(
                    "communicationConfigs.mappings.oid",
                    "OID arc is out of range",
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if arcs.len() < 2 || arcs[0] > 2 || (arcs[0] < 2 && arcs.get(1).is_none_or(|arc| *arc > 39)) {
        return Err(invalid("communicationConfigs.mappings.oid", "invalid OID"));
    }
    let oid = Oid::from(arcs.as_slice())
        .map_err(|_| invalid("communicationConfigs.mappings.oid", "invalid OID"))?;
    let normalized = arcs
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(".");
    Ok((normalized, oid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn device_data() -> Value {
        json!({"transportConfiguration": {
            "host": "127.0.0.1", "port": 1161,
            "protocolVersion": "V2C", "community": "public"
        }})
    }

    fn profile_data() -> Value {
        json!({"transportConfiguration": {
            "timeoutMs": 250,
            "retries": 2,
            "communicationConfigs": [{
                "spec": "TELEMETRY_QUERYING",
                "queryingFrequencyMs": 1000,
                "mappings": [
                    {"oid": "1.3.6.1.2.1.1.3.0", "key": "uptime", "dataType": "LONG"},
                    {"oid": "1.3.6.1.2.1.1.1.0", "key": "description", "dataType": "STRING"}
                ]
            }]
        }})
    }

    #[test]
    fn parses_java_compatible_configuration() {
        let device = DeviceTransportConfiguration::from_device_data(&device_data()).unwrap();
        let profile = ProfileTransportConfiguration::from_profile_data(&profile_data()).unwrap();
        assert_eq!(device.address(), "127.0.0.1:1161");
        assert_eq!(profile.timeout, Duration::from_millis(250));
        assert_eq!(profile.retries, 2);
        assert_eq!(profile.communication_configs[0].spec, QuerySpec::Telemetry);
        assert_eq!(profile.communication_configs[0].mappings.len(), 2);
    }

    #[test]
    fn rejects_unsupported_versions_and_types() {
        let mut device = device_data();
        device["transportConfiguration"]["protocolVersion"] = json!("V3");
        assert!(DeviceTransportConfiguration::from_device_data(&device).is_err());

        let mut profile = profile_data();
        profile["transportConfiguration"]["communicationConfigs"][0]["mappings"][0]["dataType"] =
            json!("INTEGER");
        assert!(ProfileTransportConfiguration::from_profile_data(&profile).is_err());
    }

    #[test]
    fn rejects_invalid_oid_frequency_and_duplicates() {
        for oid in ["", "1", "1.40.1", "3.1", "1.3.a", " 1.3.6"] {
            let mut profile = profile_data();
            profile["transportConfiguration"]["communicationConfigs"][0]["mappings"][0]["oid"] =
                json!(oid);
            assert!(
                ProfileTransportConfiguration::from_profile_data(&profile).is_err(),
                "OID {oid:?} should be rejected"
            );
        }

        let mut profile = profile_data();
        profile["transportConfiguration"]["communicationConfigs"][0]["queryingFrequencyMs"] =
            json!(0);
        assert!(ProfileTransportConfiguration::from_profile_data(&profile).is_err());

        let mut profile = profile_data();
        profile["transportConfiguration"]["communicationConfigs"][0]["mappings"][1]["oid"] =
            json!("01.3.6.1.2.1.1.3.0");
        assert!(ProfileTransportConfiguration::from_profile_data(&profile).is_err());
    }
}
