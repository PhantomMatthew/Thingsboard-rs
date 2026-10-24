use std::collections::HashMap;

use serde::Deserialize;
use tb_common::{TbError, TbResult};

use crate::codec::{self, Tlv};
use crate::model::Lwm2mPath;
use crate::types::{ResourceType, decode_value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Telemetry,
    Attribute,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObservedResource {
    pub path: Lwm2mPath,
    pub resource_type: ResourceType,
    pub key_name: String,
    pub kind: ValueKind,
}

#[derive(Deserialize)]
struct ProfileData {
    #[serde(rename = "observeAttr")]
    observe_attr: ObserveAttr,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObserveAttr {
    #[serde(default)]
    key_name: HashMap<String, String>,
    #[serde(default)]
    telemetry: Vec<String>,
    #[serde(default)]
    attribute: Vec<String>,
    observe_strategy: Option<String>,
}

pub fn resources_from_profile(
    profile_data_json: &str,
    resource_types: &HashMap<String, ResourceType>,
) -> TbResult<Vec<ObservedResource>> {
    let data: ProfileData = serde_json::from_str(profile_data_json)
        .map_err(|e| TbError::InvalidInput(format!("invalid LwM2M profile data: {e}")))?;
    if data
        .observe_attr
        .observe_strategy
        .as_deref()
        .is_some_and(|strategy| !strategy.eq_ignore_ascii_case("SINGLE"))
    {
        return Err(TbError::InvalidInput(
            "only SINGLE LwM2M observe strategy is supported".into(),
        ));
    }
    let telemetry = data.observe_attr.telemetry.iter().map(|path| {
        convert_resource(
            path,
            ValueKind::Telemetry,
            &data.observe_attr,
            resource_types,
        )
    });
    let attributes = data.observe_attr.attribute.iter().map(|path| {
        convert_resource(
            path,
            ValueKind::Attribute,
            &data.observe_attr,
            resource_types,
        )
    });
    telemetry.chain(attributes).collect()
}

fn convert_resource(
    raw_path: &str,
    kind: ValueKind,
    observe_attr: &ObserveAttr,
    resource_types: &HashMap<String, ResourceType>,
) -> TbResult<ObservedResource> {
    let path = Lwm2mPath::parse(raw_path)?;
    if path.instance_id.is_none() || path.resource_id.is_none() {
        return Err(TbError::InvalidInput(format!(
            "observeAttr resource must identify an object instance and resource: {raw_path}"
        )));
    }
    let canonical = canonical_path(&path);
    let key_name = observe_attr
        .key_name
        .get(raw_path)
        .or_else(|| observe_attr.key_name.get(&canonical))
        .filter(|key| !key.trim().is_empty())
        .cloned()
        .ok_or_else(|| {
            TbError::InvalidInput(format!("observeAttr keyName is missing for {raw_path}"))
        })?;
    let resource_type = resource_types
        .get(raw_path)
        .or_else(|| resource_types.get(&canonical))
        .copied()
        .ok_or_else(|| {
            TbError::InvalidInput(format!("LwM2M resource type is missing for {raw_path}"))
        })?;
    Ok(ObservedResource {
        path,
        resource_type,
        key_name,
        kind,
    })
}

fn canonical_path(path: &Lwm2mPath) -> String {
    let mut segments = vec![path.object_id.to_string()];
    if let Some(instance) = path.instance_id {
        segments.push(instance.to_string());
    }
    if let Some(resource) = path.resource_id {
        segments.push(resource.to_string());
    }
    if let Some(instance) = path.resource_instance_id {
        segments.push(instance.to_string());
    }
    format!("/{}", segments.join("/"))
}

pub fn decode_resource(
    resource: &ObservedResource,
    payload: &[u8],
) -> TbResult<tb_common::KvValue> {
    let items = codec::parse(payload)?;
    let resource_id = resource
        .path
        .resource_id
        .ok_or_else(|| TbError::InvalidInput("observed path has no resource id".into()))?;
    let raw = find_resource(
        &items,
        resource.path.instance_id,
        resource_id,
        resource.path.resource_instance_id,
    )?;
    decode_value(resource.resource_type, raw)
}

fn find_resource<'b>(
    items: &[Tlv<'b>],
    instance_id: Option<u16>,
    resource_id: u16,
    resource_instance_id: Option<u16>,
) -> TbResult<&'b [u8]> {
    for item in items {
        match item {
            Tlv::ObjectInstance { id, children }
                if instance_id.is_none() || instance_id == Some(*id) =>
            {
                return find_resource(children, None, resource_id, resource_instance_id);
            }
            Tlv::Resource { id, value } if *id == resource_id => return Ok(value),
            Tlv::MultipleResource { id, children } if *id == resource_id => {
                let mut matches = children.iter().filter_map(|child| match child {
                    Tlv::ResourceInstance { id, value }
                        if resource_instance_id.is_none_or(|expected| expected == *id) =>
                    {
                        Some(*value)
                    }
                    _ => None,
                });
                if let Some(first) = matches.next()
                    && matches.next().is_none()
                {
                    return Ok(first);
                }
                return Err(TbError::InvalidInput(
                    "multiple-resource TLV does not identify one resource instance".into(),
                ));
            }
            _ => {}
        }
    }
    Err(TbError::InvalidInput(format!(
        "TLV does not contain resource {resource_id}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tb_common::KvValue;

    #[test]
    fn thingsboard_profile_and_nested_tlv_are_decoded() {
        let profile = r#"{"observeAttr":{"keyName":{"/3303/0/5700":"temperature","/3/0/1":"model"},"observe":[],"attribute":["/3/0/1"],"telemetry":["/3303/0/5700"],"attributeLwm2m":{},"observeStrategy":"SINGLE"}}"#;
        let types = HashMap::from([
            ("/3303/0/5700".into(), ResourceType::Float),
            ("/3/0/1".into(), ResourceType::String),
        ]);
        let resources = resources_from_profile(profile, &types).unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0].key_name, "temperature");
        assert_eq!(resources[1].kind, ValueKind::Attribute);
        let body = codec::encode(&[Tlv::ObjectInstance {
            id: 0,
            children: vec![Tlv::Resource {
                id: 5700,
                value: &21.5_f32.to_be_bytes(),
            }],
        }]);
        assert_eq!(
            decode_resource(&resources[0], &body).unwrap(),
            KvValue::Double(21.5)
        );
    }

    #[test]
    fn composite_observe_strategy_is_rejected() {
        let profile =
            r#"{"observeAttr":{"observeStrategy":"COMPOSITE","telemetry":[],"attribute":[]}}"#;
        assert!(resources_from_profile(profile, &HashMap::new()).is_err());
    }

    #[test]
    fn multiple_resource_requires_an_unambiguous_instance() {
        let resource = ObservedResource {
            path: Lwm2mPath::parse("/4/0/2").unwrap(),
            resource_type: ResourceType::Integer,
            key_name: "value".into(),
            kind: ValueKind::Telemetry,
        };
        let body = codec::encode(&[Tlv::MultipleResource {
            id: 2,
            children: vec![
                Tlv::ResourceInstance { id: 0, value: &[1] },
                Tlv::ResourceInstance { id: 1, value: &[2] },
            ],
        }]);
        assert!(decode_resource(&resource, &body).is_err());
    }
}
