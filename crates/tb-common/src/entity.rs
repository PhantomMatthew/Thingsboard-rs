use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// 实体类型。与 proto tb.common.EntityType 一一对应。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntityType {
    Tenant,
    Customer,
    User,
    Device,
    DeviceProfile,
    Asset,
    AssetProfile,
    Dashboard,
    RuleChain,
    RuleNode,
    OtaPackage,
    Alarm,
    Edge,
}

impl EntityType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tenant => "TENANT",
            Self::Customer => "CUSTOMER",
            Self::User => "USER",
            Self::Device => "DEVICE",
            Self::DeviceProfile => "DEVICE_PROFILE",
            Self::Asset => "ASSET",
            Self::AssetProfile => "ASSET_PROFILE",
            Self::Dashboard => "DASHBOARD",
            Self::RuleChain => "RULE_CHAIN",
            Self::RuleNode => "RULE_NODE",
            Self::OtaPackage => "OTA_PACKAGE",
            Self::Alarm => "ALARM",
            Self::Edge => "EDGE",
        }
    }

    /// 对应的数据库表名。
    pub fn table_name(&self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Customer => "customer",
            Self::User => "tb_user",
            Self::Device => "device",
            Self::DeviceProfile => "device_profile",
            Self::Asset => "asset",
            Self::AssetProfile => "asset_profile",
            Self::Dashboard => "dashboard",
            Self::RuleChain => "rule_chain",
            Self::RuleNode => "rule_node",
            Self::OtaPackage => "ota_package",
            Self::Alarm => "alarm",
            Self::Edge => "edge",
        }
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EntityType {
    type Err = crate::error::TbError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TENANT" => Ok(Self::Tenant),
            "CUSTOMER" => Ok(Self::Customer),
            "USER" => Ok(Self::User),
            "DEVICE" => Ok(Self::Device),
            "DEVICE_PROFILE" => Ok(Self::DeviceProfile),
            "ASSET" => Ok(Self::Asset),
            "ASSET_PROFILE" => Ok(Self::AssetProfile),
            "DASHBOARD" => Ok(Self::Dashboard),
            "RULE_CHAIN" => Ok(Self::RuleChain),
            "RULE_NODE" => Ok(Self::RuleNode),
            "OTA_PACKAGE" => Ok(Self::OtaPackage),
            "ALARM" => Ok(Self::Alarm),
            "EDGE" => Ok(Self::Edge),
            other => Err(crate::error::TbError::InvalidInput(format!(
                "unknown entity type: {other}"
            ))),
        }
    }
}

/// 类型化实体 ID：UUIDv7 + 实体类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityId {
    pub entity_type: EntityType,
    pub id: Uuid,
}

impl EntityId {
    pub fn new(entity_type: EntityType) -> Self {
        Self {
            entity_type,
            id: Uuid::now_v7(),
        }
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.entity_type, self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_type_roundtrip() {
        for t in [EntityType::Tenant, EntityType::Device, EntityType::Alarm] {
            assert_eq!(EntityType::from_str(t.as_str()).unwrap(), t);
        }
    }

    #[test]
    fn entity_id_is_v7() {
        let id = EntityId::new(EntityType::Device);
        assert_eq!(id.id.get_version(), Some(uuid::Version::SortRand));
    }
}
