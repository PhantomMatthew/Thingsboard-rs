//! 与 migrations/0001_init.sql 对应的实体模型。
//! JSON 序列化采用 camelCase，对齐 ThingsBoard REST API 风格；
//! 枚举序列化为 SCREAMING_SNAKE_CASE，数据库中以 VARCHAR 存储。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
use std::fmt;
use std::str::FromStr;
use tb_common::{EntityType, TbError};
use uuid::Uuid;

macro_rules! str_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $s:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub fn as_str(&self) -> &'static str {
                match self {
                    $(Self::$variant => $s),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = TbError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($s => Ok(Self::$variant)),+,
                    other => Err(TbError::InvalidInput(format!(
                        "unknown {}: {other}",
                        stringify!($name)
                    ))),
                }
            }
        }

        impl sqlx::Type<sqlx::Postgres> for $name {
            fn type_info() -> PgTypeInfo {
                <String as sqlx::Type<sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &PgTypeInfo) -> bool {
                <String as sqlx::Type<sqlx::Postgres>>::compatible(ty)
            }
        }

        impl<'r> sqlx::Decode<'r, sqlx::Postgres> for $name {
            fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
                let s = <String as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
                Ok(s.parse()?)
            }
        }

        impl sqlx::Encode<'_, sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut PgArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <&str as sqlx::Encode<'_, sqlx::Postgres>>::encode(self.as_str(), buf)
            }
        }
    };
}

str_enum! {
    /// tb_user.authority
    Authority {
        SysAdmin => "SYS_ADMIN",
        TenantAdmin => "TENANT_ADMIN",
        CustomerUser => "CUSTOMER_USER",
    }
}

str_enum! {
    /// device_credentials.credentials_type
    CredentialsType {
        AccessToken => "ACCESS_TOKEN",
        X509Certificate => "X509_CERTIFICATE",
        MqttBasic => "MQTT_BASIC",
    }
}

str_enum! {
    /// alarm.severity
    AlarmSeverity {
        Critical => "CRITICAL",
        Major => "MAJOR",
        Minor => "MINOR",
        Warning => "WARNING",
        Indeterminate => "INDETERMINATE",
    }
}

str_enum! {
    /// alarm.status
    AlarmStatus {
        ActiveUnack => "ACTIVE_UNACK",
        ActiveAck => "ACTIVE_ACK",
        ClearedUnack => "CLEARED_UNACK",
        ClearedAck => "CLEARED_ACK",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Tenant {
    pub id: Uuid,
    pub created_time: i64,
    pub title: String,
    pub region: Option<String>,
    pub country: Option<String>,
    pub state: Option<String>,
    pub city: Option<String>,
    pub address: Option<String>,
    pub address2: Option<String>,
    pub zip: Option<String>,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub additional_info: Option<Value>,
    pub tenant_profile_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct TenantProfile {
    pub id: Uuid,
    pub created_time: i64,
    pub name: String,
    pub description: Option<String>,
    pub is_default: bool,
    pub profile_data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Customer {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub title: String,
    pub country: Option<String>,
    pub state: Option<String>,
    pub city: Option<String>,
    pub address: Option<String>,
    pub address2: Option<String>,
    pub zip: Option<String>,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub additional_info: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub customer_id: Option<Uuid>,
    pub authority: Authority,
    pub email: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub phone: Option<String>,
    pub additional_info: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct UserCredentials {
    pub id: Uuid,
    pub created_time: i64,
    pub user_id: Uuid,
    pub enabled: bool,
    pub password: Option<String>,
    pub activate_token: Option<String>,
    pub reset_token: Option<String>,
    pub failed_login_attempts: i32,
    pub last_login_ts: Option<i64>,
    pub password_changed_ts: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct DeviceProfile {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub is_default: bool,
    pub transport_type: String,
    pub provision_type: String,
    pub profile_data: Value,
    pub firmware_id: Option<Uuid>,
    pub software_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct AssetProfile {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub is_default: bool,
    pub profile_data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub customer_id: Option<Uuid>,
    pub device_profile_id: Uuid,
    pub name: String,
    #[sqlx(rename = "type")]
    pub r#type: Option<String>,
    pub label: Option<String>,
    pub external_id: Option<Uuid>,
    pub firmware_id: Option<Uuid>,
    pub software_id: Option<Uuid>,
    pub additional_info: Option<Value>,
    pub device_data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct DeviceCredentials {
    pub id: Uuid,
    pub created_time: i64,
    pub device_id: Uuid,
    pub credentials_type: CredentialsType,
    pub credentials_id: Option<String>,
    pub credentials_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub customer_id: Option<Uuid>,
    pub asset_profile_id: Uuid,
    pub name: String,
    pub label: Option<String>,
    pub external_id: Option<Uuid>,
    pub additional_info: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Dashboard {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub title: String,
    pub configuration: Value,
    pub assigned_customers: Option<Value>,
    pub mobile_hide: bool,
    pub mobile_order: Option<i32>,
    pub image: Option<String>,
    pub additional_info: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct RuleChain {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub name: String,
    pub first_rule_node_id: Option<Uuid>,
    pub root: bool,
    pub debug_mode: bool,
    pub configuration: Option<Value>,
    pub additional_info: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct RuleNode {
    pub id: Uuid,
    pub created_time: i64,
    pub rule_chain_id: Uuid,
    #[sqlx(rename = "type")]
    pub r#type: String,
    pub name: String,
    pub debug_mode: bool,
    pub singleton_mode: bool,
    pub queue_name: Option<String>,
    pub configuration: Option<Value>,
    pub additional_info: Option<Value>,
}

/// relation 表（复合主键，无自增 id）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityRelation {
    pub from_id: Uuid,
    pub from_type: EntityType,
    pub to_id: Uuid,
    pub to_type: EntityType,
    pub relation_type_group: String,
    pub relation_type: String,
    pub additional_info: Option<Value>,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for EntityRelation {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        let from_type: String = row.try_get("from_type")?;
        let to_type: String = row.try_get("to_type")?;
        Ok(Self {
            from_id: row.try_get("from_id")?,
            from_type: from_type
                .parse()
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            to_id: row.try_get("to_id")?,
            to_type: to_type
                .parse()
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            relation_type_group: row.try_get("relation_type_group")?,
            relation_type: row.try_get("relation_type")?,
            additional_info: row.try_get("additional_info")?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Alarm {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub originator_id: Uuid,
    pub originator_type: EntityType,
    #[serde(rename = "type")]
    pub r#type: String,
    pub severity: AlarmSeverity,
    pub status: AlarmStatus,
    pub start_ts: i64,
    pub end_ts: i64,
    pub ack_ts: Option<i64>,
    pub clear_ts: Option<i64>,
    pub details: Option<Value>,
    pub propagate: bool,
    pub propagate_to_owner: bool,
    pub propagate_to_tenant: bool,
    pub propagate_relation_types: Option<Value>,
    pub customer_id: Option<Uuid>,
    pub assignee_id: Option<Uuid>,
    pub assign_ts: Option<i64>,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for Alarm {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row;
        let originator_type: String = row.try_get("originator_type")?;
        Ok(Self {
            id: row.try_get("id")?,
            created_time: row.try_get("created_time")?,
            tenant_id: row.try_get("tenant_id")?,
            originator_id: row.try_get("originator_id")?,
            originator_type: originator_type
                .parse()
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            r#type: row.try_get("type")?,
            severity: row.try_get("severity")?,
            status: row.try_get("status")?,
            start_ts: row.try_get("start_ts")?,
            end_ts: row.try_get("end_ts")?,
            ack_ts: row.try_get("ack_ts")?,
            clear_ts: row.try_get("clear_ts")?,
            details: row.try_get("details")?,
            propagate: row.try_get("propagate")?,
            propagate_to_owner: row.try_get("propagate_to_owner")?,
            propagate_to_tenant: row.try_get("propagate_to_tenant")?,
            propagate_relation_types: row.try_get("propagate_relation_types")?,
            customer_id: row.try_get("customer_id")?,
            assignee_id: row.try_get("assignee_id")?,
            assign_ts: row.try_get("assign_ts")?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct Oauth2Client {
    pub id: Uuid,
    pub created_time: i64,
    /// 空 = 平台级客户端。
    pub tenant_id: Option<Uuid>,
    pub title: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub authorization_uri: String,
    pub token_uri: String,
    pub user_info_uri: String,
    pub redirect_uri_template: Option<String>,
    pub scope: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct CalculatedField {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub entity_id: Uuid,
    pub name: String,
    pub configuration: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct OtaPackage {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub device_profile_id: Uuid,
    #[sqlx(rename = "type")]
    pub r#type: String,
    pub title: String,
    pub version: String,
    pub tag: Option<String>,
    pub file_name: Option<String>,
    pub content_type: Option<String>,
    pub checksum_algorithm: Option<String>,
    pub checksum: Option<String>,
    pub size: Option<i64>,
    #[serde(skip_serializing)]
    pub data: Option<Vec<u8>>,
    pub additional_info: Option<Value>,
}
