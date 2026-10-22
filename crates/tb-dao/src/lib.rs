//! tb-dao：PostgreSQL 数据访问层（sqlx 动态查询）。

pub mod alarm;
pub mod asset;
pub mod asset_profile;
pub mod customer;
pub mod dashboard;
pub mod device;
pub mod device_credentials;
pub mod device_profile;
pub mod models;
pub mod ota;
pub mod page;
pub mod relation;
pub mod rule_chain;
pub mod tenant;
pub mod user;

pub use alarm::AlarmDao;
pub use asset::AssetDao;
pub use asset_profile::AssetProfileDao;
pub use customer::CustomerDao;
pub use dashboard::DashboardDao;
pub use device::DeviceDao;
pub use device_credentials::DeviceCredentialsDao;
pub use device_profile::DeviceProfileDao;
pub use models::*;
pub use ota::OtaPackageDao;
pub use page::{PageData, PageLink};
pub use relation::RelationDao;
pub use rule_chain::{RuleChainDao, RuleNodeDao};
pub use sqlx::PgPool;
pub use tenant::{TenantDao, TenantProfileDao};
pub use user::{UserCredentialsDao, UserDao};

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 运行 migrations/ 下的全部迁移（路径以 crate 目录为基准）。
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}
