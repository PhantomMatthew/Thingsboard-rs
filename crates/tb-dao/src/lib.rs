//! tb-dao：PostgreSQL 数据访问层（sqlx 动态查询）。

pub mod alarm;
pub mod api_usage;
pub mod asset;
pub mod asset_profile;
pub mod attributes;
pub mod audit_log;
pub mod calculated_field;
pub mod customer;
pub mod dashboard;
pub mod device;
pub mod device_credentials;
pub mod device_profile;
pub mod key_dictionary;
pub mod kv_columns;
pub mod models;
pub mod oauth2_client;
pub mod ota;
pub mod page;
pub mod relation;
pub mod rule_chain;
pub mod rule_node_debug_event;
pub mod rule_node_dedup;
pub mod rule_node_delay;
pub mod tenant;
pub mod timeseries;
pub mod user;

pub use alarm::AlarmDao;
pub use api_usage::{ApiUsageState, ApiUsageStateDao, UsageDelta};
pub use asset::AssetDao;
pub use asset_profile::AssetProfileDao;
pub use attributes::AttributesDao;
pub use audit_log::{AuditLog, AuditLogDao, AuditLogFilter};
pub use calculated_field::CalculatedFieldDao;
pub use customer::CustomerDao;
pub use dashboard::DashboardDao;
pub use device::DeviceDao;
pub use device_credentials::DeviceCredentialsDao;
pub use device_profile::DeviceProfileDao;
pub use key_dictionary::KeyDictionaryDao;
pub use models::*;
pub use oauth2_client::Oauth2ClientDao;
pub use ota::OtaPackageDao;
pub use page::{PageData, PageLink};
pub use relation::RelationDao;
pub use rule_chain::{RuleChainDao, RuleNodeDao};
pub use rule_node_debug_event::{RuleNodeDebugEvent, RuleNodeDebugEventDao};
pub use rule_node_dedup::{
    AppendResult, DedupStrategy, DedupWindow, DedupWindowState, RuleNodeDedupDao,
};
pub use rule_node_delay::{DelayEnqueueResult, DelayTask, DelayTaskState, RuleNodeDelayDao};
pub use sqlx::PgPool;
pub use tenant::{TenantDao, TenantProfileDao};
pub use timeseries::{Aggregation, TimeseriesDao, TsDeleteResult};
pub use user::{LoginAttempt, UserCredentialsDao, UserDao};

/// 连接中断、连接池耗尽、事务冲突与资源不足等可重试的数据库故障；
/// 约束冲突、数据格式等错误重试也不会成功，返回 false。
pub fn is_transient_error(err: &tb_common::TbError) -> bool {
    let tb_common::TbError::Database(db) = err else {
        return false;
    };
    match db.0.downcast_ref::<sqlx::Error>() {
        Some(sqlx::Error::Database(e)) => e
            .code()
            .is_some_and(|code| matches!(code.get(..2), Some("08" | "40" | "53" | "57" | "58"))),
        Some(
            sqlx::Error::Io(_)
            | sqlx::Error::Tls(_)
            | sqlx::Error::Protocol(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::PoolClosed
            | sqlx::Error::WorkerCrashed,
        ) => true,
        _ => false,
    }
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 运行 migrations/ 下的全部迁移（路径以 crate 目录为基准）。
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}
