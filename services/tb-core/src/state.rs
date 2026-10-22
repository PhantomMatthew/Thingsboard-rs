//! 应用状态：DAO 集合 + 认证服务，Arc 内聚 clone 廉价。

use std::ops::Deref;
use std::sync::Arc;

use sqlx::PgPool;
use tb_auth::{AuthService, JwtTokenFactory};
use tb_dao::{
    AssetDao, AssetProfileDao, AttributesDao, CustomerDao, DashboardDao, DeviceCredentialsDao,
    DeviceDao, DeviceProfileDao, KeyDictionaryDao, RelationDao, RuleChainDao, RuleNodeDao,
    TenantDao, TenantProfileDao, TimeseriesDao, UserCredentialsDao, UserDao,
};

const DEFAULT_REDIS_URL: &str = "redis://localhost:16379";

pub struct AppStateInner {
    pub pool: PgPool,
    pub tenant_dao: TenantDao,
    pub tenant_profile_dao: TenantProfileDao,
    pub customer_dao: CustomerDao,
    pub user_dao: UserDao,
    pub user_credentials_dao: UserCredentialsDao,
    pub device_dao: DeviceDao,
    pub device_credentials_dao: DeviceCredentialsDao,
    pub device_profile_dao: DeviceProfileDao,
    pub asset_dao: AssetDao,
    pub asset_profile_dao: AssetProfileDao,
    pub dashboard_dao: DashboardDao,
    pub relation_dao: RelationDao,
    pub rule_chain_dao: RuleChainDao,
    pub rule_node_dao: RuleNodeDao,
    pub key_dictionary_dao: KeyDictionaryDao,
    pub timeseries_dao: TimeseriesDao,
    pub attributes_dao: AttributesDao,
    pub redis: redis::Client,
    redis_conn: tokio::sync::OnceCell<redis::aio::ConnectionManager>,
    pub auth_service: Arc<AuthService>,
    pub jwt_factory: Arc<JwtTokenFactory>,
}

#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

impl Deref for AppState {
    type Target = AppStateInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AppState {
    pub fn new(pool: PgPool, jwt_factory: Arc<JwtTokenFactory>) -> Self {
        let key_dictionary_dao = KeyDictionaryDao::new(pool.clone());
        let redis_url = std::env::var("TB_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.into());
        let redis = redis::Client::open(redis_url.as_str())
            .unwrap_or_else(|_| redis::Client::open(DEFAULT_REDIS_URL).unwrap());
        Self {
            inner: Arc::new(AppStateInner {
                tenant_dao: TenantDao::new(pool.clone()),
                tenant_profile_dao: TenantProfileDao::new(pool.clone()),
                customer_dao: CustomerDao::new(pool.clone()),
                user_dao: UserDao::new(pool.clone()),
                user_credentials_dao: UserCredentialsDao::new(pool.clone()),
                device_dao: DeviceDao::new(pool.clone()),
                device_credentials_dao: DeviceCredentialsDao::new(pool.clone()),
                device_profile_dao: DeviceProfileDao::new(pool.clone()),
                asset_dao: AssetDao::new(pool.clone()),
                asset_profile_dao: AssetProfileDao::new(pool.clone()),
                dashboard_dao: DashboardDao::new(pool.clone()),
                relation_dao: RelationDao::new(pool.clone()),
                rule_chain_dao: RuleChainDao::new(pool.clone()),
                rule_node_dao: RuleNodeDao::new(pool.clone()),
                timeseries_dao: TimeseriesDao::new(pool.clone(), key_dictionary_dao.clone()),
                attributes_dao: AttributesDao::new(pool.clone(), key_dictionary_dao.clone()),
                key_dictionary_dao,
                redis,
                redis_conn: tokio::sync::OnceCell::new(),
                auth_service: AuthService::new(pool.clone(), jwt_factory.clone()),
                jwt_factory,
                pool,
            }),
        }
    }

    /// 惰性建立并复用 Redis 连接（自动重连）；首次失败不影响后续重试。
    pub async fn redis_manager(&self) -> redis::RedisResult<redis::aio::ConnectionManager> {
        self.redis_conn
            .get_or_try_init(|| redis::aio::ConnectionManager::new(self.redis.clone()))
            .await
            .cloned()
    }
}

impl axum::extract::FromRef<AppState> for Arc<JwtTokenFactory> {
    fn from_ref(state: &AppState) -> Self {
        state.jwt_factory.clone()
    }
}
