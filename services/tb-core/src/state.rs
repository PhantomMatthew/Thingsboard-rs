//! 应用状态：DAO 集合 + 认证服务，Arc 内聚 clone 廉价。

use std::ops::Deref;
use std::sync::Arc;

use sqlx::PgPool;
use tb_auth::{AuthService, JwtTokenFactory};
use tb_dao::{
    AssetDao, AssetProfileDao, CustomerDao, DashboardDao, DeviceCredentialsDao, DeviceDao,
    DeviceProfileDao, RelationDao, TenantDao, TenantProfileDao, UserCredentialsDao, UserDao,
};

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
                auth_service: AuthService::new(pool.clone(), jwt_factory.clone()),
                jwt_factory,
                pool,
            }),
        }
    }
}

impl axum::extract::FromRef<AppState> for Arc<JwtTokenFactory> {
    fn from_ref(state: &AppState) -> Self {
        state.jwt_factory.clone()
    }
}
