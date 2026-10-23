//! 应用状态：DAO 集合 + 认证服务，Arc 内聚 clone 廉价。

use std::ops::Deref;
use std::sync::Arc;

use sqlx::PgPool;
use tb_auth::{AuthService, JwtTokenFactory};
use tb_dao::{
    AlarmDao, ApiUsageStateDao, AssetDao, AssetProfileDao, AttributesDao, AuditLogDao, CustomerDao,
    DashboardDao, DeviceCredentialsDao, DeviceDao, DeviceProfileDao, KeyDictionaryDao,
    OtaPackageDao, RelationDao, RuleChainDao, RuleNodeDao, RuleNodeDebugEventDao, TenantDao,
    TenantProfileDao, TimeseriesDao, UserCredentialsDao, UserDao,
};
use tb_queue::KafkaQueueProducer;
use tokio::sync::mpsc;

use crate::device_session::SharedDeviceSessionRegistry;
use crate::usage::UsageTracker;

const DEFAULT_REDIS_URL: &str = "redis://localhost:16379";
const DEFAULT_KAFKA_BROKERS: &str = "localhost:29092";

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
    pub rule_node_debug_event_dao: RuleNodeDebugEventDao,
    pub key_dictionary_dao: KeyDictionaryDao,
    pub timeseries_dao: TimeseriesDao,
    pub attributes_dao: AttributesDao,
    pub alarm_dao: AlarmDao,
    pub ota_package_dao: OtaPackageDao,
    pub audit_log_dao: AuditLogDao,
    pub api_usage_state_dao: ApiUsageStateDao,
    pub usage: UsageTracker,
    /// login 审计事件通道（tb-auth 侧 try_send，receiver 已在 new() 启动）。
    pub audit_tx: mpsc::Sender<tb_dao::AuditLog>,
    pub device_sessions: SharedDeviceSessionRegistry,
    pub redis: redis::Client,
    redis_conn: tokio::sync::OnceCell<redis::aio::ConnectionManager>,
    kafka_producer: tokio::sync::OnceCell<KafkaQueueProducer>,
    kafka_brokers: String,
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
        let kafka_brokers =
            std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| DEFAULT_KAFKA_BROKERS.into());
        let audit_log_dao = AuditLogDao::new(pool.clone());
        let (audit_tx, mut audit_rx) = mpsc::channel::<tb_dao::AuditLog>(1024);
        let sink_dao = audit_log_dao.clone();
        tokio::spawn(async move {
            while let Some(entry) = audit_rx.recv().await {
                if let Err(e) = sink_dao.save(&entry).await {
                    tracing::warn!("audit sink save failed: {e}");
                }
            }
        });
        let usage = UsageTracker::default();
        let state = Self {
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
                rule_node_debug_event_dao: RuleNodeDebugEventDao::new(pool.clone()),
                timeseries_dao: TimeseriesDao::new(pool.clone(), key_dictionary_dao.clone()),
                attributes_dao: AttributesDao::new(pool.clone(), key_dictionary_dao.clone()),
                alarm_dao: AlarmDao::new(pool.clone()),
                ota_package_dao: OtaPackageDao::new(pool.clone()),
                audit_log_dao,
                api_usage_state_dao: ApiUsageStateDao::new(pool.clone()),
                usage: usage.clone(),
                audit_tx,
                device_sessions: crate::device_session::DeviceSessionRegistry::new_shared(),
                key_dictionary_dao,
                redis,
                redis_conn: tokio::sync::OnceCell::new(),
                kafka_producer: tokio::sync::OnceCell::new(),
                kafka_brokers,
                auth_service: AuthService::new(pool.clone(), jwt_factory.clone()),
                jwt_factory,
                pool,
            }),
        };
        usage.start_flush(state.clone());
        state
    }

    /// 惰性建立并复用 Redis 连接（自动重连）；首次失败不影响后续重试。
    pub async fn redis_manager(&self) -> redis::RedisResult<redis::aio::ConnectionManager> {
        self.redis_conn
            .get_or_try_init(|| redis::aio::ConnectionManager::new(self.redis.clone()))
            .await
            .cloned()
    }

    /// 惰性建立 Kafka producer（下行通知/RPC 下发用）。
    pub async fn queue_producer(&self) -> tb_common::TbResult<&KafkaQueueProducer> {
        self.kafka_producer
            .get_or_try_init(|| async {
                KafkaQueueProducer::new(&self.kafka_brokers, "tb-core-rest")
                    .map_err(tb_common::TbError::internal)
            })
            .await
    }
}

impl axum::extract::FromRef<AppState> for Arc<JwtTokenFactory> {
    fn from_ref(state: &AppState) -> Self {
        state.jwt_factory.clone()
    }
}
