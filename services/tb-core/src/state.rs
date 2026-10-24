//! 应用状态：DAO 集合 + 认证服务，Arc 内聚 clone 廉价。

use std::ops::Deref;
use std::sync::Arc;

use sqlx::PgPool;
use tb_auth::{AuthService, JwtTokenFactory};
use tb_dao::{
    AlarmDao, ApiUsageStateDao, AssetDao, AssetProfileDao, AttributesDao, AuditLogDao,
    CalculatedFieldDao, CustomerDao, DashboardDao, DeviceCredentialsDao, DeviceDao,
    DeviceProfileDao, KeyDictionaryDao, Oauth2ClientDao, OtaPackageDao, RelationDao, RuleChainDao,
    RuleNodeDao, RuleNodeDebugEventDao, TenantDao, TenantProfileDao, TimeseriesDao,
    UserCredentialsDao, UserDao,
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
    pub oauth2_client_dao: Oauth2ClientDao,
    pub calculated_field_dao: CalculatedFieldDao,
    pub usage: UsageTracker,
    /// login 审计事件通道（tb-auth 侧 try_send，receiver 已在 new() 启动）。
    pub audit_tx: mpsc::Sender<tb_dao::AuditLog>,
    pub device_sessions: SharedDeviceSessionRegistry,
    pub node_id: String,
    pub redis: redis::Client,
    redis_conn: tokio::sync::OnceCell<redis::aio::ConnectionManager>,
    kafka_producer: tokio::sync::OnceCell<KafkaQueueProducer>,
    kafka_brokers: String,
    /// tb-edqs 内网地址（代理端点转发目标）。
    pub edqs_url: String,
    pub http_client: reqwest::Client,
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

fn default_node_id() -> String {
    let hostname = std::env::var("HOSTNAME")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| {
            let output = std::process::Command::new("hostname").output().ok()?;
            output
                .status
                .success()
                .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        })
        .unwrap_or_else(|| "tb-core".into());
    let suffix = uuid::Uuid::now_v7().simple().to_string();
    format!("{hostname}-{}", &suffix[..8])
}

impl AppState {
    pub async fn new(pool: PgPool, jwt_factory: Arc<JwtTokenFactory>) -> tb_common::TbResult<Self> {
        let node_id = std::env::var("TB_CORE_NODE_ID")
            .ok()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(default_node_id);
        Self::new_with_node_id(pool, jwt_factory, node_id).await
    }

    pub async fn new_with_node_id(
        pool: PgPool,
        jwt_factory: Arc<JwtTokenFactory>,
        node_id: impl Into<String>,
    ) -> tb_common::TbResult<Self> {
        let node_id = node_id.into();
        let key_dictionary_dao = KeyDictionaryDao::new(pool.clone());
        let redis_url = std::env::var("TB_REDIS_URL").unwrap_or_else(|_| DEFAULT_REDIS_URL.into());
        let redis =
            redis::Client::open(redis_url.as_str()).map_err(tb_common::TbError::internal)?;
        let mut redis_manager = redis::aio::ConnectionManager::new(redis.clone())
            .await
            .map_err(tb_common::TbError::internal)?;
        let _: String = redis::cmd("PING")
            .query_async(&mut redis_manager)
            .await
            .map_err(tb_common::TbError::internal)?;
        let redis_conn = tokio::sync::OnceCell::new();
        let _ = redis_conn.set(redis_manager.clone());
        let device_sessions = crate::device_session::DeviceSessionRegistry::new_shared(
            redis_manager,
            node_id.clone(),
        );
        let kafka_brokers =
            std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| DEFAULT_KAFKA_BROKERS.into());
        let edqs_url =
            std::env::var("TB_EDQS_URL").unwrap_or_else(|_| "http://localhost:8090".into());
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
                oauth2_client_dao: Oauth2ClientDao::new(pool.clone()),
                calculated_field_dao: CalculatedFieldDao::new(pool.clone()),
                usage: usage.clone(),
                audit_tx,
                device_sessions,
                node_id,
                key_dictionary_dao,
                redis,
                redis_conn,
                kafka_producer: tokio::sync::OnceCell::new(),
                kafka_brokers,
                edqs_url,
                http_client: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build()
                    .unwrap_or_default(),
                auth_service: AuthService::new(pool.clone(), jwt_factory.clone()),
                jwt_factory,
                pool,
            }),
        };
        usage.start_flush(state.clone());
        Ok(state)
    }

    /// 复用启动时已验证的 Redis 连接；断线后由 ConnectionManager 自动重连。
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
