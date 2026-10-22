//! 认证服务：登录 / 刷新 / 改密，含登录失败锁定（内存 TTL map）。

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use tb_dao::models::{User, UserCredentials};
use tb_dao::{UserCredentialsDao, UserDao};
use uuid::Uuid;

use crate::password::{hash_password, verify_password};
use crate::token::{JwtTokenFactory, SecurityUser, TokenType};

const MAX_FAILED_ATTEMPTS: u32 = 5;
const LOCK_DURATION: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenPair {
    pub token: String,
    pub refresh_token: String,
}

pub struct AuthService {
    user_dao: UserDao,
    creds_dao: UserCredentialsDao,
    factory: Arc<JwtTokenFactory>,
    /// user_id → 锁定期内存在记录即视为锁定。
    lockout: moka::future::Cache<Uuid, ()>,
}

impl AuthService {
    pub fn new(pool: PgPool, factory: Arc<JwtTokenFactory>) -> Arc<Self> {
        Arc::new(Self {
            user_dao: UserDao::new(pool.clone()),
            creds_dao: UserCredentialsDao::new(pool),
            factory,
            lockout: moka::future::Cache::builder()
                .time_to_live(LOCK_DURATION)
                .build(),
        })
    }

    pub fn factory(&self) -> &Arc<JwtTokenFactory> {
        &self.factory
    }

    pub async fn login(&self, email: &str, password: &str) -> TbResult<TokenPair> {
        let invalid = || TbError::Unauthenticated("invalid username or password".into());
        let user = self
            .user_dao
            .find_by_email(email)
            .await?
            .ok_or_else(invalid)?;

        if self.lockout.get(&user.id).await.is_some() {
            return Err(TbError::RateLimitExceeded(format!(
                "account locked for {} minutes due to too many failed attempts",
                LOCK_DURATION.as_secs() / 60
            )));
        }

        let mut creds = self
            .creds_dao
            .find_by_user_id(user.id)
            .await?
            .ok_or_else(invalid)?;
        if !creds.enabled {
            return Err(TbError::PermissionDenied(
                "user credentials are disabled".into(),
            ));
        }
        let hash = creds.password.clone().ok_or_else(invalid)?;
        if !verify_password(password, &hash)? {
            self.register_failure(&mut creds).await?;
            return Err(invalid());
        }

        creds.failed_login_attempts = 0;
        creds.last_login_ts = Some(chrono::Utc::now().timestamp_millis());
        self.creds_dao.save(&creds).await?;
        self.issue_tokens(&user)
    }

    async fn register_failure(&self, creds: &mut UserCredentials) -> TbResult<()> {
        creds.failed_login_attempts += 1;
        if creds.failed_login_attempts as u32 >= MAX_FAILED_ATTEMPTS {
            self.lockout.insert(creds.user_id, ()).await;
            creds.failed_login_attempts = 0;
        }
        self.creds_dao.save(creds).await?;
        Ok(())
    }

    pub async fn refresh(&self, refresh_token: &str) -> TbResult<TokenPair> {
        let claims = self
            .factory
            .parse_typed(refresh_token, TokenType::Refresh)?;
        let user = self.load_user(claims.user_id()?).await?;
        self.issue_tokens(&user)
    }

    pub async fn change_password(&self, user_id: Uuid, current: &str, new: &str) -> TbResult<()> {
        if new.is_empty() {
            return Err(TbError::InvalidInput(
                "new password must not be empty".into(),
            ));
        }
        let mut creds = self
            .creds_dao
            .find_by_user_id(user_id)
            .await?
            .ok_or_else(|| TbError::NotFound(format!("credentials for user {user_id}")))?;
        let hash = creds
            .password
            .clone()
            .ok_or_else(|| TbError::Unauthenticated("user has no password set".into()))?;
        if !verify_password(current, &hash)? {
            return Err(TbError::Unauthenticated("current password mismatch".into()));
        }
        creds.password = Some(hash_password(new)?);
        creds.activate_token = None;
        creds.reset_token = None;
        self.creds_dao.save(&creds).await?;
        Ok(())
    }

    /// 解析 access token 并加载当前用户。
    pub async fn get_user_from_token(&self, token: &str) -> TbResult<SecurityUser> {
        let claims = self.factory.parse_typed(token, TokenType::Access)?;
        let user = self.load_user(claims.user_id()?).await?;
        Ok(security_user(&user))
    }

    pub async fn get_user(&self, user_id: Uuid) -> TbResult<User> {
        self.load_user(user_id).await
    }

    async fn load_user(&self, user_id: Uuid) -> TbResult<User> {
        self.user_dao
            .find_by_id(user_id)
            .await?
            .ok_or_else(|| TbError::NotFound(format!("user {user_id}")))
    }

    fn issue_tokens(&self, user: &User) -> TbResult<TokenPair> {
        let security_user = security_user(user);
        Ok(TokenPair {
            token: self.factory.create_access_token(&security_user)?,
            refresh_token: self.factory.create_refresh_token(&security_user)?,
        })
    }
}

pub fn security_user(user: &User) -> SecurityUser {
    SecurityUser {
        id: user.id,
        tenant_id: user.tenant_id,
        customer_id: user.customer_id,
        email: user.email.clone(),
        authority: user.authority,
    }
}
