//! 认证服务：登录 / 刷新 / 改密，含登录失败锁定（PostgreSQL 原子计数与锁定期，跨实例共享）。

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use tb_dao::models::{User, UserCredentials};
use tb_dao::{LoginAttempt, UserCredentialsDao, UserDao};
use uuid::Uuid;

use crate::password::{hash_password, verify_password};
use crate::token::{JwtTokenFactory, SecurityUser, TokenType};

const MAX_FAILED_ATTEMPTS: i32 = 5;
const LOCK_DURATION: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenPair {
    pub token: String,
    pub refresh_token: String,
}

pub struct AuthService {
    pub(crate) user_dao: UserDao,
    pub(crate) creds_dao: UserCredentialsDao,
    factory: Arc<JwtTokenFactory>,
    pub(crate) pool: PgPool,
    /// OAuth2 code 换 token / userinfo 拉取（5s 超时）。
    pub(crate) http: reqwest::Client,
}

impl AuthService {
    pub fn new(pool: PgPool, factory: Arc<JwtTokenFactory>) -> Arc<Self> {
        Arc::new(Self {
            user_dao: UserDao::new(pool.clone()),
            creds_dao: UserCredentialsDao::new(pool.clone()),
            factory,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("valid oauth2 HTTP client configuration"),
            pool,
        })
    }

    pub fn factory(&self) -> &Arc<JwtTokenFactory> {
        &self.factory
    }

    pub async fn login(&self, email: &str, password: &str) -> TbResult<TokenPair> {
        let authenticated_at = chrono::Utc::now().timestamp_millis();
        let invalid = || TbError::Unauthenticated("invalid username or password".into());
        let locked = || {
            TbError::RateLimitExceeded(format!(
                "account locked for {} minutes due to too many failed attempts",
                LOCK_DURATION.as_secs() / 60
            ))
        };
        let user = self
            .user_dao
            .find_by_email(email)
            .await?
            .ok_or_else(invalid)?;
        let creds = self
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

        let lock_until = || authenticated_at.saturating_add(LOCK_DURATION.as_millis() as i64);
        let attempt = self
            .creds_dao
            .reserve_login_attempt(user.id, authenticated_at, MAX_FAILED_ATTEMPTS, lock_until())
            .await?;
        if attempt == LoginAttempt::Locked {
            return Err(locked());
        }
        if !verify_password(password, &hash)? {
            self.creds_dao
                .lock_if_exhausted(user.id, MAX_FAILED_ATTEMPTS, lock_until())
                .await?;
            return Err(invalid());
        }

        match self
            .creds_dao
            .record_login_success(user.id, authenticated_at, &hash)
            .await?
        {
            Some(creds) => self.issue_tokens_for_credentials(&user, &creds, authenticated_at),
            None if self
                .creds_dao
                .is_login_locked(user.id, authenticated_at)
                .await? =>
            {
                Err(locked())
            }
            None => Err(invalid()),
        }
    }

    pub async fn refresh(&self, refresh_token: &str) -> TbResult<TokenPair> {
        let authenticated_at = chrono::Utc::now().timestamp_millis();
        let claims = self
            .factory
            .parse_typed(refresh_token, TokenType::Refresh)?;
        let user_id = claims.user_id()?;
        let user = self.load_user(user_id).await.map_err(|e| match e {
            TbError::NotFound(_) => TbError::Unauthenticated("token user no longer exists".into()),
            other => other,
        })?;
        let creds = self.load_enabled_credentials(user_id).await?;
        if creds
            .password_changed_ts
            .is_some_and(|changed_ts| claims.iat.saturating_mul(1000) < changed_ts)
        {
            return Err(TbError::Unauthenticated(
                "refresh token predates password change".into(),
            ));
        }
        self.issue_tokens_for_credentials(&user, &creds, authenticated_at)
    }

    pub async fn change_password(&self, user_id: Uuid, current: &str, new: &str) -> TbResult<()> {
        if new.is_empty() {
            return Err(TbError::InvalidInput(
                "new password must not be empty".into(),
            ));
        }
        let creds = self
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
        let password = hash_password(new)?;
        let changed_ts = chrono::Utc::now().timestamp_millis();
        self.creds_dao
            .update_password(user_id, &password, changed_ts, false)
            .await?
            .ok_or_else(|| TbError::NotFound(format!("credentials for user {user_id}")))?;
        Ok(())
    }

    /// 解析 access token 并加载当前用户。
    pub async fn get_user_from_token(&self, token: &str) -> TbResult<SecurityUser> {
        let claims = self.factory.parse_typed(token, TokenType::Access)?;
        let user = self
            .load_user(claims.user_id()?)
            .await
            .map_err(|e| match e {
                TbError::NotFound(_) => {
                    TbError::Unauthenticated("token user no longer exists".into())
                }
                other => other,
            })?;
        let creds = self.load_enabled_credentials(user.id).await?;
        if creds
            .password_changed_ts
            .is_some_and(|changed_ts| claims.iat.saturating_mul(1000) < changed_ts)
        {
            return Err(TbError::Unauthenticated(
                "access token predates password change".into(),
            ));
        }
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

    async fn load_enabled_credentials(&self, user_id: Uuid) -> TbResult<UserCredentials> {
        let creds = self
            .creds_dao
            .find_by_user_id(user_id)
            .await?
            .ok_or_else(|| TbError::Unauthenticated("user credentials not found".into()))?;
        if !creds.enabled {
            return Err(TbError::PermissionDenied(
                "user credentials are disabled".into(),
            ));
        }
        Ok(creds)
    }

    pub fn issue_tokens(&self, user: &User) -> TbResult<TokenPair> {
        let security_user = security_user(user);
        Ok(TokenPair {
            token: self.factory.create_access_token(&security_user)?,
            refresh_token: self.factory.create_refresh_token(&security_user)?,
        })
    }

    pub(crate) fn issue_tokens_for_credentials(
        &self,
        user: &User,
        credentials: &UserCredentials,
        authenticated_at: i64,
    ) -> TbResult<TokenPair> {
        let authenticated_at = authenticated_at.div_euclid(1000);
        let iat = credentials
            .password_changed_ts
            .map_or(authenticated_at, |changed_ts| {
                authenticated_at
                    .max(changed_ts.div_euclid(1000) + i64::from(changed_ts.rem_euclid(1000) != 0))
            });
        let security_user = security_user(user);
        Ok(TokenPair {
            token: self.factory.create_access_token_at(&security_user, iat)?,
            refresh_token: self.factory.create_refresh_token_at(&security_user, iat)?,
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
