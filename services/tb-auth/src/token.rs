//! JWT 签发与校验（HS256）。

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tb_common::{TbError, TbResult};
use tb_dao::models::Authority;
use uuid::Uuid;

/// 默认值仅供本地开发，生产环境必须通过 TB_JWT_SECRET 覆盖。
pub const DEFAULT_JWT_SECRET: &str = "tb-rs-dev-only-secret-do-not-use-in-production";
pub const DEFAULT_ACCESS_EXPIRATION_SEC: i64 = 900;
pub const DEFAULT_REFRESH_EXPIRATION_SEC: i64 = 604800;
pub const MIN_PRODUCTION_JWT_SECRET_BYTES: usize = 32;

pub fn is_production_env() -> bool {
    std::env::var("TB_ENV").is_ok_and(|value| value.eq_ignore_ascii_case("production"))
}

fn secret_from_config(production: bool, configured: Option<String>) -> TbResult<String> {
    let configured = configured.filter(|value| !value.trim().is_empty());
    if production {
        return configured
            .filter(|value| {
                value.len() >= MIN_PRODUCTION_JWT_SECRET_BYTES && value != DEFAULT_JWT_SECRET
            })
            .ok_or_else(|| {
                TbError::InvalidInput(
                    "TB_JWT_SECRET is required and must be at least 32 bytes in production".into(),
                )
            });
    }
    Ok(configured.unwrap_or_else(|| {
        tracing::warn!("TB_JWT_SECRET 未设置，使用 dev-only 默认值；生产环境必须覆盖");
        DEFAULT_JWT_SECRET.to_string()
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenType {
    Access,
    Refresh,
}

impl TokenType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Refresh => "refresh",
        }
    }
}

/// 已认证用户的安全上下文（写入 JWT claims）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecurityUser {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub customer_id: Option<Uuid>,
    pub email: String,
    pub authority: Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub tenant_id: String,
    #[serde(default)]
    pub email: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customer_id: Option<String>,
    pub authority: String,
    pub scopes: Vec<String>,
    #[serde(rename = "type")]
    pub token_type: TokenType,
    pub iat: i64,
    pub exp: i64,
}

impl Claims {
    pub fn user_id(&self) -> TbResult<Uuid> {
        self.sub
            .parse()
            .map_err(|_| TbError::Unauthenticated("invalid sub claim".into()))
    }

    pub fn to_security_user(&self) -> TbResult<SecurityUser> {
        Ok(SecurityUser {
            id: self.user_id()?,
            tenant_id: self
                .tenant_id
                .parse()
                .map_err(|_| TbError::Unauthenticated("invalid tenant_id claim".into()))?,
            customer_id: self
                .customer_id
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|_| TbError::Unauthenticated("invalid customer_id claim".into()))?,
            email: self.email.clone(),
            authority: self
                .authority
                .parse()
                .map_err(|_| TbError::Unauthenticated("invalid authority claim".into()))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JwtTokenFactory {
    secret: String,
    access_exp: i64,
    refresh_exp: i64,
}

impl JwtTokenFactory {
    pub fn new(secret: impl Into<String>, access_exp: i64, refresh_exp: i64) -> Self {
        Self {
            secret: secret.into(),
            access_exp,
            refresh_exp,
        }
    }

    /// 从环境变量构建：TB_JWT_SECRET / TB_JWT_ACCESS_EXPIRATION_SEC / TB_JWT_REFRESH_EXPIRATION_SEC。
    pub fn from_env() -> TbResult<Self> {
        let secret = secret_from_config(is_production_env(), std::env::var("TB_JWT_SECRET").ok())?;
        let access_exp = std::env::var("TB_JWT_ACCESS_EXPIRATION_SEC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_ACCESS_EXPIRATION_SEC);
        let refresh_exp = std::env::var("TB_JWT_REFRESH_EXPIRATION_SEC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_REFRESH_EXPIRATION_SEC);
        Ok(Self::new(secret, access_exp, refresh_exp))
    }

    pub fn create_access_token(&self, user: &SecurityUser) -> TbResult<String> {
        self.encode(user, TokenType::Access, self.access_exp)
    }

    pub fn create_refresh_token(&self, user: &SecurityUser) -> TbResult<String> {
        self.encode(user, TokenType::Refresh, self.refresh_exp)
    }

    pub(crate) fn create_access_token_at(&self, user: &SecurityUser, iat: i64) -> TbResult<String> {
        self.encode_at(user, TokenType::Access, iat, self.access_exp)
    }

    pub(crate) fn create_refresh_token_at(
        &self,
        user: &SecurityUser,
        iat: i64,
    ) -> TbResult<String> {
        self.encode_at(user, TokenType::Refresh, iat, self.refresh_exp)
    }

    fn encode(&self, user: &SecurityUser, token_type: TokenType, ttl: i64) -> TbResult<String> {
        self.encode_at(user, token_type, chrono::Utc::now().timestamp(), ttl)
    }

    fn encode_at(
        &self,
        user: &SecurityUser,
        token_type: TokenType,
        iat: i64,
        ttl: i64,
    ) -> TbResult<String> {
        let claims = Claims {
            sub: user.id.to_string(),
            tenant_id: user.tenant_id.to_string(),
            email: user.email.clone(),
            customer_id: user.customer_id.map(|c| c.to_string()),
            authority: user.authority.as_str().to_string(),
            scopes: vec![user.authority.as_str().to_string()],
            token_type,
            iat,
            exp: iat + ttl,
        };
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| TbError::Internal(format!("jwt encode: {e}")))
    }

    /// 校验签名与过期，返回 claims（不限制 token 类型）。
    pub fn parse(&self, token: &str) -> TbResult<Claims> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["exp", "iat", "sub"]);
        validation.leeway = 0;
        jsonwebtoken::decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )
        .map(|data| data.claims)
        .map_err(|e| TbError::Unauthenticated(format!("invalid token: {e}")))
    }

    pub fn parse_typed(&self, token: &str, expected: TokenType) -> TbResult<Claims> {
        let claims = self.parse(token)?;
        if claims.token_type != expected {
            return Err(TbError::Unauthenticated(format!(
                "expected {} token, got {}",
                expected.as_str(),
                claims.token_type.as_str()
            )));
        }
        Ok(claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user() -> SecurityUser {
        SecurityUser {
            id: Uuid::now_v7(),
            tenant_id: Uuid::now_v7(),
            customer_id: None,
            email: "a@b.c".into(),
            authority: Authority::TenantAdmin,
        }
    }

    #[test]
    fn access_token_roundtrip() {
        let factory = JwtTokenFactory::new("secret", 900, 604800);
        let token = factory.create_access_token(&user()).unwrap();
        let claims = factory.parse_typed(&token, TokenType::Access).unwrap();
        assert_eq!(claims.authority, "TENANT_ADMIN");
        assert!(factory.parse_typed(&token, TokenType::Refresh).is_err());
    }

    #[test]
    fn expired_token_rejected() {
        let factory = JwtTokenFactory::new("secret", -10, 604800);
        let token = factory.create_access_token(&user()).unwrap();
        assert!(factory.parse(&token).is_err());
    }

    #[test]
    fn wrong_secret_rejected() {
        let a = JwtTokenFactory::new("s1", 900, 604800);
        let b = JwtTokenFactory::new("s2", 900, 604800);
        let token = a.create_access_token(&user()).unwrap();
        assert!(b.parse(&token).is_err());
    }

    #[test]
    fn production_requires_a_strong_explicit_secret() {
        for secret in [
            None,
            Some(String::new()),
            Some("short".into()),
            Some(DEFAULT_JWT_SECRET.into()),
        ] {
            assert!(secret_from_config(true, secret).is_err());
        }
        assert_eq!(
            secret_from_config(true, Some("s".repeat(32))).unwrap(),
            "s".repeat(32)
        );
        assert_eq!(secret_from_config(false, None).unwrap(), DEFAULT_JWT_SECRET);
    }
}
