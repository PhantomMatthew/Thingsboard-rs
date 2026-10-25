//! OAuth2 登录（简化 OIDC 授权码流程）：公开客户端列表 + code 换 JWT。
//! 客户端必须显式绑定租户，且仅允许该租户的 CUSTOMER_USER；新用户使用 enabled
//! 随机密码凭证。email_verified 仅在 provider 明确返回 false 时拒绝。

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tb_common::{TbError, TbResult};
use tb_dao::Oauth2ClientDao;
use tb_dao::models::{Authority, Oauth2Client, User, UserCredentials};
use uuid::Uuid;

use crate::password::hash_password;
use crate::routes::{ApiError, AuthState};
use crate::service::{AuthService, TokenPair};

const OAUTH2_RESPONSE_LIMIT: usize = 1024 * 1024;

pub fn allow_insecure_oauth2_http() -> bool {
    std::env::var("TB_OAUTH2_ALLOW_INSECURE_HTTP")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(false)
}

pub struct ValidatedOAuth2Uri {
    url: reqwest::Url,
    addresses: Vec<SocketAddr>,
}

impl ValidatedOAuth2Uri {
    fn http_client(&self, development: &reqwest::Client) -> TbResult<reqwest::Client> {
        if self.addresses.is_empty() {
            return Ok(development.clone());
        }
        let host = self
            .url
            .host_str()
            .ok_or_else(|| TbError::InvalidInput("oauth2 URL has no host".into()))?;
        reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .resolve_to_addrs(host, &self.addresses)
            .build()
            .map_err(|e| TbError::Internal(format!("oauth2 HTTP client configuration: {e}")))
    }
}

pub fn validate_oauth2_browser_uri(name: &str, value: &str) -> TbResult<reqwest::Url> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| TbError::InvalidInput(format!("{name} must be an absolute HTTP(S) URL")))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(TbError::InvalidInput(format!(
            "{name} must be an absolute HTTP(S) URL"
        )));
    }
    if url.scheme() != "https" && !allow_insecure_oauth2_http() {
        return Err(TbError::InvalidInput(format!(
            "{name} must use HTTPS unless TB_OAUTH2_ALLOW_INSECURE_HTTP=true"
        )));
    }
    Ok(url)
}

pub async fn validate_oauth2_uri(name: &str, value: &str) -> TbResult<ValidatedOAuth2Uri> {
    validate_oauth2_uri_with_policy(name, value, allow_insecure_oauth2_http()).await
}

async fn validate_oauth2_uri_with_policy(
    name: &str,
    value: &str,
    allow_insecure: bool,
) -> TbResult<ValidatedOAuth2Uri> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| TbError::InvalidInput(format!("{name} must be an absolute HTTP(S) URL")))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(TbError::InvalidInput(format!(
            "{name} must be an absolute HTTP(S) URL"
        )));
    }
    if allow_insecure {
        return Ok(ValidatedOAuth2Uri {
            url,
            addresses: vec![],
        });
    }
    if url.scheme() != "https" {
        return Err(TbError::InvalidInput(format!(
            "{name} must use HTTPS unless TB_OAUTH2_ALLOW_INSECURE_HTTP=true"
        )));
    }
    let host = url.host_str().expect("checked above");
    let port = url
        .port_or_known_default()
        .expect("HTTP(S) has a default port");
    let addresses: Vec<_> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| TbError::InvalidInput(format!("{name} host cannot be resolved")))?
        .collect();
    if addresses.is_empty() {
        return Err(TbError::InvalidInput(format!(
            "{name} host cannot be resolved"
        )));
    }
    if addresses
        .iter()
        .any(|address| is_non_public_ip(address.ip()))
    {
        return Err(TbError::InvalidInput(format!(
            "{name} must not target a private, loopback, link-local, or reserved address"
        )));
    }
    Ok(ValidatedOAuth2Uri { url, addresses })
}

fn is_non_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.octets()[0] == 0
                || (ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
                || (ip.octets()[0] == 198 && (18..=19).contains(&ip.octets()[1]))
                || ip.octets()[0] >= 240
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || (ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|ip| is_non_public_ip(IpAddr::V4(ip)))
        }
    }
}

fn oauth2_tenant_id(client: &Oauth2Client) -> TbResult<Uuid> {
    client.tenant_id.ok_or_else(|| {
        TbError::InvalidInput("oauth2 client must be bound to a tenant for login".into())
    })
}

fn validate_oauth2_user(user: &User, tenant_id: Uuid) -> TbResult<()> {
    if user.tenant_id != tenant_id || user.authority != Authority::CustomerUser {
        return Err(TbError::PermissionDenied(
            "oauth2 user is not allowed by the client tenant and authority policy".into(),
        ));
    }
    Ok(())
}

/// 登录页公开信息（不含 secret 与内部 URI 明细）。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Oauth2ClientInfo {
    pub id: Uuid,
    pub title: String,
    pub client_id: String,
    pub authorization_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Oauth2LoginRequest {
    pub client_id: String,
    pub code: String,
    pub redirect_uri: String,
}

impl AuthService {
    pub fn oauth2_client_dao(&self) -> Oauth2ClientDao {
        Oauth2ClientDao::new(self.pool.clone())
    }

    /// 登录页公开列表：全部启用客户端，authorization_url 已拼好授权 query。
    pub async fn oauth2_enabled_clients(&self) -> TbResult<Vec<Oauth2ClientInfo>> {
        let clients = self.oauth2_client_dao().find_enabled().await?;
        Ok(clients.iter().map(client_info).collect())
    }

    /// 授权码登录：code → access_token → userinfo.email → 查/建用户 → 签发 JWT 对。
    pub async fn oauth2_login(&self, req: &Oauth2LoginRequest) -> TbResult<TokenPair> {
        let authenticated_at = chrono::Utc::now().timestamp_millis();
        let client = self
            .oauth2_client_dao()
            .find_by_client_id(&req.client_id)
            .await?
            .filter(|c| c.enabled)
            .ok_or_else(|| TbError::Unauthenticated("unknown or disabled oauth2 client".into()))?;
        if let Some(expected) = client.redirect_uri_template.as_deref()
            && req.redirect_uri != expected
        {
            return Err(TbError::InvalidInput(
                "redirect_uri does not match the registered value".into(),
            ));
        }
        let tenant_id = oauth2_tenant_id(&client)?;
        let token_uri = validate_oauth2_uri("tokenUri", &client.token_uri).await?;
        let user_info_uri = validate_oauth2_uri("userInfoUri", &client.user_info_uri).await?;
        let access_token = self.exchange_code(&client, req, token_uri).await?;
        let email = self.fetch_user_email(user_info_uri, &access_token).await?;
        let user = match self.user_dao.find_by_email(&email).await? {
            Some(user) => {
                validate_oauth2_user(&user, tenant_id)?;
                user
            }
            None => self.provision_oauth2_user(tenant_id, &email).await?,
        };
        let credentials = self
            .creds_dao
            .find_by_user_id(user.id)
            .await?
            .ok_or_else(|| TbError::Unauthenticated("user credentials not found".into()))?;
        if !credentials.enabled {
            return Err(TbError::PermissionDenied(
                "user credentials are disabled".into(),
            ));
        }
        self.issue_tokens_for_credentials(&user, &credentials, authenticated_at)
    }

    /// 按标准 form（client_secret 走 body）向 token_uri 换 access_token。
    async fn exchange_code(
        &self,
        client: &Oauth2Client,
        req: &Oauth2LoginRequest,
        token_uri: ValidatedOAuth2Uri,
    ) -> TbResult<String> {
        let mut form = vec![
            ("grant_type", "authorization_code".to_string()),
            ("code", req.code.clone()),
            ("redirect_uri", req.redirect_uri.clone()),
            ("client_id", client.client_id.clone()),
        ];
        if let Some(secret) = &client.client_secret {
            form.push(("client_secret", secret.clone()));
        }
        let resp = token_uri
            .http_client(&self.http)?
            .post(token_uri.url.clone())
            .form(&form)
            .send()
            .await
            .map_err(|e| TbError::Internal(format!("oauth2 token request failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let message = format!("oauth2 token endpoint rejected the code: {status}");
            return Err(
                if matches!(
                    status,
                    reqwest::StatusCode::BAD_REQUEST
                        | reqwest::StatusCode::UNAUTHORIZED
                        | reqwest::StatusCode::FORBIDDEN
                ) {
                    TbError::Unauthenticated(message)
                } else {
                    TbError::Internal(message)
                },
            );
        }
        let body = read_oauth2_json(resp, "token").await?;
        body["access_token"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| TbError::Internal("oauth2 token response missing access_token".into()))
    }

    async fn fetch_user_email(
        &self,
        user_info_uri: ValidatedOAuth2Uri,
        access_token: &str,
    ) -> TbResult<String> {
        let resp = user_info_uri
            .http_client(&self.http)?
            .get(user_info_uri.url.clone())
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| TbError::Internal(format!("oauth2 userinfo request failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let message = format!("oauth2 userinfo endpoint rejected the access token: {status}");
            return Err(
                if matches!(
                    status,
                    reqwest::StatusCode::BAD_REQUEST
                        | reqwest::StatusCode::UNAUTHORIZED
                        | reqwest::StatusCode::FORBIDDEN
                ) {
                    TbError::Unauthenticated(message)
                } else {
                    TbError::Internal(message)
                },
            );
        }
        let body = read_oauth2_json(resp, "userinfo").await?;
        if body["email_verified"].as_bool() == Some(false) {
            return Err(TbError::Unauthenticated(
                "oauth2 user email is not verified".into(),
            ));
        }
        body["email"]
            .as_str()
            .filter(|e| !e.is_empty())
            .map(str::to_string)
            .ok_or_else(|| TbError::Unauthenticated("oauth2 user info has no email".into()))
    }

    async fn provision_oauth2_user(&self, tenant_id: Uuid, email: &str) -> TbResult<User> {
        let user = User {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id,
            customer_id: None,
            authority: Authority::CustomerUser,
            email: email.into(),
            first_name: None,
            last_name: None,
            phone: None,
            additional_info: None,
        };
        let user = self.user_dao.save(&user).await?;
        let credentials = UserCredentials {
            id: Uuid::now_v7(),
            created_time: 0,
            user_id: user.id,
            enabled: true,
            password: Some(hash_password(&Uuid::now_v7().to_string())?),
            activate_token: None,
            reset_token: None,
            failed_login_attempts: 0,
            last_login_ts: None,
            password_changed_ts: None,
        };
        self.creds_dao.save(&credentials).await?;
        tracing::info!(email, tenant_id = %tenant_id, "provisioned oauth2 user");
        Ok(user)
    }
}

async fn read_oauth2_json(
    mut response: reqwest::Response,
    endpoint: &str,
) -> TbResult<serde_json::Value> {
    if response
        .content_length()
        .is_some_and(|length| length > OAUTH2_RESPONSE_LIMIT as u64)
    {
        return Err(TbError::InvalidInput(format!(
            "oauth2 {endpoint} response exceeds {OAUTH2_RESPONSE_LIMIT} bytes"
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| TbError::Internal(format!("oauth2 {endpoint} response read failed: {e}")))?
    {
        if body.len() + chunk.len() > OAUTH2_RESPONSE_LIMIT {
            return Err(TbError::InvalidInput(format!(
                "oauth2 {endpoint} response exceeds {OAUTH2_RESPONSE_LIMIT} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body)
        .map_err(|e| TbError::Internal(format!("oauth2 {endpoint} response not json: {e}")))
}

fn client_info(client: &Oauth2Client) -> Oauth2ClientInfo {
    Oauth2ClientInfo {
        id: client.id,
        title: client.title.clone(),
        client_id: client.client_id.clone(),
        authorization_url: build_authorization_url(client),
    }
}

fn build_authorization_url(client: &Oauth2Client) -> String {
    let Ok(mut url) = reqwest::Url::parse(&client.authorization_uri) else {
        return client.authorization_uri.clone();
    };
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("client_id", &client.client_id);
        q.append_pair("response_type", "code");
        if let Some(scope) = &client.scope {
            q.append_pair("scope", scope);
        }
        if let Some(redirect) = &client.redirect_uri_template {
            q.append_pair("redirect_uri", redirect);
        }
    }
    url.to_string()
}

/// 公开 OAuth2 路由（不含 /api/oauth2 前缀，由外层 nest）。
pub fn router(state: AuthState) -> Router {
    Router::new()
        .route("/clients", get(list_clients))
        .route("/login", post(oauth2_login))
        .with_state(state)
}

async fn list_clients(
    State(state): State<AuthState>,
) -> Result<Json<Vec<Oauth2ClientInfo>>, ApiError> {
    Ok(Json(state.auth_service.oauth2_enabled_clients().await?))
}

async fn oauth2_login(
    State(state): State<AuthState>,
    Json(req): Json<Oauth2LoginRequest>,
) -> Result<Json<TokenPair>, ApiError> {
    Ok(Json(state.auth_service.oauth2_login(&req).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(tenant_id: Uuid) -> User {
        User {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id,
            customer_id: None,
            authority: Authority::CustomerUser,
            email: "customer@example.com".into(),
            first_name: None,
            last_name: None,
            phone: None,
            additional_info: None,
        }
    }

    #[test]
    fn oauth2_user_policy_requires_matching_tenant_customer_user() {
        let tenant_id = Uuid::now_v7();
        let mut allowed = user(tenant_id);
        validate_oauth2_user(&allowed, tenant_id).unwrap();
        allowed.customer_id = Some(Uuid::now_v7());
        validate_oauth2_user(&allowed, tenant_id).unwrap();

        let other_tenant = user(Uuid::now_v7());
        assert!(validate_oauth2_user(&other_tenant, tenant_id).is_err());
        for authority in [Authority::SysAdmin, Authority::TenantAdmin] {
            let mut administrator = user(tenant_id);
            administrator.authority = authority;
            assert!(validate_oauth2_user(&administrator, tenant_id).is_err());
        }
    }

    #[tokio::test]
    async fn oauth2_uris_are_secure_by_default_with_explicit_development_override() {
        validate_oauth2_uri_with_policy("tokenUri", "https://8.8.8.8/token", false)
            .await
            .unwrap();
        for uri in [
            "http://8.8.8.8/token",
            "https://127.0.0.1/token",
            "https://10.0.0.1/token",
            "https://169.254.169.254/latest/meta-data",
            "https://[::1]/token",
            "https://[fe80::1]/token",
            "https://[fc00::1]/token",
            "https://localhost/token",
        ] {
            assert!(
                validate_oauth2_uri_with_policy("tokenUri", uri, false)
                    .await
                    .is_err(),
                "{uri}"
            );
        }
        for uri in ["file:///etc/passwd", "ftp://example.com/token", "/relative"] {
            assert!(
                validate_oauth2_uri_with_policy("tokenUri", uri, true)
                    .await
                    .is_err(),
                "{uri}"
            );
        }
        validate_oauth2_uri_with_policy("tokenUri", "http://127.0.0.1/token", true)
            .await
            .unwrap();
    }
}
