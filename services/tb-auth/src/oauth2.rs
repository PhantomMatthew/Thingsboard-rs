//! OAuth2 登录（简化 OIDC 授权码流程）：公开客户端列表 + code 换 JWT。
//! 用户 provisioning：oauth2_client.tenant_id 非空则用之，否则落到最早创建的非
//! System 租户；无任何可用租户时报错。新用户为 CUSTOMER_USER + enabled 随机密码凭证。

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
        let client = self
            .oauth2_client_dao()
            .find_by_client_id(&req.client_id)
            .await?
            .filter(|c| c.enabled)
            .ok_or_else(|| TbError::Unauthenticated("unknown or disabled oauth2 client".into()))?;
        let access_token = self.exchange_code(&client, req).await?;
        let email = self.fetch_user_email(&client, &access_token).await?;
        let user = match self.user_dao.find_by_email(&email).await? {
            Some(u) => u,
            None => self.provision_oauth2_user(&client, &email).await?,
        };
        self.issue_tokens(&user)
    }

    /// 按标准 form（client_secret 走 body）向 token_uri 换 access_token。
    async fn exchange_code(
        &self,
        client: &Oauth2Client,
        req: &Oauth2LoginRequest,
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
        let resp = self
            .http
            .post(&client.token_uri)
            .form(&form)
            .send()
            .await
            .map_err(|e| TbError::Internal(format!("oauth2 token request failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(TbError::Unauthenticated(format!(
                "oauth2 token endpoint rejected the code: {status}"
            )));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| TbError::Internal(format!("oauth2 token response not json: {e}")))?;
        body["access_token"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| TbError::Internal("oauth2 token response missing access_token".into()))
    }

    async fn fetch_user_email(&self, client: &Oauth2Client, access_token: &str) -> TbResult<String> {
        let resp = self
            .http
            .get(&client.user_info_uri)
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| TbError::Internal(format!("oauth2 userinfo request failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(TbError::Unauthenticated(
                "oauth2 userinfo endpoint rejected the access token".into(),
            ));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| TbError::Internal(format!("oauth2 userinfo response not json: {e}")))?;
        body["email"]
            .as_str()
            .filter(|e| !e.is_empty())
            .map(str::to_string)
            .ok_or_else(|| TbError::Unauthenticated("oauth2 user info has no email".into()))
    }

    async fn provision_oauth2_user(&self, client: &Oauth2Client, email: &str) -> TbResult<User> {
        let tenant_id = match client.tenant_id {
            Some(t) => t,
            None => self.default_tenant_id().await?,
        };
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
        };
        self.creds_dao.save(&credentials).await?;
        tracing::info!(email, tenant_id = %tenant_id, "provisioned oauth2 user");
        Ok(user)
    }

    /// 平台级客户端的 provisioning 目标：最早创建的非 System 租户。
    async fn default_tenant_id(&self) -> TbResult<Uuid> {
        sqlx::query_scalar(
            "SELECT id FROM tenant WHERE title <> 'System' ORDER BY created_time ASC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)?
        .ok_or_else(|| {
            TbError::InvalidInput("no tenant available for oauth2 user provisioning".into())
        })
    }
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
