//! OAuth2：本地假 provider 走完整 code→JWT 流程及身份/出站安全负例。
//! 单测试文件单 #[tokio::test]（共用 thingsboard_test 库，避免并发 DROP DATABASE）。

mod common;

use std::collections::HashMap;

use axum::http::{HeaderMap, StatusCode as AxumStatus};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Json as AxumJson, Router};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use tb_dao::models::{Authority, Customer, User, UserCredentials};
use tb_dao::{CustomerDao, UserCredentialsDao, UserDao};
use uuid::Uuid;

use common::{bearer, login, setup_tenant_admin, start_server_with_pool};

struct EnvGuard {
    name: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var(name).ok();
        unsafe { std::env::set_var(name, value) };
        Self { name, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        unsafe {
            if let Some(value) = &self.previous {
                std::env::set_var(self.name, value);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }
}

async fn start_fake_provider() -> String {
    async fn token_form(Form(form): Form<HashMap<String, String>>) -> Response {
        if form.get("grant_type").map(String::as_str) != Some("authorization_code") {
            return AxumStatus::BAD_REQUEST.into_response();
        }
        match form.get("code").map(String::as_str) {
            Some("bad") => (
                AxumStatus::BAD_REQUEST,
                AxumJson(json!({"error": "invalid_grant"})),
            )
                .into_response(),
            Some(code) => AxumJson(json!({
                "access_token": format!("{code}-at"),
                "token_type": "Bearer"
            }))
            .into_response(),
            None => AxumStatus::BAD_REQUEST.into_response(),
        }
    }
    async fn userinfo(headers: HeaderMap) -> Response {
        let auth = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let identity = match auth {
            "Bearer good-at" => json!({"sub": "123", "email": "oauth.user@tb.io"}),
            "Bearer sysadmin-at" => json!({
                "sub": "sysadmin",
                "email": "sysadmin@thingsboard.org",
                "email_verified": true
            }),
            "Bearer other-tenant-at" => json!({
                "sub": "other",
                "email": "other.tenant@oauth2.io",
                "email_verified": true
            }),
            "Bearer disabled-at" => json!({
                "sub": "disabled",
                "email": "disabled@oauth2.io",
                "email_verified": true
            }),
            "Bearer customer-at" => json!({
                "sub": "customer",
                "email": "assigned.customer@oauth2.io",
                "email_verified": true
            }),
            "Bearer unverified-at" => json!({
                "sub": "unverified",
                "email": "unverified@oauth2.io",
                "email_verified": false
            }),
            "Bearer platform-new-at" => json!({
                "sub": "platform-new",
                "email": "platform.new@oauth2.io"
            }),
            "Bearer noemail-at" => json!({"sub": "123"}),
            _ => return AxumStatus::UNAUTHORIZED.into_response(),
        };
        AxumJson(identity).into_response()
    }
    let app = Router::new()
        .route("/token", post(token_form))
        .route("/userinfo", get(userinfo))
        .route(
            "/token-redirect",
            post(|| async { Redirect::temporary("/token") }),
        )
        .route(
            "/userinfo-redirect",
            get(|| async { Redirect::temporary("/userinfo") }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn create_oauth2_user(
    pool: &PgPool,
    tenant_id: Uuid,
    customer_id: Option<Uuid>,
    email: &str,
    enabled: bool,
) {
    let user = UserDao::new(pool.clone())
        .save(&User {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id,
            customer_id,
            authority: Authority::CustomerUser,
            email: email.into(),
            first_name: None,
            last_name: None,
            phone: None,
            additional_info: None,
        })
        .await
        .unwrap();
    UserCredentialsDao::new(pool.clone())
        .save(&UserCredentials {
            id: Uuid::now_v7(),
            created_time: 0,
            user_id: user.id,
            enabled,
            password: None,
            activate_token: None,
            reset_token: None,
            failed_login_attempts: 0,
            last_login_ts: None,
            password_changed_ts: None,
        })
        .await
        .unwrap();
}

async fn login_with_code(
    client: &Client,
    base: &str,
    client_id: &str,
    code: &str,
) -> reqwest::Response {
    client
        .post(format!("{base}/api/oauth2/login"))
        .json(&json!({
            "clientId": client_id,
            "code": code,
            "redirectUri": "http://app/cb"
        }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn oauth2_login_end_to_end() {
    let insecure_http = EnvGuard::set("TB_OAUTH2_ALLOW_INSECURE_HTTP", "true");
    let provider = start_fake_provider().await;
    let (base, pool) = start_server_with_pool().await;
    let client = Client::builder().no_proxy().build().unwrap();

    let (tenant_token, tenant_id) =
        setup_tenant_admin(&client, &base, "OAuth2 Tenant", "t.admin@oauth2.io").await;
    let tenant_id = Uuid::parse_str(&tenant_id).unwrap();
    let (_, other_tenant_id) = setup_tenant_admin(
        &client,
        &base,
        "Other OAuth2 Tenant",
        "other.admin@oauth2.io",
    )
    .await;
    let other_tenant_id = Uuid::parse_str(&other_tenant_id).unwrap();
    let sys_token = login(&client, &base, "sysadmin@thingsboard.org", "sysadmin").await["token"]
        .as_str()
        .unwrap()
        .to_string();

    create_oauth2_user(&pool, other_tenant_id, None, "other.tenant@oauth2.io", true).await;
    create_oauth2_user(&pool, tenant_id, None, "disabled@oauth2.io", false).await;
    let customer = CustomerDao::new(pool.clone())
        .save(&Customer {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id,
            title: "OAuth2 Customer".into(),
            country: None,
            state: None,
            city: None,
            address: None,
            address2: None,
            zip: None,
            phone: None,
            email: None,
            additional_info: None,
        })
        .await
        .unwrap();
    create_oauth2_user(
        &pool,
        tenant_id,
        Some(customer.id),
        "assigned.customer@oauth2.io",
        true,
    )
    .await;

    // 平台客户端仍可管理/展示，但不能隐式选择租户或登录。
    let resp = client
        .post(format!("{base}/api/oauth2/client"))
        .header("Authorization", bearer(&sys_token))
        .json(&json!({
            "title": "Fake SSO",
            "clientId": "fake",
            "clientSecret": "s3cret",
            "authorizationUri": format!("{provider}/authorize"),
            "tokenUri": format!("{provider}/token"),
            "userInfoUri": format!("{provider}/userinfo"),
            "redirectUriTemplate": "http://app/cb",
            "scope": "email profile",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let platform_client: Value = resp.json().await.unwrap();
    let platform_client_id = platform_client["id"].as_str().unwrap().to_string();
    assert_eq!(platform_client["tenantId"], Value::Null);

    let resp = client
        .get(format!("{base}/api/oauth2/clients"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Value = resp.json().await.unwrap();
    let item = list
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["clientId"] == "fake")
        .expect("public list should contain fake client");
    let auth_url = item["authorizationUrl"].as_str().unwrap();
    assert!(auth_url.starts_with(&format!("{provider}/authorize?")));
    assert!(auth_url.contains("client_id=fake"));
    assert!(auth_url.contains("response_type=code"));
    assert!(auth_url.contains("redirect_uri=http%3A%2F%2Fapp%2Fcb"));
    assert!(item.get("clientSecret").is_none());

    let resp = login_with_code(&client, &base, "fake", "platform-new").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(
        UserDao::new(pool.clone())
            .find_by_email("platform.new@oauth2.io")
            .await
            .unwrap()
            .is_none()
    );

    let tenant_client_body = json!({
        "tenantId": tenant_id,
        "title": "Tenant SSO",
        "clientId": "tenant-fake",
        "clientSecret": "s3cret",
        "authorizationUri": format!("{provider}/authorize"),
        "tokenUri": format!("{provider}/token"),
        "userInfoUri": format!("{provider}/userinfo"),
        "redirectUriTemplate": "http://app/cb",
        "scope": "email profile",
    });
    let resp = client
        .post(format!("{base}/api/oauth2/client"))
        .header("Authorization", bearer(&tenant_token))
        .json(&tenant_client_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = client
        .post(format!("{base}/api/oauth2/client"))
        .header("Authorization", bearer(&sys_token))
        .json(&tenant_client_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tenant_client: Value = resp.json().await.unwrap();
    let tenant_client_id = tenant_client["id"].as_str().unwrap().to_string();

    // userinfo 未声明 email_verified 时保持兼容，新用户只进入客户端租户。
    let resp = login_with_code(&client, &base, "tenant-fake", "good").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let pair: Value = resp.json().await.unwrap();
    let access = pair["token"].as_str().unwrap();
    let resp = client
        .get(format!("{base}/api/auth/user"))
        .header("Authorization", bearer(access))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user: Value = resp.json().await.unwrap();
    assert_eq!(user["email"], "oauth.user@tb.io");
    assert_eq!(user["authority"], "CUSTOMER_USER");
    assert_eq!(user["tenantId"].as_str().unwrap(), tenant_id.to_string());
    let user_id = user["id"].as_str().unwrap().to_string();

    let resp = login_with_code(&client, &base, "tenant-fake", "good").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let pair: Value = resp.json().await.unwrap();
    let resp = client
        .get(format!("{base}/api/auth/user"))
        .header("Authorization", bearer(pair["token"].as_str().unwrap()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["id"], user_id);

    // 已分配 customer 的用户保持 customer 归属。
    let resp = login_with_code(&client, &base, "tenant-fake", "customer").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let pair: Value = resp.json().await.unwrap();
    let resp = client
        .get(format!("{base}/api/auth/user"))
        .header("Authorization", bearer(pair["token"].as_str().unwrap()))
        .send()
        .await
        .unwrap();
    let assigned_user: Value = resp.json().await.unwrap();
    assert_eq!(
        assigned_user["tenantId"].as_str().unwrap(),
        tenant_id.to_string()
    );
    assert_eq!(
        assigned_user["customerId"].as_str().unwrap(),
        customer.id.to_string()
    );

    for (code, status) in [
        ("bad", StatusCode::UNAUTHORIZED),
        ("noemail", StatusCode::UNAUTHORIZED),
        ("unverified", StatusCode::UNAUTHORIZED),
        ("sysadmin", StatusCode::FORBIDDEN),
        ("other-tenant", StatusCode::FORBIDDEN),
        ("disabled", StatusCode::FORBIDDEN),
    ] {
        let resp = login_with_code(&client, &base, "tenant-fake", code).await;
        assert_eq!(resp.status(), status, "code {code}");
    }
    assert!(
        UserDao::new(pool.clone())
            .find_by_email("unverified@oauth2.io")
            .await
            .unwrap()
            .is_none()
    );

    // 307 会保留方法和请求体；若跟随重定向，两个场景都会成功。
    for (client_id, token_path, userinfo_path) in [
        ("token-redirect", "/token-redirect", "/userinfo"),
        ("userinfo-redirect", "/token", "/userinfo-redirect"),
    ] {
        let resp = client
            .post(format!("{base}/api/oauth2/client"))
            .header("Authorization", bearer(&sys_token))
            .json(&json!({
                "tenantId": tenant_id,
                "title": client_id,
                "clientId": client_id,
                "authorizationUri": format!("{provider}/authorize"),
                "tokenUri": format!("{provider}{token_path}"),
                "userInfoUri": format!("{provider}{userinfo_path}"),
                "redirectUriTemplate": "http://app/cb",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = login_with_code(&client, &base, client_id, "good").await;
        assert_eq!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{client_id}"
        );
    }

    let resp = client
        .get(format!("{base}/api/oauth2/clients?pageSize=10&page=0"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 3);
    assert!(
        page["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["tenantId"] == tenant_id.to_string())
    );

    let resp = client
        .get(format!("{base}/api/oauth2/clients"))
        .header("Authorization", "Bearer invalid")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let tenant_user = UserDao::new(pool.clone())
        .find_by_email("t.admin@oauth2.io")
        .await
        .unwrap()
        .unwrap();
    let credentials_dao = UserCredentialsDao::new(pool.clone());
    let mut credentials = credentials_dao
        .find_by_user_id(tenant_user.id)
        .await
        .unwrap()
        .unwrap();
    credentials.enabled = false;
    credentials_dao.save(&credentials).await.unwrap();
    let resp = client
        .get(format!("{base}/api/oauth2/clients"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    credentials.enabled = true;
    credentials_dao.save(&credentials).await.unwrap();

    let resp = client
        .get(format!("{base}/api/oauth2/client/{platform_client_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = client
        .get(format!("{base}/api/oauth2/client/{platform_client_id}"))
        .header("Authorization", bearer(&sys_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 禁用客户端：不出现在公开列表，登录拒绝。
    let resp = client
        .post(format!("{base}/api/oauth2/client"))
        .header("Authorization", bearer(&sys_token))
        .json(&json!({
            "id": platform_client_id,
            "title": "Fake SSO",
            "clientId": "fake",
            "authorizationUri": format!("{provider}/authorize"),
            "tokenUri": format!("{provider}/token"),
            "userInfoUri": format!("{provider}/userinfo"),
            "enabled": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{base}/api/oauth2/clients"))
        .send()
        .await
        .unwrap();
    assert!(
        !resp
            .json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["clientId"] == "fake")
    );
    let resp = login_with_code(&client, &base, "fake", "good").await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let resp = client
        .delete(format!("{base}/api/oauth2/client/{platform_client_id}"))
        .header("Authorization", bearer(&sys_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{base}/api/oauth2/client/{platform_client_id}"))
        .header("Authorization", bearer(&sys_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let resp = client
        .delete(format!("{base}/api/oauth2/client/{tenant_client_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = client
        .delete(format!("{base}/api/oauth2/client/{tenant_client_id}"))
        .header("Authorization", bearer(&sys_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 关闭显式开发开关后，管理端也拒绝本地 HTTP 目标。
    drop(insecure_http);
    let _secure_default = EnvGuard::set("TB_OAUTH2_ALLOW_INSECURE_HTTP", "false");
    let resp = client
        .post(format!("{base}/api/oauth2/client"))
        .header("Authorization", bearer(&sys_token))
        .json(&tenant_client_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let resp = login_with_code(&client, &base, "no-such", "good").await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
