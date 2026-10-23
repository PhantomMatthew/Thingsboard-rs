//! Phase 9b OAuth2：本地假 provider（axum /token + /userinfo）走完整 code→JWT 流程。
//! 单测试文件单 #[tokio::test]（共用 thingsboard_test 库，避免并发 DROP DATABASE）。

mod common;

use std::collections::HashMap;

use axum::http::{HeaderMap, StatusCode as AxumStatus};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Form, Json as AxumJson, Router};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use common::{bearer, login, setup_tenant_admin, start_server};

/// 假 OAuth2 provider：code "good"→带 email 用户；"noemail"→无 email 用户；其余→400。
async fn start_fake_provider() -> String {
    async fn token_form(Form(form): Form<HashMap<String, String>>) -> Response {
        if form.get("grant_type").map(String::as_str) != Some("authorization_code") {
            return AxumStatus::BAD_REQUEST.into_response();
        }
        match form.get("code").map(String::as_str) {
            Some("good") => AxumJson(json!({"access_token": "fake-at", "token_type": "Bearer"}))
                .into_response(),
            Some("noemail") => {
                AxumJson(json!({"access_token": "noemail-at", "token_type": "Bearer"}))
                    .into_response()
            }
            _ => (
                AxumStatus::BAD_REQUEST,
                AxumJson(json!({"error": "invalid_grant"})),
            )
                .into_response(),
        }
    }
    async fn userinfo(headers: HeaderMap) -> Response {
        let auth = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        match auth {
            "Bearer fake-at" => {
                AxumJson(json!({"sub": "123", "email": "oauth.user@tb.io"})).into_response()
            }
            "Bearer noemail-at" => AxumJson(json!({"sub": "123"})).into_response(),
            _ => AxumStatus::UNAUTHORIZED.into_response(),
        }
    }
    let app = Router::new()
        .route("/token", post(token_form))
        .route("/userinfo", get(userinfo));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn oauth2_login_end_to_end() {
    let provider = start_fake_provider().await;
    let base = start_server().await;
    let client = Client::builder().no_proxy().build().unwrap();

    // 先建一个租户：平台级客户端 provisioning 应落到这个（最早非 System）租户。
    let (tenant_token, tenant_id) =
        setup_tenant_admin(&client, &base, "OAuth2 Tenant", "t.admin@oauth2.io").await;
    let sys_token = login(&client, &base, "sysadmin@thingsboard.org", "sysadmin").await["token"]
        .as_str()
        .unwrap()
        .to_string();

    // 1. SYS_ADMIN 建平台级客户端
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

    // 2. 公开列表（无认证）：最小字段 + 拼好的授权 URL
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

    // 3. 带 admin token 的列表 → 分页管理视图
    let resp = client
        .get(format!("{base}/api/oauth2/clients?pageSize=10&page=0"))
        .header("Authorization", bearer(&sys_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 1);
    assert_eq!(page["data"][0]["clientId"], "fake");

    // 4. 完整 code → JWT：自动 provisioning 到默认租户
    let resp = client
        .post(format!("{base}/api/oauth2/login"))
        .json(&json!({"clientId": "fake", "code": "good", "redirectUri": "http://app/cb"}))
        .send()
        .await
        .unwrap();
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
    assert_eq!(user["tenantId"].as_str().unwrap(), tenant_id);
    let user_id = user["id"].as_str().unwrap().to_string();

    // 5. 再次登录 → 同一用户（不重复 provisioning）
    let resp = client
        .post(format!("{base}/api/oauth2/login"))
        .json(&json!({"clientId": "fake", "code": "good", "redirectUri": "http://app/cb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let pair: Value = resp.json().await.unwrap();
    let resp = client
        .get(format!("{base}/api/auth/user"))
        .header("Authorization", bearer(pair["token"].as_str().unwrap()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["id"], user_id);

    // 6. 负例：bad code / 无 email / 未知 clientId
    for (code, what) in [("bad", "rejected code"), ("noemail", "missing email")] {
        let resp = client
            .post(format!("{base}/api/oauth2/login"))
            .json(&json!({"clientId": "fake", "code": code, "redirectUri": "http://app/cb"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{what}");
    }
    let resp = client
        .post(format!("{base}/api/oauth2/login"))
        .json(&json!({"clientId": "no-such", "code": "good", "redirectUri": "http://app/cb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 7. 禁用客户端：不出现在公开列表，登录拒绝
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
    let resp = client
        .post(format!("{base}/api/oauth2/login"))
        .json(&json!({"clientId": "fake", "code": "good", "redirectUri": "http://app/cb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 8. TENANT_ADMIN 管理本租户客户端（tenantId 强制覆写；看不到平台级）
    let resp = client
        .post(format!("{base}/api/oauth2/client"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "title": "Tenant SSO",
            "clientId": "tenant-fake",
            "authorizationUri": format!("{provider}/authorize"),
            "tokenUri": format!("{provider}/token"),
            "userInfoUri": format!("{provider}/userinfo"),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tenant_client: Value = resp.json().await.unwrap();
    assert_eq!(tenant_client["tenantId"].as_str().unwrap(), tenant_id);
    let tenant_client_id = tenant_client["id"].as_str().unwrap().to_string();

    let resp = client
        .get(format!("{base}/api/oauth2/clients?pageSize=10&page=0"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 1);
    assert_eq!(page["data"][0]["clientId"], "tenant-fake");

    // tenant admin 访问平台级客户端 → 403；sysadmin → 200
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

    // 9. 绑定租户的客户端 provisioning 落到客户端租户（新 email 用户）
    let resp = client
        .post(format!("{base}/api/oauth2/login"))
        .json(&json!({"clientId": "tenant-fake", "code": "good", "redirectUri": "http://app/cb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 10. DELETE：sysadmin 删平台级客户端 → GET 404
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

    // tenant admin 删除本租户客户端
    let resp = client
        .delete(format!("{base}/api/oauth2/client/{tenant_client_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
