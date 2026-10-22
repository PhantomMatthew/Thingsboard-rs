//! tb-core REST API 端到端集成测试。
//! 使用专属测试库 thingsboard_test（每次 DROP/CREATE），不污染开发库。

use std::sync::Arc;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_auth::JwtTokenFactory;
use tb_core::{AppState, build_router};

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
const TEST_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/thingsboard_test";

async fn start_server() -> String {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
        .await
        .expect("connect admin db");
    sqlx::query("DROP DATABASE IF EXISTS thingsboard_test WITH (FORCE)")
        .execute(&admin)
        .await
        .expect("drop test db");
    sqlx::query("CREATE DATABASE thingsboard_test")
        .execute(&admin)
        .await
        .expect("create test db");
    admin.close().await;

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(TEST_URL)
        .await
        .expect("connect test db");
    tb_dao::migrate(&pool).await.expect("migrate test db");

    let factory = Arc::new(JwtTokenFactory::new("test-secret", 900, 604800));
    let state = AppState::new(pool, factory);
    tb_core::seed::seed(&state).await.expect("seed");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

async fn login(client: &Client, base: &str, username: &str, password: &str) -> Value {
    let resp = client
        .post(format!("{base}/api/auth/login"))
        .json(&json!({ "username": username, "password": password }))
        .send()
        .await
        .expect("login request");
    assert_eq!(resp.status(), StatusCode::OK, "login {username}");
    resp.json().await.unwrap()
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

#[tokio::test]
async fn rest_api_end_to_end() {
    let base = start_server().await;
    // 禁系统代理，避免本机代理拦截 localhost 请求。
    let client = Client::builder().no_proxy().build().unwrap();

    // 1. sysadmin 登录 + 刷新 token + 当前用户
    let tokens = login(&client, &base, "sysadmin@thingsboard.org", "sysadmin").await;
    let sys_token = tokens["token"].as_str().unwrap().to_string();
    let refresh_token = tokens["refreshToken"].as_str().unwrap().to_string();

    let resp = client
        .post(format!("{base}/api/auth/token/refresh"))
        .json(&json!({ "refreshToken": refresh_token }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.json::<Value>().await.unwrap()["token"].is_string());

    let resp = client
        .get(format!("{base}/api/auth/user"))
        .header("Authorization", bearer(&sys_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let me: Value = resp.json().await.unwrap();
    assert_eq!(me["authority"], "SYS_ADMIN");

    // 未认证访问应 401
    let resp = client
        .get(format!("{base}/api/auth/user"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let err: Value = resp.json().await.unwrap();
    assert_eq!(err["status"], 401);
    assert!(err["message"].is_string());

    // 2. sysadmin 建租户
    let resp = client
        .post(format!("{base}/api/tenant"))
        .header("Authorization", bearer(&sys_token))
        .json(&json!({ "title": "Test Tenant", "email": "tenant@example.com" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tenant: Value = resp.json().await.unwrap();
    let tenant_id = tenant["id"].as_str().unwrap().to_string();

    // 分页列租户 + textSearch
    let resp = client
        .get(format!(
            "{base}/api/tenants?page=0&pageSize=10&textSearch=test"
        ))
        .header("Authorization", bearer(&sys_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: Value = resp.json().await.unwrap();
    assert!(page["totalElements"].as_i64().unwrap() >= 1);

    // 3. sysadmin 建 tenant admin（带密码直连路径）
    let resp = client
        .post(format!("{base}/api/user"))
        .header("Authorization", bearer(&sys_token))
        .json(&json!({
            "tenantId": tenant_id,
            "email": "tenant.admin@thingsboard.org",
            "authority": "TENANT_ADMIN",
            "password": "tenant",
            "firstName": "Tenant",
            "lastName": "Admin",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 4. tenant admin 登录
    let tokens = login(&client, &base, "tenant.admin@thingsboard.org", "tenant").await;
    let tenant_token = tokens["token"].as_str().unwrap().to_string();

    // tenant admin 无权访问租户管理
    let resp = client
        .get(format!("{base}/api/tenants?page=0&pageSize=10"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // 5. tenant admin 建设备（带 accessToken）
    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "name": "Test Device",
            "type": "thermostat",
            "label": "Room 1",
            "accessToken": "DEV_TOKEN_1",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let device: Value = resp.json().await.unwrap();
    let device_id = device["id"].as_str().unwrap().to_string();
    assert_eq!(device["tenantId"], tenant_id);

    // 6. 查设备凭证
    let resp = client
        .get(format!("{base}/api/device/{device_id}/credentials"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let creds: Value = resp.json().await.unwrap();
    assert_eq!(creds["credentialsId"], "DEV_TOKEN_1");
    assert_eq!(creds["credentialsType"], "ACCESS_TOKEN");

    // 7. 设备分页 + textSearch + types
    let resp = client
        .get(format!(
            "{base}/api/devices?page=0&pageSize=10&textSearch=test&sortProperty=name&sortOrder=ASC"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 1);

    let resp = client
        .get(format!("{base}/api/device/types"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.json::<Value>().await.unwrap(), json!(["thermostat"]));

    // 8. 建客户并分配设备
    let resp = client
        .post(format!("{base}/api/customer"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({ "title": "Customer A" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let customer: Value = resp.json().await.unwrap();
    let customer_id = customer["id"].as_str().unwrap().to_string();

    let resp = client
        .post(format!(
            "{base}/api/device/{device_id}/assign/{customer_id}"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.json::<Value>().await.unwrap()["customerId"],
        customer_id
    );

    // 9. 建 customer user 并登录
    let resp = client
        .post(format!("{base}/api/user"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "email": "customer.user@thingsboard.org",
            "authority": "CUSTOMER_USER",
            "customerId": customer_id,
            "password": "customer",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let tokens = login(&client, &base, "customer.user@thingsboard.org", "customer").await;
    let customer_token = tokens["token"].as_str().unwrap().to_string();

    // customer user 可读自己 customer 下的设备
    let resp = client
        .get(format!(
            "{base}/api/customer/{customer_id}/devices?page=0&pageSize=10"
        ))
        .header("Authorization", bearer(&customer_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.json::<Value>().await.unwrap()["totalElements"], 1);

    // 10. 权限负例：customer user 删设备 → 403
    let resp = client
        .delete(format!("{base}/api/device/{device_id}"))
        .header("Authorization", bearer(&customer_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let err: Value = resp.json().await.unwrap();
    assert_eq!(err["status"], 403);

    // customer user 访问 /api/devices（tenant 级列表）→ 403
    let resp = client
        .get(format!("{base}/api/devices?page=0&pageSize=10"))
        .header("Authorization", bearer(&customer_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // 11. customer user 改密并用新密码登录
    let resp = client
        .post(format!("{base}/api/auth/changePassword"))
        .header("Authorization", bearer(&customer_token))
        .json(&json!({ "currentPassword": "customer", "newPassword": "newpass" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    login(&client, &base, "customer.user@thingsboard.org", "newpass").await;

    // 12. 错误密码 → 401
    let resp = client
        .post(format!("{base}/api/auth/login"))
        .json(&json!({ "username": "tenant.admin@thingsboard.org", "password": "wrong" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // refresh token 不能当 access token 用
    let tokens = login(&client, &base, "tenant.admin@thingsboard.org", "tenant").await;
    let resp = client
        .get(format!("{base}/api/auth/user"))
        .header(
            "Authorization",
            bearer(tokens["refreshToken"].as_str().unwrap()),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
