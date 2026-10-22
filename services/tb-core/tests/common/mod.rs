//! 集成测试公共基建：专属测试库 thingsboard_test（每次 DROP/CREATE），不污染开发库。

#![allow(dead_code)]

use std::sync::Arc;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_auth::JwtTokenFactory;
use tb_core::{AppState, build_router};

pub const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
pub const TEST_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/thingsboard_test";

pub async fn start_server() -> String {
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

pub async fn login(client: &Client, base: &str, username: &str, password: &str) -> Value {
    let resp = client
        .post(format!("{base}/api/auth/login"))
        .json(&json!({ "username": username, "password": password }))
        .send()
        .await
        .expect("login request");
    assert_eq!(resp.status(), StatusCode::OK, "login {username}");
    resp.json().await.unwrap()
}

pub fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

/// 建租户 + tenant admin 并返回 (tenant_admin_token, tenant_id)。
pub async fn setup_tenant_admin(
    client: &Client,
    base: &str,
    tenant_title: &str,
    admin_email: &str,
) -> (String, String) {
    let tokens = login(client, base, "sysadmin@thingsboard.org", "sysadmin").await;
    let sys_token = tokens["token"].as_str().unwrap().to_string();

    let resp = client
        .post(format!("{base}/api/tenant"))
        .header("Authorization", bearer(&sys_token))
        .json(&json!({ "title": tenant_title }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tenant: Value = resp.json().await.unwrap();
    let tenant_id = tenant["id"].as_str().unwrap().to_string();

    let resp = client
        .post(format!("{base}/api/user"))
        .header("Authorization", bearer(&sys_token))
        .json(&json!({
            "tenantId": tenant_id,
            "email": admin_email,
            "authority": "TENANT_ADMIN",
            "password": "tenant",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let tokens = login(client, base, admin_email, "tenant").await;
    (tokens["token"].as_str().unwrap().to_string(), tenant_id)
}

/// tenant admin 建设备，返回 device_id。
pub async fn create_device(client: &Client, base: &str, token: &str, name: &str) -> String {
    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(token))
        .json(&json!({ "name": name }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create device {name}");
    let device: Value = resp.json().await.unwrap();
    device["id"].as_str().unwrap().to_string()
}
