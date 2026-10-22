//! 集成测试公共基建：专属测试库（每次 DROP/CREATE），
//! 同进程起 tb-core REST + transport-api server + ingest + tb-transport-mqtt。

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_auth::JwtTokenFactory;
use tb_core::{AppState, build_router};

pub const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
pub const KAFKA_BROKERS: &str = "localhost:29092";

/// 返回 (tb-core REST base, mqtt 监听地址)。每个调用方独立测试库，可并行。
pub async fn start_servers() -> (String, SocketAddr) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            "tb_core=debug,tb_transport_api=debug,tb_transport_mqtt=debug,tb_queue=debug",
        )
        .try_init();
    let uuid = uuid::Uuid::now_v7().simple().to_string();
    let run_id = &uuid[20..];
    let db_name = format!("tb_test_{run_id}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
        .await
        .expect("connect admin db");
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .expect("drop test db");
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin)
        .await
        .expect("create test db");
    admin.close().await;

    let test_url = format!("postgres://thingsboard:thingsboard@localhost:5433/{db_name}");
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&test_url)
        .await
        .expect("connect test db");
    tb_dao::migrate(&pool).await.expect("migrate test db");

    let factory = Arc::new(JwtTokenFactory::new("test-secret", 900, 604800));
    let state = AppState::new(pool, factory);
    tb_core::seed::seed(&state).await.expect("seed");

    // 每次起服用独立 consumer group：避免上一轮测试残留的幽灵成员拖慢 rebalance。
    let _transport_api = tb_core::transport_api::start_transport_api_for_group(
        state.clone(),
        &format!("tb-core-transport-api-{run_id}"),
    )
    .await;
    let _ingest = tb_core::telemetry_ingest::start_telemetry_ingest_for_group(
        state.clone(),
        &format!("tb-core-ingest-{run_id}"),
    )
    .await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let core_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });

    let node_id = format!("test-mqtt-{run_id}");
    let mqtt_state = tb_transport_mqtt::TransportState::new(KAFKA_BROKERS, &node_id)
        .expect("mqtt transport state");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let mqtt_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tb_transport_mqtt::serve(listener, mqtt_state)
            .await
            .unwrap();
    });

    // 等 Kafka consumer group 完成 join（auto.offset.reset=latest，未 join 会丢早到的消息）。
    tokio::time::sleep(Duration::from_secs(2)).await;

    (format!("http://{core_addr}"), mqtt_addr)
}

pub async fn login(client: &Client, base: &str, username: &str, password: &str) -> Value {
    let resp = client
        .post(format!("{base}/api/auth/login"))
        .json(&json!({ "username": username, "password": password }))
        .send()
        .await
        .expect("login request");
    if resp.status() != StatusCode::OK {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        panic!("login {username} -> {status}: {body}");
    }
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

/// tenant admin 建带固定 access token 的设备，返回 device_id。
pub async fn create_device_with_token(
    client: &Client,
    base: &str,
    token: &str,
    name: &str,
    access_token: &str,
) -> String {
    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(token))
        .json(&json!({ "name": name, "accessToken": access_token }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create device {name}");
    let device: Value = resp.json().await.unwrap();
    device["id"].as_str().unwrap().to_string()
}

/// 轮询直到闭包返回 true（Kafka 异步链路用），超时 panic。
pub async fn poll_until(timeout: Duration, mut f: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    loop {
        if f().await {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "poll_until timed out after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
