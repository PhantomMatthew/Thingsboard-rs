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
    start_server_with_pool().await.0
}

/// 同 start_server，但额外返回测试库连接池（测试需直接走 DAO 造数时）。
pub async fn start_server_with_pool() -> (String, PgPool) {
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
    let state = AppState::new(pool.clone(), factory);
    tb_core::seed::seed(&state).await.expect("seed");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    (format!("http://{addr}"), pool)
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

/// 进程内测试串行化锁：共享 Kafka 下并行测试的多个 core 实例会互相抢答
/// transport-api 校验请求（各自测试库数据不同），导致随机 valid=false 串扰。
pub static INTEGRATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 同 start_servers_with_transport，但额外起 telemetry ingest 与 rule engine（Main 队列，
/// 独立 group、1s 链缓存 TTL），使 transport 上行遥测完整走 Kafka → 规则链 → 落库。
pub async fn start_servers_with_rule_engine() -> (String, String) {
    let (base, _pool) = start_server_with_pool().await;
    let uuid = uuid::Uuid::now_v7().simple().to_string();
    let run_id = &uuid[20..];
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(TEST_URL)
        .await
        .expect("connect test db");
    let factory = Arc::new(JwtTokenFactory::new("test-secret", 900, 604800));
    let state = AppState::new(pool.clone(), factory);
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
    let _rule_engine = tb_rule_engine_server::start_rule_engine_for_group(
        &brokers,
        pool,
        "Main",
        &format!("tb-rule-engine-{run_id}"),
        std::time::Duration::from_secs(1),
    )
    .await;

    let node_id = format!("test-http-{run_id}");
    let transport_state =
        tb_transport_http::TransportState::new(&brokers, &node_id).expect("http transport state");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let transport_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, tb_transport_http::build_router(transport_state))
            .await
            .unwrap();
    });

    // 等 Kafka consumer group 完成 join（auto.offset.reset=latest，未 join 会丢早到的消息）。
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    (base, format!("http://{transport_addr}"))
}

/// 轮询直到闭包返回 true（Kafka 异步链路用），超时 panic。
pub async fn poll_until(timeout: std::time::Duration, mut f: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    loop {
        if f().await {
            return;
        }
        assert!(
            start.elapsed() < timeout,
            "poll_until timed out after {timeout:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// 同 start_server，但额外起 transport-api server（Kafka）+ tb-transport-http。
/// 返回 (core REST base, http transport base)。
pub async fn start_servers_with_transport() -> (String, String) {
    let (base, _pool) = start_server_with_pool().await;
    let uuid = uuid::Uuid::now_v7().simple().to_string();
    let run_id = &uuid[20..];
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    // 注意：transport-api server 依赖 AppState，需重新构建同库 state。
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(TEST_URL)
        .await
        .expect("connect test db");
    let factory = Arc::new(JwtTokenFactory::new("test-secret", 900, 604800));
    let state = AppState::new(pool, factory);
    let _transport_api = tb_core::transport_api::start_transport_api_for_group(
        state.clone(),
        &format!("tb-core-transport-api-{run_id}"),
    )
    .await;

    let node_id = format!("test-http-{run_id}");
    let transport_state =
        tb_transport_http::TransportState::new(&brokers, &node_id).expect("http transport state");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let transport_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, tb_transport_http::build_router(transport_state))
            .await
            .unwrap();
    });

    // 等 Kafka consumer group 完成 join（auto.offset.reset=latest，未 join 会丢早到的消息）。
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    (base, format!("http://{transport_addr}"))
}
