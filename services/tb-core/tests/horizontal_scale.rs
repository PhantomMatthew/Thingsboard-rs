//! Phase 9b 水平扩展验证：2 个 tb-core 实例（同库、transport-api/ingest/core 同 consumer
//! group）+ 2 个 tb-rule-engine 实例（同 group），经单实例 HTTP transport 发 50 条遥测
//! （10 设备 × 5 条，按设备打散 Kafka 分区），断言全部落库、latest 正确。
//!
//! 说明：transport-api 校验在两个 core 实例间的分发无法从外部直接观测——Kafka 同 group
//! 下每条校验请求天然只有一个实例应答（由 broker 分区分配决定），本测试通过“两实例共享
//! 同库、任一实例应答均有效 + 全量消息无丢失”间接覆盖；跨实例读（base1 建数、base2 断言）
//! 验证两实例服务同一数据集。

mod common;

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tb_auth::JwtTokenFactory;
use tb_core::{AppState, build_router};

use common::{ADMIN_URL, INTEGRATION_LOCK, TEST_URL, bearer, poll_until, setup_tenant_admin};

const DEVICES: usize = 10;
const MSGS_PER_DEVICE: usize = 5;

async fn serve_core(state: AppState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn two_core_two_rule_engine_horizontal_scale() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let run_uuid = uuid::Uuid::now_v7().simple().to_string();
    let run_id = &run_uuid[20..];
    let group = |name: &str| format!("{name}-scale-{run_id}");

    // 独立测试库（与其他测试文件串行执行，互不影响）
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

    let connect = || async {
        PgPoolOptions::new()
            .max_connections(5)
            .connect(TEST_URL)
            .await
            .expect("connect test db")
    };
    let pool1 = connect().await;
    tb_dao::migrate(&pool1).await.expect("migrate test db");
    let pool2 = connect().await;

    // 两个 core 实例：同库、同 JWT secret（token 互通）、不同 HTTP 端口
    let factory = Arc::new(JwtTokenFactory::new("test-secret", 900, 604800));
    let state1 = AppState::new_with_node_id(
        pool1.clone(),
        factory.clone(),
        format!("scale-core-1-{run_id}"),
    )
    .await
    .expect("app state 1");
    tb_core::seed::seed(&state1).await.expect("seed");
    let base1 = serve_core(state1.clone()).await;
    let state2 = AppState::new_with_node_id(
        pool2.clone(),
        factory.clone(),
        format!("scale-core-2-{run_id}"),
    )
    .await
    .expect("app state 2");
    let base2 = serve_core(state2.clone()).await;
    assert_ne!(base1, base2);

    // 每个后台服务两实例同 consumer group：分区在两实例间分配
    let _core_handles = [
        tb_core::transport_api::start_transport_api_for_group(
            state1.clone(),
            &group("tb-core-transport-api"),
        )
        .await,
        tb_core::transport_api::start_transport_api_for_group(
            state2.clone(),
            &group("tb-core-transport-api"),
        )
        .await,
        tb_core::telemetry_ingest::start_telemetry_ingest_for_group(
            state1.clone(),
            &group("tb-core-ingest"),
        )
        .await,
        tb_core::telemetry_ingest::start_telemetry_ingest_for_group(
            state2.clone(),
            &group("tb-core-ingest"),
        )
        .await,
        tb_core::core_consumer::start_core_consumer_for_group(
            state1.clone(),
            &group("tb-core-main"),
        )
        .await,
        tb_core::core_consumer::start_core_consumer_for_group(
            state2.clone(),
            &group("tb-core-main"),
        )
        .await,
    ];
    let _re1 = tb_rule_engine_server::start_rule_engine_for_group(
        &brokers,
        pool1.clone(),
        "Main",
        &group("tb-rule-engine"),
        Duration::from_secs(1),
    )
    .await;
    let _re2 = tb_rule_engine_server::start_rule_engine_for_group(
        &brokers,
        pool2.clone(),
        "Main",
        &group("tb-rule-engine"),
        Duration::from_secs(1),
    )
    .await;

    // 单实例 HTTP transport
    let node_id = format!("scale-http-{run_id}");
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
    let transport = format!("http://{transport_addr}");

    // 建租户 + 10 设备（经实例 1）
    let (tenant_token, _tenant_id) =
        setup_tenant_admin(&client, &base1, "Scale Tenant", "t.admin@scale.io").await;
    let mut device_ids = Vec::with_capacity(DEVICES);
    for i in 0..DEVICES {
        let resp = client
            .post(format!("{base1}/api/device"))
            .header("Authorization", bearer(&tenant_token))
            .json(&json!({ "name": format!("scale-device-{i}"), "accessToken": format!("SCALE_TOKEN_{i}") }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        device_ids.push(
            resp.json::<Value>().await.unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }

    // 预热：2 实例 consumer group join + rebalance 较慢，先轮询到首条遥测落库
    // （auto.offset.reset=latest，join 前的消息会丢）
    poll_until(Duration::from_secs(60), async || {
        let resp = client
            .post(format!("{transport}/api/v1/SCALE_TOKEN_0/telemetry"))
            .json(&json!({"warmup": 1}))
            .send()
            .await
            .unwrap();
        if resp.status() != StatusCode::OK {
            return false;
        }
        let resp = client
            .get(format!(
                "{base2}/api/telemetry/DEVICE/{}/values/timeseries?keys=warmup",
                device_ids[0]
            ))
            .header("Authorization", bearer(&tenant_token))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
            && resp.json::<Value>().await.unwrap()["warmup"][0]["value"] == json!(1)
    })
    .await;

    // 50 条遥测：设备 i 的第 j 条为 {"k{j}": i*100+j}（唯一值便于校验无丢失/无错配）
    for (i, _) in device_ids.iter().enumerate() {
        for j in 0..MSGS_PER_DEVICE {
            let resp = client
                .post(format!("{transport}/api/v1/SCALE_TOKEN_{i}/telemetry"))
                .json(&json!({ format!("k{j}"): (i * MSGS_PER_DEVICE + j) as i64 }))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    // 跨实例断言（经实例 2 读）：每台设备 5 个 key 的 latest 全部正确
    let keys = (0..MSGS_PER_DEVICE)
        .map(|j| format!("k{j}"))
        .collect::<Vec<_>>()
        .join(",");
    poll_until(Duration::from_secs(60), async || {
        for (i, device_id) in device_ids.iter().enumerate() {
            let resp = client
                .get(format!(
                    "{base2}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys={keys}"
                ))
                .header("Authorization", bearer(&tenant_token))
                .send()
                .await
                .unwrap();
            if resp.status() != StatusCode::OK {
                return false;
            }
            let body: Value = resp.json().await.unwrap();
            for j in 0..MSGS_PER_DEVICE {
                let expected = json!((i * MSGS_PER_DEVICE + j) as i64);
                if body[format!("k{j}")][0]["value"] != expected {
                    return false;
                }
            }
        }
        true
    })
    .await;
}
