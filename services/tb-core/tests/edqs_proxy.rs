//! EDQS 代理端点：core 认证 + tenantId 强制 → 转发 tb-edqs。
//! 起真实 edqs 实例（同测试库 + 独立 consumer group），遥测经 REST 落库后
//! edqs 事件经 Kafka 到达，代理查询轮询命中。

mod common;

use std::time::Duration;

use common::*;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn edqs_proxy_entity_data_query() {
    let _guard = INTEGRATION_LOCK.lock().await;

    // 先占位 edqs 端口（AppState 构造时读 TB_EDQS_URL），core 起来后再在同一 listener 上起 edqs。
    let edqs_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let edqs_addr = edqs_listener.local_addr().unwrap();
    unsafe {
        std::env::set_var("TB_EDQS_URL", format!("http://{edqs_addr}"));
        std::env::set_var("TB_EDQS_API_KEY", "edqs-proxy-test-key");
    }

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    tb_queue::admin::ensure_topic(&brokers, tb_queue::TbTopic::EDQS_EVENTS, 10)
        .await
        .unwrap();

    let base = start_server().await;
    let client = reqwest::Client::new();
    let (token, tenant_id) = setup_tenant_admin(&client, &base, "EDQS Proxy T", "edqs@t.io").await;
    let device_id = create_device(&client, &base, &token, "EDQS Proxy Device").await;

    // REST 落遥测 + 属性（同时发 edqs 事件）。
    let resp = client
        .post(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/timeseries"
        ))
        .header("Authorization", bearer(&token))
        .json(&json!({"temperature": 42}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .post(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/attributes/SERVER_SCOPE"
        ))
        .header("Authorization", bearer(&token))
        .json(&json!({"region": "eu"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 起 edqs：快照（含上面的数据）+ Kafka 消费。
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(TEST_URL)
        .await
        .expect("connect test db");
    let edqs_state = tb_edqs::EdqsState::default().with_api_key("edqs-proxy-test-key");
    tb_edqs::snapshot::load_snapshot(&pool, &edqs_state)
        .await
        .unwrap();
    let group = format!("tb-edqs-proxy-test-{}", uuid::Uuid::now_v7().simple());
    let consumer =
        tb_edqs::consumer::start_consumer_for_group(&brokers, &group, edqs_state.clone()).await;
    tokio::spawn(async move {
        axum::serve(edqs_listener, tb_edqs::build_router(edqs_state))
            .await
            .unwrap();
    });

    // 无认证 → 401。
    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .json(&json!({"tenantId": tenant_id, "entityFilter": {"type": "ALL"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 代理查询：temperature LATEST GT 40 → 命中设备；body 不带 tenantId 也应强制为 JWT 租户。
    let query_body = json!({
        "entityFilter": {"type": "ENTITY_TYPE", "entityType": "DEVICE"},
        "keyFilters": [{"key": "temperature", "source": "LATEST", "predicate": {"op": "GT", "value": 40}}],
        "pageLink": {"page": 0, "pageSize": 10},
        "fields": [
            {"key": "name", "source": "ENTITY_FIELD"},
            {"key": "temperature", "source": "LATEST"},
            {"key": "region", "source": "SERVER"}
        ]
    });
    let mut found = false;
    for _ in 0..75 {
        let resp = client
            .post(format!("{base}/api/edqs/query/entityData"))
            .header("Authorization", bearer(&token))
            .json(&query_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        if body["totalElements"] == 1 {
            assert_eq!(body["data"][0]["entityId"], device_id);
            assert_eq!(body["data"][0]["values"]["name"], "EDQS Proxy Device");
            assert_eq!(body["data"][0]["values"]["temperature"], 42);
            assert_eq!(body["data"][0]["values"]["region"], "eu");
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(found, "device did not appear in edqs proxy query");

    // 新遥测经 Kafka 事件到达（快照之后的数据）。
    let resp = client
        .post(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/timeseries"
        ))
        .header("Authorization", bearer(&token))
        .json(&json!({"humidity": 55}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let humidity_query = json!({
        "entityFilter": {"type": "ENTITY_IDS", "ids": [device_id]},
        "keyFilters": [{"key": "humidity", "source": "LATEST", "predicate": {"op": "GTE", "value": 50}}],
        "pageLink": {"page": 0, "pageSize": 10},
        "fields": [{"key": "humidity", "source": "LATEST"}]
    });
    let mut updated = false;
    for _ in 0..75 {
        let resp = client
            .post(format!("{base}/api/edqs/query/entityData"))
            .header("Authorization", bearer(&token))
            .json(&humidity_query)
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        if body["totalElements"] == 1 && body["data"][0]["values"]["humidity"] == 55 {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(updated, "post-snapshot telemetry did not reach edqs");

    consumer.abort();
}
