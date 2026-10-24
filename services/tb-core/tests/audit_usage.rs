//! 集成测试：审计日志（login + 实体 CRUD）与 API 用量统计。

mod common;

use prost::Message as _;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tb_proto::core::UsageStatsMsg;
use tb_queue::{KafkaQueueProducer, QueueProducer, TbTopic};

use common::{
    INTEGRATION_LOCK, bearer, create_device, login, setup_tenant_admin, start_server,
    start_server_with_state, start_servers_with_rule_engine,
};

/// 审计/用量写入是异步的：轮询直到条件满足或超时。
async fn wait_for<Fut>(mut check: impl FnMut() -> Fut) -> Value
where
    Fut: std::future::Future<Output = Option<Value>>,
{
    for _ in 0..50 {
        if let Some(v) = check().await {
            return v;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("condition not met within timeout");
}

#[tokio::test]
async fn audit_log_login_and_entity_crud() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let base = start_server().await;
    let client = Client::new();

    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "AuditTenant", "audit-admin@test.io").await;

    // login 失败 → FAILURE 审计（tenant 未知，sysadmin 视角可见）
    let resp = client
        .post(format!("{base}/api/auth/login"))
        .json(&json!({ "username": "audit-admin@test.io", "password": "wrong" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 建/删设备 → CREATED / DELETED 审计
    let device_id = create_device(&client, &base, &token, "audit-dev").await;
    let resp = client
        .delete(format!("{base}/api/device/{device_id}"))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // TENANT_ADMIN 查本租户审计：按 entityId + actionTypes 过滤
    let logs = wait_for(|| async {
        let resp = client
            .get(format!(
                "{base}/api/audit/logs?entityId={device_id}&actionTypes=CREATED,DELETED&pageSize=50"
            ))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        (body["totalElements"].as_i64() == Some(2)).then_some(body)
    })
    .await;
    let types: Vec<&str> = logs["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["actionType"].as_str().unwrap())
        .collect();
    assert!(types.contains(&"CREATED") && types.contains(&"DELETED"));
    assert_eq!(logs["data"][0]["entityId"].as_str().unwrap(), device_id);
    assert_eq!(logs["data"][0]["entityType"].as_str().unwrap(), "DEVICE");

    // TENANT_ADMIN 也能看到本租户的 LOGIN 成功记录
    let logs = wait_for(|| async {
        let resp = client
            .get(format!(
                "{base}/api/audit/logs?actionTypes=LOGIN&pageSize=50"
            ))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        (body["totalElements"].as_i64().unwrap_or(0) >= 1).then_some(body)
    })
    .await;
    assert!(
        logs["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["actionStatus"] == "SUCCESS")
    );

    // SYS_ADMIN 全量视角能看到 FAILURE 的 login（tenant 未知时记 nil）
    let sys = login(&client, &base, "sysadmin@thingsboard.org", "sysadmin").await;
    let sys_token = sys["token"].as_str().unwrap().to_string();
    let logs = wait_for(|| async {
        let resp = client
            .get(format!(
                "{base}/api/audit/logs?actionTypes=LOGIN&pageSize=100"
            ))
            .header("Authorization", bearer(&sys_token))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        body["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["actionStatus"] == "FAILURE")
            .then_some(body)
    })
    .await;
    assert!(logs["totalElements"].as_i64().unwrap() >= 2);

    // 越权：tenant admin 只能看本租户（过滤生效，不会看到 nil tenant 的 FAILURE 记录）
    let resp = client
        .get(format!(
            "{base}/api/audit/logs?actionTypes=LOGIN&pageSize=100"
        ))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["data"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["actionStatus"] == "SUCCESS")
    );

    let _ = tenant_id;
}

#[tokio::test]
async fn api_usage_counts() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let base = start_server().await;
    let client = Client::new();

    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "UsageTenant", "usage-admin@test.io").await;
    let device_id = create_device(&client, &base, &token, "usage-dev").await;

    // 遥测 REST POST：2 个 key → transportMsgs += 2
    let resp = client
        .post(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/timeseries"
        ))
        .header("Authorization", bearer(&token))
        .json(&json!({"temp": 21.5, "hum": 40}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // TENANT_ADMIN 查本租户用量（内存计数即时可见，无需等 60s 落库）
    let report = wait_for(|| async {
        let resp = client
            .get(format!("{base}/api/usage/{tenant_id}"))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        (body["transportMsgs"].as_i64() == Some(2)).then_some(body)
    })
    .await;
    assert!(report["restRequests"].as_i64().unwrap() > 0);

    // 跨租户拒绝：另一个租户的 admin 查该租户 → 403
    let (other_token, _) =
        setup_tenant_admin(&client, &base, "OtherTenant", "other-admin@test.io").await;
    let resp = client
        .get(format!("{base}/api/usage/{tenant_id}"))
        .header("Authorization", bearer(&other_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // SYS_ADMIN 可查任意租户
    let sys = login(&client, &base, "sysadmin@thingsboard.org", "sysadmin").await;
    let resp = client
        .get(format!("{base}/api/usage/{tenant_id}"))
        .header("Authorization", bearer(sys["token"].as_str().unwrap()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.json::<Value>().await.unwrap()["transportMsgs"]
            .as_i64()
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn usage_stats_consumer_accumulates_transport_and_re() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let (base, _pool, state) = start_server_with_state().await;
    let client = Client::new();
    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "UsageConsumer", "usage-consumer@test.io").await;
    let group = format!("tb-core-main-{}", uuid::Uuid::now_v7().simple());
    let _core = tb_core::core_consumer::start_core_consumer_for_group(state, &group).await;
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let producer = KafkaQueueProducer::new(&brokers, "tb-usage-consumer-test").unwrap();
    let baseline = wait_for(|| async {
        let usage = UsageStatsMsg {
            tenant_id: tenant_id.clone(),
            transport_msgs: 2,
            re_msgs: 3,
        };
        producer
            .send(
                TbTopic::USAGE_STATS,
                &tenant_id,
                bytes::Bytes::from(usage.encode_to_vec()),
            )
            .await
            .unwrap();
        let resp = client
            .get(format!("{base}/api/usage/{tenant_id}"))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        (body["transportMsgs"].as_i64().unwrap_or(0) > 0
            && body["reMsgs"].as_i64().unwrap_or(0) > 0)
            .then_some(body)
    })
    .await;
    let base_transport = baseline["transportMsgs"].as_i64().unwrap();
    let base_re = baseline["reMsgs"].as_i64().unwrap();

    let usage = UsageStatsMsg {
        tenant_id: tenant_id.clone(),
        transport_msgs: 1,
        re_msgs: 2,
    };
    producer
        .send(
            TbTopic::USAGE_STATS,
            &tenant_id,
            bytes::Bytes::from(usage.encode_to_vec()),
        )
        .await
        .unwrap();
    let report = wait_for(|| async {
        let resp = client
            .get(format!("{base}/api/usage/{tenant_id}"))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        (body["transportMsgs"].as_i64() == Some(base_transport + 1)
            && body["reMsgs"].as_i64() == Some(base_re + 2))
        .then_some(body)
    })
    .await;
    assert_eq!(report["transportMsgs"], base_transport + 1);
    assert_eq!(report["reMsgs"], base_re + 2);
}

#[tokio::test]
async fn rule_engine_usage_counts_once_without_legacy_ingest() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let (base, transport) = start_servers_with_rule_engine().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "UsageRuleEngine", "usage-re@test.io").await;

    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(&token))
        .json(&json!({ "name": "usage-re-device", "accessToken": "USAGE_RE_TOKEN" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .post(format!("{transport}/api/v1/USAGE_RE_TOKEN/telemetry"))
        .json(&json!({"temperature": 25, "humidity": 60}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let report = wait_for(|| async {
        let resp = client
            .get(format!("{base}/api/usage/{tenant_id}"))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        (body["transportMsgs"].as_i64() == Some(2) && body["reMsgs"].as_i64() == Some(2))
            .then_some(body)
    })
    .await;
    assert_eq!(report["transportMsgs"], 2);
    assert_eq!(report["reMsgs"], 2);
}
