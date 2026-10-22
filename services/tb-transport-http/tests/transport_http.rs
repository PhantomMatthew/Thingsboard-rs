//! 端到端：HTTP transport 上行 → Kafka → tb-core ingest 落库 → REST 读回。

mod common;

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};

use common::*;

#[tokio::test]
async fn http_transport_telemetry_and_attributes() {
    let _guard = INTEGRATION_LOCK.lock().await;
    // 禁系统代理，避免本机代理拦截 localhost 请求。
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, transport) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "Transport Tenant", "transport-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "http-device", "TEST_TOKEN").await;

    // 遥测上行：纯 {k:v}（首轮可能撞上 Kafka consumer 冷启动 → 500，轮询重试）
    poll_until(Duration::from_secs(25), async || {
        let resp = client
            .post(format!("{transport}/api/v1/TEST_TOKEN/telemetry"))
            .json(&json!({"temperature": 42}))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
    })
    .await;

    // 轮询 REST 读回（Kafka 异步）
    poll_until(Duration::from_secs(5), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=temperature"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        if resp.status() != StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body["temperature"][0]["value"] == json!(42)
    })
    .await;

    // 带 ts/values 结构 + 数组形式
    let resp = client
        .post(format!("{transport}/api/v1/TEST_TOKEN/telemetry"))
        .json(&json!([{"ts": 1700000000000i64, "values": {"humidity": 55.5}}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    poll_until(Duration::from_secs(5), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=humidity"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        if resp.status() != StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body["humidity"][0]["value"] == json!(55.5)
            && body["humidity"][0]["ts"] == json!(1700000000000i64)
    })
    .await;

    // 属性上行（CLIENT_SCOPE）
    let resp = client
        .post(format!("{transport}/api/v1/TEST_TOKEN/attributes"))
        .json(&json!({"firmware": "1.0", "active": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    poll_until(Duration::from_secs(5), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/attributes/CLIENT_SCOPE"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        if resp.status() != StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body.as_array().is_some_and(|attrs| {
            attrs
                .iter()
                .any(|a| a["key"] == "firmware" && a["value"] == "1.0")
                && attrs.iter().any(|a| a["key"] == "active" && a["value"] == true)
        })
    })
    .await;

    // 设备侧拉属性（Phase 5 起返回真实 client/shared 分组）
    let resp = client
        .get(format!(
            "{transport}/api/v1/TEST_TOKEN/attributes?clientKeys=firmware"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({"client": {"firmware": "1.0"}, "shared": {}})
    );

    // RPC 上行（无响应等待 → 202）
    let resp = client
        .post(format!("{transport}/api/v1/TEST_TOKEN/rpc"))
        .json(&json!({"method": "getStatus", "params": {}, "timeout": 5000}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // claim 未实现 → 501
    let resp = client
        .post(format!("{transport}/api/v1/TEST_TOKEN/claim"))
        .json(&json!({"secretKey": "x", "durationMs": 1000}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn http_transport_rejects_bad_token() {
    let _guard = INTEGRATION_LOCK.lock().await;
    // 禁系统代理，避免本机代理拦截 localhost 请求。
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (_core, transport) = start_servers().await;

    // 首轮可能撞上 Kafka consumer 冷启动 → 500，轮询直到拿到明确的 401
    poll_until(Duration::from_secs(25), async || {
        let resp = client
            .post(format!("{transport}/api/v1/NO_SUCH_TOKEN/telemetry"))
            .json(&json!({"temperature": 1}))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::UNAUTHORIZED
    })
    .await;

    let resp = client
        .get(format!("{transport}/api/v1/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

