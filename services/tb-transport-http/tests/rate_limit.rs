//! E2E：租户级传输限流——HTTP transport 超限返回 429。

mod common;

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::json;

use common::*;

#[tokio::test]
async fn tenant_rate_limit_returns_429() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, transport) = start_servers_with_rate_limit(3).await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "RateLimit Tenant", "ratelimit-tenant@tb.io").await;
    create_device_with_token(&client, &core, &admin_token, "rl-device", "RL_TOKEN").await;

    // 首轮可能撞上 Kafka consumer 冷启动 → 500（validate 超时，不耗令牌），轮询预热。
    poll_until(Duration::from_secs(25), async || {
        let resp = client
            .post(format!("{transport}/api/v1/RL_TOKEN/telemetry"))
            .json(&json!({"temperature": 1}))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
    })
    .await;

    // 等桶 refill 满（3/s），再连发 6 条：前 3 条放行，后 3 条 429。
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut ok = 0;
    let mut limited = 0;
    for i in 0..6 {
        let resp = client
            .post(format!("{transport}/api/v1/RL_TOKEN/telemetry"))
            .json(&json!({"temperature": i}))
            .send()
            .await
            .unwrap();
        match resp.status() {
            StatusCode::OK => ok += 1,
            StatusCode::TOO_MANY_REQUESTS => limited += 1,
            other => panic!("unexpected status {other}"),
        }
    }
    assert_eq!(ok, 3, "桶容量 3 应放行 3 条");
    assert_eq!(limited, 3, "超限的 3 条应 429");
}
