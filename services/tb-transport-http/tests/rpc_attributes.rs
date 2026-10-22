//! Phase 5 HTTP 端到端：GET attributes 同步查询（经 transport-api）、
//! GET rpc 长轮询接收服务端 RPC + POST rpc/{id} 响应回流。

mod common;

use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use common::*;

/// e) GET attributes 同步读回（client/shared 分组 + keys 过滤）+
/// GET rpc 长轮询往返 + 超时空 200。
/// 单测试：同进程多实例共享 tb_transport.api.requests，并发会互相抢答导致 401 抖动。
#[tokio::test]
async fn http_attributes_and_rpc_long_poll() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (core, http) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "HTTP Attr Tenant", "http-attr@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "http-attr-dev", "HATTR_TOKEN")
            .await;

    // CLIENT_SCOPE 属性上行（经 rule engine topic → ingest 落库）；
    // transport-api 冷启动期可能 401/500，轮询到成功。
    poll_until(Duration::from_secs(25), async || {
        client
            .post(format!("{http}/api/v1/HATTR_TOKEN/attributes"))
            .json(&json!({"firmware": "2.0", "serial": 42}))
            .send()
            .await
            .unwrap()
            .status()
            == StatusCode::OK
    })
    .await;

    // SHARED_SCOPE 由服务端 REST 写入
    let resp = client
        .post(format!(
            "{core}/api/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE"
        ))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"targetTemp": 60}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    poll_until(Duration::from_secs(15), async || {
        let resp = client
            .get(format!("{http}/api/v1/HATTR_TOKEN/attributes"))
            .send()
            .await
            .unwrap();
        if resp.status() != StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body["client"]["firmware"] == "2.0"
            && body["client"]["serial"] == 42
            && body["shared"]["targetTemp"] == 60
    })
    .await;

    // keys 过滤
    let resp = client
        .get(format!(
            "{http}/api/v1/HATTR_TOKEN/attributes?clientKeys=firmware&sharedKeys=targetTemp"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["client"], json!({"firmware": "2.0"}));
    assert_eq!(body["shared"], json!({"targetTemp": 60}));

    // 无效 token → 401
    let resp = client
        .get(format!("{http}/api/v1/NO_SUCH/attributes"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // ---- GET rpc 长轮询 ----

    // 超时路径：无 RPC 时（测试基建把长轮询调为 5s）返回空 200
    let resp = client
        .get(format!("{http}/api/v1/HATTR_TOKEN/rpc"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.json::<Value>().await.unwrap(), json!({}));

    // 第一轮长轮询：等 core 注册表建立（twoway 404=未就绪，504=已送达但无人响应）。
    let long_poll = {
        let client = client.clone();
        let http = http.clone();
        tokio::spawn(async move {
            client
                .get(format!("{http}/api/v1/HATTR_TOKEN/rpc"))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap()
        })
    };
    poll_until(Duration::from_secs(30), async || {
        let (status, _) = rest_twoway(&client, &core, &admin_token, &device_id, 1000).await;
        status != StatusCode::NOT_FOUND
    })
    .await;
    // 注册已就绪；上面最后一次 twoway 可能正好被长轮询接住，丢弃该轮结果。
    let _ = long_poll.await.unwrap();

    // 第二轮：完整往返（长轮询会话注册有 Kafka 延迟，404/空轮则重试）
    let start = std::time::Instant::now();
    let (request, twoway) = loop {
        let long_poll = {
            let client = client.clone();
            let http = http.clone();
            tokio::spawn(async move {
                client
                    .get(format!("{http}/api/v1/HATTR_TOKEN/rpc"))
                    .send()
                    .await
                    .unwrap()
                    .json::<Value>()
                    .await
                    .unwrap()
            })
        };
        // 给长轮询会话一点注册时间，再发 twoway
        tokio::time::sleep(Duration::from_millis(500)).await;
        let twoway = {
            let client = client.clone();
            let core = core.clone();
            let token = admin_token.clone();
            let device_id = device_id.clone();
            tokio::spawn(
                async move { rest_twoway(&client, &core, &token, &device_id, 10000).await },
            )
        };
        let request = long_poll.await.unwrap();
        if request.get("id").is_some() {
            break (request, twoway);
        }
        let _ = twoway.await;
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "long-poll never received rpc"
        );
    };
    let request_id = request["id"].as_i64().expect("rpc request id");
    assert_eq!(request["method"], "getStatus");
    assert_eq!(request["params"], json!({"v": 1}));

    let resp = client
        .post(format!("{http}/api/v1/HATTR_TOKEN/rpc/{request_id}"))
        .json(&json!({"status": "ok"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let (status, body) = twoway.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"status": "ok"}));
}

async fn rest_twoway(
    client: &Client,
    core: &str,
    token: &str,
    device_id: &str,
    timeout_ms: i64,
) -> (StatusCode, Value) {
    let resp = client
        .post(format!("{core}/api/rpc/twoway/{device_id}"))
        .header("Authorization", bearer(token))
        .json(&json!({"method": "getStatus", "params": {"v": 1}, "timeout": timeout_ms}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.json().await.unwrap_or(Value::Null);
    (status, body)
}
