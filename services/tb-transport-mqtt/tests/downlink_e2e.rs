//! Phase 5 下行端到端：REST twoway RPC → MQTT 设备 → 响应回流；
//! SHARED_SCOPE 属性订阅推送（含删除通知）；设备活性 server 属性。

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use reqwest::StatusCode;
use rumqttc::{AsyncClient, ConnectReturnCode, ConnectionError, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use common::*;

const CONN_WAIT: Duration = Duration::from_secs(5);

enum PumpEvent {
    Packet(Packet),
    Refused(ConnectReturnCode),
    Closed,
}

struct MqttDevice {
    client: AsyncClient,
    rx: mpsc::Receiver<PumpEvent>,
}

fn connect(addr: SocketAddr, client_id: &str, username: &str) -> MqttDevice {
    let mut opts = MqttOptions::new(client_id, addr.ip().to_string(), addr.port());
    opts.set_credentials(username, "");
    opts.set_keep_alive(Duration::from_secs(10));
    opts.set_clean_session(true);
    let (client, mut eventloop) = AsyncClient::new(opts, 32);
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(pkt)) => {
                    if tx.send(PumpEvent::Packet(pkt)).await.is_err() {
                        return;
                    }
                }
                Ok(_) => {}
                Err(ConnectionError::ConnectionRefused(code)) => {
                    let _ = tx.send(PumpEvent::Refused(code)).await;
                    return;
                }
                Err(_) => {
                    let _ = tx.send(PumpEvent::Closed).await;
                    return;
                }
            }
        }
    });
    MqttDevice { client, rx }
}

async fn wait_for(
    rx: &mut mpsc::Receiver<PumpEvent>,
    timeout: Duration,
    pred: impl Fn(&Packet) -> bool,
) -> Packet {
    let start = std::time::Instant::now();
    loop {
        let event = tokio::time::timeout(timeout, rx.recv())
            .await
            .expect("timeout waiting for mqtt packet")
            .expect("mqtt pump closed");
        match event {
            PumpEvent::Packet(pkt) => {
                if pred(&pkt) {
                    return pkt;
                }
                assert!(start.elapsed() < timeout, "wait_for timed out: {pkt:?}");
            }
            _ => panic!("connection closed while waiting for packet"),
        }
    }
}

/// 非 panic 版本：超时返回 None（轮询下行是否到达用）。
async fn wait_opt(
    rx: &mut mpsc::Receiver<PumpEvent>,
    timeout: Duration,
    pred: impl Fn(&Packet) -> bool,
) -> Option<Packet> {
    let start = std::time::Instant::now();
    loop {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(PumpEvent::Packet(pkt))) => {
                if pred(&pkt) {
                    return Some(pkt);
                }
                if start.elapsed() >= timeout {
                    return None;
                }
            }
            _ => return None,
        }
    }
}

async fn connect_until_success(addr: SocketAddr, client_id: &str, username: &str) -> MqttDevice {
    let start = std::time::Instant::now();
    loop {
        let mut device = connect(addr, client_id, username);
        let first = tokio::time::timeout(CONN_WAIT, device.rx.recv()).await;
        match first {
            Ok(Some(PumpEvent::Packet(Packet::ConnAck(ack))))
                if ack.code == ConnectReturnCode::Success =>
            {
                return device;
            }
            Ok(Some(PumpEvent::Refused(code))) => {
                tracing::debug!(?code, "conn refused, retrying");
            }
            _ => {}
        }
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "connect never succeeded"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn twoway_rpc(
    client: &reqwest::Client,
    core: &str,
    token: &str,
    device_id: &str,
    timeout_ms: i64,
) -> (StatusCode, Value) {
    let resp = client
        .post(format!("{core}/api/rpc/twoway/{device_id}"))
        .header("Authorization", bearer(token))
        .json(&json!({"method": "getStatus", "params": {"verbose": true}, "timeout": timeout_ms}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// a) REST twoway RPC → MQTT 下行 → 设备响应回流。
#[tokio::test]
async fn rpc_downlink_round_trip() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "RPC Tenant", "rpc-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "rpc-device", "RPC_TOKEN").await;

    let mut device = connect_until_success(mqtt_addr, "rpc-e2e-1", "RPC_TOKEN").await;
    device
        .client
        .subscribe("v1/devices/me/rpc/request/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;

    // 注册表经 Kafka 异步建立：轮询直到某轮 twoway 被成功送达并完成往返。
    let start = std::time::Instant::now();
    let body = loop {
        let http = {
            let client = client.clone();
            let core = core.clone();
            let token = admin_token.clone();
            let device_id = device_id.clone();
            tokio::spawn(async move { twoway_rpc(&client, &core, &token, &device_id, 5000).await })
        };
        let request = wait_opt(&mut device.rx, Duration::from_secs(4), |p| {
            matches!(p, Packet::Publish(p) if p.topic.starts_with("v1/devices/me/rpc/request/"))
        })
        .await;
        if let Some(Packet::Publish(p)) = request {
            // 下行 payload 为 {method, params}
            let payload: Value = serde_json::from_slice(&p.payload).unwrap();
            assert_eq!(payload["method"], "getStatus");
            assert_eq!(payload["params"], json!({"verbose": true}));
            let request_id = p
                .topic
                .strip_prefix("v1/devices/me/rpc/request/")
                .unwrap()
                .to_string();
            device
                .client
                .publish(
                    format!("v1/devices/me/rpc/response/{request_id}"),
                    QoS::AtLeastOnce,
                    false,
                    json!({"status": "ok", "fw": 3}).to_string(),
                )
                .await
                .unwrap();
            let (status, body) = http.await.unwrap();
            if status == StatusCode::OK {
                break body;
            }
        } else {
            let _ = http.await;
        }
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "rpc round trip never succeeded"
        );
    };
    assert_eq!(body, json!({"status": "ok", "fw": 3}));

    // oneway：只发不等，立即 200
    let resp = client
        .post(format!("{core}/api/rpc/oneway/{device_id}"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"method": "reboot", "params": {}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let pkt = wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::Publish(p) if p.topic.starts_with("v1/devices/me/rpc/request/"))
    })
    .await;
    match pkt {
        Packet::Publish(p) => {
            let payload: Value = serde_json::from_slice(&p.payload).unwrap();
            assert_eq!(payload["method"], "reboot");
        }
        other => panic!("unexpected {other:?}"),
    }

    // 未连接设备 → 404
    let (status, _) = twoway_rpc(&client, &core, &admin_token, &uuid::Uuid::now_v7().to_string(), 500)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    device.client.disconnect().await.unwrap();
}

/// b) 属性订阅下行推送：SHARED_SCOPE 保存/删除 → MQTT 设备收到 key 字符串正确的更新。
#[tokio::test]
async fn attribute_subscription_downlink() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "Attr Tenant", "attr-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "attr-device", "ATTR_TOKEN").await;

    let mut device = connect_until_success(mqtt_addr, "attr-e2e-1", "ATTR_TOKEN").await;
    device
        .client
        .subscribe("v1/devices/me/attributes", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;

    let attrs_url = format!("{core}/api/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE");
    // 订阅注册异步：重复保存直到下行到达。
    let start = std::time::Instant::now();
    let payload = loop {
        let resp = client
            .post(&attrs_url)
            .header("Authorization", bearer(&admin_token))
            .json(&json!({"targetTemp": 60, "mode": "auto"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        if let Some(Packet::Publish(p)) = wait_opt(&mut device.rx, Duration::from_secs(3), |p| {
            matches!(p, Packet::Publish(p) if p.topic == "v1/devices/me/attributes")
        })
        .await
        {
            break serde_json::from_slice::<Value>(&p.payload).unwrap();
        }
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "attribute update never arrived"
        );
    };
    // key 必须是字符串而非 key_id
    assert_eq!(payload["targetTemp"], 60);
    assert_eq!(payload["mode"], "auto");

    // 删除通知：value → JSON null
    let start = std::time::Instant::now();
    let payload = loop {
        let resp = client
            .delete(format!("{attrs_url}?keys=targetTemp"))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        if let Some(Packet::Publish(p)) = wait_opt(&mut device.rx, Duration::from_secs(3), |p| {
            matches!(p, Packet::Publish(p) if p.topic == "v1/devices/me/attributes")
        })
        .await
        {
            let v: Value = serde_json::from_slice(&p.payload).unwrap();
            if v.get("targetTemp").is_some() {
                break v;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "attribute delete notification never arrived"
        );
    };
    assert_eq!(payload["targetTemp"], Value::Null);

    device.client.disconnect().await.unwrap();
}

/// d) 设备状态：connect → active=true/lastConnectTime；disconnect → active=false/lastDisconnectTime。
#[tokio::test]
async fn device_activity_state() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "State Tenant", "state-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "state-device", "STATE_TOKEN").await;

    let server_attrs = |client: reqwest::Client, core: String, token: String, id: String| async move {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{id}/values/attributes/SERVER_SCOPE"
            ))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        resp.json::<Value>().await.unwrap_or(Value::Null)
    };

    let device = connect_until_success(mqtt_addr, "state-e2e-1", "STATE_TOKEN").await;

    poll_until(Duration::from_secs(20), async || {
        let attrs =
            server_attrs(client.clone(), core.clone(), admin_token.clone(), device_id.clone())
                .await;
        attrs.as_array().is_some_and(|a| {
            a.iter().any(|e| e["key"] == "active" && e["value"] == true)
                && a.iter().any(|e| e["key"] == "lastConnectTime")
        })
    })
    .await;

    device.client.disconnect().await.unwrap();

    poll_until(Duration::from_secs(20), async || {
        let attrs =
            server_attrs(client.clone(), core.clone(), admin_token.clone(), device_id.clone())
                .await;
        attrs.as_array().is_some_and(|a| {
            a.iter().any(|e| e["key"] == "active" && e["value"] == false)
                && a.iter().any(|e| e["key"] == "lastDisconnectTime")
        })
    })
    .await;
}
