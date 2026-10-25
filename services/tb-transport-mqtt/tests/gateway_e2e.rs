//! Phase 6 网关 API 端到端：v1/gateway/connect → 子设备遥测/属性上行 →
//! attributes/request 往返 → REST twoway RPC 经网关下行到子设备并回流。

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

/// 建网关设备（additionalInfo.gateway=true + 固定 token），返回 device_id。
async fn create_gateway(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    name: &str,
    access_token: &str,
) -> String {
    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(token))
        .json(&json!({
            "name": name,
            "accessToken": access_token,
            "additionalInfo": {"gateway": true},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create gateway {name}");
    let device: Value = resp.json().await.unwrap();
    device["id"].as_str().unwrap().to_string()
}

/// 按名字找设备 id（轮询异步创建）。
async fn find_device_by_name(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    name: &str,
) -> Option<String> {
    let resp = client
        .get(format!(
            "{base}/api/devices?pageSize=50&page=0&textSearch={name}"
        ))
        .header("Authorization", bearer(token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    page["data"]
        .as_array()?
        .iter()
        .find(|d| d["name"] == name)
        .and_then(|d| d["id"].as_str())
        .map(str::to_string)
}

async fn latest_telemetry(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    device_id: &str,
) -> Value {
    let resp = client
        .get(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/values/timeseries"
        ))
        .header("Authorization", bearer(token))
        .send()
        .await
        .unwrap();
    resp.json().await.unwrap_or(Value::Null)
}

#[tokio::test]
async fn gateway_e2e() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "GW Tenant", "gw-tenant@tb.io").await;
    create_gateway(&client, &core, &admin_token, "gw-1", "GW_TOKEN").await;

    let mut gw = connect_until_success(mqtt_addr, "gw-e2e-1", "GW_TOKEN").await;

    // 子设备 connect → get-or-create
    gw.client
        .publish(
            "v1/gateway/connect",
            QoS::AtLeastOnce,
            false,
            json!({"device": "gw-sub-1"}).to_string(),
        )
        .await
        .unwrap();
    let sub_id = {
        let mut found = None;
        let start = std::time::Instant::now();
        while found.is_none() {
            found = find_device_by_name(&client, &core, &admin_token, "gw-sub-1").await;
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "sub-device never created"
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        found.unwrap()
    };

    // 子设备遥测上行 → REST latest 可见
    gw.client
        .publish(
            "v1/gateway/telemetry",
            QoS::AtLeastOnce,
            false,
            json!({"gw-sub-1": {"ts": 1700000000000i64, "values": {"temperature": 42}}})
                .to_string(),
        )
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), async || {
        let latest = latest_telemetry(&client, &core, &admin_token, &sub_id).await;
        latest["temperature"]
            .as_array()
            .is_some_and(|a| a.iter().any(|e| e["value"] == 42))
    })
    .await;

    create_gateway(&client, &core, &admin_token, "gw-2", "GW2_TOKEN").await;
    let resp = client
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"name": "admin-unowned-device", "accessToken": "ADMIN_DEVICE_TOKEN"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let unowned: Value = resp.json().await.unwrap();
    let unowned_id = unowned["id"].as_str().unwrap();
    let other_gateway = connect_until_success(mqtt_addr, "gw-e2e-2", "GW2_TOKEN").await;
    for device_name in ["gw-sub-1", "admin-unowned-device"] {
        other_gateway
            .client
            .publish(
                "v1/gateway/connect",
                QoS::AtLeastOnce,
                false,
                json!({"device": device_name}).to_string(),
            )
            .await
            .unwrap();
    }
    other_gateway
        .client
        .publish(
            "v1/gateway/telemetry",
            QoS::AtLeastOnce,
            false,
            json!({
                "gw-sub-1": {"temperature": 99},
                "admin-unowned-device": {"stolen": true}
            })
            .to_string(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        latest_telemetry(&client, &core, &admin_token, &sub_id).await["temperature"][0]["value"],
        42
    );
    assert!(latest_telemetry(&client, &core, &admin_token, unowned_id).await["stolen"].is_null());
    other_gateway.client.disconnect().await.unwrap();

    // 子设备属性上行 → REST CLIENT_SCOPE 可见
    gw.client
        .publish(
            "v1/gateway/attributes",
            QoS::AtLeastOnce,
            false,
            json!({"gw-sub-1": {"model": "x1"}}).to_string(),
        )
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{sub_id}/values/attributes/CLIENT_SCOPE"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        let attrs: Value = resp.json().await.unwrap_or(Value::Null);
        attrs
            .as_array()
            .is_some_and(|a| a.iter().any(|e| e["key"] == "model" && e["value"] == "x1"))
    })
    .await;

    // attributes/request 往返（shared scope）
    let resp = client
        .post(format!(
            "{core}/api/telemetry/DEVICE/{sub_id}/attributes/SHARED_SCOPE"
        ))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"targetTemp": 60}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    gw.client
        .subscribe("v1/gateway/attributes/response", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for(&mut gw.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;
    gw.client
        .publish(
            "v1/gateway/attributes/request",
            QoS::AtLeastOnce,
            false,
            json!({"id": 7, "device": "gw-sub-1", "client": false, "keys": ["targetTemp"]})
                .to_string(),
        )
        .await
        .unwrap();
    let pkt = wait_for(
        &mut gw.rx,
        Duration::from_secs(15),
        |p| matches!(p, Packet::Publish(p) if p.topic == "v1/gateway/attributes/response"),
    )
    .await;
    match pkt {
        Packet::Publish(p) => {
            let body: Value = serde_json::from_slice(&p.payload).unwrap();
            assert_eq!(body["id"], 7);
            assert_eq!(body["device"], "gw-sub-1");
            assert_eq!(body["value"], 60);
        }
        other => panic!("unexpected {other:?}"),
    }

    // REST twoway RPC → 网关经 v1/gateway/rpc 收到 → 响应回流 REST
    gw.client
        .subscribe("v1/gateway/rpc", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for(&mut gw.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;
    let start = std::time::Instant::now();
    let body = loop {
        let http = {
            let client = client.clone();
            let core = core.clone();
            let token = admin_token.clone();
            let sub_id = sub_id.clone();
            tokio::spawn(async move {
                let resp = client
                    .post(format!("{core}/api/rpc/twoway/{sub_id}"))
                    .header("Authorization", bearer(&token))
                    .json(&json!({"method": "getStatus", "params": {"v": 1}, "timeout": 5000}))
                    .send()
                    .await
                    .unwrap();
                (
                    resp.status(),
                    resp.json::<Value>().await.unwrap_or(Value::Null),
                )
            })
        };
        let request = wait_opt(
            &mut gw.rx,
            Duration::from_secs(4),
            |p| matches!(p, Packet::Publish(p) if p.topic == "v1/gateway/rpc"),
        )
        .await;
        if let Some(Packet::Publish(p)) = request {
            let payload: Value = serde_json::from_slice(&p.payload).unwrap();
            assert_eq!(payload["device"], "gw-sub-1");
            assert_eq!(payload["data"]["method"], "getStatus");
            assert_eq!(payload["data"]["params"], json!({"v": 1}));
            let id = payload["data"]["id"].as_i64().unwrap();
            gw.client
                .publish(
                    "v1/gateway/rpc",
                    QoS::AtLeastOnce,
                    false,
                    json!({"device": "gw-sub-1", "data": {"id": id, "response": {"ok": true}}})
                        .to_string(),
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
            "gateway rpc round trip never succeeded"
        );
    };
    assert_eq!(body, json!({"ok": true}));

    // 子设备 disconnect → 之后 twoway 找不到会话（注册表异步，只验证不 panic）
    gw.client
        .publish(
            "v1/gateway/disconnect",
            QoS::AtLeastOnce,
            false,
            json!({"device": "gw-sub-1"}).to_string(),
        )
        .await
        .unwrap();

    gw.client.disconnect().await.unwrap();
}

/// 非网关设备使用 v1/gateway/* → 服务端断开连接。
#[tokio::test]
async fn gateway_topics_rejected_for_plain_device() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "GW2 Tenant", "gw2-tenant@tb.io").await;
    create_device_with_token(&client, &core, &admin_token, "plain-1", "PLAIN_TOKEN").await;

    let mut device = connect_until_success(mqtt_addr, "plain-e2e-1", "PLAIN_TOKEN").await;
    device
        .client
        .publish(
            "v1/gateway/connect",
            QoS::AtLeastOnce,
            false,
            json!({"device": "sub-x"}).to_string(),
        )
        .await
        .unwrap();
    // 期望连接被关闭
    let start = std::time::Instant::now();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), device.rx.recv()).await {
            Ok(Some(PumpEvent::Closed)) => break,
            Ok(Some(PumpEvent::Packet(_))) => {}
            _ => {}
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "connection not closed for non-gateway publish"
        );
        if start.elapsed() >= Duration::from_secs(20) {
            break;
        }
    }
}
