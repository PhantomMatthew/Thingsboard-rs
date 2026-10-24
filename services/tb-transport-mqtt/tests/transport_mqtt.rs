//! 端到端：rumqttc 设备 → MQTT 上行 → Kafka → tb-core ingest 落库 → REST 读回。

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use rumqttc::{AsyncClient, ConnectReturnCode, ConnectionError, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use common::*;

const CONN_WAIT: Duration = Duration::from_secs(5);

/// rumqttc 对失败的 ConnAck 返回 Err(ConnectionRefused)，泵统一转成 PumpEvent。
enum PumpEvent {
    Packet(Packet),
    Refused(ConnectReturnCode),
    Closed,
}

struct MqttDevice {
    client: AsyncClient,
    rx: mpsc::Receiver<PumpEvent>,
}

/// 建立连接并启动 eventloop 泵；返回后需自行等 ConnAck。
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

/// 等到满足条件的包，超时或连接关闭则 panic。
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

/// 首轮可能撞上 Kafka consumer 冷启动 → 认证超时断连，轮询重连直到 CONNACK success。
async fn connect_until_success(addr: SocketAddr, client_id: &str, username: &str) -> MqttDevice {
    let start = std::time::Instant::now();
    loop {
        let mut device = connect(addr, client_id, username);
        let first = tokio::time::timeout(CONN_WAIT, device.rx.recv()).await;
        if let Ok(Some(PumpEvent::Packet(Packet::ConnAck(ack)))) = first
            && ack.code == ConnectReturnCode::Success
        {
            return device;
        }
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "connect never succeeded"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn mqtt_transport_telemetry_and_attributes() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "MQTT Tenant", "mqtt-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "mqtt-device", "MQTT_TOKEN").await;

    let mut device = connect_until_success(mqtt_addr, "mqtt-e2e-1", "MQTT_TOKEN").await;

    // 订阅共享属性更新 + 服务端 RPC（Phase 5 才有实际下行，这里验证 SUBACK 授予）
    device
        .client
        .subscribe("v1/devices/me/attributes", QoS::AtLeastOnce)
        .await
        .unwrap();
    let pkt = wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;
    match pkt {
        Packet::SubAck(ack) => {
            assert_eq!(
                ack.return_codes,
                vec![rumqttc::SubscribeReasonCode::Success(QoS::AtLeastOnce)]
            )
        }
        other => panic!("unexpected {other:?}"),
    }
    device
        .client
        .subscribe("v1/devices/me/rpc/request/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;
    device
        .client
        .subscribe("v1/devices/me/unknown", QoS::AtMostOnce)
        .await
        .unwrap();
    let pkt = wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;
    match pkt {
        Packet::SubAck(ack) => assert_eq!(
            ack.return_codes,
            vec![rumqttc::SubscribeReasonCode::Failure]
        ),
        other => panic!("unexpected {other:?}"),
    }

    // 遥测上行 QoS1：{k:v}
    device
        .client
        .publish(
            "v1/devices/me/telemetry",
            QoS::AtLeastOnce,
            false,
            json!({"temperature": 66}).to_string(),
        )
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::PubAck(_))
    })
    .await;

    // 轮询 tb-core REST 读回 latest（Kafka 异步链路）
    poll_until(Duration::from_secs(15), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=temperature"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        if resp.status() != reqwest::StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body["temperature"][0]["value"] == json!(66)
    })
    .await;

    // 带 ts/values 结构
    device
        .client
        .publish(
            "v1/devices/me/telemetry",
            QoS::AtLeastOnce,
            false,
            json!({"ts": 1700000000000i64, "values": {"humidity": 55.5}}).to_string(),
        )
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::PubAck(_))
    })
    .await;
    poll_until(Duration::from_secs(15), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=humidity"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        if resp.status() != reqwest::StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body["humidity"][0]["value"] == json!(55.5)
            && body["humidity"][0]["ts"] == json!(1700000000000i64)
    })
    .await;

    // 属性上行 QoS1（CLIENT_SCOPE）
    device
        .client
        .publish(
            "v1/devices/me/attributes",
            QoS::AtLeastOnce,
            false,
            json!({"firmware": "2.0", "active": true}).to_string(),
        )
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::PubAck(_))
    })
    .await;
    poll_until(Duration::from_secs(15), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/attributes/CLIENT_SCOPE"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        if resp.status() != reqwest::StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body.as_array().is_some_and(|attrs| {
            attrs
                .iter()
                .any(|a| a["key"] == "firmware" && a["value"] == "2.0")
                && attrs
                    .iter()
                    .any(|a| a["key"] == "active" && a["value"] == true)
        })
    })
    .await;

    let resp = client
        .post(format!(
            "{core}/api/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE"
        ))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"targetTemp": 60}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    device
        .client
        .publish(
            "v1/devices/me/attributes/request/42",
            QoS::AtLeastOnce,
            false,
            json!({"clientKeys": "firmware", "sharedKeys": "targetTemp"}).to_string(),
        )
        .await
        .unwrap();
    let response = wait_for(&mut device.rx, Duration::from_secs(15), |packet| {
        matches!(packet, Packet::Publish(p) if p.topic == "v1/devices/me/attributes/response/42")
    })
    .await;
    let Packet::Publish(response) = response else {
        unreachable!()
    };
    let body: Value = serde_json::from_slice(&response.payload).unwrap();
    assert_eq!(
        body,
        json!({"client": {"firmware": "2.0"}, "shared": {"targetTemp": 60}})
    );

    // 服务端 RPC 上行 QoS1
    device
        .client
        .publish(
            "v1/devices/me/rpc/request/1",
            QoS::AtLeastOnce,
            false,
            json!({"method": "getStatus", "params": {}}).to_string(),
        )
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::PubAck(_))
    })
    .await;

    // QoS0 遥测（无 PUBACK，轮询 REST 确认落库）
    device
        .client
        .publish(
            "v1/devices/me/telemetry",
            QoS::AtMostOnce,
            false,
            json!({"qos0_key": 1}).to_string(),
        )
        .await
        .unwrap();
    poll_until(Duration::from_secs(15), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=qos0_key"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        if resp.status() != reqwest::StatusCode::OK {
            return false;
        }
        let body: Value = resp.json().await.unwrap();
        body["qos0_key"][0]["value"] == json!(1)
    })
    .await;

    device.client.disconnect().await.unwrap();
}

#[tokio::test]
async fn mqtt_transport_rejects_bad_token() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let (_core, mqtt_addr) = start_servers().await;

    // 冷启动期间认证请求可能超时断连，轮询直到拿到明确的 CONNACK 拒绝
    poll_until(Duration::from_secs(25), async || {
        let mut device = connect(mqtt_addr, "mqtt-bad-1", "NO_SUCH_TOKEN");
        match tokio::time::timeout(CONN_WAIT, device.rx.recv()).await {
            Ok(Some(PumpEvent::Refused(code))) => code == ConnectReturnCode::BadUserNamePassword,
            _ => false,
        }
    })
    .await;
}

#[tokio::test]
async fn mqtt_claim_registers_for_customer_claim() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin, _) = setup_tenant_admin(&client, &core, "MQTT Claim", "mqtt-claim@tb.io").await;
    create_device_with_token(
        &client,
        &core,
        &admin,
        "mqtt-claim-device",
        "MQTT_CLAIM_TOKEN",
    )
    .await;
    let mut device = connect_until_success(mqtt_addr, "mqtt-claim-1", "MQTT_CLAIM_TOKEN").await;
    device
        .client
        .publish(
            "v1/devices/me/claim",
            QoS::AtLeastOnce,
            false,
            json!({"secretKey": "mqtt-secret", "durationMs": 60000}).to_string(),
        )
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::PubAck(_))
    })
    .await;

    let customer: Value = client
        .post(format!("{core}/api/customer"))
        .header("Authorization", bearer(&admin))
        .json(&json!({"title": "MQTT Claim Customer"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let customer_id = customer["id"].as_str().unwrap();
    let resp = client
        .post(format!("{core}/api/user"))
        .header("Authorization", bearer(&admin))
        .json(&json!({"customerId": customer_id, "email": "mqtt-claim-customer@tb.io", "authority": "CUSTOMER_USER", "password": "customer"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let tokens = login(&client, &core, "mqtt-claim-customer@tb.io", "customer").await;
    poll_until(Duration::from_secs(10), async || {
        let resp = client
            .post(format!(
                "{core}/api/customer/device/mqtt-claim-device/claim"
            ))
            .header("Authorization", bearer(tokens["token"].as_str().unwrap()))
            .json(&json!({"secretKey": "mqtt-secret"}))
            .send()
            .await
            .unwrap();
        resp.status() == reqwest::StatusCode::OK
    })
    .await;
}
