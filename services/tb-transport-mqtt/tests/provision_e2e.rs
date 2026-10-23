//! Phase 6 provisioning 端到端：profile provisionConfiguration → MQTT username="provision"
//! → provision/request 拿凭证 → 凭证连 MQTT 发遥测；错误 secret → FAILURE。

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use reqwest::StatusCode;
use rumqttc::{AsyncClient, ConnectReturnCode, ConnectionError, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use common::*;

enum PumpEvent {
    Packet(Packet),
    Refused,
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
                    let _ = code;
                    let _ = tx.send(PumpEvent::Refused).await;
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
        let first = tokio::time::timeout(Duration::from_secs(5), device.rx.recv()).await;
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

/// provision 请求并等 provision/response。
async fn provision_request(
    addr: SocketAddr,
    device_name: &str,
    key: &str,
    secret: &str,
) -> Value {
    let mut session = connect_until_success(addr, &format!("prov-{device_name}"), "provision").await;
    session
        .client
        .subscribe("provision/response", QoS::AtLeastOnce)
        .await
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(10), session.rx.recv())
            .await
            .expect("timeout waiting for suback")
            .expect("pump closed");
        if matches!(event, PumpEvent::Packet(Packet::SubAck(_))) {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(10));
    }
    session
        .client
        .publish(
            "provision/request",
            QoS::AtLeastOnce,
            false,
            json!({
                "deviceName": device_name,
                "provisionDeviceKey": key,
                "provisionDeviceSecret": secret,
            })
            .to_string(),
        )
        .await
        .unwrap();
    let start = std::time::Instant::now();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(15), session.rx.recv())
            .await
            .expect("timeout waiting for provision response")
            .expect("pump closed");
        if let PumpEvent::Packet(Packet::Publish(p)) = event
            && p.topic == "provision/response"
        {
            session.client.disconnect().await.unwrap();
            return serde_json::from_slice(&p.payload).unwrap();
        }
        assert!(start.elapsed() < Duration::from_secs(15));
    }
}

#[tokio::test]
async fn provision_e2e() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "Prov Tenant", "prov-tenant@tb.io").await;

    // 配 ALLOW_CREATE_NEW_DEVICES 档案 + provision key/secret
    let resp = client
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({
            "name": "prov-profile",
            "provisionType": "ALLOW_CREATE_NEW_DEVICES",
            "profileData": {
                "provisionConfiguration": {
                    "provisionDeviceKey": "PROV_KEY",
                    "provisionDeviceSecret": "PROV_SECRET",
                }
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "create provision profile");

    // 错误 secret → FAILURE
    let resp = provision_request(mqtt_addr, "prov-dev-1", "PROV_KEY", "WRONG").await;
    assert_eq!(resp["status"], "FAILURE");

    // 正确 key/secret → SUCCESS + ACCESS_TOKEN 凭证
    let resp = provision_request(mqtt_addr, "prov-dev-1", "PROV_KEY", "PROV_SECRET").await;
    assert_eq!(resp["status"], "SUCCESS", "provision failed: {resp}");
    assert_eq!(resp["credentialsType"], "ACCESS_TOKEN");
    let token = resp["credentialsValue"].as_str().unwrap().to_string();

    // 用拿到的凭证连 MQTT 发遥测 → REST latest 可见
    let device = connect_until_success(mqtt_addr, "prov-e2e-1", &token).await;
    device
        .client
        .publish(
            "v1/devices/me/telemetry",
            QoS::AtLeastOnce,
            false,
            json!({"provisioned": true}).to_string(),
        )
        .await
        .unwrap();
    poll_until(Duration::from_secs(30), async || {
        let resp = client
            .get(format!(
                "{core}/api/devices?pageSize=50&page=0&textSearch=prov-dev-1"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        let page: Value = resp.json().await.unwrap();
        let Some(id) = page["data"]
            .as_array()
            .and_then(|a| a.iter().find(|d| d["name"] == "prov-dev-1"))
            .and_then(|d| d["id"].as_str())
        else {
            return false;
        };
        let resp = client
            .get(format!("{core}/api/telemetry/DEVICE/{id}/values/timeseries"))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        let latest: Value = resp.json().await.unwrap_or(Value::Null);
        latest["provisioned"]
            .as_array()
            .is_some_and(|a| a.iter().any(|e| e["value"] == true))
    })
    .await;

    device.client.disconnect().await.unwrap();
}
