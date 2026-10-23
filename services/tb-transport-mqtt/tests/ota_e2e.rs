//! Phase 6 MQTT OTA 端到端：订阅 v2/fw/response/+ → fw_* 元信息推送 →
//! v2/fw/request/{id}/chunk/{n} 分块拉取，拼接与原文一致。

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

#[tokio::test]
async fn mqtt_firmware_chunks_e2e() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "OTA Tenant", "ota-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "ota-device", "OTA_TOKEN").await;

    // 默认档案 id + 固件上传（250 字节）
    let resp = client
        .get(format!("{core}/api/deviceProfile/default"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let profile: Value = resp.json().await.unwrap();
    let profile_id = profile["id"].as_str().unwrap().to_string();

    let fw_data: Vec<u8> = (0..250u32).map(|i| (i % 251) as u8).collect();
    let part = reqwest::multipart::Part::bytes(fw_data.clone()).file_name("fw.bin");
    let form = reqwest::multipart::Form::new()
        .text("title", "fw")
        .text("version", "1.0")
        .text("type", "FIRMWARE")
        .text("deviceProfileId", profile_id)
        .part("file", part);
    let resp = client
        .post(format!("{core}/api/otaPackage"))
        .header("Authorization", bearer(&admin_token))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "upload ota package");

    // 设备当前固件版本（shared 属性）
    let resp = client
        .post(format!(
            "{core}/api/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE"
        ))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"current_fw_title": "fw", "current_fw_version": "0.9"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut device = connect_until_success(mqtt_addr, "ota-e2e-1", "OTA_TOKEN").await;
    device
        .client
        .subscribe("v2/fw/response/+", QoS::AtLeastOnce)
        .await
        .unwrap();

    // 订阅处理中先推 fw_* 元信息再回 SubAck：两个包都收，顺序不限。
    let start = std::time::Instant::now();
    let (mut saw_suback, mut fw_meta) = (false, None);
    while !(saw_suback && fw_meta.is_some()) {
        let event = tokio::time::timeout(Duration::from_secs(20), device.rx.recv())
            .await
            .expect("timeout waiting for suback/fw metadata")
            .expect("mqtt pump closed");
        if let PumpEvent::Packet(pkt) = event {
            match pkt {
                Packet::SubAck(_) => saw_suback = true,
                Packet::Publish(p)
                    if p.topic == "v1/devices/me/attributes"
                        && serde_json::from_slice::<Value>(&p.payload)
                            .is_ok_and(|v| v.get("fw_title").is_some()) =>
                {
                    fw_meta = Some(serde_json::from_slice::<Value>(&p.payload).unwrap());
                }
                _ => {}
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "fw metadata never arrived"
        );
    }
    let body = fw_meta.unwrap();
    assert_eq!(body["fw_title"], "fw");
    assert_eq!(body["fw_version"], "1.0");
    assert_eq!(body["fw_size"], 250);

    // 分块拉取：chunk 大小 100 → 100/100/50
    let mut received = Vec::new();
    for chunk in 0..3 {
        device
            .client
            .publish(
                format!("v2/fw/request/9/chunk/{chunk}"),
                QoS::AtLeastOnce,
                false,
                "100",
            )
            .await
            .unwrap();
        let pkt = wait_for(&mut device.rx, Duration::from_secs(15), |p| {
            matches!(p, Packet::Publish(p) if p.topic == format!("v2/fw/response/9/chunk/{chunk}"))
        })
        .await;
        match pkt {
            Packet::Publish(p) => received.extend_from_slice(&p.payload),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(received, fw_data);

    device.client.disconnect().await.unwrap();
}
