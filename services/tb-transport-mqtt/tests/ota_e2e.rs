//! MQTT OTA 端到端：显式绑定选包（未绑定回 v2/fw/error）→ 绑定经 shared 属性通知推 fw_* →
//! v2/fw/request/{id}/chunk/{n} 分块拉取 → 在线换绑后分块为新包 → SOFTWARE 走 v2/sw/*。

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

fn publish_payload(pkt: Packet) -> bytes::Bytes {
    match pkt {
        Packet::Publish(p) => p.payload,
        other => panic!("unexpected {other:?}"),
    }
}

/// v1/devices/me/attributes 上某 key 取值为 expected 的推送。
fn is_attribute_push(pkt: &Packet, key: &str, expected: &str) -> bool {
    matches!(pkt, Packet::Publish(p) if p.topic == "v1/devices/me/attributes"
        && serde_json::from_slice::<Value>(&p.payload)
            .is_ok_and(|v| v.get(key).and_then(Value::as_str) == Some(expected)))
}

#[allow(clippy::too_many_arguments)]
async fn upload_ota(
    client: &reqwest::Client,
    core: &str,
    token: &str,
    profile_id: &str,
    ota_type: &str,
    title: &str,
    version: &str,
    data: Vec<u8>,
) -> String {
    let part = reqwest::multipart::Part::bytes(data).file_name("ota.bin");
    let form = reqwest::multipart::Form::new()
        .text("title", title.to_string())
        .text("version", version.to_string())
        .text("type", ota_type.to_string())
        .text("deviceProfileId", profile_id.to_string())
        .part("file", part);
    let resp = client
        .post(format!("{core}/api/otaPackage"))
        .header("Authorization", bearer(token))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "upload ota package");
    resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn bind(client: &reqwest::Client, core: &str, token: &str, path: String) {
    let resp = client
        .post(format!("{core}{path}"))
        .header("Authorization", bearer(token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "bind {path}");
}

/// 拉取 v2/{fw,sw}/request/{rid}/chunk/{n} 的应答分块。
async fn request_chunk(
    device: &mut MqttDevice,
    kind: &str,
    rid: u32,
    chunk: u32,
    size: u32,
) -> Vec<u8> {
    device
        .client
        .publish(
            format!("v2/{kind}/request/{rid}/chunk/{chunk}"),
            QoS::AtLeastOnce,
            false,
            size.to_string(),
        )
        .await
        .unwrap();
    let topic = format!("v2/{kind}/response/{rid}/chunk/{chunk}");
    let pkt = wait_for(
        &mut device.rx,
        Duration::from_secs(15),
        |p| matches!(p, Packet::Publish(p) if p.topic == topic),
    )
    .await;
    publish_payload(pkt).to_vec()
}

#[tokio::test]
async fn mqtt_ota_explicit_binding_e2e() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "OTA Tenant", "ota-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "ota-device", "OTA_TOKEN").await;

    let resp = client
        .get(format!("{core}/api/deviceProfile/default"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let profile: Value = resp.json().await.unwrap();
    let profile_id = profile["id"].as_str().unwrap().to_string();

    let fw_data: Vec<u8> = (0..250u32).map(|i| (i % 251) as u8).collect();
    let fw_id = upload_ota(
        &client,
        &core,
        &admin_token,
        &profile_id,
        "FIRMWARE",
        "fw",
        "1.0",
        fw_data.clone(),
    )
    .await;

    let mut device = connect_until_success(mqtt_addr, "ota-e2e-1", "OTA_TOKEN").await;
    device
        .client
        .subscribe("v1/devices/me/attributes", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;
    // 属性订阅注册异步：重复写探针 shared 属性直到下行到达。
    let start = std::time::Instant::now();
    loop {
        let resp = client
            .post(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE"
            ))
            .header("Authorization", bearer(&admin_token))
            .json(&json!({"probe": "ready"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        if wait_opt(&mut device.rx, Duration::from_secs(3), |p| {
            is_attribute_push(p, "probe", "ready")
        })
        .await
        .is_some()
        {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "attribute subscription never became active"
        );
    }

    // 未绑定：不回退到最新上传包，订阅不推元信息，分块请求回 v2/fw/error。
    device
        .client
        .subscribe("v2/fw/response/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    wait_for(&mut device.rx, Duration::from_secs(10), |p| {
        matches!(p, Packet::SubAck(_))
    })
    .await;
    device
        .client
        .publish("v2/fw/request/1/chunk/0", QoS::AtLeastOnce, false, "100")
        .await
        .unwrap();
    let pkt = wait_for(
        &mut device.rx,
        Duration::from_secs(15),
        |p| matches!(p, Packet::Publish(p) if p.topic == "v2/fw/error"),
    )
    .await;
    assert_eq!(publish_payload(pkt).as_ref(), b"NOT_FOUND");

    // 设备级绑定 → shared 属性通知推 fw_*；分块 100/100/50 拼回原文。
    bind(
        &client,
        &core,
        &admin_token,
        format!("/api/device/{device_id}/firmware/{fw_id}"),
    )
    .await;
    let pkt = wait_for(&mut device.rx, Duration::from_secs(15), |p| {
        is_attribute_push(p, "fw_version", "1.0")
    })
    .await;
    let body: Value = serde_json::from_slice(&publish_payload(pkt)).unwrap();
    assert_eq!(body["fw_title"], "fw");
    assert_eq!(body["fw_size"], 250);
    assert_eq!(body["fw_checksum_algorithm"], "SHA256");
    let mut received = Vec::new();
    for chunk in 0..3 {
        received.extend(request_chunk(&mut device, "fw", 9, chunk, 100).await);
    }
    assert_eq!(received, fw_data);

    // 在线换绑新包：会话缓存随通知刷新，后续分块即为新包。
    let fw2_data: Vec<u8> = (0..64u32).map(|i| (i * 3 % 256) as u8).collect();
    let fw2_id = upload_ota(
        &client,
        &core,
        &admin_token,
        &profile_id,
        "FIRMWARE",
        "fw",
        "2.0",
        fw2_data.clone(),
    )
    .await;
    bind(
        &client,
        &core,
        &admin_token,
        format!("/api/device/{device_id}/firmware/{fw2_id}"),
    )
    .await;
    wait_for(&mut device.rx, Duration::from_secs(15), |p| {
        is_attribute_push(p, "fw_version", "2.0")
    })
    .await;
    assert_eq!(request_chunk(&mut device, "fw", 10, 0, 100).await, fw2_data);

    // SOFTWARE：绑定后订阅 v2/sw/response/+ 推 sw_* 元信息，分块走 sw topic。
    let sw_data: Vec<u8> = (0..120u32).map(|i| (i % 17) as u8).collect();
    let sw_id = upload_ota(
        &client,
        &core,
        &admin_token,
        &profile_id,
        "SOFTWARE",
        "app",
        "3.1",
        sw_data.clone(),
    )
    .await;
    bind(
        &client,
        &core,
        &admin_token,
        format!("/api/device/{device_id}/software/{sw_id}"),
    )
    .await;
    wait_for(&mut device.rx, Duration::from_secs(15), |p| {
        is_attribute_push(p, "sw_version", "3.1")
    })
    .await;
    device
        .client
        .subscribe("v2/sw/response/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    // 订阅处理中先推 sw_* 元信息再回 SubAck：两个包都收，顺序不限。
    let start = std::time::Instant::now();
    let (mut saw_suback, mut sw_meta) = (false, None);
    while !(saw_suback && sw_meta.is_some()) {
        let event = tokio::time::timeout(Duration::from_secs(20), device.rx.recv())
            .await
            .expect("timeout waiting for suback/sw metadata")
            .expect("mqtt pump closed");
        if let PumpEvent::Packet(pkt) = event {
            match pkt {
                Packet::SubAck(_) => saw_suback = true,
                pkt if is_attribute_push(&pkt, "sw_title", "app") => {
                    sw_meta = Some(serde_json::from_slice::<Value>(&publish_payload(pkt)).unwrap());
                }
                _ => {}
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "sw metadata never arrived"
        );
    }
    let meta = sw_meta.unwrap();
    assert_eq!(meta["sw_version"], "3.1");
    assert_eq!(meta["sw_size"], 120);
    assert!(meta.get("fw_title").is_none());
    assert_eq!(request_chunk(&mut device, "sw", 5, 0, 1000).await, sw_data);

    device.client.disconnect().await.unwrap();
}
