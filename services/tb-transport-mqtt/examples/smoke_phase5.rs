//! Phase 5 冒烟：真实 tb-core + tb-transport-mqtt 进程下联调。
//! 前置：TB_CORE_URL（默认 http://localhost:8080）、TB_MQTT_ADDR（默认 127.0.0.1:1883）。
//! 流程：建租户/设备 → MQTT 连接+订阅 rpc/attributes → REST twoway RPC 往返
//! → SHARED_SCOPE 属性下行 → 设备活性属性校验。

use std::time::Duration;

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};

fn core_url() -> String {
    std::env::var("TB_CORE_URL").unwrap_or_else(|_| "http://localhost:8080".into())
}

fn mqtt_addr() -> String {
    std::env::var("TB_MQTT_ADDR").unwrap_or_else(|_| "127.0.0.1:1883".into())
}

async fn login(client: &reqwest::Client, user: &str, pass: &str) -> String {
    let resp = client
        .post(format!("{}/api/auth/login", core_url()))
        .json(&json!({"username": user, "password": pass}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "login {user}");
    resp.json::<Value>().await.unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::main]
async fn main() {
    let run_id = &uuid::Uuid::now_v7().simple().to_string()[..8];
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    let sys = login(&client, "sysadmin@thingsboard.org", "sysadmin").await;
    let resp = client
        .post(format!("{}/api/tenant", core_url()))
        .bearer_auth(&sys)
        .json(&json!({"title": format!("Smoke {run_id}")}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let tenant_id = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let email = format!("smoke-{run_id}@tb.io");
    let resp = client
        .post(format!("{}/api/user", core_url()))
        .bearer_auth(&sys)
        .json(&json!({"tenantId": tenant_id, "email": email, "authority": "TENANT_ADMIN", "password": "tenant"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let admin = login(&client, &email, "tenant").await;

    let access_token = format!("SMOKE_{run_id}");
    let resp = client
        .post(format!("{}/api/device", core_url()))
        .bearer_auth(&admin)
        .json(&json!({"name": format!("smoke-{run_id}"), "accessToken": access_token}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let device_id = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    println!("[smoke] tenant={tenant_id} device={device_id}");

    // MQTT 设备连接 + 订阅
    let addr = mqtt_addr();
    let (host, port) = addr.split_once(':').unwrap();
    let mut opts = MqttOptions::new(format!("smoke-{run_id}"), host, port.parse().unwrap());
    opts.set_credentials(&access_token, "");
    opts.set_keep_alive(Duration::from_secs(10));
    let (mqtt, mut el) = AsyncClient::new(opts, 16);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Packet>(16);
    tokio::spawn(async move {
        loop {
            match el.poll().await {
                Ok(Event::Incoming(p)) => {
                    if tx.send(p).await.is_err() {
                        return;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[smoke] mqtt eventloop error: {e}");
                    return;
                }
            }
        }
    });
    // 等 ConnAck
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(Packet::ConnAck(_)) = rx.recv().await {
                break;
            }
        }
    })
    .await
    .expect("connack timeout");
    mqtt.subscribe("v1/devices/me/rpc/request/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    mqtt.subscribe("v1/devices/me/attributes", QoS::AtLeastOnce)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await; // 等订阅经 Kafka 到 core 注册表
    println!("[smoke] mqtt connected & subscribed");

    // twoway RPC 往返
    let rpc = {
        let client = client.clone();
        let admin = admin.clone();
        let device_id = device_id.clone();
        tokio::spawn(async move {
            client
                .post(format!("{}/api/rpc/twoway/{device_id}", core_url()))
                .bearer_auth(&admin)
                .json(&json!({"method": "getStatus", "params": {"smoke": true}, "timeout": 15000}))
                .send()
                .await
                .unwrap()
        })
    };
    let publish = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(Packet::Publish(p)) = rx.recv().await
                && p.topic.starts_with("v1/devices/me/rpc/request/")
            {
                break p;
            }
        }
    })
    .await
    .expect("rpc downlink timeout");
    let rid = publish
        .topic
        .strip_prefix("v1/devices/me/rpc/request/")
        .unwrap()
        .to_string();
    let payload: Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(payload["method"], "getStatus");
    mqtt.publish(
        format!("v1/devices/me/rpc/response/{rid}"),
        QoS::AtLeastOnce,
        false,
        json!({"status": "ok"}).to_string(),
    )
    .await
    .unwrap();
    let resp = rpc.await.unwrap();
    assert_eq!(resp.status(), 200, "twoway rpc");
    assert_eq!(resp.json::<Value>().await.unwrap(), json!({"status": "ok"}));
    println!("[smoke] twoway rpc round trip OK");

    // SHARED_SCOPE 属性下行
    let resp = client
        .post(format!(
            "{}/api/telemetry/DEVICE/{device_id}/attributes/SHARED_SCOPE",
            core_url()
        ))
        .bearer_auth(&admin)
        .json(&json!({"smokeAttr": "hello"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let publish = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(Packet::Publish(p)) = rx.recv().await
                && p.topic == "v1/devices/me/attributes"
            {
                break p;
            }
        }
    })
    .await
    .expect("attribute downlink timeout");
    let payload: Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(payload["smokeAttr"], "hello");
    println!("[smoke] shared attribute downlink OK");

    // 设备活性：active=true
    let resp = client
        .get(format!(
            "{}/api/telemetry/DEVICE/{device_id}/values/attributes/SERVER_SCOPE",
            core_url()
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    let attrs = resp.json::<Value>().await.unwrap();
    assert!(
        attrs
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["key"] == "active" && e["value"] == true),
        "active attribute: {attrs}"
    );
    println!("[smoke] device active=true OK");

    mqtt.disconnect().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let resp = client
        .get(format!(
            "{}/api/telemetry/DEVICE/{device_id}/values/attributes/SERVER_SCOPE",
            core_url()
        ))
        .bearer_auth(&admin)
        .send()
        .await
        .unwrap();
    let attrs = resp.json::<Value>().await.unwrap();
    assert!(
        attrs
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["key"] == "active" && e["value"] == false),
        "active after disconnect: {attrs}"
    );
    println!("[smoke] device active=false after disconnect OK");
    println!("[smoke] ALL OK");
}
