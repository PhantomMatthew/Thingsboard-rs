//! Phase 9a CoAP 端到端：上行遥测/属性/RPC + GET attributes + 错误码；
//! Observe 属性推送（含删除 null）；Observe RPC 往返（推送 → POST rpc/{id} → REST 拿到结果）。
//! 客户端为 coap-lite + tokio UdpSocket 直连实现。

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use coap_lite::option_value::OptionValueString;
use coap_lite::{
    CoapOption, ContentFormat, MessageClass, MessageType, Packet, RequestType, ResponseType,
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use common::*;

/// 极简 CoAP 客户端：CON 请求等匹配 message_id 的 ACK 响应，其余包进队列供 observe 断言。
struct CoapDevice {
    server: SocketAddr,
    rx: mpsc::Receiver<Packet>,
    next_mid: AtomicU16,
    next_token: std::sync::atomic::AtomicU64,
    socket: Arc<UdpSocket>,
}

impl CoapDevice {
    async fn connect(server: SocketAddr) -> Self {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let (tx, rx) = mpsc::channel(64);
        let pump_socket = socket.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1500];
            loop {
                match pump_socket.recv_from(&mut buf).await {
                    Ok((n, _)) => {
                        if let Ok(p) = Packet::from_bytes(&buf[..n])
                            && tx.send(p).await.is_err()
                        {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        });
        Self {
            server,
            rx,
            next_mid: AtomicU16::new(100),
            next_token: std::sync::atomic::AtomicU64::new(1),
            socket,
        }
    }

    fn build_request(
        &self,
        method: RequestType,
        path: &str,
        queries: &[&str],
        observe: Option<u32>,
        payload: Vec<u8>,
    ) -> Packet {
        let mut p = Packet::new();
        p.header.set_version(1);
        p.header.set_type(MessageType::Confirmable);
        p.header.code = MessageClass::Request(method);
        p.header.message_id = self.next_mid.fetch_add(1, Ordering::Relaxed);
        p.set_token(
            self.next_token
                .fetch_add(1, Ordering::Relaxed)
                .to_be_bytes()
                .to_vec(),
        );
        for seg in path.trim_start_matches('/').split('/') {
            p.add_option(CoapOption::UriPath, seg.as_bytes().to_vec());
        }
        for q in queries {
            p.add_option_as(
                CoapOption::UriQuery,
                OptionValueString(q.to_string()),
            );
        }
        if let Some(v) = observe {
            p.set_observe_value(v);
        }
        if !payload.is_empty() {
            p.set_content_format(ContentFormat::ApplicationJSON);
            p.payload = payload;
        }
        p
    }

    async fn send(&self, p: &Packet) {
        self.socket
            .send_to(&p.to_bytes().unwrap(), self.server)
            .await
            .unwrap();
    }

    /// 发请求并等 ACK 响应；途中的 NON 推送留在队列里（已被消费跳过）。
    async fn request(
        &mut self,
        method: RequestType,
        path: &str,
        queries: &[&str],
        observe: Option<u32>,
        payload: Vec<u8>,
    ) -> Packet {
        let p = self.build_request(method, path, queries, observe, payload);
        let mid = p.header.message_id;
        self.send(&p).await;
        let start = std::time::Instant::now();
        loop {
            let pkt = tokio::time::timeout(Duration::from_secs(5), self.rx.recv())
                .await
                .expect("coap response timeout")
                .expect("pump closed");
            if pkt.header.message_id == mid
                && matches!(pkt.header.code, MessageClass::Response(_))
            {
                return pkt;
            }
            assert!(start.elapsed() < Duration::from_secs(5), "no matching ack");
        }
    }

}

fn status(pkt: &Packet) -> ResponseType {
    match pkt.header.code {
        MessageClass::Response(t) => t,
        _ => panic!("not a response"),
    }
}

fn is_notification(p: &Packet) -> bool {
    p.header.get_type() == MessageType::NonConfirmable
        && matches!(p.header.code, MessageClass::Response(ResponseType::Content))
}

/// a) 上行：POST telemetry/attributes + REST 读回；GET attributes；错误 token 4.01；claim 5.01。
#[tokio::test]
async fn coap_uplink_and_get_attributes() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, coap_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "CoAP Tenant", "coap-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "coap-device", "COAP_TOKEN").await;

    let mut device = CoapDevice::connect(coap_addr).await;

    // POST telemetry（{ts,values} 形式）→ 2.04
    let resp = device
        .request(
            RequestType::Post,
            "/api/v1/COAP_TOKEN/telemetry",
            &[],
            None,
            json!({"ts": 1700000000000i64, "values": {"temperature": 25}}).to_string().into_bytes(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::Changed);

    poll_until(Duration::from_secs(20), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=temperature"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        body["temperature"]
            .as_array()
            .is_some_and(|a| a.iter().any(|e| e["value"] == 25))
    })
    .await;

    // 纯 {k:v} 形式
    let resp = device
        .request(
            RequestType::Post,
            "/api/v1/COAP_TOKEN/telemetry",
            &[],
            None,
            json!({"humidity": 60}).to_string().into_bytes(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::Changed);

    // POST attributes → CLIENT_SCOPE
    let resp = device
        .request(
            RequestType::Post,
            "/api/v1/COAP_TOKEN/attributes",
            &[],
            None,
            json!({"firmware": "1.0.0"}).to_string().into_bytes(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::Changed);

    poll_until(Duration::from_secs(20), async || {
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/attributes/CLIENT_SCOPE"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        body.as_array()
            .is_some_and(|a| a.iter().any(|e| e["key"] == "firmware" && e["value"] == "1.0.0"))
    })
    .await;

    // GET attributes（带 query）→ 2.05 {client:{firmware}}
    let resp = device
        .request(
            RequestType::Get,
            "/api/v1/COAP_TOKEN/attributes",
            &["clientKeys=firmware"],
            None,
            Vec::new(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::Content);
    let body: Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(body["client"]["firmware"], "1.0.0");
    assert!(body["shared"].as_object().unwrap().is_empty());

    // POST rpc（to-server）→ 2.04
    let resp = device
        .request(
            RequestType::Post,
            "/api/v1/COAP_TOKEN/rpc",
            &[],
            None,
            json!({"method": "getStatus", "params": {"verbose": true}})
                .to_string()
                .into_bytes(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::Changed);

    // 错误 token → 4.01
    let resp = device
        .request(
            RequestType::Post,
            "/api/v1/NO_SUCH_TOKEN/telemetry",
            &[],
            None,
            json!({"temperature": 1}).to_string().into_bytes(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::Unauthorized);

    // claim → 5.01
    let resp = device
        .request(
            RequestType::Post,
            "/api/v1/COAP_TOKEN/claim",
            &[],
            None,
            json!({"secretKey": "x", "durationMs": 1000})
                .to_string()
                .into_bytes(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::NotImplemented);

    // 未知路径 → 4.04
    let resp = device
        .request(RequestType::Get, "/api/v1/COAP_TOKEN/unknown", &[], None, Vec::new())
        .await;
    assert_eq!(status(&resp), ResponseType::NotFound);
}

/// b) observe attributes：GET+Observe=0 注册 → REST SHARED_SCOPE 保存/删除 → NON 推送。
#[tokio::test]
async fn coap_observe_attributes_downlink() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, coap_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "Obs Tenant", "obs-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "obs-device", "OBS_TOKEN").await;

    let mut device = CoapDevice::connect(coap_addr).await;
    let resp = device
        .request(
            RequestType::Get,
            "/api/v1/OBS_TOKEN/attributes",
            &[],
            Some(0),
            Vec::new(),
        )
        .await;
    assert_eq!(status(&resp), ResponseType::Content);
    assert_eq!(resp.get_observe_value().and_then(|v| v.ok()), Some(0));
    let obs_token = resp.get_token().to_vec();

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
        if let Ok(pkt) = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let pkt = device.rx.recv().await.expect("pump closed");
                if is_notification(&pkt) && pkt.get_token() == obs_token.as_slice() {
                    return pkt;
                }
            }
        })
        .await
        {
            break serde_json::from_slice::<Value>(&pkt.payload).unwrap();
        }
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "attribute update never arrived"
        );
    };
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
        if let Ok(v) = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let pkt = device.rx.recv().await.expect("pump closed");
                if is_notification(&pkt) && pkt.get_token() == obs_token.as_slice() {
                    let v: Value = serde_json::from_slice(&pkt.payload).unwrap();
                    if v.get("targetTemp").is_some() {
                        return v;
                    }
                }
            }
        })
        .await
        {
            break v;
        }
        assert!(
            start.elapsed() < Duration::from_secs(40),
            "attribute delete notification never arrived"
        );
    };
    assert_eq!(payload["targetTemp"], Value::Null);
}

/// c) observe rpc：GET+Observe=0 → REST twoway 推送 {"id","method","params"}
/// → 设备 POST rpc/{requestId} → REST 拿到响应。
#[tokio::test]
async fn coap_observe_rpc_round_trip() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, coap_addr) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "CoapRpc Tenant", "coap-rpc-tenant@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "coap-rpc-device", "CRPC_TOKEN")
            .await;

    let mut device = CoapDevice::connect(coap_addr).await;
    let resp = device
        .request(RequestType::Get, "/api/v1/CRPC_TOKEN/rpc", &[], Some(0), Vec::new())
        .await;
    assert_eq!(status(&resp), ResponseType::Content);
    let obs_token = resp.get_token().to_vec();

    async fn twoway(
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

    // 注册表经 Kafka 异步建立：轮询直到某轮 twoway 被送达并完成往返。
    let start = std::time::Instant::now();
    let body = loop {
        let http = {
            let client = client.clone();
            let core = core.clone();
            let token = admin_token.clone();
            let device_id = device_id.clone();
            tokio::spawn(async move { twoway(&client, &core, &token, &device_id, 5000).await })
        };
        let request: Option<Packet> = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let pkt = device.rx.recv().await.expect("pump closed");
                if is_notification(&pkt) && pkt.get_token() == obs_token.as_slice() {
                    return pkt;
                }
            }
        })
        .await
        .ok();
        if let Some(pkt) = request {
            let payload: Value = serde_json::from_slice(&pkt.payload).unwrap();
            assert_eq!(payload["method"], "getStatus");
            assert_eq!(payload["params"], json!({"verbose": true}));
            let request_id = payload["id"].as_i64().unwrap();
            let resp = device
                .request(
                    RequestType::Post,
                    &format!("/api/v1/CRPC_TOKEN/rpc/{request_id}"),
                    &[],
                    None,
                    json!({"status": "ok", "fw": 3}).to_string().into_bytes(),
                )
                .await;
            assert_eq!(status(&resp), ResponseType::Changed);
            let (http_status, body) = http.await.unwrap();
            if http_status == StatusCode::OK {
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
}
