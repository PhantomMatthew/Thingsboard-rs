mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use prost::Message as _;
use reqwest::StatusCode;
use rumqttc::{AsyncClient, ConnectReturnCode, ConnectionError, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_auth::JwtTokenFactory;
use tb_core::{AppState, build_router};
use tb_proto::core as core_pb;
use tb_proto::ruleengine::{TbMsgProto, ToRuleEngineMsg};
use tb_queue::{QueueConsumer as _, StreamQueueConsumer, TbTopic};
use tokio::sync::mpsc;

const CONN_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
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
        match tokio::time::timeout(CONN_WAIT, device.rx.recv()).await {
            Ok(Some(PumpEvent::Packet(Packet::ConnAck(ack))))
                if ack.code == ConnectReturnCode::Success =>
            {
                return device;
            }
            Ok(Some(PumpEvent::Refused(code))) => {
                tracing::debug!(?code, "mqtt connection refused");
            }
            _ => {}
        }
        assert!(start.elapsed() < Duration::from_secs(25));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for(
    rx: &mut mpsc::Receiver<PumpEvent>,
    timeout: Duration,
    pred: impl Fn(&Packet) -> bool,
) -> Packet {
    tokio::time::timeout(timeout, async {
        loop {
            match rx.recv().await {
                Some(PumpEvent::Packet(packet)) if pred(&packet) => return packet,
                Some(PumpEvent::Packet(_)) => {}
                other => panic!("connection closed while waiting for packet: {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for mqtt packet")
}

async fn serve_core(state: AppState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind core");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

async fn start_two_cores() -> (String, String, SocketAddr, String, String, String) {
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| KAFKA_BROKERS.into());
    let uuid = uuid::Uuid::now_v7().simple().to_string();
    let run_id = &uuid[20..];
    let db_name = format!("tb_test_multi_{run_id}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
        .await
        .expect("connect admin db");
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    let url = format!("postgres://thingsboard:thingsboard@localhost:5433/{db_name}");
    let connect = || async {
        PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .expect("connect test db")
    };
    let pool1 = connect().await;
    tb_dao::migrate(&pool1).await.unwrap();
    let pool2: PgPool = connect().await;
    let factory = Arc::new(JwtTokenFactory::new("test-secret", 900, 604800));
    let node1 = format!("core-1-{run_id}");
    let node2 = format!("core-2-{run_id}");
    let state1 = AppState::new_with_node_id(pool1, factory.clone(), node1.clone())
        .await
        .unwrap();
    tb_core::seed::seed(&state1).await.unwrap();
    let state2 = AppState::new_with_node_id(pool2, factory, node2.clone())
        .await
        .unwrap();
    let base1 = serve_core(state1.clone()).await;
    let base2 = serve_core(state2.clone()).await;
    let group = |name: &str| format!("{name}-multi-{run_id}");
    for state in [state1.clone(), state2.clone()] {
        let _transport_api = tb_core::transport_api::start_transport_api_for_group(
            state.clone(),
            &group("tb-core-transport-api"),
        )
        .await;
        let _core_consumer =
            tb_core::core_consumer::start_core_consumer_for_group(state, &group("tb-core-main"))
                .await;
    }
    let mqtt_node = format!("mqtt-multi-{run_id}");
    let mqtt_state = tb_transport_mqtt::TransportState::new(&brokers, &mqtt_node).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mqtt_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tb_transport_mqtt::serve(listener, mqtt_state)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_secs(3)).await;
    (base1, base2, mqtt_addr, node1, node2, brokers)
}

async fn twoway_rpc(
    client: &reqwest::Client,
    core: &str,
    token: &str,
    device_id: &str,
) -> (StatusCode, Value) {
    let response = client
        .post(format!("{core}/api/rpc/twoway/{device_id}"))
        .header("Authorization", bearer(token))
        .json(&json!({"method": "getStatus", "params": {}, "timeout": 5000}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.json().await.unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn rpc_response_returns_to_initiating_core() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (base1, base2, mqtt_addr, core1_node, core2_node, brokers) = start_two_cores().await;
    let (admin_token, _) =
        setup_tenant_admin(&client, &base1, "Multi RPC", "multi-rpc@tb.io").await;
    let mut response_probes = Vec::new();
    for node in [&core1_node, &core2_node] {
        let topic = TbTopic::core_rpc_responses(node);
        let group = format!("rpc-response-probe-{}", uuid::Uuid::now_v7().simple());
        response_probes
            .push(StreamQueueConsumer::new_from_beginning(&brokers, &group, &[&topic]).unwrap());
    }

    let mut forwarded = false;
    for attempt in 0..8usize {
        let (initiating_base, reply_node) = if attempt % 2 == 0 {
            (&base1, &core1_node)
        } else {
            (&base2, &core2_node)
        };
        let response_probe = &mut response_probes[attempt % 2];
        let token = format!("MULTI_RPC_TOKEN_{attempt}");
        let device_id = create_device_with_token(
            &client,
            &base1,
            &admin_token,
            &format!("multi-rpc-{attempt}"),
            &token,
        )
        .await;
        let mut device =
            connect_until_success(mqtt_addr, &format!("multi-rpc-client-{attempt}"), &token).await;
        device
            .client
            .subscribe("v1/devices/me/rpc/request/+", QoS::AtLeastOnce)
            .await
            .unwrap();
        wait_for(&mut device.rx, Duration::from_secs(10), |packet| {
            matches!(packet, Packet::SubAck(_))
        })
        .await;
        poll_until(Duration::from_secs(20), async || {
            let response = client
                .post(format!("{initiating_base}/api/rpc/oneway/{device_id}"))
                .header("Authorization", bearer(&admin_token))
                .json(&json!({"method": "warmup", "params": {}, "timeout": 1000}))
                .send()
                .await
                .unwrap();
            response.status() == StatusCode::OK
        })
        .await;
        let _ = wait_for(&mut device.rx, Duration::from_secs(10), |packet| {
            matches!(packet, Packet::Publish(p) if p.topic.starts_with("v1/devices/me/rpc/request/"))
        })
        .await;

        let http = {
            let client = client.clone();
            let core = initiating_base.to_string();
            let admin_token = admin_token.clone();
            let device_id = device_id.clone();
            tokio::spawn(async move { twoway_rpc(&client, &core, &admin_token, &device_id).await })
        };
        let request = wait_for(&mut device.rx, Duration::from_secs(10), |packet| {
            matches!(packet, Packet::Publish(p) if p.topic.starts_with("v1/devices/me/rpc/request/"))
        })
        .await;
        let Packet::Publish(request) = request else {
            unreachable!()
        };
        let request_id = request
            .topic
            .strip_prefix("v1/devices/me/rpc/request/")
            .unwrap()
            .parse::<i32>()
            .unwrap();
        let expected = json!({"status": "ok", "attempt": attempt});
        device
            .client
            .publish(
                format!("v1/devices/me/rpc/response/{request_id}"),
                QoS::AtLeastOnce,
                false,
                expected.to_string(),
            )
            .await
            .unwrap();
        let (status, body) = http.await.unwrap();
        assert_eq!(status, StatusCode::OK, "RPC attempt {attempt}: {body}");
        assert_eq!(body, expected);

        let probe_result = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let msg = response_probe.recv().await.unwrap();
                let Ok(core_msg) = core_pb::ToCoreMsg::decode(msg.payload) else {
                    continue;
                };
                let Some(core_pb::to_core_msg::Msg::RpcResponse(response)) = core_msg.msg else {
                    continue;
                };
                if response.request_id == request_id
                    && response.reply_node_id == reply_node.as_str()
                    && response
                        .session
                        .as_ref()
                        .is_some_and(|s| s.device_id == device_id)
                {
                    return true;
                }
            }
        })
        .await;
        if probe_result.is_ok() {
            forwarded = true;
            device.client.disconnect().await.unwrap();
            break;
        }
        device.client.disconnect().await.unwrap();
    }
    assert!(forwarded, "no RPC response crossed core instances");
}

#[tokio::test]
async fn credentials_update_disconnects_mqtt_device() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _) = setup_tenant_admin(&client, &core, "Kick", "kick@tb.io").await;
    let device_id =
        create_device_with_token(&client, &core, &admin_token, "kick-device", "KICK_TOKEN").await;
    let mut device = connect_until_success(mqtt_addr, "kick-client", "KICK_TOKEN").await;
    poll_until(Duration::from_secs(20), async || {
        let response = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/attributes/SERVER_SCOPE"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        response
            .json::<Value>()
            .await
            .unwrap_or(Value::Null)
            .as_array()
            .is_some_and(|attrs| {
                attrs
                    .iter()
                    .any(|attr| attr["key"] == "active" && attr["value"] == true)
            })
    })
    .await;

    let credentials_url = format!("{core}/api/device/{device_id}/credentials");
    let mut credentials: Value = client
        .get(&credentials_url)
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    credentials["credentialsId"] = json!("KICK_TOKEN_ROTATED");
    let response = client
        .post(format!("{core}/api/device/credentials"))
        .header("Authorization", bearer(&admin_token))
        .json(&credentials)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match device.rx.recv().await {
                Some(PumpEvent::Closed) => return,
                Some(PumpEvent::Packet(_)) => {}
                other => panic!("unexpected mqtt event after kick: {other:?}"),
            }
        }
    })
    .await
    .expect("device was not disconnected after credentials update");
    poll_until(Duration::from_secs(20), async || {
        let response = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/attributes/SERVER_SCOPE"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        response
            .json::<Value>()
            .await
            .unwrap_or(Value::Null)
            .as_array()
            .is_some_and(|attrs| {
                attrs
                    .iter()
                    .any(|attr| attr["key"] == "active" && attr["value"] == false)
            })
    })
    .await;
}

async fn next_rule_event(
    consumer: &mut StreamQueueConsumer,
    device_id: &str,
    msg_type: &str,
    timeout: Duration,
) -> TbMsgProto {
    tokio::time::timeout(timeout, async {
        loop {
            let msg = consumer.recv().await.unwrap();
            let Ok(envelope) = ToRuleEngineMsg::decode(msg.payload) else {
                continue;
            };
            let Some(tb_msg) = envelope.tb_msg else {
                continue;
            };
            if tb_msg.originator_id == device_id && tb_msg.r#type == msg_type {
                return tb_msg;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {msg_type}"))
}

#[tokio::test]
async fn lifecycle_and_rest_attribute_events_reach_rule_engine() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, mqtt_addr) = start_servers().await;
    let (admin_token, _) = setup_tenant_admin(&client, &core, "Lifecycle", "lifecycle@tb.io").await;
    let device_id = create_device_with_token(
        &client,
        &core,
        &admin_token,
        "lifecycle-device",
        "LIFECYCLE_TOKEN",
    )
    .await;
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| KAFKA_BROKERS.into());
    let topic = TbTopic::rule_engine("Main");
    let group = format!("lifecycle-probe-{}", uuid::Uuid::now_v7().simple());
    let mut probe = StreamQueueConsumer::new_from_beginning(&brokers, &group, &[&topic]).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let device = connect_until_success(mqtt_addr, "lifecycle-client", "LIFECYCLE_TOKEN").await;
    let connected = next_rule_event(
        &mut probe,
        &device_id,
        "CONNECT_EVENT",
        Duration::from_secs(15),
    )
    .await;
    assert_eq!(connected.data_json, "{}");
    assert_eq!(connected.metadata["deviceName"], "lifecycle-device");
    assert_eq!(connected.metadata["deviceType"], "default");

    device
        .client
        .publish(
            "v1/devices/me/telemetry",
            QoS::AtLeastOnce,
            false,
            json!({"temperature": 21}).to_string(),
        )
        .await
        .unwrap();
    next_rule_event(
        &mut probe,
        &device_id,
        "ACTIVITY_EVENT",
        Duration::from_secs(15),
    )
    .await;

    let attributes_url = format!("{core}/api/telemetry/DEVICE/{device_id}/attributes/SERVER_SCOPE");
    let response = client
        .post(&attributes_url)
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"ruleTest": 42}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let updated = next_rule_event(
        &mut probe,
        &device_id,
        "ATTRIBUTES_UPDATED",
        Duration::from_secs(15),
    )
    .await;
    let data: Value = serde_json::from_str(&updated.data_json).unwrap();
    assert_eq!(
        data,
        json!({"scope": "SERVER_SCOPE", "kv": {"ruleTest": 42}})
    );

    let response = client
        .delete(format!("{attributes_url}?keys=ruleTest"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let deleted = next_rule_event(
        &mut probe,
        &device_id,
        "ATTRIBUTES_DELETED",
        Duration::from_secs(15),
    )
    .await;
    let data: Value = serde_json::from_str(&deleted.data_json).unwrap();
    assert_eq!(
        data,
        json!({"scope": "SERVER_SCOPE", "kv": {"ruleTest": null}})
    );

    device.client.disconnect().await.unwrap();
    next_rule_event(
        &mut probe,
        &device_id,
        "DISCONNECT_EVENT",
        Duration::from_secs(15),
    )
    .await;
}
