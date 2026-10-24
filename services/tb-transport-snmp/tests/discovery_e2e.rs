use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};
use snmp2::{MessageType, Pdu, Varbinds};
use sqlx::postgres::PgPoolOptions;
use tb_transport_api::TransportApiClient;
use tb_transport_snmp::{SnmpRunner, TransportApiPublisher, discover_devices};
use tokio::net::UdpSocket;

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
const BROKERS: &str = "localhost:29092";
const UPTIME_OID: &str = "1.3.6.1.2.1.1.3.0";
const DESCRIPTION_OID: &str = "1.3.6.1.2.1.1.1.0";
const METADATA_OID: &str = "1.3.6.1.4.1.9999.1.0";

async fn start_core() -> (String, String) {
    let run = uuid::Uuid::now_v7().simple().to_string();
    let db_name = format!("tb_snmp_e2e_{run}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    let url = format!("postgres://thingsboard:thingsboard@localhost:5433/{db_name}");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap();
    tb_dao::migrate(&pool).await.unwrap();
    let state = tb_core::AppState::new(
        pool,
        Arc::new(tb_auth::JwtTokenFactory::new("test-secret", 900, 604800)),
    )
    .await
    .unwrap();
    tb_core::seed::seed(&state).await.unwrap();
    let _transport_api = tb_core::transport_api::start_transport_api_for_group(
        state.clone(),
        &format!("tb-snmp-api-{run}"),
    )
    .await;
    let _consumer = tb_core::core_consumer::start_core_consumer_for_group(
        state.clone(),
        &format!("tb-snmp-core-{run}"),
    )
    .await;
    let _ingest = tb_core::telemetry_ingest::start_telemetry_ingest_for_group(
        state.clone(),
        &format!("tb-snmp-ingest-{run}"),
    )
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, tb_core::build_router(state))
            .await
            .unwrap();
    });
    (format!("http://{addr}"), run)
}

async fn start_agent(values: HashMap<String, Vec<u8>>) -> u16 {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = socket.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut buffer = vec![0_u8; 65_507];
        while let Ok((size, peer)) = socket.recv_from(&mut buffer).await {
            let mut request = Pdu::from_bytes(&buffer[..size]).unwrap();
            if request.community != b"public" {
                continue;
            }
            let mut response_varbinds = Vec::new();
            for (oid, _) in request.varbinds.clone() {
                let name = oid.to_string();
                let value = values.get(&name).cloned().unwrap_or_else(|| vec![0x80, 0]);
                response_varbinds.extend(sequence(&[tlv(0x06, oid.as_bytes()), value]));
            }
            request.message_type = MessageType::Response;
            request.error_status = 0;
            request.error_index = 0;
            request.varbinds = Varbinds::from_bytes(&response_varbinds);
            socket
                .send_to(&request.to_bytes().unwrap(), peer)
                .await
                .unwrap();
        }
    });
    port
}

async fn login(client: &reqwest::Client, core: &str, email: &str, password: &str) -> String {
    client
        .post(format!("{core}/api/auth/login"))
        .json(&json!({"username": email, "password": password}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string()
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

#[tokio::test]
async fn rest_discovers_snmp_device_and_reads_udp_values() {
    let (core, run) = start_core().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let sys = login(&client, &core, "sysadmin@thingsboard.org", "sysadmin").await;
    let tenant: Value = client
        .post(format!("{core}/api/tenant"))
        .header("Authorization", bearer(&sys))
        .json(&json!({"title": "SNMP E2E"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tenant_id = tenant["id"].as_str().unwrap();
    let resp = client
        .post(format!("{core}/api/user"))
        .header("Authorization", bearer(&sys))
        .json(&json!({"tenantId": tenant_id, "email": "snmp@tb.io", "authority": "TENANT_ADMIN", "password": "tenant"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let admin = login(&client, &core, "snmp@tb.io", "tenant").await;
    let agent_port = start_agent(HashMap::from([
        (UPTIME_OID.into(), tlv(0x02, &42_i64.to_be_bytes())),
        (DESCRIPTION_OID.into(), tlv(0x04, b"sensor-a")),
        (METADATA_OID.into(), tlv(0x04, br#"{"site":"lab"}"#)),
    ]))
    .await;

    let profile: Value = client
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "name": "snmp-e2e-profile",
            "transportType": "SNMP",
            "profileData": {"transportConfiguration": {
                "timeoutMs": 100,
                "retries": 0,
                "communicationConfigs": [
                    {"spec": "TELEMETRY_QUERYING", "queryingFrequencyMs": 100, "mappings": [
                        {"oid": UPTIME_OID, "key": "uptime", "dataType": "LONG"},
                        {"oid": DESCRIPTION_OID, "key": "description", "dataType": "STRING"}
                    ]},
                    {"spec": "CLIENT_ATTRIBUTES_QUERYING", "queryingFrequencyMs": 100, "mappings": [
                        {"oid": METADATA_OID, "key": "metadata", "dataType": "JSON"}
                    ]}
                ]
            }}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let profile_id = profile["id"].as_str().unwrap();
    let device: Value = client
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "name": "snmp-e2e-device",
            "deviceProfileId": profile_id,
            "accessToken": "SNMP_E2E_TOKEN",
            "deviceData": {"transportConfiguration": {
                "host": "127.0.0.1", "port": agent_port,
                "protocolVersion": "V2C", "community": "public"
            }}
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let device_id = device["id"].as_str().unwrap();

    let api = TransportApiClient::new(BROKERS, &format!("snmp-e2e-{run}")).unwrap();
    let producer = tb_queue::KafkaQueueProducer::new(BROKERS, "snmp-e2e-producer").unwrap();
    let runner = Arc::new(SnmpRunner::new(TransportApiPublisher::with_activity(
        api.clone(),
        producer,
        "snmp-e2e",
    )));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut discovered = None;
    for _ in 0..6 {
        match discover_devices(&api, runner.clone(), shutdown_rx.clone()).await {
            Ok(count) => {
                discovered = Some(count);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
        }
    }
    assert_eq!(discovered, Some(1));

    let mut found_telemetry = false;
    let mut found_attributes = false;
    for _ in 0..75 {
        let telemetry = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries"
            ))
            .header("Authorization", bearer(&admin))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        let attributes = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{device_id}/values/attributes/CLIENT_SCOPE"
            ))
            .header("Authorization", bearer(&admin))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        found_telemetry = telemetry["uptime"][0]["value"] == json!(42)
            && telemetry["description"][0]["value"] == json!("sensor-a");
        found_attributes = attributes.as_array().is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry["key"] == "metadata" && entry["value"] == json!({"site": "lab"}))
        });
        if found_telemetry && found_attributes {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(found_telemetry, "SNMP telemetry was not persisted");
    assert!(found_attributes, "SNMP attributes were not persisted");
}

fn sequence(parts: &[Vec<u8>]) -> Vec<u8> {
    tlv(0x30, &parts.concat())
}

fn tlv(tag: u8, value: &[u8]) -> Vec<u8> {
    assert!(value.len() < 128);
    let mut encoded = vec![tag, value.len() as u8];
    encoded.extend_from_slice(value);
    encoded
}
