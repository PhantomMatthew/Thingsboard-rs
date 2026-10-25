use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use snmp2::{MessageType, Pdu, Varbinds};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_transport_api::TransportApiClient;
use tb_transport_snmp::{RefreshReport, SnmpDeviceManager, SnmpRunner, TransportApiPublisher};
use tokio::net::UdpSocket;

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
const BROKERS: &str = "localhost:29092";
const UPTIME_OID: &str = "1.3.6.1.2.1.1.3.0";
const DESCRIPTION_OID: &str = "1.3.6.1.2.1.1.1.0";
const METADATA_OID: &str = "1.3.6.1.4.1.9999.1.0";

async fn start_core() -> (String, String, PgPool) {
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
        pool.clone(),
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
    (format!("http://{addr}"), run, pool)
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

async fn start_uptime_agent(uptime: i64, description: &[u8]) -> u16 {
    start_agent(HashMap::from([
        (UPTIME_OID.into(), tlv(0x02, &uptime.to_be_bytes())),
        (DESCRIPTION_OID.into(), tlv(0x04, description)),
        (METADATA_OID.into(), tlv(0x04, br#"{"site":"lab"}"#)),
    ]))
    .await
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

fn snmp_device(name: &str, port: u16, id: Option<&str>, profile_id: &str) -> Value {
    let mut body = json!({
        "name": name,
        "deviceProfileId": profile_id,
        "deviceData": {"transportConfiguration": {
            "host": "127.0.0.1", "port": port,
            "protocolVersion": "V2C", "community": "public"
        }}
    });
    match id {
        Some(id) => body["id"] = json!(id),
        None => body["accessToken"] = json!(format!("SNMP_E2E_{name}")),
    }
    body
}

async fn save_device(client: &reqwest::Client, core: &str, admin: &str, body: Value) -> String {
    let resp = client
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(admin))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "{body}");
    resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn latest(client: &reqwest::Client, core: &str, admin: &str, device_id: &str) -> Value {
    client
        .get(format!(
            "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries"
        ))
        .header("Authorization", bearer(admin))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()
}

async fn wait_for_uptime(
    client: &reqwest::Client,
    core: &str,
    admin: &str,
    device_id: &str,
    uptime: i64,
) {
    for _ in 0..75 {
        if latest(client, core, admin, device_id).await["uptime"][0]["value"] == json!(uptime) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("device {device_id} never reported uptime {uptime}");
}

async fn refresh(manager: &mut SnmpDeviceManager) -> RefreshReport {
    for _ in 0..6 {
        match manager.refresh().await {
            Ok(report) => return report,
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    panic!("SNMP refresh kept failing");
}

async fn ts_rows(pool: &PgPool, device_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM ts_kv WHERE entity_id = $1::uuid")
        .bind(device_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn snmp_discovery_pages_and_tracks_device_changes() {
    let (core, run, pool) = start_core().await;
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
    let agent_a = start_uptime_agent(42, b"sensor-a").await;
    let agent_b = start_uptime_agent(7, b"sensor-b").await;

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
    let first = save_device(
        &client,
        &core,
        &admin,
        snmp_device("snmp-a", agent_a, None, profile_id),
    )
    .await;

    let api = TransportApiClient::new(BROKERS, &format!("snmp-e2e-{run}")).unwrap();
    let producer = tb_queue::KafkaQueueProducer::new(BROKERS, "snmp-e2e-producer").unwrap();
    let runner = Arc::new(SnmpRunner::new(TransportApiPublisher::with_activity(
        api.clone(),
        producer,
        "snmp-e2e",
    )));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut manager = SnmpDeviceManager::new(api, runner, shutdown_rx).with_page_size(1);
    let report = refresh(&mut manager).await;
    assert_eq!((report.started, report.running), (1, 1), "{report:?}");

    wait_for_uptime(&client, &core, &admin, &first, 42).await;
    assert_eq!(
        latest(&client, &core, &admin, &first).await["description"][0]["value"],
        json!("sensor-a")
    );
    let mut found_attributes = false;
    for _ in 0..75 {
        let attributes = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{first}/values/attributes/CLIENT_SCOPE"
            ))
            .header("Authorization", bearer(&admin))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        found_attributes = attributes.as_array().is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry["key"] == "metadata" && entry["value"] == json!({"site": "lab"}))
        });
        if found_attributes {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(found_attributes, "SNMP attributes were not persisted");

    let second = save_device(
        &client,
        &core,
        &admin,
        snmp_device("snmp-b", agent_b, None, profile_id),
    )
    .await;
    let report = refresh(&mut manager).await;
    assert_eq!(
        report,
        RefreshReport {
            started: 1,
            restarted: 0,
            stopped: 0,
            running: 2
        }
    );
    wait_for_uptime(&client, &core, &admin, &second, 7).await;

    save_device(
        &client,
        &core,
        &admin,
        snmp_device("snmp-a", agent_b, Some(&first), profile_id),
    )
    .await;
    let report = refresh(&mut manager).await;
    assert_eq!(
        report,
        RefreshReport {
            started: 0,
            restarted: 1,
            stopped: 0,
            running: 2
        }
    );
    wait_for_uptime(&client, &core, &admin, &first, 7).await;

    let resp = client
        .delete(format!("{core}/api/device/{second}"))
        .header("Authorization", bearer(&admin))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let report = refresh(&mut manager).await;
    assert_eq!(
        report,
        RefreshReport {
            started: 0,
            restarted: 0,
            stopped: 1,
            running: 1
        }
    );
    tokio::time::sleep(Duration::from_secs(1)).await;
    let drained = ts_rows(&pool, &second).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(ts_rows(&pool, &second).await, drained);

    assert_eq!(
        refresh(&mut manager).await,
        RefreshReport {
            started: 0,
            restarted: 0,
            stopped: 0,
            running: 1
        }
    );
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
