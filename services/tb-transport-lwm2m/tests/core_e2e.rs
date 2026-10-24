use std::net::SocketAddr;
use std::sync::Arc;

use coap_lite::{
    CoapOption, ContentFormat, MessageClass, MessageType, Packet, RequestType, ResponseType,
};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tb_common::TbResult;
use tb_transport_api::TransportApiClient;
use tb_transport_lwm2m::codec::{self, Tlv};
use tb_transport_lwm2m::integration::{
    CoreSessionActivity, EndpointIdentity, EndpointValidator, TransportApiUplink,
};
use tb_transport_lwm2m::objects::standard_resource_types;
use tb_transport_lwm2m::server::{Lwm2mServer, ServerConfig};
use tokio::net::UdpSocket;
use uuid::Uuid;

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
const BROKERS: &str = "localhost:29092";

struct CoreValidator {
    client: TransportApiClient,
}

impl EndpointValidator for CoreValidator {
    fn validate<'a>(
        &'a self,
        endpoint: &'a str,
        _peer: SocketAddr,
    ) -> BoxFuture<'a, TbResult<Option<EndpointIdentity>>> {
        Box::pin(async move {
            let response = self.client.validate_lwm2m(endpoint).await?;
            if !response.valid {
                return Ok(None);
            }
            let (Some(device), Some(profile)) = (response.device, response.profile) else {
                return Ok(None);
            };
            Ok(Some(EndpointIdentity {
                device,
                profile,
                resource_types: standard_resource_types(),
            }))
        })
    }
}

async fn start_core() -> (String, String) {
    let run = Uuid::now_v7().simple().to_string();
    let db_name = format!("tb_lwm2m_e2e_{run}");
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
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://thingsboard:thingsboard@localhost:5433/{db_name}"
        ))
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
        &format!("tb-lwm2m-api-{run}"),
    )
    .await;
    let _consumer = tb_core::core_consumer::start_core_consumer_for_group(
        state.clone(),
        &format!("tb-lwm2m-core-{run}"),
    )
    .await;
    let _ingest = tb_core::telemetry_ingest::start_telemetry_ingest_for_group(
        state.clone(),
        &format!("tb-lwm2m-ingest-{run}"),
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

fn packet(method: RequestType, mid: u16, path: &str, query: &[&str]) -> Packet {
    let mut packet = Packet::new();
    packet.header.set_type(MessageType::Confirmable);
    packet.header.code = MessageClass::Request(method);
    packet.header.message_id = mid;
    packet.set_token(vec![mid as u8, 0x55]);
    for segment in path.trim_matches('/').split('/') {
        packet.add_option(CoapOption::UriPath, segment.as_bytes().to_vec());
    }
    for item in query {
        packet.add_option(CoapOption::UriQuery, item.as_bytes().to_vec());
    }
    packet
}

fn response(request: &Packet, value: f32, observe: Option<u32>) -> Packet {
    let mut response = Packet::new();
    response.header.set_type(MessageType::Acknowledgement);
    response.header.code = MessageClass::Response(ResponseType::Content);
    response.header.message_id = request.header.message_id;
    response.set_token(request.get_token().to_vec());
    response.set_content_format(ContentFormat::ApplicationVndOmaLwm2mTlv);
    if let Some(observe) = observe {
        response.set_observe_value(observe);
    }
    let resource = request
        .get_option(CoapOption::UriPath)
        .unwrap()
        .iter()
        .nth(2)
        .map(|part| {
            String::from_utf8(part.clone())
                .unwrap()
                .parse::<u16>()
                .unwrap()
        })
        .unwrap();
    response.payload = codec::encode(&[Tlv::Resource {
        id: resource,
        value: &value.to_be_bytes(),
    }]);
    response
}

#[tokio::test]
async fn lwm2m_registration_observe_reaches_core() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("tb_core=debug,tb_transport_lwm2m=debug,tb_transport_api=debug")
        .try_init();
    let (core, run) = start_core().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let sys = login(&client, &core, "sysadmin@thingsboard.org", "sysadmin").await;
    let tenant: Value = client
        .post(format!("{core}/api/tenant"))
        .header("Authorization", bearer(&sys))
        .json(&json!({"title": "LwM2M E2E"}))
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
        .json(&json!({"tenantId": tenant_id, "email": "lwm2m@tb.io", "authority": "TENANT_ADMIN", "password": "tenant"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let admin = login(&client, &core, "lwm2m@tb.io", "tenant").await;
    let profile: Value = client
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "name": "lwm2m-e2e-profile",
            "transportType": "LWM2M",
            "profileData": {"observeAttr": {
                "keyName": {"/3303/0/5700": "temperature", "/3304/0/5700": "humidity"},
                "telemetry": ["/3303/0/5700"],
                "attribute": ["/3304/0/5700"],
                "observeStrategy": "SINGLE"
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
        .json(&json!({"name": "lwm2m-e2e-device", "deviceProfileId": profile_id, "accessToken": "LWM2M_TOKEN"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let device_id = device["id"].as_str().unwrap();
    let credential_response = client
        .post(format!("{core}/api/device/credentials"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "id": Uuid::now_v7(), "createdTime": 0, "deviceId": device_id,
            "credentialsType": "LWM2M", "credentialsId": "lwm2m-e2e",
            "credentialsValue": json!({"endpoint": "lwm2m-e2e", "securityConfigClientMode": "NO_SEC"}).to_string()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(credential_response.status(), reqwest::StatusCode::OK);

    let api = TransportApiClient::new(BROKERS, &format!("lwm2m-e2e-{run}")).unwrap();
    let mut validated = false;
    for _ in 0..6 {
        match api.validate_lwm2m("lwm2m-e2e").await {
            Ok(response) if response.valid => {
                validated = true;
                break;
            }
            Ok(response) => {
                if response.device.is_none() {
                    continue;
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    assert!(validated, "core rejected LwM2M credentials");
    let producer = tb_queue::KafkaQueueProducer::new(BROKERS, "lwm2m-e2e-producer").unwrap();
    let server = Lwm2mServer::new(
        ServerConfig::default(),
        Arc::new(CoreValidator {
            client: api.clone(),
        }),
        Arc::new(TransportApiUplink::new(api)),
        Arc::new(CoreSessionActivity::new(producer, "lwm2m-e2e")),
    )
    .unwrap();
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let server_addr = socket.local_addr().unwrap();
    tokio::spawn(async move { server.serve(socket).await.unwrap() });
    let client_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_socket.connect(server_addr).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let mut registration = packet(
        RequestType::Post,
        10,
        "rd",
        &["ep=lwm2m-e2e", "lt=60", "b=U"],
    );
    registration.payload = b"</3303_1.1/0>,</3304_1.1/0>".to_vec();
    let mut buffer = vec![0_u8; 2048];
    let mut registered = false;
    for _ in 0..6 {
        client_socket
            .send(&registration.to_bytes().unwrap())
            .await
            .unwrap();
        let (size, _) = client_socket.recv_from(&mut buffer).await.unwrap();
        let created = Packet::from_bytes(&buffer[..size]).unwrap();
        if created.header.code == MessageClass::Response(ResponseType::Created) {
            registered = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    assert!(registered, "LwM2M registration failed");

    let mut observations = std::collections::HashMap::new();
    for _ in 0..4 {
        let (size, _) = client_socket.recv_from(&mut buffer).await.unwrap();
        let request = Packet::from_bytes(&buffer[..size]).unwrap();
        let observe = request.get_observe_value().and_then(|value| value.ok());
        let path = request
            .get_option(CoapOption::UriPath)
            .unwrap()
            .iter()
            .map(|part| String::from_utf8(part.clone()).unwrap())
            .collect::<Vec<_>>()
            .join("/");
        if observe.is_some() {
            observations.insert(path.clone(), request.get_token().to_vec());
        }
        let value = if path.starts_with("3303") { 42.0 } else { 60.0 };
        client_socket
            .send(&response(&request, value, observe).to_bytes().unwrap())
            .await
            .unwrap();
    }

    let token = observations["3303/0/5700"].clone();
    let mut observe_request = packet(RequestType::Get, 500, "3303/0/5700", &[]);
    observe_request.set_token(token);
    observe_request.set_observe_value(1);
    observe_request.header.code = MessageClass::Response(ResponseType::Content);
    observe_request.payload = codec::encode(&[Tlv::Resource {
        id: 5700,
        value: &77.0_f32.to_be_bytes(),
    }]);
    observe_request.set_content_format(ContentFormat::ApplicationVndOmaLwm2mTlv);
    client_socket
        .send(&observe_request.to_bytes().unwrap())
        .await
        .unwrap();
    let (size, _) = client_socket.recv_from(&mut buffer).await.unwrap();
    let ack = Packet::from_bytes(&buffer[..size]).unwrap();
    assert_eq!(ack.header.code, MessageClass::Empty);

    let mut found = false;
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
        found = telemetry["temperature"][0]["value"] == json!(77.0)
            && attributes.as_array().is_some_and(|entries| {
                entries
                    .iter()
                    .any(|entry| entry["key"] == "humidity" && entry["value"] == json!(60.0))
            });
        if found {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(found, "LwM2M observed values did not reach core");
}

#[tokio::test]
#[ignore = "requires Java and prepared Leshan dependencies; see tests/leshan/prepare.sh"]
async fn leshan_client_registers_and_observes() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("tb_core=debug,tb_transport_lwm2m=debug,tb_transport_api=debug")
        .try_init();
    let (core, run) = start_core().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let sys = login(&client, &core, "sysadmin@thingsboard.org", "sysadmin").await;
    let tenant: Value = client
        .post(format!("{core}/api/tenant"))
        .header("Authorization", bearer(&sys))
        .json(&json!({"title": "Leshan Interop"}))
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
        .json(&json!({"tenantId": tenant_id, "email": "leshan@tb.io", "authority": "TENANT_ADMIN", "password": "tenant"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let admin = login(&client, &core, "leshan@tb.io", "tenant").await;
    let profile: Value = client
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "name": "leshan-profile",
            "transportType": "LWM2M",
            "profileData": {"observeAttr": {
                "keyName": {"/3303/0/5700": "temperature"},
                "telemetry": ["/3303/0/5700"],
                "observeStrategy": "SINGLE"
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
        .json(&json!({"name": "leshan-device", "deviceProfileId": profile_id, "accessToken": "LESHAN_TOKEN"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let device_id = device["id"].as_str().unwrap();
    let credential_response = client
        .post(format!("{core}/api/device/credentials"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "id": Uuid::now_v7(), "createdTime": 0, "deviceId": device_id,
            "credentialsType": "LWM2M", "credentialsId": "leshan-device",
            "credentialsValue": json!({"endpoint": "leshan-device", "securityConfigClientMode": "NO_SEC"}).to_string()
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(credential_response.status(), reqwest::StatusCode::OK);

    let api = TransportApiClient::new(BROKERS, &format!("leshan-{run}")).unwrap();
    let mut validated = false;
    for _ in 0..6 {
        if api
            .validate_lwm2m("leshan-device")
            .await
            .is_ok_and(|response| response.valid)
        {
            validated = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    assert!(validated, "core rejected Leshan credentials");
    let producer = tb_queue::KafkaQueueProducer::new(BROKERS, "leshan-producer").unwrap();
    let server = Lwm2mServer::new(
        ServerConfig::default(),
        Arc::new(CoreValidator {
            client: api.clone(),
        }),
        Arc::new(TransportApiUplink::new(api)),
        Arc::new(CoreSessionActivity::new(producer, "leshan-e2e")),
    )
    .unwrap();
    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();
    let server_addr = socket.local_addr().unwrap();
    tokio::spawn(async move { server.serve(socket).await.unwrap() });

    let manifest = env!("CARGO_MANIFEST_DIR");
    let output = tokio::process::Command::new("java")
        .args([
            "-cp",
            &format!("{manifest}/tests/leshan/target/classes:{manifest}/tests/leshan/lib/*"),
            "org.thingsboard.LeshanInteropClient",
            &format!("coap://127.0.0.1:{}", server_addr.port()),
            "leshan-device",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "Leshan client failed: {}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!(
        "Leshan stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    println!(
        "Leshan stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut found = false;
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
        found = telemetry["temperature"][0]["value"] == json!(77.0);
        if found {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    assert!(found, "Leshan notification did not reach core");
}
