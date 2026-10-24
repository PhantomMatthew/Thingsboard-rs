mod common;

use std::net::SocketAddr;
use std::time::Duration;

use rcgen::{CertificateParams, KeyPair};
use rumqttc::{
    AsyncClient, ConnectReturnCode, ConnectionError, Event, MqttOptions, Packet, TlsConfiguration,
    Transport,
};
use serde_json::{Value, json};
use uuid::Uuid;

use common::*;

struct TestCertificate {
    pem: String,
    key_pem: String,
    der: Vec<u8>,
}

fn self_signed(name: &str) -> TestCertificate {
    let cert = rcgen::generate_simple_self_signed([name.to_string()]).unwrap();
    TestCertificate {
        pem: cert.cert.pem(),
        key_pem: cert.key_pair.serialize_pem(),
        der: cert.cert.der().to_vec(),
    }
}

fn expired() -> TestCertificate {
    let mut params = CertificateParams::new(["expired-device".into()]).unwrap();
    params.not_before = rcgen::date_time_ymd(2020, 1, 1);
    params.not_after = rcgen::date_time_ymd(2021, 1, 1);
    let key = KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    TestCertificate {
        pem: cert.pem(),
        key_pem: key.serialize_pem(),
        der: cert.der().to_vec(),
    }
}

fn tls_options(
    client_id: &str,
    port: u16,
    server: &TestCertificate,
    client: Option<&TestCertificate>,
) -> MqttOptions {
    let mut options = MqttOptions::new(client_id, "127.0.0.1", port);
    options.set_keep_alive(Duration::from_secs(5));
    options.set_transport(Transport::tls_with_config(TlsConfiguration::Simple {
        ca: server.pem.as_bytes().to_vec(),
        alpn: None,
        client_auth: client.map(|cert| {
            (
                cert.pem.as_bytes().to_vec(),
                cert.key_pem.as_bytes().to_vec(),
            )
        }),
    }));
    options
}

async fn connack(options: MqttOptions) -> Option<ConnectReturnCode> {
    let (_client, mut eventloop) = AsyncClient::new(options, 8);
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => return Some(ack.code),
                Ok(_) => {}
                Err(ConnectionError::ConnectionRefused(code)) => return Some(code),
                Err(_) => return None,
            }
        }
    })
    .await
    .ok()?
}

async fn connect_until_success(options: MqttOptions) {
    let start = std::time::Instant::now();
    loop {
        if connack(options.clone()).await == Some(ConnectReturnCode::Success) {
            return;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "MQTTS connection never succeeded"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn write_tls_files(server: &TestCertificate) -> (String, String) {
    let suffix = Uuid::now_v7().simple();
    let cert = std::env::temp_dir().join(format!("tbrs-mqtts-{suffix}-cert.pem"));
    let key = std::env::temp_dir().join(format!("tbrs-mqtts-{suffix}-key.pem"));
    std::fs::write(&cert, &server.pem).unwrap();
    std::fs::write(&key, &server.key_pem).unwrap();
    (
        cert.to_string_lossy().into_owned(),
        key.to_string_lossy().into_owned(),
    )
}

async fn start_mqtts(server: &TestCertificate) -> SocketAddr {
    let (cert, key) = write_tls_files(server);
    let acceptor = tb_transport_mqtt::tls::acceptor_from_files(&cert, &key).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let node_id = format!("test-mqtts-{}", Uuid::now_v7().simple());
    let state = tb_transport_mqtt::TransportState::new(KAFKA_BROKERS, &node_id).unwrap();
    tokio::spawn(async move {
        tb_transport_mqtt::tls::serve_tls(listener, state, acceptor)
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    addr
}

async fn save_x509_credentials(
    client: &reqwest::Client,
    core: &str,
    token: &str,
    device_id: &str,
    certificate: &TestCertificate,
) -> Value {
    let response = client
        .post(format!("{core}/api/device/credentials"))
        .header("Authorization", bearer(token))
        .json(&json!({
            "id": Uuid::now_v7(),
            "createdTime": 0,
            "deviceId": device_id,
            "credentialsType": "X509_CERTIFICATE",
            "credentialsValue": certificate.pem,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.json().await.unwrap()
}

#[tokio::test]
async fn mqtts_x509_and_token_fallback_end_to_end() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, _) = start_servers().await;
    let (admin, _) = setup_tenant_admin(&http, &core, "MQTTS Tenant", "mqtts@tb.io").await;
    let device_id =
        create_device_with_token(&http, &core, &admin, "mqtts-device", "MQTTS_TOKEN").await;
    create_device_with_token(
        &http,
        &core,
        &admin,
        "mqtts-token-device",
        "MQTTS_TOKEN_FALLBACK",
    )
    .await;
    let server = self_signed("127.0.0.1");
    let registered = self_signed("registered-device");
    let unregistered = self_signed("unregistered-device");
    let mqtts_addr = start_mqtts(&server).await;

    let saved = save_x509_credentials(&http, &core, &admin, &device_id, &registered).await;
    let credential = tb_common::x509::parse_x509_credential(&registered.pem).unwrap();
    assert_eq!(saved["credentialsId"], credential.credentials_id);
    assert_eq!(saved["credentialsValue"], credential.certificate_base64);
    assert_eq!(
        saved["credentialsId"],
        tb_common::x509::x509_cert_hash_from_der(&registered.der).unwrap()
    );

    connect_until_success(tls_options(
        "registered",
        mqtts_addr.port(),
        &server,
        Some(&registered),
    ))
    .await;
    assert_eq!(
        connack(tls_options(
            "unregistered",
            mqtts_addr.port(),
            &server,
            Some(&unregistered)
        ))
        .await,
        Some(ConnectReturnCode::BadUserNamePassword)
    );

    let expired = expired();
    assert_eq!(
        connack(tls_options(
            "expired",
            mqtts_addr.port(),
            &server,
            Some(&expired)
        ))
        .await,
        None
    );

    let mut token_options = tls_options("token-fallback", mqtts_addr.port(), &server, None);
    token_options.set_credentials("MQTTS_TOKEN_FALLBACK", "");
    connect_until_success(token_options).await;
}
