mod common;

use std::net::SocketAddr;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, KeyIdMethod, KeyPair, KeyUsagePurpose, RevocationReason,
    RevokedCertParams, SerialNumber,
};
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

/// 返回 (CA PEM, 客户端链, 吊销该叶子证书的 CRL PEM)。
fn ca_signed_chain(common_name: &str) -> (String, TestCertificate, String) {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "MQTT chain root");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let mut leaf_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    leaf_params.distinguished_name = DistinguishedName::new();
    leaf_params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    leaf_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    leaf_params.serial_number = Some(SerialNumber::from(0x5001u64));
    let leaf_key = KeyPair::generate().unwrap();
    let leaf = leaf_params.signed_by(&leaf_key, &ca, &ca_key).unwrap();
    let crl = CertificateRevocationListParams {
        this_update: rcgen::date_time_ymd(2024, 1, 1),
        next_update: rcgen::date_time_ymd(2099, 1, 1),
        crl_number: SerialNumber::from(1u64),
        issuing_distribution_point: None,
        revoked_certs: vec![RevokedCertParams {
            serial_number: SerialNumber::from(0x5001u64),
            revocation_time: rcgen::date_time_ymd(2024, 1, 1),
            reason_code: Some(RevocationReason::KeyCompromise),
            invalidity_date: None,
        }],
        key_identifier_method: KeyIdMethod::Sha256,
    }
    .signed_by(&ca, &ca_key)
    .unwrap();
    (
        ca.pem(),
        TestCertificate {
            pem: format!("{}{}", leaf.pem(), ca.pem()),
            key_pem: leaf_key.serialize_pem(),
            der: leaf.der().to_vec(),
        },
        crl.pem().unwrap(),
    )
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

#[tokio::test]
async fn mqtts_x509_chain_auto_provisions_with_full_tls_chain() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, _) = start_servers().await;
    let (admin, _) = setup_tenant_admin(&http, &core, "MQTTS Chain", "mqtts-chain@tb.io").await;
    let (ca_pem, client, revoking_crl) = ca_signed_chain("mqtt-chain-device");
    let mut profile = json!({
        "name": "MQTT X509 chain",
        "provisionType": "X509_CERTIFICATE_CHAIN",
        "profileData": {
            "provisionConfiguration": {
                "certificate": ca_pem,
                "certificateRegExPattern": "^(.*)$",
                "allowCreateNewDevicesByX509Certificate": true
            }
        }
    });
    let response = http
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin))
        .json(&profile)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    profile["id"] = response.json::<Value>().await.unwrap()["id"].clone();

    let server = self_signed("127.0.0.1");
    let mqtts_addr = start_mqtts(&server).await;
    connect_until_success(tls_options(
        "chain-provisioned",
        mqtts_addr.port(),
        &server,
        Some(&client),
    ))
    .await;

    let devices = http
        .get(format!("{core}/api/devices?pageSize=100&page=0"))
        .header("Authorization", bearer(&admin))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let device = devices["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|device| device["name"] == "mqtt-chain-device")
        .unwrap();
    let credentials = http
        .get(format!(
            "{core}/api/device/{}/credentials",
            device["id"].as_str().unwrap()
        ))
        .header("Authorization", bearer(&admin))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(
        credentials["credentialsValue"],
        tb_common::x509::x509_credential_from_der(&client.der)
            .unwrap()
            .certificate_base64
    );

    connect_until_success(tls_options(
        "chain-reconnect",
        mqtts_addr.port(),
        &server,
        Some(&client),
    ))
    .await;
    profile["profileData"]["provisionConfiguration"]["certificateRevocationList"] =
        Value::String(revoking_crl);
    let response = http
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin))
        .json(&profile)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        connack(tls_options(
            "chain-revoked",
            mqtts_addr.port(),
            &server,
            Some(&client)
        ))
        .await,
        Some(ConnectReturnCode::BadUserNamePassword),
        "revoked chain is rejected for an already provisioned device"
    );
}
