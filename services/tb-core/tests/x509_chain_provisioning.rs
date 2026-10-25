mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use prost::Message as _;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateRevocationListParams,
    DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, KeyIdMethod, KeyPair,
    KeyUsagePurpose, RevocationReason, RevokedCertParams, SerialNumber,
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tb_core::device_session::SessionEntry;
use tb_proto::core::{ToTransportMsg, to_transport_msg};
use tb_proto::transport::ValidateOrCreateX509Request;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer, StreamQueueConsumer, TbTopic};
use uuid::Uuid;

struct TestCa {
    certificate: Certificate,
    key: KeyPair,
}

fn ca(common_name: &str) -> TestCa {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate().unwrap();
    let certificate = params.self_signed(&key).unwrap();
    TestCa { certificate, key }
}

fn leaf(ca: &TestCa, common_name: &str, expired: bool) -> Vec<u8> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    if expired {
        params.not_before = rcgen::date_time_ymd(2020, 1, 1);
        params.not_after = rcgen::date_time_ymd(2021, 1, 1);
    }
    let key = KeyPair::generate().unwrap();
    params
        .signed_by(&key, &ca.certificate, &ca.key)
        .unwrap()
        .der()
        .to_vec()
}

fn chain(ca: &TestCa, common_name: &str) -> Vec<Vec<u8>> {
    vec![leaf(ca, common_name, false), ca.certificate.der().to_vec()]
}

fn intermediate(root: &TestCa, common_name: &str, serial: u64) -> TestCa {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params.serial_number = Some(SerialNumber::from(serial));
    let key = KeyPair::generate().unwrap();
    let certificate = params
        .signed_by(&key, &root.certificate, &root.key)
        .unwrap();
    TestCa { certificate, key }
}

fn serial_leaf(issuer: &TestCa, common_name: &str, serial: u64) -> Vec<u8> {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.serial_number = Some(SerialNumber::from(serial));
    let key = KeyPair::generate().unwrap();
    params
        .signed_by(&key, &issuer.certificate, &issuer.key)
        .unwrap()
        .der()
        .to_vec()
}

fn crl_pem(issuer: &TestCa, revoked_serials: &[u64]) -> String {
    CertificateRevocationListParams {
        this_update: rcgen::date_time_ymd(2024, 1, 1),
        next_update: rcgen::date_time_ymd(2099, 1, 1),
        crl_number: SerialNumber::from(1u64),
        issuing_distribution_point: None,
        revoked_certs: revoked_serials
            .iter()
            .map(|serial| RevokedCertParams {
                serial_number: SerialNumber::from(*serial),
                revocation_time: rcgen::date_time_ymd(2024, 1, 1),
                reason_code: Some(RevocationReason::KeyCompromise),
                invalidity_date: None,
            })
            .collect(),
        key_identifier_method: KeyIdMethod::Sha256,
    }
    .signed_by(&issuer.certificate, &issuer.key)
    .unwrap()
    .pem()
    .unwrap()
}

fn request(chain: Vec<Vec<u8>>) -> ValidateOrCreateX509Request {
    ValidateOrCreateX509Request {
        certificate_chain: chain,
    }
}

fn profile_body(id: Option<Uuid>, name: &str, ca: &TestCa, pattern: &str, allow: bool) -> Value {
    json!({
        "id": id,
        "name": name,
        "provisionType": "X509_CERTIFICATE_CHAIN",
        "profileData": {
            "provisionConfiguration": {
                "certificate": ca.certificate.pem(),
                "certificateRegExPattern": pattern,
                "allowCreateNewDevicesByX509Certificate": allow
            }
        }
    })
}

async fn save_profile(client: &reqwest::Client, core: &str, token: &str, body: Value) -> Value {
    let response = client
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(token))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "save X509 profile");
    response.json().await.unwrap()
}

#[tokio::test]
async fn x509_chain_provisioning_security_rotation_and_concurrency() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, _pool, state) = start_server_with_state().await;
    let (admin, _) = setup_tenant_admin(&http, &core, "X509 Chain", "x509-chain@tb.io").await;

    let disabled_ca = ca("disabled-root");
    let disabled_leaf = chain(&disabled_ca, "disabled-device");
    let disabled_body = profile_body(None, "disabled", &disabled_ca, "(.*)", false);
    let disabled_profile = save_profile(&http, &core, &admin, disabled_body.clone()).await;
    let expected_key =
        tb_common::x509::x509_cert_hash_from_der(disabled_ca.certificate.der()).unwrap();
    assert_eq!(
        disabled_profile["profileData"]["provisionConfiguration"]["provisionDeviceKey"],
        expected_key
    );
    let response =
        tb_core::transport_api::validate_or_create_x509(&state, &request(disabled_leaf.clone()))
            .await;
    assert!(!response.valid, "allow=false must not create a device");

    let disabled_id = Uuid::parse_str(disabled_profile["id"].as_str().unwrap()).unwrap();
    let enabled_body = profile_body(Some(disabled_id), "disabled", &disabled_ca, "(.*)", true);
    save_profile(&http, &core, &admin, enabled_body).await;
    let expected_leaf = tb_common::x509::x509_credential_from_der(&disabled_leaf[0]).unwrap();
    let response =
        tb_core::transport_api::validate_or_create_x509(&state, &request(disabled_leaf)).await;
    assert!(response.valid, "allow=true creates device and credentials");
    assert_eq!(response.credentials_body, expected_leaf.certificate_base64);

    let main_ca = ca("main-root");
    let main_profile = save_profile(
        &http,
        &core,
        &admin,
        profile_body(None, "main", &main_ca, "^device-(.*)$", true),
    )
    .await;
    let main_profile_id = main_profile["id"].as_str().unwrap();
    let first_chain = chain(&main_ca, "device-rotation");
    let first =
        tb_core::transport_api::validate_or_create_x509(&state, &request(first_chain.clone()))
            .await;
    assert!(first.valid);
    assert_eq!(first.device.as_ref().unwrap().device_name, "rotation");
    let first_device_id = first.device.as_ref().unwrap().device_id.clone();
    let second_chain = chain(&main_ca, "device-rotation");
    let second =
        tb_core::transport_api::validate_or_create_x509(&state, &request(second_chain.clone()))
            .await;
    assert!(second.valid, "same-profile X509 rotation is allowed");
    assert_eq!(second.device.unwrap().device_id, first_device_id);
    assert_eq!(
        second.credentials_body,
        tb_common::x509::x509_credential_from_der(&second_chain[0])
            .unwrap()
            .certificate_base64
    );

    let other_ca = ca("other-root");
    save_profile(
        &http,
        &core,
        &admin,
        profile_body(None, "other", &other_ca, "^device-(.*)$", true),
    )
    .await;
    let cross_profile = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(chain(&other_ca, "device-rotation")),
    )
    .await;
    assert!(!cross_profile.valid, "cross-profile rotation is rejected");

    let wrong_ca = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(vec![
            leaf(&main_ca, "device-wrong-ca", false),
            other_ca.certificate.der().to_vec(),
        ]),
    )
    .await;
    assert!(!wrong_ca.valid);
    let missing_tail = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(vec![leaf(&main_ca, "device-missing-tail", false)]),
    )
    .await;
    assert!(!missing_tail.valid);
    let expired = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(vec![
            leaf(&main_ca, "device-expired", true),
            main_ca.certificate.der().to_vec(),
        ]),
    )
    .await;
    assert!(!expired.valid);
    let mut tampered_leaf = leaf(&main_ca, "device-tampered", false);
    let last = tampered_leaf.len() - 1;
    tampered_leaf[last] ^= 1;
    let tampered = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(vec![tampered_leaf, main_ca.certificate.der().to_vec()]),
    )
    .await;
    assert!(!tampered.valid);
    let bad_cn = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(chain(&main_ca, "wrong-prefix")),
    )
    .await;
    assert!(!bad_cn.valid);

    let token_device = http
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "name": "other-credentials",
            "deviceProfileId": main_profile_id,
            "accessToken": "OTHER_CREDENTIALS_TOKEN"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(token_device.status(), StatusCode::OK);
    let other_credentials = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(chain(&main_ca, "device-other-credentials")),
    )
    .await;
    assert!(!other_credentials.valid);

    let duplicate_ca = ca("duplicate-root");
    for name in ["duplicate-a", "duplicate-b"] {
        save_profile(
            &http,
            &core,
            &admin,
            profile_body(None, name, &duplicate_ca, "(.*)", true),
        )
        .await;
    }
    let duplicate = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(chain(&duplicate_ca, "duplicate-device")),
    )
    .await;
    assert!(!duplicate.valid, "ambiguous chain profiles are rejected");

    let concurrent_ca = ca("concurrent-root");
    save_profile(
        &http,
        &core,
        &admin,
        profile_body(None, "concurrent", &concurrent_ca, "(.*)", true),
    )
    .await;
    let concurrent_chain = chain(&concurrent_ca, "first-connection");
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let state = state.clone();
        let request = request(concurrent_chain.clone());
        tasks.push(tokio::spawn(async move {
            tb_core::transport_api::validate_or_create_x509(&state, &request).await
        }));
    }
    let mut device_ids = std::collections::HashSet::new();
    for task in tasks {
        let response = task.await.unwrap();
        assert!(
            response.valid,
            "concurrent first connection must recheck conflicts"
        );
        device_ids.insert(response.device.unwrap().device_id);
    }
    assert_eq!(device_ids.len(), 1);

    let pinned_ca = ca("pinned-root");
    save_profile(
        &http,
        &core,
        &admin,
        profile_body(None, "pinned", &pinned_ca, "(.*)", true),
    )
    .await;
    let pinned_chain = chain(&pinned_ca, "pinned-device");
    let pinned_device = http
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(&admin))
        .json(&json!({"name": "pinned-device"}))
        .send()
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let pinned_credential = tb_common::x509::x509_credential_from_der(&pinned_chain[0]).unwrap();
    let saved = http
        .post(format!("{core}/api/device/credentials"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "id": Uuid::now_v7(),
            "createdTime": 0,
            "deviceId": pinned_device["id"],
            "credentialsType": "X509_CERTIFICATE",
            "credentialsValue": pinned_credential.certificate_base64
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::OK);
    let pinned = tb_core::transport_api::validate_or_create_x509(
        &state,
        &request(vec![
            pinned_chain[0].clone(),
            other_ca.certificate.der().to_vec(),
        ]),
    )
    .await;
    assert!(pinned.valid, "pinned leaf authentication takes priority");
    assert_eq!(
        pinned.credentials_body,
        pinned_credential.certificate_base64
    );
    assert!(
        tb_core::transport_api::validate_x509(&state, &pinned_credential.credentials_id)
            .await
            .valid,
        "devices outside chain profiles keep hash-only authentication"
    );
}

#[tokio::test]
async fn x509_chain_crl_revokes_existing_devices_and_closes_sessions() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let http = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, _pool, state) = start_server_with_state().await;
    let (admin, _) = setup_tenant_admin(&http, &core, "X509 CRL", "x509-crl@tb.io").await;

    let root = ca("crl-root");
    let revoked_issuer = intermediate(&root, "crl-revoked-intermediate", 0x2001);
    let kept_issuer = intermediate(&root, "crl-kept-intermediate", 0x2002);
    let root_der = root.certificate.der().to_vec();
    let revoked_chain = vec![
        serial_leaf(&root, "revoked-device", 0x1001),
        root_der.clone(),
    ];
    let kept_chain = vec![serial_leaf(&root, "kept-device", 0x1002), root_der.clone()];
    let revoked_issuer_chain = vec![
        serial_leaf(&revoked_issuer, "revoked-issuer-device", 0x1003),
        revoked_issuer.certificate.der().to_vec(),
        root_der.clone(),
    ];
    let kept_issuer_chain = vec![
        serial_leaf(&kept_issuer, "kept-issuer-device", 0x1004),
        kept_issuer.certificate.der().to_vec(),
        root_der.clone(),
    ];

    let profile = save_profile(
        &http,
        &core,
        &admin,
        profile_body(None, "crl", &root, "(.*)", true),
    )
    .await;
    let profile_id = Uuid::parse_str(profile["id"].as_str().unwrap()).unwrap();
    let mut revoked_device_id = None;
    for chain in [
        &revoked_chain,
        &kept_chain,
        &revoked_issuer_chain,
        &kept_issuer_chain,
    ] {
        let response =
            tb_core::transport_api::validate_or_create_x509(&state, &request(chain.clone())).await;
        assert!(
            response.valid,
            "chain is valid before the CRL is configured"
        );
        revoked_device_id.get_or_insert(response.device.unwrap().device_id);
    }
    let revoked_device_id = Uuid::parse_str(&revoked_device_id.unwrap()).unwrap();
    let revoked_credential = tb_common::x509::x509_credential_from_der(&revoked_chain[0]).unwrap();
    assert!(
        !tb_core::transport_api::validate_x509(&state, &revoked_credential.credentials_id)
            .await
            .valid,
        "chain profile devices must present the full chain"
    );

    let node_id = format!("x509-crl-{}", Uuid::now_v7().simple());
    let session_id = Uuid::now_v7().to_string();
    state
        .device_sessions
        .open_session(
            revoked_device_id,
            SessionEntry {
                session_id: session_id.clone(),
                node_id: node_id.clone(),
                is_gateway: false,
                via_gateway: false,
                sub_attributes: false,
                sub_rpc: false,
                last_seen_ms: 0,
            },
        )
        .await
        .unwrap();
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let topic = TbTopic::transport_notifications(&node_id);
    KafkaQueueProducer::new(&brokers, "x509-crl-test")
        .unwrap()
        .send(&topic, "warmup", Bytes::new())
        .await
        .unwrap();
    let mut notifications = StreamQueueConsumer::new_from_beginning(
        &brokers,
        &format!("x509-crl-test-{}", Uuid::now_v7().simple()),
        &[topic.as_str()],
    )
    .unwrap();

    let mut body = profile_body(Some(profile_id), "crl", &root, "(.*)", true);
    body["profileData"]["provisionConfiguration"]["certificateRevocationList"] =
        Value::String(crl_pem(&root, &[0x1001, 0x2001]));
    save_profile(&http, &core, &admin, body.clone()).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let msg = tokio::time::timeout_at(deadline, notifications.recv())
            .await
            .expect("session close notification after CRL update")
            .unwrap();
        if msg.key.as_deref() != Some(session_id.as_bytes()) {
            continue;
        }
        let notification = ToTransportMsg::decode(&msg.payload[..]).unwrap();
        assert!(matches!(
            notification.msg,
            Some(to_transport_msg::Msg::SessionCloseNotification(_))
        ));
        break;
    }

    let validate = |chain: &Vec<Vec<u8>>| {
        let state = state.clone();
        let request = request(chain.clone());
        async move {
            tb_core::transport_api::validate_or_create_x509(&state, &request)
                .await
                .valid
        }
    };
    assert!(!validate(&revoked_chain).await, "revoked leaf is rejected");
    assert!(
        !validate(&revoked_issuer_chain).await,
        "leaf under a revoked intermediate is rejected"
    );
    assert!(
        !validate(&vec![
            serial_leaf(&revoked_issuer, "new-under-revoked", 0x1005),
            revoked_issuer.certificate.der().to_vec(),
            root_der.clone(),
        ])
        .await,
        "revoked intermediate cannot provision new devices"
    );
    assert!(validate(&kept_chain).await, "non-revoked leaf stays valid");
    assert!(
        validate(&kept_issuer_chain).await,
        "issuers without a CRL entry stay valid"
    );

    body["profileData"]["provisionConfiguration"]["certificateRevocationList"] =
        Value::String("-----BEGIN X509 CRL-----\nAAAA\n-----END X509 CRL-----\n".into());
    let invalid = http
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    assert!(
        !validate(&revoked_chain).await,
        "rejected save keeps the CRL"
    );

    body["profileData"]["provisionConfiguration"]
        .as_object_mut()
        .unwrap()
        .remove("certificateRevocationList");
    save_profile(&http, &core, &admin, body).await;
    assert!(
        validate(&revoked_chain).await,
        "removing the CRL restores access"
    );
}
