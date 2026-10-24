mod common;

use common::*;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tb_proto::transport::ValidateOrCreateX509Request;
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
}
