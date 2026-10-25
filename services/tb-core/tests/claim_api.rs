mod common;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tb_common::{AttributeScope, KvEntry, KvValue};
use tb_core::claim::{CLAIMING_ALLOWED_ATTRIBUTE, CLAIMING_DATA_ATTRIBUTE, ClaimResult};
use tb_proto::core::{ClaimDeviceMsg, SessionInfoProto};
use uuid::Uuid;

use common::{INTEGRATION_LOCK, bearer, login, setup_tenant_admin, start_server_with_state};

async fn create_customer_user(
    client: &Client,
    base: &str,
    admin_token: &str,
    title: &str,
    email: &str,
) -> (String, String) {
    let response = client
        .post(format!("{base}/api/customer"))
        .header("Authorization", bearer(admin_token))
        .json(&json!({"title": title}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let customer: Value = response.json().await.unwrap();
    let customer_id = customer["id"].as_str().unwrap().to_string();
    let response = client
        .post(format!("{base}/api/user"))
        .header("Authorization", bearer(admin_token))
        .json(&json!({
            "customerId": customer_id,
            "email": email,
            "authority": "CUSTOMER_USER",
            "password": "customer",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let tokens = login(client, base, email, "customer").await;
    (customer_id, tokens["token"].as_str().unwrap().to_string())
}

async fn create_device(client: &Client, base: &str, admin_token: &str, name: &str) -> Value {
    let response = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(admin_token))
        .json(&json!({
            "name": name,
            "additionalInfo": {"preserved": true, "nested": {"value": 7}},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

fn claim_message(
    tenant_id: &str,
    device_id: &str,
    secret_key: &str,
    duration_ms: i64,
) -> ClaimDeviceMsg {
    ClaimDeviceMsg {
        session: Some(SessionInfoProto {
            tenant_id: tenant_id.into(),
            device_id: device_id.into(),
            ..Default::default()
        }),
        secret_key: secret_key.into(),
        duration_ms,
    }
}

async fn customer_claim(
    client: &Client,
    base: &str,
    token: &str,
    device_name: &str,
    secret_key: &str,
) -> reqwest::Response {
    client
        .post(format!("{base}/api/customer/device/{device_name}/claim"))
        .header("Authorization", bearer(token))
        .json(&json!({"secretKey": secret_key}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn claim_service_and_customer_api() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (base, _pool, state) = start_server_with_state().await;
    let (admin_token, tenant_id) =
        setup_tenant_admin(&client, &base, "Claim Tenant", "claim-admin@test.io").await;
    let (customer_a, customer_a_token) = create_customer_user(
        &client,
        &base,
        &admin_token,
        "Claim Customer A",
        "claim-a@test.io",
    )
    .await;
    let (customer_b, customer_b_token) = create_customer_user(
        &client,
        &base,
        &admin_token,
        "Claim Customer B",
        "claim-b@test.io",
    )
    .await;

    let previous_claim_enabled = std::env::var_os("TB_CLAIM_ENABLED");
    unsafe { std::env::set_var("TB_CLAIM_ENABLED", "false") };
    let disabled = create_device(&client, &base, &admin_token, "claim-disabled").await;
    let disabled_id = Uuid::parse_str(disabled["id"].as_str().unwrap()).unwrap();
    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, &disabled_id.to_string(), "secret", 60_000),
    )
    .await
    .unwrap();
    let response = customer_claim(
        &client,
        &base,
        &customer_a_token,
        "claim-disabled",
        "secret",
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    state
        .attributes_dao
        .save(
            disabled_id,
            AttributeScope::ServerScope,
            &[KvEntry {
                key: CLAIMING_ALLOWED_ATTRIBUTE.into(),
                value: KvValue::Bool(true),
            }],
        )
        .await
        .unwrap();
    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, &disabled_id.to_string(), "secret", 60_000),
    )
    .await
    .unwrap();
    match previous_claim_enabled {
        Some(value) => unsafe { std::env::set_var("TB_CLAIM_ENABLED", value) },
        None => unsafe { std::env::remove_var("TB_CLAIM_ENABLED") },
    }
    let response = customer_claim(
        &client,
        &base,
        &customer_a_token,
        "claim-disabled",
        "secret",
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let device = create_device(&client, &base, &admin_token, "claim-success").await;
    let device_id = Uuid::parse_str(device["id"].as_str().unwrap()).unwrap();
    state
        .attributes_dao
        .save(
            device_id,
            AttributeScope::ServerScope,
            &[
                KvEntry {
                    key: CLAIMING_ALLOWED_ATTRIBUTE.into(),
                    value: KvValue::Bool(true),
                },
                KvEntry {
                    key: CLAIMING_DATA_ATTRIBUTE.into(),
                    value: KvValue::Json(json!({
                        "secretKey": "stale",
                        "expirationTime": chrono::Utc::now().timestamp_millis() + 60_000,
                    })),
                },
                KvEntry {
                    key: "unrelated".into(),
                    value: KvValue::Str("keep".into()),
                },
            ],
        )
        .await
        .unwrap();

    let response = customer_claim(&client, &base, &admin_token, "claim-success", "secret").await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let response = client
        .post(format!("{base}/api/customer/device/claim-success/claim"))
        .json(&json!({"secretKey": "secret"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, &device_id.to_string(), "secret", 0),
    )
    .await
    .unwrap();
    let response =
        customer_claim(&client, &base, &customer_a_token, "claim-success", "wrong").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "FAILURE");
    assert!(
        state
            .device_dao
            .find_by_id(device_id)
            .await
            .unwrap()
            .unwrap()
            .customer_id
            .is_none()
    );
    let preserved = state
        .attributes_dao
        .find(device_id, AttributeScope::ServerScope, None)
        .await
        .unwrap();
    assert!(
        preserved
            .iter()
            .any(|entry| entry.key == CLAIMING_ALLOWED_ATTRIBUTE)
    );
    assert!(
        preserved
            .iter()
            .any(|entry| entry.key == CLAIMING_DATA_ATTRIBUTE)
    );

    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, &device_id.to_string(), "", 0),
    )
    .await
    .unwrap();
    let response = customer_claim(&client, &base, &customer_a_token, "claim-success", "").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "FAILURE");
    let response =
        customer_claim(&client, &base, &customer_a_token, "claim-success", "secret").await;
    assert_eq!(response.status(), StatusCode::OK);
    let claimed: Value = response.json().await.unwrap();
    assert_eq!(claimed["response"], "SUCCESS");
    assert_eq!(claimed["device"]["customerId"], json!(customer_a));
    assert_eq!(
        claimed["device"]["additionalInfo"],
        json!({"preserved": true, "nested": {"value": 7}})
    );
    let attributes = state
        .attributes_dao
        .find(device_id, AttributeScope::ServerScope, None)
        .await
        .unwrap();
    assert!(attributes.iter().all(|entry| entry.key == "unrelated"));

    let response = customer_claim(&client, &base, &customer_b_token, "claim-success", "").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "CLAIMED");

    let update = create_device(&client, &base, &admin_token, "claim-update").await;
    let update_id = update["id"].as_str().unwrap();
    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, update_id, "first", i64::MAX),
    )
    .await
    .unwrap();
    let mut redis = state.redis_manager().await.unwrap();
    let ttl: i64 = redis::cmd("PTTL")
        .arg(format!("claim:{tenant_id}:{update_id}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(
        ttl > 0 && ttl <= 86_400_000,
        "claim TTL was not capped: {ttl}"
    );
    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, update_id, "second", 60_000),
    )
    .await
    .unwrap();
    let response = customer_claim(&client, &base, &customer_a_token, "claim-update", "first").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "FAILURE");
    let response =
        customer_claim(&client, &base, &customer_a_token, "claim-update", "second").await;
    assert_eq!(response.status(), StatusCode::OK);

    let expired = create_device(&client, &base, &admin_token, "claim-expired").await;
    let expired_id = expired["id"].as_str().unwrap();
    tb_core::claim::register(&state, &claim_message(&tenant_id, expired_id, "secret", 1))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let response =
        customer_claim(&client, &base, &customer_a_token, "claim-expired", "secret").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.unwrap(), "FAILURE");

    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, expired_id, "secret", 60_000),
    )
    .await
    .unwrap();
    let response =
        customer_claim(&client, &base, &customer_a_token, "claim-expired", "secret").await;
    assert_eq!(response.status(), StatusCode::OK);

    let concurrent = create_device(&client, &base, &admin_token, "claim-concurrent").await;
    let concurrent_id = Uuid::parse_str(concurrent["id"].as_str().unwrap()).unwrap();
    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, &concurrent_id.to_string(), "race", 60_000),
    )
    .await
    .unwrap();
    let concurrent = state
        .device_dao
        .find_by_id(concurrent_id)
        .await
        .unwrap()
        .unwrap();
    let customer_a_id = Uuid::parse_str(&customer_a).unwrap();
    let customer_b_id = Uuid::parse_str(&customer_b).unwrap();
    let (result_a, result_b) = tokio::join!(
        tb_core::claim::claim(&state, &concurrent, customer_a_id, "race"),
        tb_core::claim::claim(&state, &concurrent, customer_b_id, "race"),
    );
    let successes = [result_a.unwrap(), result_b.unwrap()]
        .into_iter()
        .filter(|result| matches!(result, ClaimResult::Success(_)))
        .count();
    assert_eq!(successes, 1);
    let owner = state
        .device_dao
        .find_by_id(concurrent_id)
        .await
        .unwrap()
        .unwrap()
        .customer_id;
    assert!(owner == Some(customer_a_id) || owner == Some(customer_b_id));

    tb_core::claim::register(
        &state,
        &claim_message(&tenant_id, &concurrent_id.to_string(), "ignored", 60_000),
    )
    .await
    .unwrap();
    let assigned = state
        .device_dao
        .find_by_id(concurrent_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(assigned.customer_id, owner);
}
