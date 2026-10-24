mod common;

use common::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tb_dao::models::{CredentialsType, DeviceCredentials};
use tb_proto::transport::{
    GetDeviceProfileRequest, GetDevicesByTransportRequest, ValidateLwm2mRequest,
};
use uuid::Uuid;

async fn post_device(client: &Client, base: &str, token: &str, body: Value) -> Value {
    let response = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(token))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    response.json().await.unwrap()
}

#[tokio::test]
async fn shared_device_configuration_and_transport_queries() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (base, _pool, state) = start_server_with_state().await;
    let (admin, tenant_id) =
        setup_tenant_admin(&client, &base, "Transport API", "transport-api@tb.io").await;

    let response = client
        .post(format!("{base}/api/deviceProfile"))
        .header("Authorization", bearer(&admin))
        .json(&json!({
            "name": "LwM2M devices",
            "transportType": "LWM2M",
            "profileData": {"transportConfiguration": {"observeAttr": {"attrs": ["temp"]}}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let profile: Value = response.json().await.unwrap();
    let profile_id = profile["id"].as_str().unwrap();

    let fetched = tb_core::transport_api::get_device_profile(
        &state,
        &GetDeviceProfileRequest {
            device_profile_id: profile_id.into(),
        },
    )
    .await
    .unwrap()
    .expect("profile");
    assert_eq!(fetched.device_profile_id, profile_id);
    assert_eq!(fetched.transport_type, "LWM2M");
    assert_eq!(
        serde_json::from_str::<Value>(&fetched.profile_data_json).unwrap(),
        profile["profileData"]
    );

    let original_data = json!({"transportConfiguration": {"powerMode": "PSM"}});
    let device = post_device(
        &client,
        &base,
        &admin,
        json!({
            "name": "endpoint-device",
            "deviceProfileId": profile_id,
            "deviceData": original_data
        }),
    )
    .await;
    assert_eq!(device["deviceData"], original_data);
    let device_id = device["id"].as_str().unwrap().to_string();

    let updated = post_device(
        &client,
        &base,
        &admin,
        json!({"id": device_id, "name": "endpoint-device-renamed"}),
    )
    .await;
    assert_eq!(updated["deviceData"], original_data);

    let replacement_data = json!({"transportConfiguration": {"powerMode": "DRX"}});
    let updated = post_device(
        &client,
        &base,
        &admin,
        json!({
            "id": device_id,
            "name": "endpoint-device-renamed",
            "deviceData": replacement_data
        }),
    )
    .await;
    assert_eq!(updated["deviceData"], replacement_data);

    let second = post_device(
        &client,
        &base,
        &admin,
        json!({
            "name": "second-lwm2m-device",
            "deviceProfileId": profile_id,
            "deviceData": {"transportConfiguration": {"powerMode": "PSM"}}
        }),
    )
    .await;

    let first_page = tb_core::transport_api::get_devices_by_transport(
        &state,
        &GetDevicesByTransportRequest {
            tenant_id: tenant_id.clone(),
            transport_type: "LWM2M".into(),
            page: 0,
            page_size: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(first_page.devices.len(), 1);
    assert_eq!(first_page.total_elements, 2);
    assert_eq!(first_page.total_pages, 2);
    assert!(first_page.has_next);
    assert_eq!(first_page.devices[0].device_profile_id, profile_id);
    assert!(!first_page.devices[0].device_data_json.is_empty());

    let second_page = tb_core::transport_api::get_devices_by_transport(
        &state,
        &GetDevicesByTransportRequest {
            tenant_id,
            transport_type: "LWM2M".into(),
            page: 1,
            page_size: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(second_page.devices.len(), 1);
    assert!(!second_page.has_next);
    assert_ne!(
        first_page.devices[0].device_id,
        second_page.devices[0].device_id
    );

    let all_tenants = tb_core::transport_api::get_devices_by_transport(
        &state,
        &GetDevicesByTransportRequest {
            tenant_id: String::new(),
            transport_type: "LWM2M".into(),
            page: 0,
            page_size: 10,
        },
    )
    .await
    .unwrap();
    assert_eq!(all_tenants.total_elements, 2);
    assert!(all_tenants.devices.iter().any(|device| {
        device.device_id == second["id"].as_str().unwrap()
            && serde_json::from_str::<Value>(&device.device_data_json).unwrap()
                == second["deviceData"]
    }));

    let endpoint = "urn:imei:123456789012345";
    let device_uuid = Uuid::parse_str(&device_id).unwrap();
    let credentials = DeviceCredentials {
        id: Uuid::now_v7(),
        created_time: 0,
        device_id: device_uuid,
        credentials_type: CredentialsType::Lwm2m,
        credentials_id: Some(endpoint.into()),
        credentials_value: Some(json!({"endpoint": endpoint}).to_string()),
    };
    state
        .device_credentials_dao
        .save(&credentials)
        .await
        .unwrap();

    let valid = tb_core::transport_api::validate_lwm2m(&state, endpoint).await;
    assert!(valid.valid, "NO_SEC endpoint credentials must be valid");
    assert_eq!(valid.device.unwrap().device_id, device_id);
    assert_eq!(valid.profile.unwrap().device_profile_id, profile_id);

    let invalid = tb_core::transport_api::validate_lwm2m(&state, "unknown-endpoint").await;
    assert!(!invalid.valid);

    let mut mismatched = credentials.clone();
    mismatched.credentials_value = Some(json!({"endpoint": "different-endpoint"}).to_string());
    state
        .device_credentials_dao
        .save(&mismatched)
        .await
        .unwrap();
    assert!(
        !tb_core::transport_api::validate_lwm2m(&state, endpoint)
            .await
            .valid
    );

    let wrong_type = DeviceCredentials {
        credentials_type: CredentialsType::AccessToken,
        ..credentials
    };
    state
        .device_credentials_dao
        .save(&wrong_type)
        .await
        .unwrap();
    let request = ValidateLwm2mRequest {
        endpoint: endpoint.into(),
    };
    assert!(
        !tb_core::transport_api::validate_lwm2m(&state, &request.endpoint)
            .await
            .valid
    );
}
