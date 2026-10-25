//! Phase 6 HTTP provisioning：POST /api/v1/provision（无 token）→ 凭证遥测上行。

mod common;

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};

use common::*;

#[tokio::test]
async fn http_provision_e2e() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, transport) = start_servers().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "Prov Tenant", "prov-http@tb.io").await;

    let resp = client
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({
            "name": "prov-profile-http",
            "provisionType": "ALLOW_CREATE_NEW_DEVICES",
            "profileData": {
                "provisionConfiguration": {
                    "provisionDeviceKey": "HTTP_KEY",
                    "provisionDeviceSecret": "HTTP_SECRET",
                }
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 错误 secret → FAILURE
    let resp = client
        .post(format!("{transport}/api/v1/provision"))
        .json(&json!({
            "deviceName": "http-prov-dev",
            "provisionDeviceKey": "HTTP_KEY",
            "provisionDeviceSecret": "WRONG",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "FAILURE");

    // 正确 → SUCCESS + token
    let resp = client
        .post(format!("{transport}/api/v1/provision"))
        .json(&json!({
            "deviceName": "http-prov-dev",
            "provisionDeviceKey": "HTTP_KEY",
            "provisionDeviceSecret": "HTTP_SECRET",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "SUCCESS", "provision failed: {body}");
    assert_eq!(body["credentialsType"], "ACCESS_TOKEN");
    let token = body["credentialsValue"].as_str().unwrap().to_string();

    // 凭证遥测上行成功
    let resp = client
        .post(format!("{transport}/api/v1/{token}/telemetry"))
        .json(&json!({"via": "http-provision"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    poll_until(Duration::from_secs(30), async || {
        let resp = client
            .get(format!(
                "{core}/api/devices?pageSize=50&page=0&textSearch=http-prov-dev"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        let page: Value = resp.json().await.unwrap();
        let Some(id) = page["data"]
            .as_array()
            .and_then(|a| a.iter().find(|d| d["name"] == "http-prov-dev"))
            .and_then(|d| d["id"].as_str())
        else {
            return false;
        };
        let resp = client
            .get(format!(
                "{core}/api/telemetry/DEVICE/{id}/values/timeseries"
            ))
            .header("Authorization", bearer(&admin_token))
            .send()
            .await
            .unwrap();
        let latest: Value = resp.json().await.unwrap_or(Value::Null);
        latest["via"]
            .as_array()
            .is_some_and(|a| a.iter().any(|e| e["value"] == "http-provision"))
    })
    .await;

    for _ in 0..20 {
        let resp = client
            .post(format!("{transport}/api/v1/provision"))
            .json(&json!({
                "deviceName": "rate-limited-device",
                "provisionDeviceKey": "HTTP_KEY",
                "provisionDeviceSecret": "WRONG",
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["status"], "FAILURE");
    }
    let resp = client
        .post(format!("{transport}/api/v1/provision"))
        .json(&json!({
            "deviceName": "rate-limited-device",
            "provisionDeviceKey": "HTTP_KEY",
            "provisionDeviceSecret": "HTTP_SECRET",
        }))
        .send()
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "FAILURE");
    assert_eq!(body["errorMsg"], "too many failed provision attempts");
}
