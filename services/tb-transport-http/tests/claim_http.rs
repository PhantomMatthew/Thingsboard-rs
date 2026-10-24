mod common;

use reqwest::StatusCode;
use serde_json::json;

use common::*;

#[tokio::test]
async fn claim_accepts_empty_and_json_bodies() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, transport) = start_servers().await;
    let (admin_token, _) =
        setup_tenant_admin(&client, &core, "Claim HTTP", "claim-http@test.io").await;
    create_device_with_token(
        &client,
        &core,
        &admin_token,
        "claim-http-device",
        "CLAIM_HTTP_TOKEN",
    )
    .await;

    let response = client
        .post(format!("{transport}/api/v1/CLAIM_HTTP_TOKEN/claim"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .post(format!("{transport}/api/v1/CLAIM_HTTP_TOKEN/claim"))
        .json(&json!({"secretKey": "secret", "durationMs": 60_000}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .post(format!("{transport}/api/v1/CLAIM_HTTP_TOKEN/claim"))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .post(format!("{transport}/api/v1/INVALID_CLAIM_TOKEN/claim"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
