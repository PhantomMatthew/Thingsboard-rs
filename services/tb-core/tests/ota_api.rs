//! Phase 6 OTA 端到端：multipart 上传 → 元数据/分页/download → 设备/档案绑定 →
//! 经 tb-transport-http 分块下载，拼接与原文一致且 sha256 匹配。

mod common;

use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::Digest as _;

use common::*;

/// 上传 OTA 包，返回 ota_package_id。
async fn upload_ota(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    profile_id: &str,
    title: &str,
    version: &str,
    data: Vec<u8>,
) -> String {
    let part = reqwest::multipart::Part::bytes(data).file_name("fw.bin");
    let form = reqwest::multipart::Form::new()
        .text("title", title.to_string())
        .text("version", version.to_string())
        .text("type", "FIRMWARE")
        .text("deviceProfileId", profile_id.to_string())
        .part("file", part);
    let resp = client
        .post(format!("{base}/api/otaPackage"))
        .header("Authorization", bearer(token))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "upload ota package");
    let package: Value = resp.json().await.unwrap();
    package["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn ota_api_e2e() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let (core, transport) = start_servers_with_transport().await;
    let (admin_token, _tenant_id) =
        setup_tenant_admin(&client, &core, "OTA Tenant", "ota-tenant@tb.io").await;

    // 先建设备：确保租户的默认 device profile 已懒创建。
    let device_id = {
        let resp = client
            .post(format!("{core}/api/device"))
            .header("Authorization", bearer(&admin_token))
            .json(&json!({"name": "ota-dev", "accessToken": "OTA_TOKEN"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let device: Value = resp.json().await.unwrap();
        device["id"].as_str().unwrap().to_string()
    };

    let resp = client
        .get(format!("{core}/api/deviceProfile/default"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let profile: Value = resp.json().await.unwrap();
    let profile_id = profile["id"].as_str().unwrap().to_string();

    // 40000 字节固件：3 个 16KB 分块（16384 + 16384 + 7232）
    let fw_data: Vec<u8> = (0..40000u32).map(|i| (i % 253) as u8).collect();
    let expected_checksum = hex::encode(sha2::Sha256::digest(&fw_data));
    let ota_id = upload_ota(
        &client,
        &core,
        &admin_token,
        &profile_id,
        "fw",
        "1.0",
        fw_data.clone(),
    )
    .await;

    // 元数据查询
    let resp = client
        .get(format!("{core}/api/otaPackage/{ota_id}"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let meta: Value = resp.json().await.unwrap();
    assert_eq!(meta["title"], "fw");
    assert_eq!(meta["version"], "1.0");
    assert_eq!(meta["size"], 40000);
    assert_eq!(meta["checksum"], expected_checksum);
    assert!(meta.get("data").is_none() || meta["data"].is_null());

    // 分页 + 过滤
    let resp = client
        .get(format!(
            "{core}/api/otaPackages?pageSize=10&page=0&deviceProfileId={profile_id}&type=FIRMWARE"
        ))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 1);
    let resp = client
        .get(format!(
            "{core}/api/otaPackages?pageSize=10&page=0&type=SOFTWARE"
        ))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 0);

    // download 字节一致
    let resp = client
        .get(format!("{core}/api/otaPackage/{ota_id}/download"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), fw_data.as_slice());

    // 设备级绑定
    let resp = client
        .post(format!("{core}/api/device/{device_id}/firmware/{ota_id}"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{core}/api/device/{device_id}"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let device: Value = resp.json().await.unwrap();
    assert_eq!(device["firmwareId"], ota_id);

    // 档案级绑定
    let resp = client
        .post(format!(
            "{core}/api/deviceProfile/{profile_id}/firmware/{ota_id}"
        ))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // HTTP 分块下载（transport）：chunk=0/1/2 拼接 == 原文
    let mut received = Vec::new();
    for chunk in 0..3 {
        let resp = client
            .get(format!(
                "{transport}/api/v1/OTA_TOKEN/firmware?title=fw&version=0.9&chunk={chunk}&size=16384"
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "chunk {chunk}");
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/octet-stream"
        );
        received.extend_from_slice(&resp.bytes().await.unwrap());
    }
    assert_eq!(received, fw_data);
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&received)),
        expected_checksum
    );

    // 版本已是最新 → 404
    let resp = client
        .get(format!(
            "{transport}/api/v1/OTA_TOKEN/firmware?title=fw&version=1.0&chunk=0&size=16384"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 删除
    let resp = client
        .delete(format!("{core}/api/otaPackage/{ota_id}"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{core}/api/otaPackage/{ota_id}"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
