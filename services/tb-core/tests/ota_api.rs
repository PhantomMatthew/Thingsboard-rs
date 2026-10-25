//! OTA 端到端：multipart 上传 → 元数据/分页/download → 显式绑定选包（设备级优先于档案级，
//! 新上传不改变既有绑定）→ 绑定校验 → 状态回写（target_* / fw_state / fw_* 属性）→
//! 经 tb-transport-http 按目标 title/version 分块下载固件与软件 → 解绑回落与删除保护。

mod common;

use std::time::Duration;

use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::Digest as _;

use common::*;

/// 上传 OTA 包，返回 ota_package_id。
#[allow(clippy::too_many_arguments)]
async fn upload_ota(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    profile_id: &str,
    ota_type: &str,
    title: &str,
    version: &str,
    data: Vec<u8>,
) -> String {
    let part = reqwest::multipart::Part::bytes(data).file_name("fw.bin");
    let form = reqwest::multipart::Form::new()
        .text("title", title.to_string())
        .text("version", version.to_string())
        .text("type", ota_type.to_string())
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

async fn post(client: &reqwest::Client, url: String, token: &str) -> StatusCode {
    client
        .post(url)
        .header("Authorization", bearer(token))
        .send()
        .await
        .unwrap()
        .status()
}

async fn delete(client: &reqwest::Client, url: String, token: &str) -> StatusCode {
    client
        .delete(url)
        .header("Authorization", bearer(token))
        .send()
        .await
        .unwrap()
        .status()
}

/// 设备 HTTP OTA 下载：kind = firmware | software。
async fn device_chunk(
    client: &reqwest::Client,
    transport: &str,
    device_token: &str,
    kind: &str,
    title: &str,
    version: &str,
    chunk: usize,
) -> reqwest::Response {
    client
        .get(format!(
            "{transport}/api/v1/{device_token}/{kind}?title={title}&version={version}&chunk={chunk}&size=16384"
        ))
        .send()
        .await
        .unwrap()
}

async fn shared_attributes(
    client: &reqwest::Client,
    core: &str,
    token: &str,
    device_id: &str,
) -> serde_json::Map<String, Value> {
    let resp = client
        .get(format!(
            "{core}/api/telemetry/DEVICE/{device_id}/values/attributes/SHARED_SCOPE"
        ))
        .header("Authorization", bearer(token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Vec<Value> = resp.json().await.unwrap();
    list.into_iter()
        .map(|entry| {
            (
                entry["key"].as_str().unwrap().to_string(),
                entry["value"].clone(),
            )
        })
        .collect()
}

async fn wait_for_attribute(
    client: &reqwest::Client,
    core: &str,
    token: &str,
    device_id: &str,
    key: &str,
    expected: Option<&str>,
) {
    poll_until(Duration::from_secs(15), async || {
        let attrs = shared_attributes(client, core, token, device_id).await;
        attrs.get(key).and_then(Value::as_str) == expected
    })
    .await;
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
        "FIRMWARE",
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

    // 未绑定：不回退到"最新上传"的包。
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "1.0", 0).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 绑定校验：类型不符、目标档案不符 → 400。
    let sw_data: Vec<u8> = (0..3000u32).map(|i| (i % 7) as u8).collect();
    let sw_id = upload_ota(
        &client,
        &core,
        &admin_token,
        &profile_id,
        "SOFTWARE",
        "app",
        "1.0",
        sw_data.clone(),
    )
    .await;
    let resp = client
        .post(format!("{core}/api/deviceProfile"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"name": "ota-other-profile"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let other_profile_id = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let other_fw_id = upload_ota(
        &client,
        &core,
        &admin_token,
        &other_profile_id,
        "FIRMWARE",
        "fw",
        "9.0",
        vec![9; 10],
    )
    .await;
    for url in [
        format!("{core}/api/device/{device_id}/firmware/{sw_id}"),
        format!("{core}/api/device/{device_id}/software/{ota_id}"),
        format!("{core}/api/device/{device_id}/firmware/{other_fw_id}"),
        format!("{core}/api/deviceProfile/{profile_id}/firmware/{other_fw_id}"),
        format!("{core}/api/deviceProfile/{profile_id}/firmware/{sw_id}"),
    ] {
        assert_eq!(
            post(&client, url.clone(), &admin_token).await,
            StatusCode::BAD_REQUEST,
            "{url}"
        );
    }

    // 档案级绑定 fw 1.0：继承设备后台回写 fw_* 属性。
    assert_eq!(
        post(
            &client,
            format!("{core}/api/deviceProfile/{profile_id}/firmware/{ota_id}"),
            &admin_token
        )
        .await,
        StatusCode::OK
    );
    wait_for_attribute(
        &client,
        &core,
        &admin_token,
        &device_id,
        "fw_version",
        Some("1.0"),
    )
    .await;

    // HTTP 分块下载（transport）：chunk=0/1/2 拼接 == 原文
    let mut received = Vec::new();
    for chunk in 0..3 {
        let resp = device_chunk(
            &client,
            &transport,
            "OTA_TOKEN",
            "firmware",
            "fw",
            "1.0",
            chunk,
        )
        .await;
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
    // 请求的 title/version 与绑定包不一致 → 400
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "0.9", 0).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 新上传不改变既有绑定。
    let fw2_data: Vec<u8> = (0..1000u32).map(|i| (i % 13) as u8).collect();
    let fw2_id = upload_ota(
        &client,
        &core,
        &admin_token,
        &profile_id,
        "FIRMWARE",
        "fw",
        "2.0",
        fw2_data.clone(),
    )
    .await;
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "1.0", 0).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &fw_data[..16384]);
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "2.0", 0).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 设备级绑定优先于档案级（同步回写）。
    assert_eq!(
        post(
            &client,
            format!("{core}/api/device/{device_id}/firmware/{fw2_id}"),
            &admin_token
        )
        .await,
        StatusCode::OK
    );
    let resp = client
        .get(format!("{core}/api/device/{device_id}"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let device: Value = resp.json().await.unwrap();
    assert_eq!(device["firmwareId"], fw2_id);
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "2.0", 0).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), fw2_data.as_slice());
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "1.0", 0).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 同档案的新设备仍取档案绑定，并在创建后回写 fw_*。
    let resp = client
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"name": "ota-dev-2", "accessToken": "OTA_TOKEN_2"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let device2_id = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = device_chunk(
        &client,
        &transport,
        "OTA_TOKEN_2",
        "firmware",
        "fw",
        "1.0",
        0,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    wait_for_attribute(
        &client,
        &core,
        &admin_token,
        &device2_id,
        "fw_version",
        Some("1.0"),
    )
    .await;

    // 设备级绑定不兼容新档案时不允许换档案。
    let resp = client
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"id": device_id, "name": "ota-dev", "deviceProfileId": other_profile_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 仅提交更新接口已有字段时，OTA 绑定与创建时间不得被清空。
    let resp = client
        .post(format!("{core}/api/device"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({"id": device_id, "name": "ota-dev-renamed"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let updated: Value = resp.json().await.unwrap();
    assert_eq!(updated["firmwareId"], fw2_id);
    assert_eq!(updated["createdTime"], device["createdTime"]);

    // 状态回写：target_* 遥测、fw_state QUEUED → INITIATED、fw_* shared 属性。
    let resp = client
        .get(format!(
            "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=target_fw_title,target_fw_version,target_fw_ts,fw_state"
        ))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let latest: Value = resp.json().await.unwrap();
    assert_eq!(latest["target_fw_title"][0]["value"], "fw");
    assert_eq!(latest["target_fw_version"][0]["value"], "2.0");
    assert!(latest["target_fw_ts"][0]["value"].as_i64().unwrap() > 0);
    assert_eq!(latest["fw_state"][0]["value"], "INITIATED");
    let resp = client
        .get(format!(
            "{core}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=fw_state&startTs=0&limit=100"
        ))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    let history: Value = resp.json().await.unwrap();
    let states: Vec<&str> = history["fw_state"]
        .as_array()
        .unwrap()
        .iter()
        .map(|point| point["value"].as_str().unwrap())
        .collect();
    assert!(states.contains(&"QUEUED") && states.contains(&"INITIATED"));
    let attrs = shared_attributes(&client, &core, &admin_token, &device_id).await;
    assert_eq!(attrs["fw_title"], "fw");
    assert_eq!(attrs["fw_version"], "2.0");
    assert_eq!(attrs["fw_size"], 1000);
    assert_eq!(attrs["fw_checksum_algorithm"], "SHA256");
    assert_eq!(
        attrs["fw_checksum"],
        hex::encode(sha2::Sha256::digest(&fw2_data))
    );

    // SOFTWARE：未绑定 404；设备级绑定后经 /software 下载并回写 sw_*。
    let resp = device_chunk(
        &client,
        &transport,
        "OTA_TOKEN",
        "software",
        "app",
        "1.0",
        0,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        post(
            &client,
            format!("{core}/api/device/{device_id}/software/{sw_id}"),
            &admin_token
        )
        .await,
        StatusCode::OK
    );
    let resp = device_chunk(
        &client,
        &transport,
        "OTA_TOKEN",
        "software",
        "app",
        "1.0",
        0,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), sw_data.as_slice());
    let attrs = shared_attributes(&client, &core, &admin_token, &device_id).await;
    assert_eq!(attrs["sw_title"], "app");
    assert_eq!(attrs["sw_version"], "1.0");

    // DAO 元数据查询本身不读取 data；数据库分块与原文一致。
    let pool = sqlx::PgPool::connect(TEST_URL).await.unwrap();
    let ota_dao = tb_dao::OtaPackageDao::new(pool);
    let ota_uuid = uuid::Uuid::parse_str(&ota_id).unwrap();
    let metadata = ota_dao.find_meta_by_id(ota_uuid).await.unwrap().unwrap();
    assert!(metadata.data.is_none());
    let page = ota_dao
        .find_meta_by_tenant_filtered(
            uuid::Uuid::parse_str(&_tenant_id).unwrap(),
            Some(uuid::Uuid::parse_str(&profile_id).unwrap()),
            Some("FIRMWARE"),
            &tb_dao::PageLink::new(0, 10),
        )
        .await
        .unwrap();
    assert!(page.data.iter().all(|package| package.data.is_none()));
    let chunk = ota_dao
        .find_data_chunk(ota_uuid, 1, 16384)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(chunk, fw_data[..16384]);

    // 被绑定的包不可删除。
    assert_eq!(
        delete(
            &client,
            format!("{core}/api/otaPackage/{ota_id}"),
            &admin_token
        )
        .await,
        StatusCode::CONFLICT
    );

    // 设备级解绑：回落到档案绑定 1.0 并回写；软件解绑删除 sw_*。
    assert_eq!(
        delete(
            &client,
            format!("{core}/api/device/{device_id}/firmware"),
            &admin_token
        )
        .await,
        StatusCode::OK
    );
    let attrs = shared_attributes(&client, &core, &admin_token, &device_id).await;
    assert_eq!(attrs["fw_version"], "1.0");
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "1.0", 0).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        delete(
            &client,
            format!("{core}/api/device/{device_id}/software"),
            &admin_token
        )
        .await,
        StatusCode::OK
    );
    let attrs = shared_attributes(&client, &core, &admin_token, &device_id).await;
    assert!(attrs.keys().all(|key| !key.starts_with("sw_")));
    let resp = device_chunk(
        &client,
        &transport,
        "OTA_TOKEN",
        "software",
        "app",
        "1.0",
        0,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 档案解绑：继承设备的 fw_* 属性在后台删除。
    assert_eq!(
        delete(
            &client,
            format!("{core}/api/deviceProfile/{profile_id}/firmware"),
            &admin_token
        )
        .await,
        StatusCode::OK
    );
    for id in [&device_id, &device2_id] {
        wait_for_attribute(&client, &core, &admin_token, id, "fw_version", None).await;
    }
    let attrs = shared_attributes(&client, &core, &admin_token, &device_id).await;
    assert!(attrs.keys().all(|key| !key.starts_with("fw_")));
    let resp = device_chunk(&client, &transport, "OTA_TOKEN", "firmware", "fw", "1.0", 0).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 解绑后可删除。
    for id in [&ota_id, &fw2_id, &sw_id, &other_fw_id] {
        assert_eq!(
            delete(&client, format!("{core}/api/otaPackage/{id}"), &admin_token).await,
            StatusCode::OK
        );
    }
    let resp = client
        .get(format!("{core}/api/otaPackage/{ota_id}"))
        .header("Authorization", bearer(&admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
