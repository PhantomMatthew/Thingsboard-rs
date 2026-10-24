//! 实体 REST 正确性：跨租户引用、更新字段保留与删除级联清理。

mod common;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use tb_common::{AttributeScope, EntityType, KvEntry, KvValue, TsKvEntry};
use tb_dao::models::{Alarm, AlarmSeverity, AlarmStatus};
use tb_dao::{AlarmDao, AssetDao, DeviceDao};
use uuid::Uuid;

use common::{bearer, create_device, setup_tenant_admin, start_server_with_pool};

fn calculated_field_config() -> Value {
    json!({
        "arguments": {"tempC": {"source": "TELEMETRY", "key": "temp"}},
        "expression": "tempC * 2",
        "output": {"type": "TELEMETRY", "key": "tempDouble"},
    })
}

async fn create_calculated_field(
    client: &Client,
    base: &str,
    token: &str,
    entity_id: Uuid,
) -> String {
    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(token))
        .json(&json!({
            "entityId": entity_id,
            "name": format!("cf-{}", Uuid::now_v7().simple()),
            "configuration": calculated_field_config(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn seed_entity_data(pool: &PgPool, tenant_id: Uuid, entity_id: Uuid) {
    let keys = tb_dao::KeyDictionaryDao::new(pool.clone());
    tb_dao::TimeseriesDao::new(pool.clone(), keys.clone())
        .save(
            entity_id,
            &[TsKvEntry {
                ts: 1000,
                key: "temp".into(),
                value: KvValue::Long(20),
            }],
        )
        .await
        .unwrap();
    tb_dao::AttributesDao::new(pool.clone(), keys)
        .save(
            entity_id,
            AttributeScope::ServerScope,
            &[KvEntry {
                key: "config".into(),
                value: KvValue::Bool(true),
            }],
        )
        .await
        .unwrap();
    AlarmDao::new(pool.clone())
        .save(&Alarm {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id,
            originator_id: entity_id,
            originator_type: EntityType::Device,
            r#type: "CascadeAlarm".into(),
            severity: AlarmSeverity::Minor,
            status: AlarmStatus::ActiveUnack,
            start_ts: 1000,
            end_ts: 1000,
            ack_ts: None,
            clear_ts: None,
            details: None,
            propagate: false,
            propagate_to_owner: false,
            propagate_to_tenant: false,
            propagate_relation_types: None,
            customer_id: None,
            assignee_id: None,
            assign_ts: None,
        })
        .await
        .unwrap();
}

async fn assert_entity_data_gone(pool: &PgPool, entity_id: Uuid) {
    for table in ["ts_kv", "ts_kv_latest", "attribute_kv"] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE entity_id = $1"
        ))
        .bind(entity_id)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "{table} rows must be deleted");
    }
    for table in ["alarm", "calculated_field"] {
        let column = if table == "alarm" {
            "originator_id"
        } else {
            "entity_id"
        };
        let count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE {column} = $1"))
                .bind(entity_id)
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(count, 0, "{table} rows must be deleted");
    }
}

#[tokio::test]
async fn entity_rest_correctness() {
    let (base, pool) = start_server_with_pool().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (token_a, tenant_a) =
        setup_tenant_admin(&client, &base, "Entity A", "entity-a@test.io").await;
    let (token_b, _tenant_b) =
        setup_tenant_admin(&client, &base, "Entity B", "entity-b@test.io").await;
    let tenant_a = Uuid::parse_str(&tenant_a).unwrap();

    let other_device = create_device(&client, &base, &token_b, "other-device").await;
    let own_device = create_device(&client, &base, &token_a, "own-device").await;
    let own_device_uuid = Uuid::parse_str(&own_device).unwrap();

    let resp = client
        .get(format!("{base}/api/deviceProfile/default"))
        .header("Authorization", bearer(&token_b))
        .send()
        .await
        .unwrap();
    let other_profile = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({"name": "cross-profile", "deviceProfileId": other_profile}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let resp = client
        .post(format!("{base}/api/asset"))
        .header("Authorization", bearer(&token_b))
        .json(&json!({"name": "other-asset"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{base}/api/assetProfile/default"))
        .header("Authorization", bearer(&token_b))
        .send()
        .await
        .unwrap();
    let other_asset_profile = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let resp = client
        .post(format!("{base}/api/asset"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({"name": "cross-asset-profile", "assetProfileId": other_asset_profile}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let form = reqwest::multipart::Form::new()
        .text("title", "other-fw")
        .text("version", "1")
        .text("type", "FIRMWARE")
        .text("deviceProfileId", other_profile.clone())
        .part(
            "file",
            reqwest::multipart::Part::bytes(vec![1, 2, 3]).file_name("fw.bin"),
        );
    let resp = client
        .post(format!("{base}/api/otaPackage"))
        .header("Authorization", bearer(&token_b))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let other_ota = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    for field in ["firmwareId", "softwareId"] {
        let resp = client
            .post(format!("{base}/api/deviceProfile"))
            .header("Authorization", bearer(&token_a))
            .json(&json!({"name": format!("bad-{field}"), (field): other_ota.clone()}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({
            "entityId": other_device,
            "name": "cross-entity",
            "configuration": calculated_field_config(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({
            "entityId": Uuid::now_v7(),
            "name": "missing-entity",
            "configuration": calculated_field_config(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = client
        .post(format!("{base}/api/customer"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({"title": "Entity Customer"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let customer_id = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let customer_uuid = Uuid::parse_str(&customer_id).unwrap();

    let device_dao = DeviceDao::new(pool.clone());
    let mut device = device_dao
        .find_by_id(own_device_uuid)
        .await
        .unwrap()
        .unwrap();
    let external_id = Uuid::now_v7();
    let firmware_id = Uuid::now_v7();
    let software_id = Uuid::now_v7();
    device.customer_id = Some(customer_uuid);
    device.external_id = Some(external_id);
    device.firmware_id = Some(firmware_id);
    device.software_id = Some(software_id);
    device.device_data = json!({"transport": {"keep": true}});
    device.additional_info = Some(json!({"keep": "device"}));
    let device = device_dao.save(&device).await.unwrap();
    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({"id": own_device, "name": "own-device-updated"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let updated: Value = resp.json().await.unwrap();
    assert_eq!(updated["createdTime"], json!(device.created_time));
    assert_eq!(updated["customerId"], json!(customer_id));
    assert_eq!(updated["externalId"], json!(external_id));
    assert_eq!(updated["firmwareId"], json!(firmware_id));
    assert_eq!(updated["softwareId"], json!(software_id));
    assert_eq!(updated["deviceData"], device.device_data);
    assert_eq!(updated["additionalInfo"], json!({"keep": "device"}));

    let resp = client
        .post(format!("{base}/api/asset"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({"name": "own-asset"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let own_asset = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let own_asset_uuid = Uuid::parse_str(&own_asset).unwrap();
    let asset_dao = AssetDao::new(pool.clone());
    let mut asset = asset_dao.find_by_id(own_asset_uuid).await.unwrap().unwrap();
    asset.customer_id = Some(customer_uuid);
    asset.external_id = Some(external_id);
    asset.additional_info = Some(json!({"keep": "asset"}));
    let asset = asset_dao.save(&asset).await.unwrap();
    let resp = client
        .post(format!("{base}/api/asset"))
        .header("Authorization", bearer(&token_a))
        .json(&json!({"id": own_asset, "name": "own-asset-updated"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let updated: Value = resp.json().await.unwrap();
    assert_eq!(updated["createdTime"], json!(asset.created_time));
    assert_eq!(updated["customerId"], json!(customer_id));
    assert_eq!(updated["externalId"], json!(external_id));
    assert_eq!(updated["additionalInfo"], json!({"keep": "asset"}));

    seed_entity_data(&pool, tenant_a, own_device_uuid).await;
    create_calculated_field(&client, &base, &token_a, own_device_uuid).await;
    let resp = client
        .delete(format!("{base}/api/device/{own_device}"))
        .header("Authorization", bearer(&token_a))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_entity_data_gone(&pool, own_device_uuid).await;

    seed_entity_data(&pool, tenant_a, own_asset_uuid).await;
    create_calculated_field(&client, &base, &token_a, own_asset_uuid).await;
    let resp = client
        .delete(format!("{base}/api/asset/{own_asset}"))
        .header("Authorization", bearer(&token_a))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_entity_data_gone(&pool, own_asset_uuid).await;
}
