//! 删除生命周期：Profile 被引用时拒绝删除；客户、设备、租户删除后不留孤儿数据，
//! 关闭设备会话并写墓碑，墓碑清理删除后迟到的写入。

mod common;

use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use tb_common::{AttributeScope, KvEntry, KvValue, TsKvEntry};
use tb_core::device_session::SessionEntry;
use tb_dao::{AssetDao, DeviceDao};
use uuid::Uuid;

use common::{
    bearer, create_device, login, poll_until, setup_tenant_admin, start_server_with_state,
};

async fn post_json(client: &Client, url: String, token: &str, body: Value) -> Uuid {
    let resp = client
        .post(url)
        .header("Authorization", bearer(token))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{body}");
    let saved: Value = resp.json().await.unwrap();
    Uuid::parse_str(saved["id"].as_str().unwrap()).unwrap()
}

async fn delete(client: &Client, url: String, token: &str) -> StatusCode {
    client
        .delete(url)
        .header("Authorization", bearer(token))
        .send()
        .await
        .unwrap()
        .status()
}

async fn seed(pool: &PgPool, tenant_id: Uuid, entity_id: Uuid, peer: Uuid) {
    let keys = tb_dao::KeyDictionaryDao::new(pool.clone());
    tb_dao::TimeseriesDao::new(pool.clone(), keys.clone())
        .save(
            entity_id,
            &[TsKvEntry {
                ts: 1000,
                key: "temp".into(),
                value: KvValue::Long(1),
            }],
        )
        .await
        .unwrap();
    tb_dao::AttributesDao::new(pool.clone(), keys)
        .save(
            entity_id,
            AttributeScope::ServerScope,
            &[KvEntry {
                key: "cfg".into(),
                value: KvValue::Bool(true),
            }],
        )
        .await
        .unwrap();
    for sql in [
        "INSERT INTO alarm (id, created_time, tenant_id, originator_id, originator_type, type, \
         severity, status, start_ts, end_ts) \
         VALUES (gen_random_uuid(), 0, $1, $2, 'DEVICE', 'seeded', 'MINOR', 'ACTIVE_UNACK', 0, 0)",
        "INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type) \
         VALUES ($2, 'DEVICE', $3, 'ASSET', 'Contains')",
        "INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type) \
         VALUES ($3, 'ASSET', $2, 'DEVICE', 'Manages')",
        "INSERT INTO calculated_field (id, created_time, tenant_id, entity_id, name, configuration) \
         VALUES (gen_random_uuid(), 0, $1, $2, gen_random_uuid()::text, '{}'::jsonb)",
        "INSERT INTO rule_node_dedup_window (id, tenant_id, rule_chain_id, rule_node_id, \
         originator_id, strategy, out_msg_type, queue_name, max_retries, state, expires_at, \
         updated_at) VALUES (gen_random_uuid(), $1, gen_random_uuid(), gen_random_uuid(), $2, \
         'FIRST', '', 'Main', 3, 'OPEN', 0, 0)",
    ] {
        sqlx::query(sql)
            .bind(tenant_id)
            .bind(entity_id)
            .bind(peer)
            .execute(pool)
            .await
            .unwrap();
    }
}

async fn entity_rows(pool: &PgPool, entity_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM ts_kv WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM ts_kv_latest WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM attribute_kv WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM alarm WHERE originator_id = $1) \
         + (SELECT COUNT(*) FROM relation WHERE from_id = $1 OR to_id = $1) \
         + (SELECT COUNT(*) FROM calculated_field WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM rule_node_dedup_window WHERE originator_id = $1)",
    )
    .bind(entity_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn tombstoned(pool: &PgPool, entity_id: Uuid) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM entity_tombstone WHERE entity_id = $1)")
        .bind(entity_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn count(pool: &PgPool, sql: &str, id: Uuid) -> i64 {
    sqlx::query_scalar(sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn entity_deletes_leave_no_orphans() {
    let (base, pool, state) = start_server_with_state().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "Lifecycle", "lifecycle@test.io").await;
    let tenant_id = Uuid::parse_str(&tenant_id).unwrap();
    let sys_token = login(&client, &base, "sysadmin@thingsboard.org", "sysadmin").await["token"]
        .as_str()
        .unwrap()
        .to_string();
    let device_dao = DeviceDao::new(pool.clone());

    let device_id =
        Uuid::parse_str(&create_device(&client, &base, &token, "lifecycle-device").await).unwrap();
    let device = device_dao.find_by_id(device_id).await.unwrap().unwrap();
    assert_eq!(
        delete(
            &client,
            format!("{base}/api/deviceProfile/{}", device.device_profile_id),
            &token
        )
        .await,
        StatusCode::CONFLICT
    );
    let asset_id = post_json(
        &client,
        format!("{base}/api/asset"),
        &token,
        json!({"name": "lifecycle-asset"}),
    )
    .await;
    let asset = AssetDao::new(pool.clone())
        .find_by_id(asset_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delete(
            &client,
            format!("{base}/api/assetProfile/{}", asset.asset_profile_id),
            &token
        )
        .await,
        StatusCode::CONFLICT
    );
    let unused_profile = post_json(
        &client,
        format!("{base}/api/deviceProfile"),
        &token,
        json!({"name": "lifecycle-unused"}),
    )
    .await;
    assert_eq!(
        delete(
            &client,
            format!("{base}/api/deviceProfile/{unused_profile}"),
            &token
        )
        .await,
        StatusCode::OK
    );

    let customer_id = post_json(
        &client,
        format!("{base}/api/customer"),
        &token,
        json!({"title": "Lifecycle Customer"}),
    )
    .await;
    let customer_user_id = post_json(
        &client,
        format!("{base}/api/user"),
        &token,
        json!({
            "customerId": customer_id,
            "authority": "CUSTOMER_USER",
            "email": "lifecycle-customer@test.io",
            "password": "customer",
        }),
    )
    .await;
    let mut assigned = device.clone();
    assigned.customer_id = Some(customer_id);
    device_dao.save(&assigned).await.unwrap();
    seed(&pool, tenant_id, customer_id, device_id).await;
    seed(&pool, tenant_id, customer_user_id, asset_id).await;
    sqlx::query(
        "INSERT INTO alarm (id, created_time, tenant_id, originator_id, originator_type, type, \
         severity, status, start_ts, end_ts, customer_id) \
         VALUES (gen_random_uuid(), 0, $1, $2, 'DEVICE', 'owned', 'MINOR', 'ACTIVE_UNACK', 0, 0, $3)",
    )
    .bind(tenant_id)
    .bind(device_id)
    .bind(customer_id)
    .execute(&pool)
    .await
    .unwrap();

    assert_eq!(
        delete(
            &client,
            format!("{base}/api/customer/{customer_id}"),
            &token
        )
        .await,
        StatusCode::OK
    );
    for id in [customer_id, customer_user_id] {
        assert_eq!(entity_rows(&pool, id).await, 0);
        assert!(tombstoned(&pool, id).await);
    }
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM tb_user WHERE id = $1",
            customer_user_id
        )
        .await,
        0
    );
    let device = device_dao.find_by_id(device_id).await.unwrap().unwrap();
    assert_eq!(device.customer_id, None);
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM alarm WHERE customer_id = $1",
            customer_id
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM alarm WHERE originator_id = $1",
            device_id
        )
        .await,
        1
    );

    seed(&pool, tenant_id, device_id, asset_id).await;
    assert_eq!(
        delete(&client, format!("{base}/api/device/{device_id}"), &token).await,
        StatusCode::OK
    );
    assert_eq!(entity_rows(&pool, device_id).await, 0);
    assert!(tombstoned(&pool, device_id).await);

    let device2 =
        Uuid::parse_str(&create_device(&client, &base, &token, "lifecycle-device-2").await)
            .unwrap();
    let admin_id: Uuid = sqlx::query_scalar("SELECT id FROM tb_user WHERE email = $1")
        .bind("lifecycle@test.io")
        .fetch_one(&pool)
        .await
        .unwrap();
    let tenant_entities = [tenant_id, device2, asset_id, admin_id];
    for id in tenant_entities {
        seed(&pool, tenant_id, id, Uuid::now_v7()).await;
    }
    for sql in [
        "INSERT INTO rule_node_debug_event (id, ts, tenant_id, rule_chain_id, rule_node_id, \
         msg_id, event_type) VALUES (gen_random_uuid(), 0, $1, gen_random_uuid(), \
         gen_random_uuid(), gen_random_uuid(), 'IN')",
        "INSERT INTO rule_node_delay (id, tenant_id, rule_chain_id, rule_node_id, next_node_id, \
         msg_id, hop_count, msg, execute_at, state, updated_at) VALUES (gen_random_uuid(), $1, \
         gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 0, \
         '{}'::jsonb, 0, 'PENDING', 0)",
        "INSERT INTO oauth2_client (id, created_time, tenant_id, title, client_id, \
         authorization_uri, token_uri, user_info_uri) VALUES (gen_random_uuid(), 0, $1, \
         'lifecycle', gen_random_uuid()::text, 'https://idp/auth', 'https://idp/token', \
         'https://idp/userinfo')",
    ] {
        sqlx::query(sql)
            .bind(tenant_id)
            .execute(&pool)
            .await
            .unwrap();
    }
    state
        .device_sessions
        .open_session(
            device2,
            SessionEntry {
                session_id: "lifecycle-session".into(),
                node_id: "lifecycle-node".into(),
                is_gateway: false,
                via_gateway: false,
                sub_attributes: true,
                sub_rpc: true,
                last_seen_ms: 0,
            },
        )
        .await
        .unwrap();

    assert_eq!(
        delete(
            &client,
            format!("{base}/api/tenant/{tenant_id}"),
            &sys_token
        )
        .await,
        StatusCode::OK
    );
    for id in tenant_entities {
        assert_eq!(entity_rows(&pool, id).await, 0);
        assert!(tombstoned(&pool, id).await);
    }
    for table in [
        "alarm",
        "calculated_field",
        "rule_node_debug_event",
        "rule_node_dedup_window",
        "rule_node_delay",
        "oauth2_client",
    ] {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE tenant_id = $1");
        assert_eq!(count(&pool, &sql, tenant_id).await, 0, "{table}");
    }
    poll_until(Duration::from_secs(10), async || {
        state
            .device_sessions
            .any_sessions(device2)
            .await
            .unwrap()
            .is_empty()
    })
    .await;

    tb_dao::TimeseriesDao::new(pool.clone(), tb_dao::KeyDictionaryDao::new(pool.clone()))
        .save(
            device2,
            &[TsKvEntry {
                ts: 2000,
                key: "late".into(),
                value: KvValue::Long(2),
            }],
        )
        .await
        .unwrap();
    assert_eq!(entity_rows(&pool, device2).await, 2);
    tb_dao::entity_cleanup::sweep_tombstones(&pool, 0, 100)
        .await
        .unwrap();
    assert_eq!(entity_rows(&pool, device2).await, 0);
}
