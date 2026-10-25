//! 遥测/属性删除后的 latest 重算，以及 PostgreSQL、Redis 缓存与 EDQS 读模型的一致性。

mod common;

use std::time::Duration;

use common::*;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tb_common::{AttributeScope, KvValue};
use uuid::Uuid;

async fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
    for _ in 0..150 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{what} did not reach edqs in time");
}

fn edqs_latest(
    state: &tb_edqs::EdqsState,
    tenant_id: Uuid,
    entity_id: Uuid,
    key: &str,
) -> Option<(i64, KvValue)> {
    state
        .tenants
        .get(&tenant_id)?
        .latest
        .get(&entity_id)?
        .get(key)
        .cloned()
}

fn edqs_has_attribute(
    state: &tb_edqs::EdqsState,
    tenant_id: Uuid,
    entity_id: Uuid,
    key: &str,
) -> bool {
    state.tenants.get(&tenant_id).is_some_and(|repo| {
        repo.attributes
            .get(&(entity_id, AttributeScope::ServerScope))
            .is_some_and(|attrs| attrs.contains_key(key))
    })
}

#[tokio::test]
async fn telemetry_and_attribute_deletes_keep_latest_cache_and_edqs_consistent() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let (base, pool, _state) = start_server_with_state().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "Delete Sync", "delete-sync@test.io").await;
    let tenant_id = Uuid::parse_str(&tenant_id).unwrap();
    let device_id = create_device(&client, &base, &token, "Delete Sync Device").await;
    let device = Uuid::parse_str(&device_id).unwrap();
    let ts_base = format!("{base}/api/telemetry/DEVICE/{device_id}");

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    tb_queue::admin::ensure_topic(&brokers, tb_queue::TbTopic::EDQS_EVENTS, 10)
        .await
        .unwrap();
    let edqs = tb_edqs::EdqsState::default().with_pool(pool.clone());
    tb_edqs::snapshot::load_snapshot(&pool, &edqs)
        .await
        .unwrap();
    let group = format!("tb-edqs-delete-sync-{}", Uuid::now_v7().simple());
    let consumer =
        tb_edqs::consumer::start_consumer_for_group(&brokers, &group, edqs.clone()).await;

    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&token))
        .json(&json!([
            {"ts": 1000, "values": {"temperature": 10, "humidity": 50}},
            {"ts": 2000, "values": {"temperature": 20}},
            {"ts": 3000, "values": {"temperature": 30}}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .post(format!("{ts_base}/attributes/SERVER_SCOPE"))
        .header("Authorization", bearer(&token))
        .json(&json!({"region": "eu"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    wait_until("initial telemetry", || {
        edqs_latest(&edqs, tenant_id, device, "temperature") == Some((3000, KvValue::Long(30)))
            && edqs_latest(&edqs, tenant_id, device, "humidity").is_some()
            && edqs_has_attribute(&edqs, tenant_id, device, "region")
    })
    .await;

    let latest = || async {
        let resp = client
            .get(format!("{ts_base}/values/timeseries"))
            .header("Authorization", bearer(&token))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json::<Value>().await.unwrap()
    };
    // 预热 Redis 全量 latest 缓存，删除后不能返回旧值。
    assert_eq!(latest().await["temperature"][0]["ts"], 3000);

    // 删除最新点：latest 回退到 ts=2000，缓存与 EDQS 同步。
    let resp = client
        .delete(format!(
            "{ts_base}/timeseries?keys=temperature&startTs=2500&endTs=3500"
        ))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.json::<Value>().await.unwrap()["deleted"], 1);
    assert_eq!(
        latest().await["temperature"],
        json!([{"ts": 2000, "value": 20}])
    );
    wait_until("range delete", || {
        edqs_latest(&edqs, tenant_id, device, "temperature") == Some((2000, KvValue::Long(20)))
    })
    .await;

    // 删除旧点：latest 不变。
    let resp = client
        .delete(format!(
            "{ts_base}/timeseries?keys=temperature&startTs=0&endTs=1000"
        ))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["deleted"], 1);
    assert_eq!(
        latest().await["temperature"],
        json!([{"ts": 2000, "value": 20}])
    );

    // 范围覆盖剩余全部点：latest 删除。
    let resp = client
        .delete(format!(
            "{ts_base}/timeseries?keys=temperature&startTs=0&endTs=5000"
        ))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["deleted"], 1);
    assert!(latest().await.get("temperature").is_none());
    wait_until("range delete of last point", || {
        edqs_latest(&edqs, tenant_id, device, "temperature").is_none()
    })
    .await;

    // 全 key 删除与属性删除。
    let resp = client
        .delete(format!("{ts_base}/timeseries?keys=humidity"))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .delete(format!("{ts_base}/attributes/SERVER_SCOPE?keys=region"))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(latest().await.get("humidity").is_none());
    wait_until("full key and attribute delete", || {
        edqs_latest(&edqs, tenant_id, device, "humidity").is_none()
            && !edqs_has_attribute(&edqs, tenant_id, device, "region")
    })
    .await;

    let resp = client
        .delete(format!(
            "{ts_base}/timeseries?keys=temperature&startTs=10&endTs=5"
        ))
        .header("Authorization", bearer(&token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    consumer.abort();
}
