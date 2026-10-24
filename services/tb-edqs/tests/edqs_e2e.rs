//! EDQS 端到端：DB 造数 → 快照加载 → 查询（实体过滤 + latest GT + 分页排序）
//! → Kafka 事件增量（latest 更新 / 属性删除 / 实体删除）→ 查询结果随事件更新。
//! 专属测试库 thingsboard_edqs_test（每次 DROP/CREATE），Kafka 用独立 consumer group。

use std::time::Duration;

use reqwest::Client;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_common::{AttributeScope, KvEntry, KvValue, TsKvEntry};
use tb_dao::models::{AssetProfile, Device, DeviceProfile, Tenant};
use tb_dao::{
    AssetProfileDao, AttributesDao, DeviceDao, DeviceProfileDao, KeyDictionaryDao, TenantDao,
    TimeseriesDao,
};
use tb_edqs::{EdqsState, build_router};
use tb_proto::edqs as pb;
use tb_queue::KafkaQueueProducer;
use uuid::Uuid;

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
const TEST_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/thingsboard_edqs_test";
const BROKERS: &str = "localhost:29092";

/// 同库 DROP/CREATE，测试间串行。
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn setup_pool() -> PgPool {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
        .await
        .expect("connect admin db");
    sqlx::query("DROP DATABASE IF EXISTS thingsboard_edqs_test WITH (FORCE)")
        .execute(&admin)
        .await
        .expect("drop test db");
    sqlx::query("CREATE DATABASE thingsboard_edqs_test")
        .execute(&admin)
        .await
        .expect("create test db");
    admin.close().await;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(TEST_URL)
        .await
        .expect("connect test db");
    tb_dao::migrate(&pool).await.expect("migrate test db");
    pool
}

async fn query(client: &Client, base: &str, body: &Value) -> Value {
    let resp = client
        .post(format!("{base}/api/query/entityData"))
        .json(body)
        .send()
        .await
        .expect("query request");
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

/// 轮询查询直到断言通过（Kafka 事件异步到达）。
async fn poll_query(client: &Client, base: &str, body: &Value, check: impl Fn(&Value) -> bool) {
    for _ in 0..75 {
        let resp = query(client, base, body).await;
        if check(&resp) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let resp = query(client, base, body).await;
    panic!("query did not converge in time: {resp}");
}

#[tokio::test]
async fn edqs_end_to_end() {
    let _guard = DB_LOCK.lock().await;
    let pool = setup_pool().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let keys = KeyDictionaryDao::new(pool.clone());
    let device_dao = DeviceDao::new(pool.clone());
    let profile_dao = DeviceProfileDao::new(pool.clone());
    let attributes_dao = AttributesDao::new(pool.clone(), keys.clone());
    let timeseries_dao = TimeseriesDao::new(pool.clone(), keys);

    let tenant = tenant_dao
        .save(&Tenant {
            id: Uuid::now_v7(),
            created_time: 0,
            title: "EDQS Tenant".into(),
            region: None,
            country: None,
            state: None,
            city: None,
            address: None,
            address2: None,
            zip: None,
            phone: None,
            email: None,
            additional_info: None,
            tenant_profile_id: None,
        })
        .await
        .unwrap();
    let tenant_id = tenant.id;
    let profile = profile_dao
        .save(&DeviceProfile {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id,
            name: "default".into(),
            description: None,
            is_default: true,
            transport_type: "DEFAULT".into(),
            provision_type: "DISABLED".into(),
            profile_data: json!({}),
            firmware_id: None,
            software_id: None,
        })
        .await
        .unwrap();

    let mut device_ids = vec![];
    for name in ["Sensor A", "Sensor B", "Sensor C"] {
        let d = device_dao
            .save(&Device {
                id: Uuid::now_v7(),
                created_time: 0,
                tenant_id,
                customer_id: None,
                device_profile_id: profile.id,
                name: name.into(),
                r#type: Some("sensor".into()),
                label: None,
                external_id: None,
                firmware_id: None,
                software_id: None,
                additional_info: None,
                device_data: json!({}),
            })
            .await
            .unwrap();
        device_ids.push(d.id);
    }
    let (a, b, c) = (device_ids[0], device_ids[1], device_ids[2]);

    for (id, temp) in [(a, 25), (b, 10), (c, 15)] {
        timeseries_dao
            .save(
                id,
                &[TsKvEntry {
                    ts: 1000,
                    key: "temperature".into(),
                    value: KvValue::Long(temp),
                }],
            )
            .await
            .unwrap();
    }
    attributes_dao
        .save(
            a,
            AttributeScope::ServerScope,
            &[
                KvEntry {
                    key: "firmware".into(),
                    value: KvValue::Str("1.0".into()),
                },
                KvEntry {
                    key: "region".into(),
                    value: KvValue::Str("eu".into()),
                },
            ],
        )
        .await
        .unwrap();

    // 起一个 edqs 实例：快照 + 消费 + HTTP。
    let state = EdqsState::default();
    let stats = tb_edqs::snapshot::load_snapshot(&pool, &state)
        .await
        .unwrap();
    assert_eq!(stats.entities, 3);
    assert_eq!(stats.latest, 3);
    let group = format!("tb-edqs-test-{}", Uuid::now_v7().simple());
    let consumer =
        tb_edqs::consumer::start_consumer_for_group(BROKERS, &group, state.clone()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = Client::new();

    // health
    let resp = client.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // 快照查询：ENTITY_TYPE=DEVICE + temperature LATEST GT 20 + 按 name ASC 分页。
    let hot_query = json!({
        "tenantId": tenant_id,
        "entityFilter": {"type": "ENTITY_TYPE", "entityType": "DEVICE"},
        "keyFilters": [{"key": "temperature", "source": "LATEST", "predicate": {"op": "GT", "value": 20}}],
        "pageLink": {"page": 0, "pageSize": 10, "sortBy": {"key": "name", "source": "ENTITY_FIELD", "direction": "ASC"}},
        "fields": [
            {"key": "name", "source": "ENTITY_FIELD"},
            {"key": "temperature", "source": "LATEST"},
            {"key": "region", "source": "SERVER"},
            {"key": "firmware", "source": "SERVER"}
        ]
    });
    let resp = query(&client, &base, &hot_query).await;
    assert_eq!(resp["totalElements"], 1, "{resp}");
    let row = &resp["data"][0];
    assert_eq!(row["entityId"], a.to_string());
    assert_eq!(row["entityType"], "DEVICE");
    assert_eq!(row["values"]["name"], "Sensor A");
    assert_eq!(row["values"]["temperature"], 25);
    assert_eq!(row["values"]["region"], "eu");
    assert_eq!(row["values"]["firmware"], "1.0");

    // NAME_CONTAINS 过滤。
    let resp = query(
        &client,
        &base,
        &json!({
            "tenantId": tenant_id,
            "entityFilter": {"type": "NAME_CONTAINS", "nameContains": "sensor b"},
            "pageLink": {"page": 0, "pageSize": 10}
        }),
    )
    .await;
    assert_eq!(resp["totalElements"], 1);
    assert_eq!(resp["data"][0]["entityId"], b.to_string());

    // 事件 1：C 的 temperature 更新为 35 → GT 20 命中 A、C。
    let producer = KafkaQueueProducer::new(BROKERS, "edqs-e2e-test").unwrap();
    tb_queue::edqs::publish_event(
        &producer,
        tenant_id,
        &pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::LatestTelemetry(
                pb::LatestTelemetryMsg {
                    entity_id: c.to_string(),
                    values: vec![tb_proto::convert::kv_to_proto_named(
                        0,
                        "temperature",
                        &KvValue::Long(35),
                    )],
                    ts: vec![2000],
                },
            )),
        },
    )
    .await
    .unwrap();
    poll_query(&client, &base, &hot_query, |resp| {
        resp["totalElements"] == 2
            && resp["data"][0]["entityId"] == a.to_string()
            && resp["data"][1]["entityId"] == c.to_string()
            && resp["data"][1]["values"]["temperature"] == 35
    })
    .await;

    // 事件 2：删除 A 的 firmware 属性 → 查询 fields 不再返回该 key，region 仍在。
    tb_queue::edqs::publish_event(
        &producer,
        tenant_id,
        &pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::AttributeUpdate(
                pb::AttributeUpdateMsg {
                    entity_id: a.to_string(),
                    scope: tb_proto::common::AttributeScope::ServerScope as i32,
                    updated: vec![],
                    deleted_keys: vec!["firmware".into()],
                    update_ts: 3000,
                },
            )),
        },
    )
    .await
    .unwrap();
    poll_query(&client, &base, &hot_query, |resp| {
        resp["data"][0]["values"].get("firmware").is_none()
            && resp["data"][0]["values"]["region"] == "eu"
    })
    .await;

    // 事件 3：删除实体 B → ALL 过滤总数 3 → 2。
    tb_queue::edqs::publish_event(
        &producer,
        tenant_id,
        &pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::EntityDelete(
                pb::EntityDeleteMsg {
                    entity_id: b.to_string(),
                    entity_type: tb_proto::common::EntityType::Device as i32,
                },
            )),
        },
    )
    .await
    .unwrap();
    let all_query = json!({
        "tenantId": tenant_id,
        "entityFilter": {"type": "ALL"},
        "pageLink": {"page": 0, "pageSize": 50}
    });
    poll_query(&client, &base, &all_query, |resp| {
        resp["totalElements"] == 2
    })
    .await;

    // 分页：pageSize=1 两页。
    let resp = query(
        &client,
        &base,
        &json!({
            "tenantId": tenant_id,
            "entityFilter": {"type": "ALL"},
            "pageLink": {"page": 0, "pageSize": 1, "sortBy": {"key": "name", "direction": "ASC"}}
        }),
    )
    .await;
    assert_eq!(resp["totalElements"], 2);
    assert_eq!(resp["totalPages"], 2);
    assert_eq!(resp["hasNext"], true);

    // 事件 4：删除整个租户 → 查询不再返回残留实体。
    tb_queue::edqs::publish_event(
        &producer,
        tenant_id,
        &pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::TenantDelete(
                pb::TenantDeleteMsg {
                    tenant_id: tenant_id.to_string(),
                },
            )),
        },
    )
    .await
    .unwrap();
    poll_query(&client, &base, &all_query, |resp| {
        resp["totalElements"] == 0
    })
    .await;

    consumer.abort();
}

/// asset 也应进入快照与 ENTITY_TYPE 过滤。
#[tokio::test]
async fn edqs_asset_snapshot() {
    let _guard = DB_LOCK.lock().await;
    let pool = setup_pool().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let asset_profile_dao = AssetProfileDao::new(pool.clone());
    let asset_dao = tb_dao::AssetDao::new(pool.clone());

    let tenant = tenant_dao
        .save(&Tenant {
            id: Uuid::now_v7(),
            created_time: 0,
            title: "Asset Tenant".into(),
            region: None,
            country: None,
            state: None,
            city: None,
            address: None,
            address2: None,
            zip: None,
            phone: None,
            email: None,
            additional_info: None,
            tenant_profile_id: None,
        })
        .await
        .unwrap();
    let profile = asset_profile_dao
        .save(&AssetProfile {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            name: "default".into(),
            description: None,
            is_default: true,
            profile_data: json!({}),
        })
        .await
        .unwrap();
    asset_dao
        .save(&tb_dao::models::Asset {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            customer_id: None,
            asset_profile_id: profile.id,
            name: "Building A".into(),
            label: Some("hq".into()),
            external_id: None,
            additional_info: None,
        })
        .await
        .unwrap();

    let state = EdqsState::default();
    let stats = tb_edqs::snapshot::load_snapshot(&pool, &state)
        .await
        .unwrap();
    assert_eq!(stats.entities, 1);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    let client = Client::new();
    let resp = query(
        &client,
        &format!("http://{addr}"),
        &json!({
            "tenantId": tenant.id,
            "entityFilter": {"type": "ENTITY_TYPE", "entityType": "ASSET"},
            "pageLink": {"page": 0, "pageSize": 10},
            "fields": [{"key": "name", "source": "ENTITY_FIELD"}, {"key": "label", "source": "ENTITY_FIELD"}]
        }),
    )
    .await;
    assert_eq!(resp["totalElements"], 1);
    assert_eq!(resp["data"][0]["entityType"], "ASSET");
    assert_eq!(resp["data"][0]["values"]["label"], "hq");
}
