//! EDQS 代理端点：core 认证 + tenantId 强制 → 转发 tb-edqs。
//! 起真实 edqs 实例（同测试库 + 独立 consumer group），遥测经 REST 落库后
//! edqs 事件经 Kafka 到达，代理查询轮询命中。

mod common;

use std::time::Duration;

use common::*;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn edqs_proxy_entity_data_query() {
    let _guard = INTEGRATION_LOCK.lock().await;

    // 先占位 edqs 端口（AppState 构造时读 TB_EDQS_URL），core 起来后再在同一 listener 上起 edqs。
    let edqs_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let edqs_addr = edqs_listener.local_addr().unwrap();
    unsafe {
        std::env::set_var("TB_EDQS_URL", format!("http://{edqs_addr}"));
        std::env::set_var("TB_EDQS_API_KEY", "edqs-proxy-test-key");
    }

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    tb_queue::admin::ensure_topic(&brokers, tb_queue::TbTopic::EDQS_EVENTS, 10)
        .await
        .unwrap();

    let base = start_server().await;
    let client = reqwest::Client::new();
    let (token, tenant_id) = setup_tenant_admin(&client, &base, "EDQS Proxy T", "edqs@t.io").await;
    let device_id = create_device(&client, &base, &token, "EDQS Proxy Device").await;

    // REST 落遥测 + 属性（同时发 edqs 事件）。
    let resp = client
        .post(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/timeseries"
        ))
        .header("Authorization", bearer(&token))
        .json(&json!({"temperature": 42}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .post(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/attributes/SERVER_SCOPE"
        ))
        .header("Authorization", bearer(&token))
        .json(&json!({"region": "eu"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 起 edqs：快照（含上面的数据）+ Kafka 消费。
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(TEST_URL)
        .await
        .expect("connect test db");
    let edqs_state = tb_edqs::EdqsState::default().with_api_key("edqs-proxy-test-key");
    tb_edqs::snapshot::load_snapshot(&pool, &edqs_state)
        .await
        .unwrap();
    let group = format!("tb-edqs-proxy-test-{}", uuid::Uuid::now_v7().simple());
    let consumer =
        tb_edqs::consumer::start_consumer_for_group(&brokers, &group, edqs_state.clone()).await;
    tokio::spawn(async move {
        axum::serve(edqs_listener, tb_edqs::build_router(edqs_state))
            .await
            .unwrap();
    });

    // 无认证 → 401。
    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .json(&json!({"tenantId": tenant_id, "entityFilter": {"type": "ALL"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 代理查询：temperature LATEST GT 40 → 命中设备；body 不带 tenantId 也应强制为 JWT 租户。
    let query_body = json!({
        "entityFilter": {"type": "ENTITY_TYPE", "entityType": "DEVICE"},
        "keyFilters": [{"key": "temperature", "source": "LATEST", "predicate": {"op": "GT", "value": 40}}],
        "pageLink": {"page": 0, "pageSize": 10},
        "fields": [
            {"key": "name", "source": "ENTITY_FIELD"},
            {"key": "temperature", "source": "LATEST"},
            {"key": "region", "source": "SERVER"}
        ]
    });
    let mut found = false;
    for _ in 0..75 {
        let resp = client
            .post(format!("{base}/api/edqs/query/entityData"))
            .header("Authorization", bearer(&token))
            .json(&query_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: Value = resp.json().await.unwrap();
        if body["totalElements"] == 1 {
            assert_eq!(body["data"][0]["entityId"], device_id);
            assert_eq!(body["data"][0]["values"]["name"], "EDQS Proxy Device");
            assert_eq!(body["data"][0]["values"]["temperature"], 42);
            assert_eq!(body["data"][0]["values"]["region"], "eu");
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(found, "device did not appear in edqs proxy query");

    let (customer_a, customer_a_token) = create_customer_user(
        &client,
        &base,
        &token,
        "EDQS Customer A",
        "customer.a@edqs.io",
    )
    .await;
    let (customer_b, _) = create_customer_user(
        &client,
        &base,
        &token,
        "EDQS Customer B",
        "customer.b@edqs.io",
    )
    .await;
    let other_device_id = create_device(&client, &base, &token, "EDQS Other Customer Device").await;
    assign_device(&client, &base, &token, &device_id, &customer_a).await;
    assign_device(&client, &base, &token, &other_device_id, &customer_b).await;

    let all_query = json!({
        "tenantId": tenant_id,
        "entityFilter": {"type": "ALL"},
        "fields": [{"key": "name", "source": "ENTITY_FIELD"}]
    });
    let mut both_visible = false;
    for _ in 0..75 {
        let resp = client
            .post(format!("{base}/api/edqs/query/entityData"))
            .header("Authorization", bearer(&token))
            .json(&all_query)
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        if body["totalElements"] == 2 {
            both_visible = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(both_visible, "customer assignments did not reach edqs");

    let scoped_query = json!({
        "tenantId": tenant_id,
        "customerId": customer_b,
        "entityFilter": {"type": "ENTITY_IDS", "ids": [device_id, other_device_id]},
        "fields": [{"key": "name", "source": "ENTITY_FIELD"}]
    });
    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&customer_a_token))
        .json(&scoped_query)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"][0]["entityId"], device_id);

    let resp = client
        .get(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&customer_a_token))
        .query(&[
            ("tenantId", tenant_id.as_str()),
            ("customerId", customer_b.as_str()),
            ("entityType", "DEVICE"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"][0]["entityId"], device_id);

    let resp = client
        .post(format!("http://{edqs_addr}/api/query/entityData"))
        .header("X-EDQS-KEY", "edqs-proxy-test-key")
        .json(&json!({
            "tenantId": tenant_id,
            "customerId": customer_a,
            "entityFilter": {"type": "ENTITY_IDS", "ids": [device_id, other_device_id]}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"][0]["entityId"], device_id);

    let resp = client
        .get(format!("http://{edqs_addr}/api/query/entityData"))
        .header("X-EDQS-KEY", "edqs-proxy-test-key")
        .query(&[
            ("tenantId", tenant_id.as_str()),
            ("customerId", customer_a.as_str()),
            ("entityType", "DEVICE"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"][0]["entityId"], device_id);

    // 新遥测经 Kafka 事件到达（快照之后的数据）。
    let resp = client
        .post(format!(
            "{base}/api/telemetry/DEVICE/{device_id}/timeseries"
        ))
        .header("Authorization", bearer(&token))
        .json(&json!({"humidity": 55}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let humidity_query = json!({
        "entityFilter": {"type": "ENTITY_IDS", "ids": [device_id]},
        "keyFilters": [{"key": "humidity", "source": "LATEST", "predicate": {"op": "GTE", "value": 50}}],
        "pageLink": {"page": 0, "pageSize": 10},
        "fields": [{"key": "humidity", "source": "LATEST"}]
    });
    let mut updated = false;
    for _ in 0..75 {
        let resp = client
            .post(format!("{base}/api/edqs/query/entityData"))
            .header("Authorization", bearer(&token))
            .json(&humidity_query)
            .send()
            .await
            .unwrap();
        let body: Value = resp.json().await.unwrap();
        if body["totalElements"] == 1 && body["data"][0]["values"]["humidity"] == 55 {
            updated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(updated, "post-snapshot telemetry did not reach edqs");

    consumer.abort();
}

async fn create_customer_user(
    client: &reqwest::Client,
    base: &str,
    admin_token: &str,
    title: &str,
    email: &str,
) -> (String, String) {
    let resp = client
        .post(format!("{base}/api/customer"))
        .header("Authorization", bearer(admin_token))
        .json(&json!({"title": title}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let customer: Value = resp.json().await.unwrap();
    let customer_id = customer["id"].as_str().unwrap().to_string();
    let resp = client
        .post(format!("{base}/api/user"))
        .header("Authorization", bearer(admin_token))
        .json(&json!({
            "email": email,
            "authority": "CUSTOMER_USER",
            "customerId": customer_id,
            "password": "customer",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tokens = login(client, base, email, "customer").await;
    (customer_id, tokens["token"].as_str().unwrap().to_string())
}

async fn assign_device(
    client: &reqwest::Client,
    base: &str,
    admin_token: &str,
    device_id: &str,
    customer_id: &str,
) {
    let resp = client
        .post(format!(
            "{base}/api/device/{device_id}/assign/{customer_id}"
        ))
        .header("Authorization", bearer(admin_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

async fn edqs_returning(status: StatusCode) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .fallback(move || async move { (status, axum::Json(json!({"source": "edqs"}))) });
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn create_fallback_device(pool: &sqlx::PgPool, tenant_id: &str, name: &str) -> String {
    let tenant_id = uuid::Uuid::parse_str(tenant_id).unwrap();
    let profile_dao = tb_dao::DeviceProfileDao::new(pool.clone());
    let profile_id = match profile_dao.find_default(tenant_id).await.unwrap() {
        Some(profile) => profile.id,
        None => {
            profile_dao
                .save(&tb_dao::DeviceProfile {
                    id: uuid::Uuid::now_v7(),
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
                .unwrap()
                .id
        }
    };
    tb_dao::DeviceDao::new(pool.clone())
        .save(&tb_dao::Device {
            id: uuid::Uuid::now_v7(),
            created_time: 0,
            tenant_id,
            customer_id: None,
            device_profile_id: profile_id,
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
        .unwrap()
        .id
        .to_string()
}

async fn save_telemetry_and_attribute(pool: &sqlx::PgPool, device_id: &str) {
    let entity_id = uuid::Uuid::parse_str(device_id).unwrap();
    let keys = tb_dao::KeyDictionaryDao::new(pool.clone());
    tb_dao::TimeseriesDao::new(pool.clone(), keys.clone())
        .save(
            entity_id,
            &[tb_common::TsKvEntry {
                ts: 1,
                key: "temperature".into(),
                value: tb_common::KvValue::Long(42),
            }],
        )
        .await
        .unwrap();
    tb_dao::AttributesDao::new(pool.clone(), keys)
        .save(
            entity_id,
            tb_common::AttributeScope::ServerScope,
            &[tb_common::KvEntry {
                key: "region".into(),
                value: tb_common::KvValue::Str("eu".into()),
            }],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn edqs_proxy_post_send_error_falls_back_to_postgres() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let edqs_addr = listener.local_addr().unwrap();
    drop(listener);
    unsafe {
        std::env::set_var("TB_EDQS_URL", format!("http://{edqs_addr}"));
    }

    let (base, pool) = start_server_with_pool().await;
    let client = reqwest::Client::new();
    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "Fallback POST T", "post@t.io").await;
    let device_id = create_fallback_device(&pool, &tenant_id, "Fallback Device").await;
    save_telemetry_and_attribute(&pool, &device_id).await;

    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&token))
        .json(&json!({
            "tenantId": uuid::Uuid::now_v7(),
            "entityFilter": {"type": "ENTITY_IDS", "ids": [device_id]},
            "keyFilters": [{"key": "temperature", "source": "LATEST", "predicate": {"op": "GT", "value": 40}}],
            "fields": [
                {"key": "name", "source": "ENTITY_FIELD"},
                {"key": "temperature", "source": "LATEST"},
                {"key": "region", "source": "SERVER"}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"][0]["entityId"], device_id);
    assert_eq!(body["data"][0]["values"]["name"], "Fallback Device");
    assert_eq!(body["data"][0]["values"]["temperature"], 42);
    assert_eq!(body["data"][0]["values"]["region"], "eu");
}

#[tokio::test]
async fn edqs_proxy_get_unavailable_falls_back_with_filter_and_pagination() {
    let _guard = INTEGRATION_LOCK.lock().await;
    unsafe {
        std::env::set_var(
            "TB_EDQS_URL",
            edqs_returning(StatusCode::SERVICE_UNAVAILABLE).await,
        );
    }

    let (base, pool) = start_server_with_pool().await;
    let client = reqwest::Client::new();
    let (token, tenant_id) = setup_tenant_admin(&client, &base, "Fallback GET T", "get@t.io").await;
    let alpha_one = create_fallback_device(&pool, &tenant_id, "Alpha One").await;
    let alpha_two = create_fallback_device(&pool, &tenant_id, "Alpha Two").await;
    create_fallback_device(&pool, &tenant_id, "Beta Device").await;

    let first = client
        .get(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&token))
        .query(&[
            ("entityType", "DEVICE"),
            ("nameContains", "alpha"),
            ("page", "0"),
            ("pageSize", "1"),
            ("sortBy", "name"),
            ("sortOrder", "DESC"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first: Value = first.json().await.unwrap();
    assert_eq!(first["totalElements"], 2);
    assert_eq!(first["totalPages"], 2);
    assert_eq!(first["hasNext"], true);
    assert_eq!(first["data"].as_array().unwrap().len(), 1);
    assert_eq!(first["data"][0]["entityId"], alpha_two);

    let second = client
        .get(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&token))
        .query(&[
            ("entityType", "DEVICE"),
            ("nameContains", "alpha"),
            ("page", "1"),
            ("pageSize", "1"),
            ("sortBy", "name"),
            ("sortOrder", "DESC"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second: Value = second.json().await.unwrap();
    assert_eq!(second["hasNext"], false);
    assert_eq!(second["data"][0]["entityId"], alpha_one);
}

#[tokio::test]
async fn edqs_proxy_fallback_does_not_load_cross_tenant_entity_ids() {
    let _guard = INTEGRATION_LOCK.lock().await;
    unsafe {
        std::env::set_var("TB_EDQS_URL", edqs_returning(StatusCode::BAD_GATEWAY).await);
    }

    let (base, pool) = start_server_with_pool().await;
    let client = reqwest::Client::new();
    let (token, tenant_id) =
        setup_tenant_admin(&client, &base, "Fallback Tenant A", "a@t.io").await;
    let (_, other_tenant_id) =
        setup_tenant_admin(&client, &base, "Fallback Tenant B", "b@t.io").await;
    let own_id = create_fallback_device(&pool, &tenant_id, "Own Device").await;
    let other_id = create_fallback_device(&pool, &other_tenant_id, "Other Device").await;
    save_telemetry_and_attribute(&pool, &other_id).await;

    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&token))
        .json(&json!({
            "entityFilter": {"type": "ENTITY_IDS", "ids": [own_id, other_id]},
            "fields": [
                {"key": "name", "source": "ENTITY_FIELD"},
                {"key": "temperature", "source": "LATEST"}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0]["entityId"], own_id);
    assert_eq!(body["data"][0]["values"]["name"], "Own Device");
    assert!(body["data"][0]["values"].get("temperature").is_none());
}

#[tokio::test]
async fn edqs_proxy_non_unavailable_response_is_not_replaced_by_fallback() {
    let _guard = INTEGRATION_LOCK.lock().await;
    unsafe {
        std::env::set_var("TB_EDQS_URL", edqs_returning(StatusCode::BAD_REQUEST).await);
    }

    let (base, pool) = start_server_with_pool().await;
    let client = reqwest::Client::new();
    let (token, tenant_id) = setup_tenant_admin(&client, &base, "No Fallback T", "none@t.io").await;
    create_fallback_device(&pool, &tenant_id, "Database Device").await;

    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&token))
        .json(&json!({"entityFilter": {"type": "ALL"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!({"source": "edqs"})
    );
}

#[tokio::test]
async fn edqs_customer_scope_is_enforced_during_postgres_fallback() {
    let _guard = INTEGRATION_LOCK.lock().await;
    unsafe {
        std::env::set_var("TB_EDQS_URL", edqs_returning(StatusCode::BAD_GATEWAY).await);
    }

    let (base, pool) = start_server_with_pool().await;
    let client = reqwest::Client::new();
    let (admin_token, tenant_id) = setup_tenant_admin(
        &client,
        &base,
        "Fallback Customers",
        "fallback.admin@edqs.io",
    )
    .await;
    let (customer_a, customer_a_token) = create_customer_user(
        &client,
        &base,
        &admin_token,
        "Fallback Customer A",
        "fallback.a@edqs.io",
    )
    .await;
    let (customer_b, _) = create_customer_user(
        &client,
        &base,
        &admin_token,
        "Fallback Customer B",
        "fallback.b@edqs.io",
    )
    .await;
    let own_id = create_fallback_device(&pool, &tenant_id, "Fallback Own").await;
    let other_id = create_fallback_device(&pool, &tenant_id, "Fallback Other").await;
    assign_device(&client, &base, &admin_token, &own_id, &customer_a).await;
    assign_device(&client, &base, &admin_token, &other_id, &customer_b).await;
    save_telemetry_and_attribute(&pool, &own_id).await;
    save_telemetry_and_attribute(&pool, &other_id).await;

    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&customer_a_token))
        .json(&json!({
            "customerId": customer_b,
            "entityFilter": {"type": "ENTITY_IDS", "ids": [own_id, other_id]},
            "fields": [
                {"key": "name", "source": "ENTITY_FIELD"},
                {"key": "temperature", "source": "LATEST"},
                {"key": "region", "source": "SERVER"}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"][0]["entityId"], own_id);
    assert_eq!(body["data"][0]["values"]["temperature"], 42);
    assert_eq!(body["data"][0]["values"]["region"], "eu");

    let resp = client
        .get(format!("{base}/api/edqs/query/entityData"))
        .header("Authorization", bearer(&customer_a_token))
        .query(&[
            ("customerId", customer_b.as_str()),
            ("entityType", "DEVICE"),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["totalElements"], 1);
    assert_eq!(body["data"][0]["entityId"], own_id);

    let resp = client
        .post(format!("{base}/api/user"))
        .header("Authorization", bearer(&admin_token))
        .json(&json!({
            "email": "unassigned@edqs.io",
            "authority": "CUSTOMER_USER",
            "password": "customer"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let unassigned = login(&client, &base, "unassigned@edqs.io", "customer").await;
    let resp = client
        .post(format!("{base}/api/edqs/query/entityData"))
        .header(
            "Authorization",
            bearer(unassigned["token"].as_str().unwrap()),
        )
        .json(&json!({"entityFilter": {"type": "ALL"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
