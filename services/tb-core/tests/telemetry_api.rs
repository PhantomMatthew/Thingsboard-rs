//! Phase 2 遥测/属性 REST API 集成测试。
//! 注意：与 rest_api.rs 共用 thingsboard_test 库，本文件只放一个 #[tokio::test] 避免并发 DROP DATABASE。

mod common;

use redis::AsyncCommands;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use common::{bearer, create_device, login, setup_tenant_admin, start_server};

#[tokio::test]
async fn telemetry_api_end_to_end() {
    let base = start_server().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (tenant_token, _tenant_id) =
        setup_tenant_admin(&client, &base, "Telemetry Tenant", "t.admin@telemetry.io").await;
    let device_id = create_device(&client, &base, &tenant_token, "Telemetry Device").await;
    let ts_base = format!("{base}/api/telemetry/DEVICE/{device_id}");

    // 1. POST timeseries：带 ts 数组形式 + 无 ts 映射形式
    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!([
            {"ts": 1000, "values": {"temp": 10, "humidity": 50}},
            {"ts": 2000, "values": {"temp": 20}},
            {"ts": 11000, "values": {"temp": 30}},
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"status": "online"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. GET latest：无 keys 全量；带 keys 子集
    let resp = client
        .get(format!("{ts_base}/values/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let latest: Value = resp.json().await.unwrap();
    assert_eq!(latest["temp"], json!([{"ts": 11000, "value": 30}]));
    assert_eq!(latest["humidity"], json!([{"ts": 1000, "value": 50}]));
    assert_eq!(latest["status"][0]["value"], "online");

    let resp = client
        .get(format!("{ts_base}/values/timeseries?keys=temp,humidity"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let latest: Value = resp.json().await.unwrap();
    assert!(latest.get("status").is_none());
    assert_eq!(latest["temp"][0]["value"], 30);

    // 3. GET keys/timeseries
    let resp = client
        .get(format!("{ts_base}/keys/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!(["humidity", "status", "temp"])
    );

    // 4. GET history：顺序与 limit
    let resp = client
        .get(format!(
            "{ts_base}/values/timeseries?keys=temp&startTs=0&endTs=20000&orderBy=ASC"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let history: Value = resp.json().await.unwrap();
    let points = history["temp"].as_array().unwrap();
    assert_eq!(points.len(), 3);
    assert_eq!(points[0], json!({"ts": 1000, "value": 10}));
    assert_eq!(points[2], json!({"ts": 11000, "value": 30}));

    let resp = client
        .get(format!(
            "{ts_base}/values/timeseries?keys=temp&startTs=0&endTs=20000&orderBy=ASC&limit=2"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["temp"][1]["ts"], 2000);

    let resp = client
        .get(format!(
            "{ts_base}/values/timeseries?keys=temp&startTs=0&endTs=20000&orderBy=DESC&limit=1"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap()["temp"][0],
        json!({"ts": 11000, "value": 30})
    );

    // 5. GET aggregated：AVG/COUNT/SUM 按 10s 分桶
    let resp = client
        .get(format!(
            "{ts_base}/values/timeseries?keys=temp&startTs=0&endTs=20000&agg=AVG&interval=10000"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let agg: Value = resp.json().await.unwrap();
    assert_eq!(
        agg["temp"],
        json!([{"ts": 0, "value": 15.0}, {"ts": 10000, "value": 30.0}])
    );

    let resp = client
        .get(format!(
            "{ts_base}/values/timeseries?keys=temp&startTs=0&endTs=20000&agg=COUNT&interval=10000"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap()["temp"],
        json!([{"ts": 0, "value": 2}, {"ts": 10000, "value": 1}])
    );

    let resp = client
        .get(format!(
            "{ts_base}/values/timeseries?keys=temp&startTs=0&endTs=20000&agg=SUM&interval=10000"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap()["temp"],
        json!([{"ts": 0, "value": 30.0}, {"ts": 10000, "value": 30.0}])
    );

    // 6. DELETE：范围删除 + 全量删除
    let resp = client
        .delete(format!(
            "{ts_base}/timeseries?keys=temp&startTs=0&endTs=10000"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.json::<Value>().await.unwrap()["deleted"], 2);

    let resp = client
        .get(format!(
            "{ts_base}/values/timeseries?keys=temp&startTs=0&endTs=20000&orderBy=ASC"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap()["temp"],
        json!([{"ts": 11000, "value": 30}])
    );

    let resp = client
        .delete(format!("{ts_base}/timeseries?keys=status"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{ts_base}/values/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    let latest: Value = resp.json().await.unwrap();
    assert!(latest.get("status").is_none());
    assert_eq!(latest["temp"][0]["value"], 30);

    // 7. Redis 缓存路径：注入假值后全量 latest 查询应命中缓存优先
    let redis_client = redis::Client::open("redis://localhost:16379").unwrap();
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let cache_key = format!("tslatest:{device_id}");
    let _: usize = redis_conn
        .hset(&cache_key, "temp", r#"{"ts":999,"value":12345}"#)
        .await
        .unwrap();
    let resp = client
        .get(format!("{ts_base}/values/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap()["temp"],
        json!([{"ts": 999, "value": 12345}]),
        "second full latest read should be served from Redis cache"
    );

    // 带时间范围的删除必须保守失效整个 latest HASH，不能继续返回旧缓存。
    let resp = client
        .delete(format!(
            "{ts_base}/timeseries?keys=temp&startTs=0&endTs=10000"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let exists: bool = redis_conn.exists(&cache_key).await.unwrap();
    assert!(!exists, "range deletion must invalidate latest cache");
    let _: usize = redis_conn.del(&cache_key).await.unwrap();

    // 8. attributes：SERVER_SCOPE / SHARED_SCOPE 读写删
    let resp = client
        .post(format!("{ts_base}/attributes/SERVER_SCOPE"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"model": "X1", "firmware": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .post(format!("{ts_base}/attributes/SHARED_SCOPE"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"sharedConf": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // CLIENT_SCOPE 不允许服务端写入
    let resp = client
        .post(format!("{ts_base}/attributes/CLIENT_SCOPE"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 指定 keys / 全部
    let resp = client
        .get(format!(
            "{ts_base}/values/attributes/SERVER_SCOPE?keys=model"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!([{"key": "model", "value": "X1"}])
    );

    let resp = client
        .get(format!("{ts_base}/values/attributes/SERVER_SCOPE"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!([{"key": "firmware", "value": 2}, {"key": "model", "value": "X1"}])
    );

    // 跨 scope 分组
    let resp = client
        .get(format!("{ts_base}/values/attributes"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let all: Value = resp.json().await.unwrap();
    assert_eq!(all["client"], json!({}));
    assert_eq!(all["server"], json!({"firmware": 2, "model": "X1"}));
    assert_eq!(all["shared"], json!({"sharedConf": true}));

    // DELETE 后再查
    let resp = client
        .delete(format!("{ts_base}/attributes/SERVER_SCOPE?keys=model"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.json::<Value>().await.unwrap()["deleted"], 1);

    let resp = client
        .get(format!("{ts_base}/values/attributes/SERVER_SCOPE"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap(),
        json!([{"key": "firmware", "value": 2}])
    );

    // 9. 权限负例：CUSTOMER_USER 对未分配实体 403；不存在实体 404
    let resp = client
        .post(format!("{base}/api/customer"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({ "title": "Telemetry Customer" }))
        .send()
        .await
        .unwrap();
    let customer: Value = resp.json().await.unwrap();
    let customer_id = customer["id"].as_str().unwrap().to_string();

    let resp = client
        .post(format!("{base}/api/user"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "email": "c.user@telemetry.io",
            "authority": "CUSTOMER_USER",
            "customerId": customer_id,
            "password": "customer",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tokens = login(&client, &base, "c.user@telemetry.io", "customer").await;
    let customer_token = tokens["token"].as_str().unwrap().to_string();

    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&customer_token))
        .json(&json!({"temp": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let resp = client
        .get(format!("{ts_base}/values/timeseries"))
        .header("Authorization", bearer(&customer_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    let missing = uuid::Uuid::now_v7();
    let resp = client
        .get(format!(
            "{base}/api/telemetry/DEVICE/{missing}/values/timeseries"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 不支持的实体类型 → 400
    let resp = client
        .get(format!(
            "{base}/api/telemetry/TENANT/{missing}/values/timeseries"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}
