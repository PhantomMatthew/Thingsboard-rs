//! Phase 9b 计算字段：REST 遥测/属性保存触发 + 规则链（Kafka 链路）落库触发。
//! 单测试文件单 #[tokio::test]，走 INTEGRATION_LOCK（共享 Kafka）。

mod common;

use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use common::{
    INTEGRATION_LOCK, bearer, poll_until, setup_tenant_admin, start_servers_with_rule_engine,
};

fn cf_config_c_to_f() -> Value {
    json!({
        "arguments": {"tempC": {"source": "TELEMETRY", "key": "temperature"}},
        "expression": "tempC * 9 / 5 + 32",
        "output": {"type": "TELEMETRY", "key": "tempF"},
    })
}

#[tokio::test]
async fn calculated_field_end_to_end() {
    let _guard = INTEGRATION_LOCK.lock().await;
    let (base, transport) = start_servers_with_rule_engine().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (tenant_token, _tenant_id) =
        setup_tenant_admin(&client, &base, "CF Tenant", "t.admin@cf.io").await;

    // 设备（带固定 access token，供 transport 路径用）
    let resp = client
        .post(format!("{base}/api/device"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({ "name": "cf-device", "accessToken": "CF_TOKEN" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let device_id = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let ts_base = format!("{base}/api/telemetry/DEVICE/{device_id}");

    // 1. REST CRUD：非法 configuration → 400；合法 → 200；GET/列表/按实体查询
    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"entityId": device_id, "name": "bad", "configuration": {"foo": 1}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"entityId": device_id, "name": "cToF", "configuration": cf_config_c_to_f()}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cf: Value = resp.json().await.unwrap();
    let cf_id = cf["id"].as_str().unwrap().to_string();

    let resp = client
        .get(format!("{base}/api/calculatedField/{cf_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.json::<Value>().await.unwrap()["name"], "cToF");

    let resp = client
        .get(format!("{base}/api/calculatedFields?pageSize=10&page=0"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap()["totalElements"], 1);

    let resp = client
        .get(format!(
            "{base}/api/entity/DEVICE/{device_id}/calculatedFields"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap().as_array().unwrap().len(), 1);

    // 2. REST 遥测触发：temperature=25 → tempF=77（latest 读回）
    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"temperature": 25}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    poll_until(Duration::from_secs(10), async || {
        let resp = client
            .get(format!("{ts_base}/values/timeseries?keys=tempF"))
            .header("Authorization", bearer(&tenant_token))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
            && resp.json::<Value>().await.unwrap()["tempF"][0]["value"] == json!(77)
    })
    .await;

    // 3. 参数缺失不算：CF 需要 temperature + humidity，仅发 temperature → 无输出；
    //    补发 humidity 后 tempC 从 latest 补齐 → tempHumSum=40
    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "entityId": device_id,
            "name": "sum",
            "configuration": {
                "arguments": {
                    "tempC": {"source": "TELEMETRY", "key": "temperature"},
                    "hum": {"source": "TELEMETRY", "key": "humidity"},
                },
                "expression": "tempC + hum",
                "output": {"type": "TELEMETRY", "key": "tempHumSum"},
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"temperature": 30}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // 等 CF 异步任务充分执行后确认无输出
    tokio::time::sleep(Duration::from_secs(2)).await;
    let resp = client
        .get(format!("{ts_base}/values/timeseries?keys=tempHumSum"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap(), json!({}));

    // temperature=30 触发了 cToF：tempF 更新为 86
    poll_until(Duration::from_secs(5), async || {
        let resp = client
            .get(format!("{ts_base}/values/timeseries?keys=tempF"))
            .header("Authorization", bearer(&tenant_token))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
            && resp.json::<Value>().await.unwrap()["tempF"][0]["value"] == json!(86)
    })
    .await;

    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"humidity": 10}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    poll_until(Duration::from_secs(10), async || {
        let resp = client
            .get(format!("{ts_base}/values/timeseries?keys=tempHumSum"))
            .header("Authorization", bearer(&tenant_token))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
            && resp.json::<Value>().await.unwrap()["tempHumSum"][0]["value"] == json!(40)
    })
    .await;

    // 4. SERVER_ATTR 输出：temperature>50 → server 属性 overheated=true
    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "entityId": device_id,
            "name": "overheat",
            "configuration": {
                "arguments": {"tempC": {"source": "TELEMETRY", "key": "temperature"}},
                "expression": "tempC > 50",
                "output": {"type": "SERVER_ATTR", "key": "overheated"},
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .post(format!("{ts_base}/timeseries"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"temperature": 60}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    poll_until(Duration::from_secs(10), async || {
        let resp = client
            .get(format!(
                "{ts_base}/values/attributes/SERVER_SCOPE?keys=overheated"
            ))
            .header("Authorization", bearer(&tenant_token))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
            && resp.json::<Value>().await.unwrap()
                == json!([{"key": "overheated", "value": true}])
    })
    .await;

    // 5. 规则链路径：transport 上行 → Kafka → 根链 save_timeseries → 触发 CF
    //    （首轮可能撞上 consumer group 冷启动 → 500，轮询重试）
    poll_until(Duration::from_secs(30), async || {
        let resp = client
            .post(format!("{transport}/api/v1/CF_TOKEN/telemetry"))
            .json(&json!({"temperature": 100}))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
    })
    .await;
    poll_until(Duration::from_secs(30), async || {
        let resp = client
            .get(format!("{ts_base}/values/timeseries?keys=tempF"))
            .header("Authorization", bearer(&tenant_token))
            .send()
            .await
            .unwrap();
        resp.status() == StatusCode::OK
            && resp.json::<Value>().await.unwrap()["tempF"][0]["value"] == json!(212)
    })
    .await;

    // 6. 更新 + 删除：改表达式为恒 0 → 更新生效；删除后 GET 404
    let resp = client
        .post(format!("{base}/api/calculatedField"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "id": cf_id,
            "entityId": device_id,
            "name": "cToF",
            "configuration": {
                "arguments": {"tempC": {"source": "TELEMETRY", "key": "temperature"}},
                "expression": "0",
                "output": {"type": "TELEMETRY", "key": "tempF"},
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{base}/api/calculatedField/{cf_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Value>().await.unwrap()["configuration"]["expression"],
        "0"
    );

    let resp = client
        .delete(format!("{base}/api/calculatedField/{cf_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{base}/api/calculatedField/{cf_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
