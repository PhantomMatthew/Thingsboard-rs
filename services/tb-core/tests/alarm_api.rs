//! Phase 5 告警 REST 集成测试：DAO 直接造告警 → 列表/详情 → ack → clear → 状态机负例。
//! 与 rest_api.rs 共用 thingsboard_test 库，本文件只放一个 #[tokio::test] 避免并发 DROP DATABASE。

mod common;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use tb_common::{EntityType, TbResult};
use tb_dao::models::{Alarm, AlarmSeverity, AlarmStatus};
use tb_dao::{AlarmDao, PageLink};
use uuid::Uuid;

use common::{bearer, create_device, setup_tenant_admin, start_server_with_pool};

async fn insert_alarm(
    pool: &PgPool,
    tenant_id: Uuid,
    device_id: Uuid,
    alarm_type: &str,
) -> TbResult<Alarm> {
    let now = chrono::Utc::now().timestamp_millis();
    let alarm = Alarm {
        id: Uuid::now_v7(),
        created_time: now,
        tenant_id,
        originator_id: device_id,
        originator_type: EntityType::Device,
        r#type: alarm_type.into(),
        severity: AlarmSeverity::Critical,
        status: AlarmStatus::ActiveUnack,
        start_ts: now,
        end_ts: now,
        ack_ts: None,
        clear_ts: None,
        details: Some(json!({"cause": "test"})),
        propagate: false,
        propagate_to_owner: false,
        propagate_to_tenant: false,
        propagate_relation_types: None,
        customer_id: None,
        assignee_id: None,
        assign_ts: None,
    };
    AlarmDao::new(pool.clone()).save(&alarm).await
}

#[tokio::test]
async fn alarm_api_end_to_end() {
    let (base, pool) = start_server_with_pool().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (tenant_token, tenant_id) =
        setup_tenant_admin(&client, &base, "Alarm Tenant", "t.admin@alarm.io").await;
    let device_id = create_device(&client, &base, &tenant_token, "Alarm Device").await;
    let tenant_uuid = Uuid::parse_str(&tenant_id).unwrap();
    let device_uuid = Uuid::parse_str(&device_id).unwrap();

    let a1 = insert_alarm(&pool, tenant_uuid, device_uuid, "HighTemperature")
        .await
        .unwrap();
    let a2 = insert_alarm(&pool, tenant_uuid, device_uuid, "LowBattery")
        .await
        .unwrap();

    // GET /api/alarm/{id}
    let resp = client
        .get(format!("{base}/api/alarm/{}", a1.id))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let alarm: Value = resp.json().await.unwrap();
    assert_eq!(alarm["type"], "HighTemperature");
    assert_eq!(alarm["status"], "ACTIVE_UNACK");
    assert_eq!(alarm["severity"], "CRITICAL");

    // 未知 id → 404
    let resp = client
        .get(format!("{base}/api/alarm/{}", Uuid::now_v7()))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // GET /api/alarms/DEVICE/{id}：分页 + status 过滤
    let resp = client
        .get(format!(
            "{base}/api/alarms/DEVICE/{device_id}?pageSize=10&sortProperty=createdTime&sortOrder=ASC"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 2);
    assert_eq!(page["data"][0]["type"], "HighTemperature");

    let resp = client
        .get(format!(
            "{base}/api/alarms/DEVICE/{device_id}?status=ACTIVE_ACK"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 0);

    // startTs 过滤（未来起点 → 空）
    let future = chrono::Utc::now().timestamp_millis() + 3_600_000;
    let resp = client
        .get(format!(
            "{base}/api/alarms/DEVICE/{device_id}?startTs={future}"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 0);

    // GET /api/alarms（租户级）
    let resp = client
        .get(format!("{base}/api/alarms?pageSize=10"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 2);

    // ack：ACTIVE_UNACK → ACTIVE_ACK，幂等再 ack
    let resp = client
        .post(format!("{base}/api/alarm/{}/ack", a1.id))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let alarm: Value = resp.json().await.unwrap();
    assert_eq!(alarm["status"], "ACTIVE_ACK");
    assert!(alarm["ackTs"].as_i64().unwrap() > 0);

    let resp = client
        .post(format!("{base}/api/alarm/{}/ack", a1.id))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // clear：ACTIVE_ACK → CLEARED_ACK
    let resp = client
        .post(format!("{base}/api/alarm/{}/clear", a1.id))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let alarm: Value = resp.json().await.unwrap();
    assert_eq!(alarm["status"], "CLEARED_ACK");
    assert!(alarm["clearTs"].as_i64().unwrap() > 0);

    // 状态机负例：已 CLEARED 不可 ack → 400
    let resp = client
        .post(format!("{base}/api/alarm/{}/ack", a1.id))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // clear 幂等
    let resp = client
        .post(format!("{base}/api/alarm/{}/clear", a1.id))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 状态过滤复核：CLEARED_ACK 命中 1 条，ACTIVE_UNACK 剩 1 条
    let resp = client
        .get(format!(
            "{base}/api/alarms/DEVICE/{device_id}?status=CLEARED_ACK"
        ))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    assert_eq!(page["totalElements"], 1);

    // DAO 层负例复核（a2 仍 ACTIVE_UNACK，clear 后 ack 直接报错）
    let dao = AlarmDao::new(pool.clone());
    dao.clear(a2.id).await.unwrap();
    assert!(dao.ack(a2.id).await.is_err());
    let page = dao
        .find_by_tenant(
            tenant_uuid,
            Some(device_uuid),
            None,
            None,
            None,
            &PageLink::new(0, 10),
        )
        .await
        .unwrap();
    assert_eq!(page.total_elements, 2);
}
