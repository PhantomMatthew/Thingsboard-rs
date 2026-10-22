//! tb-rule-engine 端到端：专属测试库 + 种子租户（自动带根链）+ 独立 consumer group，
//! 直连 Kafka producer 发 ToRuleEngineMsg，轮询 DB 验证落库/告警。

use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_common::msg::msg_type;
use tb_dao::models::{RuleNode, Tenant};
use tb_dao::{RuleChainDao, TenantDao, TimeseriesDao, migrate};
use tb_proto::ruleengine::{TbMsgProto, ToRuleEngineMsg};
use tb_queue::topics::TbTopic;
use tb_queue::{KafkaQueueProducer, QueueProducer};
use tb_rule_engine::seed;
use uuid::Uuid;

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
const KAFKA_BROKERS: &str = "localhost:29092";

async fn poll_until(timeout: Duration, mut f: impl AsyncFnMut() -> bool) {
    let start = std::time::Instant::now();
    loop {
        if f().await {
            return;
        }
        assert!(start.elapsed() < timeout, "poll_until timed out after {timeout:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn send_telemetry(producer: &KafkaQueueProducer, tenant_id: Uuid, device_id: Uuid, data: serde_json::Value) {
    let msg = ToRuleEngineMsg {
        tenant_id: tenant_id.to_string(),
        tb_msg: Some(TbMsgProto {
            id: Uuid::now_v7().to_string(),
            queue_name: "Main".to_string(),
            r#type: msg_type::POST_TELEMETRY_REQUEST.to_string(),
            originator_id: device_id.to_string(),
            originator_type: tb_proto::common::EntityType::Device as i32,
            tenant_id: tenant_id.to_string(),
            customer_id: String::new(),
            data_json: data.to_string(),
            metadata: Default::default(),
            rule_chain_id: String::new(),
            rule_node_id: String::new(),
            ts: chrono::Utc::now().timestamp_millis(),
        }),
    };
    producer
        .send(
            &TbTopic::rule_engine("Main"),
            &device_id.to_string(),
            Bytes::from(msg.encode_to_vec()),
        )
        .await
        .unwrap();
}

fn make_node(chain_id: Uuid, node_type: &str, name: &str, config: serde_json::Value) -> RuleNode {
    RuleNode {
        id: Uuid::now_v7(),
        created_time: 0,
        rule_chain_id: chain_id,
        r#type: node_type.into(),
        name: name.into(),
        debug_mode: false,
        singleton_mode: false,
        queue_name: None,
        configuration: Some(config),
        additional_info: None,
    }
}

#[tokio::test]
async fn rule_engine_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("tb_rule_engine=debug,tb_rule_engine_server=debug,tb_queue=debug")
        .try_init();
    let run_id = &Uuid::now_v7().simple().to_string()[20..];
    let db_name = format!("tb_re_test_{run_id}");
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
        .await
        .unwrap();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query(&format!("CREATE DATABASE {db_name}"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;

    let test_url = format!("postgres://thingsboard:thingsboard@localhost:5433/{db_name}");
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&test_url)
        .await
        .unwrap();
    migrate(&pool).await.unwrap();

    // 种子租户（自动带根链）
    let tenant = Tenant {
        id: Uuid::now_v7(),
        created_time: 0,
        title: format!("re-e2e-{run_id}"),
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
    };
    TenantDao::new(pool.clone()).save(&tenant).await.unwrap();
    let chain = seed::create_default_root_chain(&pool, tenant.id).await.unwrap();

    let _engine = tb_rule_engine_server::start_rule_engine_for_group(
        KAFKA_BROKERS,
        pool.clone(),
        "Main",
        &format!("tb-rule-engine-e2e-{run_id}"),
        Duration::from_millis(500),
    )
    .await;
    // 等 consumer group join
    tokio::time::sleep(Duration::from_secs(2)).await;

    let producer = KafkaQueueProducer::new(KAFKA_BROKERS, "re-e2e-producer").unwrap();
    let ts_dao = TimeseriesDao::new(
        pool.clone(),
        tb_dao::KeyDictionaryDao::new(pool.clone()),
    );

    // 1. 默认根链：POST_TELEMETRY_REQUEST → save_timeseries → ts_kv_latest 有值
    // （consumer group join 完成前发的消息会被 latest offset 跳过，故轮询窗口内重发）
    let device_id = Uuid::now_v7();
    poll_until(Duration::from_secs(15), || async {
        send_telemetry(&producer, tenant.id, device_id, json!({"ts": 1000, "values": {"temperature": 25}}))
            .await;
        !ts_dao.find_latest(device_id, None).await.unwrap().is_empty()
    })
    .await;

    // 2. 改规则链：switch → filter.script(temperature > 50) --True--> create_alarm(HighTemp/CRITICAL)
    let chain_dao = RuleChainDao::new(pool.clone());
    let switch = make_node(chain.id, "filter.msg_type_switch", "switch", json!({}));
    let filter = make_node(
        chain.id,
        "filter.script",
        "hot filter",
        json!({"script": "msg.temperature > 50"}),
    );
    let alarm_node = make_node(
        chain.id,
        "action.create_alarm",
        "high temp alarm",
        json!({"alarm_type": "HighTemp", "severity": "CRITICAL", "propagate": true}),
    );
    let nodes = vec![switch.clone(), filter.clone(), alarm_node.clone()];
    chain_dao
        .save_nodes(
            chain.id,
            &nodes,
            &[
                (switch.id, filter.id, msg_type::POST_TELEMETRY_REQUEST.to_string()),
                (filter.id, alarm_node.id, "True".to_string()),
            ],
            Some(switch.id),
        )
        .await
        .unwrap();
    // 等链缓存过期（TTL 500ms）
    tokio::time::sleep(Duration::from_secs(1)).await;

    // temperature=80 → 产生 ACTIVE_UNACK 告警（扁平 data，重发幂等：create 后为 update，行数恒为 1）
    let hot_device = Uuid::now_v7();
    poll_until(Duration::from_secs(15), || async {
        send_telemetry(&producer, tenant.id, hot_device, json!({"temperature": 80}))
            .await;
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM alarm WHERE originator_id = $1 AND type = 'HighTemp' \
             AND severity = 'CRITICAL' AND status = 'ACTIVE_UNACK'",
        )
        .bind(hot_device)
        .fetch_one(&pool)
        .await
        .unwrap();
        count == 1
    })
    .await;

    // temperature=10 → filter False，无新告警
    let cold_device = Uuid::now_v7();
    send_telemetry(&producer, tenant.id, cold_device, json!({"temperature": 10}))
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM alarm WHERE originator_id = $1")
        .bind(cold_device)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "cold telemetry must not create alarm");

    // 再发一条 hot：已 ACTIVE → Updated，不产生第二行
    send_telemetry(&producer, tenant.id, hot_device, json!({"temperature": 90}))
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM alarm WHERE originator_id = $1")
        .bind(hot_device)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "active alarm must be updated, not duplicated");
}
