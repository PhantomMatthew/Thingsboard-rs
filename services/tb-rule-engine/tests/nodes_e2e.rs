//! Phase 7 服务级 E2E：专属测试库 + 独立 queue（tb_rule_engine.<queue>，避免与其他 E2E 串扰）
//! + 独立 consumer group。
//! - nodes_composition_e2e：switch → filter.script → enrichment.originator_fields →
//!   transform.script → action.math → create_alarm；再换 clear 路径验证 clear_alarm。
//! - debug_events_rest_e2e：debug_mode 节点经引擎产生 IN/OUT 事件，tb-core REST 可查到。
//! - dlq_e2e：script 错误无 Failure 边 → tb_rule_engine.dlq。
//! - checkpoint_resume_e2e：checkpoint requeue 后引擎从 rule_node_id 恢复执行。

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_common::msg::msg_type;
use tb_dao::models::{Device, DeviceProfile, RuleNode, Tenant};
use tb_dao::{
    AlarmDao, DeviceDao, DeviceProfileDao, KeyDictionaryDao, RuleChainDao, TenantDao,
    TimeseriesDao, migrate,
};
use tb_proto::ruleengine::{TbMsgProto, ToRuleEngineMsg};
use tb_queue::topics::TbTopic;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer, StreamQueueConsumer};
use tb_rule_engine::seed;
use uuid::Uuid;

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
const KAFKA_BROKERS: &str = "localhost:29092";

fn run_id() -> String {
    Uuid::now_v7().simple().to_string()[20..].to_string()
}

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

async fn fresh_db() -> (PgPool, String) {
    let run = run_id();
    let db_name = format!("tb_re7_test_{run}");
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
    let url = format!("postgres://thingsboard:thingsboard@localhost:5433/{db_name}");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap();
    migrate(&pool).await.unwrap();
    (pool, run)
}

async fn make_tenant(pool: &PgPool, run: &str) -> Tenant {
    let t = Tenant {
        id: Uuid::now_v7(),
        created_time: 0,
        title: format!("re7-{run}"),
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
    TenantDao::new(pool.clone()).save(&t).await.unwrap();
    t
}

async fn make_device(pool: &PgPool, tenant_id: Uuid, run: &str) -> Device {
    let profile = DeviceProfile {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        name: format!("p-{run}"),
        description: None,
        is_default: false,
        transport_type: "DEFAULT".into(),
        provision_type: "DISABLED".into(),
        profile_data: json!({}),
        firmware_id: None,
        software_id: None,
    };
    DeviceProfileDao::new(pool.clone()).save(&profile).await.unwrap();
    let d = Device {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        customer_id: None,
        device_profile_id: profile.id,
        name: format!("dev-{run}"),
        r#type: Some("sensor".into()),
        label: None,
        external_id: None,
        firmware_id: None,
        software_id: None,
        additional_info: None,
        device_data: json!({}),
    };
    DeviceDao::new(pool.clone()).save(&d).await.unwrap();
    d
}

fn make_node(chain_id: Uuid, node_type: &str, name: &str, config: Value, debug: bool) -> RuleNode {
    RuleNode {
        id: Uuid::now_v7(),
        created_time: 0,
        rule_chain_id: chain_id,
        r#type: node_type.into(),
        name: name.into(),
        debug_mode: debug,
        singleton_mode: false,
        queue_name: None,
        configuration: Some(config),
        additional_info: None,
    }
}

async fn send_telemetry(
    producer: &KafkaQueueProducer,
    queue: &str,
    tenant_id: Uuid,
    device_id: Uuid,
    data: Value,
) {
    let msg = ToRuleEngineMsg {
        tenant_id: tenant_id.to_string(),
        tb_msg: Some(TbMsgProto {
            id: Uuid::now_v7().to_string(),
            queue_name: queue.to_string(),
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
            &TbTopic::rule_engine(queue),
            &device_id.to_string(),
            Bytes::from(msg.encode_to_vec()),
        )
        .await
        .unwrap();
}

async fn start_engine(pool: PgPool, queue: &str, run: &str, test: &str) -> tokio::task::JoinHandle<()> {
    tb_rule_engine_server::start_rule_engine_for_group(
        KAFKA_BROKERS,
        pool,
        queue,
        &format!("re7-{test}-{run}"),
        Duration::from_millis(500),
    )
    .await
}

#[tokio::test]
async fn nodes_composition_e2e() {
    let (pool, run) = fresh_db().await;
    let tenant = make_tenant(&pool, &run).await;
    let device = make_device(&pool, tenant.id, &run).await;
    let chain = seed::create_default_root_chain(&pool, tenant.id).await.unwrap();
    let queue = format!("Re7Comp{run}");
    let _engine = start_engine(pool.clone(), &queue, &run, "comp").await;
    let producer = KafkaQueueProducer::new(KAFKA_BROKERS, "re7-comp-producer").unwrap();
    let alarm_dao = AlarmDao::new(pool.clone());
    let chain_dao = RuleChainDao::new(pool.clone());

    // switch → filter.script(>50) --True--> originator_fields --Success--> transform.script
    //   --Success--> math(MULT temperature*factor) --Success--> create_alarm
    let switch = make_node(chain.id, "filter.msg_type_switch", "switch", json!({}), false);
    let filter = make_node(chain.id, "filter.script", "hot", json!({"script": "msg.temperature > 50"}), false);
    let enrich = make_node(
        chain.id,
        "enrichment.originator_fields",
        "fields",
        json!({"fields": ["name", "type"]}),
        false,
    );
    let transform = make_node(
        chain.id,
        "transform.script",
        "factor",
        json!({"script": "msg.factor = 2; #{msg: msg}"}),
        false,
    );
    let math = make_node(
        chain.id,
        "action.math",
        "doubled",
        json!({"operation": "MULT", "fields": ["temperature", "factor"], "result_key": "doubled"}),
        false,
    );
    let alarm_node = make_node(
        chain.id,
        "action.create_alarm",
        "alarm",
        json!({
            "alarm_type": "HighTemp",
            "severity": "CRITICAL",
            "details_script": "#{device: metadata.name, dtype: metadata.type, doubled: msg.doubled}",
        }),
        false,
    );
    chain_dao
        .save_nodes(
            chain.id,
            &[switch.clone(), filter.clone(), enrich.clone(), transform.clone(), math.clone(), alarm_node.clone()],
            &[
                (switch.id, filter.id, msg_type::POST_TELEMETRY_REQUEST.to_string()),
                (filter.id, enrich.id, "True".to_string()),
                (enrich.id, transform.id, "Success".to_string()),
                (transform.id, math.id, "Success".to_string()),
                (math.id, alarm_node.id, "Success".to_string()),
            ],
            Some(switch.id),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 告警创建：details 里可见 enrichment 的 metadata 与 math 的计算结果
    poll_until(Duration::from_secs(20), || async {
        send_telemetry(&producer, &queue, tenant.id, device.id, json!({"temperature": 80})).await;
        match alarm_dao
            .find_latest_by_originator_and_type(device.id, "HighTemp")
            .await
            .unwrap()
        {
            Some(a) => {
                a.status == tb_dao::models::AlarmStatus::ActiveUnack
                    && a.details
                        == Some(json!({
                            "device": device.name,
                            "dtype": "sensor",
                            "doubled": 160,
                        }))
            }
            None => false,
        }
    })
    .await;

    // 换 clear 路径：switch → filter.script(<50) --True--> clear_alarm
    let cold_filter = make_node(chain.id, "filter.script", "cold", json!({"script": "msg.temperature < 50"}), false);
    let clear_node = make_node(chain.id, "action.clear_alarm", "clear", json!({"alarm_type": "HighTemp"}), false);
    chain_dao
        .save_nodes(
            chain.id,
            &[switch.clone(), cold_filter.clone(), clear_node.clone()],
            &[
                (switch.id, cold_filter.id, msg_type::POST_TELEMETRY_REQUEST.to_string()),
                (cold_filter.id, clear_node.id, "True".to_string()),
            ],
            Some(switch.id),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    poll_until(Duration::from_secs(20), || async {
        send_telemetry(&producer, &queue, tenant.id, device.id, json!({"temperature": 10})).await;
        alarm_dao
            .find_latest_by_originator_and_type(device.id, "HighTemp")
            .await
            .unwrap()
            .is_some_and(|a| a.status == tb_dao::models::AlarmStatus::ClearedUnack)
    })
    .await;
}

#[tokio::test]
async fn dlq_e2e() {
    let (pool, run) = fresh_db().await;
    let tenant = make_tenant(&pool, &run).await;
    let device_id = Uuid::now_v7();
    let chain = seed::create_default_root_chain(&pool, tenant.id).await.unwrap();
    let chain_dao = RuleChainDao::new(pool.clone());

    // switch → filter.script（编译错误）-- 无 Failure 边 → DLQ
    let switch = make_node(chain.id, "filter.msg_type_switch", "switch", json!({}), false);
    let bad = make_node(chain.id, "filter.script", "bad", json!({"script": "msg. >"}), false);
    chain_dao
        .save_nodes(
            chain.id,
            &[switch.clone(), bad.clone()],
            &[(switch.id, bad.id, msg_type::POST_TELEMETRY_REQUEST.to_string())],
            Some(switch.id),
        )
        .await
        .unwrap();

    let queue = format!("Re7Dlq{run}");
    let _engine = start_engine(pool.clone(), &queue, &run, "dlq").await;
    let producer = KafkaQueueProducer::new(KAFKA_BROKERS, "re7-dlq-producer").unwrap();

    let group = format!("re7-dlq-verify-{run}");
    let mut consumer =
        StreamQueueConsumer::new(KAFKA_BROKERS, &group, &[TbTopic::RULE_ENGINE_DLQ]).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let chain_id = chain.id;
    let start = std::time::Instant::now();
    loop {
        send_telemetry(&producer, &queue, tenant.id, device_id, json!({"x": 1})).await;
        let hit = match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(m)) => {
                let Ok(to_re) = ToRuleEngineMsg::decode(m.payload) else { panic!("bad dlq payload") };
                let Some(p) = to_re.tb_msg else { panic!("dlq payload without tb_msg") };
                p.metadata.get("chainId") == Some(&chain_id.to_string())
                    && p.metadata.get("fromNodeId") == Some(&bad.id.to_string())
                    && p.metadata.contains_key("error")
            }
            _ => false,
        };
        if hit {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(30), "DLQ message not observed");
    }
}

#[tokio::test]
async fn checkpoint_resume_e2e() {
    let (pool, run) = fresh_db().await;
    let tenant = make_tenant(&pool, &run).await;
    let device_id = Uuid::now_v7();
    let chain = seed::create_default_root_chain(&pool, tenant.id).await.unwrap();
    let chain_dao = RuleChainDao::new(pool.clone());

    // switch → checkpoint --Success--> save_timeseries
    let switch = make_node(chain.id, "filter.msg_type_switch", "switch", json!({}), false);
    let checkpoint = make_node(chain.id, "flow.checkpoint", "cp", json!({}), false);
    let save = make_node(chain.id, "action.save_timeseries", "save", json!({}), false);
    chain_dao
        .save_nodes(
            chain.id,
            &[switch.clone(), checkpoint.clone(), save.clone()],
            &[
                (switch.id, checkpoint.id, msg_type::POST_TELEMETRY_REQUEST.to_string()),
                (checkpoint.id, save.id, "Success".to_string()),
            ],
            Some(switch.id),
        )
        .await
        .unwrap();

    let queue = format!("Re7Cp{run}");
    let _engine = start_engine(pool.clone(), &queue, &run, "cp").await;
    let producer = KafkaQueueProducer::new(KAFKA_BROKERS, "re7-cp-producer").unwrap();
    let ts_dao = TimeseriesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));

    // requeue 后由同一引擎 handle_at 从 save 节点恢复 → ts_kv_latest 落库
    poll_until(Duration::from_secs(30), || async {
        send_telemetry(&producer, &queue, tenant.id, device_id, json!({"ts": 5, "values": {"temperature": 21}}))
            .await;
        !ts_dao.find_latest(device_id, None).await.unwrap().is_empty()
    })
    .await;
}

#[tokio::test]
async fn debug_events_rest_e2e() {
    let (pool, run) = fresh_db().await;
    let queue = format!("Re7Dbg{run}");

    // tb-core REST（同库）
    let factory = Arc::new(tb_auth::JwtTokenFactory::new("test-secret", 900, 604800));
    let state = tb_core::AppState::new(pool.clone(), factory);
    tb_core::seed::seed(&state).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, tb_core::build_router(state)).await.unwrap();
    });
    let base = format!("http://{addr}");
    let client = reqwest::Client::builder().no_proxy().build().unwrap();

    // sysadmin 建租户（自动种子根链）+ tenant admin
    let login = |email: &str, password: &str| {
        let client = client.clone();
        let base = base.clone();
        let (email, password) = (email.to_string(), password.to_string());
        async move {
            let resp: Value = client
                .post(format!("{base}/api/auth/login"))
                .json(&json!({"username": email, "password": password}))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            resp["token"].as_str().unwrap().to_string()
        }
    };
    let sys_token = login("sysadmin@thingsboard.org", "sysadmin").await;
    let resp = client
        .post(format!("{base}/api/tenant"))
        .header("Authorization", format!("Bearer {sys_token}"))
        .json(&json!({"title": format!("dbg-{run}")}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let tenant: Value = resp.json().await.unwrap();
    let tenant_id = tenant["id"].as_str().unwrap().to_string();
    let admin_email = format!("t.admin.{run}@dbg.io");
    let resp = client
        .post(format!("{base}/api/user"))
        .header("Authorization", format!("Bearer {sys_token}"))
        .json(&json!({
            "tenantId": tenant_id, "email": admin_email,
            "authority": "TENANT_ADMIN", "password": "tenant",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let token = login(&admin_email, "tenant").await;

    // 根链换成 switch → log(debug_mode=true)
    let resp = client
        .get(format!("{base}/api/ruleChains?pageSize=100"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    let root = page["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "Root Rule Chain")
        .unwrap();
    let chain_id = root["id"].as_str().unwrap().to_string();

    let put_body = json!({
        "nodes": [
            {"type": "filter.msg_type_switch", "name": "switch", "configuration": {}},
            {"type": "action.log", "name": "dbglog", "debugMode": true, "configuration": {"prefix": "dbg"}},
        ],
        "edges": [{"fromIndex": 0, "toIndex": 1, "type": "POST_TELEMETRY_REQUEST"}],
        "firstNodeIndex": 0,
    });
    let resp = client
        .put(format!("{base}/api/ruleChain/{chain_id}/nodes"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&put_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let meta: Value = resp.json().await.unwrap();
    let log_node = meta["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["type"] == "action.log")
        .unwrap();
    let log_node_id = log_node["id"].as_str().unwrap().to_string();

    // 引擎 + 遥测
    let _engine = start_engine(pool.clone(), &queue, &run, "dbg").await;
    let producer = KafkaQueueProducer::new(KAFKA_BROKERS, "re7-dbg-producer").unwrap();
    let tenant_uuid = Uuid::parse_str(&tenant_id).unwrap();
    let device_id = Uuid::now_v7();

    // REST 轮询 debug 事件（IN+OUT）
    let client2 = client.clone();
    let base2 = base.clone();
    let token2 = token.clone();
    let node_id = log_node_id.clone();
    poll_until(Duration::from_secs(30), || async {
        send_telemetry(&producer, &queue, tenant_uuid, device_id, json!({"temperature": 1})).await;
        let resp = client2
            .get(format!("{base2}/api/ruleNode/{node_id}/events?pageSize=10"))
            .header("Authorization", format!("Bearer {token2}"))
            .send()
            .await
            .unwrap();
        if resp.status() != reqwest::StatusCode::OK {
            return false;
        }
        let page: Value = resp.json().await.unwrap();
        let Some(events) = page["data"].as_array() else { return false };
        let types: Vec<&str> = events.iter().filter_map(|e| e["eventType"].as_str()).collect();
        types.contains(&"IN") && types.contains(&"OUT")
    })
    .await;

    // 其他租户的 node id 查询 → 404/403
    let resp = client
        .get(format!("{base}/api/ruleNode/{}/events", Uuid::now_v7()))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}
