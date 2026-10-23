//! tb-rule-engine 集成测试：真实 PostgreSQL（localhost:5433，同 tb-dao 测试基建）。
//! 数据全部用随机 UUID，不依赖外键（ts_kv/attribute_kv/alarm 的 entity/tenant 列无 FK）。

use std::collections::HashMap;

use serde_json::{Value, json};
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{AttributeScope, EntityId, EntityType};
use tb_dao::models::{AlarmStatus, RuleNode, Tenant};
use tb_dao::{AlarmDao, AttributesDao, KeyDictionaryDao, TenantDao, TimeseriesDao, migrate};
use tb_queue::KafkaQueueProducer;
use tb_rule_engine::nodes::{
    CreateAlarmNode, MsgTypeSwitchNode, SaveAttributesNode, SaveTimeseriesNode, ScriptFilterNode,
    ScriptTransformNode,
};
use tb_rule_engine::{ChainLoader, Node, NodeContext, RuleChainState, RuleEngineService, seed};
use uuid::Uuid;

async fn setup() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://thingsboard:thingsboard@localhost:5433/thingsboard".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    pool
}

fn ctx(pool: &sqlx::PgPool) -> NodeContext {
    let producer = KafkaQueueProducer::new("localhost:29092", "tb-rule-engine-test").unwrap();
    NodeContext::new(pool.clone(), producer)
}

fn telemetry_msg(data: Value) -> TbMsg {
    TbMsg::new(
        msg_type::POST_TELEMETRY_REQUEST,
        EntityId::new(EntityType::Device),
        Uuid::now_v7(),
        data,
        HashMap::new(),
    )
}

fn make_node(chain_id: Uuid, node_type: &str, config: Value) -> RuleNode {
    RuleNode {
        id: Uuid::now_v7(),
        created_time: 0,
        rule_chain_id: chain_id,
        r#type: node_type.into(),
        name: node_type.into(),
        debug_mode: false,
        singleton_mode: false,
        queue_name: None,
        configuration: Some(config),
        additional_info: None,
    }
}

#[tokio::test]
async fn save_timeseries_ts_values_and_flat() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let dao = TimeseriesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));
    let node = SaveTimeseriesNode::new(&json!({})).unwrap();

    let msg = telemetry_msg(json!({"ts": 42000, "values": {"temperature": 25}}));
    let originator = msg.originator.id;
    let outs = node.on_msg(&ctx, msg).await;
    assert_eq!(outs[0].0, "Success");
    let latest = dao.find_latest(originator, None).await.unwrap();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].key, "temperature");
    assert_eq!(latest[0].ts, 42000);

    // 扁平 {k:v}：ts 取 msg.ts
    let msg = telemetry_msg(json!({"humidity": 60}));
    let originator = msg.originator.id;
    let outs = node.on_msg(&ctx, msg).await;
    assert_eq!(outs[0].0, "Success");
    let latest = dao.find_latest(originator, None).await.unwrap();
    assert_eq!(latest[0].key, "humidity");
}

#[tokio::test]
async fn save_attributes_scopes() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let dao = AttributesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));

    let node = SaveAttributesNode::new(&json!({})).unwrap();
    let msg = telemetry_msg(json!({"model": "X1"}));
    let originator = msg.originator.id;
    let outs = node.on_msg(&ctx, msg).await;
    assert_eq!(outs[0].0, "Success");
    let found = dao
        .find(originator, AttributeScope::ClientScope, None)
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].key, "model");

    // data 内 {scope, kv} 覆盖 config scope
    let node = SaveAttributesNode::new(&json!({"scope": "CLIENT_SCOPE"})).unwrap();
    let msg = telemetry_msg(json!({"scope": "SERVER_SCOPE", "kv": {"firmware": 2}}));
    let originator = msg.originator.id;
    node.on_msg(&ctx, msg).await;
    let found = dao
        .find(originator, AttributeScope::ServerScope, None)
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].key, "firmware");
}

#[tokio::test]
async fn script_filter_relations() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let node = ScriptFilterNode::new(&json!({"script": "msg.temperature > 50"})).unwrap();

    let outs = node
        .on_msg(&ctx, telemetry_msg(json!({"temperature": 80})))
        .await;
    assert_eq!(outs[0].0, "True");
    let outs = node
        .on_msg(&ctx, telemetry_msg(json!({"temperature": 10})))
        .await;
    assert_eq!(outs[0].0, "False");

    // 编译错误 → Failure
    let bad = ScriptFilterNode::new(&json!({"script": "msg. >"})).unwrap();
    let outs = bad.on_msg(&ctx, telemetry_msg(json!({}))).await;
    assert_eq!(outs[0].0, "Failure");
}

#[tokio::test]
async fn script_transform_rewrites_msg() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let node = ScriptTransformNode::new(&json!({
        "script": "msg.temperature = msg.temperature * 2; #{msg: msg, metadata: #{\"src\": \"t\"}}"
    }))
    .unwrap();
    let outs = node
        .on_msg(&ctx, telemetry_msg(json!({"temperature": 21})))
        .await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.data, json!({"temperature": 42}));
    assert_eq!(outs[0].1.metadata["src"], "t");

    let bad = ScriptTransformNode::new(&json!({"script": "msg + 1"})).unwrap();
    let outs = bad.on_msg(&ctx, telemetry_msg(json!({}))).await;
    assert_eq!(outs[0].0, "Failure");
}

#[tokio::test]
async fn msg_type_switch_routes_by_type() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let node = MsgTypeSwitchNode;
    let msg = telemetry_msg(json!({}));
    let outs = node.on_msg(&ctx, msg).await;
    assert_eq!(outs[0].0, msg_type::POST_TELEMETRY_REQUEST);
}

#[tokio::test]
async fn create_alarm_creates_then_updates() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let alarm_dao = AlarmDao::new(pool.clone());
    let node = CreateAlarmNode::new(&json!({
        "alarm_type": "HighTemp",
        "severity": "CRITICAL",
        "propagate": true,
        "details_script": "#{value: msg.temperature}",
    }))
    .unwrap();

    let msg = telemetry_msg(json!({"temperature": 80}));
    let originator = msg.originator.id;
    let tenant = msg.tenant_id;

    let outs = node.on_msg(&ctx, msg.clone()).await;
    assert_eq!(outs[0].0, "Created");
    assert_eq!(outs[0].1.msg_type, msg_type::ALARM);
    let alarm = alarm_dao
        .find_latest_by_originator_and_type(originator, "HighTemp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alarm.status, AlarmStatus::ActiveUnack);
    assert_eq!(alarm.severity, tb_dao::models::AlarmSeverity::Critical);
    assert_eq!(alarm.tenant_id, tenant);
    assert_eq!(alarm.details, Some(json!({"value": 80})));
    let first_end_ts = alarm.end_ts;

    // 同 originator+type 仍 ACTIVE → Updated，不产生新行
    let msg2 = TbMsg {
        data: json!({"temperature": 90}),
        ..msg
    };
    let outs = node.on_msg(&ctx, msg2).await;
    assert_eq!(outs[0].0, "Updated");
    let alarm = alarm_dao
        .find_latest_by_originator_and_type(originator, "HighTemp")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alarm.details, Some(json!({"value": 90})));
    assert!(alarm.end_ts >= first_end_ts);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM alarm WHERE originator_id = $1")
        .bind(originator)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn chain_execute_routes_and_drops() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let dao = TimeseriesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));
    let chain_id = Uuid::now_v7();

    let switch = make_node(chain_id, "filter.msg_type_switch", json!({}));
    let save = make_node(chain_id, "action.save_timeseries", json!({}));
    let state = RuleChainState {
        chain_id,
        nodes: [
            (
                switch.id,
                (
                    switch.clone(),
                    tb_rule_engine::registry::create_node(&switch.r#type, &json!({})).unwrap(),
                ),
            ),
            (
                save.id,
                (
                    save.clone(),
                    tb_rule_engine::registry::create_node(&save.r#type, &json!({})).unwrap(),
                ),
            ),
        ]
        .into_iter()
        .collect(),
        edges: [(
            switch.id,
            vec![(msg_type::POST_TELEMETRY_REQUEST.to_string(), save.id)],
        )]
        .into_iter()
        .collect(),
        first_node_id: Some(switch.id),
    };

    let msg = telemetry_msg(json!({"ts": 1, "values": {"temperature": 30}}));
    let originator = msg.originator.id;
    tb_rule_engine::chain::execute(&ctx, &state, switch.id, msg).await;
    let latest = dao.find_latest(originator, None).await.unwrap();
    assert_eq!(latest.len(), 1);

    // 无匹配边的类型被丢弃
    let msg = TbMsg::new(
        msg_type::CONNECT_EVENT,
        EntityId::new(EntityType::Device),
        Uuid::now_v7(),
        json!({}),
        HashMap::new(),
    );
    let originator = msg.originator.id;
    tb_rule_engine::chain::execute(&ctx, &state, switch.id, msg).await;
    assert!(dao.find_latest(originator, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn engine_default_chain_fallback_without_root_chain() {
    let pool = setup().await;
    let producer = KafkaQueueProducer::new("localhost:29092", "tb-rule-engine-test").unwrap();
    let engine = RuleEngineService::new(pool.clone(), producer);
    let dao = TimeseriesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));

    // 无根链租户：POST_TELEMETRY_REQUEST 仍落库
    let msg = telemetry_msg(json!({"ts": 7, "values": {"temperature": 18}}));
    let originator = msg.originator.id;
    engine.handle(msg).await.unwrap();
    let latest = dao.find_latest(originator, None).await.unwrap();
    assert_eq!(latest.len(), 1);

    // 其他类型丢弃
    let msg = TbMsg::new(
        msg_type::CONNECT_EVENT,
        EntityId::new(EntityType::Device),
        Uuid::now_v7(),
        json!({}),
        HashMap::new(),
    );
    let originator = msg.originator.id;
    engine.handle(msg).await.unwrap();
    assert!(dao.find_latest(originator, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn engine_loads_db_root_chain() {
    let pool = setup().await;
    let tenant = Tenant {
        id: Uuid::now_v7(),
        created_time: 0,
        title: format!("re-test-{}", Uuid::now_v7().simple()),
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
    let chain = seed::create_default_root_chain(&pool, tenant.id)
        .await
        .unwrap();
    assert!(chain.root);

    // 幂等
    let again = seed::create_default_root_chain(&pool, tenant.id)
        .await
        .unwrap();
    assert_eq!(again.id, chain.id);

    let producer = KafkaQueueProducer::new("localhost:29092", "tb-rule-engine-test").unwrap();
    let engine = RuleEngineService::new(pool.clone(), producer);
    let dao = TimeseriesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));
    let msg = TbMsg::new(
        msg_type::POST_TELEMETRY_REQUEST,
        EntityId::new(EntityType::Device),
        tenant.id,
        json!({"ts": 9, "values": {"temperature": 22}}),
        HashMap::new(),
    );
    let originator = msg.originator.id;
    engine.handle(msg).await.unwrap();
    assert_eq!(dao.find_latest(originator, None).await.unwrap().len(), 1);

    // ChainLoader 往返：switch + save_ts + save_attrs
    let state = ChainLoader::new(pool.clone())
        .load(chain.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.nodes.len(), 3);
    let first = state.first_node_id.unwrap();
    assert_eq!(state.edges[&first].len(), 2);

    TenantDao::new(pool.clone())
        .delete_by_id(tenant.id)
        .await
        .unwrap();
}
