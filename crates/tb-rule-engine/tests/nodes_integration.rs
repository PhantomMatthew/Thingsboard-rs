//! Phase 7 新节点集成测试：真实 PostgreSQL（localhost:5433）+ Kafka（localhost:29092）。
//! 纯逻辑节点（filter/math 等）走轻量 NodeContext；去重使用真实 PostgreSQL 持久化状态。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use serde_json::{Value, json};
use sqlx::PgPool;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{AttributeScope, EntityId, EntityType, KvEntry, KvValue};
use tb_dao::models::{
    AlarmStatus, Customer, Device, DeviceProfile, EntityRelation, RuleChain, RuleNode, Tenant,
};
use tb_dao::{
    AlarmDao, AttributesDao, CustomerDao, DeviceDao, DeviceProfileDao, KeyDictionaryDao,
    RelationDao, RuleChainDao, RuleNodeDebugEventDao, TenantDao, TimeseriesDao, migrate,
};
use tb_proto::core as core_pb;
use tb_proto::ruleengine::ToRuleEngineMsg;
use tb_queue::{
    KafkaQueueProducer, QueueConsumer, QueueError, QueueProducer, StreamQueueConsumer, TbTopic,
};
use tb_rule_engine::nodes::{
    ChangeOriginatorNode, ClearAlarmNode, CreateAlarmNode, DeduplicationNode, DelayNode,
    DeleteAttributesNode, DeleteKeysNode, GeoFilterNode, KafkaNode, KeyExistsFilterNode, MathNode,
    MsgTypeFilterNode, OriginatorAttributesNode, OriginatorFieldsNode, OriginatorTypeFilterNode,
    RelatedAttributesNode, RelationFilterNode, RenameKeysNode, RestCallNode, RpcOnewayNode,
    RuleChainNode, TenantCustomerFieldsNode,
};
use tb_rule_engine::{ChainLoader, Node, NodeContext, chain};
use uuid::Uuid;

async fn setup() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://thingsboard:thingsboard@localhost:5433/thingsboard".into());
    let pool = PgPool::connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    pool
}

fn ctx(pool: &PgPool) -> NodeContext {
    let producer = KafkaQueueProducer::new("localhost:29092", "tb-re-nodes-test").unwrap();
    NodeContext::new(pool.clone(), producer)
}

struct RecordingProducer {
    sent: Mutex<Vec<(String, Bytes)>>,
}

impl RecordingProducer {
    fn drain(&self) -> Vec<(String, Bytes)> {
        std::mem::take(&mut *self.sent.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl QueueProducer for RecordingProducer {
    async fn send(&self, topic: &str, _key: &str, payload: Bytes) -> Result<(), QueueError> {
        self.sent.lock().unwrap().push((topic.to_string(), payload));
        Ok(())
    }
}

fn memory_ctx() -> (NodeContext, Arc<RecordingProducer>) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://thingsboard:thingsboard@127.0.0.1:1/thingsboard")
        .unwrap();
    let producer = Arc::new(RecordingProducer {
        sent: Mutex::new(Vec::new()),
    });
    let queue: Arc<dyn QueueProducer> = producer.clone();
    (NodeContext::with_queue_producer(pool, queue), producer)
}

struct NotifyNode(tokio::sync::mpsc::UnboundedSender<Uuid>);

#[async_trait::async_trait]
impl Node for NotifyNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        self.0.send(msg.id).unwrap();
        Vec::new()
    }
}

fn msg(data: Value) -> TbMsg {
    TbMsg::new(
        msg_type::POST_TELEMETRY_REQUEST,
        EntityId::new(EntityType::Device),
        Uuid::now_v7(),
        data,
        HashMap::new(),
    )
}

fn uniq(prefix: &str) -> String {
    format!("{prefix}-{}", &Uuid::now_v7().simple().to_string()[20..])
}

async fn make_tenant(pool: &PgPool) -> Tenant {
    let t = Tenant {
        id: Uuid::now_v7(),
        created_time: 0,
        title: uniq("tenant"),
        region: None,
        country: None,
        state: None,
        city: None,
        address: None,
        address2: None,
        zip: None,
        phone: Some("12345".into()),
        email: Some(format!("{}@test.io", uniq("t"))),
        additional_info: None,
        tenant_profile_id: None,
    };
    TenantDao::new(pool.clone()).save(&t).await.unwrap()
}

async fn make_device(pool: &PgPool, tenant_id: Uuid) -> Device {
    let profile = DeviceProfile {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        name: uniq("profile"),
        description: None,
        is_default: false,
        transport_type: "DEFAULT".into(),
        provision_type: "DISABLED".into(),
        profile_data: json!({}),
        firmware_id: None,
        software_id: None,
    };
    DeviceProfileDao::new(pool.clone())
        .save(&profile)
        .await
        .unwrap();
    let d = Device {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        customer_id: None,
        device_profile_id: profile.id,
        name: uniq("dev"),
        r#type: Some("sensor".into()),
        label: Some("lab".into()),
        external_id: None,
        firmware_id: None,
        software_id: None,
        additional_info: None,
        device_data: json!({}),
    };
    DeviceDao::new(pool.clone()).save(&d).await.unwrap()
}

fn make_relation(from: EntityId, to: EntityId, relation_type: &str) -> EntityRelation {
    EntityRelation {
        from_id: from.id,
        from_type: from.entity_type,
        to_id: to.id,
        to_type: to.entity_type,
        relation_type_group: "COMMON".into(),
        relation_type: relation_type.into(),
        additional_info: None,
    }
}

fn make_chain_node(chain_id: Uuid, node_type: &str, config: Value, debug: bool) -> RuleNode {
    RuleNode {
        id: Uuid::now_v7(),
        created_time: 0,
        rule_chain_id: chain_id,
        r#type: node_type.into(),
        name: node_type.into(),
        debug_mode: debug,
        singleton_mode: false,
        queue_name: None,
        configuration: Some(config),
        additional_info: None,
    }
}

async fn save_chain(
    pool: &PgPool,
    tenant_id: Uuid,
    nodes: &[RuleNode],
    edges: &[(Uuid, Uuid, String)],
    first: Uuid,
) -> RuleChain {
    let dao = RuleChainDao::new(pool.clone());
    let chain = RuleChain {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        name: uniq("chain"),
        first_rule_node_id: None,
        root: false,
        debug_mode: false,
        configuration: None,
        additional_info: None,
    };
    let chain = dao.save(&chain).await.unwrap();
    dao.save_nodes(chain.id, nodes, edges, Some(first))
        .await
        .unwrap();
    chain
}

fn attrs_dao(pool: &PgPool) -> AttributesDao {
    AttributesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()))
}

// ---------- enrichment ----------

#[tokio::test]
async fn originator_attributes_to_metadata_and_msg() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let dao = attrs_dao(&pool);
    let device = Uuid::now_v7();
    dao.save(
        device,
        AttributeScope::ServerScope,
        &[
            KvEntry {
                key: "model".into(),
                value: KvValue::Str("X1".into()),
            },
            KvEntry {
                key: "threshold".into(),
                value: KvValue::Long(42),
            },
        ],
    )
    .await
    .unwrap();
    dao.save(
        device,
        AttributeScope::ClientScope,
        &[KvEntry {
            key: "clientKey".into(),
            value: KvValue::Bool(true),
        }],
    )
    .await
    .unwrap();

    // metadata 目标 + prefix
    let node = OriginatorAttributesNode::new(&json!({
        "server_keys": ["model", "threshold"],
        "target": "metadata",
        "prefix": "srv_",
    }))
    .unwrap();
    let mut m = msg(json!({"temperature": 1}));
    m.originator.id = device;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.metadata["srv_model"], "X1");
    assert_eq!(outs[0].1.metadata["srv_threshold"], "42");

    // msg 目标：合并进 data object
    let node = OriginatorAttributesNode::new(&json!({
        "server_keys": ["model"],
        "client_keys": ["clientKey"],
        "target": "msg",
    }))
    .unwrap();
    let mut m = msg(json!({"temperature": 1}));
    m.originator.id = device;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.data["model"], "X1");
    assert_eq!(outs[0].1.data["clientKey"], true);

    // msg 目标但 data 非 object → Failure
    let mut m = msg(json!([1, 2, 3]));
    m.originator.id = device;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Failure");
}

#[tokio::test]
async fn originator_fields_device_to_metadata() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let tenant = make_tenant(&pool).await;
    let device = make_device(&pool, tenant.id).await;

    let node = OriginatorFieldsNode::new(&json!({"fields": ["name", "type", "label"]})).unwrap();
    let mut m = msg(json!({}));
    m.originator.id = device.id;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.metadata["name"], device.name);
    assert_eq!(outs[0].1.metadata["type"], "sensor");
    assert_eq!(outs[0].1.metadata["label"], "lab");

    // 不存在的实体 → Failure
    let m = msg(json!({}));
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Failure");
}

#[tokio::test]
async fn related_attributes_from_relation_peer() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let dao = attrs_dao(&pool);
    let device = EntityId::new(EntityType::Device);
    let asset = EntityId::new(EntityType::Asset);
    RelationDao::new(pool.clone())
        .save(&make_relation(device, asset, "contains"))
        .await
        .unwrap();
    dao.save(
        asset.id,
        AttributeScope::ServerScope,
        &[KvEntry {
            key: "location".into(),
            value: KvValue::Str("warehouse-A".into()),
        }],
    )
    .await
    .unwrap();

    let node = RelatedAttributesNode::new(&json!({
        "relation_type": "contains",
        "direction": "FROM",
        "entity_types": ["ASSET"],
        "attr_keys": ["location"],
        "scope": "SERVER_SCOPE",
        "target": "metadata",
    }))
    .unwrap();
    let mut m = msg(json!({}));
    m.originator = device;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.metadata["location"], "warehouse-A");

    // 无匹配关系 → Failure
    let m = msg(json!({}));
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Failure");
}

#[tokio::test]
async fn tenant_customer_fields_to_metadata() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let tenant = make_tenant(&pool).await;
    let customer = Customer {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id: tenant.id,
        title: uniq("cust"),
        country: None,
        state: None,
        city: None,
        address: None,
        address2: None,
        zip: None,
        phone: None,
        email: Some("c@test.io".into()),
        additional_info: None,
    };
    CustomerDao::new(pool.clone())
        .save(&customer)
        .await
        .unwrap();

    let node = TenantCustomerFieldsNode::new(&json!({
        "tenant_fields": ["title", "email"],
        "customer_fields": ["title", "email"],
    }))
    .unwrap();
    let mut m = msg(json!({}));
    m.tenant_id = tenant.id;
    m.customer_id = Some(customer.id);
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.metadata["tenant_title"], tenant.title);
    assert_eq!(outs[0].1.metadata["tenant_email"], tenant.email.unwrap());
    assert_eq!(outs[0].1.metadata["customer_title"], customer.title);
    assert_eq!(outs[0].1.metadata["customer_email"], "c@test.io");

    // 无 customer 的消息：customer 字段跳过，tenant 字段照填
    let mut m = msg(json!({}));
    m.tenant_id = tenant.id;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert!(outs[0].1.metadata.contains_key("tenant_title"));
    assert!(!outs[0].1.metadata.contains_key("customer_title"));
}

// ---------- filter ----------

#[tokio::test]
async fn msg_type_and_originator_type_filters() {
    let pool = setup().await;
    let ctx = ctx(&pool);

    let node = MsgTypeFilterNode::new(&json!({"types": ["POST_TELEMETRY_REQUEST"]})).unwrap();
    assert_eq!(node.on_msg(&ctx, msg(json!({}))).await[0].0, "True");
    let m = TbMsg::new(
        msg_type::CONNECT_EVENT,
        EntityId::new(EntityType::Device),
        Uuid::now_v7(),
        json!({}),
        HashMap::new(),
    );
    assert_eq!(node.on_msg(&ctx, m).await[0].0, "False");

    let node = OriginatorTypeFilterNode::new(&json!({"types": ["DEVICE", "ASSET"]})).unwrap();
    assert_eq!(node.on_msg(&ctx, msg(json!({}))).await[0].0, "True");
    let m = TbMsg::new(
        "T",
        EntityId::new(EntityType::Customer),
        Uuid::now_v7(),
        json!({}),
        HashMap::new(),
    );
    assert_eq!(node.on_msg(&ctx, m).await[0].0, "False");
}

#[tokio::test]
async fn key_exists_filter() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let mut m = msg(json!({"a": 1}));
    m.metadata.insert("b".into(), "x".into());

    let any = KeyExistsFilterNode::new(&json!({"keys": ["a", "zz"], "check_all": false})).unwrap();
    assert_eq!(any.on_msg(&ctx, m.clone()).await[0].0, "True");
    let all = KeyExistsFilterNode::new(&json!({"keys": ["a", "b"], "check_all": true})).unwrap();
    assert_eq!(all.on_msg(&ctx, m.clone()).await[0].0, "True");
    let all = KeyExistsFilterNode::new(&json!({"keys": ["a", "zz"], "check_all": true})).unwrap();
    assert_eq!(all.on_msg(&ctx, m.clone()).await[0].0, "False");
}

#[tokio::test]
async fn relation_filter() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let device = EntityId::new(EntityType::Device);
    let asset = EntityId::new(EntityType::Asset);
    RelationDao::new(pool.clone())
        .save(&make_relation(device, asset, "contains"))
        .await
        .unwrap();

    let node = RelationFilterNode::new(&json!({
        "relation_type": "contains", "direction": "FROM", "entity_type": "ASSET",
    }))
    .unwrap();
    let mut m = msg(json!({}));
    m.originator = device;
    assert_eq!(node.on_msg(&ctx, m).await[0].0, "True");

    // entity_type 不匹配
    let node = RelationFilterNode::new(&json!({
        "relation_type": "contains", "direction": "FROM", "entity_type": "DEVICE",
    }))
    .unwrap();
    let mut m = msg(json!({}));
    m.originator = device;
    assert_eq!(node.on_msg(&ctx, m).await[0].0, "False");
}

#[tokio::test]
async fn geo_filter_circle_and_polygon() {
    let pool = setup().await;
    let ctx = ctx(&pool);

    let circle = GeoFilterNode::new(&json!({
        "latitude_key": "lat", "longitude_key": "lng",
        "circle": {"lat": 48.0, "lng": 11.0, "radius_m": 1000.0},
    }))
    .unwrap();
    assert_eq!(
        circle
            .on_msg(&ctx, msg(json!({"lat": 48.0, "lng": 11.001})))
            .await[0]
            .0,
        "True"
    );
    assert_eq!(
        circle
            .on_msg(&ctx, msg(json!({"lat": 49.0, "lng": 12.0})))
            .await[0]
            .0,
        "False"
    );
    // 缺坐标 → False
    assert_eq!(circle.on_msg(&ctx, msg(json!({}))).await[0].0, "False");

    let polygon = GeoFilterNode::new(&json!({
        "latitude_key": "lat", "longitude_key": "lng",
        "polygon": [[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0]],
    }))
    .unwrap();
    assert_eq!(
        polygon
            .on_msg(&ctx, msg(json!({"lat": 0.5, "lng": 0.5})))
            .await[0]
            .0,
        "True"
    );
    assert_eq!(
        polygon
            .on_msg(&ctx, msg(json!({"lat": 2.0, "lng": 2.0})))
            .await[0]
            .0,
        "False"
    );
    // 字符串坐标也接受
    assert_eq!(
        polygon
            .on_msg(&ctx, msg(json!({"lat": "0.5", "lng": "0.5"})))
            .await[0]
            .0,
        "True"
    );
}

// ---------- transform ----------

#[tokio::test]
async fn rename_and_delete_keys() {
    let pool = setup().await;
    let ctx = ctx(&pool);

    let rename =
        RenameKeysNode::new(&json!({"map": {"temp": "temperature"}, "target": "msg"})).unwrap();
    let outs = rename
        .on_msg(&ctx, msg(json!({"temp": 25, "keep": 1})))
        .await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.data, json!({"temperature": 25, "keep": 1}));

    let rename_md = RenameKeysNode::new(&json!({"map": {"a": "b"}, "target": "metadata"})).unwrap();
    let mut m = msg(json!({}));
    m.metadata.insert("a".into(), "1".into());
    let outs = rename_md.on_msg(&ctx, m).await;
    assert_eq!(outs[0].1.metadata["b"], "1");
    assert!(!outs[0].1.metadata.contains_key("a"));

    let delete = DeleteKeysNode::new(&json!({"keys": ["x"], "target": "msg"})).unwrap();
    let outs = delete.on_msg(&ctx, msg(json!({"x": 1, "y": 2}))).await;
    assert_eq!(outs[0].1.data, json!({"y": 2}));
}

#[tokio::test]
async fn change_originator_variants() {
    let pool = setup().await;
    let ctx = ctx(&pool);

    // TENANT
    let node = ChangeOriginatorNode::new(&json!({"to": "TENANT"})).unwrap();
    let m = msg(json!({}));
    let tenant_id = m.tenant_id;
    let prev = m.originator;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.originator.entity_type, EntityType::Tenant);
    assert_eq!(outs[0].1.originator.id, tenant_id);
    assert_eq!(outs[0].1.metadata["prevOriginatorId"], prev.id.to_string());
    assert_eq!(outs[0].1.metadata["prevOriginatorType"], "DEVICE");

    // CUSTOMER：无 customer → Failure；有 → 换
    let node = ChangeOriginatorNode::new(&json!({"to": "CUSTOMER"})).unwrap();
    let m = msg(json!({}));
    assert_eq!(node.on_msg(&ctx, m).await[0].0, "Failure");
    let mut m = msg(json!({}));
    m.customer_id = Some(Uuid::now_v7());
    let customer_id = m.customer_id.unwrap();
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].1.originator.entity_type, EntityType::Customer);
    assert_eq!(outs[0].1.originator.id, customer_id);

    // RELATED
    let device = EntityId::new(EntityType::Device);
    let asset = EntityId::new(EntityType::Asset);
    RelationDao::new(pool.clone())
        .save(&make_relation(device, asset, "contains"))
        .await
        .unwrap();
    let node = ChangeOriginatorNode::new(&json!({
        "to": "RELATED", "relation_type": "contains", "direction": "FROM",
    }))
    .unwrap();
    let mut m = msg(json!({}));
    m.originator = device;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.originator, asset);
}

// ---------- action ----------

#[tokio::test]
async fn clear_alarm_lifecycle() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let alarm_dao = AlarmDao::new(pool.clone());
    let create = CreateAlarmNode::new(&json!({
        "alarm_type": "Overheat", "severity": "MAJOR",
    }))
    .unwrap();
    let clear = ClearAlarmNode::new(&json!({
        "alarm_type": "Overheat",
        "details_script": "#{clearedBy: \"rule\"}",
    }))
    .unwrap();

    let m = msg(json!({"temperature": 90}));
    let originator = m.originator.id;
    create.on_msg(&ctx, m.clone()).await;

    let outs = clear.on_msg(&ctx, m.clone()).await;
    assert_eq!(outs[0].0, "Cleared");
    assert_eq!(outs[0].1.msg_type, msg_type::ALARM);
    let alarm = alarm_dao
        .find_latest_by_originator_and_type(originator, "Overheat")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(alarm.status, AlarmStatus::ClearedUnack);
    assert_eq!(alarm.details, Some(json!({"clearedBy": "rule"})));

    // 已无 ACTIVE → False
    let outs = clear.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "False");
}

#[tokio::test]
async fn delete_attributes_node() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let dao = attrs_dao(&pool);
    let device = Uuid::now_v7();
    dao.save(
        device,
        AttributeScope::ServerScope,
        &[
            KvEntry {
                key: "a".into(),
                value: KvValue::Long(1),
            },
            KvEntry {
                key: "b".into(),
                value: KvValue::Long(2),
            },
        ],
    )
    .await
    .unwrap();

    let node = DeleteAttributesNode::new(&json!({"scope": "SERVER_SCOPE", "keys": ["a"]})).unwrap();
    let mut m = msg(json!({}));
    m.originator.id = device;
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    let left = dao
        .find(device, AttributeScope::ServerScope, None)
        .await
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].key, "b");
}

#[tokio::test]
async fn delay_node_returns_without_sleeping() {
    let (ctx, _) = memory_ctx();
    let node = DelayNode::new(&json!({"delay_sec": 1})).unwrap();
    let start = std::time::Instant::now();
    let outs = node.on_msg(&ctx, msg(json!({}))).await;
    assert_eq!(outs[0].0, "Success");
    assert!(start.elapsed() < Duration::from_millis(100));
    DelayNode::new(&json!({"delay_sec": 9999})).unwrap();
}

#[test]
fn deduplication_node_validates_java_configuration() {
    DeduplicationNode::new(&json!({"window_sec": 60})).unwrap();
    DeduplicationNode::new(&json!({
        "interval": 1,
        "strategy": "ALL",
        "outMsgType": "DEDUP_ALL"
    }))
    .unwrap();
    assert!(DeduplicationNode::new(&json!({"interval": 0})).is_err());
    assert!(DeduplicationNode::new(&json!({"strategy": "ALL"})).is_err());
}

#[tokio::test]
async fn math_node_operations() {
    let pool = setup().await;
    let ctx = ctx(&pool);

    let add = MathNode::new(&json!({
        "operation": "ADD", "fields": ["a", "b"], "result_key": "sum",
    }))
    .unwrap();
    let outs = add.on_msg(&ctx, msg(json!({"a": 0.1, "b": 0.2}))).await;
    assert_eq!(outs[0].0, "Success");
    let sum = outs[0].1.data["sum"].as_f64().unwrap();
    assert!((sum - 0.3).abs() < 1e-9);

    let div = MathNode::new(&json!({
        "operation": "DIV", "fields": ["a", "b"], "result_key": "r", "round": 2,
    }))
    .unwrap();
    let outs = div.on_msg(&ctx, msg(json!({"a": 10, "b": 3}))).await;
    assert_eq!(outs[0].1.data["r"], json!(3.33));
    // 除零 → Failure
    let outs = div.on_msg(&ctx, msg(json!({"a": 1, "b": 0}))).await;
    assert_eq!(outs[0].0, "Failure");
    // 缺字段 → Failure
    let outs = div.on_msg(&ctx, msg(json!({"a": 1}))).await;
    assert_eq!(outs[0].0, "Failure");

    let max = MathNode::new(&json!({
        "operation": "MAX", "fields": ["a", "b", "c"], "result_key": "m",
    }))
    .unwrap();
    let outs = max.on_msg(&ctx, msg(json!({"a": 1, "b": 9, "c": 5}))).await;
    assert_eq!(outs[0].1.data["m"], json!(9));
}

// ---------- action.rpc_oneway / external ----------

/// 轮询消费 Kafka 直到 cond 命中；每轮间隔重发一次（auto.offset.reset=latest 可能错过首条）。
async fn poll_kafka(
    consumer: &mut StreamQueueConsumer,
    timeout: Duration,
    mut resend: impl AsyncFnMut(),
    mut cond: impl FnMut(&[u8]) -> bool,
) {
    let start = std::time::Instant::now();
    loop {
        resend().await;
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(m)) if cond(&m.payload) => return,
            // 新 topic 首消费常见 UnknownTopicOrPartition，等 broker 自动创建后重试。
            _ => {}
        }
        assert!(
            start.elapsed() < timeout,
            "poll_kafka timed out after {timeout:?}"
        );
    }
}

#[tokio::test]
async fn rpc_oneway_sends_to_core_topic() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let node = RpcOnewayNode::new(&json!({
        "method": "setGpio",
        "params_script": "#{pin: 4, value: 1}",
    }))
    .unwrap();

    let group = uniq("rpc-oneway-test");
    let mut consumer =
        StreamQueueConsumer::new("localhost:29092", &group, &[TbTopic::CORE]).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let m = msg(json!({"temperature": 1}));
    let device_id = m.originator.id;
    poll_kafka(
        &mut consumer,
        Duration::from_secs(20),
        || async {
            node.on_msg(&ctx, m.clone()).await;
        },
        |payload| {
            let Ok(to_core) = core_pb::ToCoreMsg::decode(payload) else {
                return false;
            };
            let Some(core_pb::to_core_msg::Msg::RpcRequest(rpc)) = to_core.msg else {
                return false;
            };
            rpc.device_id == device_id.to_string()
                && rpc.method == "setGpio"
                && rpc.params_json == json!({"pin": 4, "value": 1}).to_string().as_str()
        },
    )
    .await;

    // 非设备 originator → Failure
    let mut m = msg(json!({}));
    m.originator = EntityId::new(EntityType::Asset);
    assert_eq!(node.on_msg(&ctx, m).await[0].0, "Failure");
}

#[tokio::test]
async fn rest_call_against_local_server() {
    use axum::extract::Path;
    use axum::routing::{get, post};

    let pool = setup().await;
    let ctx = ctx(&pool);

    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(4);
    let followed_tx = tx.clone();
    let app = axum::Router::new()
        .route(
            "/ok/{dev}",
            post(move |Path(dev): Path<String>, body: String| {
                let tx = tx.clone();
                async move {
                    tx.try_send(format!("{dev}|{body}")).ok();
                    axum::http::StatusCode::OK
                }
            }),
        )
        .route(
            "/bad",
            post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .route(
            "/redirect",
            post(|| async {
                (
                    axum::http::StatusCode::FOUND,
                    [(axum::http::header::LOCATION, "/followed")],
                )
            }),
        )
        .route(
            "/followed",
            get(move || async move {
                followed_tx.try_send("followed".into()).ok();
                axum::http::StatusCode::OK
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let node = RestCallNode::new(&json!({
        "url": format!("http://{addr}/ok/{{{{metadata.dev}}}}"),
        "method": "POST",
        "headers": {"X-Source": "rule"},
        "body_script": "#{t: msg.temperature}",
    }))
    .unwrap();
    let mut m = msg(json!({"temperature": 25}));
    m.metadata.insert("dev".into(), "dev-42".into());
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Success");
    assert_eq!(outs[0].1.metadata["rest_call_status"], "200");
    let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, format!("dev-42|{}", json!({"t": 25})));

    let node = RestCallNode::new(&json!({
        "url": format!("http://{addr}/redirect"),
        "method": "POST",
    }))
    .unwrap();
    let outs = node.on_msg(&ctx, msg(json!({"x": 1}))).await;
    assert_eq!(outs[0].0, "Failure");
    assert_eq!(outs[0].1.metadata["rest_call_status"], "302");
    assert!(
        tokio::time::timeout(Duration::from_millis(250), rx.recv())
            .await
            .is_err(),
        "rest_call followed a redirect"
    );

    // 500 → Failure，状态码仍进 metadata
    let node = RestCallNode::new(&json!({
        "url": format!("http://{addr}/bad"),
        "method": "POST",
    }))
    .unwrap();
    let outs = node.on_msg(&ctx, msg(json!({"x": 1}))).await;
    assert_eq!(outs[0].0, "Failure");
    assert_eq!(outs[0].1.metadata["rest_call_status"], "500");

    // 不可达 → Failure
    let node = RestCallNode::new(&json!({"url": "http://127.0.0.1:1/x", "method": "GET"})).unwrap();
    let outs = node.on_msg(&ctx, msg(json!({}))).await;
    assert_eq!(outs[0].0, "Failure");
}

#[tokio::test]
async fn external_kafka_publishes_msg_json() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let topic = uniq("tb-ext-kafka-test");
    let node = KafkaNode::new(&json!({
        "topic": topic,
        "key_script": "metadata.k",
    }))
    .unwrap();

    let group = uniq("ext-kafka-verify");
    let mut consumer = StreamQueueConsumer::new("localhost:29092", &group, &[&topic]).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut m = msg(json!({"temperature": 7}));
    m.metadata.insert("k".into(), "key-1".into());
    let msg_id = m.id;
    poll_kafka(
        &mut consumer,
        Duration::from_secs(20),
        || async {
            node.on_msg(&ctx, m.clone()).await;
        },
        |payload| {
            let Ok(v) = serde_json::from_slice::<Value>(payload) else {
                return false;
            };
            v["id"] == json!(msg_id.to_string()) && v["data"] == json!({"temperature": 7})
        },
    )
    .await;
}

/// 需要本地 MQTT broker（localhost:1883），默认跳过。
#[tokio::test]
#[ignore = "requires local mqtt broker"]
async fn external_mqtt_publishes() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let node = tb_rule_engine::nodes::MqttNode::new(&json!({
        "host": "localhost", "port": 1883, "topic": "tb/test", "qos": 0,
    }))
    .unwrap();
    let outs = node.on_msg(&ctx, msg(json!({"temperature": 1}))).await;
    assert_eq!(outs[0].0, "Success");
}

// ---------- delayed requeue / checkpoint loop protection ----------

#[tokio::test]
async fn delay_does_not_block_following_message() {
    let (ctx, producer) = memory_ctx();
    let chain_id = Uuid::now_v7();
    let delay = make_chain_node(chain_id, "action.delay", json!({"delay_sec": 1}), false);
    let next = make_chain_node(chain_id, "action.log", json!({}), false);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let delay_impl =
        tb_rule_engine::registry::create_node(&delay.r#type, &json!({"delay_sec": 1})).unwrap();
    let next_impl: Arc<dyn Node> = Arc::new(NotifyNode(tx));
    let state = tb_rule_engine::RuleChainState {
        chain_id,
        nodes: [
            (delay.id, (delay.clone(), delay_impl)),
            (next.id, (next.clone(), next_impl)),
        ]
        .into_iter()
        .collect(),
        edges: [(delay.id, vec![("Success".to_string(), next.id)])]
            .into_iter()
            .collect(),
        first_node_id: Some(delay.id),
    };

    let mut delayed = msg(json!({"value": 1}));
    delayed.queue_name = "DelayTest".into();
    let delayed_id = delayed.id;
    let start = std::time::Instant::now();
    chain::execute(&ctx, &state, delay.id, delayed)
        .await
        .unwrap();
    assert!(start.elapsed() < Duration::from_millis(200));
    assert!(producer.drain().is_empty());

    let following_id = msg(json!({"value": 2})).id;
    let following = TbMsg {
        id: following_id,
        ..msg(json!({"value": 2}))
    };
    chain::execute(&ctx, &state, next.id, following)
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .unwrap(),
        Some(following_id)
    );
    assert!(producer.drain().is_empty());

    let deliveries = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let deliveries = producer.drain();
            if !deliveries.is_empty() {
                return deliveries;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, TbTopic::rule_engine("DelayTest"));
    let envelope = ToRuleEngineMsg::decode(deliveries[0].1.clone()).unwrap();
    let requeued = envelope.tb_msg.unwrap();
    assert_eq!(requeued.id, delayed_id.to_string());
    assert_eq!(requeued.rule_chain_id, chain_id.to_string());
    assert_eq!(requeued.rule_node_id, next.id.to_string());
    assert_eq!(requeued.metadata.get("_hops"), Some(&"1".to_string()));
}

#[tokio::test]
async fn checkpoint_loop_accumulates_hops_across_requeues_and_terminates_in_dlq() {
    let (ctx, producer) = memory_ctx();
    let chain_id = Uuid::now_v7();
    let checkpoint = make_chain_node(chain_id, "flow.checkpoint", json!({}), false);
    let implementation =
        tb_rule_engine::registry::create_node(&checkpoint.r#type, &json!({})).unwrap();
    let state = tb_rule_engine::RuleChainState {
        chain_id,
        nodes: [(checkpoint.id, (checkpoint.clone(), implementation))]
            .into_iter()
            .collect(),
        edges: [(checkpoint.id, vec![("Success".to_string(), checkpoint.id)])]
            .into_iter()
            .collect(),
        first_node_id: Some(checkpoint.id),
    };
    let mut current = msg(json!({"value": 1}));
    current.queue_name = "CheckpointLoop".into();
    let mut dead_letter = None;

    for _ in 0..=100 {
        chain::execute(&ctx, &state, checkpoint.id, current)
            .await
            .unwrap();
        let mut deliveries = producer.drain();
        assert_eq!(deliveries.len(), 1);
        let (topic, payload) = deliveries.remove(0);
        if topic == TbTopic::RULE_ENGINE_DLQ {
            dead_letter = Some(payload);
            break;
        }
        assert_eq!(topic, TbTopic::rule_engine("CheckpointLoop"));
        let envelope = ToRuleEngineMsg::decode(payload).unwrap();
        let requeued = envelope.tb_msg.unwrap();
        assert_eq!(requeued.rule_chain_id, chain_id.to_string());
        assert_eq!(requeued.rule_node_id, checkpoint.id.to_string());
        current = tb_proto::convert::tb_msg_from_proto(&requeued).unwrap();
    }

    let envelope = ToRuleEngineMsg::decode(dead_letter.expect("loop did not reach DLQ")).unwrap();
    let dlq = envelope.tb_msg.unwrap();
    assert_eq!(dlq.metadata.get("_hops"), Some(&"101".to_string()));
    assert!(
        dlq.metadata
            .get("error")
            .is_some_and(|error| error.contains("maximum rule chain hops"))
    );
    assert_eq!(
        dlq.metadata.get("fromNodeId"),
        Some(&checkpoint.id.to_string())
    );
}

#[tokio::test]
async fn checkpoint_without_success_edge_goes_to_dlq() {
    let (ctx, producer) = memory_ctx();
    let chain_id = Uuid::now_v7();
    let checkpoint = make_chain_node(chain_id, "flow.checkpoint", json!({}), false);
    let implementation =
        tb_rule_engine::registry::create_node(&checkpoint.r#type, &json!({})).unwrap();
    let state = tb_rule_engine::RuleChainState {
        chain_id,
        nodes: [(checkpoint.id, (checkpoint.clone(), implementation))]
            .into_iter()
            .collect(),
        edges: Default::default(),
        first_node_id: Some(checkpoint.id),
    };
    chain::execute(&ctx, &state, checkpoint.id, msg(json!({})))
        .await
        .unwrap();

    let deliveries = producer.drain();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, TbTopic::RULE_ENGINE_DLQ);
    let envelope = ToRuleEngineMsg::decode(deliveries[0].1.clone()).unwrap();
    let dlq = envelope.tb_msg.unwrap();
    assert!(
        dlq.metadata
            .get("error")
            .is_some_and(|error| error.contains("relation 'Success'"))
    );
}

// ---------- flow ----------

#[tokio::test]
async fn rule_chain_node_executes_target_chain() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let tenant = make_tenant(&pool).await;
    let ts_dao = TimeseriesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));

    // 目标链：单 save_timeseries 节点
    let save = make_chain_node(Uuid::nil(), "action.save_timeseries", json!({}), false);
    let chain_b = save_chain(&pool, tenant.id, std::slice::from_ref(&save), &[], save.id).await;

    // 主链：单 flow.rule_chain 节点
    let flow = make_chain_node(
        Uuid::nil(),
        "flow.rule_chain",
        json!({"rule_chain_id": chain_b.id}),
        false,
    );
    let chain_a = save_chain(&pool, tenant.id, std::slice::from_ref(&flow), &[], flow.id).await;

    let state = ChainLoader::new(pool.clone())
        .load(chain_a.id)
        .await
        .unwrap()
        .unwrap();
    let m = msg(json!({"ts": 1, "values": {"temperature": 33}}));
    let originator = m.originator.id;
    chain::execute(&ctx, &state, flow.id, m).await.unwrap();
    let latest = ts_dao.find_latest(originator, None).await.unwrap();
    assert_eq!(latest.len(), 1, "target chain should have saved telemetry");

    // 目标链不存在 → "Success"（错误处理边）
    let node = RuleChainNode::new(&json!({"rule_chain_id": Uuid::now_v7()})).unwrap();
    let outs = node.on_msg(&ctx, msg(json!({}))).await;
    assert_eq!(outs[0].0, "Success");

    // 深度超限 → Failure
    let node = RuleChainNode::new(&json!({"rule_chain_id": chain_b.id})).unwrap();
    let mut m = msg(json!({}));
    m.metadata.insert("_chainDepth".into(), "8".into());
    let outs = node.on_msg(&ctx, m).await;
    assert_eq!(outs[0].0, "Failure");
}

#[tokio::test]
async fn checkpoint_requeues_instead_of_local_forward() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let tenant = make_tenant(&pool).await;
    let ts_dao = TimeseriesDao::new(pool.clone(), KeyDictionaryDao::new(pool.clone()));

    let queue = uniq("CpTest");
    let checkpoint = make_chain_node(Uuid::nil(), "flow.checkpoint", json!({}), false);
    let save = make_chain_node(Uuid::nil(), "action.save_timeseries", json!({}), false);
    let chain_row = save_chain(
        &pool,
        tenant.id,
        &[checkpoint.clone(), save.clone()],
        &[(checkpoint.id, save.id, "Success".to_string())],
        checkpoint.id,
    )
    .await;

    let group = uniq("checkpoint-verify");
    let topic = TbTopic::rule_engine(&queue);
    let mut consumer = StreamQueueConsumer::new("localhost:29092", &group, &[&topic]).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let state = ChainLoader::new(pool.clone())
        .load(chain_row.id)
        .await
        .unwrap()
        .unwrap();
    let mut m = msg(json!({"ts": 2, "values": {"temperature": 44}}));
    m.queue_name = queue.clone();
    let originator = m.originator.id;

    // 反复执行直到 consumer 加入/topic 就绪（checkpoint 不写本地，重复无副作用）
    poll_kafka(
        &mut consumer,
        Duration::from_secs(30),
        || async {
            let _ = chain::execute(&ctx, &state, checkpoint.id, m.clone()).await;
        },
        |payload| {
            let Ok(to_re) = ToRuleEngineMsg::decode(payload) else {
                return false;
            };
            let Some(p) = to_re.tb_msg else { return false };
            p.rule_chain_id == chain_row.id.to_string() && p.rule_node_id == save.id.to_string()
        },
    )
    .await;

    // 未本地转发：ts_kv_latest 应为空
    assert!(
        ts_dao
            .find_latest(originator, None)
            .await
            .unwrap()
            .is_empty()
    );
}

// ---------- DLQ / debug ----------

#[tokio::test]
async fn failure_without_edge_goes_to_dlq() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let tenant = make_tenant(&pool).await;

    // 编译错误脚本 → 运行时 Failure；无 Failure 边 → DLQ
    let filter = make_chain_node(
        Uuid::nil(),
        "filter.script",
        json!({"script": "msg. >"}),
        false,
    );
    let chain_row = save_chain(
        &pool,
        tenant.id,
        std::slice::from_ref(&filter),
        &[],
        filter.id,
    )
    .await;

    let group = uniq("dlq-verify");
    let mut consumer =
        StreamQueueConsumer::new("localhost:29092", &group, &[TbTopic::RULE_ENGINE_DLQ]).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    let state = ChainLoader::new(pool.clone())
        .load(chain_row.id)
        .await
        .unwrap()
        .unwrap();
    let m = msg(json!({}));
    let msg_id = m.id;

    poll_kafka(
        &mut consumer,
        Duration::from_secs(30),
        || async {
            let _ = chain::execute(&ctx, &state, filter.id, m.clone()).await;
        },
        |payload| {
            let Ok(to_re) = ToRuleEngineMsg::decode(payload) else {
                return false;
            };
            let Some(p) = to_re.tb_msg else { return false };
            if p.id != msg_id.to_string() {
                return false;
            }
            p.metadata.get("fromNodeId") == Some(&filter.id.to_string())
                && p.metadata.get("chainId") == Some(&chain_row.id.to_string())
                && p.metadata.contains_key("error")
        },
    )
    .await;
}

#[tokio::test]
async fn debug_mode_writes_in_out_events() {
    let pool = setup().await;
    let ctx = ctx(&pool);
    let tenant = make_tenant(&pool).await;
    let debug_dao = RuleNodeDebugEventDao::new(pool.clone());

    let log = make_chain_node(Uuid::nil(), "action.log", json!({"prefix": "dbg"}), true);
    let chain_row = save_chain(&pool, tenant.id, std::slice::from_ref(&log), &[], log.id).await;

    let state = ChainLoader::new(pool.clone())
        .load(chain_row.id)
        .await
        .unwrap()
        .unwrap();
    let m = msg(json!({"temperature": 5}));
    let msg_id = m.id;
    chain::execute(&ctx, &state, log.id, m).await.unwrap();

    let page = debug_dao
        .find_by_node(log.id, None, None, &tb_dao::PageLink::new(0, 10))
        .await
        .unwrap();
    assert_eq!(page.total_elements, 2);
    let types: Vec<&str> = page.data.iter().map(|e| e.event_type.as_str()).collect();
    assert!(types.contains(&"IN") && types.contains(&"OUT"));
    assert!(page.data.iter().all(|e| e.msg_id == msg_id));
    assert!(page.data.iter().all(|e| e.error.is_none()));
    assert_eq!(page.data[0].rule_chain_id, chain_row.id);
    assert_eq!(page.data[0].data, Some(json!({"temperature": 5})));
}
