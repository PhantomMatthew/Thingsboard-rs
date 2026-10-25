use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{EntityId, EntityType};
use tb_dao::models::{RuleChain, RuleNode, Tenant};
use tb_dao::{RuleChainDao, TenantDao, migrate};
use tb_proto::ruleengine::ToRuleEngineMsg;
use tb_queue::{QueueError, QueueProducer, TbTopic};
use tb_rule_engine::RuleEngineService;
use uuid::Uuid;

const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";

struct TestContext {
    pool: PgPool,
    tenant_id: Uuid,
    chain_id: Uuid,
    dedup_node_id: Uuid,
    originator: EntityId,
    producer: Arc<RecordingProducer>,
}

struct RecordingProducer {
    sent: Mutex<Vec<(String, Bytes)>>,
    failures: AtomicUsize,
}

impl RecordingProducer {
    fn new() -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
            failures: AtomicUsize::new(0),
        }
    }

    fn drain(&self) -> Vec<TbMsg> {
        std::mem::take(&mut *self.sent.lock().unwrap())
            .into_iter()
            .map(|(_, payload)| {
                let envelope = ToRuleEngineMsg::decode(payload).unwrap();
                tb_proto::convert::tb_msg_from_proto(&envelope.tb_msg.unwrap()).unwrap()
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl QueueProducer for RecordingProducer {
    async fn send(&self, topic: &str, _key: &str, payload: Bytes) -> Result<(), QueueError> {
        if topic == TbTopic::USAGE_STATS {
            return Ok(());
        }
        self.sent.lock().unwrap().push((topic.to_string(), payload));
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(QueueError::Serialization("injected send failure".into()));
        }
        Ok(())
    }
}

async fn fresh_db() -> PgPool {
    let db_name = format!("tb_dedup_test_{}", Uuid::now_v7().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(ADMIN_URL)
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
    pool
}

fn rule_node(chain_id: Uuid, node_type: &str, configuration: Value) -> RuleNode {
    RuleNode {
        id: Uuid::now_v7(),
        created_time: 0,
        rule_chain_id: chain_id,
        r#type: node_type.into(),
        name: node_type.into(),
        debug_mode: false,
        singleton_mode: false,
        queue_name: None,
        configuration: Some(configuration),
        additional_info: None,
    }
}

async fn setup(dedup_config: Value) -> TestContext {
    let pool = fresh_db().await;
    let tenant = TenantDao::new(pool.clone())
        .save(&Tenant {
            id: Uuid::now_v7(),
            created_time: 0,
            title: "dedup-test".into(),
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
    let chain_id = Uuid::now_v7();
    let dedup = rule_node(chain_id, "action.deduplication", dedup_config);
    let checkpoint = rule_node(chain_id, "flow.checkpoint", json!({}));
    let math = rule_node(
        chain_id,
        "action.math",
        json!({"operation": "ADD", "fields": ["v"], "result_key": "v"}),
    );
    let dedup_node_id = dedup.id;
    let checkpoint_id = checkpoint.id;
    let math_id = math.id;
    let chain = RuleChainDao::new(pool.clone())
        .save(&RuleChain {
            id: chain_id,
            created_time: 0,
            tenant_id: tenant.id,
            name: "dedup-test".into(),
            first_rule_node_id: None,
            root: false,
            debug_mode: false,
            configuration: None,
            additional_info: None,
        })
        .await
        .unwrap();
    RuleChainDao::new(pool.clone())
        .save_nodes(
            chain.id,
            &[dedup, checkpoint, math],
            &[
                (dedup_node_id, checkpoint_id, "Success".into()),
                (checkpoint_id, math_id, "Success".into()),
            ],
            Some(dedup_node_id),
        )
        .await
        .unwrap();
    TestContext {
        pool,
        tenant_id: tenant.id,
        chain_id,
        dedup_node_id,
        originator: EntityId::new(EntityType::Device),
        producer: Arc::new(RecordingProducer::new()),
    }
}

fn engine(test: &TestContext) -> RuleEngineService {
    RuleEngineService::with_queue_producer(test.pool.clone(), test.producer.clone(), Duration::ZERO)
}

fn msg(test: &TestContext, value: i64, ts: i64) -> TbMsg {
    let mut msg = TbMsg::new(
        msg_type::POST_TELEMETRY_REQUEST,
        test.originator,
        test.tenant_id,
        json!({"v": value}),
        HashMap::new(),
    );
    msg.ts = ts;
    msg
}

async fn append(test: &TestContext, engine: &RuleEngineService, messages: &[TbMsg]) {
    for msg in messages {
        engine
            .handle_at(msg.clone(), test.chain_id, test.dedup_node_id)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn persistent_first_and_last_use_message_timestamp() {
    for (strategy, expected) in [("FIRST", 2), ("LAST", 1)] {
        let test = setup(json!({"interval": 1, "strategy": strategy})).await;
        let engine = engine(&test);
        let now = chrono::Utc::now().timestamp_millis();
        append(
            &test,
            &engine,
            &[msg(&test, 1, now + 10_000), msg(&test, 2, now)],
        )
        .await;
        assert!(test.producer.sent.lock().unwrap().is_empty());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        engine.process_due_windows("first-last", 10).await.unwrap();
        let sent = test.producer.drain();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].data, json!({"v": expected}));
    }
}

#[tokio::test]
async fn persistent_all_restores_after_engine_rebuild() {
    let test = setup(json!({
        "interval": 1,
        "strategy": "ALL",
        "outMsgType": "DEDUP_ALL"
    }))
    .await;
    let first_engine = engine(&test);
    let now = chrono::Utc::now().timestamp_millis();
    append(
        &test,
        &first_engine,
        &[msg(&test, 1, now), msg(&test, 2, now + 1)],
    )
    .await;
    drop(first_engine);
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let recovered_engine = engine(&test);
    recovered_engine
        .process_due_windows("recovered", 10)
        .await
        .unwrap();
    let sent = test.producer.drain();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].msg_type, "DEDUP_ALL");
    assert_eq!(sent[0].data[0]["msg"], json!({"v": 1}));
    assert_eq!(sent[0].data[1]["msg"], json!({"v": 2}));
}

#[tokio::test]
async fn storage_failure_routes_to_failure_without_memory_buffering() {
    let config = json!({"interval": 1, "strategy": "ALL", "outMsgType": "DEDUP_ALL"});
    let test = setup(config.clone()).await;
    let mut dedup = rule_node(test.chain_id, "action.deduplication", config);
    dedup.id = test.dedup_node_id;
    let checkpoint = rule_node(test.chain_id, "flow.checkpoint", json!({}));
    let math = rule_node(
        test.chain_id,
        "action.math",
        json!({"operation": "ADD", "fields": ["v"], "result_key": "v"}),
    );
    let (checkpoint_id, math_id) = (checkpoint.id, math.id);
    RuleChainDao::new(test.pool.clone())
        .save_nodes(
            test.chain_id,
            &[dedup, checkpoint, math],
            &[
                (test.dedup_node_id, checkpoint_id, "Success".into()),
                (test.dedup_node_id, checkpoint_id, "Failure".into()),
                (checkpoint_id, math_id, "Success".into()),
            ],
            Some(test.dedup_node_id),
        )
        .await
        .unwrap();
    let engine = engine(&test);
    let now = chrono::Utc::now().timestamp_millis();
    append(&test, &engine, &[msg(&test, 1, now)]).await;
    assert!(test.producer.sent.lock().unwrap().is_empty());

    sqlx::query("ALTER TABLE rule_node_dedup_msg RENAME TO rule_node_dedup_msg_offline")
        .execute(&test.pool)
        .await
        .unwrap();
    append(&test, &engine, &[msg(&test, 2, now + 1)]).await;
    let failed = test.producer.drain();
    assert_eq!(failed.len(), 1, "storage failure must surface on Failure");
    assert_eq!(failed[0].data, json!({"v": 2}));
    assert_eq!(failed[0].rule_node_id, Some(math_id));
    sqlx::query("ALTER TABLE rule_node_dedup_msg_offline RENAME TO rule_node_dedup_msg")
        .execute(&test.pool)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1100)).await;
    engine
        .process_due_windows("after-outage", 10)
        .await
        .unwrap();
    let sent = test.producer.drain();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].msg_type, "DEDUP_ALL");
    let window = sent[0].data.as_array().unwrap();
    assert_eq!(window.len(), 1, "failed message is not merged from memory");
    assert_eq!(window[0]["msg"], json!({"v": 1}));
}

#[tokio::test]
async fn concurrent_scanners_emit_once_and_retry_same_output() {
    let test = setup(json!({
        "interval": 1,
        "strategy": "ALL",
        "outMsgType": "DEDUP_ALL",
        "maxRetries": 1
    }))
    .await;
    let first_engine = engine(&test);
    let second_engine = engine(&test);
    let now = chrono::Utc::now().timestamp_millis();
    append(&test, &first_engine, &[msg(&test, 1, now)]).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;

    let (first, second) = tokio::join!(
        first_engine.process_due_windows("scanner-1", 10),
        second_engine.process_due_windows("scanner-2", 10)
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(test.producer.drain().len(), 1);

    let retry_test = setup(json!({
        "interval": 1,
        "strategy": "ALL",
        "outMsgType": "DEDUP_ALL",
        "maxRetries": 1
    }))
    .await;
    let retry_engine = engine(&retry_test);
    let now = chrono::Utc::now().timestamp_millis();
    append(&retry_test, &retry_engine, &[msg(&retry_test, 7, now)]).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    retry_test.producer.failures.store(1, Ordering::SeqCst);
    retry_engine
        .process_due_windows("retry-1", 10)
        .await
        .unwrap();
    let failed = retry_test.producer.drain();
    assert_eq!(failed.len(), 1);

    sqlx::query(
        "UPDATE rule_node_dedup_window SET next_attempt_at = 0, lease_until = 0, lease_owner = NULL",
    )
    .execute(&retry_test.pool)
    .await
    .unwrap();
    let recovered_engine = engine(&retry_test);
    recovered_engine
        .process_due_windows("retry-2", 10)
        .await
        .unwrap();
    let retried = retry_test.producer.drain();
    assert_eq!(retried.len(), 1);
    assert_eq!(failed[0].id, retried[0].id);
    assert_eq!(failed[0].data, retried[0].data);
    recovered_engine
        .process_due_windows("retry-3", 10)
        .await
        .unwrap();
    assert!(retry_test.producer.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn exhausted_output_uses_failure_path_and_closes_window() {
    let test = setup(json!({"interval": 1, "maxRetries": 0})).await;
    let engine = engine(&test);
    let now = chrono::Utc::now().timestamp_millis();
    append(&test, &engine, &[msg(&test, 1, now)]).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    test.producer.failures.store(1, Ordering::SeqCst);
    engine.process_due_windows("initial", 10).await.unwrap();
    assert_eq!(test.producer.drain().len(), 1);

    sqlx::query(
        "UPDATE rule_node_dedup_window SET next_attempt_at = 0, lease_until = 0, lease_owner = NULL",
    )
    .execute(&test.pool)
    .await
    .unwrap();
    engine.process_due_windows("exhausted", 10).await.unwrap();
    let failure = test.producer.drain();
    assert_eq!(failure.len(), 1);
    assert!(failure[0].metadata["error"].contains("no outgoing edge"));
    engine.process_due_windows("done", 10).await.unwrap();
    assert!(test.producer.sent.lock().unwrap().is_empty());
}

#[tokio::test]
async fn exhausted_delay_writes_dlq_before_dead() {
    let test = setup(json!({"interval": 1})).await;
    let engine = engine(&test);
    let mut delayed = msg(&test, 9, 1);
    delayed.rule_chain_id = Some(test.chain_id);
    delayed.rule_node_id = Some(test.dedup_node_id);
    delayed.metadata.insert("_hops".into(), "1".into());
    tb_dao::RuleNodeDelayDao::new(test.pool.clone())
        .enqueue(&delayed, Uuid::now_v7(), Duration::from_secs(1), 0)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    test.producer.failures.store(1, Ordering::SeqCst);
    engine
        .process_due_delays("delay-initial", 10)
        .await
        .unwrap();
    let dlq = test.producer.drain();
    assert_eq!(dlq.len(), 2);
    assert!(dlq[1].metadata["error"].contains("delayed requeue failed"));
    let state: String = sqlx::query_scalar("SELECT state FROM rule_node_delay")
        .fetch_one(&test.pool)
        .await
        .unwrap();
    assert_eq!(state, "DEAD");
    engine.process_due_delays("delay-done", 10).await.unwrap();
    assert!(test.producer.sent.lock().unwrap().is_empty());
}
