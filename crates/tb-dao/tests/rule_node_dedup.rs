use std::collections::HashMap;
use std::sync::Arc;

use tb_common::msg::TbMsg;
use tb_common::{EntityId, EntityType, TbResult};
use tb_dao::{
    AppendResult, DedupStrategy, DedupWindow, DedupWindowState, RuleNodeDedupDao, migrate,
};
use tokio::sync::{Barrier, Mutex};
use uuid::Uuid;

static TEST_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Clone, Copy)]
struct Key {
    tenant: Uuid,
    chain: Uuid,
    node: Uuid,
    originator: Uuid,
}

impl Key {
    fn new() -> Self {
        Self {
            tenant: Uuid::now_v7(),
            chain: Uuid::now_v7(),
            node: Uuid::now_v7(),
            originator: Uuid::now_v7(),
        }
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn new_msg(key: Key, ts: i64) -> TbMsg {
    let mut msg = TbMsg::new(
        "POST_TELEMETRY_REQUEST",
        EntityId {
            entity_type: EntityType::Device,
            id: key.originator,
        },
        key.tenant,
        serde_json::json!({"temperature": 20}),
        HashMap::new(),
    );
    msg.rule_chain_id = Some(key.chain);
    msg.rule_node_id = Some(key.node);
    msg.queue_name = "HighPriority".into();
    msg.ts = ts;
    msg
}

async fn setup() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://thingsboard:thingsboard@localhost:5433/thingsboard".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    pool
}

async fn open(dao: &RuleNodeDedupDao, key: Key) -> DedupWindow {
    dao.find_open(key.tenant, key.chain, key.node, key.originator)
        .await
        .unwrap()
        .expect("open dedup window")
}

#[tokio::test]
async fn concurrent_append_enforces_capacity() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let pool = setup().await;
    let dao = RuleNodeDedupDao::new(pool);
    let key = Key::new();
    let barrier = Arc::new(Barrier::new(24));
    let mut tasks = Vec::new();
    for _ in 0..24 {
        let dao = dao.clone();
        let msg = new_msg(key, now());
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            dao.append(&msg, 60_000, DedupStrategy::All, "Success", 8, 3)
                .await
        }));
    }

    let mut buffered = 0;
    let mut limited = 0;
    for task in tasks {
        match task.await.unwrap()? {
            AppendResult::Buffered => buffered += 1,
            AppendResult::LimitExceeded => limited += 1,
            AppendResult::Duplicate => panic!("unique messages must not be duplicates"),
        }
    }
    assert_eq!(buffered, 8);
    assert_eq!(limited, 16);
    let window = open(&dao, key).await;
    assert_eq!(window.pending_count, 8);
    assert_eq!(dao.find_messages(window.id).await?.len(), 8);

    dao.delete_by_rule_node(key.node).await?;
    Ok(())
}

#[tokio::test]
async fn msg_id_replay_is_duplicate_even_at_capacity() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let dao = RuleNodeDedupDao::new(setup().await);
    let key = Key::new();
    let msg = new_msg(key, now());

    assert_eq!(
        dao.append(&msg, 60_000, DedupStrategy::First, "Success", 1, 4)
            .await?,
        AppendResult::Buffered
    );
    assert_eq!(
        dao.append(&msg, 60_000, DedupStrategy::First, "Success", 1, 4)
            .await?,
        AppendResult::Duplicate
    );
    assert_eq!(
        dao.append(
            &new_msg(key, now()),
            60_000,
            DedupStrategy::First,
            "Success",
            1,
            4
        )
        .await?,
        AppendResult::LimitExceeded
    );

    let window = open(&dao, key).await;
    assert_eq!(window.strategy, DedupStrategy::First);
    assert_eq!(window.out_msg_type, "Success");
    assert_eq!(window.queue_name, "HighPriority");
    assert_eq!(window.max_retries, 4);
    assert_eq!(window.pending_count, 1);
    let stored = dao.find_messages(window.id).await?;
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].id, msg.id);
    assert_eq!(stored[0].data, msg.data);
    assert_eq!(stored[0].queue_name, msg.queue_name);

    dao.delete_by_rule_node(key.node).await?;
    Ok(())
}

#[tokio::test]
async fn append_after_expiry_creates_a_new_window() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let dao = RuleNodeDedupDao::new(setup().await);
    let key = Key::new();
    let old_msg = new_msg(key, now() - 10_000);
    dao.append(&old_msg, 1_000, DedupStrategy::Last, "Success", 10, 3)
        .await?;
    let old_window = open(&dao, key).await;
    assert!(old_window.expires_at <= now());

    let new_msg = new_msg(key, now());
    dao.append(&new_msg, 60_000, DedupStrategy::Last, "Success", 10, 3)
        .await?;
    let new_window = open(&dao, key).await;
    assert_ne!(old_window.id, new_window.id);
    assert_eq!(old_window.state, DedupWindowState::Open);
    assert_eq!(dao.find_messages(old_window.id).await?[0].id, old_msg.id);
    assert_eq!(dao.find_messages(new_window.id).await?[0].id, new_msg.id);

    dao.delete_by_rule_node(key.node).await?;
    Ok(())
}

#[tokio::test]
async fn only_one_dao_can_claim_a_due_window() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let pool = setup().await;
    let first = RuleNodeDedupDao::new(pool.clone());
    let second = RuleNodeDedupDao::new(pool);
    let key = Key::new();
    dao_append_expired(&first, key).await?;
    let window = open(&first, key).await;
    let claim_now = now();

    let (a, b) = tokio::join!(
        first.claim_due("engine-a", claim_now + 60_000, claim_now, 100),
        second.claim_due("engine-b", claim_now + 60_000, claim_now, 100)
    );
    let a = a?;
    let b = b?;
    let claims = a
        .iter()
        .chain(&b)
        .filter(|claimed| claimed.id == window.id)
        .count();
    assert_eq!(claims, 1);
    let stored = first.find_window(window.id).await?.unwrap();
    assert_eq!(stored.state, DedupWindowState::Emitting);
    assert!(stored.lease_owner.as_deref().is_some());

    first.delete_by_rule_node(key.node).await?;
    Ok(())
}

#[tokio::test]
async fn expired_lease_is_recoverable_and_old_owner_cannot_finish() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let dao = RuleNodeDedupDao::new(setup().await);
    let key = Key::new();
    dao_append_expired(&dao, key).await?;
    let window = open(&dao, key).await;
    let claim_now = now();
    let claimed = dao
        .claim_due("engine-a", claim_now - 1, claim_now, 100)
        .await?;
    assert!(claimed.iter().any(|w| w.id == window.id));

    let recovered = dao
        .claim_due("engine-b", claim_now + 60_000, claim_now + 1, 100)
        .await?;
    let recovered = recovered.iter().find(|w| w.id == window.id).unwrap();
    assert_eq!(recovered.lease_owner.as_deref(), Some("engine-b"));
    assert!(dao.mark_done(window.id, "engine-a", now()).await?.is_none());
    assert!(
        dao.record_retry(window.id, "engine-a", now(), now())
            .await?
            .is_none()
    );
    let stored = dao.find_window(window.id).await?.unwrap();
    assert_eq!(stored.state, DedupWindowState::Emitting);
    assert_eq!(stored.lease_owner.as_deref(), Some("engine-b"));
    let done = dao.mark_done(window.id, "engine-b", now()).await?.unwrap();
    assert_eq!(done.state, DedupWindowState::Done);
    assert_eq!(done.lease_owner, None);

    dao.delete_by_rule_node(key.node).await?;
    Ok(())
}

#[tokio::test]
async fn prepare_output_never_overwrites_and_retry_preserves_it() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let dao = RuleNodeDedupDao::new(setup().await);
    let key = Key::new();
    dao_append_expired(&dao, key).await?;
    let window = open(&dao, key).await;
    let claim_now = now();
    dao.claim_due("engine-a", claim_now + 60_000, claim_now, 100)
        .await?;
    let mut first = new_msg(key, now());
    first.msg_type = "Success".into();
    let mut second = first.clone();
    second.id = Uuid::now_v7();
    second.data = serde_json::json!({"replacement": true});

    let prepared = dao
        .prepare_output(window.id, "engine-a", &first, now())
        .await?
        .unwrap();
    assert_eq!(prepared.output_msg.unwrap().id, first.id);
    let prepared = dao
        .prepare_output(window.id, "engine-a", &second, now())
        .await?
        .unwrap();
    assert_eq!(prepared.output_msg.unwrap().id, first.id);

    let retry_at = now() - 1;
    let retry = dao
        .record_retry(window.id, "engine-a", now(), retry_at)
        .await?
        .unwrap();
    assert_eq!(retry.retry_count, 1);
    assert_eq!(retry.lease_owner, None);
    assert_eq!(retry.next_attempt_at, Some(retry_at));
    assert!(
        dao.prepare_output(window.id, "engine-a", &second, now())
            .await?
            .is_none()
    );
    let reclaimed = dao
        .claim_due("engine-b", now() + 60_000, now(), 100)
        .await?;
    let reclaimed = reclaimed.iter().find(|w| w.id == window.id).unwrap();
    assert_eq!(reclaimed.output_msg.as_ref().unwrap().id, first.id);
    assert_eq!(reclaimed.retry_count, 1);

    dao.delete_by_rule_node(key.node).await?;
    Ok(())
}

async fn dao_append_expired(dao: &RuleNodeDedupDao, key: Key) -> TbResult<()> {
    let result = dao
        .append(
            &new_msg(key, now() - 10_000),
            1_000,
            DedupStrategy::All,
            "Success",
            10,
            3,
        )
        .await?;
    assert_eq!(result, AppendResult::Buffered);
    Ok(())
}
