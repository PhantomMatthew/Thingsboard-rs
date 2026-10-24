use std::collections::HashMap;
use std::time::Duration;

use tb_common::msg::TbMsg;
use tb_common::{EntityId, EntityType, TbResult};
use tb_dao::{DelayEnqueueResult, DelayTask, DelayTaskState, RuleNodeDelayDao, migrate};
use tokio::sync::Mutex;
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

fn new_msg(key: Key) -> TbMsg {
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
    msg
}

async fn setup() -> sqlx::PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://thingsboard:thingsboard@localhost:5433/thingsboard".into());
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    pool
}

async fn find_task(
    pool: &sqlx::PgPool,
    key: Key,
    next_node_id: Uuid,
    msg_id: Uuid,
    hop_count: i32,
) -> DelayTask {
    sqlx::query_as::<_, DelayTask>(
        "SELECT * FROM rule_node_delay WHERE rule_node_id = $1 AND next_node_id = $2 \
         AND msg_id = $3 AND hop_count = $4",
    )
    .bind(key.node)
    .bind(next_node_id)
    .bind(msg_id)
    .bind(hop_count)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn cleanup(pool: &sqlx::PgPool, key: Key) {
    sqlx::query("DELETE FROM rule_node_delay WHERE rule_node_id = $1")
        .bind(key.node)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn enqueue_is_idempotent_and_uniqueness_includes_next_node_and_hop() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let pool = setup().await;
    let dao = RuleNodeDelayDao::new(pool.clone());
    let key = Key::new();
    let next = Uuid::now_v7();
    let mut msg = new_msg(key);
    msg.metadata.insert("_hops".into(), "7".into());

    assert_eq!(
        dao.enqueue(&msg, next, Duration::from_secs(60), 5).await?,
        DelayEnqueueResult::Scheduled
    );
    let task = find_task(&pool, key, next, msg.id, 7).await;
    assert_eq!(task.tenant_id, key.tenant);
    assert_eq!(task.rule_chain_id, key.chain);
    assert_eq!(task.rule_node_id, key.node);
    assert_eq!(task.next_node_id, next);
    assert_eq!(task.msg_id, msg.id);
    assert_eq!(task.hop_count, 7);
    assert_eq!(task.state, DelayTaskState::Pending);
    assert_eq!(task.max_retries, 5);
    assert_eq!(task.msg.rule_chain_id, Some(key.chain));
    assert_eq!(task.msg.rule_node_id, Some(next));
    assert_eq!(msg.rule_node_id, Some(key.node));

    assert_eq!(
        dao.enqueue(&msg, next, Duration::ZERO, 99).await?,
        DelayEnqueueResult::Duplicate
    );
    let replayed = dao.find_by_id(task.id).await?.unwrap();
    assert_eq!(replayed.execute_at, task.execute_at);
    assert_eq!(replayed.updated_at, task.updated_at);
    assert_eq!(replayed.max_retries, 5);

    let other_next = Uuid::now_v7();
    assert_eq!(
        dao.enqueue(&msg, other_next, Duration::from_secs(60), 5)
            .await?,
        DelayEnqueueResult::Scheduled
    );
    msg.metadata.insert("_hops".into(), "8".into());
    assert_eq!(
        dao.enqueue(&msg, next, Duration::from_secs(60), 5).await?,
        DelayEnqueueResult::Scheduled
    );
    find_task(&pool, key, other_next, msg.id, 7).await;
    find_task(&pool, key, next, msg.id, 8).await;

    cleanup(&pool, key).await;
    Ok(())
}

#[tokio::test]
async fn old_message_timestamp_does_not_determine_execute_at() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let pool = setup().await;
    let dao = RuleNodeDelayDao::new(pool.clone());
    let key = Key::new();
    let next = Uuid::now_v7();
    let mut msg = new_msg(key);
    msg.ts = now() - 3_600_000;
    let before = now();
    dao.enqueue(&msg, next, Duration::from_millis(1_500), 3)
        .await?;
    let after = now();

    let task = find_task(&pool, key, next, msg.id, 0).await;
    assert!(task.execute_at >= before + 1_000);
    assert!(task.execute_at <= after + 2_000);
    assert_eq!(task.msg.ts, msg.ts);

    cleanup(&pool, key).await;
    Ok(())
}

#[tokio::test]
async fn only_one_dao_can_claim_a_due_task() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let pool = setup().await;
    let first = RuleNodeDelayDao::new(pool.clone());
    let second = RuleNodeDelayDao::new(pool.clone());
    let key = Key::new();
    let next = Uuid::now_v7();
    let msg = new_msg(key);
    first.enqueue(&msg, next, Duration::ZERO, 3).await?;
    let task = find_task(&pool, key, next, msg.id, 0).await;
    let claim_now = now();

    let (a, b) = tokio::join!(
        first.claim_due("engine-a", claim_now + 60_000, claim_now, 100),
        second.claim_due("engine-b", claim_now + 60_000, claim_now, 100)
    );
    let a = a?;
    let b = b?;
    assert_eq!(
        a.iter()
            .chain(&b)
            .filter(|claimed| claimed.id == task.id)
            .count(),
        1
    );
    let stored = first.find_by_id(task.id).await?.unwrap();
    assert_eq!(stored.state, DelayTaskState::Emitting);
    assert!(matches!(
        stored.lease_owner.as_deref(),
        Some("engine-a" | "engine-b")
    ));
    assert_eq!(stored.lease_until, Some(claim_now + 60_000));
    assert_eq!(stored.next_attempt_at, None);

    cleanup(&pool, key).await;
    Ok(())
}

#[tokio::test]
async fn lease_renewal_and_terminal_update_require_current_owner() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let pool = setup().await;
    let dao = RuleNodeDelayDao::new(pool.clone());
    let key = Key::new();
    let next = Uuid::now_v7();
    let msg = new_msg(key);
    dao.enqueue(&msg, next, Duration::ZERO, 3).await?;
    let task = find_task(&pool, key, next, msg.id, 0).await;
    let claim_now = now();
    dao.claim_due("engine-a", claim_now + 1_000, claim_now, 100)
        .await?;

    assert!(
        !dao.renew_lease(task.id, "other", claim_now, claim_now + 5_000)
            .await?
    );
    assert!(
        dao.renew_lease(task.id, "engine-a", claim_now, claim_now + 5_000)
            .await?
    );
    assert!(
        !dao.renew_lease(task.id, "engine-a", claim_now + 5_001, claim_now + 9_000)
            .await?
    );

    let recovered = dao
        .claim_due("engine-b", claim_now + 20_000, claim_now + 5_001, 100)
        .await?;
    assert!(recovered.iter().any(|claimed| claimed.id == task.id));
    assert!(
        !dao.renew_lease(task.id, "engine-a", claim_now + 5_001, claim_now + 30_000)
            .await?
    );
    assert!(dao.mark_done(task.id, "engine-a", now()).await?.is_none());
    assert!(
        dao.record_retry(task.id, "engine-a", now(), now(), "stale owner")
            .await?
            .is_none()
    );
    assert!(
        dao.mark_dead(task.id, "engine-a", now(), "stale owner")
            .await?
            .is_none()
    );

    let done = dao.mark_done(task.id, "engine-b", now()).await?.unwrap();
    assert_eq!(done.state, DelayTaskState::Done);
    assert_eq!(done.lease_owner, None);
    assert_eq!(done.lease_until, None);
    let claims = dao
        .claim_due("engine-c", now() + 60_000, now(), 100)
        .await?;
    assert!(!claims.iter().any(|claimed| claimed.id == task.id));

    cleanup(&pool, key).await;
    Ok(())
}

#[tokio::test]
async fn retry_is_reclaimable_and_dead_is_terminal() -> TbResult<()> {
    let _guard = TEST_LOCK.lock().await;
    let pool = setup().await;
    let dao = RuleNodeDelayDao::new(pool.clone());
    let key = Key::new();
    let next = Uuid::now_v7();
    let msg = new_msg(key);
    dao.enqueue(&msg, next, Duration::ZERO, 2).await?;
    let task = find_task(&pool, key, next, msg.id, 0).await;
    let claim_now = now();
    dao.claim_due("engine-a", claim_now + 60_000, claim_now, 100)
        .await?;

    let retry_at = now() - 1;
    let retry = dao
        .record_retry(task.id, "engine-a", now(), retry_at, "temporary failure")
        .await?
        .unwrap();
    assert_eq!(retry.state, DelayTaskState::Emitting);
    assert_eq!(retry.retry_count, 1);
    assert_eq!(retry.next_attempt_at, Some(retry_at));
    assert_eq!(retry.lease_owner, None);
    assert_eq!(retry.lease_until, None);
    assert_eq!(retry.last_error.as_deref(), Some("temporary failure"));

    let reclaimed = dao
        .claim_due("engine-b", now() + 60_000, now(), 100)
        .await?;
    assert!(reclaimed.iter().any(|claimed| claimed.id == task.id));
    let retry_at = now() - 1;
    dao.record_retry(task.id, "engine-b", now(), retry_at, "second failure")
        .await?
        .unwrap();
    dao.claim_due("engine-a", now() + 60_000, now(), 100)
        .await?;
    let dead = dao
        .mark_dead(task.id, "engine-a", now(), "retry limit reached")
        .await?
        .unwrap();
    assert_eq!(dead.state, DelayTaskState::Dead);
    assert_eq!(dead.retry_count, 2);
    assert_eq!(dead.last_error.as_deref(), Some("retry limit reached"));
    assert_eq!(dead.lease_owner, None);
    assert_eq!(dead.next_attempt_at, None);

    let claims = dao
        .claim_due("engine-c", now() + 60_000, now(), 100)
        .await?;
    assert!(!claims.iter().any(|claimed| claimed.id == task.id));
    assert!(dao.mark_done(task.id, "engine-a", now()).await?.is_none());

    cleanup(&pool, key).await;
    Ok(())
}
