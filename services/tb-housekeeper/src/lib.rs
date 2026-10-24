//! 分批物理删除，限制单事务行数与 WAL 洪峰。

use std::time::Duration;

use sqlx::PgPool;

const DELETE_BATCH_SIZE: usize = 10_000;
const BATCH_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Clone, Copy)]
enum CleanupTarget {
    TsKv,
    DebugEvent,
    ClearedAlarm,
    DedupWindow,
    DelayTask,
}

impl CleanupTarget {
    fn delete_sql(self, batch_size: usize) -> String {
        let (table, predicate) = match self {
            Self::TsKv => ("ts_kv", "ts < $1"),
            Self::DebugEvent => ("rule_node_debug_event", "ts < $1"),
            Self::ClearedAlarm => (
                "alarm",
                "status IN ('CLEARED_UNACK','CLEARED_ACK') AND clear_ts IS NOT NULL AND clear_ts < $1",
            ),
            Self::DedupWindow => (
                "rule_node_dedup_window",
                "state = 'DONE' AND updated_at < $1",
            ),
            Self::DelayTask => (
                "rule_node_delay",
                "state IN ('DONE','DEAD') AND updated_at < $1",
            ),
        };
        format!(
            "DELETE FROM {table} WHERE ctid IN \
             (SELECT ctid FROM {table} WHERE {predicate} LIMIT {batch_size})"
        )
    }
}

async fn delete_in_batches(
    pool: &PgPool,
    target: CleanupTarget,
    cutoff: i64,
    batch_size: usize,
    batch_interval: Duration,
) -> Result<u64, sqlx::Error> {
    assert!(batch_size > 0);
    let sql = target.delete_sql(batch_size);
    let mut total = 0_u64;
    loop {
        let deleted = sqlx::query(&sql)
            .bind(cutoff)
            .execute(pool)
            .await?
            .rows_affected();
        if deleted == 0 {
            return Ok(total);
        }
        total += deleted;
        if !batch_interval.is_zero() {
            tokio::time::sleep(batch_interval).await;
        }
    }
}

pub async fn cleanup_ts_kv(pool: &PgPool, cutoff: i64) -> Result<u64, sqlx::Error> {
    delete_in_batches(
        pool,
        CleanupTarget::TsKv,
        cutoff,
        DELETE_BATCH_SIZE,
        BATCH_INTERVAL,
    )
    .await
}

pub async fn cleanup_debug_events(pool: &PgPool, cutoff: i64) -> Result<u64, sqlx::Error> {
    delete_in_batches(
        pool,
        CleanupTarget::DebugEvent,
        cutoff,
        DELETE_BATCH_SIZE,
        BATCH_INTERVAL,
    )
    .await
}

pub async fn cleanup_cleared_alarms(pool: &PgPool, cutoff: i64) -> Result<u64, sqlx::Error> {
    delete_in_batches(
        pool,
        CleanupTarget::ClearedAlarm,
        cutoff,
        DELETE_BATCH_SIZE,
        BATCH_INTERVAL,
    )
    .await
}

pub async fn cleanup_dedup_windows(pool: &PgPool, cutoff: i64) -> Result<u64, sqlx::Error> {
    delete_in_batches(
        pool,
        CleanupTarget::DedupWindow,
        cutoff,
        DELETE_BATCH_SIZE,
        BATCH_INTERVAL,
    )
    .await
}

pub async fn cleanup_delay_tasks(pool: &PgPool, cutoff: i64) -> Result<u64, sqlx::Error> {
    delete_in_batches(
        pool,
        CleanupTarget::DelayTask,
        cutoff,
        DELETE_BATCH_SIZE,
        BATCH_INTERVAL,
    )
    .await
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    use super::*;

    const ADMIN_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/postgres";
    const TEST_URL: &str =
        "postgres://thingsboard:thingsboard@localhost:5433/thingsboard_housekeeper_test";

    async fn setup_pool() -> PgPool {
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(ADMIN_URL)
            .await
            .expect("connect admin db");
        sqlx::query("DROP DATABASE IF EXISTS thingsboard_housekeeper_test WITH (FORCE)")
            .execute(&admin)
            .await
            .expect("drop test db");
        sqlx::query("CREATE DATABASE thingsboard_housekeeper_test")
            .execute(&admin)
            .await
            .expect("create test db");
        admin.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(TEST_URL)
            .await
            .expect("connect test db");
        tb_dao::migrate(&pool).await.expect("migrate test db");
        pool
    }

    async fn insert_alarm(pool: &PgPool, status: &str, clear_ts: Option<i64>) {
        sqlx::query(
            "INSERT INTO alarm (id, created_time, tenant_id, originator_id, originator_type, \
             type, severity, status, start_ts, end_ts, clear_ts) \
             VALUES ($1,0,$2,$3,'DEVICE','test','MINOR',$4,0,0,$5)",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(status)
        .bind(clear_ts)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_dedup_window(pool: &PgPool, state: &str, updated_at: i64) {
        sqlx::query(
            "INSERT INTO rule_node_dedup_window \
             (id, tenant_id, rule_chain_id, rule_node_id, originator_id, strategy, \
             out_msg_type, queue_name, max_retries, state, expires_at, updated_at) \
             VALUES ($1,$2,$3,$4,$5,'FIRST','','Main',3,$6,0,$7)",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(state)
        .bind(updated_at)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_delay_task(pool: &PgPool, state: &str, updated_at: i64) {
        sqlx::query(
            "INSERT INTO rule_node_delay \
             (id, tenant_id, rule_chain_id, rule_node_id, next_node_id, msg_id, hop_count, msg, \
             execute_at, state, updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,0,'{}'::jsonb,0,$7,$8)",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(state)
        .bind(updated_at)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn all_cleanup_targets_delete_multiple_batches() {
        let pool = setup_pool().await;
        let entity_id = Uuid::now_v7();
        sqlx::query("INSERT INTO key_dictionary(key) VALUES ('temperature')")
            .execute(&pool)
            .await
            .unwrap();
        let key_id: i32 =
            sqlx::query_scalar("SELECT key_id FROM key_dictionary WHERE key = 'temperature'")
                .fetch_one(&pool)
                .await
                .unwrap();
        for ts in [1, 2, 3, 4, 5, 100] {
            sqlx::query("INSERT INTO ts_kv(entity_id, key, ts, long_v) VALUES ($1,$2,$3,$4)")
                .bind(entity_id)
                .bind(key_id)
                .bind(ts)
                .bind(ts)
                .execute(&pool)
                .await
                .unwrap();

            sqlx::query(
                "INSERT INTO rule_node_debug_event \
                 (id, ts, tenant_id, rule_chain_id, rule_node_id, msg_id, event_type) \
                 VALUES ($1,$2,$3,$4,$5,$6,'IN')",
            )
            .bind(Uuid::now_v7())
            .bind(ts)
            .bind(Uuid::now_v7())
            .bind(Uuid::now_v7())
            .bind(Uuid::now_v7())
            .bind(Uuid::now_v7())
            .execute(&pool)
            .await
            .unwrap();
        }
        for clear_ts in 1..=5 {
            insert_alarm(&pool, "CLEARED_ACK", Some(clear_ts)).await;
        }
        insert_alarm(&pool, "ACTIVE_UNACK", None).await;
        insert_alarm(&pool, "CLEARED_UNACK", Some(100)).await;
        for updated_at in 1..=5 {
            insert_dedup_window(&pool, "DONE", updated_at).await;
        }
        insert_dedup_window(&pool, "OPEN", 1).await;
        insert_dedup_window(&pool, "DONE", 100).await;
        for updated_at in 1..=5 {
            insert_delay_task(&pool, "DEAD", updated_at).await;
        }
        insert_delay_task(&pool, "PENDING", 1).await;
        insert_delay_task(&pool, "DONE", 100).await;

        for target in [
            CleanupTarget::TsKv,
            CleanupTarget::DebugEvent,
            CleanupTarget::ClearedAlarm,
            CleanupTarget::DedupWindow,
            CleanupTarget::DelayTask,
        ] {
            let deleted = delete_in_batches(&pool, target, 10, 2, Duration::ZERO)
                .await
                .unwrap();
            assert_eq!(deleted, 5);
        }

        for (table, expected) in [
            ("ts_kv", 1),
            ("rule_node_debug_event", 1),
            ("alarm", 2),
            ("rule_node_dedup_window", 2),
            ("rule_node_delay", 2),
        ] {
            let remaining: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(remaining, expected, "unexpected rows in {table}");
        }
        for index in ["idx_ts_kv_ts", "idx_debug_event_ts"] {
            let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
                .bind(index)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(found.as_deref(), Some(index));
        }
    }
}
