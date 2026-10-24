//! 规则节点去重窗口的 PostgreSQL 持久化与分布式租约。

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::types::Json;
use sqlx::{FromRow, PgPool, Row};
use tb_common::msg::TbMsg;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::now_ms;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[sqlx(type_name = "VARCHAR", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DedupStrategy {
    First,
    Last,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[sqlx(type_name = "VARCHAR", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DedupWindowState {
    Open,
    Emitting,
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DedupWindow {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub rule_chain_id: Uuid,
    pub rule_node_id: Uuid,
    pub originator_id: Uuid,
    pub strategy: DedupStrategy,
    pub out_msg_type: String,
    pub queue_name: String,
    pub max_retries: i32,
    pub pending_count: i64,
    pub state: DedupWindowState,
    pub output_msg: Option<TbMsg>,
    pub retry_count: i32,
    pub next_attempt_at: Option<i64>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<i64>,
    pub expires_at: i64,
    pub updated_at: i64,
}

impl<'r> FromRow<'r, PgRow> for DedupWindow {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            tenant_id: row.try_get("tenant_id")?,
            rule_chain_id: row.try_get("rule_chain_id")?,
            rule_node_id: row.try_get("rule_node_id")?,
            originator_id: row.try_get("originator_id")?,
            strategy: row.try_get("strategy")?,
            out_msg_type: row.try_get("out_msg_type")?,
            queue_name: row.try_get("queue_name")?,
            max_retries: row.try_get("max_retries")?,
            pending_count: row.try_get("pending_count")?,
            state: row.try_get("state")?,
            output_msg: row
                .try_get::<Option<Json<TbMsg>>, _>("output_msg")?
                .map(|msg| msg.0),
            retry_count: row.try_get("retry_count")?,
            next_attempt_at: row.try_get("next_attempt_at")?,
            lease_owner: row.try_get("lease_owner")?,
            lease_until: row.try_get("lease_until")?,
            expires_at: row.try_get("expires_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendResult {
    Buffered,
    Duplicate,
    LimitExceeded,
}

#[derive(Clone)]
pub struct RuleNodeDedupDao {
    pool: PgPool,
}

impl RuleNodeDedupDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn append(
        &self,
        msg: &TbMsg,
        interval: i64,
        strategy: DedupStrategy,
        out_msg_type: &str,
        max_pending_msgs: i64,
        max_retries: i32,
    ) -> TbResult<AppendResult> {
        let rule_chain_id = msg
            .rule_chain_id
            .ok_or_else(|| TbError::InvalidInput("dedup message has no rule_chain_id".into()))?;
        let rule_node_id = msg
            .rule_node_id
            .ok_or_else(|| TbError::InvalidInput("dedup message has no rule_node_id".into()))?;
        let msg_json = serde_json::to_value(msg).map_err(TbError::internal)?;
        let now = now_ms();
        let expires_at = msg.ts.saturating_add(interval);
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;

        let lock_key = format!(
            "{}:{}:{}:{}",
            msg.tenant_id, rule_chain_id, rule_node_id, msg.originator.id
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;

        let duplicate: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM rule_node_dedup_msg \
             WHERE rule_node_id = $1 AND msg_id = $2)",
        )
        .bind(rule_node_id)
        .bind(msg.id)
        .fetch_one(&mut *tx)
        .await
        .map_err(TbError::db)?;
        if duplicate {
            tx.commit().await.map_err(TbError::db)?;
            return Ok(AppendResult::Duplicate);
        }

        let window = loop {
            let existing = sqlx::query_as::<_, DedupWindow>(
                "SELECT * FROM rule_node_dedup_window \
                 WHERE tenant_id = $1 AND rule_chain_id = $2 AND rule_node_id = $3 \
                 AND originator_id = $4 AND state = 'OPEN' FOR UPDATE",
            )
            .bind(msg.tenant_id)
            .bind(rule_chain_id)
            .bind(rule_node_id)
            .bind(msg.originator.id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(TbError::db)?;

            if let Some(window) = existing {
                if window.expires_at <= now {
                    sqlx::query(
                        "UPDATE rule_node_dedup_window SET state = 'EMITTING', lease_owner = NULL, \
                         lease_until = NULL, next_attempt_at = $2, updated_at = $2 WHERE id = $1",
                    )
                    .bind(window.id)
                    .bind(now)
                    .execute(&mut *tx)
                    .await
                    .map_err(TbError::db)?;
                    continue;
                }
                break window;
            }

            let inserted = sqlx::query_as::<_, DedupWindow>(
                "INSERT INTO rule_node_dedup_window \
                 (id, tenant_id, rule_chain_id, rule_node_id, originator_id, strategy, \
                 out_msg_type, queue_name, max_retries, pending_count, state, expires_at, updated_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,'OPEN',$10,$11) \
                 ON CONFLICT DO NOTHING RETURNING *",
            )
            .bind(Uuid::now_v7())
            .bind(msg.tenant_id)
            .bind(rule_chain_id)
            .bind(rule_node_id)
            .bind(msg.originator.id)
            .bind(strategy)
            .bind(out_msg_type)
            .bind(&msg.queue_name)
            .bind(max_retries)
            .bind(expires_at)
            .bind(now)
            .fetch_optional(&mut *tx)
            .await
            .map_err(TbError::db)?;
            if let Some(window) = inserted {
                break window;
            }
        };

        if window.pending_count >= max_pending_msgs {
            tx.commit().await.map_err(TbError::db)?;
            return Ok(AppendResult::LimitExceeded);
        }

        let inserted = sqlx::query(
            "INSERT INTO rule_node_dedup_msg (id, window_id, rule_node_id, msg_id, msg) \
             VALUES ($1,$2,$3,$4,$5) ON CONFLICT (rule_node_id, msg_id) DO NOTHING",
        )
        .bind(Uuid::now_v7())
        .bind(window.id)
        .bind(rule_node_id)
        .bind(msg.id)
        .bind(msg_json)
        .execute(&mut *tx)
        .await
        .map_err(TbError::db)?;
        if inserted.rows_affected() == 0 {
            tx.commit().await.map_err(TbError::db)?;
            return Ok(AppendResult::Duplicate);
        }

        sqlx::query(
            "UPDATE rule_node_dedup_window SET pending_count = pending_count + 1, \
             expires_at = LEAST(expires_at, $2), updated_at = $3 WHERE id = $1",
        )
        .bind(window.id)
        .bind(msg.ts.saturating_add(interval))
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(TbError::db)?;
        tx.commit().await.map_err(TbError::db)?;
        Ok(AppendResult::Buffered)
    }

    pub async fn claim_due(
        &self,
        owner: &str,
        lease_until: i64,
        now: i64,
        limit: i64,
    ) -> TbResult<Vec<DedupWindow>> {
        if limit < 0 {
            return Err(TbError::InvalidInput(
                "claim limit must not be negative".into(),
            ));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        sqlx::query_as::<_, DedupWindow>(
            "WITH due AS (\
                 SELECT id FROM rule_node_dedup_window \
                 WHERE (state = 'OPEN' AND expires_at <= $2) \
                    OR (state = 'EMITTING' AND \
                        ((next_attempt_at IS NOT NULL AND next_attempt_at <= $2) \
                         OR (lease_until IS NOT NULL AND lease_until <= $2))) \
                 ORDER BY COALESCE(next_attempt_at, expires_at), id \
                 FOR UPDATE SKIP LOCKED LIMIT $4\
             ) \
             UPDATE rule_node_dedup_window w SET state = 'EMITTING', lease_owner = $1, \
                 lease_until = $3, next_attempt_at = NULL, updated_at = $2 \
             FROM due WHERE w.id = due.id RETURNING w.*",
        )
        .bind(owner)
        .bind(now)
        .bind(lease_until)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_messages(&self, window_id: Uuid) -> TbResult<Vec<TbMsg>> {
        let messages = sqlx::query_scalar::<_, Json<TbMsg>>(
            "SELECT msg FROM rule_node_dedup_msg WHERE window_id = $1 ORDER BY seq",
        )
        .bind(window_id)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(messages.into_iter().map(|msg| msg.0).collect())
    }

    pub async fn prepare_output(
        &self,
        window_id: Uuid,
        owner: &str,
        msg: &TbMsg,
        now: i64,
    ) -> TbResult<Option<DedupWindow>> {
        let output = serde_json::to_value(msg).map_err(TbError::internal)?;
        sqlx::query_as::<_, DedupWindow>(
            "UPDATE rule_node_dedup_window SET output_msg = COALESCE(output_msg, $3), \
             updated_at = $4 WHERE id = $1 AND state = 'EMITTING' AND lease_owner = $2 \
             RETURNING *",
        )
        .bind(window_id)
        .bind(owner)
        .bind(output)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn mark_done(
        &self,
        window_id: Uuid,
        owner: &str,
        now: i64,
    ) -> TbResult<Option<DedupWindow>> {
        sqlx::query_as::<_, DedupWindow>(
            "UPDATE rule_node_dedup_window SET state = 'DONE', lease_owner = NULL, \
             lease_until = NULL, next_attempt_at = NULL, updated_at = $3 \
             WHERE id = $1 AND state = 'EMITTING' AND lease_owner = $2 RETURNING *",
        )
        .bind(window_id)
        .bind(owner)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn record_retry(
        &self,
        window_id: Uuid,
        owner: &str,
        now: i64,
        retry_at: i64,
    ) -> TbResult<Option<DedupWindow>> {
        sqlx::query_as::<_, DedupWindow>(
            "UPDATE rule_node_dedup_window SET retry_count = retry_count + 1, \
             lease_owner = NULL, lease_until = NULL, next_attempt_at = $4, updated_at = $3 \
             WHERE id = $1 AND state = 'EMITTING' AND lease_owner = $2 RETURNING *",
        )
        .bind(window_id)
        .bind(owner)
        .bind(now)
        .bind(retry_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_window(&self, window_id: Uuid) -> TbResult<Option<DedupWindow>> {
        sqlx::query_as::<_, DedupWindow>("SELECT * FROM rule_node_dedup_window WHERE id = $1")
            .bind(window_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_open(
        &self,
        tenant_id: Uuid,
        rule_chain_id: Uuid,
        rule_node_id: Uuid,
        originator_id: Uuid,
    ) -> TbResult<Option<DedupWindow>> {
        sqlx::query_as::<_, DedupWindow>(
            "SELECT * FROM rule_node_dedup_window WHERE tenant_id = $1 \
             AND rule_chain_id = $2 AND rule_node_id = $3 AND originator_id = $4 \
             AND state = 'OPEN'",
        )
        .bind(tenant_id)
        .bind(rule_chain_id)
        .bind(rule_node_id)
        .bind(originator_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn delete_by_rule_node(&self, rule_node_id: Uuid) -> TbResult<u64> {
        let result = sqlx::query("DELETE FROM rule_node_dedup_window WHERE rule_node_id = $1")
            .bind(rule_node_id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(result.rows_affected())
    }
}
