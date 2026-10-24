//! 规则节点延迟消息的 PostgreSQL 持久化与分布式租约。

use std::time::Duration;

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
pub enum DelayTaskState {
    Pending,
    Emitting,
    Done,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DelayTask {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub rule_chain_id: Uuid,
    pub rule_node_id: Uuid,
    pub next_node_id: Uuid,
    pub msg_id: Uuid,
    pub hop_count: i32,
    pub msg: TbMsg,
    pub execute_at: i64,
    pub state: DelayTaskState,
    pub retry_count: i32,
    pub max_retries: i32,
    pub next_attempt_at: Option<i64>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<i64>,
    pub last_error: Option<String>,
    pub updated_at: i64,
}

impl<'r> FromRow<'r, PgRow> for DelayTask {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            tenant_id: row.try_get("tenant_id")?,
            rule_chain_id: row.try_get("rule_chain_id")?,
            rule_node_id: row.try_get("rule_node_id")?,
            next_node_id: row.try_get("next_node_id")?,
            msg_id: row.try_get("msg_id")?,
            hop_count: row.try_get("hop_count")?,
            msg: row.try_get::<Json<TbMsg>, _>("msg")?.0,
            execute_at: row.try_get("execute_at")?,
            state: row.try_get("state")?,
            retry_count: row.try_get("retry_count")?,
            max_retries: row.try_get("max_retries")?,
            next_attempt_at: row.try_get("next_attempt_at")?,
            lease_owner: row.try_get("lease_owner")?,
            lease_until: row.try_get("lease_until")?,
            last_error: row.try_get("last_error")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayEnqueueResult {
    Scheduled,
    Duplicate,
}

#[derive(Clone)]
pub struct RuleNodeDelayDao {
    pool: PgPool,
}

impl RuleNodeDelayDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue(
        &self,
        msg: &TbMsg,
        next_node_id: Uuid,
        delay: Duration,
        max_retries: i32,
    ) -> TbResult<DelayEnqueueResult> {
        let rule_chain_id = msg
            .rule_chain_id
            .ok_or_else(|| TbError::InvalidInput("delay message has no rule_chain_id".into()))?;
        let rule_node_id = msg
            .rule_node_id
            .ok_or_else(|| TbError::InvalidInput("delay message has no rule_node_id".into()))?;
        let hop_count = msg
            .metadata
            .get("_hops")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0);
        let delay_ms = i64::try_from(delay.as_millis()).map_err(|_| {
            TbError::InvalidInput("delay exceeds the supported millisecond range".into())
        })?;
        let mut delayed_msg = msg.clone();
        delayed_msg.rule_chain_id = Some(rule_chain_id);
        delayed_msg.rule_node_id = Some(next_node_id);
        let msg_json = serde_json::to_value(delayed_msg).map_err(TbError::internal)?;

        let inserted = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO rule_node_delay \
             (id, tenant_id, rule_chain_id, rule_node_id, next_node_id, msg_id, hop_count, msg, \
             execute_at, state, max_retries, updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,\
             (EXTRACT(EPOCH FROM clock_timestamp() + $9 * INTERVAL '1 millisecond') * 1000)::BIGINT,\
             'PENDING',$10,$11) \
             ON CONFLICT (rule_node_id, next_node_id, msg_id, hop_count) DO NOTHING RETURNING id",
        )
        .bind(Uuid::now_v7())
        .bind(msg.tenant_id)
        .bind(rule_chain_id)
        .bind(rule_node_id)
        .bind(next_node_id)
        .bind(msg.id)
        .bind(hop_count)
        .bind(msg_json)
        .bind(delay_ms)
        .bind(max_retries)
        .bind(now_ms())
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)?;

        if inserted.is_some() {
            Ok(DelayEnqueueResult::Scheduled)
        } else {
            Ok(DelayEnqueueResult::Duplicate)
        }
    }

    pub async fn claim_due(
        &self,
        owner: &str,
        lease_until: i64,
        now: i64,
        limit: i64,
    ) -> TbResult<Vec<DelayTask>> {
        if limit < 0 {
            return Err(TbError::InvalidInput(
                "claim limit must not be negative".into(),
            ));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        sqlx::query_as::<_, DelayTask>(
            "WITH due AS (\
                 SELECT id FROM rule_node_delay \
                 WHERE (state = 'PENDING' AND execute_at <= $2) \
                    OR (state = 'EMITTING' AND \
                        ((next_attempt_at IS NOT NULL AND next_attempt_at <= $2) \
                         OR (lease_until IS NOT NULL AND lease_until <= $2))) \
                 ORDER BY COALESCE(next_attempt_at, execute_at), id \
                 FOR UPDATE SKIP LOCKED LIMIT $4\
             ) \
             UPDATE rule_node_delay d SET state = 'EMITTING', lease_owner = $1, \
                 lease_until = $3, next_attempt_at = NULL, updated_at = $2 \
             FROM due WHERE d.id = due.id RETURNING d.*",
        )
        .bind(owner)
        .bind(now)
        .bind(lease_until)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn renew_lease(
        &self,
        id: Uuid,
        owner: &str,
        now: i64,
        new_lease_until: i64,
    ) -> TbResult<bool> {
        let result = sqlx::query(
            "UPDATE rule_node_delay SET lease_until = $4, updated_at = $3 \
             WHERE id = $1 AND state = 'EMITTING' AND lease_owner = $2 AND lease_until >= $3",
        )
        .bind(id)
        .bind(owner)
        .bind(now)
        .bind(new_lease_until)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn mark_done(&self, id: Uuid, owner: &str, now: i64) -> TbResult<Option<DelayTask>> {
        sqlx::query_as::<_, DelayTask>(
            "UPDATE rule_node_delay SET state = 'DONE', lease_owner = NULL, lease_until = NULL, \
             next_attempt_at = NULL, updated_at = $3 \
             WHERE id = $1 AND state = 'EMITTING' AND lease_owner = $2 RETURNING *",
        )
        .bind(id)
        .bind(owner)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn record_retry(
        &self,
        id: Uuid,
        owner: &str,
        now: i64,
        retry_at: i64,
        error: &str,
    ) -> TbResult<Option<DelayTask>> {
        sqlx::query_as::<_, DelayTask>(
            "UPDATE rule_node_delay SET retry_count = retry_count + 1, lease_owner = NULL, \
             lease_until = NULL, next_attempt_at = $4, last_error = $5, updated_at = $3 \
             WHERE id = $1 AND state = 'EMITTING' AND lease_owner = $2 RETURNING *",
        )
        .bind(id)
        .bind(owner)
        .bind(now)
        .bind(retry_at)
        .bind(error)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn mark_dead(
        &self,
        id: Uuid,
        owner: &str,
        now: i64,
        error: &str,
    ) -> TbResult<Option<DelayTask>> {
        sqlx::query_as::<_, DelayTask>(
            "UPDATE rule_node_delay SET state = 'DEAD', lease_owner = NULL, lease_until = NULL, \
             next_attempt_at = NULL, last_error = $4, updated_at = $3 \
             WHERE id = $1 AND state = 'EMITTING' AND lease_owner = $2 RETURNING *",
        )
        .bind(id)
        .bind(owner)
        .bind(now)
        .bind(error)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<DelayTask>> {
        sqlx::query_as::<_, DelayTask>("SELECT * FROM rule_node_delay WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }
}
