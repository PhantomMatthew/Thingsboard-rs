//! rule_node_debug_event 数据访问（规则节点 debug_mode 的 IN/OUT 事件）。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::now_ms;
use crate::page::{PageData, PageLink};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct RuleNodeDebugEvent {
    pub id: Uuid,
    pub ts: i64,
    pub tenant_id: Uuid,
    pub rule_chain_id: Uuid,
    pub rule_node_id: Uuid,
    pub msg_id: Uuid,
    pub event_type: String,
    pub data: Option<Value>,
    pub metadata: Option<Value>,
    pub error: Option<String>,
}

impl RuleNodeDebugEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        rule_chain_id: Uuid,
        rule_node_id: Uuid,
        msg_id: Uuid,
        event_type: &str,
        data: Option<Value>,
        metadata: Option<Value>,
        error: Option<String>,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            ts: now_ms(),
            tenant_id,
            rule_chain_id,
            rule_node_id,
            msg_id,
            event_type: event_type.into(),
            data,
            metadata,
            error,
        }
    }
}

pub struct RuleNodeDebugEventDao {
    pool: PgPool,
}

impl RuleNodeDebugEventDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, event: &RuleNodeDebugEvent) -> TbResult<()> {
        sqlx::query(
            "INSERT INTO rule_node_debug_event \
             (id, ts, tenant_id, rule_chain_id, rule_node_id, msg_id, event_type, data, metadata, error) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        )
        .bind(event.id)
        .bind(event.ts)
        .bind(event.tenant_id)
        .bind(event.rule_chain_id)
        .bind(event.rule_node_id)
        .bind(event.msg_id)
        .bind(&event.event_type)
        .bind(&event.data)
        .bind(&event.metadata)
        .bind(&event.error)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(())
    }

    /// 节点维度分页查询（ts desc），支持 startTs/endTs 过滤。
    pub async fn find_by_node(
        &self,
        rule_node_id: Uuid,
        start_ts: Option<i64>,
        end_ts: Option<i64>,
        page_link: &PageLink,
    ) -> TbResult<PageData<RuleNodeDebugEvent>> {
        let cond = "rule_node_id = $1 \
             AND ($2::bigint IS NULL OR ts >= $2) \
             AND ($3::bigint IS NULL OR ts <= $3)";
        let total: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM rule_node_debug_event WHERE {cond}"
        ))
        .bind(rule_node_id)
        .bind(start_ts)
        .bind(end_ts)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, RuleNodeDebugEvent>(&format!(
            "SELECT * FROM rule_node_debug_event WHERE {cond} ORDER BY ts DESC LIMIT $4 OFFSET $5"
        ))
        .bind(rule_node_id)
        .bind(start_ts)
        .bind(end_ts)
        .bind(page_link.limit())
        .bind(page_link.offset())
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(PageData::new(
            data,
            total,
            page_link.page,
            page_link.page_size,
        ))
    }

    /// 清理早于 ts 的事件，返回删除行数。
    pub async fn delete_older_than(&self, ts: i64) -> TbResult<usize> {
        let res = sqlx::query("DELETE FROM rule_node_debug_event WHERE ts < $1")
            .bind(ts)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() as usize)
    }
}
