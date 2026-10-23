//! api_usage_state 数据访问（租户级 API 用量累计）。

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::now_ms;

#[derive(Debug, Clone, Default, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct ApiUsageState {
    pub id: Uuid,
    pub created_time: i64,
    pub tenant_id: Uuid,
    pub transport_msgs: i64,
    pub re_msgs: i64,
    pub db_storage: i64,
    pub js_exec: i64,
    pub updated_at: i64,
}

/// 一轮待累加的计数增量。
#[derive(Debug, Clone, Copy, Default)]
pub struct UsageDelta {
    pub transport_msgs: i64,
    pub re_msgs: i64,
    pub rest_requests: i64,
}

#[derive(Clone)]
pub struct ApiUsageStateDao {
    pool: PgPool,
}

impl ApiUsageStateDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_tenant(&self, tenant_id: Uuid) -> TbResult<Option<ApiUsageState>> {
        sqlx::query_as::<_, ApiUsageState>("SELECT * FROM api_usage_state WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    /// 累加式 upsert（transport_msgs / re_msgs 两类计数；REST 请求数不落库，由 core 内存态返回）。
    pub async fn add_delta(&self, tenant_id: Uuid, delta: UsageDelta) -> TbResult<()> {
        let now = now_ms();
        sqlx::query(
            "INSERT INTO api_usage_state \
             (id, created_time, tenant_id, transport_msgs, re_msgs, db_storage, js_exec, updated_at) \
             VALUES ($1,$2,$3,$4,$5,0,0,$2) \
             ON CONFLICT (tenant_id) DO UPDATE SET \
             transport_msgs = api_usage_state.transport_msgs + EXCLUDED.transport_msgs, \
             re_msgs = api_usage_state.re_msgs + EXCLUDED.re_msgs, \
             updated_at = EXCLUDED.updated_at",
        )
        .bind(Uuid::now_v7())
        .bind(now)
        .bind(tenant_id)
        .bind(delta.transport_msgs)
        .bind(delta.re_msgs)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(())
    }
}
