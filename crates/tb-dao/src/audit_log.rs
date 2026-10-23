//! audit_log 数据访问（实体 CRUD / 登录等操作的审计记录）。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::now_ms;
use crate::page::{PageData, PageLink};

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
#[serde(rename_all = "camelCase")]
pub struct AuditLog {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub entity_id: Option<Uuid>,
    pub entity_type: Option<String>,
    pub entity_name: Option<String>,
    pub user_id: Option<Uuid>,
    pub user_email: Option<String>,
    pub action_type: String,
    pub action_data: Option<Value>,
    pub action_status: String,
    pub created_time: i64,
}

impl AuditLog {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: Uuid,
        entity: Option<(Uuid, String, String)>,
        user_id: Option<Uuid>,
        user_email: Option<String>,
        action_type: &str,
        action_data: Option<Value>,
        action_status: &str,
    ) -> Self {
        let (entity_id, entity_type, entity_name) = match entity {
            Some((id, ty, name)) => (Some(id), Some(ty), Some(name)),
            None => (None, None, None),
        };
        Self {
            id: Uuid::now_v7(),
            tenant_id,
            entity_id,
            entity_type,
            entity_name,
            user_id,
            user_email,
            action_type: action_type.into(),
            action_data,
            action_status: action_status.into(),
            created_time: now_ms(),
        }
    }
}

/// 查询过滤器：None 字段不参与过滤。
#[derive(Debug, Default)]
pub struct AuditLogFilter {
    pub entity_id: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub action_types: Option<Vec<String>>,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
}

#[derive(Clone)]
pub struct AuditLogDao {
    pool: PgPool,
}

impl AuditLogDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, entry: &AuditLog) -> TbResult<()> {
        sqlx::query(
            "INSERT INTO audit_log \
             (id, tenant_id, entity_id, entity_type, entity_name, user_id, user_email, \
              action_type, action_data, action_status, created_time) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
        )
        .bind(entry.id)
        .bind(entry.tenant_id)
        .bind(entry.entity_id)
        .bind(&entry.entity_type)
        .bind(&entry.entity_name)
        .bind(entry.user_id)
        .bind(&entry.user_email)
        .bind(&entry.action_type)
        .bind(&entry.action_data)
        .bind(&entry.action_status)
        .bind(entry.created_time)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(())
    }

    /// 租户维度分页查询（created_time desc）；tenant_id 为 None 时跨租户（SYS_ADMIN）。
    pub async fn find_by_tenant(
        &self,
        tenant_id: Option<Uuid>,
        filter: &AuditLogFilter,
        page_link: &PageLink,
    ) -> TbResult<PageData<AuditLog>> {
        let cond = "($1::uuid IS NULL OR tenant_id = $1) \
             AND ($2::uuid IS NULL OR entity_id = $2) \
             AND ($3::uuid IS NULL OR user_id = $3) \
             AND ($4::text[] IS NULL OR action_type = ANY($4)) \
             AND ($5::bigint IS NULL OR created_time >= $5) \
             AND ($6::bigint IS NULL OR created_time <= $6)";
        let total: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM audit_log WHERE {cond}"))
                .bind(tenant_id)
                .bind(filter.entity_id)
                .bind(filter.user_id)
                .bind(filter.action_types.clone())
                .bind(filter.start_ts)
                .bind(filter.end_ts)
                .fetch_one(&self.pool)
                .await
                .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, AuditLog>(&format!(
            "SELECT * FROM audit_log WHERE {cond} ORDER BY created_time DESC LIMIT $7 OFFSET $8"
        ))
        .bind(tenant_id)
        .bind(filter.entity_id)
        .bind(filter.user_id)
        .bind(filter.action_types.clone())
        .bind(filter.start_ts)
        .bind(filter.end_ts)
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

    /// 清理早于 ts 的审计记录，返回删除行数。
    pub async fn delete_older_than(&self, ts: i64) -> TbResult<usize> {
        let res = sqlx::query("DELETE FROM audit_log WHERE created_time < $1")
            .bind(ts)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() as usize)
    }
}
