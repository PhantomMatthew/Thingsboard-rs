//! alarm 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Alarm;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct AlarmDao {
    pool: PgPool,
}

impl AlarmDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, alarm: &Alarm) -> TbResult<Alarm> {
        let mut a = alarm.clone();
        if a.created_time == 0 {
            a.created_time = now_ms();
        }
        sqlx::query_as::<_, Alarm>(
            "INSERT INTO alarm (id, created_time, tenant_id, originator_id, originator_type, \
             type, severity, status, start_ts, end_ts, ack_ts, clear_ts, details, propagate, \
             propagate_to_owner, propagate_to_tenant, propagate_relation_types, customer_id, \
             assignee_id, assign_ts) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, \
             originator_id=EXCLUDED.originator_id, originator_type=EXCLUDED.originator_type, \
             type=EXCLUDED.type, severity=EXCLUDED.severity, status=EXCLUDED.status, \
             start_ts=EXCLUDED.start_ts, end_ts=EXCLUDED.end_ts, ack_ts=EXCLUDED.ack_ts, \
             clear_ts=EXCLUDED.clear_ts, details=EXCLUDED.details, propagate=EXCLUDED.propagate, \
             propagate_to_owner=EXCLUDED.propagate_to_owner, \
             propagate_to_tenant=EXCLUDED.propagate_to_tenant, \
             propagate_relation_types=EXCLUDED.propagate_relation_types, \
             customer_id=EXCLUDED.customer_id, assignee_id=EXCLUDED.assignee_id, \
             assign_ts=EXCLUDED.assign_ts RETURNING *",
        )
        .bind(a.id)
        .bind(a.created_time)
        .bind(a.tenant_id)
        .bind(a.originator_id)
        .bind(a.originator_type.as_str())
        .bind(&a.r#type)
        .bind(a.severity)
        .bind(a.status)
        .bind(a.start_ts)
        .bind(a.end_ts)
        .bind(a.ack_ts)
        .bind(a.clear_ts)
        .bind(&a.details)
        .bind(a.propagate)
        .bind(a.propagate_to_owner)
        .bind(a.propagate_to_tenant)
        .bind(&a.propagate_relation_types)
        .bind(a.customer_id)
        .bind(a.assignee_id)
        .bind(a.assign_ts)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Alarm>> {
        sqlx::query_as::<_, Alarm>("SELECT * FROM alarm WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM alarm WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn find_latest_by_originator(&self, originator_id: Uuid) -> TbResult<Option<Alarm>> {
        sqlx::query_as::<_, Alarm>(
            "SELECT * FROM alarm WHERE originator_id = $1 ORDER BY created_time DESC LIMIT 1",
        )
        .bind(originator_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// originator+type 维度的最新告警（规则引擎 create_alarm 判重用）。
    pub async fn find_latest_by_originator_and_type(
        &self,
        originator_id: Uuid,
        alarm_type: &str,
    ) -> TbResult<Option<Alarm>> {
        sqlx::query_as::<_, Alarm>(
            "SELECT * FROM alarm WHERE originator_id = $1 AND type = $2 \
             ORDER BY created_time DESC LIMIT 1",
        )
        .bind(originator_id)
        .bind(alarm_type)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// ack：ACTIVE_UNACK → ACTIVE_ACK；已 ack 幂等返回；已 CLEARED 报错。
    pub async fn ack(&self, id: Uuid) -> TbResult<Alarm> {
        let updated = sqlx::query_as::<_, Alarm>(
            "UPDATE alarm SET status='ACTIVE_ACK', ack_ts=$2 \
             WHERE id=$1 AND status='ACTIVE_UNACK' RETURNING *",
        )
        .bind(id)
        .bind(now_ms())
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)?;
        if let Some(alarm) = updated {
            return Ok(alarm);
        }
        let alarm = self
            .find_by_id(id)
            .await?
            .ok_or_else(|| TbError::NotFound(format!("alarm {id}")))?;
        use crate::models::AlarmStatus as S;
        match alarm.status {
            S::ActiveAck => Ok(alarm),
            S::ClearedUnack | S::ClearedAck => Err(TbError::InvalidInput(
                "alarm already cleared, cannot ack".into(),
            )),
            S::ActiveUnack => Err(TbError::Conflict(
                "alarm status changed during ack, retry".into(),
            )),
        }
    }

    /// clear：ACTIVE_* → CLEARED_*（保留 ack 位）；已 CLEARED 幂等返回。
    pub async fn clear(&self, id: Uuid) -> TbResult<Alarm> {
        let now = now_ms();
        let updated = sqlx::query_as::<_, Alarm>(
            "UPDATE alarm SET status=CASE WHEN status='ACTIVE_ACK' \
             THEN 'CLEARED_ACK' ELSE 'CLEARED_UNACK' END, clear_ts=$2, end_ts=$3 \
             WHERE id=$1 AND status IN ('ACTIVE_UNACK','ACTIVE_ACK') RETURNING *",
        )
        .bind(id)
        .bind(now)
        .bind(now)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)?;
        if let Some(alarm) = updated {
            return Ok(alarm);
        }
        let alarm = self
            .find_by_id(id)
            .await?
            .ok_or_else(|| TbError::NotFound(format!("alarm {id}")))?;
        use crate::models::AlarmStatus as S;
        match alarm.status {
            S::ClearedUnack | S::ClearedAck => Ok(alarm),
            S::ActiveUnack | S::ActiveAck => Err(TbError::Conflict(
                "alarm status changed during clear, retry".into(),
            )),
        }
    }

    /// 租户级/实体级分页查询，支持 status 与 startTs/endTs 过滤。
    pub async fn find_by_tenant(
        &self,
        tenant_id: Uuid,
        originator_id: Option<Uuid>,
        status: Option<crate::models::AlarmStatus>,
        start_ts: Option<i64>,
        end_ts: Option<i64>,
        page_link: &PageLink,
    ) -> TbResult<PageData<Alarm>> {
        let sort = page_link.sort_column(&["created_time", "start_ts", "severity", "status"]);
        let order = page_link.sort_order_sql();
        let cond = "tenant_id = $1 \
             AND ($2::uuid IS NULL OR originator_id = $2) \
             AND ($3::varchar IS NULL OR status = $3) \
             AND ($4::bigint IS NULL OR start_ts >= $4) \
             AND ($5::bigint IS NULL OR start_ts <= $5)";
        let status_str = status.map(|s| s.as_str().to_string());
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM alarm WHERE {cond}"))
            .bind(tenant_id)
            .bind(originator_id)
            .bind(&status_str)
            .bind(start_ts)
            .bind(end_ts)
            .fetch_one(&self.pool)
            .await
            .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Alarm>(&format!(
            "SELECT * FROM alarm WHERE {cond} ORDER BY {sort} {order} LIMIT $6 OFFSET $7"
        ))
        .bind(tenant_id)
        .bind(originator_id)
        .bind(&status_str)
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

    /// housekeeper 用：删除 CLEARED_* 且 clear_ts 早于 cutoff 的告警，返回删除行数。
    pub async fn delete_cleared_older_than(&self, clear_ts: i64) -> TbResult<u64> {
        let res = sqlx::query(
            "DELETE FROM alarm WHERE status IN ('CLEARED_UNACK','CLEARED_ACK') \
             AND clear_ts IS NOT NULL AND clear_ts < $1",
        )
        .bind(clear_ts)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(res.rows_affected())
    }
}
