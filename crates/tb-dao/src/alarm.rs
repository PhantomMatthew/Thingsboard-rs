//! alarm 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Alarm;
use crate::page::{PageData, PageLink};
use crate::now_ms;

pub struct AlarmDao {
    pool: PgPool,
}

impl AlarmDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, alarm: &Alarm) -> TbResult<Alarm> {
        let mut a = alarm.clone();
        if self.find_by_id(a.id).await?.is_some() {
            sqlx::query(
                "UPDATE alarm SET tenant_id=$2, originator_id=$3, originator_type=$4, type=$5, \
                 severity=$6, status=$7, start_ts=$8, end_ts=$9, ack_ts=$10, clear_ts=$11, \
                 details=$12, propagate=$13, propagate_to_owner=$14, propagate_to_tenant=$15, \
                 propagate_relation_types=$16, customer_id=$17, assignee_id=$18, assign_ts=$19 \
                 WHERE id=$1",
            )
            .bind(a.id)
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
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if a.created_time == 0 {
                a.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO alarm (id, created_time, tenant_id, originator_id, originator_type, \
                 type, severity, status, start_ts, end_ts, ack_ts, clear_ts, details, propagate, \
                 propagate_to_owner, propagate_to_tenant, propagate_relation_types, customer_id, \
                 assignee_id, assign_ts) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)",
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
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(a)
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

    pub async fn find_by_tenant(
        &self,
        tenant_id: Uuid,
        originator_id: Option<Uuid>,
        page_link: &PageLink,
    ) -> TbResult<PageData<Alarm>> {
        let sort = page_link.sort_column(&["created_time", "start_ts", "severity", "status"]);
        let order = page_link.sort_order_sql();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM alarm WHERE tenant_id = $1 \
             AND ($2::uuid IS NULL OR originator_id = $2)",
        )
        .bind(tenant_id)
        .bind(originator_id)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Alarm>(&format!(
            "SELECT * FROM alarm WHERE tenant_id = $1 \
             AND ($2::uuid IS NULL OR originator_id = $2) \
             ORDER BY {sort} {order} LIMIT $3 OFFSET $4"
        ))
        .bind(tenant_id)
        .bind(originator_id)
        .bind(page_link.limit())
        .bind(page_link.offset())
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(PageData::new(data, total, page_link.page, page_link.page_size))
    }
}
