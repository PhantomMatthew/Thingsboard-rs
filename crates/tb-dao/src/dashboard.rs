//! dashboard 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Dashboard;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct DashboardDao {
    pool: PgPool,
}

impl DashboardDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, dashboard: &Dashboard) -> TbResult<Dashboard> {
        let mut d = dashboard.clone();
        if d.created_time == 0 {
            d.created_time = now_ms();
        }
        sqlx::query_as::<_, Dashboard>(
            "INSERT INTO dashboard (id, created_time, tenant_id, title, configuration, \
             assigned_customers, mobile_hide, mobile_order, image, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, title=EXCLUDED.title, \
             configuration=EXCLUDED.configuration, assigned_customers=EXCLUDED.assigned_customers, \
             mobile_hide=EXCLUDED.mobile_hide, mobile_order=EXCLUDED.mobile_order, \
             image=EXCLUDED.image, additional_info=EXCLUDED.additional_info RETURNING *",
        )
        .bind(d.id)
        .bind(d.created_time)
        .bind(d.tenant_id)
        .bind(&d.title)
        .bind(&d.configuration)
        .bind(&d.assigned_customers)
        .bind(d.mobile_hide)
        .bind(d.mobile_order)
        .bind(&d.image)
        .bind(&d.additional_info)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Dashboard>> {
        sqlx::query_as::<_, Dashboard>("SELECT * FROM dashboard WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM dashboard WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn find_by_tenant_id(
        &self,
        tenant_id: Uuid,
        page_link: &PageLink,
    ) -> TbResult<PageData<Dashboard>> {
        let sort = page_link.sort_column(&["title", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM dashboard WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Dashboard>(&format!(
            "SELECT * FROM dashboard WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2) \
             ORDER BY {sort} {order} LIMIT $3 OFFSET $4"
        ))
        .bind(tenant_id)
        .bind(&pattern)
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
}
