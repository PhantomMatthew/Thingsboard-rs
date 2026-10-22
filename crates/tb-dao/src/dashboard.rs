//! dashboard 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Dashboard;
use crate::page::{PageData, PageLink};
use crate::now_ms;

pub struct DashboardDao {
    pool: PgPool,
}

impl DashboardDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, dashboard: &Dashboard) -> TbResult<Dashboard> {
        let mut d = dashboard.clone();
        if self.find_by_id(d.id).await?.is_some() {
            sqlx::query(
                "UPDATE dashboard SET tenant_id=$2, title=$3, configuration=$4, \
                 assigned_customers=$5, mobile_hide=$6, mobile_order=$7, image=$8, \
                 additional_info=$9 WHERE id=$1",
            )
            .bind(d.id)
            .bind(d.tenant_id)
            .bind(&d.title)
            .bind(&d.configuration)
            .bind(&d.assigned_customers)
            .bind(d.mobile_hide)
            .bind(d.mobile_order)
            .bind(&d.image)
            .bind(&d.additional_info)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if d.created_time == 0 {
                d.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO dashboard (id, created_time, tenant_id, title, configuration, \
                 assigned_customers, mobile_hide, mobile_order, image, additional_info) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
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
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(d)
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
        Ok(PageData::new(data, total, page_link.page, page_link.page_size))
    }
}
