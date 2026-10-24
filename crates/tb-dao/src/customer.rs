//! customer 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Customer;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct CustomerDao {
    pool: PgPool,
}

impl CustomerDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, customer: &Customer) -> TbResult<Customer> {
        let mut c = customer.clone();
        if c.created_time == 0 {
            c.created_time = now_ms();
        }
        sqlx::query_as::<_, Customer>(
            "INSERT INTO customer (id, created_time, tenant_id, title, country, state, city, \
             address, address2, zip, phone, email, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, title=EXCLUDED.title, \
             country=EXCLUDED.country, state=EXCLUDED.state, city=EXCLUDED.city, \
             address=EXCLUDED.address, address2=EXCLUDED.address2, zip=EXCLUDED.zip, \
             phone=EXCLUDED.phone, email=EXCLUDED.email, additional_info=EXCLUDED.additional_info \
             RETURNING *",
        )
        .bind(c.id)
        .bind(c.created_time)
        .bind(c.tenant_id)
        .bind(&c.title)
        .bind(&c.country)
        .bind(&c.state)
        .bind(&c.city)
        .bind(&c.address)
        .bind(&c.address2)
        .bind(&c.zip)
        .bind(&c.phone)
        .bind(&c.email)
        .bind(&c.additional_info)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Customer>> {
        sqlx::query_as::<_, Customer>("SELECT * FROM customer WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM customer WHERE id = $1")
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
    ) -> TbResult<PageData<Customer>> {
        let sort = page_link.sort_column(&["title", "email", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM customer WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Customer>(&format!(
            "SELECT * FROM customer WHERE tenant_id = $1 \
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
