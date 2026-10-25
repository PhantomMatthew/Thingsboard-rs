//! tenant / tenant_profile 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::{Tenant, TenantProfile};
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct TenantDao {
    pool: PgPool,
}

impl TenantDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, tenant: &Tenant) -> TbResult<Tenant> {
        let mut t = tenant.clone();
        if t.created_time == 0 {
            t.created_time = now_ms();
        }
        sqlx::query_as::<_, Tenant>(
            "INSERT INTO tenant (id, created_time, title, region, country, state, city, \
             address, address2, zip, phone, email, additional_info, tenant_profile_id) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) \
             ON CONFLICT (id) DO UPDATE SET title=EXCLUDED.title, region=EXCLUDED.region, \
             country=EXCLUDED.country, state=EXCLUDED.state, city=EXCLUDED.city, \
             address=EXCLUDED.address, address2=EXCLUDED.address2, zip=EXCLUDED.zip, \
             phone=EXCLUDED.phone, email=EXCLUDED.email, additional_info=EXCLUDED.additional_info, \
             tenant_profile_id=EXCLUDED.tenant_profile_id RETURNING *",
        )
        .bind(t.id)
        .bind(t.created_time)
        .bind(&t.title)
        .bind(&t.region)
        .bind(&t.country)
        .bind(&t.state)
        .bind(&t.city)
        .bind(&t.address)
        .bind(&t.address2)
        .bind(&t.zip)
        .bind(&t.phone)
        .bind(&t.email)
        .bind(&t.additional_info)
        .bind(t.tenant_profile_id)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Tenant>> {
        sqlx::query_as::<_, Tenant>("SELECT * FROM tenant WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM tenant WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn find_all_ids(&self) -> TbResult<Vec<Uuid>> {
        sqlx::query_scalar("SELECT id FROM tenant ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_all(&self, page_link: &PageLink) -> TbResult<PageData<Tenant>> {
        let sort = page_link.sort_column(&["title", "email", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tenant WHERE ($1::text IS NULL OR LOWER(title) LIKE $1)",
        )
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Tenant>(&format!(
            "SELECT * FROM tenant WHERE ($1::text IS NULL OR LOWER(title) LIKE $1) \
             ORDER BY {sort} {order} LIMIT $2 OFFSET $3"
        ))
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

pub struct TenantProfileDao {
    pool: PgPool,
}

impl TenantProfileDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, profile: &TenantProfile) -> TbResult<TenantProfile> {
        let mut p = profile.clone();
        if p.created_time == 0 {
            p.created_time = now_ms();
        }
        sqlx::query_as::<_, TenantProfile>(
            "INSERT INTO tenant_profile (id, created_time, name, description, is_default, \
             profile_data) VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, \
             description=EXCLUDED.description, is_default=EXCLUDED.is_default, \
             profile_data=EXCLUDED.profile_data RETURNING *",
        )
        .bind(p.id)
        .bind(p.created_time)
        .bind(&p.name)
        .bind(&p.description)
        .bind(p.is_default)
        .bind(&p.profile_data)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<TenantProfile>> {
        sqlx::query_as::<_, TenantProfile>("SELECT * FROM tenant_profile WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    /// tenant_profile 为全局表（无 tenant_id 列），返回默认 profile。
    pub async fn find_default(&self) -> TbResult<Option<TenantProfile>> {
        sqlx::query_as::<_, TenantProfile>(
            "SELECT * FROM tenant_profile WHERE is_default = TRUE LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM tenant_profile WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }
}
