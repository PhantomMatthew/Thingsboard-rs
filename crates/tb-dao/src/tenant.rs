//! tenant / tenant_profile 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::{Tenant, TenantProfile};
use crate::page::{PageData, PageLink};
use crate::now_ms;

pub struct TenantDao {
    pool: PgPool,
}

impl TenantDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, tenant: &Tenant) -> TbResult<Tenant> {
        let mut t = tenant.clone();
        if self.find_by_id(t.id).await?.is_some() {
            sqlx::query(
                "UPDATE tenant SET title=$2, region=$3, country=$4, state=$5, city=$6, \
                 address=$7, address2=$8, zip=$9, phone=$10, email=$11, additional_info=$12, \
                 tenant_profile_id=$13 WHERE id=$1",
            )
            .bind(t.id)
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
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if t.created_time == 0 {
                t.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO tenant (id, created_time, title, region, country, state, city, \
                 address, address2, zip, phone, email, additional_info, tenant_profile_id) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
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
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(t)
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
        Ok(PageData::new(data, total, page_link.page, page_link.page_size))
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
        if self.find_by_id(p.id).await?.is_some() {
            sqlx::query(
                "UPDATE tenant_profile SET name=$2, description=$3, is_default=$4, \
                 profile_data=$5 WHERE id=$1",
            )
            .bind(p.id)
            .bind(&p.name)
            .bind(&p.description)
            .bind(p.is_default)
            .bind(&p.profile_data)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if p.created_time == 0 {
                p.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO tenant_profile (id, created_time, name, description, is_default, \
                 profile_data) VALUES ($1,$2,$3,$4,$5,$6)",
            )
            .bind(p.id)
            .bind(p.created_time)
            .bind(&p.name)
            .bind(&p.description)
            .bind(p.is_default)
            .bind(&p.profile_data)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(p)
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
