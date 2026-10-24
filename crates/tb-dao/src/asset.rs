//! asset 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Asset;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct AssetDao {
    pool: PgPool,
}

impl AssetDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, asset: &Asset) -> TbResult<Asset> {
        let mut a = asset.clone();
        if a.created_time == 0 {
            a.created_time = now_ms();
        }
        sqlx::query_as::<_, Asset>(
            "INSERT INTO asset (id, created_time, tenant_id, customer_id, asset_profile_id, \
             name, label, external_id, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, \
             customer_id=EXCLUDED.customer_id, asset_profile_id=EXCLUDED.asset_profile_id, \
             name=EXCLUDED.name, label=EXCLUDED.label, external_id=EXCLUDED.external_id, \
             additional_info=EXCLUDED.additional_info RETURNING *",
        )
        .bind(a.id)
        .bind(a.created_time)
        .bind(a.tenant_id)
        .bind(a.customer_id)
        .bind(a.asset_profile_id)
        .bind(&a.name)
        .bind(&a.label)
        .bind(a.external_id)
        .bind(&a.additional_info)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// 全表分页扫描（EDQS 快照加载用），按 id 排序保证翻页稳定。
    pub async fn scan_all(&self, offset: i64, limit: i64) -> TbResult<Vec<Asset>> {
        sqlx::query_as::<_, Asset>("SELECT * FROM asset ORDER BY id LIMIT $1 OFFSET $2")
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Asset>> {
        sqlx::query_as::<_, Asset>("SELECT * FROM asset WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM asset WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn delete_by_id_in(&self, tx: &mut sqlx::PgConnection, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM asset WHERE id = $1")
            .bind(id)
            .execute(tx)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn find_all_by_tenant(&self, tenant_id: Uuid) -> TbResult<Vec<Asset>> {
        sqlx::query_as::<_, Asset>("SELECT * FROM asset WHERE tenant_id = $1 ORDER BY id")
            .bind(tenant_id)
            .fetch_all(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_by_tenant_id(
        &self,
        tenant_id: Uuid,
        page_link: &PageLink,
    ) -> TbResult<PageData<Asset>> {
        let sort = page_link.sort_column(&["name", "label", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM asset WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(name) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Asset>(&format!(
            "SELECT * FROM asset WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(name) LIKE $2) \
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

    pub async fn find_by_customer_id(
        &self,
        tenant_id: Uuid,
        customer_id: Uuid,
        page_link: &PageLink,
    ) -> TbResult<PageData<Asset>> {
        let sort = page_link.sort_column(&["name", "label", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM asset WHERE tenant_id = $1 AND customer_id = $2 \
             AND ($3::text IS NULL OR LOWER(name) LIKE $3)",
        )
        .bind(tenant_id)
        .bind(customer_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Asset>(&format!(
            "SELECT * FROM asset WHERE tenant_id = $1 AND customer_id = $2 \
             AND ($3::text IS NULL OR LOWER(name) LIKE $3) \
             ORDER BY {sort} {order} LIMIT $4 OFFSET $5"
        ))
        .bind(tenant_id)
        .bind(customer_id)
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
