//! calculated_field 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::CalculatedField;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct CalculatedFieldDao {
    pool: PgPool,
}

impl CalculatedFieldDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, field: &CalculatedField) -> TbResult<CalculatedField> {
        let mut f = field.clone();
        if self.find_by_id(f.id).await?.is_some() {
            sqlx::query(
                "UPDATE calculated_field SET tenant_id=$2, entity_id=$3, name=$4, configuration=$5 \
                 WHERE id=$1",
            )
            .bind(f.id)
            .bind(f.tenant_id)
            .bind(f.entity_id)
            .bind(&f.name)
            .bind(&f.configuration)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if f.created_time == 0 {
                f.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO calculated_field (id, created_time, tenant_id, entity_id, name, configuration) \
                 VALUES ($1,$2,$3,$4,$5,$6)",
            )
            .bind(f.id)
            .bind(f.created_time)
            .bind(f.tenant_id)
            .bind(f.entity_id)
            .bind(&f.name)
            .bind(&f.configuration)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(f)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<CalculatedField>> {
        sqlx::query_as::<_, CalculatedField>("SELECT * FROM calculated_field WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_by_entity(&self, entity_id: Uuid) -> TbResult<Vec<CalculatedField>> {
        sqlx::query_as::<_, CalculatedField>(
            "SELECT * FROM calculated_field WHERE entity_id = $1 ORDER BY created_time",
        )
        .bind(entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_tenant(
        &self,
        tenant_id: Uuid,
        page_link: &PageLink,
    ) -> TbResult<PageData<CalculatedField>> {
        let sort = page_link.sort_column(&["name", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM calculated_field WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(name) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, CalculatedField>(&format!(
            "SELECT * FROM calculated_field WHERE tenant_id = $1 \
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

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM calculated_field WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }
}
