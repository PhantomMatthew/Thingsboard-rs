//! ota_package 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::OtaPackage;
use crate::page::{PageData, PageLink};
use crate::now_ms;

pub struct OtaPackageDao {
    pool: PgPool,
}

impl OtaPackageDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, package: &OtaPackage) -> TbResult<OtaPackage> {
        let mut p = package.clone();
        if self.find_by_id(p.id).await?.is_some() {
            sqlx::query(
                "UPDATE ota_package SET tenant_id=$2, device_profile_id=$3, type=$4, title=$5, \
                 version=$6, tag=$7, file_name=$8, content_type=$9, checksum_algorithm=$10, \
                 checksum=$11, size=$12, data=$13, additional_info=$14 WHERE id=$1",
            )
            .bind(p.id)
            .bind(p.tenant_id)
            .bind(p.device_profile_id)
            .bind(&p.r#type)
            .bind(&p.title)
            .bind(&p.version)
            .bind(&p.tag)
            .bind(&p.file_name)
            .bind(&p.content_type)
            .bind(&p.checksum_algorithm)
            .bind(&p.checksum)
            .bind(p.size)
            .bind(&p.data)
            .bind(&p.additional_info)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if p.created_time == 0 {
                p.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO ota_package (id, created_time, tenant_id, device_profile_id, type, \
                 title, version, tag, file_name, content_type, checksum_algorithm, checksum, \
                 size, data, additional_info) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
            )
            .bind(p.id)
            .bind(p.created_time)
            .bind(p.tenant_id)
            .bind(p.device_profile_id)
            .bind(&p.r#type)
            .bind(&p.title)
            .bind(&p.version)
            .bind(&p.tag)
            .bind(&p.file_name)
            .bind(&p.content_type)
            .bind(&p.checksum_algorithm)
            .bind(&p.checksum)
            .bind(p.size)
            .bind(&p.data)
            .bind(&p.additional_info)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(p)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<OtaPackage>> {
        sqlx::query_as::<_, OtaPackage>("SELECT * FROM ota_package WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM ota_package WHERE id = $1")
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
    ) -> TbResult<PageData<OtaPackage>> {
        let sort = page_link.sort_column(&["title", "version", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ota_package WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, OtaPackage>(&format!(
            "SELECT * FROM ota_package WHERE tenant_id = $1 \
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
