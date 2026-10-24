//! ota_package 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::OtaPackage;
use crate::now_ms;
use crate::page::{PageData, PageLink};

const META_COLUMNS: &str = "id, created_time, tenant_id, device_profile_id, type, title, version, \
     tag, file_name, content_type, checksum_algorithm, checksum, size, NULL::bytea AS data, \
     additional_info";

pub struct OtaPackageDao {
    pool: PgPool,
}

impl OtaPackageDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, package: &OtaPackage) -> TbResult<OtaPackage> {
        let mut p = package.clone();
        if p.created_time == 0 {
            p.created_time = now_ms();
        }
        sqlx::query_as::<_, OtaPackage>(
            "INSERT INTO ota_package (id, created_time, tenant_id, device_profile_id, type, \
             title, version, tag, file_name, content_type, checksum_algorithm, checksum, \
             size, data, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, \
             device_profile_id=EXCLUDED.device_profile_id, type=EXCLUDED.type, \
             title=EXCLUDED.title, version=EXCLUDED.version, tag=EXCLUDED.tag, \
             file_name=EXCLUDED.file_name, content_type=EXCLUDED.content_type, \
             checksum_algorithm=EXCLUDED.checksum_algorithm, checksum=EXCLUDED.checksum, \
             size=EXCLUDED.size, data=EXCLUDED.data, additional_info=EXCLUDED.additional_info \
             RETURNING *",
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
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<OtaPackage>> {
        sqlx::query_as::<_, OtaPackage>("SELECT * FROM ota_package WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_meta_by_id(&self, id: Uuid) -> TbResult<Option<OtaPackage>> {
        sqlx::query_as::<_, OtaPackage>(&format!(
            "SELECT {META_COLUMNS} FROM ota_package WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_data_chunk(
        &self,
        id: Uuid,
        start: i32,
        len: i32,
    ) -> TbResult<Option<Vec<u8>>> {
        let data = sqlx::query_scalar::<_, Option<Vec<u8>>>(
            "SELECT SUBSTRING(data FROM $2 FOR $3) FROM ota_package WHERE id = $1",
        )
        .bind(id)
        .bind(start)
        .bind(len)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)?
        .flatten();
        Ok(data)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM ota_package WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    /// 该档案下指定类型的最新 OTA 包：同 title（非空时）且版本与当前不同。
    pub async fn find_latest(
        &self,
        tenant_id: Uuid,
        device_profile_id: Uuid,
        r#type: &str,
        current_title: &str,
        current_version: &str,
    ) -> TbResult<Option<OtaPackage>> {
        sqlx::query_as::<_, OtaPackage>(&format!(
            "SELECT {META_COLUMNS} FROM ota_package WHERE tenant_id = $1 \
             AND device_profile_id = $2 AND type = $3 AND ($4 = '' OR title = $4) \
             AND version != $5 ORDER BY created_time DESC LIMIT 1"
        ))
        .bind(tenant_id)
        .bind(device_profile_id)
        .bind(r#type)
        .bind(current_title)
        .bind(current_version)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_meta_by_tenant_filtered(
        &self,
        tenant_id: Uuid,
        device_profile_id: Option<Uuid>,
        r#type: Option<&str>,
        page_link: &PageLink,
    ) -> TbResult<PageData<OtaPackage>> {
        let sort = page_link.sort_column(&["title", "version", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ota_package WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2) \
             AND ($3::uuid IS NULL OR device_profile_id = $3) \
             AND ($4::text IS NULL OR type = $4)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .bind(device_profile_id)
        .bind(r#type)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, OtaPackage>(&format!(
            "SELECT {META_COLUMNS} FROM ota_package WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2) \
             AND ($3::uuid IS NULL OR device_profile_id = $3) \
             AND ($4::text IS NULL OR type = $4) \
             ORDER BY {sort} {order} LIMIT $5 OFFSET $6"
        ))
        .bind(tenant_id)
        .bind(&pattern)
        .bind(device_profile_id)
        .bind(r#type)
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
