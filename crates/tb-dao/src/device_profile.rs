//! device_profile 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::DeviceProfile;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct DeviceProfileDao {
    pool: PgPool,
}

impl DeviceProfileDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, profile: &DeviceProfile) -> TbResult<DeviceProfile> {
        let mut p = profile.clone();
        if p.created_time == 0 {
            p.created_time = now_ms();
        }
        sqlx::query_as::<_, DeviceProfile>(
            "INSERT INTO device_profile (id, created_time, tenant_id, name, description, \
             is_default, transport_type, provision_type, profile_data, firmware_id, \
             software_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, name=EXCLUDED.name, \
             description=EXCLUDED.description, is_default=EXCLUDED.is_default, \
             transport_type=EXCLUDED.transport_type, provision_type=EXCLUDED.provision_type, \
             profile_data=EXCLUDED.profile_data, firmware_id=EXCLUDED.firmware_id, \
             software_id=EXCLUDED.software_id RETURNING *",
        )
        .bind(p.id)
        .bind(p.created_time)
        .bind(p.tenant_id)
        .bind(&p.name)
        .bind(&p.description)
        .bind(p.is_default)
        .bind(&p.transport_type)
        .bind(&p.provision_type)
        .bind(&p.profile_data)
        .bind(p.firmware_id)
        .bind(p.software_id)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<DeviceProfile>> {
        sqlx::query_as::<_, DeviceProfile>("SELECT * FROM device_profile WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_default(&self, tenant_id: Uuid) -> TbResult<Option<DeviceProfile>> {
        sqlx::query_as::<_, DeviceProfile>(
            "SELECT * FROM device_profile WHERE tenant_id = $1 AND is_default = TRUE LIMIT 1",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// 按 provision key 匹配档案（provisioning 用）；tenant_id 为 None 时全局查找。
    pub async fn find_by_provision_key(
        &self,
        tenant_id: Option<Uuid>,
        key: &str,
    ) -> TbResult<Option<DeviceProfile>> {
        sqlx::query_as::<_, DeviceProfile>(
            "SELECT * FROM device_profile \
             WHERE provision_type IN ('ALLOW_CREATE_NEW_DEVICES', 'CHECK_PRE_PROVISIONED_DEVICES') \
             AND profile_data->'provisionConfiguration'->>'provisionDeviceKey' = $1 \
             AND $1 <> '' AND ($2::uuid IS NULL OR tenant_id = $2)",
        )
        .bind(key)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_x509_chain_by_certificate_hash(
        &self,
        certificate_hash: &str,
    ) -> TbResult<Vec<DeviceProfile>> {
        sqlx::query_as::<_, DeviceProfile>(
            "SELECT * FROM device_profile WHERE provision_type = 'X509_CERTIFICATE_CHAIN' \
             AND profile_data->'provisionConfiguration'->>'provisionDeviceKey' = $1",
        )
        .bind(certificate_hash)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM device_profile WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                crate::referenced_error(
                    e,
                    "device profile is referenced by devices or OTA packages",
                )
            })?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn find_by_tenant_id(
        &self,
        tenant_id: Uuid,
        page_link: &PageLink,
    ) -> TbResult<PageData<DeviceProfile>> {
        let sort = page_link.sort_column(&["name", "transport_type", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM device_profile WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(name) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, DeviceProfile>(&format!(
            "SELECT * FROM device_profile WHERE tenant_id = $1 \
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
}
