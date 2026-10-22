//! device_profile 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::DeviceProfile;
use crate::page::{PageData, PageLink};
use crate::now_ms;

pub struct DeviceProfileDao {
    pool: PgPool,
}

impl DeviceProfileDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, profile: &DeviceProfile) -> TbResult<DeviceProfile> {
        let mut p = profile.clone();
        if self.find_by_id(p.id).await?.is_some() {
            sqlx::query(
                "UPDATE device_profile SET tenant_id=$2, name=$3, description=$4, is_default=$5, \
                 transport_type=$6, provision_type=$7, profile_data=$8, firmware_id=$9, \
                 software_id=$10 WHERE id=$1",
            )
            .bind(p.id)
            .bind(p.tenant_id)
            .bind(&p.name)
            .bind(&p.description)
            .bind(p.is_default)
            .bind(&p.transport_type)
            .bind(&p.provision_type)
            .bind(&p.profile_data)
            .bind(p.firmware_id)
            .bind(p.software_id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if p.created_time == 0 {
                p.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO device_profile (id, created_time, tenant_id, name, description, \
                 is_default, transport_type, provision_type, profile_data, firmware_id, \
                 software_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
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
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(p)
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

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM device_profile WHERE id = $1")
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
        Ok(PageData::new(data, total, page_link.page, page_link.page_size))
    }
}
