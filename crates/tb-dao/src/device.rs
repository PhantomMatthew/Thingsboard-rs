//! device 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Device;
use crate::page::{PageData, PageLink};
use crate::now_ms;

pub struct DeviceDao {
    pool: PgPool,
}

impl DeviceDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, device: &Device) -> TbResult<Device> {
        let mut d = device.clone();
        if self.find_by_id(d.id).await?.is_some() {
            sqlx::query(
                "UPDATE device SET tenant_id=$2, customer_id=$3, device_profile_id=$4, name=$5, \
                 type=$6, label=$7, external_id=$8, firmware_id=$9, software_id=$10, \
                 additional_info=$11, device_data=$12 WHERE id=$1",
            )
            .bind(d.id)
            .bind(d.tenant_id)
            .bind(d.customer_id)
            .bind(d.device_profile_id)
            .bind(&d.name)
            .bind(&d.r#type)
            .bind(&d.label)
            .bind(d.external_id)
            .bind(d.firmware_id)
            .bind(d.software_id)
            .bind(&d.additional_info)
            .bind(&d.device_data)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if d.created_time == 0 {
                d.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO device (id, created_time, tenant_id, customer_id, \
                 device_profile_id, name, type, label, external_id, firmware_id, software_id, \
                 additional_info, device_data) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            )
            .bind(d.id)
            .bind(d.created_time)
            .bind(d.tenant_id)
            .bind(d.customer_id)
            .bind(d.device_profile_id)
            .bind(&d.name)
            .bind(&d.r#type)
            .bind(&d.label)
            .bind(d.external_id)
            .bind(d.firmware_id)
            .bind(d.software_id)
            .bind(&d.additional_info)
            .bind(&d.device_data)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(d)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Device>> {
        sqlx::query_as::<_, Device>("SELECT * FROM device WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM device WHERE id = $1")
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
    ) -> TbResult<PageData<Device>> {
        let sort = page_link.sort_column(&["name", "type", "label", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM device WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(name) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Device>(&format!(
            "SELECT * FROM device WHERE tenant_id = $1 \
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

    pub async fn find_by_customer_id(
        &self,
        tenant_id: Uuid,
        customer_id: Uuid,
        page_link: &PageLink,
    ) -> TbResult<PageData<Device>> {
        let sort = page_link.sort_column(&["name", "type", "label", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM device WHERE tenant_id = $1 AND customer_id = $2 \
             AND ($3::text IS NULL OR LOWER(name) LIKE $3)",
        )
        .bind(tenant_id)
        .bind(customer_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Device>(&format!(
            "SELECT * FROM device WHERE tenant_id = $1 AND customer_id = $2 \
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
        Ok(PageData::new(data, total, page_link.page, page_link.page_size))
    }
}
