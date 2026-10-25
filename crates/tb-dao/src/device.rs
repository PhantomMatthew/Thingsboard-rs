//! device 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Device;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct DeviceDao {
    pool: PgPool,
}

impl DeviceDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, device: &Device) -> TbResult<Device> {
        let mut d = device.clone();
        if d.created_time == 0 {
            d.created_time = now_ms();
        }
        sqlx::query_as::<_, Device>(
            "INSERT INTO device (id, created_time, tenant_id, customer_id, \
             device_profile_id, name, type, label, external_id, firmware_id, software_id, \
             additional_info, device_data) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, \
             customer_id=EXCLUDED.customer_id, device_profile_id=EXCLUDED.device_profile_id, \
             name=EXCLUDED.name, type=EXCLUDED.type, label=EXCLUDED.label, \
             external_id=EXCLUDED.external_id, firmware_id=EXCLUDED.firmware_id, \
             software_id=EXCLUDED.software_id, additional_info=EXCLUDED.additional_info, \
             device_data=EXCLUDED.device_data RETURNING *",
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
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Device>> {
        sqlx::query_as::<_, Device>("SELECT * FROM device WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn assign_customer_if_unassigned(
        &self,
        device_id: Uuid,
        tenant_id: Uuid,
        customer_id: Uuid,
    ) -> TbResult<Option<Device>> {
        sqlx::query_as::<_, Device>(
            "UPDATE device SET customer_id = $3 \
             WHERE id = $1 AND tenant_id = $2 AND customer_id IS NULL RETURNING *",
        )
        .bind(device_id)
        .bind(tenant_id)
        .bind(customer_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_name(&self, tenant_id: Uuid, name: &str) -> TbResult<Option<Device>> {
        sqlx::query_as::<_, Device>("SELECT * FROM device WHERE tenant_id = $1 AND name = $2")
            .bind(tenant_id)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    /// 档案下全部设备 id（profile 级 OTA 绑定后批量通知用）。
    pub async fn find_ids_by_profile_id(&self, device_profile_id: Uuid) -> TbResult<Vec<Uuid>> {
        sqlx::query_scalar("SELECT id FROM device WHERE device_profile_id = $1")
            .bind(device_profile_id)
            .fetch_all(&self.pool)
            .await
            .map_err(TbError::db)
    }

    /// 该档案下没有设备级 OTA 绑定、继承档案绑定的设备。
    pub async fn find_ids_inheriting_ota(
        &self,
        device_profile_id: Uuid,
        software: bool,
    ) -> TbResult<Vec<Uuid>> {
        let sql = if software {
            "SELECT id FROM device WHERE device_profile_id = $1 AND software_id IS NULL"
        } else {
            "SELECT id FROM device WHERE device_profile_id = $1 AND firmware_id IS NULL"
        };
        sqlx::query_scalar(sql)
            .bind(device_profile_id)
            .fetch_all(&self.pool)
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

    pub async fn delete_by_id_in(&self, tx: &mut sqlx::PgConnection, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM device WHERE id = $1")
            .bind(id)
            .execute(tx)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    /// 全表分页扫描（EDQS 快照加载用），按 id 排序保证翻页稳定。
    pub async fn scan_all(&self, offset: i64, limit: i64) -> TbResult<Vec<Device>> {
        sqlx::query_as::<_, Device>("SELECT * FROM device ORDER BY id LIMIT $1 OFFSET $2")
            .bind(limit)
            .bind(offset)
            .fetch_all(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_all_by_tenant(&self, tenant_id: Uuid) -> TbResult<Vec<Device>> {
        sqlx::query_as::<_, Device>("SELECT * FROM device WHERE tenant_id = $1 ORDER BY id")
            .bind(tenant_id)
            .fetch_all(&self.pool)
            .await
            .map_err(TbError::db)
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
        Ok(PageData::new(
            data,
            total,
            page_link.page,
            page_link.page_size,
        ))
    }

    pub async fn find_by_transport_type(
        &self,
        tenant_id: Option<Uuid>,
        transport_type: &str,
        page_link: &PageLink,
    ) -> TbResult<PageData<Device>> {
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM device d \
             JOIN device_profile p ON p.id = d.device_profile_id \
             WHERE p.transport_type = $1 AND ($2::uuid IS NULL OR d.tenant_id = $2)",
        )
        .bind(transport_type)
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Device>(
            "SELECT d.* FROM device d \
             JOIN device_profile p ON p.id = d.device_profile_id \
             WHERE p.transport_type = $1 AND ($2::uuid IS NULL OR d.tenant_id = $2) \
             ORDER BY d.created_time DESC, d.id DESC LIMIT $3 OFFSET $4",
        )
        .bind(transport_type)
        .bind(tenant_id)
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
        Ok(PageData::new(
            data,
            total,
            page_link.page,
            page_link.page_size,
        ))
    }
}
