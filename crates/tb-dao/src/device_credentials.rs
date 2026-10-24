//! device_credentials 数据访问，含设备+凭证的事务保存。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::{Device, DeviceCredentials};
use crate::now_ms;

pub struct DeviceCredentialsDao {
    pool: PgPool,
}

impl DeviceCredentialsDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, credentials: &DeviceCredentials) -> TbResult<DeviceCredentials> {
        let mut c = credentials.clone();
        if c.created_time == 0 {
            c.created_time = now_ms();
        }
        sqlx::query_as::<_, DeviceCredentials>(
            "INSERT INTO device_credentials (id, created_time, device_id, credentials_type, \
             credentials_id, credentials_value) VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT (device_id) DO UPDATE SET credentials_type=EXCLUDED.credentials_type, \
             credentials_id=EXCLUDED.credentials_id, credentials_value=EXCLUDED.credentials_value \
             RETURNING *",
        )
        .bind(c.id)
        .bind(c.created_time)
        .bind(c.device_id)
        .bind(c.credentials_type)
        .bind(&c.credentials_id)
        .bind(&c.credentials_value)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<DeviceCredentials>> {
        sqlx::query_as::<_, DeviceCredentials>("SELECT * FROM device_credentials WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_by_device_id(&self, device_id: Uuid) -> TbResult<Option<DeviceCredentials>> {
        sqlx::query_as::<_, DeviceCredentials>(
            "SELECT * FROM device_credentials WHERE device_id = $1",
        )
        .bind(device_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// 按查找键（token / cert hash / mqtt clientId|userName）查凭证。
    pub async fn find_by_credentials_id(
        &self,
        credentials_id: &str,
    ) -> TbResult<Option<DeviceCredentials>> {
        sqlx::query_as::<_, DeviceCredentials>(
            "SELECT * FROM device_credentials WHERE credentials_id = $1",
        )
        .bind(credentials_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn delete_by_device_id(&self, device_id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM device_credentials WHERE device_id = $1")
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    /// 在同一事务中保存设备及其凭证：设备按 id、凭证按 device_id upsert。
    pub async fn save_device_with_credentials(
        &self,
        device: &Device,
        credentials: &DeviceCredentials,
    ) -> TbResult<(Device, DeviceCredentials)> {
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;
        let device_created_time = if device.created_time == 0 {
            now_ms()
        } else {
            device.created_time
        };
        let d = sqlx::query_as::<_, Device>(
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
        .bind(device.id)
        .bind(device_created_time)
        .bind(device.tenant_id)
        .bind(device.customer_id)
        .bind(device.device_profile_id)
        .bind(&device.name)
        .bind(&device.r#type)
        .bind(&device.label)
        .bind(device.external_id)
        .bind(device.firmware_id)
        .bind(device.software_id)
        .bind(&device.additional_info)
        .bind(&device.device_data)
        .fetch_one(&mut *tx)
        .await
        .map_err(TbError::db)?;

        let credentials_created_time = if credentials.created_time == 0 {
            now_ms()
        } else {
            credentials.created_time
        };
        let c = sqlx::query_as::<_, DeviceCredentials>(
            "INSERT INTO device_credentials (id, created_time, device_id, credentials_type, \
             credentials_id, credentials_value) VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT (device_id) DO UPDATE SET credentials_type=EXCLUDED.credentials_type, \
             credentials_id=EXCLUDED.credentials_id, credentials_value=EXCLUDED.credentials_value \
             RETURNING *",
        )
        .bind(credentials.id)
        .bind(credentials_created_time)
        .bind(device.id)
        .bind(credentials.credentials_type)
        .bind(&credentials.credentials_id)
        .bind(&credentials.credentials_value)
        .fetch_one(&mut *tx)
        .await
        .map_err(TbError::db)?;

        tx.commit().await.map_err(TbError::db)?;
        Ok((d, c))
    }
}
