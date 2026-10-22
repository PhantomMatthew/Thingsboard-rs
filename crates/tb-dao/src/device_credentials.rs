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
        sqlx::query(
            "INSERT INTO device_credentials (id, created_time, device_id, credentials_type, \
             credentials_id, credentials_value) VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT (device_id) DO UPDATE SET credentials_type=$4, credentials_id=$5, \
             credentials_value=$6",
        )
        .bind(c.id)
        .bind(c.created_time)
        .bind(c.device_id)
        .bind(c.credentials_type)
        .bind(&c.credentials_id)
        .bind(&c.credentials_value)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(c)
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

    /// 在同一事务中保存设备及其凭证：设备走 insert/update，凭证按 device_id upsert。
    pub async fn save_device_with_credentials(
        &self,
        device: &Device,
        credentials: &DeviceCredentials,
    ) -> TbResult<(Device, DeviceCredentials)> {
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;

        let mut d = device.clone();
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM device WHERE id = $1)")
                .bind(d.id)
                .fetch_one(&mut *tx)
                .await
                .map_err(TbError::db)?;
        if exists {
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
            .execute(&mut *tx)
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
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        }

        let mut c = credentials.clone();
        c.device_id = d.id;
        if c.created_time == 0 {
            c.created_time = now_ms();
        }
        sqlx::query(
            "INSERT INTO device_credentials (id, created_time, device_id, credentials_type, \
             credentials_id, credentials_value) VALUES ($1,$2,$3,$4,$5,$6) \
             ON CONFLICT (device_id) DO UPDATE SET credentials_type=$4, credentials_id=$5, \
             credentials_value=$6",
        )
        .bind(c.id)
        .bind(c.created_time)
        .bind(c.device_id)
        .bind(c.credentials_type)
        .bind(&c.credentials_id)
        .bind(&c.credentials_value)
        .execute(&mut *tx)
        .await
        .map_err(TbError::db)?;

        tx.commit().await.map_err(TbError::db)?;
        Ok((d, c))
    }
}
