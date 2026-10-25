//! tb_user / user_credentials 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::{User, UserCredentials};
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct UserDao {
    pool: PgPool,
}

impl UserDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, user: &User) -> TbResult<User> {
        let mut u = user.clone();
        if u.created_time == 0 {
            u.created_time = now_ms();
        }
        sqlx::query_as::<_, User>(
            "INSERT INTO tb_user (id, created_time, tenant_id, customer_id, authority, \
             email, first_name, last_name, phone, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, \
             customer_id=EXCLUDED.customer_id, authority=EXCLUDED.authority, email=EXCLUDED.email, \
             first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, phone=EXCLUDED.phone, \
             additional_info=EXCLUDED.additional_info RETURNING *",
        )
        .bind(u.id)
        .bind(u.created_time)
        .bind(u.tenant_id)
        .bind(u.customer_id)
        .bind(u.authority)
        .bind(&u.email)
        .bind(&u.first_name)
        .bind(&u.last_name)
        .bind(&u.phone)
        .bind(&u.additional_info)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<User>> {
        sqlx::query_as::<_, User>("SELECT * FROM tb_user WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_by_email(&self, email: &str) -> TbResult<Option<User>> {
        sqlx::query_as::<_, User>("SELECT * FROM tb_user WHERE email = $1")
            .bind(email)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM tb_user WHERE id = $1")
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
    ) -> TbResult<PageData<User>> {
        let sort = page_link.sort_column(&["email", "first_name", "last_name", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tb_user WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(email) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, User>(&format!(
            "SELECT * FROM tb_user WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(email) LIKE $2) \
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
    ) -> TbResult<PageData<User>> {
        let sort = page_link.sort_column(&["email", "first_name", "last_name", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tb_user WHERE tenant_id = $1 AND customer_id = $2 \
             AND ($3::text IS NULL OR LOWER(email) LIKE $3)",
        )
        .bind(tenant_id)
        .bind(customer_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, User>(&format!(
            "SELECT * FROM tb_user WHERE tenant_id = $1 AND customer_id = $2 \
             AND ($3::text IS NULL OR LOWER(email) LIKE $3) \
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

pub struct UserCredentialsDao {
    pool: PgPool,
}

impl UserCredentialsDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, credentials: &UserCredentials) -> TbResult<UserCredentials> {
        let mut c = credentials.clone();
        if c.created_time == 0 {
            c.created_time = now_ms();
        }
        sqlx::query_as::<_, UserCredentials>(
            "INSERT INTO user_credentials (id, created_time, user_id, enabled, password, \
             activate_token, reset_token, failed_login_attempts, last_login_ts, password_changed_ts) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT (user_id) DO UPDATE SET enabled=$4, password=$5, activate_token=$6, \
             reset_token=$7, failed_login_attempts=$8, last_login_ts=$9, password_changed_ts=$10 \
             RETURNING *",
        )
        .bind(c.id)
        .bind(c.created_time)
        .bind(c.user_id)
        .bind(c.enabled)
        .bind(&c.password)
        .bind(&c.activate_token)
        .bind(&c.reset_token)
        .bind(c.failed_login_attempts)
        .bind(c.last_login_ts)
        .bind(c.password_changed_ts)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn record_login_success(
        &self,
        user_id: Uuid,
        last_login_ts: i64,
        expected_password: &str,
    ) -> TbResult<Option<UserCredentials>> {
        sqlx::query_as::<_, UserCredentials>(
            "UPDATE user_credentials SET failed_login_attempts=0, last_login_ts=$2 \
             WHERE user_id=$1 AND enabled AND password=$3 RETURNING *",
        )
        .bind(user_id)
        .bind(last_login_ts)
        .bind(expected_password)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn record_login_failure(&self, user_id: Uuid) -> TbResult<i32> {
        sqlx::query_scalar(
            "UPDATE user_credentials SET failed_login_attempts=failed_login_attempts+1 \
             WHERE user_id=$1 RETURNING failed_login_attempts",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn reset_failed_login_attempts(&self, user_id: Uuid) -> TbResult<()> {
        sqlx::query("UPDATE user_credentials SET failed_login_attempts=0 WHERE user_id=$1")
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(())
    }

    pub async fn update_password(
        &self,
        user_id: Uuid,
        password: &str,
        password_changed_ts: i64,
        enable: bool,
    ) -> TbResult<Option<UserCredentials>> {
        sqlx::query_as::<_, UserCredentials>(
            "UPDATE user_credentials SET password=$2, activate_token=NULL, reset_token=NULL, \
             password_changed_ts=GREATEST(COALESCE(password_changed_ts,$3),$3), \
             enabled=enabled OR $4 WHERE user_id=$1 RETURNING *",
        )
        .bind(user_id)
        .bind(password)
        .bind(password_changed_ts)
        .bind(enable)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn upsert_password(
        &self,
        user_id: Uuid,
        password: &str,
        password_changed_ts: i64,
    ) -> TbResult<UserCredentials> {
        sqlx::query_as::<_, UserCredentials>(
            "INSERT INTO user_credentials (id, created_time, user_id, enabled, password, \
             activate_token, reset_token, failed_login_attempts, last_login_ts, password_changed_ts) \
             VALUES ($1,$2,$3,true,$4,NULL,NULL,0,NULL,$5) \
             ON CONFLICT (user_id) DO UPDATE SET password=$4, activate_token=NULL, reset_token=NULL, \
             password_changed_ts=GREATEST(COALESCE(user_credentials.password_changed_ts,$5),$5), \
             enabled=true RETURNING *",
        )
        .bind(Uuid::now_v7())
        .bind(now_ms())
        .bind(user_id)
        .bind(password)
        .bind(password_changed_ts)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_user_id(&self, user_id: Uuid) -> TbResult<Option<UserCredentials>> {
        sqlx::query_as::<_, UserCredentials>("SELECT * FROM user_credentials WHERE user_id = $1")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }
}
