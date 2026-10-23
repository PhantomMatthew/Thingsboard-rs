//! oauth2_client 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::Oauth2Client;
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct Oauth2ClientDao {
    pool: PgPool,
}

impl Oauth2ClientDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, client: &Oauth2Client) -> TbResult<Oauth2Client> {
        let mut c = client.clone();
        if self.find_by_id(c.id).await?.is_some() {
            sqlx::query(
                "UPDATE oauth2_client SET tenant_id=$2, title=$3, client_id=$4, client_secret=$5, \
                 authorization_uri=$6, token_uri=$7, user_info_uri=$8, redirect_uri_template=$9, \
                 scope=$10, enabled=$11 WHERE id=$1",
            )
            .bind(c.id)
            .bind(c.tenant_id)
            .bind(&c.title)
            .bind(&c.client_id)
            .bind(&c.client_secret)
            .bind(&c.authorization_uri)
            .bind(&c.token_uri)
            .bind(&c.user_info_uri)
            .bind(&c.redirect_uri_template)
            .bind(&c.scope)
            .bind(c.enabled)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if c.created_time == 0 {
                c.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO oauth2_client (id, created_time, tenant_id, title, client_id, \
                 client_secret, authorization_uri, token_uri, user_info_uri, \
                 redirect_uri_template, scope, enabled) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            )
            .bind(c.id)
            .bind(c.created_time)
            .bind(c.tenant_id)
            .bind(&c.title)
            .bind(&c.client_id)
            .bind(&c.client_secret)
            .bind(&c.authorization_uri)
            .bind(&c.token_uri)
            .bind(&c.user_info_uri)
            .bind(&c.redirect_uri_template)
            .bind(&c.scope)
            .bind(c.enabled)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(c)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<Oauth2Client>> {
        sqlx::query_as::<_, Oauth2Client>("SELECT * FROM oauth2_client WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_by_client_id(&self, client_id: &str) -> TbResult<Option<Oauth2Client>> {
        sqlx::query_as::<_, Oauth2Client>("SELECT * FROM oauth2_client WHERE client_id = $1")
            .bind(client_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    /// 登录页公开列表：全部启用的客户端。
    pub async fn find_enabled(&self) -> TbResult<Vec<Oauth2Client>> {
        sqlx::query_as::<_, Oauth2Client>(
            "SELECT * FROM oauth2_client WHERE enabled = true ORDER BY created_time",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// 管理端分页：tenant_id 为 None 时全量（SYS_ADMIN），否则仅该租户。
    pub async fn find_paged(
        &self,
        tenant_id: Option<Uuid>,
        page_link: &PageLink,
    ) -> TbResult<PageData<Oauth2Client>> {
        let sort = page_link.sort_column(&["title", "client_id", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM oauth2_client \
             WHERE ($1::uuid IS NULL OR tenant_id = $1) \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2 OR LOWER(client_id) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, Oauth2Client>(&format!(
            "SELECT * FROM oauth2_client \
             WHERE ($1::uuid IS NULL OR tenant_id = $1) \
             AND ($2::text IS NULL OR LOWER(title) LIKE $2 OR LOWER(client_id) LIKE $2) \
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
        let res = sqlx::query("DELETE FROM oauth2_client WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }
}
