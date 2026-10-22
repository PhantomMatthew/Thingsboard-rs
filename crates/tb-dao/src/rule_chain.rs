//! rule_chain / rule_node 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::{RuleChain, RuleNode};
use crate::page::{PageData, PageLink};
use crate::now_ms;

pub struct RuleChainDao {
    pool: PgPool,
}

impl RuleChainDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, rule_chain: &RuleChain) -> TbResult<RuleChain> {
        let mut r = rule_chain.clone();
        if self.find_by_id(r.id).await?.is_some() {
            sqlx::query(
                "UPDATE rule_chain SET tenant_id=$2, name=$3, first_rule_node_id=$4, root=$5, \
                 debug_mode=$6, configuration=$7, additional_info=$8 WHERE id=$1",
            )
            .bind(r.id)
            .bind(r.tenant_id)
            .bind(&r.name)
            .bind(r.first_rule_node_id)
            .bind(r.root)
            .bind(r.debug_mode)
            .bind(&r.configuration)
            .bind(&r.additional_info)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if r.created_time == 0 {
                r.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO rule_chain (id, created_time, tenant_id, name, first_rule_node_id, \
                 root, debug_mode, configuration, additional_info) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            )
            .bind(r.id)
            .bind(r.created_time)
            .bind(r.tenant_id)
            .bind(&r.name)
            .bind(r.first_rule_node_id)
            .bind(r.root)
            .bind(r.debug_mode)
            .bind(&r.configuration)
            .bind(&r.additional_info)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(r)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<RuleChain>> {
        sqlx::query_as::<_, RuleChain>("SELECT * FROM rule_chain WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM rule_chain WHERE id = $1")
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
    ) -> TbResult<PageData<RuleChain>> {
        let sort = page_link.sort_column(&["name", "created_time"]);
        let order = page_link.sort_order_sql();
        let pattern = page_link.search_pattern();
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rule_chain WHERE tenant_id = $1 \
             AND ($2::text IS NULL OR LOWER(name) LIKE $2)",
        )
        .bind(tenant_id)
        .bind(&pattern)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        let data = sqlx::query_as::<_, RuleChain>(&format!(
            "SELECT * FROM rule_chain WHERE tenant_id = $1 \
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

pub struct RuleNodeDao {
    pool: PgPool,
}

impl RuleNodeDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, rule_node: &RuleNode) -> TbResult<RuleNode> {
        let mut n = rule_node.clone();
        if self.find_by_id(n.id).await?.is_some() {
            sqlx::query(
                "UPDATE rule_node SET rule_chain_id=$2, type=$3, name=$4, debug_mode=$5, \
                 singleton_mode=$6, queue_name=$7, configuration=$8, additional_info=$9 \
                 WHERE id=$1",
            )
            .bind(n.id)
            .bind(n.rule_chain_id)
            .bind(&n.r#type)
            .bind(&n.name)
            .bind(n.debug_mode)
            .bind(n.singleton_mode)
            .bind(&n.queue_name)
            .bind(&n.configuration)
            .bind(&n.additional_info)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        } else {
            if n.created_time == 0 {
                n.created_time = now_ms();
            }
            sqlx::query(
                "INSERT INTO rule_node (id, created_time, rule_chain_id, type, name, debug_mode, \
                 singleton_mode, queue_name, configuration, additional_info) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            )
            .bind(n.id)
            .bind(n.created_time)
            .bind(n.rule_chain_id)
            .bind(&n.r#type)
            .bind(&n.name)
            .bind(n.debug_mode)
            .bind(n.singleton_mode)
            .bind(&n.queue_name)
            .bind(&n.configuration)
            .bind(&n.additional_info)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        }
        Ok(n)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<RuleNode>> {
        sqlx::query_as::<_, RuleNode>("SELECT * FROM rule_node WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_by_rule_chain_id(&self, rule_chain_id: Uuid) -> TbResult<Vec<RuleNode>> {
        sqlx::query_as::<_, RuleNode>(
            "SELECT * FROM rule_node WHERE rule_chain_id = $1 ORDER BY created_time ASC",
        )
        .bind(rule_chain_id)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn delete_by_id(&self, id: Uuid) -> TbResult<bool> {
        let res = sqlx::query("DELETE FROM rule_node WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }
}
