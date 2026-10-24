//! rule_chain / rule_node 数据访问。

use sqlx::PgPool;
use tb_common::{TbError, TbResult};
use uuid::Uuid;

use crate::models::{RuleChain, RuleNode};
use crate::now_ms;
use crate::page::{PageData, PageLink};

pub struct RuleChainDao {
    pool: PgPool,
}

impl RuleChainDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, rule_chain: &RuleChain) -> TbResult<RuleChain> {
        let mut r = rule_chain.clone();
        if r.created_time == 0 {
            r.created_time = now_ms();
        }
        sqlx::query_as::<_, RuleChain>(
            "INSERT INTO rule_chain (id, created_time, tenant_id, name, first_rule_node_id, \
             root, debug_mode, configuration, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, name=EXCLUDED.name, \
             first_rule_node_id=EXCLUDED.first_rule_node_id, root=EXCLUDED.root, \
             debug_mode=EXCLUDED.debug_mode, configuration=EXCLUDED.configuration, \
             additional_info=EXCLUDED.additional_info RETURNING *",
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
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_id(&self, id: Uuid) -> TbResult<Option<RuleChain>> {
        sqlx::query_as::<_, RuleChain>("SELECT * FROM rule_chain WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TbError::db)
    }

    pub async fn find_root_by_tenant(&self, tenant_id: Uuid) -> TbResult<Option<RuleChain>> {
        sqlx::query_as::<_, RuleChain>(
            "SELECT * FROM rule_chain WHERE tenant_id = $1 AND root = TRUE",
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// 保存链；root=true 时同事务清除本租户其他链的 root 标记。
    pub async fn save_rooted(&self, rule_chain: &RuleChain) -> TbResult<RuleChain> {
        if !rule_chain.root {
            return self.save(rule_chain).await;
        }
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;
        sqlx::query(
            "UPDATE rule_chain SET root = FALSE WHERE tenant_id = $1 AND root = TRUE AND id <> $2",
        )
        .bind(rule_chain.tenant_id)
        .bind(rule_chain.id)
        .execute(&mut *tx)
        .await
        .map_err(TbError::db)?;
        let saved = self.save_in(rule_chain, &mut tx).await?;
        tx.commit().await.map_err(TbError::db)?;
        Ok(saved)
    }

    /// 事务内整体替换链的节点与边（RULE 组 relation），并更新 first_rule_node_id。
    /// edges 为 (from_node_id, to_node_id, relation_type)。
    pub async fn save_nodes(
        &self,
        chain_id: Uuid,
        nodes: &[RuleNode],
        edges: &[(Uuid, Uuid, String)],
        first_node_id: Option<Uuid>,
    ) -> TbResult<()> {
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;
        sqlx::query(
            "DELETE FROM relation WHERE relation_type_group = 'RULE' AND from_type = 'RULE_NODE' \
             AND from_id IN (SELECT id FROM rule_node WHERE rule_chain_id = $1)",
        )
        .bind(chain_id)
        .execute(&mut *tx)
        .await
        .map_err(TbError::db)?;
        sqlx::query("DELETE FROM rule_node WHERE rule_chain_id = $1")
            .bind(chain_id)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        for node in nodes {
            sqlx::query(
                "INSERT INTO rule_node (id, created_time, rule_chain_id, type, name, debug_mode, \
                 singleton_mode, queue_name, configuration, additional_info) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            )
            .bind(node.id)
            .bind(if node.created_time == 0 {
                now_ms()
            } else {
                node.created_time
            })
            .bind(chain_id)
            .bind(&node.r#type)
            .bind(&node.name)
            .bind(node.debug_mode)
            .bind(node.singleton_mode)
            .bind(&node.queue_name)
            .bind(&node.configuration)
            .bind(&node.additional_info)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        }
        for (from, to, rel_type) in edges {
            sqlx::query(
                "INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type_group, \
                 relation_type) VALUES ($1,'RULE_NODE',$2,'RULE_NODE','RULE',$3)",
            )
            .bind(from)
            .bind(to)
            .bind(rel_type)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        }
        sqlx::query("UPDATE rule_chain SET first_rule_node_id = $2 WHERE id = $1")
            .bind(chain_id)
            .bind(first_node_id)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        tx.commit().await.map_err(TbError::db)?;
        Ok(())
    }

    /// 事务内删除链：RULE 边 → 节点 → 链本身。
    pub async fn delete_cascade(&self, id: Uuid) -> TbResult<bool> {
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;
        sqlx::query(
            "DELETE FROM relation WHERE relation_type_group = 'RULE' AND from_type = 'RULE_NODE' \
             AND from_id IN (SELECT id FROM rule_node WHERE rule_chain_id = $1)",
        )
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(TbError::db)?;
        sqlx::query("DELETE FROM rule_node WHERE rule_chain_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        let res = sqlx::query("DELETE FROM rule_chain WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        tx.commit().await.map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    async fn save_in(
        &self,
        rule_chain: &RuleChain,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> TbResult<RuleChain> {
        let mut r = rule_chain.clone();
        if r.created_time == 0 {
            r.created_time = now_ms();
        }
        sqlx::query_as::<_, RuleChain>(
            "INSERT INTO rule_chain (id, created_time, tenant_id, name, first_rule_node_id, \
             root, debug_mode, configuration, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
             ON CONFLICT (id) DO UPDATE SET tenant_id=EXCLUDED.tenant_id, name=EXCLUDED.name, \
             first_rule_node_id=EXCLUDED.first_rule_node_id, root=EXCLUDED.root, \
             debug_mode=EXCLUDED.debug_mode, configuration=EXCLUDED.configuration, \
             additional_info=EXCLUDED.additional_info RETURNING *",
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
        .fetch_one(&mut **tx)
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
        Ok(PageData::new(
            data,
            total,
            page_link.page,
            page_link.page_size,
        ))
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
        if n.created_time == 0 {
            n.created_time = now_ms();
        }
        sqlx::query_as::<_, RuleNode>(
            "INSERT INTO rule_node (id, created_time, rule_chain_id, type, name, debug_mode, \
             singleton_mode, queue_name, configuration, additional_info) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
             ON CONFLICT (id) DO UPDATE SET rule_chain_id=EXCLUDED.rule_chain_id, \
             type=EXCLUDED.type, name=EXCLUDED.name, debug_mode=EXCLUDED.debug_mode, \
             singleton_mode=EXCLUDED.singleton_mode, queue_name=EXCLUDED.queue_name, \
             configuration=EXCLUDED.configuration, additional_info=EXCLUDED.additional_info \
             RETURNING *",
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
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)
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
