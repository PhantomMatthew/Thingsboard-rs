//! relation（实体关系）数据访问。

use sqlx::PgPool;
use tb_common::{EntityType, TbError, TbResult};
use uuid::Uuid;

use crate::models::EntityRelation;

pub struct RelationDao {
    pool: PgPool,
}

impl RelationDao {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save(&self, relation: &EntityRelation) -> TbResult<EntityRelation> {
        sqlx::query(
            "INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type_group, \
             relation_type, additional_info) VALUES ($1,$2,$3,$4,$5,$6,$7) \
             ON CONFLICT (from_id, from_type, relation_type_group, relation_type, to_id, \
             to_type) DO UPDATE SET additional_info=$7",
        )
        .bind(relation.from_id)
        .bind(relation.from_type.as_str())
        .bind(relation.to_id)
        .bind(relation.to_type.as_str())
        .bind(&relation.relation_type_group)
        .bind(&relation.relation_type)
        .bind(&relation.additional_info)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(relation.clone())
    }

    pub async fn delete(&self, relation: &EntityRelation) -> TbResult<bool> {
        let res = sqlx::query(
            "DELETE FROM relation WHERE from_id=$1 AND from_type=$2 AND to_id=$3 AND to_type=$4 \
             AND relation_type_group=$5 AND relation_type=$6",
        )
        .bind(relation.from_id)
        .bind(relation.from_type.as_str())
        .bind(relation.to_id)
        .bind(relation.to_type.as_str())
        .bind(&relation.relation_type_group)
        .bind(&relation.relation_type)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn find_by_from(
        &self,
        from_id: Uuid,
        from_type: EntityType,
    ) -> TbResult<Vec<EntityRelation>> {
        sqlx::query_as::<_, EntityRelation>(
            "SELECT * FROM relation WHERE from_id = $1 AND from_type = $2",
        )
        .bind(from_id)
        .bind(from_type.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    pub async fn find_by_to(
        &self,
        to_id: Uuid,
        to_type: EntityType,
    ) -> TbResult<Vec<EntityRelation>> {
        sqlx::query_as::<_, EntityRelation>(
            "SELECT * FROM relation WHERE to_id = $1 AND to_type = $2",
        )
        .bind(to_id)
        .bind(to_type.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// 精确匹配 from/to/type（relation_type_group 默认 COMMON）。
    pub async fn find_relation(
        &self,
        from_id: Uuid,
        from_type: EntityType,
        to_id: Uuid,
        to_type: EntityType,
        relation_type: &str,
    ) -> TbResult<Option<EntityRelation>> {
        sqlx::query_as::<_, EntityRelation>(
            "SELECT * FROM relation WHERE from_id=$1 AND from_type=$2 AND to_id=$3 AND \
             to_type=$4 AND relation_type=$5",
        )
        .bind(from_id)
        .bind(from_type.as_str())
        .bind(to_id)
        .bind(to_type.as_str())
        .bind(relation_type)
        .fetch_optional(&self.pool)
        .await
        .map_err(TbError::db)
    }
}
