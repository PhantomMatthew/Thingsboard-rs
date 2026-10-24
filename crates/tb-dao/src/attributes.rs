//! 属性（attribute_kv）数据访问。

use sqlx::{PgPool, Row};
use tb_common::{AttributeScope, KvEntry, TbError, TbResult};
use uuid::Uuid;

use crate::KeyDictionaryDao;
use crate::kv_columns::{ValueColumns, join_value, split_value};
use crate::now_ms;

pub struct AttributesDao {
    pool: PgPool,
    keys: KeyDictionaryDao,
}

impl AttributesDao {
    pub fn new(pool: PgPool, keys: KeyDictionaryDao) -> Self {
        Self { pool, keys }
    }

    /// upsert：已存在时 version+1、last_update_ts 刷新为当前时间。
    pub async fn save(
        &self,
        entity_id: Uuid,
        scope: AttributeScope,
        entries: &[KvEntry],
    ) -> TbResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let now = now_ms();
        let mut key_ids = Vec::with_capacity(entries.len());
        for entry in entries {
            key_ids.push(self.keys.get_or_create_id(&entry.key).await?);
        }
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;
        for (e, key_id) in entries.iter().zip(key_ids) {
            let cols = split_value(&e.value);
            sqlx::query(
                "INSERT INTO attribute_kv \
                 (entity_id, attribute_type, attribute_key, bool_v, str_v, long_v, dbl_v, json_v, \
                 version, last_update_ts) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9) \
                 ON CONFLICT (entity_id, attribute_type, attribute_key) DO UPDATE SET \
                 bool_v=EXCLUDED.bool_v, str_v=EXCLUDED.str_v, long_v=EXCLUDED.long_v, \
                 dbl_v=EXCLUDED.dbl_v, json_v=EXCLUDED.json_v, \
                 version = attribute_kv.version + 1, \
                 last_update_ts = EXCLUDED.last_update_ts",
            )
            .bind(entity_id)
            .bind(scope.as_i16())
            .bind(key_id)
            .bind(cols.bool_v)
            .bind(&cols.str_v)
            .bind(cols.long_v)
            .bind(cols.dbl_v)
            .bind(&cols.json_v)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        }
        tx.commit().await.map_err(TbError::db)?;
        Ok(())
    }

    pub async fn delete_by_entity(
        &self,
        tx: &mut sqlx::PgConnection,
        entity_id: Uuid,
    ) -> TbResult<()> {
        sqlx::query("DELETE FROM attribute_kv WHERE entity_id = $1")
            .bind(entity_id)
            .execute(tx)
            .await
            .map_err(TbError::db)?;
        Ok(())
    }

    /// 单 scope 查询；keys 为 None 时返回该 scope 全部 key。
    pub async fn find(
        &self,
        entity_id: Uuid,
        scope: AttributeScope,
        keys: Option<&[String]>,
    ) -> TbResult<Vec<KvEntry>> {
        let rows = sqlx::query(
            "SELECT d.key AS key_name, a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v \
             FROM attribute_kv a JOIN key_dictionary d ON d.key_id = a.attribute_key \
             WHERE a.entity_id = $1 AND a.attribute_type = $2 \
             AND ($3::text[] IS NULL OR d.key = ANY($3)) \
             ORDER BY d.key",
        )
        .bind(entity_id)
        .bind(scope.as_i16())
        .bind(keys.map(<[_]>::to_vec))
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        rows.iter().map(row_to_kv_entry).collect()
    }

    /// 跨 scope 查询，返回 (scope, entry)。
    pub async fn find_all(&self, entity_id: Uuid) -> TbResult<Vec<(AttributeScope, KvEntry)>> {
        let rows = sqlx::query(
            "SELECT d.key AS key_name, a.attribute_type, a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v \
             FROM attribute_kv a JOIN key_dictionary d ON d.key_id = a.attribute_key \
             WHERE a.entity_id = $1 \
             ORDER BY a.attribute_type, d.key",
        )
        .bind(entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        rows.iter()
            .map(|r| {
                let scope: i16 = r.try_get("attribute_type").map_err(TbError::db)?;
                let scope = AttributeScope::from_i16(scope)
                    .ok_or_else(|| TbError::Internal(format!("unknown attribute_type {scope}")))?;
                Ok((scope, row_to_kv_entry(r)?))
            })
            .collect()
    }

    pub async fn find_by_entity_ids(
        &self,
        entity_ids: &[Uuid],
    ) -> TbResult<Vec<(Uuid, AttributeScope, KvEntry)>> {
        if entity_ids.is_empty() {
            return Ok(vec![]);
        }
        let rows = sqlx::query(
            "SELECT a.entity_id, a.attribute_type, d.key AS key_name, \
             a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v \
             FROM attribute_kv a JOIN key_dictionary d ON d.key_id = a.attribute_key \
             WHERE a.entity_id = ANY($1) \
             ORDER BY a.entity_id, a.attribute_type, a.attribute_key",
        )
        .bind(entity_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        rows.iter()
            .map(|r| {
                let scope: i16 = r.try_get("attribute_type").map_err(TbError::db)?;
                let scope = AttributeScope::from_i16(scope)
                    .ok_or_else(|| TbError::Internal(format!("unknown attribute_type {scope}")))?;
                Ok((
                    r.try_get("entity_id").map_err(TbError::db)?,
                    scope,
                    row_to_kv_entry(r)?,
                ))
            })
            .collect()
    }

    /// 全表分页扫描（EDQS 快照加载用）：返回 (entity_id, scope, entry)。
    pub async fn scan_all(
        &self,
        offset: i64,
        limit: i64,
    ) -> TbResult<Vec<(Uuid, AttributeScope, KvEntry)>> {
        let rows = sqlx::query(
            "SELECT a.entity_id, a.attribute_type, d.key AS key_name, \
             a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v \
             FROM attribute_kv a JOIN key_dictionary d ON d.key_id = a.attribute_key \
             ORDER BY a.entity_id, a.attribute_type, a.attribute_key LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        rows.iter()
            .map(|r| {
                let scope: i16 = r.try_get("attribute_type").map_err(TbError::db)?;
                let scope = AttributeScope::from_i16(scope)
                    .ok_or_else(|| TbError::Internal(format!("unknown attribute_type {scope}")))?;
                Ok((
                    r.try_get("entity_id").map_err(TbError::db)?,
                    scope,
                    row_to_kv_entry(r)?,
                ))
            })
            .collect()
    }

    /// 返回删除行数。
    pub async fn delete(
        &self,
        entity_id: Uuid,
        scope: AttributeScope,
        keys: &[String],
    ) -> TbResult<usize> {
        let res = sqlx::query(
            "DELETE FROM attribute_kv a USING key_dictionary d \
             WHERE d.key_id = a.attribute_key AND a.entity_id = $1 \
             AND a.attribute_type = $2 AND d.key = ANY($3)",
        )
        .bind(entity_id)
        .bind(scope.as_i16())
        .bind(keys)
        .execute(&self.pool)
        .await
        .map_err(TbError::db)?;
        Ok(res.rows_affected() as usize)
    }
}

fn row_to_kv_entry(row: &sqlx::postgres::PgRow) -> TbResult<KvEntry> {
    let cols = ValueColumns {
        bool_v: row.try_get("bool_v").map_err(TbError::db)?,
        str_v: row.try_get("str_v").map_err(TbError::db)?,
        long_v: row.try_get("long_v").map_err(TbError::db)?,
        dbl_v: row.try_get("dbl_v").map_err(TbError::db)?,
        json_v: row.try_get("json_v").map_err(TbError::db)?,
    };
    let value = join_value(&cols)
        .ok_or_else(|| TbError::Internal("attribute_kv row with all-null value columns".into()))?;
    Ok(KvEntry {
        key: row.try_get("key_name").map_err(TbError::db)?,
        value,
    })
}
