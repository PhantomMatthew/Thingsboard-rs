//! 遥测（ts_kv / ts_kv_latest）数据访问。

use sqlx::{PgPool, Row};
use tb_common::{TbError, TbResult, TsKvEntry};
use uuid::Uuid;

use crate::KeyDictionaryDao;
use crate::kv_columns::{ValueColumns, join_value, split_value};

/// 聚合方式（查询参数 agg）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    None,
    Count,
    Avg,
    Sum,
    Min,
    Max,
}

impl Aggregation {
    pub fn parse(s: &str) -> TbResult<Self> {
        match s.to_ascii_uppercase().as_str() {
            "NONE" => Ok(Self::None),
            "COUNT" => Ok(Self::Count),
            "AVG" => Ok(Self::Avg),
            "SUM" => Ok(Self::Sum),
            "MIN" => Ok(Self::Min),
            "MAX" => Ok(Self::Max),
            other => Err(TbError::InvalidInput(format!("unknown agg: {other}"))),
        }
    }

    fn agg_expr(&self) -> &'static str {
        match self {
            Self::None => unreachable!(),
            Self::Count => "COUNT(*)",
            Self::Avg => "AVG(COALESCE(t.dbl_v, t.long_v::double precision))",
            Self::Sum => "SUM(COALESCE(t.dbl_v, t.long_v::double precision))",
            Self::Min => "MIN(COALESCE(t.dbl_v, t.long_v::double precision))",
            Self::Max => "MAX(COALESCE(t.dbl_v, t.long_v::double precision))",
        }
    }
}

pub struct TimeseriesDao {
    pool: PgPool,
    keys: KeyDictionaryDao,
}

impl TimeseriesDao {
    pub fn new(pool: PgPool, keys: KeyDictionaryDao) -> Self {
        Self { pool, keys }
    }

    /// 事务内批量 upsert ts_kv，并按 "新 ts >= 现存 ts" 推进 ts_kv_latest（version+1）。
    pub async fn save(&self, entity_id: Uuid, entries: &[TsKvEntry]) -> TbResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;
        for e in entries {
            let key_id = self.keys.get_or_create_id(&e.key).await?;
            let cols = split_value(&e.value);
            sqlx::query(
                "INSERT INTO ts_kv (entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) \
                 ON CONFLICT (entity_id, key, ts) DO UPDATE SET \
                 bool_v=EXCLUDED.bool_v, str_v=EXCLUDED.str_v, long_v=EXCLUDED.long_v, \
                 dbl_v=EXCLUDED.dbl_v, json_v=EXCLUDED.json_v",
            )
            .bind(entity_id)
            .bind(key_id)
            .bind(e.ts)
            .bind(cols.bool_v)
            .bind(&cols.str_v)
            .bind(cols.long_v)
            .bind(cols.dbl_v)
            .bind(&cols.json_v)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
            sqlx::query(
                "INSERT INTO ts_kv_latest \
                 (entity_id, key, ts, bool_v, str_v, long_v, dbl_v, json_v, version) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1) \
                 ON CONFLICT (entity_id, key) DO UPDATE SET \
                 ts=EXCLUDED.ts, bool_v=EXCLUDED.bool_v, str_v=EXCLUDED.str_v, \
                 long_v=EXCLUDED.long_v, dbl_v=EXCLUDED.dbl_v, json_v=EXCLUDED.json_v, \
                 version = ts_kv_latest.version + 1 \
                 WHERE EXCLUDED.ts >= ts_kv_latest.ts",
            )
            .bind(entity_id)
            .bind(key_id)
            .bind(e.ts)
            .bind(cols.bool_v)
            .bind(&cols.str_v)
            .bind(cols.long_v)
            .bind(cols.dbl_v)
            .bind(&cols.json_v)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?;
        }
        tx.commit().await.map_err(TbError::db)?;
        Ok(())
    }

    /// 最新值；keys 为 None 时返回该实体全部 key。
    pub async fn find_latest(
        &self,
        entity_id: Uuid,
        keys: Option<&[String]>,
    ) -> TbResult<Vec<TsKvEntry>> {
        let rows = sqlx::query(
            "SELECT l.ts, d.key AS key_name, l.bool_v, l.str_v, l.long_v, l.dbl_v, l.json_v \
             FROM ts_kv_latest l JOIN key_dictionary d ON d.key_id = l.key \
             WHERE l.entity_id = $1 AND ($2::text[] IS NULL OR d.key = ANY($2)) \
             ORDER BY d.key",
        )
        .bind(entity_id)
        .bind(keys.map(<[_]>::to_vec))
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        rows.iter().map(row_to_entry).collect()
    }

    /// 原始历史点，[start_ts, end_ts] 闭区间。
    pub async fn find_history(
        &self,
        entity_id: Uuid,
        key: &str,
        start_ts: i64,
        end_ts: i64,
        limit: i64,
        asc: bool,
    ) -> TbResult<Vec<TsKvEntry>> {
        let order = if asc { "ASC" } else { "DESC" };
        let rows = sqlx::query(&format!(
            "SELECT t.ts, d.key AS key_name, t.bool_v, t.str_v, t.long_v, t.dbl_v, t.json_v \
             FROM ts_kv t JOIN key_dictionary d ON d.key_id = t.key \
             WHERE t.entity_id = $1 AND d.key = $2 AND t.ts >= $3 AND t.ts <= $4 \
             ORDER BY t.ts {order} LIMIT $5"
        ))
        .bind(entity_id)
        .bind(key)
        .bind(start_ts)
        .bind(end_ts)
        .bind(limit.max(1))
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        rows.iter().map(row_to_entry).collect()
    }

    /// 分桶聚合：interval_ms>0 时按 ts/interval*interval 分桶，否则整区间一桶（ts 取 start_ts）。
    /// 数值列取 COALESCE(dbl_v, long_v)；COUNT 计行数。结果按桶 ts 升序。
    pub async fn find_aggregated(
        &self,
        entity_id: Uuid,
        key: &str,
        start_ts: i64,
        end_ts: i64,
        interval_ms: i64,
        agg: Aggregation,
    ) -> TbResult<Vec<TsKvEntry>> {
        if agg == Aggregation::None {
            return self
                .find_history(entity_id, key, start_ts, end_ts, i64::MAX / 2, true)
                .await;
        }
        let rows = sqlx::query(&format!(
            "SELECT CASE WHEN $5 > 0 THEN (t.ts / $5) * $5 ELSE $3 END AS bucket, \
             {} AS agg_v \
             FROM ts_kv t JOIN key_dictionary d ON d.key_id = t.key \
             WHERE t.entity_id = $1 AND d.key = $2 AND t.ts >= $3 AND t.ts <= $4 \
             GROUP BY bucket ORDER BY bucket ASC",
            agg.agg_expr()
        ))
        .bind(entity_id)
        .bind(key)
        .bind(start_ts)
        .bind(end_ts)
        .bind(interval_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let ts: i64 = row.try_get("bucket").map_err(TbError::db)?;
            let value = if agg == Aggregation::Count {
                let v: Option<i64> = row.try_get("agg_v").map_err(TbError::db)?;
                v.map(tb_common::KvValue::Long)
            } else {
                let v: Option<f64> = row.try_get("agg_v").map_err(TbError::db)?;
                v.map(tb_common::KvValue::Double)
            };
            // 全组无非数值列时聚合结果为 NULL，跳过该桶
            if let Some(value) = value {
                out.push(TsKvEntry {
                    ts,
                    key: key.to_string(),
                    value,
                });
            }
        }
        Ok(out)
    }

    pub async fn get_all_keys(&self, entity_id: Uuid) -> TbResult<Vec<String>> {
        sqlx::query_scalar(
            "SELECT DISTINCT d.key FROM ts_kv t JOIN key_dictionary d ON d.key_id = t.key \
             WHERE t.entity_id = $1 ORDER BY d.key",
        )
        .bind(entity_id)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)
    }

    /// 删除 ts_kv 数据点；range 为 None 时连同 ts_kv_latest 一并删除这些 key。
    /// 返回 ts_kv 删除行数。
    pub async fn delete(
        &self,
        entity_id: Uuid,
        keys: &[String],
        range: Option<(i64, i64)>,
    ) -> TbResult<usize> {
        let mut tx = self.pool.begin().await.map_err(TbError::db)?;
        let res = match range {
            Some((start, end)) => sqlx::query(
                "DELETE FROM ts_kv t USING key_dictionary d \
                 WHERE d.key_id = t.key AND t.entity_id = $1 AND d.key = ANY($2) \
                 AND t.ts >= $3 AND t.ts <= $4",
            )
            .bind(entity_id)
            .bind(keys)
            .bind(start)
            .bind(end)
            .execute(&mut *tx)
            .await
            .map_err(TbError::db)?,
            None => {
                sqlx::query(
                    "DELETE FROM ts_kv_latest l USING key_dictionary d \
                     WHERE d.key_id = l.key AND l.entity_id = $1 AND d.key = ANY($2)",
                )
                .bind(entity_id)
                .bind(keys)
                .execute(&mut *tx)
                .await
                .map_err(TbError::db)?;
                sqlx::query(
                    "DELETE FROM ts_kv t USING key_dictionary d \
                     WHERE d.key_id = t.key AND t.entity_id = $1 AND d.key = ANY($2)",
                )
                .bind(entity_id)
                .bind(keys)
                .execute(&mut *tx)
                .await
                .map_err(TbError::db)?
            }
        };
        tx.commit().await.map_err(TbError::db)?;
        Ok(res.rows_affected() as usize)
    }
    /// ts_kv_latest 全表分页扫描（EDQS 快照加载用）：返回 (entity_id, entry)。
    pub async fn scan_latest(&self, offset: i64, limit: i64) -> TbResult<Vec<(Uuid, TsKvEntry)>> {
        let rows = sqlx::query(
            "SELECT l.entity_id, l.ts, d.key AS key_name, \
             l.bool_v, l.str_v, l.long_v, l.dbl_v, l.json_v \
             FROM ts_kv_latest l JOIN key_dictionary d ON d.key_id = l.key \
             ORDER BY l.entity_id, l.key LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(TbError::db)?;
        rows.iter()
            .map(|r| {
                Ok((
                    r.try_get("entity_id").map_err(TbError::db)?,
                    row_to_entry(r)?,
                ))
            })
            .collect()
    }

    /// housekeeper 用：删除 ts 早于 cutoff 的全部历史点（不动 ts_kv_latest），返回删除行数。
    pub async fn delete_older_than(&self, ts: i64) -> TbResult<u64> {
        let res = sqlx::query("DELETE FROM ts_kv WHERE ts < $1")
            .bind(ts)
            .execute(&self.pool)
            .await
            .map_err(TbError::db)?;
        Ok(res.rows_affected())
    }
}

fn row_to_entry(row: &sqlx::postgres::PgRow) -> TbResult<TsKvEntry> {
    let cols = ValueColumns {
        bool_v: row.try_get("bool_v").map_err(TbError::db)?,
        str_v: row.try_get("str_v").map_err(TbError::db)?,
        long_v: row.try_get("long_v").map_err(TbError::db)?,
        dbl_v: row.try_get("dbl_v").map_err(TbError::db)?,
        json_v: row.try_get("json_v").map_err(TbError::db)?,
    };
    let value = join_value(&cols)
        .ok_or_else(|| TbError::Internal("ts_kv row with all-null value columns".into()))?;
    Ok(TsKvEntry {
        ts: row.try_get("ts").map_err(TbError::db)?,
        key: row.try_get("key_name").map_err(TbError::db)?,
        value,
    })
}
