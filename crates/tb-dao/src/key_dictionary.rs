//! key_dictionary 数据访问：遥测/属性 key 的字符串 → int interning。

use moka::future::Cache;
use sqlx::PgPool;
use tb_common::{TbError, TbResult};

const CACHE_CAPACITY: u64 = 100_000;

#[derive(Clone)]
pub struct KeyDictionaryDao {
    pool: PgPool,
    cache: Cache<String, i32>,
}

impl KeyDictionaryDao {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: Cache::builder().max_capacity(CACHE_CAPACITY).build(),
        }
    }

    /// 幂等获取 key_id：upsert 保证并发安全，进程内 moka 缓存减少回源。
    pub async fn get_or_create_id(&self, key: &str) -> TbResult<i32> {
        if let Some(id) = self.cache.get(key).await {
            return Ok(id);
        }
        let id: i32 = sqlx::query_scalar(
            "INSERT INTO key_dictionary (key) VALUES ($1) \
             ON CONFLICT (key) DO UPDATE SET key = EXCLUDED.key RETURNING key_id",
        )
        .bind(key)
        .fetch_one(&self.pool)
        .await
        .map_err(TbError::db)?;
        self.cache.insert(key.to_string(), id).await;
        Ok(id)
    }
}
