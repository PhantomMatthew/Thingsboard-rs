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

    /// 幂等获取 key_id：冲突时不执行无意义 UPDATE，进程内 moka 缓存减少回源。
    pub async fn get_or_create_id(&self, key: &str) -> TbResult<i32> {
        if let Some(id) = self.cache.get(key).await {
            return Ok(id);
        }
        let existing =
            sqlx::query_scalar::<_, i32>("SELECT key_id FROM key_dictionary WHERE key=$1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(TbError::db)?;
        let id = match existing {
            Some(id) => id,
            None => {
                let inserted = sqlx::query_scalar::<_, i32>(
                    "INSERT INTO key_dictionary (key) VALUES ($1) \
                     ON CONFLICT (key) DO NOTHING RETURNING key_id",
                )
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(TbError::db)?;
                match inserted {
                    Some(id) => id,
                    None => sqlx::query_scalar::<_, i32>(
                        "SELECT key_id FROM key_dictionary WHERE key=$1",
                    )
                    .bind(key)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(TbError::db)?,
                }
            }
        };
        self.cache.insert(key.to_string(), id).await;
        Ok(id)
    }
}
