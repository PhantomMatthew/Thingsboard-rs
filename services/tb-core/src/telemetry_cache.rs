//! Redis latest 遥测缓存：save 后 write-through，全量 latest 查询先查缓存、未命中回源并回填。
//! HASH key 为 tslatest:{entity_id}，field=key，value 为 {"ts":..,"value":..} JSON；
//! 特殊 field __full__=1 标记哈希已含全量 key（仅全量回填时设置），否则全量查询不信任缓存。
//! Redis 不可用时降级纯 DB，只告警不报错。

use std::collections::HashMap;

use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tb_common::{KvEntry, TbResult, TsKvEntry};
use uuid::Uuid;

use crate::AppState;

const TTL_SECS: i64 = 300;
const FULL_MARKER: &str = "__full__";

fn redis_key(entity_id: Uuid) -> String {
    format!("tslatest:{entity_id}")
}

fn field_json(e: &TsKvEntry) -> String {
    serde_json::json!({"ts": e.ts, "value": e.value}).to_string()
}

async fn conn(state: &AppState) -> Option<ConnectionManager> {
    match state.redis_manager().await {
        Ok(c) => Some(c),
        Err(e) => {
            tracing::warn!("redis unavailable, latest cache degraded to DB: {e}");
            None
        }
    }
}

/// 全量 latest 读：仅当哈希带 __full__ 标记时信任缓存。
pub async fn get_latest_all(state: &AppState, entity_id: Uuid) -> Option<Vec<TsKvEntry>> {
    let mut conn = conn(state).await?;
    let hash: HashMap<String, String> = match conn.hgetall(redis_key(entity_id)).await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!("redis HGETALL failed: {e}");
            return None;
        }
    };
    if !hash.contains_key(FULL_MARKER) {
        return None;
    }
    let mut out = Vec::with_capacity(hash.len().saturating_sub(1));
    for (key, raw) in &hash {
        if key == FULL_MARKER {
            continue;
        }
        let obj: serde_json::Value = match serde_json::from_str(raw) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("corrupt latest cache entry, fall back to DB: {e}");
                return None;
            }
        };
        let (Some(ts), Ok(value)) = (
            obj["ts"].as_i64(),
            serde_json::from_value(obj["value"].clone()),
        ) else {
            tracing::warn!("corrupt latest cache entry, fall back to DB");
            return None;
        };
        out.push(TsKvEntry {
            ts,
            key: key.clone(),
            value,
        });
    }
    Some(out)
}

/// 全量回填：重建哈希并打 __full__ 标记，TTL 滑动。
pub async fn put_latest_all(state: &AppState, entity_id: Uuid, entries: &[TsKvEntry]) {
    let Some(mut conn) = conn(state).await else {
        return;
    };
    let key = redis_key(entity_id);
    let mut fields: Vec<(String, String)> = entries
        .iter()
        .map(|e| (e.key.clone(), field_json(e)))
        .collect();
    fields.push((FULL_MARKER.into(), "1".into()));
    let result: redis::RedisResult<()> = async {
        let _: usize = conn.del(&key).await?;
        let _: () = conn.hset_multiple(&key, &fields).await?;
        let _: bool = conn.expire(&key, TTL_SECS).await?;
        Ok(())
    }
    .await;
    if let Err(e) = result {
        tracing::warn!("redis latest backfill failed: {e}");
    }
}

/// write-through：同 key 仅当新 ts >= 缓存 ts 时覆盖；TTL 滑动。
pub async fn update_latest(state: &AppState, entity_id: Uuid, entries: &[TsKvEntry]) {
    let Some(mut conn) = conn(state).await else {
        return;
    };
    let key = redis_key(entity_id);
    let script = redis::Script::new(
        r#"
        local current = redis.call('HGET', KEYS[1], ARGV[1])
        if current then
            local ok, decoded = pcall(cjson.decode, current)
            if ok and type(decoded) == 'table' then
                local cached_ts = tonumber(decoded['ts'])
                if cached_ts and cached_ts > tonumber(ARGV[3]) then
                    return 0
                end
            end
        end
        redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        return 1
        "#,
    );
    for e in entries {
        let res: redis::RedisResult<usize> = script
            .key(&key)
            .arg(&e.key)
            .arg(field_json(e))
            .arg(e.ts)
            .invoke_async(&mut conn)
            .await;
        if let Err(err) = res {
            tracing::warn!("redis atomic latest update failed: {err}");
            return;
        }
    }
    if let Err(e) = conn.expire::<_, bool>(&key, TTL_SECS).await {
        tracing::warn!("redis EXPIRE failed: {e}");
    }
}

/// 遥测落库 + latest 缓存 write-through + 用量计数（REST 路由与 Kafka ingest 共用）。
/// 保存成功后异步触发计算字段评估（输出经 update_latest 回写缓存）。
pub async fn save_with_latest(
    state: &AppState,
    tenant_id: Uuid,
    entity_id: Uuid,
    entries: &[TsKvEntry],
) -> TbResult<()> {
    state.timeseries_dao.save(entity_id, entries).await?;
    update_latest(state, entity_id, entries).await;
    state
        .usage
        .inc_transport_msgs(tenant_id, entries.len() as i64);
    let trigger: Vec<KvEntry> = entries
        .iter()
        .map(|e| KvEntry {
            key: e.key.clone(),
            value: e.value.clone(),
        })
        .collect();
    crate::calculated::spawn_evaluate(state, tenant_id, entity_id, trigger);
    Ok(())
}

pub async fn invalidate_latest(state: &AppState, entity_id: Uuid) {
    let Some(mut conn) = conn(state).await else {
        return;
    };
    let res: redis::RedisResult<usize> = conn.del(redis_key(entity_id)).await;
    if let Err(e) = res {
        tracing::warn!("redis DEL failed: {e}");
    }
}

/// 全量删除部分 key 后同步剔除缓存 field（保留 __full__ 标记）。
pub async fn delete_latest(state: &AppState, entity_id: Uuid, keys: &[String]) {
    let Some(mut conn) = conn(state).await else {
        return;
    };
    let res: redis::RedisResult<usize> = conn.hdel(redis_key(entity_id), keys).await;
    if let Err(e) = res {
        tracing::warn!("redis HDEL failed: {e}");
    }
}
