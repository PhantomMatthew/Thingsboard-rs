//! 与 services/tb-core/src/telemetry_cache.rs 同源：相同的 HASH key、field JSON、
//! ts 比较与 300s TTL 语义；此处用 Lua 将比较和写入合并为原子操作。

use tb_common::TsKvEntry;
use uuid::Uuid;

use crate::context::NodeContext;

const TTL_SECS: i64 = 300;
const UPDATE_LATEST_LUA: &str = r#"
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current then
    local ok, decoded = pcall(cjson.decode, current)
    if ok and type(decoded) == 'table' then
        local old_ts = tonumber(decoded['ts'])
        if old_ts and old_ts > tonumber(ARGV[2]) then
            redis.call('EXPIRE', KEYS[1], ARGV[4])
            return 0
        end
    end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 1
"#;

fn redis_key(entity_id: Uuid) -> String {
    format!("tslatest:{entity_id}")
}

fn field_json(entry: &TsKvEntry) -> String {
    serde_json::json!({"ts": entry.ts, "value": entry.value}).to_string()
}

pub(crate) async fn update_latest(ctx: &NodeContext, entity_id: Uuid, entries: &[TsKvEntry]) {
    let Some(manager) = &ctx.redis_manager else {
        return;
    };
    let mut conn = manager.clone();
    for entry in entries {
        let result: redis::RedisResult<i32> = redis::Script::new(UPDATE_LATEST_LUA)
            .key(redis_key(entity_id))
            .arg(&entry.key)
            .arg(entry.ts)
            .arg(field_json(entry))
            .arg(TTL_SECS)
            .invoke_async(&mut conn)
            .await;
        if let Err(e) = result {
            tracing::warn!(%entity_id, key = %entry.key, "redis latest update failed: {e}");
            return;
        }
    }
}
