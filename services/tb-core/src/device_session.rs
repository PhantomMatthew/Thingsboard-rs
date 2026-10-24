//! Redis 共享设备会话注册表 + 本地 pending RPC。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use dashmap::DashMap;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tb_common::{TbError, TbResult};
use tokio::sync::oneshot;
use uuid::Uuid;

pub const SESSION_LEASE_MS: i64 = 5 * 60 * 1000;
pub const SESSION_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
const SESSION_KEY_PREFIX: &str = "tb:sess:";

/// 设备侧 RPC 结果：Ok(响应 JSON 原文) / Err(错误描述)。
pub type RpcResult = Result<String, String>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEntry {
    pub session_id: String,
    pub node_id: String,
    pub is_gateway: bool,
    pub sub_attributes: bool,
    pub sub_rpc: bool,
    pub last_seen_ms: i64,
}

pub struct DeviceSessionRegistry {
    redis: redis::aio::ConnectionManager,
    node_id: String,
    /// device_id → 上次写 server 属性 active/lastActivityTime 的时间戳（节流用）。
    last_activity_write: DashMap<Uuid, i64>,
    pending_rpc: DashMap<(Uuid, i32), oneshot::Sender<RpcResult>>,
    rpc_request_seq: AtomicI32,
}

pub type SharedDeviceSessionRegistry = Arc<DeviceSessionRegistry>;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn session_key(device_id: Uuid) -> String {
    format!("{SESSION_KEY_PREFIX}{device_id}")
}

impl DeviceSessionRegistry {
    pub fn new_shared(
        redis: redis::aio::ConnectionManager,
        node_id: impl Into<String>,
    ) -> SharedDeviceSessionRegistry {
        Arc::new(Self {
            redis,
            node_id: node_id.into(),
            last_activity_write: DashMap::new(),
            pending_rpc: DashMap::new(),
            rpc_request_seq: AtomicI32::new(0),
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn open_session(&self, device_id: Uuid, mut entry: SessionEntry) -> TbResult<()> {
        entry.last_seen_ms = now_ms();
        let raw = serde_json::to_string(&entry).map_err(TbError::internal)?;
        let _: usize = self
            .redis
            .clone()
            .hset(session_key(device_id), &entry.session_id, raw)
            .await
            .map_err(TbError::internal)?;
        Ok(())
    }

    /// 返回关闭后该设备是否无剩余会话（包括已被 REST 踢线清空的情况）。
    pub async fn close_session(&self, device_id: Uuid, session_id: &str) -> TbResult<bool> {
        let script = redis::Script::new(
            r#"
            redis.call('HDEL', KEYS[1], ARGV[1])
            local remaining = redis.call('HLEN', KEYS[1])
            if remaining == 0 then
                redis.call('DEL', KEYS[1])
                return 1
            end
            return 0
            "#,
        );
        let empty: i32 = script
            .key(session_key(device_id))
            .arg(session_id)
            .invoke_async(&mut self.redis.clone())
            .await
            .map_err(TbError::internal)?;
        let empty = empty == 1;
        if empty {
            self.last_activity_write.remove(&device_id);
        }
        Ok(empty)
    }

    pub async fn set_subscription(
        &self,
        device_id: Uuid,
        session_id: &str,
        node_id: &str,
        is_gateway: bool,
        rpc: bool,
        subscribe: bool,
    ) -> TbResult<()> {
        let key = session_key(device_id);
        let mut redis = self.redis.clone();
        let raw: Option<String> = redis
            .hget(&key, session_id)
            .await
            .map_err(TbError::internal)?;
        let mut entry = match raw {
            Some(raw) => serde_json::from_str(&raw).map_err(TbError::internal)?,
            None => SessionEntry {
                session_id: session_id.to_string(),
                node_id: node_id.to_string(),
                is_gateway,
                sub_attributes: false,
                sub_rpc: false,
                last_seen_ms: now_ms(),
            },
        };
        if rpc {
            entry.sub_rpc = subscribe;
        } else {
            entry.sub_attributes = subscribe;
        }
        entry.last_seen_ms = now_ms();
        let raw = serde_json::to_string(&entry).map_err(TbError::internal)?;
        let _: usize = redis
            .hset(key, session_id, raw)
            .await
            .map_err(TbError::internal)?;
        Ok(())
    }

    pub async fn touch_session(&self, device_id: Uuid, session_id: &str) -> TbResult<()> {
        let key = session_key(device_id);
        let mut redis = self.redis.clone();
        let raw: Option<String> = redis
            .hget(&key, session_id)
            .await
            .map_err(TbError::internal)?;
        let Some(raw) = raw else { return Ok(()) };
        let mut entry: SessionEntry = serde_json::from_str(&raw).map_err(TbError::internal)?;
        entry.last_seen_ms = now_ms();
        let raw = serde_json::to_string(&entry).map_err(TbError::internal)?;
        let _: usize = redis
            .hset(key, session_id, raw)
            .await
            .map_err(TbError::internal)?;
        Ok(())
    }

    /// 订阅了 RPC 的会话，优先非网关。
    pub async fn rpc_session(&self, device_id: Uuid) -> TbResult<Option<SessionEntry>> {
        let sessions = self.any_sessions(device_id).await?;
        Ok(sessions
            .iter()
            .find(|e| e.sub_rpc && !e.is_gateway)
            .or_else(|| sessions.iter().find(|e| e.sub_rpc))
            .cloned())
    }

    /// 订阅了属性更新的会话（可能跨多个节点）。
    pub async fn attribute_sessions(&self, device_id: Uuid) -> TbResult<Vec<SessionEntry>> {
        Ok(self
            .any_sessions(device_id)
            .await?
            .into_iter()
            .filter(|e| e.sub_attributes)
            .collect())
    }

    /// 设备全部在线会话（OTA 通知等不限订阅的场景）。
    pub async fn any_sessions(&self, device_id: Uuid) -> TbResult<Vec<SessionEntry>> {
        let raw: HashMap<String, String> = self
            .redis
            .clone()
            .hgetall(session_key(device_id))
            .await
            .map_err(TbError::internal)?;
        let mut sessions = raw
            .into_values()
            .map(|raw| serde_json::from_str(&raw).map_err(TbError::internal))
            .collect::<TbResult<Vec<SessionEntry>>>()?;
        sessions.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        Ok(sessions)
    }

    /// 原子取出并删除设备全部会话，供删除设备/轮换凭证踢线。
    pub async fn take_sessions(&self, device_id: Uuid) -> TbResult<Vec<SessionEntry>> {
        let script = redis::Script::new(
            r#"
            local entries = redis.call('HGETALL', KEYS[1])
            redis.call('DEL', KEYS[1])
            return entries
            "#,
        );
        let raw: Vec<String> = script
            .key(session_key(device_id))
            .invoke_async(&mut self.redis.clone())
            .await
            .map_err(TbError::internal)?;
        let mut sessions = Vec::with_capacity(raw.len() / 2);
        for pair in raw.chunks_exact(2) {
            sessions.push(serde_json::from_str(&pair[1]).map_err(TbError::internal)?);
        }
        self.last_activity_write.remove(&device_id);
        Ok(sessions)
    }

    pub async fn sweep_expired(&self) -> TbResult<usize> {
        self.sweep_expired_before(now_ms() - SESSION_LEASE_MS).await
    }

    async fn sweep_expired_before(&self, cutoff_ms: i64) -> TbResult<usize> {
        let mut cursor = 0;
        let mut removed_total = 0;
        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{SESSION_KEY_PREFIX}*"))
                .arg("COUNT")
                .arg(100)
                .query_async(&mut self.redis.clone())
                .await
                .map_err(TbError::internal)?;
            for key in keys {
                let script = redis::Script::new(
                    r#"
                    local entries = redis.call('HGETALL', KEYS[1])
                    local removed = 0
                    for i = 1, #entries, 2 do
                        local ok, entry = pcall(cjson.decode, entries[i + 1])
                        if ok and tonumber(entry['last_seen_ms']) < tonumber(ARGV[1]) then
                            removed = removed + redis.call('HDEL', KEYS[1], entries[i])
                        end
                    end
                    local remaining = redis.call('HLEN', KEYS[1])
                    if remaining == 0 then redis.call('DEL', KEYS[1]) end
                    return {removed, remaining}
                    "#,
                );
                let (removed, remaining): (usize, usize) = script
                    .key(&key)
                    .arg(cutoff_ms)
                    .invoke_async(&mut self.redis.clone())
                    .await
                    .map_err(TbError::internal)?;
                removed_total += removed;
                if remaining == 0
                    && let Some(device_id) = key
                        .strip_prefix(SESSION_KEY_PREFIX)
                        .and_then(|id| Uuid::parse_str(id).ok())
                {
                    self.last_activity_write.remove(&device_id);
                }
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(removed_total)
    }

    /// 节流窗口内是否已写过 activity（30s 一次）。
    pub fn activity_throttled(&self, device_id: Uuid, now: i64) -> bool {
        const WINDOW_MS: i64 = 30_000;
        let throttled = matches!(self.last_activity_write.get(&device_id), Some(last) if now - *last < WINDOW_MS);
        if throttled {
            true
        } else {
            self.last_activity_write.insert(device_id, now);
            false
        }
    }

    pub fn next_rpc_request_id(&self) -> i32 {
        self.rpc_request_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn register_pending_rpc(
        &self,
        device_id: Uuid,
        request_id: i32,
    ) -> oneshot::Receiver<RpcResult> {
        let (tx, rx) = oneshot::channel();
        self.pending_rpc.insert((device_id, request_id), tx);
        rx
    }

    pub fn complete_pending_rpc(&self, device_id: Uuid, request_id: i32, result: RpcResult) {
        if let Some((_, tx)) = self.pending_rpc.remove(&(device_id, request_id)) {
            let _ = tx.send(result);
        }
    }

    pub fn drop_pending_rpc(&self, device_id: Uuid, request_id: i32) {
        self.pending_rpc.remove(&(device_id, request_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn registry() -> SharedDeviceSessionRegistry {
        let url =
            std::env::var("TB_REDIS_URL").unwrap_or_else(|_| "redis://localhost:16379".into());
        let client = redis::Client::open(url).unwrap();
        let redis = redis::aio::ConnectionManager::new(client).await.unwrap();
        DeviceSessionRegistry::new_shared(redis, "core-test")
    }

    fn entry(session_id: &str, gateway: bool) -> SessionEntry {
        SessionEntry {
            session_id: session_id.into(),
            node_id: "n1".into(),
            is_gateway: gateway,
            sub_attributes: false,
            sub_rpc: false,
            last_seen_ms: 0,
        }
    }

    #[tokio::test]
    async fn registry_open_subscribe_close() {
        let reg = registry().await;
        let device = Uuid::now_v7();
        reg.open_session(device, entry("s1", false)).await.unwrap();
        reg.set_subscription(device, "s1", "n1", false, true, true)
            .await
            .unwrap();
        assert_eq!(
            reg.rpc_session(device).await.unwrap().unwrap().session_id,
            "s1"
        );
        assert!(!reg.close_session(device, "s1-unknown").await.unwrap());
        assert!(reg.close_session(device, "s1").await.unwrap());
        assert!(reg.rpc_session(device).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn registry_is_shared_and_prefers_non_gateway() {
        let reg1 = registry().await;
        let reg2 = DeviceSessionRegistry::new_shared(reg1.redis.clone(), "core-test-2");
        let device = Uuid::now_v7();
        reg1.open_session(device, entry("gw", true)).await.unwrap();
        reg1.open_session(device, entry("dev", false))
            .await
            .unwrap();
        reg1.set_subscription(device, "gw", "n1", true, true, true)
            .await
            .unwrap();
        reg1.set_subscription(device, "dev", "n1", false, true, true)
            .await
            .unwrap();
        assert_eq!(
            reg2.rpc_session(device).await.unwrap().unwrap().session_id,
            "dev"
        );
        reg2.take_sessions(device).await.unwrap();
    }

    #[tokio::test]
    async fn expired_sessions_are_swept() {
        let reg = registry().await;
        let device = Uuid::now_v7();
        let mut stale = entry("stale", false);
        stale.last_seen_ms = 1_000;
        let _: usize = reg
            .redis
            .clone()
            .hset(
                session_key(device),
                "stale",
                serde_json::to_string(&stale).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reg.sweep_expired_before(2_000).await.unwrap(), 1);
        assert!(reg.any_sessions(device).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn activity_throttle_window() {
        let reg = registry().await;
        let device = Uuid::now_v7();
        assert!(!reg.activity_throttled(device, 1000));
        assert!(reg.activity_throttled(device, 2000));
        assert!(!reg.activity_throttled(device, 1000 + 30_000));
    }

    #[tokio::test]
    async fn rpc_request_ids_start_at_one() {
        let reg = registry().await;
        assert_eq!(reg.next_rpc_request_id(), 1);
        assert_eq!(reg.next_rpc_request_id(), 2);
    }
}
