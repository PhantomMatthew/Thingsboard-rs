//! 设备会话注册表 + pending RPC：tb_core 消费者维护，REST/RPC 路由查询。

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use dashmap::DashMap;
use tokio::sync::oneshot;
use uuid::Uuid;

/// 设备侧 RPC 结果：Ok(响应 JSON 原文) / Err(错误描述)。
pub type RpcResult = Result<String, String>;

#[derive(Debug, Clone)]
pub struct SessionEntry {
    pub session_id: String,
    pub node_id: String,
    pub is_gateway: bool,
    pub sub_attributes: bool,
    pub sub_rpc: bool,
}

#[derive(Default)]
pub struct DeviceSessionRegistry {
    sessions: DashMap<Uuid, Vec<SessionEntry>>,
    /// device_id → 上次写 server 属性 active/lastActivityTime 的时间戳（节流用）。
    last_activity_write: DashMap<Uuid, i64>,
    pending_rpc: DashMap<(Uuid, i32), oneshot::Sender<RpcResult>>,
    rpc_request_seq: AtomicI32,
}

pub type SharedDeviceSessionRegistry = Arc<DeviceSessionRegistry>;

impl DeviceSessionRegistry {
    pub fn new_shared() -> SharedDeviceSessionRegistry {
        Arc::new(Self::default())
    }

    pub fn open_session(&self, device_id: Uuid, entry: SessionEntry) {
        let mut sessions = self.sessions.entry(device_id).or_default();
        sessions.retain(|e| e.session_id != entry.session_id);
        sessions.push(entry);
    }

    /// 返回该设备是否已无剩余会话。
    pub fn close_session(&self, device_id: Uuid, session_id: &str) -> bool {
        let mut empty = false;
        if let Some(mut sessions) = self.sessions.get_mut(&device_id) {
            sessions.retain(|e| e.session_id != session_id);
            empty = sessions.is_empty();
        }
        if empty {
            self.sessions.remove(&device_id);
        }
        empty
    }

    pub fn set_subscription(
        &self,
        device_id: Uuid,
        session_id: &str,
        node_id: &str,
        is_gateway: bool,
        rpc: bool,
        subscribe: bool,
    ) {
        let mut sessions = self.sessions.entry(device_id).or_default();
        if let Some(entry) = sessions.iter_mut().find(|e| e.session_id == session_id) {
            if rpc {
                entry.sub_rpc = subscribe;
            } else {
                entry.sub_attributes = subscribe;
            }
        } else {
            // 会话事件乱序（subscribe 先于 open 到达）时补建占位条目。
            sessions.push(SessionEntry {
                session_id: session_id.to_string(),
                node_id: node_id.to_string(),
                is_gateway,
                sub_attributes: !rpc && subscribe,
                sub_rpc: rpc && subscribe,
            });
        }
    }

    /// 订阅了 RPC 的会话，优先非网关。
    pub fn rpc_session(&self, device_id: Uuid) -> Option<SessionEntry> {
        let sessions = self.sessions.get(&device_id)?;
        sessions
            .iter()
            .find(|e| e.sub_rpc && !e.is_gateway)
            .or_else(|| sessions.iter().find(|e| e.sub_rpc))
            .cloned()
    }

    /// 订阅了属性更新的会话（可能跨多个节点）。
    pub fn attribute_sessions(&self, device_id: Uuid) -> Vec<SessionEntry> {
        self.sessions
            .get(&device_id)
            .map(|s| {
                s.iter()
                    .filter(|e| e.sub_attributes)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 节流窗口内是否已写过 activity（30s 一次）。
    pub fn activity_throttled(&self, device_id: Uuid, now: i64) -> bool {
        const WINDOW_MS: i64 = 30_000;
        // 注意先释放 get 读守卫再 insert，否则 DashMap 同分片死锁。
        let throttled =
            matches!(self.last_activity_write.get(&device_id), Some(last) if now - *last < WINDOW_MS);
        if throttled {
            true
        } else {
            self.last_activity_write.insert(device_id, now);
            false
        }
    }

    pub fn next_rpc_request_id(&self) -> i32 {
        self.rpc_request_seq.fetch_add(1, Ordering::Relaxed).max(1)
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

    fn entry(session_id: &str, gateway: bool) -> SessionEntry {
        SessionEntry {
            session_id: session_id.into(),
            node_id: "n1".into(),
            is_gateway: gateway,
            sub_attributes: false,
            sub_rpc: false,
        }
    }

    #[test]
    fn registry_open_subscribe_close() {
        let reg = DeviceSessionRegistry::default();
        let device = Uuid::now_v7();
        reg.open_session(device, entry("s1", false));
        reg.set_subscription(device, "s1", "n1", false, true, true);
        assert_eq!(reg.rpc_session(device).unwrap().session_id, "s1");
        assert!(!reg.close_session(device, "s1-unknown"));
        assert!(reg.close_session(device, "s1"));
        assert!(reg.rpc_session(device).is_none());
    }

    #[test]
    fn registry_prefers_non_gateway() {
        let reg = DeviceSessionRegistry::default();
        let device = Uuid::now_v7();
        reg.open_session(device, entry("gw", true));
        reg.open_session(device, entry("dev", false));
        reg.set_subscription(device, "gw", "n1", true, true, true);
        reg.set_subscription(device, "dev", "n1", false, true, true);
        assert_eq!(reg.rpc_session(device).unwrap().session_id, "dev");
    }

    #[test]
    fn activity_throttle_window() {
        let reg = DeviceSessionRegistry::default();
        let device = Uuid::now_v7();
        assert!(!reg.activity_throttled(device, 1000));
        assert!(reg.activity_throttled(device, 2000));
        assert!(!reg.activity_throttled(device, 1000 + 30_000));
    }
}
