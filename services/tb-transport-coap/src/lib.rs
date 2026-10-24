//! tb-transport-coap：设备 CoAP 接入（UDP，coap-lite 手工收发）。
//! 上行：POST telemetry/attributes/rpc、GET attributes（同 HTTP 语义，JSON body）。
//! 下行：GET attributes/rpc 带 Observe=0 注册观察（内存注册表 session_id → observer），
//! observe 注册时向 tb_core 发 session_open + subscribe_attributes/subscribe_rpc，
//! 本节点 notifications consumer 收到 attribute_update/to_device_rpc 后 NON 推送；
//! observer 无入站活动超时（10 分钟）清理并发 session_close，推送不续期。
//! CON 请求回 ACK 响应并缓存 60 秒去重，推送一律 NON。

pub mod downlink;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU16, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use coap_lite::option_value::OptionValueString;
use coap_lite::{
    CoapOption, CoapRequest, ContentFormat, MessageClass, MessageType, Packet, RequestType,
    ResponseType,
};
use dashmap::DashMap;
use prost::Message as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tb_common::{KvEntry, KvValue, TbError, TbResult, TsKvEntry};
use tb_proto::core as core_pb;
use tb_proto::transport as pb;
use tb_queue::{KafkaQueueProducer, QueueError, QueueProducer as _, TbTopic};
use tb_transport_api::TransportApiClient;
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, OwnedMutexGuard, Semaphore};
use uuid::Uuid;

/// observer 无活动超时：清理注册并发 session_close。
pub const OBSERVE_TIMEOUT: Duration = Duration::from_secs(600);
const OBSERVE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// 单请求处理超时：UDP 无重传窗口控制，超时即放弃响应（客户端自行重试）。
const HANDLER_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CONCURRENT_HANDLERS: usize = 1024;
const ADDRESS_RATE_PER_SECOND: f64 = 100.0;
const CON_DEDUP_TTL: Duration = Duration::from_secs(60);

struct AddressRateLimiter {
    tokens: f64,
    last_refill: Instant,
}

impl AddressRateLimiter {
    fn new(now: Instant) -> Self {
        Self {
            tokens: ADDRESS_RATE_PER_SECOND,
            last_refill: now,
        }
    }

    fn allow(&mut self, now: Instant) -> bool {
        let elapsed = now
            .checked_duration_since(self.last_refill)
            .unwrap_or_default()
            .as_secs_f64();
        self.tokens =
            (self.tokens + elapsed * ADDRESS_RATE_PER_SECOND).min(ADDRESS_RATE_PER_SECOND);
        self.last_refill = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

struct CachedConResponse {
    bytes: Vec<u8>,
    stored_at: Instant,
}

#[derive(Default)]
struct ConResponseCache {
    entries: DashMap<(SocketAddr, u16), CachedConResponse>,
    locks: DashMap<(SocketAddr, u16), Arc<Mutex<()>>>,
}

impl ConResponseCache {
    async fn lock(&self, addr: SocketAddr, message_id: u16) -> OwnedMutexGuard<()> {
        let lock = self
            .locks
            .entry((addr, message_id))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        lock.lock_owned().await
    }

    fn get(&self, addr: SocketAddr, message_id: u16, now: Instant) -> Option<Vec<u8>> {
        let key = (addr, message_id);
        let cached = self.entries.get(&key).and_then(|entry| {
            (now.checked_duration_since(entry.stored_at)
                .unwrap_or_default()
                < CON_DEDUP_TTL)
                .then(|| entry.bytes.clone())
        });
        if cached.is_none() {
            self.entries.remove(&key);
        }
        cached
    }

    fn insert(&self, addr: SocketAddr, message_id: u16, bytes: Vec<u8>, now: Instant) {
        self.entries.insert(
            (addr, message_id),
            CachedConResponse {
                bytes,
                stored_at: now,
            },
        );
    }

    fn retain_fresh(&self, now: Instant) {
        self.entries.retain(|_, entry| {
            now.checked_duration_since(entry.stored_at)
                .unwrap_or_default()
                < CON_DEDUP_TTL
        });
        self.locks.retain(|_, lock| Arc::strong_count(lock) > 1);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ObserverKind {
    Attributes,
    Rpc,
}

/// 一条 observe 关系 = 一个 sync 会话（session_id 即 Kafka 下行路由键）。
pub struct Observer {
    pub addr: SocketAddr,
    /// 客户端 CoAP token，推送时原样回带。
    pub token: Vec<u8>,
    pub device: pb::DeviceInfoProto,
    pub info: core_pb::SessionInfoProto,
    pub kind: ObserverKind,
    pub observe_seq: AtomicU32,
    pub last_activity_ms: AtomicI64,
    pub consecutive_send_failures: AtomicU8,
    pub last_push_mid: AtomicU16,
    /// request_id → (reply_node_id, expiration_ts)。
    pub rpc_reply_nodes: DashMap<i32, (String, i64)>,
}

#[derive(Clone)]
pub struct TransportState {
    pub transport_api: TransportApiClient,
    pub producer: KafkaQueueProducer,
    pub node_id: String,
    pub brokers: String,
    pub observers: Arc<DashMap<String, Arc<Observer>>>,
    pub next_mid: Arc<AtomicU16>,
    handler_permits: Arc<Semaphore>,
    rate_limiters: Arc<DashMap<SocketAddr, AddressRateLimiter>>,
    con_responses: Arc<ConResponseCache>,
}

impl TransportState {
    pub fn new(brokers: &str, node_id: &str) -> Result<Self, QueueError> {
        Ok(Self {
            transport_api: TransportApiClient::new(brokers, node_id)?,
            producer: KafkaQueueProducer::new(brokers, &format!("tb-transport-coap-{node_id}"))?,
            node_id: node_id.to_string(),
            brokers: brokers.to_string(),
            observers: Arc::new(DashMap::new()),
            next_mid: Arc::new(AtomicU16::new(1)),
            handler_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_HANDLERS)),
            rate_limiters: Arc::new(DashMap::new()),
            con_responses: Arc::new(ConResponseCache::default()),
        })
    }

    pub fn alloc_mid(&self) -> u16 {
        self.next_mid.fetch_add(1, Ordering::Relaxed)
    }
}

/// 在已绑定的 UDP socket 上跑服务：收包 → 派发 handler task；附带 observe 清理 task。
pub async fn serve(state: TransportState, socket: UdpSocket) -> io::Result<()> {
    let socket = Arc::new(socket);
    tokio::spawn(downlink::run(state.clone(), socket.clone()));
    tokio::spawn(sweep_observers(state.clone()));
    let mut buf = vec![0u8; 1500];
    loop {
        let (n, peer) = socket.recv_from(&mut buf).await?;
        let Ok(packet) = Packet::from_bytes(&buf[..n]) else {
            tracing::debug!(%peer, "undecodable coap packet");
            continue;
        };
        let Ok(permit) = state.handler_permits.clone().try_acquire_owned() else {
            tracing::debug!(%peer, "CoAP handler capacity exhausted; dropping packet");
            continue;
        };
        let state = state.clone();
        let socket = socket.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_packet(state, socket, packet, peer).await;
        });
    }
}

async fn handle_packet(
    state: TransportState,
    socket: Arc<UdpSocket>,
    packet: Packet,
    peer: SocketAddr,
) {
    if packet.header.get_type() == MessageType::Reset {
        reset_observers(&state, peer, packet.get_token(), packet.header.message_id).await;
        return;
    }
    if !matches!(packet.header.code, MessageClass::Request(_)) {
        return;
    }
    let dedup_key = (packet.header.get_type() == MessageType::Confirmable)
        .then_some((peer, packet.header.message_id));
    let _con_guard = match dedup_key {
        Some((addr, message_id)) => Some(state.con_responses.lock(addr, message_id).await),
        None => None,
    };
    if let Some((addr, message_id)) = dedup_key
        && let Some(bytes) = state.con_responses.get(addr, message_id, Instant::now())
    {
        if let Err(e) = socket.send_to(&bytes, peer).await {
            tracing::debug!(%peer, "cached CoAP response send failed: {e}");
        }
        return;
    }

    let request = CoapRequest::from_packet(packet, peer);
    touch_observers(&state, peer);
    let now = Instant::now();
    let allowed = {
        let mut limiter = state
            .rate_limiters
            .entry(peer)
            .or_insert_with(|| AddressRateLimiter::new(now));
        limiter.allow(now)
    };
    let result = if allowed {
        tokio::time::timeout(HANDLER_TIMEOUT, dispatch(&state, &request))
            .await
            .unwrap_or_else(|_| reply(&request, ResponseType::InternalServerError, Vec::new()))
    } else {
        reply(&request, ResponseType::TooManyRequests, Vec::new())
    };
    if let Some(bytes) = result {
        if let Some((addr, message_id)) = dedup_key {
            state
                .con_responses
                .insert(addr, message_id, bytes.clone(), Instant::now());
        }
        if let Err(e) = socket.send_to(&bytes, peer).await {
            tracing::debug!(%peer, "coap response send failed: {e}");
        }
    }
}

/// 同地址 observer 有入站请求即续期；下行推送不续期。
fn touch_observers(state: &TransportState, addr: SocketAddr) {
    let now = now_ms();
    for entry in state.observers.iter() {
        if entry.addr == addr {
            entry.last_activity_ms.store(now, Ordering::Relaxed);
        }
    }
}

async fn notify_observer_close(state: &TransportState, observer: &Observer, reason: &str) {
    send_core(
        state,
        &observer.device,
        core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
            session: Some(observer.info.clone()),
            reason: reason.into(),
        }),
    )
    .await;
}

pub(crate) async fn close_observer(state: &TransportState, session_id: &str, reason: &str) {
    if let Some((_, observer)) = state.observers.remove(session_id) {
        tracing::info!(session_id, reason, "observe closed");
        notify_observer_close(state, &observer, reason).await;
    }
}

fn resettable_observers(
    state: &TransportState,
    addr: SocketAddr,
    token: &[u8],
    message_id: u16,
) -> Vec<String> {
    state
        .observers
        .iter()
        .filter(|entry| {
            if entry.addr != addr {
                return false;
            }
            if token.is_empty() {
                let last_push_mid = entry.last_push_mid.load(Ordering::Relaxed);
                last_push_mid != 0 && last_push_mid == message_id
            } else {
                entry.token == token
            }
        })
        .map(|entry| entry.key().clone())
        .collect()
}

async fn reset_observers(state: &TransportState, addr: SocketAddr, token: &[u8], message_id: u16) {
    for session_id in resettable_observers(state, addr, token, message_id) {
        close_observer(state, &session_id, "coap observer reset").await;
    }
}

/// 周期清理超时 observer，并为剩余 observer 发送会话租约心跳。
async fn sweep_observers(state: TransportState) {
    loop {
        tokio::time::sleep(OBSERVE_SWEEP_INTERVAL).await;
        let now = now_ms();
        let instant = Instant::now();
        state.con_responses.retain_fresh(instant);
        state.rate_limiters.retain(|_, limiter| {
            instant
                .checked_duration_since(limiter.last_refill)
                .unwrap_or_default()
                < OBSERVE_TIMEOUT
        });
        let expired: Vec<String> = state
            .observers
            .iter()
            .filter(|e| {
                now - e.last_activity_ms.load(Ordering::Relaxed)
                    > OBSERVE_TIMEOUT.as_millis() as i64
            })
            .map(|e| e.key().clone())
            .collect();
        for session_id in expired {
            close_observer(&state, &session_id, "coap observe timeout").await;
        }
        let active: Vec<_> = state
            .observers
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        for observer in active {
            send_core(
                &state,
                &observer.device,
                core_pb::to_core_msg::Msg::DeviceActivity(core_pb::DeviceActivityMsg {
                    session: Some(observer.info.clone()),
                    last_activity_ts: now_ms(),
                }),
            )
            .await;
        }
    }
}

async fn dispatch(state: &TransportState, request: &CoapRequest<SocketAddr>) -> Option<Vec<u8>> {
    let method = *request.get_method();
    let segments = request.get_path_as_vec().unwrap_or_default();
    // /api/v1/{token}/{feature}[/{requestId}]
    if segments.len() < 4 || segments[0] != "api" || segments[1] != "v1" {
        return reply(request, ResponseType::NotFound, Vec::new());
    }
    let token = &segments[2];
    let feature = segments[3].as_str();
    let request_id = segments.get(4);

    let observe = request
        .message
        .get_observe_value()
        .and_then(|v| v.ok())
        .map(|v| v == 0);

    match (method, feature, request_id) {
        (RequestType::Post, "telemetry", None) => {
            let device = match validate(state, token).await {
                Ok(device) => device,
                Err(status) => return reply(request, status, Vec::new()),
            };
            let payload: TelemetryPayload = match serde_json::from_slice(&request.message.payload) {
                Ok(p) => p,
                Err(_) => return reply(request, ResponseType::BadRequest, Vec::new()),
            };
            match state
                .transport_api
                .publish_telemetry(&device, payload.into_entries(now_ms()))
                .await
            {
                Ok(()) => reply(request, ResponseType::Changed, Vec::new()),
                Err(error) => reply(request, transport_error_response(&error), Vec::new()),
            }
        }
        (RequestType::Post, "attributes", None) => {
            let device = match validate(state, token).await {
                Ok(device) => device,
                Err(status) => return reply(request, status, Vec::new()),
            };
            let values: HashMap<String, KvValue> =
                match serde_json::from_slice(&request.message.payload) {
                    Ok(v) => v,
                    Err(_) => return reply(request, ResponseType::BadRequest, Vec::new()),
                };
            let entries: Vec<KvEntry> = values
                .into_iter()
                .map(|(key, value)| KvEntry { key, value })
                .collect();
            match state
                .transport_api
                .publish_attributes(&device, entries)
                .await
            {
                Ok(()) => reply(request, ResponseType::Changed, Vec::new()),
                Err(error) => reply(request, transport_error_response(&error), Vec::new()),
            }
        }
        (RequestType::Get, "attributes", None) => {
            let device = match validate(state, token).await {
                Ok(device) => device,
                Err(status) => return reply(request, status, Vec::new()),
            };
            match observe {
                Some(true) => {
                    observe_register(state, request, device.clone(), ObserverKind::Attributes)
                        .await;
                    let body = current_attributes(state, request, &device.device_id).await;
                    reply_observed(request, ResponseType::Content, body)
                }
                Some(false) => {
                    observe_deregister(state, request, ObserverKind::Attributes).await;
                    reply(request, ResponseType::Content, Vec::new())
                }
                None => {
                    let body = current_attributes(state, request, &device.device_id).await;
                    match body {
                        Some(b) => reply_json(request, ResponseType::Content, b),
                        None => reply(request, ResponseType::InternalServerError, Vec::new()),
                    }
                }
            }
        }
        (RequestType::Get, "rpc", None) => {
            let device = match validate(state, token).await {
                Ok(device) => device,
                Err(status) => return reply(request, status, Vec::new()),
            };
            match observe {
                Some(true) => {
                    observe_register(state, request, device, ObserverKind::Rpc).await;
                    reply_observed(request, ResponseType::Content, None)
                }
                Some(false) => {
                    observe_deregister(state, request, ObserverKind::Rpc).await;
                    reply(request, ResponseType::Content, Vec::new())
                }
                None => reply(request, ResponseType::BadRequest, Vec::new()),
            }
        }
        (RequestType::Post, "rpc", None) => {
            let device = match validate(state, token).await {
                Ok(device) => device,
                Err(status) => return reply(request, status, Vec::new()),
            };
            #[derive(Deserialize)]
            struct RpcPayload {
                method: String,
                #[serde(default)]
                params: Value,
                #[serde(default)]
                timeout: Option<i64>,
            }
            let payload: RpcPayload = match serde_json::from_slice(&request.message.payload) {
                Ok(p) => p,
                Err(_) => return reply(request, ResponseType::BadRequest, Vec::new()),
            };
            match state
                .transport_api
                .publish_to_server_rpc(
                    &device,
                    &payload.method,
                    payload.params,
                    payload.timeout.unwrap_or(0),
                )
                .await
            {
                Ok(()) => reply(request, ResponseType::Changed, Vec::new()),
                Err(error) => reply(request, transport_error_response(&error), Vec::new()),
            }
        }
        (RequestType::Post, "rpc", Some(request_id)) => {
            let device = match validate(state, token).await {
                Ok(device) => device,
                Err(status) => return reply(request, status, Vec::new()),
            };
            let Ok(request_id) = request_id.parse::<i32>() else {
                return reply(request, ResponseType::BadRequest, Vec::new());
            };
            let route = state.observers.iter().find_map(|observer| {
                if observer.device.device_id != device.device_id {
                    return None;
                }
                let (_, (reply_node_id, _)) = observer.rpc_reply_nodes.remove(&request_id)?;
                Some((observer.info.clone(), reply_node_id))
            });
            let (info, reply_node_id) =
                route.unwrap_or_else(|| (session_info(state, &device, ""), String::new()));
            send_core(
                state,
                &device,
                core_pb::to_core_msg::Msg::RpcResponse(core_pb::RpcResponseMsg {
                    session: Some(info),
                    request_id,
                    payload_json: String::from_utf8_lossy(&request.message.payload).into_owned(),
                    error: String::new(),
                    reply_node_id,
                }),
            )
            .await;
            reply(request, ResponseType::Changed, Vec::new())
        }
        (RequestType::Post, "claim", None) => {
            let device = match validate(state, token).await {
                Ok(device) => device,
                Err(status) => return reply(request, status, Vec::new()),
            };
            let payload = if request.message.payload.iter().all(u8::is_ascii_whitespace) {
                ClaimPayload::default()
            } else {
                match serde_json::from_slice::<serde_json::Map<String, Value>>(
                    &request.message.payload,
                )
                .ok()
                .and_then(|map| serde_json::from_value(Value::Object(map)).ok())
                {
                    Some(payload) => payload,
                    None => return reply(request, ResponseType::BadRequest, Vec::new()),
                }
            };
            let session = session_info(state, &device, &uuid::Uuid::now_v7().to_string());
            match send_core_result(
                state,
                &device,
                core_pb::to_core_msg::Msg::ClaimDevice(core_pb::ClaimDeviceMsg {
                    session: Some(session),
                    secret_key: payload.secret_key,
                    duration_ms: payload.duration_ms,
                }),
            )
            .await
            {
                Ok(()) => reply(request, ResponseType::Created, Vec::new()),
                Err(e) => {
                    tracing::warn!("CoAP claim publish failed: {e}");
                    reply(request, ResponseType::InternalServerError, Vec::new())
                }
            }
        }
        _ => reply(request, ResponseType::NotFound, Vec::new()),
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaimPayload {
    #[serde(default)]
    secret_key: String,
    #[serde(default)]
    duration_ms: i64,
}

/// GET attributes 同步查询：query 参数 clientKeys/sharedKeys → {client:{},shared:{}}。
async fn current_attributes(
    state: &TransportState,
    request: &CoapRequest<SocketAddr>,
    device_id: &str,
) -> Option<Vec<u8>> {
    let queries = request
        .message
        .get_options_as::<OptionValueString>(CoapOption::UriQuery)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|q| q.ok().map(|s| s.0))
        .collect::<Vec<_>>();
    let split = |name: &str| -> Vec<String> {
        queries
            .iter()
            .find_map(|q| q.strip_prefix(&format!("{name}=")).map(str::to_string))
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|k| !k.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    let resp = state
        .transport_api
        .get_attributes(device_id, split("clientKeys"), split("sharedKeys"))
        .await
        .ok()?;
    let body = json!({
        "client": kvs_to_json(&resp.client_attributes),
        "shared": kvs_to_json(&resp.shared_attributes),
    });
    Some(body.to_string().into_bytes())
}

enum ExistingObserve {
    New,
    Reuse,
    Replaced(Arc<Observer>),
}

fn prepare_observe_registration(
    state: &TransportState,
    addr: SocketAddr,
    token: &[u8],
    device_id: &str,
    kind: ObserverKind,
) -> ExistingObserve {
    let existing = state
        .observers
        .iter()
        .find(|entry| entry.addr == addr && entry.token == token && entry.kind == kind)
        .map(|entry| (entry.key().clone(), entry.value().clone()));
    let Some((session_id, observer)) = existing else {
        return ExistingObserve::New;
    };
    if observer.device.device_id == device_id {
        observer.last_activity_ms.store(now_ms(), Ordering::Relaxed);
        return ExistingObserve::Reuse;
    }
    match state.observers.remove(&session_id) {
        Some((_, observer)) => ExistingObserve::Replaced(observer),
        None => ExistingObserve::New,
    }
}

/// observe 注册：同键同设备续期；同键跨设备替换并关闭旧 core 会话。
async fn observe_register(
    state: &TransportState,
    request: &CoapRequest<SocketAddr>,
    device: pb::DeviceInfoProto,
    kind: ObserverKind,
) {
    let addr = request.source.unwrap();
    let token = request.message.get_token().to_vec();
    match prepare_observe_registration(state, addr, &token, &device.device_id, kind) {
        ExistingObserve::Reuse => return,
        ExistingObserve::Replaced(observer) => {
            notify_observer_close(state, &observer, "observe replaced by another device").await;
        }
        ExistingObserve::New => {}
    }
    let session_id = Uuid::now_v7().to_string();
    let info = session_info(state, &device, &session_id);
    send_core(
        state,
        &device,
        core_pb::to_core_msg::Msg::SessionOpen(core_pb::SessionOpenMsg {
            session: Some(info.clone()),
        }),
    )
    .await;
    let subscribe = match kind {
        ObserverKind::Attributes => {
            core_pb::to_core_msg::Msg::SubscribeAttributes(core_pb::SubscribeAttributesMsg {
                session: Some(info.clone()),
                subscribe: true,
            })
        }
        ObserverKind::Rpc => core_pb::to_core_msg::Msg::SubscribeRpc(core_pb::SubscribeRpcMsg {
            session: Some(info.clone()),
            subscribe: true,
        }),
    };
    send_core(state, &device, subscribe).await;
    state.observers.insert(
        session_id.clone(),
        Arc::new(Observer {
            addr,
            token,
            device,
            info,
            kind,
            observe_seq: AtomicU32::new(0),
            last_activity_ms: AtomicI64::new(now_ms()),
            consecutive_send_failures: AtomicU8::new(0),
            last_push_mid: AtomicU16::new(0),
            rpc_reply_nodes: DashMap::new(),
        }),
    );
    tracing::info!(session_id, %addr, ?kind, "observe registered");
}

/// observe 注销（Observe=1）：按 (addr, token, kind) 找会话 → 移除 + session_close。
async fn observe_deregister(
    state: &TransportState,
    request: &CoapRequest<SocketAddr>,
    kind: ObserverKind,
) {
    let addr = request.source.unwrap();
    let token = request.message.get_token().to_vec();
    let found = state
        .observers
        .iter()
        .find(|e| e.addr == addr && e.token == token && e.kind == kind)
        .map(|e| e.key().clone());
    if let Some(session_id) = found {
        close_observer(state, &session_id, "coap observe deregister").await;
    }
}

fn validation_response(
    response: TbResult<pb::ValidateCredentialsResponse>,
) -> Result<pb::DeviceInfoProto, ResponseType> {
    match response {
        Ok(response) if response.valid => response.device.ok_or_else(|| {
            tracing::warn!("valid CoAP credentials without device info");
            ResponseType::InternalServerError
        }),
        Ok(_) => Err(ResponseType::Unauthorized),
        Err(error) => {
            tracing::warn!("CoAP credential validation infrastructure failed: {error}");
            Err(ResponseType::InternalServerError)
        }
    }
}

async fn validate(
    state: &TransportState,
    token: &str,
) -> Result<pb::DeviceInfoProto, ResponseType> {
    validation_response(state.transport_api.validate_token(token).await)
}

fn transport_error_response(error: &TbError) -> ResponseType {
    match error {
        TbError::RateLimitExceeded(_) => ResponseType::TooManyRequests,
        _ => ResponseType::InternalServerError,
    }
}

fn session_info(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    session_id: &str,
) -> core_pb::SessionInfoProto {
    core_pb::SessionInfoProto {
        node_id: state.node_id.clone(),
        session_id: session_id.to_string(),
        tenant_id: device.tenant_id.clone(),
        device_id: device.device_id.clone(),
        device_profile_id: device.device_profile_id.clone(),
        device_name: device.device_name.clone(),
        device_type: device.device_type.clone(),
        is_gateway: false,
        gateway_session_id: String::new(),
    }
}

async fn send_core_result(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    msg: core_pb::to_core_msg::Msg,
) -> Result<(), tb_queue::QueueError> {
    let m = core_pb::ToCoreMsg {
        tenant_id: device.tenant_id.clone(),
        entity_id: device.device_id.clone(),
        msg: Some(msg),
    };
    state
        .producer
        .send(
            TbTopic::CORE,
            &device.device_id,
            Bytes::from(m.encode_to_vec()),
        )
        .await
}

pub(crate) async fn send_core(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    msg: core_pb::to_core_msg::Msg,
) {
    if let Err(e) = send_core_result(state, device, msg).await {
        tracing::warn!("send to tb_core failed: {e}");
    }
}

/// 常规响应：CON→ACK / NON→NON（coap-lite CoapResponse::new 已处理类型与 message_id）。
pub(crate) fn reply(
    request: &CoapRequest<SocketAddr>,
    status: ResponseType,
    payload: Vec<u8>,
) -> Option<Vec<u8>> {
    let mut response = request.response.clone()?;
    response.set_status(status);
    response.message.payload = payload;
    response.message.to_bytes().ok()
}

fn reply_json(
    request: &CoapRequest<SocketAddr>,
    status: ResponseType,
    payload: Vec<u8>,
) -> Option<Vec<u8>> {
    let mut response = request.response.clone()?;
    response.set_status(status);
    response
        .message
        .set_content_format(ContentFormat::ApplicationJSON);
    response.message.payload = payload;
    response.message.to_bytes().ok()
}

/// observe 注册的 ACK 响应：带 Observe=0。
fn reply_observed(
    request: &CoapRequest<SocketAddr>,
    status: ResponseType,
    payload: Option<Vec<u8>>,
) -> Option<Vec<u8>> {
    let mut response = request.response.clone()?;
    response.set_status(status);
    response.message.set_observe_value(0);
    if let Some(p) = payload {
        response
            .message
            .set_content_format(ContentFormat::ApplicationJSON);
        response.message.payload = p;
    }
    response.message.to_bytes().ok()
}

fn kvs_to_json(kvs: &[tb_proto::common::KeyValueProto]) -> Value {
    let mut map = serde_json::Map::new();
    for kv in kvs {
        if let Some((key, value)) = tb_proto::convert::kv_from_proto(kv, kv.key.clone()) {
            map.insert(key, serde_json::to_value(&value).unwrap_or_default());
        }
    }
    Value::Object(map)
}

pub(crate) fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TelemetryPayload {
    Points(Vec<TsValues>),
    WithTs(TsValues),
    Map(HashMap<String, KvValue>),
}

#[derive(Debug, Deserialize)]
struct TsValues {
    ts: Option<i64>,
    values: HashMap<String, KvValue>,
}

impl TelemetryPayload {
    fn into_entries(self, now: i64) -> Vec<TsKvEntry> {
        match self {
            Self::Points(points) => points
                .into_iter()
                .flat_map(|p| {
                    let ts = p.ts.unwrap_or(now);
                    p.values
                        .into_iter()
                        .map(move |(key, value)| TsKvEntry { ts, key, value })
                })
                .collect(),
            Self::WithTs(p) => {
                let ts = p.ts.unwrap_or(now);
                p.values
                    .into_iter()
                    .map(|(key, value)| TsKvEntry { ts, key, value })
                    .collect()
            }
            Self::Map(values) => values
                .into_iter()
                .map(|(key, value)| TsKvEntry {
                    ts: now,
                    key,
                    value,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> TransportState {
        TransportState::new("localhost:29092", &format!("coap-unit-{}", Uuid::now_v7())).unwrap()
    }

    fn observer(
        addr: SocketAddr,
        token: &[u8],
        device_id: &str,
        session_id: &str,
    ) -> Arc<Observer> {
        Arc::new(Observer {
            addr,
            token: token.to_vec(),
            device: pb::DeviceInfoProto {
                device_id: device_id.into(),
                ..Default::default()
            },
            info: core_pb::SessionInfoProto {
                session_id: session_id.into(),
                device_id: device_id.into(),
                ..Default::default()
            },
            kind: ObserverKind::Attributes,
            observe_seq: AtomicU32::new(0),
            last_activity_ms: AtomicI64::new(0),
            consecutive_send_failures: AtomicU8::new(0),
            last_push_mid: AtomicU16::new(0),
            rpc_reply_nodes: DashMap::new(),
        })
    }

    #[test]
    fn address_token_bucket_limits_and_refills() {
        let now = Instant::now();
        let mut limiter = AddressRateLimiter::new(now);
        for _ in 0..ADDRESS_RATE_PER_SECOND as usize {
            assert!(limiter.allow(now));
        }
        assert!(!limiter.allow(now));
        assert!(limiter.allow(now + Duration::from_millis(20)));
    }

    #[test]
    fn confirmable_response_cache_replays_and_expires() {
        let cache = ConResponseCache::default();
        let addr: SocketAddr = "127.0.0.1:5683".parse().unwrap();
        let now = Instant::now();
        cache.insert(addr, 7, vec![1, 2, 3], now);
        assert_eq!(
            cache.get(addr, 7, now + Duration::from_secs(59)),
            Some(vec![1, 2, 3])
        );
        assert_eq!(cache.get(addr, 7, now + CON_DEDUP_TTL), None);
    }

    #[tokio::test]
    async fn confirmable_retransmission_waits_for_the_first_handler() {
        let cache = Arc::new(ConResponseCache::default());
        let addr: SocketAddr = "127.0.0.1:5683".parse().unwrap();
        let first = cache.lock(addr, 7).await;
        let waiting = {
            let cache = cache.clone();
            tokio::spawn(async move {
                let _guard = cache.lock(addr, 7).await;
            })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(first);
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn observe_rekey_between_devices_replaces_old_session() {
        let state = test_state();
        let addr: SocketAddr = "127.0.0.1:5683".parse().unwrap();
        state
            .observers
            .insert("old".into(), observer(addr, b"token", "device-a", "old"));

        assert!(matches!(
            prepare_observe_registration(
                &state,
                addr,
                b"token",
                "device-a",
                ObserverKind::Attributes
            ),
            ExistingObserve::Reuse
        ));
        let replaced = prepare_observe_registration(
            &state,
            addr,
            b"token",
            "device-b",
            ObserverKind::Attributes,
        );
        let ExistingObserve::Replaced(old) = replaced else {
            panic!("cross-device registration must replace the old observer");
        };
        assert_eq!(old.info.session_id, "old");
        assert!(state.observers.is_empty());
    }

    #[tokio::test]
    async fn reset_matches_token_or_latest_push_mid() {
        let state = test_state();
        let addr: SocketAddr = "127.0.0.1:5683".parse().unwrap();
        let first = observer(addr, b"one", "device-a", "first");
        first.last_push_mid.store(41, Ordering::Relaxed);
        let second = observer(addr, b"two", "device-b", "second");
        second.last_push_mid.store(42, Ordering::Relaxed);
        state.observers.insert("first".into(), first);
        state.observers.insert("second".into(), second);

        assert_eq!(
            resettable_observers(&state, addr, b"", 41),
            vec!["first".to_string()]
        );
        assert_eq!(
            resettable_observers(&state, addr, b"two", 0),
            vec!["second".to_string()]
        );
        assert!(resettable_observers(&state, addr, b"", 99).is_empty());
    }

    #[test]
    fn credential_and_rate_limit_errors_have_distinct_response_codes() {
        assert_eq!(
            validation_response(Ok(pb::ValidateCredentialsResponse::default())).unwrap_err(),
            ResponseType::Unauthorized
        );
        assert_eq!(
            validation_response(Err(TbError::Internal("timeout".into()))).unwrap_err(),
            ResponseType::InternalServerError
        );
        assert_eq!(
            transport_error_response(&TbError::RateLimitExceeded("flood".into())),
            ResponseType::TooManyRequests
        );
    }
}
