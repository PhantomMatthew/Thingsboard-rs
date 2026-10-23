//! tb-transport-coap：设备 CoAP 接入（UDP，coap-lite 手工收发）。
//! 上行：POST telemetry/attributes/rpc、GET attributes（同 HTTP 语义，JSON body）。
//! 下行：GET attributes/rpc 带 Observe=0 注册观察（内存注册表 session_id → observer），
//! observe 注册时向 tb_core 发 session_open + subscribe_attributes/subscribe_rpc，
//! 本节点 notifications consumer 收到 attribute_update/to_device_rpc 后 NON 推送；
//! observer 无活动超时（10 分钟）清理并发 session_close。
//! CON 请求回 ACK 响应，推送一律 NON。

pub mod downlink;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU16, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use coap_lite::option_value::OptionValueString;
use coap_lite::{
    CoapOption, CoapRequest, ContentFormat, MessageClass, Packet, RequestType,
    ResponseType,
};
use dashmap::DashMap;
use prost::Message as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tb_common::{KvEntry, KvValue, TsKvEntry};
use tb_proto::core as core_pb;
use tb_proto::transport as pb;
use tb_queue::{KafkaQueueProducer, QueueError, QueueProducer as _, TbTopic};
use tb_transport_api::TransportApiClient;
use tokio::net::UdpSocket;
use uuid::Uuid;

/// observer 无活动超时：清理注册并发 session_close。
pub const OBSERVE_TIMEOUT: Duration = Duration::from_secs(600);
const OBSERVE_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// 单请求处理超时：UDP 无重传窗口控制，超时即放弃响应（客户端自行重试）。
const HANDLER_TIMEOUT: Duration = Duration::from_secs(15);

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
}

#[derive(Clone)]
pub struct TransportState {
    pub transport_api: TransportApiClient,
    pub producer: KafkaQueueProducer,
    pub node_id: String,
    pub brokers: String,
    pub observers: Arc<DashMap<String, Arc<Observer>>>,
    pub next_mid: Arc<AtomicU16>,
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
        let state = state.clone();
        let socket = socket.clone();
        tokio::spawn(async move {
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
    // 只处理请求；ACK/RST（客户端对 NON 推送的反应）直接忽略。
    if !matches!(packet.header.code, MessageClass::Request(_)) {
        return;
    }
    let request = CoapRequest::from_packet(packet, peer);
    touch_observers(&state, peer);
    let result = tokio::time::timeout(
        HANDLER_TIMEOUT,
        dispatch(&state, &request),
    )
    .await
    .unwrap_or_else(|_| reply(&request, ResponseType::InternalServerError, Vec::new()));
    if let Some(bytes) = result
        && let Err(e) = socket.send_to(&bytes, peer).await
    {
        tracing::debug!(%peer, "coap response send failed: {e}");
    }
}

/// 同地址 observer 有任何活动即续期。
fn touch_observers(state: &TransportState, addr: SocketAddr) {
    let now = now_ms();
    for entry in state.observers.iter() {
        if entry.addr == addr {
            entry.last_activity_ms.store(now, Ordering::Relaxed);
        }
    }
}

/// 周期清理超时 observer：移除注册 + session_close。
async fn sweep_observers(state: TransportState) {
    loop {
        tokio::time::sleep(OBSERVE_SWEEP_INTERVAL).await;
        let now = now_ms();
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
            if let Some((_, observer)) = state.observers.remove(&session_id) {
                tracing::info!(session_id, "observe timed out");
                send_core(
                    &state,
                    &observer.device,
                    core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
                        session: Some(observer.info.clone()),
                        reason: "coap observe timeout".into(),
                    }),
                )
                .await;
            }
        }
    }
}

async fn dispatch(
    state: &TransportState,
    request: &CoapRequest<SocketAddr>,
) -> Option<Vec<u8>> {
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
            let Some(device) = validate(state, token).await else {
                return unauthorized(request);
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
                Err(_) => reply(request, ResponseType::InternalServerError, Vec::new()),
            }
        }
        (RequestType::Post, "attributes", None) => {
            let Some(device) = validate(state, token).await else {
                return unauthorized(request);
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
                Err(_) => reply(request, ResponseType::InternalServerError, Vec::new()),
            }
        }
        (RequestType::Get, "attributes", None) => {
            let Some(device) = validate(state, token).await else {
                return unauthorized(request);
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
            let Some(device) = validate(state, token).await else {
                return unauthorized(request);
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
            let Some(device) = validate(state, token).await else {
                return unauthorized(request);
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
                Err(_) => reply(request, ResponseType::InternalServerError, Vec::new()),
            }
        }
        (RequestType::Post, "rpc", Some(request_id)) => {
            let Some(device) = validate(state, token).await else {
                return unauthorized(request);
            };
            let Ok(request_id) = request_id.parse::<i32>() else {
                return reply(request, ResponseType::BadRequest, Vec::new());
            };
            let info = session_info(state, &device, "");
            send_core(
                state,
                &device,
                core_pb::to_core_msg::Msg::RpcResponse(core_pb::RpcResponseMsg {
                    session: Some(info),
                    request_id,
                    payload_json: String::from_utf8_lossy(&request.message.payload).into_owned(),
                    error: String::new(),
                }),
            )
            .await;
            reply(request, ResponseType::Changed, Vec::new())
        }
        (RequestType::Post, "claim", None) => {
            reply(request, ResponseType::NotImplemented, Vec::new())
        }
        _ => reply(request, ResponseType::NotFound, Vec::new()),
    }
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

/// observe 注册：建 sync 会话 → session_open + subscribe_*，登记注册表。
async fn observe_register(
    state: &TransportState,
    request: &CoapRequest<SocketAddr>,
    device: pb::DeviceInfoProto,
    kind: ObserverKind,
) {
    let addr = request.source.unwrap();
    let token = request.message.get_token().to_vec();
    // 同 (addr, token, kind) 重复注册视为续期：直接沿用原会话。
    if let Some(existing) = state
        .observers
        .iter()
        .find(|e| e.addr == addr && e.token == token && e.kind == kind)
        .map(|e| e.key().clone())
    {
        if let Some(entry) = state.observers.get(&existing) {
            entry.last_activity_ms.store(now_ms(), Ordering::Relaxed);
        }
        return;
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
        ObserverKind::Attributes => core_pb::to_core_msg::Msg::SubscribeAttributes(
            core_pb::SubscribeAttributesMsg {
                session: Some(info.clone()),
                subscribe: true,
            },
        ),
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
    if let Some(session_id) = found
        && let Some((_, observer)) = state.observers.remove(&session_id)
    {
        send_core(
            state,
            &observer.device,
            core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
                session: Some(observer.info.clone()),
                reason: "coap observe deregister".into(),
            }),
        )
        .await;
        tracing::info!(session_id, %addr, ?kind, "observe deregistered");
    }
}

async fn validate(state: &TransportState, token: &str) -> Option<pb::DeviceInfoProto> {
    match state.transport_api.validate_token(token).await {
        Ok(resp) if resp.valid => resp.device,
        _ => None,
    }
}

fn unauthorized(request: &CoapRequest<SocketAddr>) -> Option<Vec<u8>> {
    reply(request, ResponseType::Unauthorized, Vec::new())
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

pub(crate) async fn send_core(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    msg: core_pb::to_core_msg::Msg,
) {
    let m = core_pb::ToCoreMsg {
        tenant_id: device.tenant_id.clone(),
        entity_id: device.device_id.clone(),
        msg: Some(msg),
    };
    if let Err(e) = state
        .producer
        .send(
            TbTopic::CORE,
            &device.device_id,
            Bytes::from(m.encode_to_vec()),
        )
        .await
    {
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
    response.message.set_content_format(ContentFormat::ApplicationJSON);
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
        response.message.set_content_format(ContentFormat::ApplicationJSON);
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
