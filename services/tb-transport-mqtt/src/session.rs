//! 单连接会话：CONNECT 认证 → tb_core session_open → topic 路由 → session_close。
//! topic 约定对齐 Java MqttTopics（v1/devices/me/...）。

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use serde::Deserialize;
use tb_common::{KvEntry, KvValue, TbResult, TsKvEntry};
use tb_proto::core as core_pb;
use tb_proto::transport as pb;
use tb_queue::{QueueProducer as _, TbTopic};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::codec::{
    ConnectPacket, LEVEL_3_1_1, MqttCodec, MqttPacket, PublishPacket, connack_code,
};
use crate::{SessionCommand, SessionHandle, SessionRegistry, TransportState, UNSUBSCRIBED};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DOWNLINK_CHANNEL_SIZE: usize = 32;
const ACTIVITY_HEARTBEAT: Duration = Duration::from_secs(60);
const NO_KEEPALIVE: Duration = Duration::from_secs(24 * 3600);

pub const TOPIC_TELEMETRY: &str = "v1/devices/me/telemetry";
pub const TOPIC_ATTRIBUTES: &str = "v1/devices/me/attributes";
pub const TOPIC_ATTRIBUTES_REQUEST_PREFIX: &str = "v1/devices/me/attributes/request/";
pub const TOPIC_ATTRIBUTES_RESPONSE_PREFIX: &str = "v1/devices/me/attributes/response/";
pub const TOPIC_RPC_REQUEST_PREFIX: &str = "v1/devices/me/rpc/request/";
pub const TOPIC_RPC_RESPONSE_PREFIX: &str = "v1/devices/me/rpc/response/";
pub const TOPIC_RPC_SUBSCRIBE: &str = "v1/devices/me/rpc/request/+";

pub trait MqttIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> MqttIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
pub type MqttStream = Box<dyn MqttIo>;

pub async fn run(socket: TcpStream, peer: SocketAddr, state: TransportState) {
    if let Err(e) = socket.set_nodelay(true) {
        tracing::debug!(%peer, "set_nodelay failed: {e}");
    }
    run_with_x509(Box::new(socket), peer, state, None).await;
}

pub async fn run_with_x509(
    socket: MqttStream,
    peer: SocketAddr,
    state: TransportState,
    x509_der: Option<Vec<u8>>,
) {
    let mut framed = Framed::new(socket, MqttCodec::default());
    let Some(mut session) = connect(&mut framed, peer, &state, x509_der.as_deref()).await else {
        return;
    };
    let session_id = session.info.session_id.clone();
    let reason = serve(&mut framed, &state, &mut session).await;
    // 网关断开：全部子设备逻辑会话一并关闭。
    crate::gateway::close_all_sub_devices(&state, &mut session, &reason).await;
    state.sessions.remove(&session_id);
    state.ota_packages.invalidate(&session_id).await;
    send_core(
        &state,
        &session.device,
        core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
            session: Some(session.info.clone()),
            reason: reason.clone(),
        }),
    )
    .await;
    tracing::debug!(%peer, session_id, reason, "session closed");
}

/// 等 CONNECT 并完成认证；失败已回 CONNACK（或协议错误直接断开），返回 None。
async fn connect(
    framed: &mut Framed<MqttStream, MqttCodec>,
    peer: SocketAddr,
    state: &TransportState,
    x509_der: Option<&[u8]>,
) -> Option<Session> {
    let first = tokio::time::timeout(CONNECT_TIMEOUT, framed.next()).await;
    let connect = match first {
        Ok(Some(Ok(MqttPacket::Connect(c)))) => *c,
        Ok(Some(Ok(other))) => {
            tracing::debug!(%peer, ?other, "first packet is not CONNECT");
            return None;
        }
        Ok(Some(Err(e))) => {
            tracing::debug!(%peer, "CONNECT decode failed: {e}");
            return None;
        }
        Ok(None) => return None,
        Err(_) => {
            tracing::debug!(%peer, "CONNECT timeout");
            return None;
        }
    };
    if connect.protocol_level != LEVEL_3_1_1 {
        // 仅支持 3.1.1：按 3.1.1 CONNACK 语义回「不支持的协议版本」。
        send(
            framed,
            MqttPacket::ConnAck {
                session_present: false,
                return_code: connack_code::UNACCEPTABLE_PROTOCOL_VERSION,
            },
        )
        .await;
        return None;
    }
    if connect.client_id.is_empty() && !connect.clean_session {
        send(
            framed,
            MqttPacket::ConnAck {
                session_present: false,
                return_code: connack_code::IDENTIFIER_REJECTED,
            },
        )
        .await;
        return None;
    }
    // provisioning 会话：username="provision"，不认证不注册设备会话。
    if connect.username.as_deref() == Some(crate::provision::PROVISION_USERNAME) {
        send(
            framed,
            MqttPacket::ConnAck {
                session_present: false,
                return_code: connack_code::ACCEPTED,
            },
        )
        .await;
        crate::provision::serve(framed, state).await;
        return None;
    }
    let device = match authenticate(state, &connect, x509_der).await {
        Ok(Some((device, _profile))) => device,
        Ok(None) => {
            send(
                framed,
                MqttPacket::ConnAck {
                    session_present: false,
                    return_code: connack_code::BAD_USERNAME_PASSWORD,
                },
            )
            .await;
            tracing::debug!(%peer, client_id = connect.client_id, "invalid credentials");
            return None;
        }
        Err(e) => {
            tracing::warn!(%peer, "transport-api authentication infrastructure failed: {e}");
            return None;
        }
    };
    send(
        framed,
        MqttPacket::ConnAck {
            session_present: false,
            return_code: connack_code::ACCEPTED,
        },
    )
    .await;
    let info = core_pb::SessionInfoProto {
        node_id: state.node_id.clone(),
        session_id: Uuid::now_v7().to_string(),
        tenant_id: device.tenant_id.clone(),
        device_id: device.device_id.clone(),
        device_profile_id: device.device_profile_id.clone(),
        device_name: device.device_name.clone(),
        device_type: device.device_type.clone(),
        is_gateway: false,
        gateway_session_id: String::new(),
    };
    let is_gateway = serde_json::from_str::<serde_json::Value>(&device.additional_info_json)
        .ok()
        .and_then(|v| v.get("gateway").and_then(|g| g.as_bool()))
        .unwrap_or(false);
    let (tx, rx) = mpsc::channel(DOWNLINK_CHANNEL_SIZE);
    let session = Session {
        info,
        device,
        keep_alive: connect.keep_alive,
        tx: tx.clone(),
        rx,
        is_gateway,
        sub_devices: HashMap::new(),
        attributes_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
        rpc_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
        gateway_rpc_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
        next_packet_id: 0,
        inflight: HashSet::new(),
        _registry_guard: RegistryCleanupGuard {
            sessions: state.sessions.clone(),
            tx: tx.clone(),
        },
    };
    state.sessions.insert(
        session.info.session_id.clone(),
        SessionHandle {
            tx,
            attributes_qos: session.attributes_qos.clone(),
            rpc_qos: session.rpc_qos.clone(),
            gateway_device: None,
        },
    );
    send_core(
        state,
        &session.device,
        core_pb::to_core_msg::Msg::SessionOpen(core_pb::SessionOpenMsg {
            session: Some(session.info.clone()),
        }),
    )
    .await;
    tracing::info!(%peer, device = session.info.device_name, session_id = session.info.session_id, "session opened");
    Some(session)
}

/// 客户端证书优先走 X509；否则保持 Java 的 MQTT_BASIC/access token 顺序。
async fn authenticate(
    state: &TransportState,
    connect: &ConnectPacket,
    x509_der: Option<&[u8]>,
) -> TbResult<Option<(pb::DeviceInfoProto, pb::DeviceProfileProto)>> {
    let username = connect.username.as_deref().unwrap_or("");
    let password = connect.password.as_deref().map(String::from_utf8_lossy);
    let resp = if let Some(der) = x509_der {
        let hash = tb_common::x509::x509_cert_hash_from_der(der)?;
        state.transport_api.validate_x509(&hash).await
    } else if !username.is_empty() && password.is_some() && !connect.client_id.is_empty() {
        state
            .transport_api
            .validate_mqtt_basic(
                &connect.client_id,
                username,
                password.as_deref().unwrap_or(""),
            )
            .await
    } else if !username.is_empty() {
        state.transport_api.validate_token(username).await
    } else {
        return Ok(None);
    };
    let resp = resp?;
    if !resp.valid {
        return Ok(None);
    }
    match (resp.device, resp.profile) {
        (Some(device), Some(profile)) => Ok(Some((device, profile))),
        _ => Err(tb_common::TbError::Internal(
            "valid credentials without device/profile".into(),
        )),
    }
}

struct RegistryCleanupGuard {
    sessions: SessionRegistry,
    tx: mpsc::Sender<SessionCommand>,
}

impl Drop for RegistryCleanupGuard {
    fn drop(&mut self) {
        self.sessions
            .retain(|_, handle| !handle.tx.same_channel(&self.tx));
    }
}

/// 网关子设备的逻辑会话。
pub struct SubDevice {
    pub info: core_pb::SessionInfoProto,
    pub device: pb::DeviceInfoProto,
}

pub struct Session {
    pub info: core_pb::SessionInfoProto,
    pub device: pb::DeviceInfoProto,
    keep_alive: u16,
    /// 下行 channel 发送端：子设备会话复用同一物理连接。
    pub tx: mpsc::Sender<SessionCommand>,
    rx: mpsc::Receiver<SessionCommand>,
    /// additional_info.gateway=true：允许 v1/gateway/* topic。
    pub is_gateway: bool,
    /// 子设备名 → 逻辑会话。
    pub sub_devices: HashMap<String, SubDevice>,
    attributes_qos: Arc<AtomicU8>,
    rpc_qos: Arc<AtomicU8>,
    /// v1/gateway/rpc 的订阅等级（应用到全部子设备会话）。
    pub gateway_rpc_qos: Arc<AtomicU8>,
    next_packet_id: u16,
    /// 下行 QoS1 未 ack 的 packet_id 集合（PUBACK 清除；断连即整会话丢弃，无需重传）。
    inflight: HashSet<u16>,
    _registry_guard: RegistryCleanupGuard,
}

impl Session {
    fn alloc_packet_id(&mut self) -> Option<u16> {
        for _ in 0..u16::MAX {
            self.next_packet_id = self.next_packet_id.wrapping_add(1).max(1);
            if !self.inflight.contains(&self.next_packet_id) {
                return Some(self.next_packet_id);
            }
        }
        None
    }
}

pub(crate) async fn send(framed: &mut Framed<MqttStream, MqttCodec>, packet: MqttPacket) {
    if let Err(e) = framed.send(packet).await {
        tracing::debug!("send failed: {e}");
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

/// 主循环；返回关闭原因。
async fn serve(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &mut Session,
) -> String {
    let keep_alive = if session.keep_alive > 0 {
        Duration::from_millis(u64::from(session.keep_alive) * 1500)
    } else {
        NO_KEEPALIVE
    };
    let keepalive = tokio::time::sleep(keep_alive);
    tokio::pin!(keepalive);
    let mut heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + ACTIVITY_HEARTBEAT,
        ACTIVITY_HEARTBEAT,
    );
    loop {
        tokio::select! {
            cmd = session.rx.recv() => {
                let Some(command) = cmd else {
                    return "downlink channel closed".into();
                };
                let mut packet = match command {
                    SessionCommand::Packet(packet) => packet,
                    SessionCommand::Close { session_id, reason } => {
                        if session_id == session.info.session_id {
                            return reason;
                        }
                        let sub_device = session
                            .sub_devices
                            .iter()
                            .find(|(_, sub)| sub.info.session_id == session_id)
                            .map(|(name, _)| name.clone());
                        if let Some(name) = sub_device {
                            crate::gateway::close_sub_device(state, session, &name, &reason).await;
                        } else {
                            state.sessions.remove(&session_id);
                        }
                        continue;
                    }
                };
                if let MqttPacket::Publish(p) = &mut packet
                    && p.qos == 1 && p.packet_id.is_none()
                {
                    let Some(packet_id) = session.alloc_packet_id() else {
                        return "too many inflight downlink packets".into();
                    };
                    p.packet_id = Some(packet_id);
                    session.inflight.insert(packet_id);
                }
                if let Err(e) = framed.send(packet).await {
                    return format!("downlink send failed: {e}");
                }
            }
            _ = heartbeat.tick() => {
                report_activity(state, session).await;
                let sub_devices: Vec<_> = session
                    .sub_devices
                    .values()
                    .map(|sub| (sub.device.clone(), sub.info.clone()))
                    .collect();
                for (device, info) in sub_devices {
                    report_device_activity(state, &device, &info).await;
                }
            }
            _ = &mut keepalive => {
                return "keepalive timeout".into();
            }
            pkt = framed.next() => {
                keepalive.as_mut().reset(tokio::time::Instant::now() + keep_alive);
                match pkt {
                    None => return "eof".into(),
                    Some(Err(e)) => return format!("codec error: {e}"),
                    Some(Ok(packet)) => {
                        if let Err(reason) = handle(framed, state, session, packet).await {
                            return reason;
                        }
                    }
                }
            }
        }
    }
}

async fn handle(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &mut Session,
    packet: MqttPacket,
) -> Result<(), String> {
    match packet {
        MqttPacket::Publish(p) => handle_publish(framed, state, session, p).await,
        MqttPacket::PubAck(packet_id) => {
            // 下行 QoS1 的设备 ack：清 inflight；未知 id 仅记录（可能为重发 ack）。
            if !session.inflight.remove(&packet_id) {
                tracing::debug!(packet_id, "PUBACK for unknown packet");
            }
            Ok(())
        }
        MqttPacket::Subscribe { packet_id, filters } => {
            let mut granted = Vec::with_capacity(filters.len());
            for (filter, qos) in filters {
                let granted_qos = qos.min(1);
                match filter.as_str() {
                    TOPIC_ATTRIBUTES => {
                        session.attributes_qos.store(granted_qos, Ordering::Relaxed);
                        send_core(
                            state,
                            &session.device,
                            core_pb::to_core_msg::Msg::SubscribeAttributes(
                                core_pb::SubscribeAttributesMsg {
                                    session: Some(session.info.clone()),
                                    subscribe: true,
                                },
                            ),
                        )
                        .await;
                    }
                    TOPIC_RPC_SUBSCRIBE => {
                        session.rpc_qos.store(granted_qos, Ordering::Relaxed);
                        send_core(
                            state,
                            &session.device,
                            core_pb::to_core_msg::Msg::SubscribeRpc(core_pb::SubscribeRpcMsg {
                                session: Some(session.info.clone()),
                                subscribe: true,
                            }),
                        )
                        .await;
                    }
                    crate::gateway::TOPIC_RPC => {
                        if !session.is_gateway {
                            granted.push(0x80);
                            continue;
                        }
                        crate::gateway::subscribe_rpc(state, session, granted_qos).await;
                    }
                    crate::ota::TOPIC_FW_SUBSCRIBE => {
                        // 订阅即视为 OTA 就绪：主动检查并推 fw_* 元信息。
                        crate::ota::check_firmware(framed, state, session).await;
                    }
                    other => {
                        tracing::debug!(topic = other, "rejected unknown subscribe");
                        granted.push(0x80);
                        continue;
                    }
                }
                granted.push(granted_qos);
            }
            send(framed, MqttPacket::SubAck { packet_id, granted }).await;
            Ok(())
        }
        MqttPacket::Unsubscribe { packet_id, filters } => {
            for filter in &filters {
                match filter.as_str() {
                    TOPIC_ATTRIBUTES => {
                        session
                            .attributes_qos
                            .store(UNSUBSCRIBED, Ordering::Relaxed);
                        send_core(
                            state,
                            &session.device,
                            core_pb::to_core_msg::Msg::SubscribeAttributes(
                                core_pb::SubscribeAttributesMsg {
                                    session: Some(session.info.clone()),
                                    subscribe: false,
                                },
                            ),
                        )
                        .await;
                    }
                    TOPIC_RPC_SUBSCRIBE => {
                        session.rpc_qos.store(UNSUBSCRIBED, Ordering::Relaxed);
                        send_core(
                            state,
                            &session.device,
                            core_pb::to_core_msg::Msg::SubscribeRpc(core_pb::SubscribeRpcMsg {
                                session: Some(session.info.clone()),
                                subscribe: false,
                            }),
                        )
                        .await;
                    }
                    other => tracing::debug!(topic = other, "ignored unsubscribe"),
                }
            }
            send(framed, MqttPacket::UnsubAck(packet_id)).await;
            Ok(())
        }
        MqttPacket::PingReq => {
            send(framed, MqttPacket::PingResp).await;
            Ok(())
        }
        MqttPacket::Disconnect => Err("client disconnect".into()),
        other => {
            tracing::debug!(?other, "unexpected packet from client");
            Err("protocol violation".into())
        }
    }
}

async fn handle_publish(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &mut Session,
    p: PublishPacket,
) -> Result<(), String> {
    if p.qos == 2 {
        return Err("qos 2 unsupported".into());
    }
    let topic = p.topic.as_str();
    if topic.starts_with("v1/gateway/") {
        if !session.is_gateway {
            return Err("gateway api not allowed for non-gateway device".into());
        }
        let packet_id = (p.qos == 1).then_some(p.packet_id).flatten();
        crate::gateway::handle_publish(framed, state, session, p).await;
        if let Some(packet_id) = packet_id {
            send(framed, MqttPacket::PubAck(packet_id)).await;
        }
        return Ok(());
    }
    if let Some(rest) = topic.strip_prefix(crate::ota::TOPIC_FW_REQUEST_PREFIX) {
        let packet_id = (p.qos == 1).then_some(p.packet_id).flatten();
        crate::ota::handle_chunk_request(framed, state, session, rest, &p.payload).await;
        if let Some(packet_id) = packet_id {
            send(framed, MqttPacket::PubAck(packet_id)).await;
        }
        return Ok(());
    }
    if topic == TOPIC_TELEMETRY {
        match parse_telemetry(&p.payload) {
            Ok(entries) => {
                let res = state
                    .transport_api
                    .publish_telemetry(&session.device, entries)
                    .await;
                crate::log_publish_result(res, "publish telemetry failed");
            }
            Err(e) => tracing::warn!("bad telemetry payload: {e}"),
        }
        report_activity(state, session).await;
    } else if topic == TOPIC_ATTRIBUTES {
        match serde_json::from_slice::<HashMap<String, KvValue>>(&p.payload) {
            Ok(values) => {
                let entries: Vec<KvEntry> = values
                    .into_iter()
                    .map(|(key, value)| KvEntry { key, value })
                    .collect();
                let res = state
                    .transport_api
                    .publish_attributes(&session.device, entries)
                    .await;
                crate::log_publish_result(res, "publish attributes failed");
            }
            Err(e) => tracing::warn!("bad attributes payload: {e}"),
        }
        report_activity(state, session).await;
    } else if let Some(request_id) = topic.strip_prefix(TOPIC_RPC_REQUEST_PREFIX) {
        match serde_json::from_slice::<RpcPayload>(&p.payload) {
            Ok(rpc) => {
                if let Err(e) = state
                    .transport_api
                    .publish_to_server_rpc(
                        &session.device,
                        &rpc.method,
                        rpc.params,
                        rpc.timeout.unwrap_or(0),
                    )
                    .await
                {
                    tracing::warn!(request_id, "publish to-server rpc failed: {e}");
                }
            }
            Err(e) => tracing::warn!(request_id, "bad rpc payload: {e}"),
        }
        report_activity(state, session).await;
    } else if let Some(request_id) = topic.strip_prefix(TOPIC_RPC_RESPONSE_PREFIX) {
        // 服务端 RPC 的设备响应：回流 tb_core 完成 pending。
        match request_id.parse::<i32>() {
            Ok(request_id) => {
                let route_key = (session.info.session_id.clone(), request_id);
                let reply_node_id = state
                    .rpc_reply_nodes
                    .get(&route_key)
                    .await
                    .unwrap_or_default();
                state.rpc_reply_nodes.invalidate(&route_key).await;
                send_core(
                    state,
                    &session.device,
                    core_pb::to_core_msg::Msg::RpcResponse(core_pb::RpcResponseMsg {
                        session: Some(session.info.clone()),
                        request_id,
                        payload_json: String::from_utf8_lossy(&p.payload).into_owned(),
                        error: String::new(),
                        reply_node_id,
                    }),
                )
                .await;
            }
            Err(_) => tracing::warn!(topic, "bad rpc response request id"),
        }
    } else if let Some(request_id) = topic.strip_prefix(TOPIC_ATTRIBUTES_REQUEST_PREFIX) {
        handle_attributes_request(framed, state, session, request_id, &p.payload).await;
    } else {
        tracing::debug!(topic, "ignored publish topic");
    }
    if p.qos == 1
        && let Some(packet_id) = p.packet_id
    {
        send(framed, MqttPacket::PubAck(packet_id)).await;
    }
    Ok(())
}

fn split_attribute_keys(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(str::to_string)
        .collect()
}

fn attributes_to_json(kvs: &[tb_proto::common::KeyValueProto]) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for kv in kvs {
        if let Some((key, value)) = tb_proto::convert::kv_from_proto(kv, kv.key.clone()) {
            map.insert(key, serde_json::to_value(&value).unwrap_or_default());
        }
    }
    serde_json::Value::Object(map)
}

/// 响应格式为 {"client":{...},"shared":{...}}，两种 scope 保留分组。
async fn handle_attributes_request(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &Session,
    request_id: &str,
    payload: &[u8],
) {
    let request = if payload.is_empty() {
        AttributesRequestPayload::default()
    } else {
        match serde_json::from_slice::<AttributesRequestPayload>(payload) {
            Ok(request) => request,
            Err(e) => {
                tracing::warn!(request_id, "bad attributes request payload: {e}");
                return;
            }
        }
    };
    let response = match state
        .transport_api
        .get_attributes(
            &session.device.device_id,
            split_attribute_keys(request.client_keys.as_deref()),
            split_attribute_keys(request.shared_keys.as_deref()),
        )
        .await
    {
        Ok(response) => response,
        Err(e) => {
            tracing::warn!(request_id, "attributes request failed: {e}");
            return;
        }
    };
    let body = serde_json::json!({
        "client": attributes_to_json(&response.client_attributes),
        "shared": attributes_to_json(&response.shared_attributes),
    });
    send(
        framed,
        MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: 0,
            retain: false,
            topic: format!("{TOPIC_ATTRIBUTES_RESPONSE_PREFIX}{request_id}"),
            packet_id: None,
            payload: Bytes::from(body.to_string()),
        }),
    )
    .await;
}

async fn report_activity(state: &TransportState, session: &Session) {
    report_device_activity(state, &session.device, &session.info).await;
}

async fn report_device_activity(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    info: &core_pb::SessionInfoProto,
) {
    send_core(
        state,
        device,
        core_pb::to_core_msg::Msg::DeviceActivity(core_pb::DeviceActivityMsg {
            session: Some(info.clone()),
            last_activity_ts: chrono::Utc::now().timestamp_millis(),
        }),
    )
    .await;
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttributesRequestPayload {
    client_keys: Option<String>,
    shared_keys: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RpcPayload {
    method: String,
    #[serde(default)]
    params: serde_json::Value,
    #[serde(default)]
    timeout: Option<i64>,
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

pub(crate) fn parse_telemetry(payload: &[u8]) -> Result<Vec<TsKvEntry>, serde_json::Error> {
    let now = chrono::Utc::now().timestamp_millis();
    let parsed: TelemetryPayload = serde_json::from_slice(payload)?;
    Ok(match parsed {
        TelemetryPayload::Points(points) => points
            .into_iter()
            .flat_map(|p| {
                let ts = p.ts.unwrap_or(now);
                p.values
                    .into_iter()
                    .map(move |(key, value)| TsKvEntry { ts, key, value })
            })
            .collect(),
        TelemetryPayload::WithTs(p) => {
            let ts = p.ts.unwrap_or(now);
            p.values
                .into_iter()
                .map(|(key, value)| TsKvEntry { ts, key, value })
                .collect()
        }
        TelemetryPayload::Map(values) => values
            .into_iter()
            .map(|(key, value)| TsKvEntry {
                ts: now,
                key,
                value,
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_telemetry_plain_map() {
        let entries = parse_telemetry(br#"{"temperature": 66, "ok": true}"#).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|e| e.key == "temperature" && e.value == KvValue::Long(66))
        );
    }

    #[test]
    fn parse_telemetry_ts_values() {
        let entries =
            parse_telemetry(br#"{"ts": 1700000000000, "values": {"humidity": 55.5}}"#).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].ts, 1700000000000);
        assert_eq!(entries[0].value, KvValue::Double(55.5));
    }

    #[test]
    fn parse_telemetry_array() {
        let entries =
            parse_telemetry(br#"[{"ts": 1, "values": {"a": 1}}, {"ts": 2, "values": {"b": "x"}}]"#)
                .unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn parse_telemetry_invalid() {
        assert!(parse_telemetry(b"not json").is_err());
    }

    fn empty_session() -> Session {
        let (tx, rx) = mpsc::channel(1);
        let sessions = Arc::new(dashmap::DashMap::new());
        Session {
            info: core_pb::SessionInfoProto::default(),
            device: pb::DeviceInfoProto::default(),
            keep_alive: 0,
            tx: tx.clone(),
            rx,
            is_gateway: false,
            sub_devices: HashMap::new(),
            attributes_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
            rpc_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
            gateway_rpc_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
            next_packet_id: 0,
            inflight: HashSet::new(),
            _registry_guard: RegistryCleanupGuard { sessions, tx },
        }
    }

    #[test]
    fn packet_id_allocation_skips_zero_and_inflight_ids() {
        let mut session = empty_session();
        session.next_packet_id = u16::MAX;
        session.inflight.extend([1, 2]);
        assert_eq!(session.alloc_packet_id(), Some(3));
        session.inflight.insert(3);
        assert_eq!(session.alloc_packet_id(), Some(4));
    }

    #[test]
    fn registry_guard_removes_all_sessions_on_the_same_connection() {
        let registry: SessionRegistry = Arc::new(dashmap::DashMap::new());
        let (tx, _rx) = mpsc::channel(1);
        let (other_tx, _other_rx) = mpsc::channel(1);
        let handle = |tx: &mpsc::Sender<SessionCommand>| SessionHandle {
            tx: tx.clone(),
            attributes_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
            rpc_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
            gateway_device: None,
        };
        registry.insert("root".into(), handle(&tx));
        registry.insert("sub-device".into(), handle(&tx));
        registry.insert("unrelated".into(), handle(&other_tx));

        drop(RegistryCleanupGuard {
            sessions: registry.clone(),
            tx,
        });

        assert!(!registry.contains_key("root"));
        assert!(!registry.contains_key("sub-device"));
        assert!(registry.contains_key("unrelated"));
    }

    #[test]
    fn attribute_request_keys_are_trimmed() {
        assert_eq!(
            split_attribute_keys(Some(" firmware, serial,, ")),
            vec!["firmware".to_string(), "serial".to_string()]
        );
        assert!(split_attribute_keys(None).is_empty());
    }
}
