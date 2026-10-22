//! 单连接会话：CONNECT 认证 → tb_core session_open → topic 路由 → session_close。
//! topic 约定对齐 Java MqttTopics（v1/devices/me/...）。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use serde::Deserialize;
use tb_common::{KvEntry, KvValue, TsKvEntry};
use tb_proto::core as core_pb;
use tb_proto::transport as pb;
use tb_queue::{QueueProducer as _, TbTopic};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::codec::{
    ConnectPacket, LEVEL_3_1_1, MqttCodec, MqttPacket, PublishPacket, connack_code,
};
use crate::{SessionHandle, TransportState, UNSUBSCRIBED};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DOWNLINK_CHANNEL_SIZE: usize = 32;
const NO_KEEPALIVE: Duration = Duration::from_secs(24 * 3600);

pub const TOPIC_TELEMETRY: &str = "v1/devices/me/telemetry";
pub const TOPIC_ATTRIBUTES: &str = "v1/devices/me/attributes";
pub const TOPIC_ATTRIBUTES_REQUEST_PREFIX: &str = "v1/devices/me/attributes/request/";
pub const TOPIC_RPC_REQUEST_PREFIX: &str = "v1/devices/me/rpc/request/";
pub const TOPIC_RPC_SUBSCRIBE: &str = "v1/devices/me/rpc/request/+";

pub async fn run(socket: TcpStream, peer: SocketAddr, state: TransportState) {
    if let Err(e) = socket.set_nodelay(true) {
        tracing::debug!(%peer, "set_nodelay failed: {e}");
    }
    let mut framed = Framed::new(socket, MqttCodec);
    let Some(mut session) = connect(&mut framed, peer, &state).await else {
        return;
    };
    let session_id = session.info.session_id.clone();
    let reason = serve(&mut framed, &state, &mut session).await;
    state.sessions.remove(&session_id);
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
    framed: &mut Framed<TcpStream, MqttCodec>,
    peer: SocketAddr,
    state: &TransportState,
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
    let Some((device, _profile)) = authenticate(state, &connect).await else {
        send(
            framed,
            MqttPacket::ConnAck {
                session_present: false,
                return_code: connack_code::BAD_USERNAME_PASSWORD,
            },
        )
        .await;
        tracing::debug!(%peer, client_id = connect.client_id, "authentication failed");
        return None;
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
    let (tx, rx) = mpsc::channel(DOWNLINK_CHANNEL_SIZE);
    let session = Session {
        info,
        device,
        keep_alive: connect.keep_alive,
        rx,
        attributes_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
        rpc_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
        next_packet_id: 0,
    };
    state.sessions.insert(
        session.info.session_id.clone(),
        SessionHandle {
            tx,
            attributes_qos: session.attributes_qos.clone(),
            rpc_qos: session.rpc_qos.clone(),
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

/// Java 语义：username+password+client_id 齐备先走 MQTT_BASIC（内部回退 token）；
/// 仅 username 非空按 access token。
async fn authenticate(
    state: &TransportState,
    connect: &ConnectPacket,
) -> Option<(pb::DeviceInfoProto, pb::DeviceProfileProto)> {
    let username = connect.username.as_deref().unwrap_or("");
    let password = connect.password.as_deref().map(String::from_utf8_lossy);
    let resp = if !username.is_empty() && password.is_some() && !connect.client_id.is_empty() {
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
        return None;
    };
    match resp {
        Ok(r) if r.valid => match (r.device, r.profile) {
            (Some(d), Some(p)) => Some((d, p)),
            _ => {
                tracing::warn!("valid credentials without device/profile");
                None
            }
        },
        Ok(_) => None,
        Err(e) => {
            tracing::warn!("transport-api validate failed: {e}");
            None
        }
    }
}

struct Session {
    info: core_pb::SessionInfoProto,
    device: pb::DeviceInfoProto,
    keep_alive: u16,
    rx: mpsc::Receiver<MqttPacket>,
    attributes_qos: Arc<AtomicU8>,
    rpc_qos: Arc<AtomicU8>,
    next_packet_id: u16,
}

impl Session {
    fn alloc_packet_id(&mut self) -> u16 {
        self.next_packet_id = self.next_packet_id.wrapping_add(1).max(1);
        self.next_packet_id
    }
}

async fn send(framed: &mut Framed<TcpStream, MqttCodec>, packet: MqttPacket) {
    if let Err(e) = framed.send(packet).await {
        tracing::debug!("send failed: {e}");
    }
}

async fn send_core(
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
    framed: &mut Framed<TcpStream, MqttCodec>,
    state: &TransportState,
    session: &mut Session,
) -> String {
    let keep_alive = if session.keep_alive > 0 {
        Duration::from_millis(u64::from(session.keep_alive) * 1500)
    } else {
        NO_KEEPALIVE
    };
    loop {
        tokio::select! {
            cmd = session.rx.recv() => {
                let Some(mut packet) = cmd else {
                    return "kicked by core".into();
                };
                if let MqttPacket::Publish(p) = &mut packet
                    && p.qos == 1 && p.packet_id.is_none()
                {
                    p.packet_id = Some(session.alloc_packet_id());
                }
                if let Err(e) = framed.send(packet).await {
                    return format!("downlink send failed: {e}");
                }
            }
            pkt = tokio::time::timeout(keep_alive, framed.next()) => {
                match pkt {
                    Err(_) => return "keepalive timeout".into(),
                    Ok(None) => return "eof".into(),
                    Ok(Some(Err(e))) => return format!("codec error: {e}"),
                    Ok(Some(Ok(packet))) => {
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
    framed: &mut Framed<TcpStream, MqttCodec>,
    state: &TransportState,
    session: &mut Session,
    packet: MqttPacket,
) -> Result<(), String> {
    match packet {
        MqttPacket::Publish(p) => handle_publish(framed, state, session, p).await,
        MqttPacket::PubAck(_) => Ok(()),
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
                    other => tracing::debug!(topic = other, "ignored subscribe"),
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
    framed: &mut Framed<TcpStream, MqttCodec>,
    state: &TransportState,
    session: &Session,
    p: PublishPacket,
) -> Result<(), String> {
    if p.qos == 2 {
        return Err("qos 2 unsupported".into());
    }
    let topic = p.topic.as_str();
    if topic == TOPIC_TELEMETRY {
        match parse_telemetry(&p.payload) {
            Ok(entries) => {
                if let Err(e) = state
                    .transport_api
                    .publish_telemetry(&session.device, entries)
                    .await
                {
                    tracing::warn!("publish telemetry failed: {e}");
                }
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
                if let Err(e) = state
                    .transport_api
                    .publish_attributes(&session.device, entries)
                    .await
                {
                    tracing::warn!("publish attributes failed: {e}");
                }
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
    } else if topic.starts_with(TOPIC_ATTRIBUTES_REQUEST_PREFIX) {
        // Phase 5 补全 get_attributes 请求/响应。
        tracing::debug!(topic, "attributes request not supported yet");
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

async fn report_activity(state: &TransportState, session: &Session) {
    send_core(
        state,
        &session.device,
        core_pb::to_core_msg::Msg::DeviceActivity(core_pb::DeviceActivityMsg {
            session: Some(session.info.clone()),
            last_activity_ts: chrono::Utc::now().timestamp_millis(),
        }),
    )
    .await;
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

fn parse_telemetry(payload: &[u8]) -> Result<Vec<TsKvEntry>, serde_json::Error> {
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
}
