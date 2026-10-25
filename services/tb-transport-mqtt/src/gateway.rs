//! 网关 API：v1/gateway/* 上行处理 + 子设备逻辑会话管理。
//! 仅 additional_info.gateway=true 的设备允许使用（session 层拦截）。
//! 子设备会话注册进 SessionRegistry（gateway_device=子设备名），下行 RPC 由 downlink 包装成
//! v1/gateway/rpc；子设备遥测/属性的 originator 均为子设备自身。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use serde::Deserialize;
use serde_json::Value;
use tb_common::{KvEntry, KvValue};
use tb_proto::core as core_pb;
use tokio_util::codec::Framed;
use uuid::Uuid;

use crate::codec::{MqttCodec, MqttPacket, PublishPacket};
use crate::session::{self, MqttStream, Session, SubDevice};
use crate::{SessionHandle, TransportState, UNSUBSCRIBED};

pub const TOPIC_CONNECT: &str = "v1/gateway/connect";
pub const TOPIC_DISCONNECT: &str = "v1/gateway/disconnect";
pub const TOPIC_TELEMETRY: &str = "v1/gateway/telemetry";
pub const TOPIC_ATTRIBUTES: &str = "v1/gateway/attributes";
pub const TOPIC_ATTRIBUTES_REQUEST: &str = "v1/gateway/attributes/request";
pub const TOPIC_ATTRIBUTES_RESPONSE: &str = "v1/gateway/attributes/response";
pub const TOPIC_RPC: &str = "v1/gateway/rpc";

pub async fn handle_publish(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &mut Session,
    p: PublishPacket,
) {
    match p.topic.as_str() {
        TOPIC_CONNECT => on_connect(state, session, &p.payload).await,
        TOPIC_DISCONNECT => on_disconnect(state, session, &p.payload).await,
        TOPIC_TELEMETRY => on_telemetry(state, session, &p.payload).await,
        TOPIC_ATTRIBUTES => on_attributes(state, session, &p.payload).await,
        TOPIC_ATTRIBUTES_REQUEST => on_attributes_request(framed, state, session, &p.payload).await,
        TOPIC_RPC => on_rpc(state, session, &p.payload).await,
        other => tracing::debug!(topic = other, "ignored gateway publish topic"),
    }
}

#[derive(Debug, Deserialize)]
struct ConnectPayload {
    device: String,
    r#type: Option<String>,
}

async fn on_connect(state: &TransportState, session: &mut Session, payload: &[u8]) {
    let Ok(req) = serde_json::from_slice::<ConnectPayload>(payload) else {
        tracing::warn!("bad gateway connect payload");
        return;
    };
    let device = state
        .transport_api
        .get_or_create_gateway_device(
            &session.device,
            &req.device,
            req.r#type.as_deref().unwrap_or(""),
        )
        .await;
    let device = match device {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(device = req.device, "gateway get-or-create failed: {e}");
            return;
        }
    };
    // 同名重复 connect：先关闭旧子会话。
    if let Some(old) = session.sub_devices.remove(&device.device_name) {
        session::send_core(
            state,
            &old.device,
            core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
                session: Some(old.info.clone()),
                reason: "replaced by reconnect".into(),
            }),
        )
        .await;
        state.sessions.remove(&old.info.session_id);
    }
    let info = core_pb::SessionInfoProto {
        node_id: state.node_id.clone(),
        session_id: Uuid::now_v7().to_string(),
        tenant_id: device.tenant_id.clone(),
        device_id: device.device_id.clone(),
        device_profile_id: device.device_profile_id.clone(),
        device_name: device.device_name.clone(),
        device_type: device.device_type.clone(),
        is_gateway: false,
        gateway_session_id: session.info.session_id.clone(),
    };
    let rpc_subscribed = session.gateway_rpc_qos.load(Ordering::Relaxed);
    state.sessions.insert(
        info.session_id.clone(),
        SessionHandle {
            tx: session.tx.clone(),
            attributes_qos: Arc::new(AtomicU8::new(UNSUBSCRIBED)),
            rpc_qos: Arc::new(AtomicU8::new(rpc_subscribed)),
            gateway_device: Some(device.device_name.clone()),
        },
    );
    session::send_core(
        state,
        &device,
        core_pb::to_core_msg::Msg::SessionOpen(core_pb::SessionOpenMsg {
            session: Some(info.clone()),
        }),
    )
    .await;
    if rpc_subscribed != UNSUBSCRIBED {
        session::send_core(
            state,
            &device,
            core_pb::to_core_msg::Msg::SubscribeRpc(core_pb::SubscribeRpcMsg {
                session: Some(info.clone()),
                subscribe: true,
            }),
        )
        .await;
    }
    tracing::info!(device = device.device_name, "gateway sub-device connected");
    session
        .sub_devices
        .insert(device.device_name.clone(), SubDevice { info, device });
}

#[derive(Debug, Deserialize)]
struct DisconnectPayload {
    device: String,
}

async fn on_disconnect(state: &TransportState, session: &mut Session, payload: &[u8]) {
    let Ok(req) = serde_json::from_slice::<DisconnectPayload>(payload) else {
        tracing::warn!("bad gateway disconnect payload");
        return;
    };
    close_sub_device(state, session, &req.device, "gateway disconnect").await;
}

/// 关闭单个子设备会话：session_close 回流 core + 移除注册表。
pub async fn close_sub_device(
    state: &TransportState,
    session: &mut Session,
    device_name: &str,
    reason: &str,
) {
    let Some(sub) = session.sub_devices.remove(device_name) else {
        return;
    };
    session::send_core(
        state,
        &sub.device,
        core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
            session: Some(sub.info.clone()),
            reason: reason.into(),
        }),
    )
    .await;
    state.sessions.remove(&sub.info.session_id);
}

/// 网关物理会话关闭时清掉全部子设备会话。
pub async fn close_all_sub_devices(state: &TransportState, session: &mut Session, reason: &str) {
    let names: Vec<String> = session.sub_devices.keys().cloned().collect();
    for name in names {
        close_sub_device(state, session, &name, reason).await;
    }
}

/// {"deviceA": {"ts":ms,"values":{...}} | [...] | {k:v}}：逐子设备上行，originator=子设备。
async fn on_telemetry(state: &TransportState, session: &Session, payload: &[u8]) {
    let Ok(map) = serde_json::from_slice::<HashMap<String, Value>>(payload) else {
        tracing::warn!("bad gateway telemetry payload");
        return;
    };
    for (name, value) in map {
        let Some(sub) = session.sub_devices.get(&name) else {
            tracing::warn!(device = name, "telemetry for unknown sub-device");
            continue;
        };
        match session::parse_telemetry(&serde_json::to_vec(&value).unwrap_or_default()) {
            Ok(entries) => {
                let res = state
                    .transport_api
                    .publish_telemetry(&sub.device, entries)
                    .await;
                crate::log_publish_result(res, "gateway telemetry publish failed");
            }
            Err(e) => tracing::warn!(device = name, "bad gateway telemetry values: {e}"),
        }
    }
}

/// {"deviceA": {k:v}}：逐子设备 CLIENT_SCOPE 属性上行。
async fn on_attributes(state: &TransportState, session: &Session, payload: &[u8]) {
    let Ok(map) = serde_json::from_slice::<HashMap<String, HashMap<String, KvValue>>>(payload)
    else {
        tracing::warn!("bad gateway attributes payload");
        return;
    };
    for (name, values) in map {
        let Some(sub) = session.sub_devices.get(&name) else {
            tracing::warn!(device = name, "attributes for unknown sub-device");
            continue;
        };
        let entries: Vec<KvEntry> = values
            .into_iter()
            .map(|(key, value)| KvEntry { key, value })
            .collect();
        let res = state
            .transport_api
            .publish_attributes(&sub.device, entries)
            .await;
        crate::log_publish_result(res, "gateway attributes publish failed");
    }
}

#[derive(Debug, Deserialize)]
struct AttributesRequestPayload {
    id: Value,
    device: String,
    #[serde(default)]
    client: bool,
    #[serde(default)]
    keys: Vec<String>,
}

fn kvs_to_map(kvs: &[tb_proto::common::KeyValueProto]) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    for kv in kvs {
        if let Some((key, value)) = tb_proto::convert::kv_from_proto(kv, kv.key.clone()) {
            map.insert(key, serde_json::to_value(&value).unwrap_or_default());
        }
    }
    map
}

/// {"id","device","client","keys"} → v1/gateway/attributes/response。
async fn on_attributes_request(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &Session,
    payload: &[u8],
) {
    let Ok(req) = serde_json::from_slice::<AttributesRequestPayload>(payload) else {
        tracing::warn!("bad gateway attributes request payload");
        return;
    };
    let Some(sub) = session.sub_devices.get(&req.device) else {
        tracing::warn!(
            device = req.device,
            "attributes request for unknown sub-device"
        );
        return;
    };
    let (client_keys, shared_keys) = if req.client {
        (req.keys.clone(), Vec::new())
    } else {
        (Vec::new(), req.keys.clone())
    };
    let resp = state
        .transport_api
        .get_attributes(&sub.device.device_id, client_keys, shared_keys)
        .await;
    let Ok(resp) = resp else {
        tracing::warn!(device = req.device, "gateway attributes request failed");
        return;
    };
    let kvs = if req.client {
        &resp.client_attributes
    } else {
        &resp.shared_attributes
    };
    let values = kvs_to_map(kvs);
    // Java 语义：单 key 回 "value"，多 key 回 "values" 映射。
    let mut body = serde_json::json!({"id": req.id, "device": req.device});
    if req.keys.len() == 1 {
        let value = values.get(&req.keys[0]).cloned().unwrap_or(Value::Null);
        body["value"] = value;
    } else {
        body["values"] = Value::Object(values);
    }
    session::send(
        framed,
        MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: 0,
            retain: false,
            topic: TOPIC_ATTRIBUTES_RESPONSE.into(),
            packet_id: None,
            payload: body.to_string().into(),
        }),
    )
    .await;
}

#[derive(Debug, Deserialize)]
struct RpcPayload {
    device: String,
    data: RpcData,
}

#[derive(Debug, Deserialize)]
struct RpcData {
    id: i32,
    response: Option<Value>,
    error: Option<String>,
}

/// 子设备 RPC 响应上行：{"device","data":{"id","response"|"error"}} → core 完成 pending。
async fn on_rpc(state: &TransportState, session: &Session, payload: &[u8]) {
    let Ok(rpc) = serde_json::from_slice::<RpcPayload>(payload) else {
        tracing::warn!("bad gateway rpc payload");
        return;
    };
    let Some(sub) = session.sub_devices.get(&rpc.device) else {
        tracing::warn!(device = rpc.device, "rpc response for unknown sub-device");
        return;
    };
    let (payload_json, error) = match (rpc.data.response, rpc.data.error) {
        (Some(response), _) => (response.to_string(), String::new()),
        (None, Some(error)) => (String::new(), error),
        (None, None) => (String::new(), "empty rpc response".into()),
    };
    let route_key = (sub.info.session_id.clone(), rpc.data.id);
    let reply_node_id = state
        .rpc_reply_nodes
        .get(&route_key)
        .await
        .unwrap_or_default();
    state.rpc_reply_nodes.invalidate(&route_key).await;
    session::send_core(
        state,
        &sub.device,
        core_pb::to_core_msg::Msg::RpcResponse(core_pb::RpcResponseMsg {
            session: Some(sub.info.clone()),
            request_id: rpc.data.id,
            payload_json,
            error,
            reply_node_id,
        }),
    )
    .await;
}

/// 网关订阅 v1/gateway/rpc：存量子设备逐个发 subscribe_rpc，后续 connect 的子设备自动带上。
pub async fn subscribe_rpc(state: &TransportState, session: &Session, granted_qos: u8) {
    session
        .gateway_rpc_qos
        .store(granted_qos, Ordering::Relaxed);
    for sub in session.sub_devices.values() {
        if let Some(handle) = state.sessions.get(&sub.info.session_id) {
            handle.rpc_qos.store(granted_qos, Ordering::Relaxed);
        }
        session::send_core(
            state,
            &sub.device,
            core_pb::to_core_msg::Msg::SubscribeRpc(core_pb::SubscribeRpcMsg {
                session: Some(sub.info.clone()),
                subscribe: true,
            }),
        )
        .await;
    }
}
