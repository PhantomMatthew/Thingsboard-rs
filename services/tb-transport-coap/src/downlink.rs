//! 下行通知：消费 tb_transport.notifications.<node_id>，按 session_id 找 observer，
//! attribute_update → NON {k:v} JSON；to_device_rpc → NON {"id","method","params"}；
//! session_close_notification → 移除注册。推送带递增 Observe 序号。

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use coap_lite::{ContentFormat, MessageClass, MessageType, Packet, ResponseType};
use prost::Message as _;
use tb_proto::core as core_pb;
use tb_queue::{QueueConsumer as _, StreamQueueConsumer, TbTopic};
use tokio::net::UdpSocket;

use crate::{Observer, ObserverKind, TransportState, now_ms};

pub async fn run(state: TransportState, socket: Arc<UdpSocket>) {
    let topic = TbTopic::transport_notifications(&state.node_id);
    if let Err(e) = tb_queue::admin::ensure_topic(&state.brokers, &topic, 1).await {
        tracing::warn!("ensure notifications topic failed: {e}");
    }
    let group_id = format!("tb-transport-coap-{}", state.node_id);
    let mut consumer = loop {
        match StreamQueueConsumer::new(&state.brokers, &group_id, &[&topic]) {
            Ok(c) => break c,
            Err(e) => {
                tracing::error!("notifications consumer failed to start: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    loop {
        match consumer.recv().await {
            Ok(msg) => handle(&state, &socket, msg.payload).await,
            Err(e) => {
                tracing::error!("notifications consumer error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn handle(state: &TransportState, socket: &Arc<UdpSocket>, payload: Bytes) {
    let Ok(msg) = core_pb::ToTransportMsg::decode(payload) else {
        tracing::warn!("undecodable transport notification");
        return;
    };
    let Some(oneof) = msg.msg else {
        return;
    };
    match oneof {
        core_pb::to_transport_msg::Msg::AttributeUpdate(n) => {
            let Some(observer) = state.observers.get(&msg.session_id) else {
                return;
            };
            if observer.kind != ObserverKind::Attributes {
                return;
            }
            // value 缺省 = 删除通知，JSON null。
            let mut body = serde_json::Map::new();
            for kv in &n.attributes {
                let key = if kv.key.is_empty() {
                    kv.key_id.to_string()
                } else {
                    kv.key.clone()
                };
                let value = match tb_proto::convert::kv_from_proto(kv, key.clone()) {
                    Some((_, v)) => serde_json::to_value(&v).unwrap_or_default(),
                    None => serde_json::Value::Null,
                };
                body.insert(key, value);
            }
            push(state, socket, &observer, serde_json::to_vec(&body).unwrap_or_default())
                .await;
        }
        core_pb::to_transport_msg::Msg::ToDeviceRpc(rpc) => {
            if rpc.expiration_ts > 0 && rpc.expiration_ts < now_ms() {
                return;
            }
            let Some(observer) = state.observers.get(&msg.session_id) else {
                return;
            };
            if observer.kind != ObserverKind::Rpc {
                return;
            }
            observer
                .rpc_reply_nodes
                .retain(|_, (_, expiration_ts)| *expiration_ts == 0 || *expiration_ts >= now_ms());
            observer.rpc_reply_nodes.insert(
                rpc.request_id,
                (rpc.reply_node_id.clone(), rpc.expiration_ts),
            );
            let params: serde_json::Value =
                serde_json::from_str(&rpc.params_json).unwrap_or(serde_json::Value::Null);
            let body = serde_json::json!({
                "id": rpc.request_id,
                "method": rpc.method,
                "params": params,
            });
            push(
                state,
                socket,
                &observer,
                body.to_string().into_bytes(),
            )
            .await;
        }
        core_pb::to_transport_msg::Msg::SessionCloseNotification(n) => {
            tracing::info!(session_id = msg.session_id, reason = n.reason, "kick observer");
            state.observers.remove(&msg.session_id);
        }
        other => tracing::debug!(?other, "ignored transport notification"),
    }
}

/// NON Content 推送：token 原样回带，Observe 序号递增。
async fn push(
    state: &TransportState,
    socket: &Arc<UdpSocket>,
    observer: &Observer,
    payload: Vec<u8>,
) {
    let seq = observer.observe_seq.fetch_add(1, Ordering::Relaxed) + 1;
    let mut packet = Packet::new();
    packet.header.set_version(1);
    packet.header.set_type(MessageType::NonConfirmable);
    packet.header.code = MessageClass::Response(ResponseType::Content);
    packet.header.message_id = state.alloc_mid();
    packet.set_token(observer.token.clone());
    packet.set_observe_value(seq);
    packet.set_content_format(ContentFormat::ApplicationJSON);
    packet.payload = payload;
    match packet.to_bytes() {
        Ok(bytes) => {
            observer.last_activity_ms.store(now_ms(), Ordering::Relaxed);
            if let Err(e) = socket.send_to(&bytes, observer.addr).await {
                tracing::debug!(addr = %observer.addr, "coap push failed: {e}");
            }
        }
        Err(e) => tracing::warn!("coap push encode failed: {e}"),
    }
}
