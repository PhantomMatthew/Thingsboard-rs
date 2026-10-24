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

use crate::{Observer, ObserverKind, TransportState, close_observer, now_ms};

const MAX_CONSECUTIVE_SEND_FAILURES: u8 = 3;

pub async fn run(state: TransportState, socket: Arc<UdpSocket>) {
    let topic = TbTopic::transport_notifications(&state.node_id);
    if let Err(e) = tb_queue::admin::ensure_topic(&state.brokers, &topic, 1).await {
        tracing::warn!("ensure notifications topic failed: {e}");
    }
    let group_id = format!("tb-transport-coap-{}", state.node_id);
    let mut consumer = loop {
        match StreamQueueConsumer::new_manual_commit(&state.brokers, &group_id, &[&topic]) {
            Ok(c) => break c,
            Err(e) => {
                tracing::error!("notifications consumer failed to start: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    loop {
        match consumer.recv().await {
            Ok(msg) => match handle(&state, &socket, msg.payload.clone()).await {
                Ok(()) => {
                    if let Err(e) = consumer.commit(&msg).await {
                        tracing::error!("notifications commit failed: {e}");
                    }
                }
                Err(e) => tracing::warn!("notification delivery failed: {e}"),
            },
            Err(e) => {
                tracing::error!("notifications consumer error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn handle(
    state: &TransportState,
    socket: &Arc<UdpSocket>,
    payload: Bytes,
) -> Result<(), String> {
    let Ok(msg) = core_pb::ToTransportMsg::decode(payload) else {
        tracing::warn!("undecodable transport notification");
        return Ok(());
    };
    let Some(oneof) = msg.msg else {
        return Ok(());
    };
    match oneof {
        core_pb::to_transport_msg::Msg::AttributeUpdate(n) => {
            let Some(observer) = state
                .observers
                .get(&msg.session_id)
                .map(|entry| entry.value().clone())
            else {
                return Ok(());
            };
            if observer.kind != ObserverKind::Attributes {
                return Ok(());
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
            push(
                state,
                socket,
                &observer,
                serde_json::to_vec(&body).unwrap_or_default(),
            )
            .await?;
        }
        core_pb::to_transport_msg::Msg::ToDeviceRpc(rpc) => {
            if rpc.expiration_ts > 0 && rpc.expiration_ts < now_ms() {
                return Ok(());
            }
            let Some(observer) = state
                .observers
                .get(&msg.session_id)
                .map(|entry| entry.value().clone())
            else {
                return Ok(());
            };
            if observer.kind != ObserverKind::Rpc {
                return Ok(());
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
            push(state, socket, &observer, body.to_string().into_bytes()).await?;
        }
        core_pb::to_transport_msg::Msg::SessionCloseNotification(n) => {
            tracing::info!(
                session_id = msg.session_id,
                reason = n.reason,
                "kick observer"
            );
            state.observers.remove(&msg.session_id);
        }
        other => tracing::debug!(?other, "ignored transport notification"),
    }
    Ok(())
}

/// NON Content 推送：token 原样回带，Observe 序号递增；推送不刷新入站租约。
async fn push(
    state: &TransportState,
    socket: &Arc<UdpSocket>,
    observer: &Observer,
    payload: Vec<u8>,
) -> Result<(), String> {
    let seq = observer.observe_seq.fetch_add(1, Ordering::Relaxed) + 1;
    let mut packet = Packet::new();
    packet.header.set_version(1);
    packet.header.set_type(MessageType::NonConfirmable);
    packet.header.code = MessageClass::Response(ResponseType::Content);
    packet.header.message_id = state.alloc_mid();
    observer
        .last_push_mid
        .store(packet.header.message_id, Ordering::Relaxed);
    packet.set_token(observer.token.clone());
    packet.set_observe_value(seq);
    packet.set_content_format(ContentFormat::ApplicationJSON);
    packet.payload = payload;
    let bytes = packet
        .to_bytes()
        .map_err(|error| format!("coap push encode failed: {error}"))?;
    match socket.send_to(&bytes, observer.addr).await {
        Ok(_) => {
            observer
                .consecutive_send_failures
                .store(0, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => {
            let failures = observer
                .consecutive_send_failures
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if failures >= MAX_CONSECUTIVE_SEND_FAILURES {
                close_observer(
                    state,
                    &observer.info.session_id,
                    "coap push failed three consecutive times",
                )
                .await;
            }
            Err(format!("coap push failed: {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU16, AtomicU32};

    #[tokio::test]
    async fn successful_push_does_not_refresh_inbound_liveness() {
        let state = TransportState::new(
            "localhost:29092",
            &format!("coap-push-unit-{}", uuid::Uuid::now_v7()),
        )
        .unwrap();
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let observer = Observer {
            addr: client.local_addr().unwrap(),
            token: vec![0x42],
            device: tb_proto::transport::DeviceInfoProto::default(),
            info: core_pb::SessionInfoProto::default(),
            kind: ObserverKind::Attributes,
            observe_seq: AtomicU32::new(0),
            last_activity_ms: AtomicI64::new(123),
            consecutive_send_failures: AtomicU8::new(0),
            last_push_mid: AtomicU16::new(0),
            rpc_reply_nodes: dashmap::DashMap::new(),
        };

        push(&state, &socket, &observer, b"{\"value\":1}".to_vec())
            .await
            .unwrap();

        assert_eq!(observer.last_activity_ms.load(Ordering::Relaxed), 123);
        assert_eq!(
            observer.consecutive_send_failures.load(Ordering::Relaxed),
            0
        );
        assert_ne!(observer.last_push_mid.load(Ordering::Relaxed), 0);
        let mut buf = [0u8; 1500];
        let size = tokio::time::timeout(Duration::from_secs(1), client.recv(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let packet = Packet::from_bytes(&buf[..size]).unwrap();
        assert_eq!(packet.get_token(), [0x42]);
        assert_eq!(packet.payload, b"{\"value\":1}");
    }
}
