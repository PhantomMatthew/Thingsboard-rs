//! 下行通知：消费 tb_transport.notifications.<node_id>，按 session_id 路由到在线会话。
//! Phase 3b 处理 attribute_update / to_device_rpc / session_close_notification，其余 debug 忽略。

use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use tb_proto::core as core_pb;
use tb_queue::{QueueConsumer as _, StreamQueueConsumer, TbTopic};

use crate::codec::{MqttPacket, PublishPacket};
use crate::{SessionRegistry, TransportState, UNSUBSCRIBED, session};

pub async fn run(state: TransportState) {
    let topic = TbTopic::transport_notifications(&state.node_id);
    if let Err(e) = tb_queue::admin::ensure_topic(&state.brokers, &topic, 1).await {
        tracing::warn!("ensure notifications topic failed: {e}");
    }
    let group_id = format!("tb-transport-mqtt-{}", state.node_id);
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
            Ok(msg) => handle(&state.sessions, msg.payload).await,
            Err(e) => {
                tracing::error!("notifications consumer error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn handle(registry: &SessionRegistry, payload: Bytes) {
    let Ok(msg) = core_pb::ToTransportMsg::decode(payload) else {
        tracing::warn!("undecodable transport notification");
        return;
    };
    let Some(oneof) = msg.msg else {
        return;
    };
    match oneof {
        core_pb::to_transport_msg::Msg::AttributeUpdate(n) => {
            let Some(handle) = registry.get(&msg.session_id) else {
                return;
            };
            let qos = handle.attributes_qos.load(Ordering::Relaxed);
            if qos == UNSUBSCRIBED {
                return;
            }
            // proto 仅携带 key_id，key 反查待 Phase 5 core 侧补齐；先按 id 字符串下发。
            let mut body = serde_json::Map::new();
            for kv in &n.attributes {
                if let Some((key, value)) =
                    tb_proto::convert::kv_from_proto(kv, kv.key_id.to_string())
                {
                    body.insert(key, serde_json::to_value(&value).unwrap_or_default());
                }
            }
            send(
                handle,
                session::TOPIC_ATTRIBUTES.to_string(),
                serde_json::to_vec(&body).unwrap_or_default(),
                qos,
            )
            .await;
        }
        core_pb::to_transport_msg::Msg::ToDeviceRpc(rpc) => {
            if rpc.expiration_ts > 0 && rpc.expiration_ts < chrono::Utc::now().timestamp_millis() {
                return;
            }
            let Some(handle) = registry.get(&msg.session_id) else {
                return;
            };
            let qos = handle.rpc_qos.load(Ordering::Relaxed);
            if qos == UNSUBSCRIBED {
                return;
            }
            let params: serde_json::Value =
                serde_json::from_str(&rpc.params_json).unwrap_or(serde_json::Value::Null);
            let body = serde_json::json!({"method": rpc.method, "params": params});
            send(
                handle,
                format!("{}{}", session::TOPIC_RPC_REQUEST_PREFIX, rpc.request_id),
                body.to_string().into_bytes(),
                qos,
            )
            .await;
        }
        core_pb::to_transport_msg::Msg::SessionCloseNotification(n) => {
            // 移除注册表项使会话侧 channel 关闭，会话 task 自行清理退出。
            tracing::info!(
                session_id = msg.session_id,
                reason = n.reason,
                "kick session"
            );
            registry.remove(&msg.session_id);
        }
        other => tracing::debug!(?other, "ignored transport notification"),
    }
}

async fn send(
    handle: dashmap::mapref::one::Ref<'_, String, crate::SessionHandle>,
    topic: String,
    payload: Vec<u8>,
    qos: u8,
) {
    let packet = MqttPacket::Publish(PublishPacket {
        dup: false,
        qos,
        retain: false,
        topic,
        packet_id: None, // QoS 1 由会话 task 分配
        payload: Bytes::from(payload),
    });
    if handle.tx.send(packet).await.is_err() {
        tracing::debug!("session channel closed before downlink");
    }
}
