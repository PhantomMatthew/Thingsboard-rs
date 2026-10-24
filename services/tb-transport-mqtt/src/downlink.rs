//! 下行通知：消费 tb_transport.notifications.<node_id>，按 session_id 路由到在线会话。
//! 处理 attribute_update（KeyValueProto.key 字符串，value 缺省=删除→JSON null）/
//! to_device_rpc / session_close_notification，其余 debug 忽略。

use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use tb_proto::core as core_pb;
use tb_queue::{QueueConsumer as _, StreamQueueConsumer, TbTopic};
use tokio::sync::mpsc;

use crate::codec::{MqttPacket, PublishPacket};
use crate::{SessionCommand, TransportState, UNSUBSCRIBED, session};

pub async fn run(state: TransportState) {
    let topic = TbTopic::transport_notifications(&state.node_id);
    if let Err(e) = tb_queue::admin::ensure_topic(&state.brokers, &topic, 1).await {
        tracing::warn!("ensure notifications topic failed: {e}");
    }
    let group_id = format!("tb-transport-mqtt-{}", state.node_id);
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
            Ok(msg) => match handle(&state, msg.payload.clone()).await {
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

struct DownlinkTarget {
    tx: mpsc::Sender<SessionCommand>,
    gateway_device: Option<String>,
}

fn target(state: &TransportState, session_id: &str) -> Option<DownlinkTarget> {
    state.sessions.get(session_id).map(|handle| DownlinkTarget {
        tx: handle.tx.clone(),
        gateway_device: handle.gateway_device.clone(),
    })
}

async fn handle(state: &TransportState, payload: Bytes) -> Result<(), String> {
    let registry = &state.sessions;
    let Ok(msg) = core_pb::ToTransportMsg::decode(payload) else {
        tracing::warn!("undecodable transport notification");
        return Ok(());
    };
    let Some(oneof) = msg.msg else {
        return Ok(());
    };
    match oneof {
        core_pb::to_transport_msg::Msg::AttributeUpdate(n) => {
            let Some((target, qos)) = registry.get(&msg.session_id).map(|handle| {
                (
                    DownlinkTarget {
                        tx: handle.tx.clone(),
                        gateway_device: handle.gateway_device.clone(),
                    },
                    handle.attributes_qos.load(Ordering::Relaxed),
                )
            }) else {
                return Ok(());
            };
            if qos == UNSUBSCRIBED {
                return Ok(());
            }
            // value 缺省 = 删除通知，JSON null；key 用字符串（core 侧已带）。
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
            send(
                &target.tx,
                session::TOPIC_ATTRIBUTES.to_string(),
                serde_json::to_vec(&body).unwrap_or_default(),
                qos,
            )
            .await?;
        }
        core_pb::to_transport_msg::Msg::ToDeviceRpc(rpc) => {
            if rpc.expiration_ts > 0 && rpc.expiration_ts < chrono::Utc::now().timestamp_millis() {
                return Ok(());
            }
            let Some(target) = target(state, &msg.session_id) else {
                return Ok(());
            };
            let Some(qos) = registry
                .get(&msg.session_id)
                .map(|handle| handle.rpc_qos.load(Ordering::Relaxed))
            else {
                return Ok(());
            };
            if qos == UNSUBSCRIBED {
                return Ok(());
            }
            state
                .rpc_reply_nodes
                .insert(
                    (msg.session_id.clone(), rpc.request_id),
                    rpc.reply_node_id.clone(),
                )
                .await;
            let params: serde_json::Value =
                serde_json::from_str(&rpc.params_json).unwrap_or(serde_json::Value::Null);
            // 网关子设备会话：包装成 v1/gateway/rpc {"device","data":{"id","method","params"}}。
            let (topic, body) = match &target.gateway_device {
                Some(name) => (
                    crate::gateway::TOPIC_RPC.to_string(),
                    serde_json::json!({
                        "device": name,
                        "data": {"id": rpc.request_id, "method": rpc.method, "params": params},
                    }),
                ),
                None => (
                    format!("{}{}", session::TOPIC_RPC_REQUEST_PREFIX, rpc.request_id),
                    serde_json::json!({"method": rpc.method, "params": params}),
                ),
            };
            send(&target.tx, topic, body.to_string().into_bytes(), qos).await?;
        }
        core_pb::to_transport_msg::Msg::OtaUpdate(n) => {
            let Some((target, qos)) = registry.get(&msg.session_id).map(|handle| {
                (
                    DownlinkTarget {
                        tx: handle.tx.clone(),
                        gateway_device: handle.gateway_device.clone(),
                    },
                    handle.attributes_qos.load(Ordering::Relaxed),
                )
            }) else {
                return Ok(());
            };
            let Some(pkg) = n.ota_package else {
                return Ok(());
            };
            state
                .ota_packages
                .insert(msg.session_id.clone(), pkg.clone())
                .await;
            let body = crate::ota::fw_attributes_body(&pkg);
            let qos = if qos == UNSUBSCRIBED { 0 } else { qos };
            send(
                &target.tx,
                session::TOPIC_ATTRIBUTES.to_string(),
                body.to_string().into_bytes(),
                qos,
            )
            .await?;
        }
        core_pb::to_transport_msg::Msg::SessionCloseNotification(n) => {
            tracing::info!(
                session_id = msg.session_id,
                reason = n.reason,
                "kick session"
            );
            let tx = registry
                .get(&msg.session_id)
                .map(|handle| handle.tx.clone());
            let delivered = if let Some(tx) = tx {
                tx.send(SessionCommand::Close {
                    session_id: msg.session_id.clone(),
                    reason: n.reason,
                })
                .await
                .is_ok()
            } else {
                false
            };
            if !delivered {
                registry.remove(&msg.session_id);
            }
        }
        other => tracing::debug!(?other, "ignored transport notification"),
    }
    Ok(())
}

async fn send(
    tx: &mpsc::Sender<SessionCommand>,
    topic: String,
    payload: Vec<u8>,
    qos: u8,
) -> Result<(), String> {
    let packet = MqttPacket::Publish(PublishPacket {
        dup: false,
        qos,
        retain: false,
        topic,
        packet_id: None, // QoS 1 由会话 task 分配
        payload: Bytes::from(payload),
    });
    tx.send(SessionCommand::Packet(packet))
        .await
        .map_err(|_| "session channel closed before downlink".to_string())
}
