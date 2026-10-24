//! 下行通知：消费 tb_transport.notifications.<node_id>，把 to_device_rpc 投递到长轮询会话。

use std::time::Duration;

use prost::Message as _;
use tb_proto::core as core_pb;
use tb_queue::{QueueConsumer as _, StreamQueueConsumer, TbTopic};

use crate::{RpcSession, TransportState, remove_rpc_session};

pub async fn run(state: TransportState) {
    let topic = TbTopic::transport_notifications(&state.node_id);
    if let Err(e) = tb_queue::admin::ensure_topic(&state.brokers, &topic, 1).await {
        tracing::warn!("ensure notifications topic failed: {e}");
    }
    let group_id = format!("tb-transport-http-{}", state.node_id);
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

fn rpc_session(state: &TransportState, session_id: &str) -> Option<std::sync::Arc<RpcSession>> {
    let device_id = state
        .rpc_session_ids
        .get(session_id)
        .map(|entry| entry.clone())?;
    state
        .rpc_sessions
        .get(&device_id)
        .map(|entry| entry.value().clone())
}

async fn handle(state: &TransportState, payload: bytes::Bytes) -> Result<(), String> {
    let Ok(msg) = core_pb::ToTransportMsg::decode(payload) else {
        tracing::warn!("undecodable transport notification");
        return Ok(());
    };
    match msg.msg {
        Some(core_pb::to_transport_msg::Msg::ToDeviceRpc(rpc)) => {
            if rpc.expiration_ts > 0 && rpc.expiration_ts < chrono::Utc::now().timestamp_millis() {
                return Ok(());
            }
            let Some(session) = rpc_session(state, &msg.session_id) else {
                return Ok(());
            };
            if session.tx.send(rpc).await.is_err() {
                remove_rpc_session(state, &msg.session_id);
                return Err("RPC long-poll session channel closed".into());
            }
        }
        Some(core_pb::to_transport_msg::Msg::SessionCloseNotification(n)) => {
            tracing::info!(
                session_id = msg.session_id,
                reason = n.reason,
                "kick session"
            );
            remove_rpc_session(state, &msg.session_id);
        }
        other => tracing::debug!(?other, "ignored transport notification"),
    }
    Ok(())
}
