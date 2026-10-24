//! 下行通知：消费 tb_transport.notifications.<node_id>，把 to_device_rpc 投递到长轮询会话。

use std::time::Duration;

use prost::Message as _;
use tb_proto::core as core_pb;
use tb_queue::{QueueConsumer as _, StreamQueueConsumer, TbTopic};

use crate::TransportState;

pub async fn run(state: TransportState) {
    let topic = TbTopic::transport_notifications(&state.node_id);
    if let Err(e) = tb_queue::admin::ensure_topic(&state.brokers, &topic, 1).await {
        tracing::warn!("ensure notifications topic failed: {e}");
    }
    let group_id = format!("tb-transport-http-{}", state.node_id);
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
            Ok(msg) => handle(&state, msg.payload).await,
            Err(e) => {
                tracing::error!("notifications consumer error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn handle(state: &TransportState, payload: bytes::Bytes) {
    let Ok(msg) = core_pb::ToTransportMsg::decode(payload) else {
        tracing::warn!("undecodable transport notification");
        return;
    };
    match msg.msg {
        Some(core_pb::to_transport_msg::Msg::ToDeviceRpc(rpc)) => {
            if rpc.expiration_ts > 0 && rpc.expiration_ts < chrono::Utc::now().timestamp_millis() {
                return;
            }
            if let Some(tx) = state.rpc_sessions.get(&msg.session_id) {
                let _ = tx.send(rpc).await;
            }
        }
        Some(core_pb::to_transport_msg::Msg::SessionCloseNotification(n)) => {
            tracing::info!(
                session_id = msg.session_id,
                reason = n.reason,
                "kick session"
            );
            state.rpc_sessions.remove(&msg.session_id);
            state
                .rpc_reply_routes
                .retain(|_, route| route.session_id != msg.session_id);
        }
        other => tracing::debug!(?other, "ignored transport notification"),
    }
}
