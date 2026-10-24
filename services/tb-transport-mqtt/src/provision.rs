//! provisioning 会话：CONNECT username="provision" 的连接仅处理 provision/request，
//! 不注册设备会话。响应发 provision/response。

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio_util::codec::Framed;

use crate::TransportState;
use crate::codec::{MqttCodec, MqttPacket, PublishPacket};
use crate::session::MqttStream;

pub const PROVISION_USERNAME: &str = "provision";
pub const TOPIC_REQUEST: &str = "provision/request";
pub const TOPIC_RESPONSE: &str = "provision/response";

const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_ATTEMPTS: u8 = 20;
const MAX_CONSECUTIVE_FAILURES: u8 = 5;

#[derive(Default)]
struct ProvisionLimits {
    attempts: u8,
    consecutive_failures: u8,
}

impl ProvisionLimits {
    fn begin_attempt(&mut self) -> bool {
        if self.attempts >= MAX_ATTEMPTS {
            return false;
        }
        self.attempts += 1;
        true
    }

    fn record_result(&mut self, success: bool) -> bool {
        if success {
            self.consecutive_failures = 0;
        } else {
            self.consecutive_failures += 1;
        }
        self.attempts >= MAX_ATTEMPTS || self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProvisionRequest {
    device_name: String,
    provision_device_key: String,
    provision_device_secret: String,
}

pub async fn serve(framed: &mut Framed<MqttStream, MqttCodec>, state: &TransportState) {
    let mut limits = ProvisionLimits::default();
    loop {
        match tokio::time::timeout(IDLE_TIMEOUT, framed.next()).await {
            Err(_) | Ok(None) => return,
            Ok(Some(Err(e))) => {
                tracing::debug!("provision codec error: {e}");
                return;
            }
            Ok(Some(Ok(packet))) => match packet {
                MqttPacket::Publish(p) if p.topic == TOPIC_REQUEST => {
                    if !limits.begin_attempt() {
                        tracing::warn!("provision attempt limit reached");
                        return;
                    }
                    let success = handle_request(framed, state, &p.payload).await;
                    if limits.record_result(success) {
                        tracing::warn!("provision connection closed by failure/attempt limit");
                        return;
                    }
                }
                MqttPacket::Subscribe { packet_id, filters } => {
                    let granted = filters
                        .iter()
                        .map(|(f, qos)| {
                            if f == TOPIC_RESPONSE {
                                (*qos).min(1)
                            } else {
                                0x80
                            }
                        })
                        .collect();
                    let _ = framed.send(MqttPacket::SubAck { packet_id, granted }).await;
                }
                MqttPacket::PingReq => {
                    let _ = framed.send(MqttPacket::PingResp).await;
                }
                MqttPacket::Disconnect => return,
                other => tracing::debug!(?other, "ignored packet in provision session"),
            },
        }
    }
}

async fn handle_request(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    payload: &[u8],
) -> bool {
    let (body, success) = match serde_json::from_slice::<ProvisionRequest>(payload) {
        Ok(req) => match state
            .transport_api
            .provision_device(
                "",
                &req.device_name,
                &req.provision_device_key,
                &req.provision_device_secret,
            )
            .await
        {
            Ok(resp) if resp.status == "SUCCESS" => (
                json!({
                    "status": "SUCCESS",
                    "credentialsType": resp.credentials_type,
                    "credentialsValue": resp.credentials_value,
                    "deviceName": resp.device_name,
                }),
                true,
            ),
            Ok(resp) => (
                json!({"status": "FAILURE", "errorMsg": resp.error_msg}),
                false,
            ),
            Err(e) => (
                json!({"status": "FAILURE", "errorMsg": e.to_string()}),
                false,
            ),
        },
        Err(e) => (
            json!({"status": "FAILURE", "errorMsg": format!("bad request: {e}")}),
            false,
        ),
    };
    if let Err(e) = framed
        .send(MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: 0,
            retain: false,
            topic: TOPIC_RESPONSE.into(),
            packet_id: None,
            payload: Bytes::from(body.to_string()),
        }))
        .await
    {
        tracing::debug!("provision response send failed: {e}");
    }
    success
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_consecutive_failures_close_provision_session() {
        let mut limits = ProvisionLimits::default();
        for _ in 0..MAX_CONSECUTIVE_FAILURES - 1 {
            assert!(limits.begin_attempt());
            assert!(!limits.record_result(false));
        }
        assert!(limits.begin_attempt());
        assert!(limits.record_result(false));
    }

    #[test]
    fn success_resets_failure_streak_and_attempts_are_capped() {
        let mut limits = ProvisionLimits::default();
        for _ in 0..4 {
            assert!(limits.begin_attempt());
            assert!(!limits.record_result(false));
        }
        assert!(limits.begin_attempt());
        assert!(!limits.record_result(true));
        for _ in 0..4 {
            assert!(limits.begin_attempt());
            assert!(!limits.record_result(false));
        }
        assert!(limits.begin_attempt());
        assert!(!limits.record_result(true));

        let mut limits = ProvisionLimits::default();
        for _ in 0..MAX_ATTEMPTS - 1 {
            assert!(limits.begin_attempt());
            assert!(!limits.record_result(true));
        }
        assert!(limits.begin_attempt());
        assert!(limits.record_result(true));
        assert!(!limits.begin_attempt());
    }
}
