//! provisioning 会话：CONNECT username="provision" 的连接仅处理 provision/request，
//! 不注册设备会话。响应发 provision/response。

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::TransportState;
use crate::codec::{MqttCodec, MqttPacket, PublishPacket};

pub const PROVISION_USERNAME: &str = "provision";
pub const TOPIC_REQUEST: &str = "provision/request";
pub const TOPIC_RESPONSE: &str = "provision/response";

const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProvisionRequest {
    device_name: String,
    provision_device_key: String,
    provision_device_secret: String,
}

pub async fn serve(framed: &mut Framed<TcpStream, MqttCodec>, state: &TransportState) {
    loop {
        match tokio::time::timeout(IDLE_TIMEOUT, framed.next()).await {
            Err(_) | Ok(None) => return,
            Ok(Some(Err(e))) => {
                tracing::debug!("provision codec error: {e}");
                return;
            }
            Ok(Some(Ok(packet))) => match packet {
                MqttPacket::Publish(p) if p.topic == TOPIC_REQUEST => {
                    handle_request(framed, state, &p.payload).await;
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
    framed: &mut Framed<TcpStream, MqttCodec>,
    state: &TransportState,
    payload: &[u8],
) {
    let body = match serde_json::from_slice::<ProvisionRequest>(payload) {
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
            Ok(resp) if resp.status == "SUCCESS" => json!({
                "status": "SUCCESS",
                "credentialsType": resp.credentials_type,
                "credentialsValue": resp.credentials_value,
                "deviceName": resp.device_name,
            }),
            Ok(resp) => json!({"status": "FAILURE", "errorMsg": resp.error_msg}),
            Err(e) => json!({"status": "FAILURE", "errorMsg": e.to_string()}),
        },
        Err(e) => json!({"status": "FAILURE", "errorMsg": format!("bad request: {e}")}),
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
}
