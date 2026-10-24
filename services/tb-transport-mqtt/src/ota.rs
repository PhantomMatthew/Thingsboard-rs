//! OTA 固件分块升级：订阅 v2/fw/response/+ 触发元信息检查（current_fw_* shared 属性比对），
//! 元信息经 v1/devices/me/attributes 推送（fw_title/fw_version/fw_size/fw_checksum/fw_tag）；
//! 设备 publish v2/fw/request/{requestId}/chunk/{chunkIndex}（payload 可带 chunkSize），
//! 经 transport-api 拉取分块后回 v2/fw/response/{requestId}/chunk/{chunkIndex}。

use bytes::Bytes;
use tb_proto::common::OtaPackageType;
use tb_proto::transport as pb;
use tokio_util::codec::Framed;

use crate::TransportState;
use crate::codec::{MqttCodec, MqttPacket, PublishPacket};
use crate::session::{self, MqttStream, Session};

pub const TOPIC_FW_SUBSCRIBE: &str = "v2/fw/response/+";
pub const TOPIC_FW_REQUEST_PREFIX: &str = "v2/fw/request/";
pub const DEFAULT_CHUNK_SIZE: i32 = 16 * 1024;
pub const MAX_CHUNK_SIZE: i32 = 256 * 1024;

fn requested_chunk_size(payload: &[u8]) -> i32 {
    std::str::from_utf8(payload)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .filter(|s| *s > 0)
        .unwrap_or(DEFAULT_CHUNK_SIZE)
        .min(MAX_CHUNK_SIZE)
}

pub fn fw_attributes_body(pkg: &pb::OtaPackageProto) -> serde_json::Value {
    let tag = if pkg.tag.is_empty() {
        format!("{} {}", pkg.title, pkg.version)
    } else {
        pkg.tag.clone()
    };
    serde_json::json!({
        "fw_title": pkg.title,
        "fw_version": pkg.version,
        "fw_tag": tag,
        "fw_size": pkg.size,
        "fw_checksum": pkg.checksum,
        "fw_checksum_algorithm": "SHA256",
    })
}

/// 读取设备当前固件版本（shared 属性 current_fw_title/current_fw_version），
/// 有可更新包时缓存元数据并向设备推 fw_* 属性。
pub async fn check_firmware(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &Session,
) {
    let Some(pkg) = resolve_firmware(state, &session.device).await else {
        return;
    };
    state
        .ota_packages
        .insert(session.info.session_id.clone(), pkg.clone())
        .await;
    let body = fw_attributes_body(&pkg);
    session::send(
        framed,
        MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: 0,
            retain: false,
            topic: session::TOPIC_ATTRIBUTES.into(),
            packet_id: None,
            payload: Bytes::from(body.to_string()),
        }),
    )
    .await;
}

/// 经 transport-api 查可更新固件包（无则 None）。
async fn resolve_firmware(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
) -> Option<pb::OtaPackageProto> {
    let attrs = state
        .transport_api
        .get_attributes(
            &device.device_id,
            Vec::new(),
            vec!["current_fw_title".into(), "current_fw_version".into()],
        )
        .await
        .map_err(|e| tracing::warn!("ota get_attributes failed: {e}"))
        .ok()?;
    let str_attr = |key: &str| {
        attrs
            .shared_attributes
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| match &kv.value {
                Some(tb_proto::common::key_value_proto::Value::StrV(s)) => Some(s.clone()),
                _ => None,
            })
    };
    let title = str_attr("current_fw_title").unwrap_or_default();
    let version = str_attr("current_fw_version").unwrap_or_default();
    state
        .transport_api
        .get_ota_package(
            &device.tenant_id,
            &device.device_profile_id,
            OtaPackageType::Firmware,
            &title,
            &version,
        )
        .await
        .map_err(|e| tracing::warn!("ota get_ota_package failed: {e}"))
        .ok()
        .flatten()
}

/// v2/fw/request/{requestId}/chunk/{chunkIndex}：payload 为可选 chunkSize（ASCII 数字）。
pub async fn handle_chunk_request(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &Session,
    rest: &str,
    payload: &[u8],
) {
    let Some((request_id, chunk)) = rest.split_once("/chunk/") else {
        tracing::warn!(topic = rest, "bad firmware request topic");
        return;
    };
    let Ok(chunk_index) = chunk.parse::<i32>() else {
        tracing::warn!(chunk, "bad firmware chunk index");
        return;
    };
    let chunk_size = requested_chunk_size(payload);
    let pkg = match state.ota_packages.get(&session.info.session_id).await {
        Some(pkg) => Some(pkg),
        None => resolve_firmware(state, &session.device).await,
    };
    let Some(pkg) = pkg else {
        tracing::debug!("firmware chunk requested but no ota package available");
        return;
    };
    state
        .ota_packages
        .insert(session.info.session_id.clone(), pkg.clone())
        .await;
    match state
        .transport_api
        .get_ota_chunk(&pkg.ota_package_id, chunk_index, chunk_size)
        .await
    {
        Ok(data) => {
            session::send(
                framed,
                MqttPacket::Publish(PublishPacket {
                    dup: false,
                    qos: 0,
                    retain: false,
                    topic: format!("v2/fw/response/{request_id}/chunk/{chunk_index}"),
                    packet_id: None,
                    payload: data,
                }),
            )
            .await;
        }
        Err(e) => tracing::warn!("ota get_ota_chunk failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_size_is_positive_and_capped() {
        assert_eq!(requested_chunk_size(b"100"), 100);
        assert_eq!(requested_chunk_size(b"0"), DEFAULT_CHUNK_SIZE);
        assert_eq!(requested_chunk_size(b"invalid"), DEFAULT_CHUNK_SIZE);
        assert_eq!(
            requested_chunk_size((MAX_CHUNK_SIZE + 1).to_string().as_bytes()),
            MAX_CHUNK_SIZE
        );
    }
}
