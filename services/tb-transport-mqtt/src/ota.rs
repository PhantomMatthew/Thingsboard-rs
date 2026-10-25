//! OTA 固件/软件分块升级（fw / sw 两套 topic 同构）：
//! 订阅 v2/{fw,sw}/response/+（或 .../+/chunk/+）时，若设备有显式绑定的包（设备级优先于档案级），
//! 经 v1/devices/me/attributes 推 {fw,sw}_title/version/tag?/size/checksum_algorithm/checksum；
//! 设备 publish v2/{fw,sw}/request/{requestId}/chunk/{chunkIndex}（payload 可带 chunkSize），
//! 经 transport-api 拉取分块后回 v2/{fw,sw}/response/{requestId}/chunk/{chunkIndex}；
//! 无绑定包时回 v2/{fw,sw}/error（payload NOT_FOUND）。

use bytes::Bytes;
use tb_proto::common::OtaPackageType;
use tb_proto::transport as pb;
use tokio_util::codec::Framed;

use crate::TransportState;
use crate::codec::{MqttCodec, MqttPacket, PublishPacket};
use crate::session::{self, MqttStream, Session};

pub const DEFAULT_CHUNK_SIZE: i32 = 16 * 1024;
pub const MAX_CHUNK_SIZE: i32 = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OtaKind {
    Firmware,
    Software,
}

impl OtaKind {
    pub const ALL: [OtaKind; 2] = [OtaKind::Firmware, OtaKind::Software];

    fn segment(self) -> &'static str {
        match self {
            Self::Firmware => "fw",
            Self::Software => "sw",
        }
    }

    fn proto(self) -> OtaPackageType {
        match self {
            Self::Firmware => OtaPackageType::Firmware,
            Self::Software => OtaPackageType::Software,
        }
    }

    pub fn from_proto(value: i32) -> Option<Self> {
        match OtaPackageType::try_from(value) {
            Ok(OtaPackageType::Firmware) => Some(Self::Firmware),
            Ok(OtaPackageType::Software) => Some(Self::Software),
            _ => None,
        }
    }

    /// 订阅过滤器 v2/{fw,sw}/response/+ 或 v2/{fw,sw}/response/+/chunk/+。
    pub fn from_subscribe_filter(filter: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| {
            filter
                .strip_prefix("v2/")
                .and_then(|rest| rest.strip_prefix(kind.segment()))
                .is_some_and(|rest| rest == "/response/+" || rest == "/response/+/chunk/+")
        })
    }

    /// v2/{fw,sw}/request/{rest}：返回类型与 {requestId}/chunk/{chunkIndex} 部分。
    pub fn from_request_topic(topic: &str) -> Option<(Self, &str)> {
        Self::ALL.into_iter().find_map(|kind| {
            topic
                .strip_prefix("v2/")
                .and_then(|rest| rest.strip_prefix(kind.segment()))
                .and_then(|rest| rest.strip_prefix("/request/"))
                .map(|rest| (kind, rest))
        })
    }
}

fn requested_chunk_size(payload: &[u8]) -> i32 {
    std::str::from_utf8(payload)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .filter(|s| *s > 0)
        .unwrap_or(DEFAULT_CHUNK_SIZE)
        .min(MAX_CHUNK_SIZE)
}

/// 与 core 写入的 {fw,sw}_* shared 属性同构（tag 为空时省略）。
pub fn ota_attributes_body(kind: OtaKind, pkg: &pb::OtaPackageProto) -> serde_json::Value {
    let p = kind.segment();
    let mut body = serde_json::Map::new();
    body.insert(format!("{p}_title"), pkg.title.clone().into());
    body.insert(format!("{p}_version"), pkg.version.clone().into());
    if !pkg.tag.is_empty() {
        body.insert(format!("{p}_tag"), pkg.tag.clone().into());
    }
    body.insert(format!("{p}_size"), pkg.size.into());
    body.insert(format!("{p}_checksum_algorithm"), "SHA256".into());
    body.insert(format!("{p}_checksum"), pkg.checksum.clone().into());
    serde_json::Value::Object(body)
}

fn cache_key(session: &Session, kind: OtaKind) -> (String, OtaKind) {
    (session.info.session_id.clone(), kind)
}

/// 订阅即视为 OTA 就绪：有绑定包时缓存元数据并推 {fw,sw}_* 属性。
pub async fn check_package(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &Session,
    kind: OtaKind,
) {
    let Some(pkg) = resolve_package(state, &session.device, kind).await else {
        return;
    };
    state
        .ota_packages
        .insert(cache_key(session, kind), pkg.clone())
        .await;
    let body = ota_attributes_body(kind, &pkg);
    publish(
        framed,
        session::TOPIC_ATTRIBUTES.into(),
        Bytes::from(body.to_string()),
    )
    .await;
}

/// 经 transport-api 查设备当前绑定的包（无则 None）。
async fn resolve_package(
    state: &TransportState,
    device: &pb::DeviceInfoProto,
    kind: OtaKind,
) -> Option<pb::OtaPackageProto> {
    state
        .transport_api
        .get_ota_package(&device.tenant_id, &device.device_id, kind.proto())
        .await
        .map_err(|e| tracing::warn!("ota get_ota_package failed: {e}"))
        .ok()
        .flatten()
}

/// v2/{fw,sw}/request/{requestId}/chunk/{chunkIndex}：payload 为可选 chunkSize（ASCII 数字）。
pub async fn handle_chunk_request(
    framed: &mut Framed<MqttStream, MqttCodec>,
    state: &TransportState,
    session: &Session,
    kind: OtaKind,
    rest: &str,
    payload: &[u8],
) {
    let Some((request_id, chunk)) = rest.split_once("/chunk/") else {
        tracing::warn!(topic = rest, "bad ota request topic");
        return;
    };
    let Ok(chunk_index) = chunk.parse::<i32>() else {
        tracing::warn!(chunk, "bad ota chunk index");
        return;
    };
    let chunk_size = requested_chunk_size(payload);
    let key = cache_key(session, kind);
    let pkg = match state.ota_packages.get(&key).await {
        Some(pkg) => Some(pkg),
        None => resolve_package(state, &session.device, kind).await,
    };
    let Some(pkg) = pkg else {
        publish(
            framed,
            format!("v2/{}/error", kind.segment()),
            Bytes::from_static(b"NOT_FOUND"),
        )
        .await;
        return;
    };
    state.ota_packages.insert(key, pkg.clone()).await;
    match state
        .transport_api
        .get_ota_chunk(&pkg.ota_package_id, chunk_index, chunk_size)
        .await
    {
        Ok(data) => {
            publish(
                framed,
                format!(
                    "v2/{}/response/{request_id}/chunk/{chunk_index}",
                    kind.segment()
                ),
                data,
            )
            .await;
        }
        Err(e) => tracing::warn!("ota get_ota_chunk failed: {e}"),
    }
}

async fn publish(framed: &mut Framed<MqttStream, MqttCodec>, topic: String, payload: Bytes) {
    session::send(
        framed,
        MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: 0,
            retain: false,
            topic,
            packet_id: None,
            payload,
        }),
    )
    .await;
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

    #[test]
    fn topics_select_firmware_or_software() {
        assert_eq!(
            OtaKind::from_subscribe_filter("v2/fw/response/+"),
            Some(OtaKind::Firmware)
        );
        assert_eq!(
            OtaKind::from_subscribe_filter("v2/sw/response/+/chunk/+"),
            Some(OtaKind::Software)
        );
        assert_eq!(OtaKind::from_subscribe_filter("v2/sw/response/#"), None);
        assert_eq!(OtaKind::from_subscribe_filter("v2/xx/response/+"), None);
        assert_eq!(
            OtaKind::from_request_topic("v2/sw/request/7/chunk/0"),
            Some((OtaKind::Software, "7/chunk/0"))
        );
        assert_eq!(
            OtaKind::from_request_topic("v2/fw/request/1/chunk/2"),
            Some((OtaKind::Firmware, "1/chunk/2"))
        );
        assert_eq!(
            OtaKind::from_request_topic("v2/fw/response/1/chunk/2"),
            None
        );
    }

    #[test]
    fn attributes_body_uses_type_prefix_and_omits_empty_tag() {
        let pkg = pb::OtaPackageProto {
            ota_package_id: "id".into(),
            title: "app".into(),
            version: "2.0".into(),
            tag: String::new(),
            file_name: String::new(),
            content_type: String::new(),
            checksum: "abc".into(),
            size: 42,
        };
        let body = ota_attributes_body(OtaKind::Software, &pkg);
        assert_eq!(
            body,
            serde_json::json!({
                "sw_title": "app",
                "sw_version": "2.0",
                "sw_size": 42,
                "sw_checksum_algorithm": "SHA256",
                "sw_checksum": "abc",
            })
        );
        let tagged = pb::OtaPackageProto {
            tag: "stable".into(),
            ..pkg
        };
        assert_eq!(
            ota_attributes_body(OtaKind::Firmware, &tagged)["fw_tag"],
            "stable"
        );
    }
}
