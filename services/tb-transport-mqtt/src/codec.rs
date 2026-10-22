//! MQTT 3.1.1（protocol level 4）二进制 codec。
//! 仅实现设备侧所需控制包；QoS 2 流程（PUBREC/PUBREL/PUBCOMP）不支持。
//! CONNECT 可识别 level 5，由 session 层回 CONNACK 拒绝。

use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub const MAX_PACKET_SIZE: usize = 64 * 1024 * 1024;

pub const LEVEL_3_1_1: u8 = 4;

pub mod connack_code {
    pub const ACCEPTED: u8 = 0;
    pub const UNACCEPTABLE_PROTOCOL_VERSION: u8 = 1;
    pub const IDENTIFIER_REJECTED: u8 = 2;
    pub const BAD_USERNAME_PASSWORD: u8 = 4;
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectPacket {
    pub protocol_level: u8,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    pub will: Option<LastWill>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LastWill {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishPacket {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MqttPacket {
    Connect(Box<ConnectPacket>),
    ConnAck {
        session_present: bool,
        return_code: u8,
    },
    Publish(PublishPacket),
    PubAck(u16),
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    SubAck {
        packet_id: u16,
        granted: Vec<u8>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    UnsubAck(u16),
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("malformed packet: {0}")]
    Malformed(&'static str),
    #[error("packet too large: {0} bytes")]
    PacketTooLarge(usize),
    #[error("unsupported packet type: {0}")]
    UnsupportedType(u8),
    #[error("string too long")]
    StringTooLong,
}

#[derive(Default)]
pub struct MqttCodec;

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_u8(&mut self) -> Result<u8, CodecError> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or(CodecError::Malformed("unexpected end of packet"))?;
        self.pos += 1;
        Ok(b)
    }

    fn read_u16(&mut self) -> Result<u16, CodecError> {
        if self.remaining() < 2 {
            return Err(CodecError::Malformed("unexpected end of packet"));
        }
        let v = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.remaining() < n {
            return Err(CodecError::Malformed("unexpected end of packet"));
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn read_bin(&mut self) -> Result<Vec<u8>, CodecError> {
        let len = self.read_u16()? as usize;
        Ok(self.read_bytes(len)?.to_vec())
    }

    fn read_utf8(&mut self) -> Result<String, CodecError> {
        let len = self.read_u16()? as usize;
        let raw = self.read_bytes(len)?;
        String::from_utf8(raw.to_vec()).map_err(|_| CodecError::Malformed("invalid utf8 string"))
    }

    /// MQTT 5 属性段：varint 长度前缀 + 内容，仅跳过。
    fn skip_properties(&mut self) -> Result<(), CodecError> {
        let len = read_varint_buf(self, "properties length")?;
        self.read_bytes(len)?;
        Ok(())
    }
}

fn read_varint_buf(cur: &mut Cursor<'_>, what: &'static str) -> Result<usize, CodecError> {
    let mut value = 0usize;
    for i in 0..4 {
        let b = cur.read_u8()?;
        value |= ((b & 0x7f) as usize) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(CodecError::Malformed(what))
}

/// 从帧头区域读 remaining length：返回 (值, varint 字节数)；数据不足返回 None。
fn read_remaining_length(src: &[u8]) -> Result<Option<(usize, usize)>, CodecError> {
    let mut value = 0usize;
    for i in 0..4 {
        let Some(&b) = src.get(i) else {
            return Ok(None);
        };
        value |= ((b & 0x7f) as usize) << (7 * i);
        if b & 0x80 == 0 {
            if value > MAX_PACKET_SIZE {
                return Err(CodecError::PacketTooLarge(value));
            }
            return Ok(Some((value, i + 1)));
        }
    }
    Err(CodecError::Malformed("remaining length exceeds 4 bytes"))
}

impl Decoder for MqttCodec {
    type Item = MqttPacket;
    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<MqttPacket>, CodecError> {
        if src.len() < 2 {
            return Ok(None);
        }
        let Some((remaining, rl_bytes)) = read_remaining_length(&src[1..])? else {
            return Ok(None);
        };
        let total = 1 + rl_bytes + remaining;
        if src.len() < total {
            return Ok(None);
        }
        let frame = src.split_to(total);
        let byte1 = frame[0];
        let packet_type = byte1 >> 4;
        let flags = byte1 & 0x0f;
        let body = &frame[1 + rl_bytes..];
        match packet_type {
            1 => decode_connect(body),
            2 => {
                let mut cur = Cursor::new(body);
                let session_present = cur.read_u8()? & 0x01 != 0;
                let return_code = cur.read_u8()?;
                Ok(Some(MqttPacket::ConnAck {
                    session_present,
                    return_code,
                }))
            }
            3 => decode_publish(flags, body),
            4 => {
                let mut cur = Cursor::new(body);
                Ok(Some(MqttPacket::PubAck(cur.read_u16()?)))
            }
            8 => decode_subscribe(flags, body),
            9 => {
                let mut cur = Cursor::new(body);
                let packet_id = cur.read_u16()?;
                let granted = cur.read_bytes(cur.remaining())?.to_vec();
                Ok(Some(MqttPacket::SubAck { packet_id, granted }))
            }
            10 => decode_unsubscribe(flags, body),
            11 => {
                let mut cur = Cursor::new(body);
                Ok(Some(MqttPacket::UnsubAck(cur.read_u16()?)))
            }
            12 => Ok(Some(MqttPacket::PingReq)),
            13 => Ok(Some(MqttPacket::PingResp)),
            14 => Ok(Some(MqttPacket::Disconnect)),
            other => Err(CodecError::UnsupportedType(other)),
        }
    }
}

fn decode_connect(body: &[u8]) -> Result<Option<MqttPacket>, CodecError> {
    let mut cur = Cursor::new(body);
    let protocol_name = cur.read_utf8()?;
    if protocol_name != "MQTT" {
        return Err(CodecError::Malformed("unknown protocol name"));
    }
    let protocol_level = cur.read_u8()?;
    let connect_flags = cur.read_u8()?;
    let keep_alive = cur.read_u16()?;
    if connect_flags & 0x01 != 0 {
        return Err(CodecError::Malformed("connect flags reserved bit set"));
    }
    if protocol_level == 5 {
        cur.skip_properties()?;
        let client_id = cur.read_utf8()?;
        return Ok(Some(MqttPacket::Connect(Box::new(ConnectPacket {
            protocol_level,
            clean_session: connect_flags & 0x02 != 0,
            keep_alive,
            client_id,
            username: None,
            password: None,
            will: None,
        }))));
    }
    if protocol_level != LEVEL_3_1_1 {
        return Err(CodecError::Malformed("unsupported protocol level"));
    }
    let username_flag = connect_flags & 0x80 != 0;
    let password_flag = connect_flags & 0x40 != 0;
    let will_retain = connect_flags & 0x20 != 0;
    let will_qos = (connect_flags >> 3) & 0x03;
    let will_flag = connect_flags & 0x04 != 0;
    if password_flag && !username_flag {
        return Err(CodecError::Malformed("password flag without username flag"));
    }
    if !will_flag && (will_qos != 0 || will_retain) {
        return Err(CodecError::Malformed("will flags set without will"));
    }
    if will_qos == 3 {
        return Err(CodecError::Malformed("invalid will qos"));
    }
    let client_id = cur.read_utf8()?;
    let will = if will_flag {
        let topic = cur.read_utf8()?;
        let payload = cur.read_bin()?;
        Some(LastWill {
            topic,
            payload,
            qos: will_qos,
            retain: will_retain,
        })
    } else {
        None
    };
    let username = if username_flag {
        Some(cur.read_utf8()?)
    } else {
        None
    };
    let password = if password_flag {
        Some(cur.read_bin()?)
    } else {
        None
    };
    Ok(Some(MqttPacket::Connect(Box::new(ConnectPacket {
        protocol_level,
        clean_session: connect_flags & 0x02 != 0,
        keep_alive,
        client_id,
        username,
        password,
        will,
    }))))
}

fn decode_publish(flags: u8, body: &[u8]) -> Result<Option<MqttPacket>, CodecError> {
    let qos = (flags >> 1) & 0x03;
    if qos == 3 {
        return Err(CodecError::Malformed("invalid publish qos"));
    }
    let mut cur = Cursor::new(body);
    let topic = cur.read_utf8()?;
    if topic.is_empty() || topic.contains('+') || topic.contains('#') {
        return Err(CodecError::Malformed("invalid publish topic"));
    }
    let packet_id = if qos > 0 { Some(cur.read_u16()?) } else { None };
    let payload = Bytes::copy_from_slice(cur.read_bytes(cur.remaining())?);
    Ok(Some(MqttPacket::Publish(PublishPacket {
        dup: flags & 0x08 != 0,
        qos,
        retain: flags & 0x01 != 0,
        topic,
        packet_id,
        payload,
    })))
}

fn decode_subscribe(flags: u8, body: &[u8]) -> Result<Option<MqttPacket>, CodecError> {
    if flags != 0x02 {
        return Err(CodecError::Malformed("invalid subscribe flags"));
    }
    let mut cur = Cursor::new(body);
    let packet_id = cur.read_u16()?;
    let mut filters = Vec::new();
    while cur.remaining() > 0 {
        let filter = cur.read_utf8()?;
        let qos = cur.read_u8()?;
        if qos > 2 {
            return Err(CodecError::Malformed("invalid subscribe qos"));
        }
        filters.push((filter, qos));
    }
    if filters.is_empty() {
        return Err(CodecError::Malformed("subscribe without filters"));
    }
    Ok(Some(MqttPacket::Subscribe { packet_id, filters }))
}

fn decode_unsubscribe(flags: u8, body: &[u8]) -> Result<Option<MqttPacket>, CodecError> {
    if flags != 0x02 {
        return Err(CodecError::Malformed("invalid unsubscribe flags"));
    }
    let mut cur = Cursor::new(body);
    let packet_id = cur.read_u16()?;
    let mut filters = Vec::new();
    while cur.remaining() > 0 {
        filters.push(cur.read_utf8()?);
    }
    if filters.is_empty() {
        return Err(CodecError::Malformed("unsubscribe without filters"));
    }
    Ok(Some(MqttPacket::Unsubscribe { packet_id, filters }))
}

fn put_utf8(dst: &mut BytesMut, s: &str) -> Result<(), CodecError> {
    let len = u16::try_from(s.len()).map_err(|_| CodecError::StringTooLong)?;
    dst.put_u16(len);
    dst.put_slice(s.as_bytes());
    Ok(())
}

fn put_bin(dst: &mut BytesMut, b: &[u8]) -> Result<(), CodecError> {
    let len = u16::try_from(b.len()).map_err(|_| CodecError::StringTooLong)?;
    dst.put_u16(len);
    dst.put_slice(b);
    Ok(())
}

fn put_varint(dst: &mut BytesMut, mut value: usize) {
    loop {
        let mut b = (value & 0x7f) as u8;
        value >>= 7;
        if value > 0 {
            b |= 0x80;
        }
        dst.put_u8(b);
        if value == 0 {
            break;
        }
    }
}

fn frame(dst: &mut BytesMut, byte1: u8, body_len: usize) {
    dst.put_u8(byte1);
    put_varint(dst, body_len);
}

impl Encoder<MqttPacket> for MqttCodec {
    type Error = CodecError;

    fn encode(&mut self, packet: MqttPacket, dst: &mut BytesMut) -> Result<(), CodecError> {
        match packet {
            MqttPacket::Connect(c) => {
                let mut body = BytesMut::new();
                put_utf8(&mut body, "MQTT")?;
                body.put_u8(c.protocol_level);
                let mut flags = 0u8;
                if c.username.is_some() {
                    flags |= 0x80;
                }
                if c.password.is_some() {
                    flags |= 0x40;
                }
                if let Some(w) = &c.will {
                    flags |= 0x04 | (w.qos << 3);
                    if w.retain {
                        flags |= 0x20;
                    }
                }
                if c.clean_session {
                    flags |= 0x02;
                }
                body.put_u8(flags);
                body.put_u16(c.keep_alive);
                put_utf8(&mut body, &c.client_id)?;
                if let Some(w) = &c.will {
                    put_utf8(&mut body, &w.topic)?;
                    put_bin(&mut body, &w.payload)?;
                }
                if let Some(u) = &c.username {
                    put_utf8(&mut body, u)?;
                }
                if let Some(p) = &c.password {
                    put_bin(&mut body, p)?;
                }
                frame(dst, 0x10, body.len());
                dst.extend_from_slice(&body);
            }
            MqttPacket::ConnAck {
                session_present,
                return_code,
            } => {
                frame(dst, 0x20, 2);
                dst.put_u8(session_present as u8);
                dst.put_u8(return_code);
            }
            MqttPacket::Publish(p) => {
                if p.qos > 2 || (p.qos == 0 && p.packet_id.is_some()) {
                    return Err(CodecError::Malformed("invalid publish encode"));
                }
                let mut body = BytesMut::new();
                put_utf8(&mut body, &p.topic)?;
                if let Some(id) = p.packet_id {
                    body.put_u16(id);
                }
                body.extend_from_slice(&p.payload);
                let byte1 = 0x30 | ((p.dup as u8) << 3) | (p.qos << 1) | (p.retain as u8);
                frame(dst, byte1, body.len());
                dst.extend_from_slice(&body);
            }
            MqttPacket::PubAck(id) => {
                frame(dst, 0x40, 2);
                dst.put_u16(id);
            }
            MqttPacket::Subscribe { packet_id, filters } => {
                if filters.is_empty() {
                    return Err(CodecError::Malformed("subscribe without filters"));
                }
                let mut body = BytesMut::new();
                body.put_u16(packet_id);
                for (filter, qos) in &filters {
                    put_utf8(&mut body, filter)?;
                    body.put_u8(*qos);
                }
                frame(dst, 0x82, body.len());
                dst.extend_from_slice(&body);
            }
            MqttPacket::SubAck { packet_id, granted } => {
                frame(dst, 0x90, 2 + granted.len());
                dst.put_u16(packet_id);
                dst.extend_from_slice(&granted);
            }
            MqttPacket::Unsubscribe { packet_id, filters } => {
                if filters.is_empty() {
                    return Err(CodecError::Malformed("unsubscribe without filters"));
                }
                let mut body = BytesMut::new();
                body.put_u16(packet_id);
                for filter in &filters {
                    put_utf8(&mut body, filter)?;
                }
                frame(dst, 0xA2, body.len());
                dst.extend_from_slice(&body);
            }
            MqttPacket::UnsubAck(id) => {
                frame(dst, 0xB0, 2);
                dst.put_u16(id);
            }
            MqttPacket::PingReq => {
                frame(dst, 0xC0, 0);
            }
            MqttPacket::PingResp => {
                frame(dst, 0xD0, 0);
            }
            MqttPacket::Disconnect => {
                frame(dst, 0xE0, 0);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(packet: MqttPacket) {
        let mut codec = MqttCodec;
        let mut buf = BytesMut::new();
        codec.encode(packet.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().expect("complete packet");
        assert_eq!(decoded, packet);
        assert!(buf.is_empty());
    }

    fn roundtrip_fragmented(packet: MqttPacket) {
        let mut codec = MqttCodec;
        let mut wire = BytesMut::new();
        codec.encode(packet.clone(), &mut wire).unwrap();
        let mut buf = BytesMut::new();
        for i in 0..wire.len() {
            buf.extend_from_slice(&wire[i..i + 1]);
            let decoded = codec.decode(&mut buf).unwrap();
            if i + 1 < wire.len() {
                assert!(decoded.is_none(), "decoded early at byte {i}");
            } else {
                assert_eq!(decoded, Some(packet.clone()));
            }
        }
    }

    fn sample_packets() -> Vec<MqttPacket> {
        vec![
            MqttPacket::Connect(Box::new(ConnectPacket {
                protocol_level: LEVEL_3_1_1,
                clean_session: true,
                keep_alive: 60,
                client_id: "device-1".into(),
                username: Some("TOKEN".into()),
                password: Some(b"secret".to_vec()),
                will: Some(LastWill {
                    topic: "v1/devices/me/telemetry".into(),
                    payload: b"offline".to_vec(),
                    qos: 1,
                    retain: true,
                }),
            })),
            MqttPacket::Connect(Box::new(ConnectPacket {
                protocol_level: LEVEL_3_1_1,
                clean_session: false,
                keep_alive: 0,
                client_id: "c".into(),
                username: None,
                password: None,
                will: None,
            })),
            MqttPacket::ConnAck {
                session_present: true,
                return_code: connack_code::ACCEPTED,
            },
            MqttPacket::Publish(PublishPacket {
                dup: false,
                qos: 0,
                retain: false,
                topic: "v1/devices/me/telemetry".into(),
                packet_id: None,
                payload: Bytes::from_static(b"{\"t\":1}"),
            }),
            MqttPacket::Publish(PublishPacket {
                dup: true,
                qos: 1,
                retain: true,
                topic: "a/b".into(),
                packet_id: Some(0xABCD),
                payload: Bytes::from_static(b"payload"),
            }),
            MqttPacket::PubAck(42),
            MqttPacket::Subscribe {
                packet_id: 7,
                filters: vec![
                    ("v1/devices/me/attributes".into(), 1),
                    ("v1/devices/me/rpc/request/+".into(), 0),
                ],
            },
            MqttPacket::SubAck {
                packet_id: 7,
                granted: vec![1, 0],
            },
            MqttPacket::Unsubscribe {
                packet_id: 9,
                filters: vec!["v1/devices/me/attributes".into()],
            },
            MqttPacket::UnsubAck(9),
            MqttPacket::PingReq,
            MqttPacket::PingResp,
            MqttPacket::Disconnect,
        ]
    }

    #[test]
    fn roundtrip_all_packet_types() {
        for p in sample_packets() {
            roundtrip(p);
        }
    }

    #[test]
    fn fragmented_feed_returns_none_until_complete() {
        for p in sample_packets() {
            roundtrip_fragmented(p);
        }
    }

    #[test]
    fn multi_byte_remaining_length() {
        let payload = Bytes::from(vec![0xAB; 70_000]);
        roundtrip(MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: 0,
            retain: false,
            topic: "t".into(),
            packet_id: None,
            payload,
        }));
    }

    #[test]
    fn rejects_oversized_remaining_length() {
        let mut codec = MqttCodec;
        // remaining length = 64MB + 1 → 超出上限
        let mut buf = BytesMut::from(&[0x30u8, 0x81, 0x80, 0x80, 0x40][..]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, CodecError::PacketTooLarge(_)));
    }

    #[test]
    fn rejects_malformed_varint() {
        let mut codec = MqttCodec;
        let mut buf = BytesMut::from(&[0x30u8, 0x80, 0x80, 0x80, 0x80][..]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, CodecError::Malformed(_)));
    }

    #[test]
    fn rejects_publish_with_wildcard_topic() {
        let mut codec = MqttCodec;
        let mut buf = BytesMut::new();
        put_utf8(&mut buf, "a/+/b").unwrap();
        buf.extend_from_slice(b"x");
        let mut framed = BytesMut::new();
        frame(&mut framed, 0x30, buf.len());
        framed.extend_from_slice(&buf);
        let err = codec.decode(&mut framed).unwrap_err();
        assert!(matches!(err, CodecError::Malformed(_)));
    }

    #[test]
    fn rejects_unsupported_type() {
        let mut codec = MqttCodec;
        // PUBREL (type 6) 不支持
        let mut buf = BytesMut::from(&[0x62u8, 0x02, 0x00, 0x01][..]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert!(matches!(err, CodecError::UnsupportedType(6)));
    }

    #[test]
    fn decodes_mqtt5_connect_minimally() {
        let mut body = BytesMut::new();
        put_utf8(&mut body, "MQTT").unwrap();
        body.put_u8(5);
        body.put_u8(0x02);
        body.put_u16(30);
        body.put_u8(0); // properties length
        put_utf8(&mut body, "v5-client").unwrap();
        let mut buf = BytesMut::new();
        frame(&mut buf, 0x10, body.len());
        buf.extend_from_slice(&body);
        let mut codec = MqttCodec;
        let packet = codec.decode(&mut buf).unwrap().unwrap();
        match packet {
            MqttPacket::Connect(c) => {
                assert_eq!(c.protocol_level, 5);
                assert_eq!(c.client_id, "v5-client");
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
