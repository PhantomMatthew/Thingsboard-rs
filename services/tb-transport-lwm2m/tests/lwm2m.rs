#![allow(dead_code)]

#[path = "../src/codec.rs"]
mod codec;
#[path = "../src/integration.rs"]
mod integration;
#[path = "../src/model.rs"]
mod model;
#[path = "../src/profile.rs"]
mod profile;
#[path = "../src/server.rs"]
mod server;
#[path = "../src/types.rs"]
mod types;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coap_lite::{
    CoapOption, ContentFormat, MessageClass, MessageType, Packet, RequestType, ResponseType,
};
use futures::future::BoxFuture;
use integration::{EndpointIdentity, EndpointValidator, SessionActivity, UplinkSink};
use profile::ValueKind;
use server::{Lwm2mServer, ServerConfig};
use tb_common::{KvValue, TbResult};
use tb_proto::{core as core_pb, transport as pb};
use tokio::net::UdpSocket;

struct MockValidator {
    identity: EndpointIdentity,
}

impl EndpointValidator for MockValidator {
    fn validate<'a>(
        &'a self,
        endpoint: &'a str,
        _peer: SocketAddr,
    ) -> BoxFuture<'a, TbResult<Option<EndpointIdentity>>> {
        Box::pin(
            async move { Ok((endpoint == "integration-device").then(|| self.identity.clone())) },
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Published {
    key: String,
    kind: ValueKind,
    value: KvValue,
}

#[derive(Default)]
struct MockUplink {
    values: Mutex<Vec<Published>>,
}

impl UplinkSink for MockUplink {
    fn publish<'a>(
        &'a self,
        _device: &'a pb::DeviceInfoProto,
        resource: &'a profile::ObservedResource,
        value: KvValue,
        _timestamp_ms: i64,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            self.values.lock().unwrap().push(Published {
                key: resource.key_name.clone(),
                kind: resource.kind,
                value,
            });
            Ok(())
        })
    }
}

#[derive(Default)]
struct MockActivity {
    events: Mutex<Vec<&'static str>>,
    close_gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

impl SessionActivity for MockActivity {
    fn open<'a>(
        &'a self,
        _session: &'a core_pb::SessionInfoProto,
        _device: &'a pb::DeviceInfoProto,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            self.events.lock().unwrap().push("open");
            Ok(())
        })
    }

    fn activity<'a>(
        &'a self,
        _session: &'a core_pb::SessionInfoProto,
        _device: &'a pb::DeviceInfoProto,
        _timestamp_ms: i64,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            self.events.lock().unwrap().push("activity");
            Ok(())
        })
    }

    fn close<'a>(
        &'a self,
        _session: &'a core_pb::SessionInfoProto,
        _device: &'a pb::DeviceInfoProto,
        _reason: &'a str,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            self.events.lock().unwrap().push("close");
            if let Some((entered, release)) = &self.close_gate {
                entered.notify_waiters();
                release.notified().await;
            }
            Ok(())
        })
    }
}

struct GatedValidator {
    identity: EndpointIdentity,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    validations: AtomicUsize,
}

impl EndpointValidator for GatedValidator {
    fn validate<'a>(
        &'a self,
        endpoint: &'a str,
        _peer: SocketAddr,
    ) -> BoxFuture<'a, TbResult<Option<EndpointIdentity>>> {
        Box::pin(async move {
            self.validations.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_waiters();
            self.release.notified().await;
            Ok((endpoint == "integration-device").then(|| self.identity.clone()))
        })
    }
}

fn test_identity(profile_json: &str) -> EndpointIdentity {
    EndpointIdentity {
        device: pb::DeviceInfoProto {
            device_id: "device-id".into(),
            tenant_id: "tenant-id".into(),
            device_profile_id: "profile-id".into(),
            device_name: "integration-device".into(),
            device_type: "sensor".into(),
            ..Default::default()
        },
        profile: pb::DeviceProfileProto {
            profile_data_json: profile_json.into(),
            ..Default::default()
        },
        resource_types: HashMap::from([
            ("/3303/0/5700".into(), types::ResourceType::Integer),
            ("/3/0/1".into(), types::ResourceType::Integer),
        ]),
    }
}

struct TestServer {
    server: Lwm2mServer,
    addr: SocketAddr,
    client: UdpSocket,
    uplink: Arc<MockUplink>,
    activity: Arc<MockActivity>,
    task: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start(profile_json: &str, config: ServerConfig) -> Self {
        let identity = test_identity(profile_json);
        Self::start_with_validator(config, Arc::new(MockValidator { identity })).await
    }

    async fn start_with_validator(
        config: ServerConfig,
        validator: Arc<dyn EndpointValidator>,
    ) -> Self {
        Self::start_with_activity(config, validator, Arc::new(MockActivity::default())).await
    }

    async fn start_with_activity(
        config: ServerConfig,
        validator: Arc<dyn EndpointValidator>,
        activity: Arc<MockActivity>,
    ) -> Self {
        let uplink = Arc::new(MockUplink::default());
        let server = Lwm2mServer::new(config, validator, uplink.clone(), activity.clone()).unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let running = server.clone();
        let task = tokio::spawn(async move {
            running.serve(socket).await.unwrap();
        });
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        Self {
            server,
            addr,
            client,
            uplink,
            activity,
            task,
        }
    }

    async fn exchange(&self, packet: &Packet) -> Packet {
        self.send(packet).await;
        self.recv().await
    }

    async fn send(&self, packet: &Packet) {
        send_packet(&self.client, self.addr, packet).await;
    }

    async fn recv(&self) -> Packet {
        recv_packet(&self.client).await
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn send_packet(socket: &UdpSocket, addr: SocketAddr, packet: &Packet) {
    socket
        .send_to(&packet.to_bytes().unwrap(), addr)
        .await
        .unwrap();
}

async fn recv_bytes(socket: &UdpSocket) -> Vec<u8> {
    let mut buffer = [0; 2048];
    let (size, peer) = tokio::time::timeout(Duration::from_secs(3), socket.recv_from(&mut buffer))
        .await
        .expect("UDP response timeout")
        .unwrap();
    assert_eq!(peer.ip(), socket.local_addr().unwrap().ip());
    buffer[..size].to_vec()
}

async fn recv_packet(socket: &UdpSocket) -> Packet {
    Packet::from_bytes(&recv_bytes(socket).await).unwrap()
}

fn request(method: RequestType, mid: u16, path: &str, query: &[&str]) -> Packet {
    let mut packet = Packet::new();
    packet.header.set_type(MessageType::Confirmable);
    packet.header.code = MessageClass::Request(method);
    packet.header.message_id = mid;
    packet.set_token(vec![mid as u8, 0x5a]);
    for segment in path.trim_matches('/').split('/') {
        packet.add_option(CoapOption::UriPath, segment.as_bytes().to_vec());
    }
    for item in query {
        packet.add_option(CoapOption::UriQuery, item.as_bytes().to_vec());
    }
    packet
}

fn path(packet: &Packet) -> String {
    packet
        .get_option(CoapOption::UriPath)
        .unwrap()
        .iter()
        .map(|part| String::from_utf8(part.clone()).unwrap())
        .collect::<Vec<_>>()
        .join("/")
}

fn location(packet: &Packet) -> String {
    packet
        .get_option(CoapOption::LocationPath)
        .unwrap()
        .iter()
        .map(|part| String::from_utf8(part.clone()).unwrap())
        .collect::<Vec<_>>()
        .join("/")
}

fn integer_payload(resource: u16, value: i8) -> Vec<u8> {
    codec::encode(&[codec::Tlv::Resource {
        id: resource,
        value: &value.to_be_bytes(),
    }])
}

fn respond(request: &Packet, mid: u16, separate: bool, observe: Option<u32>) -> Packet {
    let mut response = Packet::new();
    response.header.set_type(if separate {
        MessageType::Confirmable
    } else {
        MessageType::Acknowledgement
    });
    response.header.code = MessageClass::Response(ResponseType::Content);
    response.header.message_id = if separate {
        mid
    } else {
        request.header.message_id
    };
    response.set_token(request.get_token().to_vec());
    response.set_content_format(ContentFormat::ApplicationVndOmaLwm2mTlv);
    if let Some(observe) = observe {
        response.set_observe_value(observe);
    }
    let resource = path(request)
        .split('/')
        .nth(2)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    response.payload = integer_payload(resource, 7);
    response
}

async fn wait_for_values(uplink: &MockUplink, count: usize) -> Vec<Published> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let values = uplink.values.lock().unwrap().clone();
            if values.len() >= count {
                return values;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn udp_registration_read_observe_notification_update_and_delete() {
    let profile = r#"{"observeAttr":{"keyName":{"/3303/0/5700":"temperature","/3/0/1":"model"},"telemetry":["/3303/0/5700"],"attribute":["/3/0/1"],"observeStrategy":"SINGLE"}}"#;
    let test = TestServer::start(profile, ServerConfig::default()).await;
    let mut registration = request(
        RequestType::Post,
        10,
        "rd",
        &["ep=integration-device", "lt=30", "b=U"],
    );
    registration.payload = b"</1/0>,</3303/0/5700>,</3/0/1>".to_vec();
    let created = test.exchange(&registration).await;
    assert_eq!(
        created.header.code,
        MessageClass::Response(ResponseType::Created)
    );
    assert_eq!(created.header.get_type(), MessageType::Acknowledgement);
    assert_eq!(created.header.message_id, 10);
    let location = location(&created);

    let mut reads = HashMap::new();
    for _ in 0..2 {
        let read = test.recv().await;
        assert_eq!(read.header.get_type(), MessageType::Confirmable);
        assert!(read.get_observe_value().is_none());
        reads.insert(path(&read), read);
    }
    assert!(reads.contains_key("3303/0/5700"));
    assert!(reads.contains_key("3/0/1"));
    let read_tokens: Vec<_> = reads
        .values()
        .map(|packet| packet.get_token().to_vec())
        .collect();
    assert_ne!(read_tokens[0], read_tokens[1]);
    for read in reads.values() {
        test.send(&respond(read, 0, false, None)).await;
    }

    let mut observations = HashMap::new();
    for _ in 0..2 {
        let observe = test.recv().await;
        assert_eq!(observe.get_observe_value().unwrap().unwrap(), 0);
        assert!(!read_tokens.contains(&observe.get_token().to_vec()));
        observations.insert(path(&observe), observe);
    }
    let observation_tokens: Vec<_> = observations
        .values()
        .map(|packet| packet.get_token().to_vec())
        .collect();
    assert_ne!(observation_tokens[0], observation_tokens[1]);
    for (index, observe) in observations.values().enumerate() {
        let mid = 100 + index as u16;
        let initial = respond(observe, mid, true, Some(0));
        test.send(&initial).await;
        let ack = test.recv().await;
        assert_eq!(ack.header.get_type(), MessageType::Acknowledgement);
        assert_eq!(ack.header.code, MessageClass::Empty);
        assert_eq!(ack.header.message_id, mid);
        test.send(&initial).await;
        assert_eq!(test.recv().await.header.message_id, mid);
    }
    let values = wait_for_values(&test.uplink, 4).await;
    assert!(
        values
            .iter()
            .any(|value| value.kind == ValueKind::Telemetry)
    );
    assert!(
        values
            .iter()
            .any(|value| value.kind == ValueKind::Attribute)
    );

    let observe = &observations["3303/0/5700"];
    let mut notification = respond(observe, 500, true, Some(1));
    notification.payload = integer_payload(5700, 9);
    test.send(&notification).await;
    let ack = test.recv().await;
    assert_eq!(ack.header.message_id, 500);
    test.send(&notification).await;
    let duplicate_ack = test.recv().await;
    assert_eq!(duplicate_ack.header.message_id, 500);
    let values = wait_for_values(&test.uplink, 5).await;
    assert_eq!(values.last().unwrap().value, KvValue::Long(9));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(test.uplink.values.lock().unwrap().len(), 5);

    let update = request(RequestType::Put, 20, &location, &["lt=60"]);
    let changed = test.exchange(&update).await;
    assert_eq!(
        changed.header.code,
        MessageClass::Response(ResponseType::Changed)
    );
    let delete = request(RequestType::Delete, 21, &location, &[]);
    let deleted = test.exchange(&delete).await;
    assert_eq!(
        deleted.header.code,
        MessageClass::Response(ResponseType::Deleted)
    );
    assert_eq!(test.server.registration_count(), 0);
    assert!(test.activity.events.lock().unwrap().contains(&"close"));
}

#[tokio::test]
async fn udp_con_read_retransmits_same_mid_and_token() {
    let profile = r#"{"observeAttr":{"keyName":{"/3303/0/5700":"temperature"},"telemetry":["/3303/0/5700"],"attribute":[],"observeStrategy":"SINGLE"}}"#;
    let config = ServerConfig {
        ack_timeout: Duration::from_millis(40),
        max_ack_timeout: Duration::from_millis(80),
        max_retransmissions: 1,
        ..ServerConfig::default()
    };
    let test = TestServer::start(profile, config).await;
    let registration = request(RequestType::Post, 10, "rd", &["ep=integration-device"]);
    test.exchange(&registration).await;
    let first = test.recv().await;
    let second = test.recv().await;
    assert_eq!(first.header.message_id, second.header.message_id);
    assert_eq!(first.get_token(), second.get_token());
    test.send(&respond(&second, 0, false, None)).await;
    let observe = test.recv().await;
    test.send(&respond(&observe, 0, false, Some(0))).await;
}

#[tokio::test]
async fn udp_registration_lifetime_expires_and_closes_session() {
    let profile = r#"{"observeAttr":{"telemetry":[],"attribute":[]}}"#;
    let config = ServerConfig {
        cleanup_interval: Duration::from_millis(20),
        ..ServerConfig::default()
    };
    let test = TestServer::start(profile, config).await;
    let registration = request(
        RequestType::Post,
        10,
        "rd",
        &["ep=integration-device", "lt=1"],
    );
    let created = test.exchange(&registration).await;
    assert_eq!(
        created.header.code,
        MessageClass::Response(ResponseType::Created)
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while test.server.registration_count() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(test.activity.events.lock().unwrap().contains(&"close"));
}

#[tokio::test]
async fn udp_registration_rejects_unknown_endpoint() {
    let profile = r#"{"observeAttr":{"telemetry":[],"attribute":[]}}"#;
    let test = TestServer::start(profile, ServerConfig::default()).await;
    let registration = request(RequestType::Post, 10, "rd", &["ep=unknown"]);
    let response = test.exchange(&registration).await;
    assert_eq!(
        response.header.code,
        MessageClass::Response(ResponseType::Unauthorized)
    );
    assert_eq!(test.server.registration_count(), 0);
}

fn empty_ack(request: &Packet) -> Packet {
    let mut ack = Packet::new();
    ack.header.set_type(MessageType::Acknowledgement);
    ack.header.code = MessageClass::Empty;
    ack.header.message_id = request.header.message_id;
    ack
}

#[tokio::test]
async fn udp_observe_ignores_older_sequence_without_overwriting_new_value() {
    let profile = r#"{"observeAttr":{"keyName":{"/3303/0/5700":"temperature"},"telemetry":["/3303/0/5700"],"attribute":[],"observeStrategy":"SINGLE"}}"#;
    let test = TestServer::start(profile, ServerConfig::default()).await;
    let registration = request(RequestType::Post, 10, "rd", &["ep=integration-device"]);
    test.exchange(&registration).await;

    let read = test.recv().await;
    test.send(&respond(&read, 0, false, None)).await;
    let observe = test.recv().await;
    assert_eq!(observe.get_observe_value().unwrap().unwrap(), 0);
    test.send(&respond(&observe, 0, false, Some(0))).await;
    wait_for_values(&test.uplink, 2).await;

    let mut newest = respond(&observe, 500, true, Some(2));
    newest.payload = integer_payload(5700, 20);
    test.send(&newest).await;
    let ack = test.recv().await;
    assert_eq!(ack.header.message_id, 500);
    let values = wait_for_values(&test.uplink, 3).await;
    assert_eq!(values.last().unwrap().value, KvValue::Long(20));

    let mut stale = respond(&observe, 501, true, Some(1));
    stale.payload = integer_payload(5700, 10);
    test.send(&stale).await;
    let ack = test.recv().await;
    assert_eq!(ack.header.message_id, 501);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let values = test.uplink.values.lock().unwrap().clone();
    assert_eq!(values.len(), 3, "stale notification must not be published");
    assert_eq!(values.last().unwrap().value, KvValue::Long(20));
    assert!(!values.iter().any(|value| value.value == KvValue::Long(10)));
}

#[tokio::test]
async fn udp_read_empty_ack_stops_retransmission_then_accepts_separate_content() {
    let profile = r#"{"observeAttr":{"keyName":{"/3303/0/5700":"temperature"},"telemetry":["/3303/0/5700"],"attribute":[],"observeStrategy":"SINGLE"}}"#;
    let config = ServerConfig {
        ack_timeout: Duration::from_millis(40),
        max_ack_timeout: Duration::from_millis(40),
        max_retransmissions: 1,
        ..ServerConfig::default()
    };
    let test = TestServer::start(profile, config).await;
    let registration = request(RequestType::Post, 10, "rd", &["ep=integration-device"]);
    test.exchange(&registration).await;

    let read = test.recv().await;
    assert_eq!(read.header.get_type(), MessageType::Confirmable);
    test.send(&empty_ack(&read)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut buffer = [0; 2048];
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            test.client.recv_from(&mut buffer)
        )
        .await
        .is_err(),
        "an acknowledged Read must not be retransmitted"
    );

    let content = respond(&read, 100, true, None);
    test.send(&content).await;
    let ack = test.recv().await;
    assert_eq!(ack.header.get_type(), MessageType::Acknowledgement);
    assert_eq!(ack.header.code, MessageClass::Empty);
    assert_eq!(ack.header.message_id, 100);

    let observe = test.recv().await;
    assert_eq!(observe.get_observe_value().unwrap().unwrap(), 0);
    test.send(&respond(&observe, 0, false, Some(0))).await;
    let values = wait_for_values(&test.uplink, 2).await;
    assert_eq!(values.len(), 2);
}

#[tokio::test]
async fn udp_update_without_lifetime_preserves_short_registration_expiry() {
    let profile = r#"{"observeAttr":{"telemetry":[],"attribute":[]}}"#;
    let config = ServerConfig {
        cleanup_interval: Duration::from_millis(20),
        ..ServerConfig::default()
    };
    let test = TestServer::start(profile, config).await;
    let registration = request(
        RequestType::Post,
        10,
        "rd",
        &["ep=integration-device", "lt=1"],
    );
    let created = test.exchange(&registration).await;
    let location = location(&created);

    let update = request(RequestType::Put, 20, &location, &[]);
    let changed = test.exchange(&update).await;
    assert_eq!(
        changed.header.code,
        MessageClass::Response(ResponseType::Changed)
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while test.server.registration_count() != 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("an Update without lt must not extend a one-second registration");
    assert!(test.activity.events.lock().unwrap().contains(&"close"));
}

#[tokio::test]
async fn udp_concurrent_duplicate_registration_shares_one_inflight_result() {
    let profile = r#"{"observeAttr":{"telemetry":[],"attribute":[]}}"#;
    let validator = Arc::new(GatedValidator {
        identity: test_identity(profile),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        validations: AtomicUsize::new(0),
    });
    let test = TestServer::start_with_validator(ServerConfig::default(), validator.clone()).await;
    let registration = request(RequestType::Post, 10, "rd", &["ep=integration-device"]);

    let entered = validator.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    test.send(&registration).await;
    tokio::time::timeout(Duration::from_secs(1), entered)
        .await
        .unwrap();
    test.send(&registration).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        validator.validations.load(Ordering::SeqCst),
        1,
        "the duplicate must wait for the in-flight validation"
    );

    validator.release.notify_waiters();
    let first_bytes = recv_bytes(&test.client).await;
    let second_bytes = recv_bytes(&test.client).await;
    assert_eq!(first_bytes, second_bytes);
    let first = Packet::from_bytes(&first_bytes).unwrap();
    let second = Packet::from_bytes(&second_bytes).unwrap();
    assert_eq!(
        first.header.code,
        MessageClass::Response(ResponseType::Created)
    );
    assert_eq!(location(&first), location(&second));
    assert_eq!(test.server.registration_count(), 1);
    assert_eq!(
        test.activity
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| **event == "open")
            .count(),
        1
    );
}

#[tokio::test]
async fn udp_update_from_new_address_moves_read_and_observe_to_new_peer() {
    let profile = r#"{"observeAttr":{"keyName":{"/3303/0/5700":"temperature"},"telemetry":["/3303/0/5700"],"attribute":[],"observeStrategy":"SINGLE"}}"#;
    let config = ServerConfig {
        ack_timeout: Duration::from_millis(40),
        max_ack_timeout: Duration::from_millis(40),
        max_retransmissions: 4,
        ..ServerConfig::default()
    };
    let test = TestServer::start(profile, config).await;
    let registration = request(
        RequestType::Post,
        10,
        "rd",
        &["ep=integration-device", "lt=30"],
    );
    let created = test.exchange(&registration).await;
    let location = location(&created);
    let original_read = test.recv().await;

    let new_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    assert_ne!(
        new_client.local_addr().unwrap(),
        test.client.local_addr().unwrap()
    );
    let update = request(RequestType::Put, 20, &location, &["lt=30"]);
    send_packet(&new_client, test.addr, &update).await;
    let changed = recv_packet(&new_client).await;
    assert_eq!(
        changed.header.code,
        MessageClass::Response(ResponseType::Changed)
    );

    let read = recv_packet(&new_client).await;
    assert_eq!(read.header.message_id, original_read.header.message_id);
    assert_eq!(read.get_token(), original_read.get_token());
    send_packet(&new_client, test.addr, &respond(&read, 0, false, None)).await;
    let observe = recv_packet(&new_client).await;
    assert_eq!(observe.get_observe_value().unwrap().unwrap(), 0);
    send_packet(
        &new_client,
        test.addr,
        &respond(&observe, 0, false, Some(0)),
    )
    .await;
    wait_for_values(&test.uplink, 2).await;

    let mut notification = respond(&observe, 500, true, Some(1));
    notification.payload = integer_payload(5700, 20);
    send_packet(&new_client, test.addr, &notification).await;
    let ack = recv_packet(&new_client).await;
    assert_eq!(ack.header.message_id, 500);
    let values = wait_for_values(&test.uplink, 3).await;
    assert_eq!(values.last().unwrap().value, KvValue::Long(20));
}

#[tokio::test]
async fn udp_concurrent_reregistration_leaves_one_active_session() {
    let profile = r#"{"observeAttr":{"telemetry":[],"attribute":[]}}"#;
    let close_entered = Arc::new(tokio::sync::Notify::new());
    let close_release = Arc::new(tokio::sync::Notify::new());
    let activity = Arc::new(MockActivity {
        close_gate: Some((close_entered.clone(), close_release.clone())),
        ..MockActivity::default()
    });
    let test = TestServer::start_with_activity(
        ServerConfig::default(),
        Arc::new(MockValidator {
            identity: test_identity(profile),
        }),
        activity,
    )
    .await;
    let initial = request(RequestType::Post, 10, "rd", &["ep=integration-device"]);
    test.exchange(&initial).await;

    let first_close = close_entered.notified();
    tokio::pin!(first_close);
    first_close.as_mut().enable();
    test.send(&request(
        RequestType::Post,
        11,
        "rd",
        &["ep=integration-device"],
    ))
    .await;
    tokio::time::timeout(Duration::from_secs(1), first_close)
        .await
        .unwrap();
    test.send(&request(
        RequestType::Post,
        12,
        "rd",
        &["ep=integration-device"],
    ))
    .await;
    let second_close = close_entered.notified();
    tokio::pin!(second_close);
    second_close.as_mut().enable();
    close_release.notify_one();
    let first = test.recv().await;
    assert_eq!(
        first.header.code,
        MessageClass::Response(ResponseType::Created)
    );

    tokio::time::timeout(Duration::from_secs(1), second_close)
        .await
        .expect("the second registration must deactivate the first replacement");
    close_release.notify_one();
    let second = test.recv().await;
    assert_eq!(
        second.header.code,
        MessageClass::Response(ResponseType::Created)
    );
    assert_ne!(location(&first), location(&second));
    assert_eq!(test.server.registration_count(), 1);
    let events = test.activity.events.lock().unwrap();
    assert_eq!(events.iter().filter(|event| **event == "open").count(), 3);
    assert_eq!(events.iter().filter(|event| **event == "close").count(), 2);
}
