use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value as JsonValue, json};
use snmp2::{MessageType, Pdu, Varbinds};
use tb_common::{KvEntry, KvValue, TbResult, TsKvEntry};
use tb_proto::transport::DeviceInfoProto;
use tb_transport_snmp::{
    DeviceTransportConfiguration, ProfileTransportConfiguration, SnmpRunner, UplinkPublisher,
};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

const LONG_OID: &str = "1.3.6.1.2.1.1.3.0";
const STRING_OID: &str = "1.3.6.1.2.1.1.1.0";
const ATTRIBUTE_OID: &str = "1.3.6.1.4.1.9999.1.0";

#[derive(Debug)]
enum Published {
    Telemetry(String, Vec<TsKvEntry>),
    Attributes(String, Vec<KvEntry>),
}

#[derive(Clone, Default)]
struct MockPublisher {
    published: Arc<Mutex<Vec<Published>>>,
}

impl MockPublisher {
    fn take(&self) -> Vec<Published> {
        std::mem::take(&mut *self.published.lock().unwrap())
    }
}

#[async_trait]
impl UplinkPublisher for MockPublisher {
    async fn publish_telemetry(
        &self,
        device: &DeviceInfoProto,
        entries: Vec<TsKvEntry>,
    ) -> TbResult<()> {
        self.published
            .lock()
            .unwrap()
            .push(Published::Telemetry(device.device_id.clone(), entries));
        Ok(())
    }

    async fn publish_attributes(
        &self,
        device: &DeviceInfoProto,
        entries: Vec<KvEntry>,
    ) -> TbResult<()> {
        self.published
            .lock()
            .unwrap()
            .push(Published::Attributes(device.device_id.clone(), entries));
        Ok(())
    }
}

struct Agent {
    port: u16,
    requests: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl Agent {
    async fn start(
        community: &'static [u8],
        values: HashMap<String, Vec<u8>>,
        respond: bool,
    ) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = socket.local_addr().unwrap().port();
        let requests = Arc::new(AtomicUsize::new(0));
        let task_requests = requests.clone();
        let task = tokio::spawn(async move {
            let mut buffer = vec![0_u8; 65_507];
            while let Ok((size, peer)) = socket.recv_from(&mut buffer).await {
                task_requests.fetch_add(1, Ordering::SeqCst);
                let mut request = Pdu::from_bytes(&buffer[..size]).unwrap();
                assert_eq!(request.message_type, MessageType::GetRequest);
                if !respond || request.community != community {
                    continue;
                }

                let mut response_varbinds = Vec::new();
                for (oid, _) in request.varbinds.clone() {
                    let name = oid.to_string();
                    let value = values.get(&name).cloned().unwrap_or_else(|| vec![0x80, 0]);
                    response_varbinds.extend(sequence(&[tlv(0x06, oid.as_bytes()), value]));
                }
                request.message_type = MessageType::Response;
                request.error_status = 0;
                request.error_index = 0;
                request.varbinds = Varbinds::from_bytes(&response_varbinds);
                let response = request.to_bytes().unwrap();
                socket.send_to(&response, peer).await.unwrap();
            }
        });
        Self {
            port,
            requests,
            task,
        }
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn device_config(port: u16, community: &str) -> DeviceTransportConfiguration {
    DeviceTransportConfiguration::from_device_data(&json!({"transportConfiguration": {
        "host": "127.0.0.1",
        "port": port,
        "protocolVersion": "V2C",
        "community": community,
    }}))
    .unwrap()
}

fn profile(configs: JsonValue) -> ProfileTransportConfiguration {
    ProfileTransportConfiguration::from_profile_data(&json!({"transportConfiguration": {
        "timeoutMs": 100,
        "retries": 0,
        "communicationConfigs": configs,
    }}))
    .unwrap()
}

fn mapping(oid: &str, key: &str, data_type: &str) -> JsonValue {
    json!({"oid": oid, "key": key, "dataType": data_type})
}

fn device(id: &str) -> DeviceInfoProto {
    DeviceInfoProto {
        device_id: id.into(),
        tenant_id: "tenant".into(),
        device_name: format!("device-{id}"),
        device_type: "snmp".into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn real_udp_get_publishes_exact_telemetry_and_attributes() {
    let agent = Agent::start(
        b"public",
        HashMap::from([
            (LONG_OID.into(), integer(42)),
            (STRING_OID.into(), octets(b"sensor-a")),
            (ATTRIBUTE_OID.into(), octets(br#"{"site":"lab"}"#)),
        ]),
        true,
    )
    .await;
    let profile = profile(json!([
        {
            "spec": "TELEMETRY_QUERYING",
            "queryingFrequencyMs": 1000,
            "mappings": [
                mapping(LONG_OID, "uptime", "LONG"),
                mapping(STRING_OID, "description", "STRING")
            ]
        },
        {
            "spec": "CLIENT_ATTRIBUTES_QUERYING",
            "queryingFrequencyMs": 2000,
            "mappings": [mapping(ATTRIBUTE_OID, "metadata", "JSON")]
        }
    ]));
    let publisher = MockPublisher::default();
    let runner = SnmpRunner::new(publisher.clone())
        .with_oid_chunk_size(1)
        .unwrap();

    let report = runner
        .poll_once(
            &device("device-a"),
            &device_config(agent.port, "public"),
            &profile,
        )
        .await
        .unwrap();

    assert_eq!(report.telemetry_values, 2);
    assert_eq!(report.attribute_values, 1);
    assert_eq!(agent.requests.load(Ordering::SeqCst), 3);
    let published = publisher.take();
    assert_eq!(published.len(), 2);
    match &published[0] {
        Published::Telemetry(id, entries) => {
            assert_eq!(id, "device-a");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].key, "uptime");
            assert_eq!(entries[0].value, KvValue::Long(42));
            assert_eq!(entries[1].key, "description");
            assert_eq!(entries[1].value, KvValue::Str("sensor-a".into()));
            assert!(entries.iter().all(|entry| entry.ts == entries[0].ts));
        }
        other => panic!("unexpected publish: {other:?}"),
    }
    match &published[1] {
        Published::Attributes(id, entries) => {
            assert_eq!(id, "device-a");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].key, "metadata");
            assert_eq!(entries[0].value, KvValue::Json(json!({"site": "lab"})));
        }
        other => panic!("unexpected publish: {other:?}"),
    }
}

#[tokio::test]
async fn wrong_community_does_not_publish() {
    let agent = Agent::start(b"public", HashMap::new(), true).await;
    let profile = profile(json!([{
        "spec": "TELEMETRY_QUERYING",
        "queryingFrequencyMs": 1000,
        "mappings": [mapping(LONG_OID, "uptime", "LONG")]
    }]));
    let publisher = MockPublisher::default();
    let runner = SnmpRunner::new(publisher.clone());

    assert!(
        runner
            .poll_once(
                &device("device-a"),
                &device_config(agent.port, "private"),
                &profile
            )
            .await
            .is_err()
    );
    assert!(publisher.take().is_empty());
}

#[tokio::test]
async fn timeout_does_not_publish() {
    let agent = Agent::start(b"public", HashMap::new(), false).await;
    let profile = profile(json!([{
        "spec": "TELEMETRY_QUERYING",
        "queryingFrequencyMs": 1000,
        "mappings": [mapping(LONG_OID, "uptime", "LONG")]
    }]));
    let publisher = MockPublisher::default();
    let runner = SnmpRunner::new(publisher.clone());

    assert!(
        runner
            .poll_once(
                &device("device-a"),
                &device_config(agent.port, "public"),
                &profile
            )
            .await
            .is_err()
    );
    assert!(publisher.take().is_empty());
}

#[tokio::test]
async fn unknown_oid_does_not_publish() {
    let agent = Agent::start(b"public", HashMap::new(), true).await;
    let profile = profile(json!([{
        "spec": "CLIENT_ATTRIBUTES_QUERYING",
        "queryingFrequencyMs": 1000,
        "mappings": [mapping(LONG_OID, "missing", "LONG")]
    }]));
    let publisher = MockPublisher::default();
    let runner = SnmpRunner::new(publisher.clone());

    let report = runner
        .poll_once(
            &device("device-a"),
            &device_config(agent.port, "public"),
            &profile,
        )
        .await
        .unwrap();
    assert_eq!(report.skipped_values, 1);
    assert_eq!(report.attribute_values, 0);
    assert!(publisher.take().is_empty());
}

fn integer(value: i64) -> Vec<u8> {
    tlv(0x02, &value.to_be_bytes())
}

fn octets(value: &[u8]) -> Vec<u8> {
    tlv(0x04, value)
}

fn sequence(parts: &[Vec<u8>]) -> Vec<u8> {
    tlv(0x30, &parts.concat())
}

fn tlv(tag: u8, value: &[u8]) -> Vec<u8> {
    assert!(value.len() < 128);
    let mut encoded = vec![tag, value.len() as u8];
    encoded.extend_from_slice(value);
    encoded
}
