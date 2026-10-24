use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use snmp2::{AsyncSession, Oid, Pdu};
use tb_common::{KvEntry, TbError, TbResult, TsKvEntry};
use tb_proto::transport::DeviceInfoProto;
use tb_queue::QueueProducer as _;
use tb_transport_api::TransportApiClient;
use thiserror::Error;
use tokio::sync::watch;

use crate::config::{
    CommunicationConfig, DeviceTransportConfiguration, ProfileTransportConfiguration, QuerySpec,
};
use crate::mapping;

pub const DEFAULT_OID_CHUNK_SIZE: usize = 100;

#[derive(Debug, Error)]
pub enum SnmpError {
    #[error("OID chunk size must be greater than zero")]
    InvalidChunkSize,
    #[error("SNMP session error: {0}")]
    Session(#[from] std::io::Error),
    #[error("SNMP GET failed after retries: {0}")]
    Get(#[from] snmp2::Error),
    #[error("SNMP GET timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("invalid SNMP response: {0}")]
    Response(String),
    #[error("cannot map SNMP value for {oid}: {message}")]
    Mapping { oid: String, message: String },
    #[error("uplink publish failed: {0}")]
    Publish(#[from] TbError),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PollReport {
    pub telemetry_values: usize,
    pub attribute_values: usize,
    pub skipped_values: usize,
}

impl PollReport {
    fn add(&mut self, other: Self) {
        self.telemetry_values += other.telemetry_values;
        self.attribute_values += other.attribute_values;
        self.skipped_values += other.skipped_values;
    }
}

#[async_trait]
pub trait UplinkPublisher: Send + Sync {
    async fn publish_telemetry(
        &self,
        device: &DeviceInfoProto,
        entries: Vec<TsKvEntry>,
    ) -> TbResult<()>;

    async fn publish_attributes(
        &self,
        device: &DeviceInfoProto,
        entries: Vec<KvEntry>,
    ) -> TbResult<()>;

    async fn publish_activity(&self, _device: &DeviceInfoProto) -> TbResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct TransportApiPublisher {
    client: TransportApiClient,
    producer: Option<(tb_queue::KafkaQueueProducer, String)>,
}

impl TransportApiPublisher {
    pub fn new(client: TransportApiClient) -> Self {
        Self {
            client,
            producer: None,
        }
    }

    pub fn with_activity(
        client: TransportApiClient,
        producer: tb_queue::KafkaQueueProducer,
        node_id: impl Into<String>,
    ) -> Self {
        Self {
            client,
            producer: Some((producer, node_id.into())),
        }
    }
}

#[async_trait]
impl UplinkPublisher for TransportApiPublisher {
    async fn publish_telemetry(
        &self,
        device: &DeviceInfoProto,
        entries: Vec<TsKvEntry>,
    ) -> TbResult<()> {
        self.client.publish_telemetry(device, entries).await
    }

    async fn publish_attributes(
        &self,
        device: &DeviceInfoProto,
        entries: Vec<KvEntry>,
    ) -> TbResult<()> {
        self.client.publish_attributes(device, entries).await
    }

    async fn publish_activity(&self, device: &DeviceInfoProto) -> TbResult<()> {
        let Some((producer, node_id)) = &self.producer else {
            return Ok(());
        };
        let session = tb_proto::core::SessionInfoProto {
            node_id: node_id.clone(),
            session_id: uuid::Uuid::now_v7().to_string(),
            tenant_id: device.tenant_id.clone(),
            device_id: device.device_id.clone(),
            device_profile_id: device.device_profile_id.clone(),
            device_name: device.device_name.clone(),
            device_type: device.device_type.clone(),
            is_gateway: false,
            gateway_session_id: String::new(),
        };
        let msg = tb_proto::core::ToCoreMsg {
            tenant_id: device.tenant_id.clone(),
            entity_id: device.device_id.clone(),
            msg: Some(tb_proto::core::to_core_msg::Msg::DeviceActivity(
                tb_proto::core::DeviceActivityMsg {
                    session: Some(session),
                    last_activity_ts: chrono::Utc::now().timestamp_millis(),
                },
            )),
        };
        producer
            .send(
                tb_queue::TbTopic::CORE,
                &device.device_id,
                bytes::Bytes::from(prost::Message::encode_to_vec(&msg)),
            )
            .await
            .map_err(TbError::internal)
    }
}

pub struct SnmpRunner {
    publisher: Arc<dyn UplinkPublisher>,
    oid_chunk_size: usize,
}

impl SnmpRunner {
    pub fn new(publisher: impl UplinkPublisher + 'static) -> Self {
        Self {
            publisher: Arc::new(publisher),
            oid_chunk_size: DEFAULT_OID_CHUNK_SIZE,
        }
    }

    pub fn from_shared(publisher: Arc<dyn UplinkPublisher>) -> Self {
        Self {
            publisher,
            oid_chunk_size: DEFAULT_OID_CHUNK_SIZE,
        }
    }

    pub fn with_oid_chunk_size(mut self, oid_chunk_size: usize) -> Result<Self, SnmpError> {
        if oid_chunk_size == 0 {
            return Err(SnmpError::InvalidChunkSize);
        }
        self.oid_chunk_size = oid_chunk_size;
        Ok(self)
    }

    pub async fn poll_once(
        &self,
        device: &DeviceInfoProto,
        device_config: &DeviceTransportConfiguration,
        profile_config: &ProfileTransportConfiguration,
    ) -> Result<PollReport, SnmpError> {
        let mut report = PollReport::default();
        for communication in &profile_config.communication_configs {
            let current = self
                .poll_communication(device, device_config, profile_config, communication)
                .await?;
            report.add(current);
        }
        Ok(report)
    }

    pub async fn poll_communication(
        &self,
        device: &DeviceInfoProto,
        device_config: &DeviceTransportConfiguration,
        profile_config: &ProfileTransportConfiguration,
        communication: &CommunicationConfig,
    ) -> Result<PollReport, SnmpError> {
        let mut session = AsyncSession::new_v2c(
            device_config.address(),
            device_config.community.as_bytes(),
            1,
        )
        .await?;
        let mut values = Vec::new();
        let mut skipped_values = 0;

        for chunk in communication.mappings.chunks(self.oid_chunk_size) {
            let oids = chunk
                .iter()
                .map(|mapping| &mapping.parsed_oid)
                .collect::<Vec<_>>();
            let mapped = self
                .get_with_retries(&mut session, &oids, profile_config, chunk)
                .await?;
            skipped_values += mapped.skipped_values;
            values.extend(mapped.values);
        }

        if values.is_empty() {
            return Ok(PollReport {
                skipped_values,
                ..PollReport::default()
            });
        }

        let mut report = PollReport {
            skipped_values,
            ..PollReport::default()
        };
        match communication.spec {
            QuerySpec::Telemetry => {
                let ts = chrono::Utc::now().timestamp_millis();
                let entries = values
                    .into_iter()
                    .map(|(key, value)| TsKvEntry { ts, key, value })
                    .collect::<Vec<_>>();
                report.telemetry_values = entries.len();
                self.publisher.publish_telemetry(device, entries).await?;
            }
            QuerySpec::ClientAttributes => {
                let entries = values
                    .into_iter()
                    .map(|(key, value)| KvEntry { key, value })
                    .collect::<Vec<_>>();
                report.attribute_values = entries.len();
                self.publisher.publish_attributes(device, entries).await?;
            }
        }
        Ok(report)
    }

    pub async fn run_device(
        self: Arc<Self>,
        device: DeviceInfoProto,
        device_config: DeviceTransportConfiguration,
        profile_config: ProfileTransportConfiguration,
        mut shutdown: watch::Receiver<bool>,
    ) {
        if *shutdown.borrow() {
            return;
        }
        let mut tasks = tokio::task::JoinSet::new();
        for communication in profile_config.communication_configs.iter().cloned() {
            let runner = self.clone();
            let device = device.clone();
            let device_config = device_config.clone();
            let profile_config = profile_config.clone();
            let mut shutdown = shutdown.clone();
            tasks.spawn(async move {
                let mut interval = tokio::time::interval(communication.querying_frequency);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            match runner.poll_communication(
                                &device,
                                &device_config,
                                &profile_config,
                                &communication,
                            ).await {
                                Ok(report) if report.telemetry_values + report.attribute_values > 0 => {
                                    if let Err(error) = runner.publisher.publish_activity(&device).await {
                                        tracing::warn!(device_id = device.device_id, ?error, "SNMP activity publish failed");
                                    }
                                }
                                Ok(_) => {}
                                Err(error) => tracing::warn!(device_id = device.device_id, ?error, "SNMP polling failed"),
                            }
                        }
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
            });
        }
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                Some(result) = tasks.join_next() => {
                    if let Err(error) = result {
                        tracing::warn!(?error, "SNMP polling task failed");
                    }
                }
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }

    async fn get_with_retries(
        &self,
        session: &mut AsyncSession,
        oids: &[&Oid<'_>],
        profile: &ProfileTransportConfiguration,
        mappings: &[crate::Mapping],
    ) -> Result<MappedValues, SnmpError> {
        let mut last_error = None;
        for _ in 0..=profile.retries {
            match tokio::time::timeout(profile.timeout, session.get_many(oids)).await {
                Ok(Ok(response)) => {
                    if response.error_status != 0 {
                        return Err(SnmpError::Response(format!(
                            "agent returned error status {} at index {}",
                            response.error_status, response.error_index
                        )));
                    }
                    return map_response(&response, mappings);
                }
                Ok(Err(error)) => last_error = Some(SnmpError::Get(error)),
                Err(_) => last_error = Some(SnmpError::Timeout(profile.timeout)),
            }
        }
        Err(last_error.unwrap_or(SnmpError::Timeout(profile.timeout)))
    }
}

struct MappedValues {
    values: Vec<(String, tb_common::KvValue)>,
    skipped_values: usize,
}

fn map_response(
    response: &Pdu<'_>,
    mappings: &[crate::Mapping],
) -> Result<MappedValues, SnmpError> {
    let by_oid = mappings
        .iter()
        .map(|mapping| (mapping.oid.as_str(), mapping))
        .collect::<HashMap<_, _>>();
    let mut seen = std::collections::HashSet::new();
    let mut values = Vec::new();
    let mut skipped_values = 0;
    for (oid, value) in response.varbinds.clone() {
        let oid = oid.to_string();
        let Some(mapping) = by_oid.get(oid.as_str()) else {
            continue;
        };
        if !seen.insert(oid.clone()) {
            return Err(SnmpError::Response(format!(
                "duplicate varbind for OID {oid}"
            )));
        }
        match mapping::convert(value, mapping.data_type) {
            Ok(Some(value)) => values.push((mapping.key.clone(), value)),
            Ok(None) => skipped_values += 1,
            Err(message) => {
                return Err(SnmpError::Mapping {
                    oid: mapping.oid.clone(),
                    message,
                });
            }
        }
    }
    Ok(MappedValues {
        values,
        skipped_values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_keep_the_default_limit() {
        let values = (0..205).collect::<Vec<_>>();
        let sizes = values
            .chunks(DEFAULT_OID_CHUNK_SIZE)
            .map(<[i32]>::len)
            .collect::<Vec<_>>();
        assert_eq!(sizes, vec![100, 100, 5]);
    }
}
