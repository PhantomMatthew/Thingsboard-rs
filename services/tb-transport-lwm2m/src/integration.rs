use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::Bytes;
use futures::future::BoxFuture;
use prost::Message as _;
use tb_common::{KvEntry, KvValue, TbResult, TsKvEntry};
use tb_proto::{core as core_pb, transport as pb};
use tb_queue::{KafkaQueueProducer, QueueProducer as _, TbTopic};
use tb_transport_api::TransportApiClient;
use uuid::Uuid;

use crate::profile::{ObservedResource, ValueKind};
use crate::types::ResourceType;

#[derive(Debug, Clone)]
pub struct EndpointIdentity {
    pub device: pb::DeviceInfoProto,
    pub profile: pb::DeviceProfileProto,
    pub resource_types: HashMap<String, ResourceType>,
}

pub trait EndpointValidator: Send + Sync {
    fn validate<'a>(
        &'a self,
        endpoint: &'a str,
        peer: SocketAddr,
    ) -> BoxFuture<'a, TbResult<Option<EndpointIdentity>>>;
}

pub trait UplinkSink: Send + Sync {
    fn publish<'a>(
        &'a self,
        device: &'a pb::DeviceInfoProto,
        resource: &'a ObservedResource,
        value: KvValue,
        timestamp_ms: i64,
    ) -> BoxFuture<'a, TbResult<()>>;
}

pub trait SessionActivity: Send + Sync {
    fn open<'a>(
        &'a self,
        session: &'a core_pb::SessionInfoProto,
        device: &'a pb::DeviceInfoProto,
    ) -> BoxFuture<'a, TbResult<()>>;

    fn activity<'a>(
        &'a self,
        session: &'a core_pb::SessionInfoProto,
        device: &'a pb::DeviceInfoProto,
        timestamp_ms: i64,
    ) -> BoxFuture<'a, TbResult<()>>;

    fn close<'a>(
        &'a self,
        session: &'a core_pb::SessionInfoProto,
        device: &'a pb::DeviceInfoProto,
        reason: &'a str,
    ) -> BoxFuture<'a, TbResult<()>>;
}

#[derive(Clone)]
pub struct TransportApiUplink {
    client: TransportApiClient,
}

impl TransportApiUplink {
    pub fn new(client: TransportApiClient) -> Self {
        Self { client }
    }
}

impl UplinkSink for TransportApiUplink {
    fn publish<'a>(
        &'a self,
        device: &'a pb::DeviceInfoProto,
        resource: &'a ObservedResource,
        value: KvValue,
        timestamp_ms: i64,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            match resource.kind {
                ValueKind::Telemetry => {
                    self.client
                        .publish_telemetry(
                            device,
                            vec![TsKvEntry {
                                ts: timestamp_ms,
                                key: resource.key_name.clone(),
                                value,
                            }],
                        )
                        .await
                }
                ValueKind::Attribute => {
                    self.client
                        .publish_attributes(
                            device,
                            vec![KvEntry {
                                key: resource.key_name.clone(),
                                value,
                            }],
                        )
                        .await
                }
            }
        })
    }
}

#[derive(Clone)]
pub struct CoreSessionActivity {
    producer: KafkaQueueProducer,
    node_id: String,
}

impl CoreSessionActivity {
    pub fn new(producer: KafkaQueueProducer, node_id: impl Into<String>) -> Self {
        Self {
            producer,
            node_id: node_id.into(),
        }
    }

    pub fn session_info(
        &self,
        device: &pb::DeviceInfoProto,
        session_id: &str,
    ) -> core_pb::SessionInfoProto {
        core_pb::SessionInfoProto {
            node_id: self.node_id.clone(),
            session_id: session_id.into(),
            tenant_id: device.tenant_id.clone(),
            device_id: device.device_id.clone(),
            device_profile_id: device.device_profile_id.clone(),
            device_name: device.device_name.clone(),
            device_type: device.device_type.clone(),
            is_gateway: false,
            gateway_session_id: String::new(),
        }
    }

    async fn send(
        &self,
        device: &pb::DeviceInfoProto,
        msg: core_pb::to_core_msg::Msg,
    ) -> TbResult<()> {
        let msg = core_pb::ToCoreMsg {
            tenant_id: device.tenant_id.clone(),
            entity_id: device.device_id.clone(),
            msg: Some(msg),
        };
        self.producer
            .send(
                TbTopic::CORE,
                &device.device_id,
                Bytes::from(encode_core_message(&msg)),
            )
            .await
            .map_err(tb_common::TbError::internal)
    }
}

impl SessionActivity for CoreSessionActivity {
    fn open<'a>(
        &'a self,
        session: &'a core_pb::SessionInfoProto,
        device: &'a pb::DeviceInfoProto,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            self.send(
                device,
                core_pb::to_core_msg::Msg::SessionOpen(core_pb::SessionOpenMsg {
                    session: Some(session.clone()),
                }),
            )
            .await
        })
    }

    fn activity<'a>(
        &'a self,
        session: &'a core_pb::SessionInfoProto,
        device: &'a pb::DeviceInfoProto,
        timestamp_ms: i64,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            self.send(
                device,
                core_pb::to_core_msg::Msg::DeviceActivity(core_pb::DeviceActivityMsg {
                    session: Some(session.clone()),
                    last_activity_ts: timestamp_ms,
                }),
            )
            .await
        })
    }

    fn close<'a>(
        &'a self,
        session: &'a core_pb::SessionInfoProto,
        device: &'a pb::DeviceInfoProto,
        reason: &'a str,
    ) -> BoxFuture<'a, TbResult<()>> {
        Box::pin(async move {
            self.send(
                device,
                core_pb::to_core_msg::Msg::SessionClose(core_pb::SessionCloseMsg {
                    session: Some(session.clone()),
                    reason: reason.into(),
                }),
            )
            .await
        })
    }
}

pub fn new_session_id() -> String {
    Uuid::now_v7().to_string()
}

fn encode_core_message(msg: &core_pb::ToCoreMsg) -> Vec<u8> {
    msg.encode_to_vec()
}
