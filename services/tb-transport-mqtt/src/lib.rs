//! tb-transport-mqtt：设备侧 MQTT 3.1.1 服务端（QoS 0/1）。
//! 上行：认证走 transport-api，遥测/属性/RPC 封装进 tb_rule_engine.Main；
//! 会话事件（open/close/subscribe/activity）发 tb_core；
//! 下行：消费 tb_transport.notifications.<node_id> 推给在线会话。

pub mod codec;
pub mod downlink;
pub mod session;

use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use dashmap::DashMap;
use tb_queue::{KafkaQueueProducer, QueueError};
use tb_transport_api::TransportApiClient;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::codec::MqttPacket;

/// 订阅标志位 sentinel：未订阅。
pub const UNSUBSCRIBED: u8 = 0xFF;

/// 在线会话句柄：下行包经 tx 交给会话 task；qos 原子位记录已授予的订阅等级。
pub struct SessionHandle {
    pub tx: mpsc::Sender<MqttPacket>,
    pub attributes_qos: Arc<AtomicU8>,
    pub rpc_qos: Arc<AtomicU8>,
}

pub type SessionRegistry = Arc<DashMap<String, SessionHandle>>;

#[derive(Clone)]
pub struct TransportState {
    pub transport_api: TransportApiClient,
    pub producer: KafkaQueueProducer,
    pub node_id: String,
    pub brokers: String,
    pub sessions: SessionRegistry,
}

impl TransportState {
    pub fn new(brokers: &str, node_id: &str) -> Result<Self, QueueError> {
        Ok(Self {
            transport_api: TransportApiClient::new(brokers, node_id)?,
            producer: KafkaQueueProducer::new(brokers, &format!("tb-transport-mqtt-{node_id}"))?,
            node_id: node_id.to_string(),
            brokers: brokers.to_string(),
            sessions: Arc::new(DashMap::new()),
        })
    }
}

/// accept 循环 + 下行通知 consumer，直到 listener 出错。
pub async fn serve(listener: TcpListener, state: TransportState) -> std::io::Result<()> {
    tokio::spawn(downlink::run(state.clone()));
    loop {
        let (socket, peer) = listener.accept().await?;
        let st = state.clone();
        tokio::spawn(async move {
            session::run(socket, peer, st).await;
        });
    }
}
