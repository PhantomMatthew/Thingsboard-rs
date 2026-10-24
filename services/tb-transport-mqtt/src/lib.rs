//! tb-transport-mqtt：设备侧 MQTT 3.1.1 服务端（QoS 0/1）。
//! 上行：认证走 transport-api，遥测/属性/RPC 封装进 tb_rule_engine.Main；
//! 会话事件（open/close/subscribe/activity）发 tb_core；
//! 下行：消费 tb_transport.notifications.<node_id> 推给在线会话。

pub mod codec;
pub mod downlink;
pub mod gateway;
pub mod ota;
pub mod provision;
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

/// 上行 publish 结果记录：限流静默丢弃（debug，MQTT 无错误回执语义），其余 warn。
pub fn log_publish_result(result: tb_common::TbResult<()>, context: &str) {
    match result {
        Ok(()) => {}
        Err(tb_common::TbError::RateLimitExceeded(_)) => {
            tracing::debug!("{context}: dropped by tenant rate limit");
        }
        Err(e) => tracing::warn!("{context}: {e}"),
    }
}

pub enum SessionCommand {
    Packet(MqttPacket),
    Close { session_id: String, reason: String },
}

/// 在线会话句柄：下行包经 tx 交给会话 task；qos 原子位记录已授予的订阅等级。
/// gateway_device 非空表示这是网关子设备的逻辑会话（下行 RPC 包装成 v1/gateway/rpc）。
pub struct SessionHandle {
    pub tx: mpsc::Sender<SessionCommand>,
    pub attributes_qos: Arc<AtomicU8>,
    pub rpc_qos: Arc<AtomicU8>,
    pub gateway_device: Option<String>,
}

pub type SessionRegistry = Arc<DashMap<String, SessionHandle>>;

#[derive(Clone)]
pub struct TransportState {
    pub transport_api: TransportApiClient,
    pub producer: KafkaQueueProducer,
    pub node_id: String,
    pub brokers: String,
    pub sessions: SessionRegistry,
    /// session_id → 当前可更新的固件包元数据（OTA 分块请求用）。
    pub ota_packages: moka::future::Cache<String, tb_proto::transport::OtaPackageProto>,
    /// (session_id, request_id) → 发起 RPC 的 core 实例。
    pub rpc_reply_nodes: moka::future::Cache<(String, i32), String>,
}

impl TransportState {
    pub fn new(brokers: &str, node_id: &str) -> Result<Self, QueueError> {
        Ok(Self {
            transport_api: TransportApiClient::new(brokers, node_id)?,
            producer: KafkaQueueProducer::new(brokers, &format!("tb-transport-mqtt-{node_id}"))?,
            node_id: node_id.to_string(),
            brokers: brokers.to_string(),
            sessions: Arc::new(DashMap::new()),
            ota_packages: moka::future::Cache::builder()
                .time_to_live(std::time::Duration::from_secs(3600))
                .build(),
            rpc_reply_nodes: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_live(std::time::Duration::from_secs(120))
                .build(),
        })
    }
}

/// accept 循环 + 下行通知 consumer；临时 accept 错误退避后继续监听。
pub async fn serve(listener: TcpListener, state: TransportState) -> std::io::Result<()> {
    tokio::spawn(downlink::run(state.clone()));
    let mut consecutive_errors = 0u32;
    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                consecutive_errors = 0;
                let st = state.clone();
                tokio::spawn(async move {
                    session::run(socket, peer, st).await;
                });
            }
            Err(e) => {
                consecutive_errors = consecutive_errors.saturating_add(1);
                let backoff = std::time::Duration::from_millis(
                    10u64.saturating_mul(1u64 << consecutive_errors.min(6)),
                );
                tracing::warn!(error = %e, ?backoff, "MQTT accept failed; retrying");
                tokio::time::sleep(backoff).await;
            }
        }
    }
}
