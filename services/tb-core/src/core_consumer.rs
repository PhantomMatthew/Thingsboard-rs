//! tb_core 消费者：维护设备会话注册表、设备活性 server 属性、pending RPC 完成。
//! topic `tb_core`（key=device_id 保证同设备有序），group `tb-core-main`。

use prost::Message as _;
use tb_common::{AttributeScope, KvEntry, KvValue};
use tb_proto::core as pb;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer, StreamQueueConsumer, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::device_session::SessionEntry;

const CORE_PARTITIONS: i32 = 10;

pub async fn start_core_consumer(state: AppState) -> tokio::task::JoinHandle<()> {
    start_core_consumer_for_group(state, "tb-core-main").await
}

/// group_id 可定制：集成测试用独立 group 避免幽灵成员拖慢 rebalance。
pub async fn start_core_consumer_for_group(
    state: AppState,
    group_id: &str,
) -> tokio::task::JoinHandle<()> {
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    if let Err(e) = tb_queue::admin::ensure_topic(&brokers, TbTopic::CORE, CORE_PARTITIONS).await {
        tracing::warn!("ensure tb_core topic failed: {e}");
    }
    let group_id = group_id.to_string();
    tokio::spawn(async move {
        let producer = match KafkaQueueProducer::new(&brokers, &group_id) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("core consumer producer init failed: {e}");
                return;
            }
        };
        let mut consumer = match StreamQueueConsumer::new(&brokers, &group_id, &[TbTopic::CORE]) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("core consumer init failed: {e}");
                return;
            }
        };
        tracing::info!("tb_core consumer started");
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let Ok(m) = pb::ToCoreMsg::decode(msg.payload) else {
                        tracing::warn!("undecodable ToCoreMsg");
                        continue;
                    };
                    handle(&state, &producer, m).await;
                }
                Err(e) => {
                    tracing::error!("tb_core consumer error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    })
}

fn session_device_id(session: Option<&pb::SessionInfoProto>) -> Option<Uuid> {
    Uuid::parse_str(session?.device_id.as_str()).ok()
}

async fn write_server_attributes(state: &AppState, device_id: Uuid, entries: Vec<KvEntry>) {
    if let Err(e) = state
        .attributes_dao
        .save(device_id, AttributeScope::ServerScope, &entries)
        .await
    {
        tracing::warn!(%device_id, "write device state attributes failed: {e}");
    }
}

async fn handle(state: &AppState, producer: &KafkaQueueProducer, msg: pb::ToCoreMsg) {
    use pb::to_core_msg::Msg;
    let Some(oneof) = msg.msg else { return };
    match oneof {
        Msg::SessionOpen(m) => {
            let Some(session) = m.session else { return };
            let Some(device_id) = session_device_id(Some(&session)) else {
                return;
            };
            state.device_sessions.open_session(
                device_id,
                SessionEntry {
                    session_id: session.session_id.clone(),
                    node_id: session.node_id.clone(),
                    is_gateway: session.is_gateway,
                    sub_attributes: false,
                    sub_rpc: false,
                },
            );
            let now = chrono::Utc::now().timestamp_millis();
            write_server_attributes(
                state,
                device_id,
                vec![
                    KvEntry {
                        key: "active".into(),
                        value: KvValue::Bool(true),
                    },
                    KvEntry {
                        key: "lastConnectTime".into(),
                        value: KvValue::Long(now),
                    },
                ],
            )
            .await;
        }
        Msg::SessionClose(m) => {
            let Some(session) = m.session else { return };
            let Some(device_id) = session_device_id(Some(&session)) else {
                return;
            };
            if state
                .device_sessions
                .close_session(device_id, &session.session_id)
            {
                let now = chrono::Utc::now().timestamp_millis();
                write_server_attributes(
                    state,
                    device_id,
                    vec![
                        KvEntry {
                            key: "active".into(),
                            value: KvValue::Bool(false),
                        },
                        KvEntry {
                            key: "lastDisconnectTime".into(),
                            value: KvValue::Long(now),
                        },
                    ],
                )
                .await;
            }
        }
        Msg::SubscribeAttributes(m) => {
            let Some(session) = m.session else { return };
            if let Some(device_id) = session_device_id(Some(&session)) {
                state.device_sessions.set_subscription(
                    device_id,
                    &session.session_id,
                    &session.node_id,
                    session.is_gateway,
                    false,
                    m.subscribe,
                );
            }
        }
        Msg::SubscribeRpc(m) => {
            let Some(session) = m.session else { return };
            if let Some(device_id) = session_device_id(Some(&session)) {
                state.device_sessions.set_subscription(
                    device_id,
                    &session.session_id,
                    &session.node_id,
                    session.is_gateway,
                    true,
                    m.subscribe,
                );
            }
        }
        Msg::DeviceActivity(m) => {
            let Some(session) = m.session else { return };
            let Some(device_id) = session_device_id(Some(&session)) else {
                return;
            };
            if !state
                .device_sessions
                .activity_throttled(device_id, m.last_activity_ts)
            {
                write_server_attributes(
                    state,
                    device_id,
                    vec![
                        KvEntry {
                            key: "active".into(),
                            value: KvValue::Bool(true),
                        },
                        KvEntry {
                            key: "lastActivityTime".into(),
                            value: KvValue::Long(m.last_activity_ts),
                        },
                    ],
                )
                .await;
            }
        }
        Msg::RpcResponse(m) => {
            let Some(session) = m.session else { return };
            let Some(device_id) = session_device_id(Some(&session)) else {
                return;
            };
            let result = if m.error.is_empty() {
                Ok(m.payload_json)
            } else {
                Err(m.error)
            };
            state
                .device_sessions
                .complete_pending_rpc(device_id, m.request_id, result);
        }
        Msg::ClaimDevice(_) => {
            tracing::debug!("claim_device not implemented");
        }
        Msg::RpcRequest(m) => {
            // 规则引擎 action.rpc_oneway 的下行 RPC：oneway 语义，不注册 pending。
            let Ok(device_id) = Uuid::parse_str(&m.device_id) else {
                tracing::warn!(device_id = %m.device_id, "rpc_request: bad device_id");
                return;
            };
            let Some(session) = state.device_sessions.rpc_session(device_id) else {
                tracing::warn!(%device_id, method = %m.method, "rpc_request: device offline or not subscribed");
                return;
            };
            let request_id = state.device_sessions.next_rpc_request_id();
            let to_transport = pb::ToTransportMsg {
                session_id: session.session_id.clone(),
                entity_id: device_id.to_string(),
                msg: Some(pb::to_transport_msg::Msg::ToDeviceRpc(
                    pb::ToDeviceRpcRequestMsg {
                        request_id,
                        method: m.method,
                        params_json: m.params_json,
                        expiration_ts: chrono::Utc::now().timestamp_millis() + 60_000,
                        persisted: false,
                    },
                )),
            };
            let topic = TbTopic::transport_notifications(&session.node_id);
            if let Err(e) = producer
                .send(
                    &topic,
                    &session.session_id,
                    bytes::Bytes::from(to_transport.encode_to_vec()),
                )
                .await
            {
                tracing::error!("rpc_request dispatch failed: {e}");
            }
        }
        other => tracing::debug!(?other, "ignored ToCoreMsg"),
    }
}
