//! tb_core 消费者：维护设备会话注册表、设备活性 server 属性、pending RPC 完成。
//! topic `tb_core`（key=device_id 保证同设备有序），group `tb-core-main`。

use prost::Message as _;
use tb_common::{AttributeScope, KvEntry, KvValue};
use tb_proto::core as pb;
use tb_queue::{KafkaQueueProducer, QueueConsumer, StreamQueueConsumer, TbTopic};
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
        let _producer = match KafkaQueueProducer::new(&brokers, &group_id) {
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
                    handle(&state, m).await;
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

async fn handle(state: &AppState, msg: pb::ToCoreMsg) {
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
            if !state.device_sessions.activity_throttled(device_id, m.last_activity_ts) {
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
        other => tracing::debug!(?other, "ignored ToCoreMsg"),
    }
}
