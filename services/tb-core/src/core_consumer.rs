//! tb_core 消费者：维护 Redis 设备会话、设备活性、规则引擎生命周期事件与 RPC 回流。
//! topic `tb_core`（key=device_id 保证同设备有序），group `tb-core-main`。
//! 独立消费 `tb_usage_stats`，由 core 的 UsageTracker 聚合租户遥测与规则引擎消息用量。

use prost::Message as _;
use tb_common::msg::msg_type;
use tb_common::{AttributeScope, KvEntry, KvValue, TbError, TbResult};
use tb_proto::core as pb;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer, StreamQueueConsumer, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::device_session::{SESSION_SWEEP_INTERVAL, SessionEntry};

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
    let responses_topic = TbTopic::core_rpc_responses(&state.node_id);
    if let Err(e) = tb_queue::admin::ensure_topic(&brokers, &responses_topic, 1).await {
        tracing::warn!("ensure core rpc responses topic failed: {e}");
    }
    if let Err(e) =
        tb_queue::admin::ensure_topic(&brokers, TbTopic::USAGE_STATS, CORE_PARTITIONS).await
    {
        tracing::warn!("ensure usage stats topic failed: {e}");
    }
    let group_id = group_id.to_string();
    let usage_group = format!("{group_id}-usage-stats");
    tokio::spawn(async move {
        let main_state = state.clone();
        let main_brokers = brokers.clone();
        let main_group = group_id.clone();
        let response_state = state.clone();
        let response_brokers = brokers.clone();
        let usage_state = state.clone();
        let usage_brokers = brokers.clone();
        tokio::select! {
            _ = run_main(main_state, main_brokers, main_group) => {},
            _ = run_rpc_responses(response_state, response_brokers) => {},
            _ = run_usage_stats(usage_state, usage_brokers, usage_group) => {},
            _ = run_session_sweeper(state) => {},
        }
    })
}

async fn run_usage_stats(state: AppState, brokers: String, group_id: String) {
    let mut consumer = match StreamQueueConsumer::new_manual_commit(
        &brokers,
        &group_id,
        &[TbTopic::USAGE_STATS],
    ) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("usage stats consumer init failed: {e}");
            return;
        }
    };
    tracing::info!("usage stats consumer started");
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                match pb::UsageStatsMsg::decode(msg.payload.clone()) {
                    Ok(usage) => match Uuid::parse_str(&usage.tenant_id) {
                        Ok(tenant_id) => {
                            state
                                .usage
                                .inc_transport_msgs(tenant_id, usage.transport_msgs);
                            state.usage.inc_re_msgs(tenant_id, usage.re_msgs);
                        }
                        Err(e) => {
                            tracing::warn!(tenant_id = %usage.tenant_id, "usage stats has bad tenant_id: {e}");
                        }
                    },
                    Err(e) => tracing::warn!("undecodable UsageStatsMsg: {e}"),
                }
                if let Err(e) = consumer.commit(&msg).await {
                    tracing::error!("usage stats commit failed: {e}");
                }
            }
            Err(e) => {
                tracing::error!("usage stats consumer error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn run_main(state: AppState, brokers: String, group_id: String) {
    let producer = match KafkaQueueProducer::new(&brokers, &group_id) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("core consumer producer init failed: {e}");
            return;
        }
    };
    let mut consumer =
        match StreamQueueConsumer::new_manual_commit(&brokers, &group_id, &[TbTopic::CORE]) {
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
                let Ok(m) = pb::ToCoreMsg::decode(msg.payload.clone()) else {
                    tracing::warn!("undecodable ToCoreMsg");
                    continue;
                };
                match handle(&state, &producer, m).await {
                    Ok(()) => {
                        if let Err(e) = consumer.commit(&msg).await {
                            tracing::error!("tb_core commit failed: {e}");
                        }
                    }
                    Err(e) => tracing::error!("tb_core message processing failed: {e}"),
                }
            }
            Err(e) => {
                tracing::error!("tb_core consumer error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn run_rpc_responses(state: AppState, brokers: String) {
    let topic = TbTopic::core_rpc_responses(&state.node_id);
    let group_id = format!("tb-core-rpc-{}", state.node_id);
    let mut consumer =
        match StreamQueueConsumer::new_from_beginning_manual(&brokers, &group_id, &[&topic]) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("core rpc response consumer init failed: {e}");
                return;
            }
        };
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let Ok(m) = pb::ToCoreMsg::decode(msg.payload.clone()) else {
                    tracing::warn!("undecodable routed rpc response");
                    continue;
                };
                let result = match m.msg {
                    Some(pb::to_core_msg::Msg::RpcResponse(response)) => {
                        complete_rpc_response(&state, response)
                    }
                    _ => {
                        tracing::warn!("non-rpc message on core rpc response topic");
                        Ok(())
                    }
                };
                match result {
                    Ok(()) => {
                        if let Err(e) = consumer.commit(&msg).await {
                            tracing::error!("core rpc response commit failed: {e}");
                        }
                    }
                    Err(e) => tracing::error!("routed rpc response processing failed: {e}"),
                }
            }
            Err(e) => {
                tracing::error!("core rpc response consumer error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn run_session_sweeper(state: AppState) {
    loop {
        tokio::time::sleep(SESSION_SWEEP_INTERVAL).await;
        match state.device_sessions.sweep_expired().await {
            Ok(0) => {}
            Ok(removed) => tracing::info!(removed, "expired device sessions swept"),
            Err(e) => tracing::error!("device session sweep failed: {e}"),
        }
    }
}

fn session_device_id(session: Option<&pb::SessionInfoProto>) -> Option<Uuid> {
    Uuid::parse_str(session?.device_id.as_str()).ok()
}

async fn write_server_attributes(
    state: &AppState,
    device_id: Uuid,
    entries: Vec<KvEntry>,
) -> TbResult<()> {
    state
        .attributes_dao
        .save(device_id, AttributeScope::ServerScope, &entries)
        .await
}

fn complete_rpc_response(state: &AppState, m: pb::RpcResponseMsg) -> TbResult<()> {
    let Some(device_id) = session_device_id(m.session.as_ref()) else {
        return Err(TbError::InvalidInput(
            "rpc response without valid device".into(),
        ));
    };
    let result = if m.error.is_empty() {
        Ok(m.payload_json)
    } else {
        Err(m.error)
    };
    state
        .device_sessions
        .complete_pending_rpc(device_id, m.request_id, result);
    Ok(())
}

async fn handle(
    state: &AppState,
    producer: &KafkaQueueProducer,
    msg: pb::ToCoreMsg,
) -> TbResult<()> {
    use pb::to_core_msg::Msg;
    if let Some(Msg::RpcResponse(response)) = &msg.msg
        && !response.reply_node_id.is_empty()
        && response.reply_node_id != state.node_id
    {
        let topic = TbTopic::core_rpc_responses(&response.reply_node_id);
        producer
            .send(
                &topic,
                &msg.entity_id,
                bytes::Bytes::from(msg.encode_to_vec()),
            )
            .await
            .map_err(TbError::internal)?;
        return Ok(());
    }
    let Some(oneof) = msg.msg else { return Ok(()) };
    match oneof {
        Msg::SessionOpen(m) => {
            let Some(session) = m.session else {
                return Ok(());
            };
            let Some(device_id) = session_device_id(Some(&session)) else {
                return Ok(());
            };
            state
                .device_sessions
                .open_session(
                    device_id,
                    SessionEntry {
                        session_id: session.session_id.clone(),
                        node_id: session.node_id.clone(),
                        is_gateway: session.is_gateway,
                        via_gateway: !session.gateway_session_id.is_empty(),
                        sub_attributes: false,
                        sub_rpc: false,
                        last_seen_ms: 0,
                    },
                )
                .await?;
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
            .await?;
            crate::rule_engine_events::publish_session_event(
                producer,
                &session,
                msg_type::CONNECT_EVENT,
            )
            .await?;
        }
        Msg::SessionClose(m) => {
            let Some(session) = m.session else {
                return Ok(());
            };
            let Some(device_id) = session_device_id(Some(&session)) else {
                return Ok(());
            };
            if state
                .device_sessions
                .close_session(device_id, &session.session_id)
                .await?
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
                .await?;
            }
            crate::rule_engine_events::publish_session_event(
                producer,
                &session,
                msg_type::DISCONNECT_EVENT,
            )
            .await?;
        }
        Msg::SubscribeAttributes(m) => {
            let Some(session) = m.session else {
                return Ok(());
            };
            if let Some(device_id) = session_device_id(Some(&session)) {
                state
                    .device_sessions
                    .set_subscription(
                        device_id,
                        &session.session_id,
                        &session.node_id,
                        session.is_gateway,
                        false,
                        m.subscribe,
                    )
                    .await?;
            }
        }
        Msg::SubscribeRpc(m) => {
            let Some(session) = m.session else {
                return Ok(());
            };
            if let Some(device_id) = session_device_id(Some(&session)) {
                state
                    .device_sessions
                    .set_subscription(
                        device_id,
                        &session.session_id,
                        &session.node_id,
                        session.is_gateway,
                        true,
                        m.subscribe,
                    )
                    .await?;
            }
        }
        Msg::DeviceActivity(m) => {
            let Some(session) = m.session else {
                return Ok(());
            };
            let Some(device_id) = session_device_id(Some(&session)) else {
                return Ok(());
            };
            state
                .device_sessions
                .touch_session(device_id, &session.session_id)
                .await?;
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
                .await?;
                crate::rule_engine_events::publish_session_event(
                    producer,
                    &session,
                    msg_type::ACTIVITY_EVENT,
                )
                .await?;
            }
        }
        Msg::RpcResponse(m) => complete_rpc_response(state, m)?,
        Msg::ClaimDevice(m) => crate::claim::register(state, &m).await?,
        Msg::RpcRequest(m) => {
            let Ok(device_id) = Uuid::parse_str(&m.device_id) else {
                tracing::warn!(device_id = %m.device_id, "rpc_request: bad device_id");
                return Ok(());
            };
            let Some(session) = state.device_sessions.rpc_session(device_id).await? else {
                tracing::warn!(%device_id, method = %m.method, "rpc_request: device offline or not subscribed");
                return Ok(());
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
                        reply_node_id: state.node_id.clone(),
                    },
                )),
            };
            let topic = TbTopic::transport_notifications(&session.node_id);
            producer
                .send(
                    &topic,
                    &session.session_id,
                    bytes::Bytes::from(to_transport.encode_to_vec()),
                )
                .await
                .map_err(TbError::internal)?;
        }
        other => tracing::debug!(?other, "ignored ToCoreMsg"),
    }
    Ok(())
}
