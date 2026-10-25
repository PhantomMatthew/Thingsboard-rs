//! tb_core 消费者：维护 Redis 设备会话、设备活性、规则引擎生命周期事件与 RPC 回流。
//! topic `tb_core`（key=device_id 保证同设备有序），group `tb-core-main`。
//! 独立消费 `tb_usage_stats`，由 core 的 UsageTracker 聚合租户遥测与规则引擎消息用量。

use std::future::Future;
use std::time::Duration;

use prost::Message as _;
use tb_common::msg::msg_type;
use tb_common::{AttributeScope, KvEntry, KvValue, TbError, TbResult};
use tb_proto::core as pb;
use tb_queue::consumer::ConsumedMessage;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer, StreamQueueConsumer, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::device_session::{SESSION_SWEEP_INTERVAL, SessionEntry};

const CORE_PARTITIONS: i32 = 10;

/// 暂时性失败的有界重试：耗尽后进入 DLQ 并提交，避免单条消息长期堵住分区。
#[derive(Clone, Copy)]
struct RetryPolicy {
    max_attempts: u32,
    base_delay: Duration,
    max_delay: Duration,
}

const RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_attempts: 8,
    base_delay: Duration::from_millis(200),
    max_delay: Duration::from_secs(5),
};

impl RetryPolicy {
    fn delay(&self, attempt: u32) -> Duration {
        self.base_delay
            .saturating_mul(1 << attempt.saturating_sub(1).min(16))
            .min(self.max_delay)
    }
}

/// 数据库连接类故障，以及映射为 Internal 的 Redis/Kafka 故障可以重试；
/// 输入错误、未找到、权限与约束冲突重试也不会成功。
fn is_retryable(err: &TbError) -> bool {
    tb_dao::is_transient_error(err) || matches!(err, TbError::Internal(_))
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DeadLetter<'a> {
    source_topic: &'a str,
    partition: i32,
    offset: i64,
    key: Option<String>,
    node_id: &'a str,
    error: &'a str,
    attempts: u32,
    ts: i64,
    /// 已脱敏时为重新编码后的消息，否则为原始字节。
    payload_hex: String,
    redacted: bool,
}

/// 写入 DLQ 直到成功：只有死信落盘后才允许提交原 offset。
async fn dead_letter<P: QueueProducer>(
    producer: &P,
    node_id: &str,
    msg: &ConsumedMessage,
    payload: &[u8],
    redacted: bool,
    error: &str,
    attempts: u32,
) {
    let key = msg
        .key
        .as_ref()
        .map(|key| String::from_utf8_lossy(key).into_owned());
    let letter = DeadLetter {
        source_topic: &msg.topic,
        partition: msg.partition,
        offset: msg.offset,
        key: key.clone(),
        node_id,
        error,
        attempts,
        ts: chrono::Utc::now().timestamp_millis(),
        payload_hex: hex::encode(payload),
        redacted,
    };
    let body = bytes::Bytes::from(serde_json::to_vec(&letter).unwrap_or_default());
    let dlq_key = key.unwrap_or_else(|| format!("{}-{}", msg.topic, msg.partition));
    loop {
        match producer
            .send(TbTopic::CORE_DLQ, &dlq_key, body.clone())
            .await
        {
            Ok(()) => {
                tracing::warn!(
                    topic = %msg.topic,
                    partition = msg.partition,
                    offset = msg.offset,
                    attempts,
                    "message sent to {}: {error}",
                    TbTopic::CORE_DLQ
                );
                return;
            }
            Err(e) => {
                tracing::error!(
                    topic = %msg.topic,
                    offset = msg.offset,
                    "core DLQ send failed; offset not committed: {e}"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// 认领密钥不得进入死信。
fn redact(mut msg: pb::ToCoreMsg) -> pb::ToCoreMsg {
    if let Some(pb::to_core_msg::Msg::ClaimDevice(claim)) = &mut msg.msg
        && !claim.secret_key.is_empty()
    {
        claim.secret_key = "<redacted>".into();
    }
    msg
}

/// 返回时消息已处理成功或已写入 DLQ，调用方可以提交 offset。
async fn process_record<P, F, Fut>(
    dlq: &P,
    node_id: &str,
    msg: &ConsumedMessage,
    policy: RetryPolicy,
    handler: F,
) where
    P: QueueProducer,
    F: Fn(pb::ToCoreMsg) -> Fut,
    Fut: Future<Output = TbResult<()>>,
{
    let decoded = match pb::ToCoreMsg::decode(msg.payload.clone()) {
        Ok(decoded) => decoded,
        Err(e) => {
            let error = format!("undecodable ToCoreMsg: {e}");
            dead_letter(dlq, node_id, msg, &msg.payload, false, &error, 1).await;
            return;
        }
    };
    let mut attempt = 0;
    loop {
        attempt += 1;
        let err = match handler(decoded.clone()).await {
            Ok(()) => return,
            Err(err) => err,
        };
        if is_retryable(&err) && attempt < policy.max_attempts {
            tracing::warn!(
                topic = %msg.topic,
                offset = msg.offset,
                attempt,
                "core message processing failed, retrying: {err}"
            );
            tokio::time::sleep(policy.delay(attempt)).await;
            continue;
        }
        let payload = redact(decoded).encode_to_vec();
        let error = format!("processing failed: {err}");
        dead_letter(dlq, node_id, msg, &payload, true, &error, attempt).await;
        return;
    }
}

async fn commit_until_ok<C: QueueConsumer>(consumer: &mut C, msg: &ConsumedMessage) {
    loop {
        match consumer.commit(msg).await {
            Ok(()) => return,
            Err(e) => {
                tracing::error!(topic = %msg.topic, offset = msg.offset, "commit failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

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
    if let Err(e) =
        tb_queue::admin::ensure_topic(&brokers, TbTopic::CORE_DLQ, CORE_PARTITIONS).await
    {
        tracing::warn!("ensure core DLQ topic failed: {e}");
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
    let producer = match KafkaQueueProducer::new(&brokers, &group_id) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("usage stats DLQ producer init failed: {e}");
            return;
        }
    };
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
                let error = match pb::UsageStatsMsg::decode(msg.payload.clone()) {
                    Ok(usage) => match Uuid::parse_str(&usage.tenant_id) {
                        Ok(tenant_id) => {
                            state
                                .usage
                                .inc_transport_msgs(tenant_id, usage.transport_msgs);
                            state.usage.inc_re_msgs(tenant_id, usage.re_msgs);
                            None
                        }
                        Err(e) => Some(format!("usage stats has bad tenant_id: {e}")),
                    },
                    Err(e) => Some(format!("undecodable UsageStatsMsg: {e}")),
                };
                if let Some(error) = error {
                    dead_letter(
                        &producer,
                        &state.node_id,
                        &msg,
                        &msg.payload,
                        false,
                        &error,
                        1,
                    )
                    .await;
                }
                commit_until_ok(&mut consumer, &msg).await;
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
                process_record(&producer, &state.node_id, &msg, RETRY_POLICY, |m| {
                    handle(&state, &producer, m)
                })
                .await;
                commit_until_ok(&mut consumer, &msg).await;
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
    let producer = match KafkaQueueProducer::new(&brokers, &group_id) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("core rpc response DLQ producer init failed: {e}");
            return;
        }
    };
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
                process_record(&producer, &state.node_id, &msg, RETRY_POLICY, |m| {
                    let result = match m.msg {
                        Some(pb::to_core_msg::Msg::RpcResponse(response)) => {
                            complete_rpc_response(&state, response)
                        }
                        _ => Err(TbError::InvalidInput(
                            "non-rpc message on core rpc response topic".into(),
                        )),
                    };
                    std::future::ready(result)
                })
                .await;
                commit_until_ok(&mut consumer, &msg).await;
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use tb_queue::QueueError;

    use super::*;

    const FAST: RetryPolicy = RetryPolicy {
        max_attempts: 3,
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
    };

    #[derive(Default)]
    struct RecordingProducer {
        sent: Mutex<Vec<(String, String, Bytes)>>,
        failures_left: AtomicU32,
    }

    #[async_trait::async_trait]
    impl QueueProducer for RecordingProducer {
        async fn send(&self, topic: &str, key: &str, payload: Bytes) -> Result<(), QueueError> {
            if self
                .failures_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(QueueError::Serialization("injected".into()));
            }
            self.sent
                .lock()
                .unwrap()
                .push((topic.into(), key.into(), payload));
            Ok(())
        }
    }

    impl RecordingProducer {
        fn letters(&self) -> Vec<serde_json::Value> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|(topic, _, payload)| {
                    assert_eq!(topic, TbTopic::CORE_DLQ);
                    serde_json::from_slice(payload).unwrap()
                })
                .collect()
        }
    }

    fn record(payload: Bytes) -> ConsumedMessage {
        ConsumedMessage {
            topic: TbTopic::CORE.into(),
            partition: 3,
            offset: 42,
            key: Some(Bytes::from_static(b"device-1")),
            payload,
        }
    }

    fn claim_msg() -> pb::ToCoreMsg {
        pb::ToCoreMsg {
            tenant_id: "t".into(),
            entity_id: "d".into(),
            msg: Some(pb::to_core_msg::Msg::ClaimDevice(pb::ClaimDeviceMsg {
                session: None,
                secret_key: "top-secret".into(),
                duration_ms: 1000,
            })),
        }
    }

    #[tokio::test]
    async fn poison_message_goes_to_dlq_with_source_context() {
        let producer = RecordingProducer::default();
        let calls = AtomicU32::new(0);
        let poison = Bytes::from_static(&[0xff, 0xff, 0xff]);
        process_record(&producer, "node-a", &record(poison), FAST, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(()))
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let letters = producer.letters();
        assert_eq!(letters.len(), 1);
        let letter = &letters[0];
        assert_eq!(letter["sourceTopic"], TbTopic::CORE);
        assert_eq!(letter["partition"], 3);
        assert_eq!(letter["offset"], 42);
        assert_eq!(letter["key"], "device-1");
        assert_eq!(letter["nodeId"], "node-a");
        assert_eq!(letter["payloadHex"], "ffffff");
        assert_eq!(letter["redacted"], false);
        assert!(letter["error"].as_str().unwrap().contains("undecodable"));
    }

    #[tokio::test]
    async fn transient_failure_is_retried_until_success() {
        let producer = RecordingProducer::default();
        let calls = AtomicU32::new(0);
        let payload = Bytes::from(claim_msg().encode_to_vec());
        process_record(&producer, "node-a", &record(payload), FAST, |_| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(if n < 2 {
                Err(TbError::Internal("redis unavailable".into()))
            } else {
                Ok(())
            })
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(producer.letters().is_empty());
    }

    #[tokio::test]
    async fn exhausted_retries_and_permanent_errors_go_to_redacted_dlq() {
        let payload = Bytes::from(claim_msg().encode_to_vec());
        let transient = || TbError::Internal("redis unavailable".into());
        let permanent = || TbError::InvalidInput("bad claim".into());
        let cases: [(&dyn Fn() -> TbError, u32); 2] = [(&transient, 3), (&permanent, 1)];
        for (make_error, expected_attempts) in cases {
            let producer = RecordingProducer::default();
            let calls = AtomicU32::new(0);
            let message = make_error().to_string();
            process_record(&producer, "node-a", &record(payload.clone()), FAST, |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err(make_error()))
            })
            .await;
            assert_eq!(calls.load(Ordering::SeqCst), expected_attempts);
            let letters = producer.letters();
            assert_eq!(letters.len(), 1, "{message}");
            let letter = &letters[0];
            assert_eq!(letter["attempts"], expected_attempts);
            assert_eq!(letter["redacted"], true);
            assert!(letter["error"].as_str().unwrap().contains(&message));
            let stored = pb::ToCoreMsg::decode(
                hex::decode(letter["payloadHex"].as_str().unwrap())
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let Some(pb::to_core_msg::Msg::ClaimDevice(claim)) = stored.msg else {
                panic!("claim message expected");
            };
            assert_eq!(claim.secret_key, "<redacted>");
            assert_eq!(claim.duration_ms, 1000);
        }
    }

    #[tokio::test]
    async fn dlq_send_is_retried_before_returning() {
        let producer = Arc::new(RecordingProducer {
            failures_left: AtomicU32::new(2),
            ..Default::default()
        });
        tokio::time::timeout(
            Duration::from_secs(10),
            process_record(
                producer.as_ref(),
                "node-a",
                &record(Bytes::from_static(&[0xff])),
                FAST,
                |_| std::future::ready(Ok(())),
            ),
        )
        .await
        .expect("dead letter eventually written");
        assert_eq!(producer.letters().len(), 1);
    }

    #[test]
    fn retry_delay_grows_and_is_capped() {
        assert_eq!(RETRY_POLICY.delay(1), Duration::from_millis(200));
        assert_eq!(RETRY_POLICY.delay(2), Duration::from_millis(400));
        assert_eq!(RETRY_POLICY.delay(10), Duration::from_secs(5));
        assert!(is_retryable(&TbError::Internal("x".into())));
        assert!(!is_retryable(&TbError::InvalidInput("x".into())));
        assert!(!is_retryable(&TbError::NotFound("x".into())));
    }
}
