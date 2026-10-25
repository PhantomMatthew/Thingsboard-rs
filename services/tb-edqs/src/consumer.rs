//! Kafka 事件消费：earliest + 手动提交；连续错误后指数退避重建 consumer。
//! 租户重同步标记与定期对账在同一任务内从数据库重载租户。

use std::collections::BTreeSet;
use std::time::Duration;

use prost::Message as _;
use tb_proto::edqs::EdqsEventMsg;
use tb_proto::edqs::edqs_event_msg::Event;
use tb_queue::{QueueConsumer, QueueError, StreamQueueConsumer, TbTopic};
use uuid::Uuid;

use crate::state::EdqsState;

pub const DEFAULT_GROUP: &str = "tb-edqs";
const MAX_CONSECUTIVE_ERRORS: u32 = 3;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const RESYNC_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct ReconnectBackoff {
    delay: Duration,
}

impl Default for ReconnectBackoff {
    fn default() -> Self {
        Self {
            delay: INITIAL_RECONNECT_DELAY,
        }
    }
}

impl ReconnectBackoff {
    fn next_delay(&mut self) -> Duration {
        let delay = self.delay;
        self.delay = self.delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
        delay
    }

    fn reset(&mut self) {
        self.delay = INITIAL_RECONNECT_DELAY;
    }
}

fn new_consumer(brokers: &str, group_id: &str) -> Result<StreamQueueConsumer, QueueError> {
    StreamQueueConsumer::new_from_beginning_manual(brokers, group_id, &[TbTopic::EDQS_EVENTS])
}

fn stream_ended(error: &QueueError) -> bool {
    matches!(error, QueueError::Serialization(message) if message == "consumer stream ended")
}

fn should_reconnect(error: &QueueError, consecutive_errors: u32) -> bool {
    stream_ended(error) || consecutive_errors >= MAX_CONSECUTIVE_ERRORS
}

/// 待从数据库重载的租户；重载失败（数据库不可用）时保留，按间隔重试。
#[derive(Debug, Default)]
struct PendingResync {
    tenants: BTreeSet<Uuid>,
}

impl PendingResync {
    fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    /// 在 consumer 任务内串行执行：重载期间不应用事件，之后的事件按 ts 规则幂等覆盖。
    async fn drain(&mut self, state: &EdqsState) {
        let Some(pool) = state.pool.clone() else {
            if !self.tenants.is_empty() {
                tracing::warn!(
                    tenants = self.tenants.len(),
                    "edqs resync requested but no database configured"
                );
                self.tenants.clear();
            }
            return;
        };
        while let Some(tenant_id) = self.tenants.first().copied() {
            match crate::snapshot::reload_tenant(&pool, state, tenant_id).await {
                Ok(()) => {
                    self.tenants.remove(&tenant_id);
                    state.consumer_health.beat();
                }
                Err(e) => {
                    tracing::warn!(%tenant_id, pending = self.tenants.len(), "edqs tenant resync failed: {e}");
                    return;
                }
            }
        }
    }
}

fn resync_target(event: &EdqsEventMsg) -> Option<Uuid> {
    let Some(Event::TenantResync(resync)) = &event.event else {
        return None;
    };
    match Uuid::parse_str(&resync.tenant_id) {
        Ok(tenant_id) if resync.tenant_id == event.tenant_id => Some(tenant_id),
        _ => {
            tracing::warn!(
                tenant_id = event.tenant_id,
                resync_tenant_id = resync.tenant_id,
                "edqs resync with bad tenant_id"
            );
            None
        }
    }
}

async fn next_reconcile(timer: &mut Option<tokio::time::Interval>) {
    match timer {
        Some(timer) => {
            timer.tick().await;
        }
        None => std::future::pending().await,
    }
}

async fn consume_until_reconnect(
    consumer: &mut StreamQueueConsumer,
    state: &EdqsState,
    pending: &mut PendingResync,
) -> bool {
    let mut consecutive_errors = 0;
    let mut made_progress = false;
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut retry = tokio::time::interval(RESYNC_RETRY_INTERVAL);
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut reconcile = state.reconcile_interval.map(|interval| {
        let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        timer
    });
    loop {
        tokio::select! {
            biased;
            _ = retry.tick(), if !pending.is_empty() => {
                pending.drain(state).await;
            }
            _ = next_reconcile(&mut reconcile) => {
                if let Some(pool) = &state.pool {
                    match crate::snapshot::reconcile_tenant_ids(pool, state).await {
                        Ok(ids) => {
                            tracing::info!(tenants = ids.len(), "edqs periodic reconcile");
                            pending.tenants.extend(ids);
                            pending.drain(state).await;
                        }
                        Err(e) => tracing::warn!("edqs reconcile tenant listing failed: {e}"),
                    }
                }
            }
            received = consumer.recv() => {
                match received {
                    Ok(msg) => {
                        state.consumer_health.beat();
                        match EdqsEventMsg::decode(msg.payload.as_ref()) {
                            Ok(event) => match resync_target(&event) {
                                Some(tenant_id) => {
                                    pending.tenants.insert(tenant_id);
                                    pending.drain(state).await;
                                }
                                None => state.apply_event(&event),
                            },
                            Err(e) => tracing::warn!("undecodable EdqsEventMsg; dropping: {e}"),
                        }
                        if let Err(e) = consumer.commit(&msg).await {
                            consecutive_errors += 1;
                            tracing::error!(consecutive_errors, "edqs consumer commit failed: {e}");
                            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                return made_progress;
                            }
                        } else {
                            consecutive_errors = 0;
                            made_progress = true;
                        }
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        let reconnect = should_reconnect(&e, consecutive_errors);
                        tracing::error!(consecutive_errors, reconnect, "edqs consumer receive failed: {e}");
                        if reconnect {
                            return made_progress;
                        }
                    }
                }
            }
            _ = heartbeat.tick() => {
                state.consumer_health.beat();
            }
        }
    }
}

async fn run_consumer(
    brokers: String,
    group_id: String,
    state: EdqsState,
    mut consumer: StreamQueueConsumer,
) {
    let mut backoff = ReconnectBackoff::default();
    let mut pending = PendingResync::default();
    loop {
        if consume_until_reconnect(&mut consumer, &state, &mut pending).await {
            backoff.reset();
        }
        state.consumer_health.disconnect();
        loop {
            let delay = backoff.next_delay();
            tracing::warn!(?delay, "reconnecting edqs consumer");
            tokio::time::sleep(delay).await;
            match new_consumer(&brokers, &group_id) {
                Ok(reconnected) => {
                    consumer = reconnected;
                    state.consumer_health.beat();
                    tracing::info!(group_id, "edqs consumer reconnected");
                    break;
                }
                Err(e) => tracing::error!("edqs consumer reconnect failed: {e}"),
            }
        }
    }
}

/// 初始化失败返回 Err，供服务入口终止进程；运行期错误由消费任务自动重连。
pub async fn start_consumer_for_group_checked(
    brokers: &str,
    group_id: &str,
    state: EdqsState,
) -> Result<tokio::task::JoinHandle<()>, QueueError> {
    tb_queue::admin::ensure_topic(brokers, TbTopic::EDQS_EVENTS, 10).await?;
    let consumer = new_consumer(brokers, group_id)?;
    state.consumer_health.beat();
    tracing::info!(group_id, "edqs consumer started");
    let brokers = brokers.to_string();
    let group_id = group_id.to_string();
    Ok(tokio::spawn(async move {
        run_consumer(brokers, group_id, state, consumer).await;
    }))
}

/// 兼容集成测试的启动入口；测试环境初始化失败时立即 panic。
pub async fn start_consumer_for_group(
    brokers: &str,
    group_id: &str,
    state: EdqsState,
) -> tokio::task::JoinHandle<()> {
    start_consumer_for_group_checked(brokers, group_id, state)
        .await
        .unwrap_or_else(|e| panic!("edqs consumer init failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_delay_is_exponential_and_capped() {
        let mut backoff = ReconnectBackoff::default();
        for expected in [1, 2, 4, 8, 16, 32, 60, 60] {
            assert_eq!(backoff.next_delay(), Duration::from_secs(expected));
        }
        backoff.reset();
        assert_eq!(backoff.next_delay(), INITIAL_RECONNECT_DELAY);
    }

    #[test]
    fn stream_end_reconnects_immediately_and_errors_require_a_streak() {
        let ended = QueueError::Serialization("consumer stream ended".into());
        assert!(should_reconnect(&ended, 1));
        let transient = QueueError::Timeout;
        assert!(!should_reconnect(&transient, MAX_CONSECUTIVE_ERRORS - 1));
        assert!(should_reconnect(&transient, MAX_CONSECUTIVE_ERRORS));
    }
}
