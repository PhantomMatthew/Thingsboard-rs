//! EDQS 事件发布：编码 EdqsEventMsg 发往 `tb_edqs.events`，key = tenant_id。

use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use tb_proto::edqs::{EdqsEventMsg, TenantResyncMsg, edqs_event_msg::Event};
use uuid::Uuid;

use crate::producer::{KafkaQueueProducer, QueueProducer};
use crate::{QueueError, TbTopic};

const SEND_ATTEMPTS: u32 = 3;
const RETRY_BASE_DELAY: Duration = Duration::from_millis(200);

pub async fn publish_event(
    producer: &KafkaQueueProducer,
    tenant_id: Uuid,
    event: &EdqsEventMsg,
) -> Result<(), QueueError> {
    send(producer, tenant_id, event).await
}

async fn send<P: QueueProducer>(
    producer: &P,
    tenant_id: Uuid,
    event: &EdqsEventMsg,
) -> Result<(), QueueError> {
    producer
        .send(
            TbTopic::EDQS_EVENTS,
            &tenant_id.to_string(),
            Bytes::from(event.encode_to_vec()),
        )
        .await
}

pub fn tenant_resync_event(tenant_id: Uuid) -> EdqsEventMsg {
    EdqsEventMsg {
        tenant_id: tenant_id.to_string(),
        event: Some(Event::TenantResync(TenantResyncMsg {
            tenant_id: tenant_id.to_string(),
        })),
    }
}

/// 有界退避重试；仍失败时发布租户重同步标记，让 EDQS 从数据库重载该租户。
/// 返回 Err 表示标记也未送达，只能等待 EDQS 定期对账。
pub async fn publish_event_or_resync<P: QueueProducer>(
    producer: &P,
    tenant_id: Uuid,
    event: &EdqsEventMsg,
) -> Result<(), QueueError> {
    let mut last_error = None;
    for attempt in 0..SEND_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(RETRY_BASE_DELAY * (1 << (attempt - 1))).await;
        }
        match send(producer, tenant_id, event).await {
            Ok(()) => return Ok(()),
            Err(e) => last_error = Some(e),
        }
    }
    if let Some(e) = last_error {
        tracing::warn!(%tenant_id, "edqs event publish failed, requesting tenant resync: {e}");
    }
    send(producer, tenant_id, &tenant_resync_event(tenant_id)).await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;

    /// 前 `failures` 次发送失败，记录成功送达的事件。
    struct FlakyProducer {
        failures: Mutex<u32>,
        sent: Mutex<Vec<EdqsEventMsg>>,
    }

    impl FlakyProducer {
        fn new(failures: u32) -> Self {
            Self {
                failures: Mutex::new(failures),
                sent: Mutex::new(vec![]),
            }
        }
    }

    #[async_trait]
    impl QueueProducer for FlakyProducer {
        async fn send(&self, topic: &str, key: &str, payload: Bytes) -> Result<(), QueueError> {
            assert_eq!(topic, TbTopic::EDQS_EVENTS);
            let mut failures = self.failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                return Err(QueueError::Timeout);
            }
            let event = EdqsEventMsg::decode(payload).unwrap();
            assert_eq!(key, event.tenant_id);
            self.sent.lock().unwrap().push(event);
            Ok(())
        }
    }

    fn sample_event(tenant_id: Uuid) -> EdqsEventMsg {
        EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(Event::TenantDelete(tb_proto::edqs::TenantDeleteMsg {
                tenant_id: tenant_id.to_string(),
            })),
        }
    }

    #[tokio::test]
    async fn transient_failures_are_retried() {
        let tenant_id = Uuid::now_v7();
        let producer = FlakyProducer::new(SEND_ATTEMPTS - 1);
        publish_event_or_resync(&producer, tenant_id, &sample_event(tenant_id))
            .await
            .unwrap();
        assert_eq!(
            *producer.sent.lock().unwrap(),
            vec![sample_event(tenant_id)]
        );
    }

    #[tokio::test]
    async fn exhausted_retries_fall_back_to_tenant_resync() {
        let tenant_id = Uuid::now_v7();
        let producer = FlakyProducer::new(SEND_ATTEMPTS);
        publish_event_or_resync(&producer, tenant_id, &sample_event(tenant_id))
            .await
            .unwrap();
        assert_eq!(
            *producer.sent.lock().unwrap(),
            vec![tenant_resync_event(tenant_id)]
        );

        let producer = FlakyProducer::new(SEND_ATTEMPTS + 1);
        assert!(
            publish_event_or_resync(&producer, tenant_id, &sample_event(tenant_id))
                .await
                .is_err()
        );
    }
}
