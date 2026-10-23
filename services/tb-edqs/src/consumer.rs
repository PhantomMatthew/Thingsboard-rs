//! Kafka 事件消费：group `tb-edqs`，从 earliest 起消费（快照加载期间的事件重放，upsert 幂等）。

use prost::Message as _;
use tb_proto::edqs::EdqsEventMsg;
use tb_queue::{QueueConsumer, StreamQueueConsumer, TbTopic};

use crate::state::EdqsState;

pub const DEFAULT_GROUP: &str = "tb-edqs";

/// group_id 可定制：集成测试用独立 group 避免共享 offset 互相干扰。
pub async fn start_consumer_for_group(
    brokers: &str,
    group_id: &str,
    state: EdqsState,
) -> tokio::task::JoinHandle<()> {
    if let Err(e) = tb_queue::admin::ensure_topic(brokers, TbTopic::EDQS_EVENTS, 10).await {
        tracing::warn!("ensure edqs topic failed: {e}");
    }
    let brokers = brokers.to_string();
    let group_id = group_id.to_string();
    tokio::spawn(async move {
        let mut consumer = match StreamQueueConsumer::new_from_beginning(
            &brokers,
            &group_id,
            &[TbTopic::EDQS_EVENTS],
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("edqs consumer init failed: {e}");
                return;
            }
        };
        tracing::info!(group_id, "edqs consumer started");
        loop {
            match consumer.recv().await {
                Ok(msg) => match EdqsEventMsg::decode(msg.payload) {
                    Ok(event) => state.apply_event(&event),
                    Err(e) => tracing::warn!("undecodable EdqsEventMsg: {e}"),
                },
                Err(e) => {
                    tracing::error!("edqs consumer error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    })
}
