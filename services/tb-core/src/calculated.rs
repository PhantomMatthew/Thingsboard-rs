//! 计算字段接线：遥测/属性保存后异步评估，输出落库由 tb_calculated::evaluate_and_save
//! 完成（含 EDQS 事件）；本模块负责 TELEMETRY 输出的 Redis latest write-through。
//! 全程 fire-and-forget：任何失败仅 warn，不阻塞保存主路径。

use tb_common::{KvEntry, TsKvEntry};
use uuid::Uuid;

use crate::AppState;

pub fn spawn_evaluate(state: &AppState, tenant_id: Uuid, entity_id: Uuid, trigger: Vec<KvEntry>) {
    if trigger.is_empty() {
        return;
    }
    let state = state.clone();
    tokio::spawn(async move {
        let producer = state.queue_producer().await.ok().cloned();
        let outputs = tb_calculated::evaluate_and_save(
            &state.pool,
            producer.as_ref(),
            tenant_id,
            entity_id,
            &trigger,
        )
        .await;
        let ts_entries: Vec<TsKvEntry> = outputs
            .iter()
            .filter(|o| o.output_type == tb_calculated::CfOutputType::Telemetry)
            .map(|o| TsKvEntry {
                ts: o.ts,
                key: o.key.clone(),
                value: o.value.clone(),
            })
            .collect();
        if !ts_entries.is_empty() {
            crate::telemetry_cache::update_latest(&state, entity_id, &ts_entries).await;
        }
    });
}
