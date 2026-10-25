//! 规则引擎服务：租户根链解析（moka 缓存 TTL 30s）+ 无根链时的内置默认链。

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::{TbError, TbResult};
use tb_dao::RuleChainDao;
use tb_queue::KafkaQueueProducer;
use uuid::Uuid;

use crate::chain::{self, ChainLoader, RuleChainState};
use crate::context::NodeContext;
use crate::nodes::SaveAttributesNode;

const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);

pub struct RuleEngineService {
    ctx: NodeContext,
    loader: ChainLoader,
    chain_dao: RuleChainDao,
    /// tenant_id → 根链执行状态（None 表示该租户无根链，走内置默认链）。
    chain_cache: moka::future::Cache<Uuid, Option<Arc<RuleChainState>>>,
    /// chain_id → 链执行状态（checkpoint 恢复入口按 chain_id 加载）。
    chain_by_id: moka::future::Cache<Uuid, Option<Arc<RuleChainState>>>,
}

impl RuleEngineService {
    pub fn new(pool: PgPool, kafka_producer: KafkaQueueProducer) -> Self {
        Self::with_cache_ttl(pool, kafka_producer, DEFAULT_CACHE_TTL)
    }

    pub fn with_cache_ttl(
        pool: PgPool,
        kafka_producer: KafkaQueueProducer,
        cache_ttl: Duration,
    ) -> Self {
        let ctx = NodeContext::new(pool.clone(), kafka_producer);
        Self::build(pool, ctx, cache_ttl)
    }

    pub fn with_cache_ttl_and_redis(
        pool: PgPool,
        kafka_producer: KafkaQueueProducer,
        cache_ttl: Duration,
        redis_manager: redis::aio::ConnectionManager,
    ) -> Self {
        let ctx = NodeContext::new(pool.clone(), kafka_producer).with_redis_manager(redis_manager);
        Self::build(pool, ctx, cache_ttl)
    }

    pub fn with_queue_producer(
        pool: PgPool,
        producer: Arc<dyn tb_queue::QueueProducer>,
        cache_ttl: Duration,
    ) -> Self {
        let ctx = NodeContext::with_queue_producer(pool.clone(), producer);
        Self::build(pool, ctx, cache_ttl)
    }

    fn build(pool: PgPool, ctx: NodeContext, cache_ttl: Duration) -> Self {
        Self {
            ctx,
            loader: ChainLoader::new(pool.clone()),
            chain_dao: RuleChainDao::new(pool),
            chain_cache: moka::future::Cache::builder()
                .time_to_live(cache_ttl)
                .build(),
            chain_by_id: moka::future::Cache::builder()
                .time_to_live(cache_ttl)
                .build(),
        }
    }

    pub async fn handle(&self, msg: TbMsg) -> TbResult<()> {
        match self.root_chain(msg.tenant_id).await? {
            Some(state) => {
                if let Some(first) = state.first_node_id {
                    chain::execute(&self.ctx, &state, first, msg).await?;
                } else {
                    tracing::debug!(tenant_id = %msg.tenant_id, "root chain has no first node, dropped");
                }
            }
            None => self.default_chain(msg).await?,
        }
        Ok(())
    }

    /// checkpoint/delay 恢复入口：从指定链的指定节点继续执行。
    pub async fn handle_at(&self, msg: TbMsg, chain_id: Uuid, node_id: Uuid) -> TbResult<()> {
        match self.chain_by_id(chain_id).await? {
            Some(state) => {
                if state.nodes.contains_key(&node_id) {
                    chain::execute(&self.ctx, &state, node_id, msg).await?;
                } else {
                    tracing::warn!(%chain_id, %node_id, "checkpoint target node not in chain, dropped");
                }
            }
            None => {
                tracing::warn!(%chain_id, "checkpoint target chain not found, dropped");
            }
        }
        Ok(())
    }

    /// 扫描并处理一批到期或需要恢复的延迟任务。
    pub async fn process_due_delays(&self, owner: &str, limit: i64) -> TbResult<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let tasks = self
            .ctx
            .delay_dao
            .claim_due(owner, now + 30_000, now, limit)
            .await?;
        for task in tasks {
            if let Err(e) = self.process_delay_task(task, owner).await {
                tracing::warn!(error = %e, owner, "process delayed message failed");
            }
        }
        Ok(())
    }

    async fn process_delay_task(&self, task: tb_dao::DelayTask, owner: &str) -> TbResult<()> {
        let now = chrono::Utc::now().timestamp_millis();
        if !self
            .ctx
            .delay_dao
            .renew_lease(task.id, owner, now, now + 30_000)
            .await?
        {
            return Ok(());
        }
        match chain::requeue(
            &self.ctx.kafka_producer,
            task.rule_chain_id,
            task.next_node_id,
            task.msg.clone(),
        )
        .await
        {
            Ok(()) => {
                self.ctx.delay_dao.mark_done(task.id, owner, now).await?;
            }
            Err(error) if task.retry_count < task.max_retries => {
                self.ctx
                    .delay_dao
                    .record_retry(task.id, owner, now, now + 10_000, &error.to_string())
                    .await?;
            }
            Err(error) => {
                let error = format!("delayed requeue failed: {error}");
                match chain::send_dlq(
                    &self.ctx.kafka_producer,
                    task.rule_chain_id,
                    task.rule_node_id,
                    task.msg.clone(),
                    error.clone(),
                )
                .await
                {
                    Ok(()) => {
                        self.ctx
                            .delay_dao
                            .mark_dead(task.id, owner, now, &error)
                            .await?;
                    }
                    Err(dlq_error) => {
                        self.ctx
                            .delay_dao
                            .record_retry(
                                task.id,
                                owner,
                                now,
                                now + 10_000,
                                &format!("{error}; DLQ failed: {dlq_error}"),
                            )
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// 扫描并处理一批到期或需要恢复的去重窗口。
    pub async fn process_due_windows(&self, owner: &str, limit: i64) -> TbResult<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let lease_until = now + 30_000;
        let windows = self
            .ctx
            .dedup_dao
            .claim_due(owner, lease_until, now, limit)
            .await?;
        for window in windows {
            if let Err(e) = self.process_dedup_window(window, owner).await {
                tracing::warn!(error = %e, owner, "process deduplication window failed");
            }
        }
        Ok(())
    }

    async fn process_dedup_window(&self, window: tb_dao::DedupWindow, owner: &str) -> TbResult<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let window = if window.output_msg.is_some() {
            window
        } else {
            let messages = self.ctx.dedup_dao.find_messages(window.id).await?;
            let output = build_dedup_output(&window, messages, now)?;
            self.ctx
                .dedup_dao
                .prepare_output(window.id, owner, &output, now)
                .await?
                .ok_or_else(|| {
                    TbError::Conflict("deduplication window lease lost before output".into())
                })?
        };
        let Some(output) = window.output_msg.clone() else {
            return Err(TbError::Internal(
                "deduplication output was not persisted".into(),
            ));
        };
        let Some(state) = self.chain_by_id(window.rule_chain_id).await? else {
            tracing::warn!(window_id = %window.id, chain_id = %window.rule_chain_id, "deduplication rule chain deleted; closing window");
            self.ctx.dedup_dao.mark_done(window.id, owner, now).await?;
            return Ok(());
        };
        let exhausted = window.retry_count > window.max_retries;
        let relation = if exhausted { "Failure" } else { "Success" };
        match chain::execute_from_relation(&self.ctx, &state, window.rule_node_id, relation, output)
            .await
        {
            Ok(()) => {
                self.ctx.dedup_dao.mark_done(window.id, owner, now).await?;
            }
            Err(e) if !exhausted => {
                self.ctx
                    .dedup_dao
                    .record_retry(window.id, owner, now, now + 10_000)
                    .await?;
                tracing::warn!(window_id = %window.id, error = %e, "deduplication output failed; retry scheduled");
            }
            Err(e) => {
                self.ctx
                    .dedup_dao
                    .record_retry(window.id, owner, now, now + 10_000)
                    .await?;
                return Err(e);
            }
        }
        Ok(())
    }

    async fn chain_by_id(&self, chain_id: Uuid) -> TbResult<Option<Arc<RuleChainState>>> {
        if let Some(cached) = self.chain_by_id.get(&chain_id).await {
            return Ok(cached);
        }
        let loaded = self.loader.load(chain_id).await?.map(Arc::new);
        self.chain_by_id.insert(chain_id, loaded.clone()).await;
        Ok(loaded)
    }

    async fn root_chain(&self, tenant_id: Uuid) -> TbResult<Option<Arc<RuleChainState>>> {
        if let Some(cached) = self.chain_cache.get(&tenant_id).await {
            return Ok(cached);
        }
        let loaded = match self.chain_dao.find_root_by_tenant(tenant_id).await? {
            Some(chain) => self.loader.load(chain.id).await?.map(Arc::new),
            None => None,
        };
        self.chain_cache.insert(tenant_id, loaded.clone()).await;
        Ok(loaded)
    }

    /// 内置默认链（租户无根链时）：对齐 msg_type_switch 语义。
    /// 默认链没有 Failure 出边，保存失败作为处理错误返回，由消费端决定重试或进入 DLQ。
    async fn default_chain(&self, msg: TbMsg) -> TbResult<()> {
        match msg.msg_type.as_str() {
            msg_type::POST_TELEMETRY_REQUEST => {
                chain::report_execution(&self.ctx, &msg).await;
                crate::nodes::save_timeseries(&self.ctx, &msg).await
            }
            msg_type::POST_ATTRIBUTES_REQUEST => {
                chain::report_execution(&self.ctx, &msg).await;
                SaveAttributesNode::default().save(&self.ctx, &msg).await
            }
            other => {
                tracing::debug!(msg_type = other, "default chain: message dropped");
                Ok(())
            }
        }
    }
}

fn build_dedup_output(
    window: &tb_dao::DedupWindow,
    messages: Vec<TbMsg>,
    now: i64,
) -> TbResult<TbMsg> {
    if messages.is_empty() {
        return Err(TbError::Internal(format!(
            "deduplication window {} has no messages",
            window.id
        )));
    }
    match window.strategy {
        tb_dao::DedupStrategy::First | tb_dao::DedupStrategy::Last => {
            let mut messages = messages.into_iter();
            let mut selected = messages
                .next()
                .ok_or_else(|| TbError::Internal("empty FIRST/LAST window".into()))?;
            for msg in messages {
                let replace = match window.strategy {
                    tb_dao::DedupStrategy::First => msg.ts < selected.ts,
                    tb_dao::DedupStrategy::Last => msg.ts > selected.ts,
                    tb_dao::DedupStrategy::All => unreachable!(),
                };
                if replace {
                    selected = msg;
                }
            }
            selected.id = Uuid::now_v7();
            Ok(selected)
        }
        tb_dao::DedupStrategy::All => {
            let first = &messages[0];
            let data = serde_json::Value::Array(
                messages
                    .iter()
                    .map(|msg| {
                        serde_json::json!({
                            "msg": msg.data,
                            "metadata": msg.metadata,
                        })
                    })
                    .collect(),
            );
            let mut metadata = std::collections::HashMap::new();
            metadata.insert("ts".into(), now.to_string());
            Ok(TbMsg {
                id: Uuid::now_v7(),
                queue_name: window.queue_name.clone(),
                msg_type: if window.out_msg_type.is_empty() {
                    return Err(TbError::Internal(
                        "ALL deduplication window has no out_msg_type".into(),
                    ));
                } else {
                    window.out_msg_type.clone()
                },
                originator: first.originator,
                tenant_id: window.tenant_id,
                customer_id: first.customer_id,
                rule_chain_id: Some(window.rule_chain_id),
                rule_node_id: Some(window.rule_node_id),
                data,
                metadata,
                ts: now,
            })
        }
    }
}
