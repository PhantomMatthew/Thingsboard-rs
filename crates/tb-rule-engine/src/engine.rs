//! 规则引擎服务：租户根链解析（moka 缓存 TTL 30s）+ 无根链时的内置默认链。

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tb_common::msg::{TbMsg, msg_type};
use tb_common::TbResult;
use tb_dao::RuleChainDao;
use tb_queue::KafkaQueueProducer;
use uuid::Uuid;

use crate::chain::{self, ChainLoader, RuleChainState};
use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::{SaveAttributesNode, SaveTimeseriesNode};

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
        Self {
            ctx: NodeContext::new(pool.clone(), kafka_producer),
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
                    chain::execute(&self.ctx, &state, first, msg).await;
                } else {
                    tracing::debug!(tenant_id = %msg.tenant_id, "root chain has no first node, dropped");
                }
            }
            None => self.default_chain(msg).await,
        }
        Ok(())
    }

    /// checkpoint 恢复入口：从指定链的指定节点继续执行。
    pub async fn handle_at(&self, msg: TbMsg, chain_id: Uuid, node_id: Uuid) -> TbResult<()> {
        match self.chain_by_id(chain_id).await? {
            Some(state) => {
                if state.nodes.contains_key(&node_id) {
                    chain::execute(&self.ctx, &state, node_id, msg).await;
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
    async fn default_chain(&self, msg: TbMsg) {
        match msg.msg_type.as_str() {
            msg_type::POST_TELEMETRY_REQUEST => {
                SaveTimeseriesNode::default().on_msg(&self.ctx, msg).await;
            }
            msg_type::POST_ATTRIBUTES_REQUEST => {
                SaveAttributesNode::default().on_msg(&self.ctx, msg).await;
            }
            other => {
                tracing::debug!(msg_type = other, "default chain: message dropped");
            }
        }
    }
}
