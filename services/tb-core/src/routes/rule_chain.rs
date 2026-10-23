//! /api/ruleChain(s)：规则链与链内节点/边管理（重新设计，非 Java 兼容）。仅 TENANT_ADMIN。

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tb_auth::{ApiError, AuthUser};
use tb_common::{EntityType, TbError};
use tb_dao::models::{Authority, RuleChain, RuleNode};
use tb_dao::{PageData, PageLink};
use uuid::Uuid;

use crate::AppState;
use crate::audit::audit;
use crate::auth_middleware::{check_tenant_scope, require_authority};
use crate::routes::PageQuery;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/ruleChain", post(save_rule_chain))
        .route(
            "/api/ruleChain/{id}",
            get(get_rule_chain).delete(delete_rule_chain),
        )
        .route("/api/ruleChain/{id}/nodes", get(get_nodes).put(put_nodes))
        .route("/api/ruleChains", get(list_rule_chains))
        .route(
            "/api/ruleNode/{ruleNodeId}/events",
            get(get_node_debug_events),
        )
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DebugEventsQuery {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
    pub start_ts: Option<i64>,
    pub end_ts: Option<i64>,
}

/// 规则节点 debug 事件查询（TENANT_ADMIN；租户校验经 rule_node → rule_chain）。
async fn get_node_debug_events(
    State(state): State<AppState>,
    user: AuthUser,
    Path(node_id): Path<Uuid>,
    Query(query): Query<DebugEventsQuery>,
) -> Result<Json<PageData<tb_dao::RuleNodeDebugEvent>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let node = state
        .rule_node_dao
        .find_by_id(node_id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("rule node {node_id}")))?;
    load_chain(&state, &user, node.rule_chain_id).await?;
    let page_link = PageLink::new(query.page.unwrap_or(0), query.page_size.unwrap_or(10));
    Ok(Json(
        state
            .rule_node_debug_event_dao
            .find_by_node(node_id, query.start_ts, query.end_ts, &page_link)
            .await?,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveRuleChainRequest {
    pub id: Option<Uuid>,
    pub name: String,
    pub root: Option<bool>,
    pub debug_mode: Option<bool>,
}

async fn save_rule_chain(
    State(state): State<AppState>,
    user: AuthUser,
    Json(req): Json<SaveRuleChainRequest>,
) -> Result<Json<RuleChain>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let tenant_id = user.0.tenant_id;
    let is_new = req.id.is_none();
    let (id, created_time, first_rule_node_id, existing_root) = match req.id {
        Some(id) => {
            let existing = state
                .rule_chain_dao
                .find_by_id(id)
                .await?
                .ok_or_else(|| TbError::NotFound(format!("rule chain {id}")))?;
            check_tenant_scope(&user, existing.tenant_id)?;
            (
                id,
                existing.created_time,
                existing.first_rule_node_id,
                existing.root,
            )
        }
        None => (Uuid::now_v7(), 0, None, false),
    };
    let chain = RuleChain {
        id,
        created_time,
        tenant_id,
        name: req.name,
        first_rule_node_id,
        root: req.root.unwrap_or(existing_root),
        debug_mode: req.debug_mode.unwrap_or(false),
        configuration: None,
        additional_info: None,
    };
    let saved = state.rule_chain_dao.save_rooted(&chain).await?;
    audit(
        &state,
        &user,
        Some((saved.id, EntityType::RuleChain, saved.name.clone())),
        if is_new { "CREATED" } else { "UPDATED" },
        None,
        "SUCCESS",
    );
    Ok(Json(saved))
}

async fn get_rule_chain(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<RuleChain>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let chain = load_chain(&state, &user, id).await?;
    Ok(Json(chain))
}

async fn delete_rule_chain(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let chain = load_chain(&state, &user, id).await?;
    state.rule_chain_dao.delete_cascade(id).await?;
    audit(
        &state,
        &user,
        Some((chain.id, EntityType::RuleChain, chain.name)),
        "DELETED",
        None,
        "SUCCESS",
    );
    Ok(StatusCode::OK)
}

async fn list_rule_chains(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<PageQuery>,
) -> Result<Json<PageData<RuleChain>>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let page_link: PageLink = query.into();
    Ok(Json(
        state
            .rule_chain_dao
            .find_by_tenant_id(user.0.tenant_id, &page_link)
            .await?,
    ))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuleChainMetadata {
    pub nodes: Vec<RuleNode>,
    pub edges: Vec<EdgeView>,
    pub first_node_id: Option<Uuid>,
}

#[derive(Debug, Serialize)]
pub struct EdgeView {
    pub from: Uuid,
    pub to: Uuid,
    #[serde(rename = "type")]
    pub r#type: String,
}

async fn get_nodes(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
) -> Result<Json<RuleChainMetadata>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    let chain = load_chain(&state, &user, id).await?;
    let nodes = state.rule_node_dao.find_by_rule_chain_id(id).await?;
    let mut edges = Vec::new();
    for node in &nodes {
        for rel in state
            .relation_dao
            .find_by_from(node.id, EntityType::RuleNode)
            .await?
        {
            if rel.relation_type_group == "RULE" {
                edges.push(EdgeView {
                    from: rel.from_id,
                    to: rel.to_id,
                    r#type: rel.relation_type,
                });
            }
        }
    }
    Ok(Json(RuleChainMetadata {
        nodes,
        edges,
        first_node_id: chain.first_rule_node_id,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaveNodesRequest {
    pub nodes: Vec<NodeSpec>,
    #[serde(default)]
    pub edges: Vec<EdgeSpec>,
    pub first_node_index: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSpec {
    pub id: Option<Uuid>,
    #[serde(rename = "type")]
    pub r#type: String,
    pub name: String,
    pub debug_mode: Option<bool>,
    pub configuration: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EdgeSpec {
    pub from_index: usize,
    pub to_index: usize,
    #[serde(rename = "type")]
    pub r#type: String,
}

async fn put_nodes(
    State(state): State<AppState>,
    user: AuthUser,
    Path(id): Path<Uuid>,
    Json(req): Json<SaveNodesRequest>,
) -> Result<Json<RuleChainMetadata>, ApiError> {
    require_authority(&user, &[Authority::TenantAdmin])?;
    load_chain(&state, &user, id).await?;

    for node in &req.nodes {
        if !tb_rule_engine::known_node_types().contains(&node.r#type.as_str()) {
            return Err(
                TbError::InvalidInput(format!("unknown rule node type: {}", node.r#type)).into(),
            );
        }
    }
    for edge in &req.edges {
        if edge.from_index >= req.nodes.len() || edge.to_index >= req.nodes.len() {
            return Err(TbError::InvalidInput(format!(
                "edge endpoint out of range: {} -> {}",
                edge.from_index, edge.to_index
            ))
            .into());
        }
    }
    if let Some(first) = req.first_node_index {
        if first >= req.nodes.len() {
            return Err(
                TbError::InvalidInput(format!("firstNodeIndex out of range: {first}")).into(),
            );
        }
    } else if !req.nodes.is_empty() {
        return Err(TbError::InvalidInput(
            "firstNodeIndex is required when nodes are present".into(),
        )
        .into());
    }

    let nodes: Vec<RuleNode> = req
        .nodes
        .into_iter()
        .map(|n| RuleNode {
            id: n.id.unwrap_or_else(Uuid::now_v7),
            created_time: 0,
            rule_chain_id: id,
            r#type: n.r#type,
            name: n.name,
            debug_mode: n.debug_mode.unwrap_or(false),
            singleton_mode: false,
            queue_name: None,
            configuration: n.configuration,
            additional_info: None,
        })
        .collect();
    let edges: Vec<(Uuid, Uuid, String)> = req
        .edges
        .into_iter()
        .map(|e| (nodes[e.from_index].id, nodes[e.to_index].id, e.r#type))
        .collect();
    let first_node_id = req.first_node_index.map(|i| nodes[i].id);
    state
        .rule_chain_dao
        .save_nodes(id, &nodes, &edges, first_node_id)
        .await?;
    Ok(Json(RuleChainMetadata {
        nodes,
        edges: edges
            .into_iter()
            .map(|(from, to, r#type)| EdgeView { from, to, r#type })
            .collect(),
        first_node_id,
    }))
}

async fn load_chain(state: &AppState, user: &AuthUser, id: Uuid) -> Result<RuleChain, ApiError> {
    let chain = state
        .rule_chain_dao
        .find_by_id(id)
        .await?
        .ok_or_else(|| TbError::NotFound(format!("rule chain {id}")))?;
    check_tenant_scope(user, chain.tenant_id)?;
    Ok(chain)
}

/// POST /api/tenant 创建新租户后调用：自动建默认根规则链。
pub async fn seed_default_root_chain(state: &AppState, tenant_id: Uuid) -> tb_common::TbResult<()> {
    let chain = tb_rule_engine::seed::create_default_root_chain(&state.pool, tenant_id).await?;
    tracing::info!(tenant_id = %tenant_id, chain_id = %chain.id, "seeded default root rule chain");
    Ok(())
}
