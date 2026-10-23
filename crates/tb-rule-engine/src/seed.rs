//! 租户默认根规则链种子：msg_type_switch(first) → save_timeseries / save_attributes。

use sqlx::PgPool;
use tb_common::TbResult;
use tb_common::msg::msg_type;
use tb_dao::RuleChainDao;
use tb_dao::models::{RuleChain, RuleNode};
use uuid::Uuid;

use crate::registry;

pub const ROOT_CHAIN_NAME: &str = "Root Rule Chain";

fn new_node(
    chain_id: Uuid,
    node_type: &str,
    name: &str,
    configuration: serde_json::Value,
) -> RuleNode {
    RuleNode {
        id: Uuid::now_v7(),
        created_time: 0,
        rule_chain_id: chain_id,
        r#type: node_type.into(),
        name: name.into(),
        debug_mode: false,
        singleton_mode: false,
        queue_name: None,
        configuration: Some(configuration),
        additional_info: None,
    }
}

/// 幂等：租户已有根链时直接返回现有链。
pub async fn create_default_root_chain(pool: &PgPool, tenant_id: Uuid) -> TbResult<RuleChain> {
    let dao = RuleChainDao::new(pool.clone());
    if let Some(existing) = dao.find_root_by_tenant(tenant_id).await? {
        return Ok(existing);
    }
    let chain = RuleChain {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        name: ROOT_CHAIN_NAME.into(),
        first_rule_node_id: None,
        root: true,
        debug_mode: false,
        configuration: None,
        additional_info: None,
    };
    let chain = dao.save_rooted(&chain).await?;

    let switch = new_node(
        chain.id,
        registry::MSG_TYPE_SWITCH,
        "Message Type Switch",
        serde_json::json!({}),
    );
    let save_ts = new_node(
        chain.id,
        registry::SAVE_TIMESERIES,
        "Save Timeseries",
        serde_json::json!({}),
    );
    let save_attrs = new_node(
        chain.id,
        registry::SAVE_ATTRIBUTES,
        "Save Attributes",
        serde_json::json!({}),
    );
    dao.save_nodes(
        chain.id,
        &[save_ts.clone(), save_attrs.clone(), switch.clone()],
        &[
            (
                switch.id,
                save_ts.id,
                msg_type::POST_TELEMETRY_REQUEST.to_string(),
            ),
            (
                switch.id,
                save_attrs.id,
                msg_type::POST_ATTRIBUTES_REQUEST.to_string(),
            ),
        ],
        Some(switch.id),
    )
    .await?;
    Ok(chain)
}
