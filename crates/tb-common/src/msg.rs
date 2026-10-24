//! 规则引擎消息类型常量与基础结构。

use crate::entity::EntityId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub mod msg_type {
    pub const POST_TELEMETRY_REQUEST: &str = "POST_TELEMETRY_REQUEST";
    pub const POST_ATTRIBUTES_REQUEST: &str = "POST_ATTRIBUTES_REQUEST";
    pub const ATTRIBUTES_UPDATED: &str = "ATTRIBUTES_UPDATED";
    pub const ATTRIBUTES_DELETED: &str = "ATTRIBUTES_DELETED";
    pub const TO_SERVER_RPC_REQUEST: &str = "TO_SERVER_RPC_REQUEST";
    pub const CONNECT_EVENT: &str = "CONNECT_EVENT";
    pub const DISCONNECT_EVENT: &str = "DISCONNECT_EVENT";
    pub const ACTIVITY_EVENT: &str = "ACTIVITY_EVENT";
    pub const INACTIVITY_EVENT: &str = "INACTIVITY_EVENT";
    pub const ALARM: &str = "ALARM";
}

/// 规则引擎中流动的消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TbMsg {
    pub id: Uuid,
    pub queue_name: String,
    pub msg_type: String,
    pub originator: EntityId,
    pub tenant_id: Uuid,
    pub customer_id: Option<Uuid>,
    #[serde(default)]
    pub rule_chain_id: Option<Uuid>,
    #[serde(default)]
    pub rule_node_id: Option<Uuid>,
    pub data: serde_json::Value,
    pub metadata: HashMap<String, String>,
    pub ts: i64,
}

impl TbMsg {
    pub fn new(
        msg_type: impl Into<String>,
        originator: EntityId,
        tenant_id: Uuid,
        data: serde_json::Value,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            queue_name: "Main".to_string(),
            msg_type: msg_type.into(),
            originator,
            tenant_id,
            customer_id: None,
            rule_chain_id: None,
            rule_node_id: None,
            data,
            metadata,
            ts: chrono::Utc::now().timestamp_millis(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn old_json_without_checkpoint_ids_is_supported() {
        let id = Uuid::now_v7();
        let tenant_id = Uuid::now_v7();
        let originator_id = Uuid::now_v7();
        let msg: TbMsg = serde_json::from_value(json!({
            "id": id,
            "queue_name": "Main",
            "msg_type": "POST_TELEMETRY_REQUEST",
            "originator": {"entity_type": "DEVICE", "id": originator_id},
            "tenant_id": tenant_id,
            "customer_id": null,
            "data": {"temperature": 20},
            "metadata": {},
            "ts": 1
        }))
        .unwrap();

        assert_eq!(msg.rule_chain_id, None);
        assert_eq!(msg.rule_node_id, None);
    }
}
