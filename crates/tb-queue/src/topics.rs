//! 主题命名。

pub struct TbTopic;

impl TbTopic {
    pub const TRANSPORT_API_REQUESTS: &'static str = "tb_transport.api.requests";
    pub const CORE: &'static str = "tb_core";
    pub const CORE_NOTIFICATIONS: &'static str = "tb_core.notifications";
    pub const HOUSEKEEPER: &'static str = "tb_housekeeper";

    pub fn transport_api_responses(node_id: &str) -> String {
        format!("tb_transport.api.responses.{node_id}")
    }

    pub fn transport_notifications(node_id: &str) -> String {
        format!("tb_transport.notifications.{node_id}")
    }

    pub fn core_rpc_responses(node_id: &str) -> String {
        format!("tb_core.rpc.responses.{node_id}")
    }

    pub fn rule_engine(queue_name: &str) -> String {
        format!("tb_rule_engine.{queue_name}")
    }

    pub const RULE_ENGINE_NOTIFICATIONS: &'static str = "tb_rule_engine.notifications";
    pub const RULE_ENGINE_DLQ: &'static str = "tb_rule_engine.dlq";

    /// EDQS 读模型事件流，message key = tenant_id（按租户分区保序）。
    pub const EDQS_EVENTS: &'static str = "tb_edqs.events";
}
