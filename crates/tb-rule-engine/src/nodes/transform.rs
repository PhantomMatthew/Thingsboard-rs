//! transform.*：改写消息内容/originator。

use std::collections::HashMap;

use async_trait::async_trait;
use serde::Deserialize;
use tb_common::msg::TbMsg;
use tb_common::{EntityId, EntityType, TbError, TbResult};

use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::parse_config;

fn valid_target(target: &str) -> TbResult<()> {
    match target {
        "msg" | "metadata" => Ok(()),
        other => Err(TbError::InvalidInput(format!(
            "target must be msg|metadata, got {other}"
        ))),
    }
}

/// transform.rename_keys：按 map {old: new} 重命名 msg.data 或 metadata 的 key。
pub struct RenameKeysNode {
    map: HashMap<String, String>,
    target: String,
}

impl RenameKeysNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            map: HashMap<String, String>,
            #[serde(default = "default_target")]
            target: String,
        }
        fn default_target() -> String {
            "msg".into()
        }
        let c: Config = parse_config(config)?;
        valid_target(&c.target)?;
        Ok(Self {
            map: c.map,
            target: c.target,
        })
    }
}

#[async_trait]
impl Node for RenameKeysNode {
    async fn on_msg(&self, _ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        match self.target.as_str() {
            "msg" => {
                if let Some(obj) = msg.data.as_object_mut() {
                    for (old, new) in &self.map {
                        if let Some(v) = obj.remove(old) {
                            obj.insert(new.clone(), v);
                        }
                    }
                }
            }
            _ => {
                for (old, new) in &self.map {
                    if let Some(v) = msg.metadata.remove(old) {
                        msg.metadata.insert(new.clone(), v);
                    }
                }
            }
        }
        vec![("Success".into(), msg)]
    }
}

/// transform.delete_keys：删除 msg.data 或 metadata 中指定 key。
pub struct DeleteKeysNode {
    keys: Vec<String>,
    target: String,
}

impl DeleteKeysNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            keys: Vec<String>,
            #[serde(default = "default_target")]
            target: String,
        }
        fn default_target() -> String {
            "msg".into()
        }
        let c: Config = parse_config(config)?;
        valid_target(&c.target)?;
        Ok(Self {
            keys: c.keys,
            target: c.target,
        })
    }
}

#[async_trait]
impl Node for DeleteKeysNode {
    async fn on_msg(&self, _ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        match self.target.as_str() {
            "msg" => {
                if let Some(obj) = msg.data.as_object_mut() {
                    for k in &self.keys {
                        obj.remove(k);
                    }
                }
            }
            _ => {
                for k in &self.keys {
                    msg.metadata.remove(k);
                }
            }
        }
        vec![("Success".into(), msg)]
    }
}

/// transform.change_originator：换 originator；原 originator 记入
/// metadata.prevOriginatorId / prevOriginatorType。
pub struct ChangeOriginatorNode {
    to: ChangeTarget,
    relation_type: Option<String>,
    direction: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ChangeTarget {
    Customer,
    Tenant,
    Related,
}

impl ChangeOriginatorNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            to: ChangeTarget,
            relation_type: Option<String>,
            #[serde(default = "default_direction")]
            direction: String,
        }
        fn default_direction() -> String {
            "FROM".into()
        }
        let c: Config = parse_config(config)?;
        if c.direction != "FROM" && c.direction != "TO" {
            return Err(TbError::InvalidInput(format!(
                "direction must be FROM|TO, got {}",
                c.direction
            )));
        }
        Ok(Self {
            to: c.to,
            relation_type: c.relation_type,
            direction: c.direction,
        })
    }

    async fn resolve(&self, ctx: &NodeContext, msg: &TbMsg) -> TbResult<Option<EntityId>> {
        match self.to {
            ChangeTarget::Customer => Ok(msg.customer_id.map(|id| EntityId {
                entity_type: EntityType::Customer,
                id,
            })),
            ChangeTarget::Tenant => Ok(Some(EntityId {
                entity_type: EntityType::Tenant,
                id: msg.tenant_id,
            })),
            ChangeTarget::Related => {
                let rels = if self.direction == "FROM" {
                    ctx.relation_dao
                        .find_by_from(msg.originator.id, msg.originator.entity_type)
                        .await?
                } else {
                    ctx.relation_dao
                        .find_by_to(msg.originator.id, msg.originator.entity_type)
                        .await?
                };
                Ok(rels
                    .into_iter()
                    .filter(|r| {
                        r.relation_type_group == "COMMON"
                            && self
                                .relation_type
                                .as_deref()
                                .is_none_or(|t| t == r.relation_type)
                    })
                    .map(|r| {
                        if self.direction == "FROM" {
                            EntityId {
                                entity_type: r.to_type,
                                id: r.to_id,
                            }
                        } else {
                            EntityId {
                                entity_type: r.from_type,
                                id: r.from_id,
                            }
                        }
                    })
                    .next())
            }
        }
    }
}

#[async_trait]
impl Node for ChangeOriginatorNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        match self.resolve(ctx, &msg).await {
            Ok(Some(new_originator)) => {
                msg.metadata
                    .insert("prevOriginatorId".into(), msg.originator.id.to_string());
                msg.metadata.insert(
                    "prevOriginatorType".into(),
                    msg.originator.entity_type.as_str().into(),
                );
                msg.originator = new_originator;
                vec![("Success".into(), msg)]
            }
            Ok(None) => {
                tracing::debug!("change_originator: target entity not found");
                vec![("Failure".into(), msg)]
            }
            Err(e) => {
                tracing::warn!("change_originator failed: {e}");
                vec![("Failure".into(), msg)]
            }
        }
    }
}
