//! enrichment.*：查询实体/属性/关系丰富消息内容。

use async_trait::async_trait;
use serde::Deserialize;
use tb_common::msg::TbMsg;
use tb_common::{AttributeScope, EntityId, EntityType, TbError, TbResult};

use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::{kv_to_json, merge_attrs, parse_config, parse_scope};

fn merge_or_failure(
    msg: &mut TbMsg,
    target: &str,
    prefix: Option<&str>,
    attrs: Vec<(String, serde_json::Value)>,
) -> String {
    match merge_attrs(msg, target, prefix, attrs) {
        Ok(()) => "Success".into(),
        Err(e) => {
            tracing::warn!("enrichment merge failed: {e}");
            "Failure".into()
        }
    }
}

/// enrichment.originator_attributes：查 originator 的属性合并进 msg.data 或 metadata。
pub struct OriginatorAttributesNode {
    client_keys: Vec<String>,
    server_keys: Vec<String>,
    shared_keys: Vec<String>,
    target: String,
    prefix: Option<String>,
}

impl OriginatorAttributesNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize, Default)]
        struct Config {
            #[serde(default)]
            client_keys: Vec<String>,
            #[serde(default)]
            server_keys: Vec<String>,
            #[serde(default)]
            shared_keys: Vec<String>,
            #[serde(default = "default_target")]
            target: String,
            prefix: Option<String>,
        }
        fn default_target() -> String {
            "metadata".into()
        }
        let c: Config = parse_config(config)?;
        if c.target != "msg" && c.target != "metadata" {
            return Err(TbError::InvalidInput(format!(
                "target must be msg|metadata, got {}",
                c.target
            )));
        }
        Ok(Self {
            client_keys: c.client_keys,
            server_keys: c.server_keys,
            shared_keys: c.shared_keys,
            target: c.target,
            prefix: c.prefix,
        })
    }
}

#[async_trait]
impl Node for OriginatorAttributesNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        let scopes = [
            (AttributeScope::ClientScope, &self.client_keys),
            (AttributeScope::ServerScope, &self.server_keys),
            (AttributeScope::SharedScope, &self.shared_keys),
        ];
        let mut attrs = Vec::new();
        for (scope, keys) in scopes {
            if keys.is_empty() {
                continue;
            }
            match ctx
                .attributes_dao
                .find(msg.originator.id, scope, Some(keys))
                .await
            {
                Ok(entries) => attrs.extend(
                    entries
                        .iter()
                        .map(|e| (e.key.clone(), kv_to_json(&e.value))),
                ),
                Err(e) => {
                    tracing::warn!("originator_attributes query failed: {e}");
                    return vec![("Failure".into(), msg)];
                }
            }
        }
        let rel = merge_or_failure(&mut msg, &self.target, self.prefix.as_deref(), attrs);
        vec![(rel, msg)]
    }
}

/// enrichment.originator_fields：originator 实体的 name/type/label 进 metadata。
pub struct OriginatorFieldsNode {
    fields: Vec<String>,
}

impl OriginatorFieldsNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            fields: Vec<String>,
        }
        Ok(Self {
            fields: parse_config::<Config>(config)?.fields,
        })
    }
}

#[async_trait]
impl Node for OriginatorFieldsNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        let found: TbResult<Option<Vec<(String, String)>>> = async {
            match msg.originator.entity_type {
                EntityType::Device => {
                    let d = ctx.device_dao.find_by_id(msg.originator.id).await?;
                    Ok(d.map(|d| {
                        let mut out = vec![("name".to_string(), d.name)];
                        if let Some(t) = d.r#type {
                            out.push(("type".to_string(), t));
                        }
                        if let Some(l) = d.label {
                            out.push(("label".to_string(), l));
                        }
                        out
                    }))
                }
                EntityType::Asset => {
                    let a = ctx.asset_dao.find_by_id(msg.originator.id).await?;
                    Ok(a.map(|a| {
                        let mut out = vec![("name".to_string(), a.name)];
                        if let Some(l) = a.label {
                            out.push(("label".to_string(), l));
                        }
                        out
                    }))
                }
                other => Err(TbError::InvalidInput(format!(
                    "originator_fields supports DEVICE/ASSET originators, got {other}"
                ))),
            }
        }
        .await;
        match found {
            Ok(Some(fields)) => {
                for (k, v) in fields {
                    if self.fields.contains(&k) {
                        msg.metadata.insert(k, v);
                    }
                }
                vec![("Success".into(), msg)]
            }
            Ok(None) => {
                tracing::warn!(originator = %msg.originator, "originator_fields: entity not found");
                vec![("Failure".into(), msg)]
            }
            Err(e) => {
                tracing::warn!("originator_fields failed: {e}");
                vec![("Failure".into(), msg)]
            }
        }
    }
}

/// enrichment.related_attributes：relation 表查单层关联实体，取第一个匹配实体的属性。
pub struct RelatedAttributesNode {
    relation_type: String,
    direction: String,
    entity_types: Vec<String>,
    attr_keys: Vec<String>,
    scope: String,
    target: String,
    prefix: Option<String>,
}

impl RelatedAttributesNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            relation_type: String,
            #[serde(default = "default_direction")]
            direction: String,
            #[serde(default)]
            entity_types: Vec<String>,
            #[serde(default)]
            attr_keys: Vec<String>,
            #[serde(default = "default_scope")]
            scope: String,
            #[serde(default = "default_target")]
            target: String,
            prefix: Option<String>,
        }
        fn default_direction() -> String {
            "FROM".into()
        }
        fn default_scope() -> String {
            "SERVER_SCOPE".into()
        }
        fn default_target() -> String {
            "metadata".into()
        }
        let c: Config = parse_config(config)?;
        if c.direction != "FROM" && c.direction != "TO" {
            return Err(TbError::InvalidInput(format!(
                "direction must be FROM|TO, got {}",
                c.direction
            )));
        }
        parse_scope(&c.scope)?;
        Ok(Self {
            relation_type: c.relation_type,
            direction: c.direction,
            entity_types: c.entity_types,
            attr_keys: c.attr_keys,
            scope: c.scope,
            target: c.target,
            prefix: c.prefix,
        })
    }

    /// 第一个匹配的关联实体（relation_type_group=COMMON）。
    async fn first_related(
        &self,
        ctx: &NodeContext,
        originator: &EntityId,
    ) -> TbResult<Option<EntityId>> {
        let rels = if self.direction == "FROM" {
            ctx.relation_dao
                .find_by_from(originator.id, originator.entity_type)
                .await?
        } else {
            ctx.relation_dao
                .find_by_to(originator.id, originator.entity_type)
                .await?
        };
        Ok(rels
            .into_iter()
            .filter(|r| r.relation_type_group == "COMMON" && r.relation_type == self.relation_type)
            .filter_map(|r| {
                let (id, ty) = if self.direction == "FROM" {
                    (r.to_id, r.to_type)
                } else {
                    (r.from_id, r.from_type)
                };
                (self.entity_types.is_empty()
                    || self.entity_types.contains(&ty.as_str().to_string()))
                .then_some(EntityId {
                    entity_type: ty,
                    id,
                })
            })
            .next())
    }
}

#[async_trait]
impl Node for RelatedAttributesNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        let related = match self.first_related(ctx, &msg.originator).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("related_attributes relation query failed: {e}");
                return vec![("Failure".into(), msg)];
            }
        };
        let Some(entity) = related else {
            tracing::debug!("related_attributes: no matching relation");
            return vec![("Failure".into(), msg)];
        };
        let keys = (!self.attr_keys.is_empty()).then_some(self.attr_keys.as_slice());
        let scope = match parse_scope(&self.scope) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("related_attributes bad scope: {e}");
                return vec![("Failure".into(), msg)];
            }
        };
        match ctx.attributes_dao.find(entity.id, scope, keys).await {
            Ok(entries) => {
                let attrs: Vec<_> = entries
                    .iter()
                    .map(|e| (e.key.clone(), kv_to_json(&e.value)))
                    .collect();
                let rel = merge_or_failure(&mut msg, &self.target, self.prefix.as_deref(), attrs);
                vec![(rel, msg)]
            }
            Err(e) => {
                tracing::warn!("related_attributes query failed: {e}");
                vec![("Failure".into(), msg)]
            }
        }
    }
}

/// enrichment.tenant_customer_fields：tenant/customer 的 title/email/phone 等进 metadata，
/// key 形如 tenant_title / customer_email。
pub struct TenantCustomerFieldsNode {
    tenant_fields: Vec<String>,
    customer_fields: Vec<String>,
}

fn contact_fields(
    fields: &[String],
    title: &str,
    email: Option<&str>,
    phone: Option<&str>,
) -> Vec<(String, String)> {
    let lookup = |f: &str| -> Option<String> {
        match f {
            "title" => Some(title.to_string()),
            "email" => email.map(str::to_string),
            "phone" => phone.map(str::to_string),
            _ => None,
        }
    };
    fields
        .iter()
        .filter_map(|f| lookup(f).map(|v| (f.clone(), v)))
        .collect()
}

impl TenantCustomerFieldsNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize, Default)]
        struct Config {
            #[serde(default)]
            tenant_fields: Vec<String>,
            #[serde(default)]
            customer_fields: Vec<String>,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            tenant_fields: c.tenant_fields,
            customer_fields: c.customer_fields,
        })
    }
}

#[async_trait]
impl Node for TenantCustomerFieldsNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        match ctx.tenant_dao.find_by_id(msg.tenant_id).await {
            Ok(Some(t)) => {
                for (f, v) in contact_fields(
                    &self.tenant_fields,
                    &t.title,
                    t.email.as_deref(),
                    t.phone.as_deref(),
                ) {
                    msg.metadata.insert(format!("tenant_{f}"), v);
                }
            }
            Ok(None) => {
                tracing::warn!(tenant_id = %msg.tenant_id, "tenant_customer_fields: tenant not found");
                return vec![("Failure".into(), msg)];
            }
            Err(e) => {
                tracing::warn!("tenant_customer_fields tenant query failed: {e}");
                return vec![("Failure".into(), msg)];
            }
        }
        // customer 缺失（消息未归属客户）不算失败，仅跳过。
        if let Some(customer_id) = msg.customer_id {
            match ctx.customer_dao.find_by_id(customer_id).await {
                Ok(Some(c)) => {
                    for (f, v) in contact_fields(
                        &self.customer_fields,
                        &c.title,
                        c.email.as_deref(),
                        c.phone.as_deref(),
                    ) {
                        msg.metadata.insert(format!("customer_{f}"), v);
                    }
                }
                Ok(None) => {
                    tracing::debug!(%customer_id, "tenant_customer_fields: customer not found, skipped");
                }
                Err(e) => {
                    tracing::warn!("tenant_customer_fields customer query failed: {e}");
                    return vec![("Failure".into(), msg)];
                }
            }
        }
        vec![("Success".into(), msg)]
    }
}
