//! 内存读模型：tenant → TenantRepo（实体 / 属性 / latest 遥测）。

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tb_common::{AttributeScope, EntityType, KvValue};
use tb_proto::convert;
use tb_proto::edqs::{EdqsEventMsg, edqs_event_msg::Event};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct EntityRecord {
    pub id: Uuid,
    pub entity_type: EntityType,
    pub customer_id: Option<Uuid>,
    pub name: String,
    pub type_or_profile: String,
    pub label: Option<String>,
    pub created_time: i64,
}

#[derive(Debug, Default)]
pub struct TenantRepo {
    pub entities: HashMap<Uuid, EntityRecord>,
    pub attributes: HashMap<(Uuid, AttributeScope), HashMap<String, KvValue>>,
    pub latest: HashMap<Uuid, HashMap<String, (i64, KvValue)>>,
}

impl TenantRepo {
    pub fn upsert_entity(&mut self, record: EntityRecord) {
        self.entities.insert(record.id, record);
    }

    pub fn delete_entity(&mut self, entity_id: Uuid) {
        self.entities.remove(&entity_id);
        self.latest.remove(&entity_id);
        self.attributes.retain(|(id, _), _| *id != entity_id);
    }

    pub fn apply_attribute_update(
        &mut self,
        entity_id: Uuid,
        scope: AttributeScope,
        updated: Vec<(String, KvValue)>,
        deleted_keys: &[String],
    ) {
        if !deleted_keys.is_empty()
            && let Some(attrs) = self.attributes.get_mut(&(entity_id, scope))
        {
            for key in deleted_keys {
                attrs.remove(key);
            }
        }
        if !updated.is_empty() {
            let attrs = self.attributes.entry((entity_id, scope)).or_default();
            for (key, value) in updated {
                attrs.insert(key, value);
            }
        }
    }

    /// 同 key 仅当新 ts >= 现存 ts 时覆盖（与 ts_kv_latest 语义一致）。
    pub fn apply_latest(&mut self, entity_id: Uuid, points: Vec<(String, i64, KvValue)>) {
        let latest = self.latest.entry(entity_id).or_default();
        for (key, ts, value) in points {
            match latest.get(&key) {
                Some((old_ts, _)) if *old_ts > ts => {}
                _ => {
                    latest.insert(key, (ts, value));
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct EdqsState {
    pub tenants: Arc<DashMap<Uuid, TenantRepo>>,
}

impl EdqsState {
    pub fn apply_event(&self, msg: &EdqsEventMsg) {
        let Ok(tenant_id) = Uuid::parse_str(&msg.tenant_id) else {
            tracing::warn!(tenant_id = msg.tenant_id, "edqs event with bad tenant_id");
            return;
        };
        let Some(event) = &msg.event else {
            return;
        };
        let mut repo = self.tenants.entry(tenant_id).or_default();
        match event {
            Event::EntityUpsert(e) => {
                let Ok(entity_id) = Uuid::parse_str(&e.entity_id) else {
                    return;
                };
                let Ok(entity_type) = tb_common::EntityType::try_from(e.entity_type()) else {
                    return;
                };
                repo.upsert_entity(EntityRecord {
                    id: entity_id,
                    entity_type,
                    customer_id: Uuid::parse_str(&e.customer_id).ok(),
                    name: e.name.clone(),
                    type_or_profile: e.type_or_profile.clone(),
                    label: if e.label.is_empty() {
                        None
                    } else {
                        Some(e.label.clone())
                    },
                    created_time: e.created_time,
                });
            }
            Event::EntityDelete(e) => {
                if let Ok(entity_id) = Uuid::parse_str(&e.entity_id) {
                    repo.delete_entity(entity_id);
                }
            }
            Event::AttributeUpdate(e) => {
                let Ok(entity_id) = Uuid::parse_str(&e.entity_id) else {
                    return;
                };
                let Some(scope) = tb_proto::common::AttributeScope::try_from(e.scope)
                    .ok()
                    .and_then(|s| AttributeScope::try_from(s).ok())
                else {
                    return;
                };
                let updated = e
                    .updated
                    .iter()
                    .filter_map(|kv| convert::kv_from_proto(kv, kv.key.clone()))
                    .collect();
                repo.apply_attribute_update(entity_id, scope, updated, &e.deleted_keys);
            }
            Event::LatestTelemetry(e) => {
                let Ok(entity_id) = Uuid::parse_str(&e.entity_id) else {
                    return;
                };
                let points = e
                    .values
                    .iter()
                    .enumerate()
                    .filter_map(|(i, kv)| {
                        let (key, value) = convert::kv_from_proto(kv, kv.key.clone())?;
                        Some((key, e.ts.get(i).copied().unwrap_or_default(), value))
                    })
                    .collect();
                repo.apply_latest(entity_id, points);
            }
        }
    }
}
