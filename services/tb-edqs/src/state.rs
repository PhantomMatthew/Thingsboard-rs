//! 内存读模型：tenant → TenantRepo（实体 / 属性 / latest 遥测）。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use sqlx::PgPool;
use tb_common::{AttributeScope, EntityType, KvValue};
use tb_proto::convert;
use tb_proto::edqs::{EdqsEventMsg, edqs_event_msg::Event};
use uuid::Uuid;

const CONSUMER_HEARTBEAT_TIMEOUT_MS: i64 = 30_000;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Default)]
pub struct ConsumerHealth {
    connected: AtomicBool,
    last_heartbeat_ms: AtomicI64,
}

impl ConsumerHealth {
    pub fn beat(&self) {
        self.last_heartbeat_ms.store(now_ms(), Ordering::SeqCst);
        self.connected.store(true, Ordering::SeqCst);
    }

    pub fn disconnect(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    pub fn is_healthy(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
            && now_ms().saturating_sub(self.last_heartbeat_ms.load(Ordering::SeqCst))
                <= CONSUMER_HEARTBEAT_TIMEOUT_MS
    }
}

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
    attribute_ts: HashMap<(Uuid, AttributeScope, String), i64>,
}

impl TenantRepo {
    pub fn upsert_entity(&mut self, record: EntityRecord) {
        self.entities.insert(record.id, record);
    }

    pub fn delete_entity(&mut self, entity_id: Uuid) {
        self.entities.remove(&entity_id);
        self.latest.remove(&entity_id);
        self.attributes.retain(|(id, _), _| *id != entity_id);
        self.attribute_ts.retain(|(id, _, _), _| *id != entity_id);
    }

    fn accept_attribute_update(
        &mut self,
        entity_id: Uuid,
        scope: AttributeScope,
        key: &str,
        update_ts: i64,
    ) -> bool {
        let version_key = (entity_id, scope, key.to_string());
        if self
            .attribute_ts
            .get(&version_key)
            .is_some_and(|old_ts| *old_ts > update_ts)
        {
            return false;
        }
        self.attribute_ts.insert(version_key, update_ts);
        true
    }

    pub fn apply_attribute_update(
        &mut self,
        entity_id: Uuid,
        scope: AttributeScope,
        updated: Vec<(String, KvValue)>,
        deleted_keys: &[String],
        update_ts: i64,
    ) {
        for key in deleted_keys {
            if !self.accept_attribute_update(entity_id, scope, key, update_ts) {
                continue;
            }
            if let Some(attrs) = self.attributes.get_mut(&(entity_id, scope)) {
                attrs.remove(key);
            }
        }
        for (key, value) in updated {
            if self.accept_attribute_update(entity_id, scope, &key, update_ts) {
                self.attributes
                    .entry((entity_id, scope))
                    .or_default()
                    .insert(key, value);
            }
        }
    }

    /// 移除 ts 落在 [start_ts, end_ts] 的 latest；更新的 latest（并发写入）保留。
    pub fn delete_latest(&mut self, entity_id: Uuid, keys: &[String], start_ts: i64, end_ts: i64) {
        let Some(latest) = self.latest.get_mut(&entity_id) else {
            return;
        };
        for key in keys {
            if latest
                .get(key)
                .is_some_and(|(ts, _)| (start_ts..=end_ts).contains(ts))
            {
                latest.remove(key);
            }
        }
        if latest.is_empty() {
            self.latest.remove(&entity_id);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entities.is_empty() && self.attributes.is_empty() && self.latest.is_empty()
    }

    /// 同 key 仅当新 ts >= 现存 ts 时覆盖（与 ts_kv_latest 语义一致）。
    pub fn apply_latest(&mut self, entity_id: Uuid, points: Vec<(String, i64, KvValue)>) {
        if points.is_empty() {
            return;
        }
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

#[derive(Debug, Clone)]
pub struct EdqsState {
    pub tenants: Arc<DashMap<Uuid, TenantRepo>>,
    pub(crate) consumer_health: Arc<ConsumerHealth>,
    api_key: Arc<str>,
    /// 租户重同步与定期对账的数据源；None 时忽略重同步事件。
    pub(crate) pool: Option<PgPool>,
    pub(crate) reconcile_interval: Option<Duration>,
}

impl Default for EdqsState {
    fn default() -> Self {
        Self {
            tenants: Arc::new(DashMap::new()),
            consumer_health: Arc::new(ConsumerHealth::default()),
            api_key: Arc::from(std::env::var("TB_EDQS_API_KEY").unwrap_or_default()),
            pool: None,
            reconcile_interval: None,
        }
    }
}

impl EdqsState {
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Arc::from(api_key.into());
        self
    }

    pub fn with_pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// 周期性从数据库重载全部租户，兜底进程崩溃等无法发出重同步标记的增量丢失。
    pub fn with_reconcile_interval(mut self, interval: Duration) -> Self {
        self.reconcile_interval = Some(interval);
        self
    }

    pub(crate) fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn consumer_healthy(&self) -> bool {
        self.consumer_health.is_healthy()
    }

    pub fn remove_tenant(&self, tenant_id: Uuid) {
        self.tenants.remove(&tenant_id);
    }

    pub fn apply_event(&self, msg: &EdqsEventMsg) {
        let Ok(tenant_id) = Uuid::parse_str(&msg.tenant_id) else {
            tracing::warn!(tenant_id = msg.tenant_id, "edqs event with bad tenant_id");
            return;
        };
        let Some(event) = &msg.event else {
            return;
        };
        match event {
            Event::EntityUpsert(e) => {
                let Ok(entity_id) = Uuid::parse_str(&e.entity_id) else {
                    return;
                };
                let Ok(entity_type) = tb_common::EntityType::try_from(e.entity_type()) else {
                    return;
                };
                self.tenants
                    .entry(tenant_id)
                    .or_default()
                    .upsert_entity(EntityRecord {
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
                let Ok(entity_id) = Uuid::parse_str(&e.entity_id) else {
                    return;
                };
                if let Some(mut repo) = self.tenants.get_mut(&tenant_id) {
                    repo.delete_entity(entity_id);
                }
            }
            Event::TenantDelete(e) => {
                let Ok(delete_tenant_id) = Uuid::parse_str(&e.tenant_id) else {
                    tracing::warn!(
                        tenant_id = e.tenant_id,
                        "edqs tenant delete with bad tenant_id"
                    );
                    return;
                };
                if delete_tenant_id != tenant_id {
                    tracing::warn!(
                        %tenant_id,
                        %delete_tenant_id,
                        "edqs tenant delete tenant mismatch"
                    );
                    return;
                }
                self.remove_tenant(tenant_id);
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
                let Some(updated) = e
                    .updated
                    .iter()
                    .map(|kv| convert::kv_from_proto(kv, kv.key.clone()))
                    .collect::<Option<Vec<_>>>()
                else {
                    tracing::warn!(%tenant_id, %entity_id, "edqs attribute event with bad value");
                    return;
                };
                if updated.is_empty() && e.deleted_keys.is_empty() {
                    return;
                }
                if updated.is_empty() {
                    if let Some(mut repo) = self.tenants.get_mut(&tenant_id) {
                        repo.apply_attribute_update(
                            entity_id,
                            scope,
                            updated,
                            &e.deleted_keys,
                            e.update_ts,
                        );
                    }
                } else {
                    self.tenants
                        .entry(tenant_id)
                        .or_default()
                        .apply_attribute_update(
                            entity_id,
                            scope,
                            updated,
                            &e.deleted_keys,
                            e.update_ts,
                        );
                }
            }
            Event::LatestTelemetry(e) => {
                if e.values.len() != e.ts.len() {
                    tracing::warn!(
                        values = e.values.len(),
                        timestamps = e.ts.len(),
                        "edqs latest event values/ts length mismatch; dropping event"
                    );
                    return;
                }
                let Ok(entity_id) = Uuid::parse_str(&e.entity_id) else {
                    return;
                };
                let Some(points) = e
                    .values
                    .iter()
                    .zip(&e.ts)
                    .map(|(kv, ts)| {
                        convert::kv_from_proto(kv, kv.key.clone())
                            .map(|(key, value)| (key, *ts, value))
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    tracing::warn!(%tenant_id, %entity_id, "edqs latest event with bad value");
                    return;
                };
                if points.is_empty() {
                    return;
                }
                self.tenants
                    .entry(tenant_id)
                    .or_default()
                    .apply_latest(entity_id, points);
            }
            Event::LatestDelete(e) => {
                if e.replacements.len() != e.replacement_ts.len() {
                    tracing::warn!(
                        replacements = e.replacements.len(),
                        timestamps = e.replacement_ts.len(),
                        "edqs latest delete replacements/ts length mismatch; dropping event"
                    );
                    return;
                }
                let Ok(entity_id) = Uuid::parse_str(&e.entity_id) else {
                    return;
                };
                let Some(replacements) = e
                    .replacements
                    .iter()
                    .zip(&e.replacement_ts)
                    .map(|(kv, ts)| {
                        convert::kv_from_proto(kv, kv.key.clone())
                            .map(|(key, value)| (key, *ts, value))
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    tracing::warn!(%tenant_id, %entity_id, "edqs latest delete with bad value");
                    return;
                };
                let Some(mut repo) = self.tenants.get_mut(&tenant_id) else {
                    return;
                };
                repo.delete_latest(entity_id, &e.keys, e.start_ts, e.end_ts);
                repo.apply_latest(entity_id, replacements);
            }
            // 需要访问数据库，由 consumer 处理。
            Event::TenantResync(_) => {}
        }
    }

    /// 用数据库重载结果整体替换租户读模型；租户已无数据时移除。
    pub fn replace_tenant(&self, tenant_id: Uuid, repo: TenantRepo) {
        if repo.is_empty() {
            self.tenants.remove(&tenant_id);
        } else {
            self.tenants.insert(tenant_id, repo);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tb_proto::common as common_pb;

    fn event(tenant_id: Uuid, event: Event) -> EdqsEventMsg {
        EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(event),
        }
    }

    fn upsert(tenant_id: Uuid, entity_id: Uuid) -> EdqsEventMsg {
        event(
            tenant_id,
            Event::EntityUpsert(tb_proto::edqs::EntityUpsertMsg {
                entity_id: entity_id.to_string(),
                entity_type: common_pb::EntityType::Device as i32,
                customer_id: String::new(),
                name: "device".into(),
                type_or_profile: "sensor".into(),
                label: String::new(),
                created_time: 1,
            }),
        )
    }

    fn attribute(
        tenant_id: Uuid,
        entity_id: Uuid,
        update_ts: i64,
        value: Option<&str>,
        deleted_keys: Vec<String>,
    ) -> EdqsEventMsg {
        event(
            tenant_id,
            Event::AttributeUpdate(tb_proto::edqs::AttributeUpdateMsg {
                entity_id: entity_id.to_string(),
                scope: common_pb::AttributeScope::ServerScope as i32,
                updated: value
                    .map(|value| {
                        vec![tb_proto::convert::kv_to_proto_named(
                            0,
                            "firmware",
                            &KvValue::Str(value.into()),
                        )]
                    })
                    .unwrap_or_default(),
                deleted_keys,
                update_ts,
            }),
        )
    }

    fn latest(
        tenant_id: Uuid,
        entity_id: Uuid,
        values: Vec<(&str, i64)>,
        ts: Vec<i64>,
    ) -> EdqsEventMsg {
        event(
            tenant_id,
            Event::LatestTelemetry(tb_proto::edqs::LatestTelemetryMsg {
                entity_id: entity_id.to_string(),
                values: values
                    .into_iter()
                    .map(|(key, value)| {
                        tb_proto::convert::kv_to_proto_named(0, key, &KvValue::Long(value))
                    })
                    .collect(),
                ts,
            }),
        )
    }

    #[test]
    fn attribute_updates_reject_older_timestamps_and_resurrection() {
        let state = EdqsState::default();
        let tenant_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        state.apply_event(&attribute(tenant_id, entity_id, 200, Some("new"), vec![]));
        state.apply_event(&attribute(tenant_id, entity_id, 100, Some("old"), vec![]));
        state.apply_event(&attribute(
            tenant_id,
            entity_id,
            150,
            None,
            vec!["firmware".into()],
        ));
        let repo = state.tenants.get(&tenant_id).unwrap();
        let attrs = repo
            .attributes
            .get(&(entity_id, AttributeScope::ServerScope))
            .unwrap();
        assert_eq!(attrs.get("firmware"), Some(&KvValue::Str("new".into())));
        drop(repo);

        state.apply_event(&attribute(
            tenant_id,
            entity_id,
            300,
            None,
            vec!["firmware".into()],
        ));
        state.apply_event(&attribute(tenant_id, entity_id, 250, Some("stale"), vec![]));
        let repo = state.tenants.get(&tenant_id).unwrap();
        assert!(
            repo.attributes
                .get(&(entity_id, AttributeScope::ServerScope))
                .unwrap()
                .get("firmware")
                .is_none()
        );
    }

    #[test]
    fn latest_length_mismatch_drops_whole_event() {
        let state = EdqsState::default();
        let tenant_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        state.apply_event(&latest(
            tenant_id,
            entity_id,
            vec![("temperature", 10)],
            vec![100],
        ));
        state.apply_event(&latest(
            tenant_id,
            entity_id,
            vec![("temperature", 20), ("humidity", 50)],
            vec![200],
        ));
        let repo = state.tenants.get(&tenant_id).unwrap();
        let latest = repo.latest.get(&entity_id).unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest.get("temperature"), Some(&(100, KvValue::Long(10))));
    }

    #[test]
    fn tenant_delete_removes_data_and_deletes_do_not_create_empty_repos() {
        let state = EdqsState::default();
        let tenant_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        state.apply_event(&upsert(tenant_id, entity_id));
        state.apply_event(&attribute(tenant_id, entity_id, 100, Some("1.0"), vec![]));
        state.apply_event(&latest(
            tenant_id,
            entity_id,
            vec![("temperature", 10)],
            vec![100],
        ));
        assert_eq!(state.tenants.get(&tenant_id).unwrap().entities.len(), 1);

        state.apply_event(&event(
            tenant_id,
            Event::TenantDelete(tb_proto::edqs::TenantDeleteMsg {
                tenant_id: tenant_id.to_string(),
            }),
        ));
        assert!(state.tenants.get(&tenant_id).is_none());

        state.apply_event(&event(
            tenant_id,
            Event::EntityDelete(tb_proto::edqs::EntityDeleteMsg {
                entity_id: entity_id.to_string(),
                entity_type: common_pb::EntityType::Device as i32,
            }),
        ));
        state.apply_event(&attribute(
            tenant_id,
            entity_id,
            400,
            None,
            vec!["firmware".into()],
        ));
        state.apply_event(&latest(
            tenant_id,
            entity_id,
            vec![("temperature", 20)],
            vec![],
        ));
        assert!(state.tenants.get(&tenant_id).is_none());
    }

    fn latest_delete(
        tenant_id: Uuid,
        entity_id: Uuid,
        keys: &[&str],
        range: (i64, i64),
        replacements: Vec<(&str, i64, i64)>,
    ) -> EdqsEventMsg {
        event(
            tenant_id,
            Event::LatestDelete(tb_proto::edqs::LatestDeleteMsg {
                entity_id: entity_id.to_string(),
                keys: keys.iter().map(|k| k.to_string()).collect(),
                start_ts: range.0,
                end_ts: range.1,
                replacement_ts: replacements.iter().map(|(_, ts, _)| *ts).collect(),
                replacements: replacements
                    .into_iter()
                    .map(|(key, _, value)| {
                        tb_proto::convert::kv_to_proto_named(0, key, &KvValue::Long(value))
                    })
                    .collect(),
            }),
        )
    }

    #[test]
    fn latest_delete_rewrites_only_values_inside_the_range() {
        let state = EdqsState::default();
        let tenant_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        state.apply_event(&upsert(tenant_id, entity_id));
        state.apply_event(&latest(
            tenant_id,
            entity_id,
            vec![("temperature", 30), ("humidity", 50), ("pressure", 7)],
            vec![300, 300, 300],
        ));

        // 范围删除命中 latest：替换为重算值（ts 更旧也生效）。
        state.apply_event(&latest_delete(
            tenant_id,
            entity_id,
            &["temperature"],
            (250, 400),
            vec![("temperature", 200, 20)],
        ));
        // 全量删除：无替换值时移除。
        state.apply_event(&latest_delete(
            tenant_id,
            entity_id,
            &["humidity"],
            (i64::MIN, i64::MAX),
            vec![],
        ));
        // 并发写入的新值不在删除范围内：保留，旧的替换值被 ts 规则拒绝。
        state.apply_event(&latest(
            tenant_id,
            entity_id,
            vec![("pressure", 9)],
            vec![500],
        ));
        state.apply_event(&latest_delete(
            tenant_id,
            entity_id,
            &["pressure"],
            (250, 400),
            vec![("pressure", 100, 1)],
        ));

        let repo = state.tenants.get(&tenant_id).unwrap();
        let latest = repo.latest.get(&entity_id).unwrap();
        assert_eq!(latest.get("temperature"), Some(&(200, KvValue::Long(20))));
        assert_eq!(latest.get("humidity"), None);
        assert_eq!(latest.get("pressure"), Some(&(500, KvValue::Long(9))));
    }

    #[test]
    fn replace_tenant_swaps_repo_and_drops_empty_tenants() {
        let state = EdqsState::default();
        let tenant_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        state.apply_event(&upsert(tenant_id, entity_id));
        let mut repo = TenantRepo::default();
        repo.apply_latest(entity_id, vec![("temperature".into(), 1, KvValue::Long(1))]);
        state.replace_tenant(tenant_id, repo);
        assert!(state.tenants.get(&tenant_id).unwrap().entities.is_empty());
        state.replace_tenant(tenant_id, TenantRepo::default());
        assert!(state.tenants.get(&tenant_id).is_none());
    }

    #[test]
    fn consumer_health_requires_recent_heartbeat() {
        let health = ConsumerHealth::default();
        assert!(!health.is_healthy());
        health.beat();
        assert!(health.is_healthy());
        health.last_heartbeat_ms.store(
            now_ms() - CONSUMER_HEARTBEAT_TIMEOUT_MS - 1,
            Ordering::SeqCst,
        );
        assert!(!health.is_healthy());
        health.beat();
        health.disconnect();
        assert!(!health.is_healthy());
    }
}
