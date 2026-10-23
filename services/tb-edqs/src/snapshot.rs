//! 启动快照：从 PostgreSQL 全量加载实体/属性/latest 遥测进内存。
//! 加载期间的事件由 consumer（earliest offset）之后重放兜底，upsert 幂等。

use std::collections::HashMap;

use sqlx::PgPool;
use tb_common::{EntityType, TbResult};
use tb_dao::{AssetDao, AttributesDao, DeviceDao, KeyDictionaryDao, TimeseriesDao};
use uuid::Uuid;

use crate::state::{EdqsState, EntityRecord};

const PAGE: i64 = 1000;

#[derive(Debug, Default)]
pub struct SnapshotStats {
    pub entities: usize,
    pub attributes: usize,
    pub latest: usize,
}

pub async fn load_snapshot(pool: &PgPool, state: &EdqsState) -> TbResult<SnapshotStats> {
    let started = std::time::Instant::now();
    let keys = KeyDictionaryDao::new(pool.clone());
    let device_dao = DeviceDao::new(pool.clone());
    let asset_dao = AssetDao::new(pool.clone());
    let attributes_dao = AttributesDao::new(pool.clone(), keys.clone());
    let timeseries_dao = TimeseriesDao::new(pool.clone(), keys);

    let mut stats = SnapshotStats::default();
    // entity_id → tenant_id 索引：attribute_kv / ts_kv_latest 不带 tenant 列，靠它归位。
    let mut entity_tenant: HashMap<Uuid, Uuid> = HashMap::new();

    let mut offset = 0;
    loop {
        let page = device_dao.scan_all(offset, PAGE).await?;
        let done = page.len() < PAGE as usize;
        for d in page {
            entity_tenant.insert(d.id, d.tenant_id);
            state
                .tenants
                .entry(d.tenant_id)
                .or_default()
                .upsert_entity(EntityRecord {
                    id: d.id,
                    entity_type: EntityType::Device,
                    customer_id: d.customer_id,
                    name: d.name,
                    type_or_profile: d.r#type.unwrap_or_default(),
                    label: d.label,
                    created_time: d.created_time,
                });
            stats.entities += 1;
        }
        offset += PAGE;
        if done {
            break;
        }
    }

    let mut offset = 0;
    loop {
        let page = asset_dao.scan_all(offset, PAGE).await?;
        let done = page.len() < PAGE as usize;
        for a in page {
            entity_tenant.insert(a.id, a.tenant_id);
            // asset 模型无 type 列，type_or_profile 留空。
            state
                .tenants
                .entry(a.tenant_id)
                .or_default()
                .upsert_entity(EntityRecord {
                    id: a.id,
                    entity_type: EntityType::Asset,
                    customer_id: a.customer_id,
                    name: a.name,
                    type_or_profile: String::new(),
                    label: a.label,
                    created_time: a.created_time,
                });
            stats.entities += 1;
        }
        offset += PAGE;
        if done {
            break;
        }
    }

    let mut offset = 0;
    loop {
        let page = attributes_dao.scan_all(offset, PAGE).await?;
        let done = page.len() < PAGE as usize;
        for (entity_id, scope, entry) in page {
            // 无实体记录的行（非 device/asset 实体）跳过。
            let Some(tenant_id) = entity_tenant.get(&entity_id) else {
                continue;
            };
            state
                .tenants
                .entry(*tenant_id)
                .or_default()
                .apply_attribute_update(entity_id, scope, vec![(entry.key, entry.value)], &[]);
            stats.attributes += 1;
        }
        offset += PAGE;
        if done {
            break;
        }
    }

    let mut offset = 0;
    loop {
        let page = timeseries_dao.scan_latest(offset, PAGE).await?;
        let done = page.len() < PAGE as usize;
        for (entity_id, entry) in page {
            let Some(tenant_id) = entity_tenant.get(&entity_id) else {
                continue;
            };
            state
                .tenants
                .entry(*tenant_id)
                .or_default()
                .apply_latest(entity_id, vec![(entry.key, entry.ts, entry.value)]);
            stats.latest += 1;
        }
        offset += PAGE;
        if done {
            break;
        }
    }

    tracing::info!(
        entities = stats.entities,
        attributes = stats.attributes,
        latest = stats.latest,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "edqs snapshot loaded"
    );
    Ok(stats)
}
