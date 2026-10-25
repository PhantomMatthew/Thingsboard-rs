//! 启动快照：从 PostgreSQL 全量加载实体/属性/latest 遥测进内存。
//! 加载期间的事件由 consumer（earliest offset）之后重放兜底，upsert 幂等。

use std::collections::HashMap;

use sqlx::PgPool;
use tb_common::{EntityType, TbResult};
use tb_dao::{AssetDao, AttributesDao, DeviceDao, KeyDictionaryDao, TenantDao, TimeseriesDao};
use uuid::Uuid;

use crate::state::{EdqsState, EntityRecord, TenantRepo};

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
                .apply_attribute_update(entity_id, scope, vec![(entry.key, entry.value)], &[], 0);
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

/// 单租户读模型：重同步、定期对账与 core 的 PostgreSQL fallback 共用。
pub async fn load_tenant_repo(pool: &PgPool, tenant_id: Uuid) -> TbResult<TenantRepo> {
    let keys = KeyDictionaryDao::new(pool.clone());
    let device_dao = DeviceDao::new(pool.clone());
    let asset_dao = AssetDao::new(pool.clone());
    let attributes_dao = AttributesDao::new(pool.clone(), keys.clone());
    let timeseries_dao = TimeseriesDao::new(pool.clone(), keys);

    let (devices, assets) = tokio::try_join!(
        device_dao.find_all_by_tenant(tenant_id),
        asset_dao.find_all_by_tenant(tenant_id)
    )?;
    let mut repo = TenantRepo::default();
    for device in devices {
        repo.upsert_entity(EntityRecord {
            id: device.id,
            entity_type: EntityType::Device,
            customer_id: device.customer_id,
            name: device.name,
            type_or_profile: device.r#type.unwrap_or_default(),
            label: device.label,
            created_time: device.created_time,
        });
    }
    for asset in assets {
        repo.upsert_entity(EntityRecord {
            id: asset.id,
            entity_type: EntityType::Asset,
            customer_id: asset.customer_id,
            name: asset.name,
            type_or_profile: String::new(),
            label: asset.label,
            created_time: asset.created_time,
        });
    }

    let entity_ids: Vec<_> = repo.entities.keys().copied().collect();
    let (attributes, latest) = tokio::try_join!(
        attributes_dao.find_by_entity_ids(&entity_ids),
        timeseries_dao.find_latest_by_entity_ids(&entity_ids)
    )?;
    for (entity_id, scope, entry) in attributes {
        repo.apply_attribute_update(entity_id, scope, vec![(entry.key, entry.value)], &[], 0);
    }
    for (entity_id, entry) in latest {
        repo.apply_latest(entity_id, vec![(entry.key, entry.ts, entry.value)]);
    }
    Ok(repo)
}

/// 从数据库重载并整体替换一个租户；调用方需暂停该租户的事件应用（consumer 内串行执行）。
pub async fn reload_tenant(pool: &PgPool, state: &EdqsState, tenant_id: Uuid) -> TbResult<()> {
    let repo = load_tenant_repo(pool, tenant_id).await?;
    state.replace_tenant(tenant_id, repo);
    Ok(())
}

/// 对账范围：数据库中的租户及内存中残留的租户（后者可能已被删除）。
pub async fn reconcile_tenant_ids(pool: &PgPool, state: &EdqsState) -> TbResult<Vec<Uuid>> {
    let mut ids = TenantDao::new(pool.clone()).find_all_ids().await?;
    ids.extend(state.tenants.iter().map(|entry| *entry.key()));
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}
