//! 实体删除时清理没有外键级联的关联数据，并写墓碑供 housekeeper 清理迟到写入。

use sqlx::{PgConnection, PgPool};
use tb_common::{TbError, TbResult};
use uuid::Uuid;

const ENTITY_DATA_DELETES: [&str; 7] = [
    "DELETE FROM ts_kv_latest WHERE entity_id = ANY($1)",
    "DELETE FROM ts_kv WHERE entity_id = ANY($1)",
    "DELETE FROM attribute_kv WHERE entity_id = ANY($1)",
    "DELETE FROM alarm WHERE originator_id = ANY($1)",
    "DELETE FROM relation WHERE from_id = ANY($1) OR to_id = ANY($1)",
    "DELETE FROM calculated_field WHERE entity_id = ANY($1)",
    "DELETE FROM rule_node_dedup_window WHERE originator_id = ANY($1)",
];

const TENANT_DATA_DELETES: [&str; 6] = [
    "DELETE FROM alarm WHERE tenant_id = $1",
    "DELETE FROM calculated_field WHERE tenant_id = $1",
    "DELETE FROM rule_node_debug_event WHERE tenant_id = $1",
    "DELETE FROM rule_node_dedup_window WHERE tenant_id = $1",
    "DELETE FROM rule_node_delay WHERE tenant_id = $1",
    "DELETE FROM oauth2_client WHERE tenant_id = $1",
];

/// 删除租户时需要在提交后做运行期清理的实体；`entity_ids` 含租户自身与全部设备。
#[derive(Debug, Default)]
pub struct TenantEntities {
    pub device_ids: Vec<Uuid>,
    pub entity_ids: Vec<Uuid>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TombstoneSweep {
    pub purged_rows: u64,
    pub expired: u64,
}

/// 删除实体名下的遥测、latest、属性、告警、关系（两端）、计算字段与去重窗口，返回删除行数。
pub async fn purge_entity_data(conn: &mut PgConnection, entity_ids: &[Uuid]) -> TbResult<u64> {
    if entity_ids.is_empty() {
        return Ok(0);
    }
    let mut deleted = 0;
    for sql in ENTITY_DATA_DELETES {
        deleted += sqlx::query(sql)
            .bind(entity_ids)
            .execute(&mut *conn)
            .await
            .map_err(TbError::db)?
            .rows_affected();
    }
    Ok(deleted)
}

/// 清理实体数据并写墓碑；须与实体删除在同一事务内执行。
pub async fn purge_deleted_entities(
    conn: &mut PgConnection,
    tenant_id: Uuid,
    entity_ids: &[Uuid],
) -> TbResult<()> {
    if entity_ids.is_empty() {
        return Ok(());
    }
    purge_entity_data(conn, entity_ids).await?;
    sqlx::query(
        "INSERT INTO entity_tombstone (entity_id, tenant_id, deleted_at) \
         SELECT id, $2, $3 FROM UNNEST($1::uuid[]) AS t(id) \
         ON CONFLICT (entity_id) DO NOTHING",
    )
    .bind(entity_ids)
    .bind(tenant_id)
    .bind(crate::now_ms())
    .execute(&mut *conn)
    .await
    .map_err(TbError::db)?;
    Ok(())
}

/// 客户自身及其用户（用户随客户级联删除）。
pub async fn customer_entity_ids(
    conn: &mut PgConnection,
    customer_id: Uuid,
) -> TbResult<Vec<Uuid>> {
    sqlx::query_scalar("SELECT $1::uuid UNION ALL SELECT id FROM tb_user WHERE customer_id = $1")
        .bind(customer_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(TbError::db)
}

/// 清空告警上的客户归属；设备与资产的 customer_id 由外键置空。
pub async fn detach_customer(conn: &mut PgConnection, customer_id: Uuid) -> TbResult<()> {
    sqlx::query("UPDATE alarm SET customer_id = NULL WHERE customer_id = $1")
        .bind(customer_id)
        .execute(&mut *conn)
        .await
        .map_err(TbError::db)?;
    Ok(())
}

/// 清理租户全部实体与租户级无外键数据并写墓碑；须在级联删除租户之前、同一事务内执行。
pub async fn purge_tenant_data(
    conn: &mut PgConnection,
    tenant_id: Uuid,
) -> TbResult<TenantEntities> {
    let rows: Vec<(Uuid, bool)> = sqlx::query_as(
        "SELECT $1::uuid, false \
         UNION ALL SELECT id, false FROM customer WHERE tenant_id = $1 \
         UNION ALL SELECT id, false FROM tb_user WHERE tenant_id = $1 \
         UNION ALL SELECT id, true FROM device WHERE tenant_id = $1 \
         UNION ALL SELECT id, false FROM asset WHERE tenant_id = $1",
    )
    .bind(tenant_id)
    .fetch_all(&mut *conn)
    .await
    .map_err(TbError::db)?;
    let mut entities = TenantEntities::default();
    for (id, is_device) in rows {
        if is_device {
            entities.device_ids.push(id);
        }
        entities.entity_ids.push(id);
    }
    purge_deleted_entities(conn, tenant_id, &entities.entity_ids).await?;
    for sql in TENANT_DATA_DELETES {
        sqlx::query(sql)
            .bind(tenant_id)
            .execute(&mut *conn)
            .await
            .map_err(TbError::db)?;
    }
    Ok(entities)
}

/// 按墓碑分批重复清理迟到写入，再删除 `deleted_at < expire_before` 的墓碑。
pub async fn sweep_tombstones(
    pool: &PgPool,
    expire_before: i64,
    batch_size: i64,
) -> TbResult<TombstoneSweep> {
    assert!(batch_size > 0);
    let mut sweep = TombstoneSweep::default();
    let mut after = Uuid::nil();
    loop {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT entity_id FROM entity_tombstone WHERE entity_id > $1 \
             ORDER BY entity_id LIMIT $2",
        )
        .bind(after)
        .bind(batch_size)
        .fetch_all(pool)
        .await
        .map_err(TbError::db)?;
        let Some(&last) = ids.last() else {
            break;
        };
        let mut tx = pool.begin().await.map_err(TbError::db)?;
        sweep.purged_rows += purge_entity_data(&mut tx, &ids).await?;
        tx.commit().await.map_err(TbError::db)?;
        if (ids.len() as i64) < batch_size {
            break;
        }
        after = last;
    }
    sweep.expired = sqlx::query("DELETE FROM entity_tombstone WHERE deleted_at < $1")
        .bind(expire_before)
        .execute(pool)
        .await
        .map_err(TbError::db)?
        .rows_affected();
    Ok(sweep)
}
