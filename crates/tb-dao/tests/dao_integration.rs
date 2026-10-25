//! tb-dao 集成测试：连接 localhost:5433 的真实 PostgreSQL。
//! 运行前执行 migrate；测试数据全部使用随机 UUID / 随机名称避免冲突，
//! 结尾删除顶层 tenant 借助 ON DELETE CASCADE 清理。

use tb_common::{EntityType, TbResult};
use tb_dao::models::*;
use tb_dao::page::PageLink;
use tb_dao::{
    AlarmDao, CustomerDao, DeviceCredentialsDao, DeviceDao, DeviceProfileDao, RelationDao,
    TenantDao, UserDao, migrate,
};
use uuid::Uuid;

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://thingsboard:thingsboard@localhost:5433/thingsboard".into())
}

async fn setup() -> sqlx::PgPool {
    let pool = sqlx::PgPool::connect(&db_url()).await.unwrap();
    migrate(&pool).await.unwrap();
    pool
}

fn uniq(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::now_v7().simple())
}

fn new_tenant(title: &str) -> Tenant {
    Tenant {
        id: Uuid::now_v7(),
        created_time: 0,
        title: title.into(),
        region: None,
        country: None,
        state: None,
        city: None,
        address: None,
        address2: None,
        zip: None,
        phone: None,
        email: None,
        additional_info: None,
        tenant_profile_id: None,
    }
}

fn new_device_profile(tenant_id: Uuid, name: &str, is_default: bool) -> DeviceProfile {
    DeviceProfile {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        name: name.into(),
        description: None,
        is_default,
        transport_type: "DEFAULT".into(),
        provision_type: "DISABLED".into(),
        profile_data: serde_json::json!({}),
        firmware_id: None,
        software_id: None,
    }
}

fn new_device(tenant_id: Uuid, profile_id: Uuid, name: &str) -> Device {
    Device {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        customer_id: None,
        device_profile_id: profile_id,
        name: name.into(),
        r#type: Some("default".into()),
        label: None,
        external_id: None,
        firmware_id: None,
        software_id: None,
        additional_info: None,
        device_data: serde_json::json!({}),
    }
}

#[tokio::test]
async fn tenant_customer_user_cascade() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let customer_dao = CustomerDao::new(pool.clone());
    let user_dao = UserDao::new(pool.clone());

    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;
    assert!(tenant.created_time > 0);

    let customer = Customer {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id: tenant.id,
        title: uniq("customer"),
        country: None,
        state: None,
        city: None,
        address: None,
        address2: None,
        zip: None,
        phone: None,
        email: None,
        additional_info: None,
    };
    let customer = customer_dao.save(&customer).await?;

    let email = format!("{}@test.io", Uuid::now_v7().simple());
    let user = User {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id: tenant.id,
        customer_id: Some(customer.id),
        authority: Authority::CustomerUser,
        email: email.clone(),
        first_name: Some("Alice".into()),
        last_name: None,
        phone: None,
        additional_info: None,
    };
    let user = user_dao.save(&user).await?;

    let found = user_dao
        .find_by_email(&email)
        .await?
        .expect("user by email");
    assert_eq!(found.id, user.id);
    assert_eq!(found.authority, Authority::CustomerUser);

    let tenant_users = user_dao
        .find_by_tenant_id(tenant.id, &PageLink::new(0, 10))
        .await?;
    assert!(tenant_users.data.iter().any(|u| u.id == user.id));

    let customer_users = user_dao
        .find_by_customer_id(tenant.id, customer.id, &PageLink::new(0, 10))
        .await?;
    assert_eq!(customer_users.total_elements, 1);

    // 更新路径：created_time 保留
    let created = user.created_time;
    let mut updated = user.clone();
    updated.first_name = Some("Bob".into());
    let updated = user_dao.save(&updated).await?;
    let reloaded = user_dao.find_by_id(user.id).await?.unwrap();
    assert_eq!(reloaded.created_time, created);
    assert_eq!(updated.id, reloaded.id);
    assert_eq!(reloaded.first_name.as_deref(), Some("Bob"));

    // 删除 tenant 级联清理 customer/user
    assert!(tenant_dao.delete_by_id(tenant.id).await?);
    assert!(customer_dao.find_by_id(customer.id).await?.is_none());
    assert!(user_dao.find_by_id(user.id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn device_with_credentials_transaction() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let profile_dao = DeviceProfileDao::new(pool.clone());
    let cred_dao = DeviceCredentialsDao::new(pool.clone());

    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;
    let profile = profile_dao
        .save(&new_device_profile(tenant.id, &uniq("profile"), true))
        .await?;

    let default = profile_dao
        .find_default(tenant.id)
        .await?
        .expect("default profile");
    assert_eq!(default.id, profile.id);

    let device = new_device(tenant.id, profile.id, &uniq("device"));
    let token = uniq("token");
    let credentials = DeviceCredentials {
        id: Uuid::now_v7(),
        created_time: 0,
        device_id: device.id,
        credentials_type: CredentialsType::AccessToken,
        credentials_id: Some(token.clone()),
        credentials_value: None,
    };

    let (saved_device, saved_cred) = cred_dao
        .save_device_with_credentials(&device, &credentials)
        .await?;
    assert!(saved_device.created_time > 0);
    assert_eq!(saved_cred.device_id, saved_device.id);

    let by_token = cred_dao
        .find_by_credentials_id(&token)
        .await?
        .expect("credentials by token");
    assert_eq!(by_token.device_id, saved_device.id);
    assert_eq!(by_token.credentials_type, CredentialsType::AccessToken);

    let by_device = cred_dao
        .find_by_device_id(saved_device.id)
        .await?
        .expect("credentials by device");
    assert_eq!(by_device.credentials_id.as_deref(), Some(token.as_str()));

    // 再次保存：走 update + credentials upsert 路径
    let new_token = uniq("token");
    let mut device2 = saved_device.clone();
    device2.label = Some("updated".into());
    let cred2 = DeviceCredentials {
        id: Uuid::now_v7(),
        created_time: 0,
        device_id: saved_device.id,
        credentials_type: CredentialsType::AccessToken,
        credentials_id: Some(new_token.clone()),
        credentials_value: None,
    };
    cred_dao
        .save_device_with_credentials(&device2, &cred2)
        .await?;
    let reloaded = cred_dao.find_by_device_id(saved_device.id).await?.unwrap();
    assert_eq!(reloaded.credentials_id.as_deref(), Some(new_token.as_str()));

    assert!(tenant_dao.delete_by_id(tenant.id).await?);
    assert!(cred_dao.find_by_device_id(saved_device.id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn pagination_text_search() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let profile_dao = DeviceProfileDao::new(pool.clone());
    let device_dao = DeviceDao::new(pool.clone());

    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;
    let profile = profile_dao
        .save(&new_device_profile(tenant.id, &uniq("profile"), false))
        .await?;

    let marker = uniq("thermostat");
    for i in 0..5 {
        let d = new_device(tenant.id, profile.id, &format!("{marker}-{i}"));
        device_dao.save(&d).await?;
    }
    // 干扰数据：不应被 text_search 命中
    device_dao
        .save(&new_device(tenant.id, profile.id, &uniq("other")))
        .await?;

    let mut link = PageLink::new(0, 2);
    link.text_search = Some(marker.clone());
    let page0 = device_dao.find_by_tenant_id(tenant.id, &link).await?;
    assert_eq!(page0.total_elements, 5);
    assert_eq!(page0.total_pages, 3);
    assert_eq!(page0.data.len(), 2);
    assert!(page0.has_next);

    let mut link = PageLink::new(2, 2);
    link.text_search = Some(marker.clone());
    let page2 = device_dao.find_by_tenant_id(tenant.id, &link).await?;
    assert_eq!(page2.data.len(), 1);
    assert!(!page2.has_next);

    // 排序白名单：非法列回退 created_time，不产生 SQL 注入
    let mut link = PageLink::new(0, 10);
    link.sort_property = Some("name; DROP TABLE device;--".into());
    let page = device_dao.find_by_tenant_id(tenant.id, &link).await?;
    assert_eq!(page.total_elements, 6);

    assert!(tenant_dao.delete_by_id(tenant.id).await?);
    Ok(())
}

#[tokio::test]
async fn relation_crud() -> TbResult<()> {
    let pool = setup().await;
    let relation_dao = RelationDao::new(pool.clone());

    let from_id = Uuid::now_v7();
    let to_id = Uuid::now_v7();
    let relation = EntityRelation {
        from_id,
        from_type: EntityType::Device,
        to_id,
        to_type: EntityType::Asset,
        relation_type_group: "COMMON".into(),
        relation_type: "Contains".into(),
        additional_info: None,
    };

    relation_dao.save(&relation).await?;

    let found = relation_dao
        .find_relation(
            from_id,
            EntityType::Device,
            to_id,
            EntityType::Asset,
            "Contains",
        )
        .await?
        .expect("exact relation");
    assert_eq!(found.relation_type_group, "COMMON");

    let by_from = relation_dao
        .find_by_from(from_id, EntityType::Device)
        .await?;
    assert_eq!(by_from.len(), 1);
    let by_to = relation_dao.find_by_to(to_id, EntityType::Asset).await?;
    assert_eq!(by_to.len(), 1);

    let mut rule_relation = relation.clone();
    rule_relation.relation_type_group = "RULE".into();
    relation_dao.save(&rule_relation).await?;
    let common = relation_dao
        .find_relation(
            from_id,
            EntityType::Device,
            to_id,
            EntityType::Asset,
            "Contains",
        )
        .await?
        .expect("COMMON relation");
    assert_eq!(common.relation_type_group, "COMMON");
    assert!(relation_dao.delete(&rule_relation).await?);

    assert!(relation_dao.delete(&relation).await?);
    assert!(
        relation_dao
            .find_relation(
                from_id,
                EntityType::Device,
                to_id,
                EntityType::Asset,
                "Contains"
            )
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn alarm_latest_and_paged() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let alarm_dao = AlarmDao::new(pool.clone());

    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;
    let originator = Uuid::now_v7();

    for i in 0..3 {
        let alarm = Alarm {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            originator_id: originator,
            originator_type: EntityType::Device,
            r#type: format!("HighTemperature{i}"),
            severity: AlarmSeverity::Major,
            status: AlarmStatus::ActiveUnack,
            start_ts: 1000 + i,
            end_ts: 0,
            ack_ts: None,
            clear_ts: None,
            details: None,
            propagate: false,
            propagate_to_owner: false,
            propagate_to_tenant: false,
            propagate_relation_types: None,
            customer_id: None,
            assignee_id: None,
            assign_ts: None,
        };
        alarm_dao.save(&alarm).await?;
    }

    let latest = alarm_dao
        .find_latest_by_originator(originator)
        .await?
        .expect("latest alarm");
    assert_eq!(latest.start_ts, 1002);

    let page = alarm_dao
        .find_by_tenant(
            tenant.id,
            Some(originator),
            None,
            None,
            None,
            &PageLink::new(0, 10),
        )
        .await?;
    assert_eq!(page.total_elements, 3);

    let all = alarm_dao
        .find_by_tenant(tenant.id, None, None, None, None, &PageLink::new(0, 100))
        .await?;
    assert!(all.total_elements >= 3);

    // 清理 alarm（无外键级联）
    for a in &page.data {
        alarm_dao.delete_by_id(a.id).await?;
    }
    tenant_dao.delete_by_id(tenant.id).await?;
    Ok(())
}

#[tokio::test]
async fn rule_node_debug_event_dao() -> TbResult<()> {
    let pool = setup().await;
    let dao = tb_dao::RuleNodeDebugEventDao::new(pool.clone());
    let node_id = Uuid::now_v7();
    let chain_id = Uuid::now_v7();
    let tenant_id = Uuid::now_v7();

    let mut e = tb_dao::RuleNodeDebugEvent::new(
        tenant_id,
        chain_id,
        node_id,
        Uuid::now_v7(),
        "IN",
        Some(serde_json::json!({"a": 1})),
        None,
        None,
    );
    dao.save(&e).await?;
    e.ts += 10;
    let mut out = e.clone();
    out.id = Uuid::now_v7();
    out.event_type = "OUT".into();
    out.error = Some("boom".into());
    dao.save(&out).await?;

    let page = dao
        .find_by_node(node_id, None, None, &PageLink::new(0, 10))
        .await?;
    assert_eq!(page.total_elements, 2);
    assert_eq!(page.data[0].event_type, "OUT"); // ts desc
    assert_eq!(page.data[0].error.as_deref(), Some("boom"));

    // startTs 过滤
    let page = dao
        .find_by_node(node_id, Some(out.ts), None, &PageLink::new(0, 10))
        .await?;
    assert_eq!(page.total_elements, 1);

    // 清理
    let removed = dao.delete_older_than(out.ts + 1).await?;
    assert!(removed >= 2);
    assert_eq!(
        dao.find_by_node(node_id, None, None, &PageLink::new(0, 10))
            .await?
            .total_elements,
        0
    );
    Ok(())
}

#[tokio::test]
async fn housekeeper_cleanup() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let key_dao = tb_dao::KeyDictionaryDao::new(pool.clone());
    let ts_dao = tb_dao::TimeseriesDao::new(pool.clone(), key_dao);
    let debug_dao = tb_dao::RuleNodeDebugEventDao::new(pool.clone());
    let alarm_dao = AlarmDao::new(pool.clone());
    let audit_dao = tb_dao::AuditLogDao::new(pool.clone());

    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let day = 24 * 3600 * 1000;
    let old = now - 10 * day;
    let cutoff = now - day;

    // ts_kv：旧点应删，新点保留，latest 不动
    let entity_id = Uuid::now_v7();
    let mk = |ts: i64| tb_common::TsKvEntry {
        ts,
        key: "temp".into(),
        value: tb_common::KvValue::Long(1),
    };
    ts_dao.save(entity_id, &[mk(old)]).await?;
    ts_dao.save(entity_id, &[mk(now)]).await?;
    let deleted = ts_dao.delete_older_than(cutoff).await?;
    assert!(deleted >= 1);
    let history = ts_dao
        .find_history(entity_id, "temp", 0, now + 1, 10, true)
        .await?;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].ts, now);
    let latest = ts_dao.find_latest(entity_id, None).await?;
    assert_eq!(latest.len(), 1);

    // debug event：旧事件应删
    let mut ev = tb_dao::RuleNodeDebugEvent::new(
        tenant.id,
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        "IN",
        None,
        None,
        None,
    );
    ev.ts = old;
    debug_dao.save(&ev).await?;
    let removed = debug_dao.delete_older_than(cutoff).await?;
    assert!(removed >= 1);

    // alarm：仅清 CLEARED_* 且 clear_ts 早于阈值
    let mk_alarm = |status: AlarmStatus, clear_ts: Option<i64>| Alarm {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id: tenant.id,
        originator_id: entity_id,
        originator_type: EntityType::Device,
        r#type: "T".into(),
        severity: AlarmSeverity::Minor,
        status,
        start_ts: old,
        end_ts: 0,
        ack_ts: None,
        clear_ts,
        details: None,
        propagate: false,
        propagate_to_owner: false,
        propagate_to_tenant: false,
        propagate_relation_types: None,
        customer_id: None,
        assignee_id: None,
        assign_ts: None,
    };
    alarm_dao
        .save(&mk_alarm(AlarmStatus::ClearedAck, Some(old)))
        .await?;
    alarm_dao
        .save(&mk_alarm(AlarmStatus::ClearedAck, Some(now)))
        .await?;
    alarm_dao
        .save(&mk_alarm(AlarmStatus::ActiveUnack, None))
        .await?;
    let removed = alarm_dao.delete_cleared_older_than(cutoff).await?;
    assert!(removed >= 1);
    let page = alarm_dao
        .find_by_tenant(
            tenant.id,
            Some(entity_id),
            None,
            None,
            None,
            &PageLink::new(0, 10),
        )
        .await?;
    assert_eq!(page.total_elements, 2);

    // audit_log：save / find_by_tenant 过滤 / delete_older_than
    let entry_old = tb_dao::audit_log::AuditLog {
        id: Uuid::now_v7(),
        tenant_id: tenant.id,
        entity_id: Some(entity_id),
        entity_type: Some("DEVICE".into()),
        entity_name: Some("d".into()),
        user_id: None,
        user_email: None,
        action_type: "DELETED".into(),
        action_data: None,
        action_status: "SUCCESS".into(),
        created_time: old,
    };
    audit_dao.save(&entry_old).await?;
    audit_dao
        .save(&tb_dao::audit_log::AuditLog::new(
            tenant.id,
            Some((entity_id, "DEVICE".into(), "d".into())),
            None,
            None,
            "CREATED",
            None,
            "SUCCESS",
        ))
        .await?;
    let page = audit_dao
        .find_by_tenant(
            Some(tenant.id),
            &tb_dao::audit_log::AuditLogFilter {
                action_types: Some(vec!["CREATED".into()]),
                ..Default::default()
            },
            &PageLink::new(0, 10),
        )
        .await?;
    assert_eq!(page.total_elements, 1);
    assert_eq!(page.data[0].action_type, "CREATED");
    let removed = audit_dao.delete_older_than(cutoff).await?;
    assert!(removed >= 1);

    // 清理（audit_log / ts_kv 无外键级联，手动清）
    audit_dao.delete_older_than(now + day).await?;
    ts_dao.delete(entity_id, &["temp".into()], None).await?;
    for a in &page.data {
        alarm_dao.delete_by_id(a.id).await?;
    }
    tenant_dao.delete_by_id(tenant.id).await?;
    Ok(())
}

#[tokio::test]
async fn api_usage_state_accumulate() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let usage_dao = tb_dao::ApiUsageStateDao::new(pool.clone());
    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;

    let delta = tb_dao::UsageDelta {
        transport_msgs: 5,
        re_msgs: 2,
        rest_requests: 0,
    };
    usage_dao.add_delta(tenant.id, delta).await?;
    usage_dao.add_delta(tenant.id, delta).await?;
    let state = usage_dao
        .find_by_tenant(tenant.id)
        .await?
        .expect("usage state");
    assert_eq!(state.transport_msgs, 10);
    assert_eq!(state.re_msgs, 4);
    assert!(state.updated_at > 0);

    tenant_dao.delete_by_id(tenant.id).await?;
    assert!(usage_dao.find_by_tenant(tenant.id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn concurrent_alarm_ack_and_clear_do_not_lose_ack() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let alarm_dao = AlarmDao::new(pool);
    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;

    for _ in 0..16 {
        let now = tb_dao::models::Alarm {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            originator_id: Uuid::now_v7(),
            originator_type: EntityType::Device,
            r#type: "ConcurrentAlarm".into(),
            severity: AlarmSeverity::Major,
            status: AlarmStatus::ActiveUnack,
            start_ts: 1,
            end_ts: 1,
            ack_ts: None,
            clear_ts: None,
            details: None,
            propagate: false,
            propagate_to_owner: false,
            propagate_to_tenant: false,
            propagate_relation_types: None,
            customer_id: None,
            assignee_id: None,
            assign_ts: None,
        };
        let alarm = alarm_dao.save(&now).await?;
        let (ack_result, clear_result) =
            tokio::join!(alarm_dao.ack(alarm.id), alarm_dao.clear(alarm.id));
        clear_result.expect("clear must succeed or be idempotent");
        let stored = alarm_dao.find_by_id(alarm.id).await?.unwrap();
        assert!(stored.clear_ts.is_some());
        match ack_result {
            Ok(acked) => {
                assert_eq!(acked.status, AlarmStatus::ActiveAck);
                assert_eq!(stored.status, AlarmStatus::ClearedAck);
                assert!(stored.ack_ts.is_some());
            }
            Err(tb_common::TbError::InvalidInput(_)) => {
                assert_eq!(stored.status, AlarmStatus::ClearedUnack);
                assert!(stored.ack_ts.is_none());
            }
            Err(other) => panic!("unexpected concurrent ack failure: {other}"),
        }
        alarm_dao.delete_by_id(alarm.id).await?;
    }

    tenant_dao.delete_by_id(tenant.id).await?;
    Ok(())
}

#[tokio::test]
async fn key_resolution_works_with_a_single_connection() -> TbResult<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url())
        .await
        .map_err(tb_common::TbError::db)?;
    migrate(&pool).await.map_err(tb_common::TbError::db)?;
    let keys = tb_dao::KeyDictionaryDao::new(pool.clone());
    let key = uniq("single-connection-key");
    let (first, second, third) = tokio::join!(
        keys.get_or_create_id(&key),
        keys.get_or_create_id(&key),
        keys.get_or_create_id(&key)
    );
    let key_id = first?;
    assert_eq!(second?, key_id);
    assert_eq!(third?, key_id);

    let entity_id = Uuid::now_v7();
    let ts_dao = tb_dao::TimeseriesDao::new(pool.clone(), keys.clone());
    let attributes_dao = tb_dao::AttributesDao::new(pool.clone(), keys);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        ts_dao
            .save(
                entity_id,
                &[tb_common::TsKvEntry {
                    ts: 1000,
                    key: key.clone(),
                    value: tb_common::KvValue::Long(1),
                }],
            )
            .await?;
        attributes_dao
            .save(
                entity_id,
                tb_common::AttributeScope::ServerScope,
                &[tb_common::KvEntry {
                    key: key.clone(),
                    value: tb_common::KvValue::Long(2),
                }],
            )
            .await?;
        Ok::<(), tb_common::TbError>(())
    })
    .await
    .expect("key resolution must not borrow from a pool held by the transaction")?;

    ts_dao
        .delete(entity_id, std::slice::from_ref(&key), None)
        .await?;
    attributes_dao
        .delete(entity_id, tb_common::AttributeScope::ServerScope, &[key])
        .await?;
    Ok(())
}

#[tokio::test]
async fn ota_metadata_omits_data_and_chunks_read_only_requested_bytes() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let profile_dao = DeviceProfileDao::new(pool.clone());
    let ota_dao = tb_dao::OtaPackageDao::new(pool);
    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;
    let profile = profile_dao
        .save(&new_device_profile(tenant.id, &uniq("profile"), false))
        .await?;
    let data: Vec<u8> = (0..40000u32).map(|i| (i % 251) as u8).collect();
    let package = ota_dao
        .save(&OtaPackage {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            device_profile_id: profile.id,
            r#type: "FIRMWARE".into(),
            title: "fw".into(),
            version: "1".into(),
            tag: None,
            file_name: Some("fw.bin".into()),
            content_type: None,
            checksum_algorithm: None,
            checksum: None,
            size: Some(data.len() as i64),
            data: Some(data.clone()),
            additional_info: None,
        })
        .await?;

    let metadata = ota_dao.find_meta_by_id(package.id).await?.unwrap();
    assert!(metadata.data.is_none());
    let page = ota_dao
        .find_meta_by_tenant_filtered(
            tenant.id,
            Some(profile.id),
            Some("FIRMWARE"),
            &PageLink::new(0, 10),
        )
        .await?;
    assert!(page.data.iter().all(|p| p.data.is_none()));
    let chunk = ota_dao
        .find_data_chunk(package.id, 16385, 16384)
        .await?
        .unwrap();
    assert_eq!(chunk, data[16384..32768]);
    let beyond = ota_dao
        .find_data_chunk(package.id, 40001, 1024)
        .await?
        .unwrap();
    assert!(beyond.is_empty());

    ota_dao.delete_by_id(package.id).await?;
    tenant_dao.delete_by_id(tenant.id).await?;
    Ok(())
}

#[tokio::test]
async fn ota_bindings_track_references_and_inheriting_devices() -> TbResult<()> {
    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let profile_dao = DeviceProfileDao::new(pool.clone());
    let device_dao = DeviceDao::new(pool.clone());
    let ota_dao = tb_dao::OtaPackageDao::new(pool);
    let tenant = tenant_dao.save(&new_tenant(&uniq("tenant"))).await?;
    let mut profile = profile_dao
        .save(&new_device_profile(tenant.id, &uniq("profile"), false))
        .await?;
    let package = ota_dao
        .save(&OtaPackage {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            device_profile_id: profile.id,
            r#type: "SOFTWARE".into(),
            title: "sw".into(),
            version: "1".into(),
            tag: None,
            file_name: None,
            content_type: None,
            checksum_algorithm: None,
            checksum: None,
            size: Some(1),
            data: Some(vec![1]),
            additional_info: None,
        })
        .await?;
    assert!(!ota_dao.is_referenced(package.id).await?);

    let inheriting = device_dao
        .save(&new_device(tenant.id, profile.id, &uniq("inherits")))
        .await?;
    let mut bound = new_device(tenant.id, profile.id, &uniq("bound"));
    bound.software_id = Some(package.id);
    let bound = device_dao.save(&bound).await?;
    assert!(ota_dao.is_referenced(package.id).await?);
    assert_eq!(
        device_dao.find_ids_inheriting_ota(profile.id, true).await?,
        vec![inheriting.id]
    );
    let mut firmware_inheriting = device_dao
        .find_ids_inheriting_ota(profile.id, false)
        .await?;
    firmware_inheriting.sort();
    let mut all = vec![inheriting.id, bound.id];
    all.sort();
    assert_eq!(firmware_inheriting, all);

    device_dao.delete_by_id(bound.id).await?;
    assert!(!ota_dao.is_referenced(package.id).await?);
    profile.software_id = Some(package.id);
    profile = profile_dao.save(&profile).await?;
    assert!(ota_dao.is_referenced(package.id).await?);
    profile.software_id = None;
    profile_dao.save(&profile).await?;
    assert!(!ota_dao.is_referenced(package.id).await?);

    device_dao.delete_by_id(inheriting.id).await?;
    ota_dao.delete_by_id(package.id).await?;
    tenant_dao.delete_by_id(tenant.id).await?;
    Ok(())
}

async fn seed_entity_rows(pool: &sqlx::PgPool, tenant_id: Uuid, entity_id: Uuid, peer: Uuid) {
    let keys = tb_dao::KeyDictionaryDao::new(pool.clone());
    tb_dao::TimeseriesDao::new(pool.clone(), keys.clone())
        .save(entity_id, &[point("cleanup_temp", 1000, 1)])
        .await
        .unwrap();
    tb_dao::AttributesDao::new(pool.clone(), keys)
        .save(
            entity_id,
            tb_common::AttributeScope::ServerScope,
            &[tb_common::KvEntry {
                key: "cleanup_attr".into(),
                value: tb_common::KvValue::Bool(true),
            }],
        )
        .await
        .unwrap();
    for sql in [
        "INSERT INTO alarm (id, created_time, tenant_id, originator_id, originator_type, type, \
         severity, status, start_ts, end_ts) \
         VALUES (gen_random_uuid(), 0, $1, $2, 'DEVICE', 'cleanup', 'MINOR', 'ACTIVE_UNACK', 0, 0)",
        "INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type) \
         VALUES ($2, 'DEVICE', $3, 'ASSET', 'Contains')",
        "INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type) \
         VALUES ($3, 'ASSET', $2, 'DEVICE', 'Contains')",
        "INSERT INTO calculated_field (id, created_time, tenant_id, entity_id, name, configuration) \
         VALUES (gen_random_uuid(), 0, $1, $2, gen_random_uuid()::text, '{}'::jsonb)",
        "INSERT INTO rule_node_dedup_window (id, tenant_id, rule_chain_id, rule_node_id, \
         originator_id, strategy, out_msg_type, queue_name, max_retries, state, expires_at, \
         updated_at) VALUES (gen_random_uuid(), $1, gen_random_uuid(), gen_random_uuid(), $2, \
         'FIRST', '', 'Main', 3, 'DONE', 0, 0)",
    ] {
        sqlx::query(sql)
            .bind(tenant_id)
            .bind(entity_id)
            .bind(peer)
            .execute(pool)
            .await
            .unwrap();
    }
}

async fn entity_row_count(pool: &sqlx::PgPool, entity_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM ts_kv WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM ts_kv_latest WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM attribute_kv WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM alarm WHERE originator_id = $1) \
         + (SELECT COUNT(*) FROM relation WHERE from_id = $1 OR to_id = $1) \
         + (SELECT COUNT(*) FROM calculated_field WHERE entity_id = $1) \
         + (SELECT COUNT(*) FROM rule_node_dedup_window WHERE originator_id = $1)",
    )
    .bind(entity_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn tombstone_count(pool: &sqlx::PgPool, ids: &[Uuid]) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM entity_tombstone WHERE entity_id = ANY($1)")
        .bind(ids)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn entity_cleanup_purges_data_and_sweeps_late_writes() -> TbResult<()> {
    use tb_dao::entity_cleanup;

    let pool = setup().await;
    let tenant_dao = TenantDao::new(pool.clone());
    let profile_dao = DeviceProfileDao::new(pool.clone());
    let device_dao = DeviceDao::new(pool.clone());
    let tenant = tenant_dao.save(&new_tenant(&uniq("cleanup"))).await?;
    let profile = profile_dao
        .save(&new_device_profile(tenant.id, &uniq("profile"), false))
        .await?;
    let device = device_dao
        .save(&new_device(tenant.id, profile.id, &uniq("device")))
        .await?;
    assert!(matches!(
        profile_dao.delete_by_id(profile.id).await,
        Err(tb_common::TbError::Conflict(_))
    ));

    let peer = Uuid::now_v7();
    let deleted = [device.id, Uuid::now_v7(), Uuid::now_v7()];
    for id in deleted {
        seed_entity_rows(&pool, tenant.id, id, peer).await;
        assert_eq!(entity_row_count(&pool, id).await, 8);
    }
    sqlx::query(
        "INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type) \
         VALUES ($1, 'ASSET', $2, 'ASSET', 'Contains')",
    )
    .bind(peer)
    .bind(Uuid::now_v7())
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = pool.begin().await.unwrap();
    entity_cleanup::purge_deleted_entities(&mut tx, tenant.id, &deleted).await?;
    assert!(device_dao.delete_by_id_in(&mut tx, device.id).await?);
    tx.commit().await.unwrap();
    for id in deleted {
        assert_eq!(entity_row_count(&pool, id).await, 0);
    }
    assert_eq!(entity_row_count(&pool, peer).await, 1);
    assert_eq!(tombstone_count(&pool, &deleted).await, 3);

    for id in deleted {
        seed_entity_rows(&pool, tenant.id, id, peer).await;
    }
    let sweep = entity_cleanup::sweep_tombstones(&pool, 0, 1).await?;
    assert!(sweep.purged_rows >= 24, "{sweep:?}");
    for id in deleted {
        assert_eq!(entity_row_count(&pool, id).await, 0);
    }
    assert_eq!(entity_row_count(&pool, peer).await, 1);
    assert_eq!(tombstone_count(&pool, &deleted).await, 3);

    sqlx::query("UPDATE entity_tombstone SET deleted_at = 1 WHERE entity_id = ANY($1)")
        .bind(&deleted[..])
        .execute(&pool)
        .await
        .unwrap();
    let sweep = entity_cleanup::sweep_tombstones(&pool, 2, 1000).await?;
    assert!(sweep.expired >= 3, "{sweep:?}");
    assert_eq!(tombstone_count(&pool, &deleted).await, 0);

    assert!(profile_dao.delete_by_id(profile.id).await?);
    sqlx::query("DELETE FROM relation WHERE from_id = $1")
        .bind(peer)
        .execute(&pool)
        .await
        .unwrap();
    tenant_dao.delete_by_id(tenant.id).await?;
    Ok(())
}

fn point(key: &str, ts: i64, value: i64) -> tb_common::TsKvEntry {
    tb_common::TsKvEntry {
        ts,
        key: key.into(),
        value: tb_common::KvValue::Long(value),
    }
}

#[tokio::test]
async fn timeseries_delete_recomputes_latest() -> TbResult<()> {
    let pool = setup().await;
    let ts_dao = tb_dao::TimeseriesDao::new(pool.clone(), tb_dao::KeyDictionaryDao::new(pool));
    let entity_id = Uuid::now_v7();
    let (temp, hum) = (uniq("temp"), uniq("hum"));
    ts_dao
        .save(
            entity_id,
            &[
                point(&temp, 100, 1),
                point(&temp, 200, 2),
                point(&temp, 300, 3),
                point(&hum, 100, 50),
            ],
        )
        .await?;
    let latest = |key: String| {
        let ts_dao = &ts_dao;
        async move {
            let keys = [key];
            Ok::<_, tb_common::TbError>(
                ts_dao
                    .find_latest(entity_id, Some(&keys))
                    .await?
                    .into_iter()
                    .next(),
            )
        }
    };

    // 删除最新点：latest 回退到剩余最新点。
    let result = ts_dao
        .delete(entity_id, std::slice::from_ref(&temp), Some((250, 400)))
        .await?;
    assert_eq!(result.deleted, 1);
    assert_eq!(result.rewritten_latest, vec![point(&temp, 200, 2)]);
    assert!(result.removed_latest.is_empty());
    assert_eq!(latest(temp.clone()).await?, Some(point(&temp, 200, 2)));

    // 删除旧点：latest 不变，边界为闭区间。
    let result = ts_dao
        .delete(entity_id, std::slice::from_ref(&temp), Some((0, 100)))
        .await?;
    assert_eq!(result.deleted, 1);
    assert!(!result.latest_changed());
    assert_eq!(latest(temp.clone()).await?, Some(point(&temp, 200, 2)));

    // 范围覆盖全部剩余点：latest 删除。
    let result = ts_dao
        .delete(entity_id, std::slice::from_ref(&temp), Some((200, 200)))
        .await?;
    assert_eq!(result.deleted, 1);
    assert_eq!(result.removed_latest, vec![temp.clone()]);
    assert_eq!(latest(temp.clone()).await?, None);

    // 全量删除。
    let result = ts_dao
        .delete(entity_id, std::slice::from_ref(&hum), None)
        .await?;
    assert_eq!(result.deleted, 1);
    assert_eq!(result.removed_latest, vec![hum.clone()]);
    assert_eq!(latest(hum.clone()).await?, None);

    let err = ts_dao
        .delete(entity_id, std::slice::from_ref(&temp), Some((10, 5)))
        .await
        .unwrap_err();
    assert!(matches!(err, tb_common::TbError::InvalidInput(_)));
    Ok(())
}

#[tokio::test]
async fn timeseries_delete_and_concurrent_save_keep_latest_consistent() -> TbResult<()> {
    let pool = setup().await;
    let ts_dao = tb_dao::TimeseriesDao::new(pool.clone(), tb_dao::KeyDictionaryDao::new(pool));
    let key = uniq("concurrent");
    for round in 0..20 {
        let entity_id = Uuid::now_v7();
        ts_dao
            .save(entity_id, &[point(&key, 100, 1), point(&key, 200, 2)])
            .await?;
        let new_ts = if round % 2 == 0 { 150 } else { 500 };
        let concurrent = [point(&key, new_ts, 9)];
        let (deleted, saved) = tokio::join!(
            ts_dao.delete(entity_id, std::slice::from_ref(&key), Some((150, 300))),
            ts_dao.save(entity_id, &concurrent)
        );
        deleted?;
        saved?;
        let history = ts_dao
            .find_history(entity_id, &key, i64::MIN, i64::MAX, 10, false)
            .await?;
        let latest = ts_dao
            .find_latest(entity_id, Some(std::slice::from_ref(&key)))
            .await?;
        assert_eq!(
            latest.first(),
            history.first(),
            "round {round}: latest must equal the newest remaining point"
        );
        ts_dao
            .delete(entity_id, std::slice::from_ref(&key), None)
            .await?;
    }
    Ok(())
}
