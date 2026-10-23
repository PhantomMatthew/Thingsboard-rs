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
            r#type: "HighTemperature".into(),
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
        .find_by_tenant(tenant.id, Some(originator), None, None, None, &PageLink::new(0, 10))
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
