//! 启动种子数据（幂等）：默认 tenant profile、系统租户 + sysadmin。

use serde_json::json;
use tb_common::TbResult;
use tb_dao::models::{Authority, Tenant, TenantProfile, User, UserCredentials};
use uuid::Uuid;

use crate::AppState;

pub const SYSADMIN_EMAIL: &str = "sysadmin@thingsboard.org";
pub const SYSADMIN_DEFAULT_PASSWORD: &str = "sysadmin";

pub async fn seed(state: &AppState) -> TbResult<()> {
    if state.tenant_profile_dao.find_default().await?.is_none() {
        let profile = TenantProfile {
            id: Uuid::now_v7(),
            created_time: 0,
            name: "Default".into(),
            description: Some("Default tenant profile".into()),
            is_default: true,
            profile_data: json!({}),
        };
        state.tenant_profile_dao.save(&profile).await?;
        tracing::info!("seeded default tenant profile");
    }

    if state
        .user_dao
        .find_by_email(SYSADMIN_EMAIL)
        .await?
        .is_none()
    {
        let tenant = Tenant {
            id: Uuid::now_v7(),
            created_time: 0,
            title: "System".into(),
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
            tenant_profile_id: state.tenant_profile_dao.find_default().await?.map(|p| p.id),
        };
        state.tenant_dao.save(&tenant).await?;

        let user = User {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            customer_id: None,
            authority: Authority::SysAdmin,
            email: SYSADMIN_EMAIL.into(),
            first_name: Some("System".into()),
            last_name: Some("Administrator".into()),
            phone: None,
            additional_info: None,
        };
        state.user_dao.save(&user).await?;

        let credentials = UserCredentials {
            id: Uuid::now_v7(),
            created_time: 0,
            user_id: user.id,
            enabled: true,
            password: Some(tb_auth::password::hash_password(SYSADMIN_DEFAULT_PASSWORD)?),
            activate_token: None,
            reset_token: None,
            failed_login_attempts: 0,
            last_login_ts: None,
        };
        state.user_credentials_dao.save(&credentials).await?;
        tracing::info!(email = SYSADMIN_EMAIL, "seeded system tenant and sysadmin");
    }
    Ok(())
}
