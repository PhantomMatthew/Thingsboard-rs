//! 启动种子数据（幂等）：默认 tenant profile、系统租户 + sysadmin。

use serde_json::json;
use tb_common::{TbError, TbResult};
use tb_dao::models::{Authority, Tenant, TenantProfile, User, UserCredentials};
use uuid::Uuid;

use crate::AppState;

pub const SYSADMIN_EMAIL: &str = "sysadmin@thingsboard.org";
pub const SYSADMIN_DEFAULT_PASSWORD: &str = "sysadmin";
pub const MIN_PRODUCTION_SYSADMIN_PASSWORD_BYTES: usize = 12;

fn sysadmin_password_from_config(production: bool, configured: Option<String>) -> TbResult<String> {
    let configured = configured.filter(|value| !value.trim().is_empty());
    if production {
        return configured
            .filter(|value| value.len() >= MIN_PRODUCTION_SYSADMIN_PASSWORD_BYTES)
            .ok_or_else(|| {
                TbError::InvalidInput(
                    "TB_SYSADMIN_PASSWORD is required and must be at least 12 bytes in production"
                        .into(),
                )
            });
    }
    Ok(configured.unwrap_or_else(|| SYSADMIN_DEFAULT_PASSWORD.into()))
}

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
        let sysadmin_password = sysadmin_password_from_config(
            tb_auth::token::is_production_env(),
            std::env::var("TB_SYSADMIN_PASSWORD").ok(),
        )?;
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
            password: Some(tb_auth::password::hash_password(&sysadmin_password)?),
            activate_token: None,
            reset_token: None,
            failed_login_attempts: 0,
            last_login_ts: None,
            password_changed_ts: None,
        };
        state.user_credentials_dao.save(&credentials).await?;
        tracing::info!(email = SYSADMIN_EMAIL, "seeded system tenant and sysadmin");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_sysadmin_password_is_explicit_and_strong() {
        for password in [None, Some(String::new()), Some("sysadmin".into())] {
            assert!(sysadmin_password_from_config(true, password).is_err());
        }
        assert_eq!(
            sysadmin_password_from_config(true, Some("production-secret".into())).unwrap(),
            "production-secret"
        );
        assert_eq!(
            sysadmin_password_from_config(false, None).unwrap(),
            SYSADMIN_DEFAULT_PASSWORD
        );
    }
}
