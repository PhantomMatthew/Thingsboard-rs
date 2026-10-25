use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tb_auth::oauth2::Oauth2LoginRequest;
use tb_auth::{AuthService, JwtTokenFactory, TokenType};
use tb_common::{TbError, TbResult};
use tb_dao::models::{Authority, Oauth2Client, Tenant, User, UserCredentials};
use tb_dao::{Oauth2ClientDao, TenantDao, UserCredentialsDao, UserDao, migrate};
use uuid::Uuid;

const OLD_PASSWORD: &str = "old-password";
const NEW_PASSWORD: &str = "new-password";

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://thingsboard:thingsboard@localhost:5433/thingsboard".into())
}

async fn setup() -> PgPool {
    let pool = PgPool::connect(&db_url()).await.unwrap();
    migrate(&pool).await.unwrap();
    pool
}

fn auth_service(pool: &PgPool) -> Arc<AuthService> {
    AuthService::new(
        pool.clone(),
        Arc::new(JwtTokenFactory::new(
            "auth-integration-test-secret",
            900,
            604800,
        )),
    )
}

async fn create_user(pool: &PgPool, enabled: bool) -> TbResult<(User, UserCredentials)> {
    let tenant = TenantDao::new(pool.clone())
        .save(&Tenant {
            id: Uuid::now_v7(),
            created_time: 0,
            title: format!("auth-test-{}", Uuid::now_v7().simple()),
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
        })
        .await?;
    let user = UserDao::new(pool.clone())
        .save(&User {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: tenant.id,
            customer_id: None,
            authority: Authority::TenantAdmin,
            email: format!("{}@auth-test.local", Uuid::now_v7().simple()),
            first_name: None,
            last_name: None,
            phone: None,
            additional_info: None,
        })
        .await?;
    let credentials = UserCredentialsDao::new(pool.clone())
        .save(&UserCredentials {
            id: Uuid::now_v7(),
            created_time: 0,
            user_id: user.id,
            enabled,
            password: Some(tb_auth::password::hash_password(OLD_PASSWORD)?),
            activate_token: None,
            reset_token: None,
            failed_login_attempts: 0,
            last_login_ts: None,
            password_changed_ts: None,
        })
        .await?;
    Ok((user, credentials))
}

async fn delete_user_tenant(pool: &PgPool, user: &User) -> TbResult<()> {
    TenantDao::new(pool.clone())
        .delete_by_id(user.tenant_id)
        .await?;
    Ok(())
}

#[tokio::test]
async fn disabled_credentials_reject_refresh_and_access_tokens() -> TbResult<()> {
    let pool = setup().await;
    let auth = auth_service(&pool);
    let (user, mut credentials) = create_user(&pool, true).await?;
    let pair = auth.issue_tokens(&user)?;

    credentials.enabled = false;
    UserCredentialsDao::new(pool.clone())
        .save(&credentials)
        .await?;

    let err = auth.refresh(&pair.refresh_token).await.unwrap_err();
    assert!(matches!(err, TbError::PermissionDenied(_)), "{err:?}");
    let err = auth.get_user_from_token(&pair.token).await.unwrap_err();
    assert!(matches!(err, TbError::PermissionDenied(_)), "{err:?}");

    delete_user_tenant(&pool, &user).await
}

#[tokio::test]
async fn password_change_rejects_old_refresh_and_accepts_new_login() -> TbResult<()> {
    let pool = setup().await;
    let auth = auth_service(&pool);
    let (user, _) = create_user(&pool, true).await?;
    let old_pair = auth.issue_tokens(&user)?;

    tokio::time::sleep(Duration::from_millis(2)).await;
    auth.change_password(user.id, OLD_PASSWORD, NEW_PASSWORD)
        .await?;
    let credentials = UserCredentialsDao::new(pool.clone())
        .find_by_user_id(user.id)
        .await?
        .expect("credentials after password change");
    assert!(credentials.password_changed_ts.is_some());

    let err = auth.refresh(&old_pair.refresh_token).await.unwrap_err();
    assert!(matches!(err, TbError::Unauthenticated(_)), "{err:?}");
    let err = auth.get_user_from_token(&old_pair.token).await.unwrap_err();
    assert!(matches!(err, TbError::Unauthenticated(_)), "{err:?}");

    let new_pair = auth.login(&user.email, NEW_PASSWORD).await?;
    auth.factory()
        .parse_typed(&new_pair.token, TokenType::Access)?;
    auth.get_user_from_token(&new_pair.token).await?;
    let after_login = UserCredentialsDao::new(pool.clone())
        .find_by_user_id(user.id)
        .await?
        .expect("credentials after new login");
    assert_eq!(after_login.password, credentials.password);
    assert_eq!(
        after_login.password_changed_ts,
        credentials.password_changed_ts
    );
    let credentials_dao = UserCredentialsDao::new(pool.clone());
    let stale = credentials_dao
        .upsert_password(
            user.id,
            after_login.password.as_deref().unwrap(),
            credentials.password_changed_ts.unwrap() - 1,
        )
        .await?;
    assert_eq!(stale.password_changed_ts, credentials.password_changed_ts);
    let concurrent_reset = credentials_dao
        .record_login_success(
            user.id,
            chrono::Utc::now().timestamp_millis(),
            "not-the-current-password-hash",
        )
        .await?;
    assert!(concurrent_reset.is_none());
    assert!(auth.login(&user.email, OLD_PASSWORD).await.is_err());

    delete_user_tenant(&pool, &user).await
}

#[tokio::test]
async fn login_lockout_is_shared_across_instances_and_restarts() -> TbResult<()> {
    let pool = setup().await;
    let (user, _) = create_user(&pool, true).await?;
    let instances = [auth_service(&pool), auth_service(&pool)];

    // 未达上限时成功登录清零计数。
    for i in 0..4 {
        let err = instances[i % 2]
            .login(&user.email, "wrong-password")
            .await
            .unwrap_err();
        assert!(matches!(err, TbError::Unauthenticated(_)), "{err:?}");
    }
    instances[0].login(&user.email, OLD_PASSWORD).await?;

    // 交替实例连续 5 次失败：两个实例及新实例（重启）连正确密码也拒绝。
    for i in 0..5 {
        let err = instances[i % 2]
            .login(&user.email, "wrong-password")
            .await
            .unwrap_err();
        assert!(matches!(err, TbError::Unauthenticated(_)), "{err:?}");
    }
    for auth in [&instances[0], &instances[1], &auth_service(&pool)] {
        let err = auth.login(&user.email, OLD_PASSWORD).await.unwrap_err();
        assert!(matches!(err, TbError::RateLimitExceeded(_)), "{err:?}");
    }

    // 锁定期过后恢复。
    sqlx::query("UPDATE user_credentials SET locked_until = 1 WHERE user_id = $1")
        .bind(user.id)
        .execute(&pool)
        .await
        .unwrap();
    instances[1].login(&user.email, OLD_PASSWORD).await?;

    delete_user_tenant(&pool, &user).await
}

#[tokio::test]
async fn concurrent_wrong_passwords_cannot_exceed_attempt_budget() -> TbResult<()> {
    let pool = setup().await;
    let (user, _) = create_user(&pool, true).await?;
    let instances = [auth_service(&pool), auth_service(&pool)];

    let mut attempts = tokio::task::JoinSet::new();
    for i in 0..20 {
        let auth = instances[i % 2].clone();
        let email = user.email.clone();
        attempts.spawn(async move { auth.login(&email, "wrong-password").await });
    }
    let (mut verified, mut locked) = (0, 0);
    while let Some(result) = attempts.join_next().await {
        match result.unwrap().unwrap_err() {
            TbError::Unauthenticated(_) => verified += 1,
            TbError::RateLimitExceeded(_) => locked += 1,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(verified <= 5, "{verified} wrong passwords were verified");
    assert_eq!(verified + locked, 20);
    let err = instances[0]
        .login(&user.email, OLD_PASSWORD)
        .await
        .unwrap_err();
    assert!(matches!(err, TbError::RateLimitExceeded(_)), "{err:?}");

    delete_user_tenant(&pool, &user).await
}

#[tokio::test]
async fn access_token_uses_current_identity_and_rejects_deleted_user() -> TbResult<()> {
    let pool = setup().await;
    let auth = auth_service(&pool);
    let (mut user, _) = create_user(&pool, true).await?;
    let pair = auth.issue_tokens(&user)?;

    let customer = tb_dao::CustomerDao::new(pool.clone())
        .save(&tb_dao::models::Customer {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: user.tenant_id,
            title: "auth integration customer".into(),
            country: None,
            state: None,
            city: None,
            address: None,
            address2: None,
            zip: None,
            phone: None,
            email: None,
            additional_info: None,
        })
        .await?;
    user.authority = Authority::CustomerUser;
    user.customer_id = Some(customer.id);
    UserDao::new(pool.clone()).save(&user).await?;
    let current = auth.get_user_from_token(&pair.token).await?;
    assert_eq!(current.authority, Authority::CustomerUser);
    assert_eq!(current.customer_id, user.customer_id);

    UserDao::new(pool.clone()).delete_by_id(user.id).await?;
    let err = auth.get_user_from_token(&pair.token).await.unwrap_err();
    assert!(matches!(err, TbError::Unauthenticated(_)), "{err:?}");
    let err = auth.refresh(&pair.refresh_token).await.unwrap_err();
    assert!(matches!(err, TbError::Unauthenticated(_)), "{err:?}");

    delete_user_tenant(&pool, &user).await
}

#[tokio::test]
async fn oauth2_rejects_mismatched_redirect_uri_before_code_exchange() -> TbResult<()> {
    let pool = setup().await;
    let auth = auth_service(&pool);
    let client_dao = Oauth2ClientDao::new(pool.clone());
    let client = client_dao
        .save(&Oauth2Client {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: None,
            title: "auth integration test".into(),
            client_id: format!("client-{}", Uuid::now_v7().simple()),
            client_secret: None,
            authorization_uri: "https://idp.example/authorize".into(),
            token_uri: "http://127.0.0.1:1/token".into(),
            user_info_uri: "http://127.0.0.1:1/userinfo".into(),
            redirect_uri_template: Some("https://app.example/callback".into()),
            scope: None,
            enabled: true,
        })
        .await?;

    let err = auth
        .oauth2_login(&Oauth2LoginRequest {
            client_id: client.client_id.clone(),
            code: "unused".into(),
            redirect_uri: "https://evil.example/callback".into(),
        })
        .await
        .unwrap_err();
    assert!(matches!(err, TbError::InvalidInput(_)), "{err:?}");

    client_dao.delete_by_id(client.id).await?;
    Ok(())
}
