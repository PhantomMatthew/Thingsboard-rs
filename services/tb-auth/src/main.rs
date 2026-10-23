//! tb-auth 独立可执行：连库、迁移、起 HTTP 服务。

use std::sync::Arc;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tb_auth::{AuthService, AuthState, JwtTokenFactory};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub const DEFAULT_DATABASE_URL: &str =
    "postgres://thingsboard:thingsboard@localhost:5433/thingsboard";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_auth=info,tower_http=info".into()),
        )
        .init();

    let database_url =
        std::env::var("TB_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.into());
    let port: u16 = std::env::var("TB_HTTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8081);

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    tb_dao::migrate(&pool).await?;
    tracing::info!("migrations applied");

    let factory = Arc::new(JwtTokenFactory::from_env());
    let auth_service = AuthService::new(pool, factory.clone());
    let auth_state = AuthState::new(auth_service, factory);
    let app = axum::Router::new()
        .nest("/api/auth", tb_auth::router(auth_state.clone()))
        .nest("/api/oauth2", tb_auth::oauth2::router(auth_state))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "tb-auth listening");
    axum::serve(listener, app).await?;
    Ok(())
}
