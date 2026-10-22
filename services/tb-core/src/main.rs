//! tb-core 可执行入口：读环境变量 → 连库 → 迁移 → 种子 → 起服务。

use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use tb_auth::JwtTokenFactory;
use tb_core::{AppState, build_router};

const DEFAULT_DATABASE_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/thingsboard";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_core=info,tb_auth=info,tower_http=info".into()),
        )
        .init();

    let database_url =
        std::env::var("TB_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.into());
    let port: u16 = std::env::var("TB_HTTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8080);

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await?;
    tb_dao::migrate(&pool).await?;
    tracing::info!("migrations applied");

    let state = AppState::new(pool, Arc::new(JwtTokenFactory::from_env()));
    tb_core::seed::seed(&state).await?;

    let transport_api = tb_core::transport_api::start_transport_api(state.clone()).await;
    // Phase 4 起遥测落库由 tb-rule-engine 接管；TB_INGEST_ENABLED=true 时才开旧直通 ingest。
    let ingest_enabled = std::env::var("TB_INGEST_ENABLED")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let ingest = if ingest_enabled {
        Some(tb_core::telemetry_ingest::start_telemetry_ingest(state.clone()).await)
    } else {
        None
    };

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "tb-core listening");
    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    transport_api.abort();
    if let Some(ingest) = ingest {
        ingest.abort();
    }
    Ok(())
}
