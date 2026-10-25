//! tb-edqs 可执行入口：连库 → 全量快照 → 起 Kafka 消费 → HTTP 查询服务。

use sqlx::postgres::PgPoolOptions;
use tb_edqs::{EdqsState, build_router};

const DEFAULT_DATABASE_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/thingsboard";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_edqs=info,tb_queue=info".into()),
        )
        .init();

    let database_url =
        std::env::var("TB_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.into());
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let port: u16 = std::env::var("TB_EDQS_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8090);
    let group =
        std::env::var("TB_EDQS_GROUP").unwrap_or_else(|_| tb_edqs::consumer::DEFAULT_GROUP.into());

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let reconcile_secs: u64 = std::env::var("TB_EDQS_RECONCILE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600);
    let mut state = EdqsState::default().with_pool(pool.clone());
    if reconcile_secs > 0 {
        state = state.with_reconcile_interval(std::time::Duration::from_secs(reconcile_secs));
    }
    tb_edqs::snapshot::load_snapshot(&pool, &state).await?;
    let consumer =
        tb_edqs::consumer::start_consumer_for_group_checked(&brokers, &group, state.clone())
            .await?;

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "tb-edqs listening");
    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    consumer.abort();
    Ok(())
}
