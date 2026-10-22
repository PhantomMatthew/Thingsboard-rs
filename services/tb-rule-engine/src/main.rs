//! tb-rule-engine 可执行入口：读环境变量 → 连库 → 迁移 → 消费规则引擎队列。

use sqlx::postgres::PgPoolOptions;

const DEFAULT_DATABASE_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/thingsboard";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_rule_engine=info,tb_queue=info".into()),
        )
        .init();

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let database_url =
        std::env::var("TB_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DATABASE_URL.into());
    let queue = std::env::var("TB_RE_QUEUE").unwrap_or_else(|_| "Main".into());

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    tb_dao::migrate(&pool).await?;
    tracing::info!("migrations applied");

    let handle = tb_rule_engine_server::start_rule_engine(&brokers, pool, &queue).await;
    tokio::signal::ctrl_c().await?;
    handle.abort();
    Ok(())
}
