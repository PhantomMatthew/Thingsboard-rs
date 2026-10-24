//! tb-housekeeper：周期清理过期数据（ts_kv / 调试事件 / 已清除告警 / 终态去重窗口）。
//!
//! 环境变量：
//! - TB_DATABASE_URL（默认 postgres://thingsboard:thingsboard@localhost:5433/thingsboard）
//! - TB_HOUSEKEEPER_INTERVAL_SEC（默认 3600）
//! - TB_TS_TTL_DAYS（默认 0=不清理 ts_kv）
//! - TB_DEBUG_EVENT_TTL_DAYS（默认 7）
//! - TB_ALARM_TTL_DAYS（默认 0=不清理；仅清 CLEARED_* 且 clear_ts 早于阈值的告警）
//! - TB_DEDUP_WINDOW_TTL_DAYS（默认 7；仅清 DONE 窗口）

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const DEFAULT_DATABASE_URL: &str = "postgres://thingsboard:thingsboard@localhost:5433/thingsboard";
const DAY_MS: i64 = 24 * 3600 * 1000;

struct Config {
    database_url: String,
    interval: Duration,
    ts_ttl_days: i64,
    debug_event_ttl_days: i64,
    alarm_ttl_days: i64,
    dedup_window_ttl_days: i64,
}

impl Config {
    fn from_env() -> Self {
        let env_i64 = |key: &str, default: i64| {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Self {
            database_url: std::env::var("TB_DATABASE_URL")
                .unwrap_or_else(|_| DEFAULT_DATABASE_URL.into()),
            interval: Duration::from_secs(
                env_i64("TB_HOUSEKEEPER_INTERVAL_SEC", 3600).max(1) as u64
            ),
            ts_ttl_days: env_i64("TB_TS_TTL_DAYS", 0),
            debug_event_ttl_days: env_i64("TB_DEBUG_EVENT_TTL_DAYS", 7),
            alarm_ttl_days: env_i64("TB_ALARM_TTL_DAYS", 0),
            dedup_window_ttl_days: env_i64("TB_DEDUP_WINDOW_TTL_DAYS", 7),
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn cutoff(now: i64, ttl_days: i64) -> i64 {
    now.saturating_sub(ttl_days.saturating_mul(DAY_MS))
}

/// 跑一轮清理，返回各类删除行数。
async fn cleanup_round(config: &Config, pool: &PgPool) -> (u64, u64, u64, u64) {
    let now = now_ms();
    let mut ts_deleted = 0;
    if config.ts_ttl_days > 0 {
        match tb_housekeeper::cleanup_ts_kv(pool, cutoff(now, config.ts_ttl_days)).await {
            Ok(n) => ts_deleted = n,
            Err(e) => tracing::error!("ts_kv cleanup failed: {e}"),
        }
    }
    let mut debug_deleted = 0;
    if config.debug_event_ttl_days > 0 {
        match tb_housekeeper::cleanup_debug_events(pool, cutoff(now, config.debug_event_ttl_days))
            .await
        {
            Ok(n) => debug_deleted = n,
            Err(e) => tracing::error!("debug event cleanup failed: {e}"),
        }
    }
    let mut alarm_deleted = 0;
    if config.alarm_ttl_days > 0 {
        match tb_housekeeper::cleanup_cleared_alarms(pool, cutoff(now, config.alarm_ttl_days)).await
        {
            Ok(n) => alarm_deleted = n,
            Err(e) => tracing::error!("alarm cleanup failed: {e}"),
        }
    }
    let mut dedup_deleted = 0;
    if config.dedup_window_ttl_days > 0 {
        match tb_housekeeper::cleanup_dedup_windows(pool, cutoff(now, config.dedup_window_ttl_days))
            .await
        {
            Ok(n) => dedup_deleted = n,
            Err(e) => tracing::error!("deduplication window cleanup failed: {e}"),
        }
    }
    tracing::info!(
        ts_kv_deleted = ts_deleted,
        debug_event_deleted = debug_deleted,
        alarm_deleted = alarm_deleted,
        dedup_window_deleted = dedup_deleted,
        "housekeeper round done"
    );
    (ts_deleted, debug_deleted, alarm_deleted, dedup_deleted)
}

async fn run(config: Config, pool: PgPool) {
    let mut ticker = tokio::time::interval(config.interval);
    loop {
        ticker.tick().await;
        cleanup_round(&config, &pool).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_housekeeper=info".into()),
        )
        .init();
    let config = Config::from_env();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await?;
    tb_dao::migrate(&pool).await?;
    tracing::info!(
        interval_sec = config.interval.as_secs(),
        ts_ttl_days = config.ts_ttl_days,
        debug_event_ttl_days = config.debug_event_ttl_days,
        alarm_ttl_days = config.alarm_ttl_days,
        dedup_window_ttl_days = config.dedup_window_ttl_days,
        "tb-housekeeper started"
    );
    run(config, pool).await;
    Ok(())
}
