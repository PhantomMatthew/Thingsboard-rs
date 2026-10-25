//! 租户级令牌桶限流：容量=rate（1s 突发），按时间 refill。
//!
//! - `RateLimiter`：进程内，每个传输实例独立计数。
//! - `SharedRateLimiter`：Redis 上按租户共享，所有传输实例合计不超过 rate。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use dashmap::DashMap;

struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

pub struct RateLimiter {
    max_per_sec: u32,
    buckets: DashMap<String, TokenBucket>,
}

impl RateLimiter {
    pub fn new(max_per_sec: u32) -> Self {
        Self {
            max_per_sec,
            buckets: DashMap::new(),
        }
    }

    /// 取一个令牌；桶空返回 false（超限）。
    pub fn check(&self, key: &str) -> bool {
        self.check_n(key, 1)
    }

    /// 取 count 枚令牌；不足时不扣减并返回 false。
    pub fn check_n(&self, key: &str, count: u32) -> bool {
        if count == 0 {
            return true;
        }
        let cap = self.max_per_sec as f64;
        let count = f64::from(count);
        let mut bucket = self
            .buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket {
                tokens: cap,
                last_refill: Instant::now(),
            });
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * cap).min(cap);
        bucket.last_refill = now;
        if bucket.tokens >= count {
            bucket.tokens -= count;
            true
        } else {
            false
        }
    }
}

const SHARED_KEY_PREFIX: &str = "tb:transport:rl:";
const LEASE_TTL: Duration = Duration::from_secs(1);
const LEASE_DIVISOR: u32 = 20;
const REDIS_TIMEOUT: Duration = Duration::from_millis(500);
const REDIS_BACKOFF: Duration = Duration::from_secs(5);

/// 按 Redis 服务器时间 refill，最多发放 want 枚，返回实际发放数。
const TAKE_TOKENS_LUA: &str = r#"
local rate = tonumber(ARGV[1])
local want = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = rate
  ts = now
end
tokens = math.min(rate, tokens + math.max(0, now - ts) * rate / 1000)
local granted = math.min(want, math.floor(tokens))
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens - granted), 'ts', now)
redis.call('PEXPIRE', KEYS[1], 2000)
return granted
"#;

struct Lease {
    tokens: u32,
    expires_at: Instant,
}

/// 各实例按批（rate/20）从共享桶预取令牌并在本地消费；预取令牌 1 秒后作废，
/// 因此全部实例的消耗之和不超过共享桶的发放量。
pub struct SharedRateLimiter {
    client: redis::Client,
    conn: tokio::sync::OnceCell<redis::aio::ConnectionManager>,
    script: redis::Script,
    max_per_sec: u32,
    lease_size: u32,
    leases: DashMap<String, Lease>,
    backoff_until: Mutex<Option<Instant>>,
}

impl SharedRateLimiter {
    pub fn new(client: redis::Client, max_per_sec: u32) -> Self {
        assert!(max_per_sec > 0);
        Self {
            client,
            conn: tokio::sync::OnceCell::new(),
            script: redis::Script::new(TAKE_TOKENS_LUA),
            max_per_sec,
            lease_size: (max_per_sec / LEASE_DIVISOR).max(1),
            leases: DashMap::new(),
            backoff_until: Mutex::new(None),
        }
    }

    /// 取 count 枚令牌。None 表示 Redis 不可用（失败后退避 5 秒），调用方只按本地限流处理。
    pub async fn check_n(&self, key: &str, count: u32) -> Option<bool> {
        if count == 0 {
            return Some(true);
        }
        let now = Instant::now();
        if let Some(mut lease) = self.leases.get_mut(key)
            && lease.expires_at > now
            && lease.tokens >= count
        {
            lease.tokens -= count;
            return Some(true);
        }
        if self.in_backoff(now) {
            return None;
        }
        let granted = match tokio::time::timeout(REDIS_TIMEOUT, self.take(key, count)).await {
            Ok(Ok(granted)) => granted,
            Ok(Err(error)) => {
                self.start_backoff(now, &error.to_string());
                return None;
            }
            Err(_) => {
                self.start_backoff(now, "timeout");
                return None;
            }
        };
        let mut lease = self.leases.entry(key.to_string()).or_insert(Lease {
            tokens: 0,
            expires_at: now,
        });
        if lease.expires_at <= now {
            lease.tokens = 0;
        }
        lease.tokens = lease.tokens.saturating_add(granted);
        lease.expires_at = now + LEASE_TTL;
        if lease.tokens >= count {
            lease.tokens -= count;
            Some(true)
        } else {
            Some(false)
        }
    }

    async fn take(&self, key: &str, count: u32) -> redis::RedisResult<u32> {
        let conn = self
            .conn
            .get_or_try_init(|| redis::aio::ConnectionManager::new(self.client.clone()))
            .await?;
        self.script
            .key(format!("{SHARED_KEY_PREFIX}{key}"))
            .arg(self.max_per_sec)
            .arg(count.max(self.lease_size))
            .invoke_async(&mut conn.clone())
            .await
    }

    fn in_backoff(&self, now: Instant) -> bool {
        let backoff = self.backoff_until.lock().unwrap_or_else(|e| e.into_inner());
        backoff.is_some_and(|until| now < until)
    }

    fn start_backoff(&self, now: Instant, reason: &str) {
        *self.backoff_until.lock().unwrap_or_else(|e| e.into_inner()) = Some(now + REDIS_BACKOFF);
        tracing::warn!(
            reason,
            "shared transport rate limiter unavailable; using per-instance limit only"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn burst_up_to_capacity_then_rejected() {
        let limiter = RateLimiter::new(3);
        for _ in 0..3 {
            assert!(limiter.check("t1"));
        }
        assert!(!limiter.check("t1"));
    }

    #[test]
    fn keys_are_independent() {
        let limiter = RateLimiter::new(1);
        assert!(limiter.check("t1"));
        assert!(limiter.check("t2"));
        assert!(!limiter.check("t1"));
    }

    #[test]
    fn refill_after_interval() {
        let limiter = RateLimiter::new(20);
        for _ in 0..20 {
            assert!(limiter.check("t1"));
        }
        assert!(!limiter.check("t1"));
        std::thread::sleep(std::time::Duration::from_millis(120));
        assert!(limiter.check("t1"), "0.12s 应 refill ~2 个令牌");
    }

    #[test]
    fn batch_charge_is_all_or_nothing() {
        let limiter = RateLimiter::new(3);
        assert!(limiter.check_n("t1", 2));
        assert!(!limiter.check_n("t1", 2));
        assert!(limiter.check("t1"), "批量扣费失败不应消耗剩余令牌");
        assert!(!limiter.check("t1"));
    }

    fn redis_client() -> redis::Client {
        let url =
            std::env::var("TB_REDIS_URL").unwrap_or_else(|_| "redis://localhost:16379".into());
        redis::Client::open(url).unwrap()
    }

    #[tokio::test]
    async fn shared_budget_is_split_across_instances() {
        let tenant = uuid::Uuid::now_v7().to_string();
        let node_a = SharedRateLimiter::new(redis_client(), 40);
        let node_b = SharedRateLimiter::new(redis_client(), 40);
        let mut allowed = 0;
        for _ in 0..60 {
            for node in [&node_a, &node_b] {
                if node.check_n(&tenant, 1).await.expect("redis available") {
                    allowed += 1;
                }
            }
        }
        assert!(
            (38..=44).contains(&allowed),
            "two instances must share one 40-token budget, allowed {allowed}"
        );
        assert_eq!(node_a.check_n(&tenant, 41).await, Some(false));
    }

    #[tokio::test]
    async fn shared_budget_refills() {
        let tenant = uuid::Uuid::now_v7().to_string();
        let limiter = SharedRateLimiter::new(redis_client(), 20);
        while limiter.check_n(&tenant, 1).await == Some(true) {}
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(limiter.check_n(&tenant, 1).await, Some(true));
    }

    #[tokio::test]
    async fn unavailable_redis_backs_off_without_blocking() {
        let limiter =
            SharedRateLimiter::new(redis::Client::open("redis://127.0.0.1:1").unwrap(), 5);
        assert_eq!(limiter.check_n("tenant", 1).await, None);
        let started = Instant::now();
        assert_eq!(limiter.check_n("tenant", 1).await, None);
        assert!(started.elapsed() < Duration::from_millis(50));
    }
}
