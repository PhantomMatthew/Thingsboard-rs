//! 租户级令牌桶限流：容量=rate（1s 突发），按时间 refill。

use std::time::Instant;

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
        let cap = self.max_per_sec as f64;
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
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
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
}
