//! API 用量统计：内存计数器（DashMap）+ 每 60s 累加落库 api_usage_state。
//! 维度：telemetry 写入条数（REST 直接计数，规则引擎经 tb_usage_stats 计入 transport_msgs）、
//! 规则引擎消息数（经 tb_usage_stats 计入 re_msgs，旧直通 ingest 保留原有计数）、
//! REST 请求数（middleware 按 JWT tenant 计入，仅内存不落库）。

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::Response;
use dashmap::DashMap;
use serde::Serialize;
use tb_auth::TokenType;
use tb_dao::UsageDelta;
use uuid::Uuid;

use crate::AppState;

const FLUSH_INTERVAL: Duration = Duration::from_secs(60);

/// REST 请求计数 middleware：按 access token 的 tenant 归类；无/坏 token 不计（也不拦截）。
pub async fn count_rest_request(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let tenant_id = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .and_then(|t| state.jwt_factory.parse_typed(t, TokenType::Access).ok())
        .and_then(|c| c.to_security_user().ok())
        .map(|u| u.tenant_id);
    if let Some(tenant_id) = tenant_id {
        state.usage.inc_rest_request(tenant_id);
    }
    next.run(req).await
}

#[derive(Debug, Default)]
struct Counters {
    transport_msgs: i64,
    re_msgs: i64,
    rest_requests: i64,
}

#[derive(Clone, Default)]
pub struct UsageTracker {
    counters: Arc<DashMap<Uuid, Counters>>,
}

/// GET /api/usage/{tenantId} 响应：DB 累计 + 内存未落库部分。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageReport {
    pub tenant_id: Uuid,
    pub transport_msgs: i64,
    pub re_msgs: i64,
    pub rest_requests: i64,
}

impl UsageTracker {
    pub fn inc_transport_msgs(&self, tenant_id: Uuid, n: i64) {
        if tenant_id.is_nil() || n <= 0 {
            return;
        }
        self.counters.entry(tenant_id).or_default().transport_msgs += n;
    }

    pub fn inc_re_msgs(&self, tenant_id: Uuid, n: i64) {
        if tenant_id.is_nil() || n <= 0 {
            return;
        }
        self.counters.entry(tenant_id).or_default().re_msgs += n;
    }

    pub fn inc_rest_request(&self, tenant_id: Uuid) {
        if tenant_id.is_nil() {
            return;
        }
        self.counters.entry(tenant_id).or_default().rest_requests += 1;
    }

    /// 合并 DB 累计与内存未落库计数。
    pub async fn report(
        &self,
        state: &AppState,
        tenant_id: Uuid,
    ) -> tb_common::TbResult<UsageReport> {
        let stored = state.api_usage_state_dao.find_by_tenant(tenant_id).await?;
        let pending = self.counters.get(&tenant_id);
        let (p_transport, p_re, p_rest) = pending
            .as_ref()
            .map(|c| (c.transport_msgs, c.re_msgs, c.rest_requests))
            .unwrap_or_default();
        Ok(UsageReport {
            tenant_id,
            transport_msgs: stored.as_ref().map(|s| s.transport_msgs).unwrap_or(0) + p_transport,
            re_msgs: stored.as_ref().map(|s| s.re_msgs).unwrap_or(0) + p_re,
            rest_requests: p_rest,
        })
    }

    fn snapshot(&self) -> Vec<(Uuid, UsageDelta)> {
        self.counters
            .iter()
            .map(|e| {
                (
                    *e.key(),
                    UsageDelta {
                        transport_msgs: e.transport_msgs,
                        re_msgs: e.re_msgs,
                        rest_requests: e.rest_requests,
                    },
                )
            })
            .collect()
    }

    fn subtract_snapshot(&self, tenant_id: Uuid, delta: UsageDelta) {
        if let Some(mut c) = self.counters.get_mut(&tenant_id) {
            c.transport_msgs -= delta.transport_msgs;
            c.re_msgs -= delta.re_msgs;
            c.rest_requests -= delta.rest_requests;
        }
    }

    async fn flush(&self, state: &AppState) {
        for (tenant_id, delta) in self.snapshot() {
            if delta.transport_msgs == 0 && delta.re_msgs == 0 && delta.rest_requests == 0 {
                continue;
            }
            let result = if delta.transport_msgs == 0 && delta.re_msgs == 0 {
                Ok(())
            } else {
                state.api_usage_state_dao.add_delta(tenant_id, delta).await
            };
            match result {
                Ok(()) => self.subtract_snapshot(tenant_id, delta),
                Err(e) => {
                    tracing::warn!(%tenant_id, "usage flush failed, counters kept: {e}")
                }
            }
        }
    }

    /// 周期性把内存计数累加进 api_usage_state（rest_requests 不落库，仅清零）。
    pub fn start_flush(self, state: AppState) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
            ticker.tick().await; // 跳过首次立即触发
            loop {
                ticker.tick().await;
                self.flush(&state).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_requests_use_snapshot_subtract_without_losing_new_counts() {
        let tracker = UsageTracker::default();
        let tenant_id = Uuid::now_v7();
        tracker.inc_rest_request(tenant_id);
        tracker.inc_rest_request(tenant_id);
        let snapshot = tracker.snapshot();
        let (_, delta) = snapshot
            .into_iter()
            .find(|(id, _)| *id == tenant_id)
            .unwrap();
        assert_eq!(delta.rest_requests, 2);

        tracker.inc_rest_request(tenant_id);
        tracker.subtract_snapshot(tenant_id, delta);
        assert_eq!(tracker.counters.get(&tenant_id).unwrap().rest_requests, 1);
    }
}
