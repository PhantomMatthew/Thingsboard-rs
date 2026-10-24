//! tb-core：REST API 服务（租户/客户/用户/设备/资产等实体管理）。
//! 开发环境 CORS 默认仅允许 http://localhost:3000；部署时用 TB_CORS_ALLOWED_ORIGINS
//! 配置逗号分隔的允许来源。

pub mod audit;
pub mod auth_middleware;
pub mod calculated;
pub mod claim;
pub mod core_consumer;
pub mod device_session;
pub mod edqs;
pub mod routes;
pub mod rule_engine_events;
pub mod seed;
pub mod state;
pub mod telemetry_cache;
pub mod telemetry_ingest;
pub mod transport_api;
pub mod usage;

use axum::Router;
use axum::http::HeaderValue;
use tb_auth::AuthState;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

pub use state::AppState;

const DEFAULT_CORS_ALLOWED_ORIGINS: &str = "http://localhost:3000";

fn parse_cors_origins(raw: &str) -> Vec<HeaderValue> {
    raw.split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .filter_map(|origin| match HeaderValue::from_str(origin) {
            Ok(value) => Some(value),
            Err(e) => {
                tracing::warn!(%origin, "ignoring invalid CORS origin: {e}");
                None
            }
        })
        .collect()
}

fn cors_layer() -> CorsLayer {
    let configured = std::env::var("TB_CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| DEFAULT_CORS_ALLOWED_ORIGINS.into());
    CorsLayer::new()
        .allow_origin(parse_cors_origins(&configured))
        .allow_methods(Any)
        .allow_headers(Any)
}

/// 组装完整 HTTP 路由（含 /api/auth 子服务与通用 layer）。
pub fn build_router(state: AppState) -> Router {
    let auth_state = AuthState::new(state.auth_service.clone(), state.jwt_factory.clone())
        .with_audit_tx(state.audit_tx.clone());
    Router::new()
        .merge(routes::api_router())
        .nest_service("/api/auth", tb_auth::router(auth_state))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            usage::count_rest_request,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(cors_layer())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cors_origins_are_comma_separated_and_validated() {
        let origins = parse_cors_origins(
            " http://localhost:3000, https://console.example.com,invalid\norigin ",
        );
        assert_eq!(origins.len(), 2);
        assert_eq!(origins[0], "http://localhost:3000");
        assert_eq!(origins[1], "https://console.example.com");
    }
}
