//! tb-core：REST API 服务（租户/客户/用户/设备/资产等实体管理）。

pub mod auth_middleware;
pub mod routes;
pub mod seed;
pub mod state;
pub mod telemetry_cache;
pub mod telemetry_ingest;
pub mod transport_api;

use axum::Router;
use tb_auth::AuthState;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

pub use state::AppState;

/// 组装完整 HTTP 路由（含 /api/auth 子服务与通用 layer）。
pub fn build_router(state: AppState) -> Router {
    let auth_state = AuthState::new(state.auth_service.clone(), state.jwt_factory.clone());
    Router::new()
        .merge(routes::api_router())
        .nest_service("/api/auth", tb_auth::router(auth_state))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state)
}
