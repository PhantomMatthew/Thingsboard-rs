//! 浏览器 CORS 策略，tb-core 与 tb-auth 共用：默认仅允许 http://localhost:3000，
//! 部署时用 TB_CORS_ALLOWED_ORIGINS 配置逗号分隔的允许来源。

use axum::http::HeaderValue;
use tower_http::cors::{Any, CorsLayer};

pub const DEFAULT_CORS_ALLOWED_ORIGINS: &str = "http://localhost:3000";

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

fn cors_layer_for(raw: &str) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(parse_cors_origins(raw))
        .allow_methods(Any)
        .allow_headers(Any)
}

pub fn cors_layer() -> CorsLayer {
    let configured = std::env::var("TB_CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| DEFAULT_CORS_ALLOWED_ORIGINS.into());
    cors_layer_for(&configured)
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

    #[tokio::test]
    async fn preflight_allows_only_configured_origins() {
        let app = axum::Router::new()
            .route("/api/auth/login", axum::routing::post(|| async { "ok" }))
            .layer(cors_layer_for("https://console.example.com"));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/api/auth/login", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let preflight = |origin: &'static str| {
            client
                .request(reqwest::Method::OPTIONS, &url)
                .header("Origin", origin)
                .header("Access-Control-Request-Method", "POST")
                .header(
                    "Access-Control-Request-Headers",
                    "authorization,content-type",
                )
                .send()
        };

        let allowed = preflight("https://console.example.com").await.unwrap();
        assert_eq!(
            allowed.headers()["access-control-allow-origin"],
            "https://console.example.com"
        );
        let denied = preflight("https://evil.example.com").await.unwrap();
        assert!(
            denied
                .headers()
                .get("access-control-allow-origin")
                .is_none()
        );
    }
}
