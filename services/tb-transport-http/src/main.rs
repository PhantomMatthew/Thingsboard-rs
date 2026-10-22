//! tb-transport-http 可执行入口。

use tb_transport_api::TransportApiClient;
use tb_transport_http::{TransportState, build_router};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_transport_http=info,tb_transport_api=info".into()),
        )
        .init();

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let node_id = std::env::var("TB_NODE_ID").unwrap_or_else(|_| uuid::Uuid::now_v7().to_string());
    let port: u16 = std::env::var("TB_TRANSPORT_HTTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8088);

    let transport_api = TransportApiClient::new(&brokers, &node_id)?;
    let state = TransportState { transport_api };

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, node_id, "tb-transport-http listening");
    axum::serve(listener, build_router(state)).await?;
    Ok(())
}
