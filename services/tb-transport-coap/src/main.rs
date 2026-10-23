//! tb-transport-coap 可执行入口。

use tb_transport_coap::TransportState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_transport_coap=info,tb_transport_api=info".into()),
        )
        .init();

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let node_id = std::env::var("TB_NODE_ID").unwrap_or_else(|_| uuid::Uuid::now_v7().to_string());
    let bind = std::env::var("TB_COAP_BIND").unwrap_or_else(|_| "0.0.0.0:5683".into());

    let socket = tokio::net::UdpSocket::bind(&bind).await?;
    tracing::info!(bind, node_id, "tb-transport-coap listening");
    let state = TransportState::new(&brokers, &node_id)?;
    tb_transport_coap::serve(state, socket).await?;
    Ok(())
}
