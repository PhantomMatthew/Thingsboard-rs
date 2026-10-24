//! tb-transport-mqtt 可执行入口。

use tb_transport_mqtt::{TransportState, serve};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_transport_mqtt=info,tb_transport_api=info".into()),
        )
        .init();

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let bind = std::env::var("TB_MQTT_BIND").unwrap_or_else(|_| "0.0.0.0:1883".into());
    let node_id = std::env::var("TB_NODE_ID").unwrap_or_else(|_| {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "node".into());
        let uuid = uuid::Uuid::now_v7().simple().to_string();
        format!("{hostname}-{}", &uuid[..8])
    });

    let state = TransportState::new(&brokers, &node_id)?;
    if let Ok(bind) = std::env::var("TB_MQTTS_BIND")
        && !bind.trim().is_empty()
    {
        let cert = std::env::var("TB_MQTT_TLS_CERT")?;
        let key = std::env::var("TB_MQTT_TLS_KEY")?;
        let acceptor = tb_transport_mqtt::tls::acceptor_from_files(&cert, &key)?;
        let listener = tokio::net::TcpListener::bind(&bind).await?;
        tracing::info!(bind, node_id, "tb-transport-mqtt TLS listening");
        let tls_state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = tb_transport_mqtt::tls::serve_tls(listener, tls_state, acceptor).await {
                tracing::error!("MQTTS listener failed: {e}");
            }
        });
    }
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(bind, node_id, "tb-transport-mqtt listening");
    serve(listener, state).await?;
    Ok(())
}
