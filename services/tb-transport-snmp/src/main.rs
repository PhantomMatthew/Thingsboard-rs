use std::sync::Arc;

use tb_transport_api::TransportApiClient;
use tb_transport_snmp::{SnmpRunner, TransportApiPublisher, discover_devices};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_transport_snmp=info,tb_transport_api=info".into()),
        )
        .init();

    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let node_id = std::env::var("TB_NODE_ID").unwrap_or_else(|_| uuid::Uuid::now_v7().to_string());
    let client = TransportApiClient::new(&brokers, &node_id)?;
    let producer = tb_queue::KafkaQueueProducer::new(&brokers, &format!("tb-snmp-{node_id}"))?;
    let runner = Arc::new(SnmpRunner::new(TransportApiPublisher::with_activity(
        client.clone(),
        producer,
        node_id.clone(),
    )));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let started = discover_devices(&client, runner, shutdown_rx).await?;
    tracing::info!(
        node_id,
        devices = started,
        "tb-transport-snmp v2c polling started"
    );
    tokio::signal::ctrl_c().await?;
    Ok(())
}
