use std::sync::Arc;
use std::time::Duration;

use tb_transport_api::TransportApiClient;
use tb_transport_snmp::{SnmpDeviceManager, SnmpRunner, TransportApiPublisher};

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
    let refresh_interval = Duration::from_secs(
        std::env::var("TB_SNMP_DISCOVERY_INTERVAL_SEC")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60)
            .max(1),
    );
    let client = TransportApiClient::new(&brokers, &node_id)?;
    let producer = tb_queue::KafkaQueueProducer::new(&brokers, &format!("tb-snmp-{node_id}"))?;
    let runner = Arc::new(SnmpRunner::new(TransportApiPublisher::with_activity(
        client.clone(),
        producer,
        node_id.clone(),
    )));
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let manager = SnmpDeviceManager::new(client, runner, shutdown_rx);
    tracing::info!(
        node_id,
        refresh_interval_sec = refresh_interval.as_secs(),
        "tb-transport-snmp v2c polling started"
    );
    tokio::select! {
        _ = manager.run(refresh_interval) => {}
        result = tokio::signal::ctrl_c() => result?,
    }
    Ok(())
}
