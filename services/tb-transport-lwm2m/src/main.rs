use std::net::SocketAddr;
use std::sync::Arc;

use futures::future::BoxFuture;
use tb_common::TbResult;
use tb_transport_api::TransportApiClient;
use tb_transport_lwm2m::integration::{
    CoreSessionActivity, EndpointIdentity, EndpointValidator, TransportApiUplink,
};
use tb_transport_lwm2m::objects::standard_resource_types;
use tb_transport_lwm2m::server::{Lwm2mServer, ServerConfig};

struct CoreEndpointValidator {
    client: TransportApiClient,
}

impl EndpointValidator for CoreEndpointValidator {
    fn validate<'a>(
        &'a self,
        endpoint: &'a str,
        _peer: SocketAddr,
    ) -> BoxFuture<'a, TbResult<Option<EndpointIdentity>>> {
        Box::pin(async move {
            let response = self.client.validate_lwm2m(endpoint).await?;
            if !response.valid {
                return Ok(None);
            }
            let (Some(device), Some(profile)) = (response.device, response.profile) else {
                return Ok(None);
            };
            Ok(Some(EndpointIdentity {
                device,
                profile,
                resource_types: standard_resource_types(),
            }))
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tb_transport_lwm2m=info,tb_transport_api=info".into()),
        )
        .init();
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    let bind = std::env::var("TB_LWM2M_BIND").unwrap_or_else(|_| "0.0.0.0:5685".into());
    let node_id = std::env::var("TB_NODE_ID")
        .unwrap_or_else(|_| format!("lwm2m-{}", &uuid::Uuid::now_v7().simple().to_string()[..8]));
    let client = TransportApiClient::new(&brokers, &node_id)?;
    let producer = tb_queue::KafkaQueueProducer::new(&brokers, &format!("tb-lwm2m-{node_id}"))?;
    let validator = Arc::new(CoreEndpointValidator {
        client: client.clone(),
    });
    let uplink = Arc::new(TransportApiUplink::new(client));
    let activity = Arc::new(CoreSessionActivity::new(producer, node_id.clone()));
    let bind_addr: SocketAddr = bind.parse()?;
    tracing::info!(%bind_addr, node_id, "tb-transport-lwm2m listening (NoSec/TLV/SINGLE)");
    let server = Lwm2mServer::new(
        ServerConfig {
            bind_addr,
            node_id,
            ..ServerConfig::default()
        },
        validator,
        uplink,
        activity,
    )?;
    server.run().await?;
    Ok(())
}
