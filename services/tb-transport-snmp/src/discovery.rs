use std::sync::Arc;

use tb_common::TbResult;
use tb_transport_api::TransportApiClient;
use tokio::sync::watch;

use crate::config::{DeviceTransportConfiguration, ProfileTransportConfiguration};
use crate::runner::SnmpRunner;

pub async fn discover_devices(
    client: &TransportApiClient,
    runner: Arc<SnmpRunner>,
    shutdown: watch::Receiver<bool>,
) -> TbResult<usize> {
    let devices = client
        .get_devices_by_transport(None, "SNMP", 0, 1000)
        .await?
        .devices;
    let mut started = 0_usize;
    for device in devices {
        let device_data = match serde_json::from_str(&device.device_data_json) {
            Ok(data) => data,
            Err(error) => {
                tracing::warn!(
                    device_id = device.device_id,
                    ?error,
                    "invalid SNMP device JSON"
                );
                continue;
            }
        };
        let device_config = match DeviceTransportConfiguration::from_device_data(&device_data) {
            Ok(config) => config,
            Err(error) => {
                tracing::warn!(
                    device_id = device.device_id,
                    ?error,
                    "invalid SNMP device config"
                );
                continue;
            }
        };
        let profile = match client.get_device_profile(&device.device_profile_id).await {
            Ok(profile) => profile,
            Err(error) => {
                tracing::warn!(
                    device_id = device.device_id,
                    ?error,
                    "SNMP profile lookup failed"
                );
                continue;
            }
        };
        let profile_data = match serde_json::from_str(&profile.profile_data_json) {
            Ok(data) => data,
            Err(error) => {
                tracing::warn!(
                    device_id = device.device_id,
                    ?error,
                    "invalid SNMP profile JSON"
                );
                continue;
            }
        };
        let profile_config = match ProfileTransportConfiguration::from_profile_data(&profile_data) {
            Ok(config) => config,
            Err(error) => {
                tracing::warn!(
                    device_id = device.device_id,
                    ?error,
                    "invalid SNMP profile config"
                );
                continue;
            }
        };
        let runner = runner.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            runner
                .run_device(device, device_config, profile_config, shutdown)
                .await;
        });
        started += 1;
    }
    Ok(started)
}
