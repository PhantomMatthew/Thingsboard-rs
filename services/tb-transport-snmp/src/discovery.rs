//! SNMP 设备发现：分页拉取全部 SNMP 设备，周期刷新时启动新设备、重启配置变化的设备、停止已删除设备。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tb_common::TbResult;
use tb_proto::transport::DeviceInfoProto;
use tb_transport_api::TransportApiClient;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{DeviceTransportConfiguration, ProfileTransportConfiguration};
use crate::runner::SnmpRunner;

pub const DEFAULT_DISCOVERY_PAGE_SIZE: i32 = 1000;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RefreshReport {
    pub started: usize,
    pub restarted: usize,
    pub stopped: usize,
    pub running: usize,
}

struct RunningDevice {
    device: DeviceInfoProto,
    profile_data_json: String,
    task: JoinHandle<()>,
}

struct ReadyDevice {
    device: DeviceInfoProto,
    profile_data_json: String,
    device_config: DeviceTransportConfiguration,
    profile_config: ProfileTransportConfiguration,
}

enum Resolved {
    Ready(Box<ReadyDevice>),
    /// 设备或 Profile 配置无效，已在运行的轮询须停止。
    Invalid,
    /// Profile 暂时取不到，保持现状等下一轮。
    Unavailable,
}

pub struct SnmpDeviceManager {
    client: TransportApiClient,
    runner: Arc<SnmpRunner>,
    shutdown: watch::Receiver<bool>,
    page_size: i32,
    running: HashMap<String, RunningDevice>,
}

impl SnmpDeviceManager {
    pub fn new(
        client: TransportApiClient,
        runner: Arc<SnmpRunner>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            client,
            runner,
            shutdown,
            page_size: DEFAULT_DISCOVERY_PAGE_SIZE,
            running: HashMap::new(),
        }
    }

    pub fn with_page_size(mut self, page_size: i32) -> Self {
        assert!(page_size > 0);
        self.page_size = page_size;
        self
    }

    pub fn running_devices(&self) -> usize {
        self.running.len()
    }

    /// 拉取全部 SNMP 设备并对齐运行中的轮询；拉取失败时不改动现有轮询。
    pub async fn refresh(&mut self) -> TbResult<RefreshReport> {
        let devices = self.list_devices().await?;
        let mut profiles = HashMap::new();
        let mut seen = HashSet::new();
        let mut report = RefreshReport::default();
        for device in devices {
            let device_id = device.device_id.clone();
            seen.insert(device_id.clone());
            match self.resolve(device, &mut profiles).await {
                Resolved::Ready(ready) => {
                    match self.running.get(&device_id) {
                        Some(running)
                            if running.device == ready.device
                                && running.profile_data_json == ready.profile_data_json
                                && !running.task.is_finished() =>
                        {
                            continue;
                        }
                        Some(_) => report.restarted += 1,
                        None => report.started += 1,
                    }
                    if let Some(previous) = self.running.remove(&device_id) {
                        previous.task.abort();
                    }
                    let ReadyDevice {
                        device,
                        profile_data_json,
                        device_config,
                        profile_config,
                    } = *ready;
                    let task = tokio::spawn(self.runner.clone().run_device(
                        device.clone(),
                        device_config,
                        profile_config,
                        self.shutdown.clone(),
                    ));
                    self.running.insert(
                        device_id,
                        RunningDevice {
                            device,
                            profile_data_json,
                            task,
                        },
                    );
                }
                Resolved::Invalid => {
                    if let Some(previous) = self.running.remove(&device_id) {
                        previous.task.abort();
                        report.stopped += 1;
                    }
                }
                Resolved::Unavailable => {}
            }
        }
        self.running.retain(|device_id, running| {
            if seen.contains(device_id) {
                return true;
            }
            running.task.abort();
            report.stopped += 1;
            false
        });
        report.running = self.running.len();
        Ok(report)
    }

    /// 按 `interval` 周期刷新直到 shutdown，退出时停止全部轮询。
    pub async fn run(mut self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut shutdown = self.shutdown.clone();
        loop {
            tokio::select! {
                _ = ticker.tick() => match self.refresh().await {
                    Ok(report) => tracing::info!(
                        started = report.started,
                        restarted = report.restarted,
                        stopped = report.stopped,
                        running = report.running,
                        "SNMP device refresh"
                    ),
                    Err(error) => tracing::warn!(?error, "SNMP device refresh failed"),
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
    }

    async fn list_devices(&self) -> TbResult<Vec<DeviceInfoProto>> {
        let mut devices = Vec::new();
        let mut ids = HashSet::new();
        let mut page = 0;
        loop {
            let response = self
                .client
                .get_devices_by_transport(None, "SNMP", page, self.page_size)
                .await?;
            let empty = response.devices.is_empty();
            for device in response.devices {
                if ids.insert(device.device_id.clone()) {
                    devices.push(device);
                }
            }
            if !response.has_next || empty {
                return Ok(devices);
            }
            page += 1;
        }
    }

    async fn resolve(
        &self,
        device: DeviceInfoProto,
        profiles: &mut HashMap<String, Option<String>>,
    ) -> Resolved {
        let device_data = match serde_json::from_str::<Value>(&device.device_data_json) {
            Ok(data) => data,
            Err(error) => {
                tracing::warn!(
                    device_id = device.device_id,
                    ?error,
                    "invalid SNMP device JSON"
                );
                return Resolved::Invalid;
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
                return Resolved::Invalid;
            }
        };
        let profile_data_json = match profiles.get(&device.device_profile_id) {
            Some(cached) => cached.clone(),
            None => {
                let fetched = match self
                    .client
                    .get_device_profile(&device.device_profile_id)
                    .await
                {
                    Ok(profile) => Some(profile.profile_data_json),
                    Err(error) => {
                        tracing::warn!(
                            device_id = device.device_id,
                            ?error,
                            "SNMP profile lookup failed"
                        );
                        None
                    }
                };
                profiles.insert(device.device_profile_id.clone(), fetched.clone());
                fetched
            }
        };
        let Some(profile_data_json) = profile_data_json else {
            return Resolved::Unavailable;
        };
        let profile_data = match serde_json::from_str::<Value>(&profile_data_json) {
            Ok(data) => data,
            Err(error) => {
                tracing::warn!(
                    device_id = device.device_id,
                    ?error,
                    "invalid SNMP profile JSON"
                );
                return Resolved::Invalid;
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
                return Resolved::Invalid;
            }
        };
        Resolved::Ready(Box::new(ReadyDevice {
            device,
            profile_data_json,
            device_config,
            profile_config,
        }))
    }
}

impl Drop for SnmpDeviceManager {
    fn drop(&mut self) {
        for (_, running) in self.running.drain() {
            running.task.abort();
        }
    }
}
