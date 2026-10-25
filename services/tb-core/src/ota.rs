//! OTA 显式绑定解析与状态回写（对齐 Java OtaPackageStateService）。
//!
//! 生效包 = 设备级绑定，缺省时取档案级绑定；无绑定即无包，不再回退到"最新上传"。
//! 生效包变化时：先经 transport 通知刷新会话包缓存，再写 `target_{fw,sw}_*` 与
//! `{fw,sw}_state`（QUEUED → INITIATED）遥测、`{fw,sw}_*` shared 属性并通知设备；
//! 解绑时删除 `{fw,sw}_*` shared 属性。两类通知都以 session_id 为 key 发往同一
//! notifications topic，transport 按序处理，设备拿到新属性时缓存已是新包。

use bytes::Bytes;
use prost::Message as _;
use tb_common::{AttributeScope, KvEntry, KvValue, TbError, TbResult, TsKvEntry};
use tb_dao::models::{Device, DeviceProfile, OtaPackage};
use tb_proto::common::{self as common_pb, OtaPackageType};
use tb_proto::core as core_pb;
use tb_queue::{QueueProducer as _, TbTopic};
use uuid::Uuid;

use crate::AppState;
use crate::telemetry_cache;

pub const STATE_QUEUED: &str = "QUEUED";
pub const STATE_INITIATED: &str = "INITIATED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtaKind {
    Firmware,
    Software,
}

impl OtaKind {
    pub const ALL: [OtaKind; 2] = [OtaKind::Firmware, OtaKind::Software];

    pub fn from_proto(value: i32) -> Option<Self> {
        match OtaPackageType::try_from(value) {
            Ok(OtaPackageType::Firmware) => Some(Self::Firmware),
            Ok(OtaPackageType::Software) => Some(Self::Software),
            _ => None,
        }
    }

    pub fn proto(self) -> OtaPackageType {
        match self {
            Self::Firmware => OtaPackageType::Firmware,
            Self::Software => OtaPackageType::Software,
        }
    }

    /// `ota_package.type` 列取值。
    pub fn type_name(self) -> &'static str {
        match self {
            Self::Firmware => "FIRMWARE",
            Self::Software => "SOFTWARE",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Firmware => "fw",
            Self::Software => "sw",
        }
    }

    pub fn device_binding(self, device: &Device) -> Option<Uuid> {
        match self {
            Self::Firmware => device.firmware_id,
            Self::Software => device.software_id,
        }
    }

    pub fn set_device_binding(self, device: &mut Device, id: Option<Uuid>) {
        match self {
            Self::Firmware => device.firmware_id = id,
            Self::Software => device.software_id = id,
        }
    }

    pub fn profile_binding(self, profile: &DeviceProfile) -> Option<Uuid> {
        match self {
            Self::Firmware => profile.firmware_id,
            Self::Software => profile.software_id,
        }
    }

    pub fn set_profile_binding(self, profile: &mut DeviceProfile, id: Option<Uuid>) {
        match self {
            Self::Firmware => profile.firmware_id = id,
            Self::Software => profile.software_id = id,
        }
    }
}

/// 设备当前生效的绑定 id：设备级优先于档案级。
pub fn effective_binding(
    kind: OtaKind,
    device: &Device,
    profile: Option<&DeviceProfile>,
) -> Option<Uuid> {
    kind.device_binding(device)
        .or_else(|| profile.and_then(|p| kind.profile_binding(p)))
}

/// 包可绑定到该租户的该档案：类型一致、目标档案一致且有数据。
pub fn validate_binding(
    package: &OtaPackage,
    kind: OtaKind,
    tenant_id: Uuid,
    profile_id: Uuid,
) -> TbResult<()> {
    if package.tenant_id != tenant_id {
        return Err(TbError::InvalidInput(format!(
            "ota package {} belongs to another tenant",
            package.id
        )));
    }
    if package.r#type != kind.type_name() {
        return Err(TbError::InvalidInput(format!(
            "ota package {} is {}, expected {}",
            package.id,
            package.r#type,
            kind.type_name()
        )));
    }
    if package.device_profile_id != profile_id {
        return Err(TbError::InvalidInput(format!(
            "ota package {} targets another device profile",
            package.id
        )));
    }
    if package.size.unwrap_or(0) <= 0 {
        return Err(TbError::InvalidInput(format!(
            "ota package {} has no data",
            package.id
        )));
    }
    Ok(())
}

/// 设备换档案时，设备级绑定必须兼容新档案。
pub async fn validate_device_bindings(state: &AppState, device: &Device) -> TbResult<()> {
    for kind in OtaKind::ALL {
        let Some(id) = kind.device_binding(device) else {
            continue;
        };
        let package = state
            .ota_package_dao
            .find_meta_by_id(id)
            .await?
            .ok_or_else(|| TbError::NotFound(format!("ota package {id}")))?;
        validate_binding(&package, kind, device.tenant_id, device.device_profile_id)?;
    }
    Ok(())
}

/// 设备当前生效的包元数据；悬空或不兼容的绑定视为无包。
pub async fn assigned_package(
    state: &AppState,
    device: &Device,
    kind: OtaKind,
) -> TbResult<Option<OtaPackage>> {
    let binding = match kind.device_binding(device) {
        Some(id) => Some(id),
        None => state
            .device_profile_dao
            .find_by_id(device.device_profile_id)
            .await?
            .and_then(|profile| kind.profile_binding(&profile)),
    };
    let Some(id) = binding else {
        return Ok(None);
    };
    let Some(package) = state.ota_package_dao.find_meta_by_id(id).await? else {
        tracing::warn!(device_id = %device.id, ota_package_id = %id, "bound ota package is missing");
        return Ok(None);
    };
    if let Err(e) = validate_binding(&package, kind, device.tenant_id, device.device_profile_id) {
        tracing::warn!(device_id = %device.id, "ignoring incompatible ota binding: {e}");
        return Ok(None);
    }
    Ok(Some(package))
}

fn ts_entry(ts: i64, key: String, value: KvValue) -> TsKvEntry {
    TsKvEntry { ts, key, value }
}

pub fn state_entry(kind: OtaKind, ts: i64, status: &str) -> TsKvEntry {
    ts_entry(
        ts,
        format!("{}_state", kind.prefix()),
        KvValue::Str(status.to_string()),
    )
}

/// 指派时写入的目标遥测：target_{p}_title/version/tag?/ts 与 {p}_state=QUEUED。
pub fn target_telemetry(package: &OtaPackage, kind: OtaKind, ts: i64) -> Vec<TsKvEntry> {
    let p = kind.prefix();
    let mut entries = vec![
        ts_entry(
            ts,
            format!("target_{p}_title"),
            KvValue::Str(package.title.clone()),
        ),
        ts_entry(
            ts,
            format!("target_{p}_version"),
            KvValue::Str(package.version.clone()),
        ),
    ];
    if let Some(tag) = package.tag.as_ref().filter(|tag| !tag.is_empty()) {
        entries.push(ts_entry(
            ts,
            format!("target_{p}_tag"),
            KvValue::Str(tag.clone()),
        ));
    }
    entries.push(ts_entry(ts, format!("target_{p}_ts"), KvValue::Long(ts)));
    entries.push(state_entry(kind, ts, STATE_QUEUED));
    entries
}

/// 推给设备的 shared 属性：{p}_title/version/tag?/size/checksum_algorithm?/checksum?。
pub fn shared_attributes(package: &OtaPackage, kind: OtaKind) -> Vec<KvEntry> {
    let p = kind.prefix();
    let entry = |key: &str, value: KvValue| KvEntry {
        key: format!("{p}_{key}"),
        value,
    };
    let mut entries = vec![
        entry("title", KvValue::Str(package.title.clone())),
        entry("version", KvValue::Str(package.version.clone())),
    ];
    if let Some(tag) = package.tag.as_ref().filter(|tag| !tag.is_empty()) {
        entries.push(entry("tag", KvValue::Str(tag.clone())));
    }
    entries.push(entry("size", KvValue::Long(package.size.unwrap_or(0))));
    if let Some(algorithm) = &package.checksum_algorithm {
        entries.push(entry("checksum_algorithm", KvValue::Str(algorithm.clone())));
    }
    if let Some(checksum) = &package.checksum {
        entries.push(entry("checksum", KvValue::Str(checksum.clone())));
    }
    entries
}

/// 该类型全部 shared 属性 key（解绑时整体删除）。
pub fn attribute_keys(kind: OtaKind) -> Vec<String> {
    let p = kind.prefix();
    [
        "title",
        "version",
        "tag",
        "url",
        "size",
        "checksum_algorithm",
        "checksum",
    ]
    .iter()
    .map(|key| format!("{p}_{key}"))
    .collect()
}

/// 生效包与 previous 不同时回写状态并通知设备；相同则不动。
pub async fn sync_device(
    state: &AppState,
    device: &Device,
    kind: OtaKind,
    previous: Option<Uuid>,
) -> TbResult<()> {
    let package = assigned_package(state, device, kind).await?;
    if package.as_ref().map(|p| p.id) == previous {
        return Ok(());
    }
    notify_transport(state, device.id, kind, package.as_ref()).await?;
    match &package {
        Some(package) => initiate(state, device, kind, package).await,
        None => {
            let deleted = remove_shared(state, device, &attribute_keys(kind)).await?;
            publish_shared(state, device, &[], deleted).await
        }
    }
}

/// 档案绑定从 previous 变化后，后台同步全部继承档案绑定的设备。
pub fn spawn_profile_sync(
    state: &AppState,
    profile_id: Uuid,
    kind: OtaKind,
    previous: Option<Uuid>,
) {
    let state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = sync_profile_devices(&state, profile_id, kind, previous).await {
            tracing::warn!(%profile_id, "ota profile sync failed: {e}");
        }
    });
}

async fn sync_profile_devices(
    state: &AppState,
    profile_id: Uuid,
    kind: OtaKind,
    previous: Option<Uuid>,
) -> TbResult<()> {
    let device_ids = state
        .device_dao
        .find_ids_inheriting_ota(profile_id, kind == OtaKind::Software)
        .await?;
    for device_id in device_ids {
        let Some(device) = state.device_dao.find_by_id(device_id).await? else {
            continue;
        };
        if let Err(e) = sync_device(state, &device, kind, previous).await {
            tracing::warn!(%device_id, "ota state sync failed: {e}");
        }
    }
    Ok(())
}

/// 新建设备继承档案绑定时，后台写入初始 OTA 状态。
pub fn spawn_new_device_sync(state: &AppState, device: &Device, profile: &DeviceProfile) {
    let kinds: Vec<OtaKind> = OtaKind::ALL
        .into_iter()
        .filter(|kind| effective_binding(*kind, device, Some(profile)).is_some())
        .collect();
    if kinds.is_empty() {
        return;
    }
    let state = state.clone();
    let device = device.clone();
    tokio::spawn(async move {
        for kind in kinds {
            if let Err(e) = sync_device(&state, &device, kind, None).await {
                tracing::warn!(device_id = %device.id, "ota state sync failed: {e}");
            }
        }
    });
}

async fn initiate(
    state: &AppState,
    device: &Device,
    kind: OtaKind,
    package: &OtaPackage,
) -> TbResult<()> {
    let ts = now_ms();
    save_telemetry(state, device, &target_telemetry(package, kind, ts)).await?;
    let attributes = shared_attributes(package, kind);
    let stale: Vec<String> = attribute_keys(kind)
        .into_iter()
        .filter(|key| attributes.iter().all(|a| &a.key != key))
        .collect();
    let deleted = remove_shared(state, device, &stale).await?;
    state
        .attributes_dao
        .save(device.id, AttributeScope::SharedScope, &attributes)
        .await?;
    publish_shared(state, device, &attributes, deleted).await?;
    save_telemetry(state, device, &[state_entry(kind, ts + 1, STATE_INITIATED)]).await
}

async fn save_telemetry(state: &AppState, device: &Device, entries: &[TsKvEntry]) -> TbResult<()> {
    state.timeseries_dao.save(device.id, entries).await?;
    telemetry_cache::update_latest(state, device.id, entries).await;
    crate::edqs::publish_latest(state, device.tenant_id, device.id, entries);
    Ok(())
}

/// 删除已存在的 shared 属性，返回实际删除的 key。
async fn remove_shared(
    state: &AppState,
    device: &Device,
    keys: &[String],
) -> TbResult<Vec<String>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let existing: Vec<String> = state
        .attributes_dao
        .find(device.id, AttributeScope::SharedScope, Some(keys))
        .await?
        .into_iter()
        .map(|entry| entry.key)
        .collect();
    if !existing.is_empty() {
        state
            .attributes_dao
            .delete(device.id, AttributeScope::SharedScope, &existing)
            .await?;
    }
    Ok(existing)
}

async fn publish_shared(
    state: &AppState,
    device: &Device,
    updated: &[KvEntry],
    deleted: Vec<String>,
) -> TbResult<()> {
    if updated.is_empty() && deleted.is_empty() {
        return Ok(());
    }
    let mut kvs = Vec::with_capacity(updated.len() + deleted.len());
    for entry in updated {
        let key_id = state
            .key_dictionary_dao
            .get_or_create_id(&entry.key)
            .await?;
        kvs.push(tb_proto::convert::kv_to_proto_named(
            key_id,
            &entry.key,
            &entry.value,
        ));
    }
    for key in &deleted {
        let key_id = state.key_dictionary_dao.get_or_create_id(key).await?;
        kvs.push(common_pb::KeyValueProto {
            key_id,
            key: key.clone(),
            value: None,
        });
    }
    crate::edqs::publish_attribute_update(
        state,
        device.tenant_id,
        device.id,
        AttributeScope::SharedScope,
        updated,
        deleted,
    );
    crate::routes::telemetry::notify_shared_attribute_update(
        state,
        tb_common::EntityType::Device,
        device.id,
        kvs,
    )
    .await;
    Ok(())
}

/// 向设备全部在线会话推 ota_update，供 transport 刷新会话包缓存。
async fn notify_transport(
    state: &AppState,
    device_id: Uuid,
    kind: OtaKind,
    package: Option<&OtaPackage>,
) -> TbResult<()> {
    let sessions = state.device_sessions.any_sessions(device_id).await?;
    if sessions.is_empty() {
        return Ok(());
    }
    let producer = state.queue_producer().await?;
    for session in sessions {
        let msg = core_pb::ToTransportMsg {
            session_id: session.session_id.clone(),
            entity_id: device_id.to_string(),
            msg: Some(core_pb::to_transport_msg::Msg::OtaUpdate(
                core_pb::OtaUpdateNotification {
                    ota_type: kind.proto() as i32,
                    ota_package: package.map(crate::transport_api::ota_proto),
                },
            )),
        };
        let topic = TbTopic::transport_notifications(&session.node_id);
        producer
            .send(
                &topic,
                &session.session_id,
                Bytes::from(msg.encode_to_vec()),
            )
            .await
            .map_err(TbError::internal)?;
    }
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn package(kind: OtaKind, profile_id: Uuid, tenant_id: Uuid) -> OtaPackage {
        OtaPackage {
            id: Uuid::now_v7(),
            created_time: 1,
            tenant_id,
            device_profile_id: profile_id,
            r#type: kind.type_name().into(),
            title: "fw".into(),
            version: "1.0".into(),
            tag: None,
            file_name: None,
            content_type: None,
            checksum_algorithm: Some("SHA256".into()),
            checksum: Some("abc".into()),
            size: Some(10),
            data: None,
            additional_info: None,
        }
    }

    fn device(profile_id: Uuid, firmware_id: Option<Uuid>) -> Device {
        Device {
            id: Uuid::now_v7(),
            created_time: 1,
            tenant_id: Uuid::now_v7(),
            customer_id: None,
            device_profile_id: profile_id,
            name: "d".into(),
            r#type: None,
            label: None,
            external_id: None,
            firmware_id,
            software_id: None,
            additional_info: None,
            device_data: json!({}),
        }
    }

    fn profile(id: Uuid, firmware_id: Option<Uuid>) -> DeviceProfile {
        DeviceProfile {
            id,
            created_time: 1,
            tenant_id: Uuid::now_v7(),
            name: "p".into(),
            description: None,
            is_default: false,
            transport_type: "DEFAULT".into(),
            provision_type: "DISABLED".into(),
            profile_data: json!({}),
            firmware_id,
            software_id: None,
        }
    }

    #[test]
    fn device_binding_overrides_profile_binding() {
        let profile_id = Uuid::now_v7();
        let (device_pkg, profile_pkg) = (Uuid::now_v7(), Uuid::now_v7());
        let p = profile(profile_id, Some(profile_pkg));
        let bound = device(profile_id, Some(device_pkg));
        let inherited = device(profile_id, None);
        assert_eq!(
            effective_binding(OtaKind::Firmware, &bound, Some(&p)),
            Some(device_pkg)
        );
        assert_eq!(
            effective_binding(OtaKind::Firmware, &inherited, Some(&p)),
            Some(profile_pkg)
        );
        assert_eq!(effective_binding(OtaKind::Software, &bound, Some(&p)), None);
        assert_eq!(effective_binding(OtaKind::Firmware, &inherited, None), None);
    }

    #[test]
    fn binding_requires_same_tenant_type_profile_and_data() {
        let (tenant_id, profile_id) = (Uuid::now_v7(), Uuid::now_v7());
        let ok = package(OtaKind::Firmware, profile_id, tenant_id);
        assert!(validate_binding(&ok, OtaKind::Firmware, tenant_id, profile_id).is_ok());
        assert!(validate_binding(&ok, OtaKind::Software, tenant_id, profile_id).is_err());
        assert!(validate_binding(&ok, OtaKind::Firmware, Uuid::now_v7(), profile_id).is_err());
        assert!(validate_binding(&ok, OtaKind::Firmware, tenant_id, Uuid::now_v7()).is_err());
        let empty = OtaPackage {
            size: Some(0),
            ..ok.clone()
        };
        assert!(validate_binding(&empty, OtaKind::Firmware, tenant_id, profile_id).is_err());
    }

    #[test]
    fn target_telemetry_and_attributes_use_type_prefix() {
        let mut pkg = package(OtaKind::Software, Uuid::now_v7(), Uuid::now_v7());
        let keys = |entries: Vec<TsKvEntry>| -> Vec<String> {
            entries.into_iter().map(|e| e.key).collect()
        };
        assert_eq!(
            keys(target_telemetry(&pkg, OtaKind::Software, 7)),
            [
                "target_sw_title",
                "target_sw_version",
                "target_sw_ts",
                "sw_state"
            ]
        );
        pkg.tag = Some("stable".into());
        let telemetry = target_telemetry(&pkg, OtaKind::Software, 7);
        assert!(telemetry.iter().all(|e| e.ts == 7));
        assert!(telemetry.iter().any(|e| e.key == "target_sw_tag"));
        assert_eq!(
            telemetry.last().unwrap().value,
            KvValue::Str(STATE_QUEUED.into())
        );
        let attributes: Vec<String> = shared_attributes(&pkg, OtaKind::Software)
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(
            attributes,
            [
                "sw_title",
                "sw_version",
                "sw_tag",
                "sw_size",
                "sw_checksum_algorithm",
                "sw_checksum"
            ]
        );
        assert!(
            attributes
                .iter()
                .all(|key| attribute_keys(OtaKind::Software).contains(key))
        );
    }
}
