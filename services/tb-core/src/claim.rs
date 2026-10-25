use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tb_common::{AttributeScope, KvValue, TbError, TbResult};
use tb_dao::models::Device;
use tb_proto::core::ClaimDeviceMsg;
use uuid::Uuid;

use crate::AppState;

pub const CLAIMING_ALLOWED_ATTRIBUTE: &str = "claimingAllowed";
pub const CLAIMING_DATA_ATTRIBUTE: &str = "claimingData";
const DEFAULT_CLAIM_DURATION_MS: i64 = 86_400_000;
const DEFAULT_MAX_CLAIM_DURATION_MS: i64 = 86_400_000;

#[derive(Debug)]
pub enum ClaimResult {
    Success(Box<Device>),
    Failure,
    Claimed,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaimingData {
    secret_key: String,
    expiration_time: i64,
}

pub async fn register(state: &AppState, msg: &ClaimDeviceMsg) -> TbResult<()> {
    let session = msg
        .session
        .as_ref()
        .ok_or_else(|| TbError::InvalidInput("claim session is required".into()))?;
    let tenant_id = Uuid::parse_str(&session.tenant_id)
        .map_err(|_| TbError::InvalidInput("invalid claim tenant id".into()))?;
    let device_id = Uuid::parse_str(&session.device_id)
        .map_err(|_| TbError::InvalidInput("invalid claim device id".into()))?;
    if msg.secret_key.trim().is_empty() {
        return Ok(());
    }
    let Some(device) = state.device_dao.find_by_id(device_id).await? else {
        return Ok(());
    };
    if device.tenant_id != tenant_id || device.customer_id.is_some() {
        return Ok(());
    }

    if !claiming_enabled() && !claiming_allowed(state, device_id).await? {
        return Ok(());
    }
    let duration_ms = if msg.duration_ms > 0 {
        msg.duration_ms
    } else {
        default_duration_ms()
    }
    .min(max_duration_ms());
    let data = ClaimingData {
        secret_key: msg.secret_key.clone(),
        expiration_time: chrono::Utc::now()
            .timestamp_millis()
            .saturating_add(duration_ms),
    };
    let value = serde_json::to_string(&data).map_err(TbError::internal)?;
    let mut redis = state.redis_manager().await.map_err(TbError::internal)?;
    let _: Option<String> = redis::cmd("SET")
        .arg(redis_key(tenant_id, device_id))
        .arg(value)
        .arg("PX")
        .arg(duration_ms)
        .query_async(&mut redis)
        .await
        .map_err(TbError::internal)?;
    Ok(())
}

pub async fn claim(
    state: &AppState,
    device: &Device,
    customer_id: Uuid,
    secret_key: &str,
) -> TbResult<ClaimResult> {
    if device.customer_id.is_some() {
        clear_registration(state, device, true).await?;
        return Ok(ClaimResult::Claimed);
    }
    if secret_key.trim().is_empty() {
        return Ok(ClaimResult::Failure);
    }

    let raw = match redis_claiming_data(state, device).await? {
        Some(data) => Some(data),
        None => attribute_claiming_data(state, device.id).await?,
    };
    let data = match raw.as_deref().map(parse_claiming_data).transpose() {
        Ok(data) => data,
        Err(_) => {
            clear_registration(state, device, false).await?;
            return Ok(ClaimResult::Failure);
        }
    };
    let Some(data) = data else {
        if state
            .device_dao
            .find_by_id(device.id)
            .await?
            .is_some_and(|current| current.customer_id.is_some())
        {
            clear_registration(state, device, true).await?;
            return Ok(ClaimResult::Claimed);
        }
        return Ok(ClaimResult::Failure);
    };
    if data.expiration_time <= chrono::Utc::now().timestamp_millis() {
        clear_registration(state, device, false).await?;
        return Ok(ClaimResult::Failure);
    }
    if data.secret_key != secret_key {
        return Ok(ClaimResult::Failure);
    }

    match state
        .device_dao
        .assign_customer_if_unassigned(device.id, device.tenant_id, customer_id)
        .await?
    {
        Some(device) => {
            clear_registration(state, &device, true).await?;
            Ok(ClaimResult::Success(Box::new(device)))
        }
        None => {
            clear_registration(state, device, true).await?;
            Ok(ClaimResult::Claimed)
        }
    }
}

fn claiming_enabled() -> bool {
    std::env::var("TB_CLAIM_ENABLED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(false)
}

fn default_duration_ms() -> i64 {
    std::env::var("TB_CLAIM_DURATION_MS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|duration| *duration > 0)
        .unwrap_or(DEFAULT_CLAIM_DURATION_MS)
}

fn max_duration_ms() -> i64 {
    std::env::var("TB_CLAIM_MAX_DURATION_MS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|duration| *duration > 0)
        .unwrap_or(DEFAULT_MAX_CLAIM_DURATION_MS)
}

async fn claiming_allowed(state: &AppState, device_id: Uuid) -> TbResult<bool> {
    let attributes = state
        .attributes_dao
        .find(
            device_id,
            AttributeScope::ServerScope,
            Some(&[CLAIMING_ALLOWED_ATTRIBUTE.into()]),
        )
        .await?;
    Ok(attributes
        .iter()
        .any(|entry| matches!(entry.value, KvValue::Bool(true))))
}

async fn redis_claiming_data(state: &AppState, device: &Device) -> TbResult<Option<String>> {
    let mut redis = state.redis_manager().await.map_err(TbError::internal)?;
    redis
        .get(redis_key(device.tenant_id, device.id))
        .await
        .map_err(TbError::internal)
}

async fn attribute_claiming_data(state: &AppState, device_id: Uuid) -> TbResult<Option<String>> {
    let attributes = state
        .attributes_dao
        .find(
            device_id,
            AttributeScope::ServerScope,
            Some(&[CLAIMING_DATA_ATTRIBUTE.into()]),
        )
        .await?;
    let Some(entry) = attributes.first() else {
        return Ok(None);
    };
    let value = serde_json::to_value(&entry.value).map_err(TbError::internal)?;
    let raw = match value {
        serde_json::Value::String(value) => value,
        value => value.to_string(),
    };
    Ok(Some(raw))
}

fn parse_claiming_data(raw: &str) -> TbResult<ClaimingData> {
    serde_json::from_str(raw).map_err(|_| TbError::InvalidInput("invalid claiming data".into()))
}

async fn clear_registration(
    state: &AppState,
    device: &Device,
    clear_allowed: bool,
) -> TbResult<()> {
    let mut redis = state.redis_manager().await.map_err(TbError::internal)?;
    let _: usize = redis
        .del(redis_key(device.tenant_id, device.id))
        .await
        .map_err(TbError::internal)?;
    let mut keys = vec![CLAIMING_DATA_ATTRIBUTE.to_string()];
    if clear_allowed {
        keys.push(CLAIMING_ALLOWED_ATTRIBUTE.to_string());
    }
    state
        .attributes_dao
        .delete(device.id, AttributeScope::ServerScope, &keys)
        .await?;
    Ok(())
}

fn redis_key(tenant_id: Uuid, device_id: Uuid) -> String {
    format!("claim:{tenant_id}:{device_id}")
}
