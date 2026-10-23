//! 审计日志：实体 CRUD 的异步落库（tokio::spawn，失败仅 warn）。
//! login 审计走 AppState.audit_tx 通道（sink 在 AppState::new 启动）。

use serde_json::Value;
use tb_auth::AuthUser;
use tb_common::EntityType;
use tb_dao::audit_log::AuditLog;
use uuid::Uuid;

use crate::AppState;

/// 记录一条审计：异步写库，失败仅 warn，不影响主流程。
pub fn audit(
    state: &AppState,
    user: &AuthUser,
    entity: Option<(Uuid, EntityType, String)>,
    action_type: &str,
    action_data: Option<Value>,
    status: &str,
) {
    let entry = AuditLog::new(
        user.0.tenant_id,
        entity.map(|(id, ty, name)| (id, ty.as_str().to_string(), name)),
        Some(user.0.id),
        Some(user.0.email.clone()),
        action_type,
        action_data,
        status,
    );
    let dao = state.audit_log_dao.clone();
    tokio::spawn(async move {
        if let Err(e) = dao.save(&entry).await {
            tracing::warn!("audit save failed: {e}");
        }
    });
}
