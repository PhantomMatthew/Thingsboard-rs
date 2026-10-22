//! 权限辅助：基于 tb-auth 的 AuthUser extractor。

use tb_auth::AuthUser;
use tb_common::{TbError, TbResult};
use tb_dao::models::Authority;
use uuid::Uuid;

/// 调用方 authority 必须命中白名单，否则 PermissionDenied。
pub fn require_authority(user: &AuthUser, allowed: &[Authority]) -> TbResult<()> {
    if allowed.contains(&user.0.authority) {
        Ok(())
    } else {
        Err(TbError::PermissionDenied(format!(
            "authority {} not allowed",
            user.0.authority
        )))
    }
}

/// 实体归属的租户必须与调用方一致。
pub fn check_tenant_scope(user: &AuthUser, entity_tenant_id: Uuid) -> TbResult<()> {
    if user.0.authority == Authority::SysAdmin || user.0.tenant_id == entity_tenant_id {
        Ok(())
    } else {
        Err(TbError::PermissionDenied(
            "entity belongs to another tenant".into(),
        ))
    }
}
