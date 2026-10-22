//! bcrypt 密码散列封装。

use tb_common::{TbError, TbResult};

pub fn hash_password(plain: &str) -> TbResult<String> {
    bcrypt::hash(plain, bcrypt::DEFAULT_COST)
        .map_err(|e| TbError::Internal(format!("bcrypt hash: {e}")))
}

pub fn verify_password(plain: &str, hash: &str) -> TbResult<bool> {
    bcrypt::verify(plain, hash).map_err(|e| TbError::Internal(format!("bcrypt verify: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify() {
        let hash = hash_password("secret").unwrap();
        assert!(verify_password("secret", &hash).unwrap());
        assert!(!verify_password("wrong", &hash).unwrap());
    }
}
