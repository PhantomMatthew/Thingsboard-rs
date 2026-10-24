use thiserror::Error;

pub type TbResult<T> = Result<T, TbError>;

#[derive(Debug, Error)]
pub enum TbError {
    #[error("entity not found: {0}")]
    NotFound(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("rate limit exceeded: {0}")]
    RateLimitExceeded(String),

    #[error("database error: {0}")]
    Database(#[source] sqlx_like::DbErr),

    #[error("internal error: {0}")]
    Internal(String),
}

// sqlx 不该出现在 common 的 API 里；这里用一个轻包装避免 common 依赖 sqlx。
pub mod sqlx_like {
    use std::error::Error;
    use std::fmt;

    pub struct DbErr(pub Box<dyn Error + Send + Sync + 'static>);

    impl fmt::Debug for DbErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Debug::fmt(&self.0, f)
        }
    }

    impl fmt::Display for DbErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            fmt::Display::fmt(&self.0, f)
        }
    }

    impl Error for DbErr {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&*self.0)
        }
    }
}

impl TbError {
    pub fn internal(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Internal(err.to_string())
    }

    pub fn db(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Database(sqlx_like::DbErr(Box::new(err)))
    }

    /// 可安全返回给客户端的错误信息。
    pub fn client_message(&self) -> String {
        match self {
            Self::Database(_) | Self::Internal(_) => "internal server error".into(),
            Self::NotFound(message)
            | Self::InvalidInput(message)
            | Self::Unauthenticated(message)
            | Self::PermissionDenied(message)
            | Self::Conflict(message)
            | Self::RateLimitExceeded(message) => message.clone(),
        }
    }

    /// HTTP 状态码映射（REST 层使用）。
    pub fn http_status(&self) -> u16 {
        match self {
            Self::NotFound(_) => 404,
            Self::InvalidInput(_) => 400,
            Self::Unauthenticated(_) => 401,
            Self::PermissionDenied(_) => 403,
            Self::Conflict(_) => 409,
            Self::RateLimitExceeded(_) => 429,
            Self::Database(_) | Self::Internal(_) => 500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_message_hides_internal_details() {
        let errors = [
            TbError::Internal("sensitive internal detail".into()),
            TbError::db(std::io::Error::other("sensitive database detail")),
        ];
        for err in errors {
            assert_eq!(err.client_message(), "internal server error");
            assert_eq!(err.http_status(), 500);
        }
    }

    #[test]
    fn client_message_preserves_public_detail() {
        let err = TbError::InvalidInput("bad request field".into());
        assert_eq!(err.client_message(), "bad request field");
        assert_eq!(err.http_status(), 400);
    }
}
