//! tb-auth：用户认证（JWT/bcrypt）。同时作为库被 tb-core 引用。

pub mod cors;
pub mod extract;
pub mod oauth2;
pub mod password;
pub mod routes;
pub mod service;
pub mod token;

pub use extract::AuthUser;
pub use routes::{ApiError, AuthState, error_response, router};
pub use service::{AuthService, TokenPair};
pub use token::{Claims, JwtTokenFactory, SecurityUser, TokenType};
