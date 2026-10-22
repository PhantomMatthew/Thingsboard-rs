//! tb-common：跨服务共享的基础类型。

pub mod entity;
pub mod error;
pub mod kv;
pub mod msg;

pub use entity::{EntityId, EntityType};
pub use error::{TbError, TbResult};
pub use kv::{AttributeScope, KvEntry, KvValue, TsKvEntry};
