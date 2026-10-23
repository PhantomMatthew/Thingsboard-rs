//! tb-edqs：实体数据查询服务（对标 Java 版 EDQS）。
//! 启动时从 PostgreSQL 全量加载快照，随后消费 Kafka `tb_edqs.events` 增量事件，
//! 实体+属性+latest 遥测查询全部走内存，把这类读负载从 PostgreSQL 卸掉。

pub mod api;
pub mod consumer;
pub mod query;
pub mod snapshot;
pub mod state;

pub use query::{EntityDataQuery, EntityDataResult, PageDataResult};
pub use state::EdqsState;

use axum::Router;

pub fn build_router(state: EdqsState) -> Router {
    api::router().with_state(state)
}
