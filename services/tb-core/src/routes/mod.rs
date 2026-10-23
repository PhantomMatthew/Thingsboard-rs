//! REST 路由汇总。分页参数对齐 ThingsBoard：page/pageSize/textSearch/sortProperty/sortOrder。

pub mod alarm;
pub mod asset;
pub mod asset_profile;
pub mod customer;
pub mod device;
pub mod device_profile;
pub mod ota;
pub mod rpc;
pub mod rule_chain;
pub mod telemetry;
pub mod tenant;
pub mod user;

use axum::Router;
use serde::Deserialize;
use tb_dao::PageLink;

use crate::AppState;

pub fn api_router() -> Router<AppState> {
    Router::new()
        .merge(tenant::router())
        .merge(customer::router())
        .merge(user::router())
        .merge(device::router())
        .merge(device_profile::router())
        .merge(asset::router())
        .merge(asset_profile::router())
        .merge(rule_chain::router())
        .merge(telemetry::router())
        .merge(alarm::router())
        .merge(rpc::router())
        .merge(ota::router())
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageQuery {
    pub page: Option<i64>,
    pub page_size: Option<i64>,
    pub text_search: Option<String>,
    pub sort_property: Option<String>,
    pub sort_order: Option<String>,
}

impl From<PageQuery> for PageLink {
    fn from(q: PageQuery) -> Self {
        Self {
            page: q.page.unwrap_or(0),
            page_size: q.page_size.unwrap_or(10),
            text_search: q.text_search,
            sort_property: q.sort_property,
            sort_order: q.sort_order,
        }
    }
}
