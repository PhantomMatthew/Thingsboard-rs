//! HTTP 查询 API。TB_EDQS_API_KEY 非空时校验 X-EDQS-KEY；空密钥仅用于开发模式。

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

use crate::query::{
    EntityDataQuery, EntityFilter, PageDataResult, PageLinkReq, SortBy, SortDirection,
};
use crate::state::EdqsState;

const EDQS_KEY_HEADER: &str = "X-EDQS-KEY";

pub fn router() -> Router<EdqsState> {
    Router::new()
        .route("/api/query/entityData", post(query_entity_data))
        .route("/api/query/entityData", get(query_entity_data_get))
        .route("/health", get(health))
}

fn authorize(state: &EdqsState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let expected = state.api_key();
    if expected.is_empty() {
        return Ok(());
    }
    let provided = headers
        .get(EDQS_KEY_HEADER)
        .and_then(|value| value.to_str().ok());
    if provided == Some(expected) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn health(State(state): State<EdqsState>) -> (StatusCode, &'static str) {
    if state.consumer_healthy() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "consumer unavailable")
    }
}

fn empty_page() -> Json<PageDataResult> {
    Json(PageDataResult {
        data: vec![],
        total_elements: 0,
        total_pages: 0,
        has_next: false,
    })
}

async fn query_entity_data(
    State(state): State<EdqsState>,
    headers: HeaderMap,
    Json(query): Json<EntityDataQuery>,
) -> Result<Json<PageDataResult>, StatusCode> {
    authorize(&state, &headers)?;
    let Some(repo) = state.tenants.get(&query.tenant_id) else {
        return Ok(empty_page());
    };
    Ok(Json(crate::query::execute_query(&repo, &query)))
}

/// GET 简易变体：仅实体过滤 + 分页排序，无 keyFilters/fields。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetQueryParams {
    pub tenant_id: Uuid,
    pub customer_id: Option<Uuid>,
    pub entity_type: Option<String>,
    pub name_contains: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

pub fn entity_data_query(p: GetQueryParams) -> EntityDataQuery {
    let entity_filter = match (&p.entity_type, &p.name_contains) {
        (Some(t), None) => EntityFilter::EntityType {
            entity_type: t.clone(),
        },
        (_, Some(n)) => EntityFilter::NameContains {
            name_contains: n.clone(),
            entity_type: p.entity_type.clone(),
        },
        (None, None) => EntityFilter::All,
    };
    EntityDataQuery {
        tenant_id: p.tenant_id,
        customer_id: p.customer_id,
        entity_filter,
        key_filters: vec![],
        page_link: PageLinkReq {
            page: p.page.unwrap_or(0),
            page_size: p.page_size.unwrap_or(50),
            sort_by: p.sort_by.map(|key| SortBy {
                key,
                source: crate::query::KeySource::EntityField,
                direction: match p.sort_order.as_deref() {
                    Some(o) if o.eq_ignore_ascii_case("desc") => SortDirection::Desc,
                    _ => SortDirection::Asc,
                },
            }),
        },
        fields: vec![],
    }
}

async fn query_entity_data_get(
    State(state): State<EdqsState>,
    headers: HeaderMap,
    Query(p): Query<GetQueryParams>,
) -> Result<Json<PageDataResult>, StatusCode> {
    authorize(&state, &headers)?;
    let tenant_id = p.tenant_id;
    let query = entity_data_query(p);
    let Some(repo) = state.tenants.get(&tenant_id) else {
        return Ok(empty_page());
    };
    Ok(Json(crate::query::execute_query(&repo, &query)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tb_proto::common as common_pb;
    use tb_proto::edqs as pb;

    #[test]
    fn shared_key_is_enforced_only_when_configured() {
        let development = EdqsState::default().with_api_key("");
        assert_eq!(authorize(&development, &HeaderMap::new()), Ok(()));

        let state = EdqsState::default().with_api_key("secret");
        assert_eq!(
            authorize(&state, &HeaderMap::new()),
            Err(StatusCode::UNAUTHORIZED)
        );
        let mut wrong = HeaderMap::new();
        wrong.insert(EDQS_KEY_HEADER, "wrong".parse().unwrap());
        assert_eq!(authorize(&state, &wrong), Err(StatusCode::UNAUTHORIZED));
        let mut correct = HeaderMap::new();
        correct.insert(EDQS_KEY_HEADER, "secret".parse().unwrap());
        assert_eq!(authorize(&state, &correct), Ok(()));
    }

    #[tokio::test]
    async fn http_query_enforces_shared_key() {
        let state = EdqsState::default().with_api_key("secret");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, crate::build_router(state))
                .await
                .unwrap();
        });
        let client = reqwest::Client::new();
        let query = serde_json::json!({
            "tenantId": Uuid::now_v7(),
            "entityFilter": {"type": "ALL"}
        });
        let unauthorized = client
            .post(format!("http://{addr}/api/query/entityData"))
            .json(&query)
            .send()
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let authorized = client
            .post(format!("http://{addr}/api/query/entityData"))
            .header(EDQS_KEY_HEADER, "secret")
            .json(&query)
            .send()
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
        server.abort();
    }

    #[tokio::test]
    async fn health_reflects_consumer_liveness() {
        let state = EdqsState::default();
        assert_eq!(
            health(State(state.clone())).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        state.consumer_health.beat();
        assert_eq!(health(State(state)).await.0, StatusCode::OK);
    }

    #[tokio::test]
    async fn tenant_delete_leaves_no_queryable_ghost_data() {
        let state = EdqsState::default().with_api_key("");
        let tenant_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        state.apply_event(&pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::EntityUpsert(
                pb::EntityUpsertMsg {
                    entity_id: entity_id.to_string(),
                    entity_type: common_pb::EntityType::Device as i32,
                    customer_id: String::new(),
                    name: "device".into(),
                    type_or_profile: "sensor".into(),
                    label: String::new(),
                    created_time: 1,
                },
            )),
        });
        state.apply_event(&pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::AttributeUpdate(
                pb::AttributeUpdateMsg {
                    entity_id: entity_id.to_string(),
                    scope: common_pb::AttributeScope::ServerScope as i32,
                    updated: vec![tb_proto::convert::kv_to_proto_named(
                        0,
                        "region",
                        &tb_common::KvValue::Str("eu".into()),
                    )],
                    deleted_keys: vec![],
                    update_ts: 100,
                },
            )),
        });
        state.apply_event(&pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::LatestTelemetry(
                pb::LatestTelemetryMsg {
                    entity_id: entity_id.to_string(),
                    values: vec![tb_proto::convert::kv_to_proto_named(
                        0,
                        "temperature",
                        &tb_common::KvValue::Long(20),
                    )],
                    ts: vec![100],
                },
            )),
        });
        let query = EntityDataQuery {
            tenant_id,
            customer_id: None,
            entity_filter: EntityFilter::All,
            key_filters: vec![],
            page_link: PageLinkReq::default(),
            fields: vec![],
        };
        let before = query_entity_data(State(state.clone()), HeaderMap::new(), Json(query))
            .await
            .unwrap();
        assert_eq!(before.total_elements, 1);

        state.apply_event(&pb::EdqsEventMsg {
            tenant_id: tenant_id.to_string(),
            event: Some(pb::edqs_event_msg::Event::TenantDelete(
                pb::TenantDeleteMsg {
                    tenant_id: tenant_id.to_string(),
                },
            )),
        });
        let query = EntityDataQuery {
            tenant_id,
            customer_id: None,
            entity_filter: EntityFilter::All,
            key_filters: vec![],
            page_link: PageLinkReq::default(),
            fields: vec![],
        };
        let after = query_entity_data(State(state), HeaderMap::new(), Json(query))
            .await
            .unwrap();
        assert_eq!(after.total_elements, 0);
        assert!(after.data.is_empty());
    }
}
