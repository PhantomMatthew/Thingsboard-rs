//! 查询引擎：实体过滤 + key 过滤 + 内存排序分页，全部命中 TenantRepo 的 HashMap。

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tb_common::{AttributeScope, EntityType, KvValue};
use uuid::Uuid;

use crate::state::{EntityRecord, TenantRepo};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityDataQuery {
    pub tenant_id: Uuid,
    pub entity_filter: EntityFilter,
    #[serde(default)]
    pub key_filters: Vec<KeyFilter>,
    #[serde(default)]
    pub page_link: PageLinkReq,
    #[serde(default)]
    pub fields: Vec<FieldReq>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntityFilter {
    All,
    EntityType {
        #[serde(rename = "entityType")]
        entity_type: String,
    },
    NameContains {
        #[serde(rename = "nameContains")]
        name_contains: String,
        #[serde(rename = "entityType", default)]
        entity_type: Option<String>,
    },
    EntityIds {
        ids: Vec<Uuid>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum KeySource {
    #[serde(rename = "LATEST")]
    Latest,
    #[serde(rename = "SERVER")]
    Server,
    #[serde(rename = "SHARED")]
    Shared,
    #[serde(rename = "CLIENT")]
    Client,
    #[serde(rename = "ENTITY_FIELD")]
    EntityField,
}

impl KeySource {
    fn scope(self) -> Option<AttributeScope> {
        match self {
            Self::Server => Some(AttributeScope::ServerScope),
            Self::Shared => Some(AttributeScope::SharedScope),
            Self::Client => Some(AttributeScope::ClientScope),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct KeyFilter {
    pub key: String,
    pub source: KeySource,
    pub predicate: Predicate,
}

#[derive(Debug, Deserialize)]
pub struct Predicate {
    pub op: PredicateOp,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredicateOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Contains,
    StartsWith,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageLinkReq {
    #[serde(default)]
    pub page: i64,
    #[serde(default = "default_page_size")]
    pub page_size: i64,
    #[serde(default)]
    pub sort_by: Option<SortBy>,
}

impl Default for PageLinkReq {
    fn default() -> Self {
        Self {
            page: 0,
            page_size: default_page_size(),
            sort_by: None,
        }
    }
}

fn default_page_size() -> i64 {
    50
}

#[derive(Debug, Deserialize)]
pub struct SortBy {
    pub key: String,
    #[serde(default = "default_sort_source")]
    pub source: KeySource,
    #[serde(default)]
    pub direction: SortDirection,
}

fn default_sort_source() -> KeySource {
    KeySource::EntityField
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub enum SortDirection {
    #[default]
    #[serde(rename = "ASC")]
    Asc,
    #[serde(rename = "DESC")]
    Desc,
}

#[derive(Debug, Deserialize)]
pub struct FieldReq {
    pub key: String,
    pub source: KeySource,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EntityDataResult {
    pub entity_id: Uuid,
    pub entity_type: String,
    pub values: serde_json::Map<String, Value>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageDataResult {
    pub data: Vec<EntityDataResult>,
    pub total_elements: i64,
    pub total_pages: i64,
    pub has_next: bool,
}

fn json_to_kv(v: &Value) -> Option<KvValue> {
    match v {
        Value::Bool(b) => Some(KvValue::Bool(*b)),
        Value::Number(n) => {
            if let Some(l) = n.as_i64() {
                Some(KvValue::Long(l))
            } else {
                n.as_f64().map(KvValue::Double)
            }
        }
        Value::String(s) => Some(KvValue::Str(s.clone())),
        other if !other.is_null() => Some(KvValue::Json(other.clone())),
        _ => None,
    }
}

fn as_f64(v: &KvValue) -> Option<f64> {
    match v {
        KvValue::Long(l) => Some(*l as f64),
        KvValue::Double(d) => Some(*d),
        _ => None,
    }
}

/// 数值感知相等：Long(1) 与 Double(1.0) 视为相等。
fn kv_eq(a: &KvValue, b: &KvValue) -> bool {
    match (as_f64(a), as_f64(b)) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// 缺失 key：仅 NEQ 视为通过（值不等于任何东西），其余 op 一律不通过。
fn eval_predicate(pred: &Predicate, value: Option<&KvValue>) -> bool {
    let Some(v) = value else {
        return pred.op == PredicateOp::Neq;
    };
    let target = json_to_kv(&pred.value);
    match pred.op {
        PredicateOp::Eq => target.is_some_and(|t| kv_eq(v, &t)),
        PredicateOp::Neq => target.is_none_or(|t| !kv_eq(v, &t)),
        PredicateOp::Gt | PredicateOp::Gte | PredicateOp::Lt | PredicateOp::Lte => {
            let (Some(x), Some(t)) = (as_f64(v), target.as_ref().and_then(as_f64)) else {
                return false;
            };
            match pred.op {
                PredicateOp::Gt => x > t,
                PredicateOp::Gte => x >= t,
                PredicateOp::Lt => x < t,
                _ => x <= t,
            }
        }
        PredicateOp::In => match &pred.value {
            Value::Array(items) => items.iter().filter_map(json_to_kv).any(|t| kv_eq(v, &t)),
            _ => false,
        },
        PredicateOp::Contains => match (v, pred.value.as_str()) {
            (KvValue::Str(s), Some(t)) => s.contains(t),
            _ => false,
        },
        PredicateOp::StartsWith => match (v, pred.value.as_str()) {
            (KvValue::Str(s), Some(t)) => s.starts_with(t),
            _ => false,
        },
    }
}

fn entity_field(record: &EntityRecord, key: &str) -> Option<KvValue> {
    match key {
        "name" => Some(KvValue::Str(record.name.clone())),
        "label" => record.label.clone().map(KvValue::Str),
        "type" => Some(KvValue::Str(record.type_or_profile.clone())),
        "createdTime" | "created_time" => Some(KvValue::Long(record.created_time)),
        "customerId" | "customer_id" => record.customer_id.map(|c| KvValue::Str(c.to_string())),
        _ => None,
    }
}

fn lookup(repo: &TenantRepo, entity_id: Uuid, source: KeySource, key: &str) -> Option<KvValue> {
    match source {
        KeySource::EntityField => entity_field(repo.entities.get(&entity_id)?, key),
        KeySource::Latest => repo
            .latest
            .get(&entity_id)?
            .get(key)
            .map(|(_, v)| v.clone()),
        source => repo
            .attributes
            .get(&(entity_id, source.scope()?))?
            .get(key)
            .cloned(),
    }
}

fn entity_type_ci(s: &str) -> Option<EntityType> {
    s.to_ascii_uppercase().parse().ok()
}

fn matches_filter(record: &EntityRecord, filter: &EntityFilter) -> bool {
    match filter {
        EntityFilter::All => true,
        EntityFilter::EntityType { entity_type } => {
            entity_type_ci(entity_type) == Some(record.entity_type)
        }
        EntityFilter::NameContains {
            name_contains,
            entity_type,
        } => {
            if let Some(t) = entity_type
                && entity_type_ci(t) != Some(record.entity_type)
            {
                return false;
            }
            record
                .name
                .to_lowercase()
                .contains(&name_contains.to_lowercase())
        }
        EntityFilter::EntityIds { ids } => ids.contains(&record.id),
    }
}

/// 排序值：缺失排最后；数值 < 字符串 < 布尔。
#[derive(PartialEq)]
enum SortVal {
    Missing,
    Num(f64),
    Str(String),
    Bool(bool),
}

impl SortVal {
    fn of(repo: &TenantRepo, record: &EntityRecord, sort: &SortBy) -> Self {
        match lookup(repo, record.id, sort.source, &sort.key) {
            None => Self::Missing,
            Some(KvValue::Long(l)) => Self::Num(l as f64),
            Some(KvValue::Double(d)) => Self::Num(d),
            Some(KvValue::Str(s)) => Self::Str(s),
            Some(KvValue::Bool(b)) => Self::Bool(b),
            Some(KvValue::Json(j)) => Self::Str(j.to_string()),
        }
    }

    fn rank(&self) -> u8 {
        match self {
            Self::Num(_) => 0,
            Self::Str(_) => 1,
            Self::Bool(_) => 2,
            Self::Missing => 3,
        }
    }

    fn cmp_asc(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Num(a), Self::Num(b)) => a.total_cmp(b),
            (Self::Str(a), Self::Str(b)) => a.cmp(b),
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

pub fn execute_query(repo: &TenantRepo, query: &EntityDataQuery) -> PageDataResult {
    let mut matched: Vec<&EntityRecord> = repo
        .entities
        .values()
        .filter(|r| matches_filter(r, &query.entity_filter))
        .filter(|r| {
            query.key_filters.iter().all(|f| {
                eval_predicate(&f.predicate, lookup(repo, r.id, f.source, &f.key).as_ref())
            })
        })
        .collect();

    if let Some(sort) = &query.page_link.sort_by {
        let desc = sort.direction == SortDirection::Desc;
        matched.sort_by(|a, b| {
            let ord = SortVal::of(repo, a, sort).cmp_asc(&SortVal::of(repo, b, sort));
            if desc { ord.reverse() } else { ord }
        });
    } else {
        matched.sort_by_key(|r| r.created_time);
    }

    let total = matched.len() as i64;
    let page_size = query.page_link.page_size.max(1);
    let total_pages = (total + page_size - 1) / page_size;
    let start = (query.page_link.page.max(0) * page_size) as usize;
    let data = matched
        .into_iter()
        .skip(start)
        .take(page_size as usize)
        .map(|r| {
            let mut values = serde_json::Map::new();
            for f in &query.fields {
                if let Some(v) = lookup(repo, r.id, f.source, &f.key) {
                    values.insert(f.key.clone(), kv_to_json(v));
                }
            }
            EntityDataResult {
                entity_id: r.id,
                entity_type: r.entity_type.as_str().to_string(),
                values,
            }
        })
        .collect();
    PageDataResult {
        data,
        total_elements: total,
        total_pages,
        has_next: query.page_link.page + 1 < total_pages,
    }
}

fn kv_to_json(v: KvValue) -> Value {
    match v {
        KvValue::Json(j) => j,
        other => json!(other),
    }
}
