//! filter.*：纯判定节点，产出 "True"/"False"（内部错误按 "False" 处理并记日志）。

use async_trait::async_trait;
use serde::Deserialize;
use tb_common::TbResult;
use tb_common::msg::TbMsg;

use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::parse_config;

/// filter.msg_type：msg.msg_type ∈ types → "True"。
pub struct MsgTypeFilterNode {
    types: Vec<String>,
}

impl MsgTypeFilterNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            types: Vec<String>,
        }
        Ok(Self {
            types: parse_config::<Config>(config)?.types,
        })
    }
}

#[async_trait]
impl Node for MsgTypeFilterNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rel = if self.types.contains(&msg.msg_type) {
            "True"
        } else {
            "False"
        };
        vec![(rel.into(), msg)]
    }
}

/// filter.originator_type：originator 实体类型 ∈ types → "True"。
pub struct OriginatorTypeFilterNode {
    types: Vec<String>,
}

impl OriginatorTypeFilterNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            types: Vec<String>,
        }
        Ok(Self {
            types: parse_config::<Config>(config)?.types,
        })
    }
}

#[async_trait]
impl Node for OriginatorTypeFilterNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rel = if self
            .types
            .contains(&msg.originator.entity_type.as_str().to_string())
        {
            "True"
        } else {
            "False"
        };
        vec![(rel.into(), msg)]
    }
}

/// filter.key_exists：key 在 msg.data 或 metadata 中存在；check_all=true 要求全部存在。
pub struct KeyExistsFilterNode {
    keys: Vec<String>,
    check_all: bool,
}

impl KeyExistsFilterNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            keys: Vec<String>,
            #[serde(default)]
            check_all: bool,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            keys: c.keys,
            check_all: c.check_all,
        })
    }
}

#[async_trait]
impl Node for KeyExistsFilterNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let exists = |k: &str| {
            msg.data.as_object().is_some_and(|o| o.contains_key(k)) || msg.metadata.contains_key(k)
        };
        let hit = if self.check_all {
            self.keys.iter().all(|k| exists(k))
        } else {
            self.keys.iter().any(|k| exists(k))
        };
        vec![(if hit { "True" } else { "False" }.into(), msg)]
    }
}

/// filter.relation：originator 存在指定 COMMON 关系 → "True"。
/// direction=FROM：originator 为 from 端；TO：originator 为 to 端。entity_type 为空 = 不限。
pub struct RelationFilterNode {
    relation_type: String,
    direction: String,
    entity_type: Option<String>,
}

impl RelationFilterNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            relation_type: String,
            #[serde(default = "default_direction")]
            direction: String,
            entity_type: Option<String>,
        }
        fn default_direction() -> String {
            "FROM".into()
        }
        let c: Config = parse_config(config)?;
        if c.direction != "FROM" && c.direction != "TO" {
            return Err(tb_common::TbError::InvalidInput(format!(
                "direction must be FROM|TO, got {}",
                c.direction
            )));
        }
        Ok(Self {
            relation_type: c.relation_type,
            direction: c.direction,
            entity_type: c.entity_type,
        })
    }
}

#[async_trait]
impl Node for RelationFilterNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rels = if self.direction == "FROM" {
            ctx.relation_dao
                .find_by_from(msg.originator.id, msg.originator.entity_type)
                .await
        } else {
            ctx.relation_dao
                .find_by_to(msg.originator.id, msg.originator.entity_type)
                .await
        };
        let found = match rels {
            Ok(rels) => rels.iter().any(|r| {
                if r.relation_type_group != "COMMON" || r.relation_type != self.relation_type {
                    return false;
                }
                let peer = if self.direction == "FROM" {
                    r.to_type
                } else {
                    r.from_type
                };
                self.entity_type
                    .as_deref()
                    .is_none_or(|t| t == peer.as_str())
            }),
            Err(e) => {
                tracing::warn!("relation filter query failed: {e}");
                false
            }
        };
        vec![(if found { "True" } else { "False" }.into(), msg)]
    }
}

/// filter.geo：从 msg.data 取坐标（latitude_key/longitude_key），判 circle 或 polygon 包含。
/// 缺坐标/坐标非法 → "False"。
pub struct GeoFilterNode {
    latitude_key: String,
    longitude_key: String,
    shape: GeoShape,
}

#[derive(Debug, Clone)]
enum GeoShape {
    Circle {
        lat: f64,
        lng: f64,
        radius_m: f64,
    },
    /// 顶点列表 [(lat, lng), ...]
    Polygon(Vec<(f64, f64)>),
}

impl GeoFilterNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Circle {
            lat: f64,
            lng: f64,
            radius_m: f64,
        }
        #[derive(Deserialize)]
        struct Config {
            latitude_key: String,
            longitude_key: String,
            circle: Option<Circle>,
            polygon: Option<Vec<(f64, f64)>>,
        }
        let c: Config = parse_config(config)?;
        let shape = match (c.circle, c.polygon) {
            (Some(c), None) => GeoShape::Circle {
                lat: c.lat,
                lng: c.lng,
                radius_m: c.radius_m,
            },
            (None, Some(p)) if p.len() >= 3 => GeoShape::Polygon(p),
            _ => {
                return Err(tb_common::TbError::InvalidInput(
                    "geo filter requires exactly one of circle | polygon(>=3 points)".into(),
                ));
            }
        };
        Ok(Self {
            latitude_key: c.latitude_key,
            longitude_key: c.longitude_key,
            shape,
        })
    }

    fn contains(&self, lat: f64, lng: f64) -> bool {
        match &self.shape {
            GeoShape::Circle {
                lat: clat,
                lng: clng,
                radius_m,
            } => haversine_m(lat, lng, *clat, *clng) <= *radius_m,
            GeoShape::Polygon(points) => point_in_polygon(lat, lng, points),
        }
    }
}

#[async_trait]
impl Node for GeoFilterNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let coord = |key: &str| {
            msg.data
                .get(key)
                .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok()))
        };
        let hit = match (coord(&self.latitude_key), coord(&self.longitude_key)) {
            (Some(lat), Some(lng)) => self.contains(lat, lng),
            _ => false,
        };
        vec![(if hit { "True" } else { "False" }.into(), msg)]
    }
}

/// 球面距离（米）。
fn haversine_m(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    const EARTH_R_M: f64 = 6_371_000.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlng = (lng2 - lng1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlng / 2.0).sin().powi(2);
    2.0 * EARTH_R_M * a.sqrt().asin()
}

/// 射线法（x=lng, y=lat）。
fn point_in_polygon(lat: f64, lng: f64, points: &[(f64, f64)]) -> bool {
    let mut inside = false;
    let mut j = points.len() - 1;
    for i in 0..points.len() {
        let (yi, xi) = points[i];
        let (yj, xj) = points[j];
        if (yi > lat) != (yj > lat) && lng < (xj - xi) * (lat - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}
