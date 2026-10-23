//! tb-script：Rhai 脚本沙箱（规则引擎 filter/transform 节点用）。
//! 编译结果按脚本内容 hash 缓存（容量 1000）；执行限制 max_operations=100_000。
//! Rhai 默认不带文件/系统 IO，本 crate 也不注册任何危险 API。

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use rhai::{Dynamic, Engine, Scope};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ScriptError {
    #[error("script compile error: {0}")]
    Compile(String),

    #[error("script eval error: {0}")]
    Eval(String),

    #[error("script returned invalid value: {0}")]
    InvalidReturn(String),
}

pub struct TransformOutput {
    pub msg: Value,
    pub metadata: HashMap<String, String>,
    pub msg_type: String,
}

const MAX_OPERATIONS: u64 = 100_000;
const CACHE_CAPACITY: u64 = 1000;

pub struct ScriptEngine {
    engine: Engine,
    cache: moka::sync::Cache<u64, Arc<rhai::AST>>,
}

impl Default for ScriptEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptEngine {
    pub fn new() -> Self {
        let mut engine = Engine::new();
        engine.set_max_operations(MAX_OPERATIONS);
        Self {
            engine,
            cache: moka::sync::Cache::new(CACHE_CAPACITY),
        }
    }

    fn compile(&self, script: &str) -> Result<Arc<rhai::AST>, ScriptError> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        script.hash(&mut hasher);
        let key = hasher.finish();
        if let Some(ast) = self.cache.get(&key) {
            return Ok(ast);
        }
        let ast = Arc::new(
            self.engine
                .compile(script)
                .map_err(|e| ScriptError::Compile(e.to_string()))?,
        );
        self.cache.insert(key, ast.clone());
        Ok(ast)
    }

    fn run(
        &self,
        script: &str,
        msg: &Value,
        metadata: &HashMap<String, String>,
        msg_type: &str,
    ) -> Result<Dynamic, ScriptError> {
        let ast = self.compile(script)?;
        let mut scope = Scope::new();
        scope.push_dynamic("msg", json_to_dynamic(msg));
        scope.push_dynamic("metadata", metadata_to_dynamic(metadata));
        scope.push_dynamic("msgType", Dynamic::from(msg_type.to_string()));
        self.engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .map_err(|e| ScriptError::Eval(e.to_string()))
    }

    /// filter 脚本：最后表达式必须返回 bool。
    pub fn eval_filter(
        &self,
        script: &str,
        msg: &Value,
        metadata: &HashMap<String, String>,
        msg_type: &str,
    ) -> Result<bool, ScriptError> {
        self.run(script, msg, metadata, msg_type)?
            .try_cast::<bool>()
            .ok_or_else(|| ScriptError::InvalidReturn("filter script must return bool".into()))
    }

    /// transform 脚本：返回 `#{msg: ..., metadata: ..., msgType: ...}` object map；
    /// metadata/msgType 缺省时保留原值。
    pub fn eval_transform(
        &self,
        script: &str,
        msg: &Value,
        metadata: &HashMap<String, String>,
        msg_type: &str,
    ) -> Result<TransformOutput, ScriptError> {
        let result = self.run(script, msg, metadata, msg_type)?;
        let map = result.try_cast::<rhai::Map>().ok_or_else(|| {
            ScriptError::InvalidReturn("transform script must return an object map".into())
        })?;
        let new_msg = match map.get("msg") {
            Some(d) => dynamic_to_json(d)?,
            None => msg.clone(),
        };
        let new_metadata = match map.get("metadata") {
            Some(d) => {
                let m = d.clone().try_cast::<rhai::Map>().ok_or_else(|| {
                    ScriptError::InvalidReturn("metadata must be a map of strings".into())
                })?;
                let mut out = HashMap::with_capacity(m.len());
                for (k, v) in m {
                    let s = v.into_string().map_err(|_| {
                        ScriptError::InvalidReturn("metadata values must be strings".into())
                    })?;
                    out.insert(k.to_string(), s);
                }
                out
            }
            None => metadata.clone(),
        };
        let new_type = match map.get("msgType") {
            Some(d) => d
                .clone()
                .into_string()
                .map_err(|_| ScriptError::InvalidReturn("msgType must be a string".into()))?,
            None => msg_type.to_string(),
        };
        Ok(TransformOutput {
            msg: new_msg,
            metadata: new_metadata,
            msg_type: new_type,
        })
    }

    /// 返回 object map 的脚本（如 create_alarm 的 details_script），转为 JSON 对象。
    pub fn eval_map(
        &self,
        script: &str,
        msg: &Value,
        metadata: &HashMap<String, String>,
        msg_type: &str,
    ) -> Result<Value, ScriptError> {
        let result = self.run(script, msg, metadata, msg_type)?;
        if !result.is_map() {
            return Err(ScriptError::InvalidReturn(
                "script must return an object map".into(),
            ));
        }
        dynamic_to_json(&result)
    }

    /// 计算字段用：参数作为顶层变量注入 scope（如 expression "tempC * 9 / 5 + 32"），
    /// 结果转 JSON（标量或对象均可）。
    pub fn eval_value(
        &self,
        script: &str,
        vars: &HashMap<String, Value>,
    ) -> Result<Value, ScriptError> {
        let ast = self.compile(script)?;
        let mut scope = Scope::new();
        for (k, v) in vars {
            scope.push_dynamic(k.clone(), json_to_dynamic(v));
        }
        let result = self
            .engine
            .eval_ast_with_scope::<Dynamic>(&mut scope, &ast)
            .map_err(|e| ScriptError::Eval(e.to_string()))?;
        dynamic_to_json(&result)
    }

    /// 返回字符串的脚本（如 external.kafka 的 key_script）；标量值转字符串。
    pub fn eval_string(
        &self,
        script: &str,
        msg: &Value,
        metadata: &HashMap<String, String>,
        msg_type: &str,
    ) -> Result<String, ScriptError> {
        let result = self.run(script, msg, metadata, msg_type)?;
        if let Ok(s) = result.clone().into_string() {
            return Ok(s);
        }
        dynamic_to_json(&result).map(|v| v.to_string())
    }
}

fn metadata_to_dynamic(metadata: &HashMap<String, String>) -> Dynamic {
    let mut map = rhai::Map::new();
    for (k, v) in metadata {
        map.insert(k.as_str().into(), Dynamic::from(v.clone()));
    }
    map.into()
}

fn json_to_dynamic(v: &Value) -> Dynamic {
    match v {
        Value::Null => ().into(),
        Value::Bool(b) => (*b).into(),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into()
            } else {
                n.as_f64().unwrap_or_default().into()
            }
        }
        Value::String(s) => s.clone().into(),
        Value::Array(a) => a
            .iter()
            .map(json_to_dynamic)
            .collect::<rhai::Array>()
            .into(),
        Value::Object(o) => {
            let mut map = rhai::Map::new();
            for (k, v) in o {
                map.insert(k.as_str().into(), json_to_dynamic(v));
            }
            map.into()
        }
    }
}

fn dynamic_to_json(d: &Dynamic) -> Result<Value, ScriptError> {
    if d.is_unit() {
        Ok(Value::Null)
    } else if let Some(b) = d.clone().try_cast::<bool>() {
        Ok(Value::Bool(b))
    } else if let Some(i) = d.clone().try_cast::<rhai::INT>() {
        Ok(Value::Number(i.into()))
    } else if let Some(f) = d.clone().try_cast::<rhai::FLOAT>() {
        Ok(serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or(Value::Null))
    } else if let Some(s) = d.clone().try_cast::<String>() {
        Ok(Value::String(s))
    } else if let Some(a) = d.clone().try_cast::<rhai::Array>() {
        a.iter().map(dynamic_to_json).collect()
    } else if let Some(m) = d.clone().try_cast::<rhai::Map>() {
        let mut obj = serde_json::Map::new();
        for (k, v) in &m {
            obj.insert(k.to_string(), dynamic_to_json(v)?);
        }
        Ok(Value::Object(obj))
    } else {
        Err(ScriptError::InvalidReturn(format!(
            "unsupported value type: {}",
            d.type_name()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn engine() -> ScriptEngine {
        ScriptEngine::new()
    }

    fn md(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn filter_true_false() {
        let e = engine();
        let msg = json!({"temperature": 80});
        assert!(
            e.eval_filter(
                "msg.temperature > 50",
                &msg,
                &HashMap::new(),
                "POST_TELEMETRY_REQUEST"
            )
            .unwrap()
        );
        assert!(
            !e.eval_filter(
                "msg.temperature > 100",
                &msg,
                &HashMap::new(),
                "POST_TELEMETRY_REQUEST"
            )
            .unwrap()
        );
    }

    #[test]
    fn filter_uses_metadata_and_msg_type() {
        let e = engine();
        let msg = json!({});
        let metadata = md(&[("deviceType", "sensor")]);
        let script = "metadata.deviceType == \"sensor\" && msgType == \"POST_TELEMETRY_REQUEST\"";
        assert!(
            e.eval_filter(script, &msg, &metadata, "POST_TELEMETRY_REQUEST")
                .unwrap()
        );
        assert!(
            !e.eval_filter(script, &msg, &metadata, "CONNECT_EVENT")
                .unwrap()
        );
    }

    #[test]
    fn transform_updates_msg_and_metadata() {
        let e = engine();
        let msg = json!({"temperature": 80});
        let metadata = md(&[("deviceName", "d1")]);
        let out = e
            .eval_transform(
                "msg.temperature = msg.temperature + 1; \
                 #{msg: msg, metadata: #{\"origin\": \"transform\"}, msgType: \"TRANSFORMED\"}",
                &msg,
                &metadata,
                "POST_TELEMETRY_REQUEST",
            )
            .unwrap();
        assert_eq!(out.msg, json!({"temperature": 81}));
        assert_eq!(out.metadata, md(&[("origin", "transform")]));
        assert_eq!(out.msg_type, "TRANSFORMED");
    }

    #[test]
    fn transform_defaults_keep_original() {
        let e = engine();
        let msg = json!({"a": 1});
        let metadata = md(&[("k", "v")]);
        let out = e
            .eval_transform("#{msg: #{\"b\": 2}}", &msg, &metadata, "T")
            .unwrap();
        assert_eq!(out.msg, json!({"b": 2}));
        assert_eq!(out.metadata, metadata);
        assert_eq!(out.msg_type, "T");
    }

    #[test]
    fn compile_error() {
        let e = engine();
        let err = e
            .eval_filter("msg. >", &json!({}), &HashMap::new(), "T")
            .unwrap_err();
        assert!(matches!(err, ScriptError::Compile(_)));
    }

    #[test]
    fn filter_non_bool_return_rejected() {
        let e = engine();
        let err = e
            .eval_filter(
                "msg.temperature + 1",
                &json!({"temperature": 1}),
                &HashMap::new(),
                "T",
            )
            .unwrap_err();
        assert!(matches!(err, ScriptError::InvalidReturn(_)));
    }

    #[test]
    fn operation_limit_interrupts() {
        let e = engine();
        let err = e
            .eval_filter("loop { }", &json!({}), &HashMap::new(), "T")
            .unwrap_err();
        assert!(matches!(err, ScriptError::Eval(_)));
    }

    #[test]
    fn eval_value_with_variables() {
        let e = engine();
        let vars: HashMap<String, Value> = [("tempC".to_string(), json!(25))].into_iter().collect();
        let v = e.eval_value("tempC * 9 / 5 + 32", &vars).unwrap();
        assert_eq!(v, json!(77));
        let v = e
            .eval_value("tempC * 9.0 / 5.0 + 32.0", &vars)
            .unwrap();
        assert_eq!(v, json!(77.0));
        assert!(e.eval_value("tempC +", &vars).is_err());
        assert!(e.eval_value("unknown_var + 1", &vars).is_err());
    }

    #[test]
    fn eval_map_returns_json_object() {
        let e = engine();
        let msg = json!({"temperature": 80});
        let v = e
            .eval_map(
                "#{value: msg.temperature, note: \"hot\"}",
                &msg,
                &HashMap::new(),
                "T",
            )
            .unwrap();
        assert_eq!(v, json!({"value": 80, "note": "hot"}));
    }
}
