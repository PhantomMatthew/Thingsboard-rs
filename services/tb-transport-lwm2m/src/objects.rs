use std::collections::HashMap;

use crate::types::ResourceType;

pub fn standard_resource_types() -> HashMap<String, ResourceType> {
    let mut types = HashMap::new();
    for (path, value_type) in [
        ("/3300/0/5700", ResourceType::Float),
        ("/3301/0/5700", ResourceType::Float),
        ("/3302/0/5500", ResourceType::Integer),
        ("/3302/0/5501", ResourceType::Integer),
        ("/3302/0/5750", ResourceType::String),
        ("/3303/0/5700", ResourceType::Float),
        ("/3303/0/5701", ResourceType::Float),
        ("/3303/0/5601", ResourceType::Float),
        ("/3303/0/5602", ResourceType::Float),
        ("/3304/0/5700", ResourceType::Float),
        ("/3304/0/5701", ResourceType::Float),
        ("/3305/0/5700", ResourceType::Float),
        ("/3305/0/5701", ResourceType::Float),
        ("/3200/0/5500", ResourceType::Integer),
        ("/3200/0/5501", ResourceType::Integer),
        ("/3200/0/5750", ResourceType::String),
        ("/3201/0/5550", ResourceType::Integer),
        ("/3201/0/5551", ResourceType::Boolean),
        ("/3201/0/5750", ResourceType::String),
        ("/1/0/0", ResourceType::Integer),
        ("/1/0/1", ResourceType::Integer),
        ("/1/0/2", ResourceType::Integer),
        ("/1/0/3", ResourceType::Integer),
        ("/1/0/6", ResourceType::Integer),
        ("/1/0/7", ResourceType::String),
        ("/1/0/8", ResourceType::String),
        ("/1/0/9", ResourceType::Integer),
    ] {
        types.insert(path.into(), value_type);
    }
    types
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temperature_and_humidity_models_are_explicit() {
        let types = standard_resource_types();
        assert_eq!(types["/3303/0/5700"], ResourceType::Float);
        assert_eq!(types["/3304/0/5700"], ResourceType::Float);
        assert!(!types.contains_key("/9999/0/1"));
    }
}
