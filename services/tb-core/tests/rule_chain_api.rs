//! Phase 4 规则链 REST API 集成测试。
//! 注意：与 rest_api.rs / telemetry_api.rs 共用 thingsboard_test 库，本文件只放一个 #[tokio::test]。

mod common;

use reqwest::{Client, StatusCode};
use serde_json::{Value, json};

use common::{bearer, setup_tenant_admin, start_server};

#[tokio::test]
async fn rule_chain_api_end_to_end() {
    let base = start_server().await;
    let client = Client::builder().no_proxy().build().unwrap();
    let (tenant_token, _tenant_id) =
        setup_tenant_admin(&client, &base, "RuleChain Tenant", "t.admin@rulechain.io").await;

    // 1. 建租户自动种子根链
    let resp = client
        .get(format!("{base}/api/ruleChains?pageSize=100"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: Value = resp.json().await.unwrap();
    let chains = page["data"].as_array().unwrap();
    let root = chains
        .iter()
        .find(|c| c["name"] == "Root Rule Chain")
        .expect("tenant should have seeded root rule chain");
    assert_eq!(root["root"], true);
    let root_id = root["id"].as_str().unwrap().to_string();

    // 2. 根链节点：switch(first) → save_timeseries / save_attributes
    let resp = client
        .get(format!("{base}/api/ruleChain/{root_id}/nodes"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let meta: Value = resp.json().await.unwrap();
    assert_eq!(meta["nodes"].as_array().unwrap().len(), 3);
    let first = meta["firstNodeId"].as_str().unwrap();
    let first_node = meta["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == first)
        .unwrap();
    assert_eq!(first_node["type"], "filter.msg_type_switch");
    let edges = meta["edges"].as_array().unwrap();
    assert_eq!(edges.len(), 2);
    let edge_types: Vec<&str> = edges.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert!(edge_types.contains(&"POST_TELEMETRY_REQUEST"));
    assert!(edge_types.contains(&"POST_ATTRIBUTES_REQUEST"));

    // 3. POST 新链 + PUT nodes + GET nodes 往返一致
    let resp = client
        .post(format!("{base}/api/ruleChain"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"name": "Secondary"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let chain: Value = resp.json().await.unwrap();
    assert_eq!(chain["root"], false);
    let chain_id = chain["id"].as_str().unwrap().to_string();

    let put_body = json!({
        "nodes": [
            {"type": "filter.msg_type_switch", "name": "switch", "configuration": {}},
            {"type": "filter.script", "name": "hot", "configuration": {"script": "msg.temperature > 50"}},
            {"type": "action.create_alarm", "name": "alarm", "configuration": {"alarmType": "HighTemp", "severity": "CRITICAL"}},
        ],
        "edges": [
            {"fromIndex": 0, "toIndex": 1, "type": "POST_TELEMETRY_REQUEST"},
            {"fromIndex": 1, "toIndex": 2, "type": "True"},
        ],
        "firstNodeIndex": 0,
    });
    let resp = client
        .put(format!("{base}/api/ruleChain/{chain_id}/nodes"))
        .header("Authorization", bearer(&tenant_token))
        .json(&put_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{base}/api/ruleChain/{chain_id}/nodes"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let meta: Value = resp.json().await.unwrap();
    let nodes = meta["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    let types: Vec<&str> = nodes.iter().map(|n| n["type"].as_str().unwrap()).collect();
    assert!(types.contains(&"filter.msg_type_switch"));
    assert!(types.contains(&"filter.script"));
    assert!(types.contains(&"action.create_alarm"));
    let filter_node = nodes.iter().find(|n| n["type"] == "filter.script").unwrap();
    assert_eq!(
        filter_node["configuration"]["script"],
        "msg.temperature > 50"
    );
    let first = meta["firstNodeId"].as_str().unwrap();
    assert_eq!(
        nodes.iter().find(|n| n["id"] == first).unwrap()["type"],
        "filter.msg_type_switch"
    );
    let edges = meta["edges"].as_array().unwrap();
    assert_eq!(edges.len(), 2);
    let filter_id = filter_node["id"].as_str().unwrap();
    assert!(edges.iter().any(|e| e["from"] == first
        && e["to"] == filter_id
        && e["type"] == "POST_TELEMETRY_REQUEST"));
    assert!(
        edges
            .iter()
            .any(|e| e["from"] == filter_id && e["type"] == "True")
    );

    // 4. 校验：未知节点类型 / 越界 edge / 缺 firstNodeIndex → 400
    let resp = client
        .put(format!("{base}/api/ruleChain/{chain_id}/nodes"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "nodes": [{"type": "no.such.node", "name": "x", "configuration": {}}],
            "edges": [],
            "firstNodeIndex": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let resp = client
        .put(format!("{base}/api/ruleChain/{chain_id}/nodes"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "nodes": [{"type": "action.log", "name": "x", "configuration": {}}],
            "edges": [{"fromIndex": 0, "toIndex": 5, "type": "Success"}],
            "firstNodeIndex": 0,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let resp = client
        .put(format!("{base}/api/ruleChain/{chain_id}/nodes"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({
            "nodes": [{"type": "action.log", "name": "x", "configuration": {}}],
            "edges": [],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // 5. root 切换：POST root=true → 租户只剩一个根链
    let resp = client
        .post(format!("{base}/api/ruleChain"))
        .header("Authorization", bearer(&tenant_token))
        .json(&json!({"name": "New Root", "root": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let new_root_id = resp.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let resp = client
        .get(format!("{base}/api/ruleChains?pageSize=100"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    let page: Value = resp.json().await.unwrap();
    let roots: Vec<&Value> = page["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["root"] == true)
        .collect();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0]["id"], new_root_id);

    // 6. DELETE 级联删除
    let resp = client
        .delete(format!("{base}/api/ruleChain/{chain_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = client
        .get(format!("{base}/api/ruleChain/{chain_id}"))
        .header("Authorization", bearer(&tenant_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // 7. 跨租户访问 → 404/403（用新租户 token 访问旧链）
    let (other_token, _) =
        setup_tenant_admin(&client, &base, "Other Tenant", "t.admin@other.io").await;
    let resp = client
        .get(format!("{base}/api/ruleChain/{new_root_id}"))
        .header("Authorization", bearer(&other_token))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
