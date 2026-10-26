// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Dynamic index lifecycle: `PUT /{index}` on an index that is not declared
//! in the configuration, provisioning from the first document, rediscovery
//! from metadata after a restart, and `DELETE /{index}`.

use std::sync::Arc;

use axum::http::StatusCode;
use lakesoul_es_gateway::config::{GatewayConfig, LakesoulConfig};
use lakesoul_es_gateway::{build_router, build_state};
use serde_json::Value;

mod common;

use common::call;

fn dynamic_config() -> GatewayConfig {
    GatewayConfig {
        server: Default::default(),
        lakesoul: LakesoulConfig {
            namespace: "default".to_string(),
            warehouse_prefix: None,
            endpoint: None,
            s3_bucket: None,
            s3_access_key: None,
            s3_secret_key: None,
            s3_virtual_host_style: false,
            provision_on_start: true,
        },
        defaults: Default::default(),
        indexes: Vec::new(),
    }
}

fn chunk_ids(body: &Value) -> Vec<String> {
    body["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["_source"]["chunk_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_index_is_provisioned_on_first_write() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    // Mixed case exercises the table-name folding LakeSoul applies
    // (index `DynGw...` → table `dyngw...`).
    let index = format!("DynGw{}", &suffix[..12]);
    let config = dynamic_config();

    let state = build_state(config.clone()).await.unwrap();
    let app = build_router(Arc::clone(&state));

    // The admin creates the index from WeKnora; no table exists yet.
    let (status, _, _) = call(&app, "HEAD", &format!("/{index}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, body) = call(&app, "PUT", &format!("/{index}"), Some("{}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["acknowledged"], true);
    let (status, _, _) = call(&app, "HEAD", &format!("/{index}"), None).await;
    assert_eq!(status, StatusCode::OK);

    // Until a document arrives the dimension is unknown, so the mapping has
    // no embedding field; WeKnora only inspects `chunk_id` here.
    let (status, _, body) = call(&app, "GET", &format!("/{index}/_mapping"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let properties = &body[index.to_string()]["mappings"]["properties"];
    assert_eq!(properties["chunk_id"]["type"], "keyword");
    assert_eq!(properties["content"]["type"], "text");
    assert!(properties.get("embedding").is_none(), "{body}");

    // The first write fixes the vector dimension and provisions the table.
    let docs = [
        ("c1", "apple apple", [3.0, 0.0, 0.0, 0.0]),
        ("c2", "apple banana", [0.0, 2.0, 0.0, 0.0]),
        ("c3", "banana split", [0.0, 0.0, 5.0, 0.0]),
    ];
    let mut bulk = String::new();
    for (chunk, content, embedding) in docs {
        bulk.push_str("{\"create\":{}}\n");
        bulk.push_str(&format!(
            "{{\"content\":\"{content}\",\"chunk_id\":\"{chunk}\",\
             \"knowledge_id\":\"k1\",\"knowledge_base_id\":\"kb1\",\
             \"tag_id\":\"\",\"is_enabled\":true,\
             \"embedding\":{}}}\n",
            serde_json::to_string(&embedding).unwrap()
        ));
    }
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false, "{body}");

    // The mapping now reports the fixed dimension.
    let (_, _, body) = call(&app, "GET", &format!("/{index}/_mapping"), None).await;
    let embedding = &body[index.to_string()]["mappings"]["properties"]["embedding"];
    assert_eq!(embedding["type"], "dense_vector", "{body}");
    assert_eq!(embedding["dims"], 4, "{body}");

    // Keyword and vector retrieval both see the documents.
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_search"),
        Some(r#"{"query":{"bool":{"filter":[{"term":{"is_enabled":true}}],"must":[{"match":{"content":"banana"}}]}},"size":10,"_source":{"excludes":["embedding"]}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut keyword_hits = chunk_ids(&body);
    keyword_hits.sort();
    assert_eq!(keyword_hits, vec!["c2", "c3"], "{body}");
    assert!(body["hits"]["hits"][0].get("_score").is_some());

    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_search"),
        Some(r#"{"query":{"script_score":{"query":{"bool":{"filter":[{"term":{"is_enabled":true}}]}},"script":{"source":"Math.max(cosineSimilarity(params.query_vector, 'embedding'), 0.0)","params":{"query_vector":[1.0,0.0,0.0,0.0]}},"min_score":0.0}},"size":10,"_source":{"excludes":["embedding"]}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(chunk_ids(&body)[0], "c1", "{body}");

    // A new gateway process (same configuration) rediscovers the table from
    // LakeSoul metadata: no declaration, no in-memory state.
    let restarted = build_state(config).await.unwrap();
    let app = build_router(Arc::clone(&restarted));
    let (status, _, _) = call(&app, "HEAD", &format!("/{index}"), None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_search"),
        Some(r#"{"query":{"match":{"content":"apple"}},"size":10}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(chunk_ids(&body).len(), 2, "{body}");

    // Deleting the dynamic index drops its table.
    let (status, _, body) = call(&app, "DELETE", &format!("/{index}"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _, _) = call(&app, "HEAD", &format!("/{index}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let table = index.to_lowercase();
    let _ = std::fs::remove_dir_all(
        std::env::current_dir().unwrap().join("default").join(table),
    );
}
