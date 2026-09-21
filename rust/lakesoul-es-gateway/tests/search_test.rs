// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Keyword `_search` contract tests: BM25 ordering, `_score`, filters,
//! stale-candidate verification and the filter-only copy path.

use std::sync::Arc;

use axum::http::StatusCode;
use lakesoul_es_gateway::{build_router, build_state};
use serde_json::Value;

mod common;

use common::{call, cleanup, test_config};

fn hit_chunk_ids(body: &Value) -> Vec<String> {
    body["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["_source"]["chunk_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_keyword_search_contract() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_search_{}", &suffix[..10]);
    let index = format!("esgwsearch{}", &suffix[..10]);
    let state = build_state(test_config(&index, &table, Some(3), 1))
        .await
        .unwrap();
    let app = build_router(Arc::clone(&state));
    let search_path = format!("/{index}/_search");

    // Two documents with different term frequencies (equal lengths, so BM25
    // ranks by frequency), one disabled and one with a missing is_enabled
    // (which must count as enabled).
    let bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"apple apple apple","source_id":"s1","source_type":1,"chunk_id":"c1","knowledge_id":"k1","knowledge_base_id":"kb1","tag_id":"","embedding":[3.0,0.0,0.0],"is_enabled":true,"is_recommended":false}"#,
        r#"{"create":{}}"#,
        r#"{"content":"apple banana","source_id":"s2","source_type":1,"chunk_id":"c2","knowledge_id":"k1","knowledge_base_id":"kb1","tag_id":"","embedding":[0.0,2.0,0.0],"is_enabled":true,"is_recommended":false}"#,
        r#"{"create":{}}"#,
        r#"{"content":"banana split","source_id":"s3","source_type":1,"chunk_id":"c3","knowledge_id":"k2","knowledge_base_id":"kb1","tag_id":"","embedding":[0.0,0.0,5.0],"is_enabled":false,"is_recommended":false}"#,
        r#"{"create":{}}"#,
        r#"{"content":"banana bread","source_id":"s4","source_type":1,"chunk_id":"c4","knowledge_id":"k2","knowledge_base_id":"kb2","tag_id":"","embedding":[1.0,1.0,0.0],"is_recommended":true}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false);

    // Keyword search with a filter and the v8 `_source.excludes`.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}],"filter":[{"term":{"knowledge_base_id":"kb1"}}]}},"size":10,"_source":{"excludes":["embedding"]}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], 2);
    let ids = hit_chunk_ids(&body);
    assert_eq!(ids, vec!["c1", "c2"], "BM25 order: {body}");
    let first = &body["hits"]["hits"][0];
    assert!(first["_id"].is_string(), "numeric _id: {body}");
    assert!(first["_score"].as_f64().is_some_and(|score| score > 0.0));
    assert!(first["_source"].get("embedding").is_none());

    // size is honoured.
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"size":1}"#),
    )
    .await;
    assert_eq!(hit_chunk_ids(&body), vec!["c1"]);

    // must_not is_enabled:false excludes the disabled document, and a missing
    // is_enabled counts as enabled.
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"banana"}}],"must_not":[{"term":{"is_enabled":false}}]}},"size":10}"#,
        ),
    )
    .await;
    let mut ids = hit_chunk_ids(&body);
    ids.sort();
    assert_eq!(ids, vec!["c2", "c4"]);

    // Filter-only copy path: no scoring, full `_source` including embedding,
    // from/size pagination.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"filter":[{"term":{"knowledge_base_id":"kb1"}}]}},"from":0,"size":500}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], 3);
    assert_eq!(body["hits"]["hits"][0]["_score"], 0.0);
    let embedding = body["hits"]["hits"][0]["_source"]["embedding"]
        .as_array()
        .expect("copy path returns embeddings")
        .len();
    assert_eq!(embedding, 3);

    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"filter":[{"term":{"knowledge_base_id":"kb1"}}]}},"from":1,"size":1}"#,
        ),
    )
    .await;
    assert_eq!(body["hits"]["total"]["value"], 1);

    // Deleting a document makes its old index candidate unsearchable.
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_delete_by_query"),
        Some(r#"{"query":{"terms":{"source_id":["s1"]}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"size":10}"#),
    )
    .await;
    assert_eq!(hit_chunk_ids(&body), vec!["c2"]);

    // A match on a non-indexed column is rejected.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"query":{"match":{"chunk_id":"c1"}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "unsupported_operation_exception");

    cleanup(&state, &table).await;
}
