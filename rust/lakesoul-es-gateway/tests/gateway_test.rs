// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end gateway contract tests against a live PostgreSQL metadata
//! store and local `file://` tables.
//!
//! The request bodies mirror the exact shapes the WeKnora ES drivers send
//! (see the design note), so the tests double as a compatibility fixture.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use axum::http::StatusCode;
use lakesoul_es_gateway::{build_router, build_state};

mod common;

use common::{call, cleanup, scalar_u64, test_config};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_write_path_contract() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_{}", &suffix[..12]);
    let index = format!("esgw{}", &suffix[..12]);
    let config = test_config(&index, &table, Some(3), 1);
    let state = build_state(config).await.unwrap();
    let app = build_router(Arc::clone(&state));

    // Undeclared indexes are rejected (HEAD 404, PUT ES-shaped 404).
    let (status, _, _) = call(&app, "HEAD", "/undeclared", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, body) = call(&app, "PUT", "/undeclared", Some("{}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "index_not_found_exception");

    // Root info and product header (the v8 client requires both).
    let (status, headers, body) = call(&app, "GET", "/", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-elastic-product"], "Elasticsearch");
    assert_eq!(body["version"]["number"], "8.19.6");

    // Declared index: HEAD/PUT and the mapping probe.
    let (status, headers, _) = call(&app, "HEAD", &format!("/{index}"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-elastic-product"], "Elasticsearch");
    let (status, _, body) = call(
        &app,
        "PUT",
        &format!("/{index}"),
        Some(r#"{"settings":{"number_of_shards":"4","number_of_replicas":"1"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["acknowledged"], true);
    let (status, _, body) = call(&app, "GET", &format!("/{index}/_mapping"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body[index.to_string()]["mappings"]["properties"]["chunk_id"]["type"],
        "keyword"
    );
    assert_eq!(
        body[index.to_string()]["mappings"]["properties"]["content"]["type"],
        "text"
    );
    assert_eq!(
        body[index.to_string()]["mappings"]["properties"]["embedding"]["type"],
        "dense_vector"
    );

    // v8 bulk: `create` actions without `_id`.
    let v8_bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"apple apple apple","source_id":"s1","source_type":1,"chunk_id":"c1","knowledge_id":"k1","knowledge_base_id":"kb1","tag_id":"","embedding":[3.0,0.0,0.0],"is_enabled":true,"is_recommended":false}"#,
        r#"{"create":{}}"#,
        r#"{"content":"banana bread","source_id":"s2","source_type":1,"chunk_id":"c2","knowledge_id":"k1","knowledge_base_id":"kb1","tag_id":"","embedding":[0.0,2.0,0.0],"is_enabled":true,"is_recommended":false}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&v8_bulk)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["errors"], false);
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    assert_eq!(body["items"][0]["create"]["status"], 201);
    assert!(body["items"][0]["create"]["_id"].is_string());

    // v7 bulk: `index` actions carrying `_id`.
    let v7_bulk = [
        r#"{"index":{"_id":"client-generated-id"}}"#,
        r#"{"content":"cherry tart","source_id":"s3","source_type":1,"chunk_id":"c3","knowledge_id":"k2","knowledge_base_id":"kb1","embedding":[0.0,0.0,5.0],"is_enabled":true,"is_recommended":true}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&v7_bulk)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["items"][0]["index"]["status"], 201);

    // v7 single-document create.
    let (status, _, body) = call(
        &app,
        "PUT",
        &format!("/{index}/_doc/ignored-id/_create"),
        Some(
            r#"{"content":"date cake","source_id":"s4","source_type":1,"chunk_id":"c4","knowledge_id":"k2","knowledge_base_id":"kb2","is_enabled":false,"is_recommended":false}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "created");
    assert!(body["_id"].is_string());

    assert_eq!(
        scalar_u64(
            &state,
            &format!("select count(*) from lakesoul.default.{table}")
        )
        .await,
        4
    );

    // Embeddings are stored L2-normalized (IP == cosine).
    let batches = state
        .session
        .sql(&format!(
            "select embedding from lakesoul.default.{table} where chunk_id = 'c1'"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::FixedSizeListArray>()
        .unwrap();
    let values = list.value(0);
    let values = values.as_primitive::<Float32Type>();
    let norm: f32 = values.values().iter().map(|v| v * v).sum::<f32>().sqrt();
    assert!(
        (norm - 1.0).abs() < 1e-5,
        "embedding not normalized: {norm}"
    );

    // Update by query: disable one document via the fixed script.
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_update_by_query"),
        Some(
            r#"{"query":{"terms":{"chunk_id":["c1"]}},"script":{"source":"ctx._source.is_enabled = false"}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 1);
    assert_eq!(body["updated"], 1);
    assert_eq!(
        scalar_u64(
            &state,
            &format!(
                "select count(*) from lakesoul.default.{table} where chunk_id = 'c1' and is_enabled = false"
            )
        )
        .await,
        1
    );

    // Move script with strict counters (the client requires total == updated).
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_update_by_query?refresh=true"),
        Some(
            r#"{"query":{"bool":{"filter":[{"term":{"knowledge_base_id":"kb1"}},{"term":{"knowledge_id":"k1"}}]}},"script":{"source":"ctx._source.knowledge_base_id = params.target; ctx._source.tag_id = ''","params":{"target":"kb9"}}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "move update failed: {body}");
    assert_eq!(body["total"], 2, "move counters: {body}");
    assert_eq!(body["updated"], 2, "move counters: {body}");
    assert_eq!(body["version_conflicts"], 0);
    assert_eq!(
        scalar_u64(
            &state,
            &format!(
                "select count(*) from lakesoul.default.{table} where knowledge_base_id = 'kb9'"
            )
        )
        .await,
        2
    );

    // Delete by query: tombstone rows disappear from reads.
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_delete_by_query"),
        Some(r#"{"query":{"terms":{"source_id":["s3"]}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], 1);
    assert_eq!(
        scalar_u64(
            &state,
            &format!("select count(*) from lakesoul.default.{table}")
        )
        .await,
        3
    );

    // Unsupported query shapes fail loudly (ES-shaped error).
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_delete_by_query"),
        Some(r#"{"query":{"match":{"content":"apple"}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "unsupported_operation_exception");

    // Cleanup.
    cleanup(&state, &table).await;
}
