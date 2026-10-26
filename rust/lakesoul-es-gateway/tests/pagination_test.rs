// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Pagination and streaming: filter-only `from`/`size` pages (WeKnora's
//! knowledge-base copy) cost one page of rows, and delete/update by query
//! stream the match set instead of materializing it.

use std::collections::HashSet;
use std::sync::Arc;

use axum::http::StatusCode;
use lakesoul_es_gateway::{build_router, build_state};
use serde_json::Value;

mod common;

use common::{call, cleanup, scalar_u64, test_config};

const DOCS: usize = 25;

fn chunk_ids(body: &Value) -> Vec<String> {
    body["hits"]["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["_source"]["chunk_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filter_only_pages_and_streams_updates() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_page_{}", &suffix[..10]);
    let index = format!("esgwpage{}", &suffix[..10]);
    let state = build_state(test_config(&index, &table, Some(3), 1))
        .await
        .unwrap();
    let app = build_router(Arc::clone(&state));

    let mut bulk = String::new();
    for doc in 0..DOCS {
        bulk.push_str("{\"create\":{}}\n");
        bulk.push_str(&format!(
            "{{\"content\":\"apple chunk {doc}\",\"chunk_id\":\"c{doc:02}\",\
             \"knowledge_id\":\"k1\",\"knowledge_base_id\":\"kb1\",\
             \"tag_id\":\"\",\"is_enabled\":true,\
             \"embedding\":[1.0,0.0,0.0]}}\n"
        ));
    }
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false, "{body}");

    // Pages of 10: disjoint, complete, and the reported total is a lower
    // bound (`gte`) computed without scanning the whole match set.
    let mut seen = HashSet::new();
    for from in [0usize, 10, 20] {
        let (status, _, body) = call(
            &app,
            "POST",
            &format!("/{index}/_search"),
            Some(&format!(
                r#"{{"query":{{"bool":{{"filter":[{{"term":{{"is_enabled":true}}}}],"must_not":[{{"terms":{{"knowledge_id":["k2"]}}}}]}}}},"from":{from},"size":10,"_source":{{"excludes":["embedding"]}}}}"#
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let ids = chunk_ids(&body);
        let expected = if from + 10 <= DOCS { 10 } else { DOCS - from };
        assert_eq!(ids.len(), expected, "page from={from}: {body}");
        assert_eq!(
            body["hits"]["total"]["relation"], "gte",
            "page from={from}: {body}"
        );
        assert_eq!(
            body["hits"]["total"]["value"].as_u64().unwrap() as usize,
            from + ids.len(),
            "page from={from}: {body}"
        );
        for id in ids {
            assert!(seen.insert(id.clone()), "duplicate {} across pages", id);
        }
    }
    assert_eq!(seen.len(), DOCS, "pages must cover every document");
    assert_eq!(
        scalar_u64(
            &state,
            &format!("select count(*) from lakesoul.default.{table}")
        )
        .await,
        DOCS as u64
    );

    // update_by_query over the whole index (WeKnora disables a knowledge
    // base): streamed, then invisible to the default search filter.
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_update_by_query"),
        Some(r#"{"query":{"terms":{"knowledge_id":["k1"]}},"script":{"source":"ctx._source.is_enabled = false","params":{}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], DOCS, "{body}");
    assert_eq!(body["updated"], DOCS, "{body}");

    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_search"),
        Some(r#"{"query":{"bool":{"filter":[{"term":{"is_enabled":true}}],"must_not":[{"term":{"is_enabled":false}}]}},"size":10}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], 0, "{body}");

    // delete_by_query over the whole index: streamed tombstones.
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_delete_by_query"),
        Some(r#"{"query":{"terms":{"knowledge_id":["k1"]}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["deleted"], DOCS, "{body}");
    assert_eq!(
        scalar_u64(
            &state,
            &format!("select count(*) from lakesoul.default.{table}")
        )
        .await,
        0,
        "merge-on-read must hide every tombstoned row"
    );

    cleanup(&state, &table).await;
}
