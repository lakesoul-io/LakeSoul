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

    // User text with stray syntax characters is parsed leniently instead of
    // failing the request.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"apple )"}}]}},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], 2, "{body}");

    // A whitespace-free Chinese sentence is analyzed into terms (match
    // semantics), not parsed as an exact phrase.
    let bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"重新获取取件码。首先来到丰巢快递柜前,点击屏幕上的取快递。","source_id":"s5","chunk_id":"c5","knowledge_base_id":"kb9","is_enabled":true}"#,
        "",
    ]
    .join("\n");
    let (status, _, _) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"蜂巢取快递验证码摁错怎么办"}}]}},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(hit_chunk_ids(&body), vec!["c5"], "{body}");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyword_search_refills_when_candidates_are_deleted() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_refill_{}", &suffix[..10]);
    let index = format!("esgwrefill{}", &suffix[..10]);
    // Two buckets so the shard statistics merge path is involved too.
    let state = build_state(test_config(&index, &table, None, 2))
        .await
        .unwrap();
    let app = build_router(Arc::clone(&state));

    // 120 documents match "apple"; deleting the first 110 leaves ten live
    // rows that the index no longer covers.  The first candidate pass (100
    // per shard) is all stale, so the search must widen its budget to fill
    // `size`.
    let mut lines: Vec<String> = Vec::new();
    let mut chunk_ids: Vec<String> = Vec::new();
    for number in 0..120 {
        lines.push(r#"{"create":{}}"#.to_string());
        lines.push(format!(
            r#"{{"content":"apple document {number}","source_id":"s{number}","chunk_id":"c{number}","knowledge_base_id":"kb1","embedding":[1.0,0.0,0.0],"is_enabled":true}}"#
        ));
        chunk_ids.push(format!("c{number}"));
    }
    lines.push(String::new());
    let bulk = lines.join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false);

    let deleted: Vec<String> = chunk_ids[..110]
        .iter()
        .map(|id| format!(r#""{id}""#))
        .collect();
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_delete_by_query"),
        Some(&format!(
            r#"{{"query":{{"terms":{{"chunk_id":[{}]}}}}}}"#,
            deleted.join(",")
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_search"),
        Some(r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"size":10}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["hits"]["total"]["value"], 10,
        "the retry must find the ten live documents: {body}"
    );

    cleanup(&state, &table).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_match_analyzer_override() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_analyzer_{}", &suffix[..10]);
    let index = format!("esgwana{}", &suffix[..10]);
    // The default (index-time) analyzer is jieba.
    let state = build_state(test_config(&index, &table, None, 1))
        .await
        .unwrap();
    let app = build_router(Arc::clone(&state));
    let search_path = format!("/{index}/_search");

    let bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"机器学习与向量检索","source_id":"s1","chunk_id":"c1","knowledge_base_id":"kb1","is_enabled":true}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // `whitespace` analyzes the query into the jieba index tokens.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":{"query":"机器 学习","analyzer":"whitespace"}}}]}},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(hit_chunk_ids(&body), vec!["c1"], "{body}");

    // An override equal to the index-time analyzer keeps the normal path.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":{"query":"机器学习","analyzer":"jieba"}}}]}},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(hit_chunk_ids(&body), vec!["c1"], "{body}");

    // Unknown analyzers and syntax queries with an override are rejected.
    let (status, _, _) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":{"query":"machine","analyzer":"nope"}}}]}}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _, _) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":{"query":"\"machine learning\"","analyzer":"whitespace"}}}]}}}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    cleanup(&state, &table).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_highlight_contract() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_highlight_{}", &suffix[..10]);
    let index = format!("esgwhl{}", &suffix[..10]);
    let state = build_state(test_config(&index, &table, None, 1))
        .await
        .unwrap();
    let app = build_router(Arc::clone(&state));
    let search_path = format!("/{index}/_search");

    let bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"apple apple apple","source_id":"s1","chunk_id":"c1","knowledge_base_id":"kb1","is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"apple banana","source_id":"s2","chunk_id":"c2","knowledge_base_id":"kb1","is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"机器学习与向量检索","source_id":"s3","chunk_id":"c3","knowledge_base_id":"kb1","is_enabled":true}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false);

    // Default tags wrap every matched term in the fragment.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"highlight":{"fields":{"content":{}}},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first = &body["hits"]["hits"][0];
    assert!(first["_id"].is_string(), "{body}");
    let fragment = first["highlight"]["content"][0]
        .as_str()
        .unwrap_or_else(|| panic!("missing highlight: {body}"));
    assert!(
        fragment.contains("<em>apple</em>"),
        "fragment must wrap the term: {fragment}"
    );

    // Custom tags are honored.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"highlight":{"fields":{"content":{}},"pre_tags":["<b>"],"post_tags":["</b>"]},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let fragment = body["hits"]["hits"][0]["highlight"]["content"][0]
        .as_str()
        .unwrap();
    assert!(fragment.contains("<b>apple</b>"), "{fragment}");

    // Chinese sentences highlight the segmented words.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"机器学习"}}]}},"highlight":{"fields":{"content":{}}},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let fragment = body["hits"]["hits"][0]["highlight"]["content"][0]
        .as_str()
        .unwrap();
    assert!(fragment.contains("<em>机器学习</em>"), "{fragment}");

    // Without a highlight request the response shape is unchanged.
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"size":10}"#),
    )
    .await;
    assert!(body["hits"]["hits"][0].get("highlight").is_none(), "{body}");

    cleanup(&state, &table).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_sort_and_aggregations_contract() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_sortagg_{}", &suffix[..10]);
    let index = format!("esgwsort{}", &suffix[..10]);
    let state = build_state(test_config(&index, &table, None, 1))
        .await
        .unwrap();
    let app = build_router(Arc::clone(&state));
    let search_path = format!("/{index}/_search");

    // c4 has no source_type/is_enabled, so the missing-value rules and the
    // terms buckets are exercised too.
    let bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"apple","source_id":"s1","source_type":3,"chunk_id":"c1","knowledge_base_id":"kb2","is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"apple banana","source_id":"s2","source_type":1,"chunk_id":"c2","knowledge_base_id":"kb1","is_enabled":false}"#,
        r#"{"create":{}}"#,
        r#"{"content":"banana","source_id":"s3","source_type":2,"chunk_id":"c3","knowledge_base_id":"kb2","is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"cherry","source_id":"s4","chunk_id":"c4","knowledge_base_id":"kb1"}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false);

    // Ascending sort; ties stay in primary-key order and the doc missing the
    // field sorts last even though the sort field is excluded from _source.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"match_all":{}},"sort":[{"source_type":"asc"}],"_source":{"includes":["chunk_id"]},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(hit_chunk_ids(&body), vec!["c2", "c3", "c1", "c4"], "{body}");
    assert!(
        body["hits"]["hits"][0]["_source"]
            .get("source_type")
            .is_none()
    );

    // Descending order keeps missing values last.
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"match_all":{}},"sort":[{"source_type":{"order":"desc"}}],"_source":{"includes":["chunk_id"]},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(hit_chunk_ids(&body), vec!["c1", "c3", "c2", "c4"], "{body}");

    // The string shorthand sorts ascending; `_id` pagination happens after
    // sorting.
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"match_all":{}},"sort":["chunk_id"],"_source":{"includes":["chunk_id"]},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(hit_chunk_ids(&body), vec!["c1", "c2", "c3", "c4"], "{body}");
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"match_all":{}},"sort":[{"chunk_id":"desc"}],"_source":{"includes":["chunk_id"]},"size":10}"#,
        ),
    )
    .await;
    assert_eq!(hit_chunk_ids(&body), vec!["c4", "c3", "c2", "c1"], "{body}");
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"match_all":{}},"sort":["chunk_id"],"from":1,"size":2,"_source":{"includes":["chunk_id"]}}"#,
        ),
    )
    .await;
    assert_eq!(hit_chunk_ids(&body), vec!["c2", "c3"], "{body}");

    // Keyword search honours an explicit sort instead of the relevance order
    // (c1 outranks c2 by BM25, the sort reverses that).
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"sort":[{"chunk_id":"desc"}],"size":10}"#,
        ),
    )
    .await;
    assert_eq!(hit_chunk_ids(&body), vec!["c2", "c1"], "{body}");

    // Unsupported sort keys and orders are rejected.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"sort":[{"nope":"asc"}]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"sort":[{"chunk_id":"sideways"}]}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // Aggregations run over the full verified match set, independent of the
    // page (size 0) and of the sort.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"match_all":{}},"size":0,"aggs":{
                "by_type":{"terms":{"field":"source_type"}},
                "by_kb":{"terms":{"field":"knowledge_base_id","order":{"_key":"asc"}}},
                "top_kb":{"terms":{"field":"knowledge_base_id","size":1}},
                "enabled":{"terms":{"field":"is_enabled"}},
                "avg_type":{"avg":{"field":"source_type"}},
                "stats_type":{"stats":{"field":"source_type"}},
                "distinct_kb":{"cardinality":{"field":"knowledge_base_id"}},
                "chunks":{"value_count":{"field":"chunk_id"}},
                "nested":{"terms":{"field":"knowledge_base_id"},"aggs":{"avg_type":{"avg":{"field":"source_type"}}}}
            }}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["hits"]["total"]["value"], 0);
    let aggregations = &body["aggregations"];

    let by_type = aggregations["by_type"]["buckets"].as_array().unwrap();
    assert_eq!(by_type.len(), 3);
    for (index, key) in [1, 2, 3].iter().enumerate() {
        assert_eq!(by_type[index]["key"], *key, "{body}");
        assert_eq!(by_type[index]["doc_count"], 1);
    }
    assert_eq!(aggregations["by_type"]["sum_other_doc_count"], 0);
    assert_eq!(aggregations["by_type"]["doc_count_error_upper_bound"], 0);

    let by_kb = aggregations["by_kb"]["buckets"].as_array().unwrap();
    assert_eq!(by_kb[0]["key"], "kb1");
    assert_eq!(by_kb[1]["key"], "kb2");

    // size caps the buckets and reports the rest as sum_other_doc_count.
    let top_kb = aggregations["top_kb"]["buckets"].as_array().unwrap();
    assert_eq!(top_kb.len(), 1);
    assert_eq!(top_kb[0]["key"], "kb1");
    assert_eq!(aggregations["top_kb"]["sum_other_doc_count"], 2);

    // Boolean term keys are 1/0 with the ES-style string form; the doc
    // missing is_enabled is not in a bucket.
    let enabled = aggregations["enabled"]["buckets"].as_array().unwrap();
    assert_eq!(enabled[0]["key"], 1);
    assert_eq!(enabled[0]["key_as_string"], "true");
    assert_eq!(enabled[0]["doc_count"], 2);
    assert_eq!(enabled[1]["key"], 0);
    assert_eq!(enabled[1]["doc_count"], 1);

    // Metrics skip the missing value: (3 + 1 + 2) / 3.
    assert_eq!(aggregations["avg_type"]["value"], 2.0);
    assert_eq!(aggregations["stats_type"]["count"], 3);
    assert_eq!(aggregations["stats_type"]["min"], 1.0);
    assert_eq!(aggregations["stats_type"]["max"], 3.0);
    assert_eq!(aggregations["stats_type"]["avg"], 2.0);
    assert_eq!(aggregations["stats_type"]["sum"], 6.0);
    assert_eq!(aggregations["distinct_kb"]["value"], 2);
    assert_eq!(aggregations["chunks"]["value"], 4);

    let nested = aggregations["nested"]["buckets"].as_array().unwrap();
    assert_eq!(nested[0]["key"], "kb1");
    assert_eq!(nested[0]["avg_type"]["value"], 1.0);
    assert_eq!(nested[1]["key"], "kb2");
    assert_eq!(nested[1]["avg_type"]["value"], 2.5);

    // Aggregations also run on the keyword path.
    let (_, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(
            r#"{"query":{"bool":{"must":[{"match":{"content":"apple"}}]}},"size":0,"aggs":{"by_kb":{"terms":{"field":"knowledge_base_id"}}}}"#,
        ),
    )
    .await;
    let buckets = body["aggregations"]["by_kb"]["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 2, "{body}");
    assert_eq!(buckets[0]["key"], "kb1");
    assert_eq!(buckets[0]["doc_count"], 1);
    assert_eq!(buckets[1]["key"], "kb2");
    assert_eq!(buckets[1]["doc_count"], 1);

    // Unknown aggregation types and fields are rejected.
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"aggs":{"hist":{"histogram":{"field":"source_type"}}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _, body) = call(
        &app,
        "POST",
        &search_path,
        Some(r#"{"aggs":{"nope":{"terms":{"field":"nope"}}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    cleanup(&state, &table).await;
}
