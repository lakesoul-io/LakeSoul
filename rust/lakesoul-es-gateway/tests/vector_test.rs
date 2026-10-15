// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector `_search` contract tests: `script_score`/cosine, `min_score`,
//! filters and stale-row handling.

use std::sync::Arc;

use axum::http::StatusCode;
use lakesoul_es_gateway::{build_router, build_state};
use serde_json::Value;

mod common;

use common::{call, cleanup, test_config, test_config_multi};

fn hit_list(body: &Value) -> &Vec<Value> {
    body["hits"]["hits"].as_array().unwrap()
}

fn chunk_id(hit: &Value) -> &str {
    hit["_source"]["chunk_id"].as_str().unwrap()
}

fn score(hit: &Value) -> f64 {
    hit["_score"].as_f64().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_vector_search_contract() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_vector_{}", &suffix[..10]);
    let index = format!("esgwvector{}", &suffix[..10]);
    let state = build_state(test_config(&index, &table, Some(3), 1))
        .await
        .unwrap();
    let app = build_router(Arc::clone(&state));
    let search_path = format!("/{index}/_search");

    let bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"apple","source_id":"s1","source_type":1,"chunk_id":"c1","knowledge_id":"k1","knowledge_base_id":"kb1","tag_id":"","embedding":[1.0,0.0,0.0],"is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"banana","source_id":"s2","source_type":1,"chunk_id":"c2","knowledge_id":"k1","knowledge_base_id":"kb1","tag_id":"","embedding":[0.6,0.8,0.0],"is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"cherry","source_id":"s3","source_type":1,"chunk_id":"c3","knowledge_id":"k2","knowledge_base_id":"kb2","tag_id":"","embedding":[0.0,1.0,0.0],"is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"date","source_id":"s4","source_type":1,"chunk_id":"c4","knowledge_id":"k2","knowledge_base_id":"kb2","tag_id":"","embedding":[-1.0,0.0,0.0],"is_enabled":true}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false);

    // v8 script spelling (a space after the comma) with `_source.excludes`.
    let v8_body = r#"{"query":{"script_score":{"query":{"bool":{"filter":[]}},"script":{"source":"Math.max(cosineSimilarity(params.query_vector, 'embedding'), 0.0)","params":{"query_vector":[1.0,0.0,0.0]}}}},"size":10,"_source":{"excludes":["embedding"]}}"#;
    let (status, _, body) = call(&app, "POST", &search_path, Some(v8_body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = hit_list(&body);
    assert_eq!(hits.len(), 4, "all four rows: {body}");
    assert_eq!(chunk_id(&hits[0]), "c1");
    assert!((score(&hits[0]) - 1.0).abs() < 1e-5, "{}", score(&hits[0]));
    assert_eq!(chunk_id(&hits[1]), "c2");
    assert!((score(&hits[1]) - 0.6).abs() < 1e-5, "{}", score(&hits[1]));
    for hit in hits {
        let score = score(hit);
        assert!((0.0..=1.0).contains(&score), "score out of range: {score}");
        assert!(hit["_source"].get("embedding").is_none());
    }

    // min_score filters the clamped cosine scores.
    let min_score_body = r#"{"query":{"script_score":{"query":{"match_all":{}},"script":{"source":"Math.max(cosineSimilarity(params.query_vector, 'embedding'), 0.0)","params":{"query_vector":[1.0,0.0,0.0]}},"min_score":0.5}},"size":10}"#;
    let (_, _, body) = call(&app, "POST", &search_path, Some(min_score_body)).await;
    let ids: Vec<&str> = hit_list(&body).iter().map(chunk_id).collect();
    assert_eq!(ids, vec!["c1", "c2"], "{body}");

    // Non-scoring filters combine with the vector query.
    let filtered_body = r#"{"query":{"script_score":{"query":{"bool":{"filter":[{"term":{"knowledge_base_id":"kb1"}}]}},"script":{"source":"Math.max(cosineSimilarity(params.query_vector, 'embedding'), 0.0)","params":{"query_vector":[1.0,0.0,0.0]}},"min_score":0.5}},"size":10}"#;
    let (_, _, body) = call(&app, "POST", &search_path, Some(filtered_body)).await;
    let ids: Vec<&str> = hit_list(&body).iter().map(chunk_id).collect();
    assert_eq!(ids, vec!["c1", "c2"]);

    // The v7 spelling has no space after the comma.
    let v7_body = r#"{"query":{"script_score":{"query":{"bool":{"filter":[]}},"script":{"source":"Math.max(cosineSimilarity(params.query_vector,'embedding'), 0.0)","params":{"query_vector":[1.0,0.0,0.0]}}}},"size":1,"_source":{"excludes":["embedding"]}}"#;
    let (status, _, body) = call(&app, "POST", &search_path, Some(v7_body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(chunk_id(&hit_list(&body)[0]), "c1");

    // Older WeKnora images send the unclamped spelling.
    let bare_body = r#"{"query":{"script_score":{"query":{"bool":{"filter":[]}},"script":{"source":"cosineSimilarity(params.query_vector, 'embedding')","params":{"query_vector":[1.0,0.0,0.0]}}}},"size":10,"_source":{"excludes":["embedding"]}}"#;
    let (status, _, body) = call(&app, "POST", &search_path, Some(bare_body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = hit_list(&body);
    assert_eq!(chunk_id(&hits[0]), "c1");
    for hit in hits {
        let score = score(hit);
        assert!((0.0..=1.0).contains(&score), "score out of range: {score}");
    }

    // Dimension mismatch and unknown scripts are rejected.
    let bad_dim = r#"{"query":{"script_score":{"query":{"match_all":{}},"script":{"source":"Math.max(cosineSimilarity(params.query_vector, 'embedding'), 0.0)","params":{"query_vector":[1.0,0.0]}},"min_score":0.0}},"size":10}"#;
    let (status, _, body) = call(&app, "POST", &search_path, Some(bad_dim)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let bad_script = r#"{"query":{"script_score":{"query":{"match_all":{}},"script":{"source":"1 - cosineDistance(params.query_vector, 'embedding')","params":{"query_vector":[1.0,0.0,0.0]}}}},"size":10}"#;
    let (status, _, _) = call(&app, "POST", &search_path, Some(bad_script)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // A soft-deleted row disappears from vector results.
    let (status, _, body) = call(
        &app,
        "POST",
        &format!("/{index}/_delete_by_query"),
        Some(r#"{"query":{"terms":{"source_id":["s1"]}}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, _, body) = call(&app, "POST", &search_path, Some(v8_body)).await;
    let hits = hit_list(&body);
    assert_eq!(hits.len(), 3, "{body}");
    assert_eq!(chunk_id(&hits[0]), "c2");

    cleanup(&state, &table).await;
}

/// Multi-vector `knn`: several declared vector columns in one request, with
/// Elasticsearch `max` semantics by default and the `sum` fusion extension.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_multi_vector_knn_contract() {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = format!("es_gw_knn_{}", &suffix[..10]);
    let index = format!("esgwknn{}", &suffix[..10]);
    let state = build_state(test_config_multi(
        &index,
        &table,
        &[("embedding", 3), ("image_embedding", 2)],
        1,
    ))
    .await
    .unwrap();
    let app = build_router(Arc::clone(&state));
    let search_path = format!("/{index}/_search");

    // c1: text-nearest; c2: text-max; c3: middling in both; c4: image-only;
    // c5: a good text match without an image vector.
    let bulk = [
        r#"{"create":{}}"#,
        r#"{"content":"one","chunk_id":"c1","embedding":[0.8,0.6,0.0],"image_embedding":[1.0,0.0],"is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"two","chunk_id":"c2","embedding":[1.0,0.0,0.0],"image_embedding":[0.0,1.0],"is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"three","chunk_id":"c3","embedding":[0.6,0.8,0.0],"image_embedding":[0.6,0.8],"is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"four","chunk_id":"c4","embedding":[0.0,0.0,1.0],"image_embedding":[1.0,0.0],"is_enabled":true}"#,
        r#"{"create":{}}"#,
        r#"{"content":"five","chunk_id":"c5","embedding":[0.9,0.1,0.0],"is_enabled":true}"#,
        "",
    ]
    .join("\n");
    let (status, _, body) =
        call(&app, "POST", &format!("/{index}/_bulk"), Some(&bulk)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["errors"], false);

    // Elasticsearch semantics: `knn` as an array is combined like dis_max —
    // a row may come from a single clause and the best boosted similarity
    // wins.  Rows missing a clause's vector still rank through the others,
    // and the vector fields stay out of `_source`.
    let max_body = concat!(
        r#"{"knn":[{"field":"embedding","query_vector":[1.0,0.0,0.0],"k":10,"boost":1.0},"#,
        r#"{"field":"image_embedding","query_vector":[1.0,0.0],"k":10,"boost":0.5}],"#,
        r#""size":10,"_source":{"excludes":["embedding","image_embedding"]}}"#
    );
    let (status, _, body) = call(&app, "POST", &search_path, Some(max_body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = hit_list(&body);
    assert_eq!(
        hits.iter().map(chunk_id).collect::<Vec<_>>(),
        vec!["c2", "c5", "c1", "c3", "c4"],
        "{body}"
    );
    // Vectors are L2-normalized on write, so c5's cosine is
    // `0.9 / |[0.9, 0.1]|`.
    let five = 0.9f64 / (0.9f64 * 0.9 + 0.1 * 0.1).sqrt();
    let max_scores: Vec<f64> = hits.iter().map(score).collect();
    for (got, expected) in max_scores.iter().zip([1.0, five, 0.8, 0.6, 0.5]) {
        assert!((got - expected).abs() < 1e-4, "{got} != {expected}: {body}");
    }
    assert!(hits[0]["_source"].get("embedding").is_none(), "{body}");
    assert!(
        hits[0]["_source"].get("image_embedding").is_none(),
        "{body}"
    );

    // The `sum` fusion (a LakeSoul extension) intersects the candidate sets
    // and adds the boosted similarities, so every clause must contribute:
    // c5 (no image vector) drops out and c1 outranks c2.
    let sum_body = max_body.replace(r#""size":10,"#, r#""knn_fusion":"sum","size":10,"#);
    let (status, _, body) = call(&app, "POST", &search_path, Some(&sum_body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = hit_list(&body);
    assert_eq!(
        hits.iter().map(chunk_id).collect::<Vec<_>>(),
        vec!["c1", "c2", "c3", "c4"],
        "{body}"
    );
    let sum_scores: Vec<f64> = hits.iter().map(score).collect();
    for (got, expected) in sum_scores.iter().zip([1.3, 1.0, 0.9, 0.5]) {
        assert!((got - expected).abs() < 1e-4, "{got} != {expected}: {body}");
    }

    // A per-clause `min_score` drops the rows that clause cannot score
    // highly; the body-level `min_score` filters the combined score.
    let clause_min =
        sum_body.replace(r#""boost":0.5}"#, r#""boost":0.5,"min_score":0.4}"#);
    let (_, _, body) = call(&app, "POST", &search_path, Some(&clause_min)).await;
    let hits = hit_list(&body);
    assert_eq!(
        hits.iter().map(chunk_id).collect::<Vec<_>>(),
        vec!["c1", "c4"],
        "{body}"
    );
    let body_min = clause_min.replace(r#""size":10,"#, r#""min_score":1.2,"size":10,"#);
    let (_, _, body) = call(&app, "POST", &search_path, Some(&body_min)).await;
    let hits = hit_list(&body);
    assert_eq!(hits.len(), 1, "{body}");
    assert_eq!(chunk_id(&hits[0]), "c1", "{body}");

    // A single clause object, and the image column searched on its own.
    let single =
        r#"{"knn":{"field":"image_embedding","query_vector":[1.0,0.0],"k":1},"size":1}"#;
    let (status, _, body) = call(&app, "POST", &search_path, Some(single)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = hit_list(&body);
    assert_eq!(hits.len(), 1, "{body}");
    assert_eq!(score(&hits[0]), 1.0, "{body}");
    assert_eq!(chunk_id(&hits[0]), "c1", "{body}");

    // `script_score` now accepts every declared vector column as well.
    let script_body = r#"{"query":{"script_score":{"query":{"bool":{"filter":[]}},"script":{"source":"Math.max(cosineSimilarity(params.query_vector, 'image_embedding'), 0.0)","params":{"query_vector":[1.0,0.0]}}}},"size":2,"_source":{"excludes":["embedding","image_embedding"]}}"#;
    let (status, _, body) = call(&app, "POST", &search_path, Some(script_body)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = hit_list(&body);
    assert_eq!(hits.len(), 2, "{body}");
    assert_eq!(hits.iter().map(score).collect::<Vec<_>>(), vec![1.0, 1.0]);
    assert!(
        hits[0]["_source"].get("image_embedding").is_none(),
        "{body}"
    );

    // Validation: unknown fields, per-clause dimensions, mixing clauses and
    // an unknown fusion mode.
    let unknown = r#"{"knn":{"field":"nope","query_vector":[1.0,0.0]},"size":1}"#;
    let (status, _, _) = call(&app, "POST", &search_path, Some(unknown)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let bad_dim =
        r#"{"knn":{"field":"image_embedding","query_vector":[1.0,0.0,0.0]},"size":1}"#;
    let (status, _, _) = call(&app, "POST", &search_path, Some(bad_dim)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let mixed = r#"{"knn":{"field":"embedding","query_vector":[1.0,0.0,0.0]},"query":{"match":{"content":"apple"}},"size":1}"#;
    let (status, _, _) = call(&app, "POST", &search_path, Some(mixed)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let bad_fusion = r#"{"knn":{"field":"embedding","query_vector":[1.0,0.0,0.0]},"knn_fusion":"rrf","size":1}"#;
    let (status, _, _) = call(&app, "POST", &search_path, Some(bad_fusion)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The clause filter narrows the candidate rows.
    let filtered = concat!(
        r#"{"knn":[{"field":"embedding","query_vector":[1.0,0.0,0.0],"k":10},"#,
        r#"{"field":"image_embedding","query_vector":[1.0,0.0],"k":10,"boost":0.5,"filter":{"term":{"chunk_id":"c1"}}}],"#,
        r#""knn_fusion":"sum","size":10}"#
    );
    let (status, _, body) = call(&app, "POST", &search_path, Some(filtered)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let hits = hit_list(&body);
    assert_eq!(hits.len(), 1, "{body}");
    assert_eq!(chunk_id(&hits[0]), "c1", "{body}");

    cleanup(&state, &table).await;
}
