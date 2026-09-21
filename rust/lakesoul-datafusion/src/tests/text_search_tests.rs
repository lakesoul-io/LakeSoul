// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Text search through DataFusion SQL.
//!
//! `WHERE text_match(column, query) LIMIT k` is rewritten by
//! [`TextSearchPushdownRule`](crate::planner::text_search_rule::TextSearchPushdownRule)
//! into a scan over the text index candidates; the rewritten exact predicate
//! above the scan removes stale candidates.  These tests cover the SQL
//! surface end-to-end against real tables (index built by the write path).

use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::execution::context::SessionContext;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_metadata::MetaDataClient;

use crate::cli::CoreArgs;
use crate::lakesoul_table::LakeSoulTable;
use crate::tests::create_table_with_text_index;

fn text_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("body", DataType::Utf8, true),
    ]))
}

fn text_configs() -> Vec<crate::text_index::TextIndexTableConfig> {
    vec![crate::text_index::TextIndexTableConfig {
        column: "body".to_string(),
        params: crate::text_index::TextIndexParams {
            tokenizer: "jieba".to_string(),
            with_positions: true,
            stored: false,
        },
        management: crate::index::IndexManagementConfig {
            rebuild_mode: "auto".to_string(),
            max_delta_ratio: 1.0,
            gc_enabled: true,
            gc_grace_seconds: 3600,
            gc_keep_generations: 1,
        },
    }]
}

fn batch(rows: &[(u64, &str)]) -> RecordBatch {
    let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();
    let texts: Vec<&str> = rows.iter().map(|(_, text)| *text).collect();
    RecordBatch::try_new(
        text_schema(),
        vec![
            Arc::new(UInt64Array::from(ids)),
            Arc::new(StringArray::from(texts)),
        ],
    )
    .unwrap()
}

fn clean_table_dir(table_name: &str) {
    let path = std::path::Path::new("default").join(table_name);
    let _ = std::fs::remove_dir_all(&path);
}

fn default_args() -> CoreArgs {
    CoreArgs {
        warehouse_prefix: None,
        endpoint: None,
        s3_bucket: None,
        s3_access_key: None,
        s3_secret_key: None,
        s3_virtual_host_style: false,
        worker_threads: 2,
    }
}

/// Number of uploaded split files of a table.
fn split_count(table_name: &str) -> usize {
    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .join("_text_index");
    let mut count = 0usize;
    let mut stack = vec![root];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.to_string_lossy().ends_with(".split") {
                count += 1;
            }
        }
    }
    count
}

async fn explain_plan(ctx: &Arc<SessionContext>, sql: &str) -> String {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut text = String::new();
    for batch in &batches {
        for col in batch.columns() {
            for row in 0..batch.num_rows() {
                text.push_str(
                    &array_value_to_string(col.as_ref(), row).unwrap_or_default(),
                );
                text.push('\n');
            }
        }
    }
    text
}

async fn query_ids(ctx: &Arc<SessionContext>, sql: &str) -> Vec<u64> {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        ids.extend(values.values().iter().copied());
    }
    ids.sort_unstable();
    ids
}

/// Collect the first column preserving the result order.
async fn query_ids_in_order(ctx: &Arc<SessionContext>, sql: &str) -> Vec<u64> {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        ids.extend(values.values().iter().copied());
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_text_search_is_exact_over_upserts() {
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "text_search_sql_exact";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(text_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_text_index(
        client.clone(),
        table_name,
        builder.build(),
        &text_configs(),
    )
    .await
    .unwrap();

    // The write path auto-builds the text index from the new files.
    LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch(&[
            (1, "apple pie"),
            (2, "banana bread"),
            (3, "机器学习与向量检索"),
        ]))
        .await
        .unwrap();
    assert!(split_count(table_name) > 0, "text index was not built");

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let base = format!("\"lakesoul\".default.{table_name}");

    // English and Chinese queries through SQL.
    let sql = format!("select id from {base} where text_match(body, 'apple') limit 10");
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("LakeSoulTextSearchExec"),
        "plan must use the text-index exec:\n{explain}"
    );
    assert_eq!(query_ids(&ctx, &sql).await, vec![1]);

    let sql =
        format!("select id from {base} where text_match(body, '机器学习') limit 10");
    assert_eq!(query_ids(&ctx, &sql).await, vec![3]);

    // Update id=1 so its current text no longer matches "apple": the stale
    // index candidate must be filtered by the exact predicate.
    LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch(&[(1, "cherry tart")]))
        .await
        .unwrap();
    let sql = format!("select id from {base} where text_match(body, 'apple') limit 10");
    assert!(
        query_ids(&ctx, &sql).await.is_empty(),
        "stale candidate leaked through SQL"
    );
    let sql = format!("select id from {base} where text_match(body, 'cherry') limit 10");
    assert_eq!(query_ids(&ctx, &sql).await, vec![1]);

    // Without LIMIT there is no candidate pushdown (the index path is
    // top-k based); the UDF still answers exactly over a full scan.
    let sql = format!("select id from {base} where text_match(body, 'banana')");
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        !explain.contains("LakeSoulTextSearchExec"),
        "no-LIMIT query must fall back:\n{explain}"
    );
    assert_eq!(query_ids(&ctx, &sql).await, vec![2]);

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_index_compacts_drifted_shards() {
    use lakesoul_common::IndexKind;
    use lakesoul_io::index::prefix::shard_index_prefix;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "text_search_compaction";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // One shard for the whole table, so split counts are unambiguous.
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(text_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_text_index(
        client.clone(),
        table_name,
        builder.build(),
        &text_configs(),
    )
    .await
    .unwrap();

    let table = LakeSoulTable::for_name(table_name).await.unwrap();
    for text in ["apple pie", "cherry tart", "date cake"] {
        table.execute_upsert(batch(&[(1, text)])).await.unwrap();
    }
    // Three deltas: the latest split is 1/3 of the shard's documents.
    assert_eq!(split_count(table_name), 3, "expected three delta splits");

    // The fourth write pushes stale/fresh over max_delta_ratio=1.0 and the
    // shard is rebuilt from every active file; `delete_term` keeps only the
    // newest version of the row.
    table
        .execute_upsert(batch(&[(1, "elderberry pie")]))
        .await
        .unwrap();

    let files = client
        .get_data_files_by_table_name(table_name, "default")
        .await
        .unwrap();
    let prefix = shard_index_prefix(&files, IndexKind::Text, "body");
    let catalog = client.index_catalog::<lakesoul_text::TextSplitEntry>(IndexKind::Text);
    let view = catalog
        .resolve(&prefix)
        .await
        .unwrap()
        .expect("text index commit");
    assert_eq!(
        view.segments.len(),
        1,
        "drifted shard must compact into one split"
    );
    assert_eq!(
        view.segments[0].num_docs, 1,
        "only the newest row version may survive the rebuild"
    );
    assert!(
        view.generation >= 2,
        "compaction publishes a new generation"
    );
    // The three old splits stay on disk during the GC grace period.
    assert_eq!(split_count(table_name), 4);

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let base = format!("\"lakesoul\".default.{table_name}");
    for stale in ["apple", "cherry", "date"] {
        let sql =
            format!("select id from {base} where text_match(body, '{stale}') limit 10");
        assert!(
            query_ids(&ctx, &sql).await.is_empty(),
            "superseded text '{stale}' must not match after compaction"
        );
    }
    let sql =
        format!("select id from {base} where text_match(body, 'elderberry') limit 10");
    assert_eq!(query_ids(&ctx, &sql).await, vec![1]);

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_text_search_orders_by_bm25() {
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "text_search_sql_order";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // A single shard so all documents share one BM25 statistics set: ranking
    // across shards is approximate by design (per-split statistics).
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(text_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_text_index(
        client.clone(),
        table_name,
        builder.build(),
        &text_configs(),
    )
    .await
    .unwrap();
    LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch(&[
            (1, "apple apple apple"),
            (2, "apple apple banana"),
            (3, "apple banana banana"),
            (4, "banana banana banana"),
        ]))
        .await
        .unwrap();

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let base = format!("\"lakesoul\".default.{table_name}");

    // Equal-length documents, higher term frequency must rank first; the
    // scan returns the global top-k across buckets.
    let sql = format!(
        "select id from {base} where text_match(body, 'apple') \
         order by text_score(body, 'apple') desc limit 2"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("LakeSoulTextSearchExec"),
        "plan must use the text-index exec:\n{explain}"
    );
    assert!(
        explain.contains("order_by=true"),
        "plan must mark the relevance order:\n{explain}"
    );
    assert_eq!(query_ids_in_order(&ctx, &sql).await, vec![1, 2]);

    let sql = format!(
        "select id from {base} where text_match(body, 'apple') \
         order by text_score(body, 'apple') desc limit 3"
    );
    assert_eq!(query_ids_in_order(&ctx, &sql).await, vec![1, 2, 3]);

    // The updated row no longer contains the term: verification drops its
    // stale candidate, and the remaining rows keep their relevance order.
    LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch(&[(1, "banana banana banana")]))
        .await
        .unwrap();
    let sql = format!(
        "select id from {base} where text_match(body, 'apple') \
         order by text_score(body, 'apple') desc limit 3"
    );
    assert_eq!(query_ids_in_order(&ctx, &sql).await, vec![2, 3]);

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_create_table_declares_text_index_via_option() {
    // The `text_index_columns` OPTIONS entry of `CREATE EXTERNAL TABLE` must
    // be validated at creation and stored as a table property (so writes
    // auto-build the index), mirroring `vector_index_columns`.
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "text_search_sql_create_opt";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let text_option =
        serde_json::json!([{"column": "body", "tokenizer": "jieba"}]).to_string();
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            body STRING
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('text_index_columns' '{text_option}', 'hashBucketNum' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    let table_info = client
        .get_table_info_by_table_name(table_name, "default")
        .await
        .unwrap()
        .expect("table must exist");
    let configs =
        crate::text_index::parse_text_index_from_table_properties(&table_info.properties)
            .unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].column, "body");

    // An unusable declaration is rejected before any metadata is created.
    let table_name2 = "text_search_sql_create_badopt";
    let _ = client.drop_table(table_name2, "default").await;
    clean_table_dir(table_name2);
    let bad_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name2} (
            id BIGINT NOT NULL PRIMARY KEY,
            body STRING
         ) STORED AS LAKESOUL \
         LOCATION 'default/{table_name2}' \
         OPTIONS ('text_index_columns' 'not-json')"
    );
    let err = ctx.sql(&bad_sql).await.unwrap_err();
    assert!(
        err.to_string().contains("invalid text_index_columns"),
        "expected an option validation error, got: {err}"
    );

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let _ = client.drop_table(table_name2, "default").await;
    clean_table_dir(table_name2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_match_works_without_an_index() {
    // A table without `text_index_columns`: the exact UDF still works, just
    // over a full scan.
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "text_search_sql_plain";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(text_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    crate::tests::create_table(client.clone(), table_name, builder.build())
        .await
        .unwrap();
    LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch(&[(1, "apple pie"), (2, "banana bread")]))
        .await
        .unwrap();
    assert_eq!(split_count(table_name), 0, "no index should be built");

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         where text_match(body, 'apple') limit 10"
    );
    assert_eq!(query_ids(&ctx, &sql).await, vec![1]);

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}
