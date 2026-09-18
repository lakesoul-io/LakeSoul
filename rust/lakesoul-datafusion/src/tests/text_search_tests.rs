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
