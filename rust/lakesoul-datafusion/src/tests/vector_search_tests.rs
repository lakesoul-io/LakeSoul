// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Vector search through DataFusion SQL.
//!
//! `ORDER BY array_distance(vec, q) LIMIT k` (and the inner-product
//! variant) is rewritten by [`VectorSearchPushdownRule`] into a scan over
//! the IVF+RaBitQ index candidates.  These tests cover the optimizer rule
//! directly and end-to-end through SQL against a real table with a built
//! index.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::OptimizerRule;
use datafusion::prelude::{col, lit};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_metadata::MetaDataClient;

use crate::cli::CoreArgs;
use crate::planner::vector_search_rule::VectorSearchPushdownRule;
use crate::udf::vector_search_marker::LakeSoulVectorSearchOptions;

const DIM: usize = 8;

fn vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn random_vectors(n: usize) -> Vec<Vec<f32>> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..n)
        .map(|_| (0..DIM).map(|_| rng.r#gen::<f32>() * 2.0 - 1.0).collect())
        .collect()
}

fn brute_force_topk(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u64> {
    let mut scored: Vec<(f32, u64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let d: f32 = v.iter().zip(query).map(|(a, b)| (a - b) * (a - b)).sum();
            (d, i as u64)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// Build a `Limit(Sort(array_distance(vec, ARRAY[...])))` plan over a
/// mem-table source with the vector schema.
fn build_limit_sort_plan(sort_expr: Expr, limit: usize) -> DFResult<LogicalPlan> {
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]])?);
    let source = provider_as_source(mem_table);
    LogicalPlanBuilder::scan("t", source, None)?
        .sort(vec![sort_expr.sort(true, false)])?
        .limit(0, Some(limit))?
        .build()
}

fn udf_call(udf: Arc<datafusion::logical_expr::ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        udf, args,
    ))
}

fn array_literal(values: &[f32]) -> Expr {
    udf_call(
        datafusion::functions_nested::make_array::make_array_udf(),
        values.iter().map(|v| lit(*v as f64)).collect(),
    )
}

fn array_distance_expr() -> Expr {
    udf_call(
        datafusion::functions_nested::distance::array_distance_udf(),
        vec![
            col("vec"),
            array_literal(&[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
        ],
    )
}

fn cosine_distance_expr() -> Expr {
    udf_call(
        datafusion::functions_nested::cosine_distance::cosine_distance_udf(),
        vec![
            col("vec"),
            array_literal(&[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
        ],
    )
}

fn inner_product_expr() -> Expr {
    udf_call(
        datafusion::functions_nested::inner_product::inner_product_udf(),
        vec![
            col("vec"),
            array_literal(&[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
        ],
    )
}

fn has_marker(plan: &LogicalPlan) -> bool {
    let mut found = false;
    plan.apply(&mut |node: &LogicalPlan| {
        if let LogicalPlan::TableScan(ts) = node
            && ts.filters.iter().any(|f| {
                matches!(f, Expr::ScalarFunction(call)
                    if call.func.name() == crate::udf::vector_search_marker::VECTOR_SEARCH_MARKER)
            })
        {
            found = true;
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

#[test]
fn rule_rewrites_array_distance_limit() {
    let plan = build_limit_sort_plan(array_distance_expr(), 10).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(
            plan.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )
        .unwrap()
        .data;
    assert!(has_marker(&rewritten), "marker must be injected");
    // idempotent: a second pass must not change the plan again
    let second = VectorSearchPushdownRule
        .rewrite(
            rewritten.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )
        .unwrap();
    assert!(!second.transformed);
}

#[test]
fn rule_ignores_cosine_distance() {
    let plan = build_limit_sort_plan(cosine_distance_expr(), 10).unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(!result.transformed, "cosine_distance must fall back");
}

#[test]
fn rule_ignores_inner_product_asc() {
    // inner_product must be DESC; an ASC sort is a different semantic.
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let source = provider_as_source(mem_table);
    let plan = LogicalPlanBuilder::scan("t", source, None)
        .unwrap()
        .sort(vec![inner_product_expr().sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(!result.transformed, "inner_product ASC must not match");
}

#[test]
fn rule_ignores_missing_limit() {
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let source = provider_as_source(mem_table);
    let plan = LogicalPlanBuilder::scan("t", source, None)
        .unwrap()
        .sort(vec![array_distance_expr().sort(true, false)])
        .unwrap()
        .build()
        .unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(!result.transformed, "no LIMIT must not match");
}

// ---------------------------------------------------------------------------
// E2E (requires PostgreSQL)
// ---------------------------------------------------------------------------

fn make_batch(ids: &[u64], vectors: &[Vec<f32>]) -> RecordBatch {
    let id_array = arrow::array::UInt64Array::from(ids.to_vec());
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    RecordBatch::try_new(
        vector_schema(),
        vec![Arc::new(id_array), Arc::new(vec_array)],
    )
    .unwrap()
}

fn clean_table_dir(table_name: &str) {
    // The native index/files are not removed by drop_table; remove the
    // on-disk state so each run starts from a clean table.
    let path = std::path::Path::new("default").join(table_name);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(
        std::env::current_dir()
            .unwrap()
            .join("default")
            .join(table_name),
    );
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

/// The vector index configuration used by the tests (declared at table
/// creation through the `vector_index_columns` table property).
fn vector_configs() -> Vec<crate::vector_index::VectorIndexTableConfig> {
    vec![crate::vector_index::VectorIndexTableConfig {
        column: "vec".to_string(),
        dim: DIM,
        nlist: 4,
        total_bits: 7,
        metric: "L2".to_string(),
        rotator_type: "FhtKac".to_string(),
        seed: 42,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    }]
}

/// Assert that the table's vector index is committed: the
/// `_vector_index/vec` tree contains at least one `LATEST` manifest (each
/// built shard directory is sealed with one).
fn assert_vector_index_built(table_name: &str) {
    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let index_dir = root.join("_vector_index").join("vec");
    assert!(index_dir.exists(), "no _vector_index dir at {index_dir:?}");
    let mut latest_count = 0usize;
    let mut stack = vec![index_dir.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().map(|n| n == "LATEST").unwrap_or(false) {
                latest_count += 1;
            }
        }
    }
    assert!(
        latest_count >= 1,
        "expected a committed LATEST manifest under {index_dir:?}"
    );
}

/// Reads the first data parquet file of `table_name` and returns the
/// DataType of its `vec` column (to assert what was actually stored).
fn stored_vec_column_type(table_name: &str) -> DataType {
    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let mut found = None;
    for entry in std::fs::read_dir(&root).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().map(|e| e == "parquet").unwrap_or(false) {
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            let file = std::fs::File::open(&path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader.take(1) {
                let batch = batch.unwrap();
                if let Some(field) = batch.schema().column_with_name("vec") {
                    found = Some(field.1.data_type().clone());
                    break;
                }
            }
        }
    }
    found.expect("no data parquet file found for table")
}

async fn explain_plan(
    ctx: &Arc<datafusion::execution::context::SessionContext>,
    sql: &str,
) -> String {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_end_to_end() {
    use crate::catalog::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_e2e";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // 1. Create the table (u64 pk + FixedSizeList<Float32, 8>) and declare
    //    the vector index through the `vector_index_columns` property.
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    // 2. Write 200 random vectors — the index is auto-built after commit.
    let n = 200u64;
    let vectors = random_vectors(n as usize);
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_batch(&ids, &vectors);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    // 4. Query through SQL.
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );

    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert_eq!(ids_result.len(), 5, "LIMIT 5 rows: {ids_result:?}");

    let truth = brute_force_topk(&vectors, &query, 5);
    let recall = ids_result.iter().filter(|id| truth.contains(id)).count() as f64 / 5.0;
    assert!(
        recall >= 0.5,
        "recall too low: {recall} (got {ids_result:?}, truth {truth:?})"
    );

    // 5. The physical plan uses the vector-index scan.
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "plan must use the vector-index exec:\n{explain}"
    );

    // 6. cosine_distance falls back to the full scan.
    let cosine_sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by cosine_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {cosine_sql}")).await;
    assert!(
        !explain.contains("LakeSoulVectorSearchExec"),
        "cosine_distance must fall back:\n{explain}"
    );

    // 7. An incremental write auto-builds delta segments: vectors written
    //    afterwards become searchable.
    let more = random_vectors(100);
    let more_ids: Vec<u64> = (200..300).collect();
    let more_batch = make_batch(&more_ids, &more);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(more_batch)
        .await
        .unwrap();

    let probe = more[7].clone();
    let pq = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let incremental_sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{pq}]) limit 5"
    );
    let df = ctx.sql(&incremental_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut incremental_ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        incremental_ids.extend(arr.values().iter().copied());
    }
    assert!(
        incremental_ids.iter().any(|id| *id >= 200),
        "incremental write must be searchable: {incremental_ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_with_where_and_nprobe() {
    use crate::catalog::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_where";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 120u64;
    let vectors = random_vectors(n as usize);
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_batch(&ids, &vectors);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    // A session with a custom nprobe extension.
    let session_config = crate::create_lakesoul_session_config()
        .unwrap()
        .with_extension(Arc::new(LakeSoulVectorSearchOptions { nprobe: 1 }));
    let ctx = crate::create_lakesoul_session_ctx_with_config(
        client,
        &default_args(),
        session_config,
    )
    .unwrap();

    let q = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8]
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         where id % 2 = 0 \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "WHERE variant must still use the vector-index exec:\n{explain}"
    );

    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert!(
        !ids_result.is_empty(),
        "expected at least one candidate row passing the WHERE filter"
    );
    assert!(
        ids_result.iter().all(|id| id % 2 == 0),
        "WHERE must be applied on candidate rows: {ids_result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_falls_back_without_index() {
    use crate::catalog::create_table;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_noindex";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table(client.clone(), table_name, builder.build())
        .await
        .unwrap();

    let n = 60u64;
    let vectors = random_vectors(n as usize);
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_batch(&ids, &vectors);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    // No vector index is built.

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let q = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8]
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        !explain.contains("LakeSoulVectorSearchExec"),
        "without an index the query must fall back to a full scan:\n{explain}"
    );

    // The result is still correct (exact top-5 over all rows).
    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    let truth = brute_force_topk(&vectors, &q_str_to_vec(&q), 5);
    assert_eq!(ids_result, truth, "fallback must be exact");
}

fn q_str_to_vec(q: &str) -> Vec<f32> {
    q.split(',').map(|s| s.trim().parse().unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_create_table_declares_vector_index_via_option() {
    // The `vector_index_columns` OPTIONS entry of `CREATE EXTERNAL TABLE`
    // must be validated at creation and stored as a table property (no
    // post-hoc property update needed).
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_create_opt";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let vector_option = serde_json::json!([{
        "column": "vec",
        "dim": DIM,
        "nlist": 4,
        "total_bits": 7,
        "metric": "L2",
    }])
    .to_string();

    // A `FLOAT[]` column validates against the vector-index rules (List of
    // floats); the option is stored as the table property.
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"LAKESOUL\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('vector_index_columns' '{vector_option}', 'hash_bucket_num' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    // The property round-trips: the provider must expose the declared
    // vector configs, and a fresh table lookup keeps them.
    let table_info = client
        .get_table_info_by_table_name(table_name, "default")
        .await
        .unwrap()
        .expect("table must exist");
    let configs = crate::vector_index::parse_vector_index_from_table_properties(
        &table_info.properties,
    )
    .unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].column, "vec");
    assert_eq!(configs[0].dim, DIM);

    // An invalid option value is rejected before any metadata is created.
    let table_name2 = "vec_search_sql_create_badopt";
    let _ = client.drop_table(table_name2, "default").await;
    clean_table_dir(table_name2);
    let bad_sql = format!(
        "CREATE EXTERNAL TABLE \"LAKESOUL\".default.{table_name2} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION 'default/{table_name2}' \
         OPTIONS ('vector_index_columns' 'not-json')"
    );
    let err = ctx.sql(&bad_sql).await.unwrap_err();
    assert!(
        err.to_string().contains("invalid vector_index_columns"),
        "expected an option validation error, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_full_chain_insert_auto_builds_index() {
    // Full SQL chain: CREATE EXTERNAL TABLE (declaring the vector index
    // through OPTIONS) -> INSERT INTO -> the auto index build must seal a
    // LATEST manifest, EXPLAIN must pick LakeSoulVectorSearchExec, and the
    // search must return rows.  `FLOAT[]` maps to List<Float32>, so this
    // exercises the native Float32 index path.
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_sql_chain_f32";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();

    let cfg = serde_json::json!([{"column": "vec", "dim": 8, "nlist": 4, "total_bits": 7, "metric": "L2"}])
        .to_string();
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"LAKESOUL\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL LOCATION '{location}'
         OPTIONS ('vector_index_columns' '{cfg}', 'hash_bucket_num' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    let insert_sql = format!(
        "INSERT INTO \"LAKESOUL\".default.{table_name}
         SELECT CAST(g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value*0.1, 0.5, -0.5, 1.0, -1.0, 0.0]
         FROM generate_series(0, 99) AS g(value)"
    );
    ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();

    // The index must be committed (a LATEST manifest per shard), and the
    // data column stored as Float32 lists (SQL FLOAT -> Float32).
    assert_vector_index_built(table_name);
    let stored = stored_vec_column_type(table_name);
    assert!(
        matches!(&stored, DataType::List(f)
            if matches!(f.data_type(), DataType::Float32)),
        "FLOAT[] column should be stored as List<Float32>, got {stored:?}"
    );

    // EXPLAIN confirms the query is served by the vector-index scan.
    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let select_sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {select_sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "SQL insert path must be served by the vector-index exec:\n{explain}"
    );

    // And the query returns results from the inserted rows.
    let df = ctx.sql(&select_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        ids.extend(arr.values().iter().copied());
    }
    assert_eq!(ids.len(), 5, "SQL insert search returned: {ids:?}");
    assert!(
        ids.iter().all(|id| (0..100).contains(id)),
        "ids must come from the inserted batch: {ids:?}"
    );

    // An incremental SQL insert is auto-indexed too (delta build).
    let insert2_sql = format!(
        "INSERT INTO \"LAKESOUL\".default.{table_name}
         SELECT CAST(100 + g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value*0.2, -0.5, 0.5, 0.0, 1.0, -1.0]
         FROM generate_series(0, 99) AS g(value)"
    );
    ctx.sql(&insert2_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let probe: Vec<f32> = [
        (0f64).sin() as f32,
        (50f64).cos() as f32,
        (50f64 * 0.2) as f32,
        -0.5,
        0.5,
        0.0,
        1.0,
        -1.0,
    ]
    .to_vec();
    let pq = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let incremental_sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{pq}]) limit 5"
    );
    let df = ctx.sql(&incremental_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut incremental_ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        incremental_ids.extend(arr.values().iter().copied());
    }
    assert!(
        incremental_ids.contains(&150),
        "incremental SQL insert must be searchable: {incremental_ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_insert_float64_vectors_converted_to_f32_before_indexing() {
    // SQL `DOUBLE[]` maps to List<Float64>: the rows stay Float64 on disk,
    // and the auto index build converts them to Float32 (the index and its
    // search math are float32). Without the conversion this write fails.
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_sql_chain_f64";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();

    let cfg = serde_json::json!([{"column": "vec", "dim": 8, "nlist": 4, "total_bits": 7, "metric": "L2"}])
        .to_string();
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"LAKESOUL\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec DOUBLE[] NOT NULL
         ) STORED AS LAKESOUL LOCATION '{location}'
         OPTIONS ('vector_index_columns' '{cfg}', 'hash_bucket_num' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    let insert_sql = format!(
        "INSERT INTO \"LAKESOUL\".default.{table_name}
         SELECT CAST(g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value*0.1, 0.5, -0.5, 1.0, -1.0, 0.0]
         FROM generate_series(0, 99) AS g(value)"
    );
    ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();

    // Data stays Float64 on disk; the index build converted it to f32.
    let stored = stored_vec_column_type(table_name);
    assert!(
        matches!(&stored, DataType::List(f)
            if matches!(f.data_type(), DataType::Float64)),
        "DOUBLE[] column should be stored as List<Float64>, got {stored:?}"
    );
    assert_vector_index_built(table_name);

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let select_sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {select_sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "Float64 insert path must be served by the vector-index exec:\n{explain}"
    );

    let df = ctx.sql(&select_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        ids.extend(arr.values().iter().copied());
    }
    assert_eq!(ids.len(), 5, "Float64 insert search returned: {ids:?}");
}

/// Read the `generation` field of every LATEST manifest under the table's
/// `_vector_index` tree.
fn latest_generations(table_name: &str) -> Vec<u64> {
    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let mut out = Vec::new();
    let mut stack = vec![root.join("_vector_index")];
    while let Some(dir) = stack.pop() {
        if !dir.exists() {
            continue;
        }
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().map(|n| n == "LATEST").unwrap_or(false) {
                let text = std::fs::read_to_string(&path).unwrap();
                let generation = text.split(':').next().unwrap().parse::<u64>().unwrap();
                out.push(generation);
            }
        }
    }
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_writes_auto_rebuild_when_delta_ratio_exceeded() {
    use crate::catalog::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_auto_rebuild_drift";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // Aggressive ratio: two incremental writes of ~10% drift already
    // exceed it, so the third write must trigger a full rebuild.
    let mut configs = vector_configs();
    configs[0].rebuild_mode = "auto".to_string();
    configs[0].max_delta_ratio = 0.05;
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    // w1: fresh build (100 rows).
    let mut id = 0u64;
    let mut write = |n: usize, ctx: &mut Vec<Vec<f32>>| {
        let vectors = random_vectors(n);
        ctx.extend(vectors.iter().cloned());
        let ids: Vec<u64> = (id..id + n as u64).collect();
        id += n as u64;
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };
    let mut stored = Vec::new();
    write(100, &mut stored).await;
    assert!(
        latest_generations(table_name).iter().all(|g| *g == 1),
        "fresh build publishes generation 1: {:?}",
        latest_generations(table_name)
    );

    // w2: 10 more rows (10% drift vs base=100) — still incremental (the
    // ratio is only checked on the *next* write), generation stays at 1.
    write(10, &mut stored).await;
    write(10, &mut stored).await;
    let gens = latest_generations(table_name);
    assert!(
        gens.iter().any(|g| *g >= 2),
        "drift past max_delta_ratio must rebuild (new generation): {gens:?}"
    );

    // The rebuilt index still serves searches, including late rows.
    let probe = &stored[115];
    let q = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert!(
        ids_result.contains(&115),
        "rebuilt index must find drifted rows: {ids_result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_mode_none_never_rebuilds() {
    use crate::catalog::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_no_auto_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let mut configs = vector_configs();
    configs[0].rebuild_mode = "none".to_string();
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    let write = |n: u64| {
        let vectors = random_vectors(n as usize);
        let ids: Vec<u64> = (0..n).collect();
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };

    // A whole sequence of equal-sized writes must never bump the
    // generation when rebuild_mode is "none".
    write(50).await;
    write(50).await;
    write(50).await;
    write(50).await;
    let gens = latest_generations(table_name);
    assert!(
        gens.iter().all(|g| *g == 1),
        "rebuild_mode 'none' must never rebuild: {gens:?}"
    );
    let _ = client;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_rebuild_vector_index_rebuilds_all_shards() {
    use crate::catalog::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_manual_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // Disable auto rebuild so the manual call is the only way generations
    // bump.
    let mut configs = vector_configs();
    configs[0].rebuild_mode = "none".to_string();
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    let mut offset = 0u64;
    for _ in 0..4 {
        let vectors = random_vectors(50);
        let ids: Vec<u64> = (offset..offset + 50).collect();
        offset += 50;
        let batch = make_batch(&ids, &vectors);
        table.execute_upsert(batch).await.unwrap();
    }
    assert!(
        latest_generations(table_name).iter().all(|g| *g == 1),
        "no auto rebuild expected: {:?}",
        latest_generations(table_name)
    );

    let rebuilt = table.rebuild_vector_index().await.unwrap();
    assert!(rebuilt >= 1, "at least one shard rebuilt, got {rebuilt}");
    assert!(
        latest_generations(table_name).iter().any(|g| *g >= 2),
        "manual rebuild must publish a new generation: {:?}",
        latest_generations(table_name)
    );

    // Search still works and reaches late rows.
    let vectors = random_vectors(1);
    let q = vectors[0]
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut rows = 0usize;
    for batch in &batches {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 5);
}

/// Vectors concentrated near `per_group` evenly-distributed anchor points
/// (with small noise) so a table write spreads ~evenly across its clusters.
fn clustered_vectors(
    anchors: &[[f32; DIM]],
    per_group: usize,
    noise: f32,
    rng: &mut rand::rngs::StdRng,
) -> Vec<Vec<f32>> {
    use rand::Rng;
    let mut out = Vec::new();
    for anchor in anchors {
        for _ in 0..per_group {
            let v: Vec<f32> = anchor
                .iter()
                .map(|x| x + (rng.r#gen::<f32>() - 0.5) * noise)
                .collect();
            out.push(v);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_skew_triggers_rebuild_even_when_shard_ratio_is_low() {
    use crate::catalog::create_table_with_vector_index;
    use rand::SeedableRng;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_cluster_skew_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // Single hash bucket, so one shard holds the whole table and each of
    // the 4 clusters has a meaningful base size (~40 vectors).
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let mut rng = rand::rngs::StdRng::seed_from_u64(99);
    let anchors: [[f32; DIM]; 4] = [
        [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4],
        [-0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4],
        [0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4],
        [-0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4],
    ];
    let mut next_id = 0u64;
    let upsert = |vectors: Vec<Vec<f32>>, start_id: u64| {
        let ids: Vec<u64> = (start_id..start_id + vectors.len() as u64).collect();
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };

    // w1: 160 vectors spread evenly over the 4 anchors (40 per cluster).
    upsert(clustered_vectors(&anchors, 40, 0.05, &mut rng), next_id).await;
    next_id = 160;
    assert!(
        latest_generations(table_name).iter().all(|g| *g == 1),
        "fresh build publishes generation 1: {:?}",
        latest_generations(table_name)
    );

    // w2 + w3: 40 evenly-spread vectors each (10 per cluster, cumulative
    // per-cluster delta 20/40 = 0.5 < 1.0) — still incremental.
    for _ in 0..2 {
        upsert(clustered_vectors(&anchors, 10, 0.05, &mut rng), next_id).await;
        next_id += 40;
    }
    assert!(
        latest_generations(table_name).iter().all(|g| *g == 1),
        "mild uniform growth must not rebuild: {:?}",
        latest_generations(table_name)
    );

    // w4: 60 copies of anchor[0] all land in one cluster, taking its
    // cumulative delta to 80 vs ~40 base (> 1.0) while the shard-wide
    // ratio is (80 + 80) / 160 = 1.0 — exactly at (not above) the old
    // shard threshold.  The rebuild decision runs on the *next* write.
    upsert(vec![anchors[0].to_vec(); 60], next_id).await;
    next_id += 60;
    assert!(
        latest_generations(table_name).iter().all(|g| *g == 1),
        "drift becomes visible on the write after the skewed flush: {:?}",
        latest_generations(table_name)
    );

    // w5: a tiny write.  Pre-write, one cluster has ~80 delta vs ~40 base
    // (ratio > 1.0) while the shard ratio is still 1.0: the per-cluster
    // rule rebuilds, the old shard-level (>1.0) rule would not.
    upsert(clustered_vectors(&anchors[..1], 2, 0.05, &mut rng), next_id).await;
    let gens = latest_generations(table_name);
    assert!(
        gens.iter().any(|g| *g >= 2),
        "skewed cluster growth must trigger a rebuild: {gens:?}"
    );

    // The rebuilt index serves the skewed rows.
    let probe = anchors[0];
    let q = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"LAKESOUL\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert!(
        ids_result.iter().any(|id| *id >= 240),
        "rebuilt index must find the skewed cluster's rows: {ids_result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uniform_growth_does_not_trigger_per_cluster_rebuild_before_ratio() {
    use crate::catalog::create_table_with_vector_index;
    use rand::SeedableRng;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_cluster_uniform_no_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let anchors: [[f32; DIM]; 4] = [
        [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4],
        [-0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4],
        [0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4],
        [-0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4],
    ];
    let mut next_id = 0u64;
    let upsert = |vectors: Vec<Vec<f32>>, start_id: u64| {
        let ids: Vec<u64> = (start_id..start_id + vectors.len() as u64).collect();
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };

    // Base 160, then three even writes of 40 (10 per cluster each):
    // cumulative per-cluster delta stays 30 vs base ~40 (< 1.0).
    upsert(clustered_vectors(&anchors, 40, 0.05, &mut rng), next_id).await;
    next_id = 160;
    for _ in 0..3 {
        upsert(clustered_vectors(&anchors, 10, 0.05, &mut rng), next_id).await;
        next_id += 40;
    }
    let gens = latest_generations(table_name);
    assert!(
        gens.iter().all(|g| *g == 1),
        "even growth below the per-cluster ratio must never rebuild: {gens:?}"
    );
}
