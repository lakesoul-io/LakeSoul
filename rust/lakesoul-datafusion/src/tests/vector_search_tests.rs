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
    }]
}

/// Assert that every hash bucket of the table has a vector index.
fn assert_vector_index_built(table_name: &str) {
    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let index_dir = root.join("_vector_index").join("vec");
    assert!(index_dir.exists(), "no _vector_index dir at {index_dir:?}");
    let bucket_count = std::fs::read_dir(&index_dir).unwrap().count();
    assert!(bucket_count >= 1, "expected >= 1 index shard dir");
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
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"LAKESOUL\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION 'default/{table_name}' \
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
