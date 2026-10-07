// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Differential oracle for the incremental executor.
//!
//! Every round appends a random batch of inserts/updates/deletes to the source,
//! runs `INSERT INTO mv <definition>` through the executor and then compares
//! the user-visible MV rows with the full definition query over the live source
//! rows.  Running the statement twice in a row also checks idempotence.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    BoolAggKind, DistinctAggKind, IVM_SOURCE_COLUMN, IvmRuntime, IvmSqlExecutor,
    IvmTableOptions, MinMaxKind, PhysicalFormat, VarianceKind, WindowColumn,
    WindowFunction, WindowGroupSpec, approx_distinct_mv_schema_for,
    array_agg_expr_mv_schema_for, array_agg_groups_mv_schema_for,
    array_agg_mv_schema_for, avg_mv_schema_for, bool_agg_groups_mv_schema_for,
    distinct_agg_groups_mv_schema_for, distinct_agg_mv_schema_for,
    grouping_sets_mv_schema_for, keyed_join_output_primary_keys,
    keyed_join_view_schema_for, median_groups_mv_schema_for, median_mv_schema_for,
    min_max_expr_mv_schema_for, min_max_groups_mv_schema_for, min_max_mv_schema_for,
    multi_window_mv_schema_for, semi_anti_mv_schema_for, string_agg_expr_mv_schema_for,
    string_agg_groups_mv_schema_for, string_agg_mv_schema_for,
    sum_count_groups_mv_schema_for, sum_count_mv_schema_for, sum_expr_mv_schema_for,
    top_k_mv_schema_for, union_all_mv_schema_for, union_distinct_mv_schema_for,
    union_output_schema_for, variance_groups_mv_schema_for, variance_mv_schema_for,
    window_aggregate_mv_schema_for, window_columns_mv_schema_for,
    window_ranking_mv_schema_for, window_value_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

/// A tiny deterministic PRNG so failures are reproducible.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn range(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

/// The default source schema with a nullable value column.
fn nullable_source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, String, Option<i64>, &str)], schema: &SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.1.as_str()),
            )),
            Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// One appended source row: `(key, group, value, change)`.
type SourceRow = (i64, String, Option<i64>, &'static str);

/// One random value; about a quarter of them are NULL when the workload
/// allows NULLs.
fn oracle_value(rng: &mut Lcg, nullable: bool) -> Option<i64> {
    if nullable && rng.range(4) == 0 {
        None
    } else {
        Some(rng.range(100) as i64)
    }
}

async fn collect_rows(ctx: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|column| {
                        let array = batch.column(column);
                        if array.is_null(row) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(array.as_ref(), row).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

async fn run_oracle(
    tag: &str,
    source_count: usize,
    mv_schema: SchemaRef,
    mv_primary_keys: Vec<String>,
    definition: &str,
    reference: &str,
    mv_query: &str,
) {
    run_oracle_with(
        tag,
        source_count,
        source_schema(),
        false,
        mv_schema,
        mv_primary_keys,
        definition,
        reference,
        mv_query,
    )
    .await;
}

/// Run one differential oracle over random mutation rounds.
///
/// `schema` seeds the source tables; when `nullable_values` is set some
/// inserted and updated rows carry a NULL value.
#[allow(clippy::too_many_arguments)]
async fn run_oracle_with(
    tag: &str,
    source_count: usize,
    schema: SchemaRef,
    nullable_values: bool,
    mv_schema: SchemaRef,
    mv_primary_keys: Vec<String>,
    definition: &str,
    reference: &str,
    mv_query: &str,
) {
    run_oracle_seeded(
        tag,
        source_count,
        schema,
        nullable_values,
        &[],
        mv_schema,
        mv_primary_keys,
        definition,
        reference,
        mv_query,
    )
    .await;
}

/// One initial row of a seeded oracle: `(source index, key, group, value)`.
type SeedRow = (usize, i64, &'static str, Option<i64>);

/// Run a differential oracle with initial rows in the sources, so joins on
/// keys or values that the random rounds would rarely produce still have
/// matches.
#[allow(clippy::too_many_arguments)]
async fn run_oracle_seeded(
    tag: &str,
    source_count: usize,
    schema: SchemaRef,
    nullable_values: bool,
    seed_rows: &[SeedRow],
    mv_schema: SchemaRef,
    mv_primary_keys: Vec<String>,
    definition: &str,
    reference: &str,
    mv_query: &str,
) {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let mv_name = format!("oracle_{tag}_mv_{suffix}");
    let ctx = SessionContext::new();
    let mut sources = Vec::with_capacity(source_count);
    let mut source_names = Vec::with_capacity(source_count);
    for index in 0..source_count {
        let source_name = format!("oracle_{tag}_src{index}_{suffix}");
        let source = runtime
            .create_table(
                IvmTableOptions::new(
                    source_name.clone(),
                    format!(
                        "file://{}",
                        dir.path().join(format!("src{index}")).display()
                    ),
                    schema.clone(),
                )
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN)
                .with_file_format(PhysicalFormat::Vortex),
            )
            .await
            .unwrap();
        ctx.register_table(
            source_name.as_str(),
            Arc::new(runtime.table_provider(&source)),
        )
        .unwrap();
        sources.push(source);
        source_names.push(source_name);
    }
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                mv_name.clone(),
                format!("file://{}", dir.path().join("mv").display()),
                mv_schema,
            )
            .with_primary_keys(mv_primary_keys)
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    ctx.register_table(mv_name.as_str(), Arc::new(runtime.table_provider(&mv)))
        .unwrap();

    let substitute = |sql: &str| {
        let mut sql = sql.to_string();
        for (index, name) in source_names.iter().enumerate() {
            sql = sql.replace(&format!("__SRC{index}__"), name);
        }
        sql.replace("__SRC__", &source_names[0])
            .replace("__MV__", &mv_name)
    };
    // The executor's definition has no `op <> 'delete'` filter: `delete` rows
    // are retractions for the view.  The reference recompute must filter them
    // explicitly.
    let definition_sql = substitute(definition);
    let reference_sql = substitute(reference);
    let mv_sql = substitute(mv_query);
    let insert_sql = format!("INSERT INTO {mv_name} {definition_sql}");

    let executor = IvmSqlExecutor::new(runtime).with_session(ctx.clone());
    // The seed (and the round count) can be varied to hunt for bugs:
    // `IVM_ORACLE_SEED=7 IVM_ORACLE_ROUNDS=30 cargo test -p lakesoul-ivm \
    //  --test sql_oracle`. Every oracle mixes its tag into the seed so one run
    // covers distinct mutation streams.
    // A custom seed or round count may drive the random data into a sparse
    // state (the reference itself never produces a row); the run is then
    // vacuous rather than wrong, because every round still compares the view
    // with the reference.
    let custom_seed = std::env::var("IVM_ORACLE_SEED").is_ok()
        || std::env::var("IVM_ORACLE_ROUNDS").is_ok();
    let mut seed = std::env::var("IVM_ORACLE_SEED")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(42);
    for byte in tag.bytes() {
        seed = seed.wrapping_mul(31).wrapping_add(u64::from(byte));
    }
    let rounds = std::env::var("IVM_ORACLE_ROUNDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let mut rng = Lcg(seed);
    let mut live: Vec<Vec<(i64, String, Option<i64>)>> = vec![Vec::new(); source_count];
    let mut next_key = 0i64;
    let mut mv_rows = 0usize;
    for (source_index, key, group, value) in seed_rows {
        let rows = vec![(*key, (*group).to_string(), *value, "insert")];
        sources[*source_index]
            .append_batch(executor.runtime().client(), batch(&rows, &schema))
            .await
            .unwrap();
        live[*source_index].push((*key, (*group).to_string(), *value));
        next_key = next_key.max(*key + 1);
    }

    for round in 0..rounds {
        let mutations = 1 + rng.range(3) as usize;
        let mut rows_per_source: Vec<Vec<SourceRow>> = vec![Vec::new(); source_count];
        for _ in 0..mutations {
            let source_index = rng.range(source_count as u64) as usize;
            let live_rows = &mut live[source_index];
            let operation = rng.range(3);
            if operation == 0 || live_rows.is_empty() {
                // A new key.
                let key = next_key;
                next_key += 1;
                let group = format!("g{}", key % 3);
                let value = oracle_value(&mut rng, nullable_values);
                rows_per_source[source_index].push((key, group.clone(), value, "insert"));
                live_rows.push((key, group, value));
            } else {
                let index = rng.range(live_rows.len() as u64) as usize;
                let (key, group, _) = live_rows[index].clone();
                if operation == 1 {
                    // An update of the same key.
                    let value = oracle_value(&mut rng, nullable_values);
                    rows_per_source[source_index].push((
                        key,
                        group.clone(),
                        value,
                        "insert",
                    ));
                    live_rows[index] = (key, group, value);
                } else {
                    // A delete of the same key.
                    let value = live_rows[index].2;
                    rows_per_source[source_index].push((
                        key,
                        group.clone(),
                        value,
                        "delete",
                    ));
                    live_rows.swap_remove(index);
                }
            }
        }
        for (index, rows) in rows_per_source.iter().enumerate() {
            if rows.is_empty() {
                continue;
            }
            sources[index]
                .append_batch(executor.runtime().client(), batch(rows, &schema))
                .await
                .unwrap();
        }

        let execution = executor.execute(&insert_sql).await.unwrap();
        // Running the same statement again must be a no-op.
        executor.execute(&insert_sql).await.unwrap();

        let expected = collect_rows(&ctx, &reference_sql).await;
        let actual = collect_rows(&ctx, &mv_sql).await;
        mv_rows += actual.len();
        assert_eq!(
            actual, expected,
            "{tag}: round {round} after {} ({})",
            execution.action, execution.definition_hash
        );
    }
    assert!(
        mv_rows > 0 || custom_seed,
        "{tag}: the view stayed empty for every round"
    );
}

#[test_log::test(tokio::test)]
async fn oracle_multi_window_clauses_matches_full_recompute() {
    // Two window clauses with different partition/order in one statement.
    let groups = vec![
        WindowGroupSpec {
            partition_keys: vec!["v".to_string()],
            order_keys: Vec::new(),
            order_by: Vec::new(),
            columns: vec![WindowColumn {
                function: WindowFunction::Sum,
                value_column: Some("v".to_string()),
                window_args: None,
                window_filter: None,
                ignore_nulls: false,
                window_frame: None,
                column: "cnt".to_string(),
            }],
        },
        WindowGroupSpec {
            partition_keys: vec!["g".to_string()],
            order_keys: vec!["v".to_string()],
            order_by: Vec::new(),
            columns: vec![WindowColumn {
                function: WindowFunction::RowNumber,
                value_column: None,
                window_args: None,
                window_filter: None,
                ignore_nulls: false,
                window_frame: None,
                column: "rn".to_string(),
            }],
        },
    ];
    run_oracle(
        "multiwindowclauses",
        1,
        multi_window_mv_schema_for(&source_schema(), &["k".to_string()], &groups)
            .unwrap(),
        vec!["k".to_string()],
        "SELECT k, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn, \
                SUM(v) OVER (PARTITION BY v) AS cnt FROM __SRC__",
        "SELECT k, v, g, SUM(v) OVER (PARTITION BY v) AS cnt, \
                ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn \
         FROM __SRC__ WHERE op <> 'delete'",
        "SELECT k, v, g, cnt, rn FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_min_max_group_expr_matches_full_recompute() {
    // MIN over a computed group key.
    run_oracle(
        "minmaxgroupexpr",
        1,
        min_max_groups_mv_schema_for(
            &source_schema(),
            &["bucket".to_string()],
            &["v % 10".to_string()],
            Some("v"),
            None,
            MinMaxKind::Min,
        )
        .unwrap(),
        vec!["bucket".to_string()],
        "SELECT v % 10 AS bucket, MIN(v) FROM __SRC__ WHERE v > 30 GROUP BY bucket",
        "SELECT v % 10 AS bucket, MIN(v) AS value FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY bucket",
        "SELECT bucket, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_distinct_group_expr_matches_full_recompute() {
    // COUNT(DISTINCT) over a computed group key.
    run_oracle(
        "distinctgroupexpr",
        1,
        distinct_agg_groups_mv_schema_for(
            &source_schema(),
            &["bucket".to_string()],
            &["v % 10".to_string()],
            "g",
            DistinctAggKind::Count,
        )
        .unwrap(),
        vec!["bucket".to_string()],
        "SELECT v % 10 AS bucket, COUNT(DISTINCT g) FROM __SRC__ \
         WHERE v > 30 GROUP BY bucket",
        "SELECT v % 10 AS bucket, COUNT(DISTINCT g) AS value FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY bucket",
        "SELECT bucket, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_semi_join_matches_full_recompute() {
    // `WHERE EXISTS` decorrelates into a LeftSemi join.
    run_oracle(
        "semijoin",
        2,
        semi_anti_mv_schema_for(&source_schema(), &["k".to_string(), "g".to_string()])
            .unwrap(),
        vec!["k".to_string()],
        "SELECT k, g FROM __SRC__ f \
         WHERE EXISTS (SELECT 1 FROM __SRC1__ d WHERE d.g = f.g)",
        "SELECT k, g FROM __SRC__ f \
         WHERE EXISTS (SELECT 1 FROM __SRC1__ d WHERE d.g = f.g)",
        "SELECT k, g FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_anti_join_matches_full_recompute() {
    // `WHERE NOT EXISTS` decorrelates into a LeftAnti join.
    run_oracle(
        "antijoin",
        2,
        semi_anti_mv_schema_for(&source_schema(), &["k".to_string(), "g".to_string()])
            .unwrap(),
        vec!["k".to_string()],
        "SELECT k, g FROM __SRC__ f \
         WHERE NOT EXISTS (SELECT 1 FROM __SRC1__ d WHERE d.k = f.k)",
        "SELECT k, g FROM __SRC__ f \
         WHERE NOT EXISTS (SELECT 1 FROM __SRC1__ d WHERE d.k = f.k)",
        "SELECT k, g FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_cte_matches_full_recompute() {
    // A CTE is inlined by the planner; the view sees the filtered source.
    run_oracle(
        "cte",
        1,
        sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v")).unwrap(),
        vec!["g".to_string()],
        "WITH filtered AS (SELECT g, v FROM __SRC__ WHERE v > 30) \
         SELECT g, SUM(v) FROM filtered GROUP BY g",
        "SELECT g, SUM(v) AS sum_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, sum_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_group_expr_matches_full_recompute() {
    // The group key is a rendered expression instead of a plain column.
    run_oracle(
        "groupexpr",
        1,
        sum_count_groups_mv_schema_for(
            &source_schema(),
            &["bucket".to_string()],
            &["v % 10".to_string()],
            Some("v"),
            None,
            false,
        )
        .unwrap(),
        vec!["bucket".to_string()],
        "SELECT v % 10 AS bucket, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM __SRC__ WHERE v > 30 GROUP BY bucket",
        "SELECT v % 10 AS bucket, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30 GROUP BY bucket",
        "SELECT bucket, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_sum_expr_matches_full_recompute() {
    // The value is a rendered expression instead of a plain column.
    run_oracle(
        "sumexpr",
        1,
        sum_expr_mv_schema_for(&source_schema(), &["g".to_string()], "v * 2", false)
            .unwrap(),
        vec!["g".to_string()],
        "SELECT g, SUM(v * 2) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE v > 30 GROUP BY g",
        "SELECT g, SUM(v * 2) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_sum_count_matches_full_recompute() {
    run_oracle(
        "sum",
        1,
        sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v")).unwrap(),
        vec!["g".to_string()],
        "SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ GROUP BY g",
        "SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY g",
        "SELECT g, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_aggregate_filter_matches_full_recompute() {
    // The aggregate FILTER is materialized while the row count stays
    // unfiltered, so groups without a matching row keep their NULL sum.
    run_oracle(
        "aggfilter",
        1,
        sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v")).unwrap(),
        vec!["g".to_string()],
        "SELECT g, SUM(v) FILTER (WHERE v > 30) AS sum_v, COUNT(*) AS count_v \
         FROM __SRC__ GROUP BY g",
        "SELECT g, SUM(v) FILTER (WHERE v > 30) AS sum_v, COUNT(*) AS count_v \
         FROM __SRC__ WHERE op <> 'delete' GROUP BY g",
        "SELECT g, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_count_filter_matches_full_recompute() {
    // COUNT(column) FILTER counts the non-NULL values that match.
    let schema = nullable_source_schema();
    run_oracle_with(
        "countfilter",
        1,
        schema.clone(),
        true,
        sum_count_mv_schema_for(&schema, &["g".to_string()], None).unwrap(),
        vec!["g".to_string()],
        "SELECT g, COUNT(v) FILTER (WHERE v > 30) AS count_v FROM __SRC__ \
         GROUP BY g",
        "SELECT g, COUNT(v) FILTER (WHERE v > 30) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY g",
        "SELECT g, \"__ivm_nonnull_count\" FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_array_agg_matches_full_recompute() {
    run_oracle(
        "arrayagg",
        1,
        array_agg_mv_schema_for(&source_schema(), &["g".to_string()], "v").unwrap(),
        vec!["g".to_string()],
        "SELECT g, ARRAY_AGG(v ORDER BY k) FROM __SRC__ GROUP BY g",
        "SELECT g, ARRAY_AGG(v ORDER BY k) AS array_agg_v FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY g",
        "SELECT g, array_agg_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_array_agg_expr_matches_full_recompute() {
    run_oracle(
        "arrayaggexpr",
        1,
        array_agg_expr_mv_schema_for(&source_schema(), &["g".to_string()], "v * 2")
            .unwrap(),
        vec!["g".to_string()],
        "SELECT g, ARRAY_AGG(v * 2 ORDER BY k) FROM __SRC__ GROUP BY g",
        "SELECT g, ARRAY_AGG(v * 2 ORDER BY k) AS array_agg_value FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY g",
        "SELECT g, array_agg_value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_string_agg_expr_matches_full_recompute() {
    // The concatenated value is an expression (a cast), not a plain column.
    run_oracle(
        "stringaggexpr",
        1,
        string_agg_expr_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            "CAST(v AS VARCHAR)",
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, STRING_AGG(CAST(v AS VARCHAR), '|' ORDER BY k) FROM __SRC__ \
         GROUP BY g",
        "SELECT g, STRING_AGG(CAST(v AS VARCHAR), '|' ORDER BY k) AS string_agg_value \
         FROM __SRC__ WHERE op <> 'delete' GROUP BY g",
        "SELECT g, string_agg_value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_string_agg_matches_full_recompute() {
    // The aggregate ordering makes the concatenation deterministic, so the
    // incremental result must match the full recompute exactly.
    run_oracle(
        "stringagg",
        1,
        string_agg_mv_schema_for(&source_schema(), &["g".to_string()], "g").unwrap(),
        vec!["g".to_string()],
        "SELECT g, STRING_AGG(g, '|' ORDER BY k) FROM __SRC__ GROUP BY g",
        "SELECT g, STRING_AGG(g, '|' ORDER BY k) AS string_agg_g FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY g",
        "SELECT g, string_agg_g FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_count_column_matches_full_recompute() {
    // COUNT(column) counts non-NULL values through the shared non-NULL count.
    let schema = nullable_source_schema();
    run_oracle_with(
        "countcolumn",
        1,
        schema.clone(),
        true,
        sum_count_mv_schema_for(&schema, &["g".to_string()], None).unwrap(),
        vec!["g".to_string()],
        "SELECT g, COUNT(v) AS count_v FROM __SRC__ GROUP BY g",
        "SELECT g, COUNT(v) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY g",
        "SELECT g, \"__ivm_nonnull_count\" FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_min_expr_matches_full_recompute() {
    run_oracle(
        "minexpr",
        1,
        min_max_expr_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            "v * 2",
            MinMaxKind::Min,
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, MIN(v * 2) AS value FROM __SRC__ \
         WHERE v > 30 GROUP BY g",
        "SELECT g, MIN(v * 2) AS value FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_min_matches_full_recompute() {
    run_oracle(
        "min",
        1,
        min_max_mv_schema_for(&source_schema(), &["g".to_string()], "v", MinMaxKind::Min)
            .unwrap(),
        vec!["g".to_string()],
        "SELECT g, MIN(v) AS value FROM __SRC__ GROUP BY g",
        "SELECT g, MIN(v) AS value FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY g",
        "SELECT g, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_sum_count_where_matches_full_recompute() {
    // Updates crossing the threshold in both directions are the interesting
    // cases: the old value must be retracted only when it matched.
    run_oracle(
        "sumwhere",
        1,
        sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v")).unwrap(),
        vec!["g".to_string()],
        "SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE v > 30 GROUP BY g",
        "SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_min_where_matches_full_recompute() {
    run_oracle(
        "minwhere",
        1,
        min_max_mv_schema_for(&source_schema(), &["g".to_string()], "v", MinMaxKind::Min)
            .unwrap(),
        vec!["g".to_string()],
        "SELECT g, MIN(v) AS value FROM __SRC__ \
         WHERE v > 30 AND g <> 'g1' GROUP BY g",
        "SELECT g, MIN(v) AS value FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 AND g <> 'g1' GROUP BY g",
        "SELECT g, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}
#[test_log::test(tokio::test)]
async fn oracle_window_where_matches_full_recompute() {
    run_oracle(
        "windowwhere",
        1,
        window_ranking_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            WindowFunction::RowNumber,
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT k, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v, k) \
         FROM __SRC__ WHERE v > 30",
        "SELECT g, k, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v, k) AS \"row_number\" \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT g, k, \"row_number\" FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_window_order_expr_matches_full_recompute() {
    // The ordering key is a rendered expression; the runtime appends the
    // primary keys to the ordering to break ties.
    run_oracle(
        "windoworderexpr",
        1,
        window_ranking_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            WindowFunction::RowNumber,
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT k, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v % 10) \
         FROM __SRC__",
        "SELECT g, k, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v % 10, k) \
             AS \"row_number\" \
         FROM __SRC__ WHERE op <> 'delete'",
        "SELECT g, k, \"row_number\" FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_string_agg_order_expr_matches_full_recompute() {
    // The aggregate ordering mixes a rendered expression with a plain column,
    // which keeps the concatenation deterministic.
    run_oracle(
        "stringaggorderexpr",
        1,
        string_agg_expr_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            "CAST(v AS VARCHAR)",
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, STRING_AGG(CAST(v AS VARCHAR), '|' ORDER BY v % 10, k) \
         FROM __SRC__ WHERE v > 30 GROUP BY g",
        "SELECT g, STRING_AGG(CAST(v AS VARCHAR), '|' ORDER BY v % 10, k) \
             AS string_agg_value \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, string_agg_value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_desc_window_matches_full_recompute() {
    // The runtime appends the primary keys to the descending ordering, so the
    // reference uses `v DESC, k`.
    run_oracle(
        "descwindow",
        1,
        window_ranking_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            WindowFunction::RowNumber,
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT k, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) \
         FROM __SRC__",
        "SELECT g, k, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC, k) \
             AS \"row_number\" \
         FROM __SRC__ WHERE op <> 'delete'",
        "SELECT g, k, \"row_number\" FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_top_k_desc_matches_full_recompute() {
    run_oracle(
        "topkdesc",
        1,
        top_k_mv_schema_for(
            &source_schema(),
            &["k".to_string(), "g".to_string(), "v".to_string()],
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT k, g, v FROM (SELECT k, g, v, \
             ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) AS rn \
             FROM __SRC__) t WHERE rn <= 2",
        "SELECT k, g, v FROM (SELECT k, g, v, \
             ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC, k) AS rn \
             FROM __SRC__ WHERE op <> 'delete') t WHERE rn <= 2",
        "SELECT k, g, v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_top_k_where_matches_full_recompute() {
    run_oracle(
        "topkwhere",
        1,
        top_k_mv_schema_for(
            &source_schema(),
            &["k".to_string(), "g".to_string(), "v".to_string()],
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT k, g, v FROM (SELECT k, g, v, \
             ROW_NUMBER() OVER (PARTITION BY g ORDER BY v, k) AS rn \
             FROM __SRC__ WHERE v > 30) t WHERE rn <= 2",
        "SELECT k, g, v FROM (SELECT k, g, v, \
             ROW_NUMBER() OVER (PARTITION BY g ORDER BY v, k) AS rn \
             FROM __SRC__ WHERE op <> 'delete' AND v > 30) t WHERE rn <= 2",
        "SELECT k, g, v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_multi_window_matches_full_recompute() {
    // Several window functions sharing one window specification.
    let columns = vec![
        WindowColumn::new(WindowFunction::Sum)
            .with_value("v")
            .with_column("total"),
        WindowColumn::new(WindowFunction::Count).with_column("n"),
    ];
    run_oracle(
        "multiwindow",
        1,
        window_columns_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            &columns,
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT g, k, SUM(v) OVER w AS total, COUNT(*) OVER w AS n \
         FROM __SRC__ WHERE v > 30 WINDOW w AS (PARTITION BY g)",
        "SELECT g, k, SUM(v) OVER (PARTITION BY g) AS total, \
                COUNT(*) OVER (PARTITION BY g) AS n \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT g, k, total, n FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_row_number_matches_full_recompute() {
    // A global window without PARTITION BY; the runtime appends the primary
    // keys to the ordering.
    run_oracle(
        "globalrownumber",
        1,
        window_ranking_mv_schema_for(
            &source_schema(),
            &[],
            &["k".to_string()],
            WindowFunction::RowNumber,
        )
        .unwrap(),
        vec!["k".to_string()],
        "SELECT k, ROW_NUMBER() OVER (ORDER BY v, k) AS row_number \
         FROM __SRC__ WHERE v > 30",
        "SELECT k, ROW_NUMBER() OVER (ORDER BY v, k) AS row_number \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT k, row_number FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_window_filter_matches_full_recompute() {
    // FILTER membership changes as values move across the threshold.
    run_oracle(
        "windowfilter",
        1,
        window_aggregate_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            WindowFunction::Sum,
            Some("v"),
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT g, k, SUM(v) FILTER (WHERE v > 50) OVER (PARTITION BY g ORDER BY v, k \
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sum_v \
         FROM __SRC__ WHERE v > 30",
        "SELECT g, k, SUM(v) FILTER (WHERE v > 50) OVER (PARTITION BY g ORDER BY v, k \
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sum_v \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT g, k, sum_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_ntile_matches_full_recompute() {
    // The runtime appends the primary keys to the ordering, so the reference
    // uses the same refined ordering.
    run_oracle(
        "ntile",
        1,
        window_ranking_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            WindowFunction::Ntile,
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT g, k, NTILE(3) OVER (PARTITION BY g ORDER BY v, k) AS ntile \
         FROM __SRC__ WHERE v > 30",
        "SELECT g, k, NTILE(3) OVER (PARTITION BY g ORDER BY v, k) AS ntile \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT g, k, ntile FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_first_value_matches_full_recompute() {
    // A whole-partition frame; the ordering includes the source key, so the
    // first value is deterministic.
    run_oracle(
        "firstvalue",
        1,
        window_value_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            WindowFunction::FirstValue,
            "v",
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT g, k, FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v, k \
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_value_v \
         FROM __SRC__ WHERE v > 30",
        "SELECT g, k, FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v, k \
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_value_v \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT g, k, first_value_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_lag_matches_full_recompute() {
    // The runtime appends the primary keys to the ordering, so the reference
    // uses the same refined ordering.
    run_oracle(
        "lag",
        1,
        window_value_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &["k".to_string()],
            WindowFunction::Lag,
            "v",
        )
        .unwrap(),
        vec!["g".to_string(), "k".to_string()],
        "SELECT g, k, LAG(v, 1, 0) OVER (PARTITION BY g ORDER BY v, k) AS lag_v \
         FROM __SRC__ WHERE v > 30",
        "SELECT g, k, LAG(v, 1, 0) OVER (PARTITION BY g ORDER BY v, k) AS lag_v \
         FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT g, k, lag_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_select_distinct_matches_full_recompute() {
    // DISTINCT over the value column: values enter and leave the view as
    // updates and deletes change the multiset.
    run_oracle(
        "selectdistinct",
        1,
        sum_count_mv_schema_for(&source_schema(), &["v".to_string()], None).unwrap(),
        vec!["v".to_string()],
        "SELECT DISTINCT v FROM __SRC__ WHERE v > 30",
        "SELECT DISTINCT v FROM __SRC__ WHERE op <> 'delete' AND v > 30",
        "SELECT v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_count_distinct_matches_full_recompute() {
    // The optimizer splits COUNT(DISTINCT v); the SQL entry must reconstruct
    // the distinct view from the two-level aggregate plan.
    run_oracle(
        "countdistinct",
        1,
        distinct_agg_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            "v",
            DistinctAggKind::Count,
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, COUNT(DISTINCT v) AS value FROM __SRC__ \
         WHERE v > 30 GROUP BY g",
        "SELECT g, COUNT(DISTINCT v) AS value FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_sum_distinct_matches_full_recompute() {
    run_oracle(
        "sumdistinct",
        1,
        distinct_agg_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            "v",
            DistinctAggKind::Sum,
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, SUM(DISTINCT v) AS value FROM __SRC__ \
         WHERE v > 30 GROUP BY g",
        "SELECT g, SUM(DISTINCT v) AS value FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_array_agg_group_expr_matches_full_recompute() {
    // A computed group key together with a computed element.
    run_oracle(
        "arrayagg_groupexpr",
        1,
        array_agg_groups_mv_schema_for(
            &source_schema(),
            &["bucket".to_string()],
            &["v % 10".to_string()],
            None,
            Some("v * 2"),
        )
        .unwrap(),
        vec!["bucket".to_string()],
        "SELECT v % 10 AS bucket, ARRAY_AGG(v * 2 ORDER BY k) FROM __SRC__ \
         GROUP BY bucket",
        "SELECT v % 10 AS bucket, ARRAY_AGG(v * 2 ORDER BY k) AS array_agg_value \
         FROM __SRC__ WHERE op <> 'delete' GROUP BY bucket",
        "SELECT bucket, array_agg_value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_string_agg_group_expr_matches_full_recompute() {
    // A computed group key together with a computed value.
    run_oracle(
        "stringagg_groupexpr",
        1,
        string_agg_groups_mv_schema_for(
            &source_schema(),
            &["bucket".to_string()],
            &["v % 10".to_string()],
            None,
            Some("CAST(v AS VARCHAR)"),
        )
        .unwrap(),
        vec!["bucket".to_string()],
        "SELECT v % 10 AS bucket, STRING_AGG(CAST(v AS VARCHAR), '|' ORDER BY k) \
         FROM __SRC__ GROUP BY bucket",
        "SELECT v % 10 AS bucket, STRING_AGG(CAST(v AS VARCHAR), '|' ORDER BY k) \
         AS string_agg_value FROM __SRC__ WHERE op <> 'delete' GROUP BY bucket",
        "SELECT bucket, string_agg_value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_median_group_expr_matches_full_recompute() {
    // The median family recomputes the affected computed groups.
    run_oracle(
        "median_groupexpr",
        1,
        median_groups_mv_schema_for(
            &source_schema(),
            &["bucket".to_string()],
            &["v % 10".to_string()],
            Some("v"),
            None,
        )
        .unwrap(),
        vec!["bucket".to_string()],
        "SELECT v % 10 AS bucket, MEDIAN(v) AS median_v FROM __SRC__ \
         WHERE v > 30 GROUP BY bucket",
        "SELECT v % 10 AS bucket, MEDIAN(v) AS median_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY bucket",
        "SELECT bucket, median_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_variance_expr_matches_full_recompute() {
    // The variance argument may be a rendered expression.
    run_oracle(
        "varianceexpr",
        1,
        variance_groups_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &[],
            None,
            Some("v * 2"),
            VarianceKind::VarSamp,
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, VAR_SAMP(v * 2) AS variance_v FROM __SRC__ WHERE v > 30 \
         GROUP BY g",
        "SELECT g, VAR_SAMP(v * 2) AS variance_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, variance_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_median_expr_matches_full_recompute() {
    // The median argument may be a rendered expression.
    run_oracle(
        "medianexpr",
        1,
        median_groups_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &[],
            None,
            Some("v * 2"),
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, MEDIAN(v * 2) AS median_v FROM __SRC__ WHERE v > 30 GROUP BY g",
        "SELECT g, MEDIAN(v * 2) AS median_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, median_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_median_matches_full_recompute() {
    // The median is order-independent, so the pruned incremental recompute is
    // bit-identical to the full one.
    run_oracle(
        "median",
        1,
        median_mv_schema_for(&source_schema(), &["g".to_string()], "v").unwrap(),
        vec!["g".to_string()],
        "SELECT g, MEDIAN(v) AS median_v FROM __SRC__ WHERE v > 30 GROUP BY g",
        "SELECT g, MEDIAN(v) AS median_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, median_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_variance_matches_full_recompute() {
    run_oracle(
        "variance",
        1,
        variance_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            "v",
            VarianceKind::VarSamp,
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, VAR_SAMP(v) AS variance_v FROM __SRC__ WHERE v > 30 GROUP BY g",
        "SELECT g, VAR_SAMP(v) AS variance_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, variance_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_variance_group_expr_matches_full_recompute() {
    // The variance family recomputes the affected computed groups.
    run_oracle(
        "variance_groupexpr",
        1,
        variance_groups_mv_schema_for(
            &source_schema(),
            &["bucket".to_string()],
            &["v % 10".to_string()],
            Some("v"),
            None,
            VarianceKind::VarSamp,
        )
        .unwrap(),
        vec!["bucket".to_string()],
        "SELECT v % 10 AS bucket, VAR_SAMP(v) AS variance_v FROM __SRC__ \
         WHERE v > 30 GROUP BY bucket",
        "SELECT v % 10 AS bucket, VAR_SAMP(v) AS variance_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY bucket",
        "SELECT bucket, variance_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_union_distinct_projection_matches_full_recompute() {
    // The distinct key is a projected subset (renamed), so updates to the
    // other columns must not change the occurrence counts.
    run_oracle(
        "uniondistinctproj",
        2,
        union_distinct_mv_schema_for(
            &union_output_schema_for(
                &source_schema(),
                &["id".to_string(), "name".to_string()],
                &["k".to_string(), "g".to_string()],
            )
            .unwrap(),
        ),
        vec!["id".to_string(), "name".to_string()],
        "SELECT k AS id, g AS name FROM __SRC__ UNION SELECT k AS id, g AS name FROM __SRC1__",
        "SELECT k AS id, g AS name, count(*) AS count_v FROM (SELECT k, g FROM __SRC__ \
         WHERE op <> 'delete' UNION ALL SELECT k, g FROM __SRC1__ \
         WHERE op <> 'delete') t GROUP BY k, g",
        "SELECT id, name, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_union_distinct_matches_full_recompute() {
    // Two keyed sources unioned with deduplication: the MV keeps the
    // occurrence counts, so the reference counts the unioned logical rows.
    run_oracle(
        "uniondistinct",
        2,
        union_distinct_mv_schema_for(
            &union_output_schema_for(
                &source_schema(),
                &["k".to_string(), "g".to_string(), "v".to_string()],
                &[],
            )
            .unwrap(),
        ),
        vec!["k".to_string(), "g".to_string(), "v".to_string()],
        "SELECT k, g, v FROM __SRC__ UNION SELECT k, g, v FROM __SRC1__",
        "SELECT k, g, v, count(*) AS count_v FROM (SELECT k, g, v FROM __SRC__ \
         WHERE op <> 'delete' UNION ALL SELECT k, g, v FROM __SRC1__ \
         WHERE op <> 'delete') t GROUP BY k, g, v",
        "SELECT k, g, v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_avg_matches_full_recompute() {
    // AVG is derived from the maintained sum and non-NULL count; integer sums
    // keep the incremental division bit-identical to the full recompute.
    run_oracle(
        "avg",
        1,
        avg_mv_schema_for(&source_schema(), &["g".to_string()], "v").unwrap(),
        vec!["g".to_string()],
        "SELECT g, AVG(v) AS avg_v FROM __SRC__ WHERE v > 30 GROUP BY g",
        "SELECT g, AVG(v) AS avg_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g",
        "SELECT g, avg_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_sum_count_having_matches_full_recompute() {
    // HAVING keeps qualifying groups only; the incremental path recomputes
    // groups that were previously kept out of the MV.
    run_oracle(
        "having",
        1,
        sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v")).unwrap(),
        vec!["g".to_string()],
        "SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE v > 30 GROUP BY g HAVING COUNT(*) >= 2 OR g = 'g2'",
        "SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g \
         HAVING COUNT(*) >= 2 OR g = 'g2'",
        "SELECT g, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_min_having_matches_full_recompute() {
    run_oracle(
        "minhaving",
        1,
        min_max_mv_schema_for(&source_schema(), &["g".to_string()], "v", MinMaxKind::Min)
            .unwrap(),
        vec!["g".to_string()],
        "SELECT g, MIN(v) AS value FROM __SRC__ WHERE v > 30 GROUP BY g \
         HAVING MIN(v) <= 40 OR g = 'g2'",
        "SELECT g, MIN(v) AS value FROM __SRC__ \
         WHERE op <> 'delete' AND v > 30 GROUP BY g \
         HAVING MIN(v) <= 40 OR g = 'g2'",
        "SELECT g, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_union_all_where_matches_full_recompute() {
    run_oracle(
        "unionwhere",
        2,
        union_all_mv_schema_for(&source_schema()).unwrap(),
        vec![IVM_SOURCE_COLUMN.to_string(), "k".to_string()],
        "SELECT * FROM __SRC0__ WHERE v > 30 \
         UNION ALL SELECT * FROM __SRC1__ WHERE g <> 'g2'",
        "SELECT k, g, v, 0 AS __ivm_source FROM __SRC0__ \
         WHERE op <> 'delete' AND v > 30 \
         UNION ALL SELECT k, g, v, 1 AS __ivm_source FROM __SRC1__ \
         WHERE op <> 'delete' AND g <> 'g2'",
        "SELECT k, g, v, __ivm_source FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_inner_join_names_matches_full_recompute() {
    // A differently named inner join key (`a.k = b.v`); the MV keeps the left
    // key name and both key columns stay out of the payloads.
    let schema = source_schema();
    let keys = vec!["k".to_string()];
    let seed = vec![
        (0usize, 0i64, "g0", Some(0i64)),
        (0, 1, "g1", Some(1)),
        (1, 2, "g0", Some(0)),
        (1, 3, "g1", Some(1)),
    ];
    run_oracle_seeded(
        "joininnames",
        2,
        schema.clone(),
        false,
        &seed,
        keyed_join_view_schema_for(&schema, &schema, &keys, &keys, &keys, "g", "g")
            .unwrap(),
        keyed_join_output_primary_keys(&keys, &keys),
        "SELECT a.k, a.g, b.g FROM __SRC__ a JOIN __SRC1__ b ON a.k = b.v",
        "SELECT a.k, a.g, b.g, a.k AS lpk, b.k AS rpk FROM __SRC__ a \
         JOIN __SRC1__ b ON a.k = b.v \
         WHERE a.op <> 'delete' AND b.op <> 'delete'",
        "SELECT k, left_value, right_value, \"__left_pk_k\", \"__right_pk_k\" \
         FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_theta_join_matches_full_recompute() {
    // An inner join with a non-equality pair condition and a side filter.
    let schema = source_schema();
    let keys = vec!["k".to_string()];
    let seed = vec![
        (0usize, 0i64, "g0", Some(50i64)),
        (0, 1, "g1", Some(10)),
        (1, 0, "g0", Some(20)),
        (1, 1, "g1", Some(60)),
    ];
    run_oracle_seeded(
        "thetajoin",
        2,
        schema.clone(),
        false,
        &seed,
        keyed_join_view_schema_for(&schema, &schema, &keys, &keys, &keys, "v", "v")
            .unwrap(),
        keyed_join_output_primary_keys(&keys, &keys),
        "SELECT a.k, a.v, b.v FROM __SRC__ a JOIN __SRC1__ b ON a.k = b.k \
         WHERE a.v > 20 AND a.v > b.v",
        "SELECT a.k, a.v, b.v, a.k AS lpk, b.k AS rpk FROM __SRC__ a \
         JOIN __SRC1__ b ON a.k = b.k WHERE a.op <> 'delete' \
         AND b.op <> 'delete' AND a.v > 20 AND a.v > b.v",
        "SELECT k, left_value, right_value, \"__left_pk_k\", \"__right_pk_k\" \
         FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_multi_distinct_matches_full_recompute() {
    // The multi-column distinct count recomputes its affected groups. The
    // reference cannot run the multi-argument aggregate (DataFusion does not
    // execute it), so it counts the equivalent concatenated tuples.
    let schema = source_schema();
    run_oracle(
        "multidistinct",
        1,
        distinct_agg_groups_mv_schema_for(
            &schema,
            &["g".to_string()],
            &[],
            "v",
            DistinctAggKind::Count,
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, COUNT(DISTINCT v, k) FROM __SRC__ GROUP BY g",
        "SELECT g, COUNT(DISTINCT CAST(v AS VARCHAR) || '/' || CAST(k AS VARCHAR)) AS value \
         FROM __SRC__ WHERE op <> 'delete' GROUP BY g",
        "SELECT g, value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_semi_anti_filters_matches_full_recompute() {
    // Filters on both sides of an EXISTS subquery.
    run_oracle(
        "semifilters",
        2,
        semi_anti_mv_schema_for(&source_schema(), &["k".to_string(), "g".to_string()])
            .unwrap(),
        vec!["k".to_string()],
        "SELECT k, g FROM __SRC__ f WHERE f.v > 10 \
         AND EXISTS (SELECT 1 FROM __SRC1__ d WHERE d.g = f.g AND d.v > 10)",
        "SELECT k, g FROM __SRC__ f WHERE f.op <> 'delete' AND f.v > 10 \
         AND EXISTS (SELECT 1 FROM __SRC1__ d \
                     WHERE d.op <> 'delete' AND d.g = f.g AND d.v > 10)",
        "SELECT k, g FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_intersect_matches_full_recompute() {
    // INTERSECT over unique, non-nullable tuples is maintained by the semi
    // join; the seeds make both sides share a tuple.
    let schema = source_schema();
    let seed = vec![
        (0usize, 0i64, "g0", Some(5i64)),
        (0, 1, "g1", Some(7)),
        (0, 2, "g2", Some(9)),
        (1, 0, "g0", Some(5)),
        (1, 1, "g1", Some(7)),
        (1, 2, "g2", Some(9)),
    ];
    let keys = vec!["k".to_string(), "v".to_string()];
    run_oracle_seeded(
        "intersect",
        2,
        schema.clone(),
        false,
        &seed,
        semi_anti_mv_schema_for(&schema, &keys).unwrap(),
        vec!["k".to_string()],
        "SELECT k, v FROM __SRC__ INTERSECT SELECT k, v FROM __SRC1__",
        "(SELECT k, v FROM __SRC__ WHERE op <> 'delete') \
         INTERSECT (SELECT k, v FROM __SRC1__ WHERE op <> 'delete')",
        "SELECT k, v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_except_matches_full_recompute() {
    // EXCEPT over unique, non-nullable tuples is maintained by the anti join.
    let schema = source_schema();
    let seed = vec![
        (0usize, 0i64, "g0", Some(5i64)),
        (0, 1, "g1", Some(7)),
        (1, 0, "g0", Some(5)),
        (1, 1, "g2", Some(8)),
    ];
    let keys = vec!["k".to_string(), "v".to_string()];
    run_oracle_seeded(
        "except",
        2,
        schema.clone(),
        false,
        &seed,
        semi_anti_mv_schema_for(&schema, &keys).unwrap(),
        vec!["k".to_string()],
        "SELECT k, v FROM __SRC__ EXCEPT SELECT k, v FROM __SRC1__",
        "(SELECT k, v FROM __SRC__ WHERE op <> 'delete') \
         EXCEPT (SELECT k, v FROM __SRC1__ WHERE op <> 'delete')",
        "SELECT k, v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_sum_matches_full_recompute() {
    // A global SUM/COUNT/AVG over the whole source: the single MV row is
    // recomputed on every refresh.
    run_oracle(
        "globalsum",
        1,
        avg_mv_schema_for(&source_schema(), &[], "v").unwrap(),
        Vec::new(),
        "SELECT SUM(v), COUNT(*), AVG(v) FROM __SRC__",
        "SELECT SUM(v) AS sum_v, COUNT(*) AS count_v, COUNT(v) AS nonnull, \
         AVG(v) AS avg_v FROM __SRC__ WHERE op <> 'delete'",
        "SELECT sum_v, count_v, \"__ivm_nonnull_count\", avg_v FROM __MV__ \
         WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_min_matches_full_recompute() {
    // A global MIN over the whole source.
    run_oracle(
        "globalmin",
        1,
        min_max_mv_schema_for(&source_schema(), &[], "v", MinMaxKind::Min).unwrap(),
        Vec::new(),
        "SELECT MIN(v) FROM __SRC__",
        "SELECT MIN(v) AS value FROM __SRC__ WHERE op <> 'delete'",
        "SELECT \"value\" FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_distinct_matches_full_recompute() {
    // A global COUNT(DISTINCT v) over the whole source.
    run_oracle(
        "globaldistinct",
        1,
        distinct_agg_mv_schema_for(&source_schema(), &[], "v", DistinctAggKind::Count)
            .unwrap(),
        Vec::new(),
        "SELECT COUNT(DISTINCT v) FROM __SRC__",
        "SELECT COUNT(DISTINCT v) AS value FROM __SRC__ WHERE op <> 'delete'",
        "SELECT \"value\" FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_variance_matches_full_recompute() {
    // A global VAR_SAMP over the whole source.
    run_oracle(
        "globalvariance",
        1,
        variance_mv_schema_for(&source_schema(), &[], "v", VarianceKind::VarSamp)
            .unwrap(),
        Vec::new(),
        "SELECT VAR_SAMP(v) FROM __SRC__",
        "SELECT VAR_SAMP(v) AS variance_v FROM __SRC__ WHERE op <> 'delete'",
        "SELECT variance_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_median_matches_full_recompute() {
    // A global MEDIAN over the whole source.
    run_oracle(
        "globalmedian",
        1,
        median_mv_schema_for(&source_schema(), &[], "v").unwrap(),
        Vec::new(),
        "SELECT MEDIAN(v) FROM __SRC__",
        "SELECT MEDIAN(v) AS median_v FROM __SRC__ WHERE op <> 'delete'",
        "SELECT median_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_union_all_branches_matches_full_recompute() {
    // Three keyed sources unioned without deduplication.
    run_oracle(
        "unionallbranches",
        3,
        union_all_mv_schema_for(
            &union_output_schema_for(
                &source_schema(),
                &["k".to_string(), "g".to_string(), "v".to_string()],
                &[],
            )
            .unwrap(),
        )
        .unwrap(),
        vec![IVM_SOURCE_COLUMN.to_string(), "k".to_string()],
        "SELECT k, g, v FROM __SRC__ UNION ALL SELECT k, g, v FROM __SRC1__ \
         UNION ALL SELECT k, g, v FROM __SRC2__",
        "SELECT k, g, v, 0 AS __ivm_source FROM __SRC__ WHERE op <> 'delete' \
         UNION ALL SELECT k, g, v, 1 AS __ivm_source FROM __SRC1__ WHERE op <> 'delete' \
         UNION ALL SELECT k, g, v, 2 AS __ivm_source FROM __SRC2__ WHERE op <> 'delete'",
        "SELECT k, g, v, __ivm_source FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_union_distinct_branches_matches_full_recompute() {
    // Three keyed sources unioned with deduplication.
    run_oracle(
        "uniondistinctbranches",
        3,
        union_distinct_mv_schema_for(
            &union_output_schema_for(
                &source_schema(),
                &["k".to_string(), "g".to_string(), "v".to_string()],
                &[],
            )
            .unwrap(),
        ),
        vec!["k".to_string(), "g".to_string(), "v".to_string()],
        "SELECT k, g, v FROM __SRC__ UNION SELECT k, g, v FROM __SRC1__ \
         UNION SELECT k, g, v FROM __SRC2__",
        "SELECT k, g, v, count(*) AS count_v FROM (SELECT k, g, v FROM __SRC__ \
         WHERE op <> 'delete' UNION ALL SELECT k, g, v FROM __SRC1__ WHERE op <> 'delete' \
         UNION ALL SELECT k, g, v FROM __SRC2__ WHERE op <> 'delete') t GROUP BY k, g, v",
        "SELECT k, g, v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_string_agg_matches_full_recompute() {
    // A global STRING_AGG over the whole source.
    run_oracle(
        "globalstringagg",
        1,
        string_agg_mv_schema_for(&source_schema(), &[], "g").unwrap(),
        Vec::new(),
        "SELECT STRING_AGG(g, ',' ORDER BY k) FROM __SRC__",
        "SELECT STRING_AGG(g, ',' ORDER BY k) AS string_agg_g FROM __SRC__ \
         WHERE op <> 'delete'",
        "SELECT string_agg_g FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_global_array_agg_matches_full_recompute() {
    // A global ARRAY_AGG over the whole source.
    run_oracle(
        "globalarrayagg",
        1,
        array_agg_mv_schema_for(&source_schema(), &[], "v").unwrap(),
        Vec::new(),
        "SELECT ARRAY_AGG(v ORDER BY k) FROM __SRC__",
        "SELECT ARRAY_AGG(v ORDER BY k) AS array_agg_v FROM __SRC__ \
         WHERE op <> 'delete'",
        "SELECT array_agg_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_grouping_sets_matches_full_recompute() {
    // A ROLLUP over one key: the per-group rows and the grand total share the
    // MV, distinguished by the grouping index.
    run_oracle(
        "groupingsets",
        1,
        grouping_sets_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &[],
            Some("v"),
            None,
            false,
        )
        .unwrap(),
        vec!["__ivm_grouping".to_string(), "g".to_string()],
        "SELECT g, SUM(v), COUNT(*) FROM __SRC__ GROUP BY ROLLUP(g)",
        "SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY ROLLUP(g)",
        "SELECT g, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_bool_agg_matches_full_recompute() {
    // A grouped BOOL_AND over a derived boolean expression, recomputed from
    // the affected groups.
    run_oracle(
        "boolagg",
        1,
        bool_agg_groups_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &[],
            None,
            Some("(v > 1)"),
            BoolAggKind::BoolAnd,
        )
        .unwrap(),
        vec!["g".to_string()],
        "SELECT g, BOOL_AND(v > 1) FROM __SRC__ WHERE v > 10 GROUP BY g",
        "SELECT g, BOOL_AND(v > 1) AS flag FROM __SRC__ \
         WHERE op <> 'delete' AND v > 10 GROUP BY g",
        "SELECT g, bool_and_value FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_grouping_sets_cube_matches_full_recompute() {
    // A CUBE over two keys: detail rows, both per-key subtotals and the grand
    // total share the MV.
    run_oracle(
        "groupingsetscube",
        1,
        grouping_sets_mv_schema_for(
            &source_schema(),
            &["g".to_string(), "v".to_string()],
            &[],
            Some("v"),
            None,
            false,
        )
        .unwrap(),
        vec![
            "__ivm_grouping".to_string(),
            "g".to_string(),
            "v".to_string(),
        ],
        "SELECT g, v, SUM(v), COUNT(*) FROM __SRC__ GROUP BY CUBE(g, v)",
        "SELECT g, v, SUM(v) AS sum_v, COUNT(*) AS count_v FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY CUBE(g, v)",
        "SELECT g, v, sum_v, count_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_grouping_sets_having_matches_full_recompute() {
    // A ROLLUP with AVG and a HAVING over the SUM: groups crossing the
    // threshold appear and disappear.
    run_oracle(
        "groupingsetshaving",
        1,
        grouping_sets_mv_schema_for(
            &source_schema(),
            &["g".to_string()],
            &[],
            Some("v"),
            None,
            true,
        )
        .unwrap(),
        vec!["__ivm_grouping".to_string(), "g".to_string()],
        "SELECT g, SUM(v), AVG(v) FROM __SRC__ GROUP BY ROLLUP(g) \
         HAVING SUM(v) > 50",
        "SELECT g, SUM(v) AS sum_v, AVG(v) AS avg_v FROM __SRC__ \
         WHERE op <> 'delete' GROUP BY ROLLUP(g) HAVING SUM(v) > 50",
        "SELECT g, sum_v, avg_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn oracle_approx_distinct_matches_full_recompute() {
    // The sketch update is order independent, so every recomputed group
    // matches DataFusion's full recompute exactly.
    run_oracle(
        "approxdistinct",
        1,
        approx_distinct_mv_schema_for(&source_schema(), &["g".to_string()], "v").unwrap(),
        vec!["g".to_string()],
        "SELECT g, APPROX_DISTINCT(v) FROM __SRC__ WHERE v > 10 GROUP BY g",
        "SELECT g, APPROX_DISTINCT(v) AS approx_distinct_v FROM __SRC__ \
         WHERE op <> 'delete' AND v > 10 GROUP BY g",
        "SELECT g, approx_distinct_v FROM __MV__ WHERE \"rowKinds\" = 'insert'",
    )
    .await;
}
