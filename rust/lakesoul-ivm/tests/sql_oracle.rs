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
    DistinctAggKind, IVM_SOURCE_COLUMN, IvmRuntime, IvmSqlExecutor, IvmTableOptions,
    MinMaxKind, PhysicalFormat, VarianceKind, WindowColumn, WindowFunction,
    avg_mv_schema_for, distinct_agg_mv_schema_for, median_mv_schema_for,
    min_max_mv_schema_for, string_agg_mv_schema_for, sum_count_mv_schema_for,
    top_k_mv_schema_for, union_all_mv_schema_for, variance_mv_schema_for,
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
    let mut rng = Lcg(42);
    let mut live: Vec<Vec<(i64, String, Option<i64>)>> = vec![Vec::new(); source_count];
    let mut next_key = 0i64;
    let mut mv_rows = 0usize;

    for round in 0..10 {
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
    assert!(mv_rows > 0, "{tag}: the view stayed empty for every round");
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
