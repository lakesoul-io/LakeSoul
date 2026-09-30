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
    IVM_SOURCE_COLUMN, IvmRuntime, IvmSqlExecutor, IvmTableOptions, MinMaxKind,
    PhysicalFormat, WindowFunction, min_max_mv_schema_for, sum_count_mv_schema_for,
    top_k_mv_schema_for, union_all_mv_schema_for, window_ranking_mv_schema_for,
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

fn batch(rows: &[(i64, String, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.1.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
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
                    source_schema(),
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
    let mut live: Vec<Vec<(i64, String, i64)>> = vec![Vec::new(); source_count];
    let mut next_key = 0i64;
    let mut mv_rows = 0usize;

    for round in 0..10 {
        let mutations = 1 + rng.range(3) as usize;
        let mut rows_per_source: Vec<Vec<(i64, String, i64, &'static str)>> =
            vec![Vec::new(); source_count];
        for _ in 0..mutations {
            let source_index = rng.range(source_count as u64) as usize;
            let live_rows = &mut live[source_index];
            let operation = rng.range(3);
            if operation == 0 || live_rows.is_empty() {
                // A new key.
                let key = next_key;
                next_key += 1;
                let group = format!("g{}", key % 3);
                let value = rng.range(100) as i64;
                rows_per_source[source_index].push((key, group.clone(), value, "insert"));
                live_rows.push((key, group, value));
            } else {
                let index = rng.range(live_rows.len() as u64) as usize;
                let (key, group, _) = live_rows[index].clone();
                if operation == 1 {
                    // An update of the same key.
                    let value = rng.range(100) as i64;
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
                .append_batch(executor.runtime().client(), batch(rows))
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
