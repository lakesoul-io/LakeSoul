// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! A `HAVING` subquery needs a second source, which a single-statement view
//! does not model.  The documented two-view pattern covers it: group the
//! source into one view, then filter that view against the subquery with the
//! ordinary correlated / uncorrelated scalar-subquery shapes.  Both forms are
//! exercised here against a full recompute over random mutations.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, row_mv_schema_for,
    semi_anti_mv_schema_for, sum_count_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// A tiny deterministic generator, so a failure is reproducible.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn range(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

async fn read_groups(context: &SessionContext, table: &str) -> Vec<(String, i64)> {
    let batches = context
        .sql(&format!("select g, sum_v from {table} order by g"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), sums.value(row)));
        }
    }
    rows
}

async fn create_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                name.to_string(),
                table_path(dir, name),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

/// The two-view pattern: `mv1` groups the source, `mv2` filters it against a
/// scalar subquery over `dim`.
async fn run_pattern(correlated: bool) {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let tag = if correlated { "corr" } else { "glob" };
    let source =
        create_source(&runtime, &dir, &format!("ivm_hv_src_{tag}_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_hv_dim_{tag}_{suffix}")).await;
    let mv1 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_hv_group_{tag}_{suffix}"),
                table_path(&dir, "group"),
                sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let mv1_columns = vec!["g".to_string(), "sum_v".to_string()];
    let (mv2_schema, predicate, reference_predicate) = if correlated {
        (
            semi_anti_mv_schema_for(&mv1.schema, &mv1_columns).unwrap(),
            format!(
                "sum_v > (select max(d.v) from {} d where d.g = {}.g)",
                dim.table_name, mv1.table_name
            ),
            "sum_v > (select max(d.v) from dim d \
             where d.op <> 'delete' and d.g = a.g)"
                .to_string(),
        )
    } else {
        (
            row_mv_schema_for(&mv1.schema, &mv1_columns).unwrap(),
            format!("sum_v > (select max(v) from {})", dim.table_name),
            "sum_v > (select max(v) from dim where op <> 'delete')".to_string(),
        )
    };
    let mv2 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_hv_filtered_{tag}_{suffix}"),
                table_path(&dir, "filtered"),
                mv2_schema,
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "g0", 10, "insert"),
                (2, "g1", 30, "insert"),
                (3, "g0", 20, "insert"),
            ]),
        )
        .await
        .unwrap();
    dim.append_batch(
        runtime.client(),
        source_batch(&[(1, "g0", 15, "insert"), (2, "g1", 25, "insert")]),
    )
    .await
    .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let group_statement = format!(
        "insert into {} select g, sum(v) as sum_v from {} group by g",
        mv1.table_name, source.table_name
    );
    let filter_statement = format!(
        "insert into {} select g, sum_v from {} where {}",
        mv2.table_name, mv1.table_name, predicate
    );

    let context = SessionContext::new();
    context
        .register_table("src", Arc::new(executor.runtime().table_provider(&source)))
        .unwrap();
    context
        .register_table("dim", Arc::new(executor.runtime().table_provider(&dim)))
        .unwrap();
    context
        .register_table(
            "filtered",
            Arc::new(executor.runtime().table_provider(&mv2)),
        )
        .unwrap();
    let context = &context;

    let mut live_src: Vec<i64> = vec![1, 2, 3];
    let mut live_dim: Vec<i64> = vec![1, 2];
    let mut next_key = 4i64;
    let mut rng = Lcg(0x5eed ^ u64::from(correlated));
    for round in 0..10 {
        let mut src_rows = Vec::new();
        let mut dim_rows = Vec::new();
        for _ in 0..3 {
            let (live, rows) = if rng.range(2) == 0 {
                (&mut live_src, &mut src_rows)
            } else {
                (&mut live_dim, &mut dim_rows)
            };
            if live.is_empty() || rng.range(3) == 0 {
                let key = next_key;
                next_key += 1;
                let group = format!("g{}", key % 2);
                rows.push((key, group, key * 10, "insert".to_string()));
                live.push(key);
            } else {
                let index = rng.range(live.len() as u64) as usize;
                let key = live[index];
                let group = format!("g{}", key % 2);
                if rng.range(2) == 0 {
                    rows.push((key, group, key * 100, "insert".to_string()));
                } else {
                    rows.push((key, group, key * 10, "delete".to_string()));
                    live.swap_remove(index);
                }
            }
        }
        for (table, rows) in [(&source, &src_rows), (&dim, &dim_rows)] {
            if rows.is_empty() {
                continue;
            }
            let borrow = rows
                .iter()
                .map(|(key, group, value, kind)| {
                    (*key, group.as_str(), *value, kind.as_str())
                })
                .collect::<Vec<_>>();
            table
                .append_batch(executor.runtime().client(), source_batch(&borrow))
                .await
                .unwrap();
        }

        executor.execute(&group_statement).await.unwrap();
        executor.execute(&filter_statement).await.unwrap();

        let expected = read_groups(
            context,
            &format!(
                "(select g, sum(v) as sum_v from src where op <> 'delete' \
                 group by g) a \
                 where {}",
                reference_predicate
            ),
        )
        .await;
        // The reference reads the derived table through the providers; the
        // subquery inside it re-derives the grouped source.
        let actual = read_groups(context, "filtered").await;
        assert_eq!(actual, expected, "{tag}: round {round}");
    }
}

#[test_log::test(tokio::test)]
async fn having_subquery_pattern_matches_full_recompute() {
    for correlated in [true, false] {
        run_pattern(correlated).await;
    }
}
