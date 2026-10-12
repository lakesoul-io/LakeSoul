// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `INTERSECT ALL` / `EXCEPT ALL` over a repeated left: the maintained view
//! keeps `min(count_l, count_r)` (INTERSECT) or `max(count_l - count_r, 0)`
//! (EXCEPT) copies per tuple.
//!
//! DataFusion evaluates a raw `INTERSECT ALL` / `EXCEPT ALL` as a membership
//! join (no per-tuple counts), so the oracle recomputes the standard counts in
//! Rust from the current source state and compares them with the view.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, semi_anti_mv_schema_for,
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

/// The tuples of one source: group -> the row ids, ordered by id.
async fn read_tuples(context: &SessionContext, table: &str) -> HashMap<String, Vec<i64>> {
    let batches = context
        .sql(&format!("SELECT g, k FROM {table} ORDER BY g, k"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut tuples: HashMap<String, Vec<i64>> = HashMap::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            tuples
                .entry(groups.value(row).to_string())
                .or_default()
                .push(ids.value(row));
        }
    }
    tuples
}

/// The groups the view should hold: each left row of a tuple competes with the
/// right tuple's multiplicity.
fn expected_groups(
    left: &HashMap<String, Vec<i64>>,
    right: &HashMap<String, Vec<i64>>,
    anti: bool,
) -> Vec<String> {
    let mut groups = Vec::new();
    for (group, rows) in left {
        let count = right.get(group).map(Vec::len).unwrap_or(0);
        for (index, _) in rows.iter().enumerate() {
            let rank = index + 1;
            if (anti && rank > count) || (!anti && rank <= count) {
                groups.push(group.clone());
            }
        }
    }
    groups.sort();
    groups
}

async fn read_mv_groups(context: &SessionContext, table: &str) -> Vec<String> {
    let batches = context
        .sql(&format!("SELECT g FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut groups = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            groups.push(values.value(row).to_string());
        }
    }
    groups.sort();
    groups
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

#[test_log::test(tokio::test)]
async fn count_set_operations_match_the_standard() {
    for (tag, operator, anti) in [
        ("inter", "INTERSECT ALL", false),
        ("except", "EXCEPT ALL", true),
    ] {
        let runtime = IvmRuntime::from_env().await.unwrap();
        runtime.init_schema().await.unwrap();
        let dir = tempdir().unwrap();
        let suffix = uuid::Uuid::new_v4().simple();
        let left =
            create_source(&runtime, &dir, &format!("ivm_count_left_{tag}_{suffix}"))
                .await;
        let right =
            create_source(&runtime, &dir, &format!("ivm_count_right_{tag}_{suffix}"))
                .await;
        let mv = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_count_mv_{tag}_{suffix}"),
                    table_path(&dir, "mv"),
                    semi_anti_mv_schema_for(
                        &source_schema(),
                        &["g".to_string(), "k".to_string()],
                    )
                    .unwrap(),
                )
                .with_primary_keys(vec!["k".to_string()]),
            )
            .await
            .unwrap();

        left.append_batch(
            runtime.client(),
            source_batch(&[
                (1, "g0", 10, "insert"),
                (2, "g1", 20, "insert"),
                (3, "g0", 30, "insert"),
            ]),
        )
        .await
        .unwrap();
        right
            .append_batch(
                runtime.client(),
                source_batch(&[(1, "g0", 5, "insert"), (2, "g1", 6, "insert")]),
            )
            .await
            .unwrap();

        let executor = IvmSqlExecutor::new(runtime);
        let statement = format!(
            "INSERT INTO ivm_count_mv_{tag}_{suffix} \
             SELECT g FROM ivm_count_left_{tag}_{suffix} {operator} \
             SELECT g FROM ivm_count_right_{tag}_{suffix}"
        );
        executor.execute(&statement).await.unwrap();

        let context = SessionContext::new();
        context
            .register_table("left", Arc::new(executor.runtime().table_provider(&left)))
            .unwrap();
        context
            .register_table("right", Arc::new(executor.runtime().table_provider(&right)))
            .unwrap();
        context
            .register_table("mv", Arc::new(executor.runtime().table_provider(&mv)))
            .unwrap();
        let context = &context;

        // A clock-free mirror of the live keys, for the mutations.
        let mut live_left: Vec<i64> = vec![1, 2, 3];
        let mut live_right: Vec<i64> = vec![1, 2];
        let mut next_key = 4i64;
        let mut rng = Lcg(0x5eed ^ tag.len() as u64);
        for round in 0..12 {
            let mut rows_left = Vec::new();
            let mut rows_right = Vec::new();
            for _ in 0..2 {
                let (live, rows) = if rng.range(2) == 0 {
                    (&mut live_left, &mut rows_left)
                } else {
                    (&mut live_right, &mut rows_right)
                };
                if live.is_empty() || rng.range(3) == 0 {
                    // A new key; the key modulo three spreads the groups.
                    let key = next_key;
                    next_key += 1;
                    let group = format!("g{}", key % 3);
                    rows.push((key, group, key * 10, "insert".to_string()));
                    live.push(key);
                } else {
                    let index = rng.range(live.len() as u64) as usize;
                    let key = live[index];
                    let group = format!("g{}", key % 3);
                    if rng.range(2) == 0 {
                        // An update: a newer version of the same key.
                        rows.push((key, group, key * 100, "insert".to_string()));
                    } else {
                        rows.push((key, group, key * 10, "delete".to_string()));
                        live.swap_remove(index);
                    }
                }
            }
            for (table, rows) in [(&left, &rows_left), (&right, &rows_right)] {
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

            executor.execute(&statement).await.unwrap();
            let left_tuples = read_tuples(context, "left").await;
            let right_tuples = read_tuples(context, "right").await;
            let expected = expected_groups(&left_tuples, &right_tuples, anti);
            let actual = read_mv_groups(context, "mv").await;
            assert_eq!(
                actual, expected,
                "{tag}: round {round} left={left_tuples:?} right={right_tuples:?}"
            );
        }
    }
}
