// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tier-2 fallback views over range-partitioned sources: a refresh whose
//! statement is partition local recomputes only the partitions that changed,
//! and a change in an unpartitioned source recomputes the whole view.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmSqlExecutor, IvmTable,
    IvmTableOptions,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

/// `fact` is range partitioned by `day`; `price` is a `DECIMAL(38, 2)`, so the
/// statement's `SUM` has no accumulator headroom and stays Tier-2.
fn fact_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("price", DataType::Decimal128(38, 2), false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn fact_batch(rows: &[(i64, &str, i128, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        fact_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(
                Decimal128Array::from_iter_values(rows.iter().map(|row| row.2))
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// `dim` is unpartitioned and its primary key is not the join key, so the
/// statement's inner join stays Tier-2.
fn dim_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn dim_batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        dim_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

/// The MV schema a fallback statement must produce: the planned statement's
/// output columns plus the row kind and epoch columns.
async fn fallback_mv_schema(
    runtime: &IvmRuntime,
    select: &str,
    tables: &[&IvmTable],
) -> SchemaRef {
    let context = SessionContext::new();
    for table in tables {
        context
            .register_table(
                table.table_name.as_str(),
                Arc::new(runtime.table_provider(table)),
            )
            .unwrap();
    }
    let plan = context
        .sql(select)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap();
    let mut fields = plan
        .schema()
        .fields()
        .iter()
        .map(|field| {
            Arc::new(Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable(),
            )) as arrow_schema::FieldRef
        })
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        IVM_ROW_KINDS_COLUMN,
        DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Arc::new(Schema::new(fields))
}

/// The version of every range partition of `table`.
async fn read_versions(runtime: &IvmRuntime, table: &IvmTable) -> HashMap<String, i32> {
    runtime
        .client()
        .get_all_partition_info(&table.table_id)
        .await
        .unwrap()
        .into_iter()
        .map(|partition| (partition.partition_desc.clone(), partition.version))
        .collect()
}

/// The `(day, sum)` rows of the MV, ordered by `day`.
async fn read_day_sums(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, String)> {
    let context = SessionContext::new();
    let batches = mv.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        datafusion::datasource::memory::MemTable::try_new(
            mv.schema.clone(),
            vec![batches],
        )
        .unwrap(),
    );
    context.register_table("mv", table).unwrap();
    let batches = context
        .sql(&format!(
            "select day, s from mv where \"{IVM_ROW_KINDS_COLUMN}\" = 'insert' order by day"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let days = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                days.value(row).to_string(),
                arrow::util::display::array_value_to_string(batch.column(1), row)
                    .unwrap(),
            ));
        }
    }
    rows
}

/// A partition-local fallback statement keeps the partitions that did not
/// change untouched.
#[test_log::test(tokio::test)]
async fn a_partition_local_fallback_recomputes_only_the_changed_partition() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fp_src_{suffix}"),
                table_path(&dir, "src"),
                fact_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let select =
        format!("SELECT day, SUM(price) AS s FROM ivm_fp_src_{suffix} GROUP BY day");
    let mv_schema = fallback_mv_schema(&runtime, &select, &[&source]).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fp_mv_{suffix}"),
                table_path(&dir, "mv"),
                mv_schema,
            )
            .with_primary_keys(vec!["day".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!("INSERT INTO ivm_fp_mv_{suffix} {select}");
    source
        .append_batch(
            executor.runtime().client(),
            fact_batch(&[
                (1, "d1", 1000, "insert"),
                (2, "d2", 2000, "insert"),
                (3, "d3", 3000, "insert"),
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    let runtime = executor.runtime();
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.view_id == mv.table_id)
        .expect("the fallback view is registered");
    let reason = info.fallback_reason.unwrap_or_default();
    assert!(reason.contains("partition pruning: eligible"), "{reason}");
    assert_eq!(
        read_day_sums(runtime, &mv).await,
        vec![
            ("d1".to_string(), "10.00".to_string()),
            ("d2".to_string(), "20.00".to_string()),
            ("d3".to_string(), "30.00".to_string()),
        ]
    );
    let before = read_versions(runtime, &mv).await;
    assert_eq!(before.len(), 3);

    // Only `d2` changes: its partition is rewritten, the others are not.
    source
        .append_batch(runtime.client(), fact_batch(&[(2, "d2", 2500, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    let runtime = executor.runtime();
    assert_eq!(
        read_day_sums(runtime, &mv).await,
        vec![
            ("d1".to_string(), "10.00".to_string()),
            ("d2".to_string(), "25.00".to_string()),
            ("d3".to_string(), "30.00".to_string()),
        ]
    );
    let after = read_versions(runtime, &mv).await;
    for (desc, version) in &after {
        if desc.contains("d2") {
            assert!(
                *version > before[desc],
                "partition {desc} must be rewritten"
            );
        } else {
            assert_eq!(
                *version, before[desc],
                "partition {desc} must stay untouched"
            );
        }
    }
}

/// A change in an unpartitioned source can affect every partition, so the
/// refresh recomputes the whole view.
#[test_log::test(tokio::test)]
async fn an_unpartitioned_source_change_recomputes_every_partition() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fp_fact_{suffix}"),
                table_path(&dir, "fact"),
                fact_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let dim = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fp_dim_{suffix}"),
                table_path(&dir, "dim"),
                dim_schema(),
            )
            .with_primary_keys(vec!["x".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let select = format!(
        "SELECT fact.day, fact.k, fact.price FROM ivm_fp_fact_{suffix} fact \
         JOIN ivm_fp_dim_{suffix} dim ON dim.w = fact.k"
    );
    let mv_schema = fallback_mv_schema(&runtime, &select, &[&fact, &dim]).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fp_join_mv_{suffix}"),
                table_path(&dir, "mv"),
                mv_schema,
            )
            .with_primary_keys(vec!["day".to_string(), "k".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!("INSERT INTO ivm_fp_join_mv_{suffix} {select}");
    fact.append_batch(
        executor.runtime().client(),
        fact_batch(&[(1, "d1", 1000, "insert"), (2, "d2", 2000, "insert")]),
    )
    .await
    .unwrap();
    dim.append_batch(
        executor.runtime().client(),
        dim_batch(&[(7, 1, "insert"), (9, 2, "insert")]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    let runtime = executor.runtime();
    let before = read_versions(runtime, &mv).await;
    assert_eq!(before.len(), 2);

    // Only the unpartitioned `dim` changes: both partitions are rewritten.
    dim.append_batch(runtime.client(), dim_batch(&[(8, 3, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    let runtime = executor.runtime();
    let after = read_versions(runtime, &mv).await;
    for (desc, version) in &after {
        assert!(
            *version > before[desc],
            "partition {desc} must be rewritten after an unpartitioned source change"
        );
    }
}
