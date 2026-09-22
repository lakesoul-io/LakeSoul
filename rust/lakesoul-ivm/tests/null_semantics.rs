// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! NULL semantics. SQL treats NULL as a regular value where rows are grouped
//! (`GROUP BY`, window `PARTITION BY`, `DISTINCT`), so the refresh pipelines
//! must match NULL keys with NULL (`IS NOT DISTINCT FROM`). Join equality
//! keeps SQL semantics (`NULL = NULL` is unknown), so INNER JOIN, SEMI and
//! ANTI never match NULL keys. Aggregates ignore NULL values and can produce
//! NULL results, which the MV state must be able to store.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    DistinctAggKind, DistinctAggView, IVM_ROW_KINDS_COLUMN, IVM_ROW_NUMBER_COLUMN,
    IVM_VALUE_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, JoinView, MinMaxKind,
    MinMaxView, SemiAntiView, SumCountView, WindowView, distinct_agg_mv_schema_for,
    join_view_schema_for, min_max_mv_schema_for, semi_anti_mv_schema,
    sum_count_mv_schema_for, value_count_state_schema_for, window_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn register(
    context: &SessionContext,
    name: &str,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| schema.clone());
    let batches = if batches.is_empty() {
        vec![RecordBatch::new_empty(schema.clone())]
    } else {
        batches
    };
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());
    context.register_table(name, table).unwrap();
}

fn string_array(values: &[Option<&str>]) -> StringArray {
    StringArray::from(values.to_vec())
}

fn optional_i64_array(values: &[Option<i64>]) -> Int64Array {
    Int64Array::from(values.to_vec())
}

// ---------------------------------------------------------------------------
// SUM/COUNT with NULL group keys
// ---------------------------------------------------------------------------

fn sum_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn sum_batch(rows: &[(i64, Option<&str>, Option<i64>, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        sum_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(string_array(
                &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(optional_i64_array(
                &rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn null_sum_state(
    state: &mut HashMap<Option<String>, (Option<i64>, i64)>,
    batch: &RecordBatch,
) {
    let schema = batch.schema();
    let keys = batch
        .column(schema.index_of("g").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = batch
        .column(schema.index_of("sum_v").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = batch
        .column(schema.index_of("count_v").unwrap())
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let kinds = batch
        .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for row in 0..batch.num_rows() {
        if kinds.value(row) == "insert" {
            let key = if keys.is_null(row) {
                None
            } else {
                Some(keys.value(row).to_string())
            };
            let sum = if sums.is_null(row) {
                None
            } else {
                Some(sums.value(row))
            };
            state.insert(key, (sum, counts.value(row)));
        }
    }
}

async fn read_null_sum_state(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> HashMap<Option<String>, (Option<i64>, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        null_sum_state(&mut state, &batch);
    }
    state
}

/// The SQL semantics of the same `SUM`/`COUNT`: NULL is a single group.
async fn full_null_sum(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<Option<String>, (Option<i64>, i64)> {
    let context = SessionContext::new();
    register(
        &context,
        "src",
        source.read_current(runtime.client()).await.unwrap(),
        &source.schema,
    );
    let frame = context
        .sql(&format!(
            "select g, sum(amount) as s, count(1) as c from src \
             where \"{CHANGE_COLUMN}\" <> 'delete' group by g"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(schema.index_of("s").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(schema.index_of("c").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = if keys.is_null(row) {
                None
            } else {
                Some(keys.value(row).to_string())
            };
            let sum = if sums.is_null(row) {
                None
            } else {
                Some(sums.value(row))
            };
            state.insert(key, (sum, counts.value(row)));
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn null_group_keys_sum_count() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_sum_src_{suffix}"),
                table_path(&dir, "src"),
                sum_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let group_keys = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_sum_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("null_sum_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("amount".to_string()),
    );

    // The NULL group holds two rows, one of them with a NULL amount.
    source
        .append_batch(
            runtime.client(),
            sum_batch(&[
                (1, None, Some(10), "insert"),
                (2, Some("a"), Some(5), "insert"),
                (3, Some("a"), None, "insert"),
                (4, None, None, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        read_null_sum_state(&runtime, &mv).await,
        HashMap::from([(None, (Some(10), 2)), (Some("a".to_string()), (Some(5), 2)),])
    );

    // An upsert retracts the old amount; the delete empties one NULL row.
    source
        .append_batch(
            runtime.client(),
            sum_batch(&[
                (2, Some("a"), Some(7), "insert"),
                (1, None, Some(0), "delete"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        read_null_sum_state(&runtime, &mv).await,
        full_null_sum(&runtime, &source).await
    );

    // Removing the last non-NULL amount makes the NULL group sum to NULL.
    source
        .append_batch(
            runtime.client(),
            sum_batch(&[
                (4, None, Some(3), "insert"),
                (5, Some("b"), Some(8), "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        read_null_sum_state(&runtime, &mv).await,
        full_null_sum(&runtime, &source).await
    );
    assert_eq!(
        read_null_sum_state(&runtime, &mv).await,
        HashMap::from([
            (None, (Some(3), 1)),
            (Some("a".to_string()), (Some(7), 2)),
            (Some("b".to_string()), (Some(8), 1)),
        ])
    );

    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        read_null_sum_state(&runtime, &mv).await,
        full_null_sum(&runtime, &source).await
    );
}

// ---------------------------------------------------------------------------
// NULL values in MIN/MAX and DISTINCT aggregates
// ---------------------------------------------------------------------------

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

type ValueRow<'a> = (i64, &'a str, Option<&'a str>, Option<i64>, &'a str);

fn value_batch(rows: &[ValueRow<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        value_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(string_array(
                &rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(optional_i64_array(
                &rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

struct ValueFixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    min_mv: IvmTable,
    min_view: MinMaxView,
    count_mv: IvmTable,
    count_view: DistinctAggView,
    sum_mv: IvmTable,
    sum_view: DistinctAggView,
}

async fn value_fixture() -> ValueFixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_value_src_{suffix}"),
                table_path(&dir, "src"),
                value_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let group_keys = vec!["tag".to_string()];
    let state_keys = vec!["tag".to_string(), IVM_VALUE_COLUMN.to_string()];

    let min_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_min_mv_{suffix}"),
                table_path(&dir, "min_mv"),
                min_max_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    "name",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let min_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_min_state_{suffix}"),
                table_path(&dir, "min_state"),
                value_count_state_schema_for(&source.schema, &group_keys, "name")
                    .unwrap(),
            )
            .with_primary_keys(state_keys.clone())
            .with_bucket_columns(group_keys.clone()),
        )
        .await
        .unwrap();
    let min_view = MinMaxView::new(
        format!("null_min_{suffix}"),
        source.clone(),
        min_mv.clone(),
        min_state,
        "tag",
        "name",
        MinMaxKind::Min,
    );

    let count_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_count_mv_{suffix}"),
                table_path(&dir, "count_mv"),
                distinct_agg_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    "name",
                    DistinctAggKind::Count,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let count_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_count_state_{suffix}"),
                table_path(&dir, "count_state"),
                value_count_state_schema_for(&source.schema, &group_keys, "name")
                    .unwrap(),
            )
            .with_primary_keys(state_keys.clone())
            .with_bucket_columns(group_keys.clone()),
        )
        .await
        .unwrap();
    let count_view = DistinctAggView::new_with_group_keys(
        format!("null_count_{suffix}"),
        source.clone(),
        count_mv.clone(),
        count_state,
        group_keys.clone(),
        "name",
        DistinctAggKind::Count,
    );

    let sum_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_dsum_mv_{suffix}"),
                table_path(&dir, "dsum_mv"),
                distinct_agg_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    "n",
                    DistinctAggKind::Sum,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let sum_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_dsum_state_{suffix}"),
                table_path(&dir, "dsum_state"),
                value_count_state_schema_for(&source.schema, &group_keys, "n").unwrap(),
            )
            .with_primary_keys(state_keys)
            .with_bucket_columns(group_keys.clone()),
        )
        .await
        .unwrap();
    let sum_view = DistinctAggView::new_with_group_keys(
        format!("null_dsum_{suffix}"),
        source.clone(),
        sum_mv.clone(),
        sum_state,
        group_keys,
        "n",
        DistinctAggKind::Sum,
    );

    ValueFixture {
        runtime,
        dir,
        source,
        min_mv,
        min_view,
        count_mv,
        count_view,
        sum_mv,
        sum_view,
    }
}

async fn read_string_value(runtime: &IvmRuntime, mv: &IvmTable) -> Option<String> {
    let mut values = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let tags = batch
            .column(schema.index_of("tag").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = batch
            .column(schema.index_of(IVM_VALUE_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                let value = if value.is_null(row) {
                    None
                } else {
                    Some(value.value(row).to_string())
                };
                values.insert(tags.value(row).to_string(), value);
            }
        }
    }
    values.remove("t").unwrap()
}

async fn read_i64_value(runtime: &IvmRuntime, mv: &IvmTable) -> Option<i64> {
    let mut values = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let tags = batch
            .column(schema.index_of("tag").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = batch
            .column(schema.index_of(IVM_VALUE_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                let value = if value.is_null(row) {
                    None
                } else {
                    Some(value.value(row))
                };
                values.insert(tags.value(row).to_string(), value);
            }
        }
    }
    values.remove("t").unwrap()
}

/// The SQL semantics of the three aggregates over the source state.
async fn full_value_aggregates(
    fixture: &ValueFixture,
) -> (Option<String>, Option<i64>, Option<i64>) {
    let context = SessionContext::new();
    register(
        &context,
        "src",
        fixture
            .source
            .read_current(fixture.runtime.client())
            .await
            .unwrap(),
        &fixture.source.schema,
    );
    let frame = context
        .sql(&format!(
            "select min(name) as m, count(distinct name) as dc, sum(distinct n) as ds \
             from src where \"{CHANGE_COLUMN}\" <> 'delete' group by tag"
        ))
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    let name = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let count = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (
        if name.is_null(0) {
            None
        } else {
            Some(name.value(0).to_string())
        },
        Some(count.value(0)),
        if sum.is_null(0) {
            None
        } else {
            Some(sum.value(0))
        },
    )
}

#[test_log::test(tokio::test)]
async fn null_values_min_max_and_distinct() {
    let fixture = value_fixture().await;
    let runtime = &fixture.runtime;
    let source = &fixture.source;
    let _keep_dir = &fixture.dir;

    // Two identical non-NULL values and two NULL-ish rows: DISTINCT sees one
    // value, MIN ignores NULL.
    source
        .append_batch(
            runtime.client(),
            value_batch(&[
                (1, "t", Some("bob"), Some(5), "insert"),
                (2, "t", None, None, "insert"),
                (3, "t", Some("bob"), Some(5), "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime
        .refresh_min_max(&fixture.min_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&fixture.count_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&fixture.sum_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            read_string_value(runtime, &fixture.min_mv).await,
            read_i64_value(runtime, &fixture.count_mv).await,
            read_i64_value(runtime, &fixture.sum_mv).await,
        ),
        (Some("bob".to_string()), Some(1), Some(5),)
    );

    // Deleting the only non-NULL rows leaves the group on NULL values only:
    // MIN and SUM(DISTINCT) become NULL, COUNT(DISTINCT) becomes 0.
    source
        .append_batch(
            runtime.client(),
            value_batch(&[
                (1, "t", Some("bob"), Some(5), "delete"),
                (3, "t", Some("bob"), Some(5), "delete"),
            ]),
        )
        .await
        .unwrap();
    runtime
        .refresh_min_max(&fixture.min_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&fixture.count_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&fixture.sum_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(full_value_aggregates(&fixture).await, (None, Some(0), None));
    assert_eq!(
        (
            read_string_value(runtime, &fixture.min_mv).await,
            read_i64_value(runtime, &fixture.count_mv).await,
            read_i64_value(runtime, &fixture.sum_mv).await,
        ),
        (None, Some(0), None)
    );

    // New values revive the group.
    source
        .append_batch(
            runtime.client(),
            value_batch(&[
                (4, "t", Some("alice"), Some(7), "insert"),
                (5, "t", None, Some(7), "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime
        .refresh_min_max(&fixture.min_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&fixture.count_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&fixture.sum_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        full_value_aggregates(&fixture).await,
        (Some("alice".to_string()), Some(1), Some(7))
    );
    assert_eq!(
        (
            read_string_value(runtime, &fixture.min_mv).await,
            read_i64_value(runtime, &fixture.count_mv).await,
            read_i64_value(runtime, &fixture.sum_mv).await,
        ),
        (Some("alice".to_string()), Some(1), Some(7))
    );

    runtime.rebuild_min_max(&fixture.min_view).await.unwrap();
    runtime
        .rebuild_distinct_agg(&fixture.count_view)
        .await
        .unwrap();
    runtime
        .rebuild_distinct_agg(&fixture.sum_view)
        .await
        .unwrap();
    assert_eq!(
        (
            read_string_value(runtime, &fixture.min_mv).await,
            read_i64_value(runtime, &fixture.count_mv).await,
            read_i64_value(runtime, &fixture.sum_mv).await,
        ),
        (Some("alice".to_string()), Some(1), Some(7))
    );
}

// ---------------------------------------------------------------------------
// NULL join keys: INNER JOIN, SEMI and ANTI follow `=`
// ---------------------------------------------------------------------------

fn join_left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("jk", DataType::Utf8, true),
        Field::new("lv", DataType::Utf8, true),
    ]))
}

fn join_right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("jk", DataType::Utf8, true),
        Field::new("rv", DataType::Int64, true),
    ]))
}

fn join_left_batch(rows: &[(Option<&str>, Option<&str>)]) -> RecordBatch {
    RecordBatch::try_new(
        join_left_schema(),
        vec![
            Arc::new(string_array(
                &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(string_array(
                &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn join_right_batch(rows: &[(Option<&str>, Option<i64>)]) -> RecordBatch {
    RecordBatch::try_new(
        join_right_schema(),
        vec![
            Arc::new(string_array(
                &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(optional_i64_array(
                &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

type JoinRow = (Option<String>, Option<String>, Option<i64>);

async fn read_join_rows(runtime: &IvmRuntime, output: &IvmTable) -> Vec<JoinRow> {
    let mut rows = Vec::new();
    for batch in output.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("jk").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let left = batch
            .column(schema.index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(schema.index_of("right_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                if keys.is_null(row) {
                    None
                } else {
                    Some(keys.value(row).to_string())
                },
                if left.is_null(row) {
                    None
                } else {
                    Some(left.value(row).to_string())
                },
                if right.is_null(row) {
                    None
                } else {
                    Some(right.value(row))
                },
            ));
        }
    }
    rows.sort();
    rows
}

/// The SQL semantics of the equality join.
async fn full_join_rows(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<JoinRow> {
    let context = SessionContext::new();
    register(
        &context,
        "l",
        left.read_current(runtime.client()).await.unwrap(),
        &left.schema,
    );
    register(
        &context,
        "r",
        right.read_current(runtime.client()).await.unwrap(),
        &right.schema,
    );
    let frame = context
        .sql("select distinct l.jk as jk, l.lv as lv, r.rv as rv from l join r on l.jk = r.jk")
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("jk").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let left_value = batch
            .column(schema.index_of("lv").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right_value = batch
            .column(schema.index_of("rv").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                if keys.is_null(row) {
                    None
                } else {
                    Some(keys.value(row).to_string())
                },
                if left_value.is_null(row) {
                    None
                } else {
                    Some(left_value.value(row).to_string())
                },
                if right_value.is_null(row) {
                    None
                } else {
                    Some(right_value.value(row))
                },
            ));
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn null_join_keys_match_sql() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_null_join_l_{suffix}"),
            table_path(&dir, "left"),
            join_left_schema(),
        ))
        .await
        .unwrap();
    let right = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_null_join_r_{suffix}"),
            table_path(&dir, "right"),
            join_right_schema(),
        ))
        .await
        .unwrap();
    let join_keys = vec!["jk".to_string()];
    let output = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_null_join_out_{suffix}"),
            table_path(&dir, "out"),
            join_view_schema_for(&left.schema, &right.schema, &join_keys, "lv", "rv")
                .unwrap(),
        ))
        .await
        .unwrap();
    let view = JoinView::new_with_join_keys(
        format!("null_join_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        join_keys,
        "lv",
        "rv",
    );

    // NULL keys on either side never match: only the "a" rows join.
    left.append_batch(
        runtime.client(),
        join_left_batch(&[
            (None, Some("L1")),
            (Some("a"), Some("L2")),
            (Some("a"), None),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            join_right_batch(&[
                (None, Some(100)),
                (Some("a"), Some(200)),
                (Some("a"), None),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(
        read_join_rows(&runtime, &output).await,
        full_join_rows(&runtime, &left, &right).await
    );
    assert_eq!(
        read_join_rows(&runtime, &output).await,
        vec![
            (Some("a".to_string()), None, None),
            (Some("a".to_string()), None, Some(200)),
            (Some("a".to_string()), Some("L2".to_string()), None),
            (Some("a".to_string()), Some("L2".to_string()), Some(200)),
        ]
    );

    // New left rows with a real key join the existing "a" rows.
    left.append_batch(
        runtime.client(),
        join_left_batch(&[(Some("a"), Some("L3")), (None, Some("L4"))]),
    )
    .await
    .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(
        read_join_rows(&runtime, &output).await,
        full_join_rows(&runtime, &left, &right).await
    );

    // New NULL right rows join nothing.
    right
        .append_batch(
            runtime.client(),
            join_right_batch(&[(None, Some(300)), (Some("a"), Some(400))]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(
        read_join_rows(&runtime, &output).await,
        full_join_rows(&runtime, &left, &right).await
    );

    runtime.rebuild_join(&view).await.unwrap();
    assert_eq!(
        read_join_rows(&runtime, &output).await,
        full_join_rows(&runtime, &left, &right).await
    );
}

// ---------------------------------------------------------------------------
// NULL join keys: SEMI/ANTI
// ---------------------------------------------------------------------------

fn semi_left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("jk", DataType::Utf8, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn semi_right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Int64, false),
        Field::new("jk", DataType::Utf8, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn semi_left_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        semi_left_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(string_array(
                &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

fn semi_right_batch(rows: &[(i64, Option<&str>, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        semi_right_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(string_array(
                &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

async fn read_semi_keys(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<i64> {
    let mut keys = Vec::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let values = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                keys.push(values.value(row));
            }
        }
    }
    keys.sort();
    keys
}

/// The SQL semantics of SEMI/ANTI: an EXISTS with `=`.
async fn full_semi_keys(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
    anti: bool,
) -> Vec<i64> {
    let context = SessionContext::new();
    register(
        &context,
        "l",
        left.read_current(runtime.client()).await.unwrap(),
        &left.schema,
    );
    register(
        &context,
        "r",
        right.read_current(runtime.client()).await.unwrap(),
        &right.schema,
    );
    let predicate = if anti { "not exists" } else { "exists" };
    let frame = context
        .sql(&format!(
            "select k from l where l.\"{CHANGE_COLUMN}\" <> 'delete' \
             and {predicate} (select 1 from r \
                              where r.jk = l.jk and r.\"{CHANGE_COLUMN}\" <> 'delete')"
        ))
        .await
        .unwrap();
    let mut keys = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let values = batch
            .column(batch.schema().index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            keys.push(values.value(row));
        }
    }
    keys.sort();
    keys
}

#[test_log::test(tokio::test)]
async fn null_semi_anti_keys_match_sql() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_semi_l_{suffix}"),
                table_path(&dir, "left"),
                semi_left_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_semi_r_{suffix}"),
                table_path(&dir, "right"),
                semi_right_schema(),
            )
            .with_primary_keys(vec!["rk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let join_keys = vec!["jk".to_string()];
    let semi_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_semi_mv_{suffix}"),
                table_path(&dir, "semi_mv"),
                semi_anti_mv_schema(&left.schema),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let anti_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_anti_mv_{suffix}"),
                table_path(&dir, "anti_mv"),
                semi_anti_mv_schema(&left.schema),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let semi = SemiAntiView::new(
        format!("null_semi_{suffix}"),
        left.clone(),
        right.clone(),
        semi_mv.clone(),
        join_keys.clone(),
        false,
    );
    let anti = SemiAntiView::new(
        format!("null_anti_{suffix}"),
        left.clone(),
        right.clone(),
        anti_mv.clone(),
        join_keys,
        true,
    );

    // NULL keys on both sides never match: SEMI only sees k=2.
    left.append_batch(
        runtime.client(),
        semi_left_batch(&[
            (1, None, "insert"),
            (2, Some("a"), "insert"),
            (3, Some("b"), "insert"),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            semi_right_batch(&[(10, None, "insert"), (11, Some("a"), "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_semi_anti(&semi).await.unwrap().unwrap();
    runtime.refresh_semi_anti(&anti).await.unwrap().unwrap();
    assert_eq!(
        read_semi_keys(&runtime, &semi_mv).await,
        full_semi_keys(&runtime, &left, &right, false).await
    );
    assert_eq!(
        read_semi_keys(&runtime, &anti_mv).await,
        full_semi_keys(&runtime, &left, &right, true).await
    );
    assert_eq!(read_semi_keys(&runtime, &semi_mv).await, vec![2]);
    assert_eq!(read_semi_keys(&runtime, &anti_mv).await, vec![1, 3]);

    // Moving a left key out of NULL makes it match.
    left.append_batch(
        runtime.client(),
        semi_left_batch(&[(1, Some("a"), "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_semi_anti(&semi).await.unwrap().unwrap();
    runtime.refresh_semi_anti(&anti).await.unwrap().unwrap();
    assert_eq!(
        read_semi_keys(&runtime, &semi_mv).await,
        full_semi_keys(&runtime, &left, &right, false).await
    );
    assert_eq!(
        read_semi_keys(&runtime, &anti_mv).await,
        full_semi_keys(&runtime, &left, &right, true).await
    );
    assert_eq!(read_semi_keys(&runtime, &semi_mv).await, vec![1, 2]);
    assert_eq!(read_semi_keys(&runtime, &anti_mv).await, vec![3]);

    // Deleting the matched right row empties the SEMI view.
    right
        .append_batch(
            runtime.client(),
            semi_right_batch(&[(11, Some("a"), "delete")]),
        )
        .await
        .unwrap();
    runtime.refresh_semi_anti(&semi).await.unwrap().unwrap();
    runtime.refresh_semi_anti(&anti).await.unwrap().unwrap();
    assert_eq!(
        read_semi_keys(&runtime, &semi_mv).await,
        full_semi_keys(&runtime, &left, &right, false).await
    );
    assert_eq!(
        read_semi_keys(&runtime, &anti_mv).await,
        full_semi_keys(&runtime, &left, &right, true).await
    );
    assert!(read_semi_keys(&runtime, &semi_mv).await.is_empty());
    assert_eq!(read_semi_keys(&runtime, &anti_mv).await, vec![1, 2, 3]);
}

// ---------------------------------------------------------------------------
// ROW_NUMBER with NULL partition keys
// ---------------------------------------------------------------------------

fn window_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("p", DataType::Utf8, true),
        Field::new("ord", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn window_batch(rows: &[(i64, Option<&str>, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        window_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(string_array(
                &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

async fn read_window_rows(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(i64, Option<String>, i64)> {
    let mut rows = Vec::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let parts = batch
            .column(schema.index_of("p").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let numbers = batch
            .column(schema.index_of(IVM_ROW_NUMBER_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                rows.push((
                    ids.value(row),
                    if parts.is_null(row) {
                        None
                    } else {
                        Some(parts.value(row).to_string())
                    },
                    numbers.value(row),
                ));
            }
        }
    }
    rows.sort();
    rows
}

/// The SQL semantics of the ranking window.
async fn full_window_rows(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> Vec<(i64, Option<String>, i64)> {
    let context = SessionContext::new();
    register(
        &context,
        "src",
        source.read_current(runtime.client()).await.unwrap(),
        &source.schema,
    );
    let frame = context
        .sql(&format!(
            "select id, p, cast(row_number() over (partition by p order by ord) as bigint) as rn \
             from src where \"{CHANGE_COLUMN}\" <> 'delete'"
        ))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let parts = batch
            .column(schema.index_of("p").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let numbers = batch
            .column(schema.index_of("rn").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                if parts.is_null(row) {
                    None
                } else {
                    Some(parts.value(row).to_string())
                },
                numbers.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn null_window_partition_keys() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_window_src_{suffix}"),
                table_path(&dir, "src"),
                window_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let partitions = vec!["p".to_string()];
    let order = vec!["ord".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_window_mv_{suffix}"),
                table_path(&dir, "mv"),
                window_mv_schema_for(&source.schema, &partitions, &["id".to_string()])
                    .unwrap(),
            )
            .with_primary_keys(vec!["p".to_string(), "id".to_string()])
            .with_bucket_columns(partitions.clone()),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("null_window_{suffix}"),
        source.clone(),
        mv.clone(),
        partitions,
        order,
    );

    // NULL rows form one partition of their own.
    source
        .append_batch(
            runtime.client(),
            window_batch(&[
                (1, None, 1, "insert"),
                (2, None, 2, "insert"),
                (3, Some("a"), 1, "insert"),
                (4, Some("a"), 2, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap().unwrap();
    assert_eq!(
        read_window_rows(&runtime, &mv).await,
        full_window_rows(&runtime, &source).await
    );

    // A new row renumbers only the NULL partition.
    source
        .append_batch(runtime.client(), window_batch(&[(5, None, 0, "insert")]))
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap().unwrap();
    assert_eq!(
        read_window_rows(&runtime, &mv).await,
        full_window_rows(&runtime, &source).await
    );
    assert_eq!(
        read_window_rows(&runtime, &mv).await,
        vec![
            (1, None, 2),
            (2, None, 3),
            (3, Some("a".to_string()), 1),
            (4, Some("a".to_string()), 2),
            (5, None, 1),
        ]
    );

    // A delete renumbers only its own partition.
    source
        .append_batch(
            runtime.client(),
            window_batch(&[(3, Some("a"), 1, "delete")]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap().unwrap();
    assert_eq!(
        read_window_rows(&runtime, &mv).await,
        full_window_rows(&runtime, &source).await
    );
    assert_eq!(
        read_window_rows(&runtime, &mv).await,
        vec![
            (1, None, 2),
            (2, None, 3),
            (4, Some("a".to_string()), 1),
            (5, None, 1),
        ]
    );

    runtime.rebuild_window(&view).await.unwrap();
    assert_eq!(
        read_window_rows(&runtime, &mv).await,
        full_window_rows(&runtime, &source).await
    );
}

// ---------------------------------------------------------------------------
// Row identity (source primary keys) still must be non-NULL
// ---------------------------------------------------------------------------

#[test_log::test(tokio::test)]
async fn nullable_source_primary_keys_are_rejected() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();

    let nullable_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("p", DataType::Utf8, true),
        Field::new("ord", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_pk_src_{suffix}"),
                table_path(&dir, "src"),
                nullable_schema,
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_pk_mv_{suffix}"),
                table_path(&dir, "mv"),
                window_mv_schema_for(
                    &source.schema,
                    &["p".to_string()],
                    &["id".to_string()],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["p".to_string(), "id".to_string()]),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("null_pk_window_{suffix}"),
        source.clone(),
        mv.clone(),
        vec!["p".to_string()],
        vec!["ord".to_string()],
    );
    let error = runtime.refresh_window(&view).await.unwrap_err();
    assert!(
        error.to_string().contains("must be non-nullable"),
        "unexpected error: {error}"
    );

    // SEMI/ANTI also use the left primary key as row identity.
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_pk_left_{suffix}"),
                table_path(&dir, "left"),
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("jk", DataType::Utf8, true),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_pk_right_{suffix}"),
                table_path(&dir, "right"),
                Arc::new(Schema::new(vec![
                    Field::new("rk", DataType::Int64, false),
                    Field::new("jk", DataType::Utf8, true),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["rk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let semi_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_pk_semi_mv_{suffix}"),
                table_path(&dir, "semi_mv"),
                semi_anti_mv_schema(&left.schema),
            )
            .with_primary_keys(vec!["id".to_string()]),
        )
        .await
        .unwrap();
    let semi = SemiAntiView::new(
        format!("null_pk_semi_{suffix}"),
        left,
        right,
        semi_mv,
        vec!["jk".to_string()],
        false,
    );
    let error = runtime.refresh_semi_anti(&semi).await.unwrap_err();
    assert!(
        error.to_string().contains("must be non-nullable"),
        "unexpected error: {error}"
    );
}
