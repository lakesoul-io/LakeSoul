// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! A composite-key multi-aggregate source must remove a group when its last
//! source row is deleted.  The compact refresh reads the before/current rows
//! for the changed key tuples; a deleted key has no current row and must not
//! be read back from its tombstone.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    DropViewOptions, IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, StateRole,
    multi_agg_mv_schema_for,
};
use tempfile::tempdir;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("part", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

async fn mv_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(schema.index_of("sum_v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of("rowKinds").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) != "insert" {
                continue;
            }
            rows.push((groups.value(row).to_string(), sums.value(row)));
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn composite_key_source_removes_a_deleted_group() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_composite_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string(), "part".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let mv_schema = multi_agg_mv_schema_for(
        &source.schema,
        &["g".to_string()],
        &[],
        &[
            ("sum_v".to_string(), DataType::Int64),
            ("sum_v2".to_string(), DataType::Int64),
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_composite_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                mv_schema,
            )
            .with_primary_keys(vec!["g".to_string()])
            .with_cdc_column(lakesoul_ivm::IVM_ROW_KINDS_COLUMN),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, 1, "a", 10, "insert"),
                (1, 2, "a", 20, "insert"),
                (2, 1, "b", 5, "insert"),
            ]),
        )
        .await
        .unwrap();
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(v) AS sum_v, SUM(v * 2) AS sum_v2 \
         FROM {} GROUP BY g",
        mv.table_name, source.table_name
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        mv_rows(runtime, &mv).await,
        vec![("a".to_string(), 30), ("b".to_string(), 5),]
    );

    let spec = runtime
        .metadata()
        .get_view_spec(&mv.table_id)
        .await
        .unwrap()
        .unwrap();
    let view_id = spec["source_table_id"]
        .as_str()
        .unwrap_or(&mv.table_id)
        .to_string();
    let states = runtime.list_states(&view_id).await.unwrap();
    assert!(
        states.iter().any(|state| state.role == StateRole::State),
        "{states:?}"
    );
    assert!(
        !states.iter().any(|state| matches!(
            state.role,
            StateRole::MultiAggRows | StateRole::MultiAggPartials
        )),
        "{states:?}"
    );

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, 1, "a", 10, "delete"),
                (1, 1, "a", 40, "insert"),
                (3, 1, "c", 7, "insert"),
                (2, 1, "b", 5, "delete"),
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        mv_rows(runtime, &mv).await,
        vec![("a".to_string(), 60), ("c".to_string(), 7),]
    );

    runtime
        .drop_view(&mv.table_id, &DropViewOptions::default())
        .await
        .unwrap();
    for table in [&source, &mv] {
        let _ = runtime
            .client()
            .drop_table(&table.table_name, &table.namespace)
            .await;
    }
}
