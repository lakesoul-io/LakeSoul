// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! M1 of the SQL frontend: open existing tables by name/id, drive a refresh
//! from a persisted `ViewSpec`, and manage the state registry (a rebuilt view
//! must be able to replace its state table) plus the stored definition hash.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmMetadata, IvmRuntime, IvmTable, IvmTableOptions,
    PhysicalFormat, StateRole, SumCountView, ViewSpec, sum_count_mv_schema_for,
};
use serde_json::json;
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

async fn read_sum(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<String, i64> {
    let context = SessionContext::new();
    let batches = mv.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(mv.schema.clone(), vec![batches]).unwrap());
    context.register_table("mv", table).unwrap();
    let frame = context
        .sql(&format!(
            "select g, sum_v from mv where \"{IVM_ROW_KINDS_COLUMN}\" = 'insert'"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
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
            state.insert(groups.value(row).to_string(), sums.value(row));
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn open_table_by_name_and_id_roundtrips_metadata() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let name = format!("m1_open_{suffix}");
    let source = runtime
        .create_table(
            IvmTableOptions::new(name.clone(), table_path(&dir, "src"), schema())
                .with_primary_keys(vec!["k".to_string()])
                .with_bucket_columns(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN)
                .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();

    let opened = runtime.open_table(&name, "default").await.unwrap();
    assert_eq!(opened.table_id, source.table_id);
    assert_eq!(opened.table_name, source.table_name);
    assert_eq!(opened.namespace, source.namespace);
    assert_eq!(opened.table_path, source.table_path);
    assert_eq!(opened.schema, source.schema);
    assert_eq!(opened.primary_keys, source.primary_keys);
    assert_eq!(opened.bucket_columns, source.bucket_columns);
    assert_eq!(opened.hash_bucket_num, source.hash_bucket_num);
    assert_eq!(opened.cdc_column, source.cdc_column);
    assert_eq!(opened.file_format, source.file_format);

    let by_id = runtime.open_table_by_id(&source.table_id).await.unwrap();
    assert_eq!(by_id.table_name, source.table_name);
    assert_eq!(by_id.primary_keys, source.primary_keys);

    assert!(
        runtime
            .open_table("m1_missing_table", "default")
            .await
            .is_err()
    );
    assert!(runtime.open_table_by_id("table_missing").await.is_err());
}

#[test_log::test(tokio::test)]
async fn refresh_spec_drives_a_registered_view() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m1_spec_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let groups = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m1_spec_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &groups, Some("v")).unwrap(),
            )
            .with_primary_keys(groups.clone()),
        )
        .await
        .unwrap();
    let view_id = format!("m1_spec_view_{suffix}");
    let view = SumCountView {
        view_id: view_id.clone(),
        source: source.clone(),
        mv: mv.clone(),
        group_keys: groups.clone(),
        value_column: Some("v".to_string()),
        count_column: None,
        filter: None,
        having: None,
        average: false,
        refresh_interval_ms: 7_000,
    };
    runtime.register_view(&view).await.unwrap();

    source
        .append_batch(
            runtime.client(),
            batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 20, "insert"),
                (3, "b", 5, "insert"),
            ]),
        )
        .await
        .unwrap();

    // The spec is all `refresh_spec` needs: it reopens every table by id.
    let spec = ViewSpec::SumCount {
        view_id: view_id.clone(),
        source_table_id: source.table_id.clone(),
        mv_table_id: mv.table_id.clone(),
        group_keys: groups.clone(),
        value_column: Some("v".to_string()),
        count_column: None,
        filter: None,
        having: None,
        average: false,
    };
    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    assert_eq!(
        read_sum(&runtime, &mv).await,
        HashMap::from([("a".to_string(), 30), ("b".to_string(), 5)])
    );

    // Incremental round: one update and one delete.
    source
        .append_batch(
            runtime.client(),
            batch(&[
                (1, "a", 11, "insert"),
                (3, "b", 5, "delete"),
                (4, "c", 7, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    // A group whose count reaches zero is removed from the view.
    assert_eq!(
        read_sum(&runtime, &mv).await,
        HashMap::from([("a".to_string(), 31), ("c".to_string(), 7)])
    );

    // The refresh preserves the registered scheduling interval instead of
    // overwriting it with the spec-only default.
    let metadata = IvmMetadata::from_env().await.unwrap();
    assert_eq!(
        metadata.view_refresh_interval_ms(&view_id).await.unwrap(),
        Some(7_000)
    );
    let states = runtime.list_states(&view_id).await.unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].table_id, mv.table_id);
}

#[test_log::test(tokio::test)]
async fn state_unregister_allows_a_rebuilt_view_to_replace_its_table() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let metadata = IvmMetadata::from_env().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let first = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m1_state_a_{suffix}"),
                table_path(&dir, "state_a"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let second = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m1_state_b_{suffix}"),
                table_path(&dir, "state_b"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view_id = format!("m1_state_view_{suffix}");

    metadata
        .register_state(&view_id, StateRole::State, &first)
        .await
        .unwrap();
    // The registry refuses to rebind a role to a different table until the
    // old binding is removed, so a rebuilt view cannot silently switch state.
    assert!(
        metadata
            .register_state(&view_id, StateRole::State, &second)
            .await
            .is_err()
    );
    assert_eq!(
        metadata
            .get_state(&view_id, StateRole::State)
            .await
            .unwrap()
            .unwrap()
            .table_id,
        first.table_id
    );

    metadata
        .unregister_state(&view_id, StateRole::State)
        .await
        .unwrap();
    metadata
        .register_state(&view_id, StateRole::State, &second)
        .await
        .unwrap();
    assert_eq!(
        metadata
            .get_state(&view_id, StateRole::State)
            .await
            .unwrap()
            .unwrap()
            .table_id,
        second.table_id
    );
}

#[test_log::test(tokio::test)]
async fn view_definition_hash_and_sql_roundtrip() {
    let metadata = IvmMetadata::from_env().await.unwrap();
    metadata.init_schema().await.unwrap();
    let view_id = format!("m1_def_{}", uuid::Uuid::new_v4().simple());
    metadata
        .upsert_view(&view_id, &json!({"kind": "sum_count"}), 0)
        .await
        .unwrap();
    assert_eq!(metadata.view_definition(&view_id).await.unwrap(), None);

    metadata
        .set_view_definition(&view_id, "hash-123", "INSERT INTO mv SELECT ...")
        .await
        .unwrap();
    assert_eq!(
        metadata.view_definition(&view_id).await.unwrap(),
        Some((
            "hash-123".to_string(),
            "INSERT INTO mv SELECT ...".to_string()
        ))
    );
}
