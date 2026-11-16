// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-view freshness is recorded after every refresh/rebuild and can be
//! queried from metadata or computed from the source cursors.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, row_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

async fn source(runtime: &IvmRuntime, dir: &tempfile::TempDir, name: &str) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(name.to_string(), table_path(dir, name), schema())
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

#[test_log::test(tokio::test)]
async fn refresh_records_timestamps_action_and_source_lag() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let src = source(&runtime, &dir, &format!("ivm_fresh_src_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fresh_mv_{suffix}"),
                table_path(&dir, "mv"),
                row_mv_schema_for(&src.schema, &["k".to_string(), "v".to_string()])
                    .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_fresh_mv_{suffix} SELECT k, v FROM ivm_fresh_src_{suffix}"
    );
    src.append_batch(executor.runtime().client(), batch(&[(1, 10, "insert")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();

    let runtime = executor.runtime();
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|info| info.view_id == mv.table_id)
        .expect("the bootstrap registers the view");
    assert!(info.last_refresh_at.is_some());
    assert!(info.last_check_at.is_some());
    assert_eq!(info.last_action.as_deref(), Some("rebuild"));

    // A source commit is visible as lag before the next refresh.
    src.append_batch(runtime.client(), batch(&[(2, 20, "insert")]))
        .await
        .unwrap();
    assert!(
        runtime
            .view_source_lag(&mv.table_id)
            .await
            .unwrap()
            .is_some()
    );

    let before = info.last_refresh_at;
    executor.execute(&statement).await.unwrap();
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|info| info.view_id == mv.table_id)
        .unwrap();
    assert_eq!(info.last_action.as_deref(), Some("incremental"));
    assert!(info.last_refresh_at >= before);
    assert!(info.last_rows_read > 0);
    assert_eq!(info.consecutive_failures, 0);

    // A no-op is a successful check and keeps the freshness timestamp fresh.
    executor.execute(&statement).await.unwrap();
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|info| info.view_id == mv.table_id)
        .unwrap();
    assert_eq!(info.last_action.as_deref(), Some("noop"));
    assert_eq!(info.consecutive_failures, 0);

    for table in [&src, &mv] {
        runtime
            .client()
            .drop_table(&table.table_name, "default")
            .await
            .unwrap();
    }
}
