// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Explicit cleanup of IVM definitions and state, addressed by target table.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    DropViewOptions, IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, MinMaxKind,
    min_max_mv_schema_for, row_mv_schema_for,
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

async fn count(runtime: &IvmRuntime, table: &IvmTable) -> usize {
    table
        .read_current(runtime.client())
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

#[test_log::test(tokio::test)]
async fn drop_by_target_removes_state_but_keeps_the_target() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let src = source(&runtime, &dir, &format!("ivm_drop_src_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_drop_mv_{suffix}"),
                table_path(&dir, "mv"),
                min_max_mv_schema_for(
                    &src.schema,
                    &["k".to_string()],
                    "v",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_drop_mv_{suffix} \
         SELECT k, MIN(v) FROM ivm_drop_src_{suffix} GROUP BY k"
    );
    src.append_batch(
        executor.runtime().client(),
        batch(&[(1, 10, "insert"), (2, 20, "insert")]),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(count(executor.runtime(), &mv).await, 2);

    let view_id = mv.table_id.clone();
    let states = executor.runtime().list_states(&view_id).await.unwrap();
    assert!(
        states
            .iter()
            .any(|state| state.role != lakesoul_ivm::StateRole::Mv),
        "the min/max view keeps a value-count state: {states:?}"
    );

    let runtime = executor.runtime();
    let reports = runtime
        .drop_views_for_target("default", &mv.table_name, &DropViewOptions::default())
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert!(reports[0].dropped);
    assert!(!reports[0].states.is_empty());

    // The target remains a normal, readable table.
    assert_eq!(count(runtime, &mv).await, 2);
    assert!(
        runtime
            .metadata()
            .get_view_spec(&view_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(runtime.list_states(&view_id).await.unwrap().is_empty());
    assert!(
        runtime
            .open_table(&format!("{}__ivm_state", mv.table_name), "default")
            .await
            .is_err(),
        "the target's internal __ivm_state view must be removed"
    );
    for table in [&src, &mv] {
        runtime
            .client()
            .drop_table(&table.table_name, "default")
            .await
            .unwrap();
    }
}

#[test_log::test(tokio::test)]
async fn drop_by_target_refuses_dependents_unless_cascaded() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let src = source(&runtime, &dir, &format!("ivm_drop2_src_{suffix}")).await;
    let upstream = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_drop2_up_{suffix}"),
                table_path(&dir, "up"),
                row_mv_schema_for(&src.schema, &["k".to_string(), "v".to_string()])
                    .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let downstream = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_drop2_down_{suffix}"),
                table_path(&dir, "down"),
                row_mv_schema_for(&upstream.schema, &["k".to_string(), "v".to_string()])
                    .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    src.append_batch(executor.runtime().client(), batch(&[(1, 10, "insert")]))
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO ivm_drop2_up_{suffix} \
             SELECT k, v FROM ivm_drop2_src_{suffix}"
        ))
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO ivm_drop2_down_{suffix} \
             SELECT k, v FROM ivm_drop2_up_{suffix}"
        ))
        .await
        .unwrap();

    let runtime = executor.runtime();
    let error = runtime
        .drop_views_for_target(
            "default",
            &upstream.table_name,
            &DropViewOptions::default(),
        )
        .await
        .unwrap_err();
    assert!(
        format!("{error}").contains("read by other IVM views"),
        "{error}"
    );

    let reports = runtime
        .drop_views_for_target(
            "default",
            &upstream.table_name,
            &DropViewOptions {
                cascade: true,
                ..DropViewOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    // Both targets keep their rows even though their IVM definitions are gone.
    assert_eq!(count(runtime, &upstream).await, 1);
    assert_eq!(count(runtime, &downstream).await, 1);
    assert!(
        runtime
            .metadata()
            .get_view_spec(&upstream.table_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .metadata()
            .get_view_spec(&downstream.table_id)
            .await
            .unwrap()
            .is_none()
    );

    for table in [&src, &upstream, &downstream] {
        runtime
            .client()
            .drop_table(&table.table_name, "default")
            .await
            .unwrap();
    }
}
