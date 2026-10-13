// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The Tier-2 full-recompute fallback: a statement whose shape the
//! incremental analyzer does not maintain is recomputed in full on every
//! refresh, the action names it, and the switch restores the explicit
//! rejection.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
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

async fn create_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(name, table_path(dir, name), source_schema())
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

/// The MV schema a fallback statement must produce: the planned statement's
/// output columns plus the row kind and epoch columns.
async fn fallback_mv_schema(
    runtime: &IvmRuntime,
    select: &str,
    source: &IvmTable,
) -> SchemaRef {
    let context = SessionContext::new();
    context
        .register_table(
            source.table_name.as_str(),
            Arc::new(runtime.table_provider(source)),
        )
        .unwrap();
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

/// The `k, g, v` rows of the MV, ordered by `k`.
async fn read_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select k, g, v from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row),
                groups.value(row).to_string(),
                values.value(row),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn a_rejected_shape_falls_back_to_a_full_recompute() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_fb1_f_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    // `LIMIT ... OFFSET` is not maintained incrementally.
    let select = format!(
        "SELECT k, g, v FROM {} ORDER BY k LIMIT 2 OFFSET 1",
        fact.table_name
    );
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fb1_mv_{suffix}"),
                table_path(&dir, "mv"),
                fallback_mv_schema(runtime, &select, &fact).await,
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let statement = format!("INSERT INTO {} {select}", mv.table_name);

    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "b", 20, "insert"),
            (3, "c", 30, "insert"),
        ]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    assert!(execution.epoch.is_some());
    // The metadata says the view is recomputed in full and why.
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.view_id == mv.table_id)
        .expect("the view is registered");
    assert!(info.fallback);
    assert!(
        info.fallback_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("OFFSET"))
    );
    assert_eq!(
        read_rows(runtime, &mv).await,
        vec![(2, "b".to_string(), 20), (3, "c".to_string(), 30)]
    );

    // Deleting a row shifts the window.
    fact.append_batch(runtime.client(), source_batch(&[(2, "b", 20, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    assert_eq!(
        read_rows(runtime, &mv).await,
        vec![(3, "c".to_string(), 30)]
    );

    // A new low key shifts it back.
    fact.append_batch(runtime.client(), source_batch(&[(0, "z", 1, "insert")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_rows(runtime, &mv).await,
        vec![(1, "a".to_string(), 10), (3, "c".to_string(), 30)]
    );

    // No source change: the refresh is a no-op and reports no epoch.
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    assert_eq!(execution.epoch, None);
    assert_eq!(
        read_rows(runtime, &mv).await,
        vec![(1, "a".to_string(), 10), (3, "c".to_string(), 30)]
    );
}

#[test_log::test(tokio::test)]
async fn an_append_only_fallback_mv_keeps_repeated_rows() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_fb2_f_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let select = format!(
        "SELECT g, v FROM {} ORDER BY k LIMIT 3 OFFSET 1",
        fact.table_name
    );
    // No primary keys: the full rewrite may repeat rows.
    let mv = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_fb2_mv_{suffix}"),
            table_path(&dir, "mv"),
            fallback_mv_schema(runtime, &select, &fact).await,
        ))
        .await
        .unwrap();
    let statement = format!("INSERT INTO {} {select}", mv.table_name);

    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "x", 1, "insert"),
            (2, "a", 2, "insert"),
            (3, "a", 3, "insert"),
        ]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "fallback");
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let rows = context
        .sql("select count(*) from mv")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    // Both `a` rows survive the rewrite.
    assert_eq!(count, 2);
}

#[test_log::test(tokio::test)]
async fn a_keyed_fallback_result_must_be_unique() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_fb3_f_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let select = format!(
        "SELECT g, v FROM {} ORDER BY k LIMIT 3 OFFSET 1",
        fact.table_name
    );
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fb3_mv_{suffix}"),
                table_path(&dir, "mv"),
                fallback_mv_schema(runtime, &select, &fact).await,
            )
            // `g` repeats in the result: merge-on-read would collapse it.
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "x", 1, "insert"),
            (2, "a", 2, "insert"),
            (3, "a", 3, "insert"),
        ]),
    )
    .await
    .unwrap();
    let error = executor
        .execute(&format!("INSERT INTO {} {select}", mv.table_name))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("repeats the primary key"), "{error}");
}

#[test_log::test(tokio::test)]
async fn the_fallback_switch_restores_the_explicit_rejection() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_fb4_f_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let select = format!(
        "SELECT k, g, v FROM {} ORDER BY k LIMIT 2 OFFSET 1",
        fact.table_name
    );
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fb4_mv_{suffix}"),
                table_path(&dir, "mv"),
                fallback_mv_schema(runtime, &select, &fact).await,
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    fact.append_batch(runtime.client(), source_batch(&[(1, "a", 10, "insert")]))
        .await
        .unwrap();
    let error = executor
        .execute(&format!("INSERT INTO {} {select}", mv.table_name))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("unsupported incremental view shape"),
        "{error}"
    );
}

#[test_log::test(tokio::test)]
async fn a_broken_statement_stays_fatal() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let _fact = create_source(&runtime, &dir, &format!("ivm_fb5_f_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let select = "SELECT k, g, v FROM missing";
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fb5_mv_{suffix}"),
                table_path(&dir, "mv"),
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("g", DataType::Utf8, false),
                    Field::new("v", DataType::Int64, false),
                    Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
                    Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
                ])),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let error = executor
        .execute(&format!("INSERT INTO {} {select}", mv.table_name))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("missing"), "{error}");
}

#[test_log::test(tokio::test)]
async fn an_incremental_view_cannot_read_a_fallback_view() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_fb6_f_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let select = format!(
        "SELECT k, g, v FROM {} ORDER BY k LIMIT 2 OFFSET 1",
        fact.table_name
    );
    let fallback_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fb6_fb_{suffix}"),
                table_path(&dir, "fb"),
                fallback_mv_schema(runtime, &select, &fact).await,
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    fact.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
    )
    .await
    .unwrap();
    let execution = executor
        .execute(&format!("INSERT INTO {} {select}", fallback_mv.table_name))
        .await
        .unwrap();
    assert_eq!(execution.action.label(), "fallback");

    // A plain incremental view over the fallback MV is rejected.
    let incremental_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fb6_inc_{suffix}"),
                table_path(&dir, "inc"),
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("g", DataType::Utf8, false),
                    Field::new("v", DataType::Int64, false),
                    Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
                    Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
                ])),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let error = executor
        .execute(&format!(
            "INSERT INTO {} SELECT k, g, v FROM {}",
            incremental_mv.table_name, fallback_mv.table_name
        ))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("fallback view"), "{error}");
}
