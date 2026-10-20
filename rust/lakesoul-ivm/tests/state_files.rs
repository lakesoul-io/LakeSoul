// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The state write layout: a refresh coalesces its output batches instead of
//! writing one file per batch per hash bucket.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, PhysicalFormat,
    row_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

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

async fn state_files(runtime: &IvmRuntime, table: &IvmTable) -> usize {
    let mut files = 0;
    for partition in runtime
        .client()
        .get_all_partition_info(&table.table_id)
        .await
        .unwrap()
    {
        files += runtime
            .client()
            .get_data_files_of_single_partition(&partition)
            .await
            .unwrap()
            .len();
    }
    files
}

#[test_log::test]
fn refresh_writes_few_state_files() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let runtime = IvmRuntime::from_env().await.unwrap();
        runtime.init_schema().await.unwrap();
        let dir = tempdir().unwrap();
        let suffix = uuid::Uuid::new_v4().simple();
        let source = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_files_src_{suffix}"),
                    format!("file://{}", dir.path().join("src").display()),
                    schema(),
                )
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN)
                .with_file_format(PhysicalFormat::Vortex),
            )
            .await
            .unwrap();
        let mv = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_files_mv_{suffix}"),
                    format!("file://{}", dir.path().join("mv").display()),
                    row_mv_schema_for(
                        &schema(),
                        &["k".to_string(), "g".to_string(), "v".to_string()],
                    )
                    .unwrap(),
                )
                .with_primary_keys(vec!["k".to_string()])
                .with_file_format(PhysicalFormat::Vortex),
            )
            .await
            .unwrap();
        let executor = IvmSqlExecutor::new(runtime);
        let rows = (0..50_000)
            .map(|key| (key, "g0", key * 2, "insert"))
            .collect::<Vec<_>>();
        source
            .append_batch(executor.runtime().client(), batch(&rows))
            .await
            .unwrap();

        // The refresh emits ~8192-row batches; coalescing must turn them into
        // one append, i.e. one file per hash bucket (four).
        executor
            .execute(&format!(
                "INSERT INTO {} SELECT k, g, v FROM {} WHERE v >= 0",
                mv.table_name, source.table_name
            ))
            .await
            .unwrap();
        let files = state_files(executor.runtime(), &mv).await;
        assert!(
            files <= 8,
            "a 50k-row initial refresh wrote {files} state files"
        );
        let rows_written = mv
            .read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum::<u64>();
        assert_eq!(rows_written, 50_000, "the coalesced write lost rows");

        // A full overwrite truncates and rewrites the view: it must coalesce
        // as well (the benchmark's full-recompute path).
        executor
            .execute(&format!(
                "INSERT OVERWRITE {} SELECT k, g, v FROM {} WHERE v >= 0",
                mv.table_name, source.table_name
            ))
            .await
            .unwrap();
        let files = state_files(executor.runtime(), &mv).await;
        assert!(files <= 8, "a 50k-row overwrite wrote {files} state files");

        // An incremental round adds the delete/insert files of its one
        // changed key: at most one file per bucket.
        source
            .append_batch(
                executor.runtime().client(),
                batch(&[(1, "g0", 7, "insert")]),
            )
            .await
            .unwrap();
        let before = state_files(executor.runtime(), &mv).await;
        executor
            .execute(&format!(
                "INSERT INTO {} SELECT k, g, v FROM {} WHERE v >= 0",
                mv.table_name, source.table_name
            ))
            .await
            .unwrap();
        let after = state_files(executor.runtime(), &mv).await;
        assert!(
            after - before <= 4,
            "one changed row added {} state files",
            after - before
        );
    });
}
