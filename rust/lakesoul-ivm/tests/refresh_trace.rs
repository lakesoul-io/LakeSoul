// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Incremental refreshes must not read their source tables in full.  The
//! refresh trace carries `full_scan` / `full_scan_tables`; these tests pin the
//! keyed paths and make a regression visible without a full 1M run.

use std::io::Write;
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTableOptions, semi_anti_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

#[derive(Clone)]
struct BufferWriter(Arc<Mutex<Vec<u8>>>);

impl Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("trace buffer").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("x", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn left_batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn right_batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
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

/// The log line of the last incremental refresh, with ANSI escapes removed.
fn last_refresh_trace(buffer: &Arc<Mutex<Vec<u8>>>, kind: &str) -> String {
    let text = String::from_utf8_lossy(&buffer.lock().expect("trace buffer")).to_string();
    let needle = format!("kind=\"{kind}\"");
    let line = text
        .lines()
        .filter(|line| line.contains("view refresh finished"))
        .rfind(|line| strip_ansi(line).contains(&needle))
        .expect("a refresh trace line");
    strip_ansi(line)
}

fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut escape = false;
    for character in line.chars() {
        if escape {
            if character.is_ascii_alphabetic() {
                escape = false;
            }
        } else if character == '\u{1b}' {
            escape = true;
        } else {
            out.push(character);
        }
    }
    out
}

#[tokio::test(flavor = "current_thread")]
async fn semi_anti_incremental_refresh_has_no_full_scan() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_trace_left_{suffix}"),
                table_path(&dir, "left"),
                left_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_trace_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema(),
            )
            .with_primary_keys(vec!["rk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_trace_mv_{suffix}"),
                table_path(&dir, "mv"),
                semi_anti_mv_schema_for(
                    &left.schema,
                    &["k".to_string(), "g".to_string()],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let writer = BufferWriter(buffer.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_trace_mv_{suffix} \
         SELECT k, g FROM ivm_trace_left_{suffix} l \
         WHERE EXISTS (SELECT 1 FROM ivm_trace_right_{suffix} r WHERE r.g = l.g)"
    );
    left.append_batch(
        executor.runtime().client(),
        left_batch(&[(1, 1, 10, "insert"), (2, 1, 20, "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            executor.runtime().client(),
            right_batch(&[(100, 1, "insert")]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    buffer.lock().expect("trace buffer").clear();

    // One left update: the incremental refresh must read the delta and the
    // keyed side states, not either full source.
    left.append_batch(
        executor.runtime().client(),
        left_batch(&[(1, 1, 11, "insert")]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");

    let runtime = executor.runtime();
    let trace = last_refresh_trace(&buffer, "semi_anti");
    assert!(
        trace.contains("full_scan=false"),
        "incremental semi/anti refresh scanned a table in full: {trace}"
    );

    for table in [&left, &right, &mv] {
        runtime
            .client()
            .drop_table(&table.table_name, "default")
            .await
            .unwrap();
    }
}
