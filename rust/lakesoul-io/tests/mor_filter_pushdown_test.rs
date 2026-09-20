// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Merge-on-read + filter pushdown correctness.
//!
//! The reader must never return a row that a newer version superseded, no
//! matter which filters are pushed into the file scans.  These tests pin the
//! contract that makes index pushdown safe:
//!
//! * non-primary-key predicates are never pushed into the per-file scans of
//!   a merge-on-read table; they run as a `FilterExec` above the merge,
//! * primary-key predicates are pushed (they are row-invariant across
//!   versions) and still resolve to the newest version,
//! * deleting a row is a file rewrite (the old file is expired), so stale
//!   index candidates simply match no current row.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion_expr::{col, lit};
use lakesoul_io::config::{LakeSoulIOConfig, OPTION_KEY_FILE_FILTER_PUSHDOWN};
use lakesoul_io::reader::LakeSoulReader;
use lakesoul_io::writer::create_writer_with_io_config;
use tempfile::tempdir;

fn batch(id: [i64; 1], val: [i64; 1]) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from_iter_values(id)) as ArrayRef),
        (
            "val",
            Arc::new(Int64Array::from_iter_values(val)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn write(path: String, b: RecordBatch) {
    let conf = LakeSoulIOConfig::builder()
        .with_file(path)
        .with_thread_num(1)
        .with_batch_size(2)
        .with_schema(b.schema())
        .with_primary_keys(vec!["id".to_string()])
        .build();
    let mut w = create_writer_with_io_config(conf).await.unwrap();
    w.write_record_batch(b).await.unwrap();
    w.flush_and_close().await.unwrap();
}

async fn read(
    paths: Vec<String>,
    schema: SchemaRef,
    filters: Vec<datafusion_expr::Expr>,
    pushdown: bool,
) -> Vec<(i64, i64)> {
    let mut builder = LakeSoulIOConfig::builder()
        .with_files(paths)
        .with_thread_num(1)
        .with_batch_size(2)
        .with_schema(schema)
        .with_primary_keys(vec!["id".to_string()])
        .with_filters(filters);
    if pushdown {
        builder = builder.with_option(OPTION_KEY_FILE_FILTER_PUSHDOWN, "true");
    }
    let mut reader = LakeSoulReader::new(builder.build()).unwrap();
    reader.start().await.unwrap();
    let mut out = vec![];
    while let Some(record) = reader.next_rb().await {
        let record = record.unwrap();
        let ids = record
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals = record
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..record.num_rows() {
            out.push((ids.value(i), vals.value(i)));
        }
    }
    out
}

/// An insert (file 1) followed by an update of the same primary key (file 2).
/// Every filter shape must observe the updated value only.
#[tokio::test]
async fn upsert_is_never_read_as_the_old_version() {
    for ext in ["parquet", "vortex"] {
        let dir = tempdir().unwrap();
        let p1 = dir
            .path()
            .join(format!("a.{ext}"))
            .to_string_lossy()
            .into_owned();
        let p2 = dir
            .path()
            .join(format!("b.{ext}"))
            .to_string_lossy()
            .into_owned();
        let b1 = batch([1], [100]);
        let b2 = batch([1], [200]);
        write(p1.clone(), b1.clone()).await;
        write(p2.clone(), b2.clone()).await;
        let schema = b1.schema();
        let paths = vec![p1, p2];

        for pushdown in [false, true] {
            // A non-primary-key filter that only the superseded version
            // matches must return nothing (the predicate stays above merge).
            let stale = read(
                paths.clone(),
                schema.clone(),
                vec![col("val").eq(lit(100_i64))],
                pushdown,
            )
            .await;
            assert!(
                stale.is_empty(),
                "stale read ({ext}, pushdown={pushdown}): {stale:?}"
            );

            // The same predicate on the current value returns the new row.
            let current = read(
                paths.clone(),
                schema.clone(),
                vec![col("val").eq(lit(200_i64))],
                pushdown,
            )
            .await;
            assert_eq!(current, vec![(1, 200)], "{ext}, pushdown={pushdown}");

            // Primary-key filters (the shape the index injects) resolve to
            // the newest version, whether pushed or not.
            for filter in [
                col("id").eq(lit(1_i64)),
                col("id").in_list(vec![lit(1_i64)], false),
            ] {
                let rows =
                    read(paths.clone(), schema.clone(), vec![filter], pushdown).await;
                assert_eq!(
                    rows,
                    vec![(1, 200)],
                    "pk filter must return the newest version ({ext}, pushdown={pushdown})"
                );
            }
        }
    }
}

/// A delete is a rewrite: the expired file is gone from the active file
/// list, so a stale index candidate for the deleted primary key matches no
/// current row.
#[tokio::test]
async fn deleted_row_is_not_resurrected_by_a_stale_candidate() {
    let dir = tempdir().unwrap();
    let p1 = dir.path().join("a.parquet").to_string_lossy().into_owned();
    let p2 = dir.path().join("b.parquet").to_string_lossy().into_owned();
    let b1 = batch([1], [100]);
    let b2 = batch([1], [200]); // rewrite: only the updated row remains active
    write(p1, b1.clone()).await;
    write(p2.clone(), b2.clone()).await;

    // Simulate the post-delete/update active file list: only the rewrite.
    let rows = read(
        vec![p2],
        b1.schema(),
        vec![col("id").in_list(vec![lit(1_i64)], false)],
        true,
    )
    .await;
    assert_eq!(rows, vec![(1, 200)]);

    // A stale index candidate for a key that no longer exists returns
    // nothing instead of resurrecting an expired file.
    let missing = read(
        vec![dir.path().join("b.parquet").to_string_lossy().into_owned()],
        b1.schema(),
        vec![col("id").in_list(vec![lit(2_i64)], false)],
        true,
    )
    .await;
    assert!(missing.is_empty(), "{missing:?}");
}
