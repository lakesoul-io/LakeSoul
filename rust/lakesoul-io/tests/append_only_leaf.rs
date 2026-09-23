// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Regression test: a work unit of an append-only table whose files do not all
//! carry the table's schema still scans as one plain leaf.
//!
//! Schema evolution leaves older files without the columns added later, and a
//! work unit holds files written before and after it. The leaf declares the
//! merged schema and the reader pads what a single file does not have, so the
//! work unit keeps the shape the distributed planner splits over a stage's
//! tasks; before, such a work unit fell back to `MergeParquetExec`, which the
//! planner pins to one task (and which costs the stage its distribution).

use std::sync::Arc;

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lakesoul_io::file_format::append_only_scan_exec;
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;

/// Writes one parquet file of the work unit and describes it for a scan.
///
/// `partition_cols` is the *table's* partition column list: a file carries one
/// value per entry, parsed from the partition it was written under.
fn file_config(
    dir: &std::path::Path,
    name: &str,
    batch: &RecordBatch,
    partition_cols: &[FieldRef],
    partition_values: Vec<ScalarValue>,
) -> FileScanConfig {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");

    // A file's scan config carries the file's own schema, as the LakeSoul scan
    // builds it: the columns the file has, plus the table's partition columns.
    let table_schema = TableSchema::builder(batch.schema())
        .with_table_partition_cols(partition_cols.to_vec())
        .build();
    let source = ParquetFormat::default().file_source(table_schema);
    let meta = ObjectMeta {
        location: object_store::path::Path::from(path.to_str().unwrap()),
        last_modified: chrono::DateTime::<chrono::Utc>::from(
            std::time::SystemTime::UNIX_EPOCH,
        ),
        size: std::fs::metadata(&path).expect("file metadata").len(),
        e_tag: None,
        version: None,
    };
    FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
        .with_file_groups(vec![FileGroup::new(vec![
            PartitionedFile::new_from_meta(meta).with_partition_values(partition_values),
        ])])
        .build()
}

/// Scans one work unit as a plain leaf and returns what it read.
async fn read_leaf(configs: &[FileScanConfig], merged: &SchemaRef) -> Vec<RecordBatch> {
    let state = SessionContext::new().state();
    let leaf = append_only_scan_exec(&state, &ParquetFormat::default(), configs, merged)
        .expect("the leaf builder must not fail")
        .expect("an append-only work unit scans as a plain leaf");
    let ctx = SessionContext::new();
    leaf.execute(0, ctx.task_ctx())
        .expect("execute the leaf")
        .try_collect()
        .await
        .expect("read the work unit")
}

/// The `column`-th column of every batch, as `Int32` values.
fn i32_column(batches: &[RecordBatch], column: usize) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 column")
                .values()
                .to_vec()
        })
        .collect()
}

/// The `column`-th column of every batch, as strings.
fn str_column(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column");
            (0..column.len())
                .map(|row| column.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn append_only_leaf_reads_files_missing_a_merged_column() {
    let dir = tempfile::tempdir().expect("tempdir").keep();

    let narrow_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let wide_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("evolved", DataType::Utf8, true),
    ]));
    let narrow = RecordBatch::try_new(
        narrow_schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .expect("narrow batch");
    let wide = RecordBatch::try_new(
        wide_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    )
    .expect("wide batch");

    // One work unit: a file written before the schema evolved and one after.
    let configs = vec![
        file_config(&dir, "part-0.parquet", &narrow, &[], vec![]),
        file_config(&dir, "part-1.parquet", &wide, &[], vec![]),
    ];
    // The merged schema the scan must emit, widened for the absent column.
    let merged: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("evolved", DataType::Utf8, true),
    ]));

    let state = SessionContext::new().state();
    let leaf =
        append_only_scan_exec(&state, &ParquetFormat::default(), &configs, &merged)
            .expect("the leaf builder must not fail")
            .expect("a work unit with drifted files still scans as a plain leaf");

    assert_eq!(leaf.schema(), merged);
    let scan = leaf
        .downcast_ref::<DataSourceExec>()
        .expect("the work unit must scan as a plain file scan")
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .expect("the scan leaf must carry a file scan config");
    assert_eq!(
        scan.file_groups.len(),
        1,
        "the work unit is one file group the planner can split"
    );
    assert_eq!(scan.file_groups[0].files().len(), 2);

    let ctx = SessionContext::new();
    let batches: Vec<RecordBatch> = leaf
        .execute(0, ctx.task_ctx())
        .expect("execute the leaf")
        .try_collect()
        .await
        .expect("read the work unit");

    let ids = i32_column(&batches, 0);
    let evolved = batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column(1);
            let column = column.as_any().downcast_ref::<StringArray>().unwrap();
            (0..column.len())
                .map(|row| {
                    if column.is_null(row) {
                        None
                    } else {
                        Some(column.value(row).to_string())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        ids,
        vec![1, 2, 3, 4],
        "both files of the work unit are read"
    );
    assert_eq!(
        evolved,
        vec![None, None, Some("x".to_string()), Some("y".to_string())],
        "a file without the evolved column reads it as NULL"
    );
}

/// A scan that reads only the *second* of its work unit's two partition
/// columns reads that column's own value.
///
/// The reader pairs a file's partition values with the declared partition
/// columns by index, so a leaf that declared only the columns the scan keeps
/// handed the surviving column the value of the one before it.
#[tokio::test]
async fn append_only_leaf_keeps_a_later_partition_column_aligned() {
    let dir = tempfile::tempdir().expect("tempdir").keep();

    let file_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(file_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])
            .expect("batch");
    let partition_cols = vec![
        Arc::new(Field::new("p1", DataType::Int32, false)) as FieldRef,
        Arc::new(Field::new("p2", DataType::Int32, false)) as FieldRef,
    ];
    // The work unit sits at p1 = 7, p2 = 8.
    let configs = vec![file_config(
        &dir,
        "part-0.parquet",
        &batch,
        &partition_cols,
        vec![ScalarValue::Int32(Some(7)), ScalarValue::Int32(Some(8))],
    )];
    // The scan reads only `p2`: `p1` is dropped although it precedes it.
    let merged: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("p2", DataType::Int32, false),
    ]));

    let batches = read_leaf(&configs, &merged).await;

    assert_eq!(
        i32_column(&batches, 1),
        vec![8, 8],
        "p2 reads its own value, not the value of the dropped p1"
    );
}

/// The same, with partition columns of different types: the value of one type
/// put into the other's column surfaces as a reader type error rather than a
/// wrong value.
#[tokio::test]
async fn append_only_leaf_keeps_mixed_type_partition_columns_aligned() {
    let dir = tempfile::tempdir().expect("tempdir").keep();

    let file_schema: SchemaRef =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(file_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])
            .expect("batch");
    let partition_cols = vec![
        Arc::new(Field::new("p1", DataType::Int32, false)) as FieldRef,
        Arc::new(Field::new("p2", DataType::Utf8, false)) as FieldRef,
    ];
    let configs = vec![file_config(
        &dir,
        "part-0.parquet",
        &batch,
        &partition_cols,
        vec![
            ScalarValue::Int32(Some(7)),
            ScalarValue::Utf8(Some("8".to_string())),
        ],
    )];
    let merged: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("p2", DataType::Utf8, false),
    ]));

    let batches = read_leaf(&configs, &merged).await;

    assert_eq!(
        str_column(&batches, 1),
        vec!["8".to_string(), "8".to_string()],
        "p2 reads its own value although p1 has another type"
    );
}
