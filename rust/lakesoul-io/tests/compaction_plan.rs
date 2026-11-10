// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Synthetic tests for the compaction planner and the vortex copy-through
//! writer.  No production data or identifiers are used here.

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use lakesoul_io::compaction::{
    CompactToOneOptions, CompactionMode, CompactionOptions, FileAction,
    compact_to_one_file, copy_vortex_ranges, plan_compaction, summarize_files,
};
use lakesoul_io::config::{
    LakeSoulIOConfigBuilder, OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES,
    OPTION_KEY_VORTEX_ROW_BLOCK_SIZE,
};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::writer::create_writer_with_io_config;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::TempDir;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::array::expr::stats::{Precision, Stat};
use vortex::array::scalar::ScalarValue;
use vortex::array::serde::SerializedArray;
use vortex::arrow::ArrowSessionExt;
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::LayoutRef;
use vortex::layout::layouts::flat::Flat;
use vortex::layout::layouts::zoned::Zoned;
use vortex::session::VortexSession;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn batch(ids: &[i64]) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(ids.iter().copied()));
    let payload: ArrayRef = Arc::new(StringArray::from_iter_values(
        ids.iter().map(|id| format!("payload-{id}")),
    ));
    RecordBatch::try_new(schema(), vec![id, payload]).expect("batch")
}

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn id_batch(ids: &[i64]) -> RecordBatch {
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(ids.iter().copied()));
    RecordBatch::try_new(id_schema(), vec![id]).expect("batch")
}

fn string_batch(keys: &[String]) -> RecordBatch {
    let id: ArrayRef = Arc::new(StringArray::from_iter_values(keys.iter().cloned()));
    let payload: ArrayRef = Arc::new(StringArray::from_iter_values(
        keys.iter().map(|key| format!("payload-{key}")),
    ));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(schema, vec![id, payload]).expect("batch")
}

fn write_parquet(path: &std::path::Path, ids: &[i64]) {
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema(), None).expect("parquet writer");
    writer.write(&batch(ids)).expect("write parquet");
    writer.close().expect("close parquet");
}

fn write_parquet_strings(path: &std::path::Path, keys: &[String]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&string_batch(keys)).expect("write parquet");
    writer.close().expect("close parquet");
}

async fn write_vortex(path: &std::path::Path, ids: &[i64]) {
    write_vortex_with_block(path, ids, None).await;
}

async fn write_vortex_single_column(
    path: &std::path::Path,
    ids: &[i64],
    block_rows: usize,
) {
    let config = LakeSoulIOConfigBuilder::new()
        .with_files(vec![path.to_string_lossy().into_owned()])
        .with_schema(id_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_physical_format(PhysicalFormat::Vortex)
        .with_batch_size(256)
        .with_option(OPTION_KEY_VORTEX_ROW_BLOCK_SIZE, block_rows)
        .with_option(OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES, 4096)
        .build();
    let mut writer = create_writer_with_io_config(config)
        .await
        .expect("vortex writer");
    writer
        .write_record_batch(id_batch(ids))
        .await
        .expect("write");
    writer.flush_and_close().await.expect("flush");
}

async fn write_vortex_with_block(
    path: &std::path::Path,
    ids: &[i64],
    block_rows: Option<usize>,
) {
    let mut builder = LakeSoulIOConfigBuilder::new()
        .with_files(vec![path.to_string_lossy().into_owned()])
        .with_schema(schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_physical_format(PhysicalFormat::Vortex)
        .with_batch_size(256);
    if let Some(rows) = block_rows {
        builder = builder
            .with_option(OPTION_KEY_VORTEX_ROW_BLOCK_SIZE, rows)
            // Keep roughly one physical block per row block so the test can
            // copy at exact block boundaries.
            .with_option(OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES, 4096);
    }
    let config = builder.build();
    let mut writer = create_writer_with_io_config(config)
        .await
        .expect("vortex writer");
    writer.write_record_batch(batch(ids)).await.expect("write");
    writer.flush_and_close().await.expect("flush");
}

async fn read_vortex_ids(path: &str) -> Vec<i64> {
    let session = VortexSession::default().with_tokio();
    let file = session
        .open_options()
        .open_path(path)
        .await
        .expect("open vortex");
    let mut stream = file
        .scan()
        .expect("scan")
        .with_ordered(true)
        .into_array_stream()
        .expect("stream");
    let mut ids = Vec::new();
    let mut ctx = session.create_execution_ctx();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk");
        let arrow = session
            .arrow()
            .execute_arrow(chunk, None, &mut ctx)
            .expect("arrow");
        let batch = RecordBatch::from(arrow.as_struct().clone());
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id type");
        ids.extend(column.values().iter().copied());
    }
    ids
}

fn options() -> CompactionOptions {
    CompactionOptions::new(vec!["id".to_string()])
}

#[tokio::test]
async fn parquet_nested_files_select_file_level() {
    let dir = TempDir::new().expect("tempdir");
    let old = dir.path().join("old.parquet");
    let new = dir.path().join("new.parquet");
    write_parquet(&old, &(0..1_000).collect::<Vec<_>>());
    write_parquet(&new, &(0..2_000).collect::<Vec<_>>());

    let paths = vec![
        old.to_string_lossy().into_owned(),
        new.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::FileLevel, "{}", plan.reason);
    assert_eq!(plan.stats.winner_rows, 2_000);
    assert_eq!(plan.stats.discard_files, 1);
    assert_eq!(plan.files[0].action, FileAction::Discard);
    assert_eq!(plan.files[1].action, FileAction::Keep);
}

#[tokio::test]
async fn parquet_disjoint_ranges_report_copyable_but_fall_back_for_parquet() {
    let dir = TempDir::new().expect("tempdir");
    let left = dir.path().join("left.parquet");
    let right = dir.path().join("right.parquet");
    write_parquet(&left, &(0..1_000).collect::<Vec<_>>());
    write_parquet(&right, &(1_000..2_000).collect::<Vec<_>>());

    let paths = vec![
        left.to_string_lossy().into_owned(),
        right.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    // The layout is fully copyable, but parquet block copy is not implemented
    // in this phase, so the planner must fall back.
    assert_eq!(plan.mode, CompactionMode::Fallback, "{}", plan.reason);
    assert_eq!(plan.stats.copy_rows, 2_000);
    assert!((plan.stats.copy_ratio - 1.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn hash_like_ranges_select_fallback() {
    let dir = TempDir::new().expect("tempdir");
    let left = dir.path().join("left.parquet");
    let right = dir.path().join("right.parquet");
    let keys = |seed: u64| {
        (0..2_000)
            .map(|index| format!("{:032x}", splitmix64(seed.wrapping_add(index))))
            .collect::<Vec<_>>()
    };
    let mut left_keys = keys(0);
    let mut right_keys = keys(1_000_000);
    left_keys.sort();
    right_keys.sort();
    write_parquet_strings(&left, &left_keys);
    write_parquet_strings(&right, &right_keys);

    let paths = vec![
        left.to_string_lossy().into_owned(),
        right.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::Fallback, "{}", plan.reason);
    assert!(plan.stats.copy_ratio < 0.5);
    // The metadata guard may or may not classify two near-full ranges as
    // hash-like; the PK merge confirms the fallback either way.
    assert!(plan.stats.duplicate_ratio >= 0.0);
}

#[tokio::test]
async fn parquet_internal_duplicates_keep_only_the_last_version() {
    let dir = TempDir::new().expect("tempdir");
    let old = dir.path().join("old.parquet");
    let new = dir.path().join("new.parquet");
    // Duplicate keys inside one file are adjacent (the writer sorts); only the
    // last occurrence may win.
    write_parquet(&old, &[0, 0, 1, 2]);
    write_parquet(&new, &[0, 1, 2, 3]);

    let paths = vec![
        old.to_string_lossy().into_owned(),
        new.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::FileLevel, "{}", plan.reason);
    assert_eq!(plan.files[0].action, FileAction::Discard);
    assert_eq!(plan.files[0].winner_rows, 0);
    assert_eq!(plan.files[1].winner_rows, 4);
    assert!((plan.stats.duplicate_ratio - 0.5).abs() < f64::EPSILON);
}

#[tokio::test]
async fn vortex_disjoint_files_copy_blocks_in_order() {
    let dir = TempDir::new().expect("tempdir");
    let left = dir.path().join("left.vortex");
    let right = dir.path().join("right.vortex");
    write_vortex(&left, &(0..1_000).collect::<Vec<_>>()).await;
    write_vortex(&right, &(1_000..2_000).collect::<Vec<_>>()).await;

    let paths = vec![
        left.to_string_lossy().into_owned(),
        right.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::BlockLevel, "{}", plan.reason);
    assert_eq!(plan.stats.copy_rows, 2_000);
    let block_plan = plan.block_plan.as_ref().expect("block plan");
    assert!(!block_plan.ranges.is_empty());
    let all_files = plan
        .files
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();

    let output = dir.path().join("out.vortex");
    copy_vortex_ranges(&all_files, &block_plan.ranges, &output.to_string_lossy())
        .await
        .expect("copy");
    let ids = read_vortex_ids(&output.to_string_lossy()).await;
    assert_eq!(ids, (0..2_000).collect::<Vec<_>>());
}

#[tokio::test]
async fn vortex_block_aligned_overlap_copies_mixed_file_ranges() {
    let dir = TempDir::new().expect("tempdir");
    let base = dir.path().join("base.vortex");
    let update = dir.path().join("update.vortex");
    // One column keeps the per-column block boundaries identical.  The older
    // file has six 512-row blocks; the newer file replaces the first two
    // blocks exactly, so the surviving output alternates source files at
    // block boundaries.
    write_vortex_single_column(&base, &(0..3_072).collect::<Vec<_>>(), 512).await;
    write_vortex_single_column(&update, &(0..1_024).collect::<Vec<_>>(), 512).await;

    let paths = vec![
        base.to_string_lossy().into_owned(),
        update.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    assert_eq!(
        summary.files[0].blocks,
        (0..=6).map(|index| index * 512).collect::<Vec<_>>()
    );
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::BlockLevel, "{}", plan.reason);
    let block_plan = plan.block_plan.as_ref().expect("block plan");
    assert_eq!(block_plan.copy_rows, 3_072);
    assert!(block_plan.ranges.len() >= 2, "expected mixed ranges");

    let output = dir.path().join("out.vortex");
    copy_vortex_ranges(&paths, &block_plan.ranges, &output.to_string_lossy())
        .await
        .expect("copy");
    let ids = read_vortex_ids(&output.to_string_lossy()).await;
    assert_eq!(ids, (0..3_072).collect::<Vec<_>>());
}

#[tokio::test]
async fn vortex_overlapping_ranges_fall_back_when_blocks_are_partial() {
    let dir = TempDir::new().expect("tempdir");
    let left = dir.path().join("left.vortex");
    let right = dir.path().join("right.vortex");
    write_vortex(&left, &(0..1_000).collect::<Vec<_>>()).await;
    write_vortex(&right, &(500..1_500).collect::<Vec<_>>()).await;

    let paths = vec![
        left.to_string_lossy().into_owned(),
        right.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::Fallback, "{}", plan.reason);
    // One file survives whole while the other is cut by the overlap, so the
    // block-copy plan is partial.  Phase 1 only accepts fully copyable plans
    // and must fall back here.
    assert!(plan.stats.copy_rows > 0);
    assert!(plan.stats.copy_rows < plan.stats.winner_rows);
    assert!(plan.stats.duplicate_ratio > 0.0);
}

#[tokio::test]
async fn single_file_is_keep_only() {
    let dir = TempDir::new().expect("tempdir");
    let file = dir.path().join("one.parquet");
    write_parquet(&file, &(0..100).collect::<Vec<_>>());
    let paths = vec![file.to_string_lossy().into_owned()];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::KeepOnly, "{}", plan.reason);
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

async fn read_vortex_id_payload(path: &str) -> Vec<(i64, String)> {
    let session = VortexSession::default().with_tokio();
    let file = session
        .open_options()
        .open_path(path)
        .await
        .expect("open vortex");
    let mut stream = file
        .scan()
        .expect("scan")
        .with_ordered(true)
        .into_array_stream()
        .expect("stream");
    let mut rows = Vec::new();
    let mut ctx = session.create_execution_ctx();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("chunk");
        let arrow = session
            .arrow()
            .execute_arrow(chunk, None, &mut ctx)
            .expect("arrow");
        let batch = RecordBatch::from(arrow.as_struct().clone());
        let ids = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id type");
        let payloads = batch.column_by_name("payload").expect("payload column");
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                arrow::util::display::array_value_to_string(payloads.as_ref(), row)
                    .expect("payload value"),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn vortex_raw_block_copy_keeps_dict_payloads_on_partial_ranges() {
    let dir = TempDir::new().expect("tempdir");
    let base = dir.path().join("base.vortex");
    let update = dir.path().join("update.vortex");
    // The older file has six 512-row blocks.  The newer file replaces the
    // first two blocks, so the output copies partial chunks from the older
    // file and must still preserve its dictionary-encoded payloads.
    write_vortex_with_block(&base, &(0..3_072).collect::<Vec<_>>(), Some(512)).await;
    write_vortex_with_block(&update, &(0..1_024).collect::<Vec<_>>(), Some(512)).await;

    let paths = vec![
        base.to_string_lossy().into_owned(),
        update.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::BlockLevel, "{}", plan.reason);
    let block_plan = plan.block_plan.as_ref().expect("block plan");
    assert!(block_plan.ranges.len() >= 2, "expected mixed ranges");
    assert!(
        block_plan.ranges.iter().any(|range| range.start_row > 0),
        "expected a partial source range"
    );

    let output = dir.path().join("out.vortex");
    copy_vortex_ranges(&paths, &block_plan.ranges, &output.to_string_lossy())
        .await
        .expect("copy");
    let rows = read_vortex_id_payload(&output.to_string_lossy()).await;
    assert_eq!(rows.len(), 3_072);
    for (index, (id, payload)) in rows.iter().enumerate() {
        assert_eq!(*id, index as i64);
        assert_eq!(payload, &format!("payload-{id}"), "row {index}");
    }
}

#[tokio::test]
async fn vortex_raw_copy_reuses_whole_file_layout_and_statistics() {
    let dir = TempDir::new().expect("tempdir");
    let input = dir.path().join("input.vortex");
    write_vortex_with_block(&input, &(0..1_024).collect::<Vec<_>>(), Some(256)).await;
    let path = input.to_string_lossy().into_owned();
    let output = dir.path().join("out.vortex");
    let ranges = vec![lakesoul_io::compaction::BlockRange {
        file: 0,
        start_row: 0,
        end_row: 1_024,
    }];
    copy_vortex_ranges(
        std::slice::from_ref(&path),
        &ranges,
        &output.to_string_lossy(),
    )
    .await
    .expect("copy");
    let rows = read_vortex_id_payload(&output.to_string_lossy()).await;
    assert_eq!(rows.len(), 1_024);
    for (index, (id, payload)) in rows.iter().enumerate() {
        assert_eq!(*id, index as i64);
        assert_eq!(payload, &format!("payload-{id}"));
    }
    let session = VortexSession::default().with_tokio();
    let file = session
        .open_options()
        .open_path(&output)
        .await
        .expect("open output");
    assert!(file.footer().statistics().is_some());
    assert!(std::fs::metadata(&output).expect("metadata").len() > 0);
}

async fn read_zone_stats(
    file: &VortexFile,
    zones: &LayoutRef,
    session: &VortexSession,
) -> RecordBatch {
    let flat = zones.as_opt::<Flat>().expect("flat zone table");
    let handle = file
        .segment_source()
        .request(flat.segment_id())
        .await
        .expect("zone segment");
    let serialized = SerializedArray::try_from(handle).expect("serialized zone table");
    let array = serialized
        .decode(
            flat.dtype(),
            zones.row_count() as usize,
            flat.array_ctx(),
            session,
        )
        .expect("decode zone table");
    let mut ctx = session.create_execution_ctx();
    let arrow = session
        .arrow()
        .execute_arrow(array, None, &mut ctx)
        .expect("zone table to arrow");
    RecordBatch::from(arrow.as_struct().clone())
}

#[tokio::test]
async fn vortex_raw_copy_preserves_partial_zone_maps_and_file_stats() {
    let dir = TempDir::new().expect("tempdir");
    let base = dir.path().join("base.vortex");
    let update = dir.path().join("update.vortex");
    write_vortex_with_block(&base, &(0..3_072).collect::<Vec<_>>(), Some(512)).await;
    write_vortex_with_block(&update, &(0..1_024).collect::<Vec<_>>(), Some(512)).await;

    let paths = vec![
        base.to_string_lossy().into_owned(),
        update.to_string_lossy().into_owned(),
    ];
    let summary = summarize_files(&paths, &["id".to_string()])
        .await
        .expect("summary");
    let plan = plan_compaction(&summary, &options()).await.expect("plan");
    assert_eq!(plan.mode, CompactionMode::BlockLevel, "{}", plan.reason);
    let block_plan = plan.block_plan.as_ref().expect("block plan");
    assert!(block_plan.ranges.iter().any(|range| range.start_row > 0));
    let output = dir.path().join("out.vortex");
    copy_vortex_ranges(&paths, &block_plan.ranges, &output.to_string_lossy())
        .await
        .expect("copy");

    let session = VortexSession::default().with_tokio();
    let file = session
        .open_options()
        .open_path(&output)
        .await
        .expect("open output");
    let root = file.footer().layout();
    let fields = root.children().expect("root fields");
    let id_ranges = fields[0].children().expect("id ranges");
    assert_eq!(id_ranges.len(), 2, "update + partial base");
    for (index, range) in id_ranges.iter().enumerate() {
        let zoned = range.as_opt::<Zoned>().expect("zoned range");
        assert_eq!(zoned.zone_len(), 512);
        let expected_zones = if index == 0 { 2 } else { 4 };
        assert_eq!(zoned.nzones(), expected_zones);
        let range_children = range.children().expect("zoned children");
        let zones = read_zone_stats(&file, &range_children[1], &session).await;
        let mins = zones
            .column_by_name("vortex.min()")
            .expect("zone min")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 zone min");
        let maxes = zones
            .column_by_name("vortex.max()")
            .expect("zone max")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 zone max");
        if index == 0 {
            assert_eq!(mins.values(), &[0, 512]);
            assert_eq!(maxes.values(), &[511, 1_023]);
        } else {
            assert_eq!(mins.values(), &[1_024, 1_536, 2_048, 2_560]);
            assert_eq!(maxes.values(), &[1_535, 2_047, 2_559, 3_071]);
        }
    }

    let statistics = file.footer().statistics().expect("file statistics");
    let id_stats = &statistics.stats_sets()[0];
    assert_eq!(
        id_stats.get(Stat::Min),
        Precision::Exact(ScalarValue::from(0i64))
    );
    assert_eq!(
        id_stats.get(Stat::Max),
        Precision::Exact(ScalarValue::from(3_071i64))
    );
    assert_eq!(
        id_stats.get(Stat::NullCount),
        Precision::Exact(ScalarValue::from(0u64))
    );
    assert_eq!(id_stats.get(Stat::Sum), Precision::Absent);
    let payload_stats = &statistics.stats_sets()[1];
    assert!(matches!(
        payload_stats.get(Stat::Min),
        Precision::Inexact(_)
    ));
    assert!(matches!(
        payload_stats.get(Stat::Max),
        Precision::Inexact(_)
    ));
    assert_eq!(
        payload_stats.get(Stat::NullCount),
        Precision::Exact(ScalarValue::from(0u64))
    );
}

#[tokio::test]
async fn vortex_raw_copy_single_partial_range_keeps_zone_map_and_stats() {
    let dir = TempDir::new().expect("tempdir");
    let input = dir.path().join("input.vortex");
    write_vortex_with_block(&input, &(0..3_072).collect::<Vec<_>>(), Some(512)).await;
    let path = input.to_string_lossy().into_owned();
    let output = dir.path().join("out.vortex");
    let ranges = vec![lakesoul_io::compaction::BlockRange {
        file: 0,
        start_row: 1_024,
        end_row: 3_072,
    }];
    copy_vortex_ranges(
        std::slice::from_ref(&path),
        &ranges,
        &output.to_string_lossy(),
    )
    .await
    .expect("copy");

    let rows = read_vortex_ids(&output.to_string_lossy()).await;
    assert_eq!(rows, (1_024..3_072).collect::<Vec<_>>());

    let session = VortexSession::default().with_tokio();
    let file = session
        .open_options()
        .open_path(&output)
        .await
        .expect("open output");
    let root = file.footer().layout();
    let fields = root.children().expect("root fields");
    let zoned = fields[0].as_opt::<Zoned>().expect("zoned partial field");
    assert_eq!(zoned.zone_len(), 512);
    assert_eq!(zoned.nzones(), 4);
    let zoned_children = fields[0].children().expect("zoned children");
    let zones = read_zone_stats(&file, &zoned_children[1], &session).await;
    let mins = zones
        .column_by_name("vortex.min()")
        .expect("zone min")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 zone min");
    let maxes = zones
        .column_by_name("vortex.max()")
        .expect("zone max")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 zone max");
    assert_eq!(mins.values(), &[1_024, 1_536, 2_048, 2_560]);
    assert_eq!(maxes.values(), &[1_535, 2_047, 2_559, 3_071]);

    let statistics = file.footer().statistics().expect("file statistics");
    let id_stats = &statistics.stats_sets()[0];
    assert_eq!(
        id_stats.get(Stat::Min),
        Precision::Exact(ScalarValue::from(1_024i64))
    );
    assert_eq!(
        id_stats.get(Stat::Max),
        Precision::Exact(ScalarValue::from(3_071i64))
    );
}

fn read_parquet_ids(path: &std::path::Path) -> Vec<i64> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).expect("open parquet output"),
    )
    .expect("parquet footer");
    let reader = builder
        .with_batch_size(1024)
        .build()
        .expect("parquet reader");
    let mut ids = Vec::new();
    for batch in reader {
        let batch = batch.expect("parquet batch");
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id type");
        ids.extend(column.values().iter().copied());
    }
    ids
}

#[tokio::test]
async fn compact_to_one_file_uses_block_copy_for_aligned_vortex() {
    let dir = TempDir::new().expect("tempdir");
    let base = dir.path().join("base.vortex");
    let update = dir.path().join("update.vortex");
    write_vortex_with_block(&base, &(0..3_072).collect::<Vec<_>>(), Some(512)).await;
    write_vortex_with_block(&update, &(0..1_024).collect::<Vec<_>>(), Some(512)).await;
    let paths = vec![
        base.to_string_lossy().into_owned(),
        update.to_string_lossy().into_owned(),
    ];
    let output = dir.path().join("out.vortex");
    let options = CompactToOneOptions::new(vec!["id".to_string()]);
    let result = compact_to_one_file(&paths, &output.to_string_lossy(), &options)
        .await
        .expect("compact");
    assert_eq!(result.mode, CompactionMode::BlockLevel);
    assert_eq!(result.input_files, 2);
    assert_eq!(result.output_rows, 3_072);
    assert!(result.output_bytes > 0);
    assert_eq!(
        read_vortex_ids(&output.to_string_lossy()).await,
        (0..3_072).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn compact_to_one_file_copies_a_fully_surviving_parquet_file() {
    let dir = TempDir::new().expect("tempdir");
    let old = dir.path().join("old.parquet");
    let new = dir.path().join("new.parquet");
    write_parquet(&old, &(0..1_000).collect::<Vec<_>>());
    write_parquet(&new, &(0..2_000).collect::<Vec<_>>());
    let paths = vec![
        old.to_string_lossy().into_owned(),
        new.to_string_lossy().into_owned(),
    ];
    let output = dir.path().join("out.parquet");
    let options = CompactToOneOptions::new(vec!["id".to_string()]);
    let result = compact_to_one_file(&paths, &output.to_string_lossy(), &options)
        .await
        .expect("compact");
    assert_eq!(result.mode, CompactionMode::FileLevel);
    assert_eq!(result.output_rows, 2_000);
    assert_eq!(read_parquet_ids(&output), (0..2_000).collect::<Vec<_>>());
}

#[tokio::test]
async fn compact_to_one_file_merges_when_output_format_differs() {
    let dir = TempDir::new().expect("tempdir");
    let old = dir.path().join("old.parquet");
    let new = dir.path().join("new.parquet");
    write_parquet(&old, &(0..1_000).collect::<Vec<_>>());
    write_parquet(&new, &(0..2_000).collect::<Vec<_>>());
    let paths = vec![
        old.to_string_lossy().into_owned(),
        new.to_string_lossy().into_owned(),
    ];
    let output = dir.path().join("out.vortex");
    let options = CompactToOneOptions::new(vec!["id".to_string()]);
    let result = compact_to_one_file(&paths, &output.to_string_lossy(), &options)
        .await
        .expect("compact");
    assert_eq!(result.mode, CompactionMode::Fallback);
    assert!(result.fallback_reason.is_some());
    assert_eq!(result.output_rows, 2_000);
    assert_eq!(
        read_vortex_ids(&output.to_string_lossy()).await,
        (0..2_000).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn compact_to_one_file_deduplicates_a_single_input() {
    let dir = TempDir::new().expect("tempdir");
    let input = dir.path().join("input.parquet");
    write_parquet(&input, &[0, 0, 1, 2]);
    let paths = vec![input.to_string_lossy().into_owned()];
    let output = dir.path().join("out.vortex");
    let options = CompactToOneOptions::new(vec!["id".to_string()]);
    let result = compact_to_one_file(&paths, &output.to_string_lossy(), &options)
        .await
        .expect("compact");
    assert_eq!(result.mode, CompactionMode::Fallback);
    assert_eq!(result.output_rows, 3);
    assert_eq!(
        read_vortex_ids(&output.to_string_lossy()).await,
        vec![0, 1, 2]
    );
}
