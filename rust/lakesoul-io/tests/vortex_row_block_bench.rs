// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! IO-cost bench for random candidate reads from vortex files (ignored; run
//! manually):
//!
//! - the vortex write layout: small LakeSoul row blocks (1024 rows, 128 KiB
//!   data blocks) vs. the writer default (8192 rows, 1 MiB);
//! - the deferred payload read: candidates are read as `id + vec` first, the
//!   payload only for the rows the limit keeps, instead of every candidate.
//!
//! Bytes are counted by the monitored object store, so they are the logical
//! bytes fetched, not wall-clock or page-cache dependent.
//!
//! ```sh
//! cargo test -p lakesoul-io --test vortex_row_block_bench -- --ignored --nocapture
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, FixedSizeListBuilder, Float32Builder, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use lakesoul_io::config::{
    LakeSoulIOConfigBuilder, OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES,
    OPTION_KEY_VORTEX_ROW_BLOCK_SIZE,
};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::object_store_metrics::MonitoredObjectStore;
use lakesoul_io::writer::create_writer_with_io_config;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use vortex::VortexSessionDefault;
use vortex::array::memory::MemorySessionExt;
use vortex::array::stream::ArrayStreamExt;
use vortex::expr::{BoundExpression, Expression, root};
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex::session::VortexSession;

const ROWS: i64 = 200_000;
const DIM: usize = 128;
const PAYLOAD_BYTES: usize = 512;
/// Candidates a vector search scans (top_k 10 × overfetch 10 × 4 buckets).
const CANDIDATES: usize = 200;
/// Rows the limit keeps (`top_k * 2 + 16` as in the deferred payload read).
const KEPT: usize = 36;
const REPS: usize = 5;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn batch() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS));
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), DIM as i32);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 33) as f32 / (1u64 << 31) as f32) * 2.0 - 1.0
    };
    for _ in 0..ROWS {
        for _ in 0..DIM {
            builder.values().append_value(next());
        }
        builder.append(true);
    }
    // Pseudo-random hex payload so it does not compress away.
    let payloads: ArrayRef =
        Arc::new(StringArray::from_iter_values((0..ROWS).map(|id| {
            let mut state = 0x0123_4567_89ab_cdefu64 ^ id as u64;
            let mut payload = String::with_capacity(PAYLOAD_BYTES);
            while payload.len() < PAYLOAD_BYTES {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                payload.push_str(&format!("{:016x}", state));
            }
            payload.truncate(PAYLOAD_BYTES);
            payload
        })));
    RecordBatch::try_new(schema(), vec![ids, Arc::new(builder.finish()), payloads])
        .unwrap()
}

async fn write_file(path: &str, options: &[(&str, &str)]) {
    let batch = batch();
    let mut builder = LakeSoulIOConfigBuilder::new()
        .with_files(vec![path.to_string()])
        .with_primary_key("id".to_string())
        .with_schema(batch.schema())
        .with_batch_size(8192)
        .with_physical_format(PhysicalFormat::Vortex);
    for (key, value) in options {
        builder = builder.with_option(*key, *value);
    }
    let mut writer = create_writer_with_io_config(builder.build())
        .await
        .expect("writer creation failed");
    writer.write_record_batch(batch).await.unwrap();
    writer.flush_and_close().await.unwrap();
}

async fn open_file(path: &str) -> (VortexFile, VortexSession) {
    let session = VortexSession::default().with_tokio();
    let store: Arc<dyn ObjectStore> = Arc::new(MonitoredObjectStore::new(
        Arc::new(LocalFileSystem::new_with_prefix("/").unwrap()),
        "local",
    ));
    let location = StorePath::from_absolute_path(path).unwrap();
    let reader = Arc::new(ObjectStoreReadAt::new_with_allocator(
        store,
        location,
        session.handle(),
        session.allocator(),
    ));
    let file = session.open_options().open_read(reader).await.unwrap();
    (file, session)
}

/// Whole-row projection when `column` is `None`, else a single field.
fn projection(file: &VortexFile, column: Option<&str>) -> BoundExpression {
    let expr: Expression = match column {
        Some(column) => vortex::expr::get_item(column.to_string(), root()),
        None => root(),
    };
    expr.bind(file.dtype()).unwrap()
}

/// Deterministic scattered row indices.
fn scattered(count: usize, spread: usize) -> Vec<u64> {
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut indices: std::collections::HashSet<u64> = std::collections::HashSet::new();
    while indices.len() < count {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        indices.insert(((state >> 33) as usize % spread) as u64);
    }
    let mut indices: Vec<u64> = indices.into_iter().collect();
    indices.sort_unstable();
    indices
}

fn read_counters(handle: &PrometheusHandle) -> (f64, f64) {
    let rendered = handle.render();
    let mut bytes = 0.0;
    let mut requests = 0.0;
    for line in rendered.lines() {
        let value = || {
            line.rsplit_once(' ')
                .and_then(|(_, value)| value.parse::<f64>().ok())
                .unwrap_or_default()
        };
        if line.starts_with("lakesoul_object_store_bytes_total{") {
            bytes += value();
        } else if line.starts_with("lakesoul_object_store_requests_total{") {
            requests += value();
        }
    }
    (bytes, requests)
}

struct Measurement {
    bytes: f64,
    requests: f64,
    mean_ms: f64,
}

async fn measure(
    file: &VortexFile,
    session: &VortexSession,
    column: Option<&str>,
    indices: &[u64],
    handle: &PrometheusHandle,
) -> Measurement {
    let indices = StrictSortedBuffer::try_new(vortex::buffer::Buffer::from_iter(
        indices.iter().copied(),
    ))
    .unwrap();
    async fn read(
        file: &VortexFile,
        _session: &VortexSession,
        column: Option<&str>,
        indices: &StrictSortedBuffer<u64>,
    ) -> usize {
        let array = file
            .scan()
            .unwrap()
            .with_projection(projection(file, column))
            .with_row_indices(indices.clone())
            .into_array_stream()
            .unwrap()
            .read_all()
            .await
            .unwrap();
        array.len()
    }

    // Warm up file metadata / page cache-independent state once.
    read(file, session, column, &indices).await;
    let (bytes_before, requests_before) = read_counters(handle);
    let started = Instant::now();
    for _ in 0..REPS {
        let rows = read(file, session, column, &indices).await;
        assert!(rows > 0);
    }
    let elapsed = started.elapsed().as_secs_f64() * 1000.0 / REPS as f64;
    let (bytes_after, requests_after) = read_counters(handle);
    Measurement {
        bytes: (bytes_after - bytes_before) / REPS as f64,
        requests: (requests_after - requests_before) / REPS as f64,
        mean_ms: elapsed,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "manual IO bench; run with --ignored --nocapture"]
async fn vortex_row_block_bench() {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("install global metrics recorder");
    let temp_dir = tempfile::tempdir().unwrap();
    let layouts: [(&str, &[(&str, &str)]); 2] = [
        ("small-blocks", &[]),
        (
            "writer-default",
            &[
                (OPTION_KEY_VORTEX_ROW_BLOCK_SIZE, "8192"),
                (OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES, "1048576"),
            ],
        ),
    ];

    let candidates = scattered(CANDIDATES, ROWS as usize);
    let kept = scattered(KEPT, ROWS as usize);
    println!(
        "\n{ROWS} rows, {DIM}-dim f32 vectors (1 KiB/row), {PAYLOAD_BYTES}B payload/row, \
         {CANDIDATES} candidates, {KEPT} kept"
    );
    println!(
        "{:>15} {:>24} {:>12} {:>9} {:>10}",
        "layout", "read", "bytes", "requests", "mean ms"
    );
    let mut summary: Vec<(String, &str, Measurement)> = Vec::new();
    for (layout, options) in layouts {
        let path = temp_dir.path().join(format!("{layout}.vortex"));
        let path = path.display().to_string();
        write_file(&path, options).await;
        let size = std::fs::metadata(&path)
            .map(|m| m.len())
            .unwrap_or_default();
        let (file, session) = open_file(&path).await;
        for (label, column, rows) in [
            ("candidates all", None, &candidates),
            ("candidates id", Some("id"), &candidates),
            ("candidates vec", Some("vec"), &candidates),
            ("kept payload", Some("payload"), &kept),
        ] {
            let measurement = measure(&file, &session, column, rows, &handle).await;
            println!(
                "{layout:>15} {label:>24} {:>12.0} {:>9.0} {:>10.3}",
                measurement.bytes, measurement.requests, measurement.mean_ms
            );
            summary.push((layout.to_string(), label, measurement));
        }
        println!("  (file {size} bytes)");
    }

    println!("\ndeferred payload vs single pass (logical bytes per query):");
    for layout in ["small-blocks", "writer-default"] {
        let bytes_of = |label: &str| {
            summary
                .iter()
                .find(|(l, row, _)| l == layout && *row == label)
                .map(|(_, _, m)| m.bytes)
                .unwrap()
        };
        let single = bytes_of("candidates all");
        let keys = bytes_of("candidates id") + bytes_of("candidates vec");
        let payload = bytes_of("kept payload");
        println!(
            "{layout:>15}: single {single:>12.0} B   deferred {:>12.0} B \
             ({:>5.1}% of single, {:>10.0} B saved)",
            keys + payload,
            (keys + payload) / single * 100.0,
            single - (keys + payload)
        );
    }
}
