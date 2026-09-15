// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Diagnostic probe (ignored): print the per-chunk layout/encoding structure of
//! a written vortex file for one column, including the encoding tree, serialized
//! (compressed) buffer bytes, and decode timings.
//!
//! Usage:
//!   PROBE_FILE=/path/to/file.vortex PROBE_FIELD=vec \
//!     cargo test -p lakesoul-io --test vortex_chunk_probe -- --ignored --nocapture

use std::sync::Arc;
use std::time::Instant;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use vortex::VortexSessionDefault;
use vortex::array::memory::MemorySessionExt;
use vortex::array::serde::SerializedArray;
use vortex::array::{ArrayRef, Canonical, IntoArray, VortexSessionExecute};
use vortex::buffer::ByteBuffer;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::LayoutRef;
use vortex::layout::display::DisplayLayoutTree;
use vortex::layout::layouts::flat::Flat;
use vortex::layout::segments::SegmentId;
use vortex::session::VortexSession;
use vortex::session::registry::ReadContext;

struct Chunk {
    index: usize,
    start: u64,
    rows: u64,
    segment_id: SegmentId,
    array_tree: Option<ByteBuffer>,
    dtype: vortex::dtype::DType,
    array_ctx: ReadContext,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "diagnostic probe; run against a real vortex file"]
async fn probe_chunk_encodings() {
    let file_path =
        std::fs::canonicalize(std::env::var("PROBE_FILE").expect("set PROBE_FILE"))
            .expect("PROBE_FILE must exist");
    let file_path = file_path.display().to_string();
    let field = std::env::var("PROBE_FIELD").unwrap_or_else(|_| "vec".to_string());
    let limit: usize = std::env::var("PROBE_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);

    let session = VortexSession::default().with_tokio();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix("/").unwrap());
    let location = StorePath::from_absolute_path(&file_path).unwrap();
    let reader = Arc::new(ObjectStoreReadAt::new_with_allocator(
        store,
        location,
        session.handle(),
        session.allocator(),
    ));
    let file = session.open_options().open_read(reader).await.unwrap();

    println!("\n── vortex chunk probe ──");
    println!("file: {file_path}");
    println!("rows: {}", file.footer().row_count());
    println!("dtype: {}", file.footer().dtype());
    if let Ok(sizes) = file.footer().compressed_field_sizes() {
        let mut fields: Vec<_> = sizes.iter().collect();
        fields.sort_by_key(|(path, _)| path.to_string());
        println!("compressed field sizes:");
        for (path, size) in fields {
            println!("  {path}: {size} B");
        }
    }

    let layout = Arc::clone(file.footer().layout());
    println!("\nlayout tree:\n{}", DisplayLayoutTree::new(layout, true));

    // Collect every Flat layout under the requested field.
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut path = Vec::new();
    collect_chunks(
        file.footer().layout(),
        &mut path,
        false,
        &field,
        0,
        &mut chunks,
    );
    println!("chunks under field '{field}': {}", chunks.len());

    let mut compressed_sizes: Vec<usize> = Vec::new();
    let mut raw_sizes: Vec<u64> = Vec::new();
    let mut decode_ms: Vec<f64> = Vec::new();
    let mut execute_ms: Vec<f64> = Vec::new();
    let mut encodings: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    // Raw payload bytes per row: the probe defaults to the GIST dimension;
    // override with PROBE_DIM for other datasets.
    let dim: u64 = std::env::var("PROBE_DIM")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(960);

    for chunk in &chunks {
        let segment = file
            .segment_source()
            .request(chunk.segment_id)
            .await
            .unwrap();
        let serialized = match &chunk.array_tree {
            Some(array_tree) => {
                SerializedArray::from_flatbuffer_and_segment(array_tree.clone(), segment)
                    .unwrap()
            }
            None => SerializedArray::try_from(segment).unwrap(),
        };
        let buffer_lengths = serialized.buffer_lengths();
        let compressed: usize = buffer_lengths.iter().sum();
        let raw = chunk.rows * dim * 4;
        compressed_sizes.push(compressed);
        raw_sizes.push(raw);

        let mut ctx = session.create_execution_ctx();
        let t = Instant::now();
        let array = serialized
            .decode(
                &chunk.dtype,
                chunk.rows as usize,
                &chunk.array_ctx,
                &session,
            )
            .unwrap();
        decode_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        let encoded_nbytes = array.nbytes();

        let t = Instant::now();
        let canonical = array.clone().execute::<Canonical>(&mut ctx).unwrap();
        execute_ms.push(t.elapsed().as_secs_f64() * 1000.0);
        let canonical_nbytes = canonical.into_array().nbytes();
        let _ = encoded_nbytes;

        if chunk.index < limit {
            println!(
                "\n── chunk #{index} rows={rows} range=[{start},{end}) segment={segment}",
                index = chunk.index,
                rows = chunk.rows,
                start = chunk.start,
                end = chunk.start + chunk.rows,
                segment = chunk.segment_id,
            );
            println!(
                "   compressed: {compressed} B  buffers={buffer_lengths:?}  raw(dim={dim}): {raw} B  ratio={ratio:.2}x",
                ratio = raw as f64 / compressed.max(1) as f64,
            );
            println!(
                "   decode={:.3}ms  canonicalize={:.3}ms  encoded_nbytes={encoded_nbytes} canonical_nbytes={canonical_nbytes}",
                decode_ms[chunk.index], execute_ms[chunk.index],
            );
            println!("   encoding tree:");
            let mut histogram = std::collections::HashMap::new();
            print_tree(&array, 2, &mut histogram);
            for (name, count) in &histogram {
                *encodings.entry(name.clone()).or_default() += count;
            }
        } else {
            let mut histogram = std::collections::HashMap::new();
            count_tree(&array, &mut histogram);
            for (name, count) in &histogram {
                *encodings.entry(name.clone()).or_default() += count;
            }
        }
    }

    let total_compressed: usize = compressed_sizes.iter().sum();
    let total_raw: u64 = raw_sizes.iter().sum();
    let min_compressed = compressed_sizes.iter().min().copied().unwrap_or(0);
    let max_compressed = compressed_sizes.iter().max().copied().unwrap_or(0);
    compressed_sizes.sort_unstable();
    let median_compressed = compressed_sizes
        .get(compressed_sizes.len() / 2)
        .copied()
        .unwrap_or(0);
    println!("\n── summary ──");
    println!(
        "chunks={} rows/chunk~{} compressed total={} B raw total={} B ratio={:.2}x",
        chunks.len(),
        chunks.first().map(|c| c.rows).unwrap_or(0),
        total_compressed,
        total_raw,
        total_raw as f64 / total_compressed.max(1) as f64,
    );
    println!(
        "chunk compressed bytes: min={min_compressed} median={median_compressed} max={max_compressed}",
    );
    println!(
        "per-chunk decode median={:.3}ms  canonicalize median={:.3}ms",
        median_f64(&mut decode_ms),
        median_f64(&mut execute_ms),
    );
    let mut encodings: Vec<_> = encodings.into_iter().collect();
    encodings.sort_by(|a, b| b.1.cmp(&a.1));
    println!("encoding histogram (nodes over all chunks):");
    for (name, count) in encodings {
        println!("  {name}: {count}");
    }
}

fn median_f64(values: &mut Vec<f64>) -> f64 {
    values.sort_by(|a, b| a.total_cmp(b));
    values.get(values.len() / 2).copied().unwrap_or(0.0)
}

/// Depth-first walk collecting `Flat` layouts under `field`.
fn collect_chunks(
    layout: &LayoutRef,
    path: &mut Vec<String>,
    in_field: bool,
    field: &str,
    offset: u64,
    chunks: &mut Vec<Chunk>,
) {
    let children = layout.children().unwrap_or_default();
    let names: Vec<String> = layout.child_names().map(|n| n.to_string()).collect();
    let encoding = layout.encoding_id().to_string();
    let is_struct = encoding.contains("struct");

    if children.is_empty() {
        if in_field && let Some(flat) = layout.as_opt::<Flat>() {
            chunks.push(Chunk {
                index: chunks.len(),
                start: offset,
                rows: layout.row_count(),
                segment_id: flat.segment_id(),
                array_tree: flat.array_tree().cloned(),
                dtype: layout.dtype().clone(),
                array_ctx: flat.array_ctx().clone(),
            });
        }
        return;
    }

    let mut next = offset;
    for (i, child) in children.into_iter().enumerate() {
        let name = names.get(i).cloned().unwrap_or_default();
        path.push(name.clone());
        let child_in_field = in_field || name == field;
        if is_struct {
            // Struct children are columns: each spans the same row range.
            collect_chunks(&child, path, child_in_field, field, offset, chunks);
        } else {
            collect_chunks(&child, path, child_in_field, field, next, chunks);
            next += child.row_count();
        }
        path.pop();
    }
}

fn count_tree(
    array: &ArrayRef,
    histogram: &mut std::collections::HashMap<String, usize>,
) {
    *histogram
        .entry(array.encoding_id().to_string())
        .or_default() += 1;
    for child in array.children() {
        count_tree(&child, histogram);
    }
}

fn print_tree(
    array: &ArrayRef,
    indent: usize,
    histogram: &mut std::collections::HashMap<String, usize>,
) {
    *histogram
        .entry(array.encoding_id().to_string())
        .or_default() += 1;
    println!(
        "{:indent$}{} len={} nbytes={}",
        "",
        array.encoding_id(),
        array.len(),
        array.nbytes(),
        indent = indent,
    );
    let names = array.children_names();
    for (i, child) in array.children().into_iter().enumerate() {
        let name = names.get(i).cloned().unwrap_or_default();
        if !name.is_empty() {
            println!("{:indent$}  [{name}]", "", indent = indent);
        }
        print_tree(&child, indent + 2, histogram);
    }
}
