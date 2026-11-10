// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Copy-through helpers for the compaction path.
//!
//! Block-aligned ranges are copied by [`crate::compaction::raw_copy`], which
//! copies encoded segments without decoding.  The array-level writer below is
//! still used for whole-file concatenation when raw copy cannot combine the
//! inputs; it serializes already-encoded arrays and never re-runs the
//! BtrBlocks compressor.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use rootcause::report;
use vortex::array::ArrayRef;
use vortex::array::arrays::Shared;
use vortex::array::arrays::Slice;
use vortex::array::arrays::shared::SharedArrayExt;
use vortex::array::arrays::slice::SliceArraySlotsExt;
use vortex::array::serde::SerializeOptions;
use vortex::array::stream::ArrayStream;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::dtype::{DType, FieldPath};
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex::io::session::RuntimeSessionExt;
use vortex::io::std_file::FileWrite;
use vortex::layout::LayoutRef;
use vortex::layout::layouts::chunked::ChunkedLayout;
use vortex::layout::layouts::flat::FlatLayout;
use vortex::layout::scan::split_by::SplitBy;
use vortex::layout::segments::SegmentSinkRef;
use vortex::layout::sequence::{SendableSequentialStream, SequencePointer};
use vortex::layout::{LayoutStrategy, LayoutWriterContext, layout_children};
use vortex::session::VortexSession;
use vortex::session::registry::ReadContext;

use crate::Result;
use crate::compaction::plan::BlockRange;

/// A per-column layout strategy that writes every incoming chunk as one flat
/// encoded segment.
///
/// The root [`WriteStrategyBuilder`] (`TableStrategy`) splits the input struct
/// into columns and routes each column here.  No compression, repartition or
/// dictionary stage runs on this path.
struct CopyColumnStrategy;

#[async_trait]
impl LayoutStrategy for CopyColumnStrategy {
    async fn write_stream(
        &self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        mut stream: SendableSequentialStream,
        _eof: SequencePointer,
        session: &VortexSession,
    ) -> vortex::error::VortexResult<LayoutRef> {
        let dtype = stream.dtype().clone();
        let mut children = Vec::new();
        let mut row_count = 0u64;
        while let Some(item) = stream.next().await {
            let (sequence_id, chunk) = item?;
            let original_rows = chunk.len() as u64;
            if original_rows == 0 {
                continue;
            }
            let chunk = unwrap_copyable(chunk)?;
            let rows = chunk.len() as u64;
            if rows != original_rows {
                return Err(vortex::error::vortex_err!(
                    "unwrapped chunk has {rows} rows, expected {original_rows}"
                ));
            }
            let buffers = chunk
                .serialize(
                    ctx.array_ctx(),
                    session,
                    &SerializeOptions {
                        offset: 0,
                        include_padding: true,
                    },
                )
                .map_err(|error| {
                    error!(
                        column = %dtype,
                        rows = chunk.len(),
                        "copy-through serialize failed: {error}"
                    );
                    error
                })?;
            let segment_id =
                segment_sink
                    .write(sequence_id, buffers)
                    .await
                    .map_err(|error| {
                        error!(
                            column = %dtype,
                            rows = chunk.len(),
                            "copy-through segment write failed: {error}"
                        );
                        error
                    })?;
            let read_ctx = ReadContext::new(ctx.array_ctx().to_ids());
            children.push(
                FlatLayout::new_with_metadata(
                    rows,
                    dtype.clone(),
                    segment_id,
                    read_ctx,
                    None,
                )
                .into_layout(),
            );
            row_count += rows;
        }
        Ok(ChunkedLayout::new(row_count, dtype, layout_children(children)).into_layout())
    }
}

/// Remove lazy `Shared` wrappers and whole-child `Slice` wrappers before
/// serializing.  A partial slice cannot be copied without decoding its child,
/// so it is rejected and the caller falls back.
fn unwrap_copyable(chunk: ArrayRef) -> vortex::error::VortexResult<ArrayRef> {
    if let Some(shared) = chunk.as_opt::<Shared>() {
        return unwrap_copyable(shared.current_array_ref().clone());
    }
    if let Some(slice) = chunk.as_opt::<Slice>() {
        let range = slice.data().slice_range().clone();
        let child = slice.child().clone();
        if range.start == 0 && range.end == child.len() {
            return unwrap_copyable(child);
        }
        return Err(vortex::error::vortex_err!(
            "partial slice {:?} of a {} row chunk is not serializable",
            range,
            child.len()
        ));
    }
    Ok(chunk)
}

async fn write_vortex_bytes(
    session: &VortexSession,
    dtype: DType,
    array_stream: impl ArrayStream + Send + 'static,
    output: &str,
) -> Result<u64> {
    let mut builder = WriteStrategyBuilder::default();
    let fields = dtype
        .as_struct_fields_opt()
        .ok_or_else(|| report!("vortex compaction output must be a struct dtype"))?;
    for name in fields.names().iter() {
        builder = builder.with_field_writer(
            FieldPath::from_name(name.clone()),
            Arc::new(CopyColumnStrategy),
        );
    }
    let write_options = session.write_options().with_strategy(builder.build());
    let handle = session.handle();
    let mut writer = FileWrite::create(output, handle)
        .await
        .map_err(|error| report!("create vortex output {output}: {error}"))?;
    let summary = write_options
        .write(&mut writer, array_stream)
        .await
        .map_err(|error| report!("write vortex output {output}: {error}"))?;
    Ok(summary.size())
}

/// Concatenate whole vortex files in the given order into one output file.
///
/// Every input must have the same dtype.  The caller is responsible for
/// ordering the files so the concatenation is sorted by primary key; the
/// planner only selects this path when the ranges are disjoint and in order.
pub async fn copy_vortex_files(files: &[String], output: &str) -> Result<u64> {
    if files.is_empty() {
        return Err(report!(
            "copy_vortex_files requires at least one input file"
        ));
    }
    let session = crate::file_format::vortex_scan_session();
    let dtype = vortex_dtype(&session, files).await?;
    let writer_session = session.clone();
    let paths = files.to_vec();
    let stream = async_stream::try_stream! {
        for path in paths {
            let file = writer_session.open_options().open_path(&path).await?;
            let mut scan = file
                .scan()?
                .with_ordered(true)
                .with_split_by(SplitBy::Layout)
                .into_array_stream()?;
            while let Some(chunk) = scan.next().await {
                yield chunk?;
            }
        }
    };
    let array_stream = ArrayStreamAdapter::new(dtype.clone(), stream);
    write_vortex_bytes(&session, dtype, array_stream, output).await
}

/// Concatenate block-aligned source ranges in output order into one vortex
/// file.  The ranges are produced by [`crate::compaction::plan`].
pub async fn copy_vortex_ranges(
    files: &[String],
    ranges: &[BlockRange],
    output: &str,
) -> Result<u64> {
    crate::compaction::raw_copy::copy_vortex_ranges_raw(files, ranges, output).await
}

async fn vortex_dtype(session: &VortexSession, files: &[String]) -> Result<DType> {
    let indices = (0..files.len()).collect::<Vec<_>>();
    vortex_dtype_for_indices(session, files, &indices).await
}

async fn vortex_dtype_for_indices(
    session: &VortexSession,
    files: &[String],
    indices: &[usize],
) -> Result<DType> {
    let mut unique = indices.to_vec();
    unique.sort_unstable();
    unique.dedup();
    let mut dtype = None;
    for index in unique {
        let path = files
            .get(index)
            .ok_or_else(|| report!("range file index {index} out of bounds"))?;
        let file = session
            .open_options()
            .open_path(path)
            .await
            .map_err(|error| report!("open vortex {path}: {error}"))?;
        match &dtype {
            None => dtype = Some(file.dtype().clone()),
            Some(expected) if expected == file.dtype() => {}
            Some(_) => {
                return Err(report!(
                    "vortex inputs have different dtypes; block copy cannot concatenate them"
                ));
            }
        }
    }
    dtype.ok_or_else(|| report!("no vortex input files"))
}

/// Copy files byte-for-byte into `output_dir` (file-level keep/discard).
///
/// This is used by local validation and by callers that keep whole source
/// files as runs.  It does not decode or rewrite any data.
pub fn copy_files(files: &[String], output_dir: &str) -> Result<Vec<String>> {
    std::fs::create_dir_all(output_dir)
        .map_err(|error| report!("create output dir {output_dir}: {error}"))?;
    let mut outputs = Vec::with_capacity(files.len());
    for path in files {
        let name = std::path::Path::new(path)
            .file_name()
            .ok_or_else(|| report!("input path has no file name: {path}"))?;
        let target = std::path::Path::new(output_dir).join(name);
        std::fs::copy(path, &target)
            .map_err(|error| report!("copy {path} to {}: {error}", target.display()))?;
        outputs.push(target.to_string_lossy().into_owned());
    }
    Ok(outputs)
}
