// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Raw Vortex segment/layout copy for block-level compaction.
//!
//! The array-level copy path re-serializes every decoded chunk, which loses
//! dictionary and Pco encodings and cannot handle lazy `Slice`/`Shared`
//! children.  This module instead copies the encoded segments of complete
//! source chunks and rebuilds the layout tree with new segment ids.  No array
//! is decoded and no compressor runs.
//!
//! A block range must align with every column's chunk boundaries.  The planner
//! already produces such ranges (`FileSummary::blocks` is the intersection of
//! all column chunk boundaries).  LakeSoul writes every column's chunk
//! boundaries on multiples of that column's row block size, which is also the
//! zoned zone length, so an aligned range is also aligned to the zone grid.
//! The source zone-map rows covered by the range are copied into a new zones
//! table, preserving per-zone pruning.  An unaligned range keeps the data but
//! drops the zoned wrapper, because reusing source zones would describe rows
//! outside the output range.
//!
//! File-level statistics are rebuilt from the output zone maps when the copy
//! is not a single whole-file range: min/max (exact for numeric, inexact for
//! bounded strings), null_count and nan_count.  Sum is omitted because zone
//! maps do not store it; a single whole-file range keeps the source file
//! statistics, including sum.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rootcause::report;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use vortex::array::aggregate_fn::NumericalAggregateOpts;
use vortex::array::aggregate_fn::fns::min_max::min_max;
use vortex::array::aggregate_fn::fns::sum::sum;
use vortex::array::arrays::Struct;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::expr::stats::{Precision, Stat};
use vortex::array::scalar::{PValue, Scalar, ScalarValue};
use vortex::array::serde::{SerializeOptions, SerializedArray};
use vortex::array::stats::StatsSet;
use vortex::array::{
    ArrayContext, ArrayRef, ArrayView, Canonical, IntoArray, VortexSessionExecute,
};
use vortex::buffer::{Alignment, ByteBuffer};
use vortex::file::{
    FileStatistics, Footer, OpenOptionsSessionExt, SegmentSpec, VortexFile,
};
use vortex::layout::layouts::chunked::ChunkedLayout;
use vortex::layout::layouts::flat::{Flat, FlatLayout};
use vortex::layout::layouts::zoned::Zoned;
use vortex::layout::segments::SegmentId;
use vortex::layout::session::LayoutSessionExt;
use vortex::layout::{LayoutBuildContext, LayoutRef, layout_children};
use vortex::session::VortexSession;
use vortex::session::registry::ReadContext;

use crate::Result;
use crate::compaction::plan::BlockRange;

/// Vortex magic bytes, stable for the v1 file format.
const MAGIC_BYTES: &[u8; 4] = b"VTXF";

struct SourceFile {
    file: VortexFile,
    rows: u64,
    array_ctx: ReadContext,
}

/// Zones covered by one selected field, in source zone indices.
struct ZonedSelection {
    source: LayoutRef,
    zone_start: usize,
    zone_end: usize,
}

/// One field selected for one output range.
struct SelectedField {
    /// Layout whose rows are exactly this range.  A whole-file range keeps the
    /// source zoned wrapper inside `data`; a partial range has only the
    /// selected data chunks.
    data: LayoutRef,
    /// Source zones for statistics.  `rebuild_zoned` says whether `data`
    /// needs a new zoned wrapper around it.
    zoned: Option<ZonedSelection>,
    rebuild_zoned: bool,
}

/// Statistics accumulated over the output's zone maps for one column.
#[derive(Clone, Default)]
struct FileColumnStats {
    min: Option<Scalar>,
    min_exact: bool,
    min_invalid: bool,
    max: Option<Scalar>,
    max_exact: bool,
    max_invalid: bool,
    null_count: Option<u64>,
    null_count_invalid: bool,
    nan_count: Option<u64>,
    nan_count_invalid: bool,
}

/// Statistics derived from one range's zone-map rows.
struct ZoneRangeStats {
    min: Option<Scalar>,
    min_exact: bool,
    min_supported: bool,
    max: Option<Scalar>,
    max_exact: bool,
    max_supported: bool,
    max_unknown: bool,
    null_count: Option<u64>,
    null_count_supported: bool,
    nan_count: Option<u64>,
    nan_count_supported: bool,
}

impl FileColumnStats {
    fn merge(&mut self, range: ZoneRangeStats) {
        if !range.min_supported {
            self.min_invalid = true;
            self.min = None;
        } else if let Some(min) = range.min
            && !self.min_invalid
        {
            match self.min.take() {
                None => {
                    self.min = Some(min);
                    self.min_exact = range.min_exact;
                }
                Some(current) => {
                    let merged = match current.partial_cmp(&min) {
                        Some(std::cmp::Ordering::Less) => Some(current),
                        Some(_) => Some(min),
                        None => {
                            self.min_exact = false;
                            Some(current)
                        }
                    };
                    self.min = merged;
                    self.min_exact &= range.min_exact;
                }
            }
        }

        if !range.max_supported || range.max_unknown {
            self.max_invalid = true;
            self.max = None;
        } else if let Some(max) = range.max
            && !self.max_invalid
        {
            match self.max.take() {
                None => {
                    self.max = Some(max);
                    self.max_exact = range.max_exact;
                }
                Some(current) => {
                    let merged = match current.partial_cmp(&max) {
                        Some(std::cmp::Ordering::Greater) => Some(current),
                        Some(_) => Some(max),
                        None => {
                            self.max_exact = false;
                            Some(current)
                        }
                    };
                    self.max = merged;
                    self.max_exact &= range.max_exact;
                }
            }
        }

        if !range.null_count_supported {
            self.null_count_invalid = true;
            self.null_count = None;
        } else if let Some(value) = range.null_count {
            if !self.null_count_invalid {
                self.null_count = Some(self.null_count.unwrap_or(0) + value);
            }
        } else {
            self.null_count_invalid = true;
            self.null_count = None;
        }

        if !range.nan_count_supported {
            self.nan_count_invalid = true;
            self.nan_count = None;
        } else if let Some(value) = range.nan_count {
            if !self.nan_count_invalid {
                self.nan_count = Some(self.nan_count.unwrap_or(0) + value);
            }
        } else {
            self.nan_count_invalid = true;
            self.nan_count = None;
        }
    }

    fn into_stats_set(self) -> StatsSet {
        let mut stats = StatsSet::default();
        if !self.min_invalid
            && let Some(value) = self.min.and_then(Scalar::into_value)
        {
            stats.set(
                Stat::Min,
                if self.min_exact {
                    Precision::Exact(value)
                } else {
                    Precision::Inexact(value)
                },
            );
        }
        if !self.max_invalid
            && let Some(value) = self.max.and_then(Scalar::into_value)
        {
            stats.set(
                Stat::Max,
                if self.max_exact {
                    Precision::Exact(value)
                } else {
                    Precision::Inexact(value)
                },
            );
        }
        if !self.null_count_invalid
            && let Some(count) = self.null_count
        {
            stats.set(Stat::NullCount, Precision::Exact(ScalarValue::from(count)));
        }
        if !self.nan_count_invalid
            && let Some(count) = self.nan_count
        {
            stats.set(Stat::NaNCount, Precision::Exact(ScalarValue::from(count)));
        }
        stats
    }
}

/// Removes a partial output file when the copy fails.
struct RemoveOnDrop(Option<std::path::PathBuf>);

impl RemoveOnDrop {
    fn new(path: &str) -> Self {
        Self(Some(std::path::PathBuf::from(path)))
    }

    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Copy block-aligned ranges from one or more vortex files into one output
/// file using raw segment copies.
pub async fn copy_vortex_ranges_raw(
    files: &[String],
    ranges: &[BlockRange],
    output: &str,
) -> Result<u64> {
    if ranges.is_empty() {
        return Err(report!("raw vortex copy requires at least one range"));
    }
    let session = crate::file_format::vortex_scan_session();

    let mut sources: HashMap<usize, SourceFile> = HashMap::new();
    for range in ranges {
        if sources.contains_key(&range.file) {
            continue;
        }
        let path = files
            .get(range.file)
            .ok_or_else(|| report!("range file index {} out of bounds", range.file))?;
        let file = session
            .open_options()
            .open_path(path)
            .await
            .map_err(|error| report!("open vortex {path}: {error}"))?;
        let array_ctx = find_array_context(file.footer().layout())
            .ok_or_else(|| report!("vortex {path} has no flat array segment"))?;
        sources.insert(
            range.file,
            SourceFile {
                rows: file.row_count(),
                file,
                array_ctx,
            },
        );
    }

    let mut array_ctx = None;
    let mut dtype = None;
    let mut root_encoding = None;
    let mut root_metadata: Option<Vec<u8>> = None;
    for source in sources.values() {
        let root = source.file.footer().layout();
        match &root_encoding {
            None => root_encoding = Some(root.encoding_id()),
            Some(expected) if *expected == root.encoding_id() => {}
            Some(_) => {
                return Err(report!(
                    "vortex inputs have different root layout encodings; raw copy cannot concatenate them"
                ));
            }
        }
        let metadata = root.dyn_metadata();
        match &root_metadata {
            None => root_metadata = Some(metadata),
            Some(expected) if *expected == metadata => {}
            Some(_) => {
                return Err(report!(
                    "vortex inputs have different root layout metadata; raw copy cannot concatenate them"
                ));
            }
        }
        match &array_ctx {
            None => array_ctx = Some(source.array_ctx.clone()),
            Some(expected) if expected.ids() == source.array_ctx.ids() => {}
            Some(_) => {
                return Err(report!(
                    "vortex inputs use different array encoding contexts; raw copy cannot concatenate them"
                ));
            }
        }
        match &dtype {
            None => dtype = Some(source.file.dtype().clone()),
            Some(expected) if expected == source.file.dtype() => {}
            Some(_) => {
                return Err(report!(
                    "vortex inputs have different dtypes; block copy cannot concatenate them"
                ));
            }
        }
    }
    let array_ctx = array_ctx.expect("at least one source");
    let dtype = dtype.expect("at least one source");
    if dtype.as_struct_fields_opt().is_none() {
        return Err(report!(
            "raw vortex copy requires a struct-rooted file, got {dtype}"
        ));
    }
    if dtype.is_nullable() {
        return Err(report!(
            "raw vortex copy does not support a nullable root struct yet"
        ));
    }
    let output_array_ctx = ArrayContext::new(array_ctx.ids().to_vec());

    // 1. Select the source layout subtree for every range, before touching
    //    segment bytes.  The selections are still source layouts.
    let mut per_range_fields: Vec<Vec<SelectedField>> = Vec::with_capacity(ranges.len());
    let mut per_range_rows = Vec::with_capacity(ranges.len());
    for range in ranges {
        let source = sources
            .get(&range.file)
            .expect("source was opened for every range");
        if range.start_row > range.end_row || range.end_row > source.rows {
            return Err(report!(
                "range {:?} is outside {} rows of source {}",
                range,
                source.rows,
                range.file
            ));
        }
        let root = source.file.footer().layout();
        let fields = root
            .children()
            .map_err(|error| report!("read vortex root layout: {error}"))?;
        let mut selected = Vec::with_capacity(fields.len());
        for field in &fields {
            selected.push(select_range_field(
                field,
                range.start_row,
                range.end_row,
                source.rows,
            )?);
        }
        per_range_rows.push(range.end_row - range.start_row);
        per_range_fields.push(selected);
    }

    let field_count = per_range_fields[0].len();
    if per_range_fields
        .iter()
        .any(|fields| fields.len() != field_count)
    {
        return Err(report!("vortex inputs have different field counts"));
    }
    for field in 0..field_count {
        let expected = per_range_fields[0][field].data.dtype();
        if per_range_fields
            .iter()
            .any(|fields| fields[field].data.dtype() != expected)
        {
            return Err(report!("vortex input field dtypes differ"));
        }
    }

    // 2. Copy every referenced source segment exactly once, in deterministic
    //    order, and record the old -> new id mapping.
    let mut guard = RemoveOnDrop::new(output);
    let file = File::create(output)
        .await
        .map_err(|error| report!("create vortex output: {error}"))?;
    let mut output = BufWriter::with_capacity(8 * 1024 * 1024, file);
    output
        .write_all(MAGIC_BYTES)
        .await
        .map_err(|error| report!("write vortex magic: {error}"))?;
    let mut position = MAGIC_BYTES.len() as u64;
    let mut specs = Vec::new();
    let mut mapping: HashMap<(usize, SegmentId), SegmentId> = HashMap::new();
    let mut copy_plan: Vec<(usize, SegmentId)> = Vec::new();
    for (range_index, fields) in per_range_fields.iter().enumerate() {
        let file = ranges[range_index].file;
        let mut seen = HashSet::new();
        for field in fields {
            collect_segments(&field.data, &mut seen, &mut |segment| {
                copy_plan.push((file, segment));
            });
        }
    }
    let mut written: HashMap<(usize, SegmentId), SegmentId> = HashMap::new();
    for (file, segment) in copy_plan {
        if written.contains_key(&(file, segment)) {
            continue;
        }
        let source = sources.get(&file).expect("source");
        let spec = source
            .file
            .footer()
            .segment_map()
            .get(*segment as usize)
            .copied()
            .ok_or_else(|| report!("segment {segment} missing from source footer"))?;
        let handle = source
            .file
            .segment_source()
            .request(segment)
            .await
            .map_err(|error| report!("read vortex segment {segment}: {error}"))?;
        if handle.len() != spec.length as usize {
            return Err(report!(
                "segment {segment} length {} does not match footer length {}",
                handle.len(),
                spec.length
            ));
        }
        let bytes = if handle.is_on_host() {
            handle.unwrap_host()
        } else {
            handle.to_host().await
        };
        let next = append_segment(
            &mut output,
            &mut position,
            &mut specs,
            std::slice::from_ref(&bytes),
            spec.alignment,
        )
        .await?;
        written.insert((file, segment), next);
        mapping.insert((file, segment), next);
    }

    // 3. Rebuild partial-range zone maps from the source zone rows covered by
    //    each range, and accumulate file-level statistics from those rows.
    let single_whole_file = ranges.len() == 1
        && ranges[0].start_row == 0
        && ranges[0].end_row == sources.get(&ranges[0].file).expect("source").rows;
    let mut zones_cache: HashMap<(usize, usize), ArrayRef> = HashMap::new();
    let mut new_zones: HashMap<(usize, usize), LayoutRef> = HashMap::new();
    let mut file_stats: Vec<Option<FileColumnStats>> =
        vec![Some(FileColumnStats::default()); field_count];
    let mut stats_ctx = session.create_execution_ctx();

    if !single_whole_file {
        for (range_index, fields) in per_range_fields.iter().enumerate() {
            let file = ranges[range_index].file;
            for (field_index, field) in fields.iter().enumerate() {
                let Some(zoned) = &field.zoned else {
                    file_stats[field_index] = None;
                    continue;
                };
                let source = sources.get(&file).expect("source");
                let zones = match zones_cache.get(&(file, field_index)) {
                    Some(zones) => zones.clone(),
                    None => {
                        let zones = match load_zones(source, zoned, &session).await {
                            Ok(zones) => zones,
                            Err(error) => {
                                tracing::warn!(
                                    "cannot read vortex zone map for field {field_index} in {}: {error}; dropping zone map for this column",
                                    ranges[range_index].file
                                );
                                file_stats[field_index] = None;
                                continue;
                            }
                        };
                        zones_cache.insert((file, field_index), zones.clone());
                        zones
                    }
                };
                let slice = zones
                    .slice(zoned.zone_start..zoned.zone_end)
                    .map_err(|error| report!("slice vortex zone map: {error}"))?;
                let canonical: Canonical = slice
                    .execute(&mut stats_ctx)
                    .map_err(|error| report!("canonicalize vortex zone map: {error}"))?;
                let canonical = canonical.into_array();

                if let Some(stats) = file_stats[field_index].as_mut() {
                    match zone_range_stats(&canonical, &mut stats_ctx) {
                        Ok(range_stats) => stats.merge(range_stats),
                        Err(error) => {
                            tracing::warn!(
                                "cannot derive file statistics from zone map for field {field_index}: {error}"
                            );
                            file_stats[field_index] = None;
                        }
                    }
                }

                if field.rebuild_zoned {
                    match write_zone_layout(
                        &mut output,
                        &mut position,
                        &mut specs,
                        &output_array_ctx,
                        &canonical,
                        zoned.zone_end - zoned.zone_start,
                        &session,
                    )
                    .await
                    {
                        Ok(layout) => {
                            new_zones.insert((range_index, field_index), layout);
                        }
                        Err(error) => {
                            tracing::warn!(
                                "cannot rebuild vortex zone map for field {field_index} in range {range_index}: {error}; dropping zone map"
                            );
                        }
                    }
                }
            }
        }
    }

    // 4. Rebuild the layout tree with the new segment ids and, for partial
    //    ranges, the new zoned wrappers.
    let build_ctx = LayoutBuildContext {
        session: &session,
        array_read_ctx: &array_ctx,
    };
    let total_rows: u64 = per_range_rows.iter().sum();
    let mut field_layouts = Vec::with_capacity(field_count);
    for field in 0..field_count {
        let children = per_range_fields
            .iter()
            .enumerate()
            .map(|(range_index, fields)| {
                let selected = &fields[field];
                let data = rebuild_layout(
                    &selected.data,
                    ranges[range_index].file,
                    &mapping,
                    &build_ctx,
                )?;
                if selected.rebuild_zoned
                    && let (Some(zoned), Some(zones)) =
                        (&selected.zoned, new_zones.get(&(range_index, field)))
                {
                    return rebuild_node(
                        zoned.source.encoding_id(),
                        zoned.source.dtype(),
                        data.row_count(),
                        &zoned.source.dyn_metadata(),
                        Vec::new(),
                        vec![data, zones.clone()],
                        &build_ctx,
                    );
                }
                Ok(data)
            })
            .collect::<Result<Vec<_>>>()?;
        if children.len() == 1 {
            field_layouts.push(children.into_iter().next().expect("one child"));
        } else {
            let field_dtype = children[0].dtype().clone();
            field_layouts.push(
                ChunkedLayout::new(total_rows, field_dtype, layout_children(children))
                    .into_layout(),
            );
        }
    }

    let root_encoding = root_encoding.expect("at least one source");
    let root_metadata = root_metadata.expect("at least one source");
    let root = rebuild_node(
        root_encoding,
        &dtype,
        total_rows,
        &root_metadata,
        Vec::new(),
        field_layouts,
        &build_ctx,
    )?;

    // 5. Write the footer.  A single whole-file range keeps the source
    //    file-level statistics (including sum); other outputs use statistics
    //    derived from their zone maps (sum is not available there).
    let statistics = if single_whole_file {
        let range = ranges[0];
        let source = sources.get(&range.file).expect("source");
        source.file.footer().statistics().cloned()
    } else {
        let stats_sets = file_stats
            .into_iter()
            .map(|stats| {
                stats
                    .map(FileColumnStats::into_stats_set)
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        Some(FileStatistics::new_with_dtype(
            Arc::from(stats_sets),
            &dtype,
        ))
    };
    let footer_ctx = ReadContext::new(output_array_ctx.to_ids());
    let footer = Footer::new(root, Arc::from(specs), statistics, footer_ctx);
    let buffers = footer
        .into_serializer()
        .with_offset(position)
        .serialize()
        .map_err(|error| report!("serialize vortex footer: {error}"))?;
    for buffer in &buffers {
        output
            .write_all(buffer.as_slice())
            .await
            .map_err(|error| report!("write vortex footer: {error}"))?;
        position += buffer.len() as u64;
    }
    output
        .flush()
        .await
        .map_err(|error| report!("flush vortex output: {error}"))?;
    guard.disarm();
    Ok(position)
}

/// Select the chunks of `layout` that exactly cover `[start, end)`.
fn select_range_field(
    layout: &LayoutRef,
    start: u64,
    end: u64,
    file_rows: u64,
) -> Result<SelectedField> {
    let whole = start == 0 && end == file_rows;
    if let Some(zoned) = layout.as_opt::<Zoned>() {
        let zone_len = zoned.zone_len();
        let nzones = zoned.nzones();
        let data_layout = layout
            .children()
            .map_err(|error| report!("read zoned children: {error}"))?
            .into_iter()
            .next()
            .ok_or_else(|| report!("zoned layout has no data child"))?;
        if whole {
            return Ok(SelectedField {
                data: Arc::clone(layout),
                zoned: (zone_len > 0).then(|| ZonedSelection {
                    source: Arc::clone(layout),
                    zone_start: 0,
                    zone_end: nzones,
                }),
                rebuild_zoned: false,
            });
        }
        let data = select_chunks(&data_layout, start, end, file_rows)?;
        let aligned = zone_len > 0
            && start.is_multiple_of(zone_len as u64)
            && (end == file_rows || end.is_multiple_of(zone_len as u64));
        if aligned {
            let zone_start = (start / zone_len as u64) as usize;
            let zone_end = if end == file_rows {
                nzones
            } else {
                (end / zone_len as u64) as usize
            };
            let expected = (end - start).div_ceil(zone_len as u64) as usize;
            if zone_end > zone_start && zone_end - zone_start == expected {
                return Ok(SelectedField {
                    data,
                    zoned: Some(ZonedSelection {
                        source: Arc::clone(layout),
                        zone_start,
                        zone_end,
                    }),
                    rebuild_zoned: true,
                });
            }
        }
        return Ok(SelectedField {
            data,
            zoned: None,
            rebuild_zoned: false,
        });
    }

    if whole {
        Ok(SelectedField {
            data: Arc::clone(layout),
            zoned: None,
            rebuild_zoned: false,
        })
    } else {
        Ok(SelectedField {
            data: select_chunks(layout, start, end, file_rows)?,
            zoned: None,
            rebuild_zoned: false,
        })
    }
}

fn select_chunks(
    layout: &LayoutRef,
    start: u64,
    end: u64,
    file_rows: u64,
) -> Result<LayoutRef> {
    let children = layout
        .children()
        .map_err(|error| report!("read chunked children: {error}"))?;
    if children.is_empty() {
        return Err(report!(
            "range {}..{} is partial, but the column has no chunked layout",
            start,
            end
        ));
    }
    let mut offsets = Vec::with_capacity(children.len() + 1);
    offsets.push(0u64);
    for child in &children {
        let next = offsets.last().copied().expect("offset") + child.row_count();
        offsets.push(next);
    }
    if offsets.last().copied() != Some(file_rows) {
        return Err(report!(
            "chunk rows {} do not add up to file rows {file_rows}",
            offsets.last().copied().unwrap_or_default()
        ));
    }
    let first = offsets
        .iter()
        .position(|offset| *offset == start)
        .ok_or_else(|| report!("range start {start} is not a chunk boundary"))?;
    let last = offsets
        .iter()
        .position(|offset| *offset == end)
        .ok_or_else(|| report!("range end {end} is not a chunk boundary"))?;
    if last <= first {
        return Err(report!("empty chunk selection {first}..{last}"));
    }
    let selected = children[first..last].to_vec();
    Ok(ChunkedLayout::new(
        end - start,
        layout.dtype().clone(),
        layout_children(selected),
    )
    .into_layout())
}

fn collect_segments(
    layout: &LayoutRef,
    seen: &mut HashSet<SegmentId>,
    push: &mut dyn FnMut(SegmentId),
) {
    for segment in layout.dyn_segment_ids() {
        if seen.insert(segment) {
            push(segment);
        }
    }
    if let Ok(children) = layout.children() {
        for child in children {
            collect_segments(&child, seen, push);
        }
    }
}

fn rebuild_layout(
    layout: &LayoutRef,
    file: usize,
    mapping: &HashMap<(usize, SegmentId), SegmentId>,
    build_ctx: &LayoutBuildContext<'_>,
) -> Result<LayoutRef> {
    let children = layout
        .children()
        .map_err(|error| report!("read vortex layout children: {error}"))?
        .iter()
        .map(|child| rebuild_layout(child, file, mapping, build_ctx))
        .collect::<Result<Vec<_>>>()?;
    let segment_ids = layout
        .dyn_segment_ids()
        .into_iter()
        .map(|segment| {
            mapping
                .get(&(file, segment))
                .copied()
                .ok_or_else(|| report!("segment {segment} was not copied"))
        })
        .collect::<Result<Vec<_>>>()?;
    rebuild_node(
        layout.encoding_id(),
        layout.dtype(),
        layout.row_count(),
        &layout.dyn_metadata(),
        segment_ids,
        children,
        build_ctx,
    )
}

fn rebuild_node(
    encoding: vortex::layout::LayoutId,
    dtype: &vortex::dtype::DType,
    row_count: u64,
    metadata: &[u8],
    segment_ids: Vec<SegmentId>,
    children: Vec<LayoutRef>,
    build_ctx: &LayoutBuildContext<'_>,
) -> Result<LayoutRef> {
    let plugin = build_ctx
        .session
        .layouts()
        .registry()
        .get(&encoding)
        .ok_or_else(|| report!("vortex layout encoding {encoding} is not registered"))?;
    let children = layout_children(children);
    plugin
        .build(
            dtype,
            row_count,
            metadata,
            segment_ids,
            children.as_ref(),
            build_ctx,
        )
        .map_err(|error| report!("rebuild vortex layout {encoding}: {error}"))
}

async fn append_segment(
    output: &mut BufWriter<File>,
    position: &mut u64,
    specs: &mut Vec<SegmentSpec>,
    buffers: &[ByteBuffer],
    alignment: Alignment,
) -> Result<SegmentId> {
    let length = buffers.iter().map(ByteBuffer::len).sum::<usize>();
    let length = u32::try_from(length)
        .map_err(|_| report!("vortex segment length exceeds u32"))?;
    let padding = position.next_multiple_of(alignment.as_usize() as u64) - *position;
    if padding > 0 {
        output
            .write_all(&vec![0u8; padding as usize])
            .await
            .map_err(|error| report!("write vortex padding: {error}"))?;
        *position += padding;
    }
    let offset = *position;
    for buffer in buffers {
        output
            .write_all(buffer.as_slice())
            .await
            .map_err(|error| report!("write vortex segment: {error}"))?;
        *position += buffer.len() as u64;
    }
    let segment = SegmentId::try_from(specs.len())
        .map_err(|error| report!("too many vortex segments: {error}"))?;
    specs.push(SegmentSpec {
        offset,
        length,
        alignment,
    });
    Ok(segment)
}

async fn load_zones(
    source: &SourceFile,
    zoned: &ZonedSelection,
    session: &VortexSession,
) -> Result<ArrayRef> {
    let children = zoned
        .source
        .children()
        .map_err(|error| report!("read zoned children: {error}"))?;
    let zones = children
        .get(1)
        .ok_or_else(|| report!("zoned layout has no zones child"))?;
    let flat: &FlatLayout = zones
        .as_opt::<Flat>()
        .ok_or_else(|| report!("zone map is not a flat layout"))?;
    let handle = source
        .file
        .segment_source()
        .request(flat.segment_id())
        .await
        .map_err(|error| report!("read vortex zone map segment: {error}"))?;
    let serialized = SerializedArray::try_from(handle)
        .map_err(|error| report!("parse vortex zone map segment: {error}"))?;
    serialized
        .decode(
            zones.dtype(),
            zones.row_count() as usize,
            flat.array_ctx(),
            session,
        )
        .map_err(|error| report!("decode vortex zone map: {error}"))
}

async fn write_zone_layout(
    output: &mut BufWriter<File>,
    position: &mut u64,
    specs: &mut Vec<SegmentSpec>,
    array_ctx: &ArrayContext,
    zones: &ArrayRef,
    zone_rows: usize,
    session: &VortexSession,
) -> Result<LayoutRef> {
    let buffers = zones
        .serialize(
            array_ctx,
            session,
            &SerializeOptions {
                offset: 0,
                include_padding: true,
            },
        )
        .map_err(|error| report!("serialize vortex zone map: {error}"))?;
    let alignment = buffers
        .first()
        .ok_or_else(|| report!("vortex zone map serialized to no buffers"))?
        .alignment();
    let segment = append_segment(output, position, specs, &buffers, alignment).await?;
    Ok(FlatLayout::new_with_metadata(
        zone_rows as u64,
        zones.dtype().clone(),
        segment,
        ReadContext::new(array_ctx.to_ids()),
        None,
    )
    .into_layout())
}

fn zone_range_stats(
    zones: &ArrayRef,
    ctx: &mut vortex::array::ExecutionCtx,
) -> Result<ZoneRangeStats> {
    let Some(zones) = zones.as_opt::<Struct>() else {
        return Err(report!("vortex zone map is not a struct array"));
    };
    let opts = NumericalAggregateOpts::default();
    let mut stats = ZoneRangeStats {
        min: None,
        min_exact: false,
        min_supported: false,
        max: None,
        max_exact: false,
        max_supported: false,
        max_unknown: false,
        null_count: None,
        null_count_supported: false,
        nan_count: None,
        nan_count_supported: false,
    };

    if let Some(field) = zones.unmasked_field_by_name_opt("vortex.min()") {
        stats.min_supported = true;
        stats.min_exact = true;
        stats.min = min_max(field, ctx, opts)
            .map_err(|error| report!("zone min: {error}"))?
            .map(|result| result.min);
    } else if let Some(name) = find_prefix(&zones, "vortex.bounded_min") {
        let field = zones
            .unmasked_field_by_name(name)
            .map_err(|error| report!("zone bounded min field: {error}"))?;
        stats.min_supported = true;
        stats.min_exact = false;
        stats.min = min_max(field, ctx, opts)
            .map_err(|error| report!("zone bounded min: {error}"))?
            .map(|result| result.min);
    } else if let Some(field) = zones.unmasked_field_by_name_opt("min") {
        stats.min_supported = true;
        stats.min_exact = !any_true(&zones, "min_is_truncated", ctx, &opts)?;
        stats.min = min_max(field, ctx, opts)
            .map_err(|error| report!("zone legacy min: {error}"))?
            .map(|result| result.min);
    }

    if let Some(field) = zones.unmasked_field_by_name_opt("vortex.max()") {
        stats.max_supported = true;
        stats.max_exact = true;
        stats.max = min_max(field, ctx, opts)
            .map_err(|error| report!("zone max: {error}"))?
            .map(|result| result.max);
    } else if let Some(field) = zones.unmasked_field_by_name_opt("max") {
        stats.max_supported = true;
        stats.max_exact = !any_true(&zones, "max_is_truncated", ctx, &opts)?;
        stats.max = min_max(field, ctx, opts)
            .map_err(|error| report!("zone legacy max: {error}"))?
            .map(|result| result.max);
    } else if let Some(name) = find_prefix(&zones, "vortex.bounded_max") {
        stats.max_supported = true;
        stats.max_exact = false;
        let bounded = zones
            .unmasked_field_by_name(name)
            .map_err(|error| report!("zone bounded max field: {error}"))?;
        let Some(bounded) = bounded.as_opt::<Struct>() else {
            return Err(report!("zone bounded max is not a struct"));
        };
        let unknown = bounded
            .unmasked_field_by_name("unknown")
            .map_err(|error| report!("zone bounded max unknown field: {error}"))?;
        let any_unknown = min_max(unknown, ctx, opts)
            .map_err(|error| report!("zone bounded max unknown: {error}"))?
            .map(|result| result.max.as_bool().value() == Some(true))
            .unwrap_or(false);
        if any_unknown {
            stats.max_unknown = true;
        } else {
            let bound = bounded
                .unmasked_field_by_name("bound")
                .map_err(|error| report!("zone bounded max bound field: {error}"))?;
            stats.max = min_max(bound, ctx, opts)
                .map_err(|error| report!("zone bounded max bound: {error}"))?
                .map(|result| result.max);
        }
    }

    if let Some(field) = zones
        .unmasked_field_by_name_opt("vortex.null_count()")
        .or_else(|| zones.unmasked_field_by_name_opt("null_count"))
    {
        stats.null_count_supported = true;
        stats.null_count = sum_u64(field, ctx)?;
    }
    if let Some(field) = zones
        .unmasked_field_by_name_opt("vortex.nan_count()")
        .or_else(|| zones.unmasked_field_by_name_opt("nan_count"))
    {
        stats.nan_count_supported = true;
        stats.nan_count = sum_u64(field, ctx)?;
    }
    Ok(stats)
}

fn any_true(
    zones: &ArrayView<'_, Struct>,
    name: &str,
    ctx: &mut vortex::array::ExecutionCtx,
    opts: &NumericalAggregateOpts,
) -> Result<bool> {
    let Some(field) = zones.unmasked_field_by_name_opt(name) else {
        return Ok(false);
    };
    Ok(min_max(field, ctx, *opts)
        .map_err(|error| report!("zone truncation flag: {error}"))?
        .map(|result| result.max.as_bool().value() == Some(true))
        .unwrap_or(false))
}

fn find_prefix<'a>(zones: &'a ArrayView<'_, Struct>, prefix: &str) -> Option<&'a str> {
    zones
        .names()
        .iter()
        .find(|name| name.as_ref().starts_with(prefix))
        .map(|name| name.as_ref())
}

fn sum_u64(
    field: &ArrayRef,
    ctx: &mut vortex::array::ExecutionCtx,
) -> Result<Option<u64>> {
    let scalar = sum(field, ctx).map_err(|error| report!("zone count sum: {error}"))?;
    Ok(
        match scalar
            .as_primitive_opt()
            .and_then(|primitive| primitive.pvalue())
        {
            Some(PValue::U64(value)) => Some(value),
            _ => None,
        },
    )
}

fn find_array_context(layout: &LayoutRef) -> Option<ReadContext> {
    if let Some(flat) = layout.as_opt::<Flat>() {
        return Some(flat.array_ctx().clone());
    }
    for child in layout.children().ok()? {
        if let Some(ctx) = find_array_context(&child) {
            return Some(ctx);
        }
    }
    None
}
