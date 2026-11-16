// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only summaries of data files for compaction planning.
//!
//! The analyzer reads only the file footer and, when a file format does not
//! expose statistics in its footer, the first/last row of the primary-key
//! columns.  It never reads a payload column.

use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use futures::StreamExt;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
};
use rootcause::report;
use vortex::array::VortexSessionExecute;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::FieldNames;
use vortex::expr::{root, select};
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::session::VortexSession;

use crate::Result;
use crate::file_format::PhysicalFormat;

/// An order-preserving encoding of a primary-key row (`arrow-row`).
///
/// Byte comparison is the lexicographic order of the composite key, so two
/// keys can be compared without decoding them back to Arrow values.
pub type PkKey = Vec<u8>;

/// Coarse shape of the primary key, used as a cheap hash-key guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkKind {
    /// A single numeric/temporal/decimal key.
    Numeric,
    /// A fixed-length 32-character hexadecimal string (MD5-like hash key).
    FixedHex,
    /// Anything else (composite key, business string, binary, ...).
    Other,
}

/// Physical file format of one compaction input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    Vortex,
}

impl FileFormat {
    pub fn from_path(path: &str) -> Result<Self> {
        match PhysicalFormat::from_extension(path)? {
            PhysicalFormat::Parquet => Ok(Self::Parquet),
            PhysicalFormat::Vortex | PhysicalFormat::VortexCompact => Ok(Self::Vortex),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
        }
    }
}

/// Metadata of one input file.
#[derive(Debug, Clone)]
pub struct FileSummary {
    pub path: String,
    pub format: FileFormat,
    pub rows: u64,
    pub size: u64,
    /// The primary-key column types, in key order.
    pub pk_types: Vec<DataType>,
    /// Coarse primary-key shape (numeric, fixed-hex hash, other).
    pub pk_kind: PkKind,
    /// First row of the file (min key under the sorted-file contract).
    pub pk_first: PkKey,
    /// Last row of the file (max key under the sorted-file contract).
    pub pk_last: PkKey,
    /// Cumulative row offsets of the file's natural blocks (row groups for
    /// parquet, layout splits for vortex).  Always starts at 0 and ends at
    /// `rows`.
    pub blocks: Vec<u64>,
}

impl FileSummary {
    pub fn blocks_range(&self) -> Range<usize> {
        0..self.blocks.len().saturating_sub(1)
    }

    /// The row range of block `index`.
    pub fn block_rows(&self, index: usize) -> Range<u64> {
        self.blocks[index]..self.blocks[index + 1]
    }
}

/// Summary of the whole input set.
///
/// `files` must be in LakeSoul version order (oldest first).  The planner's
/// UseLast merge relies on that order.
#[derive(Debug, Clone)]
pub struct InputSummary {
    pub files: Vec<FileSummary>,
    /// Shared primary-key types.  `None` means the files disagree and the
    /// planner must fall back.
    pub pk_types: Option<Vec<DataType>>,
    /// Shared primary-key shape.  `None` means the files disagree.
    pub pk_kind: Option<PkKind>,
}

/// Read footer metadata and the first/last primary-key row of every file.
///
/// `paths` must be in LakeSoul version order (oldest first).
pub async fn summarize_files(
    paths: &[String],
    primary_keys: &[String],
) -> Result<InputSummary> {
    if primary_keys.is_empty() {
        return Err(report!(
            "compaction planning requires at least one primary key"
        ));
    }
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        files.push(summarize_file(path, primary_keys).await?);
    }
    let pk_types = common_pk_types(&files);
    let pk_kind = common_pk_kind(&files);
    Ok(InputSummary {
        files,
        pk_types,
        pk_kind,
    })
}

fn common_pk_types(files: &[FileSummary]) -> Option<Vec<DataType>> {
    let first = files.first()?.pk_types.clone();
    if files.iter().all(|file| file.pk_types == first) {
        Some(first)
    } else {
        None
    }
}

fn common_pk_kind(files: &[FileSummary]) -> Option<PkKind> {
    let first = files.first()?.pk_kind;
    files
        .iter()
        .all(|file| file.pk_kind == first)
        .then_some(first)
}

/// Read one file's Arrow schema without reading any data.
pub async fn read_file_schema(path: &str) -> Result<SchemaRef> {
    match FileFormat::from_path(path)? {
        FileFormat::Parquet => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(
                File::open(path).map_err(|error| report!("open {path}: {error}"))?,
            )
            .map_err(|error| report!("read parquet footer {path}: {error}"))?;
            Ok(Arc::clone(builder.schema()))
        }
        FileFormat::Vortex => {
            let session = crate::file_format::vortex_scan_session();
            let file = session
                .open_options()
                .open_path(path)
                .await
                .map_err(|error| report!("open vortex {path}: {error}"))?;
            let schema = session
                .arrow()
                .to_arrow_schema(file.dtype())
                .map_err(|error| report!("vortex schema {path}: {error}"))?;
            Ok(Arc::new(schema))
        }
    }
}

/// Read one file's metadata.
pub async fn summarize_file(path: &str, primary_keys: &[String]) -> Result<FileSummary> {
    let format = FileFormat::from_path(path)?;
    let size = std::fs::metadata(path)
        .map_err(|error| report!("stat {path}: {error}"))?
        .len();
    match format {
        FileFormat::Parquet => summarize_parquet(path, size, primary_keys),
        FileFormat::Vortex => summarize_vortex(path, size, primary_keys).await,
    }
}

fn summarize_parquet(
    path: &str,
    size: u64,
    primary_keys: &[String],
) -> Result<FileSummary> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).map_err(|error| report!("open {path}: {error}"))?,
    )
    .map_err(|error| report!("read parquet footer {path}: {error}"))?;
    let metadata = Arc::clone(builder.metadata());
    let schema = Arc::clone(builder.schema());
    let pk_indices = pk_indices(&schema, primary_keys)?;
    let pk_types: Vec<DataType> = primary_keys
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .map(|field| field.data_type().clone())
                .map_err(|error| {
                    report!("primary key {name} missing from {path}: {error}")
                })
        })
        .collect::<Result<_>>()?;
    let rows = metadata.file_metadata().num_rows().max(0) as u64;
    let blocks = parquet_blocks(&metadata);
    if rows == 0 {
        return Ok(FileSummary {
            path: path.to_string(),
            format: FileFormat::Parquet,
            rows,
            size,
            pk_kind: kind_from_types(&pk_types),
            pk_types,
            pk_first: Vec::new(),
            pk_last: Vec::new(),
            blocks: vec![0],
        });
    }

    let (first, first_kind) =
        parquet_row_at(path, &pk_indices, &pk_types, primary_keys, 0, 0)?;
    let last_rg = metadata.num_row_groups() - 1;
    let last_rg_rows = metadata.row_group(last_rg).num_rows() as u64;
    let (last, last_kind) = parquet_row_at(
        path,
        &pk_indices,
        &pk_types,
        primary_keys,
        last_rg,
        last_rg_rows.saturating_sub(1),
    )?;
    Ok(FileSummary {
        path: path.to_string(),
        format: FileFormat::Parquet,
        rows,
        size,
        pk_kind: if first_kind == last_kind {
            first_kind
        } else {
            PkKind::Other
        },
        pk_types,
        pk_first: first,
        pk_last: last,
        blocks,
    })
}

fn parquet_blocks(metadata: &parquet::file::metadata::ParquetMetaData) -> Vec<u64> {
    let mut blocks = Vec::with_capacity(metadata.num_row_groups() + 1);
    blocks.push(0);
    let mut offset = 0u64;
    for rg in 0..metadata.num_row_groups() {
        offset += metadata.row_group(rg).num_rows().max(0) as u64;
        blocks.push(offset);
    }
    blocks
}

/// Read one row of a parquet row group and encode its primary-key columns.
fn parquet_row_at(
    path: &str,
    pk_indices: &[usize],
    pk_types: &[DataType],
    primary_keys: &[String],
    row_group: usize,
    row_in_group: u64,
) -> Result<(PkKey, PkKind)> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).map_err(|error| report!("open {path}: {error}"))?,
    )
    .map_err(|error| report!("read parquet footer {path}: {error}"))?;
    let mask =
        ProjectionMask::roots(builder.parquet_schema(), pk_indices.iter().copied());
    let selection = RowSelection::from(vec![
        RowSelector::skip(row_in_group as usize),
        RowSelector::select(1),
    ]);
    // Keep the selection inside the chosen row group; the reader applies the
    // selection relative to the selected row groups.
    let mut reader = builder
        .with_projection(mask)
        .with_row_groups(vec![row_group])
        .with_row_selection(selection)
        .with_limit(1)
        .build()
        .map_err(|error| report!("read parquet row {path}: {error}"))?;
    let batch = reader
        .next()
        .ok_or_else(|| report!("empty parquet row selection {path}"))?
        .map_err(|error| report!("read parquet row {path}: {error}"))?;
    if batch.num_rows() != 1 {
        return Err(report!(
            "parquet row selection {path} returned {} rows (rg {row_group}, row {row_in_group})",
            batch.num_rows()
        ));
    }
    // Resolve by name: a projection yields columns in file order, which may
    // differ from the requested primary-key order.
    let arrays = key_arrays_by_name(&batch, primary_keys)?;
    let kind = infer_pk_kind(&arrays, pk_types);
    Ok((encode_row(&arrays, pk_types)?, kind))
}

async fn summarize_vortex(
    path: &str,
    size: u64,
    primary_keys: &[String],
) -> Result<FileSummary> {
    let session = crate::file_format::vortex_scan_session();
    let file = session
        .open_options()
        .open_path(path)
        .await
        .map_err(|error| report!("open vortex {path}: {error}"))?;
    let rows = file.row_count();
    let dtype = file.dtype().clone();
    let pk_types = vortex_pk_types(&session, &dtype, primary_keys)?;
    let blocks = vortex_blocks(&file)?;
    if rows == 0 {
        return Ok(FileSummary {
            path: path.to_string(),
            format: FileFormat::Vortex,
            rows,
            size,
            pk_kind: kind_from_types(&pk_types),
            pk_types,
            pk_first: Vec::new(),
            pk_last: Vec::new(),
            blocks: vec![0],
        });
    }
    let (first, first_kind) =
        vortex_row_at(&file, &session, primary_keys, &pk_types, 0).await?;
    let (last, last_kind) =
        vortex_row_at(&file, &session, primary_keys, &pk_types, rows - 1).await?;
    Ok(FileSummary {
        path: path.to_string(),
        format: FileFormat::Vortex,
        rows,
        size,
        pk_kind: if first_kind == last_kind {
            first_kind
        } else {
            PkKind::Other
        },
        pk_types,
        pk_first: first,
        pk_last: last,
        blocks,
    })
}

fn vortex_pk_types(
    session: &VortexSession,
    dtype: &vortex::dtype::DType,
    primary_keys: &[String],
) -> Result<Vec<DataType>> {
    let fields = dtype
        .as_struct_fields_opt()
        .ok_or_else(|| report!("vortex file is not a struct: {dtype}"))?;
    primary_keys
        .iter()
        .map(|name| {
            let dtype = fields
                .field(name)
                .ok_or_else(|| report!("primary key {name} missing from vortex file"))?;
            session
                .arrow()
                .to_arrow_datatype(&dtype)
                .map_err(|error| report!("convert vortex key type {name}: {error}"))
        })
        .collect()
}

/// Physical row-block boundaries of a vortex file.
///
/// Scan splits are intentionally not used: they may coalesce several physical
/// blocks for IO concurrency.  The copy planner must use the layout's chunk
/// boundaries so a copied range is exactly a set of complete encoded blocks.
fn vortex_blocks(file: &VortexFile) -> Result<Vec<u64>> {
    let rows = file.row_count();
    let root = file.footer().layout();
    let children = root
        .children()
        .map_err(|error| report!("vortex layout children: {error}"))?;
    let mut common: Option<Vec<u64>> = None;
    for child in children {
        // A column without a chunked layout is a single physical block; it
        // constrains the intersection to the whole file.
        let offsets = chunk_offsets(&child, rows).unwrap_or_else(|| vec![0, rows]);
        common = Some(match common {
            None => offsets,
            Some(previous) => intersect_boundaries(&previous, &offsets),
        });
    }
    Ok(common.unwrap_or_else(|| vec![0, rows]))
}

/// Find the first chunked layout under `layout` whose children add up to
/// `rows` and return its cumulative chunk boundaries.
///
/// Columns are wrapped in zoned (statistics) layouts, so the chunked layout is
/// usually a child of the zoned layout, not a direct child of the root struct.
fn chunk_offsets(layout: &vortex::layout::LayoutRef, rows: u64) -> Option<Vec<u64>> {
    if layout.encoding_id().to_string().contains("chunked") {
        let blocks = layout.children().ok()?;
        if !blocks.is_empty() {
            let mut offsets = Vec::with_capacity(blocks.len() + 1);
            offsets.push(0u64);
            for block in &blocks {
                let next = offsets.last().copied()? + block.row_count();
                offsets.push(next);
            }
            if offsets.last().copied() == Some(rows) {
                return Some(offsets);
            }
        }
    }
    // Only descend through a transparent wrapper that keeps the column dtype
    // (the zoned data child).  Descending into a struct field, dictionary
    // values/codes, or a nested child would report boundaries the raw copy
    // cannot use for this column.
    for (slot, child_type) in layout.child_types().enumerate() {
        if matches!(child_type, vortex::layout::LayoutChildType::Transparent(_)) {
            let Some(child) = layout.slot(slot).ok().flatten() else {
                continue;
            };
            if child.dtype() == layout.dtype() {
                return chunk_offsets(&child, rows);
            }
        }
    }
    None
}

/// Boundaries present in both cumulative offset vectors.
///
/// Different columns can coalesce different numbers of row blocks, so the
/// copy unit is the intersection: a range is copyable only when every column
/// has a chunk boundary there.
fn intersect_boundaries(left: &[u64], right: &[u64]) -> Vec<u64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(left[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

async fn vortex_row_at(
    file: &VortexFile,
    session: &VortexSession,
    primary_keys: &[String],
    pk_types: &[DataType],
    row: u64,
) -> Result<(PkKey, PkKind)> {
    let projection = select(
        FieldNames::from_iter(primary_keys.iter().map(String::as_str)),
        root(),
    )
    .bind(file.dtype())
    .map_err(|error| report!("bind vortex key projection: {error}"))?;
    let mut stream = file
        .scan()
        .map_err(|error| report!("vortex scan: {error}"))?
        .with_projection(projection)
        .with_row_range(row..row + 1)
        .into_array_stream()
        .map_err(|error| report!("vortex row scan: {error}"))?;
    let chunk = stream
        .next()
        .await
        .ok_or_else(|| report!("vortex row scan returned no chunk"))?
        .map_err(|error| report!("vortex row scan: {error}"))?;
    let mut ctx = session.create_execution_ctx();
    let arrow = session
        .arrow()
        .execute_arrow(chunk, None, &mut ctx)
        .map_err(|error| report!("convert vortex key row: {error}"))?;
    let batch = RecordBatch::from(arrow.as_struct().clone());
    if batch.num_rows() != 1 {
        return Err(report!(
            "vortex row scan returned {} rows for row {row}",
            batch.num_rows()
        ));
    }
    let arrays = key_arrays_by_name(&batch, primary_keys)?;
    let kind = infer_pk_kind(&arrays, pk_types);
    Ok((encode_row(&arrays, pk_types)?, kind))
}

fn pk_indices(schema: &SchemaRef, primary_keys: &[String]) -> Result<Vec<usize>> {
    primary_keys
        .iter()
        .map(|name| {
            schema
                .index_of(name)
                .map_err(|error| report!("primary key {name} missing: {error}"))
        })
        .collect()
}

fn key_arrays_by_name(
    batch: &RecordBatch,
    primary_keys: &[String],
) -> Result<Vec<ArrayRef>> {
    primary_keys
        .iter()
        .map(|name| {
            batch
                .schema()
                .index_of(name)
                .map(|index| Arc::clone(batch.column(index)))
                .map_err(|error| report!("primary key {name} missing: {error}"))
        })
        .collect()
}

fn encode_row(arrays: &[ArrayRef], pk_types: &[DataType]) -> Result<PkKey> {
    let arrays = arrays
        .iter()
        .zip(pk_types)
        .map(|(array, dtype)| {
            if array.data_type() == dtype {
                Ok(Arc::clone(array))
            } else {
                cast(array.as_ref(), dtype)
                    .map_err(|error| report!("cast primary key to {dtype}: {error}"))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let converter = RowConverter::new(
        pk_types
            .iter()
            .cloned()
            .map(SortField::new)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| report!("create row converter: {error}"))?;
    let rows = converter
        .convert_columns(&arrays)
        .map_err(|error| report!("encode primary key row: {error}"))?;
    Ok(rows.row(0).data().to_vec())
}

/// Classify the primary key from the first row's values.
fn infer_pk_kind(arrays: &[ArrayRef], pk_types: &[DataType]) -> PkKind {
    if pk_types.len() != 1 {
        return PkKind::Other;
    }
    if is_numeric_type(&pk_types[0]) {
        return PkKind::Numeric;
    }
    let Some(array) = arrays.first() else {
        return PkKind::Other;
    };
    let text = match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(0),
        DataType::LargeUtf8 => array.as_string::<i64>().value(0),
        DataType::Utf8View => array.as_string_view().value(0),
        _ => return PkKind::Other,
    };
    if text.len() == 32 && text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        PkKind::FixedHex
    } else {
        PkKind::Other
    }
}

fn kind_from_types(pk_types: &[DataType]) -> PkKind {
    if pk_types.len() == 1 && is_numeric_type(&pk_types[0]) {
        PkKind::Numeric
    } else {
        PkKind::Other
    }
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
    )
}
