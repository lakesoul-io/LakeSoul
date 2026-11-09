// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Stored-vector payload cache for exact re-ranking.
//!
//! Exact re-rank has to read the *stored* vector of every candidate row.  In a
//! Vortex/Parquet data file the vector column is stored chunk by chunk, and a
//! chunk holds thousands of rows, so a few hundred random candidate rows
//! decode the whole column.  With four hash buckets that is close to a
//! gigabyte of decoding per query, which is what keeps exact re-rank at ~16
//! QPS and a ~1 GB peak RSS (see `向量查询优化日志.md` R39).
//!
//! This cache writes the same vector column, row by row, into one local file
//! per (data file, column) and reads candidate rows from it with `pread`.  A
//! candidate fetch then copies only the candidates' bytes (1000 rows x 800 B =
//! 800 KB) instead of decoding the column, so the fetch drops from tens of
//! milliseconds to about a millisecond, and the process RSS stays at the index
//! size because the read data lives in the page cache instead of a mapping.
//!
//! The primary key columns come from the locator's own mmapped key index (the
//! keys there are `arrow-row` encoded, which decodes straight back to Arrow
//! arrays), so a candidate batch can be produced without opening the data file
//! at all.
//!
//! It is a pure derived cache: data files are immutable, the entry name hashes
//! the data-file location, and the header repeats both that fingerprint and
//! the column layout, so a stale or colliding entry is detected instead of
//! misread.  Nothing about the table, its snapshots, partitions, merge-on-read
//! or commit semantics changes; when the cache is disabled, unsupported, or an
//! entry cannot be (re)built, the locator falls back to the regular row take.
//!
//! Environment:
//!
//! * `LAKESOUL_VECTOR_RERANK_CACHE` - `1`/`true`/`yes`/`on` enables the cache,
//!   anything else disables it.  Unset means "enabled when
//!   `LAKESOUL_VECTOR_RERANK_CACHE_DIR` names a directory".
//!   Disabled by default: the first exact re-rank of a file then pays for one
//!   full read of its vector columns and writes the payload to local disk.
//! * `LAKESOUL_VECTOR_RERANK_CACHE_DIR` - cache directory.  Defaults to
//!   `${LAKESOUL_CACHE_PATH}/rerank-payload` when the shared cache path is
//!   configured, else `${TMPDIR:-/tmp}/lakesoul-rerank-payload`.
//! * `LAKESOUL_VECTOR_RERANK_CACHE_BYTES` - total budget of the directory,
//!   default 16 GiB.  When a new payload would exceed it, the oldest local
//!   payload files are evicted first; only a payload larger than the whole
//!   budget falls back to the regular row take.
//! * `LAKESOUL_PAYLOAD_ROW_CACHE_BYTES` - optional in-memory cache of
//!   individual candidate rows (default 0 = disabled).  Rows are cached in
//!   their raw little-endian form, so a cache hit avoids one SSD `pread`
//!   without fetching a whole block and amplifying random reads.
//!
//! Delete the directory to reclaim the space; entries are rebuilt on demand.

use std::hash::{Hash, Hasher};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, Float32Array, Float64Array,
    LargeListArray, ListArray,
};
use arrow::buffer::{Buffer as ArrowBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, FieldRef, Float32Type, Float64Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use futures::StreamExt;
use moka::future::Cache;
use object_store::path::Path as StorePath;
use tracing::debug;
use vortex::array::VortexSessionExecute;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::FieldNames;
use vortex::expr::{root, select};
use vortex::file::VortexFile;
use vortex::session::VortexSession;

use super::{CachedFile, FileSource, MmapIndex, index_source_hash};

const ENV_ENABLED: &str = "LAKESOUL_VECTOR_RERANK_CACHE";
const ENV_DIR: &str = "LAKESOUL_VECTOR_RERANK_CACHE_DIR";
const ENV_BYTES: &str = "LAKESOUL_VECTOR_RERANK_CACHE_BYTES";
const ENV_CACHE_PATH: &str = "LAKESOUL_CACHE_PATH";
const ENV_ROW_CACHE_BYTES: &str = "LAKESOUL_PAYLOAD_ROW_CACHE_BYTES";

/// Header of one payload file (little-endian).
///
/// ```text
/// magic 8B | version u32 | columns u32 | rows u64 | total_len u64
/// kind_hash u64 | source_hash u64 | reserved u64 | reserved u64
/// descriptor x columns: dim u32 | elem u8 | pad 3B | offset u64
/// sections: one aligned run of `rows * dim * elem_size` bytes per column
/// ```
const PAYLOAD_MAGIC: &[u8; 8] = b"LSRPAY01";
const PAYLOAD_VERSION: u32 = 1;
const PAYLOAD_HEADER_LEN: usize = 64;
const PAYLOAD_DESC_LEN: usize = 16;
const PAYLOAD_ALIGN: u64 = 64;

/// Payload formats: the element width stored on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PayloadElem {
    F32,
    F64,
}

impl PayloadElem {
    /// The element type of a stored float list column.
    fn of_field(field: &FieldRef) -> Option<Self> {
        match field.data_type() {
            DataType::Float32 => Some(PayloadElem::F32),
            DataType::Float64 => Some(PayloadElem::F64),
            _ => None,
        }
    }

    fn tag(self) -> u8 {
        match self {
            PayloadElem::F32 => 0,
            PayloadElem::F64 => 1,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0 => Some(PayloadElem::F32),
            1 => Some(PayloadElem::F64),
            _ => None,
        }
    }

    fn size(self) -> usize {
        match self {
            PayloadElem::F32 => 4,
            PayloadElem::F64 => 8,
        }
    }
}

/// One stored vector column of a payload entry: its name and element width.
/// The vector length is read from the payload header (it is only needed once,
/// when the payload is built), so it is not part of the cache identity.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ColSpec {
    name: String,
    elem: PayloadElem,
}

/// A projected column that the payload fast path can produce.
#[derive(Clone, Copy, Debug)]
enum Slot {
    /// Position of the column in the locator's key layout.
    Key(usize),
    /// Position of the column in the payload's vector column list.
    Vector(usize),
}

static HITS: AtomicU64 = AtomicU64::new(0);
static BUILDS: AtomicU64 = AtomicU64::new(0);

/// `(hits, builds)` since process start: entries served and entries built.
pub fn stats() -> (u64, u64) {
    (
        HITS.load(AtomicOrdering::Relaxed),
        BUILDS.load(AtomicOrdering::Relaxed),
    )
}

/// Open entries, keyed by path: the file handle is shared between queries.
static VIEWS: LazyLock<Cache<String, Arc<PayloadView>>> =
    LazyLock::new(|| Cache::builder().max_capacity(256).build());

/// Optional L1 cache of individual payload rows.
///
/// Keyed by `(source hash, kind hash, column position, row id)`; values are
/// the raw little-endian row bytes (800 B for a 200d f32 vector).  The row
/// cache is deliberately row-granular: a block cache would fetch a whole
/// block for each random candidate and amplify reads by orders of magnitude.
static ROW_CACHE: LazyLock<Option<moka::sync::Cache<(u64, u64, u64, u64), Arc<[u8]>>>> =
    LazyLock::new(|| {
        let capacity = std::env::var(ENV_ROW_CACHE_BYTES)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0);
        let capacity = capacity?;
        metrics::describe_counter!(
            "lakesoul_payload_row_cache_hits_total",
            "Payload candidate rows served from the in-memory row cache"
        );
        metrics::describe_counter!(
            "lakesoul_payload_row_cache_misses_total",
            "Payload candidate rows that missed the in-memory row cache"
        );
        Some(
            moka::sync::Cache::builder()
                .max_capacity(capacity.div_ceil(1024))
                .weigher(|_key: &(u64, u64, u64, u64), value: &Arc<[u8]>| {
                    (value.len().div_ceil(1024)).min(u32::MAX as usize) as u32
                })
                .build(),
        )
    });

type RowKey = (u64, u64, u64, u64);

/// Builds are one-shot per file; serialising them keeps a burst of concurrent
/// queries from reading the same vector column several times at once.
static BUILD_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

/// Resolved cache configuration.
struct PayloadConfig {
    root: PathBuf,
    budget_bytes: u64,
}

/// Whether the payload cache is enabled.
///
/// An explicit `LAKESOUL_VECTOR_RERANK_CACHE` decides; without it, setting
/// `LAKESOUL_VECTOR_RERANK_CACHE_DIR` is an explicit opt-in of its own (a
/// deployment that names the directory clearly wants the cache, and forgetting
/// the flag costs 30-370x on exact re-rank queries).  No other environment
/// variable enables it: the payload of a large table is a real disk footprint,
/// so it is only written where someone asked for it.
fn enabled_by_env() -> bool {
    match std::env::var(ENV_ENABLED) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => std::env::var(ENV_DIR).is_ok_and(|dir| !dir.trim().is_empty()),
    }
}

fn payload_config() -> Option<PayloadConfig> {
    if !enabled_by_env() {
        return None;
    }
    let root = match std::env::var(ENV_DIR) {
        Ok(dir) if !dir.trim().is_empty() => PathBuf::from(dir),
        _ => match std::env::var(ENV_CACHE_PATH) {
            Ok(dir) if !dir.trim().is_empty() => {
                PathBuf::from(dir).join("rerank-payload")
            }
            _ => std::env::temp_dir().join("lakesoul-rerank-payload"),
        },
    };
    let budget_bytes = std::env::var(ENV_BYTES)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16 * 1024 * 1024 * 1024);
    Some(PayloadConfig { root, budget_bytes })
}

/// The element type of a stored float-list column (`List`, `LargeList` or
/// `FixedSizeList` of `Float32`/`Float64`).
fn payload_elem(data_type: &DataType) -> Option<PayloadElem> {
    match data_type {
        DataType::FixedSizeList(field, dim) if *dim > 0 => PayloadElem::of_field(field),
        DataType::List(field) | DataType::LargeList(field) => {
            PayloadElem::of_field(field)
        }
        _ => None,
    }
}

fn payload_kind_hash(columns: &[ColSpec]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    "lakesoul-rerank-payload-v1".hash(&mut hasher);
    for column in columns {
        column.name.hash(&mut hasher);
        column.elem.tag().hash(&mut hasher);
    }
    hasher.finish()
}

fn align_up(value: u64) -> u64 {
    (value + PAYLOAD_ALIGN - 1) / PAYLOAD_ALIGN * PAYLOAD_ALIGN
}

/// Number of vector elements in row `row` of a stored vector column.
fn row_len(array: &ArrayRef, row: usize) -> Option<usize> {
    match array.data_type() {
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>()?;
            Some(list.value_length() as usize)
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            let offsets = list.value_offsets();
            let start = *offsets.get(row)?;
            let end = *offsets.get(row + 1)?;
            Some((end - start) as usize)
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>()?;
            let offsets = list.value_offsets();
            let start = *offsets.get(row)?;
            let end = *offsets.get(row + 1)?;
            Some((end - start) as usize)
        }
        _ => None,
    }
}

/// The values of one chunk of a stored vector column, as one contiguous run.
///
/// List columns are only accepted when every row of the chunk holds exactly
/// the column's vector length and none of the rows is null: the payload layout
/// has a fixed row stride, and the fallback row take covers anything else.
fn chunk_values(array: &ArrayRef, rows: usize, dim: usize) -> Option<ArrayRef> {
    match array.data_type() {
        DataType::FixedSizeList(_, _) => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>()?;
            if list.value_length() as usize != dim || list.null_count() != 0 {
                return None;
            }
            let first = list.value_offset(0) as usize;
            let count = rows * dim;
            if first + count > list.values().len() {
                return None;
            }
            Some(list.values().slice(first, count))
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            if list.null_count() != 0 {
                return None;
            }
            let offsets = list.value_offsets();
            let first = *offsets.first()? as usize;
            let last = *offsets.get(rows)? as usize;
            if last.checked_sub(first)? != rows * dim {
                return None;
            }
            Some(list.values().slice(first, rows * dim))
        }
        DataType::LargeList(_) => {
            let list = array.as_any().downcast_ref::<LargeListArray>()?;
            if list.null_count() != 0 {
                return None;
            }
            let offsets = list.value_offsets();
            let first = *offsets.first()? as usize;
            let last = *offsets.get(rows)? as usize;
            if last.checked_sub(first)? != rows * dim {
                return None;
            }
            Some(list.values().slice(first, rows * dim))
        }
        _ => None,
    }
}

fn row_cache_get(key: &RowKey) -> Option<Arc<[u8]>> {
    let cache = ROW_CACHE.as_ref()?;
    let value = cache.get(key)?;
    metrics::counter!("lakesoul_payload_row_cache_hits_total").increment(1);
    Some(value)
}

fn row_cache_put(key: RowKey, data: &[u8]) {
    let Some(cache) = ROW_CACHE.as_ref() else {
        return;
    };
    cache.insert(key, Arc::from(data));
}

/// Copy the requested rows of one stored column into a fresh Arrow array of
/// the projected column's type.
///
/// Rows are fetched with `pread` instead of a mapping: the candidate rows are
/// random, so an mmap faults in (and keeps resident) almost every page of the
/// file over a few hundred queries, while a read only copies the 800 B the
/// query needs into the process.
fn take_vectors(
    file: &std::fs::File,
    rows: usize,
    offset: u64,
    elem: PayloadElem,
    dim: usize,
    field: &FieldRef,
    indices: &[u64],
    cache_id: RowKey,
) -> Option<ArrayRef> {
    let row_bytes = dim * elem.size();
    let values_len = indices.len().checked_mul(dim)?;
    let values: ArrayRef = match elem {
        PayloadElem::F32 => {
            let mut flat: Vec<f32> = vec![0.0; values_len];
            // SAFETY: `f32` has no invalid bit patterns and the buffer is
            // exactly `values_len * 4` bytes long, so viewing it as bytes is
            // sound (and the file is read little-endian on a little-endian
            // host).
            let dst = unsafe {
                std::slice::from_raw_parts_mut(
                    flat.as_mut_ptr() as *mut u8,
                    values_len * 4,
                )
            };
            for (position, index) in indices.iter().enumerate() {
                let index = *index as usize;
                if index >= rows {
                    return None;
                }
                let at = offset + (index * row_bytes) as u64;
                let target = &mut dst[position * dim * 4..(position + 1) * dim * 4];
                let key = (cache_id.0, cache_id.1, position as u64, index as u64);
                if let Some(row) = row_cache_get(&key) {
                    if row.len() != row_bytes {
                        return None;
                    }
                    target.copy_from_slice(&row);
                } else {
                    if ROW_CACHE.is_some() {
                        metrics::counter!("lakesoul_payload_row_cache_misses_total")
                            .increment(1);
                        row_cache_put(key, target);
                    }
                    file.read_exact_at(target, at).ok()?;
                }
            }
            Arc::new(Float32Array::from(flat))
        }
        PayloadElem::F64 => {
            let mut flat: Vec<f64> = vec![0.0; values_len];
            // SAFETY: as above for `f64`.
            let dst = unsafe {
                std::slice::from_raw_parts_mut(
                    flat.as_mut_ptr() as *mut u8,
                    values_len * 8,
                )
            };
            for (position, index) in indices.iter().enumerate() {
                let index = *index as usize;
                if index >= rows {
                    return None;
                }
                let at = offset + (index * row_bytes) as u64;
                let target = &mut dst[position * dim * 8..(position + 1) * dim * 8];
                let key = (cache_id.0, cache_id.1, position as u64, index as u64);
                if let Some(row) = row_cache_get(&key) {
                    if row.len() != row_bytes {
                        return None;
                    }
                    target.copy_from_slice(&row);
                } else {
                    if ROW_CACHE.is_some() {
                        metrics::counter!("lakesoul_payload_row_cache_misses_total")
                            .increment(1);
                        row_cache_put(key, target);
                    }
                    file.read_exact_at(target, at).ok()?;
                }
            }
            Arc::new(Float64Array::from(flat))
        }
    };
    let count = indices.len();
    // The list constructors take the *item* field: the list type itself is
    // rebuilt from it, so the result matches the projected column's type.
    match field.data_type() {
        DataType::FixedSizeList(item, size) => Some(Arc::new(FixedSizeListArray::new(
            Arc::clone(item),
            *size,
            values,
            None,
        ))),
        DataType::List(item) => {
            let offsets: Vec<i32> = (0..=count).map(|at| (at * dim) as i32).collect();
            Some(Arc::new(ListArray::new(
                Arc::clone(item),
                OffsetBuffer::new(offsets.into()),
                values,
                None,
            )))
        }
        DataType::LargeList(item) => {
            let offsets: Vec<i64> = (0..=count).map(|at| (at * dim) as i64).collect();
            Some(Arc::new(LargeListArray::new(
                Arc::clone(item),
                OffsetBuffer::new(offsets.into()),
                values,
                None,
            )))
        }
        _ => None,
    }
}

/// Squared L2 distance of two stored vectors, eight lanes at a time.
fn squared_distance_f32(stored: &[f32], query: &[f32]) -> f32 {
    if stored.len() != query.len() {
        return f32::INFINITY;
    }
    let mut lanes = [0.0f32; 8];
    let mut index = 0;
    while index + 8 <= stored.len() {
        for lane in 0..8 {
            let delta = stored[index + lane] - query[index + lane];
            lanes[lane] += delta * delta;
        }
        index += 8;
    }
    let mut sum: f32 = lanes.iter().sum();
    while index < stored.len() {
        let delta = stored[index] - query[index];
        sum += delta * delta;
        index += 1;
    }
    sum
}

/// Negated inner product, eight lanes at a time.
fn negated_dot_f32(stored: &[f32], query: &[f32]) -> f32 {
    if stored.len() != query.len() {
        return f32::NEG_INFINITY;
    }
    let mut lanes = [0.0f32; 8];
    let mut index = 0;
    while index + 8 <= stored.len() {
        for lane in 0..8 {
            lanes[lane] += stored[index + lane] * query[index + lane];
        }
        index += 8;
    }
    let mut sum: f32 = lanes.iter().sum();
    while index < stored.len() {
        sum += stored[index] * query[index];
        index += 1;
    }
    -sum
}

/// A payload file: the stored vectors of one data file, in row order.
pub(crate) struct PayloadView {
    file: Arc<std::fs::File>,
    rows: usize,
    columns: Vec<ColSpec>,
    dims: Vec<usize>,
    offsets: Vec<u64>,
    /// Identity for the L1 row cache; the payload file is immutable.
    source_hash: u64,
    kind_hash: u64,
}

impl PayloadView {
    fn open(
        path: &Path,
        expected_rows: usize,
        kind_hash: u64,
        source_hash: u64,
        columns: &[ColSpec],
    ) -> Option<Self> {
        let file = Arc::new(std::fs::File::open(path).ok()?);
        let size = file.metadata().ok()?.len();
        let header_len = PAYLOAD_HEADER_LEN + columns.len() * PAYLOAD_DESC_LEN;
        if size < header_len as u64 {
            return None;
        }
        let mut header = vec![0u8; header_len];
        file.read_exact_at(&mut header, 0).ok()?;
        if &header[0..8] != PAYLOAD_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(header[8..12].try_into().ok()?);
        let stored_columns = u32::from_le_bytes(header[12..16].try_into().ok()?) as usize;
        let rows = u64::from_le_bytes(header[16..24].try_into().ok()?) as usize;
        let total_len = u64::from_le_bytes(header[24..32].try_into().ok()?);
        let stored_kind = u64::from_le_bytes(header[32..40].try_into().ok()?);
        let stored_source = u64::from_le_bytes(header[40..48].try_into().ok()?);
        if version != PAYLOAD_VERSION
            || stored_columns != columns.len()
            || rows != expected_rows
            || total_len != size
            || stored_kind != kind_hash
            || stored_source != source_hash
        {
            return None;
        }
        let mut dims = Vec::with_capacity(columns.len());
        let mut offsets = Vec::with_capacity(columns.len());
        for (position, column) in columns.iter().enumerate() {
            let at = PAYLOAD_HEADER_LEN + position * PAYLOAD_DESC_LEN;
            let dim = u32::from_le_bytes(header[at..at + 4].try_into().ok()?) as usize;
            let elem = PayloadElem::from_tag(header[at + 4])?;
            let offset = u64::from_le_bytes(header[at + 8..at + 16].try_into().ok()?);
            if dim == 0 || elem != column.elem {
                return None;
            }
            let end = offset.checked_add((rows * dim * elem.size()) as u64)?;
            if offset % PAYLOAD_ALIGN != 0 || end > size {
                return None;
            }
            dims.push(dim);
            offsets.push(offset);
        }
        Some(Self {
            file,
            rows,
            columns: columns.to_vec(),
            dims,
            offsets,
            source_hash,
            kind_hash,
        })
    }

    /// Exact L2/IP score of the given rows, read straight from the payload
    /// bytes.
    ///
    /// The batch re-rank reads hundreds of thousands of candidate rows; going
    /// through an Arrow array per take would allocate and copy one list buffer
    /// per query.  This scores the rows in place instead: one `pread` of the
    /// row's `dim` floats into a reusable buffer, then an eight-lane reduction.
    /// `None` when the payload cannot reproduce the row exactly.
    pub(crate) fn score_rows(
        &self,
        indices: &[u64],
        query: &[f32],
        ascending: bool,
    ) -> Option<Vec<f32>> {
        if self.columns.len() != 1 {
            return None;
        }
        let column = &self.columns[0];
        let dim = self.dims[0];
        if dim != query.len() {
            return None;
        }
        let row_bytes = dim * column.elem.size();
        let offset = self.offsets[0];
        let mut scores = Vec::with_capacity(indices.len());
        let mut buffer = vec![0u8; row_bytes];
        let mut stored: Vec<f32> = Vec::with_capacity(dim);
        for index in indices {
            let index = *index as usize;
            if index >= self.rows {
                return None;
            }
            let at = offset + (index * row_bytes) as u64;
            let key = (self.source_hash, self.kind_hash, 0, index as u64);
            if let Some(row) = row_cache_get(&key) {
                if row.len() != row_bytes {
                    return None;
                }
                buffer.copy_from_slice(&row);
            } else {
                metrics::counter!("lakesoul_payload_row_cache_misses_total").increment(1);
                self.file.read_exact_at(&mut buffer, at).ok()?;
                row_cache_put(key, &buffer);
            }
            stored.clear();
            match column.elem {
                PayloadElem::F32 => {
                    for value in buffer.chunks_exact(4) {
                        stored.push(f32::from_le_bytes([
                            value[0], value[1], value[2], value[3],
                        ]));
                    }
                }
                PayloadElem::F64 => {
                    for value in buffer.chunks_exact(8) {
                        stored.push(f64::from_le_bytes([
                            value[0], value[1], value[2], value[3], value[4], value[5],
                            value[6], value[7],
                        ]) as f32);
                    }
                }
            }
            scores.push(if ascending {
                squared_distance_f32(&stored, query)
            } else {
                negated_dot_f32(&stored, query)
            });
        }
        Some(scores)
    }

    pub(crate) fn take(
        &self,
        indices: &[u64],
        fields: &[FieldRef],
    ) -> Option<Vec<ArrayRef>> {
        if fields.len() != self.columns.len() {
            return None;
        }
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (position, field) in fields.iter().enumerate() {
            let column = &self.columns[position];
            arrays.push(take_vectors(
                &self.file,
                self.rows,
                self.offsets[position],
                column.elem,
                self.dims[position],
                field,
                indices,
                (self.source_hash, self.kind_hash, 0, 0),
            )?);
        }
        Some(arrays)
    }
}

/// The vector length of each column, read once from the first chunk when the
/// payload is built.
async fn peek_dims(
    file: &VortexFile,
    session: &VortexSession,
    columns: &[ColSpec],
) -> Option<Vec<usize>> {
    let projection = select(
        FieldNames::from_iter(columns.iter().map(|column| column.name.as_str())),
        root(),
    )
    .bind(file.dtype())
    .ok()?;
    let mut stream = file
        .scan()
        .ok()?
        .with_projection(projection)
        .into_array_stream()
        .ok()?;
    let mut ctx = session.create_execution_ctx();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.ok()?;
        let arrow = session.arrow().execute_arrow(chunk, None, &mut ctx).ok()?;
        let batch = RecordBatch::from(arrow.as_struct().clone());
        if batch.num_rows() == 0 {
            continue;
        }
        let mut dims = Vec::with_capacity(columns.len());
        for column in columns {
            let index = batch.schema().index_of(&column.name).ok()?;
            let dim = row_len(batch.column(index), 0)?;
            if dim == 0 {
                return None;
            }
            dims.push(dim);
        }
        return Some(dims);
    }
    None
}

/// Builds the payload file of one data file by streaming its vector columns.
#[allow(clippy::too_many_arguments)]
async fn build_payload(
    config: &PayloadConfig,
    file: &VortexFile,
    session: &VortexSession,
    location: &StorePath,
    columns: &[ColSpec],
    dims: &[usize],
    kind_hash: u64,
    source_hash: u64,
    path: &Path,
) -> Option<PayloadView> {
    let started = std::time::Instant::now();
    let rows = file.row_count() as usize;
    if rows == 0 || dims.len() != columns.len() {
        return None;
    }
    let mut offsets = Vec::with_capacity(columns.len());
    let mut total =
        align_up((PAYLOAD_HEADER_LEN + columns.len() * PAYLOAD_DESC_LEN) as u64);
    for (column, dim) in columns.iter().zip(dims) {
        offsets.push(total);
        total = align_up(total + (rows * dim * column.elem.size()) as u64);
    }
    if total > config.budget_bytes {
        debug!(
            "rerank payload for {location} needs {total} bytes, over the {} byte budget",
            config.budget_bytes
        );
        return None;
    }
    if let Some(usage) = directory_usage(&config.root)
        && usage.saturating_add(total) > config.budget_bytes
    {
        let freed = evict_to_fit(&config.root, total, config.budget_bytes);
        if freed == 0 {
            debug!(
                "rerank payload directory holds {usage} of {} bytes and cannot make room for {location}",
                config.budget_bytes
            );
            return None;
        }
        debug!(
            "evicted {freed} bytes from the rerank payload directory to fit {location}"
        );
    }
    std::fs::create_dir_all(&config.root).ok()?;
    let temp = config.root.join(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let mut out = std::fs::File::create(&temp).ok()?;
    if out.set_len(total).is_err() {
        let _ = std::fs::remove_file(&temp);
        return None;
    }

    let projection = select(
        FieldNames::from_iter(columns.iter().map(|column| column.name.as_str())),
        root(),
    )
    .bind(file.dtype())
    .ok()?;
    let mut stream = file
        .scan()
        .ok()?
        .with_projection(projection)
        .into_array_stream()
        .ok()?;
    let mut ctx = session.create_execution_ctx();
    let mut row_start = 0usize;
    let mut complete = true;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                debug!("failed to scan vector column of {location}: {error}");
                complete = false;
                break;
            }
        };
        let arrow = match session.arrow().execute_arrow(chunk, None, &mut ctx) {
            Ok(arrow) => arrow,
            Err(error) => {
                debug!("failed to decode vector column of {location}: {error}");
                complete = false;
                break;
            }
        };
        let batch = RecordBatch::from(arrow.as_struct().clone());
        let batch_rows = batch.num_rows();
        if batch_rows == 0 {
            continue;
        }
        for (position, (column, dim)) in columns.iter().zip(dims).enumerate() {
            let Some(index) = batch.schema().index_of(&column.name).ok() else {
                complete = false;
                break;
            };
            let Some(flat) = chunk_values(batch.column(index), batch_rows, *dim) else {
                complete = false;
                break;
            };
            let at = offsets[position] + (row_start * dim * column.elem.size()) as u64;
            let bytes = match column.elem {
                PayloadElem::F32 => {
                    let values = flat.as_primitive::<Float32Type>().values().as_ref();
                    ArrowBuffer::from_slice_ref(values)
                }
                PayloadElem::F64 => {
                    let values = flat.as_primitive::<Float64Type>().values().as_ref();
                    ArrowBuffer::from_slice_ref(values)
                }
            };
            if out.seek(SeekFrom::Start(at)).is_err()
                || out.write_all(bytes.as_slice()).is_err()
            {
                complete = false;
                break;
            }
        }
        if !complete {
            break;
        }
        row_start += batch_rows;
    }
    if !complete || row_start != rows {
        debug!("rerank payload for {location} is incomplete: {row_start}/{rows} rows");
        drop(out);
        let _ = std::fs::remove_file(&temp);
        return None;
    }

    let mut header = vec![0u8; PAYLOAD_HEADER_LEN + columns.len() * PAYLOAD_DESC_LEN];
    header[0..8].copy_from_slice(PAYLOAD_MAGIC);
    header[8..12].copy_from_slice(&PAYLOAD_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(columns.len() as u32).to_le_bytes());
    header[16..24].copy_from_slice(&(rows as u64).to_le_bytes());
    header[24..32].copy_from_slice(&total.to_le_bytes());
    header[32..40].copy_from_slice(&kind_hash.to_le_bytes());
    header[40..48].copy_from_slice(&source_hash.to_le_bytes());
    for (position, (column, dim)) in columns.iter().zip(dims).enumerate() {
        let at = PAYLOAD_HEADER_LEN + position * PAYLOAD_DESC_LEN;
        header[at..at + 4].copy_from_slice(&(*dim as u32).to_le_bytes());
        header[at + 4] = column.elem.tag();
        header[at + 8..at + 16].copy_from_slice(&offsets[position].to_le_bytes());
    }
    if out.seek(SeekFrom::Start(0)).is_err()
        || out.write_all(&header).is_err()
        || out.flush().is_err()
    {
        drop(out);
        let _ = std::fs::remove_file(&temp);
        return None;
    }
    drop(out);
    if std::fs::rename(&temp, path).is_err() {
        let _ = std::fs::remove_file(&temp);
        return None;
    }
    debug!(
        "built rerank payload for {location} ({} columns, dims {:?}, {rows} rows, {total} bytes) in {:?}",
        columns.len(),
        dims,
        started.elapsed()
    );
    PayloadView::open(path, rows, kind_hash, source_hash, columns)
}

fn directory_usage(root: &Path) -> Option<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(root).ok()?.flatten() {
        if let Ok(metadata) = entry.metadata()
            && metadata.is_file()
        {
            total = total.saturating_add(metadata.len());
        }
    }
    Some(total)
}

/// Delete the oldest payload files until `need` more bytes fit in `budget`.
///
/// The payload files are immutable derived data: removing one only makes the
/// next exact query rebuild it.  Open readers keep their file descriptor, so
/// a concurrent query that already opened the file is unaffected on Unix.
fn evict_to_fit(root: &Path, need: u64, budget: u64) -> u64 {
    let mut entries: Vec<(std::time::SystemTime, PathBuf, u64)> = Vec::new();
    let Ok(entries_iter) = std::fs::read_dir(root) else {
        return 0;
    };
    for entry in entries_iter.flatten() {
        let path = entry.path();
        if !path.is_file()
            || path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".tmp"))
        {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let modified = metadata.modified().unwrap_or(std::time::UNIX_EPOCH);
        entries.push((modified, path, metadata.len()));
    }
    entries.sort_by_key(|(modified, _, _)| *modified);
    let mut usage = entries.iter().map(|(_, _, size)| *size).sum::<u64>();
    let mut freed = 0u64;
    for (_modified, path, size) in entries {
        if usage.saturating_add(need) <= budget {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            usage = usage.saturating_sub(size);
            freed = freed.saturating_add(size);
            metrics::counter!("lakesoul_payload_evictions_total").increment(1);
            debug!(path = %path.display(), size, "evicted rerank payload file");
        }
    }
    freed
}

/// The payload of one data file, opened or built once.
async fn load_or_build(
    config: &PayloadConfig,
    file: &VortexFile,
    session: &VortexSession,
    location: &StorePath,
    columns: &[ColSpec],
    expected_rows: usize,
) -> Option<Arc<PayloadView>> {
    let kind_hash = payload_kind_hash(columns);
    let source_hash = index_source_hash(location);
    let path = config
        .root
        .join(format!("{source_hash:016x}-{kind_hash:016x}.payload"));
    let key = path.display().to_string();
    if let Some(view) = VIEWS.get(&key).await {
        HITS.fetch_add(1, AtomicOrdering::Relaxed);
        metrics::counter!("lakesoul_payload_cache_hits_total").increment(1);
        return Some(view);
    }
    let _guard = BUILD_LOCK.lock().await;
    if let Some(view) = VIEWS.get(&key).await {
        HITS.fetch_add(1, AtomicOrdering::Relaxed);
        metrics::counter!("lakesoul_payload_cache_hits_total").increment(1);
        return Some(view);
    }
    if let Some(view) =
        PayloadView::open(&path, expected_rows, kind_hash, source_hash, columns)
    {
        HITS.fetch_add(1, AtomicOrdering::Relaxed);
        metrics::counter!("lakesoul_payload_cache_hits_total").increment(1);
        let view = Arc::new(view);
        VIEWS.insert(key, Arc::clone(&view)).await;
        return Some(view);
    }
    if path.is_file() {
        debug!("discarding stale rerank payload {}", path.display());
        let _ = std::fs::remove_file(&path);
    }
    let dims = peek_dims(file, session, columns).await?;
    let view = Arc::new(
        build_payload(
            config,
            file,
            session,
            location,
            columns,
            &dims,
            kind_hash,
            source_hash,
            &path,
        )
        .await?,
    );
    BUILDS.fetch_add(1, AtomicOrdering::Relaxed);
    VIEWS.insert(key, Arc::clone(&view)).await;
    Some(view)
}

/// Candidate rows from the payload cache and the key index.
///
/// Succeeds only when every projected column is either a primary key column
/// covered by the locator's key layout or a stored vector column, so the
/// result is exactly the batch the regular row take would return.
pub(crate) async fn try_take_rows(
    entry: &CachedFile,
    location: &StorePath,
    indices: &[u64],
    projected_schema: &SchemaRef,
    projection_names: &[String],
    key_columns: &[String],
    key_types: &[DataType],
) -> Option<Vec<RecordBatch>> {
    let config = payload_config()?;
    if indices.is_empty() || key_columns.len() != key_types.len() {
        return None;
    }
    let FileSource::Vortex { file, session } = &entry.source else {
        return None;
    };
    // A file with several sorted runs stores explicit row ids: the payload is
    // written in physical row order, which is not the key order the merge
    // expects, so keep the regular take for those files.
    if entry.index.has_row_ids() {
        return None;
    }
    let rows = entry.index.rows;
    if indices.iter().any(|index| *index as usize >= rows) {
        return None;
    }
    let mut slots = Vec::with_capacity(projection_names.len());
    let mut vector_columns: Vec<ColSpec> = Vec::new();
    let mut vector_fields: Vec<FieldRef> = Vec::new();
    for (position, name) in projection_names.iter().enumerate() {
        let field = projected_schema.fields().get(position)?;
        if let Some(key) = key_columns.iter().position(|column| column == name) {
            if field.data_type() != &key_types[key] {
                return None;
            }
            slots.push(Slot::Key(key));
            continue;
        }
        let elem = match payload_elem(field.data_type()) {
            Some(elem) => elem,
            None => return None,
        };
        slots.push(Slot::Vector(vector_columns.len()));
        vector_fields.push(Arc::clone(field));
        vector_columns.push(ColSpec {
            name: name.clone(),
            elem,
        });
    }
    if vector_columns.is_empty() {
        return None;
    }
    let view = load_or_build(
        &config,
        file,
        session,
        location,
        &vector_columns,
        file.row_count() as usize,
    )
    .await?;
    let vectors = view.take(indices, &vector_fields)?;
    let key_arrays = decode_keys(&entry.index, indices, key_types)?;
    let mut columns = Vec::with_capacity(slots.len());
    for (position, slot) in slots.iter().enumerate() {
        let array = match slot {
            Slot::Key(key) => Arc::clone(&key_arrays[*key]),
            Slot::Vector(vector) => Arc::clone(&vectors[*vector]),
        };
        if array.data_type() != projected_schema.field(position).data_type() {
            return None;
        }
        columns.push(array);
    }
    let batch = RecordBatch::try_new(Arc::clone(projected_schema), columns).ok()?;
    Some(vec![batch])
}

/// Decode the arrow-row encoded keys of the given index entries back into
/// Arrow arrays, so the candidate batch does not have to read the file.
fn decode_keys(
    index: &MmapIndex,
    indices: &[u64],
    types: &[DataType],
) -> Option<Vec<ArrayRef>> {
    let converter = RowConverter::new(
        types
            .iter()
            .cloned()
            .map(SortField::new)
            .collect::<Vec<_>>(),
    )
    .ok()?;
    let parser = converter.parser();
    let rows = indices
        .iter()
        .map(|row| parser.parse(index.row(*row as usize)))
        .collect::<Vec<_>>();
    converter.convert_rows(rows).ok()
}

/// Whether the stored-vector payload cache is enabled.
pub fn enabled() -> bool {
    payload_config().is_some()
}

/// Open (build once) one vector column's payload for a data file.
///
/// The returned view serves rows by data-file row id, so a caller that reads
/// several candidate sets from one file only pays the open once.
pub(crate) async fn open_view(
    entry: &CachedFile,
    location: &StorePath,
    field: &FieldRef,
) -> Option<Arc<PayloadView>> {
    let config = payload_config()?;
    let FileSource::Vortex { file, session } = &entry.source else {
        return None;
    };
    if entry.index.has_row_ids() {
        return None;
    }
    let elem = payload_elem(field.data_type())?;
    let columns = vec![ColSpec {
        name: field.name().clone(),
        elem,
    }];
    load_or_build(
        &config,
        file,
        session,
        location,
        &columns,
        file.row_count() as usize,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The directory budget evicts old payload files instead of refusing the
    /// new payload outright.
    #[test]
    fn evicts_payloads_to_fit_budget() {
        let dir =
            std::env::temp_dir().join(format!("lsrpay-evict-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        for index in 0..3 {
            std::fs::write(dir.join(format!("{index}.payload")), vec![0u8; 1024])
                .unwrap();
        }
        let freed = evict_to_fit(&dir, 1024, 2048);
        assert!(freed > 0, "must evict at least one payload");
        let usage = directory_usage(&dir).unwrap();
        assert!(
            usage + 1024 <= 2048,
            "payload directory must fit the budget"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Write a two-column payload entry and read rows back through it.
    #[test]
    fn payload_round_trip() {
        let dir = std::env::temp_dir().join(format!("lsrpay-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("entry.payload");
        let columns = vec![
            ColSpec {
                name: "vec".to_string(),
                elem: PayloadElem::F32,
            },
            ColSpec {
                name: "other".to_string(),
                elem: PayloadElem::F64,
            },
        ];
        let dims = [3usize, 2usize];
        let rows = 3usize;
        let mut offsets = Vec::new();
        let mut total =
            align_up((PAYLOAD_HEADER_LEN + columns.len() * PAYLOAD_DESC_LEN) as u64);
        for (column, dim) in columns.iter().zip(&dims) {
            offsets.push(total);
            total = align_up(total + (rows * dim * column.elem.size()) as u64);
        }
        let kind_hash = payload_kind_hash(&columns);
        let source_hash = 0x1234_u64;
        let mut file = std::fs::File::create(&path).unwrap();
        file.set_len(total).unwrap();
        let mut header = vec![0u8; PAYLOAD_HEADER_LEN + columns.len() * PAYLOAD_DESC_LEN];
        header[0..8].copy_from_slice(PAYLOAD_MAGIC);
        header[8..12].copy_from_slice(&PAYLOAD_VERSION.to_le_bytes());
        header[12..16].copy_from_slice(&(columns.len() as u32).to_le_bytes());
        header[16..24].copy_from_slice(&(rows as u64).to_le_bytes());
        header[24..32].copy_from_slice(&total.to_le_bytes());
        header[32..40].copy_from_slice(&kind_hash.to_le_bytes());
        header[40..48].copy_from_slice(&source_hash.to_le_bytes());
        for (position, (column, dim)) in columns.iter().zip(&dims).enumerate() {
            let at = PAYLOAD_HEADER_LEN + position * PAYLOAD_DESC_LEN;
            header[at..at + 4].copy_from_slice(&(*dim as u32).to_le_bytes());
            header[at + 4] = column.elem.tag();
            header[at + 8..at + 16].copy_from_slice(&offsets[position].to_le_bytes());
        }
        let mut body: Vec<f32> = Vec::new();
        for row in 0..rows {
            for at in 0..dims[0] {
                body.push((row * 10 + at) as f32);
            }
        }
        file.seek(SeekFrom::Start(offsets[0])).unwrap();
        for value in &body {
            file.write_all(&value.to_le_bytes()).unwrap();
        }
        let mut other: Vec<f64> = Vec::new();
        for row in 0..rows {
            for at in 0..dims[1] {
                other.push((row * 10 + at) as f64 + 0.5);
            }
        }
        file.seek(SeekFrom::Start(offsets[1])).unwrap();
        for value in &other {
            file.write_all(&value.to_le_bytes()).unwrap();
        }
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&header).unwrap();
        drop(file);

        let view = PayloadView::open(&path, rows, kind_hash, source_hash, &columns)
            .expect("payload opens");
        let vec_field = Arc::new(arrow::datatypes::Field::new(
            "vec",
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Float32,
                true,
            ))),
            false,
        ));
        let other_field = Arc::new(arrow::datatypes::Field::new(
            "other",
            DataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Float64,
                    true,
                )),
                2,
            ),
            false,
        ));
        let arrays = view
            .take(&[2, 0], &[vec_field, other_field])
            .expect("rows are served");
        assert_eq!(arrays[0].len(), 2);
        assert_eq!(arrays[1].len(), 2);
        // Rows come back in the requested order: 2 then 0.
        let list = arrays[0].as_any().downcast_ref::<ListArray>().unwrap();
        let first = list.value(0);
        let first = first.as_primitive::<Float32Type>();
        assert_eq!(first.values().as_ref(), &[20.0f32, 21.0, 22.0]);
        let second = list.value(1);
        let second = second.as_primitive::<Float32Type>();
        assert_eq!(second.values().as_ref(), &[0.0f32, 1.0, 2.0]);
        let fixed = arrays[1]
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        let row = fixed.value(0);
        let row = row.as_primitive::<Float64Type>();
        assert_eq!(row.values().as_ref(), &[20.5f64, 21.5]);

        // A different row count or layout is rejected instead of misread.
        assert!(
            PayloadView::open(&path, rows + 1, kind_hash, source_hash, &columns)
                .is_none()
        );
        assert!(
            PayloadView::open(&path, rows, kind_hash + 1, source_hash, &columns)
                .is_none()
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
