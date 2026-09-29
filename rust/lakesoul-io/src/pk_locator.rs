// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Row-level primary-key locator.
//!
//! `pk = value` and `pk IN (...)` predicates are answered by locating the
//! matching rows through a per-file sorted index instead of scanning every
//! file.  A finite candidate set can also be built for a *prefix* of a
//! composite key (`g = 'x'` on a `(g, value)` key), which covers the group
//! filters of incremental aggregation.  Keys may repeat (a non-unique
//! column), and a prefix lookup returns every row whose key starts with it.
//!
//! Keys are encoded with `arrow-row` (the same order-preserving encoding the
//! writer uses to sort files) and stored as a sorted array in a local file
//! that is `mmap`ed: the index lives in the page cache instead of the heap,
//! is reclaimable under memory pressure and is shared between the queries of
//! a process.  One entry is stored per data row, so row ids are the entry
//! positions and a lookup returns a contiguous range.
//!
//! The index is built lazily from the file's key columns and stored in the
//! process-wide object-store disk cache (see `crate::cache`): it shares the
//! cache directory, capacity and eviction policy, so there is no separate
//! switch or budget.  The cache is enabled with `LAKESOUL_CACHE`; its size is
//! `LAKESOUL_CACHE_SIZE`.  Data files are immutable, so an entry never goes
//! stale (a rewritten or compacted file gets a new location and therefore a
//! new entry).
//!
//! Only vortex data files are supported; anything else (or unsupported column
//! types) falls back to the regular scan path.  The candidate set is always a
//! superset of the rows matching the full predicate: only key prefixes are
//! used, because dropping a payload-matching row could drop the latest version
//! of a key during merge-on-read.
//!
//! The index assumes what LakeSoul's sorted writer guarantees: rows of a data
//! file are stored in primary-key order, which is re-checked while building.
//! Rows are fetched by row index in ascending order, which therefore yields
//! batches sorted by primary key as the merge-on-read path requires.

use std::collections::HashSet;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{ScalarValue, stats::Precision};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_expr::{Expr, Operator};
use futures::StreamExt;
use memmap2::Mmap;
use moka::future::Cache;
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use smallvec::SmallVec;
use tracing::{debug, warn};
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::array::memory::MemorySessionExt;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::Buffer;
use vortex::dtype::FieldNames;
use vortex::expr::{root, select};
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex::session::VortexSession;

use crate::cache::get_lakesoul_cache;
use crate::config::LakeSoulIOConfig;
use crate::file_format::PhysicalFormat;

/// Above this many candidate keys the locator is not worth it: the caller
/// is better off scanning.
pub const MAX_PK_CANDIDATES: usize = 10_000;

/// When the candidate files of a query hold fewer rows than this in total, a
/// plain scan is cheaper than building local indexes.  The decision spans all
/// files of the query: a table of many small files (IVM writes several files
/// per append, one per writer partition) is still indexed, while a handful of
/// tiny upsert files is scanned.
const MIN_INDEX_ROWS: u64 = 4_096;

/// A single index may take at most `1 / MAX_INDEX_CACHE_RATIO` of the shared
/// local cache capacity, so one huge file cannot evict everything else.
const MAX_INDEX_CACHE_RATIO: u64 = 8;

/// Environment variable that switches on the shared local disk cache.  The pk
/// index has no switch of its own: it shares the cache instance, directory and
/// capacity of the object-store page cache (`LAKESOUL_CACHE_SIZE`).
const ENV_CACHE_ENABLED: &str = "LAKESOUL_CACHE";

static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// `(hits, misses)` of the local pk index cache since process start.
///
/// A hit means an index was reused from the local disk cache; a miss means it
/// was rebuilt from the file's key columns and inserted.
pub fn cache_stats() -> (u64, u64) {
    (
        HITS.load(AtomicOrdering::Relaxed),
        MISSES.load(AtomicOrdering::Relaxed),
    )
}

/// Opened vortex files, keyed by location: opening reads the footer and layout
/// over the object store, so it is worth amortizing across queries.  The index
/// itself lives in the shared disk cache; this only keeps handles.
static VORTEX_FILES: LazyLock<Cache<String, Arc<VortexFile>>> =
    LazyLock::new(|| Cache::builder().max_capacity(1024).build());

/// Files whose index was rejected (unsorted, oversized, ...).  Remembering the
/// rejection avoids re-reading the key columns on every query; the TTL keeps a
/// transient build failure from disabling the file forever.
static REJECTED: LazyLock<Cache<String, ()>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_live(std::time::Duration::from_secs(300))
        .build()
});

/// The shared local disk cache, when enabled by `LAKESOUL_CACHE`.
fn index_disk_cache() -> Option<Arc<crate::cache::disk_cache::DiskCache>> {
    if std::env::var(ENV_CACHE_ENABLED).is_err() {
        return None;
    }
    let cache = get_lakesoul_cache();
    cache.is_enabled().then_some(cache)
}

/// A scalar value of a per-file key.
///
/// The derived ordering is only used within one column family (and to keep
/// equal prefixes adjacent), never across families.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum KeyValue {
    /// A boolean.
    Bool(bool),
    /// A signed integer (any width).
    Int(i64),
    /// An unsigned integer (any width).
    UInt(u64),
    /// A float, stored as canonicalised bits so `0.0`/`-0.0` compare equal.
    Float(u64),
    /// A UTF-8 string.
    Utf8(Box<str>),
    /// A binary value.
    Binary(Box<[u8]>),
    /// A 128-bit decimal.
    Decimal(i128),
}

impl KeyValue {
    /// The value as `i64` when it is an integer.
    fn single_int(&self) -> Option<i64> {
        match self {
            KeyValue::Int(value) => Some(*value),
            KeyValue::UInt(value) => i64::try_from(*value).ok(),
            _ => None,
        }
    }
}

/// A composite key over the pinned prefix of the primary keys.
pub type Key = SmallVec<[KeyValue; 2]>;

/// Format of the on-disk index.  Bump [`INDEX_VERSION`] when the layout
/// changes; old entries are then rebuilt.
const INDEX_MAGIC: &[u8; 8] = b"LSMKEY01";
const INDEX_VERSION: u32 = 1;
const INDEX_HEADER_LEN: usize = 48;

/// A mmapped, sorted array of arrow-Row encoded keys, one entry per data row.
///
/// Layout (little-endian):
///
/// ```text
/// header:  magic 8B | version u32 | flags u32 | rows u64 | blob_len u64 | kind hash u64 | source hash u64
/// blob:    row 0 bytes | row 1 bytes | ...
/// offsets: start of each row inside the blob, u32 x (rows + 1)
/// ```
///
/// The writer stores data rows in primary-key order (re-checked while
/// building), so entry `i` belongs to row `i` and row ids are implicit: a
/// lookup returns a contiguous range of entry positions.  A composite key is
/// the concatenation of its arrow-Row encodings, which makes every key prefix
/// a byte prefix of the full key.
pub struct MmapIndex {
    mmap: Mmap,
    rows: usize,
    blob_at: usize,
    offsets_at: usize,
}

impl MmapIndex {
    fn open(
        file: Arc<std::fs::File>,
        size: u64,
        kind_hash: u64,
        source_hash: u64,
    ) -> Option<Self> {
        if size < INDEX_HEADER_LEN as u64 {
            return None;
        }
        let mmap = unsafe { Mmap::map(&file) }.ok()?;
        if &mmap[0..8] != INDEX_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(mmap[8..12].try_into().ok()?);
        let rows = u64::from_le_bytes(mmap[16..24].try_into().ok()?) as usize;
        let blob_len = u64::from_le_bytes(mmap[24..32].try_into().ok()?);
        let stored_hash = u64::from_le_bytes(mmap[32..40].try_into().ok()?);
        let stored_source = u64::from_le_bytes(mmap[40..48].try_into().ok()?);
        if version != INDEX_VERSION
            || stored_hash != kind_hash
            || stored_source != source_hash
        {
            return None;
        }
        if INDEX_HEADER_LEN as u64 + blob_len + 4 * (rows as u64 + 1) != size {
            return None;
        }
        let offsets_at = INDEX_HEADER_LEN + blob_len as usize;
        let last = u32::from_le_bytes(
            mmap[offsets_at + 4 * rows..offsets_at + 4 * rows + 4]
                .try_into()
                .ok()?,
        );
        if u64::from(last) != blob_len {
            return None;
        }
        Some(Self {
            mmap,
            rows,
            blob_at: INDEX_HEADER_LEN,
            offsets_at,
        })
    }

    fn offset(&self, index: usize) -> usize {
        let at = self.offsets_at + 4 * index;
        u32::from_le_bytes(self.mmap[at..at + 4].try_into().unwrap()) as usize
    }

    fn row(&self, index: usize) -> &[u8] {
        let start = self.blob_at + self.offset(index);
        let end = self.blob_at + self.offset(index + 1);
        &self.mmap[start..end]
    }

    /// Index of the first row whose key is not less than `key`.
    fn lower_bound(&self, key: &[u8]) -> usize {
        let (mut low, mut high) = (0usize, self.rows);
        while low < high {
            let mid = low + (high - low) / 2;
            if self.row(mid) < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low
    }

    /// The contiguous rows whose key starts with `prefix`.
    fn rows_with_prefix(&self, prefix: &[u8]) -> std::ops::Range<usize> {
        let start = self.lower_bound(prefix);
        let (mut low, mut high) = (start, self.rows);
        while low < high {
            let mid = low + (high - low) / 2;
            if self.row(mid).starts_with(prefix) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        start..low
    }
}

/// Streaming writer of a [`MmapIndex`] file.
struct IndexWriter {
    file: BufWriter<std::fs::File>,
    kind_hash: u64,
    source_hash: u64,
    offsets: Vec<u32>,
    blob_len: u64,
    rows: u64,
    previous: Option<Vec<u8>>,
    max_bytes: u64,
}

impl IndexWriter {
    fn create(
        path: &std::path::Path,
        kind_hash: u64,
        source_hash: u64,
        max_bytes: u64,
    ) -> crate::Result<Self> {
        let mut file = std::fs::File::create(path)
            .map_err(|error| rootcause::report!("failed to create pk index: {error}"))?;
        file.write_all(&[0u8; INDEX_HEADER_LEN])
            .map_err(|error| rootcause::report!("failed to write pk index: {error}"))?;
        Ok(Self {
            file: BufWriter::with_capacity(1024 * 1024, file),
            kind_hash,
            source_hash,
            offsets: vec![0],
            blob_len: 0,
            rows: 0,
            previous: None,
            max_bytes,
        })
    }

    fn push(&mut self, key: &[u8]) -> crate::Result<()> {
        if let Some(previous) = &self.previous
            && key < previous.as_slice()
        {
            return Err(rootcause::report!("file keys are not sorted"));
        }
        if self.rows >= u32::MAX as u64
            || self.blob_len + key.len() as u64 > u32::MAX as u64
        {
            return Err(rootcause::report!("file is too large for a pk index"));
        }
        let size = INDEX_HEADER_LEN as u64
            + self.blob_len
            + key.len() as u64
            + 4 * (self.rows + 2);
        if size > self.max_bytes {
            return Err(rootcause::report!(
                "pk index needs {size} bytes, over the local cache budget of {}",
                self.max_bytes
            ));
        }
        self.file
            .write_all(key)
            .map_err(|error| rootcause::report!("failed to write pk index: {error}"))?;
        self.blob_len += key.len() as u64;
        self.offsets.push(self.blob_len as u32);
        self.rows += 1;
        match &mut self.previous {
            Some(previous) => {
                previous.clear();
                previous.extend_from_slice(key);
            }
            None => self.previous = Some(key.to_vec()),
        }
        Ok(())
    }

    fn finish(mut self) -> crate::Result<u64> {
        let rows = self.rows;
        let blob_len = self.blob_len;
        for offset in &self.offsets {
            self.file
                .write_all(&offset.to_le_bytes())
                .map_err(|error| {
                    rootcause::report!("failed to write pk index: {error}")
                })?;
        }
        let mut header = [0u8; INDEX_HEADER_LEN];
        header[0..8].copy_from_slice(INDEX_MAGIC);
        header[8..12].copy_from_slice(&INDEX_VERSION.to_le_bytes());
        header[16..24].copy_from_slice(&rows.to_le_bytes());
        header[24..32].copy_from_slice(&blob_len.to_le_bytes());
        header[32..40].copy_from_slice(&self.kind_hash.to_le_bytes());
        header[40..48].copy_from_slice(&self.source_hash.to_le_bytes());
        let mut file = self
            .file
            .into_inner()
            .map_err(|error| rootcause::report!("failed to finish pk index: {error}"))?;
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&header))
            .and_then(|_| file.flush())
            .map_err(|error| rootcause::report!("failed to finish pk index: {error}"))?;
        file.metadata()
            .map(|metadata| metadata.len())
            .map_err(|error| rootcause::report!("failed to stat pk index: {error}"))
    }
}

/// Cached per-file state: the mmapped index plus the opened vortex handle.
struct CachedFile {
    index: Arc<MmapIndex>,
    file: Arc<VortexFile>,
}

/// A finite candidate set on a prefix of a table's primary keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyConstraint {
    /// The pinned prefix of the primary keys, in key order.
    pub columns: Vec<String>,
    /// The candidate composite values of that prefix.
    pub values: Vec<Key>,
}

impl KeyConstraint {
    /// The candidates of a single integer column, for statistics pruning.
    fn single_column_ints(&self) -> Option<Vec<i64>> {
        if self.columns.len() != 1 {
            return None;
        }
        self.values
            .iter()
            .map(|value| value.first()?.single_int())
            .collect()
    }
}

/// Extract a finite candidate set from logical filters.
///
/// The candidate set is always a superset of the rows matching the full
/// filter, which is what row-level pruning needs (the caller keeps applying
/// the real filter above the scan).  Only prefixes of the primary keys are
/// considered: payload predicates cannot be pushed into a merge-on-read scan,
/// because dropping a row could drop the latest version of a wanted key.
///
/// `pk = literal`, `pk IN (literals)` and `pk = a OR pk = b` are recognised
/// per column (all `OR` branches must constrain the same column), conjuncts
/// are intersected and the longest prefix with a finite set on every column is
/// used (the cross product is capped at [`MAX_PK_CANDIDATES`]).  Returns
/// `None` when no prefix can be constrained.
pub fn extract_key_constraints(
    filters: &[Expr],
    primary_keys: &[(String, DataType)],
) -> Option<KeyConstraint> {
    if primary_keys.is_empty() {
        return None;
    }
    let mut sets = Vec::with_capacity(primary_keys.len());
    for (column, data_type) in primary_keys {
        sets.push(finite_key_set(filters, column, data_type));
    }
    let mut prefix_len = 0;
    for set in &sets {
        if set.is_none() {
            break;
        }
        prefix_len += 1;
    }
    if prefix_len == 0 {
        return None;
    }
    let mut values: Vec<Key> = vec![SmallVec::new()];
    for set in &sets[..prefix_len] {
        let set = set.as_ref().expect("prefix sets are finite");
        let mut next = Vec::new();
        for prefix in &values {
            for value in set {
                let mut candidate = prefix.clone();
                candidate.push(value.clone());
                next.push(candidate);
            }
        }
        if next.len() > MAX_PK_CANDIDATES {
            return None;
        }
        values = next;
    }
    if values.is_empty() {
        return None;
    }
    Some(KeyConstraint {
        columns: primary_keys[..prefix_len]
            .iter()
            .map(|(column, _)| column.clone())
            .collect(),
        values,
    })
}

/// A finite value set of one column carried by the filters, or `None` when no
/// conjunct constrains the column to a finite set of its own type.
fn finite_key_set(
    filters: &[Expr],
    column: &str,
    data_type: &DataType,
) -> Option<HashSet<KeyValue>> {
    let mut acc: Option<HashSet<KeyValue>> = None;
    for filter in filters {
        let Some(set) = finite_column_set(filter, column, data_type) else {
            continue;
        };
        acc = Some(match acc {
            None => set,
            Some(previous) => previous.intersection(&set).cloned().collect(),
        });
        if acc.as_ref().is_some_and(|set| set.is_empty()) {
            return None;
        }
    }
    acc
}

/// A finite value set carried by a single expression, or `None` when the
/// expression is not a finite constraint on `column` with matching types.
fn finite_column_set(
    expr: &Expr,
    column: &str,
    data_type: &DataType,
) -> Option<HashSet<KeyValue>> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let value = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), literal) if col.name == column => {
                    literal_to_key(literal, data_type)
                }
                (literal, Expr::Column(col)) if col.name == column => {
                    literal_to_key(literal, data_type)
                }
                _ => None,
            }?;
            Some(HashSet::from([value]))
        }
        Expr::InList(in_list) if !in_list.negated => {
            let Expr::Column(col) = in_list.expr.as_ref() else {
                return None;
            };
            if col.name != column {
                return None;
            }
            in_list
                .list
                .iter()
                .map(|literal| literal_to_key(literal, data_type))
                .collect::<Option<HashSet<_>>>()
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = finite_column_set(&binary.left, column, data_type)?;
            let right = finite_column_set(&binary.right, column, data_type)?;
            Some(left.union(&right).cloned().collect())
        }
        _ => None,
    }
}

fn literal_to_key(expr: &Expr, data_type: &DataType) -> Option<KeyValue> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_key(scalar, data_type),
        _ => None,
    }
}

/// The scalar as a key of a column with `data_type`, or `None` when the
/// families do not match (a mismatch is ignored so the candidate set stays a
/// superset).
fn scalar_to_key(scalar: &ScalarValue, data_type: &DataType) -> Option<KeyValue> {
    let signed = match scalar {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        _ => None,
    };
    if let Some(value) = signed {
        return matches!(
            data_type,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        )
        .then_some(KeyValue::Int(value));
    }
    let unsigned = match scalar {
        ScalarValue::UInt8(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(u64::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(*value),
        _ => None,
    };
    if let Some(value) = unsigned {
        return matches!(
            data_type,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        )
        .then_some(KeyValue::UInt(value));
    }
    let text = match scalar {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::LargeUtf8(Some(value))
        | ScalarValue::Utf8View(Some(value)) => Some(value.as_str()),
        _ => None,
    };
    if let Some(value) = text {
        return matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
        .then(|| KeyValue::Utf8(value.into()));
    }
    let blob = match scalar {
        ScalarValue::Binary(Some(value))
        | ScalarValue::LargeBinary(Some(value))
        | ScalarValue::BinaryView(Some(value)) => Some(value.as_slice()),
        _ => None,
    };
    if let Some(value) = blob {
        return matches!(
            data_type,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView
        )
        .then(|| KeyValue::Binary(value.into()));
    }
    match (scalar, data_type) {
        (ScalarValue::Boolean(Some(value)), DataType::Boolean) => {
            Some(KeyValue::Bool(*value))
        }
        (ScalarValue::Float32(Some(value)), DataType::Float32) => {
            Some(KeyValue::Float(normalize_f64(f64::from(*value))))
        }
        (ScalarValue::Float64(Some(value)), DataType::Float64) => {
            Some(KeyValue::Float(normalize_f64(*value)))
        }
        (ScalarValue::FixedSizeBinary(_, Some(value)), DataType::FixedSizeBinary(_)) => {
            Some(KeyValue::Binary(value.as_slice().into()))
        }
        (ScalarValue::Date32(Some(value)), DataType::Date32) => {
            Some(KeyValue::Int(i64::from(*value)))
        }
        (ScalarValue::Date64(Some(value)), DataType::Date64) => {
            Some(KeyValue::Int(*value))
        }
        (ScalarValue::Time32Second(Some(value)), DataType::Time32(TimeUnit::Second)) => {
            Some(KeyValue::Int(i64::from(*value)))
        }
        (
            ScalarValue::Time32Millisecond(Some(value)),
            DataType::Time32(TimeUnit::Millisecond),
        ) => Some(KeyValue::Int(i64::from(*value))),
        (
            ScalarValue::Time64Microsecond(Some(value)),
            DataType::Time64(TimeUnit::Microsecond),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::Time64Nanosecond(Some(value)),
            DataType::Time64(TimeUnit::Nanosecond),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::TimestampSecond(Some(value), _),
            DataType::Timestamp(TimeUnit::Second, _),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::TimestampMillisecond(Some(value), _),
            DataType::Timestamp(TimeUnit::Millisecond, _),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::TimestampMicrosecond(Some(value), _),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::TimestampNanosecond(Some(value), _),
            DataType::Timestamp(TimeUnit::Nanosecond, _),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::DurationSecond(Some(value)),
            DataType::Duration(TimeUnit::Second),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::DurationMillisecond(Some(value)),
            DataType::Duration(TimeUnit::Millisecond),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::DurationMicrosecond(Some(value)),
            DataType::Duration(TimeUnit::Microsecond),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::DurationNanosecond(Some(value)),
            DataType::Duration(TimeUnit::Nanosecond),
        ) => Some(KeyValue::Int(*value)),
        (
            ScalarValue::Decimal128(Some(value), _, scalar_scale),
            DataType::Decimal128(_, column_scale),
        ) if scalar_scale == column_scale => Some(KeyValue::Decimal(*value)),
        _ => None,
    }
}

/// Canonicalise a float so `0.0 == -0.0` and every NaN maps to one bit pattern.
fn normalize_f64(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

/// Whether the key column type can be encoded with `arrow-row` and addressed
/// by the typeless candidate extraction.
fn key_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Decimal128(_, _)
    )
}

/// A candidate key value as a `ScalarValue` of the column's type, so all
/// candidates of one query can be encoded in a single row-converter batch.
fn key_to_scalar(value: &KeyValue, data_type: &DataType) -> Option<ScalarValue> {
    let scalar = match (value, data_type) {
        (KeyValue::Bool(value), DataType::Boolean) => ScalarValue::Boolean(Some(*value)),
        (KeyValue::Int(value), DataType::Int8) => ScalarValue::Int8(Some(*value as i8)),
        (KeyValue::Int(value), DataType::Int16) => {
            ScalarValue::Int16(Some(*value as i16))
        }
        (KeyValue::Int(value), DataType::Int32) => {
            ScalarValue::Int32(Some(*value as i32))
        }
        (KeyValue::Int(value), DataType::Int64) => ScalarValue::Int64(Some(*value)),
        (KeyValue::UInt(value), DataType::UInt8) => {
            ScalarValue::UInt8(Some(*value as u8))
        }
        (KeyValue::UInt(value), DataType::UInt16) => {
            ScalarValue::UInt16(Some(*value as u16))
        }
        (KeyValue::UInt(value), DataType::UInt32) => {
            ScalarValue::UInt32(Some(*value as u32))
        }
        (KeyValue::UInt(value), DataType::UInt64) => ScalarValue::UInt64(Some(*value)),
        (KeyValue::Float(bits), DataType::Float32) => {
            ScalarValue::Float32(Some(f64::from_bits(*bits) as f32))
        }
        (KeyValue::Float(bits), DataType::Float64) => {
            ScalarValue::Float64(Some(f64::from_bits(*bits)))
        }
        (KeyValue::Utf8(value), DataType::Utf8) => {
            ScalarValue::Utf8(Some(value.to_string()))
        }
        (KeyValue::Utf8(value), DataType::LargeUtf8) => {
            ScalarValue::LargeUtf8(Some(value.to_string()))
        }
        (KeyValue::Utf8(value), DataType::Utf8View) => {
            ScalarValue::Utf8View(Some(value.to_string()))
        }
        (KeyValue::Binary(value), DataType::Binary) => {
            ScalarValue::Binary(Some(value.to_vec()))
        }
        (KeyValue::Binary(value), DataType::LargeBinary) => {
            ScalarValue::LargeBinary(Some(value.to_vec()))
        }
        (KeyValue::Binary(value), DataType::BinaryView) => {
            ScalarValue::BinaryView(Some(value.to_vec()))
        }
        (KeyValue::Binary(value), DataType::FixedSizeBinary(size)) => {
            ScalarValue::FixedSizeBinary(*size, Some(value.to_vec()))
        }
        (KeyValue::Int(value), DataType::Date32) => {
            ScalarValue::Date32(Some(*value as i32))
        }
        (KeyValue::Int(value), DataType::Date64) => ScalarValue::Date64(Some(*value)),
        (KeyValue::Int(value), DataType::Time32(TimeUnit::Second)) => {
            ScalarValue::Time32Second(Some(*value as i32))
        }
        (KeyValue::Int(value), DataType::Time32(TimeUnit::Millisecond)) => {
            ScalarValue::Time32Millisecond(Some(*value as i32))
        }
        (KeyValue::Int(value), DataType::Time64(TimeUnit::Microsecond)) => {
            ScalarValue::Time64Microsecond(Some(*value))
        }
        (KeyValue::Int(value), DataType::Time64(TimeUnit::Nanosecond)) => {
            ScalarValue::Time64Nanosecond(Some(*value))
        }
        (KeyValue::Int(value), DataType::Timestamp(TimeUnit::Second, timezone)) => {
            ScalarValue::TimestampSecond(Some(*value), timezone.clone())
        }
        (KeyValue::Int(value), DataType::Timestamp(TimeUnit::Millisecond, timezone)) => {
            ScalarValue::TimestampMillisecond(Some(*value), timezone.clone())
        }
        (KeyValue::Int(value), DataType::Timestamp(TimeUnit::Microsecond, timezone)) => {
            ScalarValue::TimestampMicrosecond(Some(*value), timezone.clone())
        }
        (KeyValue::Int(value), DataType::Timestamp(TimeUnit::Nanosecond, timezone)) => {
            ScalarValue::TimestampNanosecond(Some(*value), timezone.clone())
        }
        (KeyValue::Int(value), DataType::Duration(TimeUnit::Second)) => {
            ScalarValue::DurationSecond(Some(*value))
        }
        (KeyValue::Int(value), DataType::Duration(TimeUnit::Millisecond)) => {
            ScalarValue::DurationMillisecond(Some(*value))
        }
        (KeyValue::Int(value), DataType::Duration(TimeUnit::Microsecond)) => {
            ScalarValue::DurationMicrosecond(Some(*value))
        }
        (KeyValue::Int(value), DataType::Duration(TimeUnit::Nanosecond)) => {
            ScalarValue::DurationNanosecond(Some(*value))
        }
        (KeyValue::Decimal(value), DataType::Decimal128(precision, scale)) => {
            ScalarValue::Decimal128(Some(*value), *precision, *scale)
        }
        _ => return None,
    };
    Some(scalar)
}

/// Encode candidate composite keys into Arrow arrays matching `types`.
fn key_values_to_arrays(values: &[Key], types: &[DataType]) -> Option<Vec<ArrayRef>> {
    let mut arrays = Vec::with_capacity(types.len());
    for (index, data_type) in types.iter().enumerate() {
        let scalars = values
            .iter()
            .map(|key| key_to_scalar(key.get(index)?, data_type))
            .collect::<Option<Vec<_>>>()?;
        arrays.push(ScalarValue::iter_to_array(scalars).ok()?);
    }
    Some(arrays)
}

/// The single-integer candidates of a primary-key filter (compatibility
/// wrapper around [`extract_key_constraints`]).
pub fn extract_pk_candidates(
    filters: &[Expr],
    pk_column: &str,
    pk_type: &DataType,
) -> Option<Vec<i64>> {
    let constraint =
        extract_key_constraints(filters, &[(pk_column.to_string(), pk_type.clone())])?;
    constraint
        .values
        .iter()
        .map(|value| value.first()?.single_int())
        .collect()
}

/// The exact/inexact value of a statistics precision, when it is an integer.
fn precision_to_i64(precision: &Precision<ScalarValue>) -> Option<i64> {
    match precision {
        Precision::Exact(value) | Precision::Inexact(value) => match value {
            ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
            ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
            ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
            ScalarValue::Int64(Some(value)) => Some(*value),
            ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
            ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
            ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
            ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
            _ => None,
        },
        Precision::Absent => None,
    }
}

/// pk min/max of one flattened file config, when statistics are available.
fn pk_min_max(config: &FileScanConfig, pk_column: &str) -> Option<(i64, i64)> {
    let table_schema = config.file_source.table_schema().table_schema();
    let index = table_schema.index_of(pk_column).ok()?;
    let statistics = config.file_groups.first()?.file_statistics(None)?;
    let column = statistics.column_statistics.get(index)?;
    let min = precision_to_i64(&column.min_value)?;
    let max = precision_to_i64(&column.max_value)?;
    Some((min, max))
}

/// The vortex handle of one file, opened once per process.
async fn get_vortex_file(
    store: Arc<dyn ObjectStore>,
    location: &StorePath,
    session: &VortexSession,
) -> Option<Arc<VortexFile>> {
    let key = location.as_ref().to_string();
    let location = location.clone();
    let session = session.clone();
    VORTEX_FILES
        .try_get_with(key, async move {
            let read_at = Arc::new(ObjectStoreReadAt::new_with_allocator(
                store,
                location.clone(),
                session.handle(),
                session.allocator(),
            ));
            session
                .open_options()
                .open_read(read_at)
                .await
                .map(Arc::new)
                .map_err(|error| error.to_string())
        })
        .await
        .ok()
}

/// Identifier of one key layout: the pinned columns, their types and the row
/// encoding version.  Used as the disk-cache `kind` and stored in the index
/// header, so a name-hash collision is detected instead of misread.
fn index_kind(columns: &[String], types: &[DataType]) -> String {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    INDEX_VERSION.hash(&mut hasher);
    for (column, data_type) in columns.iter().zip(types) {
        column.hash(&mut hasher);
        format!("{data_type:?}").hash(&mut hasher);
    }
    format!("pk{:016x}", hasher.finish())
}

fn index_kind_hash(kind: &str) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    kind.hash(&mut hasher);
    hasher.finish()
}

/// Fingerprint of the data file the index belongs to, stored in the header so
/// a name-hash collision can never serve one file's index for another.
fn index_source_hash(location: &StorePath) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    location.as_ref().hash(&mut hasher);
    hasher.finish()
}

/// Load (or build once) the mmapped key index and vortex handle of one file.
async fn get_or_load_file(
    store: Arc<dyn ObjectStore>,
    location: StorePath,
    columns: &[String],
    types: &[DataType],
    session: VortexSession,
) -> Option<Arc<CachedFile>> {
    let cache = index_disk_cache()?;
    let file = get_vortex_file(Arc::clone(&store), &location, &session).await?;
    let kind = index_kind(columns, types);
    let kind_hash = index_kind_hash(&kind);
    let source_hash = index_source_hash(&location);
    let rejected_key = format!("{location}#{kind}");
    if REJECTED.get(&rejected_key).await.is_some() {
        return None;
    }

    if let Some((entry, size)) = cache.get_file_entry(&location, &kind).await {
        if let Some(index) = MmapIndex::open(entry, size, kind_hash, source_hash) {
            HITS.fetch_add(1, AtomicOrdering::Relaxed);
            return Some(Arc::new(CachedFile {
                index: Arc::new(index),
                file,
            }));
        }
        warn!("corrupt pk index for {location}; rebuilding");
    }

    let max_bytes = (cache.capacity_bytes() / MAX_INDEX_CACHE_RATIO).max(1);
    let temp = cache.temporary_path();
    let size = match build_index_file(
        &file,
        &location,
        columns,
        types,
        &session,
        &temp,
        kind_hash,
        source_hash,
        max_bytes,
    )
    .await
    {
        Ok(size) => size,
        Err(error) => {
            let _ = std::fs::remove_file(&temp);
            warn!("failed to build pk index for {location}: {error}");
            REJECTED.insert(rejected_key, ()).await;
            return None;
        }
    };
    let (entry, size, built) = match cache
        .insert_file_entry(&location, &kind, temp.clone(), size)
        .await
    {
        Ok(entry) => entry,
        Err(error) => {
            let _ = std::fs::remove_file(&temp);
            warn!("failed to store pk index for {location}: {error}");
            return None;
        }
    };
    let index = match MmapIndex::open(entry, size, kind_hash, source_hash) {
        Some(index) => index,
        None => {
            warn!("stored pk index for {location} is unreadable");
            REJECTED.insert(rejected_key, ()).await;
            return None;
        }
    };
    if built {
        MISSES.fetch_add(1, AtomicOrdering::Relaxed);
    } else {
        HITS.fetch_add(1, AtomicOrdering::Relaxed);
    }
    Some(Arc::new(CachedFile {
        index: Arc::new(index),
        file,
    }))
}

/// Read the key columns of one file and write them, arrow-Row encoded and
/// sorted, to `temp`.  Returns the file size, or an error (unsorted keys,
/// unsupported types, over the cache budget, ...) so the caller falls back to
/// a regular scan.
async fn build_index_file(
    file: &VortexFile,
    location: &StorePath,
    columns: &[String],
    types: &[DataType],
    session: &VortexSession,
    temp: &std::path::Path,
    kind_hash: u64,
    source_hash: u64,
    max_bytes: u64,
) -> crate::Result<u64> {
    let converter = RowConverter::new(
        types
            .iter()
            .cloned()
            .map(SortField::new)
            .collect::<Vec<_>>(),
    )
    .map_err(|error| open_error(location, error))?;
    let projection = select(FieldNames::from_iter(columns.iter().cloned()), root())
        .bind(file.dtype())
        .map_err(|error| open_error(location, error))?;
    let mut stream = file
        .scan()
        .map_err(|error| open_error(location, error))?
        .with_projection(projection)
        .into_array_stream()
        .map_err(|error| open_error(location, error))?;
    let mut ctx = session.create_execution_ctx();
    let mut writer = IndexWriter::create(temp, kind_hash, source_hash, max_bytes)?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| open_error(location, error))?;
        let arrow = session
            .arrow()
            .execute_arrow(chunk, None, &mut ctx)
            .map_err(|error| open_error(location, error))?;
        let batch = RecordBatch::from(arrow.as_struct().clone());
        if batch.num_rows() == 0 {
            continue;
        }
        let mut arrays = Vec::with_capacity(columns.len());
        for (column, data_type) in columns.iter().zip(types) {
            let index = batch.schema().index_of(column).map_err(|_| {
                rootcause::report!("column {column} missing from file {location}")
            })?;
            let array = batch.column(index);
            let found = array.data_type();
            if found != data_type {
                // The file does not hold the type the table declares, so the
                // key encoding is not comparable: bail out and let the caller
                // fall back to a regular scan instead of misreading values.
                return Err(rootcause::report!(
                    "column {column} of {location} has type {found}, expected {data_type}"
                ));
            }
            arrays.push(array.clone());
        }
        let encoded = converter
            .convert_columns(&arrays)
            .map_err(|error| open_error(location, error))?;
        for row in 0..encoded.num_rows() {
            writer.push(encoded.row(row).data())?;
        }
    }
    let size = writer.finish()?;
    debug!("built pk index for {location} on {columns:?}: {size} bytes");
    Ok(size)
}

fn open_error(location: &StorePath, error: impl std::fmt::Display) -> rootcause::Report {
    rootcause::report!("failed to read key columns of {location}: {error}")
}

/// The location of the single vortex file of one flattened config, or `None`
/// when the config is not a single supported file.
fn single_vortex_file(config: &FileScanConfig) -> Option<StorePath> {
    // The flatten step creates one single-file config per file.
    let file_group = config.file_groups.first()?;
    if config.file_groups.len() != 1 || file_group.len() != 1 {
        return None;
    }
    let file = file_group.files().first()?;
    let location = file.object_meta.location.clone();
    (PhysicalFormat::from_extension(location.as_ref()).ok()? == PhysicalFormat::Vortex)
        .then_some(location)
}

/// Build row-level inputs for a key-prefix filter, one input per flattened
/// file config.  Returns `None` (caller falls back to a regular scan) when the
/// locator is disabled or any file/column is unsupported.
///
/// Files whose statistics prove the candidate set absent are returned as an
/// empty input, so the resulting plan reads nothing for them.
pub async fn try_build_key_inputs(
    state: &dyn Session,
    io_config: &LakeSoulIOConfig,
    configs: &[FileScanConfig],
    constraint: &KeyConstraint,
) -> Option<Vec<Arc<dyn ExecutionPlan>>> {
    index_disk_cache()?;
    if constraint.values.is_empty() || constraint.values.len() > MAX_PK_CANDIDATES {
        return None;
    }
    if configs.is_empty() {
        return None;
    }
    let primary_keys = io_config.primary_keys_slice();
    if primary_keys.len() < constraint.columns.len()
        || primary_keys[..constraint.columns.len()] != constraint.columns[..]
    {
        return None;
    }
    // All configs come from one table, so the key layout is taken from the
    // first one and the candidates are encoded once for every file.
    let file_schema = configs[0].file_schema();
    let mut types = Vec::with_capacity(constraint.columns.len());
    for column in &constraint.columns {
        let field = file_schema.field_with_name(column).ok()?;
        if !key_type_supported(field.data_type()) {
            return None;
        }
        types.push(field.data_type().clone());
    }
    let arrays = key_values_to_arrays(&constraint.values, &types)?;
    let converter = RowConverter::new(
        types
            .iter()
            .cloned()
            .map(SortField::new)
            .collect::<Vec<_>>(),
    )
    .ok()?;
    let encoded = {
        let rows = converter.convert_columns(&arrays).ok()?;
        (0..rows.num_rows())
            .map(|row| rows.row(row).data().to_vec())
            .collect::<Vec<_>>()
    };
    let int_candidates = constraint.single_column_ints();
    // Built lazily: most callers (parquet tables, payload-only predicates)
    // never reach the vortex code below, and creating a vortex session costs
    // far more than this check.
    let mut session: Option<VortexSession> = None;
    let profile = std::env::var("LAKESOUL_PK_PROFILE").is_ok();
    let t_total = std::time::Instant::now();
    let mut t_cache = std::time::Duration::ZERO;
    let mut t_take = std::time::Duration::ZERO;
    let mut n_files = 0usize;
    let mut n_pruned = 0usize;
    let mut n_rows = 0usize;

    // Indexing costs one key-column read plus a small local file per data
    // file; when the candidate set as a whole is tiny, a plain scan is
    // cheaper.  The decision spans all files, so neither a single small upsert
    // file nor the many small files IVM writes per append disable the index.
    let mut total_rows: u64 = 0;
    for config in configs {
        let location = single_vortex_file(config)?;
        let store = state
            .runtime_env()
            .object_store(&config.object_store_url)
            .ok()?;
        let session = match &session {
            Some(existing) => existing.clone(),
            None => {
                let created = VortexSession::default().with_tokio();
                session = Some(created.clone());
                created
            }
        };
        let file = get_vortex_file(store, &location, &session).await?;
        total_rows = total_rows.saturating_add(file.row_count());
        if total_rows >= MIN_INDEX_ROWS {
            break;
        }
    }
    if total_rows < MIN_INDEX_ROWS {
        return None;
    }

    let mut inputs = Vec::with_capacity(configs.len());
    for config in configs {
        let location = single_vortex_file(config)?;
        let session = match &session {
            Some(existing) => existing.clone(),
            None => {
                let created = VortexSession::default().with_tokio();
                session = Some(created.clone());
                created
            }
        };

        let (projected_schema, projection_names) = projected_file_schema(config)?;

        if let Some(ints) = &int_candidates
            && let Some((min, max)) = pk_min_max(config, &constraint.columns[0])
            && !ints.iter().any(|value| *value >= min && *value <= max)
        {
            n_pruned += 1;
            inputs.push(empty_input(&projected_schema)?);
            continue;
        }
        n_files += 1;

        let store = state
            .runtime_env()
            .object_store(&config.object_store_url)
            .ok()?;
        let t = std::time::Instant::now();
        let entry = get_or_load_file(
            Arc::clone(&store),
            location.clone(),
            &constraint.columns,
            &types,
            session.clone(),
        )
        .await?;
        t_cache += t.elapsed();

        let mut indices: Vec<u64> = Vec::new();
        for key in &encoded {
            indices.extend(entry.index.rows_with_prefix(key).map(|row| row as u64));
        }
        if indices.is_empty() {
            inputs.push(empty_input(&projected_schema)?);
            continue;
        }
        indices.sort_unstable();
        indices.dedup();

        let t = std::time::Instant::now();
        let batches = take_rows(
            &entry.file,
            &location,
            &session,
            &indices,
            &projected_schema,
            &projection_names,
        )
        .await
        .inspect_err(|e| warn!("failed to fetch key candidate rows: {e}"))
        .ok()?;
        t_take += t.elapsed();
        n_rows += indices.len();
        let exec =
            MemorySourceConfig::try_new_exec(&[batches], projected_schema, None).ok()?;
        inputs.push(exec as Arc<dyn ExecutionPlan>);
    }
    if profile {
        eprintln!(
            "pk_locator: keys={} files={} pruned={} rows={} cache={:?} take={:?} total={:?}",
            constraint.values.len(),
            n_files,
            n_pruned,
            n_rows,
            t_cache,
            t_take,
            t_total.elapsed()
        );
    }
    Some(inputs)
}

fn empty_input(schema: &SchemaRef) -> Option<Arc<dyn ExecutionPlan>> {
    MemorySourceConfig::try_new_exec(&[Vec::new()], Arc::clone(schema), None)
        .ok()
        .map(|exec| exec as Arc<dyn ExecutionPlan>)
}

/// Projected schema of one file config plus the field names to select.
fn projected_file_schema(config: &FileScanConfig) -> Option<(SchemaRef, Vec<String>)> {
    let file_schema = config.file_schema();
    let indices = config.file_column_projection_indices();
    let (fields, names): (Vec<_>, Vec<_>) = match indices {
        Some(indices) => indices
            .iter()
            .filter_map(|index| file_schema.fields().get(*index))
            .map(|field| (Arc::clone(field), field.name().clone()))
            .unzip(),
        None => file_schema
            .fields()
            .iter()
            .map(|field| (Arc::clone(field), field.name().clone()))
            .unzip(),
    };
    if names.is_empty() {
        return None;
    }
    Some((Arc::new(Schema::new(fields)), names))
}

async fn take_rows(
    file: &VortexFile,
    location: &StorePath,
    session: &VortexSession,
    indices: &[u64],
    projected_schema: &SchemaRef,
    projection_names: &[String],
) -> crate::Result<Vec<RecordBatch>> {
    let projection = root()
        .bind(file.dtype())
        .map_err(|e| open_error(location, e))?;
    let row_indices =
        StrictSortedBuffer::try_new(Buffer::from_iter(indices.iter().copied()))
            .map_err(|e| open_error(location, e))?;
    let mut stream = file
        .scan()
        .map_err(|e| open_error(location, e))?
        .with_projection(projection)
        .with_row_indices(row_indices)
        .into_array_stream()
        .map_err(|e| open_error(location, e))?;
    let mut ctx = session.create_execution_ctx();
    let mut batches = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| open_error(location, e))?;
        let arrow = session
            .arrow()
            .execute_arrow(chunk, None, &mut ctx)
            .map_err(|e| open_error(location, e))?;
        let batch = RecordBatch::from(arrow.as_struct().clone());
        let mut columns = Vec::with_capacity(projection_names.len());
        for name in projection_names {
            let index = batch.schema().index_of(name).map_err(|_| {
                rootcause::report!("column {name} missing from file {location}")
            })?;
            columns.push(Arc::clone(batch.column(index)));
        }
        batches.push(
            RecordBatch::try_new(Arc::clone(projected_schema), columns)
                .map_err(|e| rootcause::report!("project candidate rows: {e}"))?,
        );
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::prelude::{col, lit};
    use datafusion_expr::Expr;
    use smallvec::smallvec;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::row::{RowConverter, SortField};
    use std::sync::Arc;

    use super::{
        IndexWriter, Key, KeyConstraint, KeyValue, MmapIndex, extract_key_constraints,
        extract_pk_candidates, key_values_to_arrays,
    };

    fn candidates(filters: &[Expr]) -> Option<Vec<i64>> {
        extract_pk_candidates(filters, "id", &DataType::Int64)
    }

    fn string_key(value: &str) -> KeyValue {
        KeyValue::Utf8(value.into())
    }

    fn composite(filters: &[Expr]) -> Option<KeyConstraint> {
        extract_key_constraints(
            filters,
            &[
                ("g".to_string(), DataType::Utf8),
                ("v".to_string(), DataType::Int64),
            ],
        )
    }

    #[test]
    fn extracts_equality() {
        assert_eq!(candidates(&[col("id").eq(lit(7i64))]), Some(vec![7]));
        assert_eq!(candidates(&[lit(9i64).eq(col("id"))]), Some(vec![9]));
    }

    #[test]
    fn extracts_in_list() {
        let filters =
            vec![col("id").in_list(vec![lit(1i64), lit(2i64), lit(3i64)], false)];
        let mut got = candidates(&filters).unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3]);
    }

    #[test]
    fn extracts_or_of_equalities() {
        let filters = vec![col("id").eq(lit(1i64)).or(col("id").eq(lit(2i64)))];
        let mut got = candidates(&filters).unwrap();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn ignores_non_pk_conjuncts() {
        let filters = vec![col("id").eq(lit(5i64)), col("value").gt(lit(3i64))];
        assert_eq!(candidates(&filters), Some(vec![5]));
    }

    #[test]
    fn bails_on_or_with_non_pk_branch() {
        let filters = vec![col("id").eq(lit(5i64)).or(col("value").gt(lit(3i64)))];
        assert_eq!(candidates(&filters), None);
    }

    #[test]
    fn intersects_and_conjuncts() {
        let filters = vec![
            col("id").in_list(vec![lit(1i64), lit(2i64)], false),
            col("id").eq(lit(2i64)),
        ];
        assert_eq!(candidates(&filters), Some(vec![2]));
    }

    #[test]
    fn ignores_negated_and_unsupported_pk_conditions() {
        assert_eq!(candidates(&[col("id").not_eq(lit(5i64))]), None);
        assert_eq!(candidates(&[col("id").gt(lit(5i64))]), None);
    }

    #[test]
    fn extracts_string_keys() {
        let filters = vec![col("id").eq(lit("abc"))];
        assert_eq!(
            extract_pk_candidates(&filters, "id", &DataType::Utf8),
            None,
            "the wrapper only serves integer keys"
        );
        let constraint =
            extract_key_constraints(&filters, &[("id".to_string(), DataType::Utf8)])
                .unwrap();
        assert_eq!(constraint.columns, vec!["id".to_string()]);
        let expected: Vec<Key> = vec![smallvec![string_key("abc")]];
        assert_eq!(constraint.values, expected);

        let filters = vec![col("id").in_list(vec![lit("a"), lit("b")], false)];
        let constraint =
            extract_key_constraints(&filters, &[("id".to_string(), DataType::Utf8)])
                .unwrap();
        let expected: Vec<Key> =
            vec![smallvec![string_key("a")], smallvec![string_key("b")]];
        let mut got = constraint.values;
        got.sort_unstable();
        assert_eq!(got, expected);
    }

    #[test]
    fn ignores_mismatched_literal_types() {
        // An integer column constrained by a string literal is not a finite
        // integer set; the locator must fall back instead of pruning wrongly.
        let filters = vec![col("id").eq(lit("abc"))];
        assert_eq!(candidates(&filters), None);
        let constraint =
            extract_key_constraints(&filters, &[("id".to_string(), DataType::Int64)]);
        assert_eq!(constraint, None);
    }

    #[test]
    fn extracts_composite_full_key() {
        let filters = vec![
            col("g").eq(lit("a")),
            col("v").in_list(vec![lit(1i64), lit(2i64)], false),
        ];
        let constraint = composite(&filters).unwrap();
        assert_eq!(constraint.columns, vec!["g".to_string(), "v".to_string()]);
        let expected: Vec<Key> = vec![
            smallvec![string_key("a"), KeyValue::Int(1)],
            smallvec![string_key("a"), KeyValue::Int(2)],
        ];
        let mut got = constraint.values;
        got.sort_unstable();
        assert_eq!(got, expected);
    }

    #[test]
    fn extracts_composite_key_prefix() {
        // Only the first key column is constrained: the candidate set is its
        // values on the prefix, which still lets the index skip file ranges.
        let filters = vec![col("g").eq(lit("a"))];
        let constraint = composite(&filters).unwrap();
        assert_eq!(constraint.columns, vec!["g".to_string()]);
        let expected: Vec<Key> = vec![smallvec![string_key("a")]];
        assert_eq!(constraint.values, expected);
    }

    #[test]
    fn bails_without_key_prefix() {
        // A predicate on a later key column alone says nothing about the
        // prefix, so no candidate set can be built.
        let filters = vec![col("v").eq(lit(1i64))];
        assert_eq!(composite(&filters), None);
    }

    #[test]
    fn bails_on_contradictory_key_conjuncts() {
        let filters = vec![col("g").eq(lit("a")), col("g").eq(lit("b"))];
        assert_eq!(composite(&filters), None);
    }

    #[test]
    fn caps_composite_candidate_count() {
        let first = (0..=super::MAX_PK_CANDIDATES / 100)
            .map(|v| lit(format!("g{v}")))
            .collect::<Vec<_>>();
        let second = (0..100).map(|v| lit(v as i64)).collect::<Vec<_>>();
        let filters = vec![
            col("g").in_list(first, false),
            col("v").in_list(second, false),
        ];
        assert_eq!(composite(&filters), None);
    }

    #[test]
    fn caps_candidate_count() {
        let list = (0..=super::MAX_PK_CANDIDATES)
            .map(|v| lit(v as i64))
            .collect::<Vec<_>>();
        let filters = vec![col("id").in_list(list, false)];
        assert_eq!(candidates(&filters), None);
    }

    fn encode_utf8(value: &str) -> Vec<u8> {
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let array = Arc::new(StringArray::from_iter_values([value])) as ArrayRef;
        let rows = converter.convert_columns(&[array]).unwrap();
        rows.row(0).data().to_vec()
    }

    /// Build an index file the same way the locator does and open it.
    fn build_test_index(keys: &[&str]) -> (tempfile::TempDir, MmapIndex) {
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let array =
            Arc::new(StringArray::from_iter_values(keys.iter().copied())) as ArrayRef;
        let rows = converter.convert_columns(&[array]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let kind_hash = super::index_kind_hash("test");
        let source_hash =
            super::index_source_hash(&object_store::path::Path::from("test"));
        let mut writer =
            IndexWriter::create(&path, kind_hash, source_hash, u64::MAX).unwrap();
        for row in 0..rows.num_rows() {
            writer.push(rows.row(row).data()).unwrap();
        }
        let size = writer.finish().unwrap();
        let file = Arc::new(std::fs::File::open(&path).unwrap());
        let index = MmapIndex::open(file, size, kind_hash, source_hash).unwrap();
        (dir, index)
    }

    #[test]
    fn index_lookup_by_full_key_and_prefix() {
        let (_dir, index) = build_test_index(&["a", "a", "b", "b", "c"]);
        // Prefix ranges are contiguous and include duplicate keys.
        assert_eq!(index.rows_with_prefix(&encode_utf8("a")), 0..2);
        assert_eq!(index.rows_with_prefix(&encode_utf8("b")), 2..4);
        assert_eq!(index.rows_with_prefix(&encode_utf8("c")), 4..5);
        // Missing keys yield an empty range; "ab" must not match "a".
        assert_eq!(index.rows_with_prefix(&encode_utf8("z")), 5..5);
        assert_eq!(index.rows_with_prefix(&encode_utf8("ab")), 2..2);
    }

    #[test]
    fn index_rejects_unsorted_keys() {
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let array = Arc::new(StringArray::from_iter_values(["b", "a"])) as ArrayRef;
        let rows = converter.convert_columns(&[array]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut writer =
            IndexWriter::create(&dir.path().join("unsorted.idx"), 1, 2, u64::MAX)
                .unwrap();
        writer.push(rows.row(0).data()).unwrap();
        assert!(writer.push(rows.row(1).data()).is_err());
    }

    #[test]
    fn index_rejects_corrupt_or_mismatched_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.idx");
        let mut writer = IndexWriter::create(&path, 1, 2, u64::MAX).unwrap();
        writer.push(b"x").unwrap();
        let size = writer.finish().unwrap();

        // Wrong kind/source hash, truncated size and corrupt magic are rejected.
        let file = Arc::new(std::fs::File::open(&path).unwrap());
        assert!(MmapIndex::open(Arc::clone(&file), size, 9, 2).is_none());
        assert!(MmapIndex::open(Arc::clone(&file), size, 1, 9).is_none());
        assert!(MmapIndex::open(Arc::clone(&file), size - 1, 1, 2).is_none());
        drop(file);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = b'X';
        std::fs::write(&path, &bytes).unwrap();
        let file = Arc::new(std::fs::File::open(&path).unwrap());
        assert!(MmapIndex::open(file, size, 1, 2).is_none());
    }

    #[test]
    fn candidate_keys_encode_like_index_rows() {
        let values: Vec<Key> = vec![
            smallvec![string_key("a"), KeyValue::Int(1)],
            smallvec![string_key("a"), KeyValue::Int(2)],
        ];
        let types = [DataType::Utf8, DataType::Int64];
        let arrays = key_values_to_arrays(&values, &types).unwrap();
        let converter =
            RowConverter::new(types.iter().cloned().map(SortField::new).collect())
                .unwrap();
        let rows = converter.convert_columns(&arrays).unwrap();

        // The prefix encoding is a byte prefix of both full-key encodings, so
        // a prefix lookup finds full keys.
        let prefix = encode_utf8("a");
        assert!(rows.row(0).data().starts_with(&prefix));
        assert!(rows.row(1).data().starts_with(&prefix));
        assert_ne!(rows.row(0).data(), rows.row(1).data());
    }
}
