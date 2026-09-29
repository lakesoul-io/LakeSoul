// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Row-level primary-key locator.
//!
//! `pk = value` and `pk IN (...)` predicates are answered by locating the
//! matching rows through a process-wide, per-file sorted `key -> row` index
//! instead of scanning every file.  A finite candidate set can also be built
//! for a *prefix* of a composite key (`g = 'x'` on a `(g, value)` key), which
//! covers the group filters of incremental aggregation.  Keys may repeat (a
//! non-unique column), and a prefix lookup returns every row whose key starts
//! with it.
//!
//! The index is built lazily from the file's key columns and is cached per
//! file location *and* key column set: data files are immutable, so an index
//! never goes stale (a rewritten or compacted file gets a new location and
//! therefore a new cache entry).  The cache is bounded by
//! `LAKESOUL_PK_CACHE_BYTES` (default 256 MiB, `0` disables the locator).
//! Only vortex data files are supported; anything else (or unsupported column
//! types) falls back to the regular scan path.  The candidate set is always a
//! superset of the rows matching the full predicate: only key prefixes are
//! used, because dropping a payload-matching row could drop the latest version
//! of a key during merge-on-read.
//!
//! The index assumes what LakeSoul's sorted writer guarantees: rows of a data
//! file are stored in primary-key order, so equal prefixes are adjacent.  Rows
//! are fetched by row index in ascending order, which therefore yields batches
//! sorted by primary key as the merge-on-read path requires.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Schema, SchemaRef,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::{ScalarValue, stats::Precision};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_expr::{Expr, Operator};
use futures::StreamExt;
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

use crate::config::LakeSoulIOConfig;
use crate::file_format::PhysicalFormat;

/// Byte budget of the process-wide pk map cache.
pub const ENV_PK_CACHE_BYTES: &str = "LAKESOUL_PK_CACHE_BYTES";

/// Above this many candidate keys the locator is not worth it: the caller
/// is better off scanning.
pub const MAX_PK_CANDIDATES: usize = 10_000;

const DEFAULT_CACHE_BYTES: u64 = 256 * 1024 * 1024;

static PK_CACHE: LazyLock<Option<Cache<String, Arc<CachedFile>>>> = LazyLock::new(|| {
    let capacity = std::env::var(ENV_PK_CACHE_BYTES)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_CACHE_BYTES);
    if capacity == 0 {
        debug!("pk locator disabled by {ENV_PK_CACHE_BYTES}=0");
        return None;
    }
    Some(
        Cache::builder()
            .max_capacity(capacity)
            .weigher(|_key: &String, entry: &Arc<CachedFile>| {
                entry.bytes.min(u32::MAX as usize) as u32
            })
            .build(),
    )
});

static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// `(hits, misses)` of the pk map cache since process start.
pub fn cache_stats() -> (u64, u64) {
    (
        HITS.load(AtomicOrdering::Relaxed),
        MISSES.load(AtomicOrdering::Relaxed),
    )
}

/// Approximate heap footprint of one key index: the inline entries plus the
/// heap payload of boxed strings/binaries.
fn index_bytes(entries: &[(Key, u32)]) -> usize {
    let payload = entries
        .iter()
        .map(|(key, _)| {
            key.iter()
                .map(|value| match value {
                    KeyValue::Utf8(value) => value.len(),
                    KeyValue::Binary(value) => value.len(),
                    _ => 0,
                })
                .sum::<usize>()
        })
        .sum::<usize>();
    entries.len() * (std::mem::size_of::<Key>() + std::mem::size_of::<u32>())
        + payload
        + 128
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

/// The sorted `key -> row` index of one file.
///
/// Keys can repeat (a non-unique column) and a lookup by a prefix yields every
/// row whose key starts with it, which keeps the index useful for the key
/// prefixes of composite merge keys.
pub struct KeyIndex {
    entries: Vec<(Key, u32)>,
}

impl KeyIndex {
    fn new(mut entries: Vec<(Key, u32)>) -> Self {
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Self { entries }
    }

    /// The rows of every key that starts with `prefix`.
    fn rows_with_prefix(&self, prefix: &[KeyValue]) -> impl Iterator<Item = u32> + '_ {
        let start = self
            .entries
            .partition_point(|(key, _)| prefix_cmp(key, prefix) == Ordering::Less);
        let end = self
            .entries
            .partition_point(|(key, _)| prefix_cmp(key, prefix) != Ordering::Greater);
        self.entries[start..end].iter().map(|(_, row)| *row)
    }

    /// Number of indexed rows.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index has no rows.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Compare the first `prefix.len()` components of `key` with `prefix`.
fn prefix_cmp(key: &[KeyValue], prefix: &[KeyValue]) -> Ordering {
    if key.len() < prefix.len() {
        return Ordering::Greater;
    }
    for index in 0..prefix.len() {
        match key[index].cmp(&prefix[index]) {
            Ordering::Equal => {}
            other => return other,
        }
    }
    Ordering::Equal
}

/// Cached per-file state: the key index plus the opened file handle
/// (opening a vortex file reads its footer and layout, which is a
/// per-query cost worth amortizing).
struct CachedFile {
    index: KeyIndex,
    file: VortexFile,
    bytes: usize,
}

/// Rough heap estimate for an open file's footer/layout metadata, on top of
/// the key index itself.
const FILE_METADATA_BYTES: usize = 64 * 1024;

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

/// The key of one row of one column, or `None` for NULL/unsupported types.
fn key_from_array(
    array: &dyn Array,
    row: usize,
    data_type: &DataType,
) -> Option<KeyValue> {
    match data_type {
        DataType::Boolean => Some(KeyValue::Bool(array.as_boolean().value(row))),
        DataType::Int8 => Some(KeyValue::Int(i64::from(
            array.as_primitive::<Int8Type>().value(row),
        ))),
        DataType::Int16 => Some(KeyValue::Int(i64::from(
            array.as_primitive::<Int16Type>().value(row),
        ))),
        DataType::Int32 => Some(KeyValue::Int(i64::from(
            array.as_primitive::<Int32Type>().value(row),
        ))),
        DataType::Int64 => {
            Some(KeyValue::Int(array.as_primitive::<Int64Type>().value(row)))
        }
        DataType::UInt8 => Some(KeyValue::UInt(u64::from(
            array.as_primitive::<UInt8Type>().value(row),
        ))),
        DataType::UInt16 => Some(KeyValue::UInt(u64::from(
            array.as_primitive::<UInt16Type>().value(row),
        ))),
        DataType::UInt32 => Some(KeyValue::UInt(u64::from(
            array.as_primitive::<UInt32Type>().value(row),
        ))),
        DataType::UInt64 => Some(KeyValue::UInt(
            array.as_primitive::<UInt64Type>().value(row),
        )),
        DataType::Float32 => Some(KeyValue::Float(normalize_f64(f64::from(
            array.as_primitive::<Float32Type>().value(row),
        )))),
        DataType::Float64 => Some(KeyValue::Float(normalize_f64(
            array.as_primitive::<Float64Type>().value(row),
        ))),
        DataType::Utf8 => {
            Some(KeyValue::Utf8(array.as_string::<i32>().value(row).into()))
        }
        DataType::LargeUtf8 => {
            Some(KeyValue::Utf8(array.as_string::<i64>().value(row).into()))
        }
        DataType::Utf8View => {
            Some(KeyValue::Utf8(array.as_string_view().value(row).into()))
        }
        DataType::Binary => {
            Some(KeyValue::Binary(array.as_binary::<i32>().value(row).into()))
        }
        DataType::LargeBinary => {
            Some(KeyValue::Binary(array.as_binary::<i64>().value(row).into()))
        }
        DataType::BinaryView => {
            Some(KeyValue::Binary(array.as_binary_view().value(row).into()))
        }
        DataType::FixedSizeBinary(_) => Some(KeyValue::Binary(
            array.as_fixed_size_binary().value(row).into(),
        )),
        DataType::Date32 => Some(KeyValue::Int(i64::from(
            array.as_primitive::<Date32Type>().value(row),
        ))),
        DataType::Date64 => {
            Some(KeyValue::Int(array.as_primitive::<Date64Type>().value(row)))
        }
        DataType::Time32(TimeUnit::Second) => Some(KeyValue::Int(i64::from(
            array.as_primitive::<Time32SecondType>().value(row),
        ))),
        DataType::Time32(TimeUnit::Millisecond) => Some(KeyValue::Int(i64::from(
            array.as_primitive::<Time32MillisecondType>().value(row),
        ))),
        DataType::Time64(TimeUnit::Microsecond) => Some(KeyValue::Int(
            array.as_primitive::<Time64MicrosecondType>().value(row),
        )),
        DataType::Time64(TimeUnit::Nanosecond) => Some(KeyValue::Int(
            array.as_primitive::<Time64NanosecondType>().value(row),
        )),
        DataType::Timestamp(TimeUnit::Second, _) => Some(KeyValue::Int(
            array.as_primitive::<TimestampSecondType>().value(row),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(KeyValue::Int(
            array.as_primitive::<TimestampMillisecondType>().value(row),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(KeyValue::Int(
            array.as_primitive::<TimestampMicrosecondType>().value(row),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(KeyValue::Int(
            array.as_primitive::<TimestampNanosecondType>().value(row),
        )),
        DataType::Duration(TimeUnit::Second) => Some(KeyValue::Int(
            array.as_primitive::<DurationSecondType>().value(row),
        )),
        DataType::Duration(TimeUnit::Millisecond) => Some(KeyValue::Int(
            array.as_primitive::<DurationMillisecondType>().value(row),
        )),
        DataType::Duration(TimeUnit::Microsecond) => Some(KeyValue::Int(
            array.as_primitive::<DurationMicrosecondType>().value(row),
        )),
        DataType::Duration(TimeUnit::Nanosecond) => Some(KeyValue::Int(
            array.as_primitive::<DurationNanosecondType>().value(row),
        )),
        DataType::Decimal128(_, _) => Some(KeyValue::Decimal(
            array.as_primitive::<Decimal128Type>().value(row),
        )),
        _ => None,
    }
}

/// Whether `key_from_array` supports this column type.
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

/// Load (or fetch from the cache) the key index and file handle of one file.
async fn get_or_load_file(
    store: Arc<dyn ObjectStore>,
    location: StorePath,
    columns: &[String],
    types: &[DataType],
    session: VortexSession,
) -> Option<Arc<CachedFile>> {
    let Some(cache) = PK_CACHE.as_ref() else {
        return build_cached_file(store, location, columns, types, &session)
            .await
            .ok();
    };
    let key = format!("{location}#{}", columns.join(","));
    if cache.get(&key).await.is_some() {
        HITS.fetch_add(1, AtomicOrdering::Relaxed);
    } else {
        MISSES.fetch_add(1, AtomicOrdering::Relaxed);
    }
    let load = async move {
        build_cached_file(store, location, columns, types, &session)
            .await
            .map_err(|e| e.to_string())
    };
    match cache.try_get_with(key, load).await {
        Ok(entry) => Some(entry),
        Err(error) => {
            warn!("failed to build key index: {error}");
            None
        }
    }
}

async fn build_cached_file(
    store: Arc<dyn ObjectStore>,
    location: StorePath,
    columns: &[String],
    types: &[DataType],
    session: &VortexSession,
) -> crate::Result<Arc<CachedFile>> {
    let read_at = Arc::new(ObjectStoreReadAt::new_with_allocator(
        store,
        location.clone(),
        session.handle(),
        session.allocator(),
    ));
    let file = session
        .open_options()
        .open_read(read_at)
        .await
        .map_err(|e| open_error(&location, e))?;
    let projection = select(FieldNames::from_iter(columns.iter().cloned()), root())
        .bind(file.dtype())
        .map_err(|e| open_error(&location, e))?;
    let mut stream = file
        .scan()
        .map_err(|e| open_error(&location, e))?
        .with_projection(projection)
        .into_array_stream()
        .map_err(|e| open_error(&location, e))?;
    let mut ctx = session.create_execution_ctx();
    let mut entries: Vec<(Key, u32)> = Vec::new();
    let mut offset: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| open_error(&location, e))?;
        let len = chunk.len() as u64;
        let arrow = session
            .arrow()
            .execute_arrow(chunk, None, &mut ctx)
            .map_err(|e| open_error(&location, e))?;
        let batch = RecordBatch::from(arrow.as_struct().clone());
        let mut indices = Vec::with_capacity(columns.len());
        for (column, data_type) in columns.iter().zip(types) {
            let index = batch.schema().index_of(column).map_err(|_| {
                rootcause::report!("column {column} missing from file {location}")
            })?;
            let found = batch.column(index).data_type();
            if found != data_type {
                // The file does not hold the type the table declares, so the
                // key encoding is not comparable: bail out and let the caller
                // fall back to a regular scan instead of misreading values.
                return Err(rootcause::report!(
                    "column {column} of {location} has type {found}, expected {data_type}"
                ));
            }
            indices.push(index);
        }
        for row in 0..batch.num_rows() {
            let mut key: Key = SmallVec::with_capacity(columns.len());
            let mut complete = true;
            for (index, data_type) in indices.iter().zip(types) {
                let array = batch.column(*index);
                if array.is_null(row) {
                    complete = false;
                    break;
                }
                match key_from_array(array.as_ref(), row, data_type) {
                    Some(value) => key.push(value),
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            if complete {
                let row = u32::try_from(offset + row as u64).map_err(|_| {
                    rootcause::report!("file {location} has more than u32::MAX rows")
                })?;
                entries.push((key, row));
            }
        }
        offset += len;
    }
    let bytes = index_bytes(&entries) + FILE_METADATA_BYTES;
    debug!(
        "built key index for {location} on {columns:?}: {} rows, {} bytes",
        entries.len(),
        bytes
    );
    Ok(Arc::new(CachedFile {
        index: KeyIndex::new(entries),
        file,
        bytes,
    }))
}

fn open_error(location: &StorePath, error: impl std::fmt::Display) -> rootcause::Report {
    rootcause::report!("failed to read key columns of {location}: {error}")
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
    if PK_CACHE.is_none() {
        return None;
    }
    if constraint.values.is_empty() || constraint.values.len() > MAX_PK_CANDIDATES {
        return None;
    }
    let primary_keys = io_config.primary_keys_slice();
    if primary_keys.len() < constraint.columns.len()
        || primary_keys[..constraint.columns.len()] != constraint.columns[..]
    {
        return None;
    }
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

    let mut inputs = Vec::with_capacity(configs.len());
    for config in configs {
        // The flatten step creates one single-file config per file.
        let file_group = config.file_groups.first()?;
        if config.file_groups.len() != 1 || file_group.len() != 1 {
            return None;
        }
        let file = file_group.files().first()?;
        let location = file.object_meta.location.clone();
        if PhysicalFormat::from_extension(location.as_ref()).ok()?
            != PhysicalFormat::Vortex
        {
            return None;
        }
        let session = match &session {
            Some(existing) => existing.clone(),
            None => {
                let created = VortexSession::default().with_tokio();
                session = Some(created.clone());
                created
            }
        };

        let file_schema = config.file_schema().clone();
        let (projected_schema, projection_names) = projected_file_schema(config)?;
        let mut types = Vec::with_capacity(constraint.columns.len());
        for column in &constraint.columns {
            let field = file_schema.field_with_name(column).ok()?;
            if !key_type_supported(field.data_type()) {
                return None;
            }
            types.push(field.data_type().clone());
        }

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
        for value in &constraint.values {
            indices.extend(
                entry
                    .index
                    .rows_with_prefix(value.as_slice())
                    .map(u64::from),
            );
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

    use super::{
        Key, KeyConstraint, KeyIndex, KeyValue, extract_key_constraints,
        extract_pk_candidates,
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
        assert_eq!(constraint.values, expected);
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
        assert_eq!(constraint.values, expected);
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

    #[test]
    fn index_lookup_by_full_key_and_prefix() {
        let key = |g: &str, v: i64, row: u32| -> (Key, u32) {
            (smallvec![string_key(g), KeyValue::Int(v)], row)
        };
        let index = KeyIndex::new(vec![
            key("a", 1, 0),
            key("a", 3, 1),
            key("b", 2, 2),
            key("b", 2, 3),
            key("c", 9, 4),
        ]);
        assert_eq!(index.len(), 5);
        let rows = |prefix: &[KeyValue]| {
            let mut rows = index.rows_with_prefix(prefix).collect::<Vec<_>>();
            rows.sort_unstable();
            rows
        };
        // Full key: only the matching row, including non-unique keys.
        assert_eq!(rows(&[string_key("a"), KeyValue::Int(3)]), vec![1]);
        assert_eq!(rows(&[string_key("b"), KeyValue::Int(2)]), vec![2, 3]);
        assert_eq!(
            rows(&[string_key("z"), KeyValue::Int(1)]),
            Vec::<u32>::new()
        );
        // Prefix: every row of the group.
        assert_eq!(rows(&[string_key("a")]), vec![0, 1]);
        assert_eq!(rows(&[string_key("c")]), vec![4]);
        assert_eq!(rows(&[]), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn index_handles_unsorted_input_and_missing_keys() {
        let key = |g: &str, row: u32| -> (Key, u32) { (smallvec![string_key(g)], row) };
        let index =
            KeyIndex::new(vec![key("b", 0), key("a", 1), key("b", 2), key("c", 3)]);
        assert_eq!(
            index
                .rows_with_prefix(&[string_key("b")])
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(
            index
                .rows_with_prefix(&[string_key("a")])
                .collect::<Vec<_>>(),
            vec![1]
        );
    }
}
