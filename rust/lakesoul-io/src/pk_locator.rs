// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Row-level primary-key locator.
//!
//! `pk = value` and `pk IN (...)` predicates are answered by locating the
//! matching rows through a process-wide, per-file `pk -> row` map instead of
//! scanning every file.  The map is built lazily from the file's primary key
//! column and is keyed by the file location alone: data files are immutable,
//! so a map never goes stale (a rewritten or compacted file gets a new
//! location and therefore a new cache entry).
//!
//! The cache is bounded by `LAKESOUL_PK_CACHE_BYTES` (default 256 MiB, `0`
//! disables the locator).  Only vortex data files and single integer
//! primary keys are supported; anything else falls back to the regular
//! scan path.
//!
//! The map assumes what LakeSoul's sorted writer guarantees: rows of a data
//! file are unique by primary key and stored in primary-key order.  Rows are
//! fetched by row index in ascending order, which therefore yields batches
//! sorted by primary key as the merge-on-read path requires.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Schema, SchemaRef};
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
use tracing::{debug, warn};
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::memory::MemorySessionExt;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::Buffer;
use vortex::dtype::PType;
use vortex::expr::{get_item, root};
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
    (HITS.load(Ordering::Relaxed), MISSES.load(Ordering::Relaxed))
}

/// Approximate heap footprint of a `HashMap<i64, u32>`.
fn map_bytes(entries: usize) -> usize {
    entries.saturating_mul(24).saturating_add(128)
}

/// Primary-key index of one data file: pk value -> row index.
pub struct PkMap {
    rows: HashMap<i64, u32>,
}

impl PkMap {
    /// Row index of `pk` in the file, if present.
    pub fn get(&self, pk: i64) -> Option<u32> {
        self.rows.get(&pk).copied()
    }

    /// Number of indexed rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the map has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Cached per-file state: the pk -> row map plus the opened file handle
/// (opening a vortex file reads its footer and layout, which is a
/// per-query cost worth amortizing).
struct CachedFile {
    map: PkMap,
    file: VortexFile,
    bytes: usize,
}

/// Rough heap estimate for an open file's footer/layout metadata, on top of
/// the pk map itself.
const FILE_METADATA_BYTES: usize = 64 * 1024;

/// Extract a finite primary-key candidate set from logical filters.
///
/// Supports `pk = literal`, `pk IN (literals)` and `pk = a OR pk = b` (all
/// `OR` branches must be primary-key equalities).  Conditions joined by
/// `AND` are intersected.  Any other expression is ignored; the returned
/// set is always a superset of the rows matching the full filter, which is
/// what row-level pruning needs (the caller keeps applying the real filter
/// above the scan).  Returns `None` when no finite set can be extracted.
pub fn extract_pk_candidates(
    filters: &[Expr],
    pk_column: &str,
    pk_type: &DataType,
) -> Option<Vec<i64>> {
    if !matches!(pk_type, DataType::Int32 | DataType::Int64) {
        return None;
    }
    let mut acc: Option<HashSet<i64>> = None;
    for filter in filters {
        let Some(set) = finite_pk_set(filter, pk_column) else {
            continue;
        };
        acc = Some(match acc {
            None => set,
            Some(prev) => prev.intersection(&set).copied().collect(),
        });
        if acc.as_ref().is_some_and(|set| set.is_empty()) {
            return None;
        }
    }
    let acc = acc?;
    if acc.is_empty() || acc.len() > MAX_PK_CANDIDATES {
        return None;
    }
    Some(acc.into_iter().collect())
}

/// A finite pk set carried by a single expression, or `None` when the
/// expression is not a finite pk-only constraint.
fn finite_pk_set(expr: &Expr, pk_column: &str) -> Option<HashSet<i64>> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            let value = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), literal) if col.name == pk_column => {
                    literal_to_i64(literal)
                }
                (literal, Expr::Column(col)) if col.name == pk_column => {
                    literal_to_i64(literal)
                }
                _ => None,
            }?;
            Some(HashSet::from([value]))
        }
        Expr::InList(in_list) if !in_list.negated => {
            let Expr::Column(col) = in_list.expr.as_ref() else {
                return None;
            };
            if col.name != pk_column {
                return None;
            }
            in_list
                .list
                .iter()
                .map(literal_to_i64)
                .collect::<Option<HashSet<_>>>()
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = finite_pk_set(&binary.left, pk_column)?;
            let right = finite_pk_set(&binary.right, pk_column)?;
            Some(left.union(&right).copied().collect())
        }
        _ => None,
    }
}

fn literal_to_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(scalar, _) => scalar_to_i64(scalar),
        _ => None,
    }
}

fn scalar_to_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        _ => None,
    }
}

fn precision_to_i64(precision: &Precision<ScalarValue>) -> Option<i64> {
    match precision {
        Precision::Exact(value) | Precision::Inexact(value) => scalar_to_i64(value),
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

/// Load (or fetch from the cache) the pk map and file handle of one file.
async fn get_or_load_file(
    store: Arc<dyn ObjectStore>,
    location: StorePath,
    pk_column: String,
    session: VortexSession,
) -> Option<Arc<CachedFile>> {
    let Some(cache) = PK_CACHE.as_ref() else {
        return build_cached_file(store, location, &pk_column, &session)
            .await
            .ok();
    };
    let key = location.to_string();
    if cache.get(&key).await.is_some() {
        HITS.fetch_add(1, Ordering::Relaxed);
    } else {
        MISSES.fetch_add(1, Ordering::Relaxed);
    }
    let load = async move {
        build_cached_file(store, location, &pk_column, &session)
            .await
            .map_err(|e| e.to_string())
    };
    match cache.try_get_with(key, load).await {
        Ok(entry) => Some(entry),
        Err(error) => {
            warn!("failed to build pk map: {error}");
            None
        }
    }
}

async fn build_cached_file(
    store: Arc<dyn ObjectStore>,
    location: StorePath,
    pk_column: &str,
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
    let projection = get_item(pk_column, root())
        .bind(file.dtype())
        .map_err(|e| open_error(&location, e))?;
    let mut stream = file
        .scan()
        .map_err(|e| open_error(&location, e))?
        .with_projection(projection)
        .into_array_stream()
        .map_err(|e| open_error(&location, e))?;
    let mut ctx = session.create_execution_ctx();
    let mut rows: HashMap<i64, u32> = HashMap::new();
    let mut offset: u64 = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| open_error(&location, e))?;
        let len = chunk.len() as u64;
        let primitive = chunk
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(|e| open_error(&location, e))?;
        for (i, value) in primitive_values(&primitive)?.into_iter().enumerate() {
            let row = offset + i as u64;
            let row = u32::try_from(row).map_err(|_| {
                rootcause::report!("file {location} has more than u32::MAX rows")
            })?;
            rows.insert(value, row);
        }
        offset += len;
    }
    let bytes = map_bytes(rows.len()) + FILE_METADATA_BYTES;
    debug!(
        "built pk map for {location}: {} rows, {} bytes",
        rows.len(),
        bytes
    );
    Ok(Arc::new(CachedFile {
        map: PkMap { rows },
        file,
        bytes,
    }))
}

fn open_error(location: &StorePath, error: impl std::fmt::Display) -> rootcause::Report {
    rootcause::report!("failed to read pk column of {location}: {error}")
}

fn primitive_values(primitive: &PrimitiveArray) -> crate::Result<Vec<i64>> {
    let values = match primitive.ptype() {
        PType::I8 => primitive
            .as_slice::<i8>()
            .iter()
            .map(|v| *v as i64)
            .collect(),
        PType::I16 => primitive
            .as_slice::<i16>()
            .iter()
            .map(|v| *v as i64)
            .collect(),
        PType::I32 => primitive
            .as_slice::<i32>()
            .iter()
            .map(|v| *v as i64)
            .collect(),
        PType::I64 => primitive.as_slice::<i64>().to_vec(),
        PType::U8 => primitive
            .as_slice::<u8>()
            .iter()
            .map(|v| *v as i64)
            .collect(),
        PType::U16 => primitive
            .as_slice::<u16>()
            .iter()
            .map(|v| *v as i64)
            .collect(),
        PType::U32 => primitive
            .as_slice::<u32>()
            .iter()
            .map(|v| *v as i64)
            .collect(),
        PType::U64 => primitive
            .as_slice::<u64>()
            .iter()
            .map(|v| i64::try_from(*v))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| rootcause::report!("primary key does not fit in i64"))?,
        other => {
            return Err(rootcause::report!(
                "unsupported primary key type for pk locator: {other:?}"
            ));
        }
    };
    Ok(values)
}

/// Build row-level inputs for a primary-key filter, one input per flattened
/// file config.  Returns `None` (caller falls back to a regular scan) when
/// the locator is disabled or any file is unsupported.
///
/// Files whose statistics prove the candidate set absent are returned as an
/// empty input, so the resulting plan reads nothing for them.
pub async fn try_build_pk_inputs(
    state: &dyn Session,
    io_config: &LakeSoulIOConfig,
    configs: &[FileScanConfig],
    candidates: &[i64],
) -> Option<Vec<Arc<dyn ExecutionPlan>>> {
    if PK_CACHE.is_none() {
        return None;
    }
    if candidates.is_empty() || candidates.len() > MAX_PK_CANDIDATES {
        return None;
    }
    let primary_keys = io_config.primary_keys_slice();
    if primary_keys.len() != 1 {
        return None;
    }
    let pk_column = primary_keys[0].clone();
    let wanted: HashSet<i64> = candidates.iter().copied().collect();
    let session = VortexSession::default().with_tokio();
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

        let file_schema = config.file_schema().clone();
        let (projected_schema, projection_names) = projected_file_schema(config)?;

        let pk_field = file_schema.field_with_name(&pk_column).ok()?;
        if !matches!(pk_field.data_type(), DataType::Int64 | DataType::Int32) {
            return None;
        }

        if let Some((min, max)) = pk_min_max(config, &pk_column)
            && !wanted.iter().any(|value| *value >= min && *value <= max)
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
            pk_column.clone(),
            session.clone(),
        )
        .await?;
        t_cache += t.elapsed();

        let mut indices: Vec<u64> = wanted
            .iter()
            .filter_map(|value| entry.map.get(*value).map(|row| row as u64))
            .collect();
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
        .inspect_err(|e| warn!("failed to fetch pk candidate rows: {e}"))
        .ok()?;
        t_take += t.elapsed();
        n_rows += indices.len();
        let exec =
            MemorySourceConfig::try_new_exec(&[batches], projected_schema, None).ok()?;
        inputs.push(exec as Arc<dyn ExecutionPlan>);
    }
    if profile {
        eprintln!(
            "pk_locator: candidates={} files={} pruned={} rows={} cache={:?} take={:?} total={:?}",
            candidates.len(),
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
    use datafusion::prelude::{col, lit};
    use datafusion_expr::Expr;

    use super::extract_pk_candidates;

    fn candidates(filters: &[Expr]) -> Option<Vec<i64>> {
        extract_pk_candidates(filters, "id", &arrow::datatypes::DataType::Int64)
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
    fn rejects_non_integer_pk() {
        let filters = vec![col("id").eq(lit("abc"))];
        assert_eq!(
            extract_pk_candidates(&filters, "id", &arrow::datatypes::DataType::Utf8),
            None
        );
    }

    #[test]
    fn caps_candidate_count() {
        let list = (0..=super::MAX_PK_CANDIDATES)
            .map(|v| lit(v as i64))
            .collect::<Vec<_>>();
        let filters = vec![col("id").in_list(list, false)];
        assert_eq!(candidates(&filters), None);
    }
}
