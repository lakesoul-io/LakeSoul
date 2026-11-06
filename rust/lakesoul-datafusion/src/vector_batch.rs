// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul Contributors

//! Multi-query (snapshot batch) vector search.
//!
//! A single-query vector search pays the lakehouse protocol once per query:
//! the SQL plan, the snapshot file listing, one native reader per bucket and
//! the index resolution.  Measurements on the GloVe-200 index-only table show
//! that roughly 10 ms of CPU per bucket is spent before the actual index
//! probe, independent of how many vectors the bucket holds, so a four-bucket
//! query burns ~40 ms of CPU even though the probe itself costs 0.6 ms.
//!
//! This entry point searches **several query vectors against one snapshot**
//! instead: the shards are resolved once, every query is probed against the
//! same loaded index (`search_resolved_shard_batch`), and the candidate rows
//! of the whole batch are verified with a single native-reader pass per
//! bucket.  It does not replace the single-query path and does not change
//! table/transaction/snapshot/merge-on-read semantics — the batch simply
//! shares one snapshot, which is stated in its contract: results are exact
//! for that snapshot, and the caller picks the snapshot by choosing when to
//! call it.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use lakesoul_common::IndexKind;
use lakesoul_io::config::{LakeSoulIOConfigBuilder, OPTION_KEY_FILE_FILTER_PUSHDOWN};
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::key::{KeyCodec, KeyLayout};
use lakesoul_io::index::prefix::derive_index_prefixes;
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_io::vector::search::{VectorHit, search_resolved_shard_batch};
use lakesoul_metadata::index_catalog::{
    IndexCommitView, VectorCatalog, VectorSegmentEntry,
};
use lakesoul_vector::{IndexKey, SegmentEntry};
use object_store::ObjectStore;
use rootcause::compat::boxed_error::IntoBoxedError;

/// Queries per snapshot-batch search.
///
/// Measured on the GloVe-200 index-only table (4 buckets, nprobe 16): the
/// per-query cost drops from 16 ms at one query per batch to 3.0-3.2 ms at
/// 32-64 queries, while a 200-query batch regresses because the verified
/// candidate union grows with the batch. 32-64 is the measured optimum; 64
/// keeps more queries per snapshot without reaching the union blow-up.
pub const DEFAULT_BATCH_SIZE: usize = 64;

/// One batch vector search request.
#[derive(Debug, Clone)]
pub struct VectorBatchRequest {
    /// Vector column to search.
    pub column: String,
    /// One query vector per requested result set.
    pub queries: Vec<Vec<f32>>,
    /// Neighbours returned per query.
    pub top_k: usize,
    /// Clusters probed per shard.
    pub nprobe: usize,
    /// Cluster-probe parallelism inside one shard.
    pub parallelism: usize,
    /// Queries per snapshot batch; `0` uses [`DEFAULT_BATCH_SIZE`].
    pub batch_size: usize,
}

impl VectorBatchRequest {
    /// Queries per snapshot batch actually used.
    pub fn batch_size(&self) -> usize {
        if self.batch_size == 0 {
            DEFAULT_BATCH_SIZE
        } else {
            self.batch_size
        }
    }
}

/// Search every query of the batch against one snapshot of the table.
///
/// `files` are the table's data file URIs (the caller obtained them from the
/// snapshot it wants the batch pinned to), `table_prefix` the table
/// directory URI, `schema` the table schema (its primary-key columns feed the
/// index key codec) and `primary_keys` the primary-key column names.
///
/// Returns one `Vec<i64>` per query with the primary keys of its nearest
/// neighbours, ranked by the index distance (index-only ordering).
pub async fn search_vector_batch(
    catalog: &VectorCatalog,
    store: &Arc<dyn ObjectStore>,
    table_prefix: &str,
    files: &[String],
    schema: &SchemaRef,
    primary_keys: &[String],
    cdc_column: &str,
    request: &VectorBatchRequest,
) -> DFResult<Vec<Vec<i64>>> {
    if request.queries.is_empty() {
        return Ok(Vec::new());
    }
    // The snapshot is listed once by the caller; the queries are searched in
    // bounded groups so the verified candidate union stays small.
    let batch_size = request.batch_size();
    if request.queries.len() > batch_size {
        let mut results = Vec::with_capacity(request.queries.len());
        for chunk in request.queries.chunks(batch_size) {
            let mut chunk_request = request.clone();
            chunk_request.queries = chunk.to_vec();
            results.extend(
                search_vector_batch_chunk(
                    catalog,
                    store,
                    table_prefix,
                    files,
                    schema,
                    primary_keys,
                    cdc_column,
                    &chunk_request,
                )
                .await?,
            );
        }
        return Ok(results);
    }
    if primary_keys.len() != 1 || primary_keys[0] != "id" {
        return Err(DataFusionError::NotImplemented(format!(
            "vector batch search currently supports a single Int64 primary key \
             named 'id', got {primary_keys:?}"
        )));
    }

    search_vector_batch_chunk(
        catalog,
        store,
        table_prefix,
        files,
        schema,
        primary_keys,
        cdc_column,
        request,
    )
    .await
}

/// Search one bounded group of queries against one snapshot.
#[allow(clippy::too_many_arguments)]
async fn search_vector_batch_chunk(
    catalog: &VectorCatalog,
    store: &Arc<dyn ObjectStore>,
    table_prefix: &str,
    files: &[String],
    schema: &SchemaRef,
    primary_keys: &[String],
    cdc_column: &str,
    request: &VectorBatchRequest,
) -> DFResult<Vec<Vec<i64>>> {
    // One shard (index) per hash bucket; the batch resolves all of them with
    // one catalog round trip.
    let mut by_bucket: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for file in files {
        let Some(bucket) = lakesoul_io::helpers::extract_hash_bucket_id(file) else {
            continue;
        };
        by_bucket.entry(bucket).or_default().push(file.clone());
    }
    if by_bucket.is_empty() {
        return Err(DataFusionError::Internal(
            "no data files for the vector batch search".to_string(),
        ));
    }
    let mut buckets: Vec<(String, Vec<String>)> = Vec::with_capacity(by_bucket.len());
    let mut prefixes: Vec<String> = Vec::with_capacity(by_bucket.len());
    for (_, bucket_files) in by_bucket {
        let derived = derive_index_prefixes(
            &bucket_files,
            table_prefix,
            IndexKind::Vector,
            &request.column,
        );
        let Some((prefix, _)) = derived.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "no index prefix for a vector batch bucket".to_string(),
            ));
        };
        prefixes.push(prefix.clone());
        buckets.push((prefix, bucket_files));
    }
    prefixes.sort_unstable();
    prefixes.dedup();
    let views = catalog
        .resolve_cached_many(&prefixes)
        .await
        .map_err(|error| {
            DataFusionError::External(
                rootcause::report!(
                    "failed to resolve vector index shards for a batch: {}",
                    error
                )
                .into_boxed_error(),
            )
        })?;

    // The index stores arrow-Row encoded primary keys; the codec must match
    // the layout the index was built with.
    let layout = KeyLayout::from_schema(schema.as_ref(), primary_keys)
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    let expected_key_kind = layout.kind_hash();
    let pk_schema = pk_only_schema(schema, primary_keys)?;
    let codec = KeyCodec::new(layout.clone())
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;

    // Phase 1 + 2 run per bucket and in parallel: probe every bucket once for
    // every query of the batch, then verify that bucket's candidate rows with
    // one native-reader pass.
    let mut probe_futures = Vec::with_capacity(buckets.len());
    for (prefix, bucket_files) in &buckets {
        let prefix = prefix.clone();
        let bucket_files = bucket_files.clone();
        // Borrow the batch-wide state instead of moving it into each future.
        let views = &views;
        let layout = &layout;
        let codec = &codec;
        let pk_schema = &pk_schema;
        let primary_keys = primary_keys;
        let table_prefix = table_prefix;
        let cdc_column = cdc_column;
        let store = store;
        let request = request;
        probe_futures.push(async move {
            let view = views.get(&prefix).and_then(Option::as_ref).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "no vector index commit for shard '{prefix}'"
                ))
            })?;
            let resolved = resolved_index(&prefix, view)?;
            let hits = search_resolved_shard_batch(
                store,
                &resolved,
                &request.queries,
                request.top_k,
                request.nprobe,
                request.parallelism,
                expected_key_kind,
            )
            .await
            .map_err(|error| DataFusionError::External(error.into_boxed_error()))?
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "the vector index at '{prefix}' cannot serve this table's key layout"
                ))
            })?;
            let mut keys: Vec<IndexKey> = Vec::new();
            let mut seen: HashSet<IndexKey> = HashSet::new();
            for per_query in &hits {
                for hit in per_query {
                    if seen.insert(hit.key.clone()) {
                        keys.push(hit.key.clone());
                    }
                }
            }
            // Skip the verification read when the index provably covers
            // every file of this snapshot and nothing can have deleted a row
            // behind its back: no index mutations, no CDC change column (the
            // table cannot hold delete tombstones) and no active file that
            // the commit does not list.  Anything else falls back to the
            // reader pass below.
            let covered: Vec<&str> = view
                .segments
                .iter()
                .flat_map(|segment| segment.data_files.iter().map(String::as_str))
                .collect();
            let force_verify =
                std::env::var("LAKESOUL_VECTOR_BATCH_FORCE_VERIFY").is_ok();
            let skip_verification = !force_verify
                && can_skip_verification(
                    cdc_column,
                    view.mutation_head.is_none(),
                    view.mutation_records,
                    &bucket_files,
                    &covered,
                );
            if skip_verification {
                tracing::debug!(
                    prefix = %prefix,
                    keys = seen.len(),
                    "batch candidate verification skipped (index covers the snapshot)"
                );
            }
            let live = if skip_verification {
                seen
            } else {
                read_live_keys(
                    &layout,
                    table_prefix,
                    &bucket_files,
                    &pk_schema,
                    primary_keys,
                    &codec,
                    &keys,
                )
                .await?
            };
            Ok::<_, DataFusionError>((hits, live))
        });
    }
    let probed = futures::future::try_join_all(probe_futures).await?;
    let mut bucket_hits: Vec<Vec<Vec<VectorHit>>> = Vec::with_capacity(probed.len());
    let mut bucket_live: Vec<HashSet<IndexKey>> = Vec::with_capacity(probed.len());
    for (hits, live) in probed {
        bucket_hits.push(hits);
        bucket_live.push(live);
    }

    // Phase 3: keep verified candidates per query, merge the buckets' ranked
    // candidates by index distance and take the global top-k per query.
    let queries = request.queries.len();
    let mut results: Vec<Vec<i64>> = Vec::with_capacity(queries);
    for query_index in 0..queries {
        let mut ranked: Vec<(f32, i64)> =
            Vec::with_capacity(bucket_hits.len() * request.top_k);
        for (bucket, hits) in bucket_hits.iter().enumerate() {
            let live = &bucket_live[bucket];
            let per_query = &hits[query_index];
            // Several hits can share a key when a delta segment holds a newer
            // vector for the same row; keep the best (lowest) distance.
            let mut best: BTreeMap<&IndexKey, f32> = BTreeMap::new();
            for hit in per_query {
                if !live.contains(&hit.key) {
                    continue;
                }
                best.entry(&hit.key)
                    .and_modify(|score| *score = score.min(hit.score))
                    .or_insert(hit.score);
            }
            for (key, score) in best {
                if let Some(id) = decode_id(&codec, key)? {
                    ranked.push((score, id));
                }
            }
        }
        // L2 index distance: smaller is closer, the same ordering the
        // single-query path uses with `lakesoul_vector_search.rerank=index`.
        ranked.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        ranked.truncate(request.top_k);
        results.push(ranked.into_iter().map(|(_, id)| id).collect());
    }
    Ok(results)
}

/// The reader only needs the primary-key columns for the verification pass.
fn pk_only_schema(schema: &SchemaRef, primary_keys: &[String]) -> DFResult<SchemaRef> {
    let fields = schema
        .fields()
        .iter()
        .filter(|field| primary_keys.iter().any(|key| key == field.name()))
        .cloned()
        .collect::<Vec<_>>();
    if fields.len() != primary_keys.len() {
        return Err(DataFusionError::Execution(format!(
            "primary key columns {primary_keys:?} are missing from the table schema"
        )));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn resolved_index(
    prefix: &str,
    view: &IndexCommitView<VectorSegmentEntry>,
) -> DFResult<ResolvedIndex> {
    let segments: Vec<SegmentEntry> = view
        .segments
        .iter()
        .map(|segment| SegmentEntry {
            cluster_id: segment.cluster_id,
            segment_version: segment.segment_version,
            segment_filename: segment.filename.clone(),
            num_vectors: segment.num_vectors,
            file_size: segment.file_size,
            data_files: segment.data_files.clone(),
            birth_version: segment.birth_version,
        })
        .collect();
    let segments = serde_json::to_value(&segments).map_err(|error| {
        DataFusionError::Execution(format!(
            "failed to serialize vector index segments of '{prefix}': {error}"
        ))
    })?;
    Ok(ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: prefix.to_string(),
        commit_id: view.commit_id,
        generation: view.generation,
        version: view.version,
        header: view.header.clone(),
        segments,
        mutation_head: view.mutation_head.clone(),
        mutation_records: view.mutation_records,
    })
}

/// Read the candidate rows of one bucket and return the keys that still exist
/// in the snapshot (merge-on-read applied by the native reader).
#[allow(clippy::too_many_arguments)]
async fn read_live_keys(
    layout: &KeyLayout,
    table_prefix: &str,
    files: &[String],
    pk_schema: &SchemaRef,
    primary_keys: &[String],
    codec: &KeyCodec,
    keys: &[IndexKey],
) -> DFResult<HashSet<IndexKey>> {
    if keys.is_empty() {
        return Ok(HashSet::new());
    }
    let decoded = codec
        .decode(keys)
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    if decoded.len() != primary_keys.len() {
        return Err(DataFusionError::Execution(
            "the index key codec did not decode one column per primary key".to_string(),
        ));
    }
    let mut filters = Vec::with_capacity(primary_keys.len());
    for (name, array) in primary_keys.iter().zip(&decoded) {
        let mut values = Vec::with_capacity(array.len());
        for row in 0..array.len() {
            values.push(Expr::Literal(
                ScalarValue::try_from_array(array, row)?,
                None,
            ));
        }
        filters.push(
            Expr::Column(Column::new_unqualified(name.clone())).in_list(values, false),
        );
    }
    let config = LakeSoulIOConfigBuilder::default()
        .with_files(files.to_vec())
        .with_primary_keys(primary_keys.to_vec())
        .with_schema(Arc::clone(pk_schema))
        .with_prefix(table_prefix.to_string())
        .with_filters(filters)
        .with_option(OPTION_KEY_FILE_FILTER_PUSHDOWN, "true")
        .build();
    let layout = layout.clone();
    // The reader's async API is not `Send`, so it runs on the blocking pool.
    tokio::task::spawn_blocking(move || -> DFResult<HashSet<IndexKey>> {
        let codec = KeyCodec::new(layout)
            .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let reader = LakeSoulReader::new(config)
            .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let mut sync_reader =
            SyncSendableMutableLakeSoulReader::new_with_global_runtime(reader);
        sync_reader
            .start_blocked()
            .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let mut live: HashSet<IndexKey> = HashSet::new();
        while let Some(batch) = sync_reader.next_rb_blocked() {
            let batch = batch
                .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
            let batch_keys = codec
                .encode_batch(&batch)
                .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
            live.extend(batch_keys);
        }
        Ok(live)
    })
    .await
    .map_err(|error| {
        DataFusionError::Execution(format!(
            "vector batch verification task failed: {error}"
        ))
    })?
}

/// Whether the candidate rows of one bucket can skip the reader pass.
///
/// The pass exists to drop index keys whose row is no longer part of the
/// snapshot (merge-on-read deletes, CDC tombstones, files the commit does not
/// cover).  When none of those can apply — the table has no CDC column, the
/// commit carries no mutation chain, and every active file of the bucket is
/// listed by the commit — every index key is a live row of the snapshot and
/// the keys can be trusted as they are.
fn can_skip_verification(
    cdc_column: &str,
    mutation_head_absent: bool,
    mutation_records: u32,
    active_files: &[String],
    covered_files: &[&str],
) -> bool {
    cdc_column.is_empty()
        && mutation_head_absent
        && mutation_records == 0
        && !covered_files.is_empty()
        && lakesoul_common::index::pending_files(active_files, covered_files).is_empty()
}

/// Decode one index key back into the single Int64 primary key.
fn decode_id(codec: &KeyCodec, key: &IndexKey) -> DFResult<Option<i64>> {
    let decoded = codec
        .decode(std::slice::from_ref(key))
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    let Some(array) = decoded.first() else {
        return Ok(None);
    };
    let Some(array) = array.as_any().downcast_ref::<arrow::array::Int64Array>() else {
        return Ok(None);
    };
    Ok(array.iter().next().flatten())
}

#[cfg(test)]
mod tests {
    use super::can_skip_verification;

    fn files(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn verification_skips_only_when_nothing_can_hide_a_row() {
        let active = files(&["a.vortex", "b.vortex"]);
        let covered = ["a.vortex", "b.vortex"];
        // Fresh index: every active file is covered, no deletes possible.
        assert!(can_skip_verification("", true, 0, &active, &covered));
        // A CDC change column can hold delete tombstones for indexed rows.
        assert!(!can_skip_verification(
            "_change_type",
            true,
            0,
            &active,
            &covered
        ));
        // An index mutation chain hides keys itself.
        assert!(!can_skip_verification("", false, 0, &active, &covered));
        assert!(!can_skip_verification("", true, 3, &active, &covered));
        // A file the commit does not cover (append, compaction, rebuild in
        // progress) needs the reader pass to resolve the rows.
        assert!(!can_skip_verification(
            "",
            true,
            0,
            &files(&["a.vortex", "c.vortex"]),
            &covered,
        ));
        // An index that does not record its covered files cannot be trusted.
        assert!(!can_skip_verification("", true, 0, &active, &[]));
        // Coverage is compared by object name, not by full URI.
        assert!(can_skip_verification(
            "",
            true,
            0,
            &files(&["file:///data/a.vortex", "s3://bucket/b.vortex"]),
            &covered,
        ));
    }
}
