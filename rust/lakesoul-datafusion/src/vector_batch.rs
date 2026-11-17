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

pub mod distributed;
pub mod native;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use futures::{StreamExt, TryStreamExt};
use lakesoul_common::IndexKind;
use lakesoul_io::config::{LakeSoulIOConfigBuilder, OPTION_KEY_FILE_FILTER_PUSHDOWN};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::index::key::{KeyCodec, KeyLayout};
use lakesoul_io::index::prefix::derive_index_prefixes;
use lakesoul_io::pk_locator::{PrefixMatch, VectorBlockSource};
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_io::vector::search::{
    LoadedShard, VectorHit, cached_typed_shard_for_store, load_typed_shard,
};
use lakesoul_metadata::index_catalog::{
    IndexCommitView, VectorCatalog, VectorSegmentEntry,
};
use lakesoul_vector::{IndexKey, SegmentEntry};
use object_store::ObjectStore;
use object_store::path::Path as StorePath;
use rootcause::compat::boxed_error::IntoBoxedError;

/// Queries per snapshot-batch search.
///
/// Measured on the GloVe-200 index-only table (4 buckets, nprobe 16): the
/// per-query cost drops from 16 ms at one query per batch to 3.0-3.2 ms at
/// 32-64 queries, while a 200-query batch regresses because the verified
/// candidate union grows with the batch. 32-64 is the measured optimum; 64
/// keeps more queries per snapshot without reaching the union blow-up.
pub const DEFAULT_BATCH_SIZE: usize = 64;

/// Queries a fast-path batch takes at most.
///
/// The fast path keeps every candidate of a batch in memory from the probe
/// until the merge, so the batch also bounds the query-phase footprint: a
/// 200-query batch costs a few MB, while keeping 10k queries in one batch
/// roughly doubles the process RSS for ~8% more QPS.  Chunking at this size
/// keeps the footprint flat and, since chunks overlap, the throughput.
fn max_fast_batch_queries() -> usize {
    std::env::var("LAKESOUL_VECTOR_BATCH_CHUNK_QUERIES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(MAX_FAST_BATCH_QUERIES)
}

const MAX_FAST_BATCH_QUERIES: usize = 1024;

/// Chunks of one batch kept in flight at the same time.
///
/// One chunk already fills the machine: its index probes use the whole rayon
/// pool and its ranking uses every core.  Overlapping two chunks hides the
/// serial fold between the probe and the ranking without multiplying the
/// memory a batch holds.
fn max_concurrent_chunks() -> usize {
    std::env::var("LAKESOUL_VECTOR_BATCH_CHUNKS_IN_FLIGHT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(MAX_CONCURRENT_CHUNKS)
}

const MAX_CONCURRENT_CHUNKS: usize = 2;

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
    /// Re-rank each query's candidates by the exact distance of their stored
    /// vectors instead of the index estimate.
    ///
    /// The exact re-rank needs the on-demand decoded-block cache
    /// (`LAKESOUL_CACHE`), a proven snapshot coverage and per-file index
    /// ranks (several data files per bucket are supported); any other table
    /// gets a clear error instead of silently approximate results.  Both L2 and IP indices are supported:
    /// the scores are the squared distance (L2) or the negated dot product
    /// (IP), so one ascending sort ranks both.
    pub exact: bool,
    /// Candidates per bucket re-ranked in exact mode (`top_k * overfetch`).
    /// Exact mode needs a wider net than index-only ordering: the index
    /// estimate only has to *find* the neighbours, not order them.
    pub exact_overfetch: usize,
    /// Distance metric of the searched column's index: `"L2"` or `"IP"`.
    ///
    /// The index reports a distance for `L2` (smaller is closer) and the dot
    /// product for `IP` (larger is closer); the batch ranking follows the
    /// same direction, and the exact re-rank negates the dot product so one
    /// ascending sort serves both.
    pub metric: String,
}

impl VectorBatchRequest {
    /// Candidates per bucket the index probe has to return.
    pub fn candidates_per_bucket(&self) -> usize {
        if self.exact {
            self.top_k.saturating_mul(self.exact_overfetch.max(1))
        } else {
            self.top_k
        }
    }

    /// True when the index score is a distance (L2); false for the inner
    /// product, where the larger score is the better one.
    pub fn ascending(&self) -> bool {
        self.metric.eq_ignore_ascii_case("L2")
    }

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
/// `(index prefix, files)` of every hash bucket of one snapshot.
/// Rank one query's candidates across the buckets and keep its top-k.
fn merge_query(
    bucket_ranked: &[Vec<Vec<(f32, i64)>>],
    query_index: usize,
    top_k: usize,
    bucket_count: usize,
    ascending: bool,
) -> Vec<i64> {
    let mut ranked: Vec<(f32, i64)> = Vec::with_capacity(bucket_count * top_k);
    for per_bucket in bucket_ranked {
        ranked.extend(per_bucket[query_index].iter().copied());
    }
    // L2 index distance: smaller is closer; IP dot product: larger is closer.
    ranked.sort_by(|left, right| {
        let order = left.0.total_cmp(&right.0);
        let order = if ascending { order } else { order.reverse() };
        order.then_with(|| left.1.cmp(&right.1))
    });
    ranked.truncate(top_k);
    ranked.into_iter().map(|(_, id)| id).collect()
}

/// Rank one bucket's per-query candidates and decode the primary keys.
///
/// Candidate keys are deduplicated per query (the index may hold a key in
/// several segments) and the newest/minimum distance is kept.  `live` filters
/// the candidates to the rows the verification read found in the snapshot;
/// `None` means the coverage proof already guarantees every candidate is live.
/// Rank one query's candidates and decode the surviving primary keys.
///
/// Candidate keys are deduplicated (the index may hold a key in several
/// segments) and the smallest index distance is kept.  `live` filters the
/// candidates to the rows the verification read found in the snapshot;
/// `None` means the coverage proof already guarantees every candidate is live.
fn rank_query(
    codec: &KeyCodec,
    per_query_hits: &[VectorHit],
    live: Option<&HashSet<IndexKey>>,
    ascending: bool,
) -> DFResult<Vec<(f32, i64)>> {
    let mut best: BTreeMap<&IndexKey, f32> = BTreeMap::new();
    for hit in per_query_hits {
        if live.is_some_and(|live| !live.contains(&hit.key)) {
            continue;
        }
        // Several segments may hold the same key: keep the better score, the
        // smaller distance or the larger dot product.
        best.entry(&hit.key)
            .and_modify(|score| {
                *score = if ascending {
                    score.min(hit.score)
                } else {
                    score.max(hit.score)
                }
            })
            .or_insert(hit.score);
    }
    if best.is_empty() {
        return Ok(Vec::new());
    }
    // The map already borrows the keys of the search results: decode them by
    // reference instead of cloning one key per candidate.
    let keys: Vec<&IndexKey> = best.keys().copied().collect();
    let ids = decode_ids_refs(codec, &keys)?;
    Ok(best.values().copied().zip(ids).collect())
}

/// Rank one bucket's candidates for every query, single-threaded.
fn rank_bucket(
    codec: &KeyCodec,
    hits: &[Vec<VectorHit>],
    live: Option<&HashSet<IndexKey>>,
    ascending: bool,
) -> DFResult<Vec<Vec<(f32, i64)>>> {
    hits.iter()
        .map(|per_query_hits| rank_query(codec, per_query_hits, live, ascending))
        .collect()
}

/// Rank one query's candidates across the buckets and keep its top-k.
fn merge_one_query(
    codec: &KeyCodec,
    bucket_hits: &[Vec<Vec<VectorHit>>],
    query_index: usize,
    top_k: usize,
    ascending: bool,
) -> DFResult<Vec<i64>> {
    let mut ranked: Vec<(f32, i64)> = Vec::with_capacity(bucket_hits.len() * top_k);
    for hits in bucket_hits {
        ranked.extend(rank_query(codec, &hits[query_index], None, ascending)?);
    }
    // L2 index distance: smaller is closer; IP dot product: larger is closer.
    ranked.sort_by(|left, right| {
        let order = left.0.total_cmp(&right.0);
        let order = if ascending { order } else { order.reverse() };
        order.then_with(|| left.1.cmp(&right.1))
    });
    ranked.truncate(top_k);
    Ok(ranked.into_iter().map(|(_, id)| id).collect())
}

/// Rank and merge every query, spreading the per-query work over all cores.
///
/// Both stages are per query, so fusing them keeps one parallel region: the
/// candidate dedupe/decode of every bucket and the cross-bucket top-k of a
/// query run on the same core, and no separate merge pass has to spawn threads
/// again.
fn rank_and_merge(
    codec: &KeyCodec,
    bucket_hits: &[Vec<Vec<VectorHit>>],
    top_k: usize,
    ascending: bool,
) -> DFResult<Vec<Vec<i64>>> {
    let queries = bucket_hits.first().map(|hits| hits.len()).unwrap_or(0);
    if queries == 0 {
        return Ok(Vec::new());
    }
    let threads = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
        .min(queries);
    if threads <= 1 {
        return (0..queries)
            .map(|query_index| {
                merge_one_query(codec, bucket_hits, query_index, top_k, ascending)
            })
            .collect();
    }
    let per_thread = queries.div_ceil(threads).max(1);
    let mut parts: Vec<(usize, Vec<i64>)> = Vec::with_capacity(queries);
    let merged: DFResult<()> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..queries)
            .step_by(per_thread)
            .map(|start| {
                let end = (start + per_thread).min(queries);
                scope.spawn(move || -> DFResult<Vec<(usize, Vec<i64>)>> {
                    let mut rows = Vec::with_capacity(end - start);
                    for query_index in start..end {
                        rows.push((
                            query_index,
                            merge_one_query(
                                codec,
                                bucket_hits,
                                query_index,
                                top_k,
                                ascending,
                            )?,
                        ));
                    }
                    Ok(rows)
                })
            })
            .collect();
        for handle in handles {
            parts.extend(handle.join().expect("vector batch rank thread")?);
        }
        Ok(())
    });
    merged?;
    parts.sort_by_key(|(query_index, _)| *query_index);
    Ok(parts.into_iter().map(|(_, ids)| ids).collect())
}

fn bucket_files_by_prefix(
    table_prefix: &str,
    files: &[String],
    column: &str,
) -> Vec<(String, Vec<String>)> {
    let mut by_bucket: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for file in files {
        let Some(bucket) = lakesoul_io::helpers::extract_hash_bucket_id(file) else {
            continue;
        };
        by_bucket.entry(bucket).or_default().push(file.clone());
    }
    let mut buckets = Vec::with_capacity(by_bucket.len());
    for (_, bucket_files) in by_bucket {
        let derived =
            derive_index_prefixes(&bucket_files, table_prefix, IndexKind::Vector, column);
        if let Some((prefix, _)) = derived.into_iter().next() {
            buckets.push((prefix, bucket_files));
        }
    }
    buckets
}

/// Default queries per batch for one snapshot.
///
/// A batch that verifies its candidates reads the union of every query's
/// candidates with one reader pass per bucket, and that union grows with the
/// batch, so it stays at [`DEFAULT_BATCH_SIZE`].  When no bucket needs the
/// pass (see [`can_skip_verification`]) the union is free and the batch can
/// take the whole query set, which amortizes the per-batch shard work: on the
/// GloVe-200 index-only table this is 3.8k QPS against 2.9k at 64 queries.
fn adaptive_batch_size(
    buckets: &[(String, Vec<String>)],
    views: &std::collections::HashMap<
        String,
        Option<Arc<IndexCommitView<VectorSegmentEntry>>>,
    >,
    cdc_column: &str,
    request: &VectorBatchRequest,
) -> usize {
    if std::env::var("LAKESOUL_VECTOR_BATCH_FORCE_VERIFY").is_ok() {
        return DEFAULT_BATCH_SIZE;
    }
    if buckets.is_empty() {
        return DEFAULT_BATCH_SIZE;
    }
    let skip_everywhere = buckets.iter().all(|(prefix, bucket_files)| {
        let Some(view) = views.get(prefix).and_then(Option::as_ref) else {
            return false;
        };
        let covered: Vec<&str> = view
            .segments
            .iter()
            .flat_map(|segment| segment.data_files.iter().map(String::as_str))
            .collect();
        can_skip_verification(
            cdc_column,
            view.mutation_head.is_none(),
            view.mutation_records,
            bucket_files,
            &covered,
        )
    });
    if skip_everywhere {
        request.queries.len().clamp(1, max_fast_batch_queries())
    } else {
        DEFAULT_BATCH_SIZE
    }
}

pub async fn search_vector_batch(
    catalog: &VectorCatalog,
    store: &Arc<dyn ObjectStore>,
    table_prefix: &str,
    files: &[String],
    locations: &[StorePath],
    schema: &SchemaRef,
    primary_keys: &[String],
    cdc_column: &str,
    request: &VectorBatchRequest,
) -> DFResult<Vec<Vec<i64>>> {
    if request.queries.is_empty() {
        return Ok(Vec::new());
    }
    // Resolve every bucket's shard once for the whole call: the batch-size
    // decision and every chunk read the same views.  Resolving per chunk used
    // to cost one catalog round trip per chunk.
    let buckets = bucket_files_by_prefix(table_prefix, files, &request.column);
    // Exact re-rank reads the stored vectors straight from the data files, so
    // it needs the object-store location of each file (the URIs are only used
    // for the index prefixes).
    let file_locations: std::collections::HashMap<String, StorePath> = files
        .iter()
        .cloned()
        .zip(locations.iter().cloned())
        .collect();
    if buckets.is_empty() {
        return Err(DataFusionError::Internal(
            "no data files for the vector batch search".to_string(),
        ));
    }
    let mut prefixes: Vec<String> =
        buckets.iter().map(|(prefix, _)| prefix.clone()).collect();
    prefixes.sort_unstable();
    prefixes.dedup();
    let t_resolve = std::time::Instant::now();
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
    if std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok() {
        tracing::debug!(
            resolve_ms = t_resolve.elapsed().as_secs_f64() * 1000.0,
            shards = prefixes.len(),
            "resolved batch shards"
        );
    }
    // The snapshot is listed once by the caller; the queries are searched in
    // bounded groups so the verified candidate union stays small.  A batch
    // that needs no verification read can take the whole query set instead.
    let batch_size = if request.batch_size != 0 {
        request.batch_size
    } else {
        adaptive_batch_size(&buckets, &views, cdc_column, request)
    };
    if request.queries.len() > batch_size {
        // The chunks are independent; overlap a bounded number of them so the
        // serial fold between probe and ranking of one chunk is hidden by the
        // other's probe, while the number of queries held in memory stays
        // bounded.  Results are concatenated in query order.
        let views = &views;
        let buckets = &buckets;
        let chunk_futures =
            request
                .queries
                .chunks(batch_size)
                .enumerate()
                .map(|(index, chunk)| {
                    let mut chunk_request = request.clone();
                    chunk_request.queries = chunk.to_vec();
                    let file_locations = file_locations.clone();
                    async move {
                        let rows = search_vector_batch_chunk(
                            views,
                            buckets,
                            store,
                            table_prefix,
                            schema,
                            primary_keys,
                            cdc_column,
                            &file_locations,
                            &chunk_request,
                        )
                        .await?;
                        Ok::<_, DataFusionError>((index, rows))
                    }
                });
        let mut chunked = futures::stream::iter(chunk_futures)
            .buffer_unordered(max_concurrent_chunks())
            .try_collect::<Vec<_>>()
            .await?;
        chunked.sort_by_key(|(index, _)| *index);
        let mut results = Vec::with_capacity(request.queries.len());
        for (_, chunk_results) in chunked {
            results.extend(chunk_results);
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
        &views,
        &buckets,
        store,
        table_prefix,
        schema,
        primary_keys,
        cdc_column,
        &file_locations,
        request,
    )
    .await
}

/// Search one bounded group of queries against one snapshot.
#[allow(clippy::too_many_arguments)]
async fn search_vector_batch_chunk(
    views: &std::collections::HashMap<
        String,
        Option<Arc<IndexCommitView<VectorSegmentEntry>>>,
    >,
    buckets: &[(String, Vec<String>)],
    store: &Arc<dyn ObjectStore>,
    table_prefix: &str,
    schema: &SchemaRef,
    primary_keys: &[String],
    cdc_column: &str,
    file_locations: &std::collections::HashMap<String, StorePath>,
    request: &VectorBatchRequest,
) -> DFResult<Vec<Vec<i64>>> {
    // The index stores arrow-Row encoded primary keys; the codec must match
    // the layout the index was built with.
    let layout = KeyLayout::from_schema(schema.as_ref(), primary_keys)
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    let expected_key_kind = layout.kind_hash();
    let pk_schema = pk_only_schema(schema, primary_keys)?;
    let codec = KeyCodec::new(layout.clone())
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;

    let profile = std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok();
    let t_chunk = std::time::Instant::now();

    // Load every shard once for the whole batch and decide, per bucket,
    // whether the coverage proof lets the verification read be skipped: no
    // index mutations, no CDC change column (the table cannot hold delete
    // tombstones) and no active file that the commit does not list.
    let force_verify = std::env::var("LAKESOUL_VECTOR_BATCH_FORCE_VERIFY").is_ok();
    let mut shards: Vec<LoadedShard> = Vec::with_capacity(buckets.len());
    let mut shard_skip: Vec<bool> = Vec::with_capacity(buckets.len());
    let mut shard_files: Vec<Vec<String>> = Vec::with_capacity(buckets.len());
    for (prefix, bucket_files) in buckets {
        let view = views.get(prefix).and_then(Option::as_ref).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "no vector index commit for shard '{prefix}'"
            ))
        })?;
        let covered: Vec<&str> = view
            .segments
            .iter()
            .flat_map(|segment| segment.data_files.iter().map(String::as_str))
            .collect();
        let skip_verification = !force_verify
            && can_skip_verification(
                cdc_column,
                view.mutation_head.is_none(),
                view.mutation_records,
                bucket_files,
                &covered,
            );
        // A hot batch takes the loaded shard straight from the cache; the
        // parsed segment list is only rebuilt when the shard has to load.
        let shard = match cached_typed_shard_for_store(
            store,
            prefix,
            view.commit_id,
            expected_key_kind,
        )
        .await
        {
            Some(shard) => shard,
            None => {
                // The catalog view already holds the parsed segment list;
                // handing it over avoids serializing a thousand entries per
                // bucket and parsing them back on every batch.
                let segments = segment_entries(view.as_ref());
                load_typed_shard(
                    store,
                    prefix,
                    view.commit_id,
                    view.version,
                    &view.header,
                    &segments,
                    view.mutation_head.as_deref(),
                    view.mutation_records,
                    expected_key_kind,
                )
                .await
                .map_err(|error| {
                    DataFusionError::External(error.into_boxed_error())
                })?
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "the vector index at '{prefix}' cannot serve this table's key layout"
                    ))
                })?
            }
        };
        shards.push(shard);
        shard_skip.push(skip_verification);
        shard_files.push(bucket_files.clone());
    }
    let load_ms = t_chunk.elapsed().as_secs_f64() * 1000.0;
    let t_probe = std::time::Instant::now();

    let refs: Vec<&[f32]> = request.queries.iter().map(Vec::as_slice).collect();
    let skip_all = shard_skip.iter().all(|skip| *skip);
    let bucket_ranked: Vec<Vec<Vec<(f32, i64)>>> = if skip_all {
        // Index-only ordering: with the coverage proof every candidate is a
        // live row, so the buckets are independent.  The index call is
        // synchronous CPU work (its own pool parallelises the queries inside a
        // shard), so probing the buckets from one task would serialize them:
        // each bucket gets its own thread.  Splitting a shard's queries into
        // extra ranges was measured and did not help - the call already fills
        // the cores - so one call per shard stays.
        let refs = &refs;
        let shards = &shards;
        let bucket_hits: Vec<Vec<Vec<VectorHit>>> = tokio::task::block_in_place(|| {
            std::thread::scope(|scope| {
                let handles: Vec<_> = shards
                    .iter()
                    .enumerate()
                    .map(|(shard_index, shard)| {
                        scope.spawn(move || -> DFResult<Vec<Vec<VectorHit>>> {
                            let t_search = std::time::Instant::now();
                            let hits = shard
                                .search_batch(
                                    refs,
                                    request.candidates_per_bucket(),
                                    request.nprobe,
                                    request.parallelism,
                                )
                                .map_err(|error| {
                                    DataFusionError::External(error.into_boxed_error())
                                })?;
                            if profile {
                                tracing::debug!(
                                    shard = shard_index,
                                    queries = hits.len(),
                                    search_ms = t_search.elapsed().as_secs_f64() * 1000.0,
                                    "profiled vector batch shard"
                                );
                            }
                            Ok(hits)
                        })
                    })
                    .collect();
                let mut probed = Vec::with_capacity(handles.len());
                for handle in handles {
                    probed.push(handle.join().map_err(|_| {
                        DataFusionError::Execution(
                            "vector batch bucket probe panicked".to_string(),
                        )
                    })??);
                }
                Ok::<_, DataFusionError>(probed)
            })
        })?;
        let probe_ms = t_probe.elapsed().as_secs_f64() * 1000.0;
        let t_rank = std::time::Instant::now();
        if request.exact {
            let file_ranks = bucket_file_ranks(views, buckets);
            let results = exact_rank_and_merge(
                store,
                buckets,
                &bucket_hits,
                file_locations,
                &file_ranks,
                schema,
                primary_keys,
                request,
                &codec,
            )
            .await?;
            if profile {
                tracing::debug!(
                    queries = request.queries.len(),
                    shards = shards.len(),
                    load_ms,
                    probe_ms,
                    rank_merge_ms = t_rank.elapsed().as_secs_f64() * 1000.0,
                    total_ms = t_chunk.elapsed().as_secs_f64() * 1000.0,
                    "profiled vector batch chunk (exact)"
                );
            }
            return Ok(results);
        }
        // Ranking and the cross-bucket top-k are per query, so they fill every
        // core even though the probe stage is limited to the bucket count.
        let results =
            rank_and_merge(&codec, &bucket_hits, request.top_k, request.ascending())?;
        if profile {
            tracing::debug!(
                queries = request.queries.len(),
                shards = shards.len(),
                load_ms,
                probe_ms,
                rank_merge_ms = t_rank.elapsed().as_secs_f64() * 1000.0,
                total_ms = t_chunk.elapsed().as_secs_f64() * 1000.0,
                skipped_verification = true,
                "profiled vector batch chunk"
            );
        }
        return Ok(results);
    } else {
        if request.exact {
            return Err(DataFusionError::NotImplemented(
                "exact batch search needs a proven snapshot coverage: the \
                 index commit must cover every data file, the table must have \
                 no CDC change column and no mutation chain"
                    .to_string(),
            ));
        }
        // Anything unproven falls back to the verification read: one native
        // reader pass per bucket over that bucket's candidate union.
        let mut ranked = Vec::with_capacity(shards.len());
        for (index, shard) in shards.iter().enumerate() {
            let hits = shard
                .search_batch(
                    &refs,
                    request.candidates_per_bucket(),
                    request.nprobe,
                    request.parallelism,
                )
                .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
            let live = if shard_skip[index] {
                None
            } else {
                let mut keys: Vec<IndexKey> = Vec::new();
                let mut seen: HashSet<IndexKey> = HashSet::new();
                for per_query in &hits {
                    for hit in per_query {
                        if seen.insert(hit.key.clone()) {
                            keys.push(hit.key.clone());
                        }
                    }
                }
                Some(
                    read_live_keys(
                        &layout,
                        table_prefix,
                        &shard_files[index],
                        &pk_schema,
                        primary_keys,
                        &codec,
                        &keys,
                    )
                    .await?,
                )
            };
            ranked.push(rank_bucket(
                &codec,
                &hits,
                live.as_ref(),
                request.ascending(),
            )?);
        }
        ranked
    };
    let probe_ms = t_probe.elapsed().as_secs_f64() * 1000.0;
    let t_merge = std::time::Instant::now();

    // Phase 3: merge the buckets' ranked candidates by index distance and
    // take the global top-k per query.
    let queries = request.queries.len();
    let top_k = request.top_k;
    let bucket_count = bucket_ranked.len();
    let merge_threads = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
        .min(queries.max(1));
    let ascending = request.ascending();
    let results: Vec<Vec<i64>> = if merge_threads <= 1 {
        (0..queries)
            .map(|query_index| {
                merge_query(&bucket_ranked, query_index, top_k, bucket_count, ascending)
            })
            .collect()
    } else {
        let per_thread = queries.div_ceil(merge_threads).max(1);
        let mut parts: Vec<(usize, Vec<Vec<i64>>)> = Vec::new();
        std::thread::scope(|scope| {
            let bucket_ranked = &bucket_ranked;
            let handles: Vec<_> = (0..queries)
                .step_by(per_thread)
                .map(|start| {
                    let end = (start + per_thread).min(queries);
                    scope.spawn(move || {
                        let rows = (start..end)
                            .map(|query_index| {
                                merge_query(
                                    bucket_ranked,
                                    query_index,
                                    top_k,
                                    bucket_count,
                                    ascending,
                                )
                            })
                            .collect::<Vec<_>>();
                        (start, rows)
                    })
                })
                .collect();
            for handle in handles {
                parts.push(handle.join().expect("vector batch merge thread"));
            }
        });
        parts.sort_by_key(|(start, _)| *start);
        parts
            .into_iter()
            .flat_map(|(_, rows)| rows)
            .collect::<Vec<_>>()
    };
    if profile {
        tracing::debug!(
            queries,
            buckets = bucket_ranked.len(),
            load_ms,
            probe_ms,
            merge_ms = t_merge.elapsed().as_secs_f64() * 1000.0,
            chunk_ms = t_chunk.elapsed().as_secs_f64() * 1000.0,
            skipped_verification = skip_all,
            "profiled vector batch chunk"
        );
    }
    Ok(results)
}

/// File ranks used by the multi-file exact resolver.
///
/// A data file's rank is the newest index-segment `birth_version` that lists
/// it; a key that occurs in several files is read from the greatest rank
/// (the same rule as the single-query `take_multi_file_ranked` path).
fn bucket_file_ranks(
    views: &HashMap<String, Option<Arc<IndexCommitView<VectorSegmentEntry>>>>,
    buckets: &[(String, Vec<String>)],
) -> HashMap<String, u64> {
    let mut ranks: HashMap<String, u64> = HashMap::new();
    for (prefix, _) in buckets {
        let Some(view) = views.get(prefix).and_then(Option::as_ref) else {
            continue;
        };
        for segment in &view.segments {
            for file in &segment.data_files {
                let rank = ranks.entry(file.clone()).or_insert(0);
                *rank = (*rank).max(segment.birth_version);
            }
        }
    }
    ranks
}

/// Unique candidate keys of one bucket, sorted by their encoded bytes.
fn bucket_candidate_keys(bucket_hits: &[Vec<VectorHit>], limit: usize) -> Vec<IndexKey> {
    let mut seen: HashSet<IndexKey> = HashSet::new();
    for hits in bucket_hits {
        for hit in hits.iter().take(limit) {
            seen.insert(hit.key.clone());
        }
    }
    let mut keys: Vec<IndexKey> = seen.into_iter().collect();
    keys.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    keys
}

#[derive(Clone, Copy, Debug)]
struct ResolvedRow {
    file: usize,
    row: u32,
}

/// Resolve every candidate key to its visible `(file, row)`.
///
/// `files` is sorted by rank descending.  A key missing from a newer file is
/// looked up in older files; a key found in a newer file shadows older copies.
/// Equal ranks that both contain the key are ambiguous and force the caller to
/// fall back to the merge reader.
fn resolve_bucket_keys(
    files: &[(Arc<VectorBlockSource>, u64)],
    keys: &[IndexKey],
) -> Option<HashMap<IndexKey, ResolvedRow>> {
    let order: Vec<&IndexKey> = keys.iter().collect();
    let prefixes: Vec<&[u8]> = order.iter().map(|key| key.as_bytes()).collect();
    let mut resolved: HashMap<IndexKey, (usize, u32, u64)> =
        HashMap::with_capacity(keys.len());
    for (file_index, (source, rank)) in files.iter().enumerate() {
        let matches = source.lookup_matches(&prefixes);
        for (key, matched) in order.iter().zip(matches) {
            match matched {
                PrefixMatch::Missing => {}
                PrefixMatch::Multiple => {
                    // A duplicate in a newer file makes the visible row
                    // ambiguous.  A duplicate in an older file can be ignored
                    // once a newer file has resolved the key.
                    if !resolved.contains_key(*key) {
                        return None;
                    }
                }
                PrefixMatch::Unique(row) => match resolved.get(*key) {
                    None => {
                        resolved.insert((*key).clone(), (file_index, row, *rank));
                    }
                    Some((_, _, seen_rank)) if *seen_rank == *rank => {
                        // Two files of the same rank both own the key.
                        return None;
                    }
                    Some(_) => {
                        // A newer file already owns the visible row.
                    }
                },
            }
        }
    }
    (resolved.len() == keys.len()).then(|| {
        resolved
            .into_iter()
            .map(|(key, (file, row, _))| (key, ResolvedRow { file, row }))
            .collect()
    })
}

/// Score candidate keys from a multi-file bucket.  Rows are grouped by file
/// so each source reads its blocks once; the result is restored to `keys`
/// order for the caller's merge.
fn score_multi_file(
    files: &[(Arc<VectorBlockSource>, u64)],
    resolved: &HashMap<IndexKey, ResolvedRow>,
    keys: &[IndexKey],
    query: &[f32],
    ascending: bool,
) -> Option<Vec<f32>> {
    let mut groups: Vec<Vec<(usize, u32)>> = vec![Vec::new(); files.len()];
    for (position, key) in keys.iter().enumerate() {
        let row = resolved.get(key)?;
        groups[row.file].push((position, row.row));
    }
    let mut scores: Vec<Option<f32>> = vec![None; keys.len()];
    for (file_index, group) in groups.iter().enumerate() {
        if group.is_empty() {
            continue;
        }
        let rows: Vec<u32> = group.iter().map(|(_, row)| *row).collect();
        let values = files[file_index].0.score_rows(&rows, query, ascending)?;
        if values.len() != group.len() {
            return None;
        }
        for ((position, _), value) in group.iter().zip(values) {
            scores[*position] = Some(value);
        }
    }
    scores.into_iter().collect()
}

/// Exact re-rank of a snapshot batch from the on-demand decoded-block cache.
///
/// Each bucket may hold several data files.  Their key indexes are opened
/// independently and the candidate keys are resolved to the newest visible
/// `(file, row)` before scoring; a bucket with one file keeps the original
/// `score_keys` fast path.  Primary keys are decoded only for the candidates
/// that win the top-k, so no key value is materialised for the losers.
#[allow(clippy::too_many_arguments)]
async fn exact_rank_and_merge(
    store: &Arc<dyn ObjectStore>,
    buckets: &[(String, Vec<String>)],
    bucket_hits: &[Vec<Vec<VectorHit>>],
    file_locations: &HashMap<String, StorePath>,
    file_ranks: &HashMap<String, u64>,
    schema: &SchemaRef,
    primary_keys: &[String],
    request: &VectorBatchRequest,
    codec: &KeyCodec,
) -> DFResult<Vec<Vec<i64>>> {
    let queries = request.queries.len();
    if queries == 0 {
        return Ok(Vec::new());
    }
    if primary_keys.len() != 1 || primary_keys[0] != "id" {
        return Err(DataFusionError::NotImplemented(format!(
            "exact batch search currently supports a single Int64 primary key \
             named 'id', got {primary_keys:?}"
        )));
    }
    let column = &request.column;
    let vector_field: FieldRef = Arc::new(
        schema
            .field_with_name(column)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "vector column '{column}' is missing from the table schema: {error}"
                ))
            })?
            .clone(),
    );
    let key_types = vec![
        schema
            .field_with_name(&primary_keys[0])
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "primary key is missing from the table schema: {error}"
                ))
            })?
            .data_type()
            .clone(),
    ];
    // Open every bucket file: the multi-file resolver needs each file's key
    // index, and the block source is where its stored vectors come from.
    let mut counts = Vec::with_capacity(buckets.len());
    let mut opens = Vec::new();
    for (_, bucket_files) in buckets {
        if bucket_files.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "exact batch search found an empty hash bucket".to_string(),
            ));
        }
        counts.push(bucket_files.len());
        for file_name in bucket_files {
            let location = file_locations.get(file_name).cloned().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "no object-store location for data file {file_name}"
                ))
            })?;
            let format =
                PhysicalFormat::from_extension(location.as_ref()).map_err(|error| {
                    DataFusionError::NotImplemented(format!(
                        "exact batch search does not support {location}: {error}"
                    ))
                })?;
            let store = Arc::clone(store);
            let field = Arc::clone(&vector_field);
            let key_columns = primary_keys.to_vec();
            let key_types = key_types.clone();
            opens.push(async move {
                VectorBlockSource::open_cached(
                    store,
                    &location,
                    format,
                    &key_columns,
                    &key_types,
                    &field,
                )
                .await
            });
        }
    }
    let opened = futures::future::join_all(opens).await;
    let mut sources: Vec<Vec<(Arc<VectorBlockSource>, u64)>> =
        Vec::with_capacity(buckets.len());
    let mut cursor = 0usize;
    for (bucket_index, count) in counts.into_iter().enumerate() {
        let mut files = Vec::with_capacity(count);
        for offset in 0..count {
            let source = opened[cursor].as_ref().ok_or_else(|| {
                DataFusionError::NotImplemented(format!(
                    "exact batch search could not open data file {}",
                    buckets[bucket_index].1[offset]
                ))
            })?;
            let rank = if count > 1 {
                *file_ranks
                    .get(&buckets[bucket_index].1[offset])
                    .ok_or_else(|| {
                        DataFusionError::NotImplemented(format!(
                            "exact batch search has no index rank for {}",
                            buckets[bucket_index].1[offset]
                        ))
                    })?
            } else {
                0
            };
            files.push((Arc::clone(source), rank));
            cursor += 1;
        }
        // Newest first.  Equal ranks are allowed only when they own disjoint
        // keys; resolve_bucket_keys() rejects overlap.
        if files.len() > 1 {
            files.sort_by(|left, right| right.1.cmp(&left.1));
        }
        sources.push(files);
    }

    // Resolve every unique candidate of each multi-file bucket once.  The
    // scoring workers below then only look rows up in this map.
    let candidates = request.candidates_per_bucket();
    let mut resolved: Vec<Option<HashMap<IndexKey, ResolvedRow>>> =
        Vec::with_capacity(sources.len());
    for (bucket, files) in sources.iter().enumerate() {
        if files.len() <= 1 {
            resolved.push(None);
            continue;
        }
        let keys = bucket_candidate_keys(&bucket_hits[bucket], candidates);
        let map = resolve_bucket_keys(files, &keys).ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "exact batch search cannot resolve the visible row for every \
                 candidate in hash bucket {} (multiple files or equal index ranks)",
                buckets[bucket].0
            ))
        })?;
        resolved.push(Some(map));
    }

    let profile = std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok();
    let t_fetch = std::time::Instant::now();
    // Candidate rows are read on scoped worker threads: every read is a pread
    // per candidate, and both the buckets and the query ranges inside a bucket
    // are independent.  One task per core is the measured optimum - enough to
    // balance the pread latency, few enough that each task builds the next
    // query's candidate keys on the fly instead of holding a whole chunk of
    // them.  `LAKESOUL_VECTOR_EXACT_CHUNK` overrides the task size for tuning.
    let cores = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1);
    let chunk = std::env::var("LAKESOUL_VECTOR_EXACT_CHUNK")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            (queries.saturating_mul(bucket_hits.len()) / cores.max(1)).clamp(32, 320)
        })
        .min(queries.max(1));
    let mut descriptors: Vec<(usize, usize, usize)> = Vec::new();
    for bucket in 0..bucket_hits.len() {
        let mut start = 0;
        while start < queries {
            let end = (start + chunk).min(queries);
            descriptors.push((bucket, start, end));
            start = end;
        }
    }
    let workers = cores.min(descriptors.len()).max(1);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let collected = tokio::task::block_in_place(|| {
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers);
            for _ in 0..workers {
                let sources = &sources;
                let resolved = &resolved;
                let descriptors = &descriptors;
                let bucket_hits = bucket_hits;
                let query_vectors = &request.queries;
                let next = &next;
                handles.push(scope.spawn(
                    move || -> DFResult<Vec<(usize, Vec<(f32, IndexKey)>)>> {
                        let mut rows = Vec::new();
                        loop {
                            let index =
                                next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if index >= descriptors.len() {
                                break;
                            }
                            let (bucket, start, end) = descriptors[index];
                            let files = &sources[bucket];
                            if files.is_empty() {
                                return Err(DataFusionError::Execution(
                                    "exact batch search lost a bucket source"
                                        .to_string(),
                                ));
                            }
                            for query in start..end {
                                let keys: Vec<IndexKey> = bucket_hits[bucket][query]
                                    .iter()
                                    .take(candidates)
                                    .map(|hit| hit.key.clone())
                                    .collect();
                                let scores = if files.len() == 1 {
                                    files[0].0.score_keys(
                                        &keys,
                                        &query_vectors[query],
                                        request.ascending(),
                                    )
                                } else {
                                    let Some(map) = resolved[bucket].as_ref() else {
                                        return Err(DataFusionError::Execution(
                                            "exact batch search lost a multi-file resolver"
                                                .to_string(),
                                        ));
                                    };
                                    score_multi_file(
                                        files,
                                        map,
                                        &keys,
                                        &query_vectors[query],
                                        request.ascending(),
                                    )
                                };
                                let Some(scores) = scores else {
                                    return Err(DataFusionError::Execution(
                                        "stored-vector block read failed"
                                            .to_string(),
                                    ));
                                };
                                rows.push((
                                    query,
                                    keys.into_iter()
                                        .zip(scores)
                                        .map(|(key, score)| (score, key))
                                        .collect(),
                                ));
                            }
                        }
                        Ok(rows)
                    },
                ));
            }
            let mut collected: Vec<(usize, Vec<(f32, IndexKey)>)> =
                Vec::with_capacity(queries);
            for handle in handles {
                collected.extend(handle.join().map_err(|_| {
                    DataFusionError::Execution(
                        "exact batch fetch thread panicked".to_string(),
                    )
                })??);
            }
            Ok::<_, DataFusionError>(collected)
        })
    })?;
    let mut per_query: Vec<Vec<(f32, IndexKey)>> = vec![Vec::new(); queries];
    for (query, scored) in collected {
        per_query[query].extend(scored);
    }
    let fetch_ms = t_fetch.elapsed().as_secs_f64() * 1000.0;
    let t_merge = std::time::Instant::now();
    // Cross-bucket top-k per query, then decode only the winning keys.  The
    // per-query work is independent, so it fills the cores of a large batch.
    let merge_one = |scored: &mut Vec<(f32, IndexKey)>| -> DFResult<Vec<i64>> {
        // `score_keys` returns a squared distance for L2 and the negated dot
        // product for IP, so ascending order is "best first" for both.
        scored.sort_by(|left, right| left.0.total_cmp(&right.0));
        if scored.len() > request.top_k {
            scored.truncate(request.top_k);
        }
        let keys: Vec<&IndexKey> = scored.iter().map(|(_, key)| key).collect();
        decode_ids_refs(codec, &keys)
    };
    let threads = std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
        .min(queries)
        .min(8);
    let mut results: Vec<Vec<i64>> = vec![Vec::new(); queries];
    if threads <= 1 {
        for (query, scored) in per_query.iter_mut().enumerate() {
            results[query] = merge_one(scored)?;
        }
    } else {
        let per_thread = queries.div_ceil(threads).max(1);
        let mut parts: Vec<(usize, Vec<DFResult<Vec<i64>>>)> =
            Vec::with_capacity(threads);
        std::thread::scope(|scope| {
            let handles: Vec<_> = per_query
                .chunks_mut(per_thread)
                .enumerate()
                .map(|(chunk_index, chunk)| {
                    let merge_one = &merge_one;
                    scope.spawn(move || {
                        let rows: Vec<DFResult<Vec<i64>>> =
                            chunk.iter_mut().map(merge_one).collect();
                        (chunk_index, rows)
                    })
                })
                .collect();
            for handle in handles {
                let (chunk_index, rows) = handle.join().map_err(|_| {
                    DataFusionError::Execution(
                        "vector batch merge thread panicked".to_string(),
                    )
                })?;
                parts.push((chunk_index, rows));
            }
            Ok::<_, DataFusionError>(())
        })?;
        parts.sort_by_key(|(chunk_index, _)| *chunk_index);
        let mut position = 0;
        for (_, rows) in parts {
            for row in rows {
                results[position] = row?;
                position += 1;
            }
        }
    }
    if profile {
        tracing::debug!(
            queries,
            buckets = buckets.len(),
            candidates,
            fetch_ms,
            merge_ms = t_merge.elapsed().as_secs_f64() * 1000.0,
            "profiled exact batch rank"
        );
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

/// The typed segment entries of a vector index view.
pub(crate) fn segment_entries(
    view: &IndexCommitView<VectorSegmentEntry>,
) -> Vec<SegmentEntry> {
    view.segments
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
        .collect()
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
/// Coverage proof for a path that **reads its rows again**.
///
/// Skipping verification additionally needs "nothing can hide a row" (no CDC
/// column, no mutation chain).  A path that re-reads every candidate row only
/// needs the index to know about every data file: the loaded shard already
/// masks the mutation chain, so mutated keys never become candidates, and a
/// CDC tombstone is dropped by the merge-on-read read plus the change
/// predicate.
pub(crate) fn candidate_files_covered(
    active_files: &[String],
    covered_files: &[&str],
) -> bool {
    !covered_files.is_empty()
        && lakesoul_common::index::pending_files(active_files, covered_files).is_empty()
}

pub(crate) fn can_skip_verification(
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

/// Decode a batch of index keys into their Int64 primary keys.
fn decode_ids_refs(codec: &KeyCodec, keys: &[&IndexKey]) -> DFResult<Vec<i64>> {
    let decoded = codec
        .decode_refs(keys)
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    let Some(array) = decoded.first() else {
        return Err(DataFusionError::Execution(
            "the index key codec decoded no primary-key column".to_string(),
        ));
    };
    let Some(array) = array.as_any().downcast_ref::<arrow::array::Int64Array>() else {
        return Err(DataFusionError::Execution(
            "the index key codec decoded a non-Int64 primary key".to_string(),
        ));
    };
    Ok(array.iter().map(|id| id.unwrap_or_default()).collect())
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
