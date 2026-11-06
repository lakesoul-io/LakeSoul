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
use lakesoul_io::index::key::{KeyCodec, KeyLayout};
use lakesoul_io::index::prefix::derive_index_prefixes;
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_io::vector::search::{
    LoadedShard, VectorHit, cached_typed_shard, load_typed_shard,
};
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
/// `(index prefix, files)` of every hash bucket of one snapshot.
/// Rank one query's candidates across the buckets and keep its top-k.
fn merge_query(
    bucket_ranked: &[Vec<Vec<(f32, i64)>>],
    query_index: usize,
    top_k: usize,
    bucket_count: usize,
) -> Vec<i64> {
    let mut ranked: Vec<(f32, i64)> = Vec::with_capacity(bucket_count * top_k);
    for per_bucket in bucket_ranked {
        ranked.extend(per_bucket[query_index].iter().copied());
    }
    // L2 index distance: smaller is closer, the same ordering the
    // single-query path uses with `lakesoul_vector_search.rerank=index`.
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
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
) -> DFResult<Vec<(f32, i64)>> {
    let mut best: BTreeMap<&IndexKey, f32> = BTreeMap::new();
    for hit in per_query_hits {
        if live.is_some_and(|live| !live.contains(&hit.key)) {
            continue;
        }
        best.entry(&hit.key)
            .and_modify(|score| *score = score.min(hit.score))
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
) -> DFResult<Vec<Vec<(f32, i64)>>> {
    hits.iter()
        .map(|per_query_hits| rank_query(codec, per_query_hits, live))
        .collect()
}

/// Rank one query's candidates across the buckets and keep its top-k.
fn merge_one_query(
    codec: &KeyCodec,
    bucket_hits: &[Vec<Vec<VectorHit>>],
    query_index: usize,
    top_k: usize,
) -> DFResult<Vec<i64>> {
    let mut ranked: Vec<(f32, i64)> = Vec::with_capacity(bucket_hits.len() * top_k);
    for hits in bucket_hits {
        ranked.extend(rank_query(codec, &hits[query_index], None)?);
    }
    // L2 index distance: smaller is closer, the same ordering the single-query
    // path uses with `lakesoul_vector_search.rerank=index`.
    ranked.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
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
            .map(|query_index| merge_one_query(codec, bucket_hits, query_index, top_k))
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
                            merge_one_query(codec, bucket_hits, query_index, top_k)?,
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
        request.queries.len().max(1)
    } else {
        DEFAULT_BATCH_SIZE
    }
}

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
    // Resolve every bucket's shard once for the whole call: the batch-size
    // decision and every chunk read the same views.  Resolving per chunk used
    // to cost one catalog round trip per chunk.
    let buckets = bucket_files_by_prefix(table_prefix, files, &request.column);
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
        // The chunks are independent, and one chunk's index probes (rayon)
        // and reader/merge work do not fill every core on their own, so run
        // them concurrently and concatenate the results in query order.
        let mut chunk_futures = Vec::new();
        let views = &views;
        let buckets = &buckets;
        for chunk in request.queries.chunks(batch_size) {
            let mut chunk_request = request.clone();
            chunk_request.queries = chunk.to_vec();
            chunk_futures.push(async move {
                search_vector_batch_chunk(
                    views,
                    buckets,
                    store,
                    table_prefix,
                    schema,
                    primary_keys,
                    cdc_column,
                    &chunk_request,
                )
                .await
            });
        }
        let mut results = Vec::with_capacity(request.queries.len());
        for chunk_results in futures::future::try_join_all(chunk_futures).await? {
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
        let shard = match cached_typed_shard(prefix, view.commit_id, expected_key_kind)
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
        // synchronous CPU work (rayon inside), so probing the buckets from
        // one task would serialize them: each bucket gets its own thread.
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
                                    request.top_k,
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
        // Ranking and the cross-bucket top-k are per query, so they fill every
        // core even though the probe stage is limited to the bucket count.
        let results = rank_and_merge(&codec, &bucket_hits, request.top_k)?;
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
        // Anything unproven falls back to the verification read: one native
        // reader pass per bucket over that bucket's candidate union.
        let mut ranked = Vec::with_capacity(shards.len());
        for (index, shard) in shards.iter().enumerate() {
            let hits = shard
                .search_batch(&refs, request.top_k, request.nprobe, request.parallelism)
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
            ranked.push(rank_bucket(&codec, &hits, live.as_ref())?);
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
    let results: Vec<Vec<i64>> = if merge_threads <= 1 {
        (0..queries)
            .map(|query_index| {
                merge_query(&bucket_ranked, query_index, top_k, bucket_count)
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
