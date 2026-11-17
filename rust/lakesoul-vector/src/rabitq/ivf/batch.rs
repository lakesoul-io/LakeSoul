//! Bounded, block-major IVF batch search.
//!
//! A query advances only when its next selected cluster is scanned. Queries
//! ready for the same cluster share the payload and scan each 32-vector block
//! before moving to the next block. This preserves the single-query probe and
//! heap-pruning order while reusing the hot block across several query LUTs.

#[cfg(test)]
#[path = "batch_tests.rs"]
pub(super) mod tests;

use std::collections::{BTreeMap, BinaryHeap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};

use rayon::prelude::*;

use super::cluster::ClusterData;
use super::lazy::LoadedCluster;
use super::lut::{HeapCandidate, HeapEntry, QueryPrecomputed};
use super::{InvalidMask, IvfRabitqIndex, SearchParams, SearchResult, push_top_k};
use crate::rabitq::fastscan_kernel::{FastScanLutView, compute_fastscan_batch};
use crate::rabitq::math::{dot, l2_distance_sqr};
use crate::rabitq::{Metric, RabitqError, simd};

/// Controls the memory and CPU budget of a single batch request.
#[derive(Debug, Clone)]
pub struct BatchSearchOptions {
    /// Queries prepared together; clamped to 1..=256 to bound LUT/heap memory.
    pub query_tile_size: usize,
    /// Concurrent tile workers, bounded by the process-wide batch pool.
    pub parallelism: usize,
    /// Checked before preparation, cluster loads, and every FastScan block.
    pub cancellation: Option<Arc<AtomicBool>>,
}

impl Default for BatchSearchOptions {
    fn default() -> Self {
        Self {
            query_tile_size: 32,
            parallelism: default_batch_parallelism(),
            cancellation: None,
        }
    }
}

/// Counters distinguish physical block visits from query/block evaluations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchSearchDiagnostics {
    pub queries: usize,
    pub query_tiles: usize,
    pub workers: usize,
    pub peak_tile_queries: usize,
    /// Ready-cluster groups visited (one shared payload view per group).
    pub cluster_visits: usize,
    pub query_cluster_visits: usize,
    /// Packed blocks whose slices were obtained once for a ready-query group.
    pub block_reads: usize,
    pub query_block_evaluations: usize,
    /// Additional query evaluations that reused a block already being scanned.
    pub shared_block_reuses: usize,
    pub pending_vector_reads: usize,
    pub refinements: usize,
    pub candidates: usize,
    pub lazy_cluster_loads: usize,
    pub cluster_cache_hits: usize,
    pub singleflight_waits: usize,
    pub cancelled_queries: usize,
}

impl BatchSearchDiagnostics {
    fn merge(&mut self, other: Self) {
        self.queries += other.queries;
        self.query_tiles += other.query_tiles;
        self.peak_tile_queries = self.peak_tile_queries.max(other.peak_tile_queries);
        self.cluster_visits += other.cluster_visits;
        self.query_cluster_visits += other.query_cluster_visits;
        self.block_reads += other.block_reads;
        self.query_block_evaluations += other.query_block_evaluations;
        self.shared_block_reuses += other.shared_block_reuses;
        self.pending_vector_reads += other.pending_vector_reads;
        self.refinements += other.refinements;
        self.candidates += other.candidates;
        self.lazy_cluster_loads += other.lazy_cluster_loads;
        self.cluster_cache_hits += other.cluster_cache_hits;
        self.singleflight_waits += other.singleflight_waits;
        self.cancelled_queries += other.cancelled_queries;
    }
}

#[derive(Debug)]
pub struct BatchSearchOutput {
    /// One result per input query, in input order, including validation errors.
    pub results: Vec<Result<Vec<SearchResult>, RabitqError>>,
    pub diagnostics: BatchSearchDiagnostics,
}

/// A single shared pool prevents nested Rayon query/cluster thread explosions.
/// The cap also bounds simultaneous lazy payload reads across requests.
static BATCH_POOL: LazyLock<Option<rayon::ThreadPool>> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(default_batch_parallelism())
        .thread_name(|index| format!("ivf-batch-{index}"))
        .build()
        .ok()
});

pub(super) fn default_batch_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(16)
}

/// Map independent CPU jobs on the shared bounded IVF pool, preserving order.
/// Each request submits at most `parallelism` workers; each worker processes a
/// sequential job slice and never starts nested cluster-parallel work.
pub fn map_batch_jobs<T, R, F>(jobs: Vec<T>, parallelism: usize, work: F) -> Vec<R>
where
    T: Send,
    R: Send,
    F: Fn(T) -> R + Send + Sync,
{
    if jobs.is_empty() {
        return Vec::new();
    }
    let workers = parallelism
        .max(1)
        .min(default_batch_parallelism())
        .min(jobs.len());
    let mut groups: Vec<Vec<(usize, T)>> = (0..workers).map(|_| Vec::new()).collect();
    for (position, job) in jobs.into_iter().enumerate() {
        groups[position % workers].push((position, job));
    }
    let run = |group: Vec<(usize, T)>| {
        group
            .into_iter()
            .map(|(position, job)| (position, work(job)))
            .collect::<Vec<_>>()
    };
    let mut results = if let Some(pool) = BATCH_POOL.as_ref() {
        pool.install(|| {
            groups
                .into_par_iter()
                .flat_map_iter(run)
                .collect::<Vec<_>>()
        })
    } else {
        groups.into_iter().flat_map(run).collect()
    };
    results.sort_unstable_by_key(|(position, _)| *position);
    results.into_iter().map(|(_, result)| result).collect()
}

pub(super) fn is_cancelled(cancel: Option<&AtomicBool>) -> bool {
    cancel.is_some_and(|flag| flag.load(Ordering::Relaxed))
}

pub(super) fn cancelled_error() -> RabitqError {
    RabitqError::InvalidConfig("vector batch search cancelled")
}

pub(super) enum ClusterView<'a> {
    Borrowed(&'a ClusterData, &'a InvalidMask),
    Shared(Arc<LoadedCluster>),
}

impl ClusterView<'_> {
    fn data(&self) -> &ClusterData {
        match self {
            Self::Borrowed(data, _) => data,
            Self::Shared(loaded) => &loaded.data,
        }
    }

    fn invalid_mask(&self) -> &InvalidMask {
        match self {
            Self::Borrowed(_, mask) => mask,
            Self::Shared(loaded) => &loaded.invalid_mask,
        }
    }
}

pub(super) trait BatchClusterSource: Sync {
    fn selector(&self) -> &IvfRabitqIndex;
    fn has_vectors(&self) -> bool;
    fn cluster(
        &self,
        position: usize,
        cancel: Option<&AtomicBool>,
        diagnostics: &mut BatchSearchDiagnostics,
    ) -> Result<ClusterView<'_>, RabitqError>;
}

impl BatchClusterSource for IvfRabitqIndex {
    fn selector(&self) -> &IvfRabitqIndex {
        self
    }

    fn has_vectors(&self) -> bool {
        !self.is_empty()
    }

    fn cluster(
        &self,
        position: usize,
        _cancel: Option<&AtomicBool>,
        _diagnostics: &mut BatchSearchDiagnostics,
    ) -> Result<ClusterView<'_>, RabitqError> {
        Ok(ClusterView::Borrowed(
            &self.clusters[position],
            &self.invalid_masks[position],
        ))
    }
}

struct PreparedQuery {
    precomputed: QueryPrecomputed,
    selected: Vec<usize>,
    next_cluster: usize,
    heap: BinaryHeap<HeapEntry>,
}

struct QuerySlot {
    prepared: Option<PreparedQuery>,
    error: Option<RabitqError>,
}

impl QuerySlot {
    fn fail(&mut self, error: RabitqError) {
        self.prepared = None;
        self.error = Some(error);
    }
}

impl IvfRabitqIndex {
    /// A cancellation-aware batch entry point with an explicit request budget.
    /// CPU parallelism applies across query tiles, never inside their clusters.
    pub fn batch_search_controlled(
        &self,
        queries: &[&[f32]],
        params: SearchParams,
        cancel: Arc<AtomicBool>,
    ) -> Vec<Result<Vec<SearchResult>, RabitqError>> {
        self.batch_search_with_options(
            queries,
            params,
            &BatchSearchOptions {
                parallelism: params.parallelism,
                cancellation: Some(cancel),
                ..Default::default()
            },
        )
        .results
    }

    /// Block-major batch search with diagnostics and bounded query preparation.
    pub fn batch_search_with_options(
        &self,
        queries: &[&[f32]],
        params: SearchParams,
        options: &BatchSearchOptions,
    ) -> BatchSearchOutput {
        search_source_batch(self, queries, params, options)
    }

    /// Select probes from an already rotated query, avoiding a second rotation.
    fn select_clusters_rotated(&self, rotated: &[f32], nprobe: usize) -> Vec<usize> {
        let mut scores: Vec<_> = self
            .clusters
            .iter()
            .enumerate()
            .map(|(id, cluster)| super::ClusterScore {
                id,
                score: match self.metric {
                    Metric::L2 => l2_distance_sqr(rotated, &cluster.centroid),
                    Metric::InnerProduct => dot(rotated, &cluster.centroid),
                },
            })
            .collect();
        let nprobe = nprobe.max(1).min(scores.len());
        let compare = match self.metric {
            Metric::L2 => super::ClusterScore::l2_order,
            Metric::InnerProduct => super::ClusterScore::ip_order,
        };
        if nprobe < scores.len() {
            scores.select_nth_unstable_by(nprobe, compare);
            scores.truncate(nprobe);
        }
        scores.sort_unstable_by(compare);
        scores.into_iter().map(super::ClusterScore::id).collect()
    }
}

pub(super) fn search_source_batch<S: BatchClusterSource>(
    source: &S,
    queries: &[&[f32]],
    params: SearchParams,
    options: &BatchSearchOptions,
) -> BatchSearchOutput {
    if queries.is_empty() {
        return BatchSearchOutput {
            results: Vec::new(),
            diagnostics: BatchSearchDiagnostics::default(),
        };
    }
    let tile_size = options.query_tile_size.clamp(1, 256);
    let tile_count = queries.len().div_ceil(tile_size);
    let workers = options
        .parallelism
        .max(1)
        .min(default_batch_parallelism())
        .min(tile_count);
    let cancel = options.cancellation.as_deref();
    let work = |worker: usize| {
        (worker..tile_count)
            .step_by(workers)
            .map(|tile| {
                let start = tile * tile_size;
                let end = (start + tile_size).min(queries.len());
                (
                    tile,
                    search_tile(source, &queries[start..end], params, cancel),
                )
            })
            .collect::<Vec<_>>()
    };
    let actual_workers = if BATCH_POOL.is_some() { workers } else { 1 };
    let mut tiles = map_batch_jobs((0..workers).collect(), workers, work)
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    tiles.sort_unstable_by_key(|(tile, _)| *tile);
    let mut results = Vec::with_capacity(queries.len());
    let mut diagnostics = BatchSearchDiagnostics {
        workers: actual_workers,
        ..Default::default()
    };
    for (_, output) in tiles {
        results.extend(output.results);
        diagnostics.merge(output.diagnostics);
    }
    BatchSearchOutput {
        results,
        diagnostics,
    }
}

fn search_tile<S: BatchClusterSource>(
    source: &S,
    queries: &[&[f32]],
    params: SearchParams,
    cancel: Option<&AtomicBool>,
) -> BatchSearchOutput {
    let selector = source.selector();
    let mut diagnostics = BatchSearchDiagnostics {
        queries: queries.len(),
        query_tiles: 1,
        peak_tile_queries: queries.len(),
        ..Default::default()
    };
    let mut slots: Vec<_> = queries
        .iter()
        .map(|query| {
            let result = if is_cancelled(cancel) {
                diagnostics.cancelled_queries += 1;
                Err(cancelled_error())
            } else if !source.has_vectors() {
                Err(RabitqError::EmptyIndex)
            } else if query.len() != selector.dim {
                Err(RabitqError::DimensionMismatch {
                    expected: selector.dim,
                    got: query.len(),
                })
            } else {
                let rotated = selector.rotator.rotate(query);
                let selected = if params.top_k == 0 {
                    Vec::new()
                } else {
                    selector.select_clusters_rotated(&rotated, params.nprobe)
                };
                let mut precomputed = QueryPrecomputed::new(rotated, selector.ex_bits);
                if params.top_k != 0 {
                    precomputed.build_lut(selector.padded_dim);
                }
                Ok(PreparedQuery {
                    precomputed,
                    selected,
                    next_cluster: 0,
                    heap: BinaryHeap::new(),
                })
            };
            match result {
                Ok(prepared) => QuerySlot {
                    prepared: Some(prepared),
                    error: None,
                },
                Err(error) => QuerySlot {
                    prepared: None,
                    error: Some(error),
                },
            }
        })
        .collect();

    loop {
        if is_cancelled(cancel) {
            for slot in &mut slots {
                if slot.prepared.is_some() {
                    slot.fail(cancelled_error());
                    diagnostics.cancelled_queries += 1;
                }
            }
            break;
        }
        // Only the next cluster is ready for each query. Grouping all selected
        // clusters here would change pruning and equal-distance tie behavior.
        let mut ready = BTreeMap::<usize, Vec<usize>>::new();
        for (index, slot) in slots.iter().enumerate() {
            if let Some(query) = &slot.prepared
                && let Some(&cluster) = query.selected.get(query.next_cluster)
            {
                ready.entry(cluster).or_default().push(index);
            }
        }
        let Some((position, query_ids)) = ready
            .into_iter()
            .max_by(|a, b| a.1.len().cmp(&b.1.len()).then_with(|| b.0.cmp(&a.0)))
        else {
            break;
        };
        diagnostics.cluster_visits += 1;
        let cluster = match source.cluster(position, cancel, &mut diagnostics) {
            Ok(cluster) => cluster,
            Err(error) => {
                if is_cancelled(cancel) {
                    continue; // Uniform cancellation is applied at the loop head.
                }
                // RabitqError can contain a non-cloneable object-store error.
                // Preserve the original error for one query and its message
                // for the other queries served by the same failing load.
                let message = error.to_string();
                let mut original = Some(error);
                for &index in &query_ids {
                    slots[index].fail(
                        original
                            .take()
                            .unwrap_or_else(|| RabitqError::Io(message.clone())),
                    );
                }
                continue;
            }
        };
        if !scan_cluster_block_major(
            selector,
            cluster.data(),
            cluster.invalid_mask(),
            &query_ids,
            &mut slots,
            params.top_k,
            cancel,
            &mut diagnostics,
        ) {
            continue; // The loop above marks cancelled query slots uniformly.
        }
        diagnostics.query_cluster_visits += query_ids.len();
        for &index in &query_ids {
            slots[index].prepared.as_mut().unwrap().next_cluster += 1;
        }
    }

    let results = slots
        .into_iter()
        .map(|slot| {
            if let Some(error) = slot.error {
                return Err(error);
            }
            let query = slot.prepared.unwrap();
            let mut candidates: Vec<_> = query
                .heap
                .into_sorted_vec()
                .into_iter()
                .map(|entry| entry.candidate)
                .collect();
            match selector.metric {
                Metric::L2 => {
                    candidates.sort_by(|a, b| a.distance.total_cmp(&b.distance))
                }
                Metric::InnerProduct => {
                    candidates.sort_by(|a, b| b.score.total_cmp(&a.score))
                }
            }
            Ok(candidates
                .into_iter()
                .map(|candidate| SearchResult {
                    id: candidate.id,
                    version: candidate.version,
                    birth_version: candidate.birth_version,
                    score: match selector.metric {
                        Metric::L2 => candidate.distance,
                        Metric::InnerProduct => candidate.score,
                    },
                })
                .collect())
        })
        .collect();
    BatchSearchOutput {
        results,
        diagnostics,
    }
}

struct ProbeConstants {
    query: usize,
    g_add: f32,
    g_error: f32,
    dot_centroid: f32,
}

fn lut_view(query: &QueryPrecomputed) -> FastScanLutView<'_> {
    if let Some(lut) = &query.lut_highacc {
        FastScanLutView::HighAcc {
            lut_low8: &lut.lut_low8,
            lut_high8: &lut.lut_high8,
            delta: lut.delta,
            sum_vl_lut: lut.sum_vl_lut,
        }
    } else {
        let lut = query.lut.as_ref().expect("batch query LUT was prepared");
        FastScanLutView::Regular {
            lut_i8: &lut.lut_i8,
            delta: lut.delta,
            sum_vl_lut: lut.sum_vl_lut,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_cluster_block_major(
    index: &IvfRabitqIndex,
    cluster: &ClusterData,
    invalid: &InvalidMask,
    query_ids: &[usize],
    slots: &mut [QuerySlot],
    top_k: usize,
    cancel: Option<&AtomicBool>,
    diagnostics: &mut BatchSearchDiagnostics,
) -> bool {
    let probes: Vec<_> = query_ids
        .iter()
        .map(|&query| {
            let rotated = &slots[query]
                .prepared
                .as_ref()
                .unwrap()
                .precomputed
                .rotated_query;
            let centroid_distance = l2_distance_sqr(rotated, &cluster.centroid);
            let dot_centroid = dot(rotated, &cluster.centroid);
            ProbeConstants {
                query,
                g_add: match index.metric {
                    Metric::L2 => centroid_distance,
                    Metric::InnerProduct => -dot_centroid,
                },
                g_error: centroid_distance.sqrt(),
                dot_centroid,
            }
        })
        .collect();
    for batch_idx in 0..cluster.num_vectors.div_ceil(simd::FASTSCAN_BATCH_SIZE) {
        if is_cancelled(cancel) {
            return false;
        }
        let start = batch_idx * simd::FASTSCAN_BATCH_SIZE;
        let actual = (cluster.num_vectors - start).min(simd::FASTSCAN_BATCH_SIZE);
        let live = u32::MAX >> (simd::FASTSCAN_BATCH_SIZE - actual);
        let valid = live & !invalid.lane_mask(batch_idx);
        if valid == 0 {
            continue;
        }
        // These zero-copy views are obtained once for all ready queries. Keep
        // the block outermost so code/parameter cache lines stay hot across LUTs.
        let codes = cluster.batch_bin_codes(batch_idx);
        let f_add = cluster.batch_f_add(batch_idx);
        let f_rescale = cluster.batch_f_rescale(batch_idx);
        let f_error = cluster.batch_f_error(batch_idx);
        diagnostics.block_reads += 1;
        diagnostics.shared_block_reuses += probes.len().saturating_sub(1);
        for probe in &probes {
            if is_cancelled(cancel) {
                return false;
            }
            let query = slots[probe.query].prepared.as_mut().unwrap();
            let precomputed = &query.precomputed;
            let mut ip = [0.0; simd::FASTSCAN_BATCH_SIZE];
            let mut estimates = [0.0; simd::FASTSCAN_BATCH_SIZE];
            let mut bounds = [0.0; simd::FASTSCAN_BATCH_SIZE];
            compute_fastscan_batch(
                lut_view(precomputed),
                codes,
                index.padded_dim,
                f_add,
                f_rescale,
                f_error,
                probe.g_add,
                probe.g_error,
                precomputed.k1x_sum_q,
                &mut ip,
                &mut estimates,
                &mut bounds,
            );
            diagnostics.query_block_evaluations += 1;
            let threshold = if query.heap.len() < top_k {
                f32::INFINITY
            } else {
                query.heap.peek().unwrap().candidate.distance
            };
            let fallback = -(probe.dot_centroid + precomputed.query_norm);
            let mut mask = simd::sanitized_lower_bound_mask_32_for_dim(
                &bounds,
                threshold,
                index.metric,
                fallback,
                index.padded_dim,
            ) & valid;
            while mask != 0 {
                let lane = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                let position = start + lane;
                let mut distance = estimates[lane];
                if index.ex_bits > 0 {
                    diagnostics.refinements += 1;
                    distance = crate::rabitq::fastscan_kernel::refine_distance_with_ex(
                        &precomputed.rotated_query,
                        &cluster.ex_codes_packed[position],
                        index.padded_dim,
                        index.ex_bits,
                        ip[lane],
                        precomputed.binary_scale,
                        precomputed.kbx_sum_q,
                        probe.g_add,
                        cluster.f_add_ex[position],
                        cluster.f_rescale_ex[position],
                        Some(index.ip_func),
                    );
                }
                if !distance.is_finite() {
                    continue;
                }
                if index.metric == Metric::L2 {
                    distance = distance.max(0.0);
                }
                diagnostics.candidates += 1;
                push_top_k(
                    &mut query.heap,
                    top_k,
                    HeapCandidate {
                        id: cluster.ids[position].clone(),
                        version: cluster.ids_version[position],
                        birth_version: cluster.ids_birth[position],
                        distance,
                        score: match index.metric {
                            Metric::L2 => distance,
                            Metric::InnerProduct => -distance,
                        },
                    },
                );
            }
        }
    }
    // Unpack pending binary codes once per vector, rather than once per query.
    for (id, vector) in cluster.pending_ids.iter().zip(&cluster.pending_vectors) {
        if is_cancelled(cancel) {
            return false;
        }
        let binary = vector.unpack_binary_code();
        diagnostics.pending_vector_reads += 1;
        for probe in &probes {
            let query = slots[probe.query].prepared.as_mut().unwrap();
            let precomputed = &query.precomputed;
            let binary_dot = binary
                .iter()
                .zip(&precomputed.rotated_query)
                .fold(0.0, |sum, (&bit, &value)| sum + bit as f32 * value);
            let mut distance = vector.f_add
                + probe.g_add
                + vector.f_rescale * (binary_dot + precomputed.k1x_sum_q);
            if index.ex_bits > 0 {
                diagnostics.refinements += 1;
                let ex_dot = (index.ip_func)(
                    &precomputed.rotated_query,
                    &vector.ex_code_packed,
                    index.padded_dim,
                );
                let term = precomputed.binary_scale * binary_dot
                    + ex_dot
                    + precomputed.kbx_sum_q;
                distance = vector.f_add_ex + probe.g_add + vector.f_rescale_ex * term;
            }
            if !distance.is_finite() {
                continue;
            }
            if index.metric == Metric::L2 {
                distance = distance.max(0.0);
            }
            diagnostics.candidates += 1;
            push_top_k(
                &mut query.heap,
                top_k,
                HeapCandidate {
                    id: id.clone(),
                    version: u32::MAX,
                    birth_version: u64::MAX,
                    distance,
                    score: match index.metric {
                        Metric::L2 => distance,
                        Metric::InnerProduct => -distance,
                    },
                },
            );
        }
    }
    !is_cancelled(cancel)
}
