//! Lazy IVF clusters backed by shared immutable payload views.
//!
//! A centroid selector determines each query's ordered probes. The same ready
//! cluster scheduler and block-major kernel serve eager and lazy searches;
//! payloads and mutation masks are retained in Arc views, never cloned into a
//! temporary index per query. Concurrent misses share a cancellation-aware
//! single-flight load, scoped by the complete object-store handle identity.

use futures::future::Future;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use super::batch::{
    BatchClusterSource, ClusterView, cancelled_error, is_cancelled, search_source_batch,
};
use super::{
    BatchSearchDiagnostics, BatchSearchOptions, BatchSearchOutput, InvalidMask,
    IvfRabitqIndex, SearchParams, SearchResult, build_invalid_mask,
};
use crate::rabitq::RabitqError;
use crate::rabitq::ivf::cluster::ClusterData;
use crate::rabitq::mutation::MutationState;
use crate::rabitq::segment::{
    IndexHeader, IndexStore, SegmentEntry, group_by_cluster, read_segment_centroid,
    read_segment_full,
};

/// The store guard prevents address reuse while a cached payload is retained.
/// Different buckets/stores with identical prefixes never share cache entries.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClusterKey {
    store: usize,
    prefix: String,
    commit_id: i64,
    cluster_id: u32,
}

pub(super) struct LoadedCluster {
    pub(super) data: ClusterData,
    pub(super) invalid_mask: InvalidMask,
    _store_guard: Arc<dyn ObjectStore>,
}

impl LoadedCluster {
    fn memory_bytes(&self) -> usize {
        self.data.memory_bytes() + self.invalid_mask.memory_usage()
    }
}

struct ClusterCache {
    entries: HashMap<ClusterKey, Arc<LoadedCluster>>,
    order: VecDeque<ClusterKey>,
    bytes: usize,
    max_bytes: usize,
}

impl ClusterCache {
    fn new() -> Self {
        let max_bytes = crate::cache_config::config().cluster_bytes();
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, key: &ClusterKey) -> Option<Arc<LoadedCluster>> {
        let value = self.entries.get(key).cloned()?;
        self.order.retain(|entry| entry != key);
        self.order.push_back(key.clone());
        Some(value)
    }

    fn put(&mut self, key: ClusterKey, value: Arc<LoadedCluster>) {
        let bytes = value.memory_bytes();
        if bytes > self.max_bytes || self.max_bytes == 0 {
            return;
        }
        if let Some(old) = self.entries.insert(key.clone(), value) {
            self.bytes = self.bytes.saturating_sub(old.memory_bytes());
            self.order.retain(|entry| entry != &key);
        }
        self.order.push_back(key);
        self.bytes = self.bytes.saturating_add(bytes);
        while self.bytes > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(old.memory_bytes());
            }
        }
    }
}

type ClusterFlight = tokio::sync::OnceCell<Arc<LoadedCluster>>;
static CLUSTER_CACHE: LazyLock<Mutex<ClusterCache>> =
    LazyLock::new(|| Mutex::new(ClusterCache::new()));
// Weak references coalesce overlapping reads without retaining evicted payloads.
static CLUSTER_FLIGHTS: LazyLock<Mutex<HashMap<ClusterKey, Weak<ClusterFlight>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Drop cached clusters and pending flight registrations for one index prefix.
pub fn invalidate_prefix(prefix: &str) {
    let prefix = prefix.trim_end_matches('/');
    if let Ok(mut cache) = CLUSTER_CACHE.lock() {
        let stale: Vec<_> = cache
            .entries
            .keys()
            .filter(|key| key.prefix == prefix)
            .cloned()
            .collect();
        for key in stale {
            if let Some(old) = cache.entries.remove(&key) {
                cache.bytes = cache.bytes.saturating_sub(old.memory_bytes());
            }
            cache.order.retain(|cached| cached != &key);
        }
    }
    if let Ok(mut flights) = CLUSTER_FLIGHTS.lock() {
        flights.retain(|key, flight| key.prefix != prefix && flight.strong_count() > 0);
    }
}

/// A shard that loads only selected cluster payloads.
pub struct LazyIvfIndex {
    store: Arc<dyn ObjectStore>,
    index_prefix: String,
    commit_id: i64,
    header: IndexHeader,
    selector: IvfRabitqIndex,
    /// Real cluster id for each position in the selector's centroid vector.
    cluster_ids: Vec<u32>,
    segments: HashMap<u32, Vec<SegmentEntry>>,
    mutation: Option<MutationState>,
    runtime: tokio::runtime::Handle,
}

impl LazyIvfIndex {
    /// Build a centroid-only selector from the first segment of each cluster.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        index_prefix: &str,
        commit_id: i64,
        header: &IndexHeader,
        segments: &[SegmentEntry],
        mutation: Option<MutationState>,
    ) -> Result<Self, RabitqError> {
        let cluster_map = group_by_cluster(segments);
        let entry_list: Vec<(u32, SegmentEntry)> = cluster_map
            .iter()
            .filter_map(|(cid, entries)| {
                entries
                    .iter()
                    .min_by_key(|entry| entry.segment_version)
                    .map(|entry| (*cid, entry.clone()))
            })
            .collect();
        let store_handle = Arc::clone(&store);
        let prefix = index_prefix.to_string();
        let padded_dim = header.padded_dim;
        let mut centroids: Vec<(u32, Vec<f32>)> = futures::stream::iter(entry_list)
            .map(|(cid, entry)| {
                let store_handle = Arc::clone(&store_handle);
                let prefix = prefix.clone();
                async move {
                    let istore = IndexStore::new(store_handle, prefix);
                    let (_, centroid) = read_segment_centroid(
                        &istore,
                        &entry.segment_filename,
                        padded_dim,
                    )
                    .await?;
                    Ok::<_, RabitqError>((cid, centroid))
                }
            })
            .buffer_unordered(64)
            .try_collect()
            .await?;
        // Async completion order must not change cluster-ID tie breaking.
        centroids.sort_unstable_by_key(|(cid, _)| *cid);
        let cluster_ids = centroids.iter().map(|(cid, _)| *cid).collect();
        let selector = IvfRabitqIndex::from_centroids(
            header,
            centroids
                .into_iter()
                .map(|(_, centroid)| centroid)
                .collect(),
        )?;
        Ok(Self {
            store,
            index_prefix: index_prefix.trim_end_matches('/').to_string(),
            commit_id,
            header: header.clone(),
            selector,
            cluster_ids,
            segments: cluster_map.into_iter().collect(),
            mutation,
            runtime: tokio::runtime::Handle::current(),
        })
    }

    pub fn dim(&self) -> usize {
        self.header.dim
    }

    pub fn cluster_count(&self) -> usize {
        self.selector.cluster_count()
    }

    /// Resident selector/metadata footprint; payloads use the shared cache.
    pub fn memory_bytes(&self) -> usize {
        self.selector.memory_bytes()
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn cluster_key(&self, cluster_id: u32) -> ClusterKey {
        ClusterKey {
            store: Arc::as_ptr(&self.store) as *const () as usize,
            prefix: self.index_prefix.clone(),
            commit_id: self.commit_id,
            cluster_id,
        }
    }

    async fn read_cluster(
        &self,
        cluster_id: u32,
        cancel: Option<&AtomicBool>,
    ) -> Result<Arc<LoadedCluster>, RabitqError> {
        let Some(entries) = self.segments.get(&cluster_id) else {
            return Err(RabitqError::InvalidPersistence(
                "cluster missing from index segments",
            ));
        };
        let istore = IndexStore::new(self.store.clone(), self.index_prefix.clone());
        let mut datas = Vec::with_capacity(entries.len());
        for entry in entries {
            if is_cancelled(cancel) {
                return Err(cancelled_error());
            }
            let mut data = read_segment_full(&istore, &entry.segment_filename).await?;
            data.birth_version = entry.birth_version;
            datas.push(data);
        }
        if is_cancelled(cancel) {
            return Err(cancelled_error());
        }
        let data = ClusterData::merge_segments_sequential(datas)?;
        let valid: Vec<_> = (0..data.num_vectors)
            .map(|position| {
                self.mutation.as_ref().is_none_or(|mutation| {
                    mutation.is_valid(
                        &data.ids[position],
                        data.ids_birth.get(position).copied().unwrap_or(0),
                    )
                })
            })
            .collect();
        Ok(Arc::new(LoadedCluster {
            data,
            invalid_mask: build_invalid_mask(&valid),
            _store_guard: Arc::clone(&self.store),
        }))
    }

    async fn load_cluster(
        &self,
        cluster_id: u32,
        cancel: Option<&AtomicBool>,
        diagnostics: &mut BatchSearchDiagnostics,
    ) -> Result<Arc<LoadedCluster>, RabitqError> {
        if is_cancelled(cancel) {
            return Err(cancelled_error());
        }
        let key = self.cluster_key(cluster_id);
        if let Ok(mut cache) = CLUSTER_CACHE.lock()
            && let Some(cluster) = cache.get(&key)
        {
            diagnostics.cluster_cache_hits += 1;
            return Ok(cluster);
        }
        let flight = {
            let mut flights = CLUSTER_FLIGHTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(flight) = flights.get(&key).and_then(Weak::upgrade) {
                flight
            } else {
                // Clean dead keys without retaining one entry per ever-seen cid.
                flights.retain(|_, flight| flight.strong_count() > 0);
                let flight = Arc::new(ClusterFlight::new());
                flights.insert(key.clone(), Arc::downgrade(&flight));
                flight
            }
        };
        let initialized_here = AtomicBool::new(false);
        let future = flight.get_or_try_init(|| async {
            // A preceding flight may have populated the LRU between our first
            // cache lookup and taking the flight registration lock.
            if let Ok(mut cache) = CLUSTER_CACHE.lock()
                && let Some(cluster) = cache.get(&key)
            {
                return Ok::<_, RabitqError>(cluster);
            }
            initialized_here.store(true, Ordering::Relaxed);
            let cluster = self.read_cluster(cluster_id, cancel).await?;
            if let Ok(mut cache) = CLUSTER_CACHE.lock() {
                cache.put(key.clone(), Arc::clone(&cluster));
            }
            Ok(cluster)
        });
        tokio::pin!(future);
        let result = if cancel.is_some() {
            loop {
                tokio::select! {
                    result = &mut future => break result,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(5)) => {
                        if is_cancelled(cancel) {
                            return Err(cancelled_error());
                        }
                    }
                }
            }
        } else {
            future.await
        };
        if initialized_here.load(Ordering::Relaxed) {
            diagnostics.lazy_cluster_loads += 1;
        } else {
            diagnostics.singleflight_waits += 1;
        }
        result.map(Arc::clone)
    }

    /// Search a single query without cloning selected cluster payloads.
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        nprobe: usize,
        parallelism: usize,
    ) -> Result<Vec<SearchResult>, RabitqError> {
        self.search_batch_with_options(
            &[query],
            SearchParams::new(top_k, nprobe),
            &BatchSearchOptions {
                parallelism,
                ..Default::default()
            },
        )
        .results
        .pop()
        .unwrap()
    }

    /// Batch search with shared payloads and block-major ready-cluster scans.
    pub fn search_batch(
        &self,
        queries: &[&[f32]],
        top_k: usize,
        nprobe: usize,
        parallelism: usize,
    ) -> Vec<Result<Vec<SearchResult>, RabitqError>> {
        self.search_batch_with_options(
            queries,
            SearchParams::new(top_k, nprobe),
            &BatchSearchOptions {
                parallelism,
                ..Default::default()
            },
        )
        .results
    }

    pub fn search_batch_controlled(
        &self,
        queries: &[&[f32]],
        top_k: usize,
        nprobe: usize,
        parallelism: usize,
        cancel: Arc<AtomicBool>,
    ) -> Vec<Result<Vec<SearchResult>, RabitqError>> {
        self.search_batch_with_options(
            queries,
            SearchParams::new(top_k, nprobe),
            &BatchSearchOptions {
                parallelism,
                cancellation: Some(cancel),
                ..Default::default()
            },
        )
        .results
    }

    pub fn search_batch_with_options(
        &self,
        queries: &[&[f32]],
        params: SearchParams,
        options: &BatchSearchOptions,
    ) -> BatchSearchOutput {
        search_source_batch(self, queries, params, options)
    }
}

impl BatchClusterSource for LazyIvfIndex {
    fn selector(&self) -> &IvfRabitqIndex {
        &self.selector
    }

    fn has_vectors(&self) -> bool {
        !self.cluster_ids.is_empty()
    }

    fn cluster(
        &self,
        position: usize,
        cancel: Option<&AtomicBool>,
        diagnostics: &mut BatchSearchDiagnostics,
    ) -> Result<ClusterView<'_>, RabitqError> {
        let cid = self.cluster_ids.get(position).copied().ok_or(
            RabitqError::InvalidPersistence("selected cluster is missing"),
        )?;
        self.block_on(self.load_cluster(cid, cancel, diagnostics))
            .map(ClusterView::Shared)
    }
}

#[cfg(test)]
#[path = "lazy_batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rabitq::{
        IdAndVecBatch, IvfRabitqBuilder, Metric, RotatorType, SearchParams,
    };
    use object_store::memory::InMemory;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lazy_search_matches_eager_search() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "lazy-test/";
        let dim = 8usize;
        let batch = IdAndVecBatch {
            ids: (0u64..512)
                .map(|id| crate::rabitq::key::IndexKey::new(id.to_le_bytes()))
                .collect(),
            vectors: (0..512)
                .flat_map(|id| {
                    (0..dim).map(move |d| ((id * 13 + d as u64 * 7) % 101) as f32)
                })
                .collect(),
        };
        let mut builder = IvfRabitqBuilder::new(
            dim,
            64,
            7,
            Metric::L2,
            RotatorType::FhtKacRotator,
            7,
            false,
            7,
        );
        builder.insert_batch(batch.clone()).unwrap();
        let index = builder
            .build(|| futures::stream::iter(vec![batch.clone()]))
            .await
            .unwrap();
        let istore = IndexStore::new(Arc::clone(&store), prefix.to_string());
        let (header, segments) = index.write_base_segments(&istore).await.unwrap();
        let eager = IvfRabitqIndex::load_from_segments(&istore, &header, &segments)
            .await
            .unwrap();
        let lazy =
            LazyIvfIndex::open(Arc::clone(&store), prefix, 1, &header, &segments, None)
                .await
                .unwrap();
        let query = vec![1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let expected = eager
            .search(&query, SearchParams::new(10, 8).with_parallelism(1))
            .unwrap();
        let actual = lazy.search(&query, 10, 8, 1).unwrap();
        assert_eq!(expected, actual);
    }
}
