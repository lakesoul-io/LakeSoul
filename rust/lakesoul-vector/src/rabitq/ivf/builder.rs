//! IvfRabitqBuilder — unified builder for fresh builds and incremental inserts.

use super::IdAndVecBatch;
use super::IvfRabitqIndex;
use super::assign_batch_to_centroids_into;
use super::cluster::{BuildPending, ClusterData};
use crate::rabitq::key::IndexKey;
use crate::rabitq::kmeans::KMeansResult;
use crate::rabitq::quantizer::{
    QuantizeScratch, RabitqConfig, quantize_with_centroid_parts_into,
};
use crate::rabitq::rotation::{DynamicRotator, RotatorType};
use crate::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
use crate::rabitq::{Metric, RabitqError};
use rand::prelude::*;
use rand::rngs::StdRng;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
enum BuilderState {
    Fresh {
        dim: usize,
        nlist: usize,
        total_bits: usize,
        metric: Metric,
        rotator_type: RotatorType,
        seed: u64,
        use_faster_config: bool,
        padded_dim: usize,
        ex_bits: usize,
        /// Fingerprint of the external key layout (see [`IndexHeader::key_kind`]).
        key_kind: u64,
        reservoir: Vec<f32>,
        reservoir_capacity: usize,
        reservoir_count: usize,
        reservoir_seen: usize,
    },
    Loaded {
        index: IvfRabitqIndex,
        /// Segment files of the resolved view, grouped by cluster; used
        /// to pick the next delta version for each dirty cluster.
        cluster_map: crate::rabitq::segment::SegmentMap,
        /// Keys with unflushed pending vectors, for cross-batch upserts.
        pending_keys: HashSet<IndexKey>,
        /// Every key accepted by this builder, written into the mutation
        /// delta at flush time.
        inserted_keys: BTreeSet<IndexKey>,
    },
}

pub struct IvfRabitqBuilder {
    state: BuilderState,
    chunk_flush: Option<ChunkFlush>,
}

/// Optional segment sink for fresh builds: full clusters are written and
/// cleared while the quantisation stream is still running.
struct ChunkFlush {
    store: IndexStore,
    max_bytes: usize,
    /// Next segment version for each cluster.
    versions: Vec<u32>,
    entries: Arc<Mutex<Vec<SegmentEntry>>>,
}

/// Keep the last occurrence of every key inside one input batch.
fn dedup_batch_last_wins(batch: IdAndVecBatch, dim: usize) -> IdAndVecBatch {
    let mut last = HashMap::with_capacity(batch.ids.len());
    for (index, id) in batch.ids.iter().enumerate() {
        last.insert(id.clone(), index);
    }
    if last.len() == batch.ids.len() {
        return batch;
    }
    let mut ids = Vec::with_capacity(last.len());
    let mut vectors = Vec::with_capacity(last.len() * dim);
    for (index, id) in batch.ids.iter().enumerate() {
        if last.get(id) == Some(&index) {
            ids.push(id.clone());
            vectors.extend_from_slice(&batch.vectors[index * dim..(index + 1) * dim]);
        }
    }
    IdAndVecBatch { ids, vectors }
}

async fn flush_ready_clusters(
    clusters: &mut [ClusterData],
    flush: &mut ChunkFlush,
) -> Result<(), RabitqError> {
    for (cid, cluster) in clusters.iter_mut().enumerate() {
        if cluster.memory_bytes() >= flush.max_bytes {
            flush_cluster(cid, cluster, flush).await?;
        }
    }
    Ok(())
}

async fn flush_cluster(
    cid: usize,
    cluster: &mut ClusterData,
    flush: &mut ChunkFlush,
) -> Result<(), RabitqError> {
    use crate::rabitq::segment::{ClusterSegmentData, segment_filename, write_segment};

    cluster.flush_pending();
    if cluster.num_vectors == 0 && cluster.ids.is_empty() {
        return Ok(());
    }
    let version = flush.versions[cid];
    flush.versions[cid] = version.saturating_add(1);
    let filename = segment_filename(cid as u32, version);
    let num_vectors = cluster.num_vectors as u32;
    let data = ClusterSegmentData::from_cluster_data(
        cid as u32,
        version,
        cluster.centroid.clone(),
        cluster.padded_dim,
        cluster.ex_bits,
        std::mem::take(&mut cluster.ids),
        std::mem::take(&mut cluster.batch_data),
        std::mem::take(&mut cluster.ex_codes_packed),
        std::mem::take(&mut cluster.f_add_ex),
        std::mem::take(&mut cluster.f_rescale_ex),
        std::mem::take(&mut cluster.delta),
        std::mem::take(&mut cluster.vl),
    );
    let file_size = write_segment(&flush.store, &filename, &data, version).await?;
    cluster.ids_version.clear();
    cluster.ids_birth.clear();
    cluster.num_vectors = 0;
    cluster.pending_ids.clear();
    cluster.pending_ids_version.clear();
    cluster.pending_vectors.clear();
    cluster.build_pending = BuildPending::default();
    flush
        .entries
        .lock()
        .expect("chunk entries lock")
        .push(SegmentEntry {
            data_files: Vec::new(),
            cluster_id: cid as u32,
            segment_version: version,
            segment_filename: filename,
            num_vectors,
            file_size,
            birth_version: 0,
        });
    Ok(())
}

impl IvfRabitqBuilder {
    /// Number of clusters (for metrics / display).
    pub fn cluster_count(&self) -> usize {
        match &self.state {
            BuilderState::Fresh { nlist, .. } => *nlist,
            BuilderState::Loaded { index, .. } => index.clusters.len(),
        }
    }

    /// Padded dimension (for metrics).
    pub fn padded_dim(&self) -> usize {
        match &self.state {
            BuilderState::Fresh { padded_dim, .. } => *padded_dim,
            BuilderState::Loaded { index, .. } => index.padded_dim,
        }
    }

    /// Create a fresh builder that will run full K-Means training.
    ///
    /// `insert_batch` reservoir-samples; `build` runs k-means + streaming
    /// quantisation.  This constructor does **not** interact with any object
    /// store — use [`load`] if you need to resume an existing index.
    pub fn new(
        dim: usize,
        nlist: usize,
        total_bits: usize,
        metric: Metric,
        rotator_type: RotatorType,
        seed: u64,
        use_faster_config: bool,
        key_kind: u64,
    ) -> Self {
        let rotator = DynamicRotator::new(dim, rotator_type, seed);
        let reservoir_capacity = nlist * 64;
        Self {
            chunk_flush: None,
            state: BuilderState::Fresh {
                dim,
                nlist,
                total_bits,
                metric,
                rotator_type,
                seed,
                use_faster_config,
                padded_dim: rotator.padded_dim(),
                ex_bits: total_bits.saturating_sub(1),
                key_kind,
                reservoir: Vec::with_capacity(reservoir_capacity * dim),
                reservoir_capacity,
                reservoir_count: 0,
                reservoir_seen: 0,
            },
        }
    }

    /// Load an existing index from its resolved header and segment list.
    ///
    /// Only the base segment (version 0) of every cluster is read, and only
    /// its centroid; data segments are merged by the search path.  In this
    /// mode `insert_batch` appends vectors directly and `flush` writes delta
    /// segments for the dirty clusters.
    pub async fn load(
        istore: &IndexStore,
        header: &IndexHeader,
        segments: &[SegmentEntry],
    ) -> Result<Self, RabitqError> {
        let cluster_map = crate::rabitq::segment::group_by_cluster(segments);
        let mut clusters = Vec::with_capacity(cluster_map.len());
        for entries in cluster_map.values() {
            // Cluster ids are dense (0..nlist) and the map is ordered, so
            // the vector position is the cluster id.
            let base = entries
                .iter()
                .find(|segment| segment.segment_version == 0)
                .ok_or(RabitqError::InvalidPersistence(
                    "cluster has no base segment",
                ))?;
            let (_, centroid) = crate::rabitq::segment::read_segment_centroid(
                istore,
                &base.segment_filename,
                header.padded_dim,
            )
            .await?;
            clusters.push(ClusterData {
                centroid,
                ids: Vec::new(),
                ids_version: Vec::new(),
                ids_birth: Vec::new(),
                batch_data: Vec::new(),
                ex_codes_packed: Vec::new(),
                f_add_ex: Vec::new(),
                f_rescale_ex: Vec::new(),
                delta: Vec::new(),
                vl: Vec::new(),
                num_vectors: 0,
                padded_dim: header.padded_dim,
                ex_bits: header.ex_bits,
                pending_ids: Vec::new(),
                pending_ids_version: Vec::new(),
                pending_vectors: Vec::new(),
                build_pending: BuildPending::default(),
            });
        }
        let rotator = DynamicRotator::deserialize(
            header.dim,
            header.padded_dim,
            header.rotator_type,
            &header.rotator_data,
        )?;
        let ip_func = crate::rabitq::simd::select_excode_ipfunc(header.ex_bits);
        let index = IvfRabitqIndex {
            dim: header.dim,
            padded_dim: header.padded_dim,
            metric: header.metric,
            rotator,
            invalid_masks: vec![super::InvalidMask::None; clusters.len()],
            clusters,
            ex_bits: header.ex_bits,
            ip_func,
            key_kind: header.key_kind,
            live_vectors: 0,
        };
        Ok(Self {
            chunk_flush: None,
            state: BuilderState::Loaded {
                index,
                cluster_map,
                pending_keys: HashSet::new(),
                inserted_keys: BTreeSet::new(),
            },
        })
    }

    /// Push one batch of ID'd vectors into the builder.
    ///
    /// - **Fresh mode**: reservoir-samples vectors for k-means (IDs ignored).
    /// - **Loaded mode**: rotates, finds centroids, quantises, appends to
    ///   cluster pending buffers with the provided external IDs.
    pub fn insert_batch(&mut self, batch: IdAndVecBatch) -> Result<(), RabitqError> {
        match &mut self.state {
            BuilderState::Fresh {
                dim,
                reservoir,
                reservoir_capacity,
                reservoir_count,
                reservoir_seen,
                seed,
                ..
            } => {
                let d = *dim;
                let n = batch.vectors.len() / d;
                let cap = *reservoir_capacity;
                let mut rng =
                    StdRng::seed_from_u64(seed.wrapping_add(*reservoir_seen as u64));
                for i in 0..n {
                    let v = &batch.vectors[i * d..(i + 1) * d];
                    if *reservoir_count < cap {
                        reservoir.extend_from_slice(v);
                        *reservoir_count += 1;
                    } else {
                        let j = rng.gen_range(0..*reservoir_seen + i + 1);
                        if j < cap {
                            let dst = j * d;
                            reservoir[dst..dst + d].copy_from_slice(v);
                        }
                    }
                }
                *reservoir_seen += n;
                Ok(())
            }
            BuilderState::Loaded {
                index,
                pending_keys,
                inserted_keys,
                ..
            } => {
                let dim = index.dim();
                let batch = dedup_batch_last_wins(batch, dim);
                // Only a cross-batch duplicate needs the pending repack;
                // ordinary appends stay allocation-light.
                let duplicate_cross_batch =
                    batch.ids.iter().any(|id| pending_keys.contains(id));
                let ids = batch.ids.clone();
                index.insert_batch(batch)?;
                for id in ids {
                    pending_keys.insert(id.clone());
                    inserted_keys.insert(id);
                }
                if duplicate_cross_batch {
                    index.dedupe_pending_last_wins();
                    pending_keys.clear();
                    pending_keys.extend(
                        index
                            .clusters
                            .iter()
                            .flat_map(|cluster| cluster.ids.iter().cloned()),
                    );
                }
                Ok(())
            }
        }
    }

    /// Finalise the builder.
    ///
    /// - **Fresh mode**: runs k-means on reservoir samples, then streams
    ///   `make_stream` to rotate + quantise all vectors.  Returns the built
    ///   index with the external IDs from the stream.
    /// - **Loaded mode**: flushes pending vectors into batch_data, returns
    ///   the index for persistence.
    /// Build a fresh index while flushing full clusters to `istore`.
    ///
    /// This is the bounded-output variant of [`build`]: after every input
    /// batch, clusters whose resident buffers exceed `max_chunk_bytes` are
    /// written as a segment (versions 0, 1, ...) and their buffers cleared.
    /// The returned index has empty cluster buffers; its header is still the
    /// canonical header for the generation.
    pub async fn build_with_chunk_flush<F, S>(
        mut self,
        make_stream: F,
        istore: IndexStore,
        max_chunk_bytes: usize,
    ) -> Result<(IvfRabitqIndex, Vec<SegmentEntry>), RabitqError>
    where
        F: FnMut() -> S,
        S: futures::Stream<Item = IdAndVecBatch> + Unpin,
    {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let nlist = self.cluster_count();
        self.chunk_flush = Some(ChunkFlush {
            store: istore,
            max_bytes: max_chunk_bytes,
            versions: vec![0; nlist],
            entries: Arc::clone(&entries),
        });
        let index = self.build(make_stream).await?;
        let entries = entries.lock().expect("chunk entries lock").clone();
        Ok((index, entries))
    }

    pub async fn build<F, S>(
        self,
        mut make_stream: F,
    ) -> Result<IvfRabitqIndex, RabitqError>
    where
        F: FnMut() -> S,
        S: futures::Stream<Item = IdAndVecBatch> + Unpin,
    {
        use futures::StreamExt;

        let IvfRabitqBuilder { state, chunk_flush } = self;
        let mut chunk_flush = chunk_flush;
        match state {
            BuilderState::Fresh {
                dim,
                nlist,
                total_bits,
                metric,
                rotator_type,
                seed,
                use_faster_config,
                padded_dim,
                ex_bits,
                key_kind,
                reservoir,
                reservoir_count,
                reservoir_seen,
                ..
            } => {
                if !crate::rabitq::is_supported_total_bits(total_bits) {
                    return Err(RabitqError::InvalidConfig(
                        "total_bits must be 1, 3 or 7: only those RaBitQ widths \
                         have an estimator kernel",
                    ));
                }
                use crate::rabitq::kmeans::{KMeansConfig, run_kmeans_on_flat};
                use rayon::prelude::*;

                println!(
                    "  Reservoir: {} / {} vectors ({:.1} MB)",
                    reservoir_count,
                    reservoir_seen,
                    reservoir_count * dim * 4 / (1024 * 1024)
                );
                if reservoir_count == 0 {
                    return Err(RabitqError::InvalidConfig("no vectors added"));
                }

                let rotator = DynamicRotator::new(dim, rotator_type, seed);

                let t_rot = Instant::now();
                let mut rotated_sample = vec![0.0f32; reservoir_count * padded_dim];
                rotated_sample
                    .par_chunks_mut(padded_dim)
                    .enumerate()
                    .for_each(|(i, chunk)| {
                        let v = &reservoir[i * dim..(i + 1) * dim];
                        chunk.copy_from_slice(&rotator.rotate(v));
                    });
                drop(reservoir);
                let t_rot = t_rot.elapsed();

                let t_km = Instant::now();
                println!("  Training k-means ({} clusters, 15 iterations)...", nlist);
                let kmeans_config = KMeansConfig {
                    niter: 15,
                    nredo: 1,
                    seed: StdRng::seed_from_u64(seed).next_u64(),
                    spherical: false,
                    max_points_per_centroid: 64,
                    decode_block_size: 32768,
                };
                let KMeansResult {
                    centroids: rotated_centroids,
                    ..
                } = run_kmeans_on_flat(
                    &rotated_sample,
                    reservoir_count,
                    padded_dim,
                    nlist,
                    kmeans_config,
                );
                drop(rotated_sample);
                let t_km = t_km.elapsed();

                let config = if use_faster_config {
                    RabitqConfig::faster(padded_dim, total_bits, seed)
                } else {
                    RabitqConfig::new(total_bits)
                };
                let mut clusters: Vec<ClusterData> = rotated_centroids
                    .iter()
                    .map(|c| ClusterData {
                        centroid: c.clone(),
                        ids: Vec::new(),
                        ids_version: Vec::new(),
                        ids_birth: Vec::new(),
                        batch_data: Vec::new(),
                        ex_codes_packed: Vec::new(),
                        f_add_ex: Vec::new(),
                        f_rescale_ex: Vec::new(),
                        delta: Vec::new(),
                        vl: Vec::new(),
                        num_vectors: 0,
                        padded_dim,
                        ex_bits,
                        pending_ids: Vec::new(),
                        pending_ids_version: Vec::new(),
                        pending_vectors: Vec::new(),
                        build_pending: BuildPending::default(),
                    })
                    .collect();

                let centroid_col: Vec<f32> = {
                    let mut col = vec![0.0f32; padded_dim * nlist];
                    for (cid, c) in rotated_centroids.iter().enumerate() {
                        for d in 0..padded_dim {
                            col[d * nlist + cid] = c[d];
                        }
                    }
                    col
                };
                let centroid_norms: Vec<f32> = rotated_centroids
                    .iter()
                    .map(|c| c.iter().map(|x| x * x).sum())
                    .collect();

                println!("  Streaming rotation + quantisation...");
                let mut total_vectors: usize = 0;
                let mut bc: usize = 0;
                let mut t_stream_rot = 0.0f64;
                let mut t_stream_gemm = 0.0f64;
                let mut t_stream_quant = 0.0f64;
                let mut t_stream_append = 0.0f64;
                const SUB_CHUNK: usize = 32_000;
                // Reused across every sub-chunk: keeps streaming peak memory
                // bounded and avoids allocator churn for the whole scan.
                let mut rb = Vec::with_capacity(SUB_CHUNK * padded_dim);
                let mut bids = Vec::with_capacity(SUB_CHUNK);
                let mut dot_products = Vec::with_capacity(SUB_CHUNK * nlist);
                let mut norms = Vec::with_capacity(SUB_CHUNK);
                let mut cluster_offsets = vec![0usize; nlist + 1];
                let mut cluster_order = Vec::with_capacity(SUB_CHUNK);
                let mut cluster_cursors = vec![0usize; nlist];

                let mut stream = make_stream();
                while let Some(batch) = stream.next().await {
                    let bn = batch.vectors.len() / dim;
                    bc += 1;
                    assert_eq!(batch.ids.len() * dim, batch.vectors.len());
                    for sub_start in (0..bn).step_by(SUB_CHUNK) {
                        let sub_end = (sub_start + SUB_CHUNK).min(bn);
                        let sub_n = sub_end - sub_start;

                        let t0 = Instant::now();
                        rb.clear();
                        rb.resize(sub_n * padded_dim, 0.0);
                        rb.par_chunks_mut(padded_dim).enumerate().for_each(
                            |(k, chunk)| {
                                let i = sub_start + k;
                                let v = &batch.vectors[i * dim..(i + 1) * dim];
                                rotator.rotate_into(v, chunk);
                            },
                        );
                        t_stream_rot += t0.elapsed().as_secs_f64();

                        let t0 = Instant::now();
                        bids.clear();
                        assign_batch_to_centroids_into(
                            &rb,
                            sub_n,
                            nlist,
                            padded_dim,
                            &centroid_col,
                            &centroid_norms,
                            Metric::L2,
                            &mut bids,
                            &mut dot_products,
                            &mut norms,
                        );
                        t_stream_gemm += t0.elapsed().as_secs_f64();

                        // Group the sub-chunk by cluster so each cluster can
                        // quantise and append its rows without cross-thread
                        // synchronisation.
                        let t0 = Instant::now();
                        cluster_offsets.fill(0);
                        for &cid in &bids {
                            cluster_offsets[cid + 1] += 1;
                        }
                        for cid in 1..=nlist {
                            cluster_offsets[cid] += cluster_offsets[cid - 1];
                        }
                        cluster_order.clear();
                        cluster_order.resize(sub_n, 0);
                        cluster_cursors.copy_from_slice(&cluster_offsets[..nlist]);
                        for (i, &cid) in bids.iter().enumerate() {
                            let pos = cluster_cursors[cid];
                            cluster_order[pos] = i;
                            cluster_cursors[cid] += 1;
                        }
                        t_stream_append += t0.elapsed().as_secs_f64();

                        let t0 = Instant::now();
                        let offsets = &cluster_offsets;
                        let order = &cluster_order;
                        clusters
                            .par_iter_mut()
                            .enumerate()
                            .map_init(
                                || QuantizeScratch::new(padded_dim),
                                |scratch, (cid, cluster)| {
                                    for &i in &order[offsets[cid]..offsets[cid + 1]] {
                                        let rv =
                                            &rb[i * padded_dim..(i + 1) * padded_dim];
                                        let params = quantize_with_centroid_parts_into(
                                            rv,
                                            &cluster.centroid,
                                            &config,
                                            metric,
                                            scratch,
                                        );
                                        cluster.append_packed_for_build(
                                            batch.ids[sub_start + i].clone(),
                                            &scratch.packed_binary,
                                            &scratch.packed_ex,
                                            params,
                                        );
                                    }
                                },
                            )
                            .for_each(drop);
                        t_stream_quant += t0.elapsed().as_secs_f64();
                    }
                    total_vectors += bn;
                    if let Some(flush) = chunk_flush.as_mut() {
                        flush_ready_clusters(&mut clusters, flush).await?;
                    }
                    if bc.is_multiple_of(10) {
                        println!("    {} vectors...", total_vectors);
                    }
                }
                if let Some(flush) = chunk_flush.as_mut() {
                    for cid in 0..clusters.len() {
                        flush_cluster(cid, &mut clusters[cid], flush).await?;
                    }
                }
                let t_flush = Instant::now();
                for c in &mut clusters {
                    c.flush_pending();
                    c.release_build_buffers();
                }
                let t_flush = t_flush.elapsed();
                println!(
                    "  Build complete: {} vectors, {} clusters",
                    total_vectors,
                    clusters.len()
                );
                println!("  ── Phase timing ──");
                println!("    rotate reservoir:  {:5.1}s", t_rot.as_secs_f64());
                println!("    k-means (15 iter): {:5.1}s", t_km.as_secs_f64());
                println!("    stream rotate:     {:5.1}s", t_stream_rot);
                println!("    stream GEMM:       {:5.1}s", t_stream_gemm);
                println!("    stream quantise:   {:5.1}s", t_stream_quant);
                println!("    stream append:     {:5.1}s", t_stream_append);
                println!("    flush_pending:     {:5.1}s", t_flush.as_secs_f64());
                let ip_func = crate::rabitq::simd::select_excode_ipfunc(ex_bits);
                let live_vectors = clusters
                    .iter()
                    .map(|cluster| cluster.num_vectors + cluster.pending_ids.len())
                    .sum();
                let invalid_masks =
                    vec![crate::rabitq::ivf::InvalidMask::None; clusters.len()];
                Ok(IvfRabitqIndex {
                    dim,
                    padded_dim,
                    metric,
                    rotator,
                    invalid_masks,
                    clusters,
                    ex_bits,
                    ip_func,
                    key_kind,
                    live_vectors,
                })
            }

            BuilderState::Loaded { mut index, .. } => {
                index.flush_all_pending();
                Ok(index)
            }
        }
    }

    /// Write delta segments for the dirty clusters.
    ///
    /// Returns the index header and the newly written segments; the caller
    /// publishes them through the metadata catalog.  No existing segment
    /// file is read, modified or deleted.  Clusters with no new vectors are
    /// left untouched, and the in-memory index is reset to centroid-only
    /// state afterwards.
    pub async fn flush(
        self,
        istore: &IndexStore,
    ) -> Result<(IndexHeader, Vec<SegmentEntry>), RabitqError> {
        let (header, segments, _) = self.flush_with_mutations(istore).await?;
        Ok((header, segments))
    }

    /// Flush delta segments and return the keys that must be recorded in the
    /// mutation delta so older copies are masked on the next load.
    pub async fn flush_with_mutations(
        self,
        istore: &IndexStore,
    ) -> Result<(IndexHeader, Vec<SegmentEntry>, Vec<IndexKey>), RabitqError> {
        match self.state {
            BuilderState::Loaded {
                mut index,
                cluster_map,
                inserted_keys,
                ..
            } => {
                use crate::rabitq::segment::{
                    ClusterSegmentData, segment_filename, write_segment,
                };

                index.flush_all_pending();

                let dirty_cids: Vec<u32> = index
                    .clusters
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.num_vectors > 0)
                    .map(|(i, _)| i as u32)
                    .collect();

                let header = index.index_header();
                let inserted_keys: Vec<IndexKey> = inserted_keys.into_iter().collect();
                if dirty_cids.is_empty() {
                    println!("Flush: no dirty clusters, nothing to commit.");
                    return Ok((header, Vec::new(), inserted_keys));
                }

                println!(
                    "Flush: {} dirty / {} total clusters (delta segments)",
                    dirty_cids.len(),
                    index.clusters.len()
                );

                let mut total_new: usize = 0;
                let mut new_segments = Vec::with_capacity(dirty_cids.len());

                for &cid_u32 in &dirty_cids {
                    let cid = cid_u32 as usize;
                    let cluster = &index.clusters[cid];

                    let latest_version = cluster_map
                        .get(&cid_u32)
                        .and_then(|segments| {
                            segments.iter().map(|s| s.segment_version).max()
                        })
                        .unwrap_or(0);
                    let new_version = latest_version + 1;
                    let n_new = cluster.num_vectors;
                    total_new += n_new;

                    let fname = segment_filename(cid_u32, new_version);
                    let seg_data = ClusterSegmentData::from_cluster_data(
                        cid_u32,
                        new_version,
                        cluster.centroid.clone(),
                        cluster.padded_dim,
                        cluster.ex_bits,
                        cluster.ids.clone(),
                        cluster.batch_data.clone(),
                        cluster.ex_codes_packed.clone(),
                        cluster.f_add_ex.clone(),
                        cluster.f_rescale_ex.clone(),
                        cluster.delta.clone(),
                        cluster.vl.clone(),
                    );
                    let file_size =
                        write_segment(istore, &fname, &seg_data, new_version).await?;

                    new_segments.push(SegmentEntry {
                        cluster_id: cid_u32,
                        segment_version: new_version,
                        segment_filename: fname,
                        num_vectors: n_new as u32,
                        file_size,
                        // The core builder works on vectors, not files; the
                        // shard builder records which data files a build read.
                        data_files: Vec::new(),
                        // Stamped by the catalog committer.
                        birth_version: 0,
                    });
                }

                // Reset clusters to centroid-only state for the next cycle.
                for &cid_u32 in &dirty_cids {
                    let c = &mut index.clusters[cid_u32 as usize];
                    c.ids.clear();
                    c.ids_version.clear();
                    c.ids_birth.clear();
                    c.batch_data.clear();
                    c.ex_codes_packed.clear();
                    c.f_add_ex.clear();
                    c.f_rescale_ex.clear();
                    c.delta.clear();
                    c.vl.clear();
                    c.pending_ids.clear();
                    c.pending_ids_version.clear();
                    c.pending_vectors.clear();
                    c.num_vectors = 0;
                }
                index.invalid_masks =
                    vec![super::InvalidMask::None; index.clusters.len()];
                index.live_vectors = 0;

                println!(
                    "Flush complete: {} delta segments ({} new vectors)",
                    new_segments.len(),
                    total_new
                );
                Ok((header, new_segments, inserted_keys))
            }
            BuilderState::Fresh { .. } => Err(RabitqError::InvalidConfig(
                "call build() before flush() for fresh builder",
            )),
        }
    }
}
