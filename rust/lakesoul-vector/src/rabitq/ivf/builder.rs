//! IvfRabitqBuilder — unified builder for fresh builds and incremental inserts.

use super::IdAndVecBatch;
use super::IvfRabitqIndex;
use super::assign_batch_to_centroids;
use super::cluster::ClusterData;
use crate::rabitq::kmeans::KMeansResult;
use crate::rabitq::manifest::ManifestStore;
use crate::rabitq::quantizer::{QuantizedVector, RabitqConfig};
use crate::rabitq::rotation::{DynamicRotator, RotatorType};
use crate::rabitq::{Metric, RabitqError};
use rand::prelude::*;
use rand::rngs::StdRng;
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
        reservoir: Vec<f32>,
        reservoir_capacity: usize,
        reservoir_count: usize,
        reservoir_seen: usize,
    },
    Loaded {
        index: IvfRabitqIndex,
        /// Original segment metadata from manifest (segment file names,
        /// versions, sizes).  Used during incremental flush to locate
        /// and replace old segments.
        cluster_map: std::collections::BTreeMap<
            u32,
            crate::rabitq::manifest::ClusterManifestEntry,
        >,
    },
}

pub struct IvfRabitqBuilder {
    state: BuilderState,
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
    ) -> Self {
        let rotator = DynamicRotator::new(dim, rotator_type, seed);
        let reservoir_capacity = nlist * 64;
        Self {
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
                reservoir: Vec::with_capacity(reservoir_capacity * dim),
                reservoir_capacity,
                reservoir_count: 0,
                reservoir_seen: 0,
            },
        }
    }

    /// Load from an object store, or initialise a fresh builder if no
    /// manifest exists.
    ///
    /// - **Fresh**: no manifest → `insert_batch` reservoir-samples; `build`
    ///   runs k-means + streaming quantisation.
    /// - **Loaded**: manifest exists → reads centroids only from segments;
    ///   `insert_batch` appends vectors directly; `build` flushes pending.
    pub async fn load(
        mstore: &ManifestStore,
        dim: usize,
        nlist: usize,
        total_bits: usize,
        metric: Metric,
        rotator_type: RotatorType,
        seed: u64,
        use_faster_config: bool,
    ) -> Result<Self, RabitqError> {
        if crate::rabitq::manifest::manifest_exists(mstore).await {
            // Try LATEST first, fall back to legacy manifest.bin
            let (header, cluster_map) =
                match crate::rabitq::manifest::read_latest(mstore).await {
                    Ok(snap) if snap.generation > 0 => {
                        crate::rabitq::manifest::load_manifest_by_gen_ver(
                            mstore,
                            snap.generation,
                            snap.version,
                        )
                        .await?
                    }
                    _ => crate::rabitq::manifest::load_manifest(mstore).await?,
                };
            let mut clusters = Vec::with_capacity(cluster_map.len());
            for (_cid, entry) in cluster_map.iter() {
                // Read centroid from the base segment (version 0).
                let base = entry.base_segment().ok_or_else(|| {
                    RabitqError::InvalidPersistence("cluster has no base segment")
                })?;
                let (_, centroid) = crate::rabitq::manifest::read_segment_centroid(
                    mstore,
                    &base.segment_filename,
                    header.padded_dim,
                )
                .await?;
                clusters.push(ClusterData {
                    centroid,
                    ids: Vec::new(),
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
                    pending_vectors: Vec::new(),
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
                clusters,
                ex_bits: header.ex_bits,
                ip_func,
            };
            return Ok(Self {
                state: BuilderState::Loaded { index, cluster_map },
            });
        }

        let rotator = DynamicRotator::new(dim, rotator_type, seed);
        let reservoir_capacity = nlist * 64;
        Ok(Self {
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
                reservoir: Vec::with_capacity(reservoir_capacity * dim),
                reservoir_capacity,
                reservoir_count: 0,
                reservoir_seen: 0,
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
            BuilderState::Loaded { index, .. } => {
                index.insert_batch(batch)?;
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
    pub async fn build<F, S>(
        self,
        mut make_stream: F,
    ) -> Result<IvfRabitqIndex, RabitqError>
    where
        F: FnMut() -> S,
        S: futures::Stream<Item = IdAndVecBatch> + Unpin,
    {
        use futures::StreamExt;

        match self.state {
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
                reservoir,
                reservoir_count,
                reservoir_seen,
                ..
            } => {
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
                        pending_vectors: Vec::new(),
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
                const SUB_CHUNK: usize = 20_000;

                let mut stream = make_stream();
                while let Some(batch) = stream.next().await {
                    let bn = batch.vectors.len() / dim;
                    bc += 1;
                    assert_eq!(batch.ids.len() * dim, batch.vectors.len());
                    for sub_start in (0..bn).step_by(SUB_CHUNK) {
                        let sub_end = (sub_start + SUB_CHUNK).min(bn);
                        let sub_n = sub_end - sub_start;

                        let t0 = Instant::now();
                        let mut rb = vec![0.0f32; sub_n * padded_dim];
                        rb.par_chunks_mut(padded_dim).enumerate().for_each(
                            |(k, chunk)| {
                                let i = sub_start + k;
                                let v = &batch.vectors[i * dim..(i + 1) * dim];
                                chunk.copy_from_slice(&rotator.rotate(v));
                            },
                        );
                        t_stream_rot += t0.elapsed().as_secs_f64();

                        let t0 = Instant::now();
                        let bids = assign_batch_to_centroids(
                            &rb,
                            sub_n,
                            nlist,
                            padded_dim,
                            &centroid_col,
                            &centroid_norms,
                        );
                        t_stream_gemm += t0.elapsed().as_secs_f64();

                        let t0 = Instant::now();
                        let ins: Vec<(usize, QuantizedVector)> = (0..sub_n)
                            .into_par_iter()
                            .map(|i| {
                                let rv = &rb[i * padded_dim..(i + 1) * padded_dim];
                                let cid = bids[i];
                                (
                                    cid,
                                    crate::rabitq::quantizer::quantize_with_centroid(
                                        rv,
                                        &clusters[cid].centroid,
                                        &config,
                                        metric,
                                    ),
                                )
                            })
                            .collect();
                        t_stream_quant += t0.elapsed().as_secs_f64();

                        let t0 = Instant::now();
                        for (i, (cid, q)) in ins.into_iter().enumerate() {
                            clusters[cid].append_vector(batch.ids[sub_start + i], q);
                        }
                        t_stream_append += t0.elapsed().as_secs_f64();
                        drop(rb);
                        drop(bids);
                    }
                    total_vectors += bn;
                    if bc.is_multiple_of(10) {
                        println!("    {} vectors...", total_vectors);
                    }
                }
                let t_flush = Instant::now();
                for c in &mut clusters {
                    c.flush_pending();
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
                Ok(IvfRabitqIndex {
                    dim,
                    padded_dim,
                    metric,
                    rotator,
                    clusters,
                    ex_bits,
                    ip_func,
                })
            }

            BuilderState::Loaded { mut index, .. } => {
                index.flush_all_pending();
                Ok(index)
            }
        }
    }

    /// Flush the (built or loaded) index to object store.
    ///
    /// - **Fresh mode**: error — call `build()` first.
    /// - **Loaded mode**: flushes pending, then for each dirty cluster writes a
    ///   *delta* segment containing only the new vectors, and appends it to the
    ///   cluster's segment list in the manifest.
    ///
    ///   No existing segment files are read, modified, or deleted — object
    ///   storage files are immutable.  Clusters with no new vectors are left
    ///   completely untouched.  After flush the in-memory index is reset to
    ///   centroid-only state, ready for the next round of inserts.
    pub async fn flush(
        self,
        mstore: &ManifestStore,
    ) -> Result<IvfRabitqIndex, RabitqError> {
        match self.state {
            BuilderState::Loaded {
                mut index,
                mut cluster_map,
                ..
            } => {
                use crate::rabitq::manifest::{
                    self, ClusterSegmentData, ManifestHeader, SegmentManifestEntry,
                };

                // 1. Flush all pending vectors (including partial batches) into batch_data.
                index.flush_all_pending();

                // 2. Collect dirty clusters.
                let dirty_cids: Vec<u32> = index
                    .clusters
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.num_vectors > 0)
                    .map(|(i, _)| i as u32)
                    .collect();

                if dirty_cids.is_empty() {
                    println!("Flush: no dirty clusters, manifest unchanged.");
                    return Ok(index);
                }

                println!(
                    "Flush: {} dirty / {} total clusters (delta segments)",
                    dirty_cids.len(),
                    index.clusters.len()
                );

                let mut total_new: usize = 0;

                for &cid_u32 in &dirty_cids {
                    let cid = cid_u32 as usize;
                    let cluster = &index.clusters[cid];
                    let entry = cluster_map.get_mut(&cid_u32).ok_or_else(|| {
                        RabitqError::InvalidPersistence("cluster missing from manifest")
                    })?;

                    let n_new = cluster.num_vectors;
                    total_new += n_new;

                    // -- write delta segment with ONLY the new vectors --
                    let new_version = entry.latest_version() + 1;
                    let fname = manifest::segment_filename(cid_u32, new_version);
                    let seg_data = ClusterSegmentData::from_cluster_data(
                        cid_u32,
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
                        manifest::write_segment(mstore, &fname, &seg_data, new_version)
                            .await?;

                    // -- append delta to cluster's segment list (no deletion) --
                    entry.segments.push(SegmentManifestEntry {
                        segment_filename: fname,
                        segment_version: new_version,
                        num_vectors: n_new as u32,
                        file_size,
                    });
                }

                // 3. CAS loop: read LATEST, write versioned manifest, CAS-write LATEST.
                let latest = manifest::read_latest(mstore).await?;
                let new_version = latest.version + 1;
                let header = ManifestHeader {
                    generation: latest.generation,
                    dim: index.dim,
                    padded_dim: index.padded_dim,
                    metric: index.metric,
                    rotator_type: index.rotator.rotator_type(),
                    rotator_data: index.rotator.serialize(),
                    ex_bits: index.ex_bits,
                    total_bits: index.ex_bits + 1,
                };

                // Write the new immutable versioned manifest.
                manifest::save_manifest(mstore, &header, &cluster_map, new_version)
                    .await?;

                // CAS the LATEST pointer.
                match manifest::write_latest(
                    mstore,
                    latest.generation,
                    new_version,
                    latest.e_tag,
                )
                .await
                {
                    Ok(_) => {} // committed
                    Err(RabitqError::VersionConflict) => {
                        // Check if generation changed (compaction happened).
                        let latest2 = manifest::read_latest(mstore).await?;
                        if latest2.generation != latest.generation {
                            return Err(RabitqError::GenerationConflict);
                        }
                        // Same generation, just concurrent flush — retry once.
                        let new_version2 = latest2.version + 1;
                        manifest::save_manifest(
                            mstore,
                            &header,
                            &cluster_map,
                            new_version2,
                        )
                        .await?;
                        manifest::write_latest(
                            mstore,
                            latest2.generation,
                            new_version2,
                            latest2.e_tag,
                        )
                        .await?;
                    }
                    Err(e) => return Err(e),
                }

                // 4. Reset clusters to centroid-only state for next insert cycle.
                for &cid_u32 in &dirty_cids {
                    let c = &mut index.clusters[cid_u32 as usize];
                    c.ids.clear();
                    c.batch_data.clear();
                    c.ex_codes_packed.clear();
                    c.f_add_ex.clear();
                    c.f_rescale_ex.clear();
                    c.delta.clear();
                    c.vl.clear();
                    c.num_vectors = 0;
                    // pending is already empty after flush_all_pending
                }

                // If we stored the updated total we could update original_total,
                // but self is consumed — next load() will re-read the manifest.

                println!(
                    "Flush complete: {} delta segments ({} new vectors), {} segments total across all clusters",
                    dirty_cids.len(),
                    total_new,
                    cluster_map
                        .values()
                        .map(|e| e.segments.len())
                        .sum::<usize>()
                );
                Ok(index)
            }
            BuilderState::Fresh { .. } => Err(RabitqError::InvalidConfig(
                "call build() before flush() for fresh builder",
            )),
        }
    }
}
