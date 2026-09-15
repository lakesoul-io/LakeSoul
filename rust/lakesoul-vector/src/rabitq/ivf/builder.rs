//! IvfRabitqBuilder — unified builder for fresh builds and incremental inserts.

use super::IdAndVecBatch;
use super::IvfRabitqIndex;
use super::assign_batch_to_centroids;
use super::cluster::ClusterData;
use crate::rabitq::kmeans::KMeansResult;
use crate::rabitq::quantizer::{QuantizedVector, RabitqConfig};
use crate::rabitq::rotation::{DynamicRotator, RotatorType};
use crate::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
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
        /// Segment files of the resolved view, grouped by cluster; used
        /// to pick the next delta version for each dirty cluster.
        cluster_map: crate::rabitq::segment::SegmentMap,
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
        Ok(Self {
            state: BuilderState::Loaded { index, cluster_map },
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
        match self.state {
            BuilderState::Loaded {
                mut index,
                cluster_map,
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
                if dirty_cids.is_empty() {
                    println!("Flush: no dirty clusters, nothing to commit.");
                    return Ok((header, Vec::new()));
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
                    });
                }

                // Reset clusters to centroid-only state for the next cycle.
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
                }

                println!(
                    "Flush complete: {} delta segments ({} new vectors)",
                    new_segments.len(),
                    total_new
                );
                Ok((header, new_segments))
            }
            BuilderState::Fresh { .. } => Err(RabitqError::InvalidConfig(
                "call build() before flush() for fresh builder",
            )),
        }
    }
}
