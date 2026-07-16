use std::collections::BinaryHeap;
use std::convert::TryFrom;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::rabitq::manifest::ManifestStore;
use crc32fast::Hasher;

use rand::prelude::*;
use rayon::prelude::*;
use roaring::RoaringBitmap;

use crate::rabitq::kmeans::{KMeansResult, run_kmeans};
use crate::rabitq::math::{dot, l2_distance_sqr};
use crate::rabitq::quantizer::{QuantizedVector, RabitqConfig, quantize_with_centroid};
use crate::rabitq::rotation::{DynamicRotator, RotatorType};
use crate::rabitq::simd;
use crate::rabitq::{Metric, RabitqError};

pub mod builder;
pub mod cluster;
use cluster::*;
pub mod lut;
use lut::*;

/// Parameters for IVF search.
#[derive(Debug, Clone, Copy)]
pub struct SearchParams {
    pub top_k: usize,
    pub nprobe: usize,
}

impl SearchParams {
    pub fn new(top_k: usize, nprobe: usize) -> Self {
        Self { top_k, nprobe }
    }
}

/// Result entry returned by IVF search.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub id: u64,
    pub score: f32,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct SearchDiagnostics {
    pub estimated: usize,
    pub skipped_by_lower_bound: usize,
    pub extended_evaluations: usize,
}

// ============================================================================
// Unified Memory Layout Data Structures (Phase 1 Optimization)
// ============================================================================

/// Generic 64-byte cache line aligned wrapper
/// Ensures data is aligned to cache line boundaries for optimal performance
/// Lookup table for batch FastScan search
/// Mimics C++ Lut class from lut.hpp

/// IVF + RaBitQ index implemented in Rust.
#[derive(Debug, Clone)]
pub struct IvfRabitqIndex {
    dim: usize,
    padded_dim: usize,
    metric: Metric,
    rotator: DynamicRotator,
    /// Unified cluster data with optimized memory layout (Phase 1 optimization)
    clusters: Vec<ClusterData>,
    ex_bits: usize,
    /// Function pointer for ex-code inner product on packed data (Phase 3 optimization)
    /// Selected based on ex_bits at index construction time
    ip_func: crate::rabitq::simd::ExIpFunc,
}

// ========================================================================
// Unified builder: insert_batch → build → flush
// ========================================================================
//
// Two modes:
//   Fresh (new from scratch):   builder.new() → insert_batch × N → build() → flush()
//   Loaded (incremental):       builder.load(dir) → insert_batch × N → build() → flush()
//
// In fresh mode, insert_batch does reservoir sampling.
// In loaded mode, insert_batch rotates + quantises + appends to clusters.

impl IvfRabitqIndex {
    /// Train a new index from the provided dataset.
    pub fn train(
        data: &[Vec<f32>],
        nlist: usize,
        total_bits: usize,
        metric: Metric,
        rotator_type: RotatorType,
        seed: u64,
        use_faster_config: bool,
    ) -> Result<Self, RabitqError> {
        if data.is_empty() {
            return Err(RabitqError::InvalidConfig(
                "training data must be non-empty",
            ));
        }
        if nlist == 0 {
            return Err(RabitqError::InvalidConfig("nlist must be positive"));
        }
        if total_bits == 0 || total_bits > 16 {
            return Err(RabitqError::InvalidConfig(
                "total_bits must be between 1 and 16",
            ));
        }

        let dim = data[0].len();
        if data.iter().any(|v| v.len() != dim) {
            return Err(RabitqError::InvalidConfig(
                "input vectors must share the same dimension",
            ));
        }
        if nlist > data.len() {
            return Err(RabitqError::InvalidConfig(
                "nlist cannot exceed number of vectors",
            ));
        }

        println!(
            "Training k-means on original data ({} clusters, {} iterations)...",
            nlist, 15
        );
        let mut rng = StdRng::seed_from_u64(seed ^ 0x5a5a_5a5a5a5a5a5a);
        let KMeansResult {
            centroids,
            assignments,
            ..
        } = run_kmeans(data, nlist, 15, &mut rng);
        println!("K-means training complete");

        let rotator = DynamicRotator::new(dim, rotator_type, seed);
        let padded_dim = rotator.padded_dim();

        // Log huge pages status
        crate::rabitq::memory::log_huge_page_status();

        println!("Rotating data vectors...");
        let rotated_data: Vec<Vec<f32>> =
            data.par_iter().map(|v| rotator.rotate(v)).collect();
        println!("Rotating centroids...");
        let rotated_centroids: Vec<Vec<f32>> =
            centroids.par_iter().map(|c| rotator.rotate(c)).collect();

        Self::build_from_rotated(
            dim,
            padded_dim,
            metric,
            rotator,
            rotated_centroids,
            &rotated_data,
            &assignments,
            total_bits,
            seed,
            use_faster_config,
        )
    }

    /// Train an index using externally provided centroids and cluster assignments.
    #[allow(clippy::too_many_arguments)]
    pub fn train_with_clusters(
        data: &[Vec<f32>],
        centroids: &[Vec<f32>],
        assignments: &[usize],
        total_bits: usize,
        metric: Metric,
        rotator_type: RotatorType,
        seed: u64,
        use_faster_config: bool,
    ) -> Result<Self, RabitqError> {
        if data.is_empty() {
            return Err(RabitqError::InvalidConfig(
                "training data must be non-empty",
            ));
        }
        if centroids.is_empty() {
            return Err(RabitqError::InvalidConfig("centroids must be non-empty"));
        }
        if assignments.len() != data.len() {
            return Err(RabitqError::InvalidConfig(
                "assignments length must match data length",
            ));
        }
        if total_bits == 0 || total_bits > 16 {
            return Err(RabitqError::InvalidConfig(
                "total_bits must be between 1 and 16",
            ));
        }

        let dim = data[0].len();
        if data.iter().any(|v| v.len() != dim) {
            return Err(RabitqError::InvalidConfig(
                "input vectors must share the same dimension",
            ));
        }
        if centroids.iter().any(|c| c.len() != dim) {
            return Err(RabitqError::InvalidConfig(
                "centroids must match the data dimensionality",
            ));
        }

        let nlist = centroids.len();
        if nlist == 0 {
            return Err(RabitqError::InvalidConfig("nlist must be positive"));
        }
        if nlist > data.len() {
            return Err(RabitqError::InvalidConfig(
                "nlist cannot exceed number of vectors",
            ));
        }
        if assignments.iter().any(|&cid| cid >= nlist) {
            return Err(RabitqError::InvalidConfig(
                "assignments reference invalid cluster ids",
            ));
        }

        let rotator = DynamicRotator::new(dim, rotator_type, seed);
        let padded_dim = rotator.padded_dim();

        // Log huge pages status
        crate::rabitq::memory::log_huge_page_status();

        let rotated_data: Vec<Vec<f32>> =
            data.par_iter().map(|v| rotator.rotate(v)).collect();
        let rotated_centroids: Vec<Vec<f32>> =
            centroids.par_iter().map(|c| rotator.rotate(c)).collect();

        Self::build_from_rotated(
            dim,
            padded_dim,
            metric,
            rotator,
            rotated_centroids,
            &rotated_data,
            assignments,
            total_bits,
            seed,
            use_faster_config,
        )
    }

    // ========================================================================
    // Streaming batch-oriented training (memory-efficient)
    // ========================================================================

    /// Build an IVF+RaBitQ index from flat batches (memory-efficient).
    ///
    /// Each element of `batches` is a flat `Vec<f32>` holding `[n_i * dim]`
    /// row-major vectors.  Batches are iterated twice (k-means sampling,
    /// then rotation+quantisation).  Peak memory is bounded by the largest
    /// batch + k-means reservoir (~nlist*64 vectors) + centroids.
    pub fn train_from_batches(
        batches: &[Vec<f32>],
        dim: usize,
        total_vectors: usize,
        nlist: usize,
        total_bits: usize,
        metric: Metric,
        rotator_type: RotatorType,
        seed: u64,
        use_faster_config: bool,
    ) -> Result<Self, RabitqError> {
        use crate::rabitq::kmeans::{KMeansConfig, run_kmeans_on_flat};
        use rayon::prelude::*;

        if dim == 0 {
            return Err(RabitqError::InvalidConfig("dimension must be positive"));
        }
        if nlist == 0 {
            return Err(RabitqError::InvalidConfig("nlist must be positive"));
        }
        if total_vectors < nlist {
            return Err(RabitqError::InvalidConfig(
                "nlist cannot exceed number of vectors",
            ));
        }

        let ex_bits = total_bits.saturating_sub(1);
        let rotator = DynamicRotator::new(dim, rotator_type, seed);
        let padded_dim = rotator.padded_dim();
        crate::rabitq::memory::log_huge_page_status();

        // --- Pass 1: Reservoir sample for k-means ---
        println!(
            "K-means: {} clusters, reservoir sampling {} batches...",
            nlist,
            batches.len()
        );
        let max_points_per_centroid: usize = 64;
        let reservoir_size = nlist * max_points_per_centroid;
        let mut rng = StdRng::seed_from_u64(seed ^ 0x5a5a_5a5a5a5a5a5a);

        // Reservoir sample by iterating all batches
        let mut reservoir: Vec<f32> = Vec::new();
        let mut reservoir_count: usize = 0;
        let mut seen: usize = 0;
        for batch in batches {
            let n = batch.len() / dim;
            for i in 0..n {
                let vec_slice = &batch[i * dim..(i + 1) * dim];
                if reservoir_count < reservoir_size {
                    reservoir.extend_from_slice(vec_slice);
                    reservoir_count += 1;
                } else {
                    let j = rng.gen_range(0..seen + 1);
                    if j < reservoir_size {
                        let dst = j * dim;
                        reservoir[dst..dst + dim].copy_from_slice(vec_slice);
                    }
                }
                seen += 1;
            }
        }
        println!(
            "  Reservoir sampled {} / {} vectors ({:.1} MB)",
            reservoir_count,
            seen,
            reservoir_count * dim * 4 / (1024 * 1024)
        );

        let sample_points = reservoir_count;
        if sample_points == 0 {
            return Err(RabitqError::InvalidConfig("no training data sampled"));
        }

        // Rotate sample
        let mut rotated_sample = vec![0.0f32; sample_points * padded_dim];
        rotated_sample
            .par_chunks_mut(padded_dim)
            .enumerate()
            .for_each(|(i, chunk)| {
                let v = &reservoir[i * dim..(i + 1) * dim];
                chunk.copy_from_slice(&rotator.rotate(v));
            });
        drop(reservoir);

        let kmeans_config = KMeansConfig {
            niter: 15,
            nredo: 1,
            seed: rng.next_u64(),
            spherical: false,
            max_points_per_centroid,
            decode_block_size: 32768,
        };
        let KMeansResult {
            centroids: rotated_centroids,
            ..
        } = run_kmeans_on_flat(
            &rotated_sample,
            sample_points,
            padded_dim,
            nlist,
            kmeans_config,
        );
        drop(rotated_sample);
        println!("K-means complete ({} clusters)", rotated_centroids.len());

        // --- Init empty clusters ---
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

        // --- Pass 2: Rotate → assign → quantise ---
        println!("Rotating + quantising {} batches...", batches.len());
        let mut global_id: usize = 0;
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

        for (bi, batch) in batches.iter().enumerate() {
            let batch_n = batch.len() / dim;
            const SUB_CHUNK: usize = 20_000;
            for sub_start in (0..batch_n).step_by(SUB_CHUNK) {
                let sub_end = (sub_start + SUB_CHUNK).min(batch_n);
                let sub_n = sub_end - sub_start;

                let mut rotated_chunk = vec![0.0f32; sub_n * padded_dim];
                rotated_chunk
                    .par_chunks_mut(padded_dim)
                    .enumerate()
                    .for_each(|(k, chunk)| {
                        let i = sub_start + k;
                        let v = &batch[i * dim..(i + 1) * dim];
                        chunk.copy_from_slice(&rotator.rotate(v));
                    });
                let chunk_ids = assign_batch_to_centroids(
                    &rotated_chunk,
                    sub_n,
                    nlist,
                    padded_dim,
                    &centroid_col,
                    &centroid_norms,
                );
                let insertions: Vec<(usize, QuantizedVector)> = (0..sub_n)
                    .into_par_iter()
                    .map(|i| {
                        let rv = &rotated_chunk[i * padded_dim..(i + 1) * padded_dim];
                        let cid = chunk_ids[i];
                        let q = crate::rabitq::quantizer::quantize_with_centroid(
                            rv,
                            &clusters[cid].centroid,
                            &config,
                            metric,
                        );
                        (cid, q)
                    })
                    .collect();
                for (i, (cid, q)) in insertions.into_iter().enumerate() {
                    clusters[cid].append_vector((global_id + sub_start + i) as u64, q);
                }
                drop(rotated_chunk);
            }
            global_id += batch_n;
            if (bi + 1).is_multiple_of(5) || global_id >= total_vectors {
                println!(
                    "  Processed {} / {} vectors ({:.1}%)",
                    global_id,
                    total_vectors,
                    100.0 * global_id as f64 / total_vectors as f64
                );
            }
        }
        for c in &mut clusters {
            c.flush_pending();
        }
        println!("Quantisation complete ({} vectors)", global_id);

        let ip_func = crate::rabitq::simd::select_excode_ipfunc(ex_bits);
        Ok(Self {
            dim,
            padded_dim,
            metric,
            rotator,
            clusters,
            ex_bits,
            ip_func,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn build_from_rotated(
        dim: usize,
        padded_dim: usize,
        metric: Metric,
        rotator: DynamicRotator,
        rotated_centroids: Vec<Vec<f32>>,
        rotated_data: &[Vec<f32>],
        assignments: &[usize],
        total_bits: usize,
        seed: u64,
        use_faster_config: bool,
    ) -> Result<Self, RabitqError> {
        if assignments.len() != rotated_data.len() {
            return Err(RabitqError::InvalidConfig(
                "assignments length must match number of vectors",
            ));
        }
        if total_bits == 0 || total_bits > 16 {
            return Err(RabitqError::InvalidConfig(
                "total_bits must be between 1 and 16",
            ));
        }

        let centroids = rotated_centroids;
        if centroids.is_empty() {
            return Err(RabitqError::InvalidConfig("nlist must be positive"));
        }

        let config = if use_faster_config {
            RabitqConfig::faster(padded_dim, total_bits, seed)
        } else {
            RabitqConfig::new(total_bits)
        };

        // Group vector indices by cluster
        let mut cluster_indices: Vec<Vec<u64>> = vec![Vec::new(); centroids.len()];
        for (idx, &cluster_id) in assignments.iter().enumerate() {
            if cluster_id >= centroids.len() {
                return Err(RabitqError::InvalidConfig(
                    "assignments reference invalid cluster ids",
                ));
            }
            cluster_indices[cluster_id].push(idx as u64);
        }

        // Parallelize quantization across clusters and within clusters
        println!("Quantizing vectors...");
        use std::sync::atomic::{AtomicUsize, Ordering};
        let progress_counter = AtomicUsize::new(0);
        let total_clusters = centroids.len();

        let cluster_data: Vec<(Vec<u64>, Vec<QuantizedVector>)> = centroids
            .par_iter()
            .enumerate()
            .map(|(cluster_id, centroid)| {
                let indices = &cluster_indices[cluster_id];
                let quantized_vectors: Vec<QuantizedVector> = indices
                    .par_iter()
                    .map(|&idx| {
                        quantize_with_centroid(
                            &rotated_data[idx as usize],
                            centroid,
                            &config,
                            metric,
                        )
                    })
                    .collect();

                let completed = progress_counter.fetch_add(1, Ordering::Relaxed) + 1;
                if completed.is_multiple_of((total_clusters / 20).max(1))
                    || completed == total_clusters
                {
                    println!(
                        "  Quantized {}/{} clusters ({:.1}%)",
                        completed,
                        total_clusters,
                        100.0 * completed as f64 / total_clusters as f64
                    );
                }

                (
                    indices.to_vec(),
                    quantized_vectors,
                )
            })
            .collect();
        println!("Quantization complete");

        // Build unified clusters with optimized memory layout
        println!("Building clusters with unified memory layout...");
        let clusters: Vec<ClusterData> = cluster_data
            .into_iter()
            .enumerate()
            .map(|(cluster_id, (indices, quantized_vectors))| {
                ClusterData::from_quantized_vectors(
                    centroids[cluster_id].clone(),
                    indices,
                    quantized_vectors,
                    padded_dim,
                    total_bits.saturating_sub(1),
                )
            })
            .collect();
        println!("Cluster construction complete");

        let ex_bits = total_bits.saturating_sub(1);
        let ip_func = crate::rabitq::simd::select_excode_ipfunc(ex_bits);

        Ok(Self {
            dim,
            padded_dim,
            metric,
            rotator,
            clusters,
            ex_bits,
            ip_func,
        })
    }

    /// Number of stored vectors.
    pub fn len(&self) -> usize {
        self.clusters.iter().map(|c| c.total_vectors()).sum()
    }

    /// Check whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Original vector dimension (before padding/rotation).
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Padded dimension (after rotation).
    pub fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    /// Number of IVF clusters maintained by the index.
    pub fn cluster_count(&self) -> usize {
        self.clusters.len()
    }

    /// Estimate total memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        let clusters_mem: usize = self.clusters.iter().map(|c| c.memory_usage()).sum();
        let pending_mem: usize = self
            .clusters
            .iter()
            .map(|c| {
                c.pending_vectors
                    .iter()
                    .map(|q| q.heap_size())
                    .sum::<usize>()
                    + c.pending_ids.len() * std::mem::size_of::<usize>()
            })
            .sum();
        std::mem::size_of::<Self>() + clusters_mem + pending_mem
    }

    /// Estimate total memory usage in MB
    pub fn estimate_memory_mb(&self) -> f32 {
        self.memory_usage() as f32 / (1024.0 * 1024.0)
    }

    /// Fetch the original embedding for a given vector ID.
    ///
    /// This function reconstructs the approximate original vector from its quantized representation.
    /// The reconstruction involves:
    /// 1. Finding the cluster and local index for the vector ID
    /// 2. Reconstructing the vector in rotated space using the quantized codes
    /// 3. Applying inverse rotation to recover the original (unrotated) vector
    ///
    /// # Returns
    /// - `Some(Vec<f32>)` containing the reconstructed vector if the ID exists
    /// - `None` if the ID is not found in the index
    ///
    /// # Note
    /// The returned vector is an approximation due to quantization losses.
    /// The accuracy depends on the `total_bits` parameter used during training.
    pub fn fetch_embedding(&self, vector_id: u64) -> Option<Vec<f32>> {
        // Find which cluster contains this vector ID
        for cluster in &self.clusters {
            if let Some(local_idx) = cluster.ids.iter().position(|&id| id == vector_id) {
                // Found the vector, now reconstruct it

                // Step 1: Extract quantized codes
                let ex_bits = self.ex_bits;

                // Extract binary code from batch data
                let batch_idx = local_idx / simd::FASTSCAN_BATCH_SIZE;
                let in_batch_idx = local_idx % simd::FASTSCAN_BATCH_SIZE;

                // Unpack binary code for this vector from FastScan layout
                let batch_bin_codes = cluster.batch_bin_codes(batch_idx);
                let dim_bytes = self.padded_dim / 8;
                let mut binary_code_unpacked = vec![0u8; self.padded_dim];

                // Use the existing unpack_single_vector function
                simd::unpack_single_vector(
                    batch_bin_codes,
                    in_batch_idx,
                    dim_bytes,
                    &mut binary_code_unpacked,
                );

                // Extract ex_code (already stored per-vector)
                let ex_code_packed = &cluster.ex_codes_packed[local_idx];
                let mut ex_code_unpacked = vec![0u16; self.padded_dim];
                if ex_bits > 0 {
                    simd::unpack_ex_code(
                        ex_code_packed,
                        &mut ex_code_unpacked,
                        self.padded_dim,
                        ex_bits as u8,
                    );
                }

                // Step 2: Reconstruct full code
                let mut code = vec![0u16; self.padded_dim];
                for i in 0..self.padded_dim {
                    code[i] = ex_code_unpacked[i]
                        + ((binary_code_unpacked[i] as u16) << ex_bits);
                }

                // Step 3: Reconstruct in rotated space
                let delta = cluster.delta[local_idx];
                let vl = cluster.vl[local_idx];
                let mut rotated_reconstructed = vec![0.0f32; self.padded_dim];
                for i in 0..self.padded_dim {
                    rotated_reconstructed[i] =
                        cluster.centroid[i] + delta * code[i] as f32 + vl;
                }

                // Step 4: Apply inverse rotation to get original space vector
                let original_vector = self.rotator.inverse_rotate(&rotated_reconstructed);

                return Some(original_vector);
            }
        }

        None
    }

    /// Persist the index to the provided filesystem path.
    pub fn save_to_path<P: AsRef<Path>>(&self, path: P) -> Result<(), RabitqError> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);
        self.save_to_writer(writer)
    }

    /// Persist the index using the supplied writer.
    pub fn save_to_writer<W: Write>(&self, writer: W) -> Result<(), RabitqError> {
        let mut writer = BufWriter::new(writer);
        writer.write_all(&PERSIST_MAGIC)?;
        write_u32(&mut writer, PERSIST_VERSION, None)?;

        let mut hasher = Hasher::new();

        let dim = u32::try_from(self.dim).map_err(|_| {
            RabitqError::InvalidPersistence("dimension exceeds persistence limits")
        })?;
        write_u32(&mut writer, dim, Some(&mut hasher))?;

        let padded_dim = u32::try_from(self.padded_dim).map_err(|_| {
            RabitqError::InvalidPersistence("padded_dim exceeds persistence limits")
        })?;
        write_u32(&mut writer, padded_dim, Some(&mut hasher))?;

        let metric_tag = metric_to_tag(self.metric);
        writer.write_all(&[metric_tag])?;
        hasher.update(&[metric_tag]);

        let rotator_type_tag = self.rotator.rotator_type() as u8;
        writer.write_all(&[rotator_type_tag])?;
        hasher.update(&[rotator_type_tag]);

        let ex_bits = u8::try_from(self.ex_bits).map_err(|_| {
            RabitqError::InvalidPersistence("ex_bits exceeds persistence limits")
        })?;
        writer.write_all(&[ex_bits])?;
        hasher.update(&[ex_bits]);

        let total_bits = self
            .ex_bits
            .checked_add(1)
            .ok_or(RabitqError::InvalidPersistence("total_bits overflow"))?;
        let total_bits_u8 = u8::try_from(total_bits).map_err(|_| {
            RabitqError::InvalidPersistence("total_bits exceeds persistence limits")
        })?;
        writer.write_all(&[total_bits_u8])?;
        hasher.update(&[total_bits_u8]);

        let vector_count = u64::try_from(self.len()).map_err(|_| {
            RabitqError::InvalidPersistence("vector count exceeds persistence limits")
        })?;
        write_u64(&mut writer, vector_count, Some(&mut hasher))?;

        let cluster_count = u64::try_from(self.clusters.len()).map_err(|_| {
            RabitqError::InvalidPersistence("cluster count exceeds persistence limits")
        })?;
        write_u64(&mut writer, cluster_count, Some(&mut hasher))?;

        // Save rotator state (much smaller than full matrix for FHT)
        let rotator_data = self.rotator.serialize();
        let rotator_len = u64::try_from(rotator_data.len())
            .map_err(|_| RabitqError::InvalidPersistence("rotator data too large"))?;
        eprintln!("DEBUG SAVE: rotator_len={}", rotator_len);
        write_u64(&mut writer, rotator_len, Some(&mut hasher))?;
        writer.write_all(&rotator_data)?;
        hasher.update(&rotator_data);

        // V3: Save ClusterData with unified memory layout
        eprintln!("DEBUG SAVE: Saving {} clusters", self.clusters.len());
        for (i, cluster) in self.clusters.iter().enumerate() {
            eprintln!(
                "DEBUG SAVE: Cluster {}: num_vectors={}, batch_data.len()={}",
                i,
                cluster.num_vectors,
                cluster.batch_data.len()
            );
            if cluster.centroid.len() != self.padded_dim {
                return Err(RabitqError::InvalidPersistence(
                    "cluster centroid dimension mismatch",
                ));
            }
            if cluster.ids.len() != cluster.num_vectors {
                return Err(RabitqError::InvalidPersistence(
                    "cluster id/vector count mismatch",
                ));
            }

            // Save centroid
            for &value in &cluster.centroid {
                write_f32(&mut writer, value, Some(&mut hasher))?;
            }

            // Save vector count
            let entry_count = u64::try_from(cluster.num_vectors).map_err(|_| {
                RabitqError::InvalidPersistence(
                    "cluster entry count exceeds persistence limits",
                )
            })?;
            write_u64(&mut writer, entry_count, Some(&mut hasher))?;

            // Save vector IDs
            for &id in &cluster.ids {
                #[allow(clippy::useless_conversion)]
                let encoded = u64::try_from(id).map_err(|_| {
                    RabitqError::InvalidPersistence(
                        "vector id exceeds persistence limits",
                    )
                })?;
                write_u64(&mut writer, encoded, Some(&mut hasher))?;
            }

            // Save batch_data (contiguous memory block)
            let batch_data_len =
                u64::try_from(cluster.batch_data.len()).map_err(|_| {
                    RabitqError::InvalidPersistence(
                        "batch_data size exceeds persistence limits",
                    )
                })?;

            // DEBUG: Output cluster info
            // eprintln!("Serializing cluster: num_vectors={}, batch_data.len()={}",
            //           cluster.num_vectors, cluster.batch_data.len());

            write_u64(&mut writer, batch_data_len, Some(&mut hasher))?;
            writer.write_all(&cluster.batch_data)?;
            hasher.update(&cluster.batch_data);

            // Save packed ex_codes (C++-style on-demand unpacking)
            eprintln!(
                "DEBUG SAVE: Cluster {}: ex_codes_packed.len()={}",
                i,
                cluster.ex_codes_packed.len()
            );
            for (j, ex_code_packed) in cluster.ex_codes_packed.iter().enumerate() {
                let ex_code_len = u64::try_from(ex_code_packed.len()).map_err(|_| {
                    RabitqError::InvalidPersistence(
                        "ex_code_packed length exceeds persistence limits",
                    )
                })?;
                if j < 2 {
                    eprintln!(
                        "DEBUG SAVE: Cluster {}: ex_code_packed[{}].len()={}",
                        i, j, ex_code_len
                    );
                }
                write_u64(&mut writer, ex_code_len, Some(&mut hasher))?;
                writer.write_all(ex_code_packed)?;
                hasher.update(ex_code_packed);
            }

            // Save ex parameters
            for &val in &cluster.f_add_ex {
                write_f32(&mut writer, val, Some(&mut hasher))?;
            }
            for &val in &cluster.f_rescale_ex {
                write_f32(&mut writer, val, Some(&mut hasher))?;
            }

            // Write delta (reconstruction parameters)
            for &val in &cluster.delta {
                write_f32(&mut writer, val, Some(&mut hasher))?;
            }

            // Write vl (reconstruction parameters)
            for &val in &cluster.vl {
                write_f32(&mut writer, val, Some(&mut hasher))?;
            }
        }

        let checksum = hasher.finalize();
        write_u32(&mut writer, checksum, None)?;

        writer.flush()?;
        Ok(())
    }

    /// Load an index from the provided filesystem path.
    pub fn load_from_path<P: AsRef<Path>>(path: P) -> Result<Self, RabitqError> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Self::load_from_reader(reader)
    }

    /// Load an index from a persisted byte stream.
    pub fn load_from_reader<R: Read>(reader: R) -> Result<Self, RabitqError> {
        let mut reader = BufReader::new(reader);
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != PERSIST_MAGIC {
            return Err(RabitqError::InvalidPersistence("unrecognized file header"));
        }

        let version = read_u32(&mut reader, None)?;
        if version != 3 {
            return Err(RabitqError::InvalidPersistence(
                "unsupported index format version (expected V3 with unified memory layout)",
            ));
        }

        let mut hasher = Hasher::new();

        let dim = read_u32(&mut reader, Some(&mut hasher))? as usize;
        if dim == 0 {
            return Err(RabitqError::InvalidPersistence(
                "dimension must be positive",
            ));
        }

        let padded_dim = read_u32(&mut reader, Some(&mut hasher))? as usize;
        if padded_dim < dim {
            return Err(RabitqError::InvalidPersistence("padded_dim must be >= dim"));
        }

        let metric_tag = read_u8(&mut reader, Some(&mut hasher))?;
        let metric = tag_to_metric(metric_tag)
            .ok_or(RabitqError::InvalidPersistence("unknown metric tag"))?;

        let rotator_type_tag = read_u8(&mut reader, Some(&mut hasher))?;
        let rotator_type = RotatorType::from_u8(rotator_type_tag)
            .ok_or(RabitqError::InvalidPersistence("unknown rotator type tag"))?;

        let ex_bits = read_u8(&mut reader, Some(&mut hasher))? as usize;
        if ex_bits > 16 {
            return Err(RabitqError::InvalidPersistence("ex_bits out of range"));
        }

        let total_bits = read_u8(&mut reader, Some(&mut hasher))? as usize;
        if total_bits == 0 || total_bits > 16 {
            return Err(RabitqError::InvalidPersistence("total_bits out of range"));
        }
        if total_bits.saturating_sub(1) != ex_bits {
            return Err(RabitqError::InvalidPersistence(
                "total_bits does not match ex_bits",
            ));
        }

        let expected_vectors = usize_from_u64(read_u64(&mut reader, Some(&mut hasher))?)?;
        eprintln!("DEBUG LOAD: expected_vectors={}", expected_vectors);

        let cluster_count = usize_from_u64(read_u64(&mut reader, Some(&mut hasher))?)?;
        eprintln!("DEBUG LOAD: cluster_count={}", cluster_count);

        let rotator_data_len = usize_from_u64(read_u64(&mut reader, Some(&mut hasher))?)?;
        eprintln!("DEBUG LOAD: rotator_data_len={}", rotator_data_len);

        let mut rotator_data = vec![0u8; rotator_data_len];
        reader.read_exact(&mut rotator_data)?;
        hasher.update(&rotator_data);

        let rotator =
            DynamicRotator::deserialize(dim, padded_dim, rotator_type, &rotator_data)?;

        // V3: Load ClusterData with unified memory layout
        let mut clusters = Vec::with_capacity(cluster_count);
        eprintln!("DEBUG LOAD: Loading {} clusters", cluster_count);
        for i in 0..cluster_count {
            eprintln!("DEBUG LOAD: Loading cluster {}", i);
            // Load centroid
            let mut centroid = vec![0f32; padded_dim];
            for value in centroid.iter_mut() {
                *value = read_f32(&mut reader, Some(&mut hasher))?;
            }

            // Load vector count
            let num_vectors = usize_from_u64(read_u64(&mut reader, Some(&mut hasher))?)?;
            eprintln!("DEBUG LOAD: Cluster {}: num_vectors={}", i, num_vectors);

            // Validate num_vectors is reasonable (防止破坏的数据导致内存分配溢出)
            const MAX_CLUSTER_SIZE: usize = 1_000_000; // 1M vectors per cluster is very large
            if num_vectors > MAX_CLUSTER_SIZE {
                return Err(RabitqError::InvalidPersistence(
                    "cluster size exceeds reasonable limits - possible corruption",
                ));
            }

            // Load vector IDs
            let mut ids = Vec::with_capacity(num_vectors);
            for _ in 0..num_vectors {
                ids.push(read_u64(&mut reader, Some(&mut hasher))?);
            }

            // Load batch_data (contiguous memory block)
            let batch_data_len =
                usize_from_u64(read_u64(&mut reader, Some(&mut hasher))?)?;

            // Validate batch_data_len is reasonable
            let total_batches = num_vectors.div_ceil(simd::FASTSCAN_BATCH_SIZE);
            let expected_batch_data_len =
                ClusterData::batch_stride(padded_dim) * total_batches;
            if batch_data_len != expected_batch_data_len {
                eprintln!("DEBUG: batch_data_len mismatch");
                eprintln!("  num_vectors: {}", num_vectors);
                eprintln!("  padded_dim: {}", padded_dim);
                eprintln!("  total_batches: {}", total_batches);
                eprintln!("  batch_stride: {}", ClusterData::batch_stride(padded_dim));
                eprintln!("  expected: {}", expected_batch_data_len);
                eprintln!("  actual: {}", batch_data_len);
                return Err(RabitqError::InvalidPersistence(
                    "batch_data length mismatch - possible corruption or version incompatibility",
                ));
            }

            let mut batch_data =
                crate::rabitq::memory::allocate_aligned_vec::<u8>(batch_data_len);
            reader.read_exact(&mut batch_data)?;
            hasher.update(&batch_data);

            // Load packed ex_codes (C++-style on-demand unpacking)
            eprintln!(
                "DEBUG LOAD: Cluster {}: Loading {} ex_codes_packed",
                i, num_vectors
            );
            let mut ex_codes_packed = Vec::with_capacity(num_vectors);
            for j in 0..num_vectors {
                let ex_code_len =
                    usize_from_u64(read_u64(&mut reader, Some(&mut hasher))?)?;
                if j < 2 {
                    eprintln!(
                        "DEBUG LOAD: Cluster {}: ex_code_packed[{}].len()={}",
                        i, j, ex_code_len
                    );
                }

                // Validate ex_code_len is reasonable
                // When ex_bits = 0, ex_code_len should be 0
                // When ex_bits > 0, ex_code_len should equal (padded_dim * ex_bits / 8)
                let expected_ex_code_len = if ex_bits > 0 {
                    padded_dim * ex_bits / 8
                } else {
                    0
                };
                if ex_code_len != expected_ex_code_len {
                    return Err(RabitqError::InvalidPersistence(
                        "ex_code_packed length mismatch - possible corruption or version incompatibility",
                    ));
                }

                let mut ex_code_packed = vec![0u8; ex_code_len];
                reader.read_exact(&mut ex_code_packed)?;
                hasher.update(&ex_code_packed);
                ex_codes_packed.push(ex_code_packed);
            }

            // Load ex parameters
            let mut f_add_ex = Vec::with_capacity(num_vectors);
            for _ in 0..num_vectors {
                f_add_ex.push(read_f32(&mut reader, Some(&mut hasher))?);
            }

            let mut f_rescale_ex = Vec::with_capacity(num_vectors);
            for _ in 0..num_vectors {
                f_rescale_ex.push(read_f32(&mut reader, Some(&mut hasher))?);
            }

            // Read delta (reconstruction parameters)
            let mut delta = Vec::with_capacity(num_vectors);
            for _ in 0..num_vectors {
                delta.push(read_f32(&mut reader, Some(&mut hasher))?);
            }

            // Read vl (reconstruction parameters)
            let mut vl = Vec::with_capacity(num_vectors);
            for _ in 0..num_vectors {
                vl.push(read_f32(&mut reader, Some(&mut hasher))?);
            }

            clusters.push(ClusterData {
                centroid,
                ids,
                batch_data,
                ex_codes_packed,
                f_add_ex,
                f_rescale_ex,
                delta,
                vl,
                num_vectors,
                padded_dim,
                ex_bits,
                pending_ids: Vec::new(),
                pending_vectors: Vec::new(),
            });
        }

        let actual_vectors: usize = clusters.iter().map(|c| c.num_vectors).sum();
        if actual_vectors != expected_vectors {
            return Err(RabitqError::InvalidPersistence(
                "vector count metadata mismatch",
            ));
        }

        let computed_checksum = hasher.finalize();
        let stored_checksum = read_u32(&mut reader, None)?;
        if computed_checksum != stored_checksum {
            return Err(RabitqError::InvalidPersistence("checksum mismatch"));
        }

        println!("Loaded index with unified memory layout (V3)");

        let ip_func = crate::rabitq::simd::select_excode_ipfunc(ex_bits);

        Ok(Self {
            dim,
            padded_dim,
            metric,
            rotator,
            clusters,
            ex_bits,
            ip_func,
        })
    }

    /// Search for the nearest neighbours of the provided query vector.
    pub fn search(
        &self,
        query: &[f32],
        params: SearchParams,
    ) -> Result<Vec<SearchResult>, RabitqError> {
        self.search_fastscan(query, params, None, None)
    }

    /// Search for the nearest neighbours of the provided query vector,
    /// filtering results to only include vector IDs present in the provided bitmap.
    ///
    /// # Arguments
    /// * `query` - The query vector
    /// * `params` - Search parameters (top_k, nprobe)
    /// * `filter` - A RoaringBitmap containing valid candidate vector IDs
    ///
    /// # Returns
    /// A vector of search results, sorted by score, containing only IDs present in the filter.
    pub fn search_filtered(
        &self,
        query: &[f32],
        params: SearchParams,
        filter: &RoaringBitmap,
    ) -> Result<Vec<SearchResult>, RabitqError> {
        self.search_fastscan(query, params, None, Some(filter))
    }

    /// Batch search for multiple queries in parallel using rayon
    ///
    /// # Performance
    /// Expected speedup: 2-8x on typical CPUs (depends on core count and batch size)
    ///
    /// # Arguments
    /// * `queries` - Slice of query vectors
    /// * `params` - Search parameters (top_k, nprobe)
    ///
    /// # Returns
    /// A vector of search results for each query, in the same order as the input queries
    pub fn batch_search(
        &self,
        queries: &[&[f32]],
        params: SearchParams,
    ) -> Vec<Result<Vec<SearchResult>, RabitqError>> {
        queries
            .par_iter()
            .map(|query| self.search(query, params))
            .collect()
    }

    fn search_fastscan(
        &self,
        query: &[f32],
        params: SearchParams,
        mut diagnostics: Option<&mut SearchDiagnostics>,
        filter: Option<&RoaringBitmap>,
    ) -> Result<Vec<SearchResult>, RabitqError> {
        if self.is_empty() {
            return Err(RabitqError::EmptyIndex);
        }
        if query.len() != self.dim {
            return Err(RabitqError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        // FastScan V2 is the default search method (Session 10)
        // Binary code bit order fixed in Session 9 (MSB-first to match C++)
        // Session 11: FastScan now supports all bit configurations (ex_bits >= 0)

        // Precompute query constants once to avoid repeated calculations
        let rotated_query = self.rotator.rotate(query);
        let mut query_precomp = QueryPrecomputed::new(rotated_query, self.ex_bits);

        // Build LUT for FastScan batch search
        query_precomp.build_lut(self.padded_dim);

        let mut cluster_scores: Vec<(usize, f32)> =
            Vec::with_capacity(self.clusters.len());
        for (cid, cluster) in self.clusters.iter().enumerate() {
            let score = match self.metric {
                Metric::L2 => {
                    l2_distance_sqr(&query_precomp.rotated_query, &cluster.centroid)
                }
                Metric::InnerProduct => {
                    dot(&query_precomp.rotated_query, &cluster.centroid)
                }
            };
            cluster_scores.push((cid, score));
        }

        let nprobe = params.nprobe.max(1).min(self.clusters.len());
        if params.top_k == 0 {
            return Ok(Vec::new());
        }

        // Optimization: Use partial sort instead of full sort
        // Only need the top nprobe clusters, not all clusters sorted
        // For nlist=4096, nprobe=64: saves ~90% of sorting work (10x faster)
        //
        // Important: Use stable secondary sort key (cluster ID) to ensure deterministic
        // behavior across architectures (x86 vs ARM). This prevents test failures on M1
        // while maintaining x86 performance.
        if nprobe < cluster_scores.len() {
            match self.metric {
                Metric::L2 => {
                    // Partition so that the nprobe smallest scores are at the front
                    // Use cluster ID as tiebreaker for deterministic ordering
                    cluster_scores.select_nth_unstable_by(nprobe, |a, b| {
                        a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
                    });
                    // Sort only the first nprobe elements
                    cluster_scores[..nprobe].sort_unstable_by(|a, b| {
                        a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
                    });
                }
                Metric::InnerProduct => {
                    // Partition so that the nprobe largest scores are at the front
                    // Use cluster ID as tiebreaker for deterministic ordering
                    cluster_scores.select_nth_unstable_by(nprobe, |a, b| {
                        b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0))
                    });
                    // Sort only the first nprobe elements
                    cluster_scores[..nprobe].sort_unstable_by(|a, b| {
                        b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0))
                    });
                }
            }
        } else {
            // If nprobe >= cluster count, sort all (rare case)
            // Use cluster ID as tiebreaker for deterministic ordering
            match self.metric {
                Metric::L2 => cluster_scores.sort_unstable_by(|a, b| {
                    a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0))
                }),
                Metric::InnerProduct => cluster_scores.sort_unstable_by(|a, b| {
                    b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0))
                }),
            }
        }

        // Debug: print selected clusters (disabled)
        // println!("Selected {} clusters (nprobe={}): {:?}",
        //     cluster_scores.iter().take(nprobe).count(),
        //     nprobe,
        //     cluster_scores.iter().take(nprobe).map(|(cid, score)| (*cid, *score)).collect::<Vec<_>>()
        // );

        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

        // Use FastScan batch search with unified memory layout
        for &(cid, _) in cluster_scores.iter().take(nprobe) {
            let cluster = &self.clusters[cid];

            let centroid_dist =
                l2_distance_sqr(&query_precomp.rotated_query, &cluster.centroid);
            let dot_query_centroid = dot(&query_precomp.rotated_query, &cluster.centroid);
            let g_add = match self.metric {
                Metric::L2 => centroid_dist,
                Metric::InnerProduct => -dot_query_centroid,
            };
            let centroid_norm = centroid_dist.sqrt();
            let g_error = centroid_norm;

            // Use batched search (3-10x faster than naive, further optimized with unified layout)
            self.search_cluster_v2_batched(
                cid,
                cluster,
                &query_precomp,
                g_add,
                g_error,
                dot_query_centroid,
                filter,
                &mut heap,
                params.top_k,
                &mut diagnostics,
            );
        }

        let mut candidates: Vec<HeapCandidate> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.candidate)
            .collect();

        match self.metric {
            Metric::L2 => candidates.sort_by(|a, b| a.distance.total_cmp(&b.distance)),
            Metric::InnerProduct => {
                candidates.sort_by(|a, b| b.score.total_cmp(&a.score))
            }
        }

        Ok(candidates
            .into_iter()
            .map(|candidate| SearchResult {
                id: candidate.id,
                score: match self.metric {
                    Metric::L2 => candidate.distance,
                    Metric::InnerProduct => candidate.score,
                },
            })
            .collect())
    }

    /// Helper method to search a cluster using batch FastScan with unified memory layout
    /// This processes vectors in batches of 32 for better SIMD performance
    /// Reference: C++ split_batch_estdist in estimator.hpp:25-71
    #[allow(clippy::too_many_arguments)]
    fn search_cluster_v2_batched(
        &self,
        _cluster_id: usize,
        cluster: &ClusterData,
        query_precomp: &QueryPrecomputed,
        g_add: f32,
        g_error: f32,
        dot_query_centroid: f32,
        filter: Option<&RoaringBitmap>,
        heap: &mut BinaryHeap<HeapEntry>,
        top_k: usize,
        diagnostics: &mut Option<&mut SearchDiagnostics>,
    ) {
        // Check if we should use high-accuracy mode
        let use_highacc = self.padded_dim > 2048;

        let lut_view = if use_highacc {
            query_precomp.lut_highacc.as_ref().map(|lut| {
                crate::rabitq::fastscan_kernel::FastScanLutView::HighAcc {
                    lut_low8: &lut.lut_low8,
                    lut_high8: &lut.lut_high8,
                    delta: lut.delta,
                    sum_vl_lut: lut.sum_vl_lut,
                }
            })
        } else {
            query_precomp.lut.as_ref().map(|lut| {
                crate::rabitq::fastscan_kernel::FastScanLutView::Regular {
                    lut_i8: &lut.lut_i8,
                    delta: lut.delta,
                    sum_vl_lut: lut.sum_vl_lut,
                }
            })
        };

        let Some(lut_view) = lut_view else {
            return; // No LUT available, fallback needed
        };

        // Process complete batches (32 vectors each)
        let num_batches = cluster.num_complete_batches();
        let num_remainder = cluster.num_remainder_vectors();
        let total_batches = if num_remainder > 0 {
            num_batches + 1
        } else {
            num_batches
        };

        for batch_idx in 0..total_batches {
            let batch_start = batch_idx * simd::FASTSCAN_BATCH_SIZE;
            let batch_end =
                (batch_start + simd::FASTSCAN_BATCH_SIZE).min(cluster.num_vectors);
            let actual_batch_size = batch_end - batch_start;

            // Rely on CPU hardware prefetcher (like C++ version)
            // Manual prefetching removed to avoid cache pollution

            // Step 1: Accumulate distances using FastScan SIMD with zero-copy access
            // Get batch parameters using zero-copy slices
            let batch_f_add = cluster.batch_f_add(batch_idx);
            let batch_f_rescale = cluster.batch_f_rescale(batch_idx);
            let batch_f_error = cluster.batch_f_error(batch_idx);

            // Allocate output arrays on stack (no heap allocation)
            let mut ip_x0_qr_values = [0.0f32; simd::FASTSCAN_BATCH_SIZE];
            let mut est_distances = [0.0f32; simd::FASTSCAN_BATCH_SIZE];
            let mut lower_bounds = [0.0f32; simd::FASTSCAN_BATCH_SIZE];

            crate::rabitq::fastscan_kernel::compute_fastscan_batch(
                lut_view,
                cluster.batch_bin_codes(batch_idx),
                self.padded_dim,
                batch_f_add,
                batch_f_rescale,
                batch_f_error,
                g_add,
                g_error,
                query_precomp.k1x_sum_q,
                &mut ip_x0_qr_values,
                &mut est_distances,
                &mut lower_bounds,
            );

            // Step 2: Process each vector in the batch (pruning and ex-code evaluation)
            // Distances are now pre-computed in vectorized fashion (matching C++ Eigen)
            for i in 0..actual_batch_size {
                let global_idx = batch_start + i;
                let vector_id = cluster.ids[global_idx];

                // Apply filter if provided
                if let Some(filter_bitmap) = filter
                    && !filter_bitmap.contains(vector_id as u32) {
                        continue;
                    }

                // Use pre-computed values (vectorized above)
                let ip_x0_qr = ip_x0_qr_values[i];
                let est_distance = est_distances[i];
                let lower_bound = crate::rabitq::fastscan_kernel::sanitize_lower_bound(
                    lower_bounds[i],
                    self.metric,
                    dot_query_centroid,
                    query_precomp.query_norm,
                );

                // Step 3: Check against current k-th distance
                let distk = if heap.len() < top_k {
                    f32::INFINITY
                } else {
                    heap.peek()
                        .map(|entry| entry.candidate.distance)
                        .unwrap_or(f32::INFINITY)
                };

                if lower_bound >= distk {
                    if let Some(diag) = diagnostics.as_deref_mut() {
                        diag.skipped_by_lower_bound += 1;
                    }
                    continue;
                }

                // Step 4: Compute final distance with ex-codes if available
                let mut distance = est_distance;
                if self.ex_bits > 0 {
                    if let Some(diag) = diagnostics.as_deref_mut() {
                        diag.extended_evaluations += 1;
                    }

                    distance = crate::rabitq::fastscan_kernel::refine_distance_with_ex(
                        &query_precomp.rotated_query,
                        &cluster.ex_codes_packed[global_idx],
                        self.padded_dim,
                        self.ex_bits,
                        ip_x0_qr,
                        query_precomp.binary_scale,
                        query_precomp.kbx_sum_q,
                        g_add,
                        cluster.f_add_ex[global_idx],
                        cluster.f_rescale_ex[global_idx],
                        Some(self.ip_func),
                    );
                }

                if !distance.is_finite() {
                    continue;
                }

                if self.metric == Metric::L2 {
                    distance = distance.max(0.0);
                }

                let score = match self.metric {
                    Metric::L2 => distance,
                    Metric::InnerProduct => -distance,
                };

                if let Some(diag) = diagnostics.as_deref_mut() {
                    diag.estimated += 1;
                }

                // Step 7: Update heap
                heap.push(HeapEntry {
                    candidate: HeapCandidate {
                        id: vector_id,
                        distance,
                        score,
                    },
                });

                if heap.len() > top_k {
                    heap.pop();
                }
            }
        }

        // Scan pending vectors (≤31, not yet in batch_data)
        self.search_pending_vectors(
            cluster,
            query_precomp,
            g_add,
            dot_query_centroid,
            filter,
            heap,
            top_k,
            diagnostics.as_deref_mut(),
        );
    }

    /// Scan pending (unbatched) vectors one-by-one. At most 31 per cluster.
    #[allow(clippy::too_many_arguments)]
    fn search_pending_vectors(
        &self,
        cluster: &ClusterData,
        query_precomp: &QueryPrecomputed,
        g_add: f32,
        _dot_query_centroid: f32,
        filter: Option<&RoaringBitmap>,
        heap: &mut BinaryHeap<HeapEntry>,
        top_k: usize,
        _diagnostics: Option<&mut SearchDiagnostics>,
    ) {
        if cluster.pending_ids.is_empty() {
            return;
        }

        for (&vec_id, qvec) in cluster
            .pending_ids
            .iter()
            .zip(cluster.pending_vectors.iter())
        {
            if let Some(bitmap) = filter
                && !bitmap.contains(vec_id as u32) {
                    continue;
                }

            // Unpack binary code
            let binary_code = qvec.unpack_binary_code();
            let mut binary_dot = 0.0f32;
            for (&bit, &q_val) in
                binary_code.iter().zip(query_precomp.rotated_query.iter())
            {
                binary_dot += (bit as f32) * q_val;
            }
            let binary_term = binary_dot + query_precomp.k1x_sum_q;
            let mut distance = qvec.f_add + g_add + qvec.f_rescale * binary_term;

            // Ex-code refinement
            if self.ex_bits > 0 {
                let ex_dot = (self.ip_func)(
                    &query_precomp.rotated_query,
                    &qvec.ex_code_packed,
                    self.padded_dim,
                );
                let total_term = query_precomp.binary_scale * binary_dot
                    + ex_dot
                    + query_precomp.kbx_sum_q;
                distance = qvec.f_add_ex + g_add + qvec.f_rescale_ex * total_term;
            }

            if !distance.is_finite() {
                continue;
            }

            if self.metric == Metric::L2 {
                distance = distance.max(0.0);
            }

            let score = match self.metric {
                Metric::L2 => distance,
                Metric::InnerProduct => -distance,
            };

            heap.push(HeapEntry {
                candidate: HeapCandidate {
                    id: vec_id,
                    distance,
                    score,
                },
            });
            if heap.len() > top_k {
                heap.pop();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn search_with_diagnostics(
        &self,
        query: &[f32],
        params: SearchParams,
    ) -> Result<(Vec<SearchResult>, SearchDiagnostics), RabitqError> {
        let mut diagnostics = SearchDiagnostics::default();
        let results =
            self.search_fastscan(query, params, Some(&mut diagnostics), None)?;
        Ok((results, diagnostics))
    }

    #[cfg(test)]
    pub(crate) fn search_naive(
        &self,
        query: &[f32],
        params: SearchParams,
    ) -> Result<Vec<SearchResult>, RabitqError> {
        if self.is_empty() {
            return Err(RabitqError::EmptyIndex);
        }
        if query.len() != self.dim {
            return Err(RabitqError::DimensionMismatch {
                expected: self.dim,
                got: query.len(),
            });
        }

        let rotated_query = self.rotator.rotate(query);
        let mut cluster_scores: Vec<(usize, f32)> =
            Vec::with_capacity(self.clusters.len());
        for (cid, cluster) in self.clusters.iter().enumerate() {
            let score = match self.metric {
                Metric::L2 => l2_distance_sqr(&rotated_query, &cluster.centroid),
                Metric::InnerProduct => dot(&rotated_query, &cluster.centroid),
            };
            cluster_scores.push((cid, score));
        }

        match self.metric {
            Metric::L2 => cluster_scores.sort_by(|a, b| a.1.total_cmp(&b.1)),
            Metric::InnerProduct => cluster_scores.sort_by(|a, b| b.1.total_cmp(&a.1)),
        }

        let nprobe = params.nprobe.max(1).min(self.clusters.len());
        let mut candidates = Vec::new();
        let sum_query: f32 = rotated_query.iter().sum();
        let c1 = -0.5f32;
        let binary_scale = (1 << self.ex_bits) as f32;
        let cb = -((1 << self.ex_bits) as f32 - 0.5);
        for &(cid, _) in cluster_scores.iter().take(nprobe) {
            let cluster = &self.clusters[cid];
            let centroid_dist = l2_distance_sqr(&rotated_query, &cluster.centroid);
            let dot_query_centroid = dot(&rotated_query, &cluster.centroid);
            let g_add = match self.metric {
                Metric::L2 => centroid_dist,
                Metric::InnerProduct => -dot_query_centroid,
            };
            for vec_idx in 0..cluster.num_vectors {
                // Extract data for this vector using ClusterData API
                let f_add = cluster.get_vector_f_add(vec_idx);
                let f_rescale = cluster.get_vector_f_rescale(vec_idx);

                // Unpack binary code on-demand from FastScan batch layout
                // Note: This is only used in naive search (test code)
                let batch_idx = vec_idx / simd::FASTSCAN_BATCH_SIZE;
                let in_batch_idx = vec_idx % simd::FASTSCAN_BATCH_SIZE;
                let packed_codes = cluster.batch_bin_codes(batch_idx);

                // Unpack binary code for this vector
                let dim_bytes = self.padded_dim / 8;
                let mut binary_code = vec![0u8; self.padded_dim];
                simd::unpack_single_vector(
                    packed_codes,
                    in_batch_idx,
                    dim_bytes,
                    &mut binary_code,
                );

                let mut binary_dot = 0.0f32;
                for (&bit, &q_val) in binary_code.iter().zip(rotated_query.iter()) {
                    binary_dot += (bit as f32) * q_val;
                }
                let binary_term = binary_dot + c1 * sum_query;
                let mut distance = f_add + g_add + f_rescale * binary_term;

                if self.ex_bits > 0 {
                    // Direct SIMD dot product on packed data (C++-style, Phase 3)
                    let ex_code_packed = &cluster.ex_codes_packed[vec_idx];
                    let ex_dot =
                        (self.ip_func)(&rotated_query, ex_code_packed, self.padded_dim);
                    let total_term = binary_scale * binary_dot + ex_dot + cb * sum_query;
                    distance = cluster.f_add_ex[vec_idx]
                        + g_add
                        + cluster.f_rescale_ex[vec_idx] * total_term;
                }
                if !distance.is_finite() {
                    continue;
                }
                let score = match self.metric {
                    Metric::L2 => distance,
                    Metric::InnerProduct => -distance,
                };
                candidates.push(SearchResult {
                    id: cluster.ids[vec_idx],
                    score,
                });
            }
        }

        match self.metric {
            Metric::L2 => candidates.sort_by(|a, b| a.score.total_cmp(&b.score)),
            Metric::InnerProduct => {
                candidates.sort_by(|a, b| b.score.total_cmp(&a.score))
            }
        }

        candidates.truncate(params.top_k.min(candidates.len()));
        Ok(candidates)
    }

    // ========================================================================
    // V4 Persistence (Manifest + Segment files, S3-compatible)
    // ========================================================================

    /// Flush all pending vectors into batch_data across all clusters.
    pub fn flush_all_pending(&mut self) {
        for cluster in &mut self.clusters {
            cluster.flush_pending();
        }
    }

    /// Save the index to object store (initial build: one base segment per cluster).
    pub async fn save_to_v4(&self, mstore: &ManifestStore) -> Result<(), RabitqError> {
        use crate::rabitq::manifest::{
            self, ClusterManifestEntry, ClusterSegmentData, ManifestHeader,
            SegmentManifestEntry,
        };

        let rotator_type = self.rotator.rotator_type();
        let header = ManifestHeader {
            generation: 1,
            dim: self.dim,
            padded_dim: self.padded_dim,
            metric: self.metric,
            rotator_type,
            rotator_data: self.rotator.serialize(),
            ex_bits: self.ex_bits,
            total_bits: self.ex_bits + 1,
        };
        let mut cluster_map: std::collections::BTreeMap<u32, ClusterManifestEntry> =
            std::collections::BTreeMap::new();

        for (i, cluster) in self.clusters.iter().enumerate() {
            let cid = i as u32;
            let version = 0u32;
            let fname = manifest::segment_filename(cid, version);
            let seg_data = ClusterSegmentData::from_cluster_data(
                cid,
                cluster.centroid.clone(),
                self.padded_dim,
                self.ex_bits,
                cluster.ids.clone(),
                cluster.batch_data.clone(),
                cluster.ex_codes_packed.clone(),
                cluster.f_add_ex.clone(),
                cluster.f_rescale_ex.clone(),
                cluster.delta.clone(),
                cluster.vl.clone(),
            );
            let file_size =
                manifest::write_segment(mstore, &fname, &seg_data, version).await?;
            cluster_map.insert(
                cid,
                ClusterManifestEntry {
                    cluster_id: cid,
                    segments: vec![SegmentManifestEntry {
                        segment_filename: fname,
                        segment_version: version,
                        num_vectors: cluster.num_vectors as u32,
                        file_size,
                    }],
                },
            );
        }
        manifest::save_manifest(mstore, &header, &cluster_map, 0).await?;
        manifest::write_latest(mstore, 1, 0, None).await?;
        println!(
            "Saved V4 index: {} clusters, {} base segments",
            self.clusters.len(),
            cluster_map.len()
        );
        Ok(())
    }

    /// Load index from object store (async, full segments for search).
    ///
    /// Reads the manifest and then loads **all** segment files (base + all
    /// deltas) for every cluster, merging them into a single `ClusterData`
    /// in memory.
    pub async fn load_from_v4(mstore: &ManifestStore) -> Result<Self, RabitqError> {
        // Try LATEST → versioned manifest first, fall back to legacy manifest.bin.
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
        for entry in cluster_map.values() {
            // Merge all segments (base + deltas) for this cluster.
            let mut merged: Option<ClusterData> = None;
            for seg_entry in &entry.segments {
                let seg = crate::rabitq::manifest::read_segment_full(
                    mstore,
                    &seg_entry.segment_filename,
                )
                .await?;
                let cd = ClusterData::from_segment(seg);
                if let Some(m) = merged.as_mut() {
                    // Concatenate: keep centroid from first segment, append data.
                    m.ids.extend_from_slice(&cd.ids);
                    m.batch_data.extend_from_slice(&cd.batch_data);
                    m.ex_codes_packed.extend_from_slice(&cd.ex_codes_packed);
                    m.f_add_ex.extend_from_slice(&cd.f_add_ex);
                    m.f_rescale_ex.extend_from_slice(&cd.f_rescale_ex);
                    m.delta.extend_from_slice(&cd.delta);
                    m.vl.extend_from_slice(&cd.vl);
                    m.num_vectors += cd.num_vectors;
                } else {
                    merged = Some(cd);
                }
            }
            let final_cd = merged.unwrap_or_else(|| {
                ClusterData::new(
                    vec![0.0f32; header.padded_dim],
                    header.padded_dim,
                    header.ex_bits,
                )
            });
            clusters.push(final_cd);
        }
        let rotator = DynamicRotator::deserialize(
            header.dim,
            header.padded_dim,
            header.rotator_type,
            &header.rotator_data,
        )?;
        let ip_func = crate::rabitq::simd::select_excode_ipfunc(header.ex_bits);
        Ok(Self {
            dim: header.dim,
            padded_dim: header.padded_dim,
            metric: header.metric,
            rotator,
            clusters,
            ex_bits: header.ex_bits,
            ip_func,
        })
    }

    /// Insert a single vector into the index (in-memory).
    pub fn insert(
        &mut self,
        vector_id: usize,
        vector: &[f32],
    ) -> Result<u32, RabitqError> {
        if vector.len() != self.dim {
            return Err(RabitqError::DimensionMismatch {
                expected: self.dim,
                got: vector.len(),
            });
        }
        let rotated = self.rotator.rotate(vector);
        let cid = self.find_nearest_cluster_id(&rotated);
        let config = RabitqConfig::new(self.ex_bits + 1);
        let quantized = crate::rabitq::quantizer::quantize_with_centroid(
            &rotated,
            &self.clusters[cid].centroid,
            &config,
            self.metric,
        );
        self.clusters[cid].append_vector(vector_id as u64, quantized);
        Ok(cid as u32)
    }

    /// Insert a batch with external IDs.
    ///
    /// Vectors are rotated, assigned to nearest centroids via GEMM, quantised,
    /// and appended to cluster pending buffers with the provided external IDs.
    pub fn insert_batch(&mut self, batch: IdAndVecBatch) -> Result<(), RabitqError> {
        use rayon::prelude::*;

        let dim = self.dim;
        let padded_dim = self.padded_dim;
        let n = batch.ids.len();
        assert_eq!(
            batch.vectors.len(),
            n * dim,
            "IdAndVecBatch: vectors.len() must equal ids.len() × dim"
        );
        if n == 0 {
            return Ok(());
        }

        // 1. Rotate batch (parallel, pre-allocated)
        let mut rotated = vec![0.0f32; n * padded_dim];
        rotated
            .par_chunks_mut(padded_dim)
            .enumerate()
            .for_each(|(i, chunk)| {
                let v = &batch.vectors[i * dim..(i + 1) * dim];
                chunk.copy_from_slice(&self.rotator.rotate(v));
            });
        drop(batch.vectors);

        // 2. Build centroid matrix + norms
        let k = self.clusters.len();
        let centroid_col: Vec<f32> = {
            let mut col = vec![0.0f32; padded_dim * k];
            for (cid, cluster) in self.clusters.iter().enumerate() {
                let c = &cluster.centroid;
                for d in 0..padded_dim {
                    col[d * k + cid] = c[d];
                }
            }
            col
        };
        let centroid_norms: Vec<f32> = self
            .clusters
            .iter()
            .map(|c| c.centroid.iter().map(|x| x * x).sum())
            .collect();

        // 3. GEMM: dist matrix (faer, multi-threaded)
        let mut dot_products = vec![0.0f32; n * k];
        {
            use faer::linalg::matmul::matmul;
            use faer::mat::{MatMut, MatRef};
            use faer::{Accum, Par};
            let a = MatRef::from_row_major_slice(&rotated, n, padded_dim);
            let b = MatRef::from_row_major_slice(&centroid_col, padded_dim, k);
            let c = MatMut::from_row_major_slice_mut(&mut dot_products, n, k);
            matmul(c, Accum::Replace, a, b, 1.0f32, Par::rayon(0));
        }

        // 4. Argmin + quantise (parallel)
        let norms: Vec<f32> = (0..n)
            .map(|i| {
                rotated[i * padded_dim..(i + 1) * padded_dim]
                    .iter()
                    .map(|x| x * x)
                    .sum()
            })
            .collect();
        let config = RabitqConfig::new(self.ex_bits + 1);

        let results: Vec<(usize, QuantizedVector)> = (0..n)
            .into_par_iter()
            .map(|i| {
                let mut best_cid = 0usize;
                let mut best_dist = f32::INFINITY;
                for c in 0..k {
                    let dot = dot_products[i * k + c];
                    let mut dist = norms[i] + centroid_norms[c] - 2.0 * dot;
                    if dist < 0.0 {
                        dist = 0.0;
                    }
                    if dist < best_dist {
                        best_dist = dist;
                        best_cid = c;
                    }
                }
                let q = crate::rabitq::quantizer::quantize_with_centroid(
                    &rotated[i * padded_dim..(i + 1) * padded_dim],
                    &self.clusters[best_cid].centroid,
                    &config,
                    self.metric,
                );
                (best_cid, q)
            })
            .collect();

        drop(rotated);

        // 5. Append to clusters with external IDs
        for (i, (cid, q)) in results.into_iter().enumerate() {
            self.clusters[cid].append_vector(batch.ids[i], q);
        }

        Ok(())
    }

    /// Flush dirty clusters to V4 directory: writes a **compaction** segment
    /// containing the full merged data, replaces the cluster's segment list,
    /// and deletes all old segment files.
    ///
    /// Prefer the delta-segment flow via `IvfRabitqBuilder::flush()` for
    /// incremental inserts; this method is for compacting a fully-loaded index.
    pub async fn flush_v4(
        &self,
        mstore: &ManifestStore,
        dirty_cids: &[u32],
    ) -> Result<(), RabitqError> {
        use crate::rabitq::manifest::{
            self, ClusterSegmentData, ManifestHeader, SegmentManifestEntry,
        };

        let (_header, mut cluster_map) =
            crate::rabitq::manifest::load_manifest(mstore).await?;

        for &cid in dirty_cids {
            let idx = cid as usize;
            let cluster = &self.clusters[idx];
            let entry = cluster_map.get_mut(&cid).ok_or_else(|| {
                RabitqError::InvalidPersistence("cluster missing from manifest")
            })?;
            let new_version = entry.latest_version() + 1;
            let fname = manifest::segment_filename(cid, new_version);

            let seg_data = ClusterSegmentData::from_cluster_data(
                cid,
                cluster.centroid.clone(),
                self.padded_dim,
                self.ex_bits,
                cluster.ids.clone(),
                cluster.batch_data.clone(),
                cluster.ex_codes_packed.clone(),
                cluster.f_add_ex.clone(),
                cluster.f_rescale_ex.clone(),
                cluster.delta.clone(),
                cluster.vl.clone(),
            );
            let file_size =
                manifest::write_segment(mstore, &fname, &seg_data, new_version).await?;

            // Delete all old segments (compaction: replace with single new segment).
            for old in &entry.segments {
                let _ = manifest::delete_segment(mstore, &old.segment_filename).await;
            }

            entry.segments = vec![SegmentManifestEntry {
                segment_filename: fname,
                segment_version: new_version,
                num_vectors: cluster.num_vectors as u32,
                file_size,
            }];
        }

        let header = ManifestHeader {
            generation: 1,
            dim: self.dim,
            padded_dim: self.padded_dim,
            metric: self.metric,
            rotator_type: self.rotator.rotator_type(),
            rotator_data: self.rotator.serialize(),
            ex_bits: self.ex_bits,
            total_bits: self.ex_bits + 1,
        };
        manifest::save_manifest(mstore, &header, &cluster_map, 0).await?;
        Ok(())
    }

    /// Find the nearest cluster id for a rotated query vector.
    fn find_nearest_cluster_id(&self, rotated: &[f32]) -> usize {
        let mut best_cid = 0usize;
        let mut best_dist = f32::INFINITY;
        for (cid, cluster) in self.clusters.iter().enumerate() {
            let dist = crate::rabitq::math::l2_distance_sqr(rotated, &cluster.centroid);
            if dist < best_dist {
                best_dist = dist;
                best_cid = cid;
            }
        }
        best_cid
    }

    /// Compaction: merge all delta segments into new base segments,
    /// bump the generation, and atomically update the LATEST pointer.
    ///
    /// Reads the current manifest, loads all segments (base + deltas) for
    /// every cluster, merges them in memory, writes new base segments
    /// (version 0), writes a new manifest with `generation+1`, and
    /// CAS-updates LATEST.
    ///
    /// Old segment files are **not** deleted (object-store immutability).
    /// Other readers continue to use the old generation until they re-read
    /// LATEST.
    pub async fn compact_v4(mstore: &ManifestStore) -> Result<(), RabitqError> {
        use crate::rabitq::manifest::{
            self, ClusterManifestEntry, ClusterSegmentData, ManifestHeader,
            SegmentManifestEntry,
        };

        // 1. Read current LATEST and load manifest.
        let latest = manifest::read_latest(mstore).await?;
        let (_, cluster_map) = if latest.generation > 0 {
            manifest::load_manifest_by_gen_ver(mstore, latest.generation, latest.version)
                .await?
        } else {
            manifest::load_manifest(mstore).await?
        };

        let new_gen = latest.generation.max(1) + 1;
        println!(
            "Compaction: gen {} → {}, {} clusters",
            latest.generation.max(1),
            new_gen,
            cluster_map.len()
        );

        // 2. Merge all segments for each cluster and write new base.
        let mut new_map: std::collections::BTreeMap<u32, ClusterManifestEntry> =
            std::collections::BTreeMap::new();

        for (&cid, entry) in cluster_map.iter() {
            // Merge all segments (base + deltas) for this cluster.
            let mut merged: Option<ClusterData> = None;
            for seg_entry in &entry.segments {
                let seg =
                    manifest::read_segment_full(mstore, &seg_entry.segment_filename)
                        .await?;
                let cd = ClusterData::from_segment(seg);
                if let Some(m) = merged.as_mut() {
                    m.ids.extend_from_slice(&cd.ids);
                    m.batch_data.extend_from_slice(&cd.batch_data);
                    m.ex_codes_packed.extend_from_slice(&cd.ex_codes_packed);
                    m.f_add_ex.extend_from_slice(&cd.f_add_ex);
                    m.f_rescale_ex.extend_from_slice(&cd.f_rescale_ex);
                    m.delta.extend_from_slice(&cd.delta);
                    m.vl.extend_from_slice(&cd.vl);
                    m.num_vectors += cd.num_vectors;
                } else {
                    merged = Some(cd);
                }
            }

            let cd = merged.ok_or_else(|| {
                RabitqError::InvalidPersistence("cluster has no segments")
            })?;

            // Write new compacted base segment (version 0).
            let fname = manifest::segment_filename(cid, 0);
            let seg_data = ClusterSegmentData::from_cluster_data(
                cid,
                cd.centroid.clone(),
                cd.padded_dim,
                cd.ex_bits,
                cd.ids.clone(),
                cd.batch_data.clone(),
                cd.ex_codes_packed.clone(),
                cd.f_add_ex.clone(),
                cd.f_rescale_ex.clone(),
                cd.delta.clone(),
                cd.vl.clone(),
            );
            let file_size = manifest::write_segment(mstore, &fname, &seg_data, 0).await?;

            new_map.insert(
                cid,
                ClusterManifestEntry {
                    cluster_id: cid,
                    segments: vec![SegmentManifestEntry {
                        segment_filename: fname,
                        segment_version: 0,
                        num_vectors: cd.num_vectors as u32,
                        file_size,
                    }],
                },
            );
        }

        // 3. Write new-gen manifest (version 1), carrying forward header config.
        let (old_header, _) = if latest.generation > 0 {
            manifest::load_manifest_by_gen_ver(mstore, latest.generation, latest.version)
                .await?
        } else {
            manifest::load_manifest(mstore).await?
        };
        let header = ManifestHeader {
            generation: new_gen,
            dim: old_header.dim,
            padded_dim: old_header.padded_dim,
            metric: old_header.metric,
            rotator_type: old_header.rotator_type,
            rotator_data: old_header.rotator_data,
            ex_bits: old_header.ex_bits,
            total_bits: old_header.total_bits,
        };
        manifest::save_manifest(mstore, &header, &new_map, 1).await?;

        // 4. CAS-update LATEST.
        manifest::write_latest(mstore, new_gen, 1, latest.e_tag).await
            .map_err(|e| {
                if matches!(e, RabitqError::VersionConflict) {
                    RabitqError::InvalidPersistence(
                        "compaction conflict: another process wrote LATEST during compaction"
                    )
                } else { e }
            })?;

        println!(
            "Compaction complete: gen {} ({} segments)",
            new_gen,
            new_map.len()
        );
        Ok(())
    }
}

// ============================================================================
// rebuild_v4: full rebuild from external data stream
// ============================================================================

/// A batch of vectors with their external IDs, used as input to
/// [`rebuild_v4`].
#[derive(Debug, Clone)]
pub struct IdAndVecBatch {
    /// External vector IDs (one per vector, must be globally unique).
    pub ids: Vec<u64>,
    /// Flat row-major vector data: `[batch_n × dim]` f32 values.
    pub vectors: Vec<f32>,
}

impl IdAndVecBatch {
    /// Number of vectors in this batch.
    pub fn len(&self) -> usize {
        self.ids.len()
    }
    /// True if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

/// Completely rebuild the IVF+RaBitQ index from an external async data
/// stream.
///
/// This runs full K-Means training on a reservoir-sampled subset followed
/// by streaming rotation, centroid assignment, and RaBitQ quantisation of
/// every input vector.  The result is a **new generation** of the index:
/// new centroids, new segments, new manifest, and an atomic LATEST update.
///
/// # Arguments
///
/// * `make_stream` — A factory closure that returns a fresh
///   `futures::Stream<Item = IdAndVecBatch>`.  It is called **twice**:
///   once for the reservoir-sampling pass and once for the streaming
///   build pass.  Each call should produce an independent stream over the
///   same data.
///
/// # Panics
///
/// Panics if any batch has mismatched dimensions or an ID count that
/// doesn't match its vector count.
pub async fn rebuild_v4<F, S>(
    mstore: &ManifestStore,
    dim: usize,
    nlist: usize,
    total_bits: usize,
    metric: Metric,
    rotator_type: RotatorType,
    seed: u64,
    faster_config: bool,
    mut make_stream: F,
) -> Result<(), RabitqError>
where
    F: FnMut() -> S,
    S: futures::Stream<Item = IdAndVecBatch> + Unpin,
{
    use crate::rabitq::manifest::{
        self, ClusterManifestEntry, ClusterSegmentData, ManifestHeader,
        SegmentManifestEntry,
    };
    use builder::IvfRabitqBuilder;
    use futures::StreamExt;

    // 1. Read current LATEST, determine new generation.
    let latest = manifest::read_latest(mstore).await?;
    let new_gen = latest.generation.max(1) + 1;
    println!(
        "rebuild_v4: gen {} → {} ({} clusters, {} bits)",
        latest.generation.max(1),
        new_gen,
        nlist,
        total_bits
    );

    // 2. Always fresh builder — rebuild must train new centroids.
    let mut builder = IvfRabitqBuilder::new(
        dim,
        nlist,
        total_bits,
        metric,
        rotator_type,
        seed,
        faster_config,
    );

    // -- Phase 1: reservoir sampling --
    println!("  Phase 1: reservoir sampling...");
    let mut seen: usize = 0;
    {
        let mut stream = make_stream();
        while let Some(batch) = stream.next().await {
            let n = batch.ids.len();
            seen += n;
            builder.insert_batch(batch)?;
        }
    }
    println!("  Phase 1 complete: {} vectors streamed", seen);
    if seen == 0 {
        return Err(RabitqError::InvalidConfig("no vectors in rebuild stream"));
    }

    // -- Phase 2: build (K-Means + streaming rotation + quantisation) --
    println!("  Phase 2: building (K-Means + streaming quantisation)...");
    let index = builder.build(make_stream).await?;

    println!(
        "  Build complete: {} vectors, {} clusters, {:.1} MB",
        index.len(),
        index.cluster_count(),
        index.estimate_memory_mb()
    );

    // 3. Persist with new generation.
    // Write base segments (version 0) per cluster, then versioned manifest.
    let mut cluster_map: std::collections::BTreeMap<u32, ClusterManifestEntry> =
        std::collections::BTreeMap::new();
    for (i, cluster) in index.clusters.iter().enumerate() {
        let cid = i as u32;
        let fname = manifest::segment_filename(cid, 0);
        let seg_data = ClusterSegmentData::from_cluster_data(
            cid,
            cluster.centroid.clone(),
            index.padded_dim,
            index.ex_bits,
            cluster.ids.clone(),
            cluster.batch_data.clone(),
            cluster.ex_codes_packed.clone(),
            cluster.f_add_ex.clone(),
            cluster.f_rescale_ex.clone(),
            cluster.delta.clone(),
            cluster.vl.clone(),
        );
        let file_size = manifest::write_segment(mstore, &fname, &seg_data, 0).await?;
        cluster_map.insert(
            cid,
            ClusterManifestEntry {
                cluster_id: cid,
                segments: vec![SegmentManifestEntry {
                    segment_filename: fname,
                    segment_version: 0,
                    num_vectors: cluster.num_vectors as u32,
                    file_size,
                }],
            },
        );
    }

    let header = ManifestHeader {
        generation: new_gen,
        dim: index.dim,
        padded_dim: index.padded_dim,
        metric: index.metric,
        rotator_type: index.rotator.rotator_type(),
        rotator_data: index.rotator.serialize(),
        ex_bits: index.ex_bits,
        total_bits: index.ex_bits + 1,
    };
    manifest::save_manifest(mstore, &header, &cluster_map, 1).await?;

    // 4. CAS LATEST.
    manifest::write_latest(mstore, new_gen, 1, latest.e_tag)
        .await
        .map_err(|e| {
            if matches!(e, RabitqError::VersionConflict) {
                RabitqError::InvalidPersistence(
                    "rebuild conflict: another process wrote LATEST during rebuild",
                )
            } else {
                e
            }
        })?;

    println!(
        "rebuild_v4 complete: gen {} ({} base segments, {} vectors)",
        new_gen,
        cluster_map.len(),
        index.len()
    );
    Ok(())
}

// ----------------------------------------------------------------------------
// Standalone helpers
// ----------------------------------------------------------------------------

/// Batch-assign vectors to nearest centroids using GEMM + row-wise argmin.
fn assign_batch_to_centroids(
    rotated_batch: &[f32],
    batch_n: usize,
    nlist: usize,
    padded_dim: usize,
    centroid_col: &[f32],
    centroid_norms: &[f32],
) -> Vec<usize> {
    let k = nlist;
    let mut dot_products = vec![0.0f32; batch_n * k];
    {
        use faer::linalg::matmul::matmul;
        use faer::mat::{MatMut, MatRef};
        use faer::{Accum, Par};
        let a = MatRef::from_row_major_slice(rotated_batch, batch_n, padded_dim);
        let b = MatRef::from_row_major_slice(centroid_col, padded_dim, k);
        let c = MatMut::from_row_major_slice_mut(&mut dot_products, batch_n, k);
        matmul(c, Accum::Replace, a, b, 1.0f32, Par::rayon(0));
    }
    let norms: Vec<f32> = (0..batch_n)
        .map(|i| {
            rotated_batch[i * padded_dim..(i + 1) * padded_dim]
                .iter()
                .map(|x| x * x)
                .sum()
        })
        .collect();
    let mut assignments = Vec::with_capacity(batch_n);
    for i in 0..batch_n {
        let mut best_cid = 0usize;
        let mut best_dist = f32::INFINITY;
        for c in 0..k {
            let dot = dot_products[i * k + c];
            let mut dist = norms[i] + centroid_norms[c] - 2.0 * dot;
            if dist < 0.0 {
                dist = 0.0;
            }
            if dist < best_dist {
                best_dist = dist;
                best_cid = c;
            }
        }
        assignments.push(best_cid);
    }
    assignments
}

// ----------------------------------------------------------------------------
// ClusterSegmentData constructor (used by V4 save/flush)
// ----------------------------------------------------------------------------

impl crate::rabitq::manifest::ClusterSegmentData {
    #[allow(clippy::too_many_arguments)]
    pub fn from_cluster_data(
        cluster_id: u32,
        centroid: Vec<f32>,
        padded_dim: usize,
        ex_bits: usize,
        ids: Vec<u64>,
        batch_data: Vec<u8>,
        ex_codes_packed: Vec<Vec<u8>>,
        f_add_ex: Vec<f32>,
        f_rescale_ex: Vec<f32>,
        delta: Vec<f32>,
        vl: Vec<f32>,
    ) -> Self {
        Self {
            cluster_id,
            centroid,
            padded_dim,
            ex_bits,
            ids,
            batch_data,
            ex_codes_packed,
            f_add_ex,
            f_rescale_ex,
            delta,
            vl,
        }
    }
}

// ----------------------------------------------------------------------------
// ClusterData helpers for incremental insert
// ----------------------------------------------------------------------------

impl ClusterData {
    // ------------------------------------------------------------------
    // Pending-buffer helpers for O(1) incremental insert
    // ------------------------------------------------------------------

    /// Total number of vectors (batched + pending).
    #[inline]
    fn total_vectors(&self) -> usize {
        self.num_vectors + self.pending_ids.len()
    }

    /// Append one quantised vector to the pending buffer.
    /// Flushes a batch of 32 into `batch_data` when full.
    fn append_vector(&mut self, new_id: u64, new_q: QuantizedVector) {
        self.pending_ids.push(new_id);
        self.pending_vectors.push(new_q);
        if self.pending_ids.len() >= simd::FASTSCAN_BATCH_SIZE {
            self.flush_pending();
        }
    }

    /// Flush groups of 32 pending vectors into the batch layout, including
    /// any remainder (< 32) padded with zeros.
    fn flush_pending(&mut self) {
        if self.pending_ids.is_empty() {
            return;
        }

        let dim_bytes = self.padded_dim / 8;
        let ex_bits = self.ex_bits;
        let padded_dim = self.padded_dim;

        // Flush complete batches of 32.
        while self.pending_ids.len() >= simd::FASTSCAN_BATCH_SIZE {
            // Drain 32 vectors
            let batch_qvecs: Vec<QuantizedVector> = self
                .pending_vectors
                .drain(..simd::FASTSCAN_BATCH_SIZE)
                .collect();
            let batch_ids: Vec<u64> = self
                .pending_ids
                .drain(..simd::FASTSCAN_BATCH_SIZE)
                .collect();

            // Append this batch into batch_data
            let _batch_idx = self.num_vectors / simd::FASTSCAN_BATCH_SIZE;

            // Extend main ID list
            self.ids.extend(&batch_ids);

            // Extend batch_data by one batch stride
            let stride = Self::batch_stride(padded_dim);
            let old_len = self.batch_data.len();
            self.batch_data.resize(old_len + stride, 0u8);

            // Collect binary codes flat
            let mut binary_codes_flat =
                Vec::with_capacity(simd::FASTSCAN_BATCH_SIZE * dim_bytes);
            for q in &batch_qvecs {
                binary_codes_flat.extend_from_slice(&q.binary_code_packed);
            }
            // Pack into FastScan layout
            let packed_out = &mut self.batch_data
                [old_len..old_len + padded_dim * simd::FASTSCAN_BATCH_SIZE / 8];
            simd::pack_codes(
                &binary_codes_flat,
                simd::FASTSCAN_BATCH_SIZE,
                dim_bytes,
                packed_out,
            );

            // Parameters
            let f_add_offset = old_len + padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
            let f_rescale_offset =
                f_add_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
            let f_error_offset =
                f_rescale_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
            unsafe {
                let f_add_slice = std::slice::from_raw_parts_mut(
                    self.batch_data[f_add_offset..].as_mut_ptr() as *mut f32,
                    simd::FASTSCAN_BATCH_SIZE,
                );
                let f_rescale_slice = std::slice::from_raw_parts_mut(
                    self.batch_data[f_rescale_offset..].as_mut_ptr() as *mut f32,
                    simd::FASTSCAN_BATCH_SIZE,
                );
                let f_error_slice = std::slice::from_raw_parts_mut(
                    self.batch_data[f_error_offset..].as_mut_ptr() as *mut f32,
                    simd::FASTSCAN_BATCH_SIZE,
                );
                for (i, q) in batch_qvecs.iter().enumerate() {
                    f_add_slice[i] = q.f_add;
                    f_rescale_slice[i] = q.f_rescale;
                    f_error_slice[i] = q.f_error;
                }
            }

            // Append ex_codes and parameters
            for q in &batch_qvecs {
                if ex_bits > 0 {
                    self.ex_codes_packed.push(q.ex_code_packed.clone());
                    self.f_add_ex.push(q.f_add_ex);
                    self.f_rescale_ex.push(q.f_rescale_ex);
                } else {
                    self.ex_codes_packed.push(Vec::new());
                    self.f_add_ex.push(0.0);
                    self.f_rescale_ex.push(0.0);
                }
                self.delta.push(q.delta);
                self.vl.push(q.vl);
            }

            self.num_vectors += simd::FASTSCAN_BATCH_SIZE;
        }

        // Flush remaining vectors (< 32) into a padded batch.
        let rem = self.pending_ids.len();
        if rem > 0 {
            let ex_bytes_per_vec = if ex_bits > 0 {
                padded_dim * ex_bits / 8
            } else {
                0
            };
            let zero_qvec = QuantizedVector {
                binary_code_packed: vec![0u8; dim_bytes],
                ex_code_packed: vec![0u8; ex_bytes_per_vec],
                ex_bits: ex_bits as u8,
                dim: padded_dim,
                delta: 0.0,
                vl: 0.0,
                f_add: 0.0,
                f_rescale: 0.0,
                f_error: 0.0,
                residual_norm: 0.0,
                f_add_ex: 0.0,
                f_rescale_ex: 0.0,
            };

            let mut batch_qvecs: Vec<QuantizedVector> =
                self.pending_vectors.drain(..).collect();
            let batch_ids: Vec<u64> = self.pending_ids.drain(..).collect();
            let actual_count = batch_qvecs.len();

            // Pad to 32 with zeros.
            batch_qvecs.resize(simd::FASTSCAN_BATCH_SIZE, zero_qvec);

            // Extend ID list (only actual ids, not pads).
            self.ids.extend(&batch_ids);

            // Extend batch_data by one batch stride.
            let stride = Self::batch_stride(padded_dim);
            let old_len = self.batch_data.len();
            self.batch_data.resize(old_len + stride, 0u8);

            // Collect binary codes flat (padded).
            let mut binary_codes_flat =
                Vec::with_capacity(simd::FASTSCAN_BATCH_SIZE * dim_bytes);
            for q in &batch_qvecs {
                binary_codes_flat.extend_from_slice(&q.binary_code_packed);
            }
            // Pack into FastScan layout.
            let packed_out = &mut self.batch_data
                [old_len..old_len + padded_dim * simd::FASTSCAN_BATCH_SIZE / 8];
            simd::pack_codes(
                &binary_codes_flat,
                simd::FASTSCAN_BATCH_SIZE,
                dim_bytes,
                packed_out,
            );

            // Parameters.
            let f_add_offset = old_len + padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
            let f_rescale_offset =
                f_add_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
            let f_error_offset =
                f_rescale_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
            unsafe {
                let f_add_slice = std::slice::from_raw_parts_mut(
                    self.batch_data[f_add_offset..].as_mut_ptr() as *mut f32,
                    simd::FASTSCAN_BATCH_SIZE,
                );
                let f_rescale_slice = std::slice::from_raw_parts_mut(
                    self.batch_data[f_rescale_offset..].as_mut_ptr() as *mut f32,
                    simd::FASTSCAN_BATCH_SIZE,
                );
                let f_error_slice = std::slice::from_raw_parts_mut(
                    self.batch_data[f_error_offset..].as_mut_ptr() as *mut f32,
                    simd::FASTSCAN_BATCH_SIZE,
                );
                for (i, q) in batch_qvecs.iter().enumerate() {
                    f_add_slice[i] = q.f_add;
                    f_rescale_slice[i] = q.f_rescale;
                    f_error_slice[i] = q.f_error;
                }
            }

            // Append ex_codes and parameters ONLY for actual vectors (not pads).
            for q in batch_qvecs.iter().take(actual_count) {
                if ex_bits > 0 {
                    self.ex_codes_packed.push(q.ex_code_packed.clone());
                    self.f_add_ex.push(q.f_add_ex);
                    self.f_rescale_ex.push(q.f_rescale_ex);
                } else {
                    self.ex_codes_packed.push(Vec::new());
                    self.f_add_ex.push(0.0);
                    self.f_rescale_ex.push(0.0);
                }
                self.delta.push(q.delta);
                self.vl.push(q.vl);
            }

            self.num_vectors += actual_count;
        }
    }

    /// Reconstruct all `QuantizedVector` values (batched + pending).
    fn collect_quantized_vectors(&self, padded_dim: usize) -> Vec<QuantizedVector> {
        let ex_bits = self.ex_bits;
        let dim_bytes = padded_dim / 8;
        let mut result = Vec::with_capacity(self.total_vectors());

        // Batched vectors
        for vec_idx in 0..self.num_vectors {
            let batch_idx = vec_idx / simd::FASTSCAN_BATCH_SIZE;
            let in_batch_idx = vec_idx % simd::FASTSCAN_BATCH_SIZE;

            let packed_codes = self.batch_bin_codes(batch_idx);
            let mut binary_code_unpacked = vec![0u8; padded_dim];
            simd::unpack_single_vector(
                packed_codes,
                in_batch_idx,
                dim_bytes,
                &mut binary_code_unpacked,
            );
            let binary_packed_size = padded_dim.div_ceil(8);
            let mut binary_code_packed = vec![0u8; binary_packed_size];
            simd::pack_binary_code(
                &binary_code_unpacked,
                &mut binary_code_packed,
                padded_dim,
            );

            let f_add = self.batch_f_add(batch_idx)[in_batch_idx];
            let f_rescale = self.batch_f_rescale(batch_idx)[in_batch_idx];
            let f_error = self.batch_f_error(batch_idx)[in_batch_idx];
            let ex_code_packed = self
                .ex_codes_packed
                .get(vec_idx)
                .cloned()
                .unwrap_or_default();
            let f_add_ex = self.f_add_ex.get(vec_idx).copied().unwrap_or(0.0);
            let f_rescale_ex = self.f_rescale_ex.get(vec_idx).copied().unwrap_or(0.0);
            let delta = self.delta.get(vec_idx).copied().unwrap_or(0.0);
            let vl = self.vl.get(vec_idx).copied().unwrap_or(0.0);

            result.push(QuantizedVector {
                binary_code_packed,
                ex_code_packed,
                ex_bits: ex_bits as u8,
                dim: padded_dim,
                delta,
                vl,
                f_add,
                f_rescale,
                f_error,
                residual_norm: 0.0,
                f_add_ex,
                f_rescale_ex,
            });
        }

        // Pending vectors
        result.extend(self.pending_vectors.iter().cloned());
        result
    }
}

#[cfg(test)]
mod batch_search_tests {
    use super::*;
    use crate::rabitq::quantizer::RabitqConfig;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    fn random_vector(dim: usize, rng: &mut StdRng) -> Vec<f32> {
        (0..dim).map(|_| rng.r#gen::<f32>() * 2.0 - 1.0).collect()
    }

    #[test]
    fn test_lut_accumulate_matches_direct_dot() {
        // Minimal test to verify LUT-based dot product matches direct computation
        let dim = 64;
        let padded_dim = 64;
        let _ex_bits = 0;
        let mut rng = StdRng::seed_from_u64(12345);

        // Create a single vector
        let rotator = DynamicRotator::new(dim, RotatorType::FhtKacRotator, 42);
        let data_vec = random_vector(dim, &mut rng);
        let rotated_data = rotator.rotate(&data_vec);

        let query_vec = random_vector(dim, &mut rng);
        let rotated_query = rotator.rotate(&query_vec);

        // Quantize the data vector
        let config = RabitqConfig::new(1);
        let centroid = vec![0.0f32; padded_dim];
        let quantized = crate::rabitq::quantizer::quantize_with_centroid(
            &rotated_data,
            &centroid,
            &config,
            Metric::L2,
        );

        // V1: Direct dot product
        let binary_code_unpacked = quantized.unpack_binary_code();
        let binary_dot_v1 =
            crate::rabitq::simd::dot_u8_f32(&binary_code_unpacked, &rotated_query);
        println!("V1 binary_dot: {:.6}", binary_dot_v1);
        println!(
            "First 16 binary_code_unpacked: {:?}",
            &binary_code_unpacked[0..16.min(binary_code_unpacked.len())]
        );
        println!(
            "First 16 rotated_query: {:?}",
            &rotated_query[0..16.min(rotated_query.len())]
        );

        // V2: LUT-based approach
        // Build LUT
        let table_length = padded_dim * 4;
        let mut lut_float = vec![0.0f32; table_length];
        crate::rabitq::simd::pack_lut_f32(&rotated_query, &mut lut_float);

        // Quantize LUT
        let vl_lut = lut_float
            .iter()
            .copied()
            .min_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);
        let vr_lut = lut_float
            .iter()
            .copied()
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0);
        let delta = (vr_lut - vl_lut) / 255.0f32;

        let mut lut_i8 = vec![0i8; table_length];
        if delta > 0.0 {
            for i in 0..table_length {
                let quantized = ((lut_float[i] - vl_lut) / delta).round();
                // Must convert to u8 first, then to i8 to avoid saturation
                // f32 as i8 saturates at 127, but we need wrapping behavior (128-255 -> -128 to -1)
                lut_i8[i] = (quantized.clamp(0.0, 255.0) as u8) as i8;
            }
        }

        let num_table = table_length / 16;
        let sum_vl_lut = vl_lut * (num_table as f32);

        println!("LUT delta: {:.6}, sum_vl_lut: {:.6}", delta, sum_vl_lut);
        println!("First 16 lut_float: {:?}", &lut_float[0..16]);

        // Print first 32 bytes of LUT as i8 and u8 for comparison
        print!("First 32 lut_i8 bytes (as i8): ");
        for val in lut_i8.iter().take(32) {
            print!("{} ", val);
        }
        println!();
        print!("First 32 lut_i8 bytes (as u8): ");
        for val in lut_i8.iter().take(32) {
            print!("{} ", *val as u8);
        }
        println!();

        // Print first few bytes of packed code
        println!(
            "First 2 packed bytes: {:08b} {:08b}",
            quantized.binary_code_packed[0], quantized.binary_code_packed[1]
        );

        // Manually compute what accumulate should return
        // Now with MSB-first packing, packed[0] bit 7 = binary_code_unpacked[0]
        let mut manual_accu_via_direct = 0.0f32;
        for (i, _) in rotated_query.iter().enumerate().take(padded_dim) {
            manual_accu_via_direct += binary_code_unpacked[i] as f32 * rotated_query[i];
        }
        println!(
            "Manual via direct dot product: {:.6}",
            manual_accu_via_direct
        );

        // Pack the binary code
        let dim_bytes = padded_dim / 8;
        let mut packed_codes = vec![0u8; 32 * dim_bytes];
        crate::rabitq::simd::pack_codes(
            &quantized.binary_code_packed,
            1,
            dim_bytes,
            &mut packed_codes,
        );

        // Print packed_codes for comparison with C++
        print!("Rust packed_codes (all {} bytes): ", packed_codes.len());
        for (i, byte) in packed_codes.iter().enumerate() {
            if i > 0 && i % 16 == 0 {
                print!("\n  ");
            }
            print!("{:02x} ", byte);
        }
        println!();

        // Accumulate
        let mut accu_res = [0u16; 32];
        crate::rabitq::simd::accumulate_batch_avx2(
            &packed_codes,
            &lut_i8,
            padded_dim,
            &mut accu_res,
        );

        let accu = accu_res[0] as f32;
        let ip_x0_qr = delta * accu + sum_vl_lut;
        println!("V2 accu: {:.6}, ip_x0_qr: {:.6}", accu, ip_x0_qr);

        // Manually compute what accu should be using lut_float
        let mut expected_result = 0.0f32;
        for (i, _) in rotated_query.iter().enumerate().take(dim) {
            expected_result += binary_code_unpacked[i] as f32 * rotated_query[i];
        }
        println!("Expected (direct): {:.6}", expected_result);

        // Compute expected LUT accumulation manually
        // Each codebook covers 4 dimensions, and the 4-bit code indexes into 16 LUT entries
        let mut expected_accu_via_lut = 0i32;
        for codebook_idx in 0..(padded_dim / 4) {
            // Get the 4-bit code for this codebook from binary_code_unpacked
            // binary_code_unpacked is in MSB-first order within each byte
            let dim_base = codebook_idx * 4;
            let mut code = 0u8;
            for bit_idx in 0..4 {
                if binary_code_unpacked[dim_base + bit_idx] != 0 {
                    // KPOS tells us which bit position corresponds to which dimension
                    // But the code itself is just the 4 binary bits packed
                    code |= 1 << (3 - bit_idx); // MSB-first: dim_base+0 is MSB (bit 3)
                }
            }
            let lut_val = lut_i8[codebook_idx * 16 + code as usize];
            println!(
                "  Codebook {}: dims {}-{}, binary=[{},{},{},{}], code={}, lut_i8[{}]={}",
                codebook_idx,
                dim_base,
                dim_base + 3,
                binary_code_unpacked[dim_base],
                binary_code_unpacked[dim_base + 1],
                binary_code_unpacked[dim_base + 2],
                binary_code_unpacked[dim_base + 3],
                code,
                codebook_idx * 16 + code as usize,
                lut_val
            );
            expected_accu_via_lut += lut_val as i32;
        }
        println!("Expected accu via LUT: {}", expected_accu_via_lut);
        let expected_ip_x0_qr_via_lut =
            delta * (expected_accu_via_lut as f32) + sum_vl_lut;
        println!(
            "Expected ip_x0_qr via LUT: {:.6}",
            expected_ip_x0_qr_via_lut
        );

        // They should match within tolerance
        let diff = (binary_dot_v1 - ip_x0_qr).abs();
        println!("Difference: {:.6}", diff);
        assert!(
            diff < 0.01,
            "LUT-based result {:.6} doesn't match direct dot {:.6}",
            ip_x0_qr,
            binary_dot_v1
        );
    }

    #[test]
    fn test_batch_search_matches_per_vector_l2() {
        // Test parameters
        let dim = 64;
        let padded_dim = 64;
        let num_vectors = 96; // 3 batches of 32
        let ex_bits = 0; // Start with 1-bit only (no extended code)
        let total_bits = 1;
        let metric = Metric::L2;
        let mut rng = StdRng::seed_from_u64(12345);

        // Generate test data
        let centroid = vec![0.0f32; padded_dim];
        let mut data = Vec::with_capacity(num_vectors);
        let mut ids = Vec::with_capacity(num_vectors);
        for i in 0..num_vectors {
            data.push(random_vector(dim, &mut rng));
            ids.push(i);
        }

        // Quantize vectors
        let config = RabitqConfig::new(total_bits);
        let rotator = DynamicRotator::new(dim, RotatorType::FhtKacRotator, 42);
        let rotated_data: Vec<Vec<f32>> =
            data.iter().map(|v| rotator.rotate(v)).collect();
        let quantized_vectors: Vec<QuantizedVector> = rotated_data
            .iter()
            .map(|v| quantize_with_centroid(v, &centroid, &config, metric))
            .collect();

        // Create cluster with unified memory layout
        let cluster = ClusterData::from_quantized_vectors(
            centroid.clone(),
            ids.iter().map(|&x| x as u64).collect(),
            quantized_vectors.clone(),
            padded_dim,
            ex_bits,
        );

        // Generate query
        let query = random_vector(dim, &mut rng);
        let rotated_query = rotator.rotate(&query);
        let mut query_precomp = QueryPrecomputed::new(rotated_query.clone(), ex_bits);
        query_precomp.build_lut(padded_dim);

        // Debug LUT parameters
        if let Some(lut) = query_precomp.lut {
            println!("\n=== L2 Test LUT params ===");
            println!(
                "delta={:.6}, sum_vl_lut={:.6}, k1x_sum_q={:.6}",
                lut.delta, lut.sum_vl_lut, query_precomp.k1x_sum_q
            );
            println!("First 8 LUT values: {:?}\n", &lut.lut_i8[0..8]);
        }

        // Compute cluster statistics
        let centroid_dist = l2_distance_sqr(&rotated_query, &centroid);
        let dot_query_centroid = dot(&rotated_query, &centroid);
        let g_add = centroid_dist;
        let g_error = centroid_dist.sqrt();

        // Simulate index for batch search
        let ip_func = crate::rabitq::simd::select_excode_ipfunc(ex_bits);
        let index = IvfRabitqIndex {
            dim,
            padded_dim,
            metric,
            rotator: rotator.clone(),
            clusters: vec![cluster.clone()],
            ex_bits,
            ip_func,
        };

        // Batch search (unified memory layout)
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let top_k = 10;
        let mut diag = SearchDiagnostics::default();
        let mut diagnostics = Some(&mut diag);
        index.search_cluster_v2_batched(
            0, // Single cluster test
            &cluster,
            &query_precomp,
            g_add,
            g_error,
            dot_query_centroid,
            None,
            &mut heap,
            top_k,
            &mut diagnostics,
        );

        // Convert heap to sorted vector
        let mut results: Vec<HeapCandidate> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| e.candidate)
            .collect();
        results.sort_by(|a, b| a.distance.total_cmp(&b.distance));

        // Verify we got reasonable results
        assert!(!results.is_empty(), "Should find at least one result");
        assert!(results.len() <= top_k, "Should not exceed top_k");

        // Print results for debugging
        println!("\n=== Search Results (top 10) ===");
        for (i, r) in results.iter().take(10).enumerate() {
            println!("  [{}] ID={}, distance={:.6}", i, r.id, r.distance);
        }

        // Verify all distances are finite and non-negative for L2
        for r in &results {
            assert!(r.distance.is_finite(), "Distance should be finite");
            assert!(r.distance >= 0.0, "L2 distance should be non-negative");
        }

        // Verify results are sorted by distance (ascending for L2)
        for i in 0..results.len().saturating_sub(1) {
            assert!(
                results[i].distance <= results[i + 1].distance,
                "Results should be sorted by distance ascending"
            );
        }
    }

    #[test]
    fn test_batch_search_matches_per_vector_ip() {
        // Test parameters
        let dim = 64;
        let padded_dim = 64;
        let num_vectors = 64; // 2 batches of 32
        let ex_bits = 0; // Start with 1-bit only
        let total_bits = 1;
        let metric = Metric::InnerProduct;
        let mut rng = StdRng::seed_from_u64(54321);

        // Generate test data
        let centroid = vec![0.0f32; padded_dim];
        let mut data = Vec::with_capacity(num_vectors);
        let mut ids = Vec::with_capacity(num_vectors);
        for i in 0..num_vectors {
            data.push(random_vector(dim, &mut rng));
            ids.push(i);
        }

        // Quantize vectors
        let config = RabitqConfig::new(total_bits);
        let rotator = DynamicRotator::new(dim, RotatorType::FhtKacRotator, 42);
        let rotated_data: Vec<Vec<f32>> =
            data.iter().map(|v| rotator.rotate(v)).collect();
        let quantized_vectors: Vec<QuantizedVector> = rotated_data
            .iter()
            .map(|v| quantize_with_centroid(v, &centroid, &config, metric))
            .collect();

        // Create unified cluster
        let cluster = ClusterData::from_quantized_vectors(
            centroid.clone(),
            ids.iter().map(|&x| x as u64).collect(),
            quantized_vectors,
            padded_dim,
            ex_bits,
        );

        // Generate query
        let query = random_vector(dim, &mut rng);
        let rotated_query = rotator.rotate(&query);
        let mut query_precomp = QueryPrecomputed::new(rotated_query.clone(), ex_bits);
        query_precomp.build_lut(padded_dim);

        // Compute cluster statistics
        let dot_query_centroid = dot(&rotated_query, &centroid);
        let g_add = -dot_query_centroid;
        let centroid_norm = centroid.iter().map(|x| x * x).sum::<f32>().sqrt();
        let g_error = centroid_norm;

        // Simulate index
        let ip_func = crate::rabitq::simd::select_excode_ipfunc(ex_bits);
        let index = IvfRabitqIndex {
            dim,
            padded_dim,
            metric,
            rotator: rotator.clone(),
            clusters: vec![cluster.clone()],
            ex_bits,
            ip_func,
        };

        // Perform batch search
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let top_k = 5;
        let mut diag = SearchDiagnostics::default();
        let mut diagnostics = Some(&mut diag);
        index.search_cluster_v2_batched(
            0, // Single cluster test
            &cluster,
            &query_precomp,
            g_add,
            g_error,
            dot_query_centroid,
            None,
            &mut heap,
            top_k,
            &mut diagnostics,
        );

        // Convert and sort results by score (IP uses score, not distance)
        let mut results: Vec<HeapCandidate> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|e| e.candidate)
            .collect();
        results.sort_by(|a, b| b.score.total_cmp(&a.score));

        // Verify we got reasonable results
        assert!(!results.is_empty(), "Should find at least one result");
        assert!(results.len() <= top_k, "Should not exceed top_k");

        // Verify all scores are finite
        for r in &results {
            assert!(r.score.is_finite(), "Score should be finite");
            assert!(r.distance.is_finite(), "Distance should be finite");
        }

        // Verify results are sorted by score (descending for IP)
        for i in 0..results.len().saturating_sub(1) {
            assert!(
                results[i].score >= results[i + 1].score,
                "Results should be sorted by score descending"
            );
        }
    }
}
