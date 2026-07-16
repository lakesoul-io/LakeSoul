use std::cmp::Ordering;

use faer::linalg::matmul::matmul;
use faer::mat::{MatMut, MatRef};
use faer::{Accum, Par};
use rand::RngCore;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rayon::prelude::*;

const RESEED_CANDIDATES: usize = 8;
const DEFAULT_MAX_POINTS_PER_CENTROID: usize = 64;
const DEFAULT_DECODE_BLOCK_SIZE: usize = 32768;

#[derive(Debug, Clone)]
pub struct KMeansConfig {
    pub niter: usize,
    pub nredo: usize,
    pub seed: u64,
    pub spherical: bool,
    /// Maximum points sampled per centroid during training
    /// (Faiss default: 256)
    pub max_points_per_centroid: usize,
    /// Batch size for assignment computation (Faiss: decode_block_size)
    /// Larger = better throughput, higher memory; default 32768
    pub decode_block_size: usize,
}

impl Default for KMeansConfig {
    fn default() -> Self {
        Self {
            niter: 25,
            nredo: 1,
            seed: 42,
            spherical: false,
            max_points_per_centroid: DEFAULT_MAX_POINTS_PER_CENTROID,
            decode_block_size: DEFAULT_DECODE_BLOCK_SIZE,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KMeansResult {
    pub centroids: Vec<Vec<f32>>,
    pub assignments: Vec<usize>,
    pub objective: f64,
}

/// Run k-means clustering using a Faiss-inspired pipeline with GEMM-powered
/// assignments and multi-restart for robustness.
pub fn run_kmeans(
    data: &[Vec<f32>],
    k: usize,
    max_iter: usize,
    rng: &mut StdRng,
) -> KMeansResult {
    let config = KMeansConfig {
        niter: max_iter,
        nredo: 1,
        seed: rng.next_u64(),
        spherical: false,
        max_points_per_centroid: DEFAULT_MAX_POINTS_PER_CENTROID,
        decode_block_size: DEFAULT_DECODE_BLOCK_SIZE,
    };
    run_kmeans_with_config(data, k, config)
}

/// Run k-means with explicit configuration
pub fn run_kmeans_with_config(
    data: &[Vec<f32>],
    k: usize,
    config: KMeansConfig,
) -> KMeansResult {
    let dim = validate_inputs(data, k, config.niter);
    let total_points = data.len();

    let flattened = flatten_dataset(data, total_points * dim);
    run_kmeans_flat(&flattened, total_points, dim, k, config)
}

/// Internal k-means implementation that works with pre-flattened data
fn run_kmeans_flat(
    flattened: &[f32],
    total_points: usize,
    dim: usize,
    k: usize,
    config: KMeansConfig,
) -> KMeansResult {
    // Select training subset
    let mut sampling_rng = StdRng::seed_from_u64(config.seed);
    let training_indices = select_training_indices(
        total_points,
        k,
        config.max_points_per_centroid,
        &mut sampling_rng,
    );

    // Optimization: if training uses all points, use reference to avoid copy
    let training_rows = training_indices.len();
    let training_data_owned;
    let training_data: &[f32] = if training_rows == total_points {
        // Full dataset training: use direct reference (saves 3.66GB for k=4096)
        flattened
    } else {
        // Sampled training: gather subset
        training_data_owned = gather_rows(flattened, &training_indices, dim);
        &training_data_owned
    };

    println!(
        "  K-means: {} points, {} clusters, {} iterations, {} restarts",
        training_rows, k, config.niter, config.nredo
    );

    // Multi-restart: run nredo times, pick best by objective
    let mut best_result: Option<KMeansResult> = None;

    for redo_idx in 0..config.nredo {
        let redo_seed = config
            .seed
            .wrapping_add((redo_idx as u64).wrapping_mul(0x9e3779b97f4a7c15));
        let mut redo_rng = StdRng::seed_from_u64(redo_seed);

        // Random Forgy initialization
        #[allow(clippy::needless_borrow)]
        let mut centroids = initialize_centroids_random(
            &training_data,
            training_rows,
            dim,
            k,
            &mut redo_rng,
        );

        let mut training_assignments = vec![0usize; training_rows];
        #[allow(clippy::needless_borrow)]
        let norms = compute_norms(&training_data, training_rows, dim);

        // Lloyd iterations
        #[allow(clippy::needless_borrow)]
        run_lloyd_iterations(
            &mut centroids,
            config.niter,
            k,
            dim,
            &training_data,
            &norms,
            &mut training_assignments,
            &mut redo_rng,
            config.spherical,
            config.decode_block_size,
        );

        // Assign full dataset and compute objective
        let full_norms = compute_norms(flattened, total_points, dim);
        let mut centroid_col = Vec::with_capacity(dim * k);
        let mut centroid_norms = Vec::with_capacity(k);
        rebuild_centroid_views(
            &centroids,
            k,
            dim,
            &mut centroid_col,
            &mut centroid_norms,
        );

        let assignments = assign_full_dataset(
            flattened,
            &full_norms,
            k,
            dim,
            &centroid_col,
            &centroid_norms,
            config.decode_block_size,
        );

        let objective =
            compute_objective(flattened, &centroids, &assignments, total_points, dim);

        let centroids_vec = centroids.chunks(dim).map(|c| c.to_vec()).collect();
        let result = KMeansResult {
            centroids: centroids_vec,
            assignments,
            objective,
        };

        if redo_idx == 0 {
            println!(
                "    Restart {}/{}: objective = {:.2e}",
                redo_idx + 1,
                config.nredo,
                objective
            );
            best_result = Some(result);
        } else {
            let is_better = objective < best_result.as_ref().unwrap().objective;
            println!(
                "    Restart {}/{}: objective = {:.2e} {}",
                redo_idx + 1,
                config.nredo,
                objective,
                if is_better { "(new best)" } else { "" }
            );
            if is_better {
                best_result = Some(result);
            }
        }
    }

    best_result.unwrap()
}

fn validate_inputs(data: &[Vec<f32>], k: usize, max_iter: usize) -> usize {
    assert!(!data.is_empty(), "k-means requires non-empty data");
    assert!(k > 0, "k must be positive");
    assert!(max_iter > 0, "max_iter must be positive");
    assert!(k <= data.len(), "k cannot exceed number of samples");

    let dim = data[0].len();
    assert!(
        data.iter().all(|v| v.len() == dim),
        "all vectors must share the same dimension",
    );
    dim
}

fn flatten_dataset(data: &[Vec<f32>], capacity: usize) -> Vec<f32> {
    let mut flattened = Vec::with_capacity(capacity);
    for vector in data {
        flattened.extend_from_slice(vector);
    }
    flattened
}

fn select_training_indices(
    total_points: usize,
    k: usize,
    max_points_per_centroid: usize,
    rng: &mut StdRng,
) -> Vec<usize> {
    let target = total_points.min(k * max_points_per_centroid).max(k);
    if target == total_points {
        return (0..total_points).collect();
    }

    let mut indices: Vec<usize> = (0..total_points).collect();
    indices.shuffle(rng);
    indices.truncate(target);
    indices.sort_unstable();
    indices
}

/// Random Forgy initialization: pick k random points as initial centroids
fn initialize_centroids_random(
    data: &[f32],
    rows: usize,
    dim: usize,
    k: usize,
    rng: &mut StdRng,
) -> Vec<f32> {
    let mut indices: Vec<usize> = (0..rows).collect();
    indices.shuffle(rng);
    indices.truncate(k);

    let mut centroids = Vec::with_capacity(k * dim);
    for &idx in &indices {
        centroids.extend_from_slice(&data[idx * dim..(idx + 1) * dim]);
    }
    centroids
}

fn compute_norms(data: &[f32], rows: usize, dim: usize) -> Vec<f32> {
    let mut norms = vec![0.0f32; rows];
    for (row, norm) in norms.iter_mut().enumerate() {
        let start = row * dim;
        let slice = &data[start..start + dim];
        *norm = slice.iter().map(|v| v * v).sum();
    }
    norms
}

fn compute_objective(
    data: &[f32],
    centroids: &[f32],
    assignments: &[usize],
    rows: usize,
    dim: usize,
) -> f64 {
    let mut total = 0.0f64;
    for row in 0..rows {
        let cluster = assignments[row];
        let point = &data[row * dim..(row + 1) * dim];
        let centroid = &centroids[cluster * dim..(cluster + 1) * dim];
        let mut dist = 0.0f64;
        for d in 0..dim {
            let delta = (point[d] - centroid[d]) as f64;
            dist += delta * delta;
        }
        total += dist;
    }
    total
}

fn gather_rows(data: &[f32], indices: &[usize], dim: usize) -> Vec<f32> {
    let mut gathered = Vec::with_capacity(indices.len() * dim);
    for &idx in indices {
        let start = idx * dim;
        let end = start + dim;
        gathered.extend_from_slice(&data[start..end]);
    }
    gathered
}

/// Run Lloyd iterations for k-means
#[allow(clippy::too_many_arguments)]
fn run_lloyd_iterations(
    centroids: &mut [f32],
    iterations: usize,
    k: usize,
    dim: usize,
    data: &[f32],
    norms: &[f32],
    assignments: &mut [usize],
    rng: &mut StdRng,
    spherical: bool,
    decode_block_size: usize,
) {
    let mut centroid_col = Vec::with_capacity(dim * k);
    let mut centroid_norms = Vec::with_capacity(k);

    for _iter in 0..iterations {
        rebuild_centroid_views(centroids, k, dim, &mut centroid_col, &mut centroid_norms);

        let summary = assign_points_for_update(
            data,
            norms,
            assignments,
            k,
            dim,
            &centroid_col,
            &centroid_norms,
            decode_block_size,
        );

        update_centroids(centroids, k, dim, data, &summary, rng);

        if spherical {
            normalize_centroids(centroids, k, dim);
        }
    }
}

fn rebuild_centroid_views(
    centroids: &[f32],
    k: usize,
    dim: usize,
    centroid_col: &mut Vec<f32>,
    centroid_norms: &mut Vec<f32>,
) {
    centroid_col.clear();
    centroid_col.resize(dim * k, 0.0);
    centroid_norms.clear();
    centroid_norms.resize(k, 0.0);

    for cluster in 0..k {
        let centroid = &centroids[cluster * dim..(cluster + 1) * dim];
        let mut norm = 0.0f32;
        for d in 0..dim {
            let value = centroid[d];
            centroid_col[d * k + cluster] = value;
            norm += value * value;
        }
        centroid_norms[cluster] = norm;
    }
}

fn normalize_centroids(centroids: &mut [f32], k: usize, dim: usize) {
    for cluster in 0..k {
        let offset = cluster * dim;
        let centroid = &mut centroids[offset..offset + dim];
        let mut norm = 0.0f32;
        for &value in centroid.iter() {
            norm += value * value;
        }
        if norm > 0.0 {
            let inv = norm.sqrt().recip();
            for value in centroid.iter_mut() {
                *value *= inv;
            }
        }
    }
}

#[derive(Debug)]
struct AssignmentSummary {
    counts: Vec<usize>,
    sums: Vec<f32>,
    candidates: Vec<(f32, usize)>,
}

/// Thread-local buffer for reusing allocations during GEMM operations
struct KMeansBuffer {
    dot_products: Vec<f32>,
    sums: Vec<f32>,
}

impl KMeansBuffer {
    fn new() -> Self {
        Self {
            dot_products: Vec::new(),
            sums: Vec::new(),
        }
    }

    fn resize_for_chunk(&mut self, len: usize, k: usize, dim: usize) {
        self.dot_products.clear();
        self.dot_products.resize(len * k, 0.0);
        self.sums.clear();
        self.sums.resize(k * dim, 0.0);
    }
}

/// Thread-local state for fold+reduce streaming assignment
struct ThreadLocalState {
    buffer: KMeansBuffer,
    counts: Vec<usize>,
    sums: Vec<f32>,
    candidates: Vec<(f32, usize)>,
    assignments: Vec<(usize, Vec<usize>)>,
}

impl ThreadLocalState {
    fn new(k: usize, dim: usize) -> Self {
        Self {
            buffer: KMeansBuffer::new(),
            counts: vec![0; k],
            sums: vec![0.0; k * dim],
            candidates: Vec::new(),
            assignments: Vec::new(),
        }
    }

    fn merge_from(&mut self, other: Self, k: usize, dim: usize) {
        for cluster in 0..k {
            self.counts[cluster] += other.counts[cluster];
            for d in 0..dim {
                self.sums[cluster * dim + d] += other.sums[cluster * dim + d];
            }
        }
        self.candidates.extend(other.candidates);
        self.assignments.extend(other.assignments);
    }

    fn into_summary(self) -> AssignmentSummary {
        AssignmentSummary {
            counts: self.counts,
            sums: self.sums,
            candidates: self.candidates,
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn assign_points_for_update(
    data: &[f32],
    norms: &[f32],
    assignments: &mut [usize],
    k: usize,
    dim: usize,
    centroid_col: &[f32],
    centroid_norms: &[f32],
    decode_block_size: usize,
) -> AssignmentSummary {
    let rows = norms.len();
    let num_chunks = rows.div_ceil(decode_block_size);

    // Sequential chunks with a single shared GEMM buffer (~512 MB).
    // faer::matmul(Par::rayon(0)) uses all cores internally — no need for
    // outer rayon parallelism that would duplicate the buffer.
    let mut state = ThreadLocalState::new(k, dim);
    for chunk_idx in 0..num_chunks {
        let start = chunk_idx * decode_block_size;
        let end = ((chunk_idx + 1) * decode_block_size).min(rows);
        let len = end - start;
        let data_chunk = &data[start * dim..end * dim];
        let norms_chunk = &norms[start..end];

        // GEMM via faer (multi-threaded, all cores)
        state.buffer.resize_for_chunk(len, k, dim);
        {
            let a = MatRef::from_row_major_slice(data_chunk, len, dim);
            let b = MatRef::from_row_major_slice(centroid_col, dim, k);
            let c =
                MatMut::from_row_major_slice_mut(&mut state.buffer.dot_products, len, k);
            matmul(c, Accum::Replace, a, b, 1.0f32, Par::rayon(0));
        }

        // Parallel argmin + accumulation (rayon, read-only on dot_products)
        let chunk_results: Vec<(usize, f32)> = (0..len)
            .into_par_iter()
            .map(|row| {
                let dot_row = &state.buffer.dot_products[row * k..(row + 1) * k];
                let norm = norms_chunk[row];
                let mut best_cluster = 0usize;
                let mut best_distance = f32::INFINITY;
                for cluster in 0..k {
                    let dot = dot_row[cluster];
                    let mut distance = norm + centroid_norms[cluster] - 2.0 * dot;
                    if distance < 0.0 {
                        distance = 0.0;
                    }
                    if distance < best_distance {
                        best_distance = distance;
                        best_cluster = cluster;
                    }
                }
                (best_cluster, best_distance)
            })
            .collect();

        // Sequential accumulation (O(len × dim), fast)
        let mut chunk_assignments = Vec::with_capacity(len);
        let mut chunk_candidates: Vec<(f32, usize)> = Vec::new();
        for (row, &(cluster, dist)) in chunk_results.iter().enumerate() {
            chunk_assignments.push(cluster);
            state.counts[cluster] += 1;
            let vector = &data_chunk[row * dim..(row + 1) * dim];
            let sum_offset = cluster * dim;
            for (sum, &v) in state.sums[sum_offset..sum_offset + dim]
                .iter_mut()
                .zip(vector.iter())
            {
                *sum += v;
            }
            insert_candidate(&mut chunk_candidates, (dist, row));
        }
        for (dist, local_idx) in chunk_candidates {
            state.candidates.push((dist, start + local_idx));
        }
        state.assignments.push((start, chunk_assignments));
    }

    // Write assignments back
    state.assignments.sort_unstable_by_key(|(start, _)| *start);
    for (start, chunk_assignments) in &state.assignments {
        let end = start + chunk_assignments.len();
        assignments[*start..end].copy_from_slice(chunk_assignments);
    }

    state.into_summary()
}

fn insert_candidate(candidates: &mut Vec<(f32, usize)>, candidate: (f32, usize)) {
    if candidates.len() < RESEED_CANDIDATES {
        candidates.push(candidate);
        candidates
            .sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
        return;
    }
    if let Some((last_dist, _)) = candidates.last()
        && candidate.0 > *last_dist
    {
        candidates.pop();
        candidates.push(candidate);
        candidates
            .sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
    }
}

fn update_centroids(
    centroids: &mut [f32],
    k: usize,
    dim: usize,
    data: &[f32],
    summary: &AssignmentSummary,
    rng: &mut StdRng,
) {
    let total_rows = data.len() / dim;
    let mut candidate_pool = summary.candidates.clone();
    candidate_pool
        .sort_unstable_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
    let mut used = vec![false; total_rows];
    let mut candidate_indices = Vec::new();
    for (_, idx) in candidate_pool.into_iter() {
        if !used[idx] {
            used[idx] = true;
            candidate_indices.push(idx);
        }
    }
    let mut candidate_iter = candidate_indices.into_iter();

    for cluster in 0..k {
        let offset = cluster * dim;
        let count = summary.counts[cluster];
        if count > 0 {
            let inv = 1.0 / count as f32;
            let sum_offset = cluster * dim;
            for d in 0..dim {
                centroids[offset + d] = summary.sums[sum_offset + d] * inv;
            }
        } else {
            let replacement_index = candidate_iter
                .next()
                .unwrap_or_else(|| rng.gen_range(0..total_rows));
            let source = &data[replacement_index * dim..(replacement_index + 1) * dim];
            centroids[offset..offset + dim].copy_from_slice(source);
        }
    }
}

fn assign_full_dataset(
    data: &[f32],
    norms: &[f32],
    k: usize,
    dim: usize,
    centroid_col: &[f32],
    centroid_norms: &[f32],
    decode_block_size: usize,
) -> Vec<usize> {
    let rows = norms.len();
    let num_chunks = rows.div_ceil(decode_block_size);
    let mut assignments = vec![0usize; rows];
    // Sequential: each chunk allocates dot_products (len×k×4 B); parallel
    // map would keep multiple such buffers alive simultaneously.
    for chunk_idx in 0..num_chunks {
        let start = chunk_idx * decode_block_size;
        let end = ((chunk_idx + 1) * decode_block_size).min(rows);
        let len = end - start;
        let data_chunk = &data[start * dim..end * dim];
        let norms_chunk = &norms[start..end];
        let chunk_assignments = compute_chunk_assignments_only(
            data_chunk,
            norms_chunk,
            len,
            k,
            dim,
            centroid_col,
            centroid_norms,
        );
        assignments[start..end].copy_from_slice(&chunk_assignments);
    }
    assignments
}

#[allow(clippy::too_many_arguments)]
fn compute_chunk_assignments_only(
    data_chunk: &[f32],
    norms_chunk: &[f32],
    len: usize,
    k: usize,
    dim: usize,
    centroid_col: &[f32],
    centroid_norms: &[f32],
) -> Vec<usize> {
    let mut dot_products = vec![0.0f32; len * k];
    {
        let a = MatRef::from_row_major_slice(data_chunk, len, dim);
        let b = MatRef::from_row_major_slice(centroid_col, dim, k);
        let c = MatMut::from_row_major_slice_mut(&mut dot_products, len, k);
        matmul(c, Accum::Replace, a, b, 1.0f32, Par::rayon(0));
    }

    // Parallel argmin: read-only on dot_products after sgemm
    let assignments: Vec<usize> = (0..len)
        .into_par_iter()
        .map(|row| {
            let dot_row = &dot_products[row * k..(row + 1) * k];
            let norm = norms_chunk[row];
            let mut best_cluster = 0usize;
            let mut best_distance = f32::INFINITY;
            for cluster in 0..k {
                let dot = dot_row[cluster];
                let mut distance = norm + centroid_norms[cluster] - 2.0 * dot;
                if distance < 0.0 {
                    distance = 0.0;
                }
                if distance < best_distance {
                    best_distance = distance;
                    best_cluster = cluster;
                }
            }
            best_cluster
        })
        .collect();
    assignments
}

// ============================================================================
// Streaming / reservoir-sampling k-means for batch-oriented input
// ============================================================================

/// Run reservoir sampling over streaming flat batches, returning a flat
/// `Vec<f32>` holding up to `reservoir_size` vectors (row-major, `res * dim`).
pub fn reservoir_sample_from_batches(
    mut next_batch: impl FnMut() -> Option<Vec<f32>>,
    dim: usize,
    reservoir_size: usize,
    rng: &mut StdRng,
) -> Vec<f32> {
    use rand::Rng;

    let mut reservoir: Vec<f32> = Vec::new(); // flat, row-major
    let mut reservoir_count: usize = 0; // number of vectors currently in reservoir
    let mut seen: usize = 0;

    while let Some(batch) = next_batch() {
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

    if reservoir_count < reservoir_size {
        reservoir.truncate(reservoir_count * dim);
    }

    println!(
        "  Reservoir sampled {} / {} vectors ({:.1} MB)",
        reservoir_count,
        seen,
        reservoir_count * dim * 4 / (1024 * 1024)
    );

    reservoir
}

/// Run k-means on a flat training set (already in memory).
/// This is the core Lloyd-iteration loop operating on row-major `&[f32]`.
pub fn run_kmeans_on_flat(
    training: &[f32],
    num_points: usize,
    dim: usize,
    k: usize,
    config: KMeansConfig,
) -> KMeansResult {
    let mut rng = StdRng::seed_from_u64(config.seed);

    // Initialize centroids
    let mut indices: Vec<usize> = (0..num_points).collect();
    indices.shuffle(&mut rng);
    indices.truncate(k);

    let mut centroids = Vec::with_capacity(k * dim);
    for &idx in &indices {
        centroids.extend_from_slice(&training[idx * dim..(idx + 1) * dim]);
    }

    let norms = compute_norms(training, num_points, dim);
    let mut assignments = vec![0usize; num_points];

    run_lloyd_iterations(
        &mut centroids,
        config.niter,
        k,
        dim,
        training,
        &norms,
        &mut assignments,
        &mut rng,
        config.spherical,
        config.decode_block_size,
    );

    let _centroid_views: Vec<f32> = centroids.clone();
    let _centroid_norms: Vec<f32> = centroids
        .chunks(dim)
        .map(|c| c.iter().map(|x| x * x).sum())
        .collect();

    let objective =
        compute_objective(training, &centroids, &assignments, num_points, dim);

    KMeansResult {
        centroids: centroids.chunks(dim).map(|c| c.to_vec()).collect(),
        assignments,
        objective,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_dataset() -> Vec<Vec<f32>> {
        let mut data = Vec::new();
        for _ in 0..16 {
            data.push(vec![0.0, 0.0]);
            data.push(vec![10.0, 9.5]);
        }
        data
    }

    #[test]
    fn training_indices_are_sampled_and_sorted() {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);
        let indices = select_training_indices(10_000, 8, 256, &mut rng);
        assert_eq!(indices.len(), 8 * 256);
        assert!(indices.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn training_indices_respect_max_points_per_centroid() {
        let mut rng = StdRng::seed_from_u64(0xDEADBEEF);

        // Custom sampling: 64 points per centroid
        let indices = select_training_indices(1_000_000, 4096, 64, &mut rng);
        assert_eq!(indices.len(), 4096 * 64);

        // Custom sampling: 128 points per centroid
        let indices = select_training_indices(1_000_000, 1024, 128, &mut rng);
        assert_eq!(indices.len(), 1024 * 128);

        // Default Faiss: 256 points per centroid
        let indices = select_training_indices(1_000_000, 512, 256, &mut rng);
        assert_eq!(indices.len(), 512 * 256);
    }

    #[test]
    fn kmeans_converges_on_simple_dataset() {
        let data = simple_dataset();
        let config = KMeansConfig {
            niter: 20,
            nredo: 3,
            seed: 0xBAD5EED,
            spherical: false,
            max_points_per_centroid: DEFAULT_MAX_POINTS_PER_CENTROID,
            decode_block_size: DEFAULT_DECODE_BLOCK_SIZE,
        };
        let result = run_kmeans_with_config(&data, 2, config);
        assert_eq!(result.centroids.len(), 2);
        assert_eq!(result.assignments.len(), data.len());
        assert!(result.objective >= 0.0);
        let mut centroids = result.centroids.clone();
        centroids.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
        let left = &centroids[0];
        let right = &centroids[1];
        assert!(left[0].abs() < 0.5 && left[1].abs() < 0.5);
        assert!((right[0] - 10.0).abs() < 0.5 && (right[1] - 9.5).abs() < 0.5);
    }

    #[test]
    fn runs_are_deterministic_given_seed() {
        let data = simple_dataset();
        let config1 = KMeansConfig {
            niter: 20,
            nredo: 1,
            seed: 0x1234_5678,
            spherical: false,
            max_points_per_centroid: DEFAULT_MAX_POINTS_PER_CENTROID,
            decode_block_size: DEFAULT_DECODE_BLOCK_SIZE,
        };
        let config2 = KMeansConfig {
            niter: 20,
            nredo: 1,
            seed: 0x1234_5678,
            spherical: false,
            max_points_per_centroid: DEFAULT_MAX_POINTS_PER_CENTROID,
            decode_block_size: DEFAULT_DECODE_BLOCK_SIZE,
        };
        let result1 = run_kmeans_with_config(&data, 2, config1);
        let result2 = run_kmeans_with_config(&data, 2, config2);
        assert_eq!(result1.assignments, result2.assignments);
        assert_eq!(result1.centroids, result2.centroids);
        assert_eq!(result1.objective, result2.objective);
    }
}
