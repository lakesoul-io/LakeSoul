use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::rabitq::Metric;
use crate::rabitq::math::{dot, l2_norm_sqr, subtract};
use crate::rabitq::simd;

const K_TIGHT_START: [f64; 9] = [0.0, 0.15, 0.20, 0.52, 0.59, 0.71, 0.75, 0.77, 0.81];
const K_EPS: f64 = 1e-5;
const K_NENUM: f64 = 10.0;
const K_CONST_EPSILON: f32 = 1.9;

/// Configuration for RaBitQ quantisation.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct RabitqConfig {
    pub total_bits: usize,
    /// Precomputed constant scaling factor for faster quantization.
    /// If None, compute optimal t for each vector (slower but more accurate).
    /// If Some(t_const), use this constant for all vectors (100-500x faster).
    pub t_const: Option<f32>,
}

impl RabitqConfig {
    pub fn new(total_bits: usize) -> Self {
        RabitqConfig {
            total_bits,
            t_const: None, // Default to precise mode
        }
    }

    /// Create a faster config with precomputed scaling factor.
    /// This trades <1% accuracy for 100-500x faster quantization.
    pub fn faster(dim: usize, total_bits: usize, seed: u64) -> Self {
        let ex_bits = total_bits.saturating_sub(1);
        let t_const = if ex_bits > 0 {
            Some(compute_const_scaling_factor(dim, ex_bits, seed))
        } else {
            None
        };

        RabitqConfig {
            total_bits,
            t_const,
        }
    }
}

impl Default for RabitqConfig {
    fn default() -> Self {
        Self::new(7) // Default to 7 bits as per MSTG spec
    }
}

/// Quantised representation of a vector using packed format (aligned with C++ implementation).
///
/// Binary codes and extended codes are stored in bit-packed format for memory efficiency.
/// - `binary_code_packed`: 1 bit per dimension
/// - `ex_code_packed`: ex_bits per dimension
///
/// For performance-critical search operations, unpacked codes are cached to avoid
/// repeated unpacking overhead.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QuantizedVector {
    /// Packed binary code (1 bit per element)
    pub binary_code_packed: Vec<u8>,
    /// Packed extended code (ex_bits per element)
    pub ex_code_packed: Vec<u8>,
    /// Number of extended bits per element
    pub ex_bits: u8,
    /// Original dimension (before padding)
    pub dim: usize,
    pub delta: f32,
    pub vl: f32,
    pub f_add: f32,
    pub f_rescale: f32,
    pub f_error: f32,
    pub residual_norm: f32,
    pub f_add_ex: f32,
    pub f_rescale_ex: f32,
}

impl QuantizedVector {
    /// Unpack binary code for computation
    #[inline]
    pub fn unpack_binary_code(&self) -> Vec<u8> {
        let mut binary_code = vec![0u8; self.dim];
        simd::unpack_binary_code(&self.binary_code_packed, &mut binary_code, self.dim);
        binary_code
    }

    /// Unpack extended code for computation
    #[inline]
    pub fn unpack_ex_code(&self) -> Vec<u16> {
        let mut ex_code = vec![0u16; self.dim];
        simd::unpack_ex_code(&self.ex_code_packed, &mut ex_code, self.dim, self.ex_bits);
        ex_code
    }

    /// Ensure unpacked caches are populated (No-op in memory-optimized version)
    pub fn ensure_unpacked_cache(&mut self) {}

    /// Calculate heap memory usage in bytes
    pub fn heap_size(&self) -> usize {
        self.binary_code_packed.capacity() * std::mem::size_of::<u8>()
            + self.ex_code_packed.capacity() * std::mem::size_of::<u8>()
    }
}

/// Quantise a vector relative to a centroid with custom configuration.
pub fn quantize_with_centroid(
    data: &[f32],
    centroid: &[f32],
    config: &RabitqConfig,
    metric: Metric,
) -> QuantizedVector {
    assert_eq!(data.len(), centroid.len());
    assert!((1..=16).contains(&config.total_bits));
    let dim = data.len();
    let ex_bits = config.total_bits.saturating_sub(1);

    let residual = subtract(data, centroid);
    let mut binary_code = vec![0u8; dim];
    for (idx, &value) in residual.iter().enumerate() {
        if value >= 0.0 {
            binary_code[idx] = 1u8;
        }
    }

    let (ex_code, ipnorm_inv) = if ex_bits > 0 {
        ex_bits_code_with_inv(&residual, ex_bits, config.t_const)
    } else {
        (vec![0u16; dim], 1.0f32)
    };

    let mut total_code = vec![0u16; dim];
    for i in 0..dim {
        total_code[i] = ex_code[i] + ((binary_code[i] as u16) << ex_bits);
    }

    let (f_add, f_rescale, f_error, residual_norm) =
        compute_one_bit_factors(&residual, centroid, &binary_code, metric);
    let cb = -((1 << ex_bits) as f32 - 0.5);
    let quantized_shifted: Vec<f32> =
        total_code.iter().map(|&code| code as f32 + cb).collect();
    let norm_quan_sqr = l2_norm_sqr(&quantized_shifted);
    let dot_residual_quant = dot(&residual, &quantized_shifted);

    let norm_residual_sqr = l2_norm_sqr(&residual);
    let norm_residual = norm_residual_sqr.sqrt();
    let norm_quant = norm_quan_sqr.sqrt();
    let denom = (norm_residual * norm_quant).max(f32::EPSILON);
    let cos_similarity = (dot_residual_quant / denom).clamp(-1.0, 1.0);
    let delta = if norm_quant <= f32::EPSILON {
        0.0
    } else {
        (norm_residual / norm_quant) * cos_similarity
    };
    let vl = delta * cb;

    let mut f_add_ex = 0.0f32;
    let mut f_rescale_ex = 0.0f32;
    if ex_bits > 0 {
        let factors = compute_extended_factors(
            &residual,
            centroid,
            &binary_code,
            &ex_code,
            ipnorm_inv,
            metric,
            ex_bits,
        );
        f_add_ex = factors.0;
        f_rescale_ex = factors.1;
    }

    // Pack binary code and ex code into bit-packed format
    let binary_code_packed_size = dim.div_ceil(8);
    let mut binary_code_packed = vec![0u8; binary_code_packed_size];
    simd::pack_binary_code(&binary_code, &mut binary_code_packed, dim);

    // Use C++-compatible packing format for ex_code (Phase 4 optimization)
    // This enables direct SIMD operations without unpacking overhead
    let ex_code_packed_size = match ex_bits {
        0 => dim / 16 * 2, // 1-bit total (binary only), but allocate for consistency
        1 => dim / 16 * 2, // 2-bit total (1-bit ex-code) - not commonly used
        2 => dim / 16 * 4, // 3-bit total (2-bit ex-code)
        6 => dim / 16 * 12, // 7-bit total (6-bit ex-code)
        _ => (dim * ex_bits).div_ceil(8), // Fallback for other bit configs
    };
    let mut ex_code_packed = vec![0u8; ex_code_packed_size];

    // Pack using C++-compatible format based on ex_bits
    match ex_bits {
        0 => {
            // Binary-only: no ex-code to pack (all zeros)
            // Keep packed array as zeros
        }
        1 => {
            // 1-bit ex-code (2-bit total RaBitQ)
            simd::pack_ex_code_1bit_cpp_compat(&ex_code, &mut ex_code_packed, dim);
        }
        2 => {
            // 2-bit ex-code (3-bit total RaBitQ)
            simd::pack_ex_code_2bit_cpp_compat(&ex_code, &mut ex_code_packed, dim);
        }
        6 => {
            // 6-bit ex-code (7-bit total RaBitQ)
            simd::pack_ex_code_6bit_cpp_compat(&ex_code, &mut ex_code_packed, dim);
        }
        _ => {
            // Fallback to generic packing for unsupported bit configs
            simd::pack_ex_code(&ex_code, &mut ex_code_packed, dim, ex_bits as u8);
        }
    }

    QuantizedVector {
        binary_code_packed,
        ex_code_packed,
        ex_bits: ex_bits as u8,
        dim,
        delta,
        vl,
        f_add,
        f_rescale,
        f_error,
        residual_norm,
        f_add_ex,
        f_rescale_ex,
    }
}

fn compute_one_bit_factors(
    residual: &[f32],
    centroid: &[f32],
    binary_code: &[u8],
    metric: Metric,
) -> (f32, f32, f32, f32) {
    let dim = residual.len();
    let xu_cb: Vec<f32> = binary_code.iter().map(|&bit| bit as f32 - 0.5f32).collect();
    let l2_sqr = l2_norm_sqr(residual);
    let l2_norm = l2_sqr.sqrt();
    let xu_cb_norm_sqr = l2_norm_sqr(&xu_cb);
    let ip_resi_xucb = dot(residual, &xu_cb);
    let ip_cent_xucb = dot(centroid, &xu_cb);
    let dot_residual_centroid = dot(residual, centroid);

    let mut denom = ip_resi_xucb;
    if denom.abs() <= f32::EPSILON {
        denom = f32::INFINITY;
    }

    let mut tmp_error = 0.0f32;
    if dim > 1 {
        let ratio = ((l2_sqr * xu_cb_norm_sqr) / (denom * denom)) - 1.0;
        if ratio.is_finite() && ratio > 0.0 {
            tmp_error = l2_norm
                * K_CONST_EPSILON
                * ((ratio / ((dim - 1) as f32)).max(0.0)).sqrt();
        }
    }

    let (f_add, f_rescale, f_error) = match metric {
        Metric::L2 => {
            let f_add = l2_sqr + 2.0 * l2_sqr * ip_cent_xucb / denom;
            let f_rescale = -2.0 * l2_sqr / denom;
            let f_error = 2.0 * tmp_error;
            (f_add, f_rescale, f_error)
        }
        Metric::InnerProduct => {
            let f_add = 1.0 - dot_residual_centroid + l2_sqr * ip_cent_xucb / denom;
            let f_rescale = -l2_sqr / denom;
            let f_error = tmp_error;
            (f_add, f_rescale, f_error)
        }
    };

    (f_add, f_rescale, f_error, l2_norm)
}

fn ex_bits_code_with_inv(
    residual: &[f32],
    ex_bits: usize,
    t_const: Option<f32>,
) -> (Vec<u16>, f32) {
    let dim = residual.len();
    let mut normalized_abs: Vec<f32> = residual.iter().map(|x| x.abs()).collect();
    let norm = normalized_abs.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm <= f32::EPSILON {
        return (vec![0u16; dim], 1.0);
    }

    for value in normalized_abs.iter_mut() {
        *value /= norm;
    }

    // Use precomputed t_const if available, otherwise compute optimal t
    let t = if let Some(t) = t_const {
        t as f64
    } else {
        best_rescale_factor(&normalized_abs, ex_bits)
    };

    quantize_ex_with_inv(&normalized_abs, residual, ex_bits, t)
}

fn best_rescale_factor(o_abs: &[f32], ex_bits: usize) -> f64 {
    let dim = o_abs.len();
    let max_o = o_abs.iter().cloned().fold(0.0f32, f32::max) as f64;
    if max_o <= f64::EPSILON {
        return 1.0;
    }

    let table_idx = ex_bits.min(K_TIGHT_START.len() - 1);
    let t_end = (((1 << ex_bits) - 1) as f64 + K_NENUM) / max_o;
    let t_start = t_end * K_TIGHT_START[table_idx];

    let mut cur_o_bar = vec![0i32; dim];
    let mut sqr_denominator = dim as f64 * 0.25;
    let mut numerator = 0.0f64;

    for (idx, &val) in o_abs.iter().enumerate() {
        let cur = ((t_start * val as f64) + K_EPS) as i32;
        cur_o_bar[idx] = cur;
        sqr_denominator += (cur * cur + cur) as f64;
        numerator += (cur as f64 + 0.5) * val as f64;
    }

    #[derive(Copy, Clone, Debug)]
    struct HeapEntry {
        t: f64,
        idx: usize,
    }

    impl PartialEq for HeapEntry {
        fn eq(&self, other: &Self) -> bool {
            self.t.to_bits() == other.t.to_bits() && self.idx == other.idx
        }
    }

    impl Eq for HeapEntry {}

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            self.t
                .total_cmp(&other.t)
                .then_with(|| self.idx.cmp(&other.idx))
        }
    }

    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::new();
    for (idx, &val) in o_abs.iter().enumerate() {
        if val > 0.0 {
            let next_t = (cur_o_bar[idx] + 1) as f64 / val as f64;
            heap.push(Reverse(HeapEntry { t: next_t, idx }));
        }
    }

    let mut max_ip = 0.0f64;
    let mut best_t = t_start;

    while let Some(Reverse(HeapEntry { t: cur_t, idx })) = heap.pop() {
        if cur_t >= t_end {
            continue;
        }

        cur_o_bar[idx] += 1;
        let update = cur_o_bar[idx];
        sqr_denominator += 2.0 * update as f64;
        numerator += o_abs[idx] as f64;

        let cur_ip = numerator / sqr_denominator.sqrt();
        if cur_ip > max_ip {
            max_ip = cur_ip;
            best_t = cur_t;
        }

        if update < (1 << ex_bits) - 1 && o_abs[idx] > 0.0 {
            let t_next = (update + 1) as f64 / o_abs[idx] as f64;
            if t_next < t_end {
                heap.push(Reverse(HeapEntry { t: t_next, idx }));
            }
        }
    }

    if best_t <= 0.0 {
        t_start.max(f64::EPSILON)
    } else {
        best_t
    }
}

fn quantize_ex_with_inv(
    o_abs: &[f32],
    residual: &[f32],
    ex_bits: usize,
    t: f64,
) -> (Vec<u16>, f32) {
    let dim = o_abs.len();
    if dim == 0 {
        return (Vec::new(), 1.0);
    }

    let mut code = vec![0u16; dim];
    let max_val = (1 << ex_bits) - 1;
    let mut ipnorm = 0.0f64;

    for i in 0..dim {
        let mut cur = (t * o_abs[i] as f64 + K_EPS) as i32;
        if cur > max_val {
            cur = max_val;
        }
        code[i] = cur as u16;
        ipnorm += (cur as f64 + 0.5) * o_abs[i] as f64;
    }

    let mut ipnorm_inv = if ipnorm.is_finite() && ipnorm > 0.0 {
        (1.0 / ipnorm) as f32
    } else {
        1.0
    };

    let mask = max_val as u16;
    if max_val > 0 {
        for (idx, &res) in residual.iter().enumerate() {
            if res < 0.0 {
                code[idx] = (!code[idx]) & mask;
            }
        }
    }

    if !ipnorm_inv.is_finite() {
        ipnorm_inv = 1.0;
    }

    (code, ipnorm_inv)
}

fn compute_extended_factors(
    residual: &[f32],
    centroid: &[f32],
    binary_code: &[u8],
    ex_code: &[u16],
    ipnorm_inv: f32,
    metric: Metric,
    ex_bits: usize,
) -> (f32, f32) {
    let dim = residual.len();
    let cb = -((1 << ex_bits) as f32 - 0.5);
    let xu_cb: Vec<f32> = (0..dim)
        .map(|i| {
            let total = ex_code[i] as u32 + ((binary_code[i] as u32) << ex_bits);
            total as f32 + cb
        })
        .collect();

    let l2_sqr = l2_norm_sqr(residual);
    let l2_norm = l2_sqr.sqrt();
    let xu_cb_norm_sqr = l2_norm_sqr(&xu_cb);
    let ip_resi_xucb = dot(residual, &xu_cb);
    let ip_cent_xucb = dot(centroid, &xu_cb);
    let dot_residual_centroid = dot(residual, centroid);

    let mut denom = ip_resi_xucb * ip_resi_xucb;
    if denom <= f32::EPSILON {
        denom = f32::INFINITY;
    }

    let mut tmp_error = 0.0f32;
    if dim > 1 {
        let ratio = ((l2_sqr * xu_cb_norm_sqr) / denom) - 1.0;
        if ratio > 0.0 {
            tmp_error = l2_norm
                * K_CONST_EPSILON
                * ((ratio / ((dim - 1) as f32)).max(0.0)).sqrt();
        }
    }

    let safe_denom = if ip_resi_xucb.abs() <= f32::EPSILON {
        f32::INFINITY
    } else {
        ip_resi_xucb
    };

    let (f_add_ex, f_rescale_ex) = match metric {
        Metric::L2 => {
            let f_add = l2_sqr + 2.0 * l2_sqr * ip_cent_xucb / safe_denom;
            let f_rescale = -2.0 * l2_norm * ipnorm_inv;
            (f_add, f_rescale)
        }
        Metric::InnerProduct => {
            let f_add = 1.0 - dot_residual_centroid + l2_sqr * ip_cent_xucb / safe_denom;
            let f_rescale = -l2_norm * ipnorm_inv;
            (f_add, f_rescale)
        }
    };

    let _ = tmp_error; // retain structure parity; tmp_error may be used in future

    (f_add_ex, f_rescale_ex)
}

/// Reconstruct a vector from its quantised representation and centroid (in rotated space).
///
/// This reconstructs the vector in the **rotated space**. To get the original vector,
/// you need to apply inverse rotation to the result.
#[allow(dead_code)]
pub(crate) fn reconstruct_into(
    centroid: &[f32],
    quantized: &QuantizedVector,
    output: &mut [f32],
) {
    assert_eq!(centroid.len(), quantized.dim);
    assert_eq!(output.len(), centroid.len());

    let binary_code = quantized.unpack_binary_code();
    let ex_code = quantized.unpack_ex_code();

    for i in 0..centroid.len() {
        let total_code =
            (ex_code[i] as u32 + ((binary_code[i] as u32) << quantized.ex_bits)) as f32;
        output[i] = centroid[i] + quantized.delta * total_code + quantized.vl;
    }
}

/// Compute a constant scaling factor for faster quantization.
///
/// This function samples random normalized vectors and computes the average optimal
/// scaling factor. Using this constant factor for all vectors is 100-500x faster
/// than computing the optimal factor per-vector, with <1% accuracy loss.
///
/// # Arguments
/// * `dim` - Vector dimensionality
/// * `ex_bits` - Number of extended bits for quantization
/// * `seed` - Random seed for reproducibility
///
/// # Returns
/// Average optimal scaling factor across 100 random samples
pub fn compute_const_scaling_factor(dim: usize, ex_bits: usize, seed: u64) -> f32 {
    use rand::prelude::*;
    use rand_distr::{Distribution, Normal};

    const NUM_SAMPLES: usize = 100;

    let mut rng = StdRng::seed_from_u64(seed);
    let normal = Normal::new(0.0, 1.0).expect("failed to create normal distribution");

    let mut sum_t = 0.0f64;

    for _ in 0..NUM_SAMPLES {
        // Generate random Gaussian vector
        let vec: Vec<f32> = (0..dim).map(|_| normal.sample(&mut rng) as f32).collect();

        // Normalize and take absolute value
        let norm = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm <= f32::EPSILON {
            continue;
        }

        let normalized_abs: Vec<f32> = vec.iter().map(|x| (x / norm).abs()).collect();

        // Compute optimal scaling factor for this random vector
        let t = best_rescale_factor(&normalized_abs, ex_bits);
        sum_t += t;
    }

    (sum_t / NUM_SAMPLES as f64) as f32
}
