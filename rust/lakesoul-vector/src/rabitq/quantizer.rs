use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::rabitq::Metric;
use crate::rabitq::math::{dot, l2_norm_sqr, subtract, subtract_into};
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

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct QuantizedParams {
    pub f_add: f32,
    pub f_rescale: f32,
    pub f_error: f32,
    pub delta: f32,
    pub vl: f32,
    pub f_add_ex: f32,
    pub f_rescale_ex: f32,
}

/// 每个量化 worker 复用的输出和中间缓冲区。
///
/// 这些缓冲区只随单个向量维度和有限位宽的 rescale 事件数增长，
/// 不随数据集规模增长；它们在 worker 生命周期内复用，避免逐向量分配。
pub(crate) struct QuantizeScratch {
    pub packed_binary: Vec<u8>,
    pub packed_ex: Vec<u8>,
    residual: Vec<f32>,
    binary: Vec<u8>,
    normalized_abs: Vec<f32>,
    ex_code: Vec<u16>,
    rescale_levels: Vec<i32>,
    rescale_events: Vec<u64>,
    one_bit_values: Vec<f32>,
    extended_values: Vec<f32>,
}

impl QuantizeScratch {
    pub(crate) fn new(dim: usize) -> Self {
        Self {
            packed_binary: Vec::with_capacity(dim.div_ceil(8)),
            packed_ex: Vec::new(),
            residual: Vec::new(),
            binary: Vec::new(),
            normalized_abs: Vec::new(),
            ex_code: Vec::new(),
            rescale_levels: Vec::new(),
            rescale_events: Vec::new(),
            one_bit_values: Vec::new(),
            extended_values: Vec::new(),
        }
    }
}

/// 量化热路径的无分配实现。
///
/// 与 `quantize_with_centroid` 使用相同的计算顺序和打包格式，但把
/// 中间向量、rescale 事件数组以及最终 packed 输出全部放进 worker scratch，
/// 避免每个输入向量反复申请多个 Vec 并再复制一次输出。
pub(crate) fn quantize_with_centroid_parts_into(
    data: &[f32],
    centroid: &[f32],
    config: &RabitqConfig,
    metric: Metric,
    scratch: &mut QuantizeScratch,
) -> QuantizedParams {
    assert_eq!(data.len(), centroid.len());
    assert!((1..=9).contains(&config.total_bits));
    let dim = data.len();
    let ex_bits = config.total_bits.saturating_sub(1);

    scratch.residual.resize(dim, 0.0);
    subtract_into(data, centroid, &mut scratch.residual);
    scratch.binary.resize(dim, 0);
    scratch.one_bit_values.resize(dim, 0.0);
    for i in 0..dim {
        let bit = (scratch.residual[i] >= 0.0) as u8;
        scratch.binary[i] = bit;
        scratch.one_bit_values[i] = bit as f32 - 0.5;
    }

    let (ipnorm_inv, norm_residual_sqr) = if ex_bits > 0 {
        scratch.normalized_abs.resize(dim, 0.0);
        let norm_sqr = scratch
            .residual
            .iter()
            .map(|value| value.abs() * value.abs())
            .sum::<f32>();
        let norm = norm_sqr.sqrt();
        let norm_residual_sqr = l2_norm_sqr(&scratch.residual);
        if norm <= f32::EPSILON {
            scratch.ex_code.resize(dim, 0);
            // resize does not initialize entries that are already present.
            // A reused worker must not carry a previous vector's codes into
            // a zero-residual vector (e.g. a singleton GIST cluster).
            scratch.ex_code.fill(0);
            (1.0, norm_residual_sqr)
        } else {
            for (dst, &value) in scratch.normalized_abs.iter_mut().zip(&scratch.residual)
            {
                *dst = value.abs() / norm;
            }
            let t = config.t_const.unwrap_or_else(|| {
                best_rescale_factor_into(
                    &scratch.normalized_abs,
                    ex_bits,
                    &mut scratch.rescale_levels,
                    &mut scratch.rescale_events,
                ) as f32
            });
            let ipnorm_inv = quantize_ex_with_inv_into(
                &scratch.normalized_abs,
                &scratch.residual,
                ex_bits,
                t as f64,
                &mut scratch.ex_code,
            );
            (ipnorm_inv, norm_residual_sqr)
        }
    } else {
        scratch.ex_code.clear();
        (1.0, l2_norm_sqr(&scratch.residual))
    };

    let cb = -((1 << ex_bits) as f32 - 0.5);
    scratch.extended_values.resize(dim, 0.0);
    for i in 0..dim {
        let total_code = scratch.ex_code.get(i).copied().unwrap_or(0) as u32
            + ((scratch.binary[i] as u32) << ex_bits);
        scratch.extended_values[i] = total_code as f32 + cb;
    }

    let norm_quan_sqr = l2_norm_sqr(&scratch.extended_values);
    let dot_residual_quant = dot(&scratch.residual, &scratch.extended_values);
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

    // 两套因子共享 residual 范数和 residual·centroid，避免对同一向量
    // 重复执行多次完整 dot/l2 扫描。
    let one_bit_norm_sqr = l2_norm_sqr(&scratch.one_bit_values);
    let dot_residual_centroid = if metric == Metric::InnerProduct {
        dot(&scratch.residual, centroid)
    } else {
        0.0
    };
    let (f_add, f_rescale, f_error, _residual_norm) = compute_one_bit_factors_precomputed(
        &scratch.residual,
        centroid,
        &scratch.one_bit_values,
        norm_residual_sqr,
        one_bit_norm_sqr,
        dot_residual_centroid,
        metric,
    );
    let (f_add_ex, f_rescale_ex) = if ex_bits > 0 {
        compute_extended_factors_precomputed(
            &scratch.residual,
            centroid,
            &scratch.extended_values,
            norm_residual_sqr,
            dot_residual_centroid,
            ipnorm_inv,
            metric,
        )
    } else {
        (0.0, 0.0)
    };

    scratch.packed_binary.resize(dim.div_ceil(8), 0);
    simd::pack_binary_code(&scratch.binary, &mut scratch.packed_binary, dim);

    let ex_code_packed_size = match ex_bits {
        0 => dim / 16 * 2,
        1 => dim / 16 * 2,
        2 => dim / 16 * 4,
        6 => dim / 16 * 12,
        _ => (dim * ex_bits).div_ceil(8),
    };
    scratch.packed_ex.resize(ex_code_packed_size, 0);
    match ex_bits {
        0 => {}
        1 => simd::pack_ex_code_1bit_cpp_compat(
            &scratch.ex_code,
            &mut scratch.packed_ex,
            dim,
        ),
        2 => simd::pack_ex_code_2bit_cpp_compat(
            &scratch.ex_code,
            &mut scratch.packed_ex,
            dim,
        ),
        6 => simd::pack_ex_code_6bit_cpp_compat(
            &scratch.ex_code,
            &mut scratch.packed_ex,
            dim,
        ),
        _ => simd::pack_ex_code(
            &scratch.ex_code,
            &mut scratch.packed_ex,
            dim,
            ex_bits as u8,
        ),
    }

    QuantizedParams {
        f_add,
        f_rescale,
        f_error,
        delta,
        vl,
        f_add_ex,
        f_rescale_ex,
    }
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

fn compute_one_bit_factors_precomputed(
    residual: &[f32],
    centroid: &[f32],
    xu_cb: &[f32],
    l2_sqr: f32,
    xu_cb_norm_sqr: f32,
    dot_residual_centroid: f32,
    metric: Metric,
) -> (f32, f32, f32, f32) {
    let dim = residual.len();
    let l2_norm = l2_sqr.sqrt();
    let ip_resi_xucb = dot(residual, xu_cb);
    let ip_cent_xucb = dot(centroid, xu_cb);
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
        Metric::L2 => (
            l2_sqr + 2.0 * l2_sqr * ip_cent_xucb / denom,
            -2.0 * l2_sqr / denom,
            2.0 * tmp_error,
        ),
        Metric::InnerProduct => (
            1.0 - dot_residual_centroid + l2_sqr * ip_cent_xucb / denom,
            -l2_sqr / denom,
            tmp_error,
        ),
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

fn best_rescale_factor_heap(o_abs: &[f32], ex_bits: usize) -> f64 {
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

#[inline]
fn rescale_event_key(level: i32, value: f64, reciprocal: f64, payload_mask: u64) -> u64 {
    let approximate = (level as f64 * reciprocal).to_bits();
    if (approximate.wrapping_add(4) & payload_mask) < 8 {
        (level as f64 / value).to_bits()
    } else {
        approximate
    }
}

/// 给定一组绝对值 o_abs 和量化位宽 ex_bits，搜索一个最优缩放因子 t，
/// 使得量化后的整数表示与原始绝对值向量的“余弦相似度”最大。
/// 函数最终返回这个缩放因子 t。
///
/// # 参数
/// * o_abs 绝对值
/// * ex_bits 量化位宽
///
/// # 返回值
/// # t 缩放因子
fn best_rescale_factor(o_abs: &[f32], ex_bits: usize) -> f64 {
    let mut levels = Vec::new();
    let mut events = Vec::new();
    best_rescale_factor_into(o_abs, ex_bits, &mut levels, &mut events)
}

/// 使用复用的事件缓冲区搜索缩放因子。
///
/// 每个维度的事件序列是单调的。先把有限位宽范围内的事件一次性写入
/// 连续数组，再按浮点 key 排序，比每个事件都维护二叉堆更快；事件序列
/// 和旧堆路径完全相同，因此不会改变最优缩放因子。
fn best_rescale_factor_into(
    o_abs: &[f32],
    ex_bits: usize,
    levels: &mut Vec<i32>,
    events: &mut Vec<u64>,
) -> f64 {
    // The packed event key reserves 9 level bits, so the optimised path only
    // covers ex_bits <= 8. The builder supports 1/3/7; wider public configs
    // keep the legacy heap search to preserve the 1..=16 quantiser range.
    if ex_bits > 8 {
        return best_rescale_factor_heap(o_abs, ex_bits);
    }
    let dim = o_abs.len();
    let max_o = o_abs.iter().cloned().fold(0.0f32, f32::max) as f64;
    if max_o <= f64::EPSILON {
        return 1.0;
    }

    let table_idx = ex_bits.min(K_TIGHT_START.len() - 1);
    let t_end = (((1 << ex_bits) - 1) as f64 + K_NENUM) / max_o;
    let t_start = t_end * K_TIGHT_START[table_idx];
    let max_level = (1 << ex_bits) - 1;
    // 低 9 位保存事件 level，后面保存维度编号；其余高位保存正 f64
    // 阈值的高位。高位相同的事件稍后用原始除法精确重排。
    const LEVEL_BITS: u32 = 9;
    const LEVEL_MASK: u64 = (1 << LEVEL_BITS) - 1;
    assert!(dim <= (u64::MAX >> LEVEL_BITS) as usize);
    let index_bits = usize::BITS - (dim - 1).leading_zeros();
    let payload_bits = index_bits + LEVEL_BITS;
    let payload_mask = u64::MAX >> (64 - payload_bits);
    let time_mask = !payload_mask;

    levels.resize(dim, 0);
    events.clear();
    let mut sqr_denominator = dim as f64 * 0.25;
    let mut numerator = 0.0f64;
    let mut required_events = 0usize;
    for (idx, &val) in o_abs.iter().enumerate() {
        let cur = ((t_start * val as f64) + K_EPS) as i32;
        levels[idx] = cur << LEVEL_BITS;
        sqr_denominator += (cur * cur + cur) as f64;
        numerator += (cur as f64 + 0.5) * val as f64;
        if val > 0.0 {
            // 旧堆路径会无条件插入每个维度的第一个事件；只有
            // 处理该事件后才检查饱和上限，因此 cur 已达到上限时
            // 仍需保留这一个越界事件以保持 bit-exact 结果。
            let last_level = if cur < max_level { max_level } else { cur + 1 };
            // 乘法只估算最后一个事件的位置，边界用原来的除法条件
            // 校正，避免为预计数重新计算每个事件的阈值。
            let mut end_level = ((t_end * val as f64).ceil() as i32 - 1)
                .min(last_level)
                .max(cur);
            while end_level > cur && end_level as f64 / val as f64 >= t_end {
                end_level -= 1;
            }
            while end_level < last_level && (end_level + 1) as f64 / (val as f64) < t_end
            {
                end_level += 1;
            }
            levels[idx] |= end_level;
            required_events += (end_level - cur) as usize;
        }
    }
    // 先精确预留事件容量，避免 Vec 按 2 倍策略增长造成每个 worker
    // 长期保留多余的排序缓冲区。
    events.reserve_exact(required_events);
    for (idx, &val) in o_abs.iter().enumerate() {
        if val > 0.0 {
            // The first pass already computed the exact inclusive end bound.
            // Store both bounds in the existing i32 scratch slot, not a new buffer.
            let cur = levels[idx] >> LEVEL_BITS;
            let end_level = levels[idx] & LEVEL_MASK as i32;
            if end_level == cur {
                continue;
            }
            let value = val as f64;
            let dimension_key = (idx as u64) << LEVEL_BITS;
            if end_level == cur + 1 {
                let key = (end_level as f64 / value).to_bits();
                events.push((key & time_mask) | dimension_key | end_level as u64);
                continue;
            }
            let reciprocal = value.recip();
            // Map<RangeInclusive<i32>> has an exact size; Vec::extend reserves
            // once for this range and fills the already reserved event storage.
            events.extend((cur + 1..=end_level).map(|next_level| {
                let key = rescale_event_key(next_level, value, reciprocal, payload_mask);
                (key & time_mask) | dimension_key | next_level as u64
            }));
        }
    }

    events.sort_unstable();
    let event_time = |event: u64| {
        let idx = ((event & payload_mask) >> LEVEL_BITS) as usize;
        (event & LEVEL_MASK) as f64 / o_abs[idx] as f64
    };
    let mut group_start = 0;
    while group_start < events.len() {
        let prefix = events[group_start] & time_mask;
        let mut group_end = group_start + 1;
        while group_end < events.len() && events[group_end] & time_mask == prefix {
            group_end += 1;
        }
        if group_end - group_start > 1 {
            events[group_start..group_end].sort_unstable_by(|&a, &b| {
                event_time(a).total_cmp(&event_time(b)).then_with(|| {
                    ((a & payload_mask) >> LEVEL_BITS)
                        .cmp(&((b & payload_mask) >> LEVEL_BITS))
                })
            });
        }
        group_start = group_end;
    }

    let mut max_ip = 0.0f64;
    let mut conservative_max_squared = 0.0f64;
    // An event always has a positive level. Remember its packed key and divide
    // only for the final winner, instead of every intermediate improvement.
    let mut winning_event = 0u64;
    for &event in events.iter() {
        let idx = ((event & payload_mask) >> LEVEL_BITS) as usize;
        // level 就是旧路径处理这个事件后的 levels[idx]，直接读取即可，
        // 不必再对 level 数组做随机读改写。
        let update = event & LEVEL_MASK;
        sqr_denominator += 2.0 * update as f64;
        numerator += o_abs[idx] as f64;

        // Screen only definite losers. Positive finite f32 magnitudes and
        // bounded levels keep these binary64 products normal and finite.
        // The downward margin covers rounding in sqrt, division and both
        // squared comparisons; near ties still use the original exact score.
        if numerator * numerator < conservative_max_squared * sqr_denominator {
            continue;
        }
        let cur_ip = numerator / sqr_denominator.sqrt();
        if cur_ip > max_ip {
            max_ip = cur_ip;
            conservative_max_squared = (cur_ip * cur_ip) * (1.0 - 16.0 * f64::EPSILON);
            winning_event = event;
        }
    }

    let best_t = if winning_event == 0 {
        t_start
    } else {
        event_time(winning_event)
    };
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
    let mut code = Vec::new();
    let ipnorm_inv = quantize_ex_with_inv_into(o_abs, residual, ex_bits, t, &mut code);
    (code, ipnorm_inv)
}

fn quantize_ex_with_inv_into(
    o_abs: &[f32],
    residual: &[f32],
    ex_bits: usize,
    t: f64,
    code: &mut Vec<u16>,
) -> f32 {
    let dim = o_abs.len();
    code.resize(dim, 0);
    if dim == 0 {
        return 1.0;
    }

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
    ipnorm_inv
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

fn compute_extended_factors_precomputed(
    residual: &[f32],
    centroid: &[f32],
    xu_cb: &[f32],
    l2_sqr: f32,
    dot_residual_centroid: f32,
    ipnorm_inv: f32,
    metric: Metric,
) -> (f32, f32) {
    let l2_norm = l2_sqr.sqrt();
    let ip_resi_xucb = dot(residual, xu_cb);
    let ip_cent_xucb = dot(centroid, xu_cb);
    let safe_denom = if ip_resi_xucb.abs() <= f32::EPSILON {
        f32::INFINITY
    } else {
        ip_resi_xucb
    };
    match metric {
        Metric::L2 => (
            l2_sqr + 2.0 * l2_sqr * ip_cent_xucb / safe_denom,
            -2.0 * l2_norm * ipnorm_inv,
        ),
        Metric::InnerProduct => (
            1.0 - dot_residual_centroid + l2_sqr * ip_cent_xucb / safe_denom,
            -l2_norm * ipnorm_inv,
        ),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    /// The scratch-based path used by the streaming builder must produce the
    /// same packed bytes and scalar parameters as the allocating public path.
    #[test]
    fn parts_path_matches_allocating_path() {
        let mut rng = StdRng::seed_from_u64(781);
        for &dim in &[1usize, 4, 16, 17, 32, 64, 960] {
            for total_bits in 1..=9 {
                // The C++-compatible 1/2/6-bit layouts require dim % 16 == 0.
                if matches!(total_bits, 2 | 3 | 7) && !dim.is_multiple_of(16) {
                    continue;
                }
                for metric in [Metric::L2, Metric::InnerProduct] {
                    let data: Vec<f32> =
                        (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
                    let centroid: Vec<f32> =
                        (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
                    for faster in [false, true] {
                        let config = if faster {
                            RabitqConfig::faster(dim, total_bits, 42)
                        } else {
                            RabitqConfig::new(total_bits)
                        };
                        let expected =
                            quantize_with_centroid(&data, &centroid, &config, metric);
                        let mut scratch = QuantizeScratch::new(dim);
                        let params = quantize_with_centroid_parts_into(
                            &data,
                            &centroid,
                            &config,
                            metric,
                            &mut scratch,
                        );
                        assert_eq!(
                            scratch.packed_binary, expected.binary_code_packed,
                            "binary dim={dim} bits={total_bits} metric={metric:?} faster={faster}"
                        );
                        assert_eq!(
                            scratch.packed_ex, expected.ex_code_packed,
                            "ex dim={dim} bits={total_bits} metric={metric:?} faster={faster}"
                        );
                        let pairs = [
                            (params.f_add, expected.f_add),
                            (params.f_rescale, expected.f_rescale),
                            (params.f_error, expected.f_error),
                            (params.delta, expected.delta),
                            (params.vl, expected.vl),
                            (params.f_add_ex, expected.f_add_ex),
                            (params.f_rescale_ex, expected.f_rescale_ex),
                        ];
                        for (actual, want) in pairs {
                            assert_eq!(
                                actual.to_bits(),
                                want.to_bits(),
                                "param dim={dim} bits={total_bits} metric={metric:?} faster={faster}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn packed_event_rescale_matches_legacy_heap() {
        let mut rng = StdRng::seed_from_u64(20481);
        for ex_bits in 1..=8 {
            for dim in [16usize, 64, 257, 960] {
                for case in 0..24 {
                    let mut values: Vec<f32> =
                        (0..dim).map(|_| rng.gen_range(0.0..1.0)).collect();
                    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
                    for value in &mut values {
                        *value /= norm;
                    }
                    if case == 0 {
                        values.fill(0.0);
                    } else if case == 1 {
                        let first = values[0];
                        values.fill(first);
                    }
                    let fast = best_rescale_factor(&values, ex_bits);
                    let legacy = best_rescale_factor_heap(&values, ex_bits);
                    assert_eq!(
                        fast.to_bits(),
                        legacy.to_bits(),
                        "ex_bits={ex_bits} dim={dim} case={case}"
                    );
                }
            }
        }
    }

    #[test]
    fn reused_scratch_clears_zero_residual_codes() {
        let dim = 16;
        let config = RabitqConfig::new(7);
        let mut scratch = QuantizeScratch::new(dim);
        let centroid = vec![0.5f32; dim];
        let data: Vec<f32> = (0..dim).map(|i| i as f32 * 0.01).collect();
        let _ = quantize_with_centroid_parts_into(
            &data,
            &centroid,
            &config,
            Metric::L2,
            &mut scratch,
        );
        assert!(scratch.ex_code.iter().any(|&code| code != 0));
        let _ = quantize_with_centroid_parts_into(
            &centroid,
            &centroid,
            &config,
            Metric::L2,
            &mut scratch,
        );
        assert!(scratch.ex_code.iter().all(|&code| code == 0));
    }
}
