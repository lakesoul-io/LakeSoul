//! Shared FastScan + refinement kernel used by IVF and MSTG.
#![allow(dead_code)]

use crate::rabitq::{Metric, simd};

/// Number of vectors one FastScan batch processes.
pub const FASTSCAN_BATCH_SIZE: usize = simd::FASTSCAN_BATCH_SIZE;

/// FastScan stage-1 accumulation for one batch of `FASTSCAN_BATCH_SIZE`
/// vectors (runtime dispatch: AVX-512, then AVX2, then scalar).
///
/// Exposed for micro-benchmarks; the search path reaches this through
/// [`compute_fastscan_batch`].
pub fn accumulate_batch(
    packed_codes: &[u8],
    lut_i8: &[i8],
    dim: usize,
    results: &mut [u16; FASTSCAN_BATCH_SIZE],
) {
    simd::accumulate_batch_avx2(packed_codes, lut_i8, dim, results);
}

/// Bitmask of the batch slots that can still enter the current top-k.
///
/// See [`simd::lower_bound_mask`]; exposed so the search loop does not have
/// to reach into the SIMD module.
pub fn candidate_mask(lower_bounds: &[f32; FASTSCAN_BATCH_SIZE], distk: f32) -> u32 {
    simd::lower_bound_mask(lower_bounds, distk)
}

/// Build the i8 FastScan lookup table for one query.
///
/// Returns `(lut_i8, delta, sum_vl_lut)`; exposed for micro-benchmarks.
pub fn build_query_lut(rotated_query: &[f32], padded_dim: usize) -> (Vec<i8>, f32, f32) {
    let lut = crate::rabitq::ivf::lut::QueryLut::new(rotated_query, padded_dim);
    (lut.lut_i8, lut.delta, lut.sum_vl_lut)
}

/// A view over the query LUT used by FastScan accumulation.
#[derive(Clone, Copy)]
pub enum FastScanLutView<'a> {
    Regular {
        lut_i8: &'a [i8],
        delta: f32,
        sum_vl_lut: f32,
    },
    HighAcc {
        lut_low8: &'a [u8],
        lut_high8: &'a [u8],
        delta: f32,
        sum_vl_lut: f32,
    },
}

/// Compute FastScan stage-1 estimates for one batch.
#[allow(clippy::too_many_arguments)]
pub fn compute_fastscan_batch(
    lut_view: FastScanLutView<'_>,
    batch_bin_codes: &[u8],
    padded_dim: usize,
    batch_f_add: &[f32],
    batch_f_rescale: &[f32],
    batch_f_error: &[f32],
    g_add: f32,
    g_error: f32,
    k1x_sum_q: f32,
    ip_x0_qr_values: &mut [f32; simd::FASTSCAN_BATCH_SIZE],
    est_distances: &mut [f32; simd::FASTSCAN_BATCH_SIZE],
    lower_bounds: &mut [f32; simd::FASTSCAN_BATCH_SIZE],
) {
    match lut_view {
        FastScanLutView::Regular {
            lut_i8,
            delta,
            sum_vl_lut,
        } => {
            let mut accu_res = [0u16; simd::FASTSCAN_BATCH_SIZE];
            simd::accumulate_batch_avx2(
                batch_bin_codes,
                lut_i8,
                padded_dim,
                &mut accu_res,
            );
            simd::compute_batch_distances_u16(
                &accu_res,
                delta,
                sum_vl_lut,
                batch_f_add,
                batch_f_rescale,
                batch_f_error,
                g_add,
                g_error,
                k1x_sum_q,
                ip_x0_qr_values,
                est_distances,
                lower_bounds,
            );
        }
        FastScanLutView::HighAcc {
            lut_low8,
            lut_high8,
            delta,
            sum_vl_lut,
        } => {
            let mut accu_res_i32 = [0i32; simd::FASTSCAN_BATCH_SIZE];
            simd::accumulate_batch_highacc_avx2(
                batch_bin_codes,
                lut_low8,
                lut_high8,
                padded_dim,
                &mut accu_res_i32,
            );
            simd::compute_batch_distances_i32(
                &accu_res_i32,
                delta,
                sum_vl_lut,
                batch_f_add,
                batch_f_rescale,
                batch_f_error,
                g_add,
                g_error,
                k1x_sum_q,
                ip_x0_qr_values,
                est_distances,
                lower_bounds,
            );
        }
    }
}

/// Convert potentially non-finite lower bound into a conservative finite value.
#[inline]
pub fn sanitize_lower_bound(
    lower_bound: f32,
    metric: Metric,
    dot_query_centroid: f32,
    query_norm: f32,
) -> f32 {
    if lower_bound.is_finite() {
        return lower_bound;
    }

    match metric {
        Metric::L2 => 0.0,
        Metric::InnerProduct => {
            let max_dot = dot_query_centroid + query_norm;
            -max_dot
        }
    }
}

/// Get packed ex-code dot-product function when the bit-width is SIMD-supported.
#[inline]
pub fn select_ex_ip_func(ex_bits: usize) -> Option<simd::ExIpFunc> {
    match ex_bits {
        0 | 2 | 6 => Some(simd::select_excode_ipfunc(ex_bits)),
        _ => None,
    }
}

/// Stage-2 refinement with extended codes.
#[allow(clippy::too_many_arguments)]
pub fn refine_distance_with_ex(
    query: &[f32],
    ex_code_packed: &[u8],
    padded_dim: usize,
    ex_bits: usize,
    ip_x0_qr: f32,
    binary_scale: f32,
    kbx_sum_q: f32,
    g_add: f32,
    f_add_ex: f32,
    f_rescale_ex: f32,
    ip_func: Option<simd::ExIpFunc>,
) -> f32 {
    let ex_dot = if let Some(ipf) = ip_func {
        ipf(query, ex_code_packed, padded_dim)
    } else if ex_code_packed.is_empty() {
        0.0
    } else {
        let mut ex_code = vec![0u16; padded_dim];
        simd::unpack_ex_code(ex_code_packed, &mut ex_code, padded_dim, ex_bits as u8);
        ex_code
            .iter()
            .zip(query.iter())
            .map(|(&code, &q)| (code as f32) * q)
            .sum()
    };

    let total_term = binary_scale * ip_x0_qr + ex_dot + kbx_sum_q;
    f_add_ex + g_add + f_rescale_ex * total_term
}

#[cfg(test)]
mod mask_tests {
    use super::*;

    #[test]
    fn candidate_mask_covers_all_32_slots() {
        for slot in 0..FASTSCAN_BATCH_SIZE {
            let mut bounds = [100.0f32; FASTSCAN_BATCH_SIZE];
            bounds[slot] = 1.0;
            let mask = candidate_mask(&bounds, 10.0);
            assert_eq!(mask, 1u32 << slot, "slot {slot} must be the only one kept");
        }
    }

    #[test]
    fn candidate_mask_keeps_non_finite_bounds() {
        let mut bounds = [100.0f32; FASTSCAN_BATCH_SIZE];
        bounds[0] = f32::NAN;
        bounds[1] = f32::INFINITY;
        bounds[2] = f32::NEG_INFINITY;
        let mask = candidate_mask(&bounds, 10.0);
        assert_eq!(mask & 0b111, 0b111, "non-finite bounds must be kept");
        assert_eq!(mask.count_ones(), 3);
    }

    #[test]
    fn candidate_mask_prunes_at_or_above_distk() {
        let bounds = [10.0f32; FASTSCAN_BATCH_SIZE];
        assert_eq!(candidate_mask(&bounds, 10.0), 0, "== distk is pruned");
        let bounds = [9.999f32; FASTSCAN_BATCH_SIZE];
        assert_eq!(candidate_mask(&bounds, 10.0), u32::MAX);
    }
}
