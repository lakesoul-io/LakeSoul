//! FastScan lookup tables — QueryLut, QueryLutHighAcc, QueryPrecomputed,
//! HeapCandidate, and HeapEntry.

use crate::rabitq::simd;
use std::cmp::Ordering;
#[derive(Debug, Clone)]
pub(crate) struct QueryLut {
    /// Quantized lookup table (i8 values for SIMD)
    pub(crate) lut_i8: Vec<i8>,

    /// Quantization delta factor
    pub(crate) delta: f32,

    /// Sum of vl across all lookup tables
    pub(crate) sum_vl_lut: f32,
}

/// High-accuracy LUT with separated low8/high8 components
/// Used for high-dimensional data (dim > 2048) to prevent overflow
#[derive(Debug, Clone)]
pub(crate) struct QueryLutHighAcc {
    /// Low 8 bits of LUT values (u8 for unsigned values)
    pub(crate) lut_low8: Vec<u8>,

    /// High 8 bits of LUT values (u8 for unsigned values)
    pub(crate) lut_high8: Vec<u8>,

    /// Quantization delta factor
    pub(crate) delta: f32,

    /// Sum of vl across all lookup tables
    pub(crate) sum_vl_lut: f32,
}

/// Select the high-accuracy LUT when the u16 accumulator could overflow or
/// when both the padded dimension and wide extended codes justify its cost.
#[inline]
pub(crate) fn should_use_highacc(padded_dim: usize, has_wide_codes: bool) -> bool {
    padded_dim >= 2048 || (padded_dim > 1024 && has_wide_codes)
}

impl QueryLutHighAcc {
    /// Build a uint16 LUT over its actual range, with separate byte planes.
    ///
    /// Only a 16-entry table lives on the stack. Two passes over the query
    /// avoid retaining a full float LUT and a temporary uint16 LUT. The owned
    /// storage is 8 * padded_dim bytes, below the ordinary LUT builder's peak.
    #[allow(clippy::chunks_exact_to_as_chunks)]
    pub(crate) fn new(query: &[f32], padded_dim: usize) -> Self {
        assert_eq!(
            query.len(),
            padded_dim,
            "query dimension must match padded_dim"
        );
        assert!(
            padded_dim.is_multiple_of(4),
            "padded_dim must be multiple of 4 for LUT"
        );
        let mut table = [0.0f32; 16];
        let mut low = 0.0f32;
        let mut high = 0.0f32;
        for values in query.chunks_exact(4) {
            simd::pack_lut_f32(values, &mut table);
            for &value in &table {
                if value.total_cmp(&low).is_lt() {
                    low = value;
                }
                if value.total_cmp(&high).is_gt() {
                    high = value;
                }
            }
        }
        let delta = (high - low) / u16::MAX as f32;
        let mut lut_low8 = vec![0u8; padded_dim * 4];
        let mut lut_high8 = vec![0u8; padded_dim * 4];
        if delta > 0.0 {
            for (group, values) in query.chunks_exact(4).enumerate() {
                simd::pack_lut_f32(values, &mut table);
                for (entry, &value) in table.iter().enumerate() {
                    let quantized =
                        ((value - low) / delta).round().clamp(0.0, u16::MAX as f32)
                            as u16;
                    lut_low8[group * 16 + entry] = quantized as u8;
                    lut_high8[group * 16 + entry] = (quantized >> 8) as u8;
                }
            }
        }
        Self {
            lut_low8,
            lut_high8,
            delta,
            sum_vl_lut: low * (padded_dim / 4) as f32,
        }
    }
}

impl QueryLut {
    /// Build LUT from rotated query
    /// Reference: C++ Lut constructor in lut.hpp:27-53
    pub(crate) fn new(rotated_query: &[f32], padded_dim: usize) -> Self {
        assert!(
            padded_dim.is_multiple_of(4),
            "padded_dim must be multiple of 4 for LUT"
        );

        let table_length = padded_dim * 4; // padded_dim << 2

        // Step 1: Generate float LUT using pack_lut_f32
        let mut lut_float = vec![0.0f32; table_length];
        simd::pack_lut_f32(rotated_query, &mut lut_float);

        // Step 2: Find min and max of LUT
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

        // Step 3: Compute delta for i8 quantization (8 bits)
        let delta = (vr_lut - vl_lut) / 255.0f32; // (1 << 8) - 1 = 255

        // Step 4: Quantize float LUT to i8
        let mut lut_i8 = vec![0i8; table_length];
        if delta > 0.0 {
            for i in 0..table_length {
                let quantized = ((lut_float[i] - vl_lut) / delta).round();
                // Must convert to u8 first, then to i8 to avoid saturation
                // f32 as i8 saturates at 127, but we need wrapping behavior (128-255 -> -128 to -1)
                lut_i8[i] = (quantized.clamp(0.0, 255.0) as u8) as i8;
            }
        }

        // Step 5: Compute sum_vl_lut
        let num_table = table_length / 16;
        let sum_vl_lut = vl_lut * (num_table as f32);

        Self {
            lut_i8,
            delta,
            sum_vl_lut,
        }
    }
}

/// Precomputed query constants to avoid repeated calculations during search.
pub(crate) struct QueryPrecomputed {
    pub(crate) rotated_query: Vec<f32>,

    pub(crate) query_norm: f32,

    pub(crate) k1x_sum_q: f32, // c1 × sum_query (precomputed)

    pub(crate) kbx_sum_q: f32, // cb × sum_query (precomputed)

    pub(crate) binary_scale: f32,

    /// Number of extended-code bits, used to choose the LUT variant.
    pub(crate) ex_bits: usize,

    /// Optional LUT for FastScan batch search
    pub(crate) lut: Option<QueryLut>,

    /// Optional high-accuracy LUT for high-dimensional data
    pub(crate) lut_highacc: Option<QueryLutHighAcc>,
}

impl QueryPrecomputed {
    pub(crate) fn new(rotated_query: Vec<f32>, ex_bits: usize) -> Self {
        let sum_query: f32 = rotated_query.iter().sum();
        let query_norm: f32 = rotated_query.iter().map(|v| v * v).sum::<f32>().sqrt();
        let c1 = -0.5f32;
        let cb = -((1 << ex_bits) as f32 - 0.5);
        let binary_scale = (1 << ex_bits) as f32;

        Self {
            rotated_query,
            query_norm,
            k1x_sum_q: c1 * sum_query,
            kbx_sum_q: cb * sum_query,
            binary_scale,
            ex_bits,
            lut: None,         // LUT built on demand for batch search
            lut_highacc: None, // High-accuracy LUT built on demand for high-dimensional data
        }
    }

    /// Build LUT for FastScan batch search
    /// Should be called once per query before searching clusters
    pub(crate) fn build_lut(&mut self, padded_dim: usize) {
        // Multi-bit refinement amplifies ordinary byte-LUT error; high
        // dimensions also need 32-bit accumulation to avoid byte-sum overflow.
        let use_highacc = should_use_highacc(padded_dim, self.ex_bits > 2);

        if use_highacc {
            if self.lut_highacc.is_none() {
                self.lut_highacc =
                    Some(QueryLutHighAcc::new(&self.rotated_query, padded_dim));
            }
        } else if self.lut.is_none() {
            self.lut = Some(QueryLut::new(&self.rotated_query, padded_dim));
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HeapCandidate {
    pub(crate) id: crate::rabitq::key::IndexKey,

    /// Segment version the entry came from (newest wins on duplicates).
    pub(crate) version: u32,

    pub(crate) distance: f32,

    pub(crate) score: f32,
}

#[derive(Debug, Clone)]
pub(crate) struct HeapEntry {
    pub(crate) candidate: HeapCandidate,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.candidate
            .distance
            .to_bits()
            .eq(&other.candidate.distance.to_bits())
            && self.candidate.id == other.candidate.id
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
        self.candidate.distance.total_cmp(&other.candidate.distance)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rabitq::simd::FASTSCAN_BATCH_SIZE;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    /// The uint16 LUT and the fused SIMD accumulators must agree with a
    /// direct per-codebook sum for every lane, including the VNNI-sized and
    /// the long-dimension kernels.
    #[test]
    fn highacc_lut_matches_direct_accumulation() {
        let mut rng = StdRng::seed_from_u64(9351);
        for dim in [960usize, 2048] {
            let query: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let lut = QueryLutHighAcc::new(&query, dim);
            assert_eq!(lut.lut_low8.len(), dim * 4);
            assert_eq!(lut.lut_high8.len(), dim * 4);

            let dim_bytes = dim / 8;
            let raw: Vec<u8> = (0..FASTSCAN_BATCH_SIZE * dim_bytes)
                .map(|_| rng.gen_range(0u8..=255))
                .collect();
            let mut packed = vec![0u8; FASTSCAN_BATCH_SIZE * dim_bytes];
            simd::pack_codes(&raw, FASTSCAN_BATCH_SIZE, dim_bytes, &mut packed);
            let mut results = [0i32; FASTSCAN_BATCH_SIZE];
            simd::accumulate_batch_highacc_avx2(
                &packed,
                &lut.lut_low8,
                &lut.lut_high8,
                dim,
                &mut results,
            );

            let mut binary = vec![0u8; dim];
            for (vector, &actual) in results.iter().enumerate() {
                simd::unpack_single_vector(&packed, vector, dim_bytes, &mut binary);
                let mut expected = 0i32;
                for group in 0..dim / 4 {
                    let mut code = 0usize;
                    for bit in 0..4 {
                        if binary[group * 4 + bit] != 0 {
                            code |= 1 << (3 - bit);
                        }
                    }
                    let idx = group * 16 + code;
                    expected += (lut.lut_low8[idx] as u32
                        | ((lut.lut_high8[idx] as u32) << 8))
                        as i32;
                }
                assert_eq!(actual, expected, "dim={dim} vector={vector}");
            }
        }
    }
}
