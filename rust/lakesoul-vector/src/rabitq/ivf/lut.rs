//! FastScan lookup tables — QueryLut, QueryLutHighAcc, QueryPrecomputed,
//! HeapCandidate, and HeapEntry.

use crate::rabitq::simd;
use std::cmp::Ordering;
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

impl QueryLutHighAcc {
    /// Build high-accuracy LUT from rotated query
    /// Splits quantized values into low8 and high8 components
    pub(crate) fn new(rotated_query: &[f32], padded_dim: usize) -> Self {
        assert!(
            padded_dim.is_multiple_of(4),
            "padded_dim must be multiple of 4 for LUT"
        );

        // First compute float values like regular LUT
        // pack_lut_f32 packs 4 dimensions into 16 LUT entries, repeated for each batch
        let lut_size = (padded_dim / 4) * 16 * (padded_dim / 32);
        let mut lut_f32 = vec![0.0f32; lut_size];
        simd::pack_lut_f32(rotated_query, &mut lut_f32);

        // Compute delta and sum_vl_lut
        let sum: f32 = lut_f32.iter().sum();
        let (delta, sum_vl) = if lut_f32.iter().all(|&x| x.abs() < f32::EPSILON) {
            (1.0, 0.0)
        } else {
            let max_val = lut_f32.iter().map(|x| x.abs()).fold(0.0f32, f32::max);
            // Use larger range for high-accuracy mode (16-bit instead of 8-bit)
            let delta = max_val / 32767.0; // i16::MAX
            let sum_vl = sum / delta;
            (delta, sum_vl)
        };

        // Quantize to 16-bit signed values first
        let mut lut_i16 = vec![0i16; lut_f32.len()];
        for (i, &val) in lut_f32.iter().enumerate() {
            let quantized = (val / delta).round();
            lut_i16[i] = quantized.clamp(i16::MIN as f32, i16::MAX as f32) as i16;
        }

        // Split into low8 and high8 components
        let mut lut_low8 = vec![0u8; lut_i16.len()];
        let mut lut_high8 = vec![0u8; lut_i16.len()];

        for (i, &val) in lut_i16.iter().enumerate() {
            // Convert i16 to u16 by adding 32768 (shift to unsigned range)
            let unsigned_val = (val as i32 + 32768) as u16;
            lut_low8[i] = (unsigned_val & 0xFF) as u8;
            lut_high8[i] = ((unsigned_val >> 8) & 0xFF) as u8;
        }

        Self {
            lut_low8,
            lut_high8,
            delta,
            sum_vl_lut: sum_vl,
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
            lut: None,         // LUT built on demand for batch search
            lut_highacc: None, // High-accuracy LUT built on demand for high-dimensional data
        }
    }

    /// Build LUT for FastScan batch search
    /// Should be called once per query before searching clusters
    pub(crate) fn build_lut(&mut self, padded_dim: usize) {
        // Decide whether to use high-accuracy mode based on dimension
        // Use high-accuracy for dimensions > 2048 to prevent overflow
        let use_highacc = padded_dim > 2048;

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
    pub(crate) id: u64,

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
