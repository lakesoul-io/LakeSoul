//! FastScan batch distance computation shared between IVF and MSTG
//!
//! This module extracts the core FastScan logic that can be reused across
//! different index types (IVF, MSTG, etc.).
#![allow(dead_code)]

use crate::rabitq::QuantizedVector;
use crate::rabitq::simd;

/// Batch size for FastScan SIMD operations (32 vectors per batch)
pub const BATCH_SIZE: usize = simd::FASTSCAN_BATCH_SIZE;

// Single source of truth for the LUT types and high-accuracy policy.
pub(crate) use crate::rabitq::ivf::lut::{QueryLut, QueryLutHighAcc, should_use_highacc};

/// Precomputed query context for batch search
#[derive(Debug, Clone)]
pub struct QueryContext {
    pub query: Vec<f32>,
    #[allow(dead_code)]
    pub query_norm: f32,
    pub k1x_sum_q: f32, // c1 × sum_query (precomputed)
    #[allow(dead_code)]
    pub kbx_sum_q: f32, // cb × sum_query (precomputed)
    #[allow(dead_code)]
    pub binary_scale: f32,
    /// Optional LUT for FastScan batch search
    pub lut: Option<QueryLut>,
    /// Optional high-accuracy LUT for high-dimensional data
    pub lut_highacc: Option<QueryLutHighAcc>,
}

impl QueryContext {
    /// Create new query context with precomputed values
    pub fn new(query: Vec<f32>, ex_bits: usize) -> Self {
        let sum_query: f32 = query.iter().sum();
        let query_norm: f32 = query.iter().map(|v| v * v).sum::<f32>().sqrt();
        let c1 = -0.5f32;
        let cb = -((1 << ex_bits) as f32 - 0.5);
        let binary_scale = (1 << ex_bits) as f32;

        Self {
            query,
            query_norm,
            k1x_sum_q: c1 * sum_query,
            kbx_sum_q: cb * sum_query,
            binary_scale,
            lut: None,
            lut_highacc: None,
        }
    }

    /// Build LUT for FastScan batch search
    /// Should be called once per query before searching
    pub fn build_lut(&mut self, padded_dim: usize) {
        let use_highacc = should_use_highacc(padded_dim, self.binary_scale > 4.0);

        if use_highacc {
            if self.lut_highacc.is_none() {
                self.lut_highacc = Some(QueryLutHighAcc::new(&self.query, padded_dim));
            }
        } else if self.lut.is_none() {
            self.lut = Some(QueryLut::new(&self.query, padded_dim));
        }
    }
}

/// Batch data with unified memory layout for FastScan
/// Layout: [binary_codes][f_add][f_rescale][f_error]
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BatchData {
    /// Contiguous memory block for batch
    /// Layout per batch (32 vectors):
    ///   - packed_binary_codes: padded_dim * 32 / 8 bytes
    ///   - f_add: 32 * 4 bytes (f32)
    ///   - f_rescale: 32 * 4 bytes (f32)
    ///   - f_error: 32 * 4 bytes (f32)
    pub data: Vec<u8>,
    pub padded_dim: usize,
}

impl BatchData {
    /// Calculate bytes per batch in contiguous layout
    #[inline(always)]
    pub fn batch_stride(padded_dim: usize) -> usize {
        let binary_codes_bytes = padded_dim * BATCH_SIZE / 8;
        let params_bytes = std::mem::size_of::<f32>() * BATCH_SIZE * 3; // f_add, f_rescale, f_error
        binary_codes_bytes + params_bytes
    }

    /// Zero-copy access to packed binary codes for a batch
    #[inline(always)]
    pub fn batch_bin_codes(&self, batch_idx: usize) -> &[u8] {
        let stride = Self::batch_stride(self.padded_dim);
        let offset = batch_idx * stride;
        let len = self.padded_dim * BATCH_SIZE / 8;
        &self.data[offset..offset + len]
    }

    /// Zero-copy access to f_add parameters for a batch
    #[inline(always)]
    pub fn batch_f_add(&self, batch_idx: usize) -> &[f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let binary_bytes = self.padded_dim * BATCH_SIZE / 8;
        let offset = batch_idx * stride + binary_bytes;
        unsafe {
            std::slice::from_raw_parts(
                self.data[offset..].as_ptr() as *const f32,
                BATCH_SIZE,
            )
        }
    }

    /// Zero-copy access to f_rescale parameters for a batch
    #[inline(always)]
    pub fn batch_f_rescale(&self, batch_idx: usize) -> &[f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let binary_bytes = self.padded_dim * BATCH_SIZE / 8;
        let f_add_bytes = std::mem::size_of::<f32>() * BATCH_SIZE;
        let offset = batch_idx * stride + binary_bytes + f_add_bytes;
        unsafe {
            std::slice::from_raw_parts(
                self.data[offset..].as_ptr() as *const f32,
                BATCH_SIZE,
            )
        }
    }

    /// Zero-copy access to f_error parameters for a batch
    #[inline(always)]
    pub fn batch_f_error(&self, batch_idx: usize) -> &[f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let binary_bytes = self.padded_dim * BATCH_SIZE / 8;
        let f_add_bytes = std::mem::size_of::<f32>() * BATCH_SIZE;
        let f_rescale_bytes = std::mem::size_of::<f32>() * BATCH_SIZE;
        let offset = batch_idx * stride + binary_bytes + f_add_bytes + f_rescale_bytes;
        unsafe {
            std::slice::from_raw_parts(
                self.data[offset..].as_ptr() as *const f32,
                BATCH_SIZE,
            )
        }
    }

    /// Pack vectors into batch data
    pub fn pack_batch(
        vectors: &[QuantizedVector],
        padded_dim: usize,
        ex_bits: usize,
    ) -> (Self, Vec<Vec<u8>>, Vec<f32>, Vec<f32>) {
        let num_vectors = vectors.len();
        let num_complete_batches = num_vectors / BATCH_SIZE;
        let num_remainder = num_vectors % BATCH_SIZE;

        let total_batches = if num_remainder > 0 {
            num_complete_batches + 1
        } else {
            num_complete_batches
        };

        // Allocate contiguous memory for all batches
        let stride = Self::batch_stride(padded_dim);
        let mut data =
            crate::rabitq::memory::allocate_aligned_vec::<u8>(stride * total_batches);

        // Pre-allocate storage for packed ex_codes and parameters
        let mut ex_codes_packed = Vec::with_capacity(num_vectors);
        let mut f_add_ex = Vec::with_capacity(num_vectors);
        let mut f_rescale_ex = Vec::with_capacity(num_vectors);

        let dim_bytes = padded_dim / 8;

        // Process complete batches
        for batch_idx in 0..num_complete_batches {
            let start_idx = batch_idx * BATCH_SIZE;
            let end_idx = start_idx + BATCH_SIZE;
            let batch_vectors = &vectors[start_idx..end_idx];

            Self::pack_batch_into_memory(
                batch_vectors,
                &mut data,
                batch_idx,
                padded_dim,
                ex_bits,
                &mut ex_codes_packed,
                &mut f_add_ex,
                &mut f_rescale_ex,
            );
        }

        // Process remainder vectors (pad with zeros)
        if num_remainder > 0 {
            let start_idx = num_complete_batches * BATCH_SIZE;
            let batch_vectors = &vectors[start_idx..];

            let mut padded_vectors = batch_vectors.to_vec();
            let ex_bytes_per_vec = if ex_bits > 0 {
                padded_dim * ex_bits / 8
            } else {
                0
            };
            padded_vectors.resize(
                BATCH_SIZE,
                QuantizedVector {
                    binary_code_packed: vec![0u8; dim_bytes],
                    ex_code_packed: vec![0u8; ex_bytes_per_vec],
                    ex_bits: ex_bits as u8,
                    dim: padded_dim,
                    delta: 0.0,
                    vl: 0.0,
                    f_add: 0.0,
                    f_rescale: 1.0,
                    f_error: 0.0,
                    residual_norm: 0.0,
                    f_add_ex: 0.0,
                    f_rescale_ex: 1.0,
                },
            );

            Self::pack_batch_into_memory(
                &padded_vectors,
                &mut data,
                num_complete_batches,
                padded_dim,
                ex_bits,
                &mut ex_codes_packed,
                &mut f_add_ex,
                &mut f_rescale_ex,
            );
        }

        (
            Self { data, padded_dim },
            ex_codes_packed,
            f_add_ex,
            f_rescale_ex,
        )
    }

    /// Pack a single batch into contiguous memory
    #[allow(clippy::too_many_arguments)]
    fn pack_batch_into_memory(
        batch_vectors: &[QuantizedVector],
        batch_data: &mut [u8],
        batch_idx: usize,
        padded_dim: usize,
        _ex_bits: usize,
        ex_codes_packed: &mut Vec<Vec<u8>>,
        f_add_ex: &mut Vec<f32>,
        f_rescale_ex: &mut Vec<f32>,
    ) {
        let stride = Self::batch_stride(padded_dim);
        let batch_offset = batch_idx * stride;
        let binary_bytes = padded_dim * BATCH_SIZE / 8;
        let dim_bytes = padded_dim / 8;

        // Collect binary codes into flat buffer
        let mut binary_codes_flat = Vec::with_capacity(BATCH_SIZE * dim_bytes);
        for vec in batch_vectors.iter() {
            binary_codes_flat.extend_from_slice(&vec.binary_code_packed);
        }

        // Pack binary codes using FastScan layout (uses simd::pack_codes)
        let packed_codes = &mut batch_data[batch_offset..batch_offset + binary_bytes];
        crate::rabitq::simd::pack_codes(
            &binary_codes_flat,
            BATCH_SIZE,
            dim_bytes,
            packed_codes,
        );

        // Pack f_add, f_rescale, f_error as contiguous arrays
        let f_add_offset = batch_offset + binary_bytes;
        let f_rescale_offset = f_add_offset + BATCH_SIZE * 4;
        let f_error_offset = f_rescale_offset + BATCH_SIZE * 4;

        for vec_idx in 0..BATCH_SIZE {
            let qvec = &batch_vectors[vec_idx];

            // f_add
            let f_add_bytes = qvec.f_add.to_le_bytes();
            batch_data[f_add_offset + vec_idx * 4..f_add_offset + vec_idx * 4 + 4]
                .copy_from_slice(&f_add_bytes);

            // f_rescale
            let f_rescale_bytes = qvec.f_rescale.to_le_bytes();
            batch_data
                [f_rescale_offset + vec_idx * 4..f_rescale_offset + vec_idx * 4 + 4]
                .copy_from_slice(&f_rescale_bytes);

            // f_error
            let f_error_bytes = qvec.f_error.to_le_bytes();
            batch_data[f_error_offset + vec_idx * 4..f_error_offset + vec_idx * 4 + 4]
                .copy_from_slice(&f_error_bytes);

            // Store ex_code and ex parameters
            ex_codes_packed.push(qvec.ex_code_packed.clone());
            f_add_ex.push(qvec.f_add_ex);
            f_rescale_ex.push(qvec.f_rescale_ex);
        }
    }
}
