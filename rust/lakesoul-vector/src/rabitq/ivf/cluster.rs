//! ClusterData — unified memory layout for IVF clusters.
//!
//! Includes V3 persistence I/O primitives, FastScan batch layout helpers,
//! and pending-buffer incremental insert helpers.
use crate::rabitq::quantizer::QuantizedVector;
use crate::rabitq::simd;
use crate::rabitq::{Metric, RabitqError};
use crc32fast::Hasher;
use std::io::{self, Read, Write};

pub(crate) fn write_u32<W: Write>(
    writer: &mut W,
    value: u32,
    hasher: Option<&mut Hasher>,
) -> io::Result<()> {
    let bytes = value.to_le_bytes();
    if let Some(h) = hasher {
        h.update(&bytes);
    }
    writer.write_all(&bytes)
}

pub(crate) fn write_u64<W: Write>(
    writer: &mut W,
    value: u64,
    hasher: Option<&mut Hasher>,
) -> io::Result<()> {
    let bytes = value.to_le_bytes();
    if let Some(h) = hasher {
        h.update(&bytes);
    }
    writer.write_all(&bytes)
}

pub(crate) fn write_f32<W: Write>(
    writer: &mut W,
    value: f32,
    hasher: Option<&mut Hasher>,
) -> io::Result<()> {
    let bytes = value.to_le_bytes();
    if let Some(h) = hasher {
        h.update(&bytes);
    }
    writer.write_all(&bytes)
}

pub(crate) fn read_u8<R: Read>(
    reader: &mut R,
    hasher: Option<&mut Hasher>,
) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    if let Some(h) = hasher {
        h.update(&buf);
    }
    Ok(buf[0])
}

pub(crate) fn read_u32<R: Read>(
    reader: &mut R,
    hasher: Option<&mut Hasher>,
) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    if let Some(h) = hasher {
        h.update(&buf);
    }
    Ok(u32::from_le_bytes(buf))
}

pub(crate) fn read_u64<R: Read>(
    reader: &mut R,
    hasher: Option<&mut Hasher>,
) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    if let Some(h) = hasher {
        h.update(&buf);
    }
    Ok(u64::from_le_bytes(buf))
}

pub(crate) fn read_f32<R: Read>(
    reader: &mut R,
    hasher: Option<&mut Hasher>,
) -> io::Result<f32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    if let Some(h) = hasher {
        h.update(&buf);
    }
    Ok(f32::from_le_bytes(buf))
}

pub(crate) fn usize_from_u64(value: u64) -> Result<usize, RabitqError> {
    usize::try_from(value)
        .map_err(|_| RabitqError::InvalidPersistence("value exceeds platform limits"))
}

pub(crate) fn metric_to_tag(metric: Metric) -> u8 {
    match metric {
        Metric::L2 => 0,
        Metric::InnerProduct => 1,
    }
}

pub(crate) fn tag_to_metric(tag: u8) -> Option<Metric> {
    match tag {
        0 => Some(Metric::L2),
        1 => Some(Metric::InnerProduct),
        _ => None,
    }
}

pub(crate) const PERSIST_MAGIC: [u8; 4] = *b"RBQ1";
pub(crate) const PERSIST_VERSION: u32 = 3; // V3: Unified memory layout with ClusterData
/// Generic 64-byte cache line aligned wrapper
/// Ensures data is aligned to cache line boundaries for optimal performance
#[repr(C, align(64))]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AlignedVec<T> {
    data: Vec<T>,
}

impl<T> AlignedVec<T> {
    #[allow(dead_code)]
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    #[allow(dead_code)]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }
}

impl<T> std::ops::Deref for AlignedVec<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> std::ops::DerefMut for AlignedVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// Unified cluster data with contiguous memory layout
/// Eliminates V1/V2 split and scattered allocations
/// Reference: OPTIMIZATION_PLAN.md Phase 1
/// 64-byte cache line aligned for better cache efficiency
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub(crate) struct ClusterData {
    /// Cluster centroid
    pub(crate) centroid: Vec<f32>,
    /// Vector IDs (all vectors in cluster)
    pub(crate) ids: Vec<u64>,
    /// Single contiguous memory block for all batches
    /// Layout: [Batch 0][Batch 1]...[Batch N]
    /// Each batch layout:
    ///   - packed_binary_codes: padded_dim * 32 / 8 bytes
    ///   - f_add: 32 * 4 bytes (f32)
    ///   - f_rescale: 32 * 4 bytes (f32)
    ///   - f_error: 32 * 4 bytes (f32)
    pub(crate) batch_data: Vec<u8>,
    /// Packed extended codes (per-vector, C++-style on-demand unpacking)
    /// Each vector: Vec<u8> containing bit-packed ex_code
    /// Memory-efficient: ~(padded_dim * ex_bits / 8) bytes per vector
    /// Unpacked on-demand only after lower-bound filtering (mimics C++ estimator.hpp:85-103)
    /// Reference: OPTIMIZATION_PLAN.md - removes 4.4x memory overhead from pre-unpacking
    pub(crate) ex_codes_packed: Vec<Vec<u8>>,
    /// Per-vector ex parameters
    pub(crate) f_add_ex: Vec<f32>,
    pub(crate) f_rescale_ex: Vec<f32>,
    /// Per-vector reconstruction parameters (for fetch_embedding)
    pub(crate) delta: Vec<f32>,
    pub(crate) vl: Vec<f32>,
    /// Metadata
    pub(crate) num_vectors: usize, // vectors packed into batch_data
    pub(crate) padded_dim: usize,
    pub(crate) ex_bits: usize,
    /// Pending vectors not yet packed into batch_data (O(1) insert).
    /// Flushed in groups of 32 when full.
    pub(crate) pending_ids: Vec<u64>,
    pub(crate) pending_vectors: Vec<QuantizedVector>,
}

impl ClusterData {
    /// Calculate bytes per batch in contiguous layout
    #[inline(always)]
    pub(crate) fn batch_stride(padded_dim: usize) -> usize {
        let binary_codes_bytes = padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        let params_bytes = std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE * 3; // f_add, f_rescale, f_error
        binary_codes_bytes + params_bytes
    }

    /// Get number of complete batches
    #[inline(always)]
    pub(crate) fn num_complete_batches(&self) -> usize {
        self.num_vectors / simd::FASTSCAN_BATCH_SIZE
    }

    /// Get number of remainder vectors
    #[inline(always)]
    pub(crate) fn num_remainder_vectors(&self) -> usize {
        self.num_vectors % simd::FASTSCAN_BATCH_SIZE
    }

    /// Zero-copy access to packed binary codes for a batch
    #[inline(always)]
    pub(crate) fn batch_bin_codes(&self, batch_idx: usize) -> &[u8] {
        let stride = Self::batch_stride(self.padded_dim);
        let offset = batch_idx * stride;
        let len = self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        &self.batch_data[offset..offset + len]
    }

    /// Zero-copy access to f_add parameters for a batch
    #[inline(always)]
    pub(crate) fn batch_f_add(&self, batch_idx: usize) -> &[f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let offset =
            batch_idx * stride + (self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8);
        unsafe {
            std::slice::from_raw_parts(
                self.batch_data[offset..].as_ptr() as *const f32,
                simd::FASTSCAN_BATCH_SIZE,
            )
        }
    }

    /// Zero-copy access to f_rescale parameters for a batch
    #[inline(always)]
    pub(crate) fn batch_f_rescale(&self, batch_idx: usize) -> &[f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let binary_bytes = self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        let f_add_bytes = std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let offset = batch_idx * stride + binary_bytes + f_add_bytes;
        unsafe {
            std::slice::from_raw_parts(
                self.batch_data[offset..].as_ptr() as *const f32,
                simd::FASTSCAN_BATCH_SIZE,
            )
        }
    }

    /// Zero-copy access to f_error parameters for a batch
    #[inline(always)]
    pub(crate) fn batch_f_error(&self, batch_idx: usize) -> &[f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let binary_bytes = self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        let f_add_bytes = std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let f_rescale_bytes = std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let offset = batch_idx * stride + binary_bytes + f_add_bytes + f_rescale_bytes;
        unsafe {
            std::slice::from_raw_parts(
                self.batch_data[offset..].as_ptr() as *const f32,
                simd::FASTSCAN_BATCH_SIZE,
            )
        }
    }

    /// Get f_add for a single vector (for naive search)
    #[allow(dead_code)]
    pub(crate) fn get_vector_f_add(&self, vec_idx: usize) -> f32 {
        let batch_idx = vec_idx / simd::FASTSCAN_BATCH_SIZE;
        let in_batch_idx = vec_idx % simd::FASTSCAN_BATCH_SIZE;
        self.batch_f_add(batch_idx)[in_batch_idx]
    }

    /// Get f_rescale for a single vector (for naive search)
    #[allow(dead_code)]
    pub(crate) fn get_vector_f_rescale(&self, vec_idx: usize) -> f32 {
        let batch_idx = vec_idx / simd::FASTSCAN_BATCH_SIZE;
        let in_batch_idx = vec_idx % simd::FASTSCAN_BATCH_SIZE;
        self.batch_f_rescale(batch_idx)[in_batch_idx]
    }

    /// Mutable access to batch binary codes
    #[allow(dead_code)]
    pub(crate) fn batch_bin_codes_mut(&mut self, batch_idx: usize) -> &mut [u8] {
        let stride = Self::batch_stride(self.padded_dim);
        let offset = batch_idx * stride;
        let len = self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        &mut self.batch_data[offset..offset + len]
    }

    /// Mutable access to f_add parameters
    #[allow(dead_code)]
    pub(crate) fn batch_f_add_mut(&mut self, batch_idx: usize) -> &mut [f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let offset =
            batch_idx * stride + (self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8);
        unsafe {
            std::slice::from_raw_parts_mut(
                self.batch_data[offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            )
        }
    }

    /// Mutable access to f_rescale parameters
    #[allow(dead_code)]
    pub(crate) fn batch_f_rescale_mut(&mut self, batch_idx: usize) -> &mut [f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let binary_bytes = self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        let f_add_bytes = std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let offset = batch_idx * stride + binary_bytes + f_add_bytes;
        unsafe {
            std::slice::from_raw_parts_mut(
                self.batch_data[offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            )
        }
    }

    /// Mutable access to f_error parameters
    #[allow(dead_code)]
    pub(crate) fn batch_f_error_mut(&mut self, batch_idx: usize) -> &mut [f32] {
        let stride = Self::batch_stride(self.padded_dim);
        let binary_bytes = self.padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        let f_add_bytes = std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let f_rescale_bytes = std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let offset = batch_idx * stride + binary_bytes + f_add_bytes + f_rescale_bytes;
        unsafe {
            std::slice::from_raw_parts_mut(
                self.batch_data[offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            )
        }
    }

    /// Reconstruct a ClusterData from deserialised segment data (used during
    /// incremental flush to merge old on-disk data with new pending vectors).

    pub(crate) fn from_segment(seg: crate::rabitq::manifest::ClusterSegmentData) -> Self {
        let nv = seg.ids.len();
        Self {
            centroid: seg.centroid,
            ids: seg.ids,
            batch_data: seg.batch_data,
            ex_codes_packed: seg.ex_codes_packed,
            f_add_ex: seg.f_add_ex,
            f_rescale_ex: seg.f_rescale_ex,
            delta: seg.delta,
            vl: seg.vl,
            num_vectors: nv,
            padded_dim: seg.padded_dim,
            ex_bits: seg.ex_bits,
            pending_ids: Vec::new(),
            pending_vectors: Vec::new(),
        }
    }

    /// Create new empty cluster
    #[allow(dead_code)]
    pub(crate) fn new(centroid: Vec<f32>, padded_dim: usize, ex_bits: usize) -> Self {
        Self {
            centroid,
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
        }
    }

    /// Calculate memory usage in bytes

    pub(crate) fn memory_usage(&self) -> usize {
        let ex_codes_heap: usize =
            self.ex_codes_packed.iter().map(|v| v.capacity()).sum();
        std::mem::size_of::<Self>()
            + self.centroid.len() * 4
            + self.ids.capacity() * std::mem::size_of::<usize>()
            + self.batch_data.capacity()
            + self.ex_codes_packed.capacity() * std::mem::size_of::<Vec<u8>>()
            + ex_codes_heap
            + self.f_add_ex.capacity() * 4
            + self.f_rescale_ex.capacity() * 4
            + self.delta.capacity() * 4
            + self.vl.capacity() * 4
    }

    /// Build ClusterData from quantized vectors
    /// Implements unified memory layout with:
    /// 1. Contiguous batch storage (eliminates scattered Vec allocations)
    /// 2. Pre-unpacked ex_codes (avoids repeated unpacking)

    pub(crate) fn from_quantized_vectors(
        centroid: Vec<f32>,
        ids: Vec<u64>,
        vectors: Vec<QuantizedVector>,
        padded_dim: usize,
        ex_bits: usize,
    ) -> Self {
        let num_vectors = vectors.len();
        let num_complete_batches = num_vectors / simd::FASTSCAN_BATCH_SIZE;
        let num_remainder = num_vectors % simd::FASTSCAN_BATCH_SIZE;
        let total_batches = if num_remainder > 0 {
            num_complete_batches + 1
        } else {
            num_complete_batches
        };
        // Allocate contiguous memory for all batches
        let stride = Self::batch_stride(padded_dim);
        let mut batch_data =
            crate::rabitq::memory::allocate_aligned_vec::<u8>(stride * total_batches);
        // Pre-allocate storage for packed ex_codes and parameters
        let mut ex_codes_packed = Vec::with_capacity(num_vectors);
        let mut f_add_ex = Vec::with_capacity(num_vectors);
        let mut f_rescale_ex = Vec::with_capacity(num_vectors);
        let mut delta = Vec::with_capacity(num_vectors);
        let mut vl = Vec::with_capacity(num_vectors);
        let dim_bytes = padded_dim / 8;
        // Process complete batches
        for batch_idx in 0..num_complete_batches {
            let start_idx = batch_idx * simd::FASTSCAN_BATCH_SIZE;
            let end_idx = start_idx + simd::FASTSCAN_BATCH_SIZE;
            let batch_vectors = &vectors[start_idx..end_idx];
            // Pack this batch into contiguous memory
            Self::pack_batch_into_memory(
                batch_vectors,
                &mut batch_data,
                batch_idx,
                padded_dim,
                ex_bits,
                &mut ex_codes_packed,
                &mut f_add_ex,
                &mut f_rescale_ex,
                &mut delta,
                &mut vl,
            );
        }
        // Process remainder vectors
        if num_remainder > 0 {
            let start_idx = num_complete_batches * simd::FASTSCAN_BATCH_SIZE;
            let batch_vectors = &vectors[start_idx..];
            // Pad with zeros
            let mut padded_vectors = batch_vectors.to_vec();
            let ex_bytes_per_vec = if ex_bits > 0 {
                padded_dim * ex_bits / 8
            } else {
                0
            };
            padded_vectors.resize(
                simd::FASTSCAN_BATCH_SIZE,
                QuantizedVector {
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
                },
            );
            // Call pack_batch_into_memory_partial to handle partial batches correctly
            Self::pack_batch_into_memory_partial(
                &padded_vectors,
                num_remainder, // actual number of vectors
                &mut batch_data,
                num_complete_batches,
                padded_dim,
                ex_bits,
                &mut ex_codes_packed,
                &mut f_add_ex,
                &mut f_rescale_ex,
                &mut delta,
                &mut vl,
            );
        }
        Self {
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
        }
    }

    /// Pack 32 vectors into contiguous batch memory
    /// Pack a partial batch into memory (handles the last batch with < 32 vectors)
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn pack_batch_into_memory_partial(
        vectors: &[QuantizedVector],
        actual_count: usize, // actual number of vectors (before padding)
        batch_data: &mut [u8],
        batch_idx: usize,
        padded_dim: usize,
        ex_bits: usize,
        ex_codes_packed: &mut Vec<Vec<u8>>,
        f_add_ex: &mut Vec<f32>,
        f_rescale_ex: &mut Vec<f32>,
        delta: &mut Vec<f32>,
        vl: &mut Vec<f32>,
    ) {
        assert_eq!(vectors.len(), simd::FASTSCAN_BATCH_SIZE);
        assert!(actual_count <= simd::FASTSCAN_BATCH_SIZE);
        let stride = Self::batch_stride(padded_dim);
        let dim_bytes = padded_dim / 8;
        // Calculate offsets in contiguous memory
        let batch_offset = batch_idx * stride;
        let binary_bytes = padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        // Collect binary codes into flat buffer (including padding)
        let mut binary_codes_flat =
            Vec::with_capacity(simd::FASTSCAN_BATCH_SIZE * dim_bytes);
        for vec in vectors.iter() {
            binary_codes_flat.extend_from_slice(&vec.binary_code_packed);
        }
        // Pack binary codes using FastScan layout
        let packed_codes = &mut batch_data[batch_offset..batch_offset + binary_bytes];
        simd::pack_codes(
            &binary_codes_flat,
            simd::FASTSCAN_BATCH_SIZE,
            dim_bytes,
            packed_codes,
        );
        // Get mutable slices for parameters
        let f_add_offset = batch_offset + binary_bytes;
        let f_rescale_offset =
            f_add_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let f_error_offset =
            f_rescale_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        unsafe {
            let f_add_slice = std::slice::from_raw_parts_mut(
                batch_data[f_add_offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            );
            let f_rescale_slice = std::slice::from_raw_parts_mut(
                batch_data[f_rescale_offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            );
            let f_error_slice = std::slice::from_raw_parts_mut(
                batch_data[f_error_offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            );
            // Copy parameters (including padding)
            for (i, vec) in vectors.iter().enumerate() {
                f_add_slice[i] = vec.f_add;
                f_rescale_slice[i] = vec.f_rescale;
                f_error_slice[i] = vec.f_error;
            }
        }
        // Store packed ex_codes - ONLY for actual vectors, not padding!
        // C++-style: Keep codes packed, unpack on-demand during search
        for vec in vectors.iter().take(actual_count) {
            // Store packed ex_code (no unpacking!)
            if ex_bits > 0 {
                ex_codes_packed.push(vec.ex_code_packed.clone());
                f_add_ex.push(vec.f_add_ex);
                f_rescale_ex.push(vec.f_rescale_ex);
            } else {
                ex_codes_packed.push(Vec::new());
                f_add_ex.push(0.0);
                f_rescale_ex.push(0.0);
            }
            // Store reconstruction parameters
            delta.push(vec.delta);
            vl.push(vec.vl);
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn pack_batch_into_memory(
        vectors: &[QuantizedVector],
        batch_data: &mut [u8],
        batch_idx: usize,
        padded_dim: usize,
        ex_bits: usize,
        ex_codes_packed: &mut Vec<Vec<u8>>,
        f_add_ex: &mut Vec<f32>,
        f_rescale_ex: &mut Vec<f32>,
        delta: &mut Vec<f32>,
        vl: &mut Vec<f32>,
    ) {
        assert_eq!(vectors.len(), simd::FASTSCAN_BATCH_SIZE);
        let stride = Self::batch_stride(padded_dim);
        let dim_bytes = padded_dim / 8;
        // Calculate offsets in contiguous memory
        let batch_offset = batch_idx * stride;
        let binary_bytes = padded_dim * simd::FASTSCAN_BATCH_SIZE / 8;
        // Collect binary codes into flat buffer
        let mut binary_codes_flat =
            Vec::with_capacity(simd::FASTSCAN_BATCH_SIZE * dim_bytes);
        for vec in vectors.iter() {
            binary_codes_flat.extend_from_slice(&vec.binary_code_packed);
        }
        // Pack binary codes using FastScan layout
        let packed_codes = &mut batch_data[batch_offset..batch_offset + binary_bytes];
        simd::pack_codes(
            &binary_codes_flat,
            simd::FASTSCAN_BATCH_SIZE,
            dim_bytes,
            packed_codes,
        );
        // Get mutable slices for parameters (using pointer arithmetic like C++)
        let f_add_offset = batch_offset + binary_bytes;
        let f_rescale_offset =
            f_add_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        let f_error_offset =
            f_rescale_offset + std::mem::size_of::<f32>() * simd::FASTSCAN_BATCH_SIZE;
        unsafe {
            let f_add_slice = std::slice::from_raw_parts_mut(
                batch_data[f_add_offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            );
            let f_rescale_slice = std::slice::from_raw_parts_mut(
                batch_data[f_rescale_offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            );
            let f_error_slice = std::slice::from_raw_parts_mut(
                batch_data[f_error_offset..].as_mut_ptr() as *mut f32,
                simd::FASTSCAN_BATCH_SIZE,
            );
            // Copy parameters
            for (i, vec) in vectors.iter().enumerate() {
                f_add_slice[i] = vec.f_add;
                f_rescale_slice[i] = vec.f_rescale;
                f_error_slice[i] = vec.f_error;
            }
        }
        // Store packed ex_codes for all vectors in batch
        // C++-style: Keep codes packed, unpack on-demand during search
        for vec in vectors.iter() {
            // Store packed ex_code (no unpacking!)
            if ex_bits > 0 {
                ex_codes_packed.push(vec.ex_code_packed.clone());
                f_add_ex.push(vec.f_add_ex);
                f_rescale_ex.push(vec.f_rescale_ex);
            } else {
                ex_codes_packed.push(Vec::new());
                f_add_ex.push(0.0);
                f_rescale_ex.push(0.0);
            }
            // Store reconstruction parameters
            delta.push(vec.delta);
            vl.push(vec.vl);
        }
    }
}
// ============================================================================
// Quantized vector for temporary use during training
// ============================================================================
/// Temporary structure used during index construction
/// After construction, vectors are packed into ClusterData's unified memory layout
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct TemporaryQuantizedVector {
    binary_code_packed: Vec<u8>,
    ex_code_packed: Vec<u8>,
    f_add: f32,
    f_rescale: f32,
    f_error: f32,
    f_add_ex: f32,
    f_rescale_ex: f32,
}
