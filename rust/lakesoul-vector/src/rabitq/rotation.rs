use rand::prelude::*;
use rand_distr::{Distribution, Normal, Uniform};

use crate::rabitq::RabitqError;
use crate::rabitq::math::{dot, normalize};

/// Type of rotator to use for data transformation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RotatorType {
    /// Matrix-based rotator using Gram-Schmidt orthogonalization
    MatrixRotator = 0,
    /// Fast Hadamard Transform (FHT) with Kac Walk rotator
    FhtKacRotator = 1,
}

impl RotatorType {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(RotatorType::MatrixRotator),
            1 => Some(RotatorType::FhtKacRotator),
            _ => None,
        }
    }

    /// Get padding requirement for the rotator type.
    pub fn padding_requirement(self, dim: usize) -> usize {
        match self {
            RotatorType::MatrixRotator => dim,
            RotatorType::FhtKacRotator => round_up_to_multiple(dim, 64),
        }
    }
}

fn round_up_to_multiple(value: usize, multiple: usize) -> usize {
    value.div_ceil(multiple) * multiple
}

/// Trait for vector rotation operations.
pub trait Rotator: Send + Sync {
    /// Get the original dimension
    fn dim(&self) -> usize;

    /// Get the padded dimension after rotation
    fn padded_dim(&self) -> usize;

    /// Apply rotation to input vector, returning rotated output
    fn rotate(&self, input: &[f32]) -> Vec<f32>;

    /// Apply rotation into an existing buffer
    fn rotate_into(&self, input: &[f32], output: &mut [f32]);

    /// Apply inverse rotation to recover original vector from rotated vector
    fn inverse_rotate(&self, rotated: &[f32]) -> Vec<f32>;

    /// Apply inverse rotation into an existing buffer
    fn inverse_rotate_into(&self, rotated: &[f32], output: &mut [f32]);

    /// Get rotator type
    fn rotator_type(&self) -> RotatorType;

    /// Serialize rotator state for persistence
    fn serialize(&self) -> Vec<u8>;

    /// Deserialize rotator state from bytes
    fn deserialize(
        dim: usize,
        padded_dim: usize,
        data: &[u8],
    ) -> Result<Self, RabitqError>
    where
        Self: Sized;
}

/// Matrix-based rotator using Gram-Schmidt orthogonalization.
#[derive(Debug, Clone)]
pub struct MatrixRotator {
    dim: usize,
    padded_dim: usize,
    matrix: Vec<f32>, // Row-major storage: padded_dim x padded_dim
}

impl MatrixRotator {
    /// Create a new matrix rotator with the provided seed.
    pub fn new(dim: usize, seed: u64) -> Self {
        let padded_dim = RotatorType::MatrixRotator.padding_requirement(dim);
        let mut rng = StdRng::seed_from_u64(seed);
        Self::with_rng(dim, padded_dim, &mut rng)
    }

    fn with_rng(dim: usize, padded_dim: usize, rng: &mut StdRng) -> Self {
        let normal = Normal::new(0.0, 1.0).expect("failed to create normal distribution");
        let mut basis: Vec<Vec<f32>> = Vec::with_capacity(padded_dim);

        for _ in 0..padded_dim {
            let mut vec: Vec<f32> =
                (0..padded_dim).map(|_| normal.sample(rng) as f32).collect();

            // Orthogonalize against previous basis vectors
            for prev in &basis {
                let proj = dot(&vec, prev);
                for (v, p) in vec.iter_mut().zip(prev.iter()) {
                    *v -= proj * *p;
                }
            }

            let mut attempts = 0;
            loop {
                let norm = normalize(&mut vec);
                if norm > f32::EPSILON {
                    break;
                }
                attempts += 1;
                if attempts > 8 {
                    // Fallback to a canonical basis vector
                    vec.fill(0.0);
                    vec[attempts % padded_dim] = 1.0;
                    break;
                }
                for value in vec.iter_mut() {
                    *value = normal.sample(rng) as f32;
                }
                for prev in &basis {
                    let proj = dot(&vec, prev);
                    for (v, p) in vec.iter_mut().zip(prev.iter()) {
                        *v -= proj * *p;
                    }
                }
            }

            basis.push(vec);
        }

        let mut matrix = Vec::with_capacity(padded_dim * padded_dim);
        for row in basis {
            matrix.extend_from_slice(&row);
        }

        Self {
            dim,
            padded_dim,
            matrix,
        }
    }
}

impl Rotator for MatrixRotator {
    fn dim(&self) -> usize {
        self.dim
    }

    fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    fn rotate(&self, input: &[f32]) -> Vec<f32> {
        assert_eq!(input.len(), self.dim);
        let mut output = vec![0.0f32; self.padded_dim];
        self.rotate_into(input, &mut output);
        output
    }

    fn rotate_into(&self, input: &[f32], output: &mut [f32]) {
        assert_eq!(input.len(), self.dim);
        assert_eq!(output.len(), self.padded_dim);

        // Pad input with zeros
        let mut padded_input = vec![0.0f32; self.padded_dim];
        padded_input[..self.dim].copy_from_slice(input);

        for (row_idx, chunk) in self.matrix.chunks(self.padded_dim).enumerate() {
            let mut acc = 0.0f32;
            for (value, &weight) in padded_input.iter().zip(chunk.iter()) {
                acc += value * weight;
            }
            output[row_idx] = acc;
        }
    }

    fn inverse_rotate(&self, rotated: &[f32]) -> Vec<f32> {
        assert_eq!(rotated.len(), self.padded_dim);
        let mut output = vec![0.0f32; self.dim];
        self.inverse_rotate_into(rotated, &mut output);
        output
    }

    #[allow(clippy::needless_range_loop)]
    fn inverse_rotate_into(&self, rotated: &[f32], output: &mut [f32]) {
        assert_eq!(rotated.len(), self.padded_dim);
        assert_eq!(output.len(), self.dim);

        // For orthogonal matrix: inverse = transpose
        // output = R^T * rotated
        // Note: Using range loops for clarity in matrix-vector multiplication
        for col_idx in 0..self.dim {
            let mut acc = 0.0f32;
            for row_idx in 0..self.padded_dim {
                // Access matrix in transposed order
                let matrix_value = self.matrix[row_idx * self.padded_dim + col_idx];
                acc += matrix_value * rotated[row_idx];
            }
            output[col_idx] = acc;
        }
    }

    fn rotator_type(&self) -> RotatorType {
        RotatorType::MatrixRotator
    }

    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for &value in &self.matrix {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    fn deserialize(
        dim: usize,
        padded_dim: usize,
        data: &[u8],
    ) -> Result<Self, RabitqError> {
        let expected_len = padded_dim * padded_dim * 4; // 4 bytes per f32
        if data.len() != expected_len {
            return Err(RabitqError::InvalidPersistence(
                "rotator matrix length mismatch",
            ));
        }

        let mut matrix = Vec::with_capacity(padded_dim * padded_dim);
        for chunk in data.chunks_exact(4) {
            let bytes: [u8; 4] = chunk.try_into().unwrap();
            matrix.push(f32::from_le_bytes(bytes));
        }

        Ok(Self {
            dim,
            padded_dim,
            matrix,
        })
    }
}

/// Fast Hadamard Transform (FHT) rotator with Kac Walk.
/// This matches the C++ FhtKacRotator implementation for performance.
#[derive(Debug, Clone)]
pub struct FhtKacRotator {
    dim: usize,
    padded_dim: usize,
    flip: Vec<u8>, // 4 * padded_dim / 8 bytes of random flip bits
    trunc_dim: usize,
    fac: f32,
}

impl FhtKacRotator {
    /// Create a new FHT rotator with the provided seed.
    pub fn new(dim: usize, seed: u64) -> Self {
        let padded_dim = RotatorType::FhtKacRotator.padding_requirement(dim);
        assert_eq!(
            padded_dim % 64,
            0,
            "FHT rotator requires dimension to be multiple of 64"
        );

        let mut rng = StdRng::seed_from_u64(seed);
        let uniform = Uniform::new_inclusive(0u8, 255u8);

        // Generate 4 sets of random flip bits (4 rounds of FHT)
        let flip_bytes = 4 * padded_dim / 8;
        let flip: Vec<u8> = (0..flip_bytes).map(|_| uniform.sample(&mut rng)).collect();

        // Compute truncated dimension (largest power of 2 <= dim)
        let bottom_log_dim = floor_log2(dim);
        let trunc_dim = 1 << bottom_log_dim;
        let fac = 1.0 / (trunc_dim as f32).sqrt();

        Self {
            dim,
            padded_dim,
            flip,
            trunc_dim,
            fac,
        }
    }

    /// Apply sign flip based on bit mask
    fn flip_sign(data: &mut [f32], flip_bits: &[u8]) {
        for (i, value) in data.iter_mut().enumerate() {
            let byte_idx = i / 8;
            let bit_idx = i % 8;
            if byte_idx < flip_bits.len() {
                let bit = (flip_bits[byte_idx] >> bit_idx) & 1;
                if bit == 1 {
                    *value = -*value;
                }
            }
        }
    }

    /// Fast Hadamard Transform (FHT) for power-of-2 dimensions
    fn fht(data: &mut [f32]) {
        let n = data.len();
        assert!(
            n.is_power_of_two(),
            "FHT requires power-of-2 dimension, got {}",
            n
        );

        let mut h = 1;
        while h < n {
            for i in (0..n).step_by(h * 2) {
                for j in i..(i + h) {
                    let x = data[j];
                    let y = data[j + h];
                    data[j] = x + y;
                    data[j + h] = x - y;
                }
            }
            h *= 2;
        }
    }

    /// Kac's walk: Hadamard-like operation
    fn kacs_walk(data: &mut [f32]) {
        let len = data.len();
        let half = len / 2;
        for i in 0..half {
            let x = data[i];
            let y = data[i + half];
            data[i] = x + y;
            data[i + half] = x - y;
        }
    }

    /// Rescale vector by constant factor
    fn rescale(data: &mut [f32], factor: f32) {
        for value in data.iter_mut() {
            *value *= factor;
        }
    }
}

impl Rotator for FhtKacRotator {
    fn dim(&self) -> usize {
        self.dim
    }

    fn padded_dim(&self) -> usize {
        self.padded_dim
    }

    fn rotate(&self, input: &[f32]) -> Vec<f32> {
        assert_eq!(input.len(), self.dim);
        let mut output = vec![0.0f32; self.padded_dim];
        self.rotate_into(input, &mut output);
        output
    }

    fn rotate_into(&self, input: &[f32], output: &mut [f32]) {
        assert_eq!(input.len(), self.dim);
        assert_eq!(output.len(), self.padded_dim);

        // Copy input and pad with zeros
        output[..self.dim].copy_from_slice(input);
        output[self.dim..].fill(0.0);

        let flip_offset = self.padded_dim / 8;

        if self.trunc_dim == self.padded_dim {
            // Case 1: trunc_dim == padded_dim (dimension is power of 2)
            // Apply 4 rounds of: flip_sign -> FHT -> rescale
            for round in 0..4 {
                let flip_start = round * flip_offset;
                let flip_end = flip_start + flip_offset;
                Self::flip_sign(output, &self.flip[flip_start..flip_end]);
                Self::fht(output);
                Self::rescale(output, self.fac);
            }
        } else {
            // Case 2: trunc_dim < padded_dim (dimension is not power of 2)
            let start = self.padded_dim - self.trunc_dim;

            // Round 1
            Self::flip_sign(output, &self.flip[0..flip_offset]);
            Self::fht(&mut output[..self.trunc_dim]);
            Self::rescale(&mut output[..self.trunc_dim], self.fac);
            Self::kacs_walk(output);

            // Round 2
            Self::flip_sign(output, &self.flip[flip_offset..2 * flip_offset]);
            Self::fht(&mut output[start..]);
            Self::rescale(&mut output[start..], self.fac);
            Self::kacs_walk(output);

            // Round 3
            Self::flip_sign(output, &self.flip[2 * flip_offset..3 * flip_offset]);
            Self::fht(&mut output[..self.trunc_dim]);
            Self::rescale(&mut output[..self.trunc_dim], self.fac);
            Self::kacs_walk(output);

            // Round 4
            Self::flip_sign(output, &self.flip[3 * flip_offset..4 * flip_offset]);
            Self::fht(&mut output[start..]);
            Self::rescale(&mut output[start..], self.fac);
            Self::kacs_walk(output);

            // Final rescale to match C++ normalization
            Self::rescale(output, 0.25);
        }
    }

    fn inverse_rotate(&self, rotated: &[f32]) -> Vec<f32> {
        assert_eq!(rotated.len(), self.padded_dim);
        let mut output = vec![0.0f32; self.dim];
        self.inverse_rotate_into(rotated, &mut output);
        output
    }

    fn inverse_rotate_into(&self, rotated: &[f32], output: &mut [f32]) {
        assert_eq!(rotated.len(), self.padded_dim);
        assert_eq!(output.len(), self.dim);

        // Copy rotated data to temporary buffer (padded_dim size for computation)
        let mut temp = vec![0.0f32; self.padded_dim];
        temp.copy_from_slice(rotated);

        let flip_offset = self.padded_dim / 8;

        if self.trunc_dim == self.padded_dim {
            // Case 1: trunc_dim == padded_dim (dimension is power of 2)
            // Reverse the 4 rounds: each round was (flip -> FHT -> rescale)
            // Inverse: (rescale^-1 -> FHT -> rescale by 1/n -> flip)
            for round in (0..4).rev() {
                let flip_start = round * flip_offset;
                let flip_end = flip_start + flip_offset;

                // Inverse rescale
                Self::rescale(&mut temp, 1.0 / self.fac);
                // FHT (self-inverse up to scaling)
                Self::fht(&mut temp);
                // Normalize by 1/n (where n = padded_dim)
                Self::rescale(&mut temp, 1.0 / (self.padded_dim as f32));
                // Flip sign (self-inverse)
                Self::flip_sign(&mut temp, &self.flip[flip_start..flip_end]);
            }
        } else {
            // Case 2: trunc_dim < padded_dim (dimension is not power of 2)
            let start = self.padded_dim - self.trunc_dim;

            // First, inverse the final rescale(0.25)
            Self::rescale(&mut temp, 4.0);

            // Round 4 inverse: reverse order of (flip -> FHT -> rescale -> kacs)
            // kacs_walk is self-inverse up to factor of 2
            Self::rescale(&mut temp, 0.5);
            Self::kacs_walk(&mut temp);
            Self::rescale(&mut temp[start..], 1.0 / self.fac);
            Self::fht(&mut temp[start..]);
            Self::rescale(&mut temp[start..], 1.0 / (self.trunc_dim as f32));
            Self::flip_sign(&mut temp, &self.flip[3 * flip_offset..4 * flip_offset]);

            // Round 3 inverse
            Self::rescale(&mut temp, 0.5);
            Self::kacs_walk(&mut temp);
            Self::rescale(&mut temp[..self.trunc_dim], 1.0 / self.fac);
            Self::fht(&mut temp[..self.trunc_dim]);
            Self::rescale(&mut temp[..self.trunc_dim], 1.0 / (self.trunc_dim as f32));
            Self::flip_sign(&mut temp, &self.flip[2 * flip_offset..3 * flip_offset]);

            // Round 2 inverse
            Self::rescale(&mut temp, 0.5);
            Self::kacs_walk(&mut temp);
            Self::rescale(&mut temp[start..], 1.0 / self.fac);
            Self::fht(&mut temp[start..]);
            Self::rescale(&mut temp[start..], 1.0 / (self.trunc_dim as f32));
            Self::flip_sign(&mut temp, &self.flip[flip_offset..2 * flip_offset]);

            // Round 1 inverse
            Self::rescale(&mut temp, 0.5);
            Self::kacs_walk(&mut temp);
            Self::rescale(&mut temp[..self.trunc_dim], 1.0 / self.fac);
            Self::fht(&mut temp[..self.trunc_dim]);
            Self::rescale(&mut temp[..self.trunc_dim], 1.0 / (self.trunc_dim as f32));
            Self::flip_sign(&mut temp, &self.flip[0..flip_offset]);
        }

        // Extract original dimension (remove padding)
        output.copy_from_slice(&temp[..self.dim]);
    }

    fn rotator_type(&self) -> RotatorType {
        RotatorType::FhtKacRotator
    }

    fn serialize(&self) -> Vec<u8> {
        // Only store the flip bits (much smaller than full matrix)
        self.flip.clone()
    }

    fn deserialize(
        dim: usize,
        padded_dim: usize,
        data: &[u8],
    ) -> Result<Self, RabitqError> {
        let expected_len = 4 * padded_dim / 8;
        if data.len() != expected_len {
            return Err(RabitqError::InvalidPersistence(
                "FHT rotator flip bits length mismatch",
            ));
        }

        let bottom_log_dim = floor_log2(dim);
        let trunc_dim = 1 << bottom_log_dim;
        let fac = 1.0 / (trunc_dim as f32).sqrt();

        Ok(Self {
            dim,
            padded_dim,
            flip: data.to_vec(),
            trunc_dim,
            fac,
        })
    }
}

/// Compute floor(log2(x)) for positive integers
fn floor_log2(x: usize) -> usize {
    assert!(x > 0, "floor_log2 requires positive input");
    (usize::BITS - 1 - x.leading_zeros()) as usize
}

/// Dynamic rotator that can be either Matrix or FHT based
#[derive(Debug, Clone)]
pub enum DynamicRotator {
    Matrix(MatrixRotator),
    Fht(FhtKacRotator),
}

impl DynamicRotator {
    /// Create a new rotator of the specified type
    pub fn new(dim: usize, rotator_type: RotatorType, seed: u64) -> Self {
        match rotator_type {
            RotatorType::MatrixRotator => {
                DynamicRotator::Matrix(MatrixRotator::new(dim, seed))
            }
            RotatorType::FhtKacRotator => {
                DynamicRotator::Fht(FhtKacRotator::new(dim, seed))
            }
        }
    }

    pub fn dim(&self) -> usize {
        match self {
            DynamicRotator::Matrix(r) => r.dim(),
            DynamicRotator::Fht(r) => r.dim(),
        }
    }

    pub fn padded_dim(&self) -> usize {
        match self {
            DynamicRotator::Matrix(r) => r.padded_dim(),
            DynamicRotator::Fht(r) => r.padded_dim(),
        }
    }

    pub fn rotate(&self, input: &[f32]) -> Vec<f32> {
        match self {
            DynamicRotator::Matrix(r) => r.rotate(input),
            DynamicRotator::Fht(r) => r.rotate(input),
        }
    }

    pub fn rotate_into(&self, input: &[f32], output: &mut [f32]) {
        match self {
            DynamicRotator::Matrix(r) => r.rotate_into(input, output),
            DynamicRotator::Fht(r) => r.rotate_into(input, output),
        }
    }

    pub fn inverse_rotate(&self, rotated: &[f32]) -> Vec<f32> {
        match self {
            DynamicRotator::Matrix(r) => r.inverse_rotate(rotated),
            DynamicRotator::Fht(r) => r.inverse_rotate(rotated),
        }
    }

    pub fn inverse_rotate_into(&self, rotated: &[f32], output: &mut [f32]) {
        match self {
            DynamicRotator::Matrix(r) => r.inverse_rotate_into(rotated, output),
            DynamicRotator::Fht(r) => r.inverse_rotate_into(rotated, output),
        }
    }

    pub fn rotator_type(&self) -> RotatorType {
        match self {
            DynamicRotator::Matrix(r) => r.rotator_type(),
            DynamicRotator::Fht(r) => r.rotator_type(),
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            DynamicRotator::Matrix(r) => r.serialize(),
            DynamicRotator::Fht(r) => r.serialize(),
        }
    }

    pub fn deserialize(
        dim: usize,
        padded_dim: usize,
        rotator_type: RotatorType,
        data: &[u8],
    ) -> Result<Self, RabitqError> {
        match rotator_type {
            RotatorType::MatrixRotator => Ok(DynamicRotator::Matrix(
                MatrixRotator::deserialize(dim, padded_dim, data)?,
            )),
            RotatorType::FhtKacRotator => Ok(DynamicRotator::Fht(
                FhtKacRotator::deserialize(dim, padded_dim, data)?,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_floor_log2() {
        assert_eq!(floor_log2(1), 0);
        assert_eq!(floor_log2(2), 1);
        assert_eq!(floor_log2(3), 1);
        assert_eq!(floor_log2(4), 2);
        assert_eq!(floor_log2(7), 2);
        assert_eq!(floor_log2(8), 3);
        assert_eq!(floor_log2(960), 9);
    }

    #[test]
    fn test_fht_basic() {
        let mut data = vec![1.0, 2.0, 3.0, 4.0];
        FhtKacRotator::fht(&mut data);
        // FHT is self-inverse (up to scaling)
        FhtKacRotator::fht(&mut data);
        for (i, &val) in data.iter().enumerate() {
            assert!((val - (i + 1) as f32 * 4.0).abs() < 1e-5);
        }
    }

    #[test]
    fn test_matrix_rotator_orthogonality() {
        let dim = 16;
        let rotator = MatrixRotator::new(dim, 12345);

        let input = vec![1.0; dim];
        let output = rotator.rotate(&input);

        assert_eq!(output.len(), dim);

        // Rotation should preserve norm (approximately)
        let input_norm: f32 = input.iter().map(|x| x * x).sum::<f32>().sqrt();
        let output_norm: f32 = output.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((input_norm - output_norm).abs() < 1e-4);
    }

    #[test]
    fn test_fht_rotator_basic() {
        let dim = 64;
        let rotator = FhtKacRotator::new(dim, 54321);

        assert_eq!(rotator.dim(), dim);
        assert_eq!(rotator.padded_dim(), 64);

        let input = vec![1.0; dim];
        let output = rotator.rotate(&input);

        assert_eq!(output.len(), 64);
    }

    #[test]
    fn test_fht_rotator_non_power_of_2() {
        let dim = 960; // GIST dataset dimension
        let rotator = FhtKacRotator::new(dim, 98765);

        assert_eq!(rotator.dim(), dim);
        assert_eq!(rotator.padded_dim(), 960); // Already multiple of 64

        let input = vec![1.0; dim];
        let output = rotator.rotate(&input);

        assert_eq!(output.len(), 960);
    }

    #[test]
    fn test_rotator_serialization() {
        let dim = 128;

        // Test MatrixRotator
        let matrix_rot = MatrixRotator::new(dim, 11111);
        let matrix_bytes = matrix_rot.serialize();
        let matrix_rot2 = MatrixRotator::deserialize(dim, dim, &matrix_bytes).unwrap();

        let input = [1.0, 2.0, 3.0]
            .iter()
            .cycle()
            .take(dim)
            .copied()
            .collect::<Vec<_>>();
        let out1 = matrix_rot.rotate(&input);
        let out2 = matrix_rot2.rotate(&input);

        for (a, b) in out1.iter().zip(out2.iter()) {
            assert!((a - b).abs() < 1e-6);
        }

        // Test FhtKacRotator
        let fht_rot = FhtKacRotator::new(dim, 22222);
        let fht_bytes = fht_rot.serialize();
        let fht_rot2 = FhtKacRotator::deserialize(dim, 128, &fht_bytes).unwrap();

        let out3 = fht_rot.rotate(&input);
        let out4 = fht_rot2.rotate(&input);

        for (a, b) in out3.iter().zip(out4.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    #[test]
    fn test_matrix_rotator_inverse() {
        let dim = 64;
        let rotator = MatrixRotator::new(dim, 12345);

        let input = [1.5, 2.3, -0.7, 4.2]
            .iter()
            .cycle()
            .take(dim)
            .copied()
            .collect::<Vec<_>>();

        // Rotate and inverse rotate should recover original
        let rotated = rotator.rotate(&input);
        let recovered = rotator.inverse_rotate(&rotated);

        assert_eq!(recovered.len(), dim);
        for (orig, recov) in input.iter().zip(recovered.iter()) {
            assert!(
                (orig - recov).abs() < 1e-4,
                "Matrix inverse rotation failed: expected {}, got {}",
                orig,
                recov
            );
        }
    }

    #[test]
    fn test_fht_rotator_inverse_power_of_2() {
        let dim = 64;
        let rotator = FhtKacRotator::new(dim, 54321);

        let input = [1.5, 2.3, -0.7, 4.2]
            .iter()
            .cycle()
            .take(dim)
            .copied()
            .collect::<Vec<_>>();

        // Rotate and inverse rotate should recover original
        let rotated = rotator.rotate(&input);
        let recovered = rotator.inverse_rotate(&rotated);

        assert_eq!(recovered.len(), dim);
        for (orig, recov) in input.iter().zip(recovered.iter()) {
            assert!(
                (orig - recov).abs() < 1e-3,
                "FHT inverse rotation failed (power of 2): expected {}, got {}",
                orig,
                recov
            );
        }
    }

    #[test]
    fn test_fht_rotator_inverse_non_power_of_2() {
        let dim = 960; // GIST dataset dimension (not power of 2)
        let rotator = FhtKacRotator::new(dim, 98765);

        let input = [1.5, 2.3, -0.7, 4.2]
            .iter()
            .cycle()
            .take(dim)
            .copied()
            .collect::<Vec<_>>();

        // Rotate and inverse rotate should recover original
        let rotated = rotator.rotate(&input);
        let recovered = rotator.inverse_rotate(&rotated);

        assert_eq!(recovered.len(), dim);
        for (orig, recov) in input.iter().zip(recovered.iter()) {
            assert!(
                (orig - recov).abs() < 1e-3,
                "FHT inverse rotation failed (non power of 2): expected {}, got {}",
                orig,
                recov
            );
        }
    }

    #[test]
    fn test_dynamic_rotator_inverse() {
        let dim = 128;

        // Test MatrixRotator through DynamicRotator
        let matrix_rot = DynamicRotator::new(dim, RotatorType::MatrixRotator, 11111);
        let input = [1.5, 2.3, -0.7]
            .iter()
            .cycle()
            .take(dim)
            .copied()
            .collect::<Vec<_>>();
        let rotated = matrix_rot.rotate(&input);
        let recovered = matrix_rot.inverse_rotate(&rotated);
        for (orig, recov) in input.iter().zip(recovered.iter()) {
            assert!((orig - recov).abs() < 1e-4);
        }

        // Test FhtKacRotator through DynamicRotator
        let fht_rot = DynamicRotator::new(dim, RotatorType::FhtKacRotator, 22222);
        let rotated = fht_rot.rotate(&input);
        let recovered = fht_rot.inverse_rotate(&rotated);
        for (orig, recov) in input.iter().zip(recovered.iter()) {
            assert!((orig - recov).abs() < 1e-3);
        }
    }
}
