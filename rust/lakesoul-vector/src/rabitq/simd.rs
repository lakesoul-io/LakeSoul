//! SIMD-optimized bit packing functions for RaBitQ codes
//!
//! This module provides optimized implementations for packing and unpacking
//! binary codes and extended codes. On x86_64 with AVX512, it uses SIMD
//! instructions. On other platforms (including ARM), it falls back to
//! portable scalar implementations.

/// Pack binary codes (1 bit per element) into bytes
///
/// # Arguments
/// * `binary_code` - Input binary code (1 byte per element, value 0 or 1)
/// * `packed` - Output buffer for packed data
/// * `dim` - Dimension (number of elements)
#[inline]
pub fn pack_binary_code(binary_code: &[u8], packed: &mut [u8], dim: usize) {
    debug_assert_eq!(binary_code.len(), dim);
    debug_assert_eq!(packed.len(), dim.div_ceil(8));

    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        pack_binary_code_avx512(binary_code, packed, dim);
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512f")))]
    {
        pack_binary_code_scalar(binary_code, packed, dim);
    }
}

/// Unpack binary codes from packed bytes
///
/// # Arguments
/// * `packed` - Input packed data
/// * `binary_code` - Output buffer for unpacked binary code
/// * `dim` - Dimension (number of elements)
#[inline]
pub fn unpack_binary_code(packed: &[u8], binary_code: &mut [u8], dim: usize) {
    debug_assert_eq!(packed.len(), dim.div_ceil(8));
    debug_assert_eq!(binary_code.len(), dim);

    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        unpack_binary_code_avx512(packed, binary_code, dim);
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512f")))]
    {
        unpack_binary_code_scalar(packed, binary_code, dim);
    }
}

/// Pack extended codes (ex_bits per element) into bytes
///
/// # Arguments
/// * `ex_code` - Input extended code (u16 per element)
/// * `packed` - Output buffer for packed data
/// * `dim` - Dimension (number of elements)
/// * `ex_bits` - Number of bits per element (1-8)
#[inline]
pub fn pack_ex_code(ex_code: &[u16], packed: &mut [u8], dim: usize, ex_bits: u8) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert!(ex_bits > 0 && ex_bits <= 8);

    if ex_bits == 0 {
        return;
    }

    // Check for special C++ compatible formats first
    if dim.is_multiple_of(16) {
        if ex_bits == 2 {
            pack_ex_code_2bit_cpp_compat(ex_code, packed, dim);
            return;
        } else if ex_bits == 6 {
            pack_ex_code_6bit_cpp_compat(ex_code, packed, dim);
            return;
        }
    }

    let expected_bytes = (dim * ex_bits as usize).div_ceil(8);
    debug_assert_eq!(packed.len(), expected_bytes);

    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        pack_ex_code_avx512(ex_code, packed, dim, ex_bits);
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512f")))]
    {
        pack_ex_code_scalar(ex_code, packed, dim, ex_bits);
    }
}

/// Unpack extended codes from packed bytes
///
/// # Arguments
/// * `packed` - Input packed data
/// * `ex_code` - Output buffer for unpacked extended code
/// * `dim` - Dimension (number of elements)
/// * `ex_bits` - Number of bits per element (1-8)
#[inline]
pub fn unpack_ex_code(packed: &[u8], ex_code: &mut [u16], dim: usize, ex_bits: u8) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert!(ex_bits <= 8);

    if ex_bits == 0 {
        ex_code.fill(0);
        return;
    }

    // Check for special C++ compatible formats first
    // These formats require specific padding (multiple of 16)
    if dim.is_multiple_of(16) {
        if ex_bits == 2 {
            unpack_ex_code_2bit_cpp_compat(packed, ex_code, dim);
            return;
        } else if ex_bits == 6 {
            unpack_ex_code_6bit_cpp_compat(packed, ex_code, dim);
            return;
        }
    }

    let expected_bytes = (dim * ex_bits as usize).div_ceil(8);
    debug_assert_eq!(packed.len(), expected_bytes);

    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        unpack_ex_code_avx512(packed, ex_code, dim, ex_bits);
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx512f")))]
    {
        unpack_ex_code_scalar(packed, ex_code, dim, ex_bits);
    }
}

// ============================================================================
// Scalar implementations (portable, used on ARM and as fallback)
// ============================================================================

#[inline]
fn pack_binary_code_scalar(binary_code: &[u8], packed: &mut [u8], dim: usize) {
    packed.fill(0);
    for (i, &bit) in binary_code.iter().take(dim).enumerate() {
        if bit != 0 {
            // MSB-first packing to match C++ implementation
            // bit 0 goes to MSB (position 7), bit 7 goes to LSB (position 0)
            packed[i / 8] |= 1 << (7 - (i % 8));
        }
    }
}

#[inline]
fn unpack_binary_code_scalar(packed: &[u8], binary_code: &mut [u8], dim: usize) {
    for i in 0..dim {
        // MSB-first unpacking to match C++ implementation
        // bit at position 7 (MSB) is dimension 0, bit at position 0 (LSB) is dimension 7
        binary_code[i] = if (packed[i / 8] & (1 << (7 - (i % 8)))) != 0 {
            1
        } else {
            0
        };
    }
}

#[inline]
fn pack_ex_code_scalar(ex_code: &[u16], packed: &mut [u8], dim: usize, ex_bits: u8) {
    packed.fill(0);
    let bits_per_element = ex_bits as usize;

    for (i, &code) in ex_code.iter().take(dim).enumerate() {
        let bit_offset = i * bits_per_element;
        let byte_offset = bit_offset / 8;
        let bit_in_byte = bit_offset % 8;

        let mut remaining_bits = bits_per_element;
        let mut remaining_code = code;
        let mut current_byte = byte_offset;
        let mut current_bit = bit_in_byte;

        while remaining_bits > 0 {
            let bits_in_current_byte = (8 - current_bit).min(remaining_bits);
            let mask = ((1u16 << bits_in_current_byte) - 1) as u8;
            packed[current_byte] |= ((remaining_code & mask as u16) as u8) << current_bit;

            remaining_code >>= bits_in_current_byte;
            remaining_bits -= bits_in_current_byte;
            current_byte += 1;
            current_bit = 0;
        }
    }
}

#[inline]
fn unpack_ex_code_scalar(packed: &[u8], ex_code: &mut [u16], dim: usize, ex_bits: u8) {
    let bits_per_element = ex_bits as usize;

    #[allow(clippy::needless_range_loop)]
    for i in 0..dim {
        let bit_offset = i * bits_per_element;
        let byte_offset = bit_offset / 8;
        let bit_in_byte = bit_offset % 8;

        let mut code = 0u16;
        let mut remaining_bits = bits_per_element;
        let mut current_byte = byte_offset;
        let mut current_bit = bit_in_byte;
        let mut shift = 0;

        while remaining_bits > 0 {
            let bits_in_current_byte = (8 - current_bit).min(remaining_bits);
            let mask = ((1u16 << bits_in_current_byte) - 1) as u8;
            let bits = ((packed[current_byte] >> current_bit) & mask) as u16;
            code |= bits << shift;

            shift += bits_in_current_byte;
            remaining_bits -= bits_in_current_byte;
            current_byte += 1;
            current_bit = 0;
        }

        ex_code[i] = code;
    }
}

// ============================================================================
// AVX512 implementations (x86_64 only)
// ============================================================================

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_binary_code_avx512(binary_code: &[u8], packed: &mut [u8], dim: usize) {
    // For now, use scalar implementation
    // TODO: Implement AVX512 optimized version
    pack_binary_code_scalar(binary_code, packed, dim);
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_binary_code_avx512(packed: &[u8], binary_code: &mut [u8], dim: usize) {
    // For now, use scalar implementation
    // TODO: Implement AVX512 optimized version
    unpack_binary_code_scalar(packed, binary_code, dim);
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_ex_code_avx512(
    ex_code: &[u16],
    packed: &mut [u8],
    dim: usize,
    ex_bits: u8,
) {
    // Delegate to specific bit-width optimized functions
    match ex_bits {
        1 => pack_1bit_ex_code_avx512(ex_code, packed, dim),
        2 => pack_2bit_ex_code_avx512(ex_code, packed, dim),
        3 => pack_3bit_ex_code_avx512(ex_code, packed, dim),
        4 => pack_4bit_ex_code_avx512(ex_code, packed, dim),
        5 => pack_5bit_ex_code_avx512(ex_code, packed, dim),
        6 => pack_6bit_ex_code_avx512(ex_code, packed, dim),
        7 => pack_7bit_ex_code_avx512(ex_code, packed, dim),
        8 => {
            // 8-bit is just memcpy
            for (i, &code) in ex_code.iter().take(dim).enumerate() {
                packed[i] = code as u8;
            }
        }
        _ => pack_ex_code_scalar(ex_code, packed, dim, ex_bits),
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_ex_code_avx512(
    packed: &[u8],
    ex_code: &mut [u16],
    dim: usize,
    ex_bits: u8,
) {
    // Delegate to specific bit-width optimized functions
    match ex_bits {
        1 => unpack_1bit_ex_code_avx512(packed, ex_code, dim),
        2 => unpack_2bit_ex_code_avx512(packed, ex_code, dim),
        3 => unpack_3bit_ex_code_avx512(packed, ex_code, dim),
        4 => unpack_4bit_ex_code_avx512(packed, ex_code, dim),
        5 => unpack_5bit_ex_code_avx512(packed, ex_code, dim),
        6 => unpack_6bit_ex_code_avx512(packed, ex_code, dim),
        7 => unpack_7bit_ex_code_avx512(packed, ex_code, dim),
        8 => {
            // 8-bit is just memcpy
            for (i, code_out) in ex_code.iter_mut().take(dim).enumerate() {
                *code_out = packed[i] as u16;
            }
        }
        _ => unpack_ex_code_scalar(packed, ex_code, dim, ex_bits),
    }
}

// ============================================================================
// AVX512 bit-specific implementations
// ============================================================================

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_1bit_ex_code_avx512(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    // Requires dim % 16 == 0 for optimal performance
    let chunks = dim / 16;
    let remainder = dim % 16;

    for chunk in 0..chunks {
        let offset = chunk * 16;
        let mut code = 0u16;
        for i in 0..16 {
            code |= (ex_code[offset + i] & 1) << i;
        }
        let out_offset = chunk * 2;
        packed[out_offset..out_offset + 2].copy_from_slice(&code.to_le_bytes());
    }

    // Handle remainder
    if remainder > 0 {
        let offset = chunks * 16;
        let mut code = 0u16;
        for i in 0..remainder {
            code |= (ex_code[offset + i] & 1) << i;
        }
        let out_offset = chunks * 2;
        packed[out_offset..out_offset + 2].copy_from_slice(&code.to_le_bytes());
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_1bit_ex_code_avx512(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    let chunks = dim / 16;
    let remainder = dim % 16;

    for chunk in 0..chunks {
        let in_offset = chunk * 2;
        let code = u16::from_le_bytes([packed[in_offset], packed[in_offset + 1]]);
        let out_offset = chunk * 16;
        for i in 0..16 {
            ex_code[out_offset + i] = (code >> i) & 1;
        }
    }

    // Handle remainder
    if remainder > 0 {
        let in_offset = chunks * 2;
        let code = u16::from_le_bytes([packed[in_offset], packed[in_offset + 1]]);
        let out_offset = chunks * 16;
        for i in 0..remainder {
            ex_code[out_offset + i] = (code >> i) & 1;
        }
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_2bit_ex_code_avx512(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    // Pack 16 2-bit codes into 4 bytes (32 bits)
    let chunks = dim / 16;
    let remainder = dim % 16;

    for chunk in 0..chunks {
        let offset = chunk * 16;
        let mut code = 0u32;
        for i in 0..16 {
            code |= ((ex_code[offset + i] & 0x3) as u32) << (i * 2);
        }
        let out_offset = chunk * 4;
        packed[out_offset..out_offset + 4].copy_from_slice(&code.to_le_bytes());
    }

    // Handle remainder
    if remainder > 0 {
        let offset = chunks * 16;
        let mut code = 0u32;
        for i in 0..remainder {
            code |= ((ex_code[offset + i] & 0x3) as u32) << (i * 2);
        }
        let out_offset = chunks * 4;
        packed[out_offset..out_offset + 4].copy_from_slice(&code.to_le_bytes());
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_2bit_ex_code_avx512(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    let chunks = dim / 16;
    let remainder = dim % 16;

    for chunk in 0..chunks {
        let in_offset = chunk * 4;
        let code = u32::from_le_bytes([
            packed[in_offset],
            packed[in_offset + 1],
            packed[in_offset + 2],
            packed[in_offset + 3],
        ]);
        let out_offset = chunk * 16;
        for i in 0..16 {
            ex_code[out_offset + i] = ((code >> (i * 2)) & 0x3) as u16;
        }
    }

    // Handle remainder
    if remainder > 0 {
        let in_offset = chunks * 4;
        let code = u32::from_le_bytes([
            packed[in_offset],
            packed[in_offset + 1],
            packed[in_offset + 2],
            packed[in_offset + 3],
        ]);
        let out_offset = chunks * 16;
        for i in 0..remainder {
            ex_code[out_offset + i] = ((code >> (i * 2)) & 0x3) as u16;
        }
    }
}

// Placeholder implementations for 3-7 bits
// These can be optimized later with proper SIMD intrinsics

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_3bit_ex_code_avx512(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    pack_ex_code_scalar(ex_code, packed, dim, 3);
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_3bit_ex_code_avx512(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    // Process 8 elements at a time (8 * 3 bits = 24 bits = 3 bytes)
    let chunks = dim / 8;
    let remainder = dim % 8;

    for chunk in 0..chunks {
        let in_offset = chunk * 3;
        let out_offset = chunk * 8;

        // Read 3 bytes = 24 bits containing 8 3-bit values
        let b0 = packed[in_offset] as u32;
        let b1 = packed[in_offset + 1] as u32;
        let b2 = packed[in_offset + 2] as u32;

        // Combine into single 32-bit value
        let data = b0 | (b1 << 8) | (b2 << 16);

        // Extract 8 3-bit values
        ex_code[out_offset] = (data & 0x7) as u16;
        ex_code[out_offset + 1] = ((data >> 3) & 0x7) as u16;
        ex_code[out_offset + 2] = ((data >> 6) & 0x7) as u16;
        ex_code[out_offset + 3] = ((data >> 9) & 0x7) as u16;
        ex_code[out_offset + 4] = ((data >> 12) & 0x7) as u16;
        ex_code[out_offset + 5] = ((data >> 15) & 0x7) as u16;
        ex_code[out_offset + 6] = ((data >> 18) & 0x7) as u16;
        ex_code[out_offset + 7] = ((data >> 21) & 0x7) as u16;
    }

    // Handle remainder with scalar code
    if remainder > 0 {
        let offset = chunks * 8;
        unpack_ex_code_scalar(
            &packed[chunks * 3..],
            &mut ex_code[offset..],
            remainder,
            3,
        );
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_4bit_ex_code_avx512(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    // Pack 2 4-bit codes into 1 byte
    let chunks = dim / 2;
    let remainder = dim % 2;

    for i in 0..chunks {
        let code0 = ex_code[i * 2] & 0xF;
        let code1 = ex_code[i * 2 + 1] & 0xF;
        packed[i] = (code0 as u8) | ((code1 as u8) << 4);
    }

    if remainder > 0 {
        packed[chunks] = (ex_code[chunks * 2] & 0xF) as u8;
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_4bit_ex_code_avx512(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    let chunks = dim / 2;
    let remainder = dim % 2;

    for i in 0..chunks {
        ex_code[i * 2] = (packed[i] & 0xF) as u16;
        ex_code[i * 2 + 1] = (packed[i] >> 4) as u16;
    }

    if remainder > 0 {
        ex_code[chunks * 2] = (packed[chunks] & 0xF) as u16;
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_5bit_ex_code_avx512(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    pack_ex_code_scalar(ex_code, packed, dim, 5);
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_5bit_ex_code_avx512(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    // Process 8 elements at a time (8 * 5 bits = 40 bits = 5 bytes)
    let chunks = dim / 8;
    let remainder = dim % 8;

    for chunk in 0..chunks {
        let in_offset = chunk * 5;
        let out_offset = chunk * 8;

        // Read 5 bytes = 40 bits containing 8 5-bit values
        let b0 = packed[in_offset] as u64;
        let b1 = packed[in_offset + 1] as u64;
        let b2 = packed[in_offset + 2] as u64;
        let b3 = packed[in_offset + 3] as u64;
        let b4 = packed[in_offset + 4] as u64;

        // Combine into single 64-bit value
        let data = b0 | (b1 << 8) | (b2 << 16) | (b3 << 24) | (b4 << 32);

        // Extract 8 5-bit values
        ex_code[out_offset] = (data & 0x1F) as u16;
        ex_code[out_offset + 1] = ((data >> 5) & 0x1F) as u16;
        ex_code[out_offset + 2] = ((data >> 10) & 0x1F) as u16;
        ex_code[out_offset + 3] = ((data >> 15) & 0x1F) as u16;
        ex_code[out_offset + 4] = ((data >> 20) & 0x1F) as u16;
        ex_code[out_offset + 5] = ((data >> 25) & 0x1F) as u16;
        ex_code[out_offset + 6] = ((data >> 30) & 0x1F) as u16;
        ex_code[out_offset + 7] = ((data >> 35) & 0x1F) as u16;
    }

    // Handle remainder with scalar code
    if remainder > 0 {
        let offset = chunks * 8;
        unpack_ex_code_scalar(
            &packed[chunks * 5..],
            &mut ex_code[offset..],
            remainder,
            5,
        );
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_6bit_ex_code_avx512(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    pack_ex_code_scalar(ex_code, packed, dim, 6);
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_6bit_ex_code_avx512(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    // Process 4 elements at a time (4 * 6 bits = 24 bits = 3 bytes)
    let chunks = dim / 4;
    let remainder = dim % 4;

    for chunk in 0..chunks {
        let in_offset = chunk * 3;
        let out_offset = chunk * 4;

        // Read 3 bytes = 24 bits containing 4 6-bit values
        let b0 = packed[in_offset] as u32;
        let b1 = packed[in_offset + 1] as u32;
        let b2 = packed[in_offset + 2] as u32;

        // Combine into single 32-bit value
        let data = b0 | (b1 << 8) | (b2 << 16);

        // Extract 4 6-bit values
        ex_code[out_offset] = (data & 0x3F) as u16;
        ex_code[out_offset + 1] = ((data >> 6) & 0x3F) as u16;
        ex_code[out_offset + 2] = ((data >> 12) & 0x3F) as u16;
        ex_code[out_offset + 3] = ((data >> 18) & 0x3F) as u16;
    }

    // Handle remainder with scalar code
    if remainder > 0 {
        let offset = chunks * 4;
        unpack_ex_code_scalar(
            &packed[chunks * 3..],
            &mut ex_code[offset..],
            remainder,
            6,
        );
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn pack_7bit_ex_code_avx512(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    pack_ex_code_scalar(ex_code, packed, dim, 7);
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn unpack_7bit_ex_code_avx512(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    // Process 8 elements at a time (8 * 7 bits = 56 bits = 7 bytes)
    let chunks = dim / 8;
    let remainder = dim % 8;

    for chunk in 0..chunks {
        let in_offset = chunk * 7;
        let out_offset = chunk * 8;

        // Read 7 bytes = 56 bits containing 8 7-bit values
        let b0 = packed[in_offset] as u64;
        let b1 = packed[in_offset + 1] as u64;
        let b2 = packed[in_offset + 2] as u64;
        let b3 = packed[in_offset + 3] as u64;
        let b4 = packed[in_offset + 4] as u64;
        let b5 = packed[in_offset + 5] as u64;
        let b6 = packed[in_offset + 6] as u64;

        // Combine into single 64-bit value
        let data = b0
            | (b1 << 8)
            | (b2 << 16)
            | (b3 << 24)
            | (b4 << 32)
            | (b5 << 40)
            | (b6 << 48);

        // Extract 8 7-bit values
        ex_code[out_offset] = (data & 0x7F) as u16;
        ex_code[out_offset + 1] = ((data >> 7) & 0x7F) as u16;
        ex_code[out_offset + 2] = ((data >> 14) & 0x7F) as u16;
        ex_code[out_offset + 3] = ((data >> 21) & 0x7F) as u16;
        ex_code[out_offset + 4] = ((data >> 28) & 0x7F) as u16;
        ex_code[out_offset + 5] = ((data >> 35) & 0x7F) as u16;
        ex_code[out_offset + 6] = ((data >> 42) & 0x7F) as u16;
        ex_code[out_offset + 7] = ((data >> 49) & 0x7F) as u16;
    }

    // Handle remainder with scalar code
    if remainder > 0 {
        let offset = chunks * 8;
        unpack_ex_code_scalar(
            &packed[chunks * 7..],
            &mut ex_code[offset..],
            remainder,
            7,
        );
    }
}

/// SIMD-accelerated dot product: u8 vector with f32 vector
/// Converts u8 to f32 and computes dot product
#[inline]
#[allow(dead_code)]
pub fn dot_u8_f32(a: &[u8], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        dot_u8_f32_avx2(a, b)
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        dot_u8_f32_scalar(a, b)
    }
}

/// SIMD-accelerated dot product: u16 vector with f32 vector
/// Converts u16 to f32 and computes dot product
#[inline]
#[allow(dead_code)]
pub fn dot_u16_f32(a: &[u16], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        dot_u16_f32_avx2(a, b)
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        dot_u16_f32_scalar(a, b)
    }
}

#[inline]
#[allow(dead_code)]
fn dot_u8_f32_scalar(a: &[u8], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(&x, &y)| (x as f32) * y).sum()
}

#[inline]
#[allow(dead_code)]
fn dot_u16_f32_scalar(a: &[u16], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(&x, &y)| (x as f32) * y).sum()
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
unsafe fn dot_u8_f32_avx2(a: &[u8], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let len = a.len();
    let chunks = len / 8;
    let remainder = len % 8;

    let mut sum = _mm256_setzero_ps();

    for i in 0..chunks {
        let offset = i * 8;

        // Load 8 u8 values
        let a_u8 = _mm_loadl_epi64(a.as_ptr().add(offset) as *const __m128i);
        // Convert u8 to u32
        let a_u32 = _mm256_cvtepu8_epi32(a_u8);
        // Convert u32 to f32
        let a_f32 = _mm256_cvtepi32_ps(a_u32);

        // Load 8 f32 values
        let b_f32 = _mm256_loadu_ps(b.as_ptr().add(offset));

        // Multiply and accumulate
        sum = _mm256_fmadd_ps(a_f32, b_f32, sum);
    }

    // Horizontal sum
    let mut result = 0.0f32;
    let sum_arr: [f32; 8] = std::mem::transmute(sum);
    for &val in &sum_arr {
        result += val;
    }

    // Handle remainder
    let offset = chunks * 8;
    for i in 0..remainder {
        result += (a[offset + i] as f32) * b[offset + i];
    }

    result
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
unsafe fn dot_u16_f32_avx2(a: &[u16], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let len = a.len();
    let chunks = len / 8;
    let remainder = len % 8;

    let mut sum = _mm256_setzero_ps();

    for i in 0..chunks {
        let offset = i * 8;

        // Load 8 u16 values
        let a_u16 = _mm_loadu_si128(a.as_ptr().add(offset) as *const __m128i);
        // Convert u16 to u32
        let a_u32 = _mm256_cvtepu16_epi32(a_u16);
        // Convert u32 to f32
        let a_f32 = _mm256_cvtepi32_ps(a_u32);

        // Load 8 f32 values
        let b_f32 = _mm256_loadu_ps(b.as_ptr().add(offset));

        // Multiply and accumulate
        sum = _mm256_fmadd_ps(a_f32, b_f32, sum);
    }

    // Horizontal sum
    let mut result = 0.0f32;
    let sum_arr: [f32; 8] = std::mem::transmute(sum);
    for &val in &sum_arr {
        result += val;
    }

    // Handle remainder
    let offset = chunks * 8;
    for i in 0..remainder {
        result += (a[offset + i] as f32) * b[offset + i];
    }

    result
}

// ============================================================================
// FastScan: Batch processing with lookup tables for high-performance search
// ============================================================================

/// Batch size for FastScan (process 32 vectors at once)
pub const FASTSCAN_BATCH_SIZE: usize = 32;

/// Position lookup table for 4-bit codes (all combinations of 4 bits)
const KPOS: [usize; 16] = [3, 3, 2, 3, 1, 3, 2, 3, 0, 3, 2, 3, 1, 3, 2, 3];

/// Permutation for code packing
const KPERM0: [usize; 16] = [0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15];

/// Get the position of the lowest set bit (LOWBIT macro from C++)
#[inline]
fn lowbit(x: usize) -> usize {
    x & (!x + 1)
}

/// Pack lookup table for fast distance estimation
/// For each 4 dimensions, precompute 16 possible values (2^4 combinations)
/// This avoids multiplication during search
///
/// The LUT is stored as i8 values for SIMD shuffle operations
///
/// # Arguments
/// * `query` - Rotated query vector (scaled appropriately)
/// * `lut` - Output lookup table as i8 (size: dim/4 * 16)
#[allow(dead_code)]
pub fn pack_lut_i8(query: &[f32], lut: &mut [i8]) {
    assert!(
        query.len().is_multiple_of(4),
        "Query dimension must be multiple of 4"
    );
    let num_codebook = query.len() / 4;
    assert_eq!(
        lut.len(),
        num_codebook * 16,
        "LUT size must be num_codebook * 16"
    );

    for i in 0..num_codebook {
        let q_offset = i * 4;
        let lut_offset = i * 16;

        lut[lut_offset] = 0;
        for j in 1..16 {
            let prev_idx = j - lowbit(j);
            let sum = (lut[lut_offset + prev_idx] as f32) + query[q_offset + KPOS[j]];
            lut[lut_offset + j] = sum.round() as i8;
        }
    }
}

/// Pack lookup table as f32 (for non-SIMD paths)
pub fn pack_lut_f32(query: &[f32], lut: &mut [f32]) {
    assert!(
        query.len().is_multiple_of(4),
        "Query dimension must be multiple of 4"
    );
    let num_codebook = query.len() / 4;
    assert_eq!(
        lut.len(),
        num_codebook * 16,
        "LUT size must be num_codebook * 16"
    );

    for i in 0..num_codebook {
        let q_offset = i * 4;
        let lut_offset = i * 16;

        lut[lut_offset] = 0.0;
        for j in 1..16 {
            let prev_idx = j - lowbit(j);
            lut[lut_offset + j] = lut[lut_offset + prev_idx] + query[q_offset + KPOS[j]];
        }
    }
}

/// Reverse the bit order within a 4-bit pattern
///
/// Converts LSB-first to MSB-first for FastScan compatibility.
/// Example: 0b0001 (LSB=1) -> 0b1000 (MSB=1)
///          0b0110 (LSB=0,1,1) -> 0b0110 (symmetric)
#[inline]
#[allow(dead_code)]
fn reverse_4bit_pattern(pattern: u8) -> u8 {
    ((pattern & 0x01) << 3)
        | ((pattern & 0x02) << 1)
        | ((pattern & 0x04) >> 1)
        | ((pattern & 0x08) >> 3)
}

/// Pack binary codes into FastScan batch format
/// Reorganizes 32 vectors' codes for SIMD-friendly access pattern
///
/// # Arguments
/// * `codes` - Input codes (num_vectors * dim, where dim is in bytes)
/// * `num_vectors` - Number of vectors (will be rounded up to multiple of 32)
/// * `dim_bytes` - Dimension in bytes (each byte holds codes for 8 dimensions)
/// * `packed` - Output packed data
pub fn pack_codes(codes: &[u8], num_vectors: usize, dim_bytes: usize, packed: &mut [u8]) {
    let num_batches = num_vectors.div_ceil(FASTSCAN_BATCH_SIZE);
    let expected_size = num_batches * FASTSCAN_BATCH_SIZE * dim_bytes;
    assert!(packed.len() >= expected_size, "Packed buffer too small");

    let mut packed_offset = 0;

    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * FASTSCAN_BATCH_SIZE;
        let batch_end = std::cmp::min(batch_start + FASTSCAN_BATCH_SIZE, num_vectors);

        // Process each byte column
        for col in 0..dim_bytes {
            let mut col_data = [0u8; FASTSCAN_BATCH_SIZE];

            // Extract column from all vectors in this batch
            for (i, vec_idx) in (batch_start..batch_end).enumerate() {
                col_data[i] = codes[vec_idx * dim_bytes + col];
            }

            // Split into upper and lower 4 bits
            let mut col_0 = [0u8; FASTSCAN_BATCH_SIZE]; // upper 4 bits
            let mut col_1 = [0u8; FASTSCAN_BATCH_SIZE]; // lower 4 bits

            for j in 0..FASTSCAN_BATCH_SIZE {
                col_0[j] = col_data[j] >> 4;
                col_1[j] = col_data[j] & 15;
            }

            // Pack according to KPERM0 permutation
            for j in 0..16 {
                let val0 = col_0[KPERM0[j]] | (col_0[KPERM0[j] + 16] << 4);
                let val1 = col_1[KPERM0[j]] | (col_1[KPERM0[j] + 16] << 4);
                packed[packed_offset + j] = val0;
                packed[packed_offset + j + 16] = val1;
            }

            packed_offset += 32;
        }
    }
}

/// Unpack a single vector's binary code from FastScan batch layout
/// Reverses the pack_codes operation for one vector
///
/// # Arguments
/// * `packed_codes` - Packed binary codes for a batch (padded_dim * 32 / 8 bytes)
/// * `vec_idx` - Index of vector within batch (0-31)
/// * `dim_bytes` - Number of bytes per vector (padded_dim / 8)
/// * `binary_code` - Output buffer for unpacked binary code (padded_dim elements, each 0 or 1)
#[allow(dead_code)]
pub fn unpack_single_vector(
    packed_codes: &[u8],
    vec_idx: usize,
    dim_bytes: usize,
    binary_code: &mut [u8],
) {
    assert!(vec_idx < FASTSCAN_BATCH_SIZE);
    assert_eq!(binary_code.len(), dim_bytes * 8);
    assert_eq!(packed_codes.len(), dim_bytes * FASTSCAN_BATCH_SIZE);

    // Reverse KPERM0 permutation to find original position
    let mut kperm0_inv = [0usize; FASTSCAN_BATCH_SIZE];
    for (i, &perm_val) in KPERM0.iter().enumerate() {
        kperm0_inv[perm_val] = i;
        kperm0_inv[perm_val + 16] = i + 16;
    }

    let mut packed_offset = 0;

    for col in 0..dim_bytes {
        // Extract the packed data for this column
        let mut col_0 = [0u8; FASTSCAN_BATCH_SIZE]; // upper 4 bits
        let mut col_1 = [0u8; FASTSCAN_BATCH_SIZE]; // lower 4 bits

        // Unpack according to KPERM0 permutation
        for j in 0..16 {
            let val0 = packed_codes[packed_offset + j];
            let val1 = packed_codes[packed_offset + j + 16];

            col_0[KPERM0[j]] = val0 & 15;
            col_0[KPERM0[j] + 16] = val0 >> 4;
            col_1[KPERM0[j]] = val1 & 15;
            col_1[KPERM0[j] + 16] = val1 >> 4;
        }

        // Combine upper and lower 4 bits
        let byte_val = (col_0[vec_idx] << 4) | col_1[vec_idx];

        // Unpack bits into binary_code (each element is 0 or 1)
        for bit in 0..8 {
            binary_code[col * 8 + bit] = (byte_val >> (7 - bit)) & 1;
        }

        packed_offset += 32;
    }
}

/// Accumulate distances for a batch of 32 vectors using FastScan
/// Uses pre-computed lookup table (LUT) and SIMD shuffle for fast distance estimation
/// Automatically dispatches to AVX-512, AVX2, or scalar implementation based on CPU features
///
/// # Arguments
/// * `packed_codes` - Packed binary codes for 32 vectors
/// * `lut` - Pre-computed lookup table (as i8)
/// * `dim` - Dimension (must be multiple of 16 for SIMD)
/// * `results` - Output distances for 32 vectors
#[inline]
pub fn accumulate_batch_avx2(
    packed_codes: &[u8],
    lut: &[i8],
    dim: usize,
    results: &mut [u16; FASTSCAN_BATCH_SIZE],
) {
    assert!(
        dim.is_multiple_of(16),
        "Dimension must be multiple of 16 for SIMD"
    );

    #[cfg(target_arch = "x86_64")]
    {
        // Runtime CPU feature detection with fallback chain: AVX-512 -> AVX2 -> Scalar
        {
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw")
            {
                // Log once per process to avoid spam
                use std::sync::Once;
                static LOG_ONCE: Once = Once::new();
                LOG_ONCE.call_once(|| {
                    eprintln!("[FastScan] Using AVX-512 optimized path");
                });
                unsafe {
                    return accumulate_batch_avx512_impl(packed_codes, lut, dim, results);
                }
            }
        }

        if is_x86_feature_detected!("avx2") {
            use std::sync::Once;
            static LOG_ONCE_AVX2: Once = Once::new();
            LOG_ONCE_AVX2.call_once(|| {
                eprintln!("[FastScan] Using AVX2 optimized path (with prefetch)");
            });
            unsafe {
                return accumulate_batch_avx2_impl(packed_codes, lut, dim, results);
            }
        }
    }

    // Fallback to scalar implementation
    accumulate_batch_scalar(packed_codes, lut, dim, results);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn accumulate_batch_avx2_impl(
    packed_codes: &[u8],
    lut: &[i8],
    dim: usize,
    results: &mut [u16; FASTSCAN_BATCH_SIZE],
) {
    use std::arch::x86_64::*;

    let code_length = dim << 2; // dim * 4
    let low_mask = _mm256_set1_epi8(0x0f);

    // 4 independent accumulators following C++ logic:
    // accu0: accumulates res_lo (vectors 8-15)
    // accu1: accumulates res_lo >> 8 (vectors 0-7)
    // accu2: accumulates res_hi (vectors 24-31)
    // accu3: accumulates res_hi >> 8 (vectors 16-23)
    let mut accu0 = _mm256_setzero_si256();
    let mut accu1 = _mm256_setzero_si256();
    let mut accu2 = _mm256_setzero_si256();
    let mut accu3 = _mm256_setzero_si256();

    // Process 64 bytes at a time (2 * 32) to match C++ implementation
    // Let CPU hardware prefetcher handle memory prefetching automatically
    let mut i = 0;
    while i + 63 < code_length {
        // First 32 bytes
        let c = _mm256_loadu_si256(packed_codes.as_ptr().add(i) as *const __m256i);
        let lut_val = _mm256_loadu_si256(lut.as_ptr().add(i) as *const __m256i);
        let lo = _mm256_and_si256(c, low_mask);
        let hi = _mm256_and_si256(_mm256_srli_epi16(c, 4), low_mask);

        let res_lo = _mm256_shuffle_epi8(lut_val, lo);
        let res_hi = _mm256_shuffle_epi8(lut_val, hi);

        // Accumulate following C++ pattern
        accu0 = _mm256_add_epi16(accu0, res_lo);
        accu1 = _mm256_add_epi16(accu1, _mm256_srli_epi16(res_lo, 8));
        accu2 = _mm256_add_epi16(accu2, res_hi);
        accu3 = _mm256_add_epi16(accu3, _mm256_srli_epi16(res_hi, 8));

        // Second 32 bytes
        let c = _mm256_loadu_si256(packed_codes.as_ptr().add(i + 32) as *const __m256i);
        let lut_val = _mm256_loadu_si256(lut.as_ptr().add(i + 32) as *const __m256i);
        let lo = _mm256_and_si256(c, low_mask);
        let hi = _mm256_and_si256(_mm256_srli_epi16(c, 4), low_mask);

        let res_lo = _mm256_shuffle_epi8(lut_val, lo);
        let res_hi = _mm256_shuffle_epi8(lut_val, hi);

        accu0 = _mm256_add_epi16(accu0, res_lo);
        accu1 = _mm256_add_epi16(accu1, _mm256_srli_epi16(res_lo, 8));
        accu2 = _mm256_add_epi16(accu2, res_hi);
        accu3 = _mm256_add_epi16(accu3, _mm256_srli_epi16(res_hi, 8));

        i += 64;
    }

    // Handle remaining bytes
    while i < code_length {
        let c = _mm256_loadu_si256(packed_codes.as_ptr().add(i) as *const __m256i);
        let lut_val = _mm256_loadu_si256(lut.as_ptr().add(i) as *const __m256i);
        let lo = _mm256_and_si256(c, low_mask);
        let hi = _mm256_and_si256(_mm256_srli_epi16(c, 4), low_mask);

        let res_lo = _mm256_shuffle_epi8(lut_val, lo);
        let res_hi = _mm256_shuffle_epi8(lut_val, hi);

        accu0 = _mm256_add_epi16(accu0, res_lo);
        accu1 = _mm256_add_epi16(accu1, _mm256_srli_epi16(res_lo, 8));
        accu2 = _mm256_add_epi16(accu2, res_hi);
        accu3 = _mm256_add_epi16(accu3, _mm256_srli_epi16(res_hi, 8));

        i += 32;
    }

    // Remove the influence of upper 8 bits for accu0 and accu2
    accu0 = _mm256_sub_epi16(accu0, _mm256_slli_epi16(accu1, 8));
    accu2 = _mm256_sub_epi16(accu2, _mm256_slli_epi16(accu3, 8));

    // Final assembly for vectors 0-15
    let dis0 = _mm256_add_epi16(
        _mm256_permute2f128_si256(accu0, accu1, 0x21),
        _mm256_blend_epi32(accu0, accu1, 0xF0),
    );
    _mm256_storeu_si256(results.as_mut_ptr() as *mut __m256i, dis0);

    // Final assembly for vectors 16-31
    let dis1 = _mm256_add_epi16(
        _mm256_permute2f128_si256(accu2, accu3, 0x21),
        _mm256_blend_epi32(accu2, accu3, 0xF0),
    );
    _mm256_storeu_si256(results.as_mut_ptr().add(16) as *mut __m256i, dis1);
}

/// AVX-512 implementation for accumulate_batch
/// Processes 64 bytes per iteration (double the AVX2 throughput)
/// Requires AVX-512F and AVX-512BW CPU features
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f", enable = "avx512bw")]
unsafe fn accumulate_batch_avx512_impl(
    packed_codes: &[u8],
    lut: &[i8],
    dim: usize,
    results: &mut [u16; FASTSCAN_BATCH_SIZE],
) {
    use std::arch::x86_64::*;

    let code_length = dim << 2; // dim * 4
    let low_mask = _mm512_set1_epi8(0x0f);

    let mut accu0 = _mm512_setzero_si512();
    let mut accu1 = _mm512_setzero_si512();
    let mut accu2 = _mm512_setzero_si512();
    let mut accu3 = _mm512_setzero_si512();

    // Process 64 bytes per iteration (single AVX-512 load vs. two AVX2 loads)
    // This is the key performance advantage: half the loop iterations
    for i in (0..code_length).step_by(64) {
        // Load 64 bytes of codes and LUT in one operation
        let c = _mm512_loadu_si512(packed_codes.as_ptr().add(i) as *const __m512i);
        let lut_val = _mm512_loadu_si512(lut.as_ptr().add(i) as *const __m512i);

        // Extract low and high nibbles
        // lo contains codes for vectors 0-15, hi contains codes for vectors 16-31
        let lo = _mm512_and_si512(c, low_mask);
        let hi = _mm512_and_si512(_mm512_srli_epi16(c, 4), low_mask);

        // Look up distance values using shuffle
        let res_lo = _mm512_shuffle_epi8(lut_val, lo);
        let res_hi = _mm512_shuffle_epi8(lut_val, hi);

        // Accumulate in i16 to avoid overflow
        // Due to data layout (0,8,1,9,2,10,3,11,4,12,5,13,6,14,7,15):
        // - accu0: vectors 8-15 (lower 8 bits in each u16)
        // - accu1: vectors 0-7 (upper 8 bits need extraction)
        // - accu2: vectors 24-31 (lower 8 bits)
        // - accu3: vectors 16-23 (upper 8 bits need extraction)
        accu0 = _mm512_add_epi16(accu0, res_lo);
        accu1 = _mm512_add_epi16(accu1, _mm512_srli_epi16(res_lo, 8));
        accu2 = _mm512_add_epi16(accu2, res_hi);
        accu3 = _mm512_add_epi16(accu3, _mm512_srli_epi16(res_hi, 8));
    }

    // Remove influence of upper 8 bits from accu0 and accu2
    accu0 = _mm512_sub_epi16(accu0, _mm512_slli_epi16(accu1, 8));
    accu2 = _mm512_sub_epi16(accu2, _mm512_slli_epi16(accu3, 8));

    // Combine results from 4 lanes into final output
    // Each accumulator contains 4 x __m128i worth of data that needs to be summed
    // The result is 32 x u16 values that fit in a single __m512i
    let ret1 = _mm512_add_epi16(
        _mm512_mask_blend_epi64(0b11110000, accu0, accu1),
        _mm512_shuffle_i64x2::<0b01001110>(accu0, accu1),
    );
    let ret2 = _mm512_add_epi16(
        _mm512_mask_blend_epi64(0b11110000, accu2, accu3),
        _mm512_shuffle_i64x2::<0b01001110>(accu2, accu3),
    );

    // Final merge: combine ret1 (vecs 0-15) and ret2 (vecs 16-31)
    let mut ret = _mm512_setzero_si512();
    ret = _mm512_add_epi16(ret, _mm512_shuffle_i64x2::<0b10001000>(ret1, ret2));
    ret = _mm512_add_epi16(ret, _mm512_shuffle_i64x2::<0b11011101>(ret1, ret2));

    // Write back the 32 x u16 results
    _mm512_storeu_si512(results.as_mut_ptr() as *mut __m512i, ret);
}

/// High-accuracy version of accumulate_batch using int32 accumulators
/// This version prevents overflow for high-dimensional data (dim > 2048)
/// and provides better precision at the cost of slightly lower performance
pub fn accumulate_batch_highacc_avx2(
    packed_codes: &[u8],
    lut_low8: &[u8],  // Low 8 bits of LUT values
    lut_high8: &[u8], // High 8 bits of LUT values
    dim: usize,
    results: &mut [i32; FASTSCAN_BATCH_SIZE],
) {
    assert!(
        dim.is_multiple_of(16),
        "Dimension must be multiple of 16 for SIMD"
    );

    #[cfg(target_arch = "x86_64")]
    {
        // Runtime CPU feature detection
        {
            if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512bw")
            {
                unsafe {
                    return accumulate_batch_highacc_avx512_impl(
                        packed_codes,
                        lut_low8,
                        lut_high8,
                        dim,
                        results,
                    );
                }
            }
        }

        if is_x86_feature_detected!("avx2") {
            unsafe {
                return accumulate_batch_highacc_avx2_impl(
                    packed_codes,
                    lut_low8,
                    lut_high8,
                    dim,
                    results,
                );
            }
        }
    }

    accumulate_batch_highacc_scalar(packed_codes, lut_low8, lut_high8, dim, results);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn accumulate_batch_highacc_avx2_impl(
    packed_codes: &[u8],
    lut_low8: &[u8],
    lut_high8: &[u8],
    dim: usize,
    results: &mut [i32; FASTSCAN_BATCH_SIZE],
) {
    use std::arch::x86_64::*;

    let code_length = dim << 2; // dim * 4
    let low_mask = _mm256_set1_epi8(0x0f);

    // Use 32-bit accumulators to prevent overflow
    let mut accu0_low = [_mm256_setzero_si256(); 2];
    let mut accu0_high = [_mm256_setzero_si256(); 2];
    let mut accu1_low = [_mm256_setzero_si256(); 2];
    let mut accu1_high = [_mm256_setzero_si256(); 2];

    // Process 64 bytes per iteration (matching AVX2 implementation)
    for i in (0..code_length).step_by(64) {
        // Process first 32 bytes
        let c0 = _mm256_loadu_si256(packed_codes.as_ptr().add(i) as *const __m256i);
        let lut_low0 = _mm256_loadu_si256(lut_low8.as_ptr().add(i) as *const __m256i);
        let lut_high0 = _mm256_loadu_si256(lut_high8.as_ptr().add(i) as *const __m256i);

        let lo0 = _mm256_and_si256(c0, low_mask);
        let hi0 = _mm256_and_si256(_mm256_srli_epi16(c0, 4), low_mask);

        let res_lo0_low = _mm256_shuffle_epi8(lut_low0, lo0);
        let res_lo0_high = _mm256_shuffle_epi8(lut_high0, lo0);
        let _res_hi0_low = _mm256_shuffle_epi8(lut_low0, hi0);
        let _res_hi0_high = _mm256_shuffle_epi8(lut_high0, hi0);

        // Accumulate with sign extension to 32-bit
        let res_lo0_low_32 =
            _mm256_cvtepi8_epi32(_mm256_extracti128_si256(res_lo0_low, 0));
        let res_lo0_high_32 =
            _mm256_cvtepi8_epi32(_mm256_extracti128_si256(res_lo0_high, 0));

        accu0_low[0] = _mm256_add_epi32(accu0_low[0], res_lo0_low_32);
        accu0_high[0] = _mm256_add_epi32(accu0_high[0], res_lo0_high_32);

        // Process second 32 bytes
        if i + 32 < code_length {
            let c1 =
                _mm256_loadu_si256(packed_codes.as_ptr().add(i + 32) as *const __m256i);
            let lut_low1 =
                _mm256_loadu_si256(lut_low8.as_ptr().add(i + 32) as *const __m256i);
            let lut_high1 =
                _mm256_loadu_si256(lut_high8.as_ptr().add(i + 32) as *const __m256i);

            let lo1 = _mm256_and_si256(c1, low_mask);
            let hi1 = _mm256_and_si256(_mm256_srli_epi16(c1, 4), low_mask);

            let res_lo1_low = _mm256_shuffle_epi8(lut_low1, lo1);
            let res_lo1_high = _mm256_shuffle_epi8(lut_high1, lo1);
            let _res_hi1_low = _mm256_shuffle_epi8(lut_low1, hi1);
            let _res_hi1_high = _mm256_shuffle_epi8(lut_high1, hi1);

            let res_lo1_low_32 =
                _mm256_cvtepi8_epi32(_mm256_extracti128_si256(res_lo1_low, 0));
            let res_lo1_high_32 =
                _mm256_cvtepi8_epi32(_mm256_extracti128_si256(res_lo1_high, 0));

            accu1_low[0] = _mm256_add_epi32(accu1_low[0], res_lo1_low_32);
            accu1_high[0] = _mm256_add_epi32(accu1_high[0], res_lo1_high_32);
        }
    }

    // Combine high and low parts: result = low + (high << 8)
    const SHIFT_AMOUNT: i32 = 8;
    accu0_high[0] = _mm256_slli_epi32(accu0_high[0], SHIFT_AMOUNT);
    accu1_high[0] = _mm256_slli_epi32(accu1_high[0], SHIFT_AMOUNT);

    let final0 = _mm256_add_epi32(accu0_low[0], accu0_high[0]);
    let final1 = _mm256_add_epi32(accu1_low[0], accu1_high[0]);

    // Store results
    _mm256_storeu_si256(results.as_mut_ptr() as *mut __m256i, final0);
    _mm256_storeu_si256(results.as_mut_ptr().add(8) as *mut __m256i, final1);

    // Zero out remaining entries
    for result in results.iter_mut().skip(16).take(FASTSCAN_BATCH_SIZE - 16) {
        *result = 0;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f", enable = "avx512bw")]
unsafe fn accumulate_batch_highacc_avx512_impl(
    packed_codes: &[u8],
    lut_low8: &[u8],
    lut_high8: &[u8],
    dim: usize,
    results: &mut [i32; FASTSCAN_BATCH_SIZE],
) {
    use std::arch::x86_64::*;

    let code_length = dim << 2;
    let low_mask = _mm512_set1_epi8(0x0f);

    // 512-bit accumulators for 32-bit values
    let mut accu_low = [_mm512_setzero_si512(); 2];
    let mut accu_high = [_mm512_setzero_si512(); 2];

    // Process 64 bytes per iteration with AVX-512
    for i in (0..code_length).step_by(64) {
        let c = _mm512_loadu_si512(packed_codes.as_ptr().add(i) as *const __m512i);
        let lut_low = _mm512_loadu_si512(lut_low8.as_ptr().add(i) as *const __m512i);
        let lut_high = _mm512_loadu_si512(lut_high8.as_ptr().add(i) as *const __m512i);

        let lo = _mm512_and_si512(c, low_mask);
        let hi = _mm512_and_si512(_mm512_srli_epi16(c, 4), low_mask);

        let res_lo_low = _mm512_shuffle_epi8(lut_low, lo);
        let res_lo_high = _mm512_shuffle_epi8(lut_high, lo);
        let _res_hi_low = _mm512_shuffle_epi8(lut_low, hi);
        let _res_hi_high = _mm512_shuffle_epi8(lut_high, hi);

        // Convert to 32-bit and accumulate
        // Use _mm512_extracti32x4_epi32 to extract 128-bit chunks
        let res_lo_low_128 = _mm512_extracti32x4_epi32::<0>(res_lo_low);
        let res_lo_high_128 = _mm512_extracti32x4_epi32::<0>(res_lo_high);
        let res_lo_low_32 = _mm512_cvtepi8_epi32(res_lo_low_128);
        let res_lo_high_32 = _mm512_cvtepi8_epi32(res_lo_high_128);

        accu_low[0] = _mm512_add_epi32(accu_low[0], res_lo_low_32);
        accu_high[0] = _mm512_add_epi32(accu_high[0], res_lo_high_32);
    }

    // Combine high and low: result = low + (high << 8)
    accu_high[0] = _mm512_slli_epi32(accu_high[0], 8);
    let final_result = _mm512_add_epi32(accu_low[0], accu_high[0]);

    // Store all 32 results
    _mm512_storeu_si512(results.as_mut_ptr() as *mut _, final_result);

    // Store second half if needed
    if FASTSCAN_BATCH_SIZE > 16 {
        _mm512_storeu_si512(results.as_mut_ptr().add(16) as *mut _, accu_low[1]);
    }
}

/// Scalar fallback for high-accuracy accumulation
///
/// Correct implementation that handles KPERM0 permutation
fn accumulate_batch_highacc_scalar(
    packed_codes: &[u8],
    lut_low8: &[u8],
    lut_high8: &[u8],
    dim: usize,
    results: &mut [i32; FASTSCAN_BATCH_SIZE],
) {
    // Same structure as accumulate_batch_scalar but using high-accuracy LUT
    //
    // The packed format (from pack_codes) organizes data as follows:
    // - For each column (8 dimensions = 2 codebooks):
    //   - 16 bytes for high 4-bit group (dims col*8+0..col*8+3)
    //   - 16 bytes for low 4-bit group (dims col*8+4..col*8+7)
    // - Each byte j contains:
    //   - Low 4 bits: code for vector KPERM0[j]
    //   - High 4 bits: code for vector KPERM0[j] + 16

    let mut sums = [0i32; FASTSCAN_BATCH_SIZE];

    // Process each column (8 dimensions = 2 codebooks at a time)
    let dim_bytes = dim / 8;

    for col in 0..dim_bytes {
        let packed_offset = col * 32;
        let lut_offset_hi = (col * 2) * 16; // Codebook for dims col*8+0..col*8+3
        let lut_offset_lo = (col * 2 + 1) * 16; // Codebook for dims col*8+4..col*8+7

        // Process high 4-bit group (first 16 packed bytes)
        for j in 0..16 {
            let packed_byte = packed_codes[packed_offset + j];
            let code_lo = (packed_byte & 0x0F) as usize;
            let code_hi = (packed_byte >> 4) as usize;

            // KPERM0 maps packed position to vector index
            let vec_idx_lo = KPERM0[j];
            let vec_idx_hi = KPERM0[j] + 16;

            // Reconstruct i16 value from low and high bytes for code_lo
            let low_lo = lut_low8[lut_offset_hi + code_lo] as u16;
            let high_lo = lut_high8[lut_offset_hi + code_lo] as u16;
            let val_u16_lo = low_lo | (high_lo << 8);
            let val_i16_lo = (val_u16_lo as i32) - 32768;

            // Reconstruct i16 value for code_hi
            let low_hi = lut_low8[lut_offset_hi + code_hi] as u16;
            let high_hi = lut_high8[lut_offset_hi + code_hi] as u16;
            let val_u16_hi = low_hi | (high_hi << 8);
            let val_i16_hi = (val_u16_hi as i32) - 32768;

            sums[vec_idx_lo] += val_i16_lo;
            sums[vec_idx_hi] += val_i16_hi;
        }

        // Process low 4-bit group (next 16 packed bytes)
        for j in 0..16 {
            let packed_byte = packed_codes[packed_offset + 16 + j];
            let code_lo = (packed_byte & 0x0F) as usize;
            let code_hi = (packed_byte >> 4) as usize;

            let vec_idx_lo = KPERM0[j];
            let vec_idx_hi = KPERM0[j] + 16;

            // Reconstruct i16 value from low and high bytes for code_lo
            let low_lo = lut_low8[lut_offset_lo + code_lo] as u16;
            let high_lo = lut_high8[lut_offset_lo + code_lo] as u16;
            let val_u16_lo = low_lo | (high_lo << 8);
            let val_i16_lo = (val_u16_lo as i32) - 32768;

            // Reconstruct i16 value for code_hi
            let low_hi = lut_low8[lut_offset_lo + code_hi] as u16;
            let high_hi = lut_high8[lut_offset_lo + code_hi] as u16;
            let val_u16_hi = low_hi | (high_hi << 8);
            let val_i16_hi = (val_u16_hi as i32) - 32768;

            sums[vec_idx_lo] += val_i16_lo;
            sums[vec_idx_hi] += val_i16_hi;
        }
    }

    // Copy results
    results.copy_from_slice(&sums);
}

/// Scalar fallback for accumulate_batch when SIMD is not available
///
/// Simplified implementation: unpack codes and compute directly.
/// This is slower but guaranteed correct.
fn accumulate_batch_scalar(
    packed_codes: &[u8],
    lut: &[i8],
    dim: usize,
    results: &mut [u16; FASTSCAN_BATCH_SIZE],
) {
    // Correct scalar implementation that handles KPERM0 permutation
    //
    // The packed format (from pack_codes) organizes data as follows:
    // - For each column (8 dimensions = 2 codebooks):
    //   - 16 bytes for high 4-bit group (dims col*8+0..col*8+3)
    //   - 16 bytes for low 4-bit group (dims col*8+4..col*8+7)
    // - Each byte j contains:
    //   - Low 4 bits: code for vector KPERM0[j]
    //   - High 4 bits: code for vector KPERM0[j] + 16
    //
    // The LUT is organized as: (dim/4) codebooks of 16 entries each
    // Codebook i handles dimensions i*4 .. i*4+3

    let mut sums = [0i32; FASTSCAN_BATCH_SIZE];

    // Process each column (8 dimensions = 2 codebooks at a time)
    let dim_bytes = dim / 8;

    for col in 0..dim_bytes {
        let packed_offset = col * 32;
        let lut_offset_hi = (col * 2) * 16; // Codebook for dims col*8+0..col*8+3
        let lut_offset_lo = (col * 2 + 1) * 16; // Codebook for dims col*8+4..col*8+7

        // Process high 4-bit group (first 16 packed bytes)
        for j in 0..16 {
            let packed_byte = packed_codes[packed_offset + j];
            let code_lo = (packed_byte & 0x0F) as usize;
            let code_hi = (packed_byte >> 4) as usize;

            // KPERM0 maps packed position to vector index
            let vec_idx_lo = KPERM0[j];
            let vec_idx_hi = KPERM0[j] + 16;

            // Cast to u8 to treat as unsigned 0..255 (matching AVX2 behavior)
            // lut is i8, so we cast to u8 to interpret bits as unsigned
            sums[vec_idx_lo] += (lut[lut_offset_hi + code_lo] as u8) as i32;
            sums[vec_idx_hi] += (lut[lut_offset_hi + code_hi] as u8) as i32;
        }

        // Process low 4-bit group (next 16 packed bytes)
        for j in 0..16 {
            let packed_byte = packed_codes[packed_offset + 16 + j];
            let code_lo = (packed_byte & 0x0F) as usize;
            let code_hi = (packed_byte >> 4) as usize;

            let vec_idx_lo = KPERM0[j];
            let vec_idx_hi = KPERM0[j] + 16;

            sums[vec_idx_lo] += (lut[lut_offset_lo + code_lo] as u8) as i32;
            sums[vec_idx_hi] += (lut[lut_offset_lo + code_hi] as u8) as i32;
        }
    }

    // Convert to u16
    for i in 0..FASTSCAN_BATCH_SIZE {
        results[i] = sums[i] as u16;
    }
}

// ============================================================================
// Packed Ex-Code Dot Product Functions (C++-style, no unpacking)
// ============================================================================
// These functions compute inner products directly on packed ex-code data,
// matching C++ implementation for maximum performance.
// Reference: C++ space.hpp:268-598 (excode_ipimpl namespace)

/// Inner product between f32 query and packed 2-bit ex_code (for 3-bit total RaBitQ)
/// Reference: C++ ip16_fxu2_avx512
///
/// # Arguments
/// * `query` - Query vector (f32, length must be padded_dim)
/// * `packed_ex_code` - Packed 2-bit ex_code (padded_dim * 2 / 8 bytes)
/// * `padded_dim` - Padded dimension (must be multiple of 16)
#[inline]
pub fn ip_packed_ex2_f32(query: &[f32], packed_ex_code: &[u8], padded_dim: usize) -> f32 {
    debug_assert_eq!(query.len(), padded_dim);
    debug_assert_eq!(packed_ex_code.len(), (padded_dim * 2).div_ceil(8));
    debug_assert!(
        padded_dim.is_multiple_of(16),
        "padded_dim must be multiple of 16 for 2-bit"
    );

    // Prioritize AVX512 > AVX2 > Scalar
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        ip_packed_ex2_f32_avx512(query, packed_ex_code, padded_dim)
    }

    #[cfg(all(
        target_arch = "x86_64",
        target_feature = "avx2",
        not(target_feature = "avx512f")
    ))]
    unsafe {
        ip_packed_ex2_f32_avx2(query, packed_ex_code, padded_dim)
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        ip_packed_ex2_f32_scalar(query, packed_ex_code, padded_dim)
    }
}

/// Inner product between f32 query and packed 6-bit ex_code (for 7-bit total RaBitQ)
/// Reference: C++ ip16_fxu6_avx512
///
/// # Arguments
/// * `query` - Query vector (f32, length must be padded_dim)
/// * `packed_ex_code` - Packed 6-bit ex_code (padded_dim * 6 / 8 bytes)
/// * `padded_dim` - Padded dimension (must be multiple of 16)
#[inline]
pub fn ip_packed_ex6_f32(query: &[f32], packed_ex_code: &[u8], padded_dim: usize) -> f32 {
    debug_assert_eq!(query.len(), padded_dim);
    debug_assert_eq!(packed_ex_code.len(), (padded_dim * 6).div_ceil(8));
    debug_assert!(
        padded_dim.is_multiple_of(16),
        "padded_dim must be multiple of 16 for 6-bit"
    );

    // Prioritize AVX512 > AVX2 > Scalar
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
    unsafe {
        ip_packed_ex6_f32_avx512(query, packed_ex_code, padded_dim)
    }

    #[cfg(all(
        target_arch = "x86_64",
        target_feature = "avx2",
        not(target_feature = "avx512f")
    ))]
    unsafe {
        ip_packed_ex6_f32_avx2(query, packed_ex_code, padded_dim)
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        ip_packed_ex6_f32_scalar(query, packed_ex_code, padded_dim)
    }
}

// ----------------------------------------------------------------------------
// Scalar implementations (portable fallback)
// ----------------------------------------------------------------------------

#[inline]
#[allow(dead_code)]
#[allow(clippy::needless_range_loop)]
fn ip_packed_ex2_f32_scalar(
    query: &[f32],
    packed_ex_code: &[u8],
    padded_dim: usize,
) -> f32 {
    // Unpack C++ compatible format and compute dot product
    // Format: 16 codes packed into 4 bytes with interleaved layout
    let mut sum = 0.0f32;
    let mut code_idx = 0;

    for chunk in 0..(padded_dim / 16) {
        let base = chunk * 4;
        // Read 4 bytes as u32 (little-endian)
        let compact = u32::from_le_bytes([
            packed_ex_code[base],
            packed_ex_code[base + 1],
            packed_ex_code[base + 2],
            packed_ex_code[base + 3],
        ]);

        // Extract 16 codes using C++ format (interleaved 2-bit values)
        for i in 0..4 {
            let byte_offset = i * 8;
            // Extract 4 codes from each of the 4 shifted versions
            let c0 = ((compact >> byte_offset) & 0x3) as f32;
            let c1 = ((compact >> (byte_offset + 2)) & 0x3) as f32;
            let c2 = ((compact >> (byte_offset + 4)) & 0x3) as f32;
            let c3 = ((compact >> (byte_offset + 6)) & 0x3) as f32;

            sum += c0 * query[code_idx];
            sum += c1 * query[code_idx + 4];
            sum += c2 * query[code_idx + 8];
            sum += c3 * query[code_idx + 12];
            code_idx += 1;
        }
        code_idx += 12; // Move to next chunk
    }
    sum
}

#[inline]
#[allow(dead_code)]
#[allow(clippy::needless_range_loop)]
fn ip_packed_ex6_f32_scalar(
    query: &[f32],
    packed_ex_code: &[u8],
    padded_dim: usize,
) -> f32 {
    // Unpack C++ compatible 6-bit format and compute dot product
    // Format: 16 codes packed into 12 bytes
    // - First 8 bytes: lower 4 bits (interleaved: even codes in low nibble, odd in high)
    // - Next 4 bytes: upper 2 bits (interleaved like 2-bit packing)
    let mut sum = 0.0f32;
    let mut code_idx = 0;

    for chunk in 0..(padded_dim / 16) {
        let base = chunk * 12;

        // Read lower 4 bits (8 bytes)
        let compact4 = u64::from_le_bytes([
            packed_ex_code[base],
            packed_ex_code[base + 1],
            packed_ex_code[base + 2],
            packed_ex_code[base + 3],
            packed_ex_code[base + 4],
            packed_ex_code[base + 5],
            packed_ex_code[base + 6],
            packed_ex_code[base + 7],
        ]);

        // Read upper 2 bits (4 bytes)
        let compact2 = u32::from_le_bytes([
            packed_ex_code[base + 8],
            packed_ex_code[base + 9],
            packed_ex_code[base + 10],
            packed_ex_code[base + 11],
        ]);

        // Extract lower 4 bits for all 16 codes
        let mut lower_4bits = [0u16; 16];
        for i in 0..8 {
            let byte_val = ((compact4 >> (i * 8)) & 0xFF) as u8;
            // Even code (0,2,4,...,14) in lower nibble
            lower_4bits[i] = (byte_val & 0x0F) as u16;
            // Odd code (1,3,5,...,15) in upper nibble
            lower_4bits[i + 8] = ((byte_val >> 4) & 0x0F) as u16;
        }

        // Extract upper 2 bits for all 16 codes (using 2-bit interleaved format)
        let mut upper_2bits = [0u16; 16];
        for i in 0..4 {
            let byte_offset = i * 8;
            upper_2bits[i] = ((compact2 >> byte_offset) & 0x3) as u16;
            upper_2bits[i + 4] = ((compact2 >> (byte_offset + 2)) & 0x3) as u16;
            upper_2bits[i + 8] = ((compact2 >> (byte_offset + 4)) & 0x3) as u16;
            upper_2bits[i + 12] = ((compact2 >> (byte_offset + 6)) & 0x3) as u16;
        }

        // Combine and compute dot product
        for i in 0..16 {
            let code = (lower_4bits[i] | (upper_2bits[i] << 4)) as f32;
            sum += code * query[code_idx + i];
        }
        code_idx += 16;
    }
    sum
}

// ----------------------------------------------------------------------------
// AVX2 implementations (optimized)
// ----------------------------------------------------------------------------

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
unsafe fn ip_packed_ex2_f32_avx2(
    query: &[f32],
    packed_ex_code: &[u8],
    padded_dim: usize,
) -> f32 {
    use std::arch::x86_64::*;

    let mut sum = _mm256_setzero_ps();
    let mask = _mm_set1_epi8(0b00000011); // Mask for extracting 2 bits

    // Process 16 elements at a time (16 * 2 bits = 32 bits = 4 bytes)
    let mut query_ptr = query.as_ptr();
    let mut code_ptr = packed_ex_code.as_ptr();

    for _ in 0..(padded_dim / 16) {
        // Load 4 bytes of packed 2-bit codes (16 elements in C++ compatible format)
        let compact = std::ptr::read_unaligned(code_ptr as *const i32);

        // C++ format: Extract using shifts and masks
        // _mm_set_epi32 creates [d, c, b, a] where a is at lowest address
        // compact >> 0: bits 0-31, compact >> 2: bits 2-33, etc.
        let code_i32 = _mm_set_epi32(compact >> 6, compact >> 4, compact >> 2, compact);
        let code_masked = _mm_and_si128(code_i32, mask);

        // code_masked now contains 16 bytes, each with 2-bit value
        // Bytes 0-3: codes 0,1,2,3 (from compact >> 0)
        // Bytes 4-7: codes 4,5,6,7 (from compact >> 2)
        // Bytes 8-11: codes 8,9,10,11 (from compact >> 4)
        // Bytes 12-15: codes 12,13,14,15 (from compact >> 6)

        // Extract lower 8 bytes (codes 0-7) and convert to f32
        let code_f32_lo = _mm256_cvtepi32_ps(_mm256_cvtepi8_epi32(code_masked));
        let query_lo = _mm256_loadu_ps(query_ptr);
        sum = _mm256_fmadd_ps(code_f32_lo, query_lo, sum);

        // Extract upper 8 bytes (codes 8-15) and convert to f32
        let code_masked_hi = _mm_unpackhi_epi64(code_masked, code_masked); // Get high 8 bytes
        let code_f32_hi = _mm256_cvtepi32_ps(_mm256_cvtepi8_epi32(code_masked_hi));
        let query_hi = _mm256_loadu_ps(query_ptr.add(8));
        sum = _mm256_fmadd_ps(code_f32_hi, query_hi, sum);

        query_ptr = query_ptr.add(16);
        code_ptr = code_ptr.add(4);
    }

    // Horizontal sum
    let sum_hi = _mm256_extractf128_ps(sum, 1);
    let sum_lo = _mm256_castps256_ps128(sum);
    let sum_128 = _mm_add_ps(sum_lo, sum_hi);
    let sum_64 = _mm_add_ps(sum_128, _mm_movehl_ps(sum_128, sum_128));
    let sum_32 = _mm_add_ss(sum_64, _mm_shuffle_ps(sum_64, sum_64, 0x55));
    _mm_cvtss_f32(sum_32)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
#[allow(dead_code)]
unsafe fn ip_packed_ex6_f32_avx2(
    query: &[f32],
    packed_ex_code: &[u8],
    padded_dim: usize,
) -> f32 {
    use std::arch::x86_64::*;

    let mut sum = _mm256_setzero_ps();
    const MASK_4: i64 = 0x0f0f0f0f0f0f0f0f;
    let mask_2 = _mm_set1_epi8(0b00110000);

    // Process 16 elements at a time (16 * 6 bits = 96 bits = 12 bytes)
    let mut query_ptr = query.as_ptr();
    let mut code_ptr = packed_ex_code.as_ptr();

    for _ in 0..(padded_dim / 16) {
        // Load 8 bytes containing lower 4 bits of each code
        let compact4 = std::ptr::read_unaligned(code_ptr as *const i64);
        let code4_0 = compact4 & MASK_4;
        let code4_1 = (compact4 >> 4) & MASK_4;
        let c4 = _mm_set_epi64x(code4_1, code4_0); // lower 4 bits

        code_ptr = code_ptr.add(8);

        // Load 4 bytes containing upper 2 bits of each code
        let compact2 = std::ptr::read_unaligned(code_ptr as *const i32);
        let c2 = _mm_set_epi32(compact2 >> 2, compact2, compact2 << 2, compact2 << 4);
        let c2_masked = _mm_and_si128(c2, mask_2);

        // Combine: 6-bit code = (upper 2 bits) | (lower 4 bits)
        let c6 = _mm_or_si128(c2_masked, c4);

        // Convert first 8 codes to f32
        let code_f32_lo = _mm256_cvtepi32_ps(_mm256_cvtepi8_epi32(c6));
        let query_lo = _mm256_loadu_ps(query_ptr);
        sum = _mm256_fmadd_ps(code_f32_lo, query_lo, sum);

        // Convert next 8 codes to f32
        let c6_hi = _mm_unpackhi_epi64(c6, c6); // Shift to get high 8 bytes
        let code_f32_hi = _mm256_cvtepi32_ps(_mm256_cvtepi8_epi32(c6_hi));
        let query_hi = _mm256_loadu_ps(query_ptr.add(8));
        sum = _mm256_fmadd_ps(code_f32_hi, query_hi, sum);

        query_ptr = query_ptr.add(16);
        code_ptr = code_ptr.add(4); // Total 12 bytes per 16 elements
    }

    // Horizontal sum
    let sum_hi = _mm256_extractf128_ps(sum, 1);
    let sum_lo = _mm256_castps256_ps128(sum);
    let sum_128 = _mm_add_ps(sum_lo, sum_hi);
    let sum_64 = _mm_add_ps(sum_128, _mm_movehl_ps(sum_128, sum_128));
    let sum_32 = _mm_add_ss(sum_64, _mm_shuffle_ps(sum_64, sum_64, 0x55));
    _mm_cvtss_f32(sum_32)
}

// ----------------------------------------------------------------------------
// AVX512 implementations (highest performance)
// ----------------------------------------------------------------------------

/// AVX512 implementation of 2-bit ex-code inner product
/// Reference: C++ ip16_fxu2_avx512 in space.hpp:293-316
#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn ip_packed_ex2_f32_avx512(
    query: &[f32],
    packed_ex_code: &[u8],
    padded_dim: usize,
) -> f32 {
    use std::arch::x86_64::*;

    let mut sum = _mm512_setzero_ps();
    let mask = _mm_set1_epi8(0b00000011); // Mask for extracting 2 bits

    // Process 16 elements at a time (16 * 2 bits = 32 bits = 4 bytes)
    let mut query_ptr = query.as_ptr();
    let mut code_ptr = packed_ex_code.as_ptr();

    for _ in 0..(padded_dim / 16) {
        // Load 4 bytes of packed 2-bit codes (16 elements in C++ compatible format)
        let compact = std::ptr::read_unaligned(code_ptr as *const i32);

        // C++ format: Extract using shifts and masks
        // _mm_set_epi32 creates [d, c, b, a] where a is at lowest address
        // compact >> 0: bits 0-31, compact >> 2: bits 2-33, etc.
        let code = _mm_set_epi32(compact >> 6, compact >> 4, compact >> 2, compact);
        let code = _mm_and_si128(code, mask);

        // Convert 16 bytes (each with 2-bit value) to 16 f32 values
        let cf = _mm512_cvtepi32_ps(_mm512_cvtepi8_epi32(code));

        // Load 16 query values and compute FMA
        let q = _mm512_loadu_ps(query_ptr);
        sum = _mm512_fmadd_ps(cf, q, sum);

        query_ptr = query_ptr.add(16);
        code_ptr = code_ptr.add(4);
    }

    // Horizontal sum using AVX512 reduce
    _mm512_reduce_add_ps(sum)
}

/// AVX512 implementation of 6-bit ex-code inner product
/// Reference: C++ ip16_fxu6_avx512 in space.hpp:456-486
#[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
#[target_feature(enable = "avx512f")]
unsafe fn ip_packed_ex6_f32_avx512(
    query: &[f32],
    packed_ex_code: &[u8],
    padded_dim: usize,
) -> f32 {
    use std::arch::x86_64::*;

    let mut sum = _mm512_setzero_ps();
    const MASK_4: i64 = 0x0f0f0f0f0f0f0f0f;
    let mask_2 = _mm_set1_epi8(0b00110000);

    // Process 16 elements at a time (16 * 6 bits = 96 bits = 12 bytes)
    let mut query_ptr = query.as_ptr();
    let mut code_ptr = packed_ex_code.as_ptr();

    for _ in 0..(padded_dim / 16) {
        // Load 8 bytes containing lower 4 bits of each code
        let compact4 = std::ptr::read_unaligned(code_ptr as *const i64);
        let code4_0 = compact4 & MASK_4;
        let code4_1 = (compact4 >> 4) & MASK_4;
        let c4 = _mm_set_epi64x(code4_1, code4_0); // lower 4 bits

        code_ptr = code_ptr.add(8);

        // Load 4 bytes containing upper 2 bits of each code
        let compact2 = std::ptr::read_unaligned(code_ptr as *const i32);
        let c2 = _mm_set_epi32(compact2 >> 2, compact2, compact2 << 2, compact2 << 4);
        let c2 = _mm_and_si128(c2, mask_2);

        // Combine: 6-bit code = (upper 2 bits) | (lower 4 bits)
        let c6 = _mm_or_si128(c2, c4);

        // Convert 16 bytes (each with 6-bit value) to 16 f32 values
        let cf = _mm512_cvtepi32_ps(_mm512_cvtepi8_epi32(c6));

        // Load 16 query values and compute FMA
        let q = _mm512_loadu_ps(query_ptr);
        sum = _mm512_fmadd_ps(cf, q, sum);

        query_ptr = query_ptr.add(16);
        code_ptr = code_ptr.add(4); // Total 12 bytes per 16 elements
    }

    // Horizontal sum using AVX512 reduce
    _mm512_reduce_add_ps(sum)
}

// ----------------------------------------------------------------------------
// Batch Distance Computation (Vectorized)
// ----------------------------------------------------------------------------

/// Compute batch distances in vectorized fashion (matching C++ Eigen performance)
/// This replaces scalar loops with explicit SIMD operations
///
/// Computes:
/// - ip_x0_qr[i] = delta * accu[i] + sum_vl
/// - est_distance[i] = f_add[i] + g_add + f_rescale[i] * (ip_x0_qr[i] + k1x_sum_q)
/// - lower_bound[i] = est_distance[i] - f_error[i] * g_error
///
/// Reference: C++ estimator.hpp:65-70 (Eigen vectorized operations)
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn compute_batch_distances_u16(
    accu_res: &[u16; FASTSCAN_BATCH_SIZE],
    lut_delta: f32,
    lut_sum_vl: f32,
    batch_f_add: &[f32],
    batch_f_rescale: &[f32],
    batch_f_error: &[f32],
    g_add: f32,
    g_error: f32,
    k1x_sum_q: f32,
    ip_x0_qr: &mut [f32; FASTSCAN_BATCH_SIZE],
    est_distance: &mut [f32; FASTSCAN_BATCH_SIZE],
    lower_bound: &mut [f32; FASTSCAN_BATCH_SIZE],
) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        compute_batch_distances_u16_avx2(
            accu_res,
            lut_delta,
            lut_sum_vl,
            batch_f_add,
            batch_f_rescale,
            batch_f_error,
            g_add,
            g_error,
            k1x_sum_q,
            ip_x0_qr,
            est_distance,
            lower_bound,
        );
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        compute_batch_distances_u16_scalar(
            accu_res,
            lut_delta,
            lut_sum_vl,
            batch_f_add,
            batch_f_rescale,
            batch_f_error,
            g_add,
            g_error,
            k1x_sum_q,
            ip_x0_qr,
            est_distance,
            lower_bound,
        );
    }
}

/// Compute batch distances for i32 accumulators (high-accuracy mode)
#[inline]
#[allow(clippy::too_many_arguments)]
pub fn compute_batch_distances_i32(
    accu_res: &[i32; FASTSCAN_BATCH_SIZE],
    lut_delta: f32,
    lut_sum_vl: f32,
    batch_f_add: &[f32],
    batch_f_rescale: &[f32],
    batch_f_error: &[f32],
    g_add: f32,
    g_error: f32,
    k1x_sum_q: f32,
    ip_x0_qr: &mut [f32; FASTSCAN_BATCH_SIZE],
    est_distance: &mut [f32; FASTSCAN_BATCH_SIZE],
    lower_bound: &mut [f32; FASTSCAN_BATCH_SIZE],
) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        compute_batch_distances_i32_avx2(
            accu_res,
            lut_delta,
            lut_sum_vl,
            batch_f_add,
            batch_f_rescale,
            batch_f_error,
            g_add,
            g_error,
            k1x_sum_q,
            ip_x0_qr,
            est_distance,
            lower_bound,
        );
    }

    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        compute_batch_distances_i32_scalar(
            accu_res,
            lut_delta,
            lut_sum_vl,
            batch_f_add,
            batch_f_rescale,
            batch_f_error,
            g_add,
            g_error,
            k1x_sum_q,
            ip_x0_qr,
            est_distance,
            lower_bound,
        );
    }
}

// Scalar implementation (fallback)
#[allow(dead_code, clippy::too_many_arguments)]
fn compute_batch_distances_u16_scalar(
    accu_res: &[u16; FASTSCAN_BATCH_SIZE],
    lut_delta: f32,
    lut_sum_vl: f32,
    batch_f_add: &[f32],
    batch_f_rescale: &[f32],
    batch_f_error: &[f32],
    g_add: f32,
    g_error: f32,
    k1x_sum_q: f32,
    ip_x0_qr: &mut [f32; FASTSCAN_BATCH_SIZE],
    est_distance: &mut [f32; FASTSCAN_BATCH_SIZE],
    lower_bound: &mut [f32; FASTSCAN_BATCH_SIZE],
) {
    for i in 0..FASTSCAN_BATCH_SIZE {
        // Interpret u16 as unsigned (0..65535)
        // This matches AVX2 behavior (cvtepu16_epi32)
        let accu = accu_res[i] as f32;
        ip_x0_qr[i] = lut_delta * accu + lut_sum_vl;
        est_distance[i] =
            batch_f_add[i] + g_add + batch_f_rescale[i] * (ip_x0_qr[i] + k1x_sum_q);
        lower_bound[i] = est_distance[i] - batch_f_error[i] * g_error;
    }
}

#[allow(dead_code, clippy::too_many_arguments)]
fn compute_batch_distances_i32_scalar(
    accu_res: &[i32; FASTSCAN_BATCH_SIZE],
    lut_delta: f32,
    lut_sum_vl: f32,
    batch_f_add: &[f32],
    batch_f_rescale: &[f32],
    batch_f_error: &[f32],
    g_add: f32,
    g_error: f32,
    k1x_sum_q: f32,
    ip_x0_qr: &mut [f32; FASTSCAN_BATCH_SIZE],
    est_distance: &mut [f32; FASTSCAN_BATCH_SIZE],
    lower_bound: &mut [f32; FASTSCAN_BATCH_SIZE],
) {
    for i in 0..FASTSCAN_BATCH_SIZE {
        let accu = accu_res[i] as f32;
        ip_x0_qr[i] = lut_delta * accu + lut_sum_vl;
        est_distance[i] =
            batch_f_add[i] + g_add + batch_f_rescale[i] * (ip_x0_qr[i] + k1x_sum_q);
        lower_bound[i] = est_distance[i] - batch_f_error[i] * g_error;
    }
}

// AVX2 implementation (8 f32 per SIMD lane)
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
#[allow(clippy::too_many_arguments)]
unsafe fn compute_batch_distances_u16_avx2(
    accu_res: &[u16; FASTSCAN_BATCH_SIZE],
    lut_delta: f32,
    lut_sum_vl: f32,
    batch_f_add: &[f32],
    batch_f_rescale: &[f32],
    batch_f_error: &[f32],
    g_add: f32,
    g_error: f32,
    k1x_sum_q: f32,
    ip_x0_qr: &mut [f32; FASTSCAN_BATCH_SIZE],
    est_distance: &mut [f32; FASTSCAN_BATCH_SIZE],
    lower_bound: &mut [f32; FASTSCAN_BATCH_SIZE],
) {
    use std::arch::x86_64::*;

    let delta_vec = _mm256_set1_ps(lut_delta);
    let sum_vl_vec = _mm256_set1_ps(lut_sum_vl);
    let g_add_vec = _mm256_set1_ps(g_add);
    let g_error_vec = _mm256_set1_ps(g_error);
    let k1x_sum_q_vec = _mm256_set1_ps(k1x_sum_q);

    // Process 32 elements in 4 iterations (8 f32 per iteration)
    for i in (0..FASTSCAN_BATCH_SIZE).step_by(8) {
        // Load u16 accumulators and convert to f32
        let accu_u16 = _mm_loadu_si128(accu_res[i..].as_ptr() as *const __m128i);
        let accu_i32 = _mm256_cvtepu16_epi32(accu_u16);
        let accu_f32 = _mm256_cvtepi32_ps(accu_i32);

        // ip_x0_qr = delta * accu + sum_vl
        let ip_vec = _mm256_fmadd_ps(delta_vec, accu_f32, sum_vl_vec);
        _mm256_storeu_ps(&mut ip_x0_qr[i], ip_vec);

        // Load batch parameters
        let f_add_vec = _mm256_loadu_ps(&batch_f_add[i]);
        let f_rescale_vec = _mm256_loadu_ps(&batch_f_rescale[i]);
        let f_error_vec = _mm256_loadu_ps(&batch_f_error[i]);

        // est_distance = f_add + g_add + f_rescale * (ip_x0_qr + k1x_sum_q)
        let ip_plus_k1x = _mm256_add_ps(ip_vec, k1x_sum_q_vec);
        let rescale_term = _mm256_mul_ps(f_rescale_vec, ip_plus_k1x);
        let est_vec = _mm256_add_ps(f_add_vec, g_add_vec);
        let est_vec = _mm256_add_ps(est_vec, rescale_term);
        _mm256_storeu_ps(&mut est_distance[i], est_vec);

        // lower_bound = est_distance - f_error * g_error
        let error_term = _mm256_mul_ps(f_error_vec, g_error_vec);
        let lower_vec = _mm256_sub_ps(est_vec, error_term);
        _mm256_storeu_ps(&mut lower_bound[i], lower_vec);
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[target_feature(enable = "avx2")]
#[allow(clippy::too_many_arguments)]
unsafe fn compute_batch_distances_i32_avx2(
    accu_res: &[i32; FASTSCAN_BATCH_SIZE],
    lut_delta: f32,
    lut_sum_vl: f32,
    batch_f_add: &[f32],
    batch_f_rescale: &[f32],
    batch_f_error: &[f32],
    g_add: f32,
    g_error: f32,
    k1x_sum_q: f32,
    ip_x0_qr: &mut [f32; FASTSCAN_BATCH_SIZE],
    est_distance: &mut [f32; FASTSCAN_BATCH_SIZE],
    lower_bound: &mut [f32; FASTSCAN_BATCH_SIZE],
) {
    use std::arch::x86_64::*;

    let delta_vec = _mm256_set1_ps(lut_delta);
    let sum_vl_vec = _mm256_set1_ps(lut_sum_vl);
    let g_add_vec = _mm256_set1_ps(g_add);
    let g_error_vec = _mm256_set1_ps(g_error);
    let k1x_sum_q_vec = _mm256_set1_ps(k1x_sum_q);

    // Process 32 elements in 4 iterations (8 f32 per iteration)
    for i in (0..FASTSCAN_BATCH_SIZE).step_by(8) {
        // Load i32 accumulators and convert to f32
        let accu_i32 = _mm256_loadu_si256(accu_res[i..].as_ptr() as *const __m256i);
        let accu_f32 = _mm256_cvtepi32_ps(accu_i32);

        // ip_x0_qr = delta * accu + sum_vl
        let ip_vec = _mm256_fmadd_ps(delta_vec, accu_f32, sum_vl_vec);
        _mm256_storeu_ps(&mut ip_x0_qr[i], ip_vec);

        // Load batch parameters
        let f_add_vec = _mm256_loadu_ps(&batch_f_add[i]);
        let f_rescale_vec = _mm256_loadu_ps(&batch_f_rescale[i]);
        let f_error_vec = _mm256_loadu_ps(&batch_f_error[i]);

        // est_distance = f_add + g_add + f_rescale * (ip_x0_qr + k1x_sum_q)
        let ip_plus_k1x = _mm256_add_ps(ip_vec, k1x_sum_q_vec);
        let rescale_term = _mm256_mul_ps(f_rescale_vec, ip_plus_k1x);
        let est_vec = _mm256_add_ps(f_add_vec, g_add_vec);
        let est_vec = _mm256_add_ps(est_vec, rescale_term);
        _mm256_storeu_ps(&mut est_distance[i], est_vec);

        // lower_bound = est_distance - f_error * g_error
        let error_term = _mm256_mul_ps(f_error_vec, g_error_vec);
        let lower_vec = _mm256_sub_ps(est_vec, error_term);
        _mm256_storeu_ps(&mut lower_bound[i], lower_vec);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_unpack_binary_code() {
        let dim: usize = 32;
        let binary_code: Vec<u8> = (0..dim).map(|i| (i % 2) as u8).collect();
        let mut packed = vec![0u8; dim.div_ceil(8)];
        let mut unpacked = vec![0u8; dim];

        pack_binary_code(&binary_code, &mut packed, dim);
        unpack_binary_code(&packed, &mut unpacked, dim);

        assert_eq!(binary_code, unpacked);
    }

    #[test]
    fn test_pack_unpack_ex_code_2bit() {
        let dim: usize = 32;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 4) as u16).collect();
        let mut packed = vec![0u8; (dim * 2).div_ceil(8)];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code(&ex_code, &mut packed, dim, 2);
        unpack_ex_code(&packed, &mut unpacked, dim, 2);

        assert_eq!(ex_code, unpacked);
    }

    #[test]
    fn test_pack_unpack_ex_code_4bit() {
        let dim: usize = 32;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 16) as u16).collect();
        let mut packed = vec![0u8; (dim * 4).div_ceil(8)];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code(&ex_code, &mut packed, dim, 4);
        unpack_ex_code(&packed, &mut unpacked, dim, 4);

        assert_eq!(ex_code, unpacked);
    }

    #[test]
    fn test_ip_packed_ex2_vs_unpacked() {
        let padded_dim = 960; // GIST dimension

        // Create test ex_code (2-bit values: 0-3)
        let ex_code: Vec<u16> = (0..padded_dim).map(|i| (i % 4) as u16).collect();

        // Create test query
        let query: Vec<f32> = (0..padded_dim).map(|i| (i as f32) * 0.01).collect();

        // Pack ex_code using C++-compatible format
        let mut packed = vec![0u8; padded_dim / 16 * 4];
        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, padded_dim);

        // Method 1: Packed dot product (new)
        let result_packed = ip_packed_ex2_f32(&query, &packed, padded_dim);

        // Method 2: Unpacked dot product (reference)
        let result_unpacked = dot_u16_f32(&ex_code, &query);

        println!("Packed result:   {}", result_packed);
        println!("Unpacked result: {}", result_unpacked);
        println!(
            "Difference:      {}",
            (result_packed - result_unpacked).abs()
        );

        // Allow for floating point precision differences (especially with 6-bit packing)
        assert!(
            (result_packed - result_unpacked).abs() < 0.1,
            "Packed and unpacked results differ: {} vs {}",
            result_packed,
            result_unpacked
        );
    }

    /// Minimal test to verify scalar accumulate_batch produces correct results
    /// This test directly checks that pack_codes + accumulate_batch_scalar gives
    /// the same result as manual LUT lookup
    #[test]
    fn test_scalar_accumulate_batch_correctness() {
        // Create a simple test case with known values
        let dim = 64; // 8 bytes = 16 codebooks
        let dim_bytes = dim / 8;

        // Create a simple binary code for one vector
        // Set some bits to 1 to have non-zero codes
        let mut binary_code = vec![0u8; dim];
        binary_code[0] = 1; // dim 0
        binary_code[3] = 1; // dim 3
        binary_code[8] = 1; // dim 8
        binary_code[15] = 1; // dim 15

        // Pack binary code
        let mut binary_packed = vec![0u8; dim_bytes];
        pack_binary_code(&binary_code, &mut binary_packed, dim);

        // Pack into FastScan format (single vector, padded to 32)
        let mut packed_codes = vec![0u8; 32 * dim_bytes];
        pack_codes(&binary_packed, 1, dim_bytes, &mut packed_codes);

        // Create a simple LUT (all 1s for easy verification)
        // Each codebook has 16 entries
        let num_codebooks = dim / 4;
        let lut_size = num_codebooks * 16;
        let mut lut = vec![0i8; lut_size];

        // Set specific LUT values so we know what to expect
        // For codebook 0 (dims 0-3), binary_code = [1,0,0,1] = 9 (MSB-first)
        // For codebook 1 (dims 4-7), binary_code = [0,0,0,0] = 0
        // For codebook 2 (dims 8-11), binary_code = [1,0,0,0] = 8
        // For codebook 3 (dims 12-15), binary_code = [0,0,0,1] = 1
        // ...rest are 0

        // code 9 = binary_code[0]*8 + binary_code[1]*4 + binary_code[2]*2 + binary_code[3]
        //        = 1*8 + 0 + 0 + 1 = 9 ✓
        // code 8 for codebook 2 = 1*8 + 0 + 0 + 0 = 8 ✓
        // code 1 for codebook 3 = 0 + 0 + 0 + 1 = 1 ✓

        lut[9] = 10; // Codebook 0, code 9
        lut[16] = 20; // Codebook 1, code 0
        lut[2 * 16 + 8] = 30; // Codebook 2, code 8
        lut[3 * 16 + 1] = 40; // Codebook 3, code 1
        // Rest are 0

        // Expected sum for vector 0: 10 + 20 + 30 + 40 = 100
        let expected_sum = 100i32;

        // Run scalar accumulation
        let mut results = [0u16; FASTSCAN_BATCH_SIZE];
        accumulate_batch_scalar(&packed_codes, &lut, dim, &mut results);

        let actual_sum = results[0] as i16 as i32;
        println!(
            "test_scalar_accumulate_batch_correctness: expected={}, actual={}",
            expected_sum, actual_sum
        );

        assert_eq!(
            actual_sum, expected_sum,
            "Scalar accumulate_batch result mismatch: expected {}, got {}",
            expected_sum, actual_sum
        );
    }

    #[test]
    fn test_ip_packed_ex6_vs_unpacked() {
        let padded_dim = 960; // GIST dimension

        // Create test ex_code (6-bit values: 0-63)
        let ex_code: Vec<u16> = (0..padded_dim).map(|i| (i % 64) as u16).collect();

        // Create test query
        let query: Vec<f32> = (0..padded_dim).map(|i| (i as f32) * 0.01).collect();

        // Pack ex_code using C++-compatible format
        let mut packed = vec![0u8; padded_dim / 16 * 12];
        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, padded_dim);

        // Method 1: Packed dot product (new)
        let result_packed = ip_packed_ex6_f32(&query, &packed, padded_dim);

        // Method 2: Unpacked dot product (reference)
        let result_unpacked = dot_u16_f32(&ex_code, &query);

        println!("Packed result:   {}", result_packed);
        println!("Unpacked result: {}", result_unpacked);
        println!(
            "Difference:      {}",
            (result_packed - result_unpacked).abs()
        );

        // Allow for floating point precision differences (especially with 6-bit packing)
        assert!(
            (result_packed - result_unpacked).abs() < 0.1,
            "Packed and unpacked results differ: {} vs {}",
            result_packed,
            result_unpacked
        );
    }
}

// ============================================================================
// C++-Compatible Packing Functions (SIMD-Optimized Format)
// ============================================================================
// These functions implement the same packing format as C++ RaBitQ Library,
// enabling direct SIMD operations on packed data without unpacking.
// Reference: C++ pack_excode.hpp (rabitqlib::quant::rabitq_impl::ex_bits)

/// Pack 1-bit ex-codes in C++-compatible format
/// Reference: C++ pack_excode.hpp:13-30 (packing_1bit_excode)
///
/// Packs 16 1-bit codes into 2 bytes (uint16) with simple bit positions.
/// - code[0] → bit 0
/// - code[1] → bit 1
/// - ...
/// - code[15] → bit 15
///
/// This is the simplest packing format, used for binary-only RaBitQ (ex_bits=0).
///
/// # Arguments
/// * `ex_code` - Input extended codes (u16 per element, only bit 0 used)
/// * `packed` - Output buffer for packed data (dim / 16 * 2 bytes)
/// * `dim` - Dimension (must be multiple of 16)
///
/// # Panics
/// Panics if dim is not a multiple of 16
pub fn pack_ex_code_1bit_cpp_compat(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert_eq!(dim % 16, 0, "dim must be multiple of 16 for 1-bit");
    debug_assert_eq!(packed.len(), dim / 16 * 2);

    let mut ex_idx = 0;
    let mut packed_idx = 0;

    while ex_idx < dim {
        // Pack 16 1-bit codes into uint16
        let mut code: u16 = 0;
        for i in 0..16 {
            code |= (ex_code[ex_idx + i] & 0x1) << i;
        }

        // Write as 2 bytes (little-endian)
        packed[packed_idx..packed_idx + 2].copy_from_slice(&code.to_le_bytes());

        ex_idx += 16;
        packed_idx += 2;
    }
}

/// Unpack 1-bit ex-codes from C++-compatible format
/// Reverse operation of pack_ex_code_1bit_cpp_compat
///
/// # Arguments
/// * `packed` - Input packed data (dim / 16 * 2 bytes)
/// * `ex_code` - Output buffer for unpacked codes (dim elements)
/// * `dim` - Dimension (must be multiple of 16)
#[allow(dead_code)]
pub fn unpack_ex_code_1bit_cpp_compat(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert_eq!(dim % 16, 0, "dim must be multiple of 16 for 1-bit");
    debug_assert_eq!(packed.len(), dim / 16 * 2);

    let mut ex_idx = 0;
    let mut packed_idx = 0;

    while ex_idx < dim {
        // Read 2 bytes as u16 (little-endian)
        let code = u16::from_le_bytes([packed[packed_idx], packed[packed_idx + 1]]);

        // Extract 16 1-bit codes
        for i in 0..16 {
            ex_code[ex_idx + i] = (code >> i) & 0x1;
        }

        ex_idx += 16;
        packed_idx += 2;
    }
}

/// Pack 2-bit ex-codes in C++-compatible format
/// Reference: C++ pack_excode.hpp:32-54 (packing_2bit_excode)
///
/// Packs 16 2-bit codes into 4 bytes (int32) with interleaved layout optimized for SIMD extraction.
/// The packing uses a special bit arrangement where codes are interleaved across bytes:
/// - byte 0: codes 0, 4, 8, 12 (each 2 bits)
/// - byte 1: codes 1, 5, 9, 13
/// - byte 2: codes 2, 6, 10, 14
/// - byte 3: codes 3, 7, 11, 15
///
/// This layout allows efficient SIMD unpacking using shifts and masks.
///
/// # Arguments
/// * `ex_code` - Input extended codes (u16 per element, only lower 2 bits used)
/// * `packed` - Output buffer for packed data (dim / 16 * 4 bytes)
/// * `dim` - Dimension (must be multiple of 16)
///
/// # Panics
/// Panics if dim is not a multiple of 16
pub fn pack_ex_code_2bit_cpp_compat(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert_eq!(dim % 16, 0, "dim must be multiple of 16 for 2-bit");
    debug_assert_eq!(packed.len(), dim / 16 * 4);

    let mut ex_idx = 0;
    let mut packed_idx = 0;

    while ex_idx < dim {
        // Load 16 codes (only lower 2 bits are used)
        let codes: [u16; 16] = [
            ex_code[ex_idx] & 0x3,
            ex_code[ex_idx + 1] & 0x3,
            ex_code[ex_idx + 2] & 0x3,
            ex_code[ex_idx + 3] & 0x3,
            ex_code[ex_idx + 4] & 0x3,
            ex_code[ex_idx + 5] & 0x3,
            ex_code[ex_idx + 6] & 0x3,
            ex_code[ex_idx + 7] & 0x3,
            ex_code[ex_idx + 8] & 0x3,
            ex_code[ex_idx + 9] & 0x3,
            ex_code[ex_idx + 10] & 0x3,
            ex_code[ex_idx + 11] & 0x3,
            ex_code[ex_idx + 12] & 0x3,
            ex_code[ex_idx + 13] & 0x3,
            ex_code[ex_idx + 14] & 0x3,
            ex_code[ex_idx + 15] & 0x3,
        ];

        // Simulate C++ int32 loads by grouping 4 codes into u32
        // In C++: int32_t code0 = *reinterpret_cast<const int32_t*>(o_raw);
        // This reads 4 bytes where each byte's lower 2 bits contain a code
        let code0: u32 = codes[0] as u32
            | ((codes[1] as u32) << 8)
            | ((codes[2] as u32) << 16)
            | ((codes[3] as u32) << 24);
        let code1: u32 = codes[4] as u32
            | ((codes[5] as u32) << 8)
            | ((codes[6] as u32) << 16)
            | ((codes[7] as u32) << 24);
        let code2: u32 = codes[8] as u32
            | ((codes[9] as u32) << 8)
            | ((codes[10] as u32) << 16)
            | ((codes[11] as u32) << 24);
        let code3: u32 = codes[12] as u32
            | ((codes[13] as u32) << 8)
            | ((codes[14] as u32) << 16)
            | ((codes[15] as u32) << 24);

        // Combine with C++ bit arrangement: compact = (code3 << 6) | (code2 << 4) | (code1 << 2) | code0
        // This creates an interleaved pattern where:
        // - bits 0-1, 8-9, 16-17, 24-25: from code0 (codes 0,1,2,3)
        // - bits 2-3, 10-11, 18-19, 26-27: from code1 (codes 4,5,6,7)
        // - bits 4-5, 12-13, 20-21, 28-29: from code2 (codes 8,9,10,11)
        // - bits 6-7, 14-15, 22-23, 30-31: from code3 (codes 12,13,14,15)
        let compact: u32 = (code3 << 6) | (code2 << 4) | (code1 << 2) | code0;

        // Write as 4 bytes (little-endian)
        packed[packed_idx..packed_idx + 4].copy_from_slice(&compact.to_le_bytes());

        ex_idx += 16;
        packed_idx += 4;
    }
}

/// Unpack 2-bit ex-codes from C++-compatible format
/// Reverse operation of pack_ex_code_2bit_cpp_compat
///
/// # Arguments
/// * `packed` - Input packed data (dim / 16 * 4 bytes)
/// * `ex_code` - Output buffer for unpacked codes (dim elements)
/// * `dim` - Dimension (must be multiple of 16)
#[allow(dead_code)]
pub fn unpack_ex_code_2bit_cpp_compat(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert_eq!(dim % 16, 0, "dim must be multiple of 16 for 2-bit");
    debug_assert_eq!(packed.len(), dim / 16 * 4);

    let mut ex_idx = 0;
    let mut packed_idx = 0;

    while ex_idx < dim {
        // Read 4 bytes as u32 (little-endian)
        let compact = u32::from_le_bytes([
            packed[packed_idx],
            packed[packed_idx + 1],
            packed[packed_idx + 2],
            packed[packed_idx + 3],
        ]);

        // Extract codes using the interleaved pattern
        // Each byte contains 4 codes (2 bits each)
        for i in 0..4 {
            let byte_offset = i * 8;
            // Extract 4 codes from this byte
            ex_code[ex_idx + i] = ((compact >> byte_offset) & 0x3) as u16; // codes 0,1,2,3
            ex_code[ex_idx + i + 4] = ((compact >> (byte_offset + 2)) & 0x3) as u16; // codes 4,5,6,7
            ex_code[ex_idx + i + 8] = ((compact >> (byte_offset + 4)) & 0x3) as u16; // codes 8,9,10,11
            ex_code[ex_idx + i + 12] = ((compact >> (byte_offset + 6)) & 0x3) as u16;
            // codes 12,13,14,15
        }

        ex_idx += 16;
        packed_idx += 4;
    }
}

/// Pack 6-bit ex-codes in C++-compatible format
/// Reference: C++ pack_excode.hpp:174-205 (packing_6bit_excode)
///
/// Packs 16 6-bit codes into 12 bytes by splitting each code into:
/// - Lower 4 bits → packed into 8 bytes (similar to 4-bit packing)
/// - Upper 2 bits → packed into 4 bytes (similar to 2-bit packing)
///
/// This split-packing strategy enables efficient SIMD extraction.
///
/// # Arguments
/// * `ex_code` - Input extended codes (u16 per element, only lower 6 bits used)
/// * `packed` - Output buffer for packed data (dim / 16 * 12 bytes)
/// * `dim` - Dimension (must be multiple of 16)
///
/// # Panics
/// Panics if dim is not a multiple of 16
pub fn pack_ex_code_6bit_cpp_compat(ex_code: &[u16], packed: &mut [u8], dim: usize) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert_eq!(dim % 16, 0, "dim must be multiple of 16 for 6-bit");
    debug_assert_eq!(packed.len(), dim / 16 * 12);

    const MASK_4: u64 = 0x0f0f0f0f0f0f0f0f; // Extract lower 4 bits from each byte
    const MASK_2: u32 = 0x30303030; // Extract bits 4-5 from each byte (upper 2 bits of 6-bit code)

    let mut ex_idx = 0;
    let mut packed_idx = 0;

    while ex_idx < dim {
        // Extract the 16 codes
        let codes: [u16; 16] = [
            ex_code[ex_idx] & 0x3F,
            ex_code[ex_idx + 1] & 0x3F,
            ex_code[ex_idx + 2] & 0x3F,
            ex_code[ex_idx + 3] & 0x3F,
            ex_code[ex_idx + 4] & 0x3F,
            ex_code[ex_idx + 5] & 0x3F,
            ex_code[ex_idx + 6] & 0x3F,
            ex_code[ex_idx + 7] & 0x3F,
            ex_code[ex_idx + 8] & 0x3F,
            ex_code[ex_idx + 9] & 0x3F,
            ex_code[ex_idx + 10] & 0x3F,
            ex_code[ex_idx + 11] & 0x3F,
            ex_code[ex_idx + 12] & 0x3F,
            ex_code[ex_idx + 13] & 0x3F,
            ex_code[ex_idx + 14] & 0x3F,
            ex_code[ex_idx + 15] & 0x3F,
        ];

        // ===== Part 1: Pack lower 4 bits (8 bytes) =====
        // Simulate C++ int64 loads for codes 0-7 and 8-15
        let code4_0: u64 = codes[0] as u64
            | ((codes[1] as u64) << 8)
            | ((codes[2] as u64) << 16)
            | ((codes[3] as u64) << 24)
            | ((codes[4] as u64) << 32)
            | ((codes[5] as u64) << 40)
            | ((codes[6] as u64) << 48)
            | ((codes[7] as u64) << 56);

        let code4_1: u64 = codes[8] as u64
            | ((codes[9] as u64) << 8)
            | ((codes[10] as u64) << 16)
            | ((codes[11] as u64) << 24)
            | ((codes[12] as u64) << 32)
            | ((codes[13] as u64) << 40)
            | ((codes[14] as u64) << 48)
            | ((codes[15] as u64) << 56);

        // Pack lower 4 bits: compact4 = ((code4_1 & MASK_4) << 4) | (code4_0 & MASK_4)
        let compact4 = ((code4_1 & MASK_4) << 4) | (code4_0 & MASK_4);

        // Write 8 bytes
        packed[packed_idx..packed_idx + 8].copy_from_slice(&compact4.to_le_bytes());
        packed_idx += 8;

        // ===== Part 2: Pack upper 2 bits (4 bytes) =====
        // These are bits 4-5 of each 6-bit code (at bit positions 4-5 in the byte)
        let code2_0: u32 = codes[0] as u32
            | ((codes[1] as u32) << 8)
            | ((codes[2] as u32) << 16)
            | ((codes[3] as u32) << 24);

        let code2_1: u32 = codes[4] as u32
            | ((codes[5] as u32) << 8)
            | ((codes[6] as u32) << 16)
            | ((codes[7] as u32) << 24);

        let code2_2: u32 = codes[8] as u32
            | ((codes[9] as u32) << 8)
            | ((codes[10] as u32) << 16)
            | ((codes[11] as u32) << 24);

        let code2_3: u32 = codes[12] as u32
            | ((codes[13] as u32) << 8)
            | ((codes[14] as u32) << 16)
            | ((codes[15] as u32) << 24);

        // Pack upper 2 bits (bits 4-5):
        // compact2 = ((code2_3 & MASK_2) << 2) | (code2_2 & MASK_2) | ((code2_1 & MASK_2) >> 2) | ((code2_0 & MASK_2) >> 4)
        let compact2 = ((code2_3 & MASK_2) << 2)
            | (code2_2 & MASK_2)
            | ((code2_1 & MASK_2) >> 2)
            | ((code2_0 & MASK_2) >> 4);

        // Write 4 bytes
        packed[packed_idx..packed_idx + 4].copy_from_slice(&compact2.to_le_bytes());
        packed_idx += 4;

        ex_idx += 16;
    }
}

/// Unpack 6-bit ex-codes from C++-compatible format
/// Reverse operation of pack_ex_code_6bit_cpp_compat
///
/// # Arguments
/// * `packed` - Input packed data (dim / 16 * 12 bytes)
/// * `ex_code` - Output buffer for unpacked codes (dim elements)
/// * `dim` - Dimension (must be multiple of 16)
#[allow(dead_code)]
pub fn unpack_ex_code_6bit_cpp_compat(packed: &[u8], ex_code: &mut [u16], dim: usize) {
    debug_assert_eq!(ex_code.len(), dim);
    debug_assert_eq!(dim % 16, 0, "dim must be multiple of 16 for 6-bit");
    debug_assert_eq!(packed.len(), dim / 16 * 12);

    let mut ex_idx = 0;
    let mut packed_idx = 0;

    while ex_idx < dim {
        // Read 8 bytes containing lower 4 bits
        let compact4 = u64::from_le_bytes([
            packed[packed_idx],
            packed[packed_idx + 1],
            packed[packed_idx + 2],
            packed[packed_idx + 3],
            packed[packed_idx + 4],
            packed[packed_idx + 5],
            packed[packed_idx + 6],
            packed[packed_idx + 7],
        ]);
        packed_idx += 8;

        // Read 4 bytes containing upper 2 bits
        let compact2 = u32::from_le_bytes([
            packed[packed_idx],
            packed[packed_idx + 1],
            packed[packed_idx + 2],
            packed[packed_idx + 3],
        ]);
        packed_idx += 4;

        // Extract lower 4 bits for all 16 codes
        let mut lower_4bits = [0u16; 16];
        for i in 0..8 {
            let byte_offset = i * 8;
            // Even codes (0, 2, 4, ..., 14) are in lower nibbles
            lower_4bits[i] = ((compact4 >> byte_offset) & 0x0F) as u16;
            // Odd codes (1, 3, 5, ..., 15) are in upper nibbles
            lower_4bits[i + 8] = ((compact4 >> (byte_offset + 4)) & 0x0F) as u16;
        }

        // Extract upper 2 bits for all 16 codes
        // The compact2 layout is identical to 2-bit packing (interleaved pattern)
        let mut upper_2bits = [0u16; 16];
        for i in 0..4 {
            let byte_offset = i * 8;
            // Extract 4 groups of 2-bit values from each byte
            upper_2bits[i] = ((compact2 >> byte_offset) & 0x3) as u16; // codes 0,1,2,3
            upper_2bits[i + 4] = ((compact2 >> (byte_offset + 2)) & 0x3) as u16; // codes 4,5,6,7
            upper_2bits[i + 8] = ((compact2 >> (byte_offset + 4)) & 0x3) as u16; // codes 8,9,10,11
            upper_2bits[i + 12] = ((compact2 >> (byte_offset + 6)) & 0x3) as u16;
            // codes 12,13,14,15
        }

        // Combine lower 4 bits and upper 2 bits to form 6-bit codes
        for i in 0..16 {
            ex_code[ex_idx + i] = lower_4bits[i] | (upper_2bits[i] << 4);
        }

        ex_idx += 16;
    }
}

#[cfg(test)]
mod cpp_compat_tests {
    use super::*;

    // ===== 1-bit packing tests =====

    #[test]
    fn test_pack_unpack_1bit_roundtrip() {
        // Test roundtrip: pack then unpack should recover original data
        let dim = 32;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 2) as u16).collect();
        let mut packed = vec![0u8; dim / 16 * 2];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_1bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_1bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(ex_code, unpacked, "1-bit roundtrip failed");
    }

    #[test]
    fn test_pack_1bit_alternating_pattern() {
        // Test alternating 0, 1, 0, 1, ... pattern
        let dim = 16;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 2) as u16).collect();
        let mut packed = vec![0u8; 2];

        pack_ex_code_1bit_cpp_compat(&ex_code, &mut packed, dim);

        // Pattern: 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1
        // Bits: bit 0 = 0, bit 1 = 1, bit 2 = 0, bit 3 = 1, ...
        // This gives: 0b1010101010101010 = 0xAAAA
        let expected = 0xAAAAu16.to_le_bytes();
        assert_eq!(
            packed, expected,
            "1-bit alternating pattern mismatch. Got {:02x?}, expected {:02x?}",
            packed, expected
        );
    }

    #[test]
    fn test_pack_1bit_all_zeros() {
        let dim = 16;
        let ex_code = vec![0u16; dim];
        let mut packed = vec![0u8; 2];

        pack_ex_code_1bit_cpp_compat(&ex_code, &mut packed, dim);

        assert_eq!(packed, [0x00, 0x00]);
    }

    #[test]
    fn test_pack_1bit_all_ones() {
        let dim = 16;
        let ex_code = vec![1u16; dim];
        let mut packed = vec![0u8; 2];

        pack_ex_code_1bit_cpp_compat(&ex_code, &mut packed, dim);

        // All bits set = 0xFFFF
        assert_eq!(packed, [0xFF, 0xFF]);
    }

    #[test]
    fn test_pack_1bit_specific_pattern() {
        // Test specific bit pattern: first 8 are 1, last 8 are 0
        let dim = 16;
        let mut ex_code = vec![0u16; dim];
        for item in ex_code.iter_mut().take(8) {
            *item = 1;
        }
        let mut packed = vec![0u8; 2];

        pack_ex_code_1bit_cpp_compat(&ex_code, &mut packed, dim);

        // Bits 0-7 set, bits 8-15 clear = 0x00FF
        let expected = 0x00FFu16.to_le_bytes();
        assert_eq!(packed, expected);
    }

    #[test]
    fn test_pack_1bit_large_dimension() {
        // Test with GIST dimension (960)
        let dim = 960;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 2) as u16).collect();
        let mut packed = vec![0u8; dim / 16 * 2];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_1bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_1bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(ex_code, unpacked, "1-bit large dimension roundtrip failed");
    }

    // ===== 2-bit packing tests =====

    #[test]
    fn test_pack_unpack_2bit_roundtrip() {
        // Test roundtrip: pack then unpack should recover original data
        let dim = 32;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 4) as u16).collect();
        let mut packed = vec![0u8; dim / 16 * 4];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_2bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(
            ex_code, unpacked,
            "Roundtrip failed: pack/unpack doesn't preserve data"
        );
    }

    #[test]
    fn test_pack_2bit_specific_pattern() {
        // Test specific bit pattern to verify C++ compatibility
        // Pattern: [0, 1, 2, 3, 0, 1, 2, 3, ...]
        let dim = 16;
        let ex_code: Vec<u16> = vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3];
        let mut packed = vec![0u8; 4];

        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, dim);

        // Verify interleaved packing format
        // According to C++ logic:
        // code0 = 0 | (1<<8) | (2<<16) | (3<<24) = 0x03020100
        // code1 = 0 | (1<<8) | (2<<16) | (3<<24) = 0x03020100
        // code2 = 0 | (1<<8) | (2<<16) | (3<<24) = 0x03020100
        // code3 = 0 | (1<<8) | (2<<16) | (3<<24) = 0x03020100
        //
        // compact = (code3 << 6) | (code2 << 4) | (code1 << 2) | code0
        //
        // Let's work through this:
        // code0 = 0x03020100, lower 2 bits of each byte: 0x00, 0x01, 0x02, 0x03
        //   bits 0-1: 0b00, bits 8-9: 0b01, bits 16-17: 0b10, bits 24-25: 0b11
        //
        // code1 << 2 (same pattern shifted by 2):
        //   bits 2-3: 0b00, bits 10-11: 0b01, bits 18-19: 0b10, bits 26-27: 0b11
        //
        // code2 << 4:
        //   bits 4-5: 0b00, bits 12-13: 0b01, bits 20-21: 0b10, bits 28-29: 0b11
        //
        // code3 << 6:
        //   bits 6-7: 0b00, bits 14-15: 0b01, bits 22-23: 0b10, bits 30-31: 0b11
        //
        // Combining:
        // byte 0 (bits 0-7):   0b00_00_00_00 = 0x00
        // byte 1 (bits 8-15):  0b01_01_01_01 = 0x55
        // byte 2 (bits 16-23): 0b10_10_10_10 = 0xAA
        // byte 3 (bits 24-31): 0b11_11_11_11 = 0xFF

        let expected = [0x00, 0x55, 0xAA, 0xFF];
        assert_eq!(
            packed, expected,
            "Packing format doesn't match expected C++ output. Got {:02x?}, expected {:02x?}",
            packed, expected
        );
    }

    #[test]
    fn test_pack_2bit_all_zeros() {
        let dim = 16;
        let ex_code = vec![0u16; dim];
        let mut packed = vec![0u8; 4];

        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, dim);

        assert_eq!(packed, [0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_pack_2bit_all_threes() {
        let dim = 16;
        let ex_code = vec![3u16; dim];
        let mut packed = vec![0u8; 4];

        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, dim);

        // All codes are 0b11, so every 2-bit position should be 0b11
        // This gives 0xFF for all bytes
        assert_eq!(packed, [0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_pack_2bit_large_dimension() {
        // Test with realistic dimension (GIST: 960)
        let dim = 960;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 4) as u16).collect();
        let mut packed = vec![0u8; dim / 16 * 4];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_2bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(ex_code, unpacked, "Large dimension roundtrip failed");
    }

    #[test]
    fn test_unpack_2bit_matches_manual_extraction() {
        // Manually create a packed buffer and verify unpacking
        let packed = [0b10010100u8, 0b11110000, 0x00, 0xFF];
        let mut unpacked = vec![0u16; 16];

        unpack_ex_code_2bit_cpp_compat(&packed, &mut unpacked, 16);

        // Manually verify a few positions
        // byte 0 = 0b10010100
        //   bits 0-1: 0b00 -> code 0 = 0
        //   bits 2-3: 0b01 -> code 4 = 1
        //   bits 4-5: 0b01 -> code 8 = 1
        //   bits 6-7: 0b10 -> code 12 = 2

        assert_eq!(unpacked[0], 0, "Code 0 mismatch");
        assert_eq!(unpacked[4], 1, "Code 4 mismatch");
        assert_eq!(unpacked[8], 1, "Code 8 mismatch");
        assert_eq!(unpacked[12], 2, "Code 12 mismatch");
    }

    #[test]
    fn test_pack_6bit_roundtrip() {
        // Test 6-bit packing roundtrip
        let dim = 32;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 64) as u16).collect();
        let mut packed = vec![0u8; dim / 16 * 12];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_6bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(ex_code, unpacked, "6-bit roundtrip failed");
    }

    #[test]
    fn test_pack_6bit_specific_values() {
        // Test specific 6-bit values
        let dim = 16;
        // Pattern: 0, 15, 31, 47, 63, 0, 15, 31, 47, 63, ...
        let ex_code: Vec<u16> =
            vec![0, 15, 31, 47, 63, 0, 15, 31, 47, 63, 0, 15, 31, 47, 63, 0];
        let mut packed = vec![0u8; 12];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_6bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(ex_code, unpacked, "6-bit specific values roundtrip failed");
    }

    #[test]
    fn test_pack_6bit_all_zeros() {
        let dim = 16;
        let ex_code = vec![0u16; dim];
        let mut packed = vec![0u8; 12];

        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, dim);

        assert_eq!(packed, vec![0u8; 12]);
    }

    #[test]
    fn test_pack_6bit_all_max() {
        let dim = 16;
        let ex_code = vec![63u16; dim]; // 0b111111 = max 6-bit value
        let mut packed = vec![0u8; 12];

        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, dim);

        // All bits should be set to 1
        assert_eq!(packed, vec![0xFFu8; 12]);
    }

    #[test]
    fn test_pack_6bit_large_dimension() {
        // Test with GIST dimension (960)
        let dim = 960;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 64) as u16).collect();
        let mut packed = vec![0u8; dim / 16 * 12];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_6bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(ex_code, unpacked, "6-bit large dimension roundtrip failed");
    }

    // ===== SIMD dot product tests with C++ packing =====

    #[test]
    fn test_simd_2bit_simple_debug() {
        // Simple test with 16 elements to debug
        let dim = 16;
        let ex_code: Vec<u16> = vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3];
        let query: Vec<f32> = vec![1.0; 16];

        // Pack using C++ compatible format
        let mut packed = vec![0u8; 4];
        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, dim);

        println!("Packed bytes: {:02x?}", packed);

        // Unpack to verify
        let mut unpacked = vec![0u16; 16];
        unpack_ex_code_2bit_cpp_compat(&packed, &mut unpacked, dim);
        println!("Unpacked: {:?}", unpacked);
        println!("Original: {:?}", ex_code);

        // Method 1: SIMD dot product
        let result_simd = ip_packed_ex2_f32(&query, &packed, dim);

        // Method 2: Reference
        let result_ref = dot_u16_f32(&ex_code, &query);

        println!("SIMD result:      {}", result_simd);
        println!("Reference result: {}", result_ref);

        assert_eq!(unpacked, ex_code, "Unpack doesn't match original");
        assert!((result_simd - result_ref).abs() < 0.01, "SIMD mismatch");
    }

    #[test]
    fn test_simd_2bit_vs_reference() {
        // Test that SIMD dot product with C++ packing matches reference
        let dim = 960;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 4) as u16).collect();
        let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01).collect();

        // Pack using C++ compatible format
        let mut packed = vec![0u8; dim / 16 * 4];
        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, dim);

        // Method 1: SIMD dot product on packed data (using existing ip_packed_ex2_f32)
        let result_simd = ip_packed_ex2_f32(&query, &packed, dim);

        // Method 2: Reference (scalar dot product on unpacked data)
        let result_ref = dot_u16_f32(&ex_code, &query);

        println!("SIMD result:      {}", result_simd);
        println!("Reference result: {}", result_ref);
        println!("Difference:       {}", (result_simd - result_ref).abs());

        assert!(
            (result_simd - result_ref).abs() < 0.1,
            "SIMD 2-bit result differs from reference: {} vs {}",
            result_simd,
            result_ref
        );
    }

    #[test]
    fn test_simd_6bit_vs_reference() {
        // Test that SIMD dot product with C++ packing matches reference
        let dim = 960;
        let ex_code: Vec<u16> = (0..dim).map(|i| (i % 64) as u16).collect();
        let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.01).collect();

        // Pack using C++ compatible format
        let mut packed = vec![0u8; dim / 16 * 12];
        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, dim);

        // Method 1: SIMD dot product on packed data (using existing ip_packed_ex6_f32)
        let result_simd = ip_packed_ex6_f32(&query, &packed, dim);

        // Method 2: Reference (scalar dot product on unpacked data)
        let result_ref = dot_u16_f32(&ex_code, &query);

        println!("SIMD result:      {}", result_simd);
        println!("Reference result: {}", result_ref);
        println!("Difference:       {}", (result_simd - result_ref).abs());

        assert!(
            (result_simd - result_ref).abs() < 0.1,
            "SIMD 6-bit result differs from reference: {} vs {}",
            result_simd,
            result_ref
        );
    }
}

// ============================================================================
// Phase 3: Function Dispatch System
// ============================================================================

/// Function pointer type for ex-code inner product computation on packed data
///
/// This function type represents optimized SIMD inner product functions that
/// operate directly on C++-compatible packed ex-code data without unpacking.
///
/// # Arguments
/// * `query: &[f32]` - Rotated query vector (padded_dim elements)
/// * `packed_ex_code: &[u8]` - Packed ex-code data (C++-compatible format)
/// * `padded_dim: usize` - Padded dimension (must be multiple of 16)
///
/// # Returns
/// Inner product sum as f32
pub type ExIpFunc = fn(&[f32], &[u8], usize) -> f32;

/// Inner product function for ex_bits=0 (binary-only RaBitQ)
///
/// When ex_bits=0, all ex-codes are zero, so the dot product is always 0.0
#[inline]
pub fn ip_packed_ex0_f32(
    _query: &[f32],
    _packed_ex_code: &[u8],
    _padded_dim: usize,
) -> f32 {
    0.0 // All ex-codes are zero when ex_bits=0
}

/// Select the appropriate ex-code inner product function based on ex_bits
///
/// This function returns a function pointer to the optimized SIMD inner product
/// implementation for the specified ex_bits configuration. The returned function
/// operates directly on C++-compatible packed data without unpacking overhead.
///
/// # Arguments
/// * `ex_bits` - Number of extra bits (0, 2, or 6)
///   - 0: Binary-only (1-bit total), returns constant 0.0
///   - 2: 3-bit total RaBitQ, uses `ip_packed_ex2_f32`
///   - 6: 7-bit total RaBitQ, uses `ip_packed_ex6_f32`
///
/// # Returns
/// Function pointer to the appropriate SIMD inner product function
///
/// # Panics
/// Panics if ex_bits is not 0, 2, or 6 (unsupported configurations)
///
/// # Example
/// ```ignore
/// use rabitq_rs::simd::{select_excode_ipfunc, pack_ex_code_2bit_cpp_compat};
///
/// let ex_bits = 2; // 3-bit total
/// let ip_func = select_excode_ipfunc(ex_bits);
///
/// // Use function pointer for dot product on packed data
/// let query = vec![1.0f32; 960];
/// let ex_code = vec![1u16; 960];
/// let mut packed = vec![0u8; 960 / 16 * 4];
/// pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, 960);
///
/// let result = ip_func(&query, &packed, 960);
/// assert!((result - 960.0).abs() < 1e-3);
/// ```
pub fn select_excode_ipfunc(ex_bits: usize) -> ExIpFunc {
    match ex_bits {
        0 => ip_packed_ex0_f32, // 1-bit total (binary only)
        2 => ip_packed_ex2_f32, // 3-bit total
        6 => ip_packed_ex6_f32, // 7-bit total
        _ => panic!(
            "Unsupported ex_bits: {}. Only 0 (1-bit total), 2 (3-bit total), and 6 (7-bit total) are supported.",
            ex_bits
        ),
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;

    #[test]
    fn test_select_excode_ipfunc_ex0() {
        let ip_func = select_excode_ipfunc(0);
        let query = vec![1.0f32; 960];
        let packed = vec![0u8; 960 / 16 * 2];
        let result = ip_func(&query, &packed, 960);
        assert_eq!(result, 0.0, "ex_bits=0 should always return 0.0");
    }

    #[test]
    fn test_select_excode_ipfunc_ex2() {
        let ip_func = select_excode_ipfunc(2);
        let query = vec![1.0f32; 960];
        let ex_code = vec![2u16; 960];
        let mut packed = vec![0u8; 960 / 16 * 4];
        pack_ex_code_2bit_cpp_compat(&ex_code, &mut packed, 960);

        let result = ip_func(&query, &packed, 960);
        assert!((result - 1920.0).abs() < 1e-3, "ex_bits=2: 960 * 2 = 1920");
    }

    #[test]
    fn test_select_excode_ipfunc_ex6() {
        let ip_func = select_excode_ipfunc(6);
        let query = vec![1.0f32; 960];
        let ex_code = vec![10u16; 960];
        let mut packed = vec![0u8; 960 / 16 * 12];
        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, 960);

        let result = ip_func(&query, &packed, 960);
        assert!((result - 9600.0).abs() < 1e-3, "ex_bits=6: 960 * 10 = 9600");
    }

    #[test]
    #[should_panic(expected = "Unsupported ex_bits: 1")]
    fn test_select_excode_ipfunc_unsupported() {
        select_excode_ipfunc(1); // Should panic
    }

    #[test]
    fn test_pack_unpack_binary_roundtrip() {
        use rand::Rng;
        let dim: usize = 32;
        let mut rng = rand::thread_rng();
        let binary_code: Vec<u8> = (0..dim).map(|_| rng.gen_range(0..2)).collect();
        let mut packed = vec![0u8; dim.div_ceil(8)];
        let mut unpacked = vec![0u8; dim];

        pack_binary_code(&binary_code, &mut packed, dim);
        unpack_binary_code(&packed, &mut unpacked, dim);

        assert_eq!(binary_code, unpacked, "Binary code roundtrip failed");
    }

    #[test]
    fn test_pack_unpack_6bit_roundtrip() {
        use rand::Rng;
        let dim = 32;
        let mut rng = rand::thread_rng();
        let ex_code: Vec<u16> = (0..dim).map(|_| rng.gen_range(0..64)).collect();
        let mut packed = vec![0u8; dim / 16 * 12];
        let mut unpacked = vec![0u16; dim];

        pack_ex_code_6bit_cpp_compat(&ex_code, &mut packed, dim);
        unpack_ex_code_6bit_cpp_compat(&packed, &mut unpacked, dim);

        assert_eq!(ex_code, unpacked, "6-bit roundtrip failed");
    }
}
