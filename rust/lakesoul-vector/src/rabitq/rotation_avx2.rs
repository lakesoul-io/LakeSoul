//! Flip eight IEEE-754 sign bits using one packed mask byte; no floating work
//! or heap workspace, and NaN payloads and signed zeros remain bit exact.

#![allow(unsafe_op_in_unsafe_fn, clippy::chunks_exact_to_as_chunks)]

use std::arch::x86_64::*;

/// Caller checks AVX2. Slice lengths bound every load/store and mask-byte read.
#[inline]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn flip_sign(data: &mut [f32], flip_bits: &[u8]) {
    let blocks = (data.len() / 8).min(flip_bits.len());
    let shifts = _mm256_setr_epi32(31, 30, 29, 28, 27, 26, 25, 24);
    let sign_bit = _mm256_set1_epi32(i32::MIN);
    for (chunk, &byte) in data[..blocks * 8]
        .chunks_exact_mut(8)
        .zip(&flip_bits[..blocks])
    {
        let bits = _mm256_set1_epi32(byte as i32);
        let mask = _mm256_and_si256(_mm256_sllv_epi32(bits, shifts), sign_bit);
        let values = _mm256_loadu_si256(chunk.as_ptr().cast());
        _mm256_storeu_si256(chunk.as_mut_ptr().cast(), _mm256_xor_si256(values, mask));
    }
    if let Some(&byte) = flip_bits.get(blocks) {
        for (bit, value) in data[blocks * 8..].iter_mut().take(7).enumerate() {
            *value = f32::from_bits(value.to_bits() ^ (((byte >> bit) & 1) as u32) << 31);
        }
    }
}

/// Execute the h=1,2,4 butterflies in registers, in the original order.
/// The caller supplies a power-of-two slice of at least eight floats and
/// checks AVX2. Every load/store is confined to one eight-float block.
#[inline]
#[target_feature(enable = "avx2")]
pub(super) unsafe fn fht_first_three(data: &mut [f32]) {
    debug_assert!(data.len() >= 8 && data.len().is_multiple_of(8));
    for block in data.chunks_exact_mut(8) {
        let v = _mm256_loadu_ps(block.as_ptr());
        let x = _mm256_moveldup_ps(v);
        let y = _mm256_movehdup_ps(v);
        let v = _mm256_blend_ps::<0xaa>(_mm256_add_ps(x, y), _mm256_sub_ps(x, y));
        let x = _mm256_shuffle_ps::<0x44>(v, v);
        let y = _mm256_shuffle_ps::<0xee>(v, v);
        let v = _mm256_blend_ps::<0xcc>(_mm256_add_ps(x, y), _mm256_sub_ps(x, y));
        let x = _mm256_permute2f128_ps::<0x00>(v, v);
        let y = _mm256_permute2f128_ps::<0x11>(v, v);
        let v = _mm256_blend_ps::<0xf0>(_mm256_add_ps(x, y), _mm256_sub_ps(x, y));
        _mm256_storeu_ps(block.as_mut_ptr(), v);
    }
}
