// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::io::{ErrorKind, Read, Result};

/// Try to fill buf with data from source, dealing with short reads such as
/// caused by Chain.
///
/// Errors: See `std::io::Read`.
fn read_bytes<R>(source: &mut R, buf: &mut [u8]) -> Result<usize>
where
    R: Read,
{
    let mut offset = 0;
    loop {
        match source.read(&mut buf[offset..]) {
            Ok(0) => {
                return Ok(offset);
            }
            Ok(n) => {
                offset += n;
                if offset == buf.len() {
                    return Ok(offset);
                }
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => {
                return Err(e);
            }
        }
    }
}


const C1: u32 = 0x85eb_ca6b;
const C2: u32 = 0xc2b2_ae35;
const R1: u32 = 16;
const R2: u32 = 13;
const M: u32 = 5;
const N: u32 = 0xe654_6b64;

pub fn spark_murmur3_32_for_bytes<T: Read>(source: &mut T, seed: u32) -> Result<u32> {
    let mut buffer: [u8; 4] = [0; 4];
    let mut processed = 0;
    let mut state = seed;
    loop {
        match read_bytes(source, &mut buffer)? {
            4 => {
                processed += 4;
                let k = u32::from_le_bytes(buffer);
                state ^= calc_k(k);
                state = state.rotate_left(R2);
                state = (state.wrapping_mul(M)).wrapping_add(N);
            }
            0 => return Ok(finish(state, processed)),
            n if n < 4 => {
                processed += n as u32;
                for k in buffer.iter().take(n) {
                    state ^= calc_k(*k as u32);
                    state = state.rotate_left(R2);
                    state = (state.wrapping_mul(M)).wrapping_add(N);
                }
            }
            _ => panic!("Internal buffer state failure"),
        }
    }
}

fn finish(state: u32, processed: u32) -> u32 {
    let mut hash = state;
    hash ^= processed;
    hash ^= hash.wrapping_shr(R1);
    hash = hash.wrapping_mul(C1);
    hash ^= hash.wrapping_shr(R2);
    hash = hash.wrapping_mul(C2);
    hash ^= hash.wrapping_shr(R1);
    hash
}

fn calc_k(k: u32) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    const R1: u32 = 15;
    k.wrapping_mul(C1).rotate_left(R1).wrapping_mul(C2)
}
