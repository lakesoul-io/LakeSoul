// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul Contributors

//! Micro-benchmarks for the FastScan pipeline.
//!
//! These attribute the per-query cost of an IVF search to its stages:
//!
//! * `accumulate` — the SIMD nibble gather over one 32-vector batch,
//! * `batch_estdist` — `accumulate` plus the distance/lower-bound post-processing,
//! * `refine_ex6` — one extended-code refinement of a candidate,
//! * `build_query_lut` — the per-query lookup table construction.
//!
//! Multiplying the measured per-operation cost with the counters returned by
//! `IvfRabitqIndex::search_with_diagnostics` gives the search time split
//! (see `--search-stats` in the vector benchmark runner).
//!
//! Dimensions are the padded ones the FhtKac rotator produces: GloVe-200d →
//! 256, GIST-960d → 960.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use lakesoul_vector::rabitq::fastscan_kernel::{
    FASTSCAN_BATCH_SIZE, FastScanLutView, accumulate_batch, build_query_lut,
    compute_fastscan_batch, refine_distance_with_ex, select_ex_ip_func,
};

/// Packed binary codes of one batch (32 vectors x `dim` bits), as the
/// unified cluster layout stores them.
fn packed_codes(dim: usize) -> Vec<u8> {
    (0..dim * FASTSCAN_BATCH_SIZE / 8)
        .map(|i| (i * 37 % 251) as u8)
        .collect()
}

/// Packed 6-bit extended codes of one vector.
fn packed_ex_codes(dim: usize) -> Vec<u8> {
    (0..(dim * 6).div_ceil(8))
        .map(|i| (i * 53 % 251) as u8)
        .collect()
}

fn synthetic_query(dim: usize) -> Vec<f32> {
    (0..dim).map(|i| ((i as f32) * 0.013).sin() * 0.5).collect()
}

fn bench_dimension(c: &mut Criterion, dim: usize) {
    let packed = packed_codes(dim);
    let query = synthetic_query(dim);
    let (lut_i8, delta, sum_vl_lut) = build_query_lut(&query, dim);

    let f_add: Vec<f32> = (0..FASTSCAN_BATCH_SIZE).map(|i| i as f32 * 0.01).collect();
    let f_rescale = vec![-1.0f32; FASTSCAN_BATCH_SIZE];
    let f_error = vec![0.5f32; FASTSCAN_BATCH_SIZE];

    let mut group = c.benchmark_group(format!("fastscan/dim{dim}"));
    group.throughput(Throughput::Elements(FASTSCAN_BATCH_SIZE as u64));

    let mut accu = [0u16; FASTSCAN_BATCH_SIZE];
    group.bench_function("accumulate", |b| {
        b.iter(|| accumulate_batch(&packed, &lut_i8, dim, &mut accu));
    });

    let mut ip_x0_qr = [0.0f32; FASTSCAN_BATCH_SIZE];
    let mut est = [0.0f32; FASTSCAN_BATCH_SIZE];
    let mut lower = [0.0f32; FASTSCAN_BATCH_SIZE];
    group.bench_function("batch_estdist", |b| {
        b.iter(|| {
            compute_fastscan_batch(
                FastScanLutView::Regular {
                    lut_i8: &lut_i8,
                    delta,
                    sum_vl_lut,
                },
                &packed,
                dim,
                &f_add,
                &f_rescale,
                &f_error,
                1.0,
                1.0,
                0.0,
                &mut ip_x0_qr,
                &mut est,
                &mut lower,
            )
        });
    });
    group.finish();

    let ex_packed = packed_ex_codes(dim);
    let ip_func = select_ex_ip_func(6).expect("6-bit refinement kernel");
    let mut group = c.benchmark_group(format!("refine/dim{dim}"));
    group.bench_function("ex6", |b| {
        b.iter(|| {
            refine_distance_with_ex(
                &query,
                &ex_packed,
                dim,
                6,
                0.5,
                1.0,
                0.0,
                1.0,
                0.0,
                1.0,
                Some(ip_func),
            )
        });
    });
    group.finish();

    let mut group = c.benchmark_group(format!("query/dim{dim}"));
    group.bench_function("build_query_lut", |b| {
        b.iter(|| build_query_lut(&query, dim));
    });
    group.finish();
}

fn bench_fastscan(c: &mut Criterion) {
    bench_dimension(c, 256);
    bench_dimension(c, 960);
}

criterion_group!(benches, bench_fastscan);
criterion_main!(benches);
