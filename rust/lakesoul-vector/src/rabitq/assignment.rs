//! Exact centroid reductions shared by training and streaming assignment.
//!
//! Every lane keeps its first strict minimum. The final reduction compares
//! cluster indices on ties, matching the original left-to-right scalar scan.

#![allow(unsafe_op_in_unsafe_fn)]

#[inline]
pub(crate) fn nearest_l2(dots: &[f32], norms: &[f32], norm: f32) -> (usize, f32) {
    debug_assert_eq!(dots.len(), norms.len());
    #[cfg(target_arch = "x86_64")]
    if dots.len() >= 16
        && dots.len() <= i32::MAX as usize
        && is_x86_feature_detected!("avx2")
    {
        // SAFETY: AVX2 is checked, and all loads are bounded by dots.len().
        return unsafe { x86::nearest_l2(dots, norms, norm) };
    }
    scalar_l2(dots, norms, norm)
}

#[inline]
pub(crate) fn nearest_ip(dots: &[f32]) -> usize {
    #[cfg(target_arch = "x86_64")]
    if dots.len() >= 16
        && dots.len() <= i32::MAX as usize
        && is_x86_feature_detected!("avx2")
    {
        // SAFETY: AVX2 is checked, and all loads are bounded by dots.len().
        return unsafe { x86::nearest_ip(dots) };
    }
    scalar_ip(dots)
}

#[inline]
fn scalar_l2(dots: &[f32], norms: &[f32], norm: f32) -> (usize, f32) {
    let mut best = (0, f32::INFINITY);
    for (cid, (&dot, &centroid_norm)) in dots.iter().zip(norms).enumerate() {
        let mut distance = norm + centroid_norm - 2.0 * dot;
        if distance < 0.0 {
            distance = 0.0;
        }
        if distance < best.1 {
            best = (cid, distance);
        }
    }
    best
}

#[inline]
fn scalar_ip(dots: &[f32]) -> usize {
    let mut best = (0, f32::INFINITY);
    for (cid, &dot) in dots.iter().enumerate() {
        let distance = -dot;
        if distance < best.1 {
            best = (cid, distance);
        }
    }
    best.0
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn nearest_l2(
        dots: &[f32],
        norms: &[f32],
        norm: f32,
    ) -> (usize, f32) {
        let mut minima = _mm256_set1_ps(f32::INFINITY);
        let mut indices = _mm256_setzero_si256();
        let mut cid = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
        let step = _mm256_set1_epi32(8);
        let norm_v = _mm256_set1_ps(norm);
        let two = _mm256_set1_ps(2.0);
        let zero = _mm256_setzero_ps();
        let len = dots.len() / 8 * 8;
        for offset in (0..len).step_by(8) {
            let dot = _mm256_loadu_ps(dots.as_ptr().add(offset));
            let center = _mm256_loadu_ps(norms.as_ptr().add(offset));
            let distance =
                _mm256_sub_ps(_mm256_add_ps(norm_v, center), _mm256_mul_ps(two, dot));
            // The second max operand preserves NaN/signed zero behavior of
            // `if distance < 0.0 { distance = 0.0 }` in the reference scan.
            let distance = _mm256_max_ps(zero, distance);
            let better = _mm256_cmp_ps::<_CMP_LT_OQ>(distance, minima);
            minima = _mm256_blendv_ps(minima, distance, better);
            indices = _mm256_blendv_epi8(indices, cid, _mm256_castps_si256(better));
            cid = _mm256_add_epi32(cid, step);
        }
        let mut values = [0.0f32; 8];
        let mut ids = [0i32; 8];
        _mm256_storeu_ps(values.as_mut_ptr(), minima);
        _mm256_storeu_si256(ids.as_mut_ptr().cast(), indices);
        let mut best = (0usize, f32::INFINITY);
        for lane in 0..8 {
            let cid = ids[lane] as usize;
            if values[lane] < best.1 || (values[lane] == best.1 && cid < best.0) {
                best = (cid, values[lane]);
            }
        }
        for cid in len..dots.len() {
            let mut distance = norm + norms[cid] - 2.0 * dots[cid];
            if distance < 0.0 {
                distance = 0.0;
            }
            if distance < best.1 {
                best = (cid, distance);
            }
        }
        best
    }

    #[target_feature(enable = "avx2")]
    pub(super) unsafe fn nearest_ip(dots: &[f32]) -> usize {
        let mut minima = _mm256_set1_ps(f32::INFINITY);
        let mut indices = _mm256_setzero_si256();
        let mut cid = _mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7);
        let step = _mm256_set1_epi32(8);
        let sign = _mm256_set1_ps(-0.0);
        let len = dots.len() / 8 * 8;
        for offset in (0..len).step_by(8) {
            let distance =
                _mm256_xor_ps(_mm256_loadu_ps(dots.as_ptr().add(offset)), sign);
            let better = _mm256_cmp_ps::<_CMP_LT_OQ>(distance, minima);
            minima = _mm256_blendv_ps(minima, distance, better);
            indices = _mm256_blendv_epi8(indices, cid, _mm256_castps_si256(better));
            cid = _mm256_add_epi32(cid, step);
        }
        let mut values = [0.0f32; 8];
        let mut ids = [0i32; 8];
        _mm256_storeu_ps(values.as_mut_ptr(), minima);
        _mm256_storeu_si256(ids.as_mut_ptr().cast(), indices);
        let mut best = (0usize, f32::INFINITY);
        for lane in 0..8 {
            let cid = ids[lane] as usize;
            if values[lane] < best.1 || (values[lane] == best.1 && cid < best.0) {
                best = (cid, values[lane]);
            }
        }
        for (cid, &dot) in dots.iter().enumerate().skip(len) {
            if -dot < best.1 {
                best = (cid, -dot);
            }
        }
        best.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    #[test]
    fn reductions_match_scalar_including_ties_and_nonfinite_values() {
        let mut rng = StdRng::seed_from_u64(3921);
        for len in [0usize, 1, 7, 8, 15, 16, 17, 31, 127, 1024, 4099] {
            for _ in 0..100 {
                let dots: Vec<f32> =
                    (0..len).map(|_| rng.gen_range(-100.0..100.0)).collect();
                let norms: Vec<f32> =
                    (0..len).map(|_| rng.gen_range(0.0..100.0)).collect();
                let norm = rng.gen_range(0.0..100.0);
                let expected = scalar_l2(&dots, &norms, norm);
                let actual = nearest_l2(&dots, &norms, norm);
                assert_eq!(
                    (actual.0, actual.1.to_bits()),
                    (expected.0, expected.1.to_bits())
                );
                assert_eq!(nearest_ip(&dots), scalar_ip(&dots));
            }
            for value in [
                0.0f32,
                -0.0,
                1.0,
                -1.0,
                f32::NAN,
                f32::INFINITY,
                f32::NEG_INFINITY,
            ] {
                let dots = vec![value; len];
                let norms = vec![1.0; len];
                assert_eq!(
                    nearest_l2(&dots, &norms, 1.0),
                    scalar_l2(&dots, &norms, 1.0)
                );
                assert_eq!(nearest_ip(&dots), scalar_ip(&dots));
            }
        }
        // Equal minima in different SIMD lanes must select the earlier ID.
        let mut dots = vec![0.0; 49];
        dots[7] = 20.0;
        dots[32] = 20.0;
        assert_eq!(nearest_ip(&dots), 7);
        assert_eq!(nearest_l2(&dots, &[100.0; 49], 100.0).0, 7);
        dots[0] = f32::NAN;
        assert_eq!(nearest_ip(&dots), 7);
        let dots = vec![f32::NAN; 32];
        assert_eq!(nearest_l2(&dots, &[f32::NAN; 32], 0.0), (0, f32::INFINITY));
    }

    #[test]
    #[ignore = "release-only paired CPU benchmark"]
    fn bench_centroid_reductions() {
        use std::{hint::black_box, time::Instant};
        fn cpu() -> f64 {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
            assert_eq!(
                unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
                0
            );
            let usage = unsafe { usage.assume_init() };
            (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
                + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6
        }
        let mut rng = StdRng::seed_from_u64(42);
        for k in [64usize, 1024, 4096] {
            let rows = 4096;
            let dots: Vec<f32> =
                (0..rows * k).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let norms = vec![3.0; k];
            for metric in ["l2", "ip"] {
                let mut trials = [Vec::new(), Vec::new()];
                let mut cpu_trials = [Vec::new(), Vec::new()];
                for round in 0..7 {
                    for which in if round % 2 == 0 { [0, 1] } else { [1, 0] } {
                        let begin = Instant::now();
                        let start_cpu = cpu();
                        for _ in 0..4 {
                            for row in dots.chunks_exact(k) {
                                if metric == "l2" {
                                    black_box(if which == 0 {
                                        scalar_l2(
                                            black_box(row),
                                            black_box(&norms),
                                            black_box(3.0),
                                        )
                                    } else {
                                        nearest_l2(
                                            black_box(row),
                                            black_box(&norms),
                                            black_box(3.0),
                                        )
                                    });
                                } else {
                                    black_box(if which == 0 {
                                        scalar_ip(black_box(row))
                                    } else {
                                        nearest_ip(black_box(row))
                                    });
                                }
                            }
                        }
                        cpu_trials[which].push(cpu() - start_cpu);
                        trials[which].push(begin.elapsed().as_secs_f64());
                    }
                }
                for group in trials.iter_mut().chain(cpu_trials.iter_mut()) {
                    group.sort_by(f64::total_cmp);
                }
                println!(
                    "REDUCTION metric={metric} k={k} baseline_wall={:.6} candidate_wall={:.6} baseline_cpu={:.6} candidate_cpu={:.6}",
                    trials[0][3], trials[1][3], cpu_trials[0][3], cpu_trials[1][3]
                );
            }
        }
    }
}
