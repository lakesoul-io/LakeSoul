use super::*;
use crate::rabitq::key::IndexKey;
use crate::rabitq::rotation::RotatorType;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub(in crate::rabitq::ivf) fn fixture(
    metric: Metric,
    bits: usize,
    dim: usize,
) -> (IvfRabitqIndex, Vec<Vec<f32>>) {
    let mut rng = StdRng::seed_from_u64(9817);
    let centroids: Vec<Vec<f32>> = (0..4)
        .map(|cluster| {
            (0..dim)
                .map(|d| if d % 4 == cluster { 2.0 } else { -0.5 })
                .collect()
        })
        .collect();
    // 67 vectors exercise full blocks, padded tails, and cluster transitions.
    let assignments: Vec<_> = (0..268).map(|id| id % 4).collect();
    let data: Vec<Vec<f32>> = assignments
        .iter()
        .map(|&cid| {
            centroids[cid]
                .iter()
                .map(|center| center + rng.gen_range(-0.6..0.6))
                .collect()
        })
        .collect();
    let index = IvfRabitqIndex::train_with_clusters(
        &data,
        &centroids,
        &assignments,
        bits,
        metric,
        RotatorType::FhtKacRotator,
        17,
        true,
    )
    .unwrap();
    let queries = data.into_iter().take(35).collect();
    (index, queries)
}

#[test]
fn block_major_batch_preserves_single_query_results_and_order() {
    for metric in [Metric::L2, Metric::InnerProduct] {
        for bits in [1, 3, 7] {
            let (mut index, queries) = fixture(metric, bits, 64);
            // Keep packed and pending records together, and exercise mutation masks.
            index.remove_keys(&[
                IndexKey::new(0u64.to_le_bytes()),
                IndexKey::new(17u64.to_le_bytes()),
            ]);
            index.insert(1001, &queries[0]).unwrap();
            let refs: Vec<_> = queries.iter().map(Vec::as_slice).collect();
            for nprobe in [0, 1, 3, 100] {
                for top_k in [0, 1, 11, 400] {
                    let params = SearchParams::new(top_k, nprobe);
                    let expected: Vec<_> = refs
                        .iter()
                        .map(|query| index.search(query, params).unwrap())
                        .collect();
                    for tile_size in [1, 7, 32] {
                        let output = index.batch_search_with_options(
                            &refs,
                            params,
                            &BatchSearchOptions {
                                query_tile_size: tile_size,
                                parallelism: 2,
                                cancellation: None,
                            },
                        );
                        assert_eq!(output.results.len(), refs.len());
                        for (qid, (actual, expected)) in
                            output.results.into_iter().zip(&expected).enumerate()
                        {
                            assert_eq!(
                                &actual.unwrap(),
                                expected,
                                "qid={qid} metric={metric:?} bits={bits} nprobe={nprobe} top_k={top_k} tile={tile_size}"
                            );
                        }
                        assert!(output.diagnostics.workers <= 2);
                        assert!(output.diagnostics.peak_tile_queries <= tile_size);
                        if top_k != 0 {
                            assert_eq!(
                                output.diagnostics.query_cluster_visits,
                                refs.len() * nprobe.max(1).min(4)
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn repeated_queries_reuse_each_block_without_changing_results() {
    let (index, queries) = fixture(Metric::L2, 7, 64);
    let refs = vec![queries[0].as_slice(); 16];
    let params = SearchParams::new(10, 3);
    let output = index.batch_search_with_options(
        &refs,
        params,
        &BatchSearchOptions {
            query_tile_size: 32,
            parallelism: 1,
            cancellation: None,
        },
    );
    let expected = index.search(&queries[0], params).unwrap();
    for result in output.results {
        assert_eq!(result.unwrap(), expected);
    }
    assert_eq!(output.diagnostics.cluster_visits, 3);
    assert_eq!(output.diagnostics.query_cluster_visits, 3 * refs.len());
    assert!(output.diagnostics.block_reads > 0);
    assert_eq!(
        output.diagnostics.query_block_evaluations,
        output.diagnostics.block_reads * refs.len()
    );
    assert_eq!(
        output.diagnostics.shared_block_reuses,
        output.diagnostics.block_reads * (refs.len() - 1)
    );
}

#[test]
fn highacc_batch_matches_single_query() {
    let (index, queries) = fixture(Metric::InnerProduct, 7, 1200);
    let refs: Vec<_> = queries.iter().take(5).map(Vec::as_slice).collect();
    let params = SearchParams::new(11, 3);
    let actual = index.batch_search_controlled(
        &refs,
        params.with_parallelism(2),
        Arc::new(AtomicBool::new(false)),
    );
    for (query, actual) in refs.iter().zip(actual) {
        assert_eq!(actual.unwrap(), index.search(query, params).unwrap());
    }
}

#[test]
fn batch_validation_empty_and_cancelled_queries_keep_input_slots() {
    let (index, queries) = fixture(Metric::L2, 3, 64);
    let bad = vec![1.0; 63];
    let refs = [queries[0].as_slice(), bad.as_slice(), queries[1].as_slice()];
    let params = SearchParams::new(10, 2);
    let actual = index.batch_search(&refs, params);
    assert_eq!(actual.len(), 3);
    assert_eq!(
        actual[0].as_ref().unwrap(),
        &index.search(refs[0], params).unwrap()
    );
    assert!(matches!(
        actual[1],
        Err(RabitqError::DimensionMismatch { .. })
    ));
    assert_eq!(
        actual[2].as_ref().unwrap(),
        &index.search(refs[2], params).unwrap()
    );
    assert!(index.batch_search(&[], params).is_empty());

    let output = index.batch_search_with_options(
        &refs,
        params,
        &BatchSearchOptions {
            cancellation: Some(Arc::new(AtomicBool::new(true))),
            ..Default::default()
        },
    );
    assert!(output.results.iter().all(Result::is_err));
    assert_eq!(output.diagnostics.cancelled_queries, refs.len());
    assert_eq!(output.diagnostics.block_reads, 0);

    let empty = IvfRabitqIndex::from_centroids(
        &index.index_header(),
        vec![vec![0.0; index.padded_dim]],
    )
    .unwrap();
    assert!(matches!(
        empty.batch_search(&refs[..1], params)[0],
        Err(RabitqError::EmptyIndex)
    ));
}

#[test]
fn shared_job_pool_preserves_order_and_request_worker_cap() {
    use std::sync::atomic::AtomicUsize;
    let active = AtomicUsize::new(0);
    let peak = AtomicUsize::new(0);
    let results = map_batch_jobs((0..17).collect(), 2, |job| {
        let count = active.fetch_add(1, Ordering::SeqCst) + 1;
        peak.fetch_max(count, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(1));
        active.fetch_sub(1, Ordering::SeqCst);
        job * 3
    });
    assert_eq!(results, (0..17).map(|value| value * 3).collect::<Vec<_>>());
    assert!(peak.load(Ordering::SeqCst) <= 2);
    assert!(peak.load(Ordering::SeqCst) <= default_batch_parallelism());
    let empty: Vec<i32> = map_batch_jobs(Vec::<i32>::new(), 20, |value| value);
    assert!(empty.is_empty());
}

#[test]
fn cancellation_between_cluster_load_and_first_block_stops_scan() {
    struct CancelAfterLoad<'a> {
        index: &'a IvfRabitqIndex,
        cancel: Arc<AtomicBool>,
    }
    impl BatchClusterSource for CancelAfterLoad<'_> {
        fn selector(&self) -> &IvfRabitqIndex {
            self.index
        }
        fn has_vectors(&self) -> bool {
            true
        }
        fn cluster(
            &self,
            position: usize,
            _cancel: Option<&AtomicBool>,
            diagnostics: &mut BatchSearchDiagnostics,
        ) -> Result<ClusterView<'_>, RabitqError> {
            self.cancel.store(true, Ordering::Relaxed);
            self.index.cluster(position, None, diagnostics)
        }
    }
    let (index, queries) = fixture(Metric::L2, 7, 64);
    let cancel = Arc::new(AtomicBool::new(false));
    let source = CancelAfterLoad {
        index: &index,
        cancel: Arc::clone(&cancel),
    };
    let refs = vec![queries[0].as_slice(); 8];
    let output = search_source_batch(
        &source,
        &refs,
        SearchParams::new(10, 4),
        &BatchSearchOptions {
            cancellation: Some(cancel),
            parallelism: 1,
            ..Default::default()
        },
    );
    assert!(output.results.iter().all(Result::is_err));
    assert_eq!(output.diagnostics.cancelled_queries, refs.len());
    assert_eq!(output.diagnostics.cluster_visits, 1);
    assert_eq!(output.diagnostics.block_reads, 0);
}
