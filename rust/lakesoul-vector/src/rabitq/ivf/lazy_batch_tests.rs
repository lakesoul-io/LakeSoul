use super::*;
use crate::rabitq::Metric;
use crate::rabitq::ivf::batch::tests::fixture;
use crate::rabitq::key::IndexKey;
use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::time::Duration;

async fn persist(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    index: &IvfRabitqIndex,
) -> (IndexHeader, Vec<SegmentEntry>) {
    index
        .write_base_segments(&IndexStore::new(store, prefix.to_string()))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_fused_batch_matches_eager_for_both_metrics_and_bit_widths() {
    for metric in [Metric::L2, Metric::InnerProduct] {
        for bits in [1, 3, 7] {
            let (index, mut queries) = fixture(metric, bits, 64);
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let prefix = "lazy-fused-equality";
            let (header, mut segments) =
                persist(Arc::clone(&store), prefix, &index).await;
            // A newer physical record survives an older mutation; old records
            // are excluded without cloning or repacking the shared payload.
            segments[0].birth_version = 5;
            let mutation = MutationState::new(
                4,
                1,
                vec![
                    (IndexKey::new(0u64.to_le_bytes()), 4),
                    (IndexKey::new(17u64.to_le_bytes()), 4),
                ],
            );
            let eager = IvfRabitqIndex::load_from_segments_with_mutations(
                &IndexStore::new(Arc::clone(&store), prefix.to_string()),
                &header,
                &segments,
                Some(&mutation),
            )
            .await
            .unwrap();
            let lazy = LazyIvfIndex::open(
                Arc::clone(&store),
                prefix,
                1,
                &header,
                &segments,
                Some(mutation),
            )
            .await
            .unwrap();
            queries.extend(vec![queries[0].clone(); 10]);
            let refs: Vec<_> = queries.iter().map(Vec::as_slice).collect();
            for nprobe in [1, 3, 100] {
                let params = SearchParams::new(15, nprobe);
                let output = lazy.search_batch_with_options(
                    &refs,
                    params,
                    &BatchSearchOptions {
                        query_tile_size: 16,
                        parallelism: 2,
                        cancellation: None,
                    },
                );
                assert_eq!(output.results.len(), refs.len());
                assert!(output.diagnostics.shared_block_reuses > 0);
                assert!(output.diagnostics.workers <= 2);
                for (query, result) in refs.iter().zip(output.results) {
                    assert_eq!(result.unwrap(), eager.search(query, params).unwrap());
                }
            }
            invalidate_prefix(prefix);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonaligned_delta_merge_matches_eager_versions_and_mutations() {
    use crate::rabitq::segment::{segment_filename, write_segment};
    let (index, queries) = fixture(Metric::L2, 7, 64);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "lazy-nonaligned-delta";
    let istore = IndexStore::new(Arc::clone(&store), prefix.to_string());
    let (header, mut segments) = persist(Arc::clone(&store), prefix, &index).await;
    let base_entries = segments.clone();
    for entry in &base_entries {
        let mut delta = read_segment_full(&istore, &entry.segment_filename)
            .await
            .unwrap();
        assert_eq!(delta.ids.len() % 32, 3);
        delta.version = 1;
        delta.birth_version = 9;
        let filename = segment_filename(entry.cluster_id, 1);
        let size = write_segment(&istore, &filename, &delta, 1).await.unwrap();
        let mut delta_entry = entry.clone();
        delta_entry.segment_filename = filename;
        delta_entry.segment_version = 1;
        delta_entry.birth_version = 9;
        delta_entry.file_size = size;
        segments.push(delta_entry);
    }
    let mutations = MutationState::new(
        8,
        1,
        (0u64..268)
            .map(|id| (IndexKey::new(id.to_le_bytes()), 8))
            .collect(),
    );
    let eager = IvfRabitqIndex::load_from_segments_with_mutations(
        &istore,
        &header,
        &segments,
        Some(&mutations),
    )
    .await
    .unwrap();
    let lazy = LazyIvfIndex::open(store, prefix, 2, &header, &segments, Some(mutations))
        .await
        .unwrap();
    let refs: Vec<_> = queries.iter().map(Vec::as_slice).collect();
    let actual = lazy.search_batch(&refs, 12, 4, 2);
    for (query, actual) in refs.iter().zip(actual) {
        let actual = actual.unwrap();
        assert_eq!(
            actual,
            eager.search(query, SearchParams::new(12, 4)).unwrap()
        );
        assert!(
            actual
                .iter()
                .all(|hit| hit.version == 1 && hit.birth_version == 9)
        );
    }
    invalidate_prefix(prefix);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_lazy_misses_share_one_payload_and_one_load() {
    let (index, _) = fixture(Metric::L2, 7, 64);
    let throttled = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let store: Arc<dyn ObjectStore> = throttled.clone();
    let prefix = "lazy-single-flight";
    let (header, segments) = persist(Arc::clone(&store), prefix, &index).await;
    let lazy = LazyIvfIndex::open(store, prefix, 1, &header, &segments, None)
        .await
        .unwrap();
    throttled.config_mut(|config| config.wait_get_per_call = Duration::from_millis(40));
    let loaded = futures::future::join_all((0..8).map(|_| async {
        let mut diagnostics = BatchSearchDiagnostics::default();
        let cluster = lazy.load_cluster(0, None, &mut diagnostics).await.unwrap();
        (cluster, diagnostics)
    }))
    .await;
    assert_eq!(
        loaded
            .iter()
            .map(|(_, diag)| diag.lazy_cluster_loads)
            .sum::<usize>(),
        1
    );
    assert_eq!(
        loaded
            .iter()
            .map(|(_, diag)| diag.singleflight_waits)
            .sum::<usize>(),
        7
    );
    for (payload, _) in &loaded {
        assert!(Arc::ptr_eq(payload, &loaded[0].0));
        assert_eq!(
            payload.data.batch_data.as_ptr(),
            loaded[0].0.data.batch_data.as_ptr()
        );
    }
    invalidate_prefix(prefix);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_cache_identity_includes_store_and_commit() {
    let (first_index, queries) = fixture(Metric::L2, 3, 64);
    let mut second_index = first_index.clone();
    for cluster in &mut second_index.clusters {
        for key in &mut cluster.ids {
            let bytes: [u8; 8] = key.as_bytes().try_into().unwrap();
            *key = IndexKey::new((u64::from_le_bytes(bytes) + 10000).to_le_bytes());
        }
    }
    let first_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let second_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "identical-lazy-prefix";
    let (header, segments) =
        persist(Arc::clone(&first_store), prefix, &first_index).await;
    let (second_header, second_segments) =
        persist(Arc::clone(&second_store), prefix, &second_index).await;
    let first = LazyIvfIndex::open(first_store, prefix, 1, &header, &segments, None)
        .await
        .unwrap();
    let second = LazyIvfIndex::open(
        second_store,
        prefix,
        1,
        &second_header,
        &second_segments,
        None,
    )
    .await
    .unwrap();
    let params = SearchParams::new(10, 4);
    let first_result = first.search(&queries[0], 10, 4, 1).unwrap();
    let second_result = second.search(&queries[0], 10, 4, 1).unwrap();
    assert_eq!(
        first_result,
        first_index.search(&queries[0], params).unwrap()
    );
    assert_eq!(
        second_result,
        second_index.search(&queries[0], params).unwrap()
    );
    assert_ne!(first_result, second_result);
    assert_ne!(first.cluster_key(0), second.cluster_key(0));
    let mut another_commit = first.cluster_key(0);
    another_commit.commit_id += 1;
    assert_ne!(first.cluster_key(0), another_commit);
    invalidate_prefix(prefix);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_of_a_waiter_does_not_cancel_shared_load() {
    let (index, _) = fixture(Metric::L2, 7, 64);
    let throttled = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let store: Arc<dyn ObjectStore> = throttled.clone();
    let prefix = "lazy-waiter-cancel";
    let (header, segments) = persist(Arc::clone(&store), prefix, &index).await;
    let lazy = LazyIvfIndex::open(store, prefix, 1, &header, &segments, None)
        .await
        .unwrap();
    throttled.config_mut(|config| config.wait_get_per_call = Duration::from_millis(100));
    let cancel = Arc::new(AtomicBool::new(false));
    let first = async {
        let mut diagnostics = BatchSearchDiagnostics::default();
        let result = lazy.load_cluster(0, None, &mut diagnostics).await;
        (result.unwrap(), diagnostics)
    };
    let waiter = async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let mut diagnostics = BatchSearchDiagnostics::default();
        lazy.load_cluster(0, Some(&cancel), &mut diagnostics).await
    };
    let trigger = async {
        tokio::time::sleep(Duration::from_millis(15)).await;
        cancel.store(true, Ordering::Relaxed);
    };
    let (first, waiter, ()) = tokio::join!(first, waiter, trigger);
    assert!(matches!(waiter, Err(RabitqError::InvalidConfig(_))));
    assert_eq!(first.1.lazy_cluster_loads, 1);
    let mut diagnostics = BatchSearchDiagnostics::default();
    let cached = lazy.load_cluster(0, None, &mut diagnostics).await.unwrap();
    assert!(Arc::ptr_eq(&first.0, &cached));
    assert_eq!(diagnostics.cluster_cache_hits, 1);
    invalidate_prefix(prefix);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_loader_can_be_retried_without_poisoning_singleflight() {
    let (index, queries) = fixture(Metric::L2, 3, 64);
    let throttled = Arc::new(ThrottledStore::new(
        InMemory::new(),
        ThrottleConfig::default(),
    ));
    let store: Arc<dyn ObjectStore> = throttled.clone();
    let prefix = "lazy-owner-cancel";
    let (header, segments) = persist(Arc::clone(&store), prefix, &index).await;
    let lazy = LazyIvfIndex::open(store, prefix, 1, &header, &segments, None)
        .await
        .unwrap();
    throttled.config_mut(|config| config.wait_get_per_call = Duration::from_millis(100));
    let cancel = Arc::new(AtomicBool::new(false));
    let load = async {
        let mut diagnostics = BatchSearchDiagnostics::default();
        lazy.load_cluster(0, Some(&cancel), &mut diagnostics).await
    };
    let trigger = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel.store(true, Ordering::Relaxed);
    };
    let (result, ()) = tokio::join!(load, trigger);
    assert!(matches!(result, Err(RabitqError::InvalidConfig(_))));
    throttled.config_mut(|config| config.wait_get_per_call = Duration::ZERO);
    let refs = [queries[0].as_slice(), queries[1].as_slice()];
    let actual =
        lazy.search_batch_controlled(&refs, 10, 4, 2, Arc::new(AtomicBool::new(false)));
    for (query, actual) in refs.iter().zip(actual) {
        assert_eq!(
            actual.unwrap(),
            index.search(query, SearchParams::new(10, 4)).unwrap()
        );
    }
    invalidate_prefix(prefix);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_lazy_segment_returns_error_in_each_query_slot() {
    use object_store::ObjectStoreExt;
    let (index, queries) = fixture(Metric::L2, 3, 64);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "lazy-missing-segment";
    let (header, segments) = persist(Arc::clone(&store), prefix, &index).await;
    let lazy =
        LazyIvfIndex::open(Arc::clone(&store), prefix, 1, &header, &segments, None)
            .await
            .unwrap();
    let path = IndexStore::new(Arc::clone(&store), prefix.to_string())
        .full_path(&segments[0].segment_filename);
    let original_bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    store.delete(&path).await.unwrap();
    let refs = [queries[0].as_slice(); 8];
    let results = lazy.search_batch(&refs, 10, 4, 2);
    assert_eq!(results.len(), refs.len());
    assert!(results.iter().all(Result::is_err));
    // A failed OnceCell initializer is not cached as a successful empty result.
    store.put(&path, original_bytes.into()).await.unwrap();
    assert_eq!(header.dim, lazy.dim());
    assert_eq!(segments.len(), lazy.cluster_count());
    lazy.search(&queries[0], 10, 4, 1).unwrap();
    invalidate_prefix(prefix);
}
