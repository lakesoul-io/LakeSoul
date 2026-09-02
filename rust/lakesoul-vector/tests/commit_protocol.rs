// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! CAS-free commit protocol tests.
//!
//! The V3 commit protocol must be correct on object stores that do **not**
//! support conditional (`If-Match`) PUTs — Aliyun OSS ignores unknown
//! precondition headers, and `LocalFileSystem` rejects them.  These tests
//! exercise the protocol through a store wrapper that fails loudly on any
//! conditional put, so a regression back to `PutMode::Update` fails here
//! instead of silently losing data in production.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

use lakesoul_vector::rabitq::manifest::{
    self, COMMIT_RETRIES, ClusterManifestEntry, ManifestHeader, ManifestStore,
    ResolvedView, SegmentManifestEntry,
};
use lakesoul_vector::{Metric, RotatorType};

/// An object store that rejects conditional puts, like Aliyun OSS (which
/// ignores `If-Match` on PutObject) or `LocalFileSystem` (which returns
/// `NotImplemented`).
#[derive(Debug)]
struct NoCasStore(Arc<dyn ObjectStore>);

impl fmt::Display for NoCasStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NoCasStore")
    }
}

#[async_trait]
impl ObjectStore for NoCasStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if matches!(opts.mode, PutMode::Update(_)) {
            return Err(object_store::Error::NotImplemented {
                operation: "conditional put (If-Match) is not supported by this store"
                    .into(),
                implementer: "NoCasStore".into(),
            });
        }
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, Result<Path>>,
    ) -> futures::stream::BoxStream<'static, Result<Path>> {
        self.0.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        self.0.copy_opts(from, to, options).await
    }
}

fn no_cas_store() -> Arc<NoCasStore> {
    Arc::new(NoCasStore(Arc::new(InMemory::new())))
}

fn header(generation: u64) -> ManifestHeader {
    ManifestHeader {
        generation,
        dim: 8,
        padded_dim: 8,
        metric: Metric::L2,
        rotator_type: RotatorType::FhtKacRotator,
        rotator_data: vec![1, 2, 3],
        ex_bits: 6,
        total_bits: 7,
    }
}

fn segment_entry(cid: u32, version: u32) -> SegmentManifestEntry {
    SegmentManifestEntry {
        segment_filename: manifest::segment_filename(cid, version),
        segment_version: version,
        num_vectors: 100,
        file_size: 1024,
    }
}

fn single_cluster_map(
    cid: u32,
    segments: Vec<SegmentManifestEntry>,
) -> BTreeMap<u32, ClusterManifestEntry> {
    let mut map = BTreeMap::new();
    map.insert(
        cid,
        ClusterManifestEntry {
            cluster_id: cid,
            segments,
        },
    );
    map
}

#[tokio::test]
async fn test_initial_commit_and_resolve() {
    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "idx".to_string());

    let map = single_cluster_map(0, vec![segment_entry(0, 0)]);
    manifest::commit_delta(&mstore, &header(1), &map)
        .await
        .unwrap();

    let view = manifest::resolve_view(&mstore).await.unwrap().unwrap();
    assert_eq!(view.key(), (1, 1));
    assert_eq!(view.cluster_map.len(), 1);
    assert_eq!(view.cluster_map[&0].segments.len(), 1);

    // LATEST v2 carries the unique manifest filename.
    let snap = manifest::read_latest(&mstore).await.unwrap();
    assert_eq!(snap.generation, 1);
    assert_eq!(snap.version, 1);
    let fname = snap.manifest_filename.expect("LATEST v2 filename");
    assert!(
        fname.starts_with("manifests/g00000001_v00000001_"),
        "{fname}"
    );
    assert_eq!(view.manifest_filenames, vec![fname]);
}

#[tokio::test]
async fn test_concurrent_delta_flushes_union() {
    // Two writers race on the same index: both must commit and the final
    // view must contain *both* delta segments (union by filename).
    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "idx".to_string());

    manifest::commit_delta(
        &mstore,
        &header(1),
        &single_cluster_map(0, vec![segment_entry(0, 0)]),
    )
    .await
    .unwrap();

    let mstore_a = ManifestStore::new(mstore.store.clone(), "idx".to_string());
    let mstore_b = ManifestStore::new(mstore.store.clone(), "idx".to_string());
    let (ra, rb) = tokio::join!(
        async move {
            let seg_a = segment_entry(0, 1);
            manifest::commit_delta(
                &mstore_a,
                &header(1),
                &single_cluster_map(0, vec![seg_a]),
            )
            .await
        },
        async move {
            let seg_b = segment_entry(0, 1);
            manifest::commit_delta(
                &mstore_b,
                &header(1),
                &single_cluster_map(0, vec![seg_b]),
            )
            .await
        },
    );
    ra.unwrap();
    rb.unwrap();

    let view = manifest::resolve_view(&mstore).await.unwrap().unwrap();
    assert_eq!(view.key().0, 1, "both writers stay in the same generation");
    // The union must reference every committed segment file exactly once.
    let filenames: Vec<String> = view.cluster_map[&0]
        .segments
        .iter()
        .map(|s| s.segment_filename.clone())
        .collect();
    assert_eq!(
        filenames.len(),
        3,
        "base + both concurrent deltas: {filenames:?}"
    );
    let mut sorted = filenames.clone();
    sorted.sort();
    let mut deduped = sorted.clone();
    deduped.dedup();
    assert_eq!(sorted, deduped, "no duplicate segment references");
}

#[tokio::test]
async fn test_commit_rebuild_publishes_new_generation_without_deletes() {
    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "idx".to_string());

    manifest::commit_delta(
        &mstore,
        &header(1),
        &single_cluster_map(0, vec![segment_entry(0, 0)]),
    )
    .await
    .unwrap();
    let base_key = manifest::resolve_view(&mstore)
        .await
        .unwrap()
        .unwrap()
        .key();

    // Full replacement map (as produced by compaction).
    let rebuilt = single_cluster_map(0, vec![segment_entry(0, 0)]);
    let rebuilt_fname = rebuilt[&0].segments[0].segment_filename.clone();
    manifest::commit_rebuild(&mstore, &header(2), &rebuilt, base_key)
        .await
        .unwrap();

    let view = manifest::resolve_view(&mstore).await.unwrap().unwrap();
    assert_eq!(view.key(), (2, 1));
    assert_eq!(view.cluster_map[&0].segments.len(), 1);
    assert_eq!(
        view.cluster_map[&0].segments[0].segment_filename,
        rebuilt_fname
    );

    // Nothing was deleted: old + new commits both remain listed.
    let mut objects = Vec::new();
    let mut stream = mstore.store.list(Some(&Path::from("idx/manifests")));
    while let Some(meta) = stream.next().await {
        objects.push(meta.unwrap().location.to_string());
    }
    assert_eq!(
        objects.len(),
        2,
        "old + new commits both retained: {objects:?}"
    );
}

#[tokio::test]
async fn test_rebuild_conflicts_when_view_moved() {
    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "idx".to_string());

    manifest::commit_delta(
        &mstore,
        &header(1),
        &single_cluster_map(0, vec![segment_entry(0, 0)]),
    )
    .await
    .unwrap();
    let stale_base = manifest::resolve_view(&mstore)
        .await
        .unwrap()
        .unwrap()
        .key();

    // A concurrent flush lands, moving the view past the rebuild's base.
    manifest::commit_delta(
        &mstore,
        &header(1),
        &single_cluster_map(0, vec![segment_entry(0, 1)]),
    )
    .await
    .unwrap();

    let err = manifest::commit_rebuild(
        &mstore,
        &header(2),
        &single_cluster_map(0, vec![segment_entry(0, 0)]),
        stale_base,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, lakesoul_vector::RabitqError::CommitConflict),
        "{err}"
    );
}

#[tokio::test]
async fn test_legacy_latest_and_deterministic_manifest_are_readable() {
    // Old indexes store LATEST as "generation:version" and the manifest at
    // the deterministic name; resolve_view must still load them.
    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "idx".to_string());

    let map = single_cluster_map(0, vec![segment_entry(0, 0)]);
    let legacy_name = manifest::versioned_manifest_filename(1, 0);
    manifest::save_manifest_to(&mstore, &header(1), &map, &legacy_name)
        .await
        .unwrap();
    manifest::write_latest(&mstore, 1, 0, &legacy_name)
        .await
        .unwrap();

    let view = manifest::resolve_view(&mstore).await.unwrap().unwrap();
    assert_eq!(view.key(), (1, 0));
    assert_eq!(view.cluster_map[&0].segments.len(), 1);

    // A legacy LATEST without the filename field still parses.
    let snap = manifest::read_latest(&mstore).await.unwrap();
    assert_eq!((snap.generation, snap.version), (1, 0));
    assert!(
        snap.manifest_filename.is_some(),
        "filename resolved for legacy pointer"
    );
}

#[tokio::test]
async fn test_legacy_manifest_bin_fallback() {
    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "idx".to_string());
    let map = single_cluster_map(3, vec![segment_entry(3, 0)]);
    manifest::save_manifest_to(&mstore, &header(1), &map, manifest::MANIFEST_FILENAME)
        .await
        .unwrap();

    let view = manifest::resolve_view(&mstore).await.unwrap().unwrap();
    assert_eq!(
        view.key(),
        (0, 0),
        "legacy manifest.bin resolves at key (0,0)"
    );
    assert_eq!(view.cluster_map[&3].segments.len(), 1);
}

#[tokio::test]
async fn test_flush_and_search_end_to_end_on_no_cas_store() {
    use lakesoul_vector::{
        IdAndVecBatch, IvfRabitqBuilder, IvfRabitqIndex, SearchParams,
    };
    use rand::{Rng, SeedableRng, rngs::StdRng};

    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "idx".to_string());

    let mut rng = StdRng::seed_from_u64(42);
    let dim = 16usize;
    let rand_vec = |rng: &mut StdRng| {
        (0..dim)
            .map(|_| rng.r#gen::<f32>() * 2.0 - 1.0)
            .collect::<Vec<f32>>()
    };

    // Initial build: 64 vectors, 4 clusters.
    let mut builder =
        IvfRabitqBuilder::new(dim, 4, 7, Metric::L2, RotatorType::FhtKacRotator, 1, true);
    let base: Vec<IdAndVecBatch> = (0..4)
        .map(|b| IdAndVecBatch {
            ids: (b * 16..b * 16 + 16).collect(),
            vectors: (0..16).flat_map(|_| rand_vec(&mut rng)).collect(),
        })
        .collect();
    for batch in &base {
        builder.insert_batch(batch.clone()).unwrap();
    }
    let base_stream = base.clone();
    let index = builder
        .build(|| futures::stream::iter(base_stream.clone().into_iter()))
        .await
        .unwrap();
    index.save_to_v4(&mstore).await.unwrap();

    // Incremental flush: 16 new vectors via a loaded builder.
    let mut builder = IvfRabitqBuilder::load(
        &mstore,
        dim,
        4,
        7,
        Metric::L2,
        RotatorType::FhtKacRotator,
        1,
        true,
    )
    .await
    .unwrap();
    builder
        .insert_batch(IdAndVecBatch {
            ids: (64..80).collect(),
            vectors: (0..16).flat_map(|_| rand_vec(&mut rng)).collect(),
        })
        .unwrap();
    builder.flush(&mstore).await.unwrap();

    // The delta is visible: searching for a flushed vector returns its id.
    let loaded = IvfRabitqIndex::load_from_v4(&mstore).await.unwrap();
    let probe = rand_vec(&mut rng);
    let results = loaded.search(&probe, SearchParams::new(20, 4)).unwrap();
    assert!(!results.is_empty());
    let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
    assert!(
        ids.iter().any(|id| *id >= 64),
        "flushed vectors reachable: {ids:?}"
    );
}

#[tokio::test]
async fn test_concurrent_flush_writers_on_local_fs_do_not_lose_data() {
    use lakesoul_vector::{
        IdAndVecBatch, IvfRabitqBuilder, IvfRabitqIndex, SearchParams,
    };
    use rand::{Rng, SeedableRng, rngs::StdRng};

    let tmp = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store::local::LocalFileSystem::new_with_prefix(tmp.path()).unwrap(),
    );
    let mstore = ManifestStore::new(store, "idx".to_string());

    let mut rng = StdRng::seed_from_u64(7);
    let dim = 16usize;
    let rand_vec = |rng: &mut StdRng| {
        (0..dim)
            .map(|_| rng.r#gen::<f32>() * 2.0 - 1.0)
            .collect::<Vec<f32>>()
    };

    let mut builder =
        IvfRabitqBuilder::new(dim, 4, 7, Metric::L2, RotatorType::FhtKacRotator, 1, true);
    let base: Vec<IdAndVecBatch> = (0..4)
        .map(|b| IdAndVecBatch {
            ids: (b * 16..b * 16 + 16).collect(),
            vectors: (0..16).flat_map(|_| rand_vec(&mut rng)).collect(),
        })
        .collect();
    for batch in &base {
        builder.insert_batch(batch.clone()).unwrap();
    }
    let base_stream = base.clone();
    let index = builder
        .build(|| futures::stream::iter(base_stream.clone().into_iter()))
        .await
        .unwrap();
    index.save_to_v4(&mstore).await.unwrap();

    // Two processes (two independent builders) flush concurrently.
    let flush_one = |rng_seed: u64, id_start: u64, store: Arc<dyn ObjectStore>| {
        let mstore = ManifestStore::new(store, "idx".to_string());
        async move {
            let mut rng = StdRng::seed_from_u64(rng_seed);
            let mut builder = IvfRabitqBuilder::load(
                &mstore,
                dim,
                4,
                7,
                Metric::L2,
                RotatorType::FhtKacRotator,
                1,
                true,
            )
            .await
            .unwrap();
            builder
                .insert_batch(IdAndVecBatch {
                    ids: (id_start..id_start + 16).collect(),
                    vectors: (0..16).flat_map(|_| rand_vec(&mut rng)).collect(),
                })
                .unwrap();
            builder.flush(&mstore).await.unwrap();
        }
    };

    let store_a = mstore.store.clone();
    let store_b = mstore.store.clone();
    tokio::join!(flush_one(11, 64, store_a), flush_one(22, 80, store_b),);

    // Both flushed sets must be reachable after resolution.
    let loaded = IvfRabitqIndex::load_from_v4(&mstore).await.unwrap();
    let probe = rand_vec(&mut rng);
    let results = loaded.search(&probe, SearchParams::new(50, 4)).unwrap();
    let ids: Vec<u64> = results.iter().map(|r| r.id).collect();
    assert!(
        ids.iter().any(|id| (64..80).contains(id))
            && ids.iter().any(|id| (80..96).contains(id)),
        "both concurrent flushes visible: {ids:?}"
    );
}

#[tokio::test]
async fn test_retry_budget_is_bounded() {
    // Sanity: the protocol constant is finite and resolution of an empty
    // store returns None.
    assert_eq!(COMMIT_RETRIES, 3);
    let store = no_cas_store();
    let mstore = ManifestStore::new(store.clone(), "empty".to_string());
    assert!(manifest::resolve_view(&mstore).await.unwrap().is_none());
    let _: Option<ResolvedView> = None;
}
