// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Catalog integration tests for durable vector commit/delete publication.
//!
//! Requires a running PostgreSQL configured through `LAKESOUL_PG_URL` (the
//! same service the rust CI provides).

use std::sync::Arc;

use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::error::Result;
use lakesoul_metadata::index_catalog::{CommitMode, VectorSegmentEntry};
use lakesoul_vector::IndexKey;
use lakesoul_vector::rabitq::mutation::load_state;
use lakesoul_vector::rabitq::segment::IndexStore;
use lakesoul_vector::vector_commit::{
    MAX_MUTATION_RECORDS, commit_vector_build, delete_vector_keys,
};
use object_store::memory::InMemory;

fn key(byte: u8) -> IndexKey {
    IndexKey::new([byte])
}

fn segment(name: &str) -> VectorSegmentEntry {
    VectorSegmentEntry {
        cluster_id: 0,
        segment_version: 0,
        filename: name.to_string(),
        num_vectors: 1,
        file_size: 16,
        data_files: Vec::new(),
        birth_version: 0,
    }
}

fn unique_prefix(tag: &str) -> String {
    format!("test_veccommit_{tag}_{}", uuid::Uuid::new_v4().simple())
}

async fn catalog() -> Result<lakesoul_metadata::index_catalog::VectorCatalog> {
    static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    let client = MetaDataClient::from_env().await?;
    let catalog = client.vector_index_catalog();
    INIT.get_or_try_init(|| catalog.init_tables()).await?;
    Ok(catalog)
}

#[tokio::test]
async fn commit_delete_and_checkpoint_rollover() -> Result<()> {
    let catalog = catalog().await?;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let prefix = unique_prefix("lifecycle");
    let istore = IndexStore::new(store.clone(), prefix.clone());

    // Rebuild: no mutation chain, birth version 1.
    let rebuilt = commit_vector_build(
        &store,
        &catalog,
        &prefix,
        b"header",
        vec![segment("cluster_0000_0000_base.seg")],
        vec![key(1)],
        CommitMode::Rebuild,
    )
    .await
    .expect("rebuild commit")
    .expect("published");
    assert_eq!((rebuilt.generation, rebuilt.version), (1, 1));
    assert_eq!(rebuilt.mutation_head, None);
    assert_eq!(rebuilt.mutation_records, 0);

    let view = catalog.resolve(&prefix).await?.expect("view");
    assert_eq!(view.segments[0].birth_version, 1);

    // Delta upsert: the new segment is stamped with the commit revision and
    // the key is recorded in the mutation sidecar.
    let upserted = commit_vector_build(
        &store,
        &catalog,
        &prefix,
        b"header",
        vec![segment("cluster_0000_0001_delta.seg")],
        vec![key(1)],
        CommitMode::Delta,
    )
    .await
    .expect("delta commit")
    .expect("published");
    assert_eq!((upserted.generation, upserted.version), (1, 2));
    assert_eq!(upserted.mutation_records, 1);
    let head = upserted.mutation_head.clone().expect("mutation head");
    assert!(head.ends_with(".delta"), "first record is a delta: {head}");

    let view = catalog.resolve(&prefix).await?.expect("view");
    let delta = view
        .segments
        .iter()
        .find(|segment| segment.filename == "cluster_0000_0001_delta.seg")
        .expect("delta segment");
    assert_eq!(delta.birth_version, 2);

    let state = load_state(&istore, Some(&head), view.mutation_records, view.version)
        .await
        .expect("load mutations");
    assert!(!state.is_valid(&key(1), 1), "old copy must be masked");
    assert!(state.is_valid(&key(1), 2), "upserted copy is visible");

    // Delete-only commit: no new segments, a second mutation record.
    let deleted = delete_vector_keys(&store, &catalog, &prefix, &[key(2)])
        .await
        .expect("delete commit")
        .expect("published");
    assert_eq!((deleted.generation, deleted.version), (1, 3));
    assert_eq!(deleted.mutation_records, 2);
    let view = catalog.resolve(&prefix).await?.expect("view");
    assert_eq!(view.segments.len(), 2, "delete must not add segments");
    let head = view.mutation_head.clone().expect("mutation head");
    let state = load_state(&istore, Some(&head), view.mutation_records, view.version)
        .await
        .expect("load mutations");
    assert!(!state.is_valid(&key(2), 1));
    assert!(state.is_valid(&key(2), 3));

    // Once the delta chain reaches the limit, the next publication merges
    // it into a checkpoint (one object again).
    let mut records = view.mutation_records;
    while records < MAX_MUTATION_RECORDS {
        let outcome = delete_vector_keys(&store, &catalog, &prefix, &[key(3)])
            .await
            .expect("delete commit")
            .expect("published");
        records = outcome.mutation_records;
    }
    let merged = delete_vector_keys(&store, &catalog, &prefix, &[key(3)])
        .await
        .expect("checkpoint commit")
        .expect("published");
    assert_eq!(merged.mutation_records, 1, "chain is merged");
    let head = merged.mutation_head.clone().expect("mutation head");
    assert!(
        head.ends_with(".checkpoint"),
        "merged record is a checkpoint: {head}"
    );
    let view = catalog.resolve(&prefix).await?.expect("view");
    let state = load_state(&istore, Some(&head), view.mutation_records, view.version)
        .await
        .expect("load checkpoint");
    assert!(!state.is_valid(&key(1), 1));
    assert!(state.is_valid(&key(1), 2));
    assert!(!state.is_valid(&key(3), view.version - 1));
    assert!(state.is_valid(&key(3), view.version));

    // A rebuild starts a new generation and drops the mutation chain.
    let rebuilt = commit_vector_build(
        &store,
        &catalog,
        &prefix,
        b"header2",
        vec![segment("cluster_0000_0000_rebuilt.seg")],
        Vec::new(),
        CommitMode::Rebuild,
    )
    .await
    .expect("rebuild commit")
    .expect("published");
    assert_eq!((rebuilt.generation, rebuilt.version), (2, 1));
    assert_eq!(rebuilt.mutation_head, None);
    assert_eq!(rebuilt.mutation_records, 0);

    catalog.delete_shard(&prefix).await?;
    Ok(())
}
