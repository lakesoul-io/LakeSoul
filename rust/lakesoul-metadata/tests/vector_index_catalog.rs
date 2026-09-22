// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the PostgreSQL-backed vector index catalog.
//!
//! They require a running PostgreSQL with `LAKESOUL_PG_*` configured (the
//! same service `rust-ci` provides) and create their own control-plane
//! tables plus random shard prefixes.

use std::time::Duration;

use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::error::Result;
use lakesoul_metadata::index_catalog::{
    CommitMode, VectorCatalog, VectorSegmentEntry, normalize_index_prefix,
};

fn segment(
    cluster_id: u32,
    segment_version: u32,
    num_vectors: u32,
) -> VectorSegmentEntry {
    VectorSegmentEntry {
        cluster_id,
        segment_version,
        filename: format!(
            "cluster_{cluster_id:04}_{segment_version:04}_{:08x}.seg",
            rand_u32()
        ),
        num_vectors,
        file_size: 1024,
        data_files: Vec::new(),
    }
}

fn rand_u32() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos()
}

async fn catalog() -> Result<VectorCatalog> {
    static INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    let client = MetaDataClient::from_env().await?;
    let catalog = client.vector_index_catalog();
    INIT.get_or_try_init(|| catalog.init_tables()).await?;
    Ok(catalog)
}

fn unique_prefix(tag: &str) -> String {
    format!("test_veccat_{tag}_{}", uuid::Uuid::new_v4())
}

#[tokio::test]
async fn commit_resolve_roundtrip() -> Result<()> {
    let catalog = catalog().await?;
    let prefix = unique_prefix("roundtrip");

    let base = vec![segment(0, 0, 100), segment(1, 0, 50)];
    let view = catalog
        .commit(&prefix, b"header-1", &base, CommitMode::Rebuild)
        .await?;
    assert_eq!((view.generation, view.version), (1, 1));

    let resolved = catalog.resolve(&prefix).await?.expect("view");
    assert_eq!(resolved.commit_id, view.commit_id);
    assert_eq!(resolved.header, b"header-1");
    assert_eq!(resolved.segments, base);

    let delta = vec![segment(0, 1, 10)];
    let view2 = catalog
        .commit(&prefix, b"header-1", &delta, CommitMode::Delta)
        .await?;
    assert_eq!((view2.generation, view2.version), (1, 2));

    let resolved = catalog.resolve(&prefix).await?.expect("view");
    assert_eq!(resolved.segments.len(), 3);
    let mut got = resolved.segments.clone();
    let mut want = vec![base[0].clone(), base[1].clone(), delta[0].clone()];
    got.sort_by_key(|segment| (segment.cluster_id, segment.segment_version));
    want.sort_by_key(|segment| (segment.cluster_id, segment.segment_version));
    assert_eq!(got, want);

    let rebuild = vec![segment(0, 0, 200)];
    let view3 = catalog
        .commit(&prefix, b"header-2", &rebuild, CommitMode::Rebuild)
        .await?;
    assert_eq!((view3.generation, view3.version), (2, 1));

    let resolved = catalog.resolve(&prefix).await?.expect("view");
    assert_eq!(resolved.generation, 2);
    assert_eq!(resolved.header, b"header-2");
    assert_eq!(resolved.segments, rebuild);

    catalog.delete_shard(&prefix).await?;
    assert!(catalog.resolve(&prefix).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn cluster_stats_track_drift() -> Result<()> {
    let catalog = catalog().await?;
    let prefix = unique_prefix("stats");

    catalog
        .commit(
            &prefix,
            b"h",
            &[segment(0, 0, 100), segment(1, 0, 0)],
            CommitMode::Rebuild,
        )
        .await?;
    catalog
        .commit(&prefix, b"h", &[segment(0, 1, 250)], CommitMode::Delta)
        .await?;

    let stats = catalog.cluster_stats(&prefix).await?.expect("stats");
    let cluster0 = stats.iter().find(|s| s.cluster_id == 0).unwrap();
    assert_eq!(cluster0.base_vectors, 100);
    assert_eq!(cluster0.delta_vectors, 250);
    assert!(cluster0.delta_ratio() > 1.0);
    let cluster1 = stats.iter().find(|s| s.cluster_id == 1).unwrap();
    assert_eq!(cluster1.base_vectors, 0);
    assert_eq!(cluster1.delta_vectors, 0);
    assert_eq!(cluster1.delta_ratio(), 0.0);

    catalog.delete_shard(&prefix).await?;
    Ok(())
}

#[tokio::test]
async fn lease_blocks_gc_until_released() -> Result<()> {
    let catalog = catalog().await?;
    let prefix = unique_prefix("lease");

    let gen1 = vec![segment(0, 0, 100)];
    catalog
        .commit(&prefix, b"h", &gen1, CommitMode::Rebuild)
        .await?;

    let lease = catalog
        .acquire_lease(&prefix, Duration::from_secs(60), "test")
        .await?
        .expect("lease");
    assert_eq!(lease.generation, 1);

    // Supersede generation 1.
    let gen2 = vec![segment(0, 0, 120)];
    catalog
        .commit(&prefix, b"h", &gen2, CommitMode::Rebuild)
        .await?;

    // With the lease held the superseded generation survives gc.
    let plan = catalog.gc_shard(&prefix, Duration::ZERO, 1).await?;
    assert_eq!(plan.current_generation, Some(2));
    assert_eq!(plan.deleted_commit_rows, 0);
    assert!(plan.retained_filenames.contains(&gen1[0].filename));

    // Releasing the lease makes it collectable.
    lease.release().await?;
    let plan = catalog.gc_shard(&prefix, Duration::ZERO, 1).await?;
    assert_eq!(plan.deleted_commit_rows, 1);
    assert!(plan.removed_filenames.contains(&gen1[0].filename));
    assert!(!plan.retained_filenames.contains(&gen1[0].filename));
    assert!(plan.retained_filenames.contains(&gen2[0].filename));

    catalog.delete_shard(&prefix).await?;
    Ok(())
}

#[tokio::test]
async fn expired_lease_does_not_block_gc() -> Result<()> {
    let catalog = catalog().await?;
    let prefix = unique_prefix("lease_expiry");

    catalog
        .commit(&prefix, b"h", &[segment(0, 0, 10)], CommitMode::Rebuild)
        .await?;
    let _lease = catalog
        .acquire_lease(&prefix, Duration::from_millis(30), "test")
        .await?
        .expect("lease");
    catalog
        .commit(&prefix, b"h", &[segment(0, 0, 20)], CommitMode::Rebuild)
        .await?;

    tokio::time::sleep(Duration::from_millis(80)).await;
    let plan = catalog.gc_shard(&prefix, Duration::ZERO, 1).await?;
    assert_eq!(plan.deleted_commit_rows, 1);

    catalog.delete_shard(&prefix).await?;
    Ok(())
}

#[tokio::test]
async fn gc_respects_grace_and_keep_generations() -> Result<()> {
    let catalog = catalog().await?;
    let prefix = unique_prefix("gc_policy");

    for _ in 0..3 {
        catalog
            .commit(&prefix, b"h", &[segment(0, 0, 10)], CommitMode::Rebuild)
            .await?;
    }
    // Current generation is 3; a grace period protects the just superseded
    // generations.
    let plan = catalog
        .gc_shard(&prefix, Duration::from_secs(3600), 1)
        .await?;
    assert_eq!(plan.deleted_commit_rows, 0);

    // Without grace, keeping 2 generations leaves generation 1 collectable
    // but keeps generation 2.
    let plan = catalog.gc_shard(&prefix, Duration::ZERO, 2).await?;
    assert_eq!(plan.deleted_commit_rows, 1);
    assert_eq!(plan.current_generation, Some(3));

    // Keeping only the current generation collects generation 2 as well.
    let plan = catalog.gc_shard(&prefix, Duration::ZERO, 1).await?;
    assert_eq!(plan.deleted_commit_rows, 1);

    catalog.delete_shard(&prefix).await?;
    Ok(())
}

#[tokio::test]
async fn delete_under_prefix_scopes_to_directory() -> Result<()> {
    let catalog = catalog().await?;
    let base = unique_prefix("drop");
    let nested = format!("{base}/_vector_index/vec/-5/0");
    let sibling = format!("{base}_sibling/_vector_index/vec/-5/0");

    catalog
        .commit(&nested, b"h", &[segment(0, 0, 1)], CommitMode::Rebuild)
        .await?;
    catalog
        .commit(&sibling, b"h", &[segment(0, 0, 1)], CommitMode::Rebuild)
        .await?;

    let deleted = catalog.delete_under_prefix(&base).await?;
    assert_eq!(deleted, 1);
    assert!(catalog.resolve(&nested).await?.is_none());
    assert!(catalog.resolve(&sibling).await?.is_some());

    catalog.delete_shard(&sibling).await?;
    Ok(())
}

#[test]
fn normalize_paths() {
    assert_eq!(normalize_index_prefix("s3://bucket/db/tbl"), "db/tbl");
    assert_eq!(normalize_index_prefix("file:///data/tbl/"), "/data/tbl");
}

#[tokio::test]
async fn drop_table_cleans_index_rows() -> Result<()> {
    use lakesoul_metadata_proto::entity::TableInfo;

    let client = MetaDataClient::from_env().await?;
    let catalog = client.vector_index_catalog();
    let name = format!("veccat_drop_{}", uuid::Uuid::new_v4().simple());
    let path = format!("file:///tmp/{name}");
    client
        .create_table(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: name.clone(),
            table_namespace: "default".to_string(),
            table_path: path.clone(),
            table_schema: "[]".to_string(),
            table_schema_arrow_ipc: Vec::new(),
            table_schema_arrow_ipc_json_hash: String::new(),
            properties: "{}".to_string(),
            partitions: String::new(),
            domain: "public".to_string(),
        })
        .await?;

    let prefix = format!(
        "{}/_vector_index/vec/-5/0",
        path.trim_start_matches("file://").trim_end_matches('/')
    );
    catalog
        .commit(&prefix, b"h", &[segment(0, 0, 1)], CommitMode::Rebuild)
        .await?;
    assert!(catalog.resolve(&prefix).await?.is_some());

    client.drop_table(&name, "default").await?;
    assert!(
        catalog.resolve(&prefix).await?.is_none(),
        "dropping the table must clean its index rows"
    );
    Ok(())
}
