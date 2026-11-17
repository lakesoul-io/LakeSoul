// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

async fn test_catalog() -> (MetaDataClient, VectorCatalog, String) {
    let client = MetaDataClient::from_env().await.unwrap();
    let catalog = client.vector_index_catalog();
    catalog.ensure_tables().await.unwrap();
    let prefix = format!(
        "/tmp/index-read-lease-{}/_vector_index/vec/-5/0",
        Uuid::new_v4()
    );
    (client, catalog, prefix)
}

fn segment(name: &str) -> VectorSegmentEntry {
    VectorSegmentEntry {
        cluster_id: 0,
        segment_version: 0,
        filename: name.into(),
        num_vectors: 1,
        file_size: 16,
        data_files: vec!["data_00000.parquet".into()],
        birth_version: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_preserves_generation_and_survives_drop_for_worker() {
    let (_, catalog, prefix) = test_catalog().await;
    let first = catalog
        .commit(
            &prefix,
            b"header",
            &[segment("old.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    let pins = vec![(prefix.clone(), first.commit_id)];
    let parent = catalog
        .acquire_pinned_read_views(&pins, "coordinator")
        .await
        .unwrap();
    let next = catalog
        .commit(
            &prefix,
            b"header2",
            &[segment("new.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    assert_ne!(next.commit_id, first.commit_id);
    assert!(matches!(
        catalog.acquire_pinned_read_views(&pins, "stale").await,
        Err(LakeSoulMetaDataError::CommitConflict(_))
    ));
    let plan = catalog.gc_shard(&prefix, Duration::ZERO, 1).await.unwrap();
    assert!(
        plan.removed_generations.is_empty(),
        "a live reader pins the superseded generation"
    );
    assert!(plan.retained_filenames.contains(&"old.seg".to_string()));

    catalog.mark_dropped(&prefix, "test").await.unwrap();
    catalog.delete_shard(&prefix).await.unwrap();
    assert!(
        catalog.has_live_read_leases(&prefix).await.unwrap(),
        "shard deletion must not cascade read leases"
    );
    let pool = catalog.core.client.lock().await.primary_only();
    let row = pool
        .query_opt(
            "select data_files from index_read_lease where lease_id = $1",
            QueryType::RW,
            &[&parent.lease_id],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.get::<_, Vec<String>>(0),
        vec!["data_00000.parquet".to_string()]
    );
    assert!(matches!(
        catalog.acquire_pinned_read_views(&pins, "after drop").await,
        Err(LakeSoulMetaDataError::CommitConflict(_))
    ));
    // A distributed worker inherits an already-protected historical view,
    // even after the coordinator's shard was logically dropped.
    let worker = catalog
        .acquire_protected_read_views(&pins, parent.lease_id, "worker")
        .await
        .unwrap();
    let cleaned = Arc::new(AtomicBool::new(false));
    let did_clean = cleaned.clone();
    let cleanup_catalog = catalog.clone();
    let cleanup_prefix = prefix.clone();
    let cleanup = tokio::spawn(async move {
        cleanup_catalog
            .with_drop_cleanup(&cleanup_prefix, || async move {
                did_clean.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!cleaned.load(Ordering::SeqCst));
    parent.release().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !cleaned.load(Ordering::SeqCst),
        "the worker now owns the object lifetime"
    );
    worker.release().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), cleanup)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(cleaned.load(Ordering::SeqCst));
    catalog.clear_dropped(&prefix).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_acquisition_waits_for_drop_lock_and_rejects_tombstone() {
    let (_, catalog, prefix) = test_catalog().await;
    let view = catalog
        .commit(
            &prefix,
            b"header",
            &[segment("old.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    let client = catalog.core.client.lock().await.primary_only();
    let mut conn = client.get(QueryType::RW).await.unwrap();
    let tx = conn
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await
        .unwrap();
    lock_index_prefix(&tx, IndexKind::Vector, &prefix)
        .await
        .unwrap();
    tx.execute("insert into index_drop(kind,index_prefix,reason) values ('vector',$1,'racing drop')", &[&prefix]).await.unwrap();
    let waiting_catalog = catalog.clone();
    let waiting_prefix = prefix.clone();
    let mut acquire = tokio::spawn(async move {
        waiting_catalog
            .acquire_pinned_read_views(
                &[(waiting_prefix, view.commit_id)],
                "racing reader",
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut acquire)
            .await
            .is_err(),
        "acquisition must not slip past an uncommitted drop"
    );
    tx.commit().await.unwrap();
    assert!(matches!(
        acquire.await.unwrap(),
        Err(LakeSoulMetaDataError::CommitConflict(_))
    ));
    assert!(!catalog.has_live_read_leases(&prefix).await.unwrap());
    catalog.delete_shard(&prefix).await.unwrap();
    catalog.clear_dropped(&prefix).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_renews_cancels_on_pin_loss_and_releases_on_drop() {
    let (_, catalog, prefix) = test_catalog().await;
    let view = catalog
        .commit(
            &prefix,
            b"header",
            &[segment("old.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    let pins = [(prefix.clone(), view.commit_id)];
    let guard = catalog
        .acquire_pinned_read_views_with_timing(
            &pins,
            "renew-test",
            Duration::from_millis(400),
            Duration::from_millis(50),
            None,
            None,
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    guard.ensure_valid().unwrap();
    assert!(
        catalog.has_live_read_leases(&prefix).await.unwrap(),
        "renewal must outlive the original TTL"
    );
    let client = catalog.core.client.lock().await.primary_only();
    client
        .execute(
            "delete from index_read_lease where lease_id = $1",
            QueryType::RW,
            &[&guard.lease_id],
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(250), guard.cancelled())
        .await
        .unwrap();
    assert!(
        guard.ensure_valid().is_err(),
        "a failed heartbeat must cancel rather than silently repin"
    );
    drop(guard);

    let guard = catalog
        .acquire_pinned_read_views(&pins, "drop-test")
        .await
        .unwrap();
    let lease_id = guard.lease_id;
    drop(guard);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let row = client
                .query_opt(
                    "select 1 from index_read_lease where lease_id = $1",
                    QueryType::RW,
                    &[&lease_id],
                )
                .await
                .unwrap();
            if row.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    catalog.delete_shard(&prefix).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_expired_rows_cannot_be_renewed_or_inherited() {
    let (_, catalog, prefix) = test_catalog().await;
    let view = catalog
        .commit(
            &prefix,
            b"header",
            &[segment("old.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    let pins = [(prefix.clone(), view.commit_id)];
    let guard = catalog
        .acquire_pinned_read_views(&pins, "expired-test")
        .await
        .unwrap();
    let client = catalog.core.client.lock().await.primary_only();
    client.execute("update index_read_lease set expires_at = clock_timestamp() - interval '1 second' where lease_id = $1", QueryType::RW, &[&guard.lease_id]).await.unwrap();
    assert!(
        renew_read_lease(&client, guard.lease_id, 1, INDEX_READ_LEASE_TTL)
            .await
            .is_err()
    );
    assert!(
        catalog
            .acquire_protected_read_views(&pins, guard.lease_id, "late-worker")
            .await
            .is_err()
    );
    guard.release().await.unwrap();
    catalog.delete_shard(&prefix).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_gc_sweep_serializes_against_new_publication() {
    let (_, catalog, prefix) = test_catalog().await;
    catalog
        .commit(
            &prefix,
            b"first",
            &[segment("first.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    catalog
        .commit(
            &prefix,
            b"second",
            &[segment("second.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let gc_catalog = catalog.clone();
    let gc_prefix = prefix.clone();
    let sweep = tokio::spawn(async move {
        gc_catalog
            .with_gc_cleanup(&gc_prefix, Duration::ZERO, 1, |plan| async move {
                assert_eq!(plan.removed_generations, vec![1]);
                entered_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(())
            })
            .await
    });
    entered_rx.await.unwrap();
    // Separate metadata clients avoid a local client lock being mistaken
    // for the cross-node PostgreSQL serialization under test.
    let other = MetaDataClient::from_env()
        .await
        .unwrap()
        .vector_index_catalog();
    let other_prefix = prefix.clone();
    let mut publish = tokio::spawn(async move {
        other
            .commit(
                &other_prefix,
                b"third",
                &[segment("third.seg")],
                CommitMode::Rebuild,
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut publish)
            .await
            .is_err()
    );
    release_tx.send(()).unwrap();
    sweep.await.unwrap().unwrap();
    assert_eq!(publish.await.unwrap().unwrap().generation, 3);
    catalog.delete_shard(&prefix).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_multi_shard_acquisition_is_all_or_nothing() {
    let (_, catalog, prefix) = test_catalog().await;
    let second_prefix = format!("{prefix}-second");
    let first = catalog
        .commit(
            &prefix,
            b"header",
            &[segment("first.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    let second = catalog
        .commit(
            &second_prefix,
            b"header",
            &[segment("second.seg")],
            CommitMode::Rebuild,
        )
        .await
        .unwrap();
    let owner = format!("atomic-pins-{}", Uuid::new_v4());
    let result = catalog
        .acquire_pinned_read_views(
            &[
                (prefix.clone(), first.commit_id),
                (second_prefix.clone(), second.commit_id + 1000),
            ],
            &owner,
        )
        .await;
    assert!(matches!(
        result,
        Err(LakeSoulMetaDataError::CommitConflict(_))
    ));
    let pool = catalog.core.client.lock().await.primary_only();
    let row = pool
        .query_opt(
            "select count(*) from index_read_lease where owner = $1",
            QueryType::RW,
            &[&owner],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 0);
    catalog.delete_shard(&prefix).await.unwrap();
    catalog.delete_shard(&second_prefix).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_snapshot_anchor_and_retirement_fences() {
    use lakesoul_metadata_proto::entity::TableInfo;
    let (client, catalog, prefix) = test_catalog().await;
    let table = TableInfo {
        table_id: format!("anchor_{}", Uuid::new_v4()),
        table_name: format!("anchor_{}", Uuid::new_v4()),
        table_namespace: "default".into(),
        table_path: prefix.split("/_vector_index/").next().unwrap().to_string(),
        properties: "{}".into(),
        table_schema: "{}".into(),
        partitions: ";id".into(),
        domain: "public".into(),
        ..Default::default()
    };
    client.create_table(table.clone()).await.unwrap();
    let empty = client
        .vector_batch_metadata(&table.table_id)
        .await
        .unwrap()
        .unwrap();
    let empty_lease = catalog
        .acquire_snapshot_read_views(&empty, &[], "empty")
        .await
        .unwrap();
    let empty_worker = catalog
        .acquire_protected_read_views(&[], empty_lease.lease_id, "empty-worker")
        .await
        .unwrap();
    empty_lease.release().await.unwrap();
    empty_worker.release().await.unwrap();
    let pool = catalog.core.client.lock().await.primary_only();
    let shard_id = ensure_shard(&pool, IndexKind::Vector, &prefix)
        .await
        .unwrap();
    write_snapshot_revision(&client, &table.table_id, shard_id, 1).await;
    let first = client
        .vector_batch_metadata(&table.table_id)
        .await
        .unwrap()
        .unwrap();
    let pins = vec![(prefix.clone(), first.index_views[&prefix].commit_id)];
    let parent = catalog
        .acquire_pinned_read_views_with_timing(
            &pins,
            "anchored",
            Duration::from_millis(400),
            Duration::from_millis(50),
            None,
            Some(&first),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(650)).await;
    parent.ensure_valid().unwrap();
    let row = pool
        .query_opt(
            "select count(*) from index_read_lease where lease_id = $1",
            QueryType::RW,
            &[&parent.lease_id],
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row.get::<_, i64>(0),
        2,
        "index pin and full table anchor both renewed"
    );
    write_snapshot_revision(&client, &table.table_id, shard_id, 2).await;
    assert!(
        catalog
            .acquire_snapshot_read_views(&first, &pins, "stale-snapshot")
            .await
            .is_err()
    );
    let worker = catalog
        .acquire_protected_read_views(&pins, parent.lease_id, "worker-after-update")
        .await
        .unwrap();
    let row = pool.query_opt("select data_files from index_read_lease where lease_id = $1 and kind = 'snapshot'", QueryType::RW, &[&worker.lease_id]).await.unwrap().unwrap();
    assert_eq!(
        row.get::<_, Vec<String>>(0),
        vec![first.data_files[0].path.clone()]
    );
    parent.release().await.unwrap();
    worker.release().await.unwrap();
    let fresh = client
        .vector_batch_metadata(&table.table_id)
        .await
        .unwrap()
        .unwrap();
    let fresh_pins = vec![(prefix.clone(), fresh.index_views[&prefix].commit_id)];
    let file = fresh.data_files[0]
        .path
        .trim_start_matches("file://")
        .to_string();
    pool.execute("insert into index_drop(kind,index_prefix,reason) values('data_file',$1,'physical deletion pending')", QueryType::RW, &[&file]).await.unwrap();
    assert!(
        catalog
            .acquire_snapshot_read_views(&fresh, &fresh_pins, "retired-file")
            .await
            .is_err()
    );
    pool.execute(
        "delete from index_drop where kind = 'data_file' and index_prefix = $1",
        QueryType::RW,
        &[&file],
    )
    .await
    .unwrap();
    let directory = file.rsplit_once('/').unwrap().0.to_string();
    let sibling = format!("{directory}-sibling");
    pool.execute(
        "insert into index_drop(kind,index_prefix) values('data_directory',$1)",
        QueryType::RW,
        &[&sibling],
    )
    .await
    .unwrap();
    catalog
        .acquire_snapshot_read_views(&fresh, &fresh_pins, "boundary-ok")
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
    pool.execute(
        "insert into index_drop(kind,index_prefix) values('data_directory',$1)",
        QueryType::RW,
        &[&directory],
    )
    .await
    .unwrap();
    assert!(
        catalog
            .acquire_snapshot_read_views(&fresh, &fresh_pins, "retired-directory")
            .await
            .is_err()
    );
    pool.execute(
        "delete from index_drop where kind = 'data_directory' and index_prefix = any($1)",
        QueryType::RW,
        &[&vec![directory, sibling]],
    )
    .await
    .unwrap();
    client
        .delete_table_by_table_info_cascade(&table)
        .await
        .unwrap();
}

async fn write_snapshot_revision(
    client: &MetaDataClient,
    table_id: &str,
    shard_id: i64,
    revision: i32,
) {
    let pool = client.pooled_client().lock().await.primary_only();
    let mut conn = pool.get(QueryType::RW).await.unwrap();
    let tx = conn.transaction().await.unwrap();
    let properties =
        format!(r#"{{"revision":{revision},"vector_index_epoch":{revision}}}"#);
    tx.execute(
        "update table_info set properties = ($2::text)::json where table_id = $1",
        &[&table_id, &properties],
    )
    .await
    .unwrap();
    let commit = Uuid::new_v4();
    let path =
        format!("file:///tmp/coherent/{table_id}/revision{revision}_00000.parquet");
    let ops = vec![crate::DataFileOp {
        path: path.clone(),
        file_op: "add".into(),
        size: revision as i64,
        file_exist_cols: "id,vec".into(),
    }];
    tx.execute(
        "insert into data_commit_info(table_id,partition_desc,commit_id,file_ops,commit_op,timestamp,committed) \
         values($1,'-5',$2,$3,'AppendCommit',$4,true)",
        &[&table_id, &commit, &ops, &(revision as i64)],
    ).await.unwrap();
    tx.execute(
        "insert into partition_info(table_id,partition_desc,version,commit_op,snapshot,expression) \
         values($1,'-5',$2,'AppendCommit',$3,'')",
        &[&table_id, &revision, &vec![commit]],
    ).await.unwrap();
    let segments = serde_json::json!([{
        "cluster_id":0, "segment_version":0,
        "filename": format!("revision{revision}.seg"),
        "num_vectors":1, "file_size":16, "data_files":[path], "birth_version":0,
    }]);
    tx.execute(
        "insert into index_commit(shard_id,generation,version,header,segments) values($1,$2,1,$3,$4)",
        &[&shard_id, &(revision as i64), &revision.to_le_bytes().to_vec(), &segments],
    ).await.unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_lease_vector_batch_metadata_is_one_coherent_primary_snapshot() {
    use lakesoul_metadata_proto::entity::TableInfo;
    let (client, catalog, prefix) = test_catalog().await;
    let table_path = prefix.split("/_vector_index/").next().unwrap().to_string();
    let table = TableInfo {
        table_id: format!("read_snapshot_{}", Uuid::new_v4()),
        table_name: format!("read_snapshot_{}", Uuid::new_v4()),
        table_namespace: "default".into(),
        table_path,
        properties: "{}".into(),
        table_schema: "{}".into(),
        partitions: ";id".into(),
        domain: "public".into(),
        ..Default::default()
    };
    client.create_table(table.clone()).await.unwrap();
    let pool = catalog.core.client.lock().await.primary_only();
    let shard_id = ensure_shard(&pool, IndexKind::Vector, &prefix)
        .await
        .unwrap();
    write_snapshot_revision(&client, &table.table_id, shard_id, 1).await;
    let writer_client = client.clone();
    let table_id = table.table_id.clone();
    let writer = tokio::spawn(async move {
        for revision in 2..=12 {
            write_snapshot_revision(&writer_client, &table_id, shard_id, revision).await;
            tokio::task::yield_now().await;
        }
    });
    for _ in 0..24 {
        let snapshot = client
            .vector_batch_metadata(&table.table_id)
            .await
            .unwrap()
            .unwrap();
        let props: serde_json::Value =
            serde_json::from_str(&snapshot.table_info.properties).unwrap();
        let revision = props["revision"].as_i64().unwrap();
        assert_eq!(snapshot.partitions.len(), 1);
        assert_eq!(snapshot.partitions[0].version as i64, revision);
        assert_eq!(snapshot.data_files.len(), 1);
        assert_eq!(snapshot.data_files[0].size, revision);
        assert_eq!(snapshot.data_files[0].bucket_id(), 0);
        let view = &snapshot.index_views[&prefix];
        assert_eq!(view.generation as i64, revision);
        assert_eq!(
            view.segments[0].data_files,
            vec![snapshot.data_files[0].path.clone()]
        );
    }
    writer.await.unwrap();
    let latest = client
        .vector_batch_metadata(&table.table_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.partitions[0].version, 12);
    client
        .delete_table_by_table_info_cascade(&table)
        .await
        .unwrap();
    assert!(
        client
            .vector_batch_metadata(&table.table_id)
            .await
            .unwrap()
            .is_none()
    );
}
