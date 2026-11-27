// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for [`MetaDataClient::commit_transaction`].
//!
//! They require a running PostgreSQL configured through `LAKESOUL_PG_*` (the
//! same service `rust-ci` provides) and use unique table names, like
//! `commit_occ.rs`.

use lakesoul_metadata::error::Result;
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClient, TransactionTableCommit};
use lakesoul_metadata_proto::entity::{
    CommitOp, DataCommitInfo, DataFileOp, FileOp, PartitionInfo, TableInfo, Uuid,
};

const NAMESPACE: &str = "default";

fn unique_name(tag: &str) -> String {
    format!("txn_{tag}_{}", uuid::Uuid::new_v4().simple())
}

async fn create_test_table(client: &MetaDataClient, name: &str) -> Result<String> {
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.to_string(),
            table_namespace: NAMESPACE.to_string(),
            table_path: format!("file:///tmp/{name}"),
            table_schema: "[]".to_string(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            domain: "public".to_string(),
            ..Default::default()
        })
        .await?;
    Ok(table_id)
}

async fn table_info(client: &MetaDataClient, table_id: &str) -> Result<TableInfo> {
    client
        .get_table_info_by_table_id(table_id)
        .await?
        .ok_or_else(|| {
            LakeSoulMetaDataError::NotFound(format!("table {table_id} not found"))
        })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64
}

fn uuid(seed: u64) -> Uuid {
    Uuid {
        high: seed,
        low: seed.wrapping_mul(0x9E37_79B9_7F4A_7C15),
    }
}

/// One staged commit, as the write path hands it to `COMMIT`.
fn staged(table_id: &str, partition_desc: &str, seed: u64) -> DataCommitInfo {
    DataCommitInfo {
        table_id: table_id.to_string(),
        partition_desc: partition_desc.to_string(),
        commit_id: Some(uuid(seed)),
        file_ops: vec![DataFileOp {
            path: format!("/tmp/{partition_desc}/f{seed}.parquet"),
            file_op: FileOp::Add as i32,
            size: 1,
            file_exist_cols: "id".to_string(),
        }],
        commit_op: CommitOp::AppendCommit as i32,
        timestamp: now_ms(),
        committed: true,
        domain: "public".to_string(),
        ..Default::default()
    }
}

/// The latest published version of one partition, `None` when it has none.
async fn latest(
    client: &MetaDataClient,
    table_id: &str,
    partition_desc: &str,
) -> Result<Option<PartitionInfo>> {
    Ok(client
        .get_partition_info_by_table_id_and_partition_list(
            table_id,
            &[partition_desc.to_string()],
        )
        .await?
        .into_iter()
        .next())
}

/// The partition versions a statement would have captured before writing.
async fn expected(
    client: &MetaDataClient,
    table_id: &str,
    partitions: &[&str],
) -> Result<Vec<PartitionInfo>> {
    let mut versions = Vec::new();
    for partition_desc in partitions {
        if let Some(info) = latest(client, table_id, partition_desc).await? {
            versions.push(info);
        }
    }
    Ok(versions)
}

#[tokio::test]
async fn publishes_every_table_in_one_transaction() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let first_name = unique_name("multi_a");
    let second_name = unique_name("multi_b");
    let first = create_test_table(&client, &first_name).await?;
    let second = create_test_table(&client, &second_name).await?;

    let commit_ids = client
        .commit_transaction(vec![
            TransactionTableCommit {
                table_info: table_info(&client, &first).await?,
                data_commits: vec![staged(&first, "p1", 1)],
                expected: Some(expected(&client, &first, &["p1"]).await?),
            },
            TransactionTableCommit {
                table_info: table_info(&client, &second).await?,
                data_commits: vec![staged(&second, "q1", 2)],
                expected: Some(expected(&client, &second, &["q1"]).await?),
            },
        ])
        .await?;
    assert_eq!(commit_ids.len(), 2, "one commit id per published partition");
    // Tables publish in `table_id` order (the lock order), whatever order the
    // caller listed them in.
    let mut table_order = [(&first, 1), (&second, 2)];
    table_order.sort_by(|left, right| left.0.cmp(right.0));
    assert_eq!(
        commit_ids,
        table_order
            .iter()
            .map(|(_, seed)| uuid_string(*seed))
            .collect::<Vec<_>>()
    );

    for (table_id, partition_desc, seed) in [(&first, "p1", 1), (&second, "q1", 2)] {
        let partition = latest(&client, table_id, partition_desc)
            .await?
            .expect("the partition exists");
        assert_eq!(partition.version, 0, "the first version");
        assert_eq!(partition.snapshot, vec![uuid(seed)]);
        let commit = client
            .get_single_data_commit_info(table_id, partition_desc, &uuid_string(seed))
            .await?
            .expect("the data commit row exists");
        assert!(commit.committed, "the data commit is published");
    }

    client.drop_table(&first_name, NAMESPACE).await?;
    client.drop_table(&second_name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn a_stale_partition_on_one_table_publishes_nothing() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let names = [unique_name("stale_a"), unique_name("stale_b")];
    let mut ids = [
        create_test_table(&client, &names[0]).await?,
        create_test_table(&client, &names[1]).await?,
    ];
    ids.sort();
    // The commit publishes tables in `table_id` order: make the *last* one the
    // stale one, so the first table's rows are really written before the
    // rollback has to undo them.
    let (first, second) = (ids[0].clone(), ids[1].clone());

    // The block read `second` at version 0.
    let second_info = table_info(&client, &second).await?;
    client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: second_info.clone(),
            data_commits: vec![staged(&second, "q1", 10)],
            expected: Some(Vec::new()),
        }])
        .await?;
    let stale = expected(&client, &second, &["q1"]).await?;
    assert_eq!(stale.len(), 1, "the block captured version 0");
    // A peer publishes again: the block's version is now behind.
    client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: second_info,
            data_commits: vec![staged(&second, "q1", 11)],
            expected: Some(expected(&client, &second, &["q1"]).await?),
        }])
        .await?;

    let error = client
        .commit_transaction(vec![
            TransactionTableCommit {
                table_info: table_info(&client, &first).await?,
                data_commits: vec![staged(&first, "p1", 12)],
                expected: Some(expected(&client, &first, &["p1"]).await?),
            },
            TransactionTableCommit {
                table_info: table_info(&client, &second).await?,
                data_commits: vec![staged(&second, "q1", 13)],
                expected: Some(stale),
            },
        ])
        .await
        .expect_err("the second table's partition moved");
    assert!(
        matches!(&error, LakeSoulMetaDataError::InsertConflict { .. }),
        "a lost version race is an InsertConflict, got {error:?}"
    );

    // Nothing of the failed commit is visible: the first table never got a
    // version, and the second one is untouched.
    assert!(
        latest(&client, &first, "p1").await?.is_none(),
        "the first table's partition must not be published"
    );
    assert!(
        client
            .get_single_data_commit_info(&first, "p1", &uuid_string(12))
            .await?
            .is_none(),
        "the first table's data rows are rolled back"
    );
    let second_partition = latest(&client, &second, "q1").await?.expect("exists");
    assert_eq!(second_partition.version, 1);
    assert_eq!(second_partition.snapshot, vec![uuid(10), uuid(11)]);
    assert!(
        client
            .get_single_data_commit_info(&second, "q1", &uuid_string(13))
            .await?
            .is_none(),
        "the failed commit's data rows are gone too"
    );

    client.drop_table(&names[0], NAMESPACE).await?;
    client.drop_table(&names[1], NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn an_append_write_conflicts_with_nothing() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("append");
    let table_id = create_test_table(&client, &name).await?;
    let info = table_info(&client, &table_id).await?;

    // A stale snapshot stays safe for append-only writes: their files are
    // disjoint, so the publication only extends the version.
    client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: info.clone(),
            data_commits: vec![staged(&table_id, "p1", 20)],
            expected: None,
        }])
        .await?;
    // Captured before the second write, so the block's view is version 0.
    let stale = expected(&client, &table_id, &["p1"]).await?;
    assert_eq!(stale.len(), 1);
    client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: info.clone(),
            data_commits: vec![staged(&table_id, "p1", 21)],
            expected: Some(stale.clone()),
        }])
        .await?;
    let partition = latest(&client, &table_id, "p1").await?.expect("exists");
    assert_eq!(partition.version, 1);
    assert_eq!(partition.snapshot, vec![uuid(20), uuid(21)]);

    // `None` skips the comparison entirely, so a snapshot that is now stale
    // (the one captured above) still cannot fail.
    for seed in [22, 23] {
        client
            .commit_transaction(vec![TransactionTableCommit {
                table_info: info.clone(),
                data_commits: vec![staged(&table_id, "p1", seed)],
                expected: None,
            }])
            .await?;
    }
    let partition = latest(&client, &table_id, "p1").await?.expect("exists");
    assert_eq!(partition.version, 3);
    assert_eq!(
        partition.snapshot,
        vec![uuid(20), uuid(21), uuid(22), uuid(23)]
    );

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn a_redefined_table_is_refused() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("redefined");
    let table_id = create_test_table(&client, &name).await?;
    let mut stale_info = table_info(&client, &table_id).await?;
    // The definition the statement was analysed against is gone by commit time.
    stale_info.table_schema = "[\"changed\"]".to_string();

    let error = client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: stale_info,
            data_commits: vec![staged(&table_id, "p1", 30)],
            expected: Some(Vec::new()),
        }])
        .await
        .expect_err("a redefined table cannot receive the files");
    assert!(
        matches!(&error, LakeSoulMetaDataError::CommitConflict(_)),
        "identity drift is a CommitConflict, got {error:?}"
    );
    assert!(
        latest(&client, &table_id, "p1").await?.is_none(),
        "nothing was published"
    );

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn statements_of_one_partition_share_a_version_in_statement_order() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("same_partition");
    let table_id = create_test_table(&client, &name).await?;

    // Two statements staged in one block, one partition, in statement order.
    let commit_ids = client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: table_info(&client, &table_id).await?,
            data_commits: vec![staged(&table_id, "p1", 40), staged(&table_id, "p1", 41)],
            expected: Some(Vec::new()),
        }])
        .await?;
    assert_eq!(commit_ids, vec![uuid_string(40), uuid_string(41)]);

    let partition = latest(&client, &table_id, "p1").await?.expect("exists");
    assert_eq!(partition.version, 0, "one version for the whole block");
    assert_eq!(
        partition.snapshot,
        vec![uuid(40), uuid(41)],
        "merge-on-read reads the snapshot as version order"
    );
    for seed in [40, 41] {
        let commit = client
            .get_single_data_commit_info(&table_id, "p1", &uuid_string(seed))
            .await?
            .expect("each statement keeps its own data commit");
        assert!(commit.committed);
    }

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn a_table_may_appear_only_once() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("twice");
    let table_id = create_test_table(&client, &name).await?;
    let info = table_info(&client, &table_id).await?;

    let error = client
        .commit_transaction(vec![
            TransactionTableCommit {
                table_info: info.clone(),
                data_commits: vec![staged(&table_id, "p1", 50)],
                expected: Some(Vec::new()),
            },
            TransactionTableCommit {
                table_info: info,
                data_commits: vec![staged(&table_id, "p2", 51)],
                expected: Some(Vec::new()),
            },
        ])
        .await
        .expect_err("one table, one entry");
    assert!(
        matches!(&error, LakeSoulMetaDataError::Internal(_)),
        "the front end must merge a table's staged writes, got {error:?}"
    );

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

fn uuid_string(seed: u64) -> String {
    uuid::Uuid::from_u64_pair(uuid(seed).high, uuid(seed).low).to_string()
}

#[tokio::test]
async fn compaction_rejects_mismatched_read_snapshot_and_intervening_compaction()
-> Result<()> {
    use lakesoul_metadata::transfusion::DataFileInfo;
    let client = MetaDataClient::from_env().await?.primary_only().await;
    let name = unique_name("compact_snapshot");
    let id = create_test_table(&client, &name).await?;
    let table = table_info(&client, &id).await?;
    let part = "p1";
    client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: table.clone(),
            data_commits: vec![staged(&id, part, 1)],
            expected: Some(vec![]),
        }])
        .await?;
    let read = latest(&client, &id, part).await?.unwrap();
    let files = vec![DataFileInfo {
        path: format!("{}/compactdir1/new_0000.parquet", table.table_path),
        partition_desc: part.into(),
        file_op: "add".into(),
        size: 10,
        ..Default::default()
    }];
    let mut wrong = read.clone();
    wrong.snapshot.clear();
    let stale_id = uuid::Uuid::new_v4();
    assert!(
        !client
            .publish_compacted_snapshot(&table, &wrong, stale_id, &files, &[])
            .await?
    );
    let publication = uuid::Uuid::new_v4();
    assert!(
        client
            .publish_compacted_snapshot(&table, &read, publication, &files, &[])
            .await?
    );
    assert!(client.compaction_is_published(&read, publication).await?);
    let head = latest(&client, &id, part).await?.unwrap();
    assert_eq!(head.version, read.version + 1);
    assert_eq!(head.commit_op, CommitOp::CompactionCommit as i32);
    let (high, low) = publication.as_u64_pair();
    assert_eq!(head.snapshot, vec![Uuid { high, low }]);
    assert!(
        !client
            .publish_compacted_snapshot(&table, &read, stale_id, &files, &[])
            .await?
    );
    assert!(!client.compaction_is_published(&read, stale_id).await?);
    Ok(())
}

#[tokio::test]
async fn compaction_preserves_repeated_retained_adds_and_their_order() -> Result<()> {
    let client = MetaDataClient::from_env().await?.primary_only().await;
    let name = unique_name("compact_repeated_add");
    let id = create_test_table(&client, &name).await?;
    let table = table_info(&client, &id).await?;
    let part = "p1";
    let mut input = staged(&id, part, 1);
    let mut retained = input.file_ops[0].clone();
    retained.path = format!("{}/retained_0000.parquet", table.table_path);
    let mut replaced = retained.clone();
    replaced.path = format!("{}/old_0001.parquet", table.table_path);
    input.file_ops = vec![retained.clone(), replaced.clone(), retained.clone()];
    client
        .commit_transaction(vec![TransactionTableCommit {
            table_info: table.clone(),
            data_commits: vec![input],
            expected: Some(vec![]),
        }])
        .await?;
    let read = latest(&client, &id, part).await?.unwrap();
    let mut files = client
        .get_data_file_infos_of_single_partition(&read)
        .await?;
    assert_eq!(files.len(), 3);
    let old = files[1].clone();
    assert_eq!(old.path, replaced.path);
    files[1].path = format!("{}/compactdir1/new_0001.parquet", table.table_path);
    let publication = uuid::Uuid::new_v4();
    assert!(matches!(
        client
            .publish_compacted_snapshot(&table, &read, publication, &files, &[&files[0]])
            .await,
        Err(LakeSoulMetaDataError::Internal(_))
    ));
    assert!(
        client
            .publish_compacted_snapshot(&table, &read, publication, &files, &[&old])
            .await?
    );
    let commit = client
        .get_single_data_commit_info(&id, part, &publication.to_string())
        .await?
        .unwrap();
    replaced.path = files[1].path.clone();
    assert_eq!(commit.file_ops, vec![retained.clone(), replaced, retained]);
    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn compaction_merges_intervening_versions_in_head_order() -> Result<()> {
    use CommitOp::{AppendCommit, DeleteCommit, MergeCommit, UpdateCommit};
    use lakesoul_metadata::transfusion::DataFileInfo;

    let client = MetaDataClient::from_env().await?.primary_only().await;
    let name = unique_name("compact_history");
    let id = create_test_table(&client, &name).await?;
    let table = table_info(&client, &id).await?;
    let cases: &[(&str, &[CommitOp], bool)] = &[
        ("append_merge", &[AppendCommit, MergeCommit], true),
        ("delete_only", &[DeleteCommit], true),
        (
            "delete_append",
            &[AppendCommit, DeleteCommit, AppendCommit],
            true,
        ),
        ("update_append", &[UpdateCommit, AppendCommit], false),
    ];
    for &(part, changes, should_publish) in cases {
        let file = |name: &str| DataFileInfo {
            partition_desc: part.into(),
            path: format!("{}/{part}/{name}_0000.parquet", table.table_path),
            file_op: "add".into(),
            size: 10,
            file_exist_cols: "id".into(),
            ..Default::default()
        };
        let input = file("original");
        client
            .commit_data_files_with_commit_op(
                &name,
                NAMESPACE,
                vec![input.clone()],
                AppendCommit,
            )
            .await?;
        let read = latest(&client, &id, part).await?.unwrap();
        let publication = uuid::Uuid::new_v4();
        let (high, low) = publication.as_u64_pair();
        let mut expected = vec![Uuid { high, low }];
        for (index, &op) in changes.iter().enumerate() {
            let head = latest(&client, &id, part).await?.unwrap();
            let added = file(&format!("write_{index}"));
            match op {
                AppendCommit | MergeCommit => {
                    let ids = client
                        .commit_data_files_with_commit_op(
                            &name,
                            NAMESPACE,
                            vec![added],
                            op,
                        )
                        .await?;
                    let (high, low) = uuid::Uuid::parse_str(&ids[0])?.as_u64_pair();
                    expected.push(Uuid { high, low });
                }
                DeleteCommit => {
                    client
                        .commit_delete(
                            &table,
                            std::slice::from_ref(&head),
                            vec![head.clone()],
                        )
                        .await?;
                    expected.truncate(1);
                }
                UpdateCommit => {
                    client
                        .commit_update_parts(
                            &name,
                            NAMESPACE,
                            vec![added],
                            vec![head],
                            vec![],
                        )
                        .await?;
                }
                _ => unreachable!(),
            }
        }
        let before = latest(&client, &id, part).await?.unwrap();
        let output = file("compact");
        let published = client
            .publish_compacted_snapshot(
                &table,
                &read,
                publication,
                std::slice::from_ref(&output),
                &[&input],
            )
            .await?;
        assert_eq!(published, should_publish, "{part}");
        let after = latest(&client, &id, part).await?.unwrap();
        let commit = client
            .get_single_data_commit_info(&id, part, &publication.to_string())
            .await?;
        if should_publish {
            assert_eq!(after.version, before.version + 1, "{part}");
            assert_eq!(after.snapshot, expected, "{part}");
            let commit = commit.unwrap();
            assert!(commit.committed);
            assert_eq!(commit.file_ops.len(), 1);
            assert_eq!(commit.file_ops[0].path, output.path);
        } else {
            assert_eq!(after, before);
            assert!(commit.is_none());
        }
    }
    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn compaction_registers_only_unpinned_discard_paths() -> Result<()> {
    use lakesoul_metadata::{DaoType, transfusion::DataFileInfo};
    let client = MetaDataClient::from_env().await?.primary_only().await;
    let name = unique_name("compact_pinned_discard");
    let id = create_test_table(&client, &name).await?;
    let table = table_info(&client, &id).await?;
    let file = |base: &str| DataFileInfo {
        partition_desc: "p1".into(),
        file_op: "add".into(),
        path: format!("{}/{base}_0000.parquet", table.table_path),
        size: 10,
        file_exist_cols: "id".into(),
        ..Default::default()
    };
    let pinned = file("first");
    let plain = file("second");
    client
        .commit_data_files_with_commit_op(
            &name,
            NAMESPACE,
            vec![pinned.clone()],
            CommitOp::AppendCommit,
        )
        .await?;
    let snapshot = client.create_snapshot(&id, "protect first input").await?;
    client
        .commit_data_files_with_commit_op(
            &name,
            NAMESPACE,
            vec![plain.clone()],
            CommitOp::AppendCommit,
        )
        .await?;
    let read = latest(&client, &id, "p1").await?.unwrap();
    let inputs = client
        .get_data_file_infos_of_single_partition(&read)
        .await?;
    assert_eq!(inputs.len(), 2);
    let discarded: Vec<_> = inputs.iter().collect();
    let publication = uuid::Uuid::new_v4();
    assert!(
        client
            .publish_compacted_snapshot(
                &table,
                &read,
                publication,
                &[file("merged")],
                &discarded
            )
            .await?
    );
    assert!(client.compaction_is_published(&read, publication).await?);
    for (path, count) in [(pinned.path, 0), (plain.path, 1)] {
        let rows = client
            .execute_query(
                DaoType::SelectDiscardCompressedFileInfoByFilePath as i32,
                path.clone(),
            )
            .await?;
        assert_eq!(rows.discard_compressed_file_info.len(), count, "{path}");
    }
    client.drop_snapshot(&id, snapshot.snapshot_id).await?;
    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}
