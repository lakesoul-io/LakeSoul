// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for optimistic commit concurrency (OCC).
//!
//! They require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and use unique table names.

use std::collections::HashSet;
use std::sync::Arc;

use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::error::Result;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata_proto::entity::{CommitOp, MetaInfo, PartitionInfo, TableInfo};
use tokio::sync::Barrier;

const NAMESPACE: &str = "default";

fn unique_name(tag: &str) -> String {
    format!("occ_{tag}_{}", uuid::Uuid::new_v4().simple())
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

fn file(partition_desc: &str, name: &str) -> DataFileInfo {
    DataFileInfo {
        partition_desc: partition_desc.to_string(),
        path: format!("/tmp/{partition_desc}/{name}.parquet"),
        file_op: "add".to_string(),
        size: 1,
        file_exist_cols: "id".to_string(),
        ..Default::default()
    }
}

fn new_uuid() -> ((u64, u64), lakesoul_metadata_proto::entity::Uuid) {
    let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
    (
        (high, low),
        lakesoul_metadata_proto::entity::Uuid { high, low },
    )
}

fn snapshot_ids(partition_info: &PartitionInfo) -> HashSet<(u64, u64)> {
    partition_info
        .snapshot
        .iter()
        .map(|id| (id.high, id.low))
        .collect()
}

async fn latest_partition_info(
    client: &MetaDataClient,
    table_id: &str,
    partition_desc: &str,
) -> Result<PartitionInfo> {
    client
        .get_partition_info_by_table_id_and_partition_list(
            table_id,
            &[partition_desc.to_string()],
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(format!(
                "partition {partition_desc} not found"
            ))
        })
}

async fn commit_with_read(
    client: &MetaDataClient,
    table_id: &str,
    partition_desc: &str,
    commit_op: CommitOp,
    snapshot: Vec<lakesoul_metadata_proto::entity::Uuid>,
    read: Option<&PartitionInfo>,
) -> Result<()> {
    let table_info = client
        .get_table_info_by_table_id(table_id)
        .await?
        .ok_or_else(|| {
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(format!(
                "table {table_id} not found"
            ))
        })?;
    client
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: table_id.to_string(),
                    partition_desc: partition_desc.to_string(),
                    snapshot,
                    ..Default::default()
                }],
                read_partition_info: read.cloned().into_iter().collect(),
            },
            commit_op,
        )
        .await
}

#[tokio::test]
async fn concurrent_appends_keep_every_commit() -> Result<()> {
    let first = Arc::new(MetaDataClient::from_env().await?);
    let second = Arc::new(MetaDataClient::from_env().await?);
    let name = unique_name("appends");
    let table_id = create_test_table(&first, &name).await?;

    // With `MAX_COMMIT_ATTEMPTS = 5`, a writer loses at most `writers - 1`
    // times, so four concurrent writers always fit into the retry budget.
    let writers = 4;
    let barrier = Arc::new(Barrier::new(writers));
    let mut tasks = Vec::new();
    for index in 0..writers {
        let client = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let name = name.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .commit_data_files(
                    &name,
                    NAMESPACE,
                    vec![file("p1", &format!("f{index}"))],
                )
                .await
        }));
    }
    for task in tasks {
        task.await.expect("writer panicked")?;
    }

    // Every writer must have published a version; a lost update would show up
    // as a lower version or a snapshot missing one file.
    let latest = latest_partition_info(&first, &table_id, "p1").await?;
    assert_eq!(latest.version, writers as i32 - 1);
    assert_eq!(latest.snapshot.len(), writers);

    let files = first
        .get_data_files_of_single_partition(&latest)
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(files.len(), writers);
    for index in 0..writers {
        assert!(
            files.contains(&format!("/tmp/p1/f{index}.parquet")),
            "file f{index} was lost, active files: {files:?}"
        );
    }

    first.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn compaction_merges_concurrent_append() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("compact_merge");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let read = latest_partition_info(&client, &table_id, "p1").await?;

    // A concurrent append lands after the compaction read its snapshot.
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f2")])
        .await?;
    let after_append = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(after_append.version, read.version + 1);

    let concurrent_ids = snapshot_ids(&after_append)
        .difference(&snapshot_ids(&read))
        .copied()
        .collect::<HashSet<_>>();
    assert_eq!(concurrent_ids.len(), 1);

    let (compaction_id, compaction_uuid) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::CompactionCommit,
        vec![compaction_uuid],
        Some(&read),
    )
    .await?;

    // snapshot = compaction output + concurrent append, no lost file
    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, after_append.version + 1);
    assert_eq!(latest.commit_op(), CommitOp::CompactionCommit);
    let expected = concurrent_ids
        .into_iter()
        .chain(std::iter::once(compaction_id))
        .collect::<HashSet<_>>();
    assert_eq!(snapshot_ids(&latest), expected);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn update_over_concurrent_append_merges() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("update_merge");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let read = latest_partition_info(&client, &table_id, "p1").await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f2")])
        .await?;
    let after_append = latest_partition_info(&client, &table_id, "p1").await?;
    let concurrent_ids = snapshot_ids(&after_append)
        .difference(&snapshot_ids(&read))
        .copied()
        .collect::<HashSet<_>>();

    let (update_id, update_uuid) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![update_uuid],
        Some(&read),
    )
    .await?;

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, after_append.version + 1);
    assert_eq!(latest.commit_op(), CommitOp::UpdateCommit);
    let expected = concurrent_ids
        .into_iter()
        .chain(std::iter::once(update_id))
        .collect::<HashSet<_>>();
    assert_eq!(snapshot_ids(&latest), expected);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn stale_update_conflicts_with_concurrent_update() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("update_conflict");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    let (_, first_update) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![first_update],
        Some(&v0),
    )
    .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;

    let (_, second_update) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![second_update],
        Some(&v1),
    )
    .await?;

    // a third update still holding v1 must be rejected, not silently dropped
    let (_, third_update) = new_uuid();
    let error = commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![third_update],
        Some(&v1),
    )
    .await
    .expect_err("stale update must conflict");
    assert!(
        error
            .to_string()
            .contains("conflicts with concurrent writes"),
        "unexpected error: {error}"
    );

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v1.version + 1);
    assert!(!snapshot_ids(&latest).contains(&(third_update.high, third_update.low)));

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn stale_compaction_is_skipped_when_update_landed() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("compact_skip");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    let (_, update) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![update],
        Some(&v0),
    )
    .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;

    // the compaction read v0 while an update already owns the partition: its
    // partition must be dropped from the commit, not merged on top
    let (_, stale_compaction) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::CompactionCommit,
        vec![stale_compaction],
        Some(&v0),
    )
    .await?;

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v1.version);
    assert_eq!(snapshot_ids(&latest), snapshot_ids(&v1));

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn stale_update_conflicts_with_compaction_carrying_appends() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("compact_conflict");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    // a compaction whose snapshot kept a concurrent append (snapshot[1..])
    let (_, compaction_output) = new_uuid();
    let (_, concurrent_append) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::CompactionCommit,
        vec![compaction_output, concurrent_append],
        Some(&v0),
    )
    .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(v1.snapshot.len(), 2);

    // an update based on v0 cannot be merged across such a compaction
    let (_, update) = new_uuid();
    let error = commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![update],
        Some(&v0),
    )
    .await
    .expect_err("update over compaction with appends must conflict");
    assert!(
        error.to_string().contains("conflicts with a compaction"),
        "unexpected error: {error}"
    );

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v1.version);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}
