// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the version-based changelog read.
//!
//! They require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and use unique table names.

use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::error::Result;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata_proto::entity::{CommitOp, MetaInfo, PartitionInfo, TableInfo};

const NAMESPACE: &str = "default";

fn unique_name(tag: &str) -> String {
    format!("changelog_{tag}_{}", uuid::Uuid::new_v4().simple())
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

fn added_paths(changelog: &lakesoul_metadata::PartitionChangelog) -> Vec<&str> {
    changelog
        .added_files
        .iter()
        .map(|file| file.path.as_str())
        .collect()
}

async fn delete_partition(
    client: &MetaDataClient,
    table_id: &str,
    desc: &str,
) -> Result<()> {
    let table_info = client
        .get_table_info_by_table_id(table_id)
        .await?
        .ok_or_else(|| {
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(format!(
                "table {table_id} not found"
            ))
        })?;
    let current = latest_partition_info(client, table_id, desc).await?;
    client
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: table_id.to_string(),
                    partition_desc: desc.to_string(),
                    ..Default::default()
                }],
                read_partition_info: vec![current],
            },
            CommitOp::DeleteCommit,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn append_only_window_is_version_based_and_replayable() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("append");
    let table_id = create_test_table(&client, &name).await?;

    // No sleeps: consecutive commits may share the same millisecond timestamp,
    // which must not affect the version-based window.
    for file_name in ["f1", "f2", "f3"] {
        client
            .commit_data_files(&name, NAMESPACE, vec![file("p1", file_name)])
            .await?;
    }

    let whole = client
        .get_partition_changelog(&table_id, "p1", -1, 2)
        .await?;
    assert_eq!(
        added_paths(&whole),
        vec![
            "/tmp/p1/f1.parquet",
            "/tmp/p1/f2.parquet",
            "/tmp/p1/f3.parquet"
        ]
    );
    assert_eq!(whole.to_version, 2);
    assert!(!whole.requires_rebuild);
    assert!(!whole.partition_deleted);

    let middle = client
        .get_partition_changelog(&table_id, "p1", 0, 1)
        .await?;
    assert_eq!(added_paths(&middle), vec!["/tmp/p1/f2.parquet"]);
    assert_eq!(middle.to_version, 1);

    // replaying the same window yields the same files
    let replayed = client
        .get_partition_changelog(&table_id, "p1", 0, 1)
        .await?;
    assert_eq!(added_paths(&replayed), added_paths(&middle));
    assert_eq!(replayed.to_timestamp, middle.to_timestamp);

    // an empty window does not move the cursor
    let empty = client
        .get_partition_changelog(&table_id, "p1", 2, 2)
        .await?;
    assert!(empty.added_files.is_empty());
    assert_eq!(empty.to_version, 2);
    assert_eq!(empty.to_timestamp, whole.to_timestamp);

    // a window beyond the latest version stops at the latest
    let beyond = client
        .get_partition_changelog(&table_id, "p1", -1, 100)
        .await?;
    assert_eq!(added_paths(&beyond), added_paths(&whole));
    assert_eq!(beyond.to_version, 2);

    // before the first commit
    let before = client
        .get_partition_changelog(&table_id, "p1", -1, -1)
        .await?;
    assert!(before.added_files.is_empty());
    assert!(!before.requires_rebuild);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn table_wide_window_covers_all_partitions() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("tablewide");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("a", "a1")])
        .await?;
    client
        .commit_data_files(&name, NAMESPACE, vec![file("b", "b1")])
        .await?;
    client
        .commit_data_files(&name, NAMESPACE, vec![file("a", "a2")])
        .await?;

    let window = client
        .get_incremental_files(&table_id, None, -1, i64::MAX)
        .await?;
    assert_eq!(window.partitions.len(), 2);
    assert!(!window.requires_rebuild);
    assert!(window.deleted_partitions.is_empty());
    let mut paths = window
        .added_files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "/tmp/a/a1.parquet",
            "/tmp/a/a2.parquet",
            "/tmp/b/b1.parquet"
        ]
    );
    let version_of = |desc: &str| {
        window
            .partitions
            .iter()
            .find(|partition| partition.partition_desc == desc)
            .map(|partition| partition.to_version)
    };
    assert_eq!(version_of("a"), Some(1));
    assert_eq!(version_of("b"), Some(0));

    // a shared cursor only returns the suffix of each partition
    let suffix = client
        .get_incremental_files(&table_id, None, 0, i64::MAX)
        .await?;
    let mut suffix_paths = suffix
        .added_files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<Vec<_>>();
    suffix_paths.sort();
    assert_eq!(suffix_paths, vec!["/tmp/a/a2.parquet"]);

    // single-partition reads agree with the table-wide read
    let single = client
        .get_incremental_files(&table_id, Some("a"), 0, i64::MAX)
        .await?;
    assert_eq!(single.partitions.len(), 1);
    assert_eq!(
        added_paths(&single.partitions[0]),
        vec!["/tmp/a/a2.parquet"]
    );
    assert_eq!(single.partitions[0].to_version, 1);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn update_commit_requires_rebuild() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("update");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    client
        .commit_data_files_with_commit_op(
            &name,
            NAMESPACE,
            vec![file("p1", "f2")],
            CommitOp::UpdateCommit,
        )
        .await?;

    // the append before the update is still readable
    let before = client
        .get_partition_changelog(&table_id, "p1", -1, 0)
        .await?;
    assert_eq!(added_paths(&before), vec!["/tmp/p1/f1.parquet"]);
    assert!(!before.requires_rebuild);

    // a window containing the update must not be applied as a changelog
    let window = client
        .get_partition_changelog(&table_id, "p1", 0, 1)
        .await?;
    assert!(window.requires_rebuild);
    assert!(window.added_files.is_empty());
    assert_eq!(window.to_version, 1);

    let table_wide = client
        .get_incremental_files(&table_id, None, 0, i64::MAX)
        .await?;
    assert!(table_wide.requires_rebuild);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn compaction_output_is_not_changelog() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("compaction");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    client
        .commit_data_files_with_commit_op(
            &name,
            NAMESPACE,
            vec![file("p1", "compacted")],
            CommitOp::CompactionCommit,
        )
        .await?;

    // the compaction version adds no changelog files and does not need a rebuild
    let compaction = client
        .get_partition_changelog(&table_id, "p1", 0, 1)
        .await?;
    assert!(compaction.added_files.is_empty());
    assert!(!compaction.requires_rebuild);
    assert!(!compaction.partition_deleted);
    assert_eq!(compaction.to_version, 1);

    // the compaction output is excluded, the appended file before it is kept
    let whole = client
        .get_partition_changelog(&table_id, "p1", -1, 1)
        .await?;
    assert_eq!(added_paths(&whole), vec!["/tmp/p1/f1.parquet"]);
    assert_eq!(whole.to_version, 1);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn delete_commit_marks_partition_deleted() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("delete");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    delete_partition(&client, &table_id, "p1").await?;

    let window = client
        .get_partition_changelog(&table_id, "p1", 0, 1)
        .await?;
    assert!(window.partition_deleted);
    assert!(window.added_files.is_empty());
    assert!(!window.requires_rebuild);
    assert_eq!(window.to_version, 1);

    let table_wide = client
        .get_incremental_files(&table_id, None, 0, i64::MAX)
        .await?;
    assert_eq!(table_wide.deleted_partitions, vec!["p1".to_string()]);
    assert!(table_wide.added_files.is_empty());

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn missing_baseline_requires_rebuild_or_marks_deleted() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("baseline");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;

    // the cursor is ahead of the partition history: diff cannot be trusted
    let ahead = client
        .get_partition_changelog(&table_id, "p1", 5, 0)
        .await?;
    assert!(ahead.requires_rebuild);
    assert!(ahead.added_files.is_empty());

    // no baseline and no partition at all: the partition is gone
    let ghost = client
        .get_partition_changelog(&table_id, "ghost", 3, i64::MAX)
        .await?;
    assert!(ghost.partition_deleted);
    assert!(ghost.added_files.is_empty());

    // a fresh partition read from the beginning has an implicit empty baseline
    let fresh = client
        .get_partition_changelog(&table_id, "p1", -1, 0)
        .await?;
    assert_eq!(added_paths(&fresh), vec!["/tmp/p1/f1.parquet"]);
    assert!(!fresh.requires_rebuild);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn deleted_partition_without_row_is_reported_as_deleted() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("physdel");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    // no delete commit: the caller still holds cursor 0 for a vanished partition
    client.delete_partition_info_by_table_id(&table_id).await?;

    let ghost = client
        .get_partition_changelog(&table_id, "p1", 0, 0)
        .await?;
    assert!(ghost.partition_deleted);
    assert!(!ghost.requires_rebuild);
    assert!(ghost.added_files.is_empty());

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}
