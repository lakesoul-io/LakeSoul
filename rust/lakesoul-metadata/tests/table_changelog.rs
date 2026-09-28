// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The table-level changelog scan returns the same windows as the
//! per-partition scan, with one version query and one commit query for the
//! whole table.

use std::collections::HashMap;

use lakesoul_metadata::error::Result;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata::{MetaDataClient, PartitionChangelog};
use lakesoul_metadata_proto::entity::{CommitOp, MetaInfo, PartitionInfo, TableInfo};

const NAMESPACE: &str = "default";

fn unique_name(tag: &str) -> String {
    format!("table_changelog_{tag}_{}", uuid::Uuid::new_v4().simple())
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
    let current = client
        .get_all_partition_info(table_id)
        .await?
        .into_iter()
        .find(|partition| partition.partition_desc == desc)
        .ok_or_else(|| {
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(format!(
                "partition {desc} not found"
            ))
        })?;
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

fn paths(changelog: &PartitionChangelog) -> Vec<String> {
    changelog
        .added_files
        .iter()
        .map(|file| file.path.clone())
        .collect()
}

fn partition<'a>(
    window: &'a lakesoul_metadata::IncrementalWindow,
    desc: &str,
) -> Option<&'a PartitionChangelog> {
    window
        .partitions
        .iter()
        .find(|partition| partition.partition_desc == desc)
}

#[test_log::test(tokio::test)]
async fn table_changelog_matches_the_per_partition_scan() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("mixed");
    let table_id = create_test_table(&client, &name).await?;

    // p1 gets two commits, p2 one.
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "p1a"), file("p2", "p2a")])
        .await?;
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "p1b")])
        .await?;

    // A full scan covers every partition.
    let window = client
        .get_table_changelog(&table_id, &HashMap::new())
        .await?;
    let p1 = partition(&window, "p1").expect("p1 in the full scan");
    let p2 = partition(&window, "p2").expect("p2 in the full scan");
    assert_eq!(p1.to_version, 1);
    assert_eq!(p2.to_version, 0);
    assert_eq!(
        paths(p1),
        vec!["/tmp/p1/p1a.parquet", "/tmp/p1/p1b.parquet"]
    );
    assert_eq!(paths(p2), vec!["/tmp/p2/p2a.parquet"]);

    // Per-partition cursors: p2 has no new version and is skipped; p1 matches
    // the per-partition API exactly.
    let from = HashMap::from([("p1".to_string(), 0i64), ("p2".to_string(), 0i64)]);
    let window = client.get_table_changelog(&table_id, &from).await?;
    assert_eq!(window.partitions.len(), 1);
    let p1 = partition(&window, "p1").expect("p1 changed");
    let per_partition = client
        .get_partition_changelog(&table_id, "p1", 0, 1)
        .await?;
    assert_eq!(paths(p1), paths(&per_partition));
    assert_eq!(p1.to_version, per_partition.to_version);
    assert_eq!(p1.to_timestamp, per_partition.to_timestamp);
    assert_eq!(p1.requires_rebuild, per_partition.requires_rebuild);
    assert_eq!(p1.partition_deleted, per_partition.partition_deleted);

    // A dropped partition is reported by both scans.
    delete_partition(&client, &table_id, "p2").await?;
    let window = client.get_table_changelog(&table_id, &from).await?;
    let p2 = partition(&window, "p2").expect("p2 deleted");
    assert!(p2.partition_deleted);
    assert!(paths(p2).is_empty());
    let per_partition = client
        .get_partition_changelog(&table_id, "p2", 0, 1)
        .await?;
    assert!(per_partition.partition_deleted);
    // The scan is stateless: p1 is still reported for its (unchanged) window.
    assert_eq!(partition(&window, "p1").expect("p1 window").to_version, 1);

    // An update commit asks both scans for a rebuild.
    client
        .commit_data_files_with_commit_op(
            &name,
            NAMESPACE,
            vec![file("p1", "p1c")],
            CommitOp::UpdateCommit,
        )
        .await?;
    let window = client.get_table_changelog(&table_id, &from).await?;
    let p1 = partition(&window, "p1").expect("p1 updated");
    assert!(p1.requires_rebuild);
    let per_partition = client
        .get_partition_changelog(&table_id, "p1", 0, 2)
        .await?;
    assert!(per_partition.requires_rebuild);
    assert_eq!(p1.to_version, per_partition.to_version);

    Ok(())
}
