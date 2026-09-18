// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for as-of partition reads.
//!
//! They require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and use unique table names.

use std::time::Duration;

use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata::error::Result;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata_proto::entity::{PartitionInfo, TableInfo};

const NAMESPACE: &str = "default";
const COMMIT_GAP: Duration = Duration::from_millis(20);

fn unique_name(tag: &str) -> String {
    format!("asof_{tag}_{}", uuid::Uuid::new_v4().simple())
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

async fn files_of(
    client: &MetaDataClient,
    partition_info: &PartitionInfo,
) -> Result<Vec<String>> {
    let mut files = client
        .get_data_files_of_single_partition(partition_info)
        .await?;
    files.sort();
    Ok(files)
}

#[tokio::test]
async fn as_of_returns_historical_partition_versions() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("history");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(v0.version, 0);
    let t0 = v0.timestamp;

    tokio::time::sleep(COMMIT_GAP).await;
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f2")])
        .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(v1.version, 1);
    let t1 = v1.timestamp;
    assert!(t1 > t0, "expected t1 ({t1}) > t0 ({t0})");

    tokio::time::sleep(COMMIT_GAP).await;
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f3")])
        .await?;
    let v2 = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(v2.version, 2);
    let t2 = v2.timestamp;
    assert!(t2 > t1, "expected t2 ({t2}) > t1 ({t1})");

    let files = |name: &str| format!("/tmp/p1/{name}.parquet");
    let expected = |names: &[&str]| {
        let mut paths = names.iter().map(|name| files(name)).collect::<Vec<_>>();
        paths.sort();
        paths
    };

    // as-of each commit boundary: the version committed at that millisecond
    // is visible because the comparison is inclusive (`<=`).
    let at_t0 = client.get_all_partition_info_as_of(&table_id, t0).await?;
    assert_eq!(at_t0.len(), 1);
    assert_eq!(at_t0[0].version, 0);
    assert_eq!(files_of(&client, &at_t0[0]).await?, expected(&["f1"]));

    let at_t1 = client.get_all_partition_info_as_of(&table_id, t1).await?;
    assert_eq!(at_t1.len(), 1);
    assert_eq!(at_t1[0].version, 1);
    assert_eq!(files_of(&client, &at_t1[0]).await?, expected(&["f1", "f2"]));

    let at_t2 = client.get_all_partition_info_as_of(&table_id, t2).await?;
    assert_eq!(at_t2.len(), 1);
    assert_eq!(at_t2[0].version, 2);
    assert_eq!(
        files_of(&client, &at_t2[0]).await?,
        expected(&["f1", "f2", "f3"])
    );

    // before the first commit the partition does not exist yet
    let before = client
        .get_all_partition_info_as_of(&table_id, t0 - 1)
        .await?;
    assert!(before.is_empty());

    // single-partition variant agrees with the table-wide variant
    let single = client
        .get_partition_info_as_of(&table_id, "p1", t1)
        .await?
        .expect("partition at t1");
    assert_eq!(single.version, 1);
    assert_eq!(files_of(&client, &single).await?, expected(&["f1", "f2"]));
    assert!(
        client
            .get_partition_info_as_of(&table_id, "p1", t0 - 1)
            .await?
            .is_none()
    );

    // as-of reads never move the latest version
    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, 2);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn as_of_covers_partitions_created_at_different_times() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("partitions");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("a", "a1")])
        .await?;
    let a_v0 = latest_partition_info(&client, &table_id, "a").await?;
    let t_a0 = a_v0.timestamp;

    tokio::time::sleep(COMMIT_GAP).await;
    client
        .commit_data_files(&name, NAMESPACE, vec![file("b", "b1")])
        .await?;
    let b_v0 = latest_partition_info(&client, &table_id, "b").await?;
    let t_b0 = b_v0.timestamp;
    assert!(t_b0 > t_a0, "expected t_b0 ({t_b0}) > t_a0 ({t_a0})");

    tokio::time::sleep(COMMIT_GAP).await;
    client
        .commit_data_files(&name, NAMESPACE, vec![file("a", "a2")])
        .await?;
    let a_v1 = latest_partition_info(&client, &table_id, "a").await?;
    let t_a1 = a_v1.timestamp;
    assert!(t_a1 > t_b0, "expected t_a1 ({t_a1}) > t_b0 ({t_b0})");

    let at_t_a0 = client.get_all_partition_info_as_of(&table_id, t_a0).await?;
    assert_eq!(at_t_a0.len(), 1);
    assert_eq!(at_t_a0[0].partition_desc, "a");
    assert_eq!(at_t_a0[0].version, 0);

    let at_t_b0 = client.get_all_partition_info_as_of(&table_id, t_b0).await?;
    assert_eq!(at_t_b0.len(), 2);
    for info in &at_t_b0 {
        assert_eq!(info.version, 0, "partition {}", info.partition_desc);
    }

    let at_t_a1 = client.get_all_partition_info_as_of(&table_id, t_a1).await?;
    assert_eq!(at_t_a1.len(), 2);
    let version_of = |infos: &[PartitionInfo], desc: &str| {
        infos
            .iter()
            .find(|info| info.partition_desc == desc)
            .map(|info| info.version)
    };
    assert_eq!(version_of(&at_t_a1, "a"), Some(1));
    assert_eq!(version_of(&at_t_a1, "b"), Some(0));

    // partition b does not exist at t_a0
    assert!(
        client
            .get_partition_info_as_of(&table_id, "b", t_a0)
            .await?
            .is_none()
    );
    let b_at_t_b0 = client
        .get_partition_info_as_of(&table_id, "b", t_b0)
        .await?
        .expect("partition b at t_b0");
    assert_eq!(b_at_t_b0.version, 0);
    let a_at_t_a1 = client
        .get_partition_info_as_of(&table_id, "a", t_a1)
        .await?
        .expect("partition a at t_a1");
    assert_eq!(a_at_t_a1.version, 1);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}
