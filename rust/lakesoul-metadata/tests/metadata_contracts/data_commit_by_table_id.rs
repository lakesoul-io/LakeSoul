// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `SelectOneDataCommitInfoByTableId` returns the newest commit info of a
//! table.  It backs the Java `DataCommitInfoDao.selectByTableId` native path,
//! which used to send the wrong DAO code (and param count).

use lakesoul_metadata::error::Result;
use lakesoul_metadata::{DaoType, MetaDataClient};
use lakesoul_metadata_proto::entity::{
    CommitOp, DataCommitInfo, DataFileOp, FileOp, TableInfo, Uuid,
};

const NAMESPACE: &str = "default";

fn unique_name(tag: &str) -> String {
    format!(
        "data_commit_by_table_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    )
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

fn commit_info(
    table_id: &str,
    commit_id: Uuid,
    timestamp: i64,
    path: &str,
) -> DataCommitInfo {
    DataCommitInfo {
        table_id: table_id.to_string(),
        partition_desc: String::new(),
        pinned: false,
        file_ops: vec![DataFileOp {
            file_op: FileOp::Add as i32,
            path: path.to_string(),
            size: 1,
            file_exist_cols: "id".to_string(),
        }],
        commit_op: CommitOp::AppendCommit as i32,
        timestamp,
        commit_id: Some(commit_id),
        committed: false,
        domain: "public".to_string(),
    }
}

#[test_log::test(tokio::test)]
async fn select_latest_data_commit_info_by_table_id() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("latest");
    let table_id = create_test_table(&client, &name).await?;

    let older_id = Uuid { high: 1, low: 1 };
    let newer_id = Uuid { high: 2, low: 2 };
    client
        .commit_data_commit_info(commit_info(
            &table_id,
            older_id,
            1_000,
            "/tmp/older.parquet",
        ))
        .await?;
    client
        .commit_data_commit_info(commit_info(
            &table_id,
            newer_id,
            2_000,
            "/tmp/newer.parquet",
        ))
        .await?;

    let wrapper = client
        .execute_query(
            DaoType::SelectOneDataCommitInfoByTableId as i32,
            table_id.clone(),
        )
        .await?;
    assert_eq!(
        wrapper.data_commit_info.len(),
        1,
        "the query returns only the latest commit"
    );
    let latest = &wrapper.data_commit_info[0];
    assert_eq!(latest.commit_id, Some(newer_id));
    assert_eq!(latest.timestamp, 2_000);
    assert_eq!(latest.file_ops[0].path, "/tmp/newer.parquet");

    // A table without commits yields no rows.
    let empty_name = unique_name("empty");
    let empty_table_id = create_test_table(&client, &empty_name).await?;
    let wrapper = client
        .execute_query(
            DaoType::SelectOneDataCommitInfoByTableId as i32,
            empty_table_id,
        )
        .await?;
    assert!(wrapper.data_commit_info.is_empty());

    Ok(())
}
