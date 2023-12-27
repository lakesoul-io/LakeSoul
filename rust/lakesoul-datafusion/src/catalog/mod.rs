// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use std::{env, path::PathBuf};

use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_metadata::MetaDataClientRef;
use proto::proto::entity::{CommitOp, DataCommitInfo, DataFileOp, FileOp, TableInfo, Uuid};

use crate::lakesoul_table::helpers::create_io_config_builder_from_table_info;
use crate::serialize::arrow_java::{schema_from_metadata_str, ArrowJavaSchema};
// use crate::transaction::TransactionMetaInfo;
use crate::error::Result;

// pub mod lakesoul_sink;
// pub mod lakesoul_source;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LakeSoulTableProperty {
    pub hash_bucket_num: Option<usize>,
}

pub(crate) async fn create_table(client: MetaDataClientRef, table_name: &str, config: LakeSoulIOConfig) -> Result<()> {
    client
        .create_table(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: table_name.to_string(),
            table_path: format!("file://{}default/{}", env::temp_dir().to_str().unwrap(), table_name),
            table_schema: serde_json::to_string::<ArrowJavaSchema>(&config.schema().into()).unwrap(),
            table_namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(4),
            })?,
            partitions: format!(
                "{};{}",
                "",
                config
                    .primary_keys_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}

pub(crate) async fn create_io_config_builder(
    client: MetaDataClientRef,
    table_name: Option<&str>,
    fetch_files: bool,
) -> Result<LakeSoulIOConfigBuilder> {
    if let Some(table_name) = table_name {
        let table_info = client.get_table_info_by_table_name(table_name, "default").await?;
        let data_files = if fetch_files {
            client
                .get_data_files_by_table_name(table_name, vec![], "default")
                .await?
        } else {
            vec![]
        };
        Ok(create_io_config_builder_from_table_info(Arc::new(table_info)).with_files(data_files))
    } else {
        Ok(LakeSoulIOConfigBuilder::new())
    }
}

pub(crate) fn parse_table_info_partitions(partitions: String) -> (Vec<String>, Vec<String>) {
    let (range_keys, hash_keys) = partitions.split_at(partitions.find(';').unwrap());
    let hash_keys = &hash_keys[1..];
    (
        range_keys
            .split(',')
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
        hash_keys
            .split(',')
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
    )
}

pub(crate) async fn commit_data(
    client: MetaDataClientRef,
    table_name: &str,
    partitions: Vec<(String, String)>,
    files: &[String],
) -> Result<()> {
    let table_name_id = client.get_table_name_id_by_table_name(table_name, "default").await?;
    client
        .commit_data_commit_info(DataCommitInfo {
            table_id: table_name_id.table_id,
            partition_desc: if partitions.is_empty() {
                "-5".to_string()
            } else {
                partitions
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(",")
            },
            file_ops: files
                .iter()
                .map(|file| DataFileOp {
                    file_op: FileOp::Add as i32,
                    path: file.clone(),
                    ..Default::default()
                })
                .collect(),
            commit_op: CommitOp::AppendCommit as i32,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            commit_id: {
                let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
                Some(Uuid { high, low })
            },
            committed: false,
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}
