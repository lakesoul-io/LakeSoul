// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! wrap catalog->split->reader
//! [WIP]
//! prototype
use std::collections::HashMap;

use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::Client;

use proto::proto::entity::{DataCommitInfo, DataFileOp, FileOp, PartitionInfo, TableInfo};

use crate::{error::Result, MetaDataClient, PreparedStatementMap};
use crate::error::LakeSoulMetaDataError;
use crate::transfusion::config::{
    LAKESOUL_HASH_PARTITION_SPLITTER, LAKESOUL_NON_PARTITION_TABLE_PART_DESC,
    LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH, LAKESOUL_RANGE_PARTITION_SPLITTER,
};

mod config {
    #![allow(unused)]

    /// copy from DBConfig
    pub const MAX_COMMIT_ATTEMPTS: i32 = 5;
    //
    pub const LAKESOUL_DEFAULT_NAMESPACE: &str = "default";
    //
    pub const LAKESOUL_NAMESPACE_LEVEL_SPLITTER: &str = ".";

    pub const LAKESOUL_NULL_STRING: &str = "__L@KE$OUL_NULL__";

    pub const LAKESOUL_EMPTY_STRING: &str = "__L@KE$OUL_EMPTY_STRING__";

    pub const LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH: &str = ";";

    pub const LAKESOUL_RANGE_PARTITION_SPLITTER: &str = ",";

    pub const LAKESOUL_HASH_PARTITION_SPLITTER: &str = ",";

    pub const LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER: &str = ",";

    pub const LAKESOUL_NON_PARTITION_TABLE_PART_DESC: &str = "-5";

    pub const LAKESOUL_PARTITION_DESC_KV_DELIM: &str = "=";

    pub const HASH_BUCKET_NUM: &str = "hashBucketNum";

    pub const DROPPED_COLUMN: &str = "droppedColumn";
    //
    pub const DROPPED_COLUMN_SPLITTER: &str = ",";
    //
    pub const LAST_TABLE_SCHEMA_CHANGE_TIME: &str = "last_schema_change_time";
}

/// partitiondesc of non-range table is "-5"
pub fn table_without_range(range_key: &str) -> bool {
    range_key == LAKESOUL_NON_PARTITION_TABLE_PART_DESC
}

/// hashbucketnum of Non-primary key table is "-1"
pub fn table_without_pk(hash_bucket_num: &str) -> bool {
    hash_bucket_num == "-1"
}

/// use raw ptr to create `MetadataClientRef`
/// stay origin memory the same
/// see https://users.rust-lang.org/t/dereferencing-a-boxed-value/86768
pub async fn split_desc_array(
    client: &Client,
    prepared: &PreparedStatementMap,
    table_name: &str,
    namespace: &str,
) -> Result<SplitDescArray> {
    // make MetadataClient
    let (mut boxed_client, mut boxed_prepared) = unsafe {
        let boxed_client = Box::from_raw(client as *const Client as *mut Client);
        let boxed_prepared = Box::from_raw(prepared as *const PreparedStatementMap as *mut PreparedStatementMap);
        (boxed_client, boxed_prepared)
    };
    let db = MetaDataClient::compose(*boxed_client, *boxed_prepared, 3);
    let ret = split_desc_array_inner(&db, table_name, namespace).await;

    // restore client and prepared

    // the origin ptr disappeared
    let (c, p) = MetaDataClient::decompose(db)?;
    *boxed_client = c;
    *boxed_prepared = p;
    Box::into_raw(boxed_client);
    Box::into_raw(boxed_prepared);
    ret
}

async fn split_desc_array_inner(db: &MetaDataClient, table_name: &str, namespace: &str) -> Result<SplitDescArray> {
    let table_info = db.get_table_info_by_table_name(table_name, namespace).await?;
    let data_files = db.get_table_data_info(&table_info.table_id).await?;

    // create splits
    let mut splits = Vec::new();
    // // split by range and hash partition
    let mut map = HashMap::new();

    for df in &data_files {
        if has_hash_partitions(&table_info) && df.bucket_id() != -1 {
            map.entry(df.partition_desc.as_str())
                .or_insert(HashMap::new())
                .entry(df.bucket_id())
                .or_insert(Vec::new())
                .push(df.path.clone());
        } else {
            map.entry(df.partition_desc.as_str())
                .or_insert(HashMap::new())
                .entry(-1)
                .or_insert(Vec::new())
                .push(df.path.clone());
        }
    }
    // hash keys
    let (_rk, pk) = parse_table_info_partitions(&table_info.partitions);

    for (range_key, value_map) in map {
        let mut range_desc = HashMap::new();
        if !table_without_range(range_key) {
            let keys: Vec<String> = range_key
                .split(LAKESOUL_RANGE_PARTITION_SPLITTER)
                .map(ToString::to_string)
                .collect();
            for k in keys {
                let (k, v) = match k.split_once('=') {
                    None => {
                        return Err(LakeSoulMetaDataError::Internal("split error".to_string()));
                    }
                    Some((k, v)) => {
                        (k.to_string(), v.to_string())
                    }
                };
                range_desc.insert(k, v);
            }
        }
        for physical_files in value_map {
            let sd = SplitDesc {
                file_paths: physical_files.1,
                primary_keys: pk.clone(),
                partition_desc: range_desc.clone(),
                table_schema: table_info.table_schema.clone(),
            };
            splits.push(sd)
        }
    }
    Ok(SplitDescArray(splits))
}

fn has_hash_partitions(table_info: &TableInfo) -> bool {
    let properties: Value = serde_json::from_str(&table_info.properties).expect("wrong properties");
    if properties["hashBucketNum"] != Value::Null && properties["hashBucketNum"] == "-1" {
        false
    } else {
        properties["hashBucketNum"] != Value::Null
    }
}

// The file name of bucketed data should have 3 parts:
//   1. some other information in the head of file name
//   2. bucket id part, some numbers, starts with "_"
//      * The other-information part may use `-` as separator and may have numbers at the end,
//        e.g. a normal parquet file without bucketing may have name:
//        part-r-00000-2dd664f9-d2c4-4ffe-878f-431234567891.gz.parquet, and we will mistakenly
//        treat `431234567891` as bucket id. So here we pick `_` as separator.
//   3. optional file extension part, in the tail of file name, starts with `.`
// An example of bucketed parquet file name with bucket id 3:
//   part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet

#[derive(Debug, Clone, Default)]
pub struct DataFileInfo {
    // range partitions
    pub partition_desc: String,
    pub path: String,
    pub file_op: String,
    pub size: i64,
    pub bucket_id: Option<isize>,
    // unix timestamp
    pub modification_time: i64,
    pub file_exist_cols: String,
}

impl DataFileInfo {
    const BUCKET_FILE_NAME_REGEX: &'static str = r#".*_(\d+)(?:\..*)?$"#;
    pub fn new() -> Self {
        Default::default()
    }

    pub(crate) fn compose(
        data_commit_info: &DataCommitInfo,
        data_file_op: &DataFileOp,
        partition_info: &PartitionInfo,
    ) -> Result<Self> {
        Ok(Self {
            partition_desc: partition_info.partition_desc.clone(),
            path: data_file_op.path.clone(),
            file_op: FileOp::try_from(data_file_op.file_op)?.as_str_name().to_string(),
            size: data_file_op.size,
            bucket_id: Self::parse_bucket_id(&data_file_op.path),
            modification_time: data_commit_info.timestamp,
            file_exist_cols: data_file_op.file_exist_cols.clone(),
        })
    }

    fn parse_bucket_id(filename: &str) -> Option<isize> {
        let re = Regex::new(DataFileInfo::BUCKET_FILE_NAME_REGEX).unwrap();
        let Some(caps) = re.captures(filename) else {
            return Some(-1);
        };
        caps[1].parse::<isize>().ok()
    }

    pub fn bucket_id(&self) -> isize {
        self.bucket_id.unwrap_or(-1)
    }
}

/// COPY from lakesoul-datafusion
pub fn parse_table_info_partitions(partitions: &str) -> (Vec<String>, Vec<String>) {
    let (range_keys, hash_keys) =
        partitions.split_at(partitions.find(LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH).unwrap());
    let hash_keys = &hash_keys[1..];
    (
        range_keys
            .split(LAKESOUL_RANGE_PARTITION_SPLITTER)
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
        hash_keys
            .split(LAKESOUL_HASH_PARTITION_SPLITTER)
            .collect::<Vec<&str>>()
            .iter()
            .filter_map(|str| if str.is_empty() { None } else { Some(str.to_string()) })
            .collect::<Vec<String>>(),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitDesc {
    pub file_paths: Vec<String>,
    pub primary_keys: Vec<String>,
    pub partition_desc: HashMap<String, String>,
    pub table_schema: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitDescArray(pub Vec<SplitDesc>);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn prop_test() {
        let s = r##"{"hashBucketNum":"2","hashPartitions":"id"}"##;
        println!("{s}");
        let x: Value = serde_json::from_str(s).unwrap();
        assert_eq!(x["hello"], Value::Null);
        assert_eq!(x["hashBucketNum"], "2")
    }

    #[test]
    fn regex_test() {
        let file_name = "part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet";
        assert_eq!(Some(3isize), DataFileInfo::parse_bucket_id(file_name));
        let file_name = "part-r-00000-2dd664f9-d2c4-4ffe-878f-431234567891.gz.parquet";
        assert_eq!(Some(-1), DataFileInfo::parse_bucket_id(file_name));
    }

    #[test]
    fn ownership_test() {
        struct A {
            x: u32,
        }
        impl A {
            fn x(&self) -> u32 {
                self.x
            }
        }
        unsafe {
            let origin = Box::new(A { x: 42 });
            // simulate a ptr from ffi
            let raw1 = Box::into_raw(origin);
            let mut origin = Box::from_raw(raw1);
            // move_back to stack
            let a = *origin;
            let a_x = a.x();
            // move to origin
            *origin = a;
            let o_x = origin.x();
            assert_eq!(o_x, a_x);
            let raw2 = Box::into_raw(origin);
            assert_eq!(raw1, raw2);
            // free
            let _f = Box::from_raw(raw2);
        }
    }

    #[test]
    fn serialize_test() {
       let sd = SplitDesc {
           file_paths: vec![],
           primary_keys: vec![],
           partition_desc: Default::default(),
           table_schema: "".to_string(),
       };
        let s = serde_json::to_string(&sd).unwrap();
        println!("{s}");
    }
}
