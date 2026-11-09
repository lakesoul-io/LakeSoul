// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Serialisable description of one IVM refresh.
//!
//! The coordinator resolves a refresh window from PostgreSQL metadata and can
//! hand workers a plain descriptor instead of the metadata client.  The
//! descriptor deliberately contains only table configuration, file paths and
//! Arrow schemas: credentials stay on the worker, and the `ivm` schema remains
//! coordinator-only.

use std::collections::HashMap;
use std::str::FromStr;

use arrow_schema::SchemaRef;
use lakesoul_io::file_format::PhysicalFormat;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::metadata::{PartitionVersion, StateVersion};
use crate::runtime::{SourceWindow, ViewSpec};
use crate::table::{IvmTable, PartitionFiles};

/// The kind of refresh a descriptor describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IvmRefreshMode {
    /// Apply a source changelog window incrementally.
    Incremental,
    /// Recompute the view from the current source state.
    Rebuild,
    /// The first materialization of a newly analysed view.
    Bootstrap,
}

/// Where a table lives and how its files are laid out.
///
/// This is the plain subset of [`IvmTable`] a worker needs to read and write
/// files.  `hash_bucket_num` is a number here (the table stores it as text for
/// the metadata properties) and `file_format` uses [`PhysicalFormat::name`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IvmTableDescriptor {
    pub table_id: String,
    pub table_name: String,
    pub namespace: String,
    pub table_path: String,
    pub schema: SchemaRef,
    pub primary_keys: Vec<String>,
    pub range_partition_columns: Vec<String>,
    pub internal_columns: Vec<String>,
    pub bucket_columns: Vec<String>,
    pub hash_bucket_num: u32,
    pub cdc_column: Option<String>,
    pub file_format: String,
    pub allow_fallback: bool,
}

impl IvmTable {
    /// A plain descriptor that can cross the worker wire.
    pub fn to_descriptor(&self) -> Result<IvmTableDescriptor> {
        let hash_bucket_num = self.hash_bucket_num.parse::<u32>().map_err(|error| {
            rootcause::report!(
                "table {} has an invalid hash bucket count {:?}: {error}",
                self.table_name,
                self.hash_bucket_num
            )
        })?;
        Ok(IvmTableDescriptor {
            table_id: self.table_id.clone(),
            table_name: self.table_name.clone(),
            namespace: self.namespace.clone(),
            table_path: self.table_path.clone(),
            schema: self.schema.clone(),
            primary_keys: self.primary_keys.clone(),
            range_partition_columns: self.range_partition_columns.clone(),
            internal_columns: self.internal_columns.clone(),
            bucket_columns: self.bucket_columns.clone(),
            hash_bucket_num,
            cdc_column: self.cdc_column.clone(),
            file_format: self.file_format.name().to_string(),
            allow_fallback: self.allow_fallback,
        })
    }

    /// Rebuild a runtime table handle from a descriptor.
    pub fn from_descriptor(descriptor: &IvmTableDescriptor) -> Result<Self> {
        Ok(Self {
            table_id: descriptor.table_id.clone(),
            table_name: descriptor.table_name.clone(),
            namespace: descriptor.namespace.clone(),
            table_path: descriptor.table_path.clone(),
            schema: descriptor.schema.clone(),
            primary_keys: descriptor.primary_keys.clone(),
            range_partition_columns: descriptor.range_partition_columns.clone(),
            internal_columns: descriptor.internal_columns.clone(),
            bucket_columns: descriptor.bucket_columns.clone(),
            hash_bucket_num: descriptor.hash_bucket_num.to_string(),
            cdc_column: descriptor.cdc_column.clone(),
            file_format: PhysicalFormat::from_str(&descriptor.file_format)?,
            allow_fallback: descriptor.allow_fallback,
        })
    }
}

/// The source files and cursor window one refresh consumes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IvmSourceSnapshot {
    pub table: IvmTableDescriptor,
    pub added_files: Vec<PartitionFiles>,
    pub cursors: Vec<crate::metadata::Cursor>,
    pub identity: Vec<(String, String, i64, i64)>,
    pub before_timestamp: i64,
    pub before_versions: HashMap<String, Option<i64>>,
}

impl IvmSourceSnapshot {
    /// Capture a source window with the plain table descriptor.
    #[allow(dead_code)] // Used by the task-descriptor builder as it lands.
    pub(crate) fn from_window(table: &IvmTable, window: &SourceWindow) -> Result<Self> {
        Ok(Self {
            table: table.to_descriptor()?,
            added_files: window.added_files.clone(),
            cursors: window.cursors.clone(),
            identity: window.identity.clone(),
            before_timestamp: window.before_timestamp,
            before_versions: window.before_versions.clone(),
        })
    }
}

/// How a view's output is routed across workers.
///
/// `state_buckets` is the number of LakeSoul hash buckets of the internal
/// state; `shuffle_partitions` is the number of execution partitions the
/// coordinator asks the distributed planner for.  Inputs are always routed by
/// the deterministic LakeSoul bucket id, never by a runtime bucket lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IvmBucketPlan {
    pub state_buckets: u32,
    pub shuffle_partitions: u32,
    pub routing_columns: Vec<String>,
}

impl IvmBucketPlan {
    pub fn for_table(table: &IvmTable) -> Result<Self> {
        let descriptor = table.to_descriptor()?;
        let routing_columns = if descriptor.bucket_columns.is_empty() {
            descriptor.primary_keys.clone()
        } else {
            descriptor.bucket_columns.clone()
        };
        Ok(Self {
            state_buckets: descriptor.hash_bucket_num.max(1),
            shuffle_partitions: descriptor.hash_bucket_num.max(1),
            routing_columns,
        })
    }
}

/// Everything a coordinator resolved for one refresh window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IvmRefreshDescriptor {
    pub view_id: String,
    pub generation: i64,
    pub epoch: i64,
    pub window_key: String,
    pub mode: IvmRefreshMode,
    pub spec: ViewSpec,
    pub sources: Vec<IvmSourceSnapshot>,
    pub states: Vec<IvmTableDescriptor>,
    pub output: IvmTableDescriptor,
    pub bucket: IvmBucketPlan,
    pub mv_versions_before: Vec<PartitionVersion>,
    pub state_versions_before: Vec<StateVersion>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn table() -> IvmTable {
        IvmTable {
            table_id: "table-id".to_string(),
            table_name: "table".to_string(),
            namespace: "default".to_string(),
            table_path: "file:///tmp/table".to_string(),
            schema: Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Utf8, true),
            ])),
            primary_keys: vec!["k".to_string()],
            range_partition_columns: Vec::new(),
            internal_columns: Vec::new(),
            bucket_columns: vec!["k".to_string()],
            hash_bucket_num: "4".to_string(),
            cdc_column: Some("rowKinds".to_string()),
            file_format: PhysicalFormat::Vortex,
            allow_fallback: true,
        }
    }

    #[test]
    fn a_table_descriptor_round_trips() {
        let table = table();
        let descriptor = table.to_descriptor().unwrap();
        assert_eq!(descriptor.hash_bucket_num, 4);
        assert_eq!(descriptor.file_format, "vortex");
        let restored = IvmTable::from_descriptor(&descriptor).unwrap();
        assert_eq!(restored.table_id, table.table_id);
        assert_eq!(restored.schema, table.schema);
        assert_eq!(restored.primary_keys, table.primary_keys);
        assert_eq!(restored.hash_bucket_num, table.hash_bucket_num);
        assert_eq!(restored.file_format, table.file_format);
    }

    #[test]
    fn a_refresh_descriptor_round_trips() {
        let table = table();
        let descriptor = IvmRefreshDescriptor {
            view_id: "view".to_string(),
            generation: 1,
            epoch: 7,
            window_key: "window".to_string(),
            mode: IvmRefreshMode::Incremental,
            spec: ViewSpec::Row {
                view_id: "view".to_string(),
                source_table_id: table.table_id.clone(),
                mv_table_id: "mv".to_string(),
                output_columns: vec!["k".to_string(), "v".to_string()],
                output_exprs: Vec::new(),
                filter: Some("k > 0".to_string()),
                scalar: None,
            },
            sources: Vec::new(),
            states: Vec::new(),
            output: table.to_descriptor().unwrap(),
            bucket: IvmBucketPlan::for_table(&table).unwrap(),
            mv_versions_before: Vec::new(),
            state_versions_before: Vec::new(),
        };
        let encoded = serde_json::to_string(&descriptor).unwrap();
        let decoded: IvmRefreshDescriptor = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, descriptor);
    }

    #[test]
    fn a_source_snapshot_keeps_the_window_and_table() {
        let table = table();
        let window = SourceWindow {
            added_files: vec![PartitionFiles {
                partition_desc: "-5".to_string(),
                files: vec!["a.vortex".to_string()],
            }],
            cursors: Vec::new(),
            identity: vec![(table.table_id.clone(), "-5".to_string(), 0, 1)],
            before_timestamp: 0,
            before_versions: HashMap::from([("-5".to_string(), None)]),
            barrier: false,
        };
        let snapshot = IvmSourceSnapshot::from_window(&table, &window).unwrap();
        assert_eq!(snapshot.table.table_id, table.table_id);
        assert_eq!(snapshot.added_files, window.added_files);
        assert_eq!(snapshot.identity, window.identity);
        let encoded = serde_json::to_string(&snapshot).unwrap();
        let decoded: IvmSourceSnapshot = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, snapshot);
    }
}
