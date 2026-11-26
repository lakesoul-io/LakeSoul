// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Coordinator state and the complete input for a single partition version.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use lakesoul_metadata_proto::entity::{PartitionInfo, TableInfo};

use super::compaction::CompactionFile;
use super::compaction_discovery::{DiscoveryConfig, register_mirror_functions};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::time::Duration;

use datafusion::common::{DataFusionError, Result};
use lakesoul_common::ser::arrow_java::schema_from_table_info_metadata;
use sha2::{Digest, Sha256};

use super::codec::CODEC_VERSION;
use super::compaction::{
    CompactionDescriptor, CompactionPolicy, CompactionTableDescriptor, RunOutput,
    RunTask, bucket_has_work, execute_distributed_runs_inner, validate_compaction_id,
};
use crate::catalog::{parse_table_info_partitions, table_file_format};
use crate::lakesoul_table::helpers::resolve_bucket_columns;

/// Complete files for one partition version, ordered by descending level.
/// Within each level, files retain the snapshot replay order.
#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
    table: TableInfo,
    partition: PartitionInfo,
    active_files: Vec<CompactionFile>,
}

impl LoadedSnapshot {
    pub(super) fn from_discovery(
        table: TableInfo,
        partition: PartitionInfo,
        active_files: Vec<CompactionFile>,
    ) -> Self {
        debug_assert!(
            active_files
                .windows(2)
                .all(|pair| pair[0].level >= pair[1].level),
            "Discovery must order active files by descending level"
        );
        Self {
            table,
            partition,
            active_files,
        }
    }

    pub fn table(&self) -> &TableInfo {
        &self.table
    }

    pub fn partition(&self) -> &PartitionInfo {
        &self.partition
    }

    pub fn active_files(&self) -> &[CompactionFile] {
        &self.active_files
    }
}

pub struct CompactionCoordinator {
    pub(super) metadata_session: Arc<SessionContext>,
    pub(super) config: DiscoveryConfig,
    execution: Option<(Arc<SessionContext>, ExecutionConfig)>,
}

impl CompactionCoordinator {
    pub fn new(metadata_session: Arc<SessionContext>, config: DiscoveryConfig) -> Self {
        register_mirror_functions(&metadata_session);
        Self {
            metadata_session,
            config,
            execution: None,
        }
    }
}

pub struct PolicyConfig {
    pub policy_defaults: CompactionPolicy,
    pub rolling_enabled: bool,
    pub l1_rolling_tables: BTreeSet<(String, String)>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            policy_defaults: CompactionPolicy::default(),
            // Spark's `compaction.lowlevel.rolling.enabled` default.
            rolling_enabled: true,
            // Empty allows every table, matching Spark.
            l1_rolling_tables: BTreeSet::new(),
        }
    }
}

pub struct ExecutionConfig {
    pub policy: PolicyConfig,
    pub reader_options: BTreeMap<String, String>,
    pub writer_options: BTreeMap<String, String>,
    pub execution_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct CompactionTask {
    task_id: String,
    input: LoadedSnapshot,
    policy: CompactionPolicy,
    policy_revision: String,
    table_descriptor: Option<CompactionTableDescriptor>,
}

/// A changed partition and the validated results that produced it.
#[derive(Debug)]
pub struct CompactionExecutionResult {
    /// Complete partition file list, including buckets that were not dispatched.
    #[allow(dead_code)] // TODO: consumed by the coordinator commit stage.
    pub(crate) final_files: Vec<CompactionFile>,
    /// The consumed positions in each run are relative to the original files
    /// for its bucket, in their order in the caller's task snapshot.
    #[allow(dead_code)] // TODO: consumed by the coordinator commit stage.
    pub(crate) run_outputs: Vec<RunOutput>,
}

impl CompactionTask {
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn input(&self) -> &LoadedSnapshot {
        &self.input
    }

    pub fn policy(&self) -> &CompactionPolicy {
        &self.policy
    }

    pub fn policy_revision(&self) -> &str {
        &self.policy_revision
    }
}

fn execution_error(error: impl ToString) -> DataFusionError {
    DataFusionError::Execution(error.to_string())
}

pub fn resolve_policy(
    config: &PolicyConfig,
    table: &TableInfo,
) -> Result<CompactionPolicy> {
    let mut policy = config.policy_defaults.clone();
    policy.rolling_enabled_for_table = config.rolling_enabled
        && (config.l1_rolling_tables.is_empty()
            || config
                .l1_rolling_tables
                .contains(&(table.table_namespace.clone(), table.table_name.clone())));
    policy.validate()?;
    Ok(policy)
}

/// Bump this version when the same effective policy can produce a different
/// compaction decision, including when the planner's semantics change.
const POLICY_REVISION_VERSION: u32 = 1;

pub fn policy_revision(policy: &CompactionPolicy) -> Result<String> {
    let mut canonical = serde_json::to_value(policy).map_err(execution_error)?;
    canonical.sort_all_objects();
    let encoded = serde_json::to_vec(&canonical).map_err(execution_error)?;
    let mut revision = format!("v{POLICY_REVISION_VERSION}:");
    for byte in Sha256::digest(&encoded) {
        write!(revision, "{byte:02x}").map_err(execution_error)?;
    }
    Ok(revision)
}

pub fn build_table_descriptor(
    table: &TableInfo,
    partition: &PartitionInfo,
) -> Result<CompactionTableDescriptor> {
    if table.table_id != partition.table_id {
        return Err(execution_error(
            "compaction table and partition identities disagree",
        ));
    }
    let (range_partition_columns, primary_keys) =
        parse_table_info_partitions(&table.partitions).map_err(execution_error)?;
    let schema = Arc::new(
        schema_from_table_info_metadata(
            &table.table_schema,
            &table.table_schema_arrow_ipc,
            &table.table_schema_arrow_ipc_json_hash,
        )
        .map_err(execution_error)?,
    );
    let properties: serde_json::Value =
        serde_json::from_str(&table.properties).map_err(execution_error)?;
    // Spark table properties are string-valued. A blank value does not
    // activate blob handling in CompactBucketIO.
    match properties.get(lakesoul_io::config::OPTION_KEY_BLOB_COLUMNS) {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::String(value)) if value.trim().is_empty() => {}
        Some(serde_json::Value::String(_)) => {
            return Err(DataFusionError::NotImplemented(
                "blob compaction is not supported".into(),
            ));
        }
        Some(_) => return Err(execution_error("blob_columns must be a string")),
    }
    let hash_bucket_num = match properties.get("hashBucketNum") {
        Some(serde_json::Value::String(value)) => {
            value.parse::<i64>().map_err(execution_error)?
        }
        Some(serde_json::Value::Null) | None => -1,
        Some(value) => value
            .as_i64()
            .ok_or_else(|| execution_error("invalid hashBucketNum"))?,
    };
    if hash_bucket_num == -1 {
        return Err(DataFusionError::NotImplemented(
            "compaction requires a hash-bucketed table".into(),
        ));
    }
    let hash_bucket_num = u32::try_from(hash_bucket_num).map_err(execution_error)?;
    let mut partition_values = BTreeMap::new();
    if partition.partition_desc != "-5" {
        for pair in partition.partition_desc.split(',') {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| execution_error("invalid range partition description"))?;
            if partition_values
                .insert(key.to_owned(), value.to_owned())
                .is_some()
            {
                return Err(execution_error("duplicate range partition column"));
            }
        }
    }
    let bucket_columns = properties
        .get("lakesoul.ivm.bucket_columns")
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_str().ok_or_else(|| {
                execution_error("lakesoul.ivm.bucket_columns must be a string")
            })
        })
        .transpose()?;
    let hash_partitioning_columns = resolve_bucket_columns(
        &primary_keys,
        properties
            .get("lakesoul.ivm.internal")
            .and_then(serde_json::Value::as_str),
        bucket_columns,
    )
    .map_err(execution_error)?;
    let descriptor = CompactionTableDescriptor {
        table_id: table.table_id.clone(),
        table_path: table.table_path.clone(),
        schema,
        hash_partitioning_columns,
        primary_keys,
        range_partition_columns,
        partition_values,
        hash_bucket_num,
        cdc_column: properties
            .get("lakesoul_cdc_change_column")
            .and_then(serde_json::Value::as_str)
            .filter(|column| !column.is_empty())
            .map(str::to_owned),
        physical_format: table_file_format(&table.properties)
            .map_err(execution_error)?
            .extension()
            .to_owned(),
        // TODO(compaction): Spark's `mergeOperatorInfo` is supplied per
        // compaction call and is not recoverable from the mirror. Discovery
        // only supports the default merge semantics; tables that require
        // custom operators must be excluded until the operator map has a
        // persisted or configured source.
        merge_operators: BTreeMap::new(),
    };
    descriptor.validate_for_execution()?;
    Ok(descriptor)
}

impl CompactionCoordinator {
    /// Resolve and capture the current execution policy for this snapshot.
    pub fn prepare_task(
        &self,
        task_id: String,
        input: LoadedSnapshot,
    ) -> Result<CompactionTask> {
        validate_compaction_id(&task_id)?;
        let (_, config) = self.execution.as_ref().ok_or_else(|| {
            execution_error("compaction execution session is not configured")
        })?;
        let policy = resolve_policy(&config.policy, &input.table)?;
        let table_descriptor = if let Some(highest) = input.active_files.first() {
            // Discovery returns files in descending level order.
            if highest.level > policy.max_level {
                return Err(execution_error(format!(
                    "input file {} is at level {}, above the configured maximum {}",
                    highest.file.path, highest.level, policy.max_level
                )));
            }
            Some(build_table_descriptor(&input.table, &input.partition)?)
        } else {
            None
        };
        let policy_revision = policy_revision(&policy)?;
        Ok(CompactionTask {
            task_id,
            input,
            policy,
            policy_revision,
            table_descriptor,
        })
    }

    /// Execute a single snapshot and return its changed files and bucket results.
    pub async fn run_task(
        &self,
        task: &CompactionTask,
    ) -> Result<Option<CompactionExecutionResult>> {
        if task.input.active_files.is_empty() {
            return Ok(None);
        }
        let mut buckets: BTreeMap<i32, Vec<CompactionFile>> = BTreeMap::new();
        for file in &task.input.active_files {
            let bucket_id = file
                .file
                .bucket_id
                .and_then(|id| i32::try_from(id).ok())
                .filter(|id| *id >= 0)
                .ok_or_else(|| {
                    DataFusionError::NotImplemented(
                        "compaction requires valid bucket IDs".into(),
                    )
                })?;
            buckets.entry(bucket_id).or_default().push(file.clone());
        }
        let runs: Vec<_> = buckets
            .iter()
            .filter(|(_, files)| bucket_has_work(files, &task.policy))
            .map(|(&bucket_id, files)| RunTask {
                bucket_id,
                input_files: files.clone(),
            })
            .collect();
        if runs.is_empty() {
            return Ok(None);
        }
        let run_outputs = self.execute_runs(task, runs).await?;
        let mut changed = false;
        for output in &run_outputs {
            let original = buckets
                .get_mut(&output.bucket_id)
                .ok_or_else(|| execution_error("worker returned an unknown bucket"))?;
            changed |= *original != output.result_files;
            *original = output.result_files.clone();
        }
        if !changed {
            return Ok(None);
        }
        // When publishing all buckets together, retain the order within each bucket.
        let mut final_files: Vec<_> = buckets.into_values().flatten().collect();
        final_files.sort_by_key(|file| std::cmp::Reverse(file.level));
        Ok(Some(CompactionExecutionResult {
            final_files,
            run_outputs,
        }))
    }

    async fn execute_runs(
        &self,
        task: &CompactionTask,
        runs: Vec<RunTask>,
    ) -> Result<Vec<RunOutput>> {
        let (session, config) = self.execution.as_ref().ok_or_else(|| {
            execution_error("compaction execution session is not configured")
        })?;
        let descriptor = CompactionDescriptor {
            protocol_version: CODEC_VERSION,
            task_id: task.task_id.clone(),
            table: task.table_descriptor.clone().ok_or_else(|| {
                execution_error("compaction run requires a prepared table descriptor")
            })?,
            partition_desc: task.input.partition.partition_desc.clone(),
            policy: task.policy.clone(),
            reader_options: config.reader_options.clone(),
            writer_options: config.writer_options.clone(),
            runs,
        };
        descriptor.validate_runs()?;
        tokio::time::timeout(
            config.execution_timeout,
            execute_distributed_runs_inner(session, descriptor),
        )
        .await
        .map_err(|_| execution_error("compaction worker execution timed out"))?
    }

    pub fn with_execution(
        mut self,
        session: Arc<SessionContext>,
        config: ExecutionConfig,
    ) -> Self {
        self.execution = Some((session, config));
        self
    }
}
