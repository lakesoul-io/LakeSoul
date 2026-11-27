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
    pub(crate) final_files: Vec<CompactionFile>,
    /// The consumed positions in each run are relative to the original files
    /// for its bucket, in their order in the caller's task snapshot.
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

    fn bucket_files(task: &CompactionTask) -> Result<BTreeMap<i32, Vec<CompactionFile>>> {
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
        Ok(buckets)
    }

    /// Select the buckets that need a compaction run. Empty means there is no
    /// worker work for this snapshot.
    fn plan_runs(&self, task: &CompactionTask) -> Result<Vec<RunTask>> {
        let buckets = Self::bucket_files(task)?;
        Ok(buckets
            .iter()
            .filter(|(_, files)| bucket_has_work(files, &task.policy))
            .map(|(&bucket_id, files)| RunTask {
                bucket_id,
                input_files: files.clone(),
            })
            .collect())
    }

    /// Execute the runs previously selected by [`Self::plan_runs`].
    async fn run_task(
        &self,
        task: &CompactionTask,
        runs: Vec<RunTask>,
    ) -> Result<Option<CompactionExecutionResult>> {
        if runs.is_empty() {
            return Ok(None);
        }
        let mut buckets = Self::bucket_files(task)?;
        let mut planned = BTreeSet::new();
        for run in &runs {
            let files = buckets.get(&run.bucket_id).ok_or_else(|| {
                execution_error("run bucket is not part of the compaction task")
            })?;
            if files != &run.input_files {
                return Err(execution_error(
                    "run input files do not match the compaction task bucket",
                ));
            }
            if !planned.insert(run.bucket_id) {
                return Err(execution_error("duplicate run bucket"));
            }
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

impl CompactionCoordinator {
    /// One runner owns one serial scan/execute/publish loop. A slow scan or
    /// partition may overrun the period; always visit at least one candidate.
    pub async fn run(
        &mut self,
        source: &lakesoul_metadata::MetaDataClient,
        period: Duration,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<()> {
        if period.is_zero() || self.execution.is_none() {
            return Err(execution_error(
                "runner requires an execution session and a positive period",
            ));
        }
        let source = source.primary_only().await;
        tokio::pin!(shutdown);
        let mut cursor = None;
        loop {
            let started = tokio::time::Instant::now();
            let deadline = started.checked_add(period).ok_or_else(|| {
                execution_error("compaction period exceeds clock range")
            })?;
            let result = self
                .tick(&source, deadline, &mut cursor, shutdown.as_mut())
                .await;
            metrics::histogram!("lakesoul_compaction_tick_duration_seconds")
                .record(started.elapsed().as_secs_f64());
            match result {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error) => tracing::warn!(%error, "compaction discovery failed"),
            }
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Returns true when shutdown was observed at a partition boundary.
    async fn tick<F: std::future::Future<Output = ()>>(
        &self,
        source: &lakesoul_metadata::MetaDataClient,
        deadline: tokio::time::Instant,
        cursor: &mut Option<(String, String)>,
        mut shutdown: std::pin::Pin<&mut F>,
    ) -> Result<bool> {
        let started = tokio::time::Instant::now();
        let scan = tokio::select! {
            biased;
            _ = shutdown.as_mut() => return Ok(true),
            scan = self.scan_candidates() => scan,
        };
        metrics::histogram!("lakesoul_compaction_scan_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        let mut candidates = scan.inspect_err(|_| {
            metrics::counter!("lakesoul_compaction_scan_errors_total").increment(1);
        })?;
        metrics::counter!("lakesoul_compaction_candidates_total")
            .increment(candidates.len() as u64);
        rotate_candidates(&mut candidates, cursor.as_ref());
        for partition in candidates {
            if futures::FutureExt::now_or_never(shutdown.as_mut()).is_some() {
                return Ok(true);
            }
            *cursor =
                Some((partition.table_id.clone(), partition.partition_desc.clone()));
            let outcome = match self.compact_candidate(source, &partition).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    tracing::warn!(%error, table=%partition.table_id, partition=%partition.partition_desc, version=partition.version, "compaction partition failed");
                    "error"
                }
            };
            metrics::counter!("lakesoul_compaction_partitions_total", "outcome" => outcome).increment(1);
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        Ok(false)
    }

    async fn compact_candidate(
        &self,
        source: &lakesoul_metadata::MetaDataClient,
        partition: &PartitionInfo,
    ) -> Result<&'static str> {
        use super::compaction_discovery::SnapshotLoad;
        let SnapshotLoad::Ready(input) = self.load_snapshot(partition).await? else {
            return Ok("not_ready");
        };
        let id = uuid::Uuid::new_v4();
        let task = self.prepare_task(id.to_string(), *input)?;
        let runs = self.plan_runs(&task)?;
        if runs.is_empty() {
            return Ok("no_work");
        }
        // The source may already have published a version the mirror has yet
        // to receive. Avoid sending that stale input back to workers.
        let latest = source
            .get_partition_info_by_table_id_and_partition_list(
                &partition.table_id,
                std::slice::from_ref(&partition.partition_desc),
            )
            .await
            .map_err(execution_error)?;
        if !latest
            .iter()
            .any(|p| p.version == partition.version && p.snapshot == partition.snapshot)
        {
            return Ok("stale");
        }
        let Some(output) = self.run_task(&task, runs).await? else {
            return Ok("unchanged");
        };
        let files: Vec<_> = output.final_files.into_iter().map(|f| f.file).collect();
        let retained: BTreeSet<_> = files.iter().map(|f| f.path.as_str()).collect();
        let discarded: Vec<_> = task
            .input
            .active_files
            .iter()
            .map(|f| &f.file)
            .filter(|f| !retained.contains(f.path.as_str()))
            .collect();
        let published = source
            .publish_compacted_snapshot(
                task.input.table(),
                partition,
                id,
                &files,
                &discarded,
            )
            .await;
        match published {
            Ok(true) => {
                tracing::info!(%id, table=%partition.table_id, partition=%partition.partition_desc, read_version=partition.version, "published compaction");
                return Ok("published");
            }
            Err(
                lakesoul_metadata::LakeSoulMetaDataError::InsertCommitOutcomeUnknown(
                    error,
                ),
            ) => {
                if source
                    .compaction_is_published(partition, id)
                    .await
                    .map_err(|lookup| {
                        execution_error(format!(
                            "commit {id}: {error}; reconciliation: {lookup}"
                        ))
                    })?
                {
                    return Ok("published");
                }
                return Err(execution_error(format!(
                    "unconfirmed commit {id}; retained outputs: {error}"
                )));
            }
            _ => {}
        }
        let context = self
            .execution
            .as_ref()
            .expect("prepared task has execution session")
            .0
            .task_ctx();
        for path in output.run_outputs.iter().flat_map(|r| &r.created_paths) {
            if let Err(error) =
                super::compaction::remove_attempt_output(&context, path).await
            {
                tracing::warn!(%id, %path, %error, "failed to remove unpublished output");
            }
        }
        published.map(|_| "not_published").map_err(execution_error)
    }
}

fn rotate_candidates(
    partitions: &mut [PartitionInfo],
    cursor: Option<&(String, String)>,
) {
    fn key(p: &PartitionInfo) -> (&String, &String) {
        (&p.table_id, &p.partition_desc)
    }
    partitions.sort_by(|a, b| key(a).cmp(&key(b)));
    if let Some((table, partition)) = cursor {
        let start = partitions.partition_point(|p| key(p) <= (table, partition));
        partitions.rotate_left(start);
    }
}

#[cfg(test)]
mod scheduler_tests {
    use super::*;
    #[test]
    fn next_scan_resumes_after_last_visited_key_even_if_it_disappeared() {
        let mut rows: Vec<_> = ["c", "a", "d"]
            .iter()
            .map(|name| PartitionInfo {
                table_id: "t".into(),
                partition_desc: name.to_string(),
                ..Default::default()
            })
            .collect();
        for (last, expected) in [("b", ["c", "d", "a"]), ("d", ["a", "c", "d"])] {
            rotate_candidates(&mut rows, Some(&("t".into(), last.into())));
            assert_eq!(
                rows.iter()
                    .map(|p| p.partition_desc.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
}

#[cfg(test)]
#[path = "compaction_scheduler_tests.rs"]
mod compaction_scheduler_tests;
