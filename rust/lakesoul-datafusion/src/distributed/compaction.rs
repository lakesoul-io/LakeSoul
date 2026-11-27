// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Worker-only bucket compaction and its result contract. No metadata client
//! or publication lives here.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    DistributedExec, DistributedLeafExec, LocalWorkerContext, NetworkBoundary,
    NetworkCoalesceExec, Stage, get_distributed_worker_resolver,
};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use futures::StreamExt;
use lakesoul_io::compaction::{CompactToOneOptions, compact_to_one_file};
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::utils::random_str;
use lakesoul_metadata::transfusion::DataFileInfo;
use object_store::ObjectStoreExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::codec::CODEC_VERSION;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPolicy {
    pub level0_file_num_limit: u32,
    pub level_file_num_limit: u32,
    pub l0_read_file_num_limit: u32,
    pub l0_read_bytes_limit: u64,
    pub merge_file_num_limit: u32,
    pub merge_input_bytes_limit: u64,
    pub read_file_max_bytes: u64,
    pub level_base_bytes: u64,
    pub low_level_multiplier: u32,
    pub level_multiplier: u32,
    pub pick_next_level_min_bytes: u64,
    pub pick_next_level_multiplier: u32,
    pub max_level: u32,
    pub rolling_enabled_for_table: bool,
    pub l1_merge_input_target_bytes: u64,
}

/// Binary byte units, matching Spark's `DBUtil.parseMemoryExpression`.
const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Spark-compatible compaction defaults.
///
/// Spark reads `spark.dmetasoul.lakesoul.compaction.*` per job and otherwise
/// uses the values compiled into `LakeSoulSQLConf`; these mirror that fallback.
///
/// Notes:
///
/// * `l0_read_file_num_limit` / `l0_read_bytes_limit` mirror the defaults of
///   the compaction job parameters (`file_num_limit` = `Int.MaxValue`,
///   `file_size_limit` = `Long.MaxValue`). The L0 batch is additionally
///   bounded by `read_file_max_bytes`.
/// * `merge_input_bytes_limit` is 5 GiB because `CompactBucketIO` falls back to
///   the default of `compaction.level.max.file.size` (5 GiB) when
///   `compaction.level.file.merge.size.limit` is unset, despite the latter's
///   documented 600 MiB default.
impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            level0_file_num_limit: 4,
            level_file_num_limit: 10,
            l0_read_file_num_limit: i32::MAX as u32,
            l0_read_bytes_limit: i64::MAX as u64,
            merge_file_num_limit: 5,
            merge_input_bytes_limit: 5 * GIB,
            read_file_max_bytes: 5 * GIB,
            level_base_bytes: GIB,
            low_level_multiplier: 5,
            level_multiplier: 10,
            pick_next_level_min_bytes: 100 * MIB,
            pick_next_level_multiplier: 5,
            max_level: 5,
            rolling_enabled_for_table: true,
            l1_merge_input_target_bytes: 256 * MIB,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionFile {
    pub file: DataFileInfo,
    pub level: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionTableDescriptor {
    pub table_id: String,
    pub table_path: String,
    pub schema: SchemaRef,
    pub primary_keys: Vec<String>,
    pub range_partition_columns: Vec<String>,
    pub partition_values: BTreeMap<String, String>,
    pub hash_partitioning_columns: Vec<String>,
    pub hash_bucket_num: u32,
    /// Column carried in the table schema. A compaction keeps tombstones,
    /// so it never enables the query reader's CDC delete filter.
    pub cdc_column: Option<String>,
    pub physical_format: String,
    /// TODO(compaction): Spark's `mergeOperatorInfo` is a per-compaction-call
    /// parameter and is not persisted in `table_info.properties`, so the mirror
    /// has no source for this map. Keep it empty until operators are persisted
    /// in table metadata or supplied by coordinator configuration.
    pub merge_operators: BTreeMap<String, String>,
}

/// One complete bucket, oldest-to-newest within each level. Never a
/// fragment of a bucket. Output directories and bucket suffixes follow Spark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTask {
    pub bucket_id: i32,
    pub input_files: Vec<CompactionFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionDescriptor {
    pub protocol_version: u32,
    pub task_id: String,
    pub table: CompactionTableDescriptor,
    pub partition_desc: String,
    pub policy: CompactionPolicy,
    pub reader_options: BTreeMap<String, String>,
    pub writer_options: BTreeMap<String, String>,
    pub runs: Vec<RunTask>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RunStats {
    /// Bytes of the original inputs, not a sum across intermediate merges.
    pub input_bytes: u64,
    /// Bytes of the complete final result, including retained files.
    pub output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunOutput {
    pub task_id: String,
    pub bucket_id: i32,
    /// Complete final bucket, including unchanged files. No-op is not empty.
    pub result_files: Vec<CompactionFile>,
    /// Newly created files referenced by the final bucket result.
    pub created_paths: Vec<String>,
    /// Positions of original inputs replaced by a merge or promotion.
    pub consumed_input_positions: Vec<usize>,
    pub stats: RunStats,
}

fn error(message: impl fmt::Display) -> DataFusionError {
    DataFusionError::Execution(message.to_string())
}

pub(super) fn validate_compaction_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(error(
            "compaction IDs must be 1..128 ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}

impl CompactionTableDescriptor {
    pub(super) fn validate_for_execution(&self) -> Result<()> {
        if self.table_id.is_empty()
            || self.primary_keys.is_empty()
            || self.hash_bucket_num == 0
        {
            return Err(error(
                "compaction requires table identity, primary keys and buckets",
            ));
        }
        let url = url::Url::parse(&self.table_path)
            .map_err(|e| error(format!("invalid table URI: {e}")))?;
        if !matches!(url.scheme(), "s3" | "file")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(error(
                "compaction requires a credential-free s3:// or file:// table URI",
            ));
        }
        PhysicalFormat::from_extension(&format!("output.{}", self.physical_format))
            .map_err(|e| error(e.to_string()))?;
        for name in self
            .primary_keys
            .iter()
            .chain(self.hash_partitioning_columns.iter())
            .chain(self.range_partition_columns.iter())
            .chain(self.cdc_column.iter())
        {
            self.schema.field_with_name(name)?;
        }
        if self.partition_values.len() != self.range_partition_columns.len()
            || self
                .range_partition_columns
                .iter()
                .any(|key| !self.partition_values.contains_key(key))
        {
            return Err(error(
                "partition values must match the range partition columns",
            ));
        }
        if !self.hash_partitioning_columns.is_empty()
            && !self
                .primary_keys
                .starts_with(&self.hash_partitioning_columns)
        {
            return Err(error("hash partition columns must be a primary-key prefix"));
        }
        if !self.merge_operators.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "custom compaction merge operators".into(),
            ));
        }
        Ok(())
    }
}

impl CompactionDescriptor {
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != CODEC_VERSION {
            return Err(error("incompatible compaction protocol version"));
        }
        self.policy.validate()?;
        validate_compaction_id(&self.task_id)?;
        self.table.validate_for_execution()?;
        self.validate_runs()
    }

    /// Validate the dispatch payload after the table descriptor has already
    /// been checked. Wire decoders still call [`Self::validate`].
    pub(crate) fn validate_runs(&self) -> Result<()> {
        if self.runs.is_empty() {
            return Err(error("compaction requires at least one run"));
        }
        let described = if self.partition_desc == "-5" {
            BTreeMap::new()
        } else {
            let mut values = BTreeMap::new();
            for part in self.partition_desc.split(',') {
                let (key, value) = part
                    .split_once('=')
                    .ok_or_else(|| error("invalid range partition description"))?;
                if values.insert(key.to_string(), value.to_string()).is_some() {
                    return Err(error("duplicate range partition column"));
                }
            }
            values
        };
        if described != self.table.partition_values {
            return Err(error("partition description and partition values disagree"));
        }
        // Credentials belong to the worker's local configuration, never the
        // serialized descriptor or the coordinator's runtime IO config.
        for (key, value) in self.reader_options.iter().chain(&self.writer_options) {
            let key = key.to_ascii_lowercase();
            if key == lakesoul_io::config::OPTION_KEY_BLOB_COLUMNS
                && !value.trim().is_empty()
            {
                return Err(DataFusionError::NotImplemented(
                    "blob compaction is not supported yet".into(),
                ));
            }
            if key == "merge_operators" || key.starts_with("_lakesoul_merge_col_name_") {
                return Err(DataFusionError::NotImplemented(
                    "custom compaction merge operators".into(),
                ));
            }
            if key.starts_with("fs.")
                || key.starts_with("aws.")
                || [
                    "secret",
                    "password",
                    "credential",
                    "access_key",
                    "accesskey",
                    "session_token",
                ]
                .iter()
                .any(|part| key.contains(part))
            {
                return Err(error(
                    "storage credentials/configuration must be supplied by the worker",
                ));
            }
        }
        let mut buckets = HashSet::new();
        for run in &self.runs {
            if !buckets.insert(run.bucket_id)
                || run.bucket_id < 0
                || run.bucket_id as u32 >= self.table.hash_bucket_num
                || run.input_files.is_empty()
            {
                return Err(error("runs require unique valid nonempty buckets"));
            }
            for input in &run.input_files {
                let input_url = url::Url::parse(&input.file.path)
                    .map_err(|e| error(format!("invalid input URI: {e}")))?;
                if !matches!(input_url.scheme(), "s3" | "file")
                    || !input_url.username().is_empty()
                    || input_url.password().is_some()
                    || input_url.query().is_some()
                    || input_url.fragment().is_some()
                {
                    return Err(error(
                        "input files require credential-free s3:// or file:// URIs",
                    ));
                }
                if input.file.partition_desc != self.partition_desc
                    || input.file.path.is_empty()
                    || input.file.file_op != "add"
                    || input.file.size < 0
                    || input.level > self.policy.max_level
                    || input.file.bucket_id != Some(run.bucket_id as isize)
                {
                    return Err(error("invalid active input file"));
                }
            }
        }
        Ok(())
    }

    /// Same layout as CompactBucketIO.initializeWriter: L0 at the table
    /// root, higher levels under compactdirN, followed by the source range
    /// descriptor with its separators changed from commas to slashes.
    pub fn output_directory(&self, level: u32) -> Result<String> {
        if level > self.policy.max_level {
            return Err(error("output level exceeds the configured maximum"));
        }
        let mut directory = self.table.table_path.trim_end_matches('/').to_string();
        if level > 0 {
            directory.push_str(&format!("/compactdir{level}"));
        }
        if self.partition_desc != "-5" {
            let sub_path = self.partition_desc.replace(',', "/");
            if sub_path.split('/').any(|part| part == "." || part == "..") {
                return Err(error("invalid partition path"));
            }
            directory.push('/');
            directory.push_str(&sub_path);
        }
        Ok(directory)
    }

    // Bind the opaque filename token to this dispatch. IDs cannot contain NUL.
    fn output_owner(&self, run: &RunTask) -> String {
        let mut hash = Sha256::new();
        hash.update(self.task_id.as_bytes());
        hash.update([0]);
        hash.update(run.bucket_id.to_le_bytes());
        let digest = hash.finalize();
        let mut prefix = [0u8; 16];
        prefix.copy_from_slice(&digest[..16]);
        uuid::Uuid::from_bytes(prefix).simple().to_string()
    }

    /// Keep the range directory and bucket suffix readable by existing readers.
    pub fn new_output_path(&self, run: &RunTask, level: u32) -> Result<String> {
        let format = PhysicalFormat::from_extension(&format!(
            "output.{}",
            self.table.physical_format
        ))
        .map_err(|e| error(e.to_string()))?;
        Ok(format!(
            "{}/part-{}{}_{:0>4}.{}",
            self.output_directory(level)?,
            self.output_owner(run),
            random_str(16),
            run.bucket_id,
            format.extension()
        ))
    }

    fn valid_created_path(&self, path: &str, run: &RunTask, owner: &str) -> Result<bool> {
        let bucket_id = run.bucket_id;
        let Some((directory, name)) = path.rsplit_once('/') else {
            return Ok(false);
        };
        let format = PhysicalFormat::from_extension(&format!(
            "output.{}",
            self.table.physical_format
        ))
        .map_err(|e| error(e.to_string()))?;
        let suffix = format!("_{bucket_id:0>4}.{}", format.extension());
        let Some(random) = name
            .strip_prefix("part-")
            .and_then(|token| token.strip_prefix(owner))
            .and_then(|name| name.strip_suffix(&suffix))
        else {
            return Ok(false);
        };
        if random.len() != 16 || !random.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Ok(false);
        }
        if directory == self.output_directory(0)? {
            return Ok(true);
        }
        let prefix =
            format!("{}/compactdir", self.table.table_path.trim_end_matches('/'));
        let level = directory
            .strip_prefix(&prefix)
            .and_then(|tail| tail.split('/').next())
            .and_then(|level| level.parse::<u32>().ok());
        match level {
            Some(level) if level > 0 && level <= self.policy.max_level => {
                Ok(directory == self.output_directory(level)?)
            }
            _ => Ok(false),
        }
    }
}

pub fn run_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("task_id", DataType::Utf8, false),
        Field::new("bucket_id", DataType::Int32, false),
        Field::new("result_files", DataType::Utf8, false),
        Field::new("created_paths", DataType::Utf8, false),
        Field::new("stats", DataType::Utf8, false),
        Field::new("consumed_input_positions", DataType::Utf8, false),
    ]))
}

impl RunOutput {
    pub fn to_batch(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            run_output_schema(),
            vec![
                Arc::new(StringArray::from(vec![self.task_id.as_str()])),
                Arc::new(Int32Array::from(vec![self.bucket_id])),
                Arc::new(StringArray::from(vec![
                    serde_json::to_string(&self.result_files)
                        .map_err(|e| error(e.to_string()))?,
                ])),
                Arc::new(StringArray::from(vec![
                    serde_json::to_string(&self.created_paths)
                        .map_err(|e| error(e.to_string()))?,
                ])),
                Arc::new(StringArray::from(vec![
                    serde_json::to_string(&self.stats)
                        .map_err(|e| error(e.to_string()))?,
                ])),
                Arc::new(StringArray::from(vec![
                    serde_json::to_string(&self.consumed_input_positions)
                        .map_err(error)?,
                ])),
            ],
        )?)
    }

    pub fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>> {
        if batch.schema().as_ref() != run_output_schema().as_ref()
            || batch
                .columns()
                .iter()
                .any(|column| column.null_count() != 0)
        {
            return Err(error(
                "invalid compaction result Arrow schema or NULL field",
            ));
        }
        let text = |column: usize, row: usize| -> Result<&str> {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|a| a.value(row))
                .ok_or_else(|| error("expected Utf8 result column"))
        };
        let buckets = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| error("expected Int32 bucket"))?;
        (0..batch.num_rows())
            .map(|row| {
                Ok(Self {
                    task_id: text(0, row)?.into(),
                    bucket_id: buckets.value(row),
                    result_files: serde_json::from_str(text(2, row)?)
                        .map_err(|e| error(e.to_string()))?,
                    created_paths: serde_json::from_str(text(3, row)?)
                        .map_err(|e| error(e.to_string()))?,
                    stats: serde_json::from_str(text(4, row)?)
                        .map_err(|e| error(e.to_string()))?,
                    consumed_input_positions: serde_json::from_str(text(5, row)?)
                        .map_err(error)?,
                })
            })
            .collect()
    }
}

/// Coordinator-side acceptance of complete, ordered bucket results.
pub struct RunOutputCollector {
    descriptor: CompactionDescriptor,
    accepted: HashMap<i32, RunOutput>,
}

impl RunOutputCollector {
    pub fn new(descriptor: CompactionDescriptor) -> Result<Self> {
        descriptor.validate()?;
        Ok(Self::new_inner(descriptor))
    }

    /// Caller must pass a descriptor that already passed `validate()`.
    pub(crate) fn new_inner(descriptor: CompactionDescriptor) -> Self {
        Self {
            descriptor,
            accepted: HashMap::new(),
        }
    }

    pub fn accept(&mut self, output: RunOutput) -> Result<bool> {
        self.accept_tracking(output, |_| {})
    }

    fn accept_tracking(
        &mut self,
        output: RunOutput,
        mut track: impl FnMut(&str),
    ) -> Result<bool> {
        if output.task_id != self.descriptor.task_id {
            return Ok(false);
        }
        let run = self
            .descriptor
            .runs
            .iter()
            .find(|run| run.bucket_id == output.bucket_id)
            .ok_or_else(|| error("unknown bucket in current compaction task"))?;

        let owner = self.descriptor.output_owner(run);
        let mut created = HashSet::new();
        let mut invalid_manifest = false;
        for path in &output.created_paths {
            if !created.insert(path.as_str())
                || !self.descriptor.valid_created_path(path, run, &owner)?
                || self
                    .descriptor
                    .runs
                    .iter()
                    .flat_map(|r| &r.input_files)
                    .any(|file| file.file.path == *path)
            {
                invalid_manifest = true;
                continue;
            }
            // Register independently verified paths even if a sibling path or
            // the bucket result is invalid. Never delete an input or a peer's file.
            track(path);
        }
        if invalid_manifest {
            return Err(error("invalid or duplicate created path for this bucket"));
        }
        if self.accepted.contains_key(&output.bucket_id) {
            return Err(error("duplicate bucket result"));
        }
        let mut consumed = HashSet::new();
        for &position in &output.consumed_input_positions {
            if position >= run.input_files.len() || !consumed.insert(position) {
                return Err(error("invalid or duplicate consumed input position"));
            }
        }
        if !consumed.is_empty() && created.is_empty() {
            return Err(error("consumed inputs require a replacement output"));
        }
        let mut retained = run
            .input_files
            .iter()
            .enumerate()
            .filter(|(position, _)| !consumed.contains(position))
            .map(|(_, file)| file);
        let mut new_paths = HashSet::new();
        let mut previous_level = u32::MAX;
        for file in &output.result_files {
            if file.file.partition_desc != self.descriptor.partition_desc
                || file.file.file_op != "add"
                || file.file.size < 0
                || file.file.bucket_id != Some(run.bucket_id as isize)
                || file.level > previous_level
            {
                return Err(error("invalid final bucket file list or level order"));
            }
            previous_level = file.level;
            if !created.contains(file.file.path.as_str()) {
                if retained.next() != Some(file) {
                    return Err(error("unexpected retained file or changed input order"));
                }
            } else {
                if !new_paths.insert(file.file.path.as_str()) {
                    return Err(error("duplicate new result file"));
                }
                let directory = file
                    .file
                    .path
                    .rsplit_once('/')
                    .map(|(directory, _)| directory);
                if directory
                    != Some(self.descriptor.output_directory(file.level)?.as_str())
                {
                    return Err(error(
                        "new result file directory does not match its level",
                    ));
                }
            }
        }
        if retained.next().is_some() {
            return Err(error(
                "bucket result omitted an input that was not consumed",
            ));
        }
        if new_paths.len() != created.len() {
            return Err(error(
                "created paths must match new files in the bucket result",
            ));
        }
        // Empty output must not silently stand for a no-op.
        if output.result_files.is_empty() {
            return Err(error(
                "empty bucket result requires explicit empty-result support",
            ));
        }
        self.accepted.insert(output.bucket_id, output);
        Ok(true)
    }

    pub fn finish(mut self) -> Result<Vec<RunOutput>> {
        self.descriptor
            .runs
            .iter()
            .map(|run| {
                self.accepted.remove(&run.bucket_id).ok_or_else(|| {
                    error(format!(
                        "missing successful output for bucket {}",
                        run.bucket_id
                    ))
                })
            })
            .collect()
    }
}

/// Worker-local storage configuration. The wrapper prevents a descriptor from
/// replacing the worker's credential settings.
#[derive(Debug, Clone)]
pub(crate) struct CompactionWorkerConfig(pub LakeSoulIOConfig);

#[derive(Debug)]
pub struct CompactionExec {
    descriptor: Arc<CompactionDescriptor>,
    properties: Arc<PlanProperties>,
}

impl CompactionExec {
    pub fn try_new(descriptor: CompactionDescriptor) -> Result<Self> {
        descriptor.validate()?;
        Self::try_new_inner(descriptor)
    }

    /// Caller must pass a descriptor that already passed `validate()`.
    pub(crate) fn try_new_inner(descriptor: CompactionDescriptor) -> Result<Self> {
        let schema = run_output_schema();
        let partitions = descriptor.runs.len();
        Ok(Self {
            descriptor: Arc::new(descriptor),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        })
    }
    pub fn descriptor(&self) -> &CompactionDescriptor {
        &self.descriptor
    }
}

impl DisplayAs for CompactionExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CompactionExec task={} buckets={}",
            self.descriptor.task_id,
            self.descriptor.runs.len()
        )
    }
}

impl ExecutionPlan for CompactionExec {
    fn name(&self) -> &str {
        "CompactionExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(
            &Arc<dyn PhysicalExpr>,
        )
            -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(error("CompactionExec has no children"));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if context
            .session_config()
            .get_extension::<LocalWorkerContext>()
            .is_none()
        {
            return Err(error("CompactionExec must run on a distributed worker"));
        }
        let run = self
            .descriptor
            .runs
            .get(partition)
            .ok_or_else(|| error("compaction partition out of bounds"))?
            .clone();
        let descriptor = Arc::clone(&self.descriptor);
        let stream = futures::stream::once(async move {
            execute_run_inner(&descriptor, &run, context)
                .await?
                .to_batch()
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            run_output_schema(),
            stream,
        )))
    }
}

/// Initial supported case: a whole bucket is one bounded legacy L0 group,
/// producing a sub-promotion-threshold L0 result. Full rolling and multilevel
/// execution will extend this function without changing the wire contract.
impl CompactionPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.max_level == 0
            || self.level0_file_num_limit == 0
            || self.l0_read_file_num_limit == 0
            || self.l0_read_bytes_limit == 0
            || self.level_file_num_limit == 0
            || self.merge_file_num_limit == 0
            || self.merge_input_bytes_limit == 0
            || self.read_file_max_bytes == 0
            || self.level_base_bytes == 0
            || self.low_level_multiplier == 0
            || self.level_multiplier == 0
            || self.pick_next_level_min_bytes == 0
            || self.pick_next_level_multiplier == 0
            || (self.rolling_enabled_for_table && self.l1_merge_input_target_bytes == 0)
        {
            return Err(error("invalid compaction policy thresholds"));
        }
        Ok(())
    }

    fn promotion_size(&self, level: u32) -> u64 {
        if level == 1 && self.rolling_enabled_for_table {
            return self.l1_merge_input_target_bytes;
        }
        self.pick_next_level_min_bytes.saturating_mul(
            u64::from(self.pick_next_level_multiplier)
                .saturating_pow(level.saturating_sub(1)),
        )
    }

    fn level_capacity(&self, level: u32) -> u64 {
        self.level_base_bytes
            .saturating_mul(
                u64::from(self.low_level_multiplier).saturating_pow(level.min(2) - 1),
            )
            .saturating_mul(
                u64::from(self.level_multiplier).saturating_pow(level.saturating_sub(2)),
            )
    }

    fn needs_level_merge(&self, level: u32, file_count: usize, input_bytes: u64) -> bool {
        file_count != 0
            && (file_count >= self.level_file_num_limit as usize
                || input_bytes >= self.level_capacity(level))
    }
}

fn bytes(files: &[CompactionFile]) -> u64 {
    files
        .iter()
        .fold(0, |total, file| total.saturating_add(file.file.size as u64))
}

pub(crate) fn bucket_has_work(
    files: &[CompactionFile],
    policy: &CompactionPolicy,
) -> bool {
    for level in 0..=policy.max_level {
        let level_files: Vec<_> = files
            .iter()
            .filter(|file| file.level == level)
            .cloned()
            .collect();
        if level == 0 {
            if level_files.len() >= policy.level0_file_num_limit as usize {
                return true;
            }
            continue;
        }
        if level < policy.max_level
            && level_files
                .first()
                .is_some_and(|file| file.file.size as u64 >= policy.promotion_size(level))
        {
            return true;
        }
        if level == 1 && policy.rolling_enabled_for_table {
            if level_files.windows(2).any(|pair| {
                pair.iter().all(|file| {
                    (file.file.size as u64) < policy.l1_merge_input_target_bytes
                })
            }) {
                return true;
            }
        } else if policy.needs_level_merge(level, level_files.len(), bytes(&level_files))
        {
            return true;
        }
    }
    false
}

pub async fn execute_run(
    descriptor: &CompactionDescriptor,
    run: &RunTask,
    context: Arc<TaskContext>,
) -> Result<RunOutput> {
    descriptor.validate()?;
    execute_run_inner(descriptor, run, context).await
}

/// Caller must pass a descriptor that already passed `validate()`.
pub(crate) async fn execute_run_inner(
    descriptor: &CompactionDescriptor,
    run: &RunTask,
    context: Arc<TaskContext>,
) -> Result<RunOutput> {
    if !descriptor.runs.iter().any(|expected| expected == run) {
        return Err(error("run does not belong to descriptor"));
    }
    let local = context
        .session_config()
        .get_extension::<CompactionWorkerConfig>()
        .ok_or_else(|| error("missing worker-local compaction IO configuration"))?;
    let options = CompactToOneOptions::new(descriptor.table.primary_keys.clone())
        .with_schema(descriptor.table.schema.clone())
        .with_task_context(context.clone())
        .with_reader_config(run_io_config(
            descriptor,
            run.bucket_id,
            &descriptor.reader_options,
            &local.0,
        ))
        .with_writer_config(run_io_config(
            descriptor,
            run.bucket_id,
            &descriptor.writer_options,
            &local.0,
        ));
    // Promotion-only runs also need the bucket store, before any reader is opened.
    let mut storage = local.0.clone();
    lakesoul_io::object_store::register_object_store(
        &descriptor.table.table_path,
        &mut storage,
        &context.runtime_env(),
    )
    .map_err(error)?;
    let mut worker = BucketCompactor {
        descriptor,
        run,
        outputs: Arc::new(AttemptOutputs::new(context)),
        options,
        consumed_input_positions: Vec::new(),
    };
    let policy = &descriptor.policy;
    let mut levels = vec![Vec::new(); policy.max_level as usize + 1];
    for (position, file) in run.input_files.iter().enumerate() {
        let level = levels.get_mut(file.level as usize).ok_or_else(|| {
            error("input compaction level exceeds the configured maximum")
        })?;
        level.push(BucketFile {
            entry: file.clone(),
            input_position: Some(position),
        });
    }
    if levels[0].len() >= policy.level0_file_num_limit as usize {
        let mut l0 = std::mem::take(&mut levels[0]);
        let budget = policy.l0_read_bytes_limit.min(policy.read_file_max_bytes);
        let limit = policy.l0_read_file_num_limit as usize;
        if policy.rolling_enabled_for_table {
            loop {
                let count = l0.len();
                l0 = worker
                    .roll(l0.into(), 0, policy.promotion_size(0), limit, budget)
                    .await?;
                if l0.len() <= 1 || l0.len() >= count {
                    break;
                }
            }
        } else {
            let mut pending = l0.into_iter();
            l0 = Vec::new();
            loop {
                let mut batch = Vec::new();
                let mut total = 0u64;
                for file in pending.by_ref().take(limit) {
                    total = total.saturating_add(file.file.size as u64);
                    batch.push(file);
                    if total > budget {
                        break;
                    }
                }
                if batch.is_empty() {
                    break;
                }
                l0.push(worker.merge(batch, 0).await?);
            }
        }
        levels[0] = l0;
        worker.promote_prefix(&mut levels, 0).await?;
    }
    for level in 1..=policy.max_level {
        let index = level as usize;
        if level == 1 && policy.rolling_enabled_for_table {
            levels[index] = worker
                .roll(
                    std::mem::take(&mut levels[index]).into(),
                    level,
                    policy.l1_merge_input_target_bytes,
                    usize::MAX,
                    u64::MAX,
                )
                .await?;
        } else if policy.needs_level_merge(
            level,
            levels[index].len(),
            levels[index]
                .iter()
                .map(|file| file.file.size as u64)
                .fold(0, u64::saturating_add),
        ) {
            levels[index] = worker
                .merge_level(std::mem::take(&mut levels[index]), level)
                .await?;
        }
        if level < policy.max_level {
            worker.promote_prefix(&mut levels, level).await?;
        }
    }
    let result_files: Vec<_> = levels
        .into_iter()
        .rev()
        .flatten()
        .map(|file| file.entry)
        .collect();
    let created_paths = worker.outputs.finish(&result_files).await?;
    Ok(RunOutput {
        task_id: descriptor.task_id.clone(),
        bucket_id: run.bucket_id,
        stats: RunStats {
            input_bytes: bytes(&run.input_files),
            output_bytes: bytes(&result_files),
        },
        result_files,
        created_paths,
        consumed_input_positions: worker.consumed_input_positions,
    })
}

#[derive(Clone)]
struct BucketFile {
    entry: CompactionFile,
    input_position: Option<usize>,
}

impl Deref for BucketFile {
    type Target = CompactionFile;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl DerefMut for BucketFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entry
    }
}

/// Tracks only output paths reserved by this attempt. A writer or copy that
/// outlives its caller keeps an Arc until it has finished touching storage.
struct AttemptOutputs {
    context: Arc<TaskContext>,
    paths: Mutex<HashSet<String>>,
}

impl AttemptOutputs {
    fn new(context: Arc<TaskContext>) -> Self {
        Self {
            context,
            paths: Mutex::new(HashSet::new()),
        }
    }

    fn record(&self, path: &str) -> Result<()> {
        if !self.paths.lock().unwrap().insert(path.to_owned()) {
            return Err(error("duplicate output reservation"));
        }
        Ok(())
    }

    fn forget(&self, path: &str) {
        self.paths.lock().unwrap().remove(path);
    }

    async fn finish(&self, files: &[CompactionFile]) -> Result<Vec<String>> {
        let tracked = self.paths.lock().unwrap().clone();
        let keep: HashSet<_> = files.iter().map(|file| file.file.path.as_str()).collect();
        for path in tracked.iter().filter(|path| !keep.contains(path.as_str())) {
            remove_attempt_output(&self.context, path).await?;
            self.forget(path);
        }
        let created: Vec<_> = files
            .iter()
            .map(|file| &file.file.path)
            .filter(|path| tracked.contains(path.as_str()))
            .cloned()
            .collect();
        for path in &created {
            self.forget(path);
        }
        Ok(created)
    }
}

impl Drop for AttemptOutputs {
    fn drop(&mut self) {
        let paths = std::mem::take(self.paths.get_mut().unwrap());
        if paths.is_empty() {
            return;
        }
        let context = Arc::clone(&self.context);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                for path in paths {
                    if let Err(err) = remove_attempt_output(&context, &path).await {
                        tracing::warn!(%path, %err, "failed to remove unpublished compaction output");
                    }
                }
            });
        }
    }
}

pub(super) async fn remove_attempt_output(
    context: &TaskContext,
    path: &str,
) -> Result<()> {
    let url = datafusion::datasource::listing::ListingTableUrl::parse(path)?;
    let store = context.runtime_env().object_store(url.object_store())?;
    match store.delete(url.prefix()).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(err) => Err(error(err)),
    }
}

struct BucketCompactor<'a> {
    descriptor: &'a CompactionDescriptor,
    run: &'a RunTask,
    outputs: Arc<AttemptOutputs>,
    options: CompactToOneOptions,
    consumed_input_positions: Vec<usize>,
}

impl BucketCompactor<'_> {
    async fn merge(&mut self, files: Vec<BucketFile>, level: u32) -> Result<BucketFile> {
        let path = self.descriptor.new_output_path(self.run, level)?;
        tracing::info!(task_id=%self.descriptor.task_id, bucket_id=%self.run.bucket_id, output=%path, "compaction writing output");
        self.consumed_input_positions
            .extend(files.iter().filter_map(|file| file.input_position));
        self.outputs.record(&path)?;
        let inputs = files
            .into_iter()
            .map(|file| file.entry.file.path)
            .collect::<Vec<_>>();
        let handle = tokio::runtime::Handle::current();
        let io_path = path.clone();
        let options = self.options.clone();
        let outputs = Arc::clone(&self.outputs);
        let span = tracing::Span::current();
        // Keep ownership of attempted output paths until the writer exits,
        // including when the remote consumer cancels its stream.
        let result = tokio::task::spawn_blocking(move || {
            let _outputs = outputs;
            let _entered = span.enter();
            handle
                .block_on(compact_to_one_file(&inputs, &io_path, &options))
                .map_err(error)
        })
        .await
        .map_err(|err| error(format!("compaction IO task failed: {err}")))??;
        Ok(BucketFile {
            input_position: None,
            entry: CompactionFile {
                level,
                file: DataFileInfo {
                    partition_desc: self.descriptor.partition_desc.clone(),
                    path,
                    file_op: "add".into(),
                    size: i64::try_from(result.output_bytes).map_err(error)?,
                    bucket_id: Some(self.run.bucket_id as isize),
                    modification_time: now_millis()?,
                    file_exist_cols: result.file_exist_cols.join(","),
                },
            },
        })
    }

    async fn roll(
        &mut self,
        mut pending: VecDeque<BucketFile>,
        level: u32,
        target: u64,
        limit: usize,
        budget: u64,
    ) -> Result<Vec<BucketFile>> {
        let mut output = Vec::new();
        while let Some(first) = pending.pop_front() {
            let mut total = first.file.size as u64;
            let mut batch = vec![first];
            while total < target && batch.len() < limit {
                let Some(next) = pending.front() else { break };
                let size = next.file.size as u64;
                if size >= target || total >= budget || size > budget - total {
                    break;
                }
                let Some(next) = pending.pop_front() else {
                    break;
                };
                batch.push(next);
                total += size;
            }
            if batch.len() == 1 {
                output.extend(batch);
                continue;
            }
            let merged = self.merge(batch, level).await?;
            if (merged.file.size as u64) < target && !pending.is_empty() {
                pending.push_front(merged);
            } else {
                output.push(merged);
            }
        }
        Ok(output)
    }

    async fn merge_level(
        &mut self,
        files: Vec<BucketFile>,
        level: u32,
    ) -> Result<Vec<BucketFile>> {
        let policy = &self.descriptor.policy;
        let mut output = Vec::new();
        let mut batch = Vec::new();
        let mut input_bytes = 0u64;
        for file in files {
            let size = file.file.size as u64;
            if batch.is_empty() && size >= policy.read_file_max_bytes {
                output.push(file);
                continue;
            }
            batch.push(file);
            input_bytes = input_bytes.saturating_add(size);
            if batch.len() >= policy.merge_file_num_limit as usize
                || input_bytes >= policy.merge_input_bytes_limit
            {
                let group = std::mem::take(&mut batch);
                if group.len() == 1 {
                    output.extend(group);
                } else {
                    output.push(self.merge(group, level).await?);
                }
                input_bytes = 0;
            }
        }
        output.extend(batch);
        Ok(output)
    }

    async fn promote_prefix(
        &mut self,
        levels: &mut [Vec<BucketFile>],
        level: u32,
    ) -> Result<()> {
        use datafusion::datasource::listing::ListingTableUrl;
        let index = level as usize;
        let target = self.descriptor.policy.promotion_size(level);
        let prefix = levels[index]
            .iter()
            .take_while(|file| file.file.size as u64 >= target)
            .count();
        let promoted: Vec<_> = levels[index].drain(..prefix).collect();
        for mut file in promoted {
            let path = self.descriptor.new_output_path(self.run, level + 1)?;
            if PhysicalFormat::from_extension(&file.file.path).map_err(error)?
                != PhysicalFormat::from_extension(&path).map_err(error)?
            {
                return Err(DataFusionError::NotImplemented(
                    "copy promotion across physical formats".into(),
                ));
            }
            let source = ListingTableUrl::parse(&file.file.path)?;
            let destination = ListingTableUrl::parse(&path)?;
            if source.object_store() != destination.object_store() {
                return Err(error("compaction promotion requires a single object store"));
            }
            self.outputs.record(&path)?;
            let outputs = Arc::clone(&self.outputs);
            let copy_path = path.clone();
            tracing::info!(task_id=%self.descriptor.task_id, bucket_id=%self.run.bucket_id, output=%path, "compaction copying output");
            tokio::spawn(async move {
                let store = outputs
                    .context
                    .runtime_env()
                    .object_store(source.object_store())?;
                match store
                    .copy_opts(
                        source.prefix(),
                        destination.prefix(),
                        object_store::CopyOptions {
                            mode: object_store::CopyMode::Create,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(err @ object_store::Error::AlreadyExists { .. }) => {
                        outputs.forget(&copy_path);
                        Err(error(err))
                    }
                    Err(err) => Err(error(err)),
                }
            })
            .await
            .map_err(error)??;
            self.consumed_input_positions
                .extend(file.input_position.take());
            file.level += 1;
            file.file.path = path;
            file.file.modification_time = now_millis()?;
            levels[index + 1].push(file);
        }
        Ok(())
    }
}

fn now_millis() -> Result<i64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(error)?;
    i64::try_from(now.as_millis()).map_err(error)
}

fn run_io_config(
    descriptor: &CompactionDescriptor,
    bucket_id: i32,
    settings: &BTreeMap<String, String>,
    local: &LakeSoulIOConfig,
) -> LakeSoulIOConfig {
    let mut builder = LakeSoulIOConfigBuilder::new_with_object_store_options(
        local.object_store_options().clone().into_inner(),
    )
    .with_schema(descriptor.table.schema.clone())
    .with_primary_keys(descriptor.table.primary_keys.clone())
    .with_hash_partitioning_columns(descriptor.table.hash_partitioning_columns.clone())
    .with_range_partitions(descriptor.table.range_partition_columns.clone())
    .with_hash_bucket_num(descriptor.table.hash_bucket_num.to_string());
    for (key, value) in &descriptor.table.partition_values {
        builder = builder.with_default_column_value(key, value);
    }
    for (key, value) in settings {
        // The native blob parser expects JSON whenever this option is present.
        if key.eq_ignore_ascii_case(lakesoul_io::config::OPTION_KEY_BLOB_COLUMNS)
            && value.trim().is_empty()
        {
            continue;
        }
        builder = builder.with_option(key, value);
    }
    builder
        .with_option(lakesoul_io::config::OPTION_KEY_HASH_BUCKET_ID, bucket_id)
        .build()
}

#[derive(Debug, Default)]
pub struct CompactionCodec;

impl PhysicalExtensionCodec for CompactionCodec {
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let exec = node
            .downcast_ref::<CompactionExec>()
            .ok_or_else(|| error("CompactionCodec cannot encode this node"))?;
        let payload =
            serde_json::to_vec(exec.descriptor()).map_err(|e| error(e.to_string()))?;
        buf.extend_from_slice(&payload);
        Ok(())
    }
    fn try_decode(
        &self,
        buf: &[u8],
        children: &[Arc<dyn ExecutionPlan>],
        _: &TaskContext,
        _: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(error("compaction descriptor must not have child plans"));
        }
        let descriptor = serde_json::from_slice(buf).map_err(|e| error(e.to_string()))?;
        Ok(Arc::new(CompactionExec::try_new(descriptor)?))
    }
}

/// Explicit worker stage, even for one run/one worker. We do not pass this
/// side-effecting leaf through cost-based SQL planning, which may elide a
/// one-task boundary and execute it on the coordinator.
pub fn distributed_compaction_plan(
    descriptor: CompactionDescriptor,
) -> Result<Arc<dyn ExecutionPlan>> {
    descriptor.validate()?;
    distributed_compaction_plan_inner(descriptor)
}

/// Caller must pass a descriptor that already passed `validate()`.
pub(crate) fn distributed_compaction_plan_inner(
    descriptor: CompactionDescriptor,
) -> Result<Arc<dyn ExecutionPlan>> {
    let original: Arc<dyn ExecutionPlan> =
        Arc::new(CompactionExec::try_new_inner(descriptor.clone())?);
    let variants = descriptor
        .runs
        .iter()
        .map(|run| {
            let mut slice = descriptor.clone();
            slice.runs = vec![run.clone()];
            CompactionExec::try_new_inner(slice)
                .map(|exec| Arc::new(exec) as Arc<dyn ExecutionPlan>)
        })
        .collect::<Result<Vec<_>>>()?;
    let leaf: Arc<dyn ExecutionPlan> =
        Arc::new(DistributedLeafExec::try_new(original, variants)?);
    let boundary = NetworkCoalesceExec::try_new(leaf, descriptor.runs.len(), 1)?;
    let Stage::Local(mut stage) = boundary.input_stage().clone() else {
        return Err(error("expected unassigned local stage"));
    };
    stage.query_id = uuid::Uuid::new_v4();
    stage.num = 1;
    let boundary: Arc<dyn ExecutionPlan> =
        boundary.with_input_stage(Stage::Local(stage))?;
    let root: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
            boundary,
        ),
    );
    Ok(Arc::new(DistributedExec::new(root)))
}

pub async fn execute_distributed_runs(
    session: &SessionContext,
    descriptor: CompactionDescriptor,
) -> Result<Vec<RunOutput>> {
    descriptor.validate()?;
    execute_distributed_runs_inner(session, descriptor).await
}

/// Caller must pass a descriptor whose table and runs already passed validation.
pub(crate) async fn execute_distributed_runs_inner(
    session: &SessionContext,
    descriptor: CompactionDescriptor,
) -> Result<Vec<RunOutput>> {
    if datafusion_distributed::DistributedConfig::from_config_options(
        session.state().config().options(),
    )?
    .dynamic_task_count
    {
        return Err(error("compaction requires static task assignment"));
    }
    let resolver = get_distributed_worker_resolver(session.state().config())?;
    if resolver.get_urls()?.is_empty() {
        return Err(error("compaction requires at least one remote worker"));
    }
    let plan = distributed_compaction_plan_inner(descriptor.clone())?;
    let context = session.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, context.clone())?;
    let outputs_to_cleanup = AttemptOutputs::new(context);
    let mut collector = RunOutputCollector::new_inner(descriptor);
    while let Some(batch) = stream.next().await {
        let mut first_error = None;
        for output in RunOutput::from_batch(&batch?)? {
            let accepted = collector.accept_tracking(output, |path| {
                outputs_to_cleanup
                    .paths
                    .lock()
                    .unwrap()
                    .insert(path.to_owned());
            });
            match accepted {
                Ok(true) => {}
                Ok(false) => {
                    first_error.get_or_insert_with(|| {
                        error("compaction result does not belong to this task")
                    });
                }
                Err(err) => {
                    first_error.get_or_insert(err);
                }
            }
        }
        if let Some(err) = first_error {
            return Err(err);
        }
    }
    let outputs = collector.finish()?;
    for path in outputs.iter().flat_map(|output| &output.created_paths) {
        outputs_to_cleanup.forget(path);
    }
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow::array::Int64Array;
    use datafusion::prelude::SessionConfig;
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

    use super::*;

    fn test_policy() -> CompactionPolicy {
        CompactionPolicy {
            level0_file_num_limit: 4,
            level_file_num_limit: 10,
            l0_read_file_num_limit: 20,
            l0_read_bytes_limit: 1_000,
            merge_file_num_limit: 5,
            merge_input_bytes_limit: 600,
            read_file_max_bytes: 1_000,
            level_base_bytes: 1_000,
            low_level_multiplier: 5,
            level_multiplier: 10,
            pick_next_level_min_bytes: 100,
            pick_next_level_multiplier: 5,
            max_level: 3,
            rolling_enabled_for_table: true,
            l1_merge_input_target_bytes: 256,
        }
    }

    fn test_worker_context() -> SessionContext {
        let mut session = SessionConfig::new();
        session.set_extension(Arc::new(CompactionWorkerConfig(
            LakeSoulIOConfigBuilder::new().build(),
        )));
        SessionContext::new_with_config(session)
    }

    fn fixture_descriptor(
        table_path: &str,
        partition_desc: &str,
        physical_schema: &SchemaRef,
        policy: CompactionPolicy,
        run: RunTask,
    ) -> CompactionDescriptor {
        let mut fields = physical_schema
            .fields()
            .iter()
            .map(|field| (**field).clone())
            .collect::<Vec<_>>();
        fields.push(Field::new("day", DataType::Utf8, false));
        let mut partition_values = BTreeMap::new();
        if partition_desc != "-5" {
            for pair in partition_desc.split(',') {
                let (key, value) = pair.split_once('=').expect("fixture partition");
                partition_values.insert(key.to_owned(), value.to_owned());
            }
        }
        CompactionDescriptor {
            protocol_version: CODEC_VERSION,
            task_id: "worker_test".into(),
            table: CompactionTableDescriptor {
                table_id: "orders".into(),
                table_path: table_path.into(),
                schema: Arc::new(Schema::new(fields)),
                primary_keys: vec!["id".into()],
                range_partition_columns: if partition_desc == "-5" {
                    vec![]
                } else {
                    vec!["day".into()]
                },
                partition_values,
                hash_partitioning_columns: vec![],
                hash_bucket_num: 1,
                cdc_column: None,
                physical_format: "parquet".into(),
                merge_operators: BTreeMap::new(),
            },
            partition_desc: partition_desc.into(),
            policy,
            reader_options: BTreeMap::new(),
            writer_options: BTreeMap::new(),
            runs: vec![run],
        }
    }

    fn write_parquet_fixture(
        warehouse: &std::path::Path,
        root: &url::Url,
        relative: &str,
        schema: SchemaRef,
        ids: Vec<i64>,
        values: Vec<i64>,
        partition_desc: &str,
        level: u32,
        metadata_size: Option<i64>,
    ) -> std::result::Result<CompactionFile, Box<dyn std::error::Error>> {
        let local = warehouse.join(relative);
        if let Some(parent) = local.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut writer =
            ArrowWriter::try_new(File::create(&local)?, schema.clone(), None)?;
        writer.write(&RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?)?;
        writer.close()?;
        let actual = i64::try_from(std::fs::metadata(&local)?.len())?;
        Ok(CompactionFile {
            level,
            file: DataFileInfo {
                partition_desc: partition_desc.into(),
                path: root.join(relative)?.to_string(),
                file_op: "add".into(),
                size: metadata_size.unwrap_or(actual),
                bucket_id: Some(0),
                file_exist_cols: "id,value".into(),
                ..Default::default()
            },
        })
    }

    fn read_rows(
        file: File,
    ) -> std::result::Result<BTreeMap<i64, i64>, Box<dyn std::error::Error>> {
        let parquet = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut rows = BTreeMap::new();
        for batch in parquet.build()? {
            let batch = batch?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column");
            for (id, value) in ids
                .values()
                .iter()
                .copied()
                .zip(values.values().iter().copied())
            {
                assert!(
                    rows.insert(id, value).is_none(),
                    "duplicate output key {id}"
                );
            }
        }
        Ok(rows)
    }

    #[test]
    fn policy_promotion_and_level_capacity_examples() {
        let mut policy = test_policy();
        policy.level_file_num_limit = 10;
        policy.level_base_bytes = 1_000;
        policy.low_level_multiplier = 5;
        policy.level_multiplier = 10;
        policy.pick_next_level_min_bytes = 100;
        policy.pick_next_level_multiplier = 5;
        policy.l1_merge_input_target_bytes = 256;

        policy.rolling_enabled_for_table = true;
        assert_eq!(policy.promotion_size(0), 100);
        assert_eq!(policy.promotion_size(1), 256);
        assert_eq!(policy.promotion_size(2), 500);

        policy.rolling_enabled_for_table = false;
        assert_eq!(policy.promotion_size(1), 100);
        assert_eq!(policy.promotion_size(2), 500);
        assert_eq!(policy.promotion_size(3), 2_500);

        assert_eq!(policy.level_capacity(1), 1_000);
        assert_eq!(policy.level_capacity(2), 5_000);
        assert_eq!(policy.level_capacity(3), 50_000);

        assert!(policy.needs_level_merge(1, 10, 0));
        assert!(!policy.needs_level_merge(1, 9, 999));
        assert!(policy.needs_level_merge(2, 1, 5_000));
        assert!(!policy.needs_level_merge(2, 9, 4_999));
        assert!(!policy.needs_level_merge(2, 0, u64::MAX));
    }

    #[test]
    fn default_policy_matches_spark_effective_defaults() -> Result<()> {
        let policy = CompactionPolicy::default();
        assert_eq!(policy.level0_file_num_limit, 4);
        assert_eq!(policy.level_file_num_limit, 10);
        assert_eq!(policy.l0_read_file_num_limit, i32::MAX as u32);
        assert_eq!(policy.l0_read_bytes_limit, i64::MAX as u64);
        assert_eq!(policy.merge_file_num_limit, 5);
        assert_eq!(policy.merge_input_bytes_limit, 5 * GIB);
        assert_eq!(policy.read_file_max_bytes, 5 * GIB);
        assert_eq!(policy.level_base_bytes, GIB);
        assert_eq!(policy.low_level_multiplier, 5);
        assert_eq!(policy.level_multiplier, 10);
        assert_eq!(policy.pick_next_level_min_bytes, 100 * MIB);
        assert_eq!(policy.pick_next_level_multiplier, 5);
        assert_eq!(policy.max_level, 5);
        assert!(policy.rolling_enabled_for_table);
        assert_eq!(policy.l1_merge_input_target_bytes, 256 * MIB);
        policy.validate()
    }

    #[test]
    fn prepare_task_uses_the_coordinator_execution_policy() -> Result<()> {
        use std::time::Duration;

        use lakesoul_metadata_proto::entity::{PartitionInfo, TableInfo};

        use super::super::compaction_coordinator::{
            CompactionCoordinator, ExecutionConfig, LoadedSnapshot, PolicyConfig,
        };

        let build_task = |rolling_enabled| {
            let session = Arc::new(SessionContext::new());
            let coordinator =
                CompactionCoordinator::new(session.clone(), Default::default())
                    .with_execution(
                        session,
                        ExecutionConfig {
                            policy: PolicyConfig {
                                policy_defaults: test_policy(),
                                rolling_enabled,
                                l1_rolling_tables: Default::default(),
                            },
                            reader_options: Default::default(),
                            writer_options: Default::default(),
                            execution_timeout: Duration::from_secs(1),
                        },
                    );
            let snapshot = LoadedSnapshot::from_discovery(
                TableInfo {
                    table_id: "orders".into(),
                    ..Default::default()
                },
                PartitionInfo {
                    table_id: "orders".into(),
                    ..Default::default()
                },
                Vec::new(),
            );
            coordinator.prepare_task("task".into(), snapshot)
        };

        let enabled = build_task(true)?;
        let disabled = build_task(false)?;
        assert!(enabled.policy().rolling_enabled_for_table);
        assert!(!disabled.policy().rolling_enabled_for_table);
        assert_ne!(enabled.policy_revision(), disabled.policy_revision());
        Ok(())
    }

    #[test]
    fn policy_revision_is_stable_and_tracks_effective_policy() -> Result<()> {
        use super::super::compaction_coordinator::policy_revision;

        let mut policy = test_policy();
        let revision = policy_revision(&policy)?;
        assert_eq!(
            revision,
            "v1:a3613a572e7598b679266d58d8d80f94206a0974c2fd8727d1292c8671fd1ef5"
        );
        policy.rolling_enabled_for_table = false;
        assert_ne!(policy_revision(&policy)?, revision);
        Ok(())
    }

    #[test]
    fn bucket_selection_checks_existing_l1_and_promotion_without_enough_l0() {
        let policy = test_policy();
        let file = |level, size| CompactionFile {
            level,
            file: DataFileInfo {
                size,
                ..Default::default()
            },
        };

        assert!(!bucket_has_work(&[file(1, 25), file(0, 20)], &policy));
        assert!(bucket_has_work(
            &[file(1, 25), file(1, 25), file(0, 20)],
            &policy
        ));
        assert!(bucket_has_work(&[file(1, 256)], &policy));
        assert!(bucket_has_work(&[file(2, 500)], &policy));
        assert!(!bucket_has_work(&[file(3, 500)], &policy));
    }
    #[test]
    fn collector_accounts_for_each_input_occurrence_and_requires_a_bucket() -> Result<()>
    {
        let retained = CompactionFile {
            level: 0,
            file: DataFileInfo {
                path: "file:///table/original_0000.parquet".into(),
                partition_desc: "-5".into(),
                file_op: "add".into(),
                size: 10,
                bucket_id: Some(0),
                ..Default::default()
            },
        };
        let run = RunTask {
            bucket_id: 0,
            input_files: vec![retained.clone(), retained.clone()],
        };
        let descriptor = CompactionDescriptor {
            protocol_version: CODEC_VERSION,
            task_id: "task".into(),
            table: CompactionTableDescriptor {
                table_id: "table".into(),
                table_path: "file:///table".into(),
                schema: Arc::new(Schema::new(vec![Field::new(
                    "id",
                    DataType::Int64,
                    false,
                )])),
                primary_keys: vec!["id".into()],
                range_partition_columns: vec![],
                partition_values: BTreeMap::new(),
                hash_partitioning_columns: vec![],
                hash_bucket_num: 1,
                cdc_column: None,
                physical_format: "parquet".into(),
                merge_operators: BTreeMap::new(),
            },
            partition_desc: "-5".into(),
            policy: test_policy(),
            reader_options: BTreeMap::new(),
            writer_options: BTreeMap::new(),
            runs: vec![run],
        };
        let path = descriptor.new_output_path(&descriptor.runs[0], 0)?;
        let mut replacement = retained.clone();
        replacement.file.path = path.clone();
        let good = RunOutput {
            task_id: "task".into(),
            bucket_id: 0,
            result_files: vec![retained, replacement],
            created_paths: vec![path],
            consumed_input_positions: vec![1],
            stats: RunStats::default(),
        };
        let mut collector = RunOutputCollector::new(descriptor.clone())?;
        let mut peer_descriptor = descriptor.clone();
        peer_descriptor.task_id = "peer_task".into();
        let peer_path = peer_descriptor.new_output_path(&peer_descriptor.runs[0], 0)?;
        let mut tracked = Vec::new();
        let mut mixed = good.clone();
        mixed.created_paths.insert(0, peer_path.clone());
        assert!(
            collector
                .accept_tracking(mixed, |p| tracked.push(p.to_owned()))
                .is_err()
        );
        assert_eq!(tracked, good.created_paths);

        let mut foreign = good.clone();
        foreign.task_id = peer_descriptor.task_id.clone();
        foreign.created_paths = vec![peer_path];
        assert!(!collector.accept_tracking(foreign, |_| panic!("tracked foreign task"))?);
        let mut incomplete = good.clone();
        incomplete.consumed_input_positions.clear();
        assert!(
            collector
                .accept_tracking(incomplete, |path| assert_eq!(
                    path,
                    good.created_paths[0].as_str()
                ))
                .unwrap_err()
                .to_string()
                .contains("omitted an input")
        );
        let mut missing_bucket = good.clone();
        missing_bucket.result_files[1].file.bucket_id = None;
        assert!(collector.accept(missing_bucket).is_err());
        let mut extra_object = good.clone();
        extra_object
            .created_paths
            .push(descriptor.new_output_path(&descriptor.runs[0], 0)?);
        assert!(
            collector
                .accept(extra_object)
                .unwrap_err()
                .to_string()
                .contains("created paths must match")
        );
        assert!(collector.accept(good)?);
        assert_eq!(collector.finish()?.len(), 1);
        Ok(())
    }
    #[tokio::test]
    async fn worker_merges_partitioned_parquet_and_cleans_promoted_intermediate()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use std::fs::{self, File};

        use arrow::array::Int64Array;
        use datafusion::prelude::SessionConfig;
        use parquet::arrow::{
            ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder,
        };

        let warehouse = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(warehouse.path())
            .expect("absolute temporary warehouse URL");
        let partition = "day=2026-11-25";
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let make_file = |directory: &str,
                         name: &str,
                         ids: Vec<i64>,
                         values: Vec<i64>,
                         level: u32|
         -> std::result::Result<
            CompactionFile,
            Box<dyn std::error::Error>,
        > {
            let relative = format!("{directory}/{name}");
            let local = warehouse.path().join(&relative);
            fs::create_dir_all(local.parent().expect("fixture file parent"))?;
            let mut writer = ArrowWriter::try_new(
                File::create(&local)?,
                physical_schema.clone(),
                None,
            )?;
            writer.write(&RecordBatch::try_new(
                physical_schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(values)),
                ],
            )?)?;
            writer.close()?;
            Ok(CompactionFile {
                level,
                file: DataFileInfo {
                    path: root.join(&relative)?.to_string(),
                    partition_desc: partition.into(),
                    file_op: "add".into(),
                    size: i64::try_from(fs::metadata(local)?.len())?,
                    bucket_id: Some(0),
                    file_exist_cols: "id,value".into(),
                    ..Default::default()
                },
            })
        };
        let upper_directory = format!("compactdir1/{partition}");
        let retained = make_file(
            &upper_directory,
            "retained_0000.parquet",
            vec![0],
            vec![7],
            1,
        )?;
        let older =
            make_file(partition, "older_0000.parquet", vec![1, 2], vec![10, 20], 0)?;
        let newer = make_file(
            partition,
            "newer_0000.parquet",
            vec![2, 3],
            vec![200, 30],
            0,
        )?;
        let run = RunTask {
            bucket_id: 0,
            input_files: vec![retained.clone(), older, newer],
        };
        let mut policy = test_policy();
        policy.level0_file_num_limit = 2;
        policy.l0_read_bytes_limit = u64::MAX;
        policy.read_file_max_bytes = u64::MAX;
        policy.pick_next_level_min_bytes = 1;
        policy.level_base_bytes = u64::MAX;
        policy.max_level = 1;
        policy.rolling_enabled_for_table = false;
        let descriptor = CompactionDescriptor {
            protocol_version: CODEC_VERSION,
            task_id: "worker_merge".into(),
            table: CompactionTableDescriptor {
                table_id: "orders".into(),
                table_path: root.as_str().trim_end_matches('/').into(),
                schema: Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("value", DataType::Int64, false),
                    Field::new("day", DataType::Utf8, false),
                ])),
                primary_keys: vec!["id".into()],
                range_partition_columns: vec!["day".into()],
                partition_values: BTreeMap::from([("day".into(), "2026-11-25".into())]),
                hash_partitioning_columns: vec![],
                hash_bucket_num: 1,
                cdc_column: None,
                physical_format: "parquet".into(),
                merge_operators: BTreeMap::new(),
            },
            partition_desc: partition.into(),
            policy,
            reader_options: BTreeMap::new(),
            writer_options: BTreeMap::new(),
            runs: vec![run],
        };
        let mut session = SessionConfig::new();
        session.set_extension(Arc::new(CompactionWorkerConfig(
            LakeSoulIOConfigBuilder::new().build(),
        )));
        let context = SessionContext::new_with_config(session);
        let run = &descriptor.runs[0];
        let output = execute_run(&descriptor, run, context.task_ctx()).await?;
        assert_eq!(output.consumed_input_positions, vec![1, 2]);
        let [unchanged, promoted] = output.result_files.as_slice() else {
            panic!("expected one retained and one promoted file");
        };
        assert_eq!(unchanged, &retained);
        assert_eq!(promoted.level, 1);
        assert_eq!(promoted.file.bucket_id, Some(0));
        assert_eq!(output.created_paths, vec![promoted.file.path.clone()]);

        let local_path =
            |uri: &str| url::Url::parse(uri).unwrap().to_file_path().unwrap();
        for original in &run.input_files {
            assert!(local_path(&original.file.path).is_file());
        }
        assert_eq!(fs::read_dir(warehouse.path().join(partition))?.count(), 2);
        assert_eq!(
            fs::read_dir(warehouse.path().join(upper_directory))?.count(),
            2
        );

        let file = File::open(local_path(&promoted.file.path))?;
        assert_eq!(promoted.file.size, i64::try_from(file.metadata()?.len())?);
        let parquet = ParquetRecordBatchReaderBuilder::try_new(file)?;
        assert_eq!(parquet.schema().fields().len(), 2);
        assert!(parquet.schema().field_with_name("day").is_err());
        assert_eq!(promoted.file.file_exist_cols, "id,value");
        let mut rows = BTreeMap::new();
        for batch in parquet.build()? {
            let batch = batch?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for (id, value) in ids
                .values()
                .iter()
                .copied()
                .zip(values.values().iter().copied())
            {
                assert!(
                    rows.insert(id, value).is_none(),
                    "duplicate output key {id}"
                );
            }
        }
        assert_eq!(rows, BTreeMap::from([(1, 10), (2, 200), (3, 30)]));

        let mut collector = RunOutputCollector::new(descriptor)?;
        let [round_tripped] = RunOutput::from_batch(&output.to_batch()?)?
            .try_into()
            .expect("one result batch row");
        assert!(collector.accept(round_tripped)?);
        assert_eq!(collector.finish()?, vec![output]);
        Ok(())
    }

    #[tokio::test]
    async fn promotion_stops_at_first_file_below_threshold()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let warehouse = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(warehouse.path()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid warehouse path",
            )
        })?;
        let table_path = root.as_str().trim_end_matches('/').to_string();
        let partition = "day=2026-11-25";
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let file = |name: &str, size: i64| CompactionFile {
            level: 1,
            file: DataFileInfo {
                partition_desc: partition.into(),
                path: format!("{table_path}/compactdir1/{partition}/{name}"),
                file_op: "add".into(),
                size,
                bucket_id: Some(0),
                file_exist_cols: "id,value".into(),
                ..Default::default()
            },
        };

        // The first file blocks promotion of the second even though the second
        // is above the threshold.
        let first = file("first_0000.parquet", 10);
        let second = file("second_0000.parquet", 100);
        let run = RunTask {
            bucket_id: 0,
            input_files: vec![first.clone(), second.clone()],
        };
        let mut policy = test_policy();
        policy.rolling_enabled_for_table = false;
        policy.max_level = 2;
        policy.level_file_num_limit = 10;
        policy.level_base_bytes = u64::MAX;
        policy.read_file_max_bytes = u64::MAX;
        policy.pick_next_level_min_bytes = 50;
        let descriptor =
            fixture_descriptor(&table_path, partition, &physical_schema, policy, run);
        let context = test_worker_context();
        let output =
            execute_run(&descriptor, &descriptor.runs[0], context.task_ctx()).await?;
        assert_eq!(output.result_files, vec![first, second]);
        assert!(output.consumed_input_positions.is_empty());
        assert!(output.created_paths.is_empty());

        let mut collector = RunOutputCollector::new(descriptor)?;
        let [round_tripped] = RunOutput::from_batch(&output.to_batch()?)?
            .try_into()
            .expect("one result batch row");
        assert!(collector.accept(round_tripped)?);
        Ok(())
    }

    #[tokio::test]
    async fn worker_rolling_merges_l0_and_promotes_to_l1()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use std::fs;

        let warehouse = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(warehouse.path()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid warehouse path",
            )
        })?;
        let table_path = root.as_str().trim_end_matches('/').to_string();
        let partition = "day=2026-11-25";
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let older = write_parquet_fixture(
            warehouse.path(),
            &root,
            &format!("{partition}/older_0000.parquet"),
            physical_schema.clone(),
            vec![1, 2],
            vec![10, 20],
            partition,
            0,
            Some(1),
        )?;
        let newer = write_parquet_fixture(
            warehouse.path(),
            &root,
            &format!("{partition}/newer_0000.parquet"),
            physical_schema.clone(),
            vec![2, 3],
            vec![200, 30],
            partition,
            0,
            Some(1),
        )?;
        let run = RunTask {
            bucket_id: 0,
            input_files: vec![older, newer],
        };
        let mut policy = test_policy();
        policy.level0_file_num_limit = 2;
        policy.l0_read_file_num_limit = 20;
        policy.l0_read_bytes_limit = u64::MAX;
        policy.read_file_max_bytes = u64::MAX;
        policy.pick_next_level_min_bytes = 10;
        policy.level_base_bytes = u64::MAX;
        policy.max_level = 1;
        policy.rolling_enabled_for_table = true;
        let descriptor =
            fixture_descriptor(&table_path, partition, &physical_schema, policy, run);
        let context = test_worker_context();
        let output =
            execute_run(&descriptor, &descriptor.runs[0], context.task_ctx()).await?;

        assert_eq!(output.consumed_input_positions, vec![0, 1]);
        let [promoted] = output.result_files.as_slice() else {
            panic!("expected one promoted L1 file");
        };
        assert_eq!(promoted.level, 1);
        assert_eq!(promoted.file.bucket_id, Some(0));
        assert_eq!(output.created_paths, vec![promoted.file.path.clone()]);

        let local_path =
            |uri: &str| url::Url::parse(uri).unwrap().to_file_path().unwrap();
        for original in &descriptor.runs[0].input_files {
            assert!(local_path(&original.file.path).is_file());
        }
        // The L0 merge intermediate is deleted; only the two originals remain.
        assert_eq!(fs::read_dir(warehouse.path().join(partition))?.count(), 2);
        let promoted_dir = warehouse.path().join("compactdir1").join(partition);
        assert_eq!(fs::read_dir(promoted_dir)?.count(), 1);

        let rows = read_rows(File::open(local_path(&promoted.file.path))?)?;
        assert_eq!(rows, BTreeMap::from([(1, 10), (2, 200), (3, 30)]));
        assert_eq!(promoted.file.file_exist_cols, "id,value");

        let mut collector = RunOutputCollector::new(descriptor)?;
        let [round_tripped] = RunOutput::from_batch(&output.to_batch()?)?
            .try_into()
            .expect("one result batch row");
        assert!(collector.accept(round_tripped)?);
        Ok(())
    }

    // Write sorted keys with disjoint ranges plus one shared key (42). Fixed
    // pseudo-random low bits keep the Parquet payload from compressing away.
    fn rolling_fixture(
        warehouse: &std::path::Path,
        root: &url::Url,
        schema: &SchemaRef,
        level: u32,
        generation: i64,
        rows: i64,
    ) -> std::result::Result<
        (CompactionFile, BTreeMap<i64, i64>),
        Box<dyn std::error::Error>,
    > {
        let partition = "day=2026-11-25";
        let directory = if level == 0 {
            partition.to_owned()
        } else {
            format!("compactdir{level}/{partition}")
        };
        let mut data = BTreeMap::from([(42, generation * 10)]);
        let mut seed = generation as u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as i64
        };
        for offset in 0..rows {
            let id =
                ((generation * 10_000 + offset) << 46) | (next() & ((1_i64 << 46) - 1));
            data.insert(id, next());
        }
        let file = write_parquet_fixture(
            warehouse,
            root,
            &format!("{directory}/gen{generation}_0000.parquet"),
            schema.clone(),
            data.keys().copied().collect(),
            data.values().copied().collect(),
            partition,
            level,
            None,
        )?;
        Ok((file, data))
    }

    fn boundary_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    async fn execute_fixture_bucket(
        root: &url::Url,
        schema: &SchemaRef,
        policy: CompactionPolicy,
        files: Vec<CompactionFile>,
    ) -> Result<RunOutput> {
        let descriptor = fixture_descriptor(
            root.as_str().trim_end_matches('/'),
            "day=2026-11-25",
            schema,
            policy,
            RunTask {
                bucket_id: 0,
                input_files: files,
            },
        );
        let output = execute_run(
            &descriptor,
            &descriptor.runs[0],
            test_worker_context().task_ctx(),
        )
        .await?;
        let mut collector = RunOutputCollector::new(descriptor)?;
        assert!(collector.accept(output.clone())?);
        Ok(output)
    }

    fn fixture_path(file: &CompactionFile) -> std::path::PathBuf {
        url::Url::parse(&file.file.path)
            .unwrap()
            .to_file_path()
            .unwrap()
    }

    #[tokio::test]
    async fn legacy_l0_batches_rewrite_singletons_and_preserve_group_order()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(dir.path()).unwrap();
        let schema = boundary_schema();
        let mut files = Vec::new();
        let mut rows = Vec::new();
        for generation in 1..=3 {
            let (file, data) =
                rolling_fixture(dir.path(), &root, &schema, 0, generation, 16)?;
            files.push(file);
            rows.push(data);
        }
        let mut first_batch = rows[0].clone();
        first_batch.extend(rows[1].clone());
        // Count limit yields [A,B],[C]. Independently, equality after A must
        // allow B into the batch before the byte-budget overshoot is detected.
        for (limit, budget) in [(2, u64::MAX), (3, files[0].file.size as u64)] {
            let mut policy = test_policy();
            policy.rolling_enabled_for_table = false;
            policy.level0_file_num_limit = 2;
            policy.l0_read_file_num_limit = limit;
            policy.l0_read_bytes_limit = budget;
            policy.read_file_max_bytes = u64::MAX;
            policy.pick_next_level_min_bytes = u64::MAX;
            let output =
                execute_fixture_bucket(&root, &schema, policy, files.clone()).await?;
            let [first, singleton] = output.result_files.as_slice() else {
                panic!("expected [A,B] and [C] as separate L0 outputs");
            };
            assert_eq!((first.level, singleton.level), (0, 0));
            assert_eq!(output.consumed_input_positions, vec![0, 1, 2]);
            assert_ne!(singleton.file.path, files[2].file.path);
            assert_eq!(
                output.created_paths,
                vec![first.file.path.clone(), singleton.file.path.clone()]
            );
            assert_eq!(read_rows(File::open(fixture_path(first))?)?, first_batch);
            assert_eq!(read_rows(File::open(fixture_path(singleton))?)?, rows[2]);
        }
        Ok(())
    }

    #[tokio::test]
    async fn rolling_l1_refills_undersized_merge_with_pending_file()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(dir.path()).unwrap();
        let schema = boundary_schema();
        let partition = "day=2026-11-25";
        let mut files = Vec::new();
        let mut expected = BTreeMap::new();
        // Full key overlap keeps the first merge small. File C also has a
        // unique key so its participation is observable in the final rows.
        for generation in 1..=3_i64 {
            let ids: Vec<_> = (0..1024 + i64::from(generation == 3)).collect();
            let values: Vec<_> = ids.iter().map(|id| id * 13 + generation).collect();
            expected.extend(ids.iter().copied().zip(values.iter().copied()));
            files.push(write_parquet_fixture(
                dir.path(),
                &root,
                &format!("compactdir1/{partition}/tail{generation}_0000.parquet"),
                schema.clone(),
                ids,
                values,
                partition,
                1,
                None,
            )?);
        }
        let mut policy = test_policy();
        policy.max_level = 1;
        policy.rolling_enabled_for_table = true;
        // A+B exactly reaches target on input, forcing a flush before C.
        let target = (files[0].file.size + files[1].file.size) as u64;
        assert!(files.iter().all(|file| (file.file.size as u64) < target));
        policy.l1_merge_input_target_bytes = target;
        let output = execute_fixture_bucket(&root, &schema, policy, files).await?;
        let [merged] = output.result_files.as_slice() else {
            panic!("undersized A+B must rejoin C in the same L1 pass");
        };
        assert_eq!(merged.level, 1);
        assert_eq!(output.consumed_input_positions, vec![0, 1, 2]);
        assert_eq!(output.created_paths, vec![merged.file.path.clone()]);
        assert_eq!(read_rows(File::open(fixture_path(merged))?)?, expected);
        assert_eq!(
            std::fs::read_dir(fixture_path(merged).parent().unwrap())?.count(),
            4
        );
        Ok(())
    }

    #[tokio::test]
    async fn level_merge_respects_head_size_count_bytes_and_unflushed_tail()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(dir.path()).unwrap();
        let schema = boundary_schema();
        let mut files = Vec::new();
        let mut rows = Vec::new();
        for (index, n) in [512, 4, 512, 4, 4, 4, 4, 4].into_iter().enumerate() {
            let (file, data) =
                rolling_fixture(dir.path(), &root, &schema, 2, index as i64 + 1, n)?;
            files.push(file);
            rows.push(data);
        }
        let mut policy = test_policy();
        policy.rolling_enabled_for_table = false;
        policy.max_level = 2;
        policy.level_file_num_limit = 1;
        policy.merge_file_num_limit = 3;
        policy.read_file_max_bytes = files[0].file.size.min(files[2].file.size) as u64;
        policy.merge_input_bytes_limit = (files[1].file.size + files[2].file.size) as u64;
        assert!(bytes(&files[3..6]) < policy.merge_input_bytes_limit);
        let output =
            execute_fixture_bucket(&root, &schema, policy.clone(), files.clone()).await?;
        let [head, by_bytes, by_count, tail_a, tail_b] = output.result_files.as_slice()
        else {
            panic!("expected retained head, two merges, and two retained tail files");
        };
        assert_eq!((head, tail_a, tail_b), (&files[0], &files[6], &files[7]));
        assert_eq!(output.consumed_input_positions, vec![1, 2, 3, 4, 5]);
        assert_eq!(
            output.created_paths,
            vec![by_bytes.file.path.clone(), by_count.file.path.clone()]
        );
        for (merged, group) in [(by_bytes, &rows[1..3]), (by_count, &rows[3..6])] {
            let mut expected = BTreeMap::new();
            for data in group {
                expected.extend(data.clone());
            }
            assert_eq!(read_rows(File::open(fixture_path(merged))?)?, expected);
        }
        // A singleton hitting the byte limit is retained even when it is
        // below the independent oversized-head threshold.
        policy.read_file_max_bytes = u64::MAX;
        policy.merge_input_bytes_limit = files[1].file.size as u64;
        let singleton =
            execute_fixture_bucket(&root, &schema, policy, vec![files[1].clone()])
                .await?;
        assert_eq!(singleton.result_files, vec![files[1].clone()]);
        assert!(singleton.created_paths.is_empty());
        assert!(singleton.consumed_input_positions.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn original_file_is_copied_at_promotion_threshold_and_stops_at_max_level()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(dir.path()).unwrap();
        let schema = boundary_schema();
        let (original, expected) = rolling_fixture(dir.path(), &root, &schema, 1, 1, 64)?;
        let mut policy = test_policy();
        policy.rolling_enabled_for_table = false;
        policy.max_level = 2;
        policy.level_base_bytes = u64::MAX;
        policy.pick_next_level_min_bytes = original.file.size as u64;
        policy.pick_next_level_multiplier = 1;
        let output =
            execute_fixture_bucket(&root, &schema, policy, vec![original.clone()])
                .await?;
        let [copy] = output.result_files.as_slice() else {
            panic!("expected one promoted file");
        };
        assert_eq!(copy.level, 2);
        assert_eq!(copy.file.size, original.file.size);
        assert_eq!(copy.file.file_exist_cols, original.file.file_exist_cols);
        assert_eq!(output.created_paths, vec![copy.file.path.clone()]);
        assert_eq!(output.consumed_input_positions, vec![0]);
        let source = fixture_path(&original);
        let destination = fixture_path(copy);
        assert_eq!(
            destination.parent().unwrap(),
            dir.path().join("compactdir2/day=2026-11-25")
        );
        assert_eq!(std::fs::read(source)?, std::fs::read(&destination)?);
        assert_eq!(read_rows(File::open(destination)?)?, expected);
        assert!(!dir.path().join("compactdir3").exists());
        Ok(())
    }

    #[tokio::test]
    async fn worker_rolls_l0_and_l1_then_merges_existing_l2()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let warehouse = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(warehouse.path()).unwrap();
        let partition = "day=2026-11-25";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let mut inputs = Vec::new();
        let mut expected = BTreeMap::new();
        for (level, generation, rows) in
            [(2, 1, 256), (1, 2, 5000), (0, 3, 2048), (0, 4, 2048)]
        {
            let (file, data) = rolling_fixture(
                warehouse.path(),
                &root,
                &schema,
                level,
                generation,
                rows,
            )?;
            inputs.push(file);
            expected.extend(data);
        }
        let mut policy = test_policy();
        policy.rolling_enabled_for_table = true;
        policy.max_level = 2;
        policy.level0_file_num_limit = 2;
        policy.level_file_num_limit = 2;
        policy.merge_file_num_limit = 2;
        policy.merge_input_bytes_limit = u64::MAX;
        policy.l0_read_bytes_limit = u64::MAX;
        policy.read_file_max_bytes = u64::MAX;
        policy.level_base_bytes = u64::MAX;
        // Each L0 input is small individually. The old L1 has more rows than
        // both L0s together, so the promoted L0 is also small at L1.
        policy.pick_next_level_min_bytes =
            inputs[2].file.size.max(inputs[3].file.size) as u64 + 1;
        policy.l1_merge_input_target_bytes = inputs[1].file.size as u64 + 1;
        let run = RunTask {
            bucket_id: 0,
            input_files: inputs,
        };
        let descriptor = fixture_descriptor(
            root.as_str().trim_end_matches('/'),
            partition,
            &schema,
            policy,
            run,
        );
        let run = &descriptor.runs[0];
        let output =
            execute_run(&descriptor, run, test_worker_context().task_ctx()).await?;
        let [final_file] = output.result_files.as_slice() else {
            panic!("expected a single merged L2 output");
        };
        assert_eq!(final_file.level, 2);
        assert_eq!(final_file.file.bucket_id, Some(0));
        assert_eq!(output.created_paths, vec![final_file.file.path.clone()]);
        let mut consumed = output.consumed_input_positions.clone();
        consumed.sort_unstable();
        assert_eq!(consumed, vec![0, 1, 2, 3]);
        let local = url::Url::parse(&final_file.file.path)?
            .to_file_path()
            .unwrap();
        assert_eq!(
            final_file.file.size,
            i64::try_from(std::fs::metadata(&local)?.len())?
        );
        assert_eq!(expected[&42], 40);
        assert_eq!(read_rows(File::open(&local)?)?, expected);

        let mut expected_paths = HashSet::from([local]);
        for file in &run.input_files {
            expected_paths
                .insert(url::Url::parse(&file.file.path)?.to_file_path().unwrap());
        }
        let mut actual_paths = HashSet::new();
        for directory in [
            partition.to_owned(),
            format!("compactdir1/{partition}"),
            format!("compactdir2/{partition}"),
        ] {
            for entry in std::fs::read_dir(warehouse.path().join(directory))? {
                actual_paths.insert(entry?.path());
            }
        }
        assert_eq!(actual_paths, expected_paths);
        assert!(RunOutputCollector::new(descriptor)?.accept(output)?);
        Ok(())
    }

    #[tokio::test]
    async fn worker_rolls_existing_l1_while_l0_is_below_threshold()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let warehouse = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(warehouse.path()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let (old, mut expected) =
            rolling_fixture(warehouse.path(), &root, &schema, 1, 1, 32)?;
        let (newer, updates) =
            rolling_fixture(warehouse.path(), &root, &schema, 1, 2, 32)?;
        expected.extend(updates);
        let (l0, l0_rows) = rolling_fixture(warehouse.path(), &root, &schema, 0, 3, 32)?;
        let mut policy = test_policy();
        policy.rolling_enabled_for_table = true;
        policy.max_level = 2;
        policy.level0_file_num_limit = 2;
        // Both L1s fit in the rolling group and its result stays at L1.
        policy.l1_merge_input_target_bytes = 64 * 1024;
        let run = RunTask {
            bucket_id: 0,
            input_files: vec![old, newer, l0.clone()],
        };
        let descriptor = fixture_descriptor(
            root.as_str().trim_end_matches('/'),
            "day=2026-11-25",
            &schema,
            policy,
            run,
        );
        let output = execute_run(
            &descriptor,
            &descriptor.runs[0],
            test_worker_context().task_ctx(),
        )
        .await?;
        let [merged, untouched] = output.result_files.as_slice() else {
            panic!("expected one merged L1 and the original L0");
        };
        assert_eq!(merged.level, 1);
        assert_eq!(untouched, &l0);
        assert_eq!(output.created_paths, vec![merged.file.path.clone()]);
        assert_eq!(output.consumed_input_positions, vec![0, 1]);
        let read = |file: &CompactionFile| -> std::result::Result<_, Box<dyn std::error::Error>> {
            read_rows(File::open(url::Url::parse(&file.file.path)?.to_file_path().unwrap())?)
        };
        assert_eq!(read(merged)?, expected);
        assert_eq!(read(untouched)?, l0_rows);
        assert!(RunOutputCollector::new(descriptor)?.accept(output)?);
        Ok(())
    }

    #[tokio::test]
    async fn worker_merges_level_files_and_promotes_to_next_level()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use std::fs;

        let warehouse = tempfile::tempdir()?;
        let root = url::Url::from_directory_path(warehouse.path()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid warehouse path",
            )
        })?;
        let table_path = root.as_str().trim_end_matches('/').to_string();
        let partition = "day=2026-11-25";
        let upper = format!("compactdir1/{partition}");
        let physical_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let older = write_parquet_fixture(
            warehouse.path(),
            &root,
            &format!("{upper}/older_0000.parquet"),
            physical_schema.clone(),
            vec![1, 2],
            vec![10, 20],
            partition,
            1,
            None,
        )?;
        let newer = write_parquet_fixture(
            warehouse.path(),
            &root,
            &format!("{upper}/newer_0000.parquet"),
            physical_schema.clone(),
            vec![2, 3],
            vec![200, 30],
            partition,
            1,
            None,
        )?;
        let run = RunTask {
            bucket_id: 0,
            input_files: vec![older, newer],
        };
        let mut policy = test_policy();
        policy.level0_file_num_limit = 4;
        policy.level_file_num_limit = 2;
        policy.merge_file_num_limit = 2;
        policy.merge_input_bytes_limit = u64::MAX;
        policy.read_file_max_bytes = u64::MAX;
        policy.level_base_bytes = u64::MAX;
        policy.pick_next_level_min_bytes = 1;
        policy.max_level = 2;
        policy.rolling_enabled_for_table = false;
        let descriptor =
            fixture_descriptor(&table_path, partition, &physical_schema, policy, run);
        let context = test_worker_context();
        let output =
            execute_run(&descriptor, &descriptor.runs[0], context.task_ctx()).await?;

        assert_eq!(output.consumed_input_positions, vec![0, 1]);
        let [promoted] = output.result_files.as_slice() else {
            panic!("expected one promoted L2 file");
        };
        assert_eq!(promoted.level, 2);
        assert_eq!(promoted.file.bucket_id, Some(0));
        assert_eq!(output.created_paths, vec![promoted.file.path.clone()]);

        let local_path =
            |uri: &str| url::Url::parse(uri).unwrap().to_file_path().unwrap();
        for original in &descriptor.runs[0].input_files {
            assert!(local_path(&original.file.path).is_file());
        }
        assert_eq!(fs::read_dir(warehouse.path().join(&upper))?.count(), 2);
        let promoted_dir = warehouse.path().join("compactdir2").join(partition);
        assert_eq!(fs::read_dir(promoted_dir)?.count(), 1);

        let rows = read_rows(File::open(local_path(&promoted.file.path))?)?;
        assert_eq!(rows, BTreeMap::from([(1, 10), (2, 200), (3, 30)]));
        assert_eq!(promoted.file.file_exist_cols, "id,value");

        let mut collector = RunOutputCollector::new(descriptor)?;
        let [round_tripped] = RunOutput::from_batch(&output.to_batch()?)?
            .try_into()
            .expect("one result batch row");
        assert!(collector.accept(round_tripped)?);
        Ok(())
    }
}
