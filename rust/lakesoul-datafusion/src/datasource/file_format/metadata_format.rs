// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::datasource::file_format::FileFormat`] implementation
//! for LakeSoul tables: the metadata/listing callback DataFusion uses for
//! LakeSoul tables, plus the LakeSoul write path.

use lakesoul_common::misc::SecretMap;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::{Arc, Once};

use arrow::array::{ArrayRef, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::common::not_impl_err;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::FileSource;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, LexRequirement, OrderingRequirements,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::{
    datasource::physical_plan::{FileScanConfig, FileSinkConfig},
    error::Result as DFResult,
    physical_plan::ExecutionPlan,
};
use datafusion_common::TableReference;
use datafusion_common::tree_node::TreeNodeRecursion;
use futures::StreamExt;
use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::execution_trace::instrument_record_batch_stream;
use lakesoul_io::file_format::{
    LakeSoulFormatRegistry, PhysicalFormat, merge_schema_refs,
};
use lakesoul_io::helpers::{
    columnar_values_to_partition_desc, columnar_values_to_sub_path, get_columnar_values,
};
use lakesoul_io::session::LakeSoulIOSession;
use lakesoul_io::writer::async_writer::AsyncBatchWriter;
use lakesoul_io::writer::create_writer;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata_proto::entity::TableInfo;
use object_store::{ObjectMeta, ObjectStore};
use rand::distr::SampleString;
use rootcause::compat::boxed_error::IntoBoxedError;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use crate::Result;
use crate::catalog::{commit_data, parse_table_info_partitions};
use crate::lakesoul_table::helpers::create_io_config_builder_from_table_info;

type PartitionedFile = HashMap<String, (Vec<String>, u64)>;

const WRITE_FILES_TOTAL: &str = "lakesoul_write_files_total";
const WRITE_BYTES_TOTAL: &str = "lakesoul_write_bytes_total";
const WRITE_ROWS_TOTAL: &str = "lakesoul_write_rows_total";
const COMMIT_DURATION_SECONDS: &str = "lakesoul_commit_duration_seconds";
const COMMIT_PARTITIONS_TOTAL: &str = "lakesoul_commit_partitions_total";
const INDEX_BUILD_DURATION_SECONDS: &str = "lakesoul_index_build_duration_seconds";
const INDEX_BUILT_SHARDS_TOTAL: &str = "lakesoul_index_built_shards_total";

static DESCRIBE_METRICS: Once = Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            WRITE_FILES_TOTAL,
            "Data files written by LakeSoul sinks"
        );
        metrics::describe_counter!(WRITE_BYTES_TOTAL, "Bytes written by LakeSoul sinks");
        metrics::describe_counter!(WRITE_ROWS_TOTAL, "Rows written by LakeSoul sinks");
        metrics::describe_histogram!(
            COMMIT_DURATION_SECONDS,
            metrics::Unit::Seconds,
            "Duration of LakeSoul metadata commits grouped by outcome"
        );
        metrics::describe_counter!(
            COMMIT_PARTITIONS_TOTAL,
            "Partitions committed by LakeSoul metadata commits"
        );
        metrics::describe_histogram!(
            INDEX_BUILD_DURATION_SECONDS,
            metrics::Unit::Seconds,
            "Index maintenance duration grouped by index type and outcome"
        );
        metrics::describe_counter!(
            INDEX_BUILT_SHARDS_TOTAL,
            "Index shards built by index maintenance grouped by index type"
        );
    });
}

fn record_index_build(
    index_type: &'static str,
    started: std::time::Instant,
    result: &DFResult<usize>,
) {
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics::histogram!(
        INDEX_BUILD_DURATION_SECONDS,
        "index_type" => index_type,
        "outcome" => outcome,
    )
    .record(started.elapsed().as_secs_f64());
    if let Ok(built) = result {
        metrics::counter!(INDEX_BUILT_SHARDS_TOTAL, "index_type" => index_type)
            .increment(*built as u64);
    }
}

/// Records *metadata* commit latency and partitions, marking the commit
/// failed when it is dropped without [`WriteCommitMetrics::success`].
///
/// Only the `commit_data` calls belong inside: writing files before and
/// building indexes after are separate phases with their own metrics, and
/// their failures must not be reported as metadata-commit failures.
struct WriteCommitMetrics {
    started: std::time::Instant,
    partitions: u64,
    finished: bool,
}

impl WriteCommitMetrics {
    fn start() -> Self {
        describe_metrics();
        Self {
            started: std::time::Instant::now(),
            partitions: 0,
            finished: false,
        }
    }

    fn record_partitions(&mut self, partitions: u64) {
        self.partitions = partitions;
    }

    fn success(mut self) {
        self.finish("success");
    }

    fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        metrics::histogram!(COMMIT_DURATION_SECONDS, "outcome" => outcome)
            .record(self.started.elapsed().as_secs_f64());
        if outcome == "success" {
            metrics::counter!(COMMIT_PARTITIONS_TOTAL).increment(self.partitions);
        }
    }
}

impl Drop for WriteCommitMetrics {
    fn drop(&mut self) {
        self.finish("error");
    }
}

/// The LakeSoul metadata [`FileFormat`]: it presents a LakeSoul table to
/// DataFusion's listing machinery and owns the LakeSoul write path.
///
/// It is deliberately *not* a physical file reader. A LakeSoul table can hold
/// files of several physical formats at the same time (historical Parquet
/// files next to newer Vortex files), so everything that touches data files
/// dispatches through [`LakeSoulFormatRegistry`] on the format of the file at
/// hand: schema inference groups the objects by their own format, and scan
/// planning (`LakeSoulTableProvider::scan`) and the sink do the same. The
/// table's declared format only answers the table-level questions DataFusion
/// asks without a file at hand (`get_ext`, `compression_type`).
pub struct LakeSoulMetaDataFormat {
    /// The metadata client.
    client: MetaDataClientRef,
    /// The table info.
    table_info: Arc<TableInfo>,
    /// The io config.
    conf: LakeSoulIOConfig,
    /// The readers/writers of the physical formats a data file may use.
    format_registry: Arc<LakeSoulFormatRegistry>,
    /// The format the table declares for its data files. It is the default for
    /// table-level answers and for files whose path carries no known
    /// extension; never assume it for a specific file.
    physical_format: PhysicalFormat,
}

impl Debug for LakeSoulMetaDataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LakeSoulMetaDataFormat")
            .field("physical_format", &self.physical_format)
            .finish()
    }
}

impl LakeSoulMetaDataFormat {
    /// Builds the metadata format of one table.
    ///
    /// The table's declared format answers the table-level questions. The io
    /// config is not consulted: one built from files alone would answer with
    /// the extension of a single data file (or the enum default when it holds
    /// no file at all), which is a per-file fact, not the table's
    /// declaration.
    ///
    /// Fails when the stored table properties cannot be read or declare a
    /// format this build cannot write: such a table must not be presented as
    /// a Parquet one.
    pub fn new(
        client: MetaDataClientRef,
        table_info: Arc<TableInfo>,
        conf: LakeSoulIOConfig,
        format_registry: Arc<LakeSoulFormatRegistry>,
    ) -> Result<Self> {
        let table = format!("{}.{}", table_info.table_namespace, table_info.table_name);
        let physical_format = crate::catalog::table_file_format(&table_info.properties)
            .map_err(|report| report.attach(table))?;
        debug!(
            "LakeSoulMetaDataFormat::new, physical_format: {}, conf: {:?}",
            physical_format, conf
        );
        Ok(Self {
            client,
            table_info,
            conf,
            format_registry,
            physical_format,
        })
    }

    fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    /// The reader/writer of the table's declared physical format.
    fn table_format(&self) -> Arc<dyn FileFormat> {
        self.format_registry.file_format(self.physical_format)
    }

    /// The physical format of one data file, taken from its path so a table
    /// holding files of several formats dispatches per file.
    fn format_for_path(&self, path: &str) -> PhysicalFormat {
        PhysicalFormat::from_extension(path).unwrap_or_else(|_| {
            debug!(
                "file '{}' has no known physical format; assuming {}",
                path, self.physical_format
            );
            self.physical_format
        })
    }
}

#[async_trait]
impl FileFormat for LakeSoulMetaDataFormat {
    fn get_ext(&self) -> String {
        self.physical_format.extension().to_string()
    }

    /// The extension/compression pair DataFusion lists and writes with. Both
    /// come from the table's own physical format, so a Vortex table never
    /// answers with Parquet's compression behavior.
    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        self.table_format()
            .get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.table_format().compression_type()
    }

    /// Infers the schema of the files this listing was built over.
    ///
    /// The objects are grouped by the physical format of each file, because a
    /// LakeSoul table keeps files of several formats at once: every group is
    /// inferred by the format that can read those files (Vortex reads its own
    /// footer and widens mixed integer widths), and the per-group schemas are
    /// merged with LakeSoul's schema-evolution rule.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        let mut groups: Vec<(PhysicalFormat, Vec<ObjectMeta>)> = vec![];
        for object in objects {
            let physical_format = self.format_for_path(object.location.as_ref());
            match groups
                .iter_mut()
                .find(|(group_format, _)| *group_format == physical_format)
            {
                Some((_, group)) => group.push(object.clone()),
                None => groups.push((physical_format, vec![object.clone()])),
            }
        }

        let mut schemas = Vec::with_capacity(groups.len());
        for (physical_format, objects) in groups {
            let format = self.format_registry.file_format(physical_format);
            schemas.push(format.infer_schema(state, store, &objects).await?);
        }
        merge_schema_refs(schemas)
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        self.format_registry
            .file_format(self.format_for_path(object.location.as_ref()))
            .infer_stats(state, store, table_schema, object)
            .await
    }

    /// A LakeSoul read is planned by the table provider, not here.
    ///
    /// `LakeSoulTableProvider::scan` resolves the committed files from the
    /// metadata, groups them by physical format and by work unit, and builds
    /// the merge/CDC plan. Planning a scan from a `ListingTable` would
    /// duplicate that planner, and it could not serve a table whose files are
    /// of several physical formats, so it is rejected explicitly instead of
    /// silently returning unmerged rows.
    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "LakeSoul scans are planned by LakeSoulTableProvider; \
             scan the table through its provider"
        )
    }

    /// Create a physical plan for the write LakeSoul table.
    /// The overall process is as follows:
    /// 1. Check if the insert operation is overwrite.
    /// 2. Create a [`LakeSoulHashSinkExec`] for the input plan.
    /// 3. Return the physical plan.
    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if conf.insert_op == InsertOp::Overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented yet for LakeSoul tables".to_string(),
            ));
        }

        Ok(Arc::new(
            LakeSoulHashSinkExec::new(
                input,
                order_requirements,
                self.table_info(),
                self.client(),
                self.conf.object_store_options().clone(),
            )
            .await
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?,
        ) as _)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.table_format().file_source(table_schema)
    }
}

/// Execution plan for writing record batches to a LakeSoul table
pub struct LakeSoulHashSinkExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,

    /// Schema describing the structure of the output data.
    sink_schema: SchemaRef,

    /// Optional required sort order for output data.
    sort_order: Option<LexRequirement>,

    /// The table info of LakeSoul table.
    table_info: Arc<TableInfo>,

    /// The metadata client.
    metadata_client: MetaDataClientRef,

    /// The range partitions.
    range_partitions: Arc<Vec<String>>,

    /// The primary keys of the table (used for the vector index build).
    primary_keys: Vec<String>,

    /// Object store configuration options forwarded to the vector index
    /// builder after the write commits.
    object_store_options: SecretMap,

    /// The properties of the plan.
    properties: Arc<PlanProperties>,
}

impl Debug for LakeSoulHashSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulHashSinkExec schema: {:?}", self.sink_schema)
    }
}

impl LakeSoulHashSinkExec {
    /// Create a plan to write to `sink`
    pub async fn new(
        input: Arc<dyn ExecutionPlan>,
        sort_order: Option<LexRequirement>,
        table_info: Arc<TableInfo>,
        metadata_client: MetaDataClientRef,
        object_store_options: SecretMap,
    ) -> Result<Self> {
        let (range_partitions, primary_keys) =
            parse_table_info_partitions(&table_info.partitions)?;
        let range_partitions = Arc::new(range_partitions);
        Ok(Self {
            input,
            sink_schema: make_sink_schema(),
            sort_order,
            table_info,
            metadata_client,
            range_partitions,
            primary_keys,
            object_store_options,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(make_sink_schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Optional sort order for output data
    pub fn _sort_order(&self) -> &Option<LexRequirement> {
        &self.sort_order
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }

    #[instrument(
        name = "table_write_partition",
        level = "info",
        skip_all,
        fields(
            table_id = %table_info.table_id,
            table_name = %table_info.table_name,
            partition,
            range_partition_count = range_partitions.len(),
        ),
        err
    )]
    async fn pull_and_sink(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
        table_info: Arc<TableInfo>,
        range_partitions: Arc<Vec<String>>,
        write_id: String,
        partitioned_file_path_and_row_count: Arc<Mutex<PartitionedFile>>,
    ) -> Result<u64> {
        debug!("{}", input.name());
        let mut data = input.execute(partition, context.clone())?;
        // O(nm), n = number of data fields, m = number of range partitions
        let schema_projection_excluding_range = data
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(
                |(idx, field)| match range_partitions.contains(field.name()) {
                    true => None,
                    false => Some(idx),
                },
            )
            .collect::<Vec<_>>();

        let mut row_count = 0;
        let physical_format = crate::catalog::table_file_format(&table_info.properties)?;
        // One writer (and one data file) per input partition; the writer is
        // chosen by the table's physical format (parquet / vortex).
        let mut partitioned_writer =
            HashMap::<String, Box<dyn AsyncBatchWriter + Send>>::new();
        while let Some(batch) = data.next().await.transpose()? {
            debug!("write record_batch with {} rows", batch.num_rows());
            let columnar_values = get_columnar_values(&batch, range_partitions.clone())?;
            let partition_desc = columnar_values_to_partition_desc(&columnar_values);
            debug!("{partition_desc}");
            let batch_excluding_range =
                batch.project(&schema_projection_excluding_range)?;
            let file_absolute_path = format!(
                "{}{}part-{}_{:0>4}.{}",
                table_info.table_path,
                columnar_values_to_sub_path(&columnar_values),
                write_id,
                partition,
                physical_format.extension()
            );

            if !partitioned_writer.contains_key(&partition_desc) {
                debug!("create writer for partition {partition_desc}");
                let io_config = create_io_config_builder_from_table_info(
                    table_info.clone(),
                    SecretMap::new(),
                    SecretMap::new(),
                )?
                // The sink already assigns one file per input partition, so
                // disable the writer's own dynamic partitioning.
                .set_dynamic_partition(false)
                .with_files(vec![file_absolute_path])
                .with_schema(batch_excluding_range.schema())
                .build();
                let new_session =
                    Arc::new(LakeSoulIOSession::from_plain_config_and_context(
                        io_config,
                        context.clone(),
                    ));
                let writer = create_writer(new_session).await?;
                partitioned_writer.insert(partition_desc.clone(), writer);
            }

            if let Some(async_writer) = partitioned_writer.get_mut(&partition_desc) {
                row_count += batch_excluding_range.num_rows();
                async_writer
                    .write_record_batch(batch_excluding_range)
                    .await?;
            }
        }

        // TODO: apply rolling strategy
        for (partition_desc, writer) in partitioned_writer.into_iter() {
            let outputs = writer.flush_and_close().await?;
            {
                let mut partitioned_file_path_and_row_count_locked =
                    partitioned_file_path_and_row_count.lock().await;
                let entry = partitioned_file_path_and_row_count_locked
                    .entry(partition_desc.clone())
                    .or_insert_with(|| (Vec::new(), 0u64));
                for output in outputs {
                    metrics::counter!(WRITE_FILES_TOTAL, "format" => physical_format.extension())
                        .increment(1);
                    metrics::counter!(WRITE_BYTES_TOTAL, "format" => physical_format.extension())
                        .increment(output.object_meta.size);
                    entry.0.push(output.file_path);
                    entry.1 += output.row_count as u64;
                }
                // release guard
            }
        }

        metrics::counter!(WRITE_ROWS_TOTAL, "format" => physical_format.extension())
            .increment(row_count as u64);
        Ok(row_count as u64)
    }

    async fn wait_for_commit(
        join_handles: Vec<JoinHandle<Result<u64>>>,
        client: MetaDataClientRef,
        table_name: String,
        primary_keys: Vec<String>,
        object_store_options: SecretMap,
        partitioned_file_path_and_row_count: Arc<Mutex<PartitionedFile>>,
    ) -> Result<u64> {
        let count = futures::future::join_all(join_handles)
            .await
            .iter()
            .try_fold(0u64, |counter, result| match &result {
                Ok(Ok(count)) => Ok(counter + count),
                Ok(Err(e)) => Err(DataFusionError::Execution(format!("{}", e))),
                Err(e) => Err(DataFusionError::Execution(format!("{}", e))),
            })?;
        let partitioned_file_path_and_row_count =
            partitioned_file_path_and_row_count.lock().await;

        // Timed and finalized around the `commit_data` loop alone: the file
        // writers above and the index maintenance below are not part of the
        // metadata commit, and index outcomes have their own metrics.
        let mut commit_metrics = WriteCommitMetrics::start();
        commit_metrics
            .record_partitions(partitioned_file_path_and_row_count.len() as u64);

        let commit_started = std::time::Instant::now();
        for (partition_desc, (files, _)) in partitioned_file_path_and_row_count.iter() {
            commit_data(client.clone(), &table_name, partition_desc.clone(), files)
                .await?;
            debug!(
                "table: {} insert success at {:?}",
                &table_name,
                std::time::SystemTime::now()
            )
        }
        debug!(
            elapsed_ms = commit_started.elapsed().as_secs_f64() * 1000.0,
            "committed metadata for {}", &table_name
        );
        commit_metrics.success();

        // Auto-build / incrementally update the vector index from the newly
        // committed files, driven by the table's `vector_index_columns`
        // property (same semantics as the Python write path).  Re-fetch the
        // table info so properties changed after plan construction apply.
        // The native builder's future is not Send, so it is driven on the
        // blocking pool via the global runtime.
        let committed_files: HashMap<String, (Vec<String>, u64)> =
            partitioned_file_path_and_row_count.clone();
        drop(partitioned_file_path_and_row_count);

        let table_ref = TableReference::from(table_name.as_str());
        let namespace = table_ref.schema().unwrap_or("default").to_string();

        let (configs, text_configs, deferred) = if let Some(fresh_info) = client
            .get_table_info_by_table_name(table_ref.table(), &namespace)
            .await?
        {
            let deferred = serde_json::from_str::<crate::catalog::LakeSoulTableProperty>(
                &fresh_info.properties,
            )
            .ok()
            .and_then(|properties| properties.index_maintenance)
            .is_some_and(|mode| mode.eq_ignore_ascii_case("deferred"));
            (
                crate::vector_index::parse_vector_index_from_table_properties(
                    &fresh_info.properties,
                )
                .map_err(|report| DataFusionError::External(report.into_boxed_error()))?,
                crate::text_index::parse_text_index_from_table_properties(
                    &fresh_info.properties,
                )
                .map_err(|report| DataFusionError::External(report.into_boxed_error()))?,
                deferred,
            )
        } else {
            (Vec::new(), Vec::new(), false)
        };

        // A deferred table commits data only; the embedder builds the
        // pending shards out of band from the recorded data-file coverage
        // (see lakesoul_common::index::pending_shard_files).
        if deferred {
            debug!("index maintenance deferred for {}", &table_name);
            return Ok(count);
        }

        // One active-files listing serves both auto-rebuild policies.
        let wants_vector_rebuild = !configs.is_empty()
            && !primary_keys.is_empty()
            && configs
                .iter()
                .any(|c| c.management.rebuild_mode.eq_ignore_ascii_case("auto"));
        let wants_text_rebuild = !text_configs.is_empty()
            && !primary_keys.is_empty()
            && text_configs
                .iter()
                .any(|c| c.management.rebuild_mode.eq_ignore_ascii_case("auto"));
        let active_files: Option<Vec<String>> =
            if wants_vector_rebuild || wants_text_rebuild {
                client
                    .get_data_files_by_table_name(table_ref.table(), &namespace)
                    .await
                    .ok()
            } else {
                None
            };

        // Text and vector index maintenance are independent; build them
        // concurrently instead of serializing two blocking builds.
        let text_committed_files = committed_files.clone();
        let text_primary_keys = primary_keys.clone();
        let text_object_store_options = object_store_options.clone();
        let vector_active_files = active_files.clone();
        let vector_configs = configs;
        let vector_primary_keys = primary_keys.clone();
        let vector_object_store_options = object_store_options.clone();
        let vector_catalog = client.vector_index_catalog();
        let text_active_files = active_files;
        let text_catalog = client.index_catalog::<lakesoul_text::TextSplitEntry>(
            lakesoul_common::IndexKind::Text,
        );
        let vector_config_count = vector_configs.len();
        let vector_build = async {
            if vector_configs.is_empty() || vector_primary_keys.is_empty() {
                return Ok(0usize);
            }
            let active = if wants_vector_rebuild {
                vector_active_files
            } else {
                None
            };
            let started = std::time::Instant::now();
            let span = tracing::Span::current();
            let built = tokio::task::spawn_blocking(move || {
                let _entered = span.enter();
                lakesoul_io::session::GLOBAL_RUNTIME.block_on(
                    crate::vector_index::auto_build_vector_index(
                        &vector_configs,
                        &vector_primary_keys,
                        &vector_object_store_options,
                        &committed_files,
                        active.as_deref(),
                        &vector_catalog,
                    ),
                )
            })
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "vector index auto build task failed: {error}"
                ))
            })
            .and_then(|built| {
                built.map_err(|report| {
                    DataFusionError::External(report.into_boxed_error())
                })
            });
            record_index_build("vector", started, &built);
            let built = built?;
            debug!(
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "auto-built {built} vector index shard(s) for {}", &table_name
            );
            tracing::Span::current().record("built_shard_count", built as u64);
            Ok::<usize, DataFusionError>(built)
        }
        .instrument(info_span!(
            "vector_index_maintenance",
            config_count = vector_config_count,
            rebuild = wants_vector_rebuild,
            built_shard_count = tracing::field::Empty,
        ));
        let text_config_count = text_configs.len();
        let text_build = async {
            if text_configs.is_empty() || text_primary_keys.is_empty() {
                return Ok(0usize);
            }
            let active = if wants_text_rebuild {
                text_active_files
            } else {
                None
            };
            let started = std::time::Instant::now();
            let span = tracing::Span::current();
            let built = tokio::task::spawn_blocking(move || {
                let _entered = span.enter();
                lakesoul_io::session::GLOBAL_RUNTIME.block_on(
                    crate::text_index::auto_build_text_index(
                        &text_configs,
                        &text_primary_keys,
                        &text_object_store_options,
                        &text_committed_files,
                        active.as_deref(),
                        &text_catalog,
                    ),
                )
            })
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "text index auto build task failed: {error}"
                ))
            })
            .and_then(|built| {
                built.map_err(|report| {
                    DataFusionError::External(report.into_boxed_error())
                })
            });
            record_index_build("text", started, &built);
            let built = built?;
            debug!(
                elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
                "auto-built {built} text index shard(s) for {}", &table_name
            );
            tracing::Span::current().record("built_shard_count", built as u64);
            Ok::<usize, DataFusionError>(built)
        }
        .instrument(info_span!(
            "text_index_maintenance",
            config_count = text_config_count,
            rebuild = wants_text_rebuild,
            built_shard_count = tracing::field::Empty,
        ));
        let (vector_built, text_built) = tokio::join!(vector_build, text_build);
        vector_built?;
        text_built?;
        Ok(count)
    }
}

impl DisplayAs for LakeSoulHashSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulHashSinkExec")
    }
}

impl ExecutionPlanProperties for LakeSoulHashSinkExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.properties.partitioning
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.properties.eq_properties
    }
}

impl ExecutionPlan for LakeSoulHashSinkExec {
    fn name(&self) -> &str {
        "LakeSoulHashSinkExec"
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.sink_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time, and so requires a single input partition.
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // The input order is either explicitly set (such as by a ListingTable),
        // or require that the [FileSinkExec] gets the data in the order the
        // input produced it (otherwise the optimizer may choose to reorder
        // the input which could result in unintended / poor UX)
        //
        // More rationale:
        // https://github.com/apache/arrow-datafusion/pull/6354#discussion_r1195284178
        match &self.sort_order {
            Some(requirements) => {
                vec![Some(OrderingRequirements::Soft(vec![requirements.clone()]))] // TODO check this
            }
            None => vec![],
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time.
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: if children.is_empty() {
                self.input.clone()
            } else {
                children[0].clone()
            },
            sink_schema: self.sink_schema.clone(),
            sort_order: self.sort_order.clone(),
            table_info: self.table_info.clone(),
            range_partitions: self.range_partitions.clone(),
            metadata_client: self.metadata_client.clone(),
            primary_keys: self.primary_keys.clone(),
            object_store_options: self.object_store_options.clone(),
            properties: self.properties.clone(),
        }))
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Preserve scoped subscribers as well as the current span across the
        // spawned write tasks. A Tokio task may be polled after moving away
        // from the thread where `execute` was called.
        let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
        let span = info_span!(
            "hash_sink_execute",
            table_id = %self.table_info.table_id,
            table_name = %self.table_info.table_name,
            partition,
            input_partition_count = tracing::field::Empty,
            outcome = tracing::field::Empty,
            output_batches = tracing::field::Empty,
            output_rows = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let entered = span.enter();

        if partition != 0 {
            return Err(DataFusionError::NotImplemented(
                "FileSinkExec can only be called on partition 0!".to_string(),
            ));
        }
        let num_input_partitions = self.input.output_partitioning().partition_count();
        span.record("input_partition_count", num_input_partitions as u64);
        debug!(num_input_partitions, "starting table sink");
        // launch one async task per *input* partition
        let mut join_handles = vec![];

        let write_id = rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 16);

        let partitioned_file_path_and_row_count =
            Arc::new(Mutex::new(HashMap::<String, (Vec<String>, u64)>::new()));
        for i in 0..num_input_partitions {
            // `#[instrument]` creates its span on the first poll. Carry both
            // the parent span and its dispatcher into the spawned task so the
            // child span remains attached even with a scoped subscriber.
            let sink_task = tokio::spawn(
                Self::pull_and_sink(
                    self.input().clone(),
                    i,
                    context.clone(),
                    self.table_info(),
                    self.range_partitions.clone(),
                    write_id.clone(),
                    partitioned_file_path_and_row_count.clone(),
                )
                .instrument(span.clone())
                .with_subscriber(dispatch.clone()),
            );
            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            join_handles.push(sink_task);
        }

        let table_ref = TableReference::Partial {
            schema: self.table_info().table_namespace.clone().into(),
            table: self.table_info().table_name.clone().into(),
        };
        let table_name = table_ref.to_string();
        // Create the commit span before spawning. Besides assigning its parent
        // deterministically, this avoids making span creation depend on which
        // executor thread first polls the commit future.
        let commit_span = info_span!(
            parent: &span,
            "table_write_commit",
            table_name = %table_name,
            input_partition_count = join_handles.len(),
            primary_key_count = self.primary_keys.len(),
            outcome = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let commit_result_span = commit_span.clone();
        let commit_future = Self::wait_for_commit(
            join_handles,
            self.metadata_client(),
            table_name,
            self.primary_keys.clone(),
            self.object_store_options.clone(),
            partitioned_file_path_and_row_count,
        );
        let join_handle = tokio::spawn(
            async move {
                let result = commit_future.await;
                match &result {
                    Ok(_) => {
                        commit_result_span.record("outcome", "success");
                    }
                    Err(error) => {
                        commit_result_span.record("outcome", "error");
                        commit_result_span
                            .record("error", tracing::field::display(error));
                        tracing::error!(
                            parent: &commit_result_span,
                            error = %error,
                            "table write commit failed"
                        );
                    }
                }
                result
            }
            .instrument(commit_span)
            .with_subscriber(dispatch),
        );

        let sink_schema = self.sink_schema.clone();

        let stream = futures::stream::once(async move {
            match join_handle.await {
                Ok(Ok(count)) => Ok(make_sink_batch(count, String::from(""))),
                Ok(Err(report)) => {
                    debug!("{report}");
                    Err(DataFusionError::External(report.into_boxed_error()))
                }
                Err(e) => {
                    debug!("{e}");
                    Err(DataFusionError::Execution(e.to_string()))
                }
            }
        })
        .boxed();

        let stream = Box::pin(RecordBatchStreamAdapter::new(sink_schema, stream))
            as SendableRecordBatchStream;
        drop(entered);
        Ok(instrument_record_batch_stream(stream, span, "hash_sink"))
    }
}

fn make_sink_batch(count: u64, msg: String) -> RecordBatch {
    let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    let msg_array = Arc::new(StringArray::from(vec![msg])) as ArrayRef;
    RecordBatch::try_from_iter_with_nullable(vec![
        ("count", count_array, false),
        ("msg", msg_array, false),
    ])
    .unwrap()
}

fn make_sink_schema() -> SchemaRef {
    // define a schema.
    Arc::new(Schema::new(vec![
        Field::new("count", DataType::UInt64, false),
        Field::new("msg", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesoul_metadata::MetaDataClient;

    fn table_info(properties: &str) -> Arc<TableInfo> {
        Arc::new(TableInfo {
            table_namespace: "default".to_string(),
            table_name: "format_metadata".to_string(),
            properties: properties.to_string(),
            ..Default::default()
        })
    }

    /// Unusable stored format metadata must fail the construction instead of
    /// being presented to DataFusion as a Parquet table. An absent
    /// `file_format` property is not an error: it means the connector default.
    #[tokio::test(flavor = "multi_thread")]
    async fn construction_rejects_unusable_table_format_metadata() {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let registry = Arc::new(
            LakeSoulFormatRegistry::new(LakeSoulIOConfig::default(), false).unwrap(),
        );

        for properties in [
            // a format this build has no reader/writer for
            r#"{"file_format": "orc"}"#,
            // properties that are not the table property JSON at all
            "{not json",
        ] {
            let error = LakeSoulMetaDataFormat::new(
                client.clone(),
                table_info(properties),
                LakeSoulIOConfig::default(),
                registry.clone(),
            )
            .expect_err("unusable format metadata must not build a format");
            assert!(
                error.to_string().contains("format_metadata"),
                "the error must name the table, got: {error}"
            );
        }

        let format = LakeSoulMetaDataFormat::new(
            client,
            table_info("{}"),
            LakeSoulIOConfig::default(),
            registry,
        )
        .expect("a table without a file_format property follows the default format");
        assert_eq!(format.get_ext(), "vortex");
    }
}

/// The sink spawns its write and commit tasks with `tokio::spawn`. A spawned
/// future does not inherit a scoped subscriber, and an `#[instrument]` async fn
/// builds its span on the first poll. The sink must therefore carry its span
/// and dispatcher into both tasks so the write, metadata commit, and nested
/// index maintenance stay in the statement trace.
#[cfg(test)]
mod span_tests {
    use super::*;
    use datafusion::physical_plan::empty::EmptyExec;
    use futures::TryStreamExt;
    use lakesoul_metadata::MetaDataClient;
    use std::sync::Mutex as StdMutex;
    use tracing::subscriber::with_default;
    use tracing_subscriber::Layer;
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

    /// `(span name, parent span name)` of every span created on this thread.
    type CreatedSpans = Arc<StdMutex<Vec<(String, Option<String>)>>>;

    #[derive(Clone, Default)]
    struct CapturedSpans(CreatedSpans);

    impl CapturedSpans {
        fn parent_of(&self, name: &str) -> Option<Option<String>> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .find(|(span, _)| span == name)
                .map(|(_, parent)| parent.clone())
        }
    }

    impl<S> Layer<S> for CapturedSpans
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            ctx: LayerContext<'_, S>,
        ) {
            let parent = ctx
                .span(id)
                .and_then(|span| span.parent().map(|parent| parent.name().to_string()));
            self.0
                .lock()
                .unwrap()
                .push((attrs.metadata().name().to_string(), parent));
        }
    }

    /// Both spawned tasks must be instrumented with the sink's span before
    /// they are spawned, so that the whole write -- metadata commit and index
    /// maintenance included -- stays in the statement trace.
    #[test]
    fn spawned_write_and_commit_tasks_join_the_sink_span() {
        let captured = CapturedSpans::default();
        let subscriber = Registry::default().with(captured.clone());

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        with_default(subscriber, || {
            runtime.block_on(async {
                let client = Arc::new(MetaDataClient::from_env().await.unwrap());
                let input: Arc<dyn ExecutionPlan> =
                    Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
                let sink = LakeSoulHashSinkExec::new(
                    input,
                    None,
                    Arc::new(TableInfo {
                        partitions: ";".to_string(),
                        properties: "{}".to_string(),
                        ..Default::default()
                    }),
                    client,
                    SecretMap::new(),
                )
                .await
                .unwrap();

                let statement = info_span!("statement_execute");
                let stream = {
                    let _entered = statement.enter();
                    sink.execute(0, Arc::new(TaskContext::default())).unwrap()
                };
                // The sink stream completes once the commit task has joined the
                // per-partition write tasks, so both spawned spans exist by then.
                let _: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            });
        });
        assert_eq!(
            captured.parent_of("hash_sink_execute"),
            Some(Some("statement_execute".to_string())),
            "the sink span must hang off the statement span"
        );
        assert_eq!(
            captured.parent_of("table_write_partition"),
            Some(Some("hash_sink_execute".to_string())),
            "the production write spawn must join the sink trace"
        );
        assert_eq!(
            captured.parent_of("table_write_commit"),
            Some(Some("hash_sink_execute".to_string())),
            "the production commit spawn must join the sink trace"
        );
    }
}

/// The commit metric covers the metadata commit alone. Writing the files
/// before it and maintaining the indexes after it are separate phases with
/// their own metrics, so neither a slow file writer nor a failed index build
/// may show up as commit latency or as a failed commit.
#[cfg(test)]
mod commit_metrics_tests {
    use super::*;
    use crate::tests::create_table_with_vector_index;
    use crate::vector_index::{VectorIndexParams, VectorIndexTableConfig};
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_metadata::MetaDataClient;
    use metrics_exporter_prometheus::PrometheusBuilder;

    /// Value of the `prefix{labels}` sample in Prometheus text exposition.
    fn sample(text: &str, prefix: &str, labels: &[&str]) -> Option<f64> {
        text.lines()
            .find(|line| {
                line.starts_with(prefix)
                    && labels.iter().all(|label| line.contains(label))
            })
            .and_then(|line| line.rsplit(' ').next()?.parse().ok())
    }

    /// The recorder is thread-local, so the commit has to run on this thread.
    fn current_thread_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime")
    }

    fn client(runtime: &tokio::runtime::Runtime) -> MetaDataClientRef {
        runtime.block_on(async { Arc::new(MetaDataClient::from_env().await.unwrap()) })
    }

    #[test]
    fn commit_latency_excludes_the_file_writers() {
        let runtime = current_thread_runtime();
        let client = client(&runtime);
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        let count = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                let writer: JoinHandle<Result<u64>> = tokio::spawn(async {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    Ok(3u64)
                });
                LakeSoulHashSinkExec::wait_for_commit(
                    vec![writer],
                    client.clone(),
                    "default.tmp_commit_metrics_absent".to_string(),
                    Vec::new(),
                    SecretMap::new(),
                    Arc::new(Mutex::new(PartitionedFile::new())),
                )
                .await
            })
        })
        .expect("committing without partitions succeeds");
        assert_eq!(count, 3, "the writers' row count is still returned");

        let rendered = handle.render();
        let latency = sample(
            &rendered,
            &format!("{COMMIT_DURATION_SECONDS}_sum"),
            &["outcome=\"success\""],
        )
        .expect("a successful commit is recorded");
        assert!(
            latency < 0.5,
            "commit latency {latency}s must not include the 1s file writer \
             wait:\n{rendered}"
        );
    }

    #[test]
    fn index_failure_is_not_reported_as_a_commit_failure() {
        let runtime = current_thread_runtime();
        let client = client(&runtime);
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        let table = "tmp_commit_metrics_index_failure";
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    4,
                ),
                true,
            ),
        ]));
        let config = LakeSoulIOConfigBuilder::new()
            .with_schema(schema)
            .with_primary_keys(vec!["pk".to_string()])
            .build();
        // Stored index metadata parses, but the build it describes must fail:
        // the native builder rejects this metric.
        let vector_index_configs = vec![VectorIndexTableConfig {
            column: "vec".to_string(),
            params: VectorIndexParams {
                dim: 4,
                nlist: 4,
                total_bits: 7,
                metric: "not-a-metric".to_string(),
                rotator_type: "FhtKac".to_string(),
                seed: 42,
                use_faster_config: true,
            },
            management: Default::default(),
        }];

        let result = metrics::with_local_recorder(&recorder, || {
            runtime.block_on(async {
                create_table_with_vector_index(
                    client.clone(),
                    table,
                    config,
                    &vector_index_configs,
                )
                .await
                .expect("create the table");
                let mut partitions = PartitionedFile::new();
                partitions.insert(
                    "hash=0".to_string(),
                    (
                        vec![
                            "file:///tmp/tmp_commit_metrics_index_failure.parquet"
                                .to_string(),
                        ],
                        1,
                    ),
                );
                LakeSoulHashSinkExec::wait_for_commit(
                    Vec::new(),
                    client.clone(),
                    format!("default.{table}"),
                    vec!["pk".to_string()],
                    SecretMap::new(),
                    Arc::new(Mutex::new(partitions)),
                )
                .await
            })
        });
        assert!(
            result.is_err(),
            "the index maintenance failure must still fail the write: \
             {result:?}"
        );

        let rendered = handle.render();
        let commit_outcomes = |outcome: &str| {
            sample(
                &rendered,
                &format!("{COMMIT_DURATION_SECONDS}_count"),
                &[&format!("outcome=\"{outcome}\"")],
            )
        };
        assert_eq!(
            commit_outcomes("success"),
            Some(1.0),
            "the metadata commit succeeded:\n{rendered}"
        );
        assert_eq!(
            commit_outcomes("error"),
            None,
            "a failed index build is not a failed commit:\n{rendered}"
        );
        assert_eq!(
            sample(&rendered, COMMIT_PARTITIONS_TOTAL, &[]),
            Some(1.0),
            "the committed partition is counted:\n{rendered}"
        );
        assert_eq!(
            sample(
                &rendered,
                &format!("{INDEX_BUILD_DURATION_SECONDS}_count"),
                &["index_type=\"vector\"", "outcome=\"error\""],
            ),
            Some(1.0),
            "the index outcome is recorded by the index metric:\n{rendered}"
        );
    }
}
