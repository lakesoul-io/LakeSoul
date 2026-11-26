// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! One-call IO interface for compacting a set of files into one output file.
//!
//! The function plans the merge from metadata and primary keys, then uses the
//! cheapest safe path:
//!
//! * a fully superseded-file keep is copied byte-for-byte when every row of the
//!   kept file survives and the output format matches;
//! * a block-copyable Vortex group is copied by
//!   [`crate::compaction::copy_vortex_ranges`];
//! * everything else goes through the normal merge-on-read reader and writer.
//!
//! The API is IO-only: it does not commit metadata, does not discard files in
//! the catalog, and does not touch the Java/JNI layer.

use arrow::datatypes::SchemaRef;
use rootcause::report;

use crate::Result;
use crate::compaction::analyze::{FileFormat, read_file_schema, summarize_files};
use crate::compaction::copy::copy_vortex_ranges;
use crate::compaction::plan::{
    CompactionMode, CompactionOptions, CompactionPlan, FileAction,
    plan_compaction_with_config,
};
use crate::config::{
    IOSchema, LakeSoulIOConfig, LakeSoulIOConfigBuilder, OPTION_KEY_CDC_COLUMN,
    OPTION_KEY_IS_COMPACTED, OPTION_KEY_PHYSICAL_FORMAT, OPTION_KEY_SKIP_MERGE_ON_READ,
};
use crate::file_format::{PhysicalFormat, merge_schema_refs};
use crate::reader::LakeSoulReader;
use crate::writer::create_writer_with_io_config;

/// Options for [`compact_to_one_file`].
#[derive(Clone)]
pub struct CompactToOneOptions {
    /// Merge key.  Input files are expected in version order (oldest first).
    pub primary_keys: Vec<String>,
    /// Reader configuration supplying object-store credentials, partition
    /// defaults and native-reader options.  Its file list, primary keys and
    /// target schema are replaced by this call.
    pub reader_config: Option<LakeSoulIOConfig>,
    /// Writer configuration supplying compression/layout options.  Its file
    /// list, primary keys, target schema and physical format are replaced.
    pub writer_config: Option<LakeSoulIOConfig>,
    /// Full output schema.  When `None`, the reader config's target schema is
    /// used if non-empty, otherwise the input file schemas are merged.
    pub schema: Option<SchemaRef>,
    /// Worker-local context: shares registered stores, function registries and
    /// the execution memory pool. Never serialized in a task descriptor.
    pub task_context: Option<std::sync::Arc<datafusion::execution::TaskContext>>,
}

impl CompactToOneOptions {
    pub fn new(primary_keys: Vec<String>) -> Self {
        Self {
            primary_keys,
            reader_config: None,
            writer_config: None,
            schema: None,
            task_context: None,
        }
    }

    pub fn with_reader_config(mut self, reader_config: LakeSoulIOConfig) -> Self {
        self.reader_config = Some(reader_config);
        self
    }

    pub fn with_writer_config(mut self, writer_config: LakeSoulIOConfig) -> Self {
        self.writer_config = Some(writer_config);
        self
    }

    pub fn with_task_context(
        mut self,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Self {
        self.task_context = Some(context);
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// What [`compact_to_one_file`] actually did.
#[derive(Debug, Clone)]
pub struct CompactToOneResult {
    /// Path used for the output.
    pub mode: CompactionMode,
    pub input_files: usize,
    pub output_rows: u64,
    pub output_bytes: u64,
    /// Actual columns present in the output, suitable for metadata publication.
    pub file_exist_cols: Vec<String>,
    pub discarded_files: usize,
    /// Why the planner's first choice was not used, when it was overridden.
    pub fallback_reason: Option<String>,
}

/// Merge `input_files` into `output_file`.
///
/// The output physical format is inferred from the output file extension:
/// `.parquet`, `.vortex` or `.vortex-compact`.  The function always produces
/// exactly one output file.  A single input file is still merged so duplicate
/// keys are deduplicated.
///
/// Plain filesystem paths retain the local file/block-copy optimizer. URI
/// inputs or output (including S3) use the native object-store merge reader
/// and writer; remote raw block copy is not attempted.
pub async fn compact_to_one_file(
    input_files: &[String],
    output_file: &str,
    options: &CompactToOneOptions,
) -> Result<CompactToOneResult> {
    if input_files.is_empty() {
        return Err(report!(
            "compact_to_one_file requires at least one input file"
        ));
    }
    if options.primary_keys.is_empty() {
        return Err(report!(
            "compact_to_one_file requires at least one primary key"
        ));
    }
    if input_files.iter().any(|path| path == output_file) {
        return Err(report!(
            "compaction output must not overwrite an input path"
        ));
    }
    if input_files.iter().any(|path| path.contains("://")) || output_file.contains("://")
    {
        return merge_to_one_file(
            input_files,
            output_file,
            options,
            0,
            Some("object-store input/output uses native read-merge-write".into()),
        )
        .await;
    }
    let output_format = PhysicalFormat::from_extension(output_file)
        .map_err(|error| report!("output file {output_file}: {error}"))?;
    let schema = resolve_schema(input_files, options).await?;
    let summary = summarize_files(input_files, &options.primary_keys).await?;

    let mut plan_options = CompactionOptions::new(options.primary_keys.clone());
    plan_options.require_single_file = true;
    // Raw block copy writes Vortex only; Parquet output always goes through
    // the merge-on-read writer.
    plan_options.metadata_only = output_format == PhysicalFormat::Parquet;
    let plan = plan_compaction_with_config(
        &summary,
        &plan_options,
        options.reader_config.as_ref(),
    )
    .await?;

    // A single surviving file can be copied only when every one of its rows
    // survives and the output keeps the same physical format.  A single input
    // file is not copied: KeepOnly does not prove that the file has no
    // duplicate keys, so it goes through the merge below.
    if plan.mode == CompactionMode::FileLevel
        && plan.stats.keep_files == 1
        && let Some(kept) = plan
            .files
            .iter()
            .find(|file| file.action != FileAction::Discard)
    {
        let all_rows_survive = plan.stats.winner_rows == summary.files[kept.index].rows;
        let same_format =
            FileFormat::from_path(&kept.path)? == FileFormat::from_path(output_file)?;
        if all_rows_survive && same_format {
            tokio::fs::copy(&kept.path, output_file)
                .await
                .map_err(|error| {
                    report!("copy {} to {output_file}: {error}", kept.path)
                })?;
            let bytes = tokio::fs::metadata(output_file)
                .await
                .map_err(|error| report!("stat {output_file}: {error}"))?
                .len();
            return Ok(CompactToOneResult {
                mode: CompactionMode::FileLevel,
                input_files: input_files.len(),
                output_rows: plan.stats.winner_rows,
                output_bytes: bytes,
                file_exist_cols: read_file_schema(output_file)
                    .await?
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect(),
                discarded_files: plan.stats.discard_files,
                fallback_reason: None,
            });
        }
    }

    if plan.mode == CompactionMode::BlockLevel
        && output_format != PhysicalFormat::Parquet
        && let Some(block_plan) = &plan.block_plan
    {
        match copy_vortex_ranges(input_files, &block_plan.ranges, output_file).await {
            Ok(bytes) => {
                return Ok(CompactToOneResult {
                    mode: CompactionMode::BlockLevel,
                    input_files: input_files.len(),
                    output_rows: plan.stats.winner_rows,
                    output_bytes: bytes,
                    file_exist_cols: read_file_schema(output_file)
                        .await?
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect(),
                    discarded_files: plan.stats.discard_files,
                    fallback_reason: None,
                });
            }
            Err(error) => {
                return merge_fallback(
                    input_files,
                    output_file,
                    &schema,
                    options,
                    &plan,
                    Some(format!("raw vortex block copy failed: {error}")),
                )
                .await;
            }
        }
    }

    let reason = if plan.mode == CompactionMode::KeepOnly {
        Some("single input file is merged to deduplicate".to_string())
    } else if plan.mode == CompactionMode::FileLevel {
        Some("kept file is rewritten so the output is fully merged".to_string())
    } else if output_format == PhysicalFormat::Parquet {
        Some("block copy is Vortex-only; output is merged".to_string())
    } else {
        None
    };
    merge_fallback(input_files, output_file, &schema, options, &plan, reason).await
}

async fn resolve_schema(
    input_files: &[String],
    options: &CompactToOneOptions,
) -> Result<SchemaRef> {
    if let Some(schema) = &options.schema {
        return Ok(schema.clone());
    }
    if let Some(config) = &options.reader_config {
        let schema = config.target_schema();
        if !schema.fields().is_empty() {
            return Ok(schema);
        }
    }
    let mut schemas = Vec::with_capacity(input_files.len());
    for file in input_files {
        schemas.push(read_file_schema(file).await?);
    }
    merge_schema_refs(schemas).map_err(|error| report!("merge input schemas: {error}"))
}

async fn merge_fallback(
    input_files: &[String],
    output_file: &str,
    schema: &SchemaRef,
    options: &CompactToOneOptions,
    plan: &CompactionPlan,
    fallback_reason: Option<String>,
) -> Result<CompactToOneResult> {
    let options = options.clone().with_schema(schema.clone());
    merge_to_one_file(
        input_files,
        output_file,
        &options,
        plan.stats.discard_files,
        fallback_reason,
    )
    .await
}

/// Object-store capable merge path. File metadata comes from FlushOutput,
/// never local `stat`, and input files are never moved or deleted.
async fn merge_to_one_file(
    input_files: &[String],
    output_file: &str,
    options: &CompactToOneOptions,
    discarded_files: usize,
    fallback_reason: Option<String>,
) -> Result<CompactToOneResult> {
    let mut reader_config = options
        .reader_config
        .clone()
        .unwrap_or_else(|| LakeSoulIOConfigBuilder::new().build());
    reader_config.files = input_files.to_vec();
    reader_config.primary_keys = options.primary_keys.clone();
    // A compaction keeps tombstones and cannot apply a user's query filter.
    reader_config.filter_strs.clear();
    reader_config.filters.clear();
    reader_config.filter_protos.clear();
    reader_config.filter_buf.clear();
    // The normal query reader filters tombstones after merging. Keep the
    // CDC column in the target schema and leave that filter disabled here so
    // deletes can still suppress rows in files outside this merge group.
    reader_config.options.remove(OPTION_KEY_CDC_COLUMN);
    reader_config.options.remove(OPTION_KEY_SKIP_MERGE_ON_READ);
    reader_config.options.remove(OPTION_KEY_IS_COMPACTED);
    // The reader needs a non-empty target schema: an empty one makes the scan
    // project zero columns and the writer then cannot find its sort keys.
    // Resolve it from an explicit option, an existing reader schema, or the
    // object-store listing (which also works for URI inputs).
    let schema = match &options.schema {
        Some(schema) => schema.clone(),
        None if !reader_config.target_schema.0.fields().is_empty() => {
            reader_config.target_schema.0.clone()
        }
        None => {
            let session =
                crate::session::LakeSoulIOSession::try_new(reader_config.clone())
                    .map_err(|error| report!("resolve merge schema: {error}"))?;
            let table_schema = session
                .get_table_schema()
                .await
                .map_err(|error| report!("resolve merge schema: {error}"))?;
            std::sync::Arc::clone(table_schema.file_schema())
        }
    };
    reader_config.target_schema = IOSchema(schema);

    let mut reader = match &options.task_context {
        Some(context) => LakeSoulReader::new_with_io_session(
            crate::session::LakeSoulIOSession::from_plain_config_and_context(
                reader_config,
                context.clone(),
            ),
        ),
        None => LakeSoulReader::new(reader_config),
    }
    .map_err(|error| report!("open merge reader: {error}"))?;
    reader
        .start()
        .await
        .map_err(|error| report!("start merge reader: {error}"))?;
    let schema = reader
        .schema
        .clone()
        .filter(|schema| !schema.fields().is_empty())
        .ok_or_else(|| report!("merge reader did not resolve a schema"))?;
    let output_format = PhysicalFormat::from_extension(output_file)
        .map_err(|error| report!("output file {output_file}: {error}"))?;
    let mut writer_config = options
        .writer_config
        .clone()
        .unwrap_or_else(|| LakeSoulIOConfigBuilder::new().build());
    writer_config.files = vec![output_file.to_string()];
    writer_config.primary_keys = options.primary_keys.clone();
    writer_config.target_schema = IOSchema(schema);
    writer_config.options.insert(
        OPTION_KEY_PHYSICAL_FORMAT.to_string(),
        output_format.to_string(),
    );
    let mut writer = match &options.task_context {
        Some(context) => {
            crate::writer::create_writer(std::sync::Arc::new(
                crate::session::LakeSoulIOSession::from_plain_config_and_context(
                    writer_config,
                    context.clone(),
                ),
            ))
            .await
        }
        None => create_writer_with_io_config(writer_config).await,
    }
    .map_err(|error| report!("open merge writer: {error}"))?;
    let mut rows = 0u64;
    let write_result = async {
        while let Some(batch) = reader.next_rb().await {
            let batch = batch.map_err(|error| report!("read merge input: {error}"))?;
            rows += batch.num_rows() as u64;
            writer
                .write_record_batch(batch)
                .await
                .map_err(|error| report!("write merge output: {error}"))?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = write_result {
        if let Err(abort_error) = writer.abort_and_close().await {
            tracing::warn!(%abort_error, %output_file, "failed to abort compaction writer");
        }
        return Err(error);
    }
    let mut outputs = writer
        .flush_and_close()
        .await
        .map_err(|error| report!("flush merge output: {error}"))?;
    if outputs.len() != 1 {
        return Err(report!(
            "single-file compaction produced {} output files",
            outputs.len()
        ));
    }
    let output = outputs.remove(0);
    Ok(CompactToOneResult {
        mode: CompactionMode::Fallback,
        input_files: input_files.len(),
        output_rows: rows,
        output_bytes: output.object_meta.size,
        file_exist_cols: output.file_exist_cols,
        discarded_files,
        fallback_reason,
    })
}
