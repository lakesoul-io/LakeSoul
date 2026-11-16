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
}

impl CompactToOneOptions {
    pub fn new(primary_keys: Vec<String>) -> Self {
        Self {
            primary_keys,
            reader_config: None,
            writer_config: None,
            schema: None,
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
/// Paths are local filesystem paths; object-store planning/raw copy is a
/// separate integration point.
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
    let mut reader_config = match &options.reader_config {
        Some(config) => config.clone(),
        None => LakeSoulIOConfigBuilder::new().build(),
    };
    reader_config.files = input_files.to_vec();
    reader_config.primary_keys = options.primary_keys.clone();
    reader_config.target_schema = IOSchema(schema.clone());
    // This call merges the requested files; filters, tombstone filtering and
    // merge skipping belong to a table read, not to compaction.
    reader_config.filter_strs.clear();
    reader_config.filters.clear();
    reader_config.filter_protos.clear();
    reader_config.filter_buf.clear();
    reader_config.options.remove(OPTION_KEY_CDC_COLUMN);
    reader_config.options.remove(OPTION_KEY_SKIP_MERGE_ON_READ);
    reader_config.options.remove(OPTION_KEY_IS_COMPACTED);

    let output_format = PhysicalFormat::from_extension(output_file)
        .map_err(|error| report!("output file {output_file}: {error}"))?;
    let mut writer_config = match &options.writer_config {
        Some(config) => config.clone(),
        None => LakeSoulIOConfigBuilder::new().build(),
    };
    writer_config.files = vec![output_file.to_string()];
    writer_config.primary_keys = options.primary_keys.clone();
    writer_config.target_schema = IOSchema(schema.clone());
    writer_config.options.insert(
        OPTION_KEY_PHYSICAL_FORMAT.to_string(),
        output_format.to_string(),
    );

    let _ = tokio::fs::remove_file(output_file).await;
    let mut reader = LakeSoulReader::new(reader_config)
        .map_err(|error| report!("open merge reader: {error}"))?;
    reader
        .start()
        .await
        .map_err(|error| report!("start merge reader: {error}"))?;
    let mut writer = create_writer_with_io_config(writer_config)
        .await
        .map_err(|error| report!("open merge writer: {error}"))?;
    let mut rows = 0u64;
    while let Some(batch) = reader.next_rb().await {
        let batch = batch.map_err(|error| report!("read merge input: {error}"))?;
        rows += batch.num_rows() as u64;
        writer
            .write_record_batch(batch)
            .await
            .map_err(|error| report!("write merge output: {error}"))?;
    }
    writer
        .flush_and_close()
        .await
        .map_err(|error| report!("flush merge output: {error}"))?;
    let bytes = tokio::fs::metadata(output_file)
        .await
        .map_err(|error| report!("stat {output_file}: {error}"))?
        .len();
    Ok(CompactToOneResult {
        mode: CompactionMode::Fallback,
        input_files: input_files.len(),
        output_rows: rows,
        output_bytes: bytes,
        discarded_files: plan.stats.discard_files,
        fallback_reason,
    })
}
