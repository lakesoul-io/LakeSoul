//! These codes are copy from vortex
//
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow_schema::DataType;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::exec_datafusion_err;
use datafusion_common::internal_datafusion_err;
use datafusion_common_runtime::JoinSet;
use datafusion_common_runtime::SpawnedTask;
use datafusion_datasource::file_sink_config::FileSink;
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::sink::DataSink;
use datafusion_datasource::write::demux::DemuxedStreamReceiver;
use datafusion_datasource::write::get_writer_schema;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::metrics::Count;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::metrics::MetricBuilder;
use datafusion_physical_plan::metrics::MetricCategory;
use datafusion_physical_plan::metrics::MetricsSet;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio_stream::wrappers::ReceiverStream;
use vortex::array::ArrayId;
use vortex::array::ArrayRef;
use vortex::array::ExecutionCtx;
use vortex::array::session::ArraySessionExt;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::arrow::ArrowSessionExt;
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::editions::{ComponentKind, EditionSessionExt};
use vortex::error::VortexResult;
use vortex::file::Footer as FileFooter;
use vortex::file::WriteOptionsSessionExt;
use vortex::file::WriteStrategyBuilder;
use vortex::file::WriteSummary;
use vortex::io::VortexWrite;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::layout::layouts::compressed::CompressorPlugin;
use vortex::session::VortexSession;
use vortex::utils::aliases::hash_set::HashSet;

use crate::config::ColumnPolicy;

/// Row block size used for vector index columns.  Candidate rows are
/// fetched by row index, and vortex reads random rows at row-block
/// granularity, so smaller blocks make those fetches much cheaper.  The
/// default of 8192 rows would read most of the vector column for a
/// scattered candidate set.
const VECTOR_ROW_BLOCK_SIZE: usize = 1024;

/// The array encodings the session's enabled editions permit, derived the same
/// way the default Vortex file writer does (`vortex-file`'s
/// `new_array_context`): serialized IDs allowed by the editions, mapped to
/// their registered in-memory encoding IDs.
fn allowed_array_encodings(session: &VortexSession) -> HashSet<ArrayId> {
    let arrays = session.arrays();
    let registry = arrays.registry();
    session
        .enabled_component_ids(ComponentKind::Array)
        .iter()
        .filter_map(|serialized_id| registry.get(serialized_id))
        .map(|plugin| plugin.id())
        .collect()
}

/// Effective write layout for one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ColumnOverride {
    /// Whether the BtrBlocks compressor is applied to the column.
    compress: bool,
    /// Row block size override; `None` keeps the writer default.
    row_block_size: Option<usize>,
    /// Data block target bytes override; `None` keeps the writer default.
    data_block_target_bytes: Option<u64>,
}

fn is_blob_dtype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// Resolve the effective layout of every column that deviates from the
/// defaults: vector columns get small row blocks, binary/blob columns skip
/// compression, and explicit policies override both.
fn resolve_column_overrides(
    schema: &SchemaRef,
    vector_columns: &[String],
    policies: &HashMap<String, ColumnPolicy>,
) -> HashMap<String, ColumnOverride> {
    let mut overrides = HashMap::new();
    for field in schema.fields() {
        let name = field.name();
        let is_vector = vector_columns.iter().any(|column| column == name);
        let policy = policies.get(name);
        let default_compress = if is_vector {
            true
        } else {
            !is_blob_dtype(field.data_type())
        };
        let compress = policy
            .and_then(|policy| policy.compress)
            .unwrap_or(default_compress);
        let row_block_size =
            policy
                .and_then(|policy| policy.row_block_size)
                .or(if is_vector {
                    Some(VECTOR_ROW_BLOCK_SIZE)
                } else {
                    None
                });
        let data_block_target_bytes =
            policy.and_then(|policy| policy.data_block_target_bytes);
        let default_override = ColumnOverride {
            compress: true,
            row_block_size: None,
            data_block_target_bytes: None,
        };
        let effective = ColumnOverride {
            compress,
            row_block_size,
            data_block_target_bytes,
        };
        if effective != default_override {
            overrides.insert(name.clone(), effective);
        }
    }
    overrides
}

/// A compressor that returns the chunk unchanged, disabling compression for
/// one column while keeping the rest of the layout pipeline.
fn no_compression_compressor() -> impl CompressorPlugin {
    |chunk: &ArrayRef, _ctx: &mut ExecutionCtx| -> VortexResult<ArrayRef> {
        Ok(chunk.clone())
    }
}

pub struct VortexSink {
    config: FileSinkConfig,
    schema: SchemaRef,
    session: VortexSession,
    is_compact: bool,
    /// Effective per-column write layout, keyed by column name.
    column_overrides: HashMap<String, ColumnOverride>,
    /// The Mutex is only used to allow inserting to HashMap from behind borrowed reference in DataSink::write_all.
    written: Arc<parking_lot::Mutex<HashMap<Path, FileFooter>>>,

    metrics: ExecutionPlanMetricsSet,
    rows_written: Count,
    bytes_written: Count,
}

impl VortexSink {
    /// schema without partition columns
    pub fn new(
        session: VortexSession,
        config: FileSinkConfig,
        schema: SchemaRef,
        is_compact: bool,
        vector_columns: Vec<String>,
        column_policies: &HashMap<String, ColumnPolicy>,
    ) -> Self {
        let column_overrides =
            resolve_column_overrides(&schema, &vector_columns, column_policies);
        let metrics = ExecutionPlanMetricsSet::new();
        let rows_written = MetricBuilder::new(&metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("rows_written");
        let bytes_written = MetricBuilder::new(&metrics)
            .with_category(MetricCategory::Bytes)
            .global_counter("bytes_written");
        Self {
            config,
            schema,
            session,
            is_compact,
            column_overrides,
            written: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            metrics,
            rows_written,
            bytes_written,
        }
    }

    /// Build the write strategy: the default strategy, optionally with the
    /// compact compressor, plus per-column overrides for vector/blob columns.
    fn write_strategy(&self) -> Arc<dyn vortex::layout::LayoutStrategy> {
        // The custom strategy replaces the default one, which would normally
        // restrict BtrBlocks schemes to the encodings permitted by the
        // session's enabled editions. Mirror that filtering here; otherwise
        // the compressor can pick an encoding such as `fastlanes.delta` that
        // the serialization context then rejects with "Serialized array ID
        // ... not permitted by ctx" while writing the file.
        let allowed = allowed_array_encodings(&self.session);
        let make_builder = || {
            let compressor = if self.is_compact {
                // use zstd in block level
                BtrBlocksCompressorBuilder::default().with_compact()
            } else {
                BtrBlocksCompressorBuilder::default()
            };
            WriteStrategyBuilder::default()
                .with_btrblocks_builder(compressor.retain_allowed_encodings(&allowed))
        };

        let mut builder = make_builder();
        for (column, policy) in &self.column_overrides {
            let mut column_builder = make_builder();
            if !policy.compress {
                column_builder =
                    column_builder.with_compressor(no_compression_compressor());
            }
            if let Some(row_block_size) = policy.row_block_size {
                column_builder = column_builder.with_row_block_size(row_block_size);
            }
            if let Some(data_block_target_bytes) = policy.data_block_target_bytes {
                column_builder = column_builder
                    .with_data_block_target_bytes(Some(data_block_target_bytes));
            }
            builder = builder.with_field_writer(
                vortex::dtype::FieldPath::from_name(column.as_str()),
                column_builder.build(),
            );
        }
        builder.build()
    }

    pub fn written(&self) -> HashMap<Path, FileFooter> {
        self.written.lock().clone()
    }
}

impl std::fmt::Debug for VortexSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexSink").finish()
    }
}

impl DisplayAs for VortexSink {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "VortexSink")
            }
        }
    }
}

#[async_trait]
impl DataSink for VortexSink {
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Returns the sink schema
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        FileSink::write_all(self, data, context).await
    }
}

#[async_trait]
impl FileSink for VortexSink {
    fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn spawn_writer_tasks_and_join(
        &self,
        _context: &Arc<TaskContext>,
        demux_task: SpawnedTask<DFResult<()>>,
        mut file_stream_rx: DemuxedStreamReceiver,
        object_store: Arc<dyn ObjectStore>,
    ) -> DFResult<u64> {
        let mut file_write_tasks: JoinSet<DFResult<(Path, WriteSummary)>> =
            JoinSet::new();
        let writer_schema = get_writer_schema(&self.config);
        let write_strategy = self.write_strategy();
        let dtype = self
            .session
            .arrow()
            .from_arrow_schema(&writer_schema)
            .map_err(|e| {
                exec_datafusion_err!(
                    "Failed to derive Vortex DType from writer schema: {e}"
                )
            })?;

        // TODO(adamg):
        // 1. We can probably be better at signaling how much memory we're consuming (potentially when reading too), see ParquetSink::spawn_writer_tasks_and_join.
        while let Some((path, rx)) = file_stream_rx.recv().await {
            let session = self.session.clone();
            let object_store = Arc::clone(&object_store);

            // We need to spawn work because there's a dependency between the different files. If one file has too many batches buffered,
            // the demux task might deadlock itself.
            let arrow_session = session.clone();
            let import_schema = Arc::clone(&writer_schema);
            let dtype = dtype.clone();
            let write_strategy = Arc::clone(&write_strategy);
            file_write_tasks.spawn(async move {
                let stream = ReceiverStream::new(rx).map(move |rb| {
                    arrow_session
                        .arrow()
                        .from_arrow_record_batch(rb, &import_schema)
                });

                let stream_adapter = ArrayStreamAdapter::new(dtype, stream);

                let mut object_writer = ObjectStoreWrite::new(object_store, &path)
                    .await
                    .map_err(|e| {
                        exec_datafusion_err!("Failed to create ObjectStoreWrite: {e}")
                    })?;

                let write_options = session
                    .write_options()
                    .with_strategy(Arc::clone(&write_strategy));

                let summary = write_options
                    .write(&mut object_writer, stream_adapter)
                    .await
                    .map_err(|e| {
                        exec_datafusion_err!("Failed to write Vortex file: {e}")
                    })?;

                object_writer.shutdown().await.map_err(|e| {
                    exec_datafusion_err!("Failed to shutdown Vortex writer: {e}")
                })?;

                Ok((path, summary))
            });
        }

        let mut row_count = 0;

        while let Some(result) = file_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    let (path, summary) = r?;

                    let rows = summary.row_count();
                    row_count += rows;

                    self.rows_written
                        .add(usize::try_from(rows).unwrap_or(usize::MAX));
                    self.bytes_written
                        .add(usize::try_from(summary.size()).unwrap_or(usize::MAX));

                    let mut written_files = self.written.lock();
                    match written_files.entry(path.clone()) {
                        Entry::Occupied(_) => {
                            return Err(internal_datafusion_err!(
                                "duplicate entry detected for partitioned file {path}"
                            ));
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(summary.footer().clone());
                        }
                    };
                    drop(written_files);

                    tracing::info!(path = %path, "Successfully written file");
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        demux_task
            .join_unwind()
            .await
            .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))??;

        Ok(row_count)
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;
    use arrow_schema::Schema;

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    8,
                ),
                false,
            ),
            Field::new("frame", DataType::Binary, false),
            Field::new("clip", DataType::LargeBinary, false),
        ]))
    }

    #[test]
    fn vector_and_blob_columns_get_default_overrides() {
        let overrides = resolve_column_overrides(
            &test_schema(),
            &["embedding".to_string()],
            &HashMap::new(),
        );

        assert!(!overrides.contains_key("id"));
        assert_eq!(
            overrides.get("embedding"),
            Some(&ColumnOverride {
                compress: true,
                row_block_size: Some(VECTOR_ROW_BLOCK_SIZE),
                data_block_target_bytes: None,
            })
        );
        assert_eq!(
            overrides.get("frame"),
            Some(&ColumnOverride {
                compress: false,
                row_block_size: None,
                data_block_target_bytes: None,
            })
        );
        assert_eq!(
            overrides.get("clip"),
            Some(&ColumnOverride {
                compress: false,
                row_block_size: None,
                data_block_target_bytes: None,
            })
        );
    }

    #[test]
    fn explicit_policies_override_defaults() {
        let policies = HashMap::from([
            (
                "frame".to_string(),
                ColumnPolicy {
                    compress: Some(true),
                    row_block_size: None,
                    data_block_target_bytes: None,
                },
            ),
            (
                "embedding".to_string(),
                ColumnPolicy {
                    compress: None,
                    row_block_size: Some(2048),
                    data_block_target_bytes: Some(4096),
                },
            ),
            (
                "id".to_string(),
                ColumnPolicy {
                    compress: None,
                    row_block_size: Some(64),
                    data_block_target_bytes: None,
                },
            ),
        ]);

        let overrides = resolve_column_overrides(
            &test_schema(),
            &["embedding".to_string()],
            &policies,
        );

        // Re-enabling compression equals the global default, so no field
        // override is emitted for `frame`.
        assert!(!overrides.contains_key("frame"));
        assert_eq!(
            overrides.get("embedding"),
            Some(&ColumnOverride {
                compress: true,
                row_block_size: Some(2048),
                data_block_target_bytes: Some(4096),
            })
        );
        assert_eq!(
            overrides.get("id"),
            Some(&ColumnOverride {
                compress: true,
                row_block_size: Some(64),
                data_block_target_bytes: None,
            })
        );
        assert_eq!(
            overrides.get("clip"),
            Some(&ColumnOverride {
                compress: false,
                row_block_size: None,
                data_block_target_bytes: None,
            })
        );
    }
}
