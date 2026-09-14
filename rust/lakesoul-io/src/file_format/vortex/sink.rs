//! These codes are copy from vortex
//
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

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
use datafusion_physical_plan::metrics::MetricsSet;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use tokio_stream::wrappers::ReceiverStream;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::arrow::ArrowSessionExt;
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::file::Footer as FileFooter;
use vortex::file::WriteOptionsSessionExt;
use vortex::file::WriteStrategyBuilder;
use vortex::file::WriteSummary;
use vortex::io::VortexWrite;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::session::VortexSession;

/// Row block size used for vector index columns.  Candidate rows are
/// fetched by row index, and vortex reads random rows at row-block
/// granularity, so smaller blocks make those fetches much cheaper.  The
/// default of 8192 rows would read most of the vector column for a
/// scattered candidate set.
const VECTOR_ROW_BLOCK_SIZE: usize = 1024;

pub struct VortexSink {
    config: FileSinkConfig,
    schema: SchemaRef,
    session: VortexSession,
    is_compact: bool,
    /// Vector index columns that get small row blocks.
    vector_columns: Vec<String>,
    /// The Mutex is only used to allow inserting to HashMap from behind borrowed reference in DataSink::write_all.
    written: Arc<parking_lot::Mutex<HashMap<Path, FileFooter>>>,
}

impl VortexSink {
    /// schema without partition columns
    pub fn new(
        session: VortexSession,
        config: FileSinkConfig,
        schema: SchemaRef,
        is_compact: bool,
        vector_columns: Vec<String>,
    ) -> Self {
        Self {
            config,
            schema,
            session,
            is_compact,
            vector_columns,
            written: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    /// Build the write strategy: the default strategy, optionally with the
    /// compact compressor, plus a small-row-block override for every vector
    /// index column.
    fn write_strategy(&self) -> Arc<dyn vortex::layout::LayoutStrategy> {
        let make_builder = || {
            let builder = WriteStrategyBuilder::default();
            if self.is_compact {
                // use zstd in block level
                builder.with_btrblocks_builder(
                    BtrBlocksCompressorBuilder::default().with_compact(),
                )
            } else {
                builder
            }
        };

        let mut builder = make_builder();
        if !self.vector_columns.is_empty() {
            let vector_strategy: Arc<dyn vortex::layout::LayoutStrategy> = make_builder()
                .with_row_block_size(VECTOR_ROW_BLOCK_SIZE)
                .build();
            for column in &self.vector_columns {
                builder = builder.with_field_writer(
                    vortex::dtype::FieldPath::from_name(column.as_str()),
                    Arc::clone(&vector_strategy),
                );
            }
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
        None
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

                    row_count += summary.row_count();

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
