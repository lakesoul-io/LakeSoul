// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Demand-driven native vector batch execution with one pinned target snapshot.

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int32Array,
    Int64Array, LargeListArray, ListArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryReservation,
};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, execute_stream,
};
use futures::StreamExt;

use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::vector_batch::native::{
    MAX_BATCH_INPUT_BYTES, MAX_BATCH_QUERIES, NativeBatchHit, NativeBatchOptions,
    NativeBatchSnapshot,
};

pub(crate) const QUERY_WINDOW_SIZE: usize = 256;

pub(crate) fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Int64, false),
        Field::new("rank", DataType::Int32, false),
        Field::new("id", DataType::Int64, false),
        Field::new("distance", DataType::Float64, false),
    ]))
}

pub(crate) fn validate_query_schema(schema: &Schema) -> Result<()> {
    if schema.fields().len() != 2 || schema.field(0).data_type() != &DataType::Int64 {
        return Err(DataFusionError::Plan(
            "query source must contain an int64 query id and a float32 vector".into(),
        ));
    }
    let element = match schema.field(1).data_type() {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => field.data_type(),
        _ => {
            return Err(DataFusionError::Plan(
                "query vector must be a list or fixed-size list of float32".into(),
            ));
        }
    };
    if element != &DataType::Float32 {
        return Err(DataFusionError::Plan(
            "query vector elements must be float32".into(),
        ));
    }
    Ok(())
}

/// Inline literals remain a real child plan and build Arrow arrays only as
/// demanded. The retained literal buffer is accounted for during execution.
#[derive(Debug)]
pub(crate) struct InlineVectorQueriesExec {
    queries: Arc<Vec<Vec<f32>>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl InlineVectorQueriesExec {
    fn retained_bytes(&self) -> Result<usize> {
        let literals = self
            .queries
            .iter()
            .try_fold(
                self.queries.capacity() * std::mem::size_of::<Vec<f32>>(),
                |sum, query| {
                    query
                        .capacity()
                        .checked_mul(4)
                        .and_then(|bytes| sum.checked_add(bytes))
                },
            )
            .ok_or_else(memory_overflow)?;
        let window = self
            .queries
            .iter()
            .take(QUERY_WINDOW_SIZE)
            .try_fold(128usize, |sum, query| {
                query
                    .len()
                    .checked_mul(4)
                    .and_then(|bytes| sum.checked_add(bytes + 16))
            })
            .ok_or_else(memory_overflow)?;
        literals.checked_add(window).ok_or_else(memory_overflow)
    }

    pub(crate) fn new(queries: Arc<Vec<Vec<f32>>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("query_id", DataType::Int64, false),
            Field::new(
                "query_vector",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                false,
            ),
        ]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            queries,
            schema,
            properties,
        }
    }
}

impl DisplayAs for InlineVectorQueriesExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "InlineVectorQueriesExec: queries={}, window={QUERY_WINDOW_SIZE}",
            self.queries.len()
        )
    }
}

impl ExecutionPlan for InlineVectorQueriesExec {
    fn name(&self) -> &str {
        "InlineVectorQueriesExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "InlineVectorQueriesExec has no children".into(),
            ));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "InlineVectorQueriesExec has one partition".into(),
            ));
        }
        let reservation =
            MemoryConsumer::new("InlineVectorQueriesExec literals and Arrow window")
                .register(&context.runtime_env().memory_pool);
        let literal_bytes = self
            .queries
            .iter()
            .try_fold(
                self.queries.capacity() * std::mem::size_of::<Vec<f32>>(),
                |sum, query| {
                    query
                        .capacity()
                        .checked_mul(4)
                        .and_then(|bytes| sum.checked_add(bytes))
                },
            )
            .ok_or_else(memory_overflow)?;
        reservation.try_grow(literal_bytes)?;
        let schema = self.schema.clone();
        let stream = futures::stream::try_unfold(
            (
                self.queries.clone(),
                0usize,
                reservation,
                0usize,
                schema.clone(),
            ),
            |(queries, offset, reservation, previous_bytes, schema)| async move {
                reservation.shrink(previous_bytes);
                if offset >= queries.len() {
                    return Ok(None);
                }
                let end = (offset + QUERY_WINDOW_SIZE).min(queries.len());
                let bytes = queries[offset..end]
                    .iter()
                    .try_fold(128usize, |sum, query| {
                        query
                            .len()
                            .checked_mul(4)
                            .and_then(|bytes| sum.checked_add(bytes + 16))
                    })
                    .ok_or_else(memory_overflow)?;
                reservation.try_grow(bytes)?;
                let mut vectors =
                    arrow::array::ListBuilder::new(arrow::array::Float32Builder::new());
                for query in &queries[offset..end] {
                    vectors.values().append_slice(query);
                    vectors.append(true);
                }
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values(
                            (offset..end).map(|id| id as i64),
                        )),
                        Arc::new(vectors.finish()),
                    ],
                )?;
                Ok(Some((batch, (queries, end, reservation, bytes, schema))))
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn retained_inline_bytes(input: &Arc<dyn ExecutionPlan>) -> Result<usize> {
    if let Some(inline) = input.as_ref().downcast_ref::<InlineVectorQueriesExec>() {
        return inline.retained_bytes();
    }
    input.children().iter().try_fold(0usize, |sum, child| {
        sum.checked_add(retained_inline_bytes(child)?)
            .ok_or_else(memory_overflow)
    })
}

#[derive(Clone)]
struct VectorBatchMetrics {
    baseline: datafusion::physical_plan::metrics::BaselineMetrics,
    windows: Count,
    output_batches: Count,
    queries: Count,
    shard_probes: Count,
    cluster_loads: Count,
    rerank_blocks: Count,
    shared_block_reuses: Count,
    peak_workers: Gauge,
    work_memory: Gauge,
    wall_time: datafusion::physical_plan::metrics::Time,
}

impl VectorBatchMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let builder = MetricBuilder::new(metrics);
        Self {
            baseline: datafusion::physical_plan::metrics::BaselineMetrics::new(
                metrics, partition,
            ),
            windows: builder.clone().counter("batch_windows", partition),
            output_batches: builder.clone().counter("batch_output_batches", partition),
            queries: builder.clone().counter("batch_queries", partition),
            shard_probes: builder.clone().counter("batch_shard_probes", partition),
            cluster_loads: builder.clone().counter("batch_cluster_loads", partition),
            rerank_blocks: builder.clone().counter("batch_rerank_blocks", partition),
            shared_block_reuses: builder
                .clone()
                .counter("batch_shared_block_reuses", partition),
            peak_workers: builder.clone().gauge("batch_peak_workers", partition),
            work_memory: builder.clone().gauge("batch_work_memory_bytes", partition),
            wall_time: builder.subset_time("batch_wall_time", partition),
        }
    }
}

#[derive(Clone, Copy)]
struct NativeMetricSnapshot {
    windows: u64,
    queries: u64,
    shard_probes: u64,
    cluster_loads: u64,
    rerank_blocks: u64,
    shared_block_reuses: u64,
    peak_workers: u64,
}

fn native_metric_snapshot(snapshot: &NativeBatchSnapshot) -> NativeMetricSnapshot {
    let metrics = &snapshot.metrics;
    NativeMetricSnapshot {
        windows: metrics.windows.load(Ordering::Relaxed),
        queries: metrics.queries.load(Ordering::Relaxed),
        shard_probes: metrics.shard_probes.load(Ordering::Relaxed),
        cluster_loads: metrics.cluster_loads.load(Ordering::Relaxed),
        rerank_blocks: metrics.rerank_blocks.load(Ordering::Relaxed),
        shared_block_reuses: metrics.shared_block_reuses.load(Ordering::Relaxed),
        peak_workers: metrics.peak_workers.load(Ordering::Relaxed),
    }
}

fn record_native_metric_delta(
    metrics: &VectorBatchMetrics,
    before: NativeMetricSnapshot,
    after: NativeMetricSnapshot,
    query_count: usize,
    shard_count: usize,
    elapsed: std::time::Duration,
) {
    metrics
        .windows
        .add(after.windows.saturating_sub(before.windows).max(1) as usize);
    metrics.queries.add(
        after
            .queries
            .saturating_sub(before.queries)
            .max(query_count as u64) as usize,
    );
    metrics.shard_probes.add(
        after
            .shard_probes
            .saturating_sub(before.shard_probes)
            .max(shard_count as u64) as usize,
    );
    metrics
        .cluster_loads
        .add(after.cluster_loads.saturating_sub(before.cluster_loads) as usize);
    metrics
        .rerank_blocks
        .add(after.rerank_blocks.saturating_sub(before.rerank_blocks) as usize);
    metrics.shared_block_reuses.add(
        after
            .shared_block_reuses
            .saturating_sub(before.shared_block_reuses) as usize,
    );
    metrics.peak_workers.set_max(after.peak_workers as usize);
    metrics.baseline.elapsed_compute().add_duration(elapsed);
}

/// A real query-source child and one native search per bounded input window.
/// The unfold stream has no eager producer: consumer backpressure stops the
/// next window before any more queries or results are allocated.
pub struct VectorBatchSearchExec {
    target: Arc<dyn TableProvider>,
    state: Arc<SessionState>,
    column: String,
    metric: String,
    options: NativeBatchOptions,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl Debug for VectorBatchSearchExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorBatchSearchExec")
            .field("column", &self.column)
            .field("metric", &self.metric)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl VectorBatchSearchExec {
    pub(crate) fn try_new(
        target: Arc<dyn TableProvider>,
        state: Arc<SessionState>,
        column: String,
        metric: String,
        options: NativeBatchOptions,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        validate_query_schema(input.schema().as_ref())?;
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            target,
            state,
            column,
            metric,
            options,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        })
    }
}

impl DisplayAs for VectorBatchSearchExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "VectorBatchSearchExec: column={}, metric={}, top_k={}, nprobe={}, rerank={}, window_max=256, metrics=[batch_windows, batch_output_batches, batch_queries, batch_shard_probes, batch_cluster_loads(total), batch_rerank_blocks(total), batch_shared_block_reuses(total), batch_peak_workers(max)]",
            self.column,
            self.metric,
            self.options.top_k,
            self.options.nprobe,
            if self.options.exact { "exact" } else { "index" }
        )
    }
}

impl ExecutionPlan for VectorBatchSearchExec {
    fn name(&self) -> &str {
        "VectorBatchSearchExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "VectorBatchSearchExec requires one query-source child".into(),
            ));
        }
        let mut replacement = Self::try_new(
            self.target.clone(),
            self.state.clone(),
            self.column.clone(),
            self.metric.clone(),
            self.options.clone(),
            children[0].clone(),
        )?;
        replacement.metrics = self.metrics.clone();
        Ok(Arc::new(replacement))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "VectorBatchSearchExec has a single output partition".into(),
            ));
        }
        let metrics = VectorBatchMetrics::new(&self.metrics, partition);
        let started = std::time::Instant::now();
        let config = context
            .session_config()
            .options()
            .extensions
            .get::<crate::udf::vector_batch::VectorBatchConfig>()
            .cloned()
            .unwrap_or_default();
        config.validate()?;
        let mut options = self.options.clone();
        options.parallelism = config.parallelism;
        options.io_concurrency = config.io_concurrency;
        options.memory_bytes = options.memory_bytes.min(config.memory_bytes);
        if let MemoryLimit::Finite(limit) =
            context.runtime_env().memory_pool.memory_limit()
        {
            // Leave room for the child batch, query IDs and result arrays.
            options.memory_bytes = options.memory_bytes.min(limit);
        }
        if options.memory_bytes == 0 {
            return Err(DataFusionError::ResourcesExhausted(
                "no vector batch work memory available".into(),
            ));
        }
        let cancel = Arc::new(AtomicBool::new(false));
        let input = execute_stream(self.input.clone(), context.clone())?;
        let reservation =
            MemoryConsumer::new("VectorBatchSearchExec input and query ids")
                .register(&context.runtime_env().memory_pool);
        let run = BatchRun {
            target: self.target.clone(),
            state: self.state.clone(),
            column: self.column.clone(),
            metric: self.metric.clone(),
            snapshot: None,
            options,
            input,
            current: None,
            offset: 0,
            current_bytes: 0,
            input_bytes: 0,
            retained_source_bytes: retained_inline_bytes(&self.input)?,
            query_window: config.query_window,
            metrics,
            started,
            seen: HashSet::new(),
            reservation,
            output_reservation: None,
            cancel: CancelOnDrop(cancel),
            context,
        };
        let stream = futures::stream::try_unfold(run, |mut run| async move {
            let window_started = std::time::Instant::now();
            run.output_reservation = None;
            run.metrics.work_memory.set(
                run.reservation
                    .size()
                    .saturating_add(run.retained_source_bytes),
            );
            if run.snapshot.is_none() {
                let provider = run
                    .target
                    .as_ref()
                    .downcast_ref::<LakeSoulTableProvider>()
                    .ok_or_else(|| {
                        DataFusionError::Plan(
                            "vector batch target must be a LakeSoul table".into(),
                        )
                    })?;
                run.snapshot = Some(
                    NativeBatchSnapshot::prepare(
                        provider,
                        &run.state,
                        &run.column,
                        &run.metric,
                    )
                    .await?,
                );
            }
            let snapshot = run.snapshot.as_ref().unwrap().clone();
            let input_ready = run.ensure_current(Some(snapshot.as_ref())).await?;
            if !input_ready {
                return Ok(None);
            }
            let base_bytes = run
                .reservation
                .size()
                .checked_add(run.retained_source_bytes)
                .ok_or_else(memory_overflow)?;
            let mut window_size = run.query_window;
            while window_size > 1
                && base_bytes
                    .checked_add(window_allocation_bytes(
                        &snapshot,
                        window_size,
                        &run.options,
                    )?)
                    .ok_or_else(memory_overflow)?
                    > run.options.memory_bytes
            {
                window_size /= 2;
            }
            if base_bytes
                .checked_add(window_allocation_bytes(
                    &snapshot,
                    window_size,
                    &run.options,
                )?)
                .ok_or_else(memory_overflow)?
                > run.options.memory_bytes
            {
                return Err(DataFusionError::ResourcesExhausted(
                    "query source and one vector query exceed the total batch memory budget".into(),
                ));
            }
            let window_memory =
                MemoryConsumer::new("VectorBatchSearchExec query window and native work")
                    .register(&run.context.runtime_env().memory_pool);
            let query_slot_bytes = window_size
                .checked_mul(std::mem::size_of::<i64>() + std::mem::size_of::<Vec<f32>>())
                .ok_or_else(memory_overflow)?;
            window_memory.try_grow(query_slot_bytes)?;
            let read_options = run.options.clone();
            let total_memory_bytes = run.options.memory_bytes;
            let retained_source_bytes = run.retained_source_bytes;
            let (query_ids, queries) = run
                .read_window(
                    snapshot.dim(),
                    window_size,
                    Some(snapshot.as_ref()),
                    &read_options,
                    total_memory_bytes,
                    retained_source_bytes,
                    &window_memory,
                )
                .await?;
            if queries.is_empty() {
                return Ok(None);
            }
            let before = native_metric_snapshot(&snapshot);
            let query_count = queries.len();
            let output_bytes = queries
                .len()
                .checked_mul(run.options.top_k)
                .and_then(|rows| {
                    rows.checked_mul(std::mem::size_of::<NativeBatchHit>() + 56)
                })
                .ok_or_else(memory_overflow)?;
            let work_bytes =
                snapshot.estimated_work_bytes(queries.len(), &run.options)?;
            window_memory.try_grow(
                output_bytes
                    .checked_add(work_bytes)
                    .ok_or_else(memory_overflow)?,
            )?;
            let window_memory = Arc::new(window_memory);
            let mut options = run.options.clone();
            // The service's temporary allocations are confined to precisely
            // the estimate reserved above; source/output buffers share the
            // total per-run ceiling enforced before extracting this window.
            options.memory_bytes = work_bytes;
            let cancel = run.cancel.0.clone();
            let held_memory = window_memory.clone();
            let worker_snapshot = snapshot.clone();
            let task_context = run.context.clone();
            // If the client drops this future, the cancellation flag is set by
            // BatchRun, while the detached task retains its memory reservation
            // until cooperative native CPU workers have actually stopped.
            let (hits, held_memory) = tokio::spawn(async move {
                let result = match crate::vector_batch::distributed::maybe_search_distributed_window(
                    worker_snapshot.clone(), &queries, &options, task_context, cancel.clone(),
                ).await {
                    Ok(Some(hits)) => Ok(hits),
                    Ok(None) => worker_snapshot.search(&queries, &options, cancel).await,
                    Err(error) => Err(error),
                };
                (result, held_memory)
            })
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!("native batch task failed: {error}"))
            })?;
            let hits = hits?;
            snapshot.ensure_valid()?;
            record_native_metric_delta(
                &run.metrics,
                before,
                native_metric_snapshot(&snapshot),
                query_count,
                snapshot.descriptor().shards.len(),
                window_started.elapsed(),
            );
            let batch = hits_batch(&query_ids, hits)?;
            run.metrics.baseline.record_output(batch.num_rows());
            run.metrics.output_batches.add(1);
            run.metrics.work_memory.set(
                run.reservation
                    .size()
                    .saturating_add(run.retained_source_bytes)
                    .saturating_add(held_memory.size()),
            );
            run.output_reservation = Some(held_memory);
            Ok(Some((batch, run)))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn window_allocation_bytes(
    snapshot: &NativeBatchSnapshot,
    count: usize,
    options: &NativeBatchOptions,
) -> Result<usize> {
    let query = snapshot
        .dim()
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or_else(memory_overflow)?;
    let output = options
        .top_k
        .checked_mul(std::mem::size_of::<NativeBatchHit>() + 56)
        .ok_or_else(memory_overflow)?;
    let buffers = query
        .checked_add(output)
        .and_then(|bytes| count.checked_mul(bytes))
        .ok_or_else(memory_overflow)?;
    buffers
        .checked_add(snapshot.estimated_work_bytes(count, options)?)
        .ok_or_else(memory_overflow)
}

fn memory_overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("vector batch memory requirement overflow".into())
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl Drop for BatchRun {
    fn drop(&mut self) {
        self.metrics.baseline.try_done();
        self.metrics.wall_time.add_duration(self.started.elapsed());
        self.metrics.work_memory.set(0);
    }
}

struct BatchRun {
    target: Arc<dyn TableProvider>,
    state: Arc<SessionState>,
    column: String,
    metric: String,
    snapshot: Option<Arc<NativeBatchSnapshot>>,
    options: NativeBatchOptions,
    input: SendableRecordBatchStream,
    current: Option<RecordBatch>,
    offset: usize,
    current_bytes: usize,
    input_bytes: usize,
    retained_source_bytes: usize,
    query_window: usize,
    metrics: VectorBatchMetrics,
    started: std::time::Instant,
    seen: HashSet<i64>,
    reservation: MemoryReservation,
    output_reservation: Option<Arc<MemoryReservation>>,
    cancel: CancelOnDrop,
    context: Arc<TaskContext>,
}

impl BatchRun {
    async fn ensure_current(
        &mut self,
        snapshot: Option<&NativeBatchSnapshot>,
    ) -> Result<bool> {
        while self
            .current
            .as_ref()
            .is_none_or(|batch| self.offset >= batch.num_rows())
        {
            self.current = None;
            self.reservation.shrink(self.current_bytes);
            self.current_bytes = 0;
            let next = if let Some(snapshot) = snapshot {
                tokio::select! {
                    batch = self.input.next() => batch,
                    _ = snapshot.cancelled() => {
                        self.cancel.0.store(true, Ordering::Release);
                        return Err(DataFusionError::Execution(
                            "batch read lease lost while reading query source".into(),
                        ));
                    }
                }
            } else {
                self.input.next().await
            };
            let Some(batch) = next.transpose()? else {
                return Ok(false);
            };
            validate_query_schema(batch.schema().as_ref())?;
            self.current_bytes = batch.get_array_memory_size();
            self.reservation.try_grow(self.current_bytes)?;
            self.current = Some(batch);
            self.offset = 0;
        }
        Ok(true)
    }

    async fn read_window(
        &mut self,
        dim: usize,
        limit: usize,
        snapshot: Option<&NativeBatchSnapshot>,
        options: &NativeBatchOptions,
        total_memory_bytes: usize,
        retained_source_bytes: usize,
        window_memory: &MemoryReservation,
    ) -> Result<(Vec<i64>, Vec<Vec<f32>>)> {
        let mut query_ids = Vec::with_capacity(limit);
        let mut queries = Vec::with_capacity(limit);
        while queries.len() < limit {
            if self
                .current
                .as_ref()
                .is_none_or(|batch| self.offset >= batch.num_rows())
                && !self.ensure_current(snapshot).await?
            {
                break;
            }
            let batch = self.current.as_ref().unwrap();
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let row = self.offset;
            if ids.is_null(row) {
                return Err(DataFusionError::Execution(
                    "query_id must not be NULL".into(),
                ));
            }
            let id = ids.value(row);
            if self.seen.contains(&id) {
                return Err(DataFusionError::Execution(format!(
                    "duplicate query_id {id}"
                )));
            }
            if self.seen.len() >= MAX_BATCH_QUERIES {
                return Err(DataFusionError::Execution(format!(
                    "batch supports at most {MAX_BATCH_QUERIES} queries"
                )));
            }
            let input_bytes = dim
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(8))
                .and_then(|bytes| self.input_bytes.checked_add(bytes))
                .ok_or_else(memory_overflow)?;
            if input_bytes > MAX_BATCH_INPUT_BYTES {
                return Err(DataFusionError::Execution(
                    "batch query input exceeds 64 MiB".into(),
                ));
            }
            if let Some(snapshot) = snapshot {
                let next_query_count = queries.len() + 1;
                let base_bytes = self
                    .reservation
                    .size()
                    .checked_add(retained_source_bytes)
                    .ok_or_else(memory_overflow)?;
                let next_total = base_bytes
                    .checked_add(window_allocation_bytes(
                        snapshot,
                        next_query_count,
                        options,
                    )?)
                    .ok_or_else(memory_overflow)?;
                if next_total > total_memory_bytes {
                    if queries.is_empty() {
                        return Err(DataFusionError::ResourcesExhausted(
                            "query source batch and one query exceed the total batch memory budget".into(),
                        ));
                    }
                    let existing_total = base_bytes
                        .checked_add(window_allocation_bytes(
                            snapshot,
                            queries.len(),
                            options,
                        )?)
                        .ok_or_else(memory_overflow)?;
                    if existing_total > total_memory_bytes {
                        return Err(DataFusionError::ResourcesExhausted(
                            "query source batch cannot coexist with the current search window within the memory budget".into(),
                        ));
                    }
                    // Keep the next row unconsumed in this Arrow batch and
                    // return the smaller, already bounded window first.
                    break;
                }
            }
            let query_bytes = dim
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(memory_overflow)?;
            window_memory.try_grow(query_bytes)?;
            let query = extract_query(batch.column(1), row, dim)?;
            self.input_bytes = input_bytes;
            // Conservative hash table accounting includes bucket and capacity
            // growth overhead, retained for global uniqueness across windows.
            self.reservation.try_grow(64)?;
            self.seen.insert(id);
            query_ids.push(id);
            queries.push(query);
            self.offset += 1;
        }
        Ok((query_ids, queries))
    }
}

fn extract_query(array: &ArrayRef, row: usize, dim: usize) -> Result<Vec<f32>> {
    if array.is_null(row) {
        return Err(DataFusionError::Execution(
            "query vector must not be NULL".into(),
        ));
    }
    let values = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        list.value(row)
    } else {
        return Err(DataFusionError::Execution(
            "query vector must be a float32 list".into(),
        ));
    };
    let floats = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("query vector elements must be float32".into())
        })?;
    if floats.len() != dim {
        return Err(DataFusionError::Execution(format!(
            "query dimension {} does not match index dimension {dim}",
            floats.len()
        )));
    }
    if floats.null_count() != 0 || floats.values().iter().any(|value| !value.is_finite())
    {
        return Err(DataFusionError::Execution(
            "query vector must contain only finite, non-NULL float32 values".into(),
        ));
    }
    Ok(floats.values().to_vec())
}

fn hits_batch(
    query_ids: &[i64],
    results: Vec<Vec<NativeBatchHit>>,
) -> Result<RecordBatch> {
    if query_ids.len() != results.len() {
        return Err(DataFusionError::Internal(
            "native batch returned a different number of query groups".into(),
        ));
    }
    let rows = results.iter().map(Vec::len).sum();
    let mut qids = Vec::with_capacity(rows);
    let mut ranks = Vec::with_capacity(rows);
    let mut ids = Vec::with_capacity(rows);
    let mut distances = Vec::with_capacity(rows);
    for (&query_id, hits) in query_ids.iter().zip(results) {
        for (rank, hit) in hits.into_iter().enumerate() {
            qids.push(query_id);
            ranks.push((rank + 1) as i32);
            ids.push(hit.id);
            distances.push(hit.distance);
        }
    }
    Ok(RecordBatch::try_new(
        output_schema(),
        vec![
            Arc::new(Int64Array::from(qids)),
            Arc::new(Int32Array::from(ranks)),
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(distances)),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Builder, ListBuilder};
    use datafusion::datasource::memory::MemTable;
    use datafusion::prelude::SessionContext;

    fn query_batch(
        ids: Vec<Option<i64>>,
        queries: Vec<Option<Vec<Option<f32>>>>,
    ) -> RecordBatch {
        let mut builder = ListBuilder::new(Float32Builder::new());
        for query in queries {
            if let Some(values) = query {
                builder.values().extend(values);
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        let vectors = builder.finish();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("qid", DataType::Int64, true),
                Field::new("vector", vectors.data_type().clone(), true),
            ])),
            vec![Arc::new(Int64Array::from(ids)), Arc::new(vectors)],
        )
        .unwrap()
    }

    async fn read_test_window(
        run: &mut BatchRun,
        dim: usize,
        limit: usize,
    ) -> Result<(Vec<i64>, Vec<Vec<f32>>)> {
        let options = run.options.clone();
        let retained_source_bytes = run.retained_source_bytes;
        let reservation = MemoryConsumer::new("batch test query window")
            .register(&run.context.runtime_env().memory_pool);
        run.read_window(
            dim,
            limit,
            None,
            &options,
            crate::vector_batch::native::DEFAULT_WORK_MEMORY,
            retained_source_bytes,
            &reservation,
        )
        .await
    }

    async fn test_run(batches: Vec<RecordBatch>) -> BatchRun {
        let context = SessionContext::new();
        let state = Arc::new(context.state());
        let target =
            Arc::new(MemTable::try_new(batches[0].schema(), vec![batches]).unwrap());
        let plan = target.scan(state.as_ref(), None, &[], None).await.unwrap();
        let task = context.task_ctx();
        let reservation = MemoryConsumer::new("batch window test")
            .register(&task.runtime_env().memory_pool);
        BatchRun {
            target,
            state,
            column: "vec".into(),
            metric: "L2".into(),
            snapshot: None,
            options: Default::default(),
            input: execute_stream(plan, task.clone()).unwrap(),
            current: None,
            offset: 0,
            current_bytes: 0,
            input_bytes: 0,
            retained_source_bytes: 0,
            query_window: QUERY_WINDOW_SIZE,
            metrics: VectorBatchMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            started: std::time::Instant::now(),
            seen: HashSet::new(),
            reservation,
            output_reservation: None,
            cancel: CancelOnDrop(Arc::new(AtomicBool::new(false))),
            context: task,
        }
    }

    #[tokio::test]
    async fn vector_batch_inline_child_is_bounded_and_memory_accounted() {
        let context = SessionContext::new();
        let queries = Arc::new(vec![vec![1.0, 2.0]; 600]);
        let plan = InlineVectorQueriesExec::new(queries);
        let task = context.task_ctx();
        let pool = task.runtime_env().memory_pool.clone();
        let mut stream = plan.execute(0, task).unwrap();
        let literals = pool.reserved();
        assert!(literals > 0);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), QUERY_WINDOW_SIZE);
        assert!(pool.reserved() > literals);
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.num_rows(), QUERY_WINDOW_SIZE);
        let third = stream.next().await.unwrap().unwrap();
        assert_eq!(third.num_rows(), 88);
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn vector_batch_inline_child_respects_memory_pool_limit() {
        use datafusion::execution::config::SessionConfig;
        use datafusion::execution::memory_pool::GreedyMemoryPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(32)))
            .build_arc()
            .unwrap();
        let context =
            SessionContext::new_with_config_rt(SessionConfig::new(), runtime.clone());
        let plan = InlineVectorQueriesExec::new(Arc::new(vec![vec![1.0; 16]]));
        assert!(plan.execute(0, context.task_ctx()).is_err());
        assert_eq!(runtime.memory_pool.reserved(), 0);
    }

    #[tokio::test]
    async fn vector_batch_query_source_is_read_in_bounded_windows() {
        let batch = query_batch(
            (0..600).map(Some).collect(),
            vec![Some(vec![Some(1.0), Some(2.0)]); 600],
        );
        let mut run = test_run(vec![batch]).await;
        let pool = run.context.runtime_env().memory_pool.clone();
        let cancel = run.cancel.0.clone();
        let (ids, queries) = read_test_window(&mut run, 2, QUERY_WINDOW_SIZE)
            .await
            .unwrap();
        assert_eq!(ids, (0..256).collect::<Vec<_>>());
        assert_eq!(queries.len(), QUERY_WINDOW_SIZE);
        assert_eq!(
            run.offset, QUERY_WINDOW_SIZE,
            "no eager scan of later windows"
        );
        assert_eq!(
            read_test_window(&mut run, 2, QUERY_WINDOW_SIZE)
                .await
                .unwrap()
                .0,
            (256..512).collect::<Vec<_>>()
        );
        assert_eq!(
            read_test_window(&mut run, 2, QUERY_WINDOW_SIZE)
                .await
                .unwrap()
                .0,
            (512..600).collect::<Vec<_>>()
        );
        assert!(
            read_test_window(&mut run, 2, QUERY_WINDOW_SIZE)
                .await
                .unwrap()
                .0
                .is_empty()
        );
        assert!(
            pool.reserved() > 0,
            "global qid uniqueness state is reserved"
        );
        drop(run);
        assert!(cancel.load(Ordering::Acquire));
        assert_eq!(
            pool.reserved(),
            0,
            "drop releases input and qid reservations"
        );
    }

    #[tokio::test]
    async fn vector_batch_windows_coalesce_many_small_child_batches() {
        let mut batches = Vec::new();
        let mut next_id = 0i64;
        while next_id < 1000 {
            let end = (next_id + 35).min(1000);
            let ids = (next_id..end).map(Some).collect();
            let queries =
                vec![Some(vec![Some(1.0), Some(2.0)]); (end - next_id) as usize];
            batches.push(query_batch(ids, queries));
            next_id = end;
        }
        let mut run = test_run(batches).await;
        let mut all_ids = Vec::new();
        let mut window_sizes = Vec::new();
        loop {
            let (ids, queries) = read_test_window(&mut run, 2, QUERY_WINDOW_SIZE)
                .await
                .unwrap();
            if queries.is_empty() {
                break;
            }
            window_sizes.push(ids.len());
            all_ids.extend(ids);
        }
        assert_eq!(window_sizes, [256, 256, 256, 232]);
        assert_eq!(all_ids, (0..1000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn vector_batch_duplicate_qids_are_rejected_across_windows() {
        let ids = (0..256).map(Some).chain([Some(7)]).collect();
        let batch = query_batch(ids, vec![Some(vec![Some(1.0)]); 257]);
        let mut run = test_run(vec![batch]).await;
        read_test_window(&mut run, 1, QUERY_WINDOW_SIZE)
            .await
            .unwrap();
        assert!(
            read_test_window(&mut run, 1, QUERY_WINDOW_SIZE)
                .await
                .unwrap_err()
                .to_string()
                .contains("duplicate query_id 7")
        );
    }

    #[tokio::test]
    async fn vector_batch_validates_null_dimensions_and_finite_elements() {
        for (id, query, dim, message) in [
            (None, Some(vec![Some(1.0)]), 1, "query_id"),
            (Some(1), None, 1, "query vector"),
            (Some(1), Some(vec![None]), 1, "non-NULL"),
            (Some(1), Some(vec![Some(f32::INFINITY)]), 1, "finite"),
            (Some(1), Some(vec![Some(1.0)]), 2, "dimension"),
        ] {
            let mut run = test_run(vec![query_batch(vec![id], vec![query])]).await;
            let error = read_test_window(&mut run, dim, QUERY_WINDOW_SIZE)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(message), "{error}");
        }
        let schema = Schema::new(vec![
            Field::new("qid", DataType::Int64, false),
            Field::new(
                "qvec",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, false))),
                false,
            ),
        ]);
        assert!(validate_query_schema(&schema).is_err());
    }

    #[tokio::test]
    async fn vector_batch_input_limits_are_cumulative_across_windows() {
        let batch = query_batch(vec![Some(100000)], vec![Some(vec![Some(1.0)])]);
        let mut run = test_run(vec![batch.clone()]).await;
        run.seen.extend(0..MAX_BATCH_QUERIES as i64);
        assert!(
            read_test_window(&mut run, 1, QUERY_WINDOW_SIZE)
                .await
                .unwrap_err()
                .to_string()
                .contains("at most")
        );
        let mut run = test_run(vec![batch]).await;
        run.input_bytes = MAX_BATCH_INPUT_BYTES - 1;
        assert!(
            read_test_window(&mut run, 1, QUERY_WINDOW_SIZE)
                .await
                .unwrap_err()
                .to_string()
                .contains("64 MiB")
        );
    }

    #[test]
    fn vector_batch_output_preserves_qids_and_resets_rank() {
        let batch = hits_batch(
            &[91, -4],
            vec![
                vec![
                    NativeBatchHit {
                        id: 2,
                        distance: 0.5,
                    },
                    NativeBatchHit {
                        id: 3,
                        distance: 0.75,
                    },
                ],
                vec![NativeBatchHit {
                    id: 7,
                    distance: 1.25,
                }],
            ],
        )
        .unwrap();
        assert_eq!(batch.schema(), output_schema());
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[91, 91, -4]
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 2, 1]
        );
    }
}
