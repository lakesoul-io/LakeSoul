// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Bounded distributed native batch windows.
//!
//! The SQL stream pins the target once and retains that snapshot while this
//! module dispatches each window. A window is an Arrow memory-source child
//! (serialized as IPC), broadcast through Flight to shard probes. No query
//! vector, storage credential, or latest-table lookup belongs to the probe
//! descriptor. Every hash bucket, including all its range partitions, stays
//! on one probe task, so per-key version reconciliation remains local.
//!
//! A separate DistributedExec per window intentionally bounds the library's
//! BroadcastExec cache, which retains its complete input until the stage ends.

use std::collections::BTreeMap;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{
    Array, FixedSizeListArray, Float32Builder, Float64Array, Int64Array, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryReservation,
};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, collect, execute_stream,
};
use datafusion_distributed::{
    BroadcastExec, DistributedConfig, DistributedExec, DistributedExt,
    DistributedTaskContext, NetworkBoundary, NetworkBoundaryExt, NetworkBroadcastExec,
    NetworkCoalesceExec, Stage, get_distributed_worker_resolver,
};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::StreamExt;
use lakesoul_metadata::MetaDataClient;
use uuid::Uuid;

use super::native::{
    DEFAULT_QUERY_WINDOW, NativeBatchHit, NativeBatchOptions, NativeBatchSnapshot,
    NativeBatchSnapshotDescriptor,
};

/// Installed only on coordinator sessions configured for distributed execution.
#[derive(Debug, Clone)]
pub struct NativeBatchDistribution {
    pub fallback_to_local: bool,
}

fn execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "distributed batch window memory estimate overflow".into(),
    )
}

fn check_cancel(cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Acquire) {
        Err(execution_error("distributed vector batch cancelled"))
    } else {
        Ok(())
    }
}

async fn wait_for_cancel(cancel: &AtomicBool) {
    // Native APIs use an AtomicBool so kernels can poll without async state.
    // Bridge it to exchange cancellation without retaining a dropped client
    // until the entire native window has finished.
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
    loop {
        if cancel.load(Ordering::Acquire) {
            return;
        }
        interval.tick().await;
    }
}

/// Returns None only before any task has been dispatched: the session is
/// local, a static worker list is empty, or the configured planning fallback
/// accepts a preflight error. Resolver failures and every post-dispatch error
/// are propagated. The caller retains its existing local engine for None.
pub(crate) async fn maybe_search_distributed_window(
    snapshot: Arc<NativeBatchSnapshot>,
    queries: &[Vec<f32>],
    options: &NativeBatchOptions,
    context: Arc<TaskContext>,
    cancel: Arc<AtomicBool>,
) -> Result<Option<Vec<Vec<NativeBatchHit>>>> {
    let Some(policy) = context
        .session_config()
        .get_extension::<NativeBatchDistribution>()
    else {
        return Ok(None);
    };
    // A metadata discovery failure must never become an accidental local run.
    let urls = get_distributed_worker_resolver(context.session_config())?.get_urls()?;
    if urls.is_empty() {
        return Ok(None);
    }
    check_cancel(&cancel)?;
    options.validate()?;
    snapshot.ensure_valid()?;
    if queries.is_empty() || snapshot.is_empty() {
        return Ok(Some(vec![Vec::new(); queries.len()]));
    }

    let prepared = (|| {
        let config = DistributedConfig::from_session_config(context.session_config())?;
        let limit = if config.max_tasks_per_stage == 0 {
            urls.len()
        } else {
            config.max_tasks_per_stage.min(urls.len())
        };
        let tasks = bucket_groups(snapshot.descriptor())?
            .len()
            .min(limit)
            .max(1);
        let plan = build_window_plan(
            snapshot.descriptor().clone(),
            queries,
            options.clone(),
            tasks,
        )?;
        // The frozen URL set fixes the mapping for this window. Changes in
        // discovery cannot resize an already-encoded probe assignment.
        let config = context
            .session_config()
            .clone()
            .with_distributed_worker_resolver(crate::distributed::WorkerSnapshot::new(
                urls.clone(),
            ))
            .with_distributed_dynamic_task_count(false)?;
        let task_context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        // Encode every worker stage before execution, while fallback remains
        // possible. The coordinator-only DistributedExec has no wire form.
        preflight_window_plan(&plan)?;
        let bytes = queries
            .len()
            .checked_mul(options.top_k.checked_add(1).ok_or_else(overflow)?)
            .and_then(|n| n.checked_mul(tasks))
            .and_then(|n| n.checked_mul(64))
            .and_then(|n| {
                queries
                    .len()
                    .checked_mul(snapshot.dim())?
                    .checked_mul(8)?
                    .checked_add(n)
            })
            .ok_or_else(overflow)?;
        let reservation = MemoryConsumer::new("Distributed native vector batch window")
            .register(context.memory_pool());
        reservation.try_grow(bytes)?;
        Ok::<_, DataFusionError>((plan, task_context, reservation, tasks))
    })();
    let (plan, task_context, _reservation, tasks) = match prepared {
        Ok(value) => value,
        Err(error) if policy.fallback_to_local => {
            warn!(%error, "distributed vector batch preflight failed; using configured local fallback");
            return Ok(None);
        }
        Err(error) => return Err(error),
    };

    // Crossing this line commits to remote execution: errors never rerun the
    // query locally. Retain the coordinator snapshot's parent lease until all
    // remote probes and the global merge have completed.
    let batches = tokio::select! {
        result = collect(plan, task_context) => result?,
        _ = snapshot.cancelled() => {
            cancel.store(true, Ordering::Release);
            return Err(execution_error("distributed batch parent read lease lost"));
        }
        _ = wait_for_cancel(&cancel) => {
            return Err(execution_error("distributed vector batch cancelled"));
        }
    };
    check_cancel(&cancel)?;
    snapshot.ensure_valid()?;
    let merged = merge_probe_batches(&batches, queries.len(), tasks, options.top_k)?;
    snapshot.metrics.windows.fetch_add(1, Ordering::Relaxed);
    snapshot
        .metrics
        .queries
        .fetch_add(queries.len() as u64, Ordering::Relaxed);
    snapshot
        .metrics
        .shard_probes
        .fetch_add(snapshot.descriptor().shards.len() as u64, Ordering::Relaxed);
    snapshot
        .metrics
        .rerank_blocks
        .fetch_add(merged.metrics.rerank_blocks, Ordering::Relaxed);
    snapshot
        .metrics
        .cluster_loads
        .fetch_add(merged.metrics.cluster_loads, Ordering::Relaxed);
    snapshot
        .metrics
        .shared_block_reuses
        .fetch_add(merged.metrics.shared_block_reuses, Ordering::Relaxed);
    snapshot
        .metrics
        .peak_workers
        .fetch_max(merged.metrics.peak_workers, Ordering::Relaxed);
    Ok(Some(merged.results))
}

pub(crate) fn preflight_window_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    let codec = crate::distributed::codec::composed_codec();
    plan.apply(|node| {
        if let Some(boundary) = node.as_network_boundary()
            && let Some(stage_plan) = boundary.input_stage().local_plan()
        {
            PhysicalPlanNode::try_from_physical_plan(stage_plan.clone(), &codec)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

/// The bucket suffix is part of LakeSoul's physical index prefix contract.
/// Range partitions with one bucket are assigned together for I/O locality;
/// primary keys remain partition-scoped and identical IDs in separate
/// partitions are distinct visible rows, not versions to deduplicate.
fn bucket_groups(descriptor: &NativeBatchSnapshotDescriptor) -> Result<Vec<Vec<usize>>> {
    let mut groups: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (index, shard) in descriptor.shards.iter().enumerate() {
        let bucket = shard
            .index_prefix
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "batch shard has no numeric hash bucket suffix: {:?}",
                    shard.index_prefix
                ))
            })?;
        groups.entry(bucket).or_default().push(index);
    }
    Ok(groups.into_values().collect())
}

pub(crate) fn assigned_snapshot(
    descriptor: &NativeBatchSnapshotDescriptor,
    task_index: usize,
    task_count: usize,
) -> Result<NativeBatchSnapshotDescriptor> {
    let groups = bucket_groups(descriptor)?;
    if task_count == 0 || task_index >= task_count || task_count > groups.len() {
        return Err(execution_error(
            "invalid distributed batch shard assignment",
        ));
    }
    let mut assigned = descriptor.clone();
    assigned.shards = groups
        .into_iter()
        .enumerate()
        .filter(|(group, _)| group % task_count == task_index)
        .flat_map(|(_, indices)| {
            indices
                .into_iter()
                .map(|index| descriptor.shards[index].clone())
        })
        .collect();
    Ok(assigned)
}

fn query_batch(queries: &[Vec<f32>], dim: usize) -> Result<RecordBatch> {
    if queries.len() > DEFAULT_QUERY_WINDOW || dim == 0 || dim > i32::MAX as usize {
        return Err(DataFusionError::Plan(
            "distributed queries must fit one native query window".into(),
        ));
    }
    let mut vectors =
        arrow::array::FixedSizeListBuilder::new(Float32Builder::new(), dim as i32);
    for query in queries {
        if query.len() != dim || query.iter().any(|value| !value.is_finite()) {
            return Err(DataFusionError::Plan(
                "invalid distributed batch query dimension or non-finite value".into(),
            ));
        }
        vectors.values().append_slice(query);
        vectors.append(true);
    }
    let vectors = vectors.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("query_index", DataType::UInt32, false),
        Field::new("query_vector", vectors.data_type().clone(), false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt32Array::from_iter_values(0..queries.len() as u32)),
            Arc::new(vectors),
        ],
    )?)
}

pub(crate) fn probe_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_index", DataType::UInt32, true),
        Field::new("worker_index", DataType::UInt32, false),
        Field::new("id", DataType::Int64, true),
        Field::new("distance", DataType::Float64, true),
        Field::new("rerank_blocks", DataType::UInt64, false),
        Field::new("cluster_loads", DataType::UInt64, false),
        Field::new("shared_block_reuses", DataType::UInt64, false),
        Field::new("peak_workers", DataType::UInt64, false),
    ]))
}

/// Construct explicit stage boundaries rather than relying on generic
/// file-scan estimation (which cannot shard a custom non-leaf vector probe).
pub(crate) fn build_window_plan(
    descriptor: NativeBatchSnapshotDescriptor,
    queries: &[Vec<f32>],
    options: NativeBatchOptions,
    task_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let batch = query_batch(queries, descriptor.dim)?;
    let source: Arc<dyn ExecutionPlan> =
        MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None)?;
    // NetworkBroadcast exposes one logical input partition per consumer. Its
    // producer has task_count virtual partitions, each reading the same IPC
    // window from the bounded Broadcast cache.
    let broadcast: Arc<dyn ExecutionPlan> =
        Arc::new(BroadcastExec::new(source.clone(), 1));
    let boundary = NetworkBroadcastExec::try_new(broadcast, 1)?;
    let Stage::Local(mut stage) = boundary.input_stage().clone() else {
        unreachable!()
    };
    stage.plan = Arc::new(BroadcastExec::new(source, task_count));
    let input = boundary.with_input_stage(Stage::Local(stage))?;
    let probe: Arc<dyn ExecutionPlan> = Arc::new(NativeBatchShardProbeExec::try_new(
        descriptor, options, task_count, input,
    )?);
    let coalesce: Arc<dyn ExecutionPlan> =
        Arc::new(NetworkCoalesceExec::try_new(probe, task_count, 1)?);
    let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(coalesce));
    // The library's boundary preparation is private. These are already the
    // correct producer heads; assign unique stage identities through its
    // public NetworkBoundary interface before DistributedExec dispatches.
    let query_id = Uuid::new_v4();
    let mut stage_id = 0;
    let plan = plan
        .transform_up(|node| {
            let Some(boundary) = node.as_network_boundary() else {
                return Ok(Transformed::no(node));
            };
            let Stage::Local(mut stage) = boundary.input_stage().clone() else {
                return Err(execution_error("new batch stage was unexpectedly remote"));
            };
            stage_id += 1;
            stage.query_id = query_id;
            stage.num = stage_id;
            Ok(Transformed::yes(
                boundary.with_input_stage(Stage::Local(stage))?,
            ))
        })?
        .data;
    Ok(Arc::new(
        DistributedExec::new(plan).with_metrics_collection(true),
    ))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct NativeBatchWorkerMetrics {
    pub rerank_blocks: u64,
    pub cluster_loads: u64,
    pub shared_block_reuses: u64,
    pub peak_workers: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct NativeBatchMerged {
    pub results: Vec<Vec<NativeBatchHit>>,
    pub metrics: NativeBatchWorkerMetrics,
}

/// Worker-side operator. The task context selects only the assigned buckets;
/// reconstruction consumes the pinned descriptor and local credentials.
#[derive(Debug)]
pub(crate) struct NativeBatchShardProbeExec {
    descriptor: NativeBatchSnapshotDescriptor,
    options: NativeBatchOptions,
    task_count: usize,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl NativeBatchShardProbeExec {
    pub(crate) fn try_new(
        descriptor: NativeBatchSnapshotDescriptor,
        options: NativeBatchOptions,
        task_count: usize,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        options.validate()?;
        if descriptor.dim == 0
            || task_count == 0
            || task_count > bucket_groups(&descriptor)?.len()
        {
            return Err(DataFusionError::Plan(
                "invalid native batch probe descriptor or task count".into(),
            ));
        }
        let schema = input.schema();
        if schema.fields().len() != 2
            || schema.field(0).data_type() != &DataType::UInt32
            || !matches!(schema.field(1).data_type(), DataType::FixedSizeList(field, size) if *size == descriptor.dim as i32 && field.data_type() == &DataType::Float32)
        {
            return Err(DataFusionError::Plan(
                "native batch probe child must be query-index/float32-vector IPC".into(),
            ));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(probe_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            descriptor,
            options,
            task_count,
            input,
            properties,
        })
    }
    pub(crate) fn descriptor(&self) -> &NativeBatchSnapshotDescriptor {
        &self.descriptor
    }
    pub(crate) fn options(&self) -> &NativeBatchOptions {
        &self.options
    }
    pub(crate) fn task_count(&self) -> usize {
        self.task_count
    }
}

impl DisplayAs for NativeBatchShardProbeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "NativeBatchShardProbeExec: table={}, epoch={}, shards={}, tasks={}, top_k={}",
            self.descriptor.table_id,
            self.descriptor.epoch,
            self.descriptor.shards.len(),
            self.task_count,
            self.options.top_k
        )
    }
}

impl ExecutionPlan for NativeBatchShardProbeExec {
    fn name(&self) -> &str {
        "NativeBatchShardProbeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(execution_error("native batch probe requires one IPC child"));
        }
        Ok(Arc::new(Self::try_new(
            self.descriptor.clone(),
            self.options.clone(),
            self.task_count,
            children[0].clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(execution_error(
                "native batch probe has one local output partition",
            ));
        }
        let task = DistributedTaskContext::from_ctx(&context);
        if task.task_count != self.task_count {
            return Err(execution_error(format!(
                "batch probe expected {} tasks, received {}",
                self.task_count, task.task_count
            )));
        }
        let descriptor =
            assigned_snapshot(&self.descriptor, task.task_index, task.task_count)?;
        let input = execute_stream(self.input.clone(), context.clone())?;
        let mut options = self.options.clone();
        if let MemoryLimit::Finite(limit) = context.memory_pool().memory_limit() {
            options.memory_bytes = options.memory_bytes.min(limit / 2);
        }
        options.validate()?;
        let task_index = task.task_index;
        let cancel = CancelOnDrop(Arc::new(AtomicBool::new(false)));
        let future = async move {
            let mut run = ProbeRun {
                input,
                queries: Vec::new(),
                reservation: MemoryConsumer::new("Native batch shard probe")
                    .register(context.memory_pool()),
                cancel,
            };
            while let Some(batch) = run.input.next().await.transpose()? {
                run.reservation.try_grow(batch.get_array_memory_size())?;
                append_queries(&batch, descriptor.dim, &mut run.queries)?;
            }
            if run.queries.is_empty() {
                return probe_batch(
                    task.task_index,
                    Vec::new(),
                    NativeBatchWorkerMetrics::default(),
                );
            }
            run.reservation.try_grow(options.memory_bytes)?;
            let path = datafusion::datasource::listing::ListingTableUrl::parse(
                &descriptor.table_path,
            )?;
            let store = context.runtime_env().object_store(path.object_store())?;
            let client = MetaDataClient::from_env()
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let snapshot = NativeBatchSnapshot::from_descriptor(
                descriptor,
                store,
                &client.vector_index_catalog(),
            )
            .await?;
            let ProbeRun {
                queries,
                reservation,
                cancel,
                ..
            } = run;
            let stop = cancel.0.clone();
            // Dropping the Flight consumer signals cancellation immediately,
            // while the detached task retains its lease and memory until the
            // cooperative native kernel has exited.
            let worker_snapshot = snapshot.clone();
            let task = tokio::spawn(async move {
                let _reservation = reservation;
                worker_snapshot.search(&queries, &options, stop).await
            });
            let results = task.await.map_err(|error| {
                execution_error(format!("batch worker probe task: {error}"))
            })??;
            let metrics = NativeBatchWorkerMetrics {
                rerank_blocks: snapshot.metrics.rerank_blocks.load(Ordering::Relaxed),
                cluster_loads: snapshot.metrics.cluster_loads.load(Ordering::Relaxed),
                shared_block_reuses: snapshot
                    .metrics
                    .shared_block_reuses
                    .load(Ordering::Relaxed),
                peak_workers: snapshot.metrics.peak_workers.load(Ordering::Relaxed),
            };
            drop(cancel);
            probe_batch(task_index, results, metrics)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            probe_schema(),
            futures::stream::once(future),
        )))
    }
}

struct CancelOnDrop(Arc<AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct ProbeRun {
    input: SendableRecordBatchStream,
    queries: Vec<Vec<f32>>,
    reservation: MemoryReservation,
    cancel: CancelOnDrop,
}

fn append_queries(
    batch: &RecordBatch,
    dim: usize,
    output: &mut Vec<Vec<f32>>,
) -> Result<()> {
    let indices = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| execution_error("batch IPC query indices changed type"))?;
    let vectors = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| execution_error("batch IPC query vectors changed type"))?;
    if output
        .len()
        .checked_add(batch.num_rows())
        .is_none_or(|size| size > DEFAULT_QUERY_WINDOW)
    {
        return Err(execution_error(
            "batch worker received more than one bounded query window",
        ));
    }
    for row in 0..batch.num_rows() {
        if indices.is_null(row)
            || indices.value(row) as usize != output.len()
            || vectors.is_null(row)
        {
            return Err(execution_error(
                "batch IPC query window lost order or contains NULL",
            ));
        }
        let array = vectors.value(row);
        let values = array
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .ok_or_else(|| execution_error("batch IPC vector elements changed type"))?;
        if values.len() != dim
            || values.null_count() != 0
            || values.values().iter().any(|v| !v.is_finite())
        {
            return Err(execution_error("invalid batch IPC vector"));
        }
        output.push(values.values().to_vec());
    }
    Ok(())
}

/// A NULL query index identifies the single worker telemetry summary. A
/// non-NULL query index with NULL id/distance identifies query completion.
/// Actual hits carry a non-null id/distance pair. This internal schema never
/// escapes the distributed helper into SQL output.
fn probe_batch(
    worker: usize,
    results: Vec<Vec<NativeBatchHit>>,
    metrics: NativeBatchWorkerMetrics,
) -> Result<RecordBatch> {
    let mut queries = Vec::new();
    let mut workers = Vec::new();
    let mut ids = Vec::new();
    let mut distances = Vec::new();
    let mut rerank_blocks = Vec::new();
    let mut cluster_loads = Vec::new();
    let mut shared_block_reuses = Vec::new();
    let mut peak_workers = Vec::new();
    for (query, hits) in results.into_iter().enumerate() {
        queries.push(Some(query as u32));
        workers.push(worker as u32);
        ids.push(None);
        distances.push(None);
        rerank_blocks.push(0);
        cluster_loads.push(0);
        shared_block_reuses.push(0);
        peak_workers.push(0);
        for hit in hits {
            queries.push(Some(query as u32));
            workers.push(worker as u32);
            ids.push(Some(hit.id));
            distances.push(Some(hit.distance));
            rerank_blocks.push(0);
            cluster_loads.push(0);
            shared_block_reuses.push(0);
            peak_workers.push(0);
        }
    }
    queries.push(None);
    workers.push(worker as u32);
    ids.push(None);
    distances.push(None);
    rerank_blocks.push(metrics.rerank_blocks);
    cluster_loads.push(metrics.cluster_loads);
    shared_block_reuses.push(metrics.shared_block_reuses);
    peak_workers.push(metrics.peak_workers);
    Ok(RecordBatch::try_new(
        probe_schema(),
        vec![
            Arc::new(UInt32Array::from(queries)),
            Arc::new(UInt32Array::from(workers)),
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(distances)),
            Arc::new(arrow::array::UInt64Array::from(rerank_blocks)),
            Arc::new(arrow::array::UInt64Array::from(cluster_loads)),
            Arc::new(arrow::array::UInt64Array::from(shared_block_reuses)),
            Arc::new(arrow::array::UInt64Array::from(peak_workers)),
        ],
    )?)
}

pub(crate) fn merge_probe_batches(
    batches: &[RecordBatch],
    query_count: usize,
    task_count: usize,
    top_k: usize,
) -> Result<NativeBatchMerged> {
    let mut completed =
        vec![false; query_count.checked_mul(task_count).ok_or_else(overflow)?];
    let mut summaries = vec![false; task_count];
    let mut metrics = NativeBatchWorkerMetrics::default();
    let mut per_query: Vec<Vec<NativeBatchHit>> = vec![Vec::new(); query_count];
    for batch in batches {
        if batch.schema() != probe_schema() {
            return Err(execution_error("batch worker returned an invalid schema"));
        }
        let queries = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let workers = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let distances = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let rerank_blocks = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let cluster_loads = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let shared_block_reuses = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let peak_workers = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if workers.is_null(row)
                || rerank_blocks.is_null(row)
                || cluster_loads.is_null(row)
                || shared_block_reuses.is_null(row)
                || peak_workers.is_null(row)
            {
                return Err(execution_error(
                    "batch worker omitted task identity or telemetry",
                ));
            }
            let worker = workers.value(row) as usize;
            if worker >= task_count {
                return Err(execution_error("batch worker returned an unknown task"));
            }
            let has_id = !ids.is_null(row);
            let has_distance = !distances.is_null(row);
            if queries.is_null(row) {
                if has_id || has_distance {
                    return Err(execution_error("worker summary cannot contain a hit"));
                }
                if std::mem::replace(&mut summaries[worker], true) {
                    return Err(execution_error("duplicate worker telemetry summary"));
                }
                metrics.rerank_blocks = metrics
                    .rerank_blocks
                    .checked_add(rerank_blocks.value(row))
                    .ok_or_else(overflow)?;
                metrics.cluster_loads = metrics
                    .cluster_loads
                    .checked_add(cluster_loads.value(row))
                    .ok_or_else(overflow)?;
                metrics.shared_block_reuses = metrics
                    .shared_block_reuses
                    .checked_add(shared_block_reuses.value(row))
                    .ok_or_else(overflow)?;
                metrics.peak_workers = metrics.peak_workers.max(peak_workers.value(row));
                continue;
            }
            let query = queries.value(row) as usize;
            if query >= query_count {
                return Err(execution_error("batch worker returned an unknown query"));
            }
            if has_id && has_distance && distances.value(row).is_finite() {
                per_query[query].push(NativeBatchHit {
                    id: ids.value(row),
                    distance: distances.value(row),
                });
            } else if !has_id && !has_distance {
                let slot = query * task_count + worker;
                if std::mem::replace(&mut completed[slot], true) {
                    return Err(execution_error(
                        "duplicate batch worker completion marker",
                    ));
                }
            } else {
                return Err(execution_error(
                    "batch worker returned a NULL or non-finite hit",
                ));
            }
            if rerank_blocks.value(row) != 0
                || cluster_loads.value(row) != 0
                || shared_block_reuses.value(row) != 0
                || peak_workers.value(row) != 0
            {
                return Err(execution_error(
                    "worker telemetry is allowed only on its summary row",
                ));
            }
        }
    }
    if completed.iter().any(|done| !done) {
        return Err(execution_error(
            "batch window ended before every worker completed every query",
        ));
    }
    if summaries.iter().any(|done| !done) {
        return Err(execution_error(
            "batch window ended before every worker reported telemetry",
        ));
    }
    for hits in &mut per_query {
        // `id` alone is not globally unique: primary keys are scoped by range
        // partition. Never deduplicate on id in this merge; the native scoped
        // snapshot search has already reconciled versions within each shard.
        hits.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        hits.truncate(top_k);
    }
    Ok(NativeBatchMerged {
        results: per_query,
        metrics,
    })
}
