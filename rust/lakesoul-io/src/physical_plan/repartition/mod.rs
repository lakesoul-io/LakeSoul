// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! This module provides the implementation of the repartition operator.

use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::{ArrayRef, RecordBatch, builder::UInt64Builder};
use arrow_schema::SchemaRef;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::{
    common::runtime::SpawnedTask,
    common::utils::transpose,
    execution::{TaskContext, memory_pool::MemoryConsumer},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PhysicalExpr, PlanProperties, RecordBatchStream,
        SendableRecordBatchStream,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder},
        sorts::streaming_merge::StreamingMergeBuilder,
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion::{physical_expr::physical_exprs_equal, physical_plan::metrics};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result as DFResult, Statistics};
use datafusion_physical_plan::apply_expression_roots;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use rootcause::{Report, bail, compat::boxed_error::IntoBoxedError};

use self::distributor_channels::{
    DistributionReceiver, DistributionSender, channels, partition_aware_channels,
};
use crate::Result;
use crate::utils::hash::create_hashes;

mod distributor_channels;

type MaybeBatch = Option<Result<RecordBatch>>;
type InputPartitionsToCurrentPartitionSender = Vec<DistributionSender<MaybeBatch>>;
type InputPartitionsToCurrentPartitionReceiver = Vec<DistributionReceiver<MaybeBatch>>;

/// Lazily initialized state
///
/// Note that the state is initialized ONCE for all partitions by a single task(thread).
/// This may take a short while.  It is also like that multiple threads
/// call execute at the same time, because we have just started "target partitions" tasks
/// which is commonly set to the number of CPU cores and all call execute at the same time.
///
/// Thus, use a **tokio** `OnceCell` for this initialization so as not to waste CPU cycles
/// in a mutex lock but instead allow other threads to do something useful.
///
/// Use a parking_lot `Mutex` to control other accesses as they are very short duration
///  (e.g., removing channels on completion) where the overhead of `await` is not warranted.
type LazyState = Arc<tokio::sync::OnceCell<Mutex<RepartitionByRangeAndHashExecState>>>;

/// Inner state of [`RepartitionByRangeAndHashExec`].
#[derive(Debug)]
struct RepartitionByRangeAndHashExecState {
    /// Channels for sending batches from input partitions to output partitions.
    /// Key is the partition number.
    channels: HashMap<
        usize,
        (
            InputPartitionsToCurrentPartitionSender,
            InputPartitionsToCurrentPartitionReceiver,
        ),
    >,
    /// Helper that ensures that that background job is killed once it is no longer needed.
    abort_helper: Arc<Vec<SpawnedTask<()>>>,
}

impl RepartitionByRangeAndHashExecState {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        range_partitioning_expr: Vec<Arc<dyn PhysicalExpr>>,
        hash_partitioning: Partitioning,
        metrics: ExecutionPlanMetricsSet,
        preserve_order: bool,
        context: Arc<TaskContext>,
    ) -> Self {
        let num_input_partitions = input.output_partitioning().partition_count();
        let num_output_partitions = hash_partitioning.partition_count();

        let (txs, rxs) = if preserve_order {
            let (txs, rxs) =
                partition_aware_channels(num_input_partitions, num_output_partitions);
            // Take transpose of senders and receivers. `state.channels` keeps track of entries per output partition
            let txs = transpose(txs);
            let rxs = transpose(rxs);
            (txs, rxs)
        } else {
            // create one channel per *output* partition
            // note we use a custom channel that ensures there is always data for each receiver
            // but limits the amount of buffering if required.
            let (txs, rxs) = channels(num_output_partitions);
            // Clone sender for each input partition
            let txs = txs
                .into_iter()
                .map(|item| vec![item; num_input_partitions])
                .collect::<Vec<_>>();
            let rxs = rxs.into_iter().map(|item| vec![item]).collect::<Vec<_>>();
            (txs, rxs)
        };

        let mut channels = HashMap::new();
        for (partition, (tx, rx)) in txs.into_iter().zip(rxs).enumerate() {
            channels.insert(partition, (tx, rx));
        }
        // launch one async task per *input* partition
        let mut spawned_tasks = Vec::with_capacity(num_input_partitions);
        for i in 0..num_input_partitions {
            let txs: HashMap<_, _> = channels
                .iter()
                .map(|(partition, (tx, _rx))| (*partition, (tx[i].clone())))
                .collect();

            let r_metrics = RepartitionMetrics::new(i, num_output_partitions, &metrics);

            let input_task =
                SpawnedTask::spawn(RepartitionByRangeAndHashExec::pull_from_input(
                    Arc::clone(&input),
                    i,
                    txs.clone(),
                    range_partitioning_expr.clone(),
                    hash_partitioning.clone(),
                    r_metrics,
                    metrics.clone(),
                    context.clone(),
                ));

            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            let wait_for_task = SpawnedTask::spawn(
                RepartitionByRangeAndHashExec::wait_for_task(input_task, txs),
            );
            spawned_tasks.push(wait_for_task);
        }

        Self {
            channels,
            abort_helper: Arc::new(spawned_tasks),
        }
    }
}

/// A utility that can be used to partition batches based on [`Partitioning`]
pub struct BatchPartitioner {
    state: BatchPartitionerState,
    timer: metrics::Time,
}

/// The state of the [`BatchPartitioner`].
struct BatchPartitionerState {
    /// The range partitioning expressions.
    range_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// The hash partitioning expressions.
    hash_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// The number of partitions.
    num_partitions: usize,
    /// The hash buffer.
    hash_buffer: Vec<u32>,
}

impl BatchPartitioner {
    /// Create a new [`BatchPartitioner`] with the provided [`Partitioning`]
    ///
    /// The time spent repartitioning will be recorded to `timer`
    pub fn try_new(
        range_partitioning_expr: Vec<Arc<dyn PhysicalExpr>>,
        hash_partitioning: Partitioning,
        timer: metrics::Time,
    ) -> Result<Self> {
        let state = match hash_partitioning {
            Partitioning::Hash(exprs, num_partitions) => BatchPartitionerState {
                range_exprs: range_partitioning_expr,
                hash_exprs: exprs,
                num_partitions,
                hash_buffer: vec![],
            },
            other => {
                bail!("Unsupported repartitioning scheme {other:?}");
            }
        };

        Ok(Self { state, timer })
    }

    /// Partition the provided [`RecordBatch`] into one or more partitioned [`RecordBatch`]
    /// based on the [`Partitioning`] specified on construction
    ///
    /// `f` will be called for each partitioned [`RecordBatch`] with the corresponding
    /// partition index. Any error returned by `f` will be immediately returned by this
    /// function without attempting to publish further [`RecordBatch`]
    ///
    /// The time spent repartitioning, not including time spent in `f` will be recorded
    /// to the [`metrics::Time`] provided on construction
    pub fn partition<F>(&mut self, batch: RecordBatch, mut f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        self.partition_iter(batch)?.try_for_each(|res| match res {
            Ok((partition, batch)) => f(partition, batch),
            Err(e) => Err(e),
        })
    }

    /// Actual implementation of [`partition`](Self::partition).
    ///
    /// The reason this was pulled out is that we need to have a variant of `partition` that works w/ sync functions,
    /// and one that works w/ async. Using an iterator as an intermediate representation was the best way to achieve
    /// this (so we don't need to clone the entire implementation).
    fn partition_iter(
        &mut self,
        batch: RecordBatch,
    ) -> Result<impl Iterator<Item = Result<(usize, RecordBatch)>> + Send + '_> {
        let BatchPartitionerState {
            // random_state,
            range_exprs,
            hash_exprs,
            num_partitions: partitions,
            hash_buffer,
        } = &mut self.state;
        let it: Box<dyn Iterator<Item = Result<(usize, RecordBatch)>> + Send> = {
            let _timer = self.timer.timer();

            let range_arrays = [range_exprs.clone()]
                .concat()
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<DFResult<Vec<_>>>()?;

            let hash_arrays = hash_exprs
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<DFResult<Vec<_>>>()?;

            hash_buffer.clear();
            hash_buffer.resize(batch.num_rows(), 0);

            let mut range_buffer = vec![0; batch.num_rows()];

            create_hashes(&hash_arrays, hash_buffer)?;
            create_hashes(&range_arrays, &mut range_buffer)?;

            let mut indices: Vec<HashMap<u32, UInt64Builder>> =
                (0..*partitions).map(|_| HashMap::new()).collect();

            for (index, (hash, range_hash)) in
                hash_buffer.iter().zip(range_buffer).enumerate()
            {
                indices[(*hash % *partitions as u32) as usize]
                    .entry(range_hash)
                    .or_insert_with(|| UInt64Builder::with_capacity(batch.num_rows()));
                if let Some(entry) =
                    indices[(*hash % *partitions as u32) as usize].get_mut(&range_hash)
                {
                    entry.append_value(index as u64);
                }
            }

            let timer_factory = self.timer.clone();

            let it = indices
                .into_iter()
                .enumerate()
                .flat_map(|(partition, mut indices_map)| {
                    let mut indices_vec = Vec::new();
                    for indices in indices_map.values_mut() {
                        indices_vec.push((partition, indices.finish()));
                    }
                    indices_vec
                })
                .map(move |(partition, indices)| {
                    // Produce batches based on indices
                    let _guard = timer_factory.timer();
                    let columns = batch
                        .columns()
                        .iter()
                        .map(|c| {
                            arrow::compute::take(c.as_ref(), &indices, None).map_err(
                                |e| DataFusionError::ArrowError(Box::new(e), None),
                            )
                        })
                        .collect::<DFResult<Vec<ArrayRef>>>()?;

                    let batch = RecordBatch::try_new(batch.schema(), columns)?;
                    Ok((partition, batch))
                });
            Box::new(it)
        };

        Ok(it)
    }

    // return the number of output partitions
    fn num_partitions(&self) -> usize {
        self.state.num_partitions
    }
}

#[derive(Debug, Clone)]
struct RepartitionMetrics {
    /// Time in nanos to execute child operator and fetch batches
    fetch_time: metrics::Time,
    /// Repartitioning elapsed time in nanos
    repartition_time: metrics::Time,
    /// Time in nanos for sending resulting batches to channels.
    /// One metric per output partition.
    send_time: Vec<metrics::Time>,
}

impl RepartitionMetrics {
    pub fn new(
        input_partition: usize,
        num_output_partitions: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        // Time in nanos to execute child operator and fetch batches
        let fetch_time =
            MetricBuilder::new(metrics).subset_time("fetch_time", input_partition);

        // Time in nanos to perform repartitioning
        let repartition_time =
            MetricBuilder::new(metrics).subset_time("repartition_time", input_partition);

        // Time in nanos for sending resulting batches to channels
        let send_time = (0..num_output_partitions)
            .map(|output_partition| {
                let label =
                    metrics::Label::new("outputPartition", output_partition.to_string());
                MetricBuilder::new(metrics)
                    .with_label(label)
                    .subset_time("send_time", input_partition)
            })
            .collect();

        Self {
            fetch_time,
            repartition_time,
            send_time,
        }
    }
}

/// The repartition operator that repartitions the input batches into the specified number of output partitions.
///
/// It uses the range partitioning and hash partitioning schemes to repartition the input batches.
/// Each input batch is repartitioned into output batches which will first be sorted by both range partitioning and hash partitioning,
/// then rows of the same range hash value will be grouped together into output batches.
/// Output batches are emitted in the order of the hash partitioning.
///
#[derive(Debug)]
pub struct RepartitionByRangeAndHashExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,

    /// Partitioning scheme to use
    range_partitioning_expr: Vec<Arc<dyn PhysicalExpr>>,

    /// Partitioning scheme to use
    hash_partitioning: Partitioning,

    /// Inner state that is initialized when the first output stream is created.
    state: LazyState,

    /// Boolean flag to decide whether to preserve ordering. If true means
    /// `SortPreservingRepartitionExec`, false means `RepartitionExec`.
    preserve_order: bool,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Execution properties
    plan_properties: Arc<PlanProperties>,
}

impl RepartitionByRangeAndHashExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Range Partitioning scheme to use
    pub fn range_partitioning(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.range_partitioning_expr.clone()
    }

    /// Hash Partitioning scheme to use
    pub fn hash_partitioning(&self) -> Partitioning {
        self.hash_partitioning.clone()
    }

    /// Get name used to display this Exec
    pub fn name(&self) -> &str {
        "RepartitionByRangeAndHashExec"
    }
}

impl DisplayAs for RepartitionByRangeAndHashExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "{}: hash_partitioning={}, input_partitions={}",
                    self.name(),
                    self.hash_partitioning,
                    self.input.output_partitioning().partition_count()
                )?;

                if let Some(sort_exprs) = self.sort_exprs() {
                    write!(f, ", sort_exprs={:?}", sort_exprs)?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => todo!(),
        }
    }
}

impl RepartitionByRangeAndHashExec {
    /// Create a new RepartitionExec, that produces output `partitioning`, and
    /// does not preserve the order of the input (see [`datafusion::physical_plan::repartition::RepartitionExec::preserve_order`]
    /// for more details)
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        range_partitioning_expr: Vec<Arc<dyn PhysicalExpr>>,
        hash_partitioning: Partitioning,
        metrics: ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let hash_exprs = match &hash_partitioning {
            Partitioning::Hash(hash_exprs, _) => hash_exprs.clone(),
            _ => {
                bail!(
                    "Invalid hash_partitioning={} for RepartitionByRangeAndHashExec",
                    hash_partitioning
                );
            }
        };
        // Batches are split by range and then hashed by the primary keys, and
        // the partitioning writer appends them to the range/bucket files in
        // encounter order without sorting again. The files are only sorted
        // (which merge-on-read relies on to deduplicate adjacent keys) if the
        // input is ordered by `range_partitions + primary_keys` *globally*,
        // which requires a single input partition: output_ordering() only
        // declares a per-partition order.
        if input.output_partitioning().partition_count() > 1 {
            bail!(
                "RepartitionByRangeAndHashExec requires a single input partition \
                to keep the bucket files ordered (got {} input partitions); coalesce \
                the sorted partitions (e.g. SortPreservingMergeExec) before repartitioning",
                input.output_partitioning().partition_count(),
            );
        }
        // Require that sequence to be a prefix of the input ordering: `SortExec`
        // keeps the input's pre-existing ordering after the sort prefix, so the
        // ordering it exposes can be longer than `range + hash`. An absent or
        // unrelated ordering must still be rejected.
        let required_order: Vec<Arc<dyn PhysicalExpr>> =
            [range_partitioning_expr.clone(), hash_exprs].concat();
        let input_ordering = match input.output_ordering() {
            Some(ordering) => ordering,
            None => bail!(
                "Input is not ordered by the range partitions and primary keys required by RepartitionByRangeAndHashExec (range_partitioning_expr={:?}, hash_partitioning={})",
                range_partitioning_expr,
                hash_partitioning,
            ),
        };
        let input_order: Vec<Arc<dyn PhysicalExpr>> = input_ordering
            .iter()
            .map(|sort_expr| sort_expr.expr.clone())
            .collect();
        if input_order.len() < required_order.len()
            || !physical_exprs_equal(
                &input_order[..required_order.len()],
                &required_order,
            )
        {
            bail!(
                "Input ordering {:?} is not compatible with \
                RepartitionByRangeAndHashExec (range_partitioning_expr={:?}, hash_partitioning={})",
                input_ordering,
                range_partitioning_expr,
                hash_partitioning,
            );
        }
        let preserve_order = false;
        Ok(Self {
            plan_properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(input.schema()),
                hash_partitioning.clone(),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            input,
            range_partitioning_expr,
            hash_partitioning,
            state: Default::default(),
            metrics,
            preserve_order,
        })
    }

    /// Return the sort expressions that are used to merge
    fn sort_exprs(&self) -> Option<&LexOrdering> {
        self.input.output_ordering()
    }

    /// Pulls data from the specified input plan, feeding it to the
    /// output partitions based on the desired partitioning
    ///
    /// txs hold the output sending channels for each output partition
    async fn pull_from_input(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        mut output_channels: HashMap<usize, DistributionSender<MaybeBatch>>,
        range_partitioning: Vec<Arc<dyn PhysicalExpr>>,
        hash_partitioning: Partitioning,
        metrics: RepartitionMetrics,
        all_metrics: ExecutionPlanMetricsSet,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let mut partitioner = BatchPartitioner::try_new(
            range_partitioning,
            hash_partitioning,
            metrics.repartition_time.clone(),
        )?;

        // execute the child operator
        let timer = metrics.fetch_time.timer();
        let mut stream = input.execute(partition, context)?;
        timer.done();

        // While there are still outputs to send to, keep pulling inputs
        let mut batches_until_yield = partitioner.num_partitions();

        while !output_channels.is_empty() {
            // fetch the next batch
            let timer = metrics.fetch_time.timer();
            let result = stream.next().await;
            timer.done();

            // Input is done
            let batch = match result {
                Some(result) => result?,
                None => break,
            };

            for res in partitioner.partition_iter(batch)? {
                let (partition, batch) = res?;

                let timer = metrics.send_time[partition].timer();
                // if there is still a receiver, send to it
                if let Some(tx) = output_channels.get_mut(&partition)
                    && tx.send(Some(Ok(batch))).await.is_err()
                {
                    // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                    output_channels.remove(&partition);
                }
                timer.done();
            }

            // If the input stream is endless, we may spin forever and
            // never yield back to tokio.  See
            // https://github.com/apache/arrow-datafusion/issues/5278.
            //
            // However, yielding on every batch causes a bottleneck
            // when running with multiple cores. See
            // https://github.com/apache/arrow-datafusion/issues/6290
            //
            // Thus, heuristically yield after producing num_partition
            // batches
            //
            // In round-robin this is ideal as each input will get a
            // new batch. In hash partitioning it may yield too often
            // on uneven distributions even if some partition can not
            // make progress, but parallelism is going to be limited
            // in that case anyway
            if batches_until_yield == 0 {
                tokio::task::yield_now().await;
                batches_until_yield = partitioner.num_partitions();
            } else {
                batches_until_yield -= 1;
            }
        }

        if let Some(ms) = input.metrics() {
            for m in ms.iter() {
                all_metrics.register(Arc::clone(m));
            }
        }

        Ok(())
    }

    fn capture_params(&self) -> Arc<RepartitionParams> {
        Arc::new(RepartitionParams {
            input: self.input.clone(),
            range_exprs: self.range_partitioning_expr.clone(),
            hash_partitioning: self.hash_partitioning.clone(),
            metrics: self.metrics.clone(),
            preserve_order: self.preserve_order,
            sort_exprs: self.sort_exprs().cloned(),
            schema: self.schema(),
            name: self.name().to_string(),
        })
    }

    /// Waits for `input_task` which is consuming one of the inputs to
    /// complete. Upon each successful completion, sends a `None` to
    /// each of the output tx channels to signal one of the inputs is
    /// complete. Upon error, propagates the errors to all output tx
    /// channels.
    async fn wait_for_task(
        input_task: SpawnedTask<Result<()>>,
        txs: HashMap<usize, DistributionSender<MaybeBatch>>,
    ) {
        // wait for completion, and propagate error
        // note we ignore errors on send (.ok) as that means the receiver has already shutdown.

        match input_task.join().await {
            // Error in joining task
            Err(e) => {
                let e = Arc::new(e);
                for (_, tx) in txs {
                    tx.send(Some(Err(e.clone().into()))).await.ok();
                }
            }
            // Error from running input task
            Ok(Err(e)) => {
                let e = e.into_cloneable();
                for (_, tx) in txs {
                    tx.send(Some(Err(e.clone().context("error from input task").into())))
                        .await
                        .ok();
                }
            }
            // Input task completed successfully
            Ok(Ok(())) => {
                // notify each output partition that this input partition has no more data
                for (_, tx) in txs {
                    tx.send(None).await.ok();
                }
            }
        }
    }
}

impl ExecutionPlanProperties for RepartitionByRangeAndHashExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.hash_partitioning
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        // Only preserve ordering if input has single partition
        if self.input.output_partitioning().partition_count() <= 1 {
            self.input.properties().output_ordering()
        } else {
            None
        }
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        // Repartitioning preserves equivalence properties
        self.input.properties().equivalence_properties()
    }
}

impl ExecutionPlan for RepartitionByRangeAndHashExec {
    fn name(&self) -> &str {
        self.name()
    }

    fn metrics(&self) -> Option<metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // We preserve ordering when input partitioning is 1
        vec![self.input().output_partitioning().partition_count() <= 1]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        let hash_exprs: &[Arc<dyn PhysicalExpr>] = match &self.hash_partitioning {
            Partitioning::Hash(exprs, _) => exprs,
            _ => &[],
        };
        apply_expression_roots(
            self.range_partitioning_expr.iter().chain(hash_exprs.iter()),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let repartition = RepartitionByRangeAndHashExec::try_new(
            children.swap_remove(0),
            self.range_partitioning_expr.clone(),
            self.hash_partitioning.clone(),
            ExecutionPlanMetricsSet::new(), // children's metrics is not propagated
        )
        .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;

        Ok(Arc::new(repartition))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let params = self.capture_params();
        let paras_captured = Arc::clone(&params);
        let paras_captured_move = Arc::clone(&params);
        let state_ref = Arc::clone(&self.state);
        let stream = futures::stream::once(async move {
            let context_captured = Arc::clone(&context);
            // only exec once
            let state = state_ref
                .get_or_try_init(|| async move {
                    Ok::<Mutex<RepartitionByRangeAndHashExecState>, Report>(Mutex::new(
                        RepartitionByRangeAndHashExecState::new(
                            paras_captured_move.input.clone(),
                            paras_captured_move.range_exprs.clone(),
                            paras_captured_move.hash_partitioning.clone(),
                            paras_captured_move.metrics.clone(),
                            paras_captured_move.preserve_order,
                            context_captured,
                        ),
                    ))
                })
                .await
                .map_err(|rep| DataFusionError::External(rep.into()))?;

            trace!(
                "Before returning stream in {}::execute for partition: {}",
                paras_captured.name, partition
            );

            // lock scope
            let (mut rx, abort_helper) = {
                // lock mutexes
                let mut state = state.lock();

                let (_tx, rx) = state.channels.remove(&partition).ok_or(
                    DataFusionError::Internal("partition isn't used yet".to_string()),
                )?;
                (rx, Arc::clone(&state.abort_helper))
            };

            if paras_captured.preserve_order {
                // Store streams from all the input partitions:
                let input_streams = rx
                    .into_iter()
                    .map(|receiver| {
                        Box::pin(PerPartitionStream {
                            schema: Arc::clone(&paras_captured.schema),
                            receiver,
                            _drop_helper: Arc::clone(&abort_helper),
                        }) as SendableRecordBatchStream
                    })
                    .collect::<Vec<_>>();
                // Note that receiver size (`rx.len()`) and `num_input_partitions` are same.

                // Merge streams (while preserving ordering) coming from
                // input partitions to this partition:
                let fetch = None;
                let merge_reservation = MemoryConsumer::new(format!(
                    "{}[Merge {partition}]",
                    paras_captured.name
                ))
                .with_can_spill(true)
                .register(context.memory_pool());
                let sort_exprs = paras_captured.sort_exprs.as_ref();
                let mut builder = StreamingMergeBuilder::new()
                    .with_streams(input_streams)
                    .with_schema(Arc::clone(&paras_captured.schema))
                    .with_metrics(BaselineMetrics::new(
                        &paras_captured.metrics,
                        partition,
                    ))
                    .with_batch_size(context.session_config().batch_size())
                    .with_fetch(fetch)
                    .with_reservation(merge_reservation);
                if let Some(ordering) = sort_exprs {
                    builder = builder.with_expressions(ordering);
                }
                builder.build()
            } else {
                Ok(Box::pin(RepartitionStream {
                    num_input_partitions: paras_captured
                        .input
                        .output_partitioning()
                        .partition_count(),
                    num_input_partitions_processed: 0,
                    schema: paras_captured.input.schema(),
                    input: rx.swap_remove(0),
                    _drop_helper: abort_helper,
                }) as SendableRecordBatchStream)
            }
        })
        .try_flatten();

        let stream = RecordBatchStreamAdapter::new(Arc::clone(&params.schema), stream);
        Ok(Box::pin(stream))
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DFResult<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }
}

struct RepartitionParams {
    input: Arc<dyn ExecutionPlan>,
    range_exprs: Vec<Arc<dyn PhysicalExpr>>,
    hash_partitioning: Partitioning,
    metrics: ExecutionPlanMetricsSet,
    preserve_order: bool,
    sort_exprs: Option<LexOrdering>,
    schema: SchemaRef,
    name: String,
}

/// [`RepartitionStream`] is executed stream for [`RepartitionByRangeAndHashExec`].
struct RepartitionStream {
    /// Number of input partitions that will be sending batches to this output channel
    num_input_partitions: usize,

    /// Number of input partitions that have finished sending batches to this output channel
    num_input_partitions_processed: usize,

    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    input: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    _drop_helper: Arc<Vec<SpawnedTask<()>>>,
}

impl Stream for RepartitionStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.recv().poll_unpin(cx) {
                Poll::Ready(Some(Some(v))) => {
                    return Poll::Ready(Some(v.map_err(|report| {
                        DataFusionError::External(report.into_boxed_error())
                    })));
                }
                Poll::Ready(Some(None)) => {
                    self.num_input_partitions_processed += 1;

                    if self.num_input_partitions == self.num_input_partitions_processed {
                        // all input partitions have finished sending batches
                        return Poll::Ready(None);
                    } else {
                        // other partitions still have data to send
                        continue;
                    }
                }
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl RecordBatchStream for RepartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// This struct converts a receiver to a stream.
/// The Receiver receives data on an SPSC channel.
struct PerPartitionStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    receiver: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    _drop_helper: Arc<Vec<SpawnedTask<()>>>,
}

impl Stream for PerPartitionStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.receiver.recv().poll_unpin(cx) {
            Poll::Ready(Some(Some(v))) => {
                Poll::Ready(Some(v.map_err(|report| {
                    DataFusionError::External(report.into_boxed_error())
                })))
            }
            Poll::Ready(Some(None)) => {
                // Input partition has finished sending batches
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for PerPartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema, SortOptions};
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion_physical_expr::expressions::col;
    use parking_lot::lock_api::RwLock;

    use crate::helpers::InMemGenerator;

    /// The planner sorts the input by `range_partitions + primary_keys` before
    /// this operator, but the optimizer may drop that sort when the input is
    /// already ordered by a longer prefix (e.g. `(id, value)` for hash key
    /// `id`). Construction must accept such inputs instead of failing.
    #[test]
    fn try_new_accepts_input_ordered_by_more_columns_than_required() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 3])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();
        let memory = LazyMemoryExec::try_new(
            schema.clone(),
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(vec![batch]).unwrap(),
            ))],
        )
        .unwrap();
        // The input is ordered by (id, value); the required hash key is only `id`.
        let input = SortExec::new(
            LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    col("id", &schema).unwrap(),
                    SortOptions::default(),
                ),
                PhysicalSortExpr::new(
                    col("value", &schema).unwrap(),
                    SortOptions::default(),
                ),
            ])
            .unwrap(),
            Arc::new(memory),
        );
        assert!(
            input.properties().output_ordering().is_some(),
            "test input must expose an ordering"
        );

        let hash_exprs = vec![col("id", &schema).unwrap()];
        let exec = RepartitionByRangeAndHashExec::try_new(
            Arc::new(input),
            vec![],
            Partitioning::Hash(hash_exprs, 2),
            ExecutionPlanMetricsSet::new(),
        );
        assert!(
            exec.is_ok(),
            "expected construction to succeed, got {:?}",
            exec.err()
        );
    }

    #[test]
    fn try_new_rejects_missing_ordering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let input = LazyMemoryExec::try_new(
            schema.clone(),
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(vec![]).unwrap(),
            ))],
        )
        .unwrap();
        assert!(input.properties().output_ordering().is_none());

        let exec = RepartitionByRangeAndHashExec::try_new(
            Arc::new(input),
            vec![],
            Partitioning::Hash(vec![col("id", &schema).unwrap()], 2),
            ExecutionPlanMetricsSet::new(),
        );
        assert!(exec.is_err(), "an unordered input must be rejected");
    }

    #[test]
    fn try_new_rejects_incompatible_ordering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let memory = LazyMemoryExec::try_new(
            schema.clone(),
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(vec![]).unwrap(),
            ))],
        )
        .unwrap();
        // Ordered by (value, id), but the hash key is `id`.
        let input = SortExec::new(
            LexOrdering::new(vec![
                PhysicalSortExpr::new(
                    col("value", &schema).unwrap(),
                    SortOptions::default(),
                ),
                PhysicalSortExpr::new(
                    col("id", &schema).unwrap(),
                    SortOptions::default(),
                ),
            ])
            .unwrap(),
            Arc::new(memory),
        );

        let exec = RepartitionByRangeAndHashExec::try_new(
            Arc::new(input),
            vec![],
            Partitioning::Hash(vec![col("id", &schema).unwrap()], 2),
            ExecutionPlanMetricsSet::new(),
        );
        assert!(exec.is_err(), "an unrelated ordering must be rejected");
    }

    #[test]
    fn try_new_rejects_ordering_shorter_than_required() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let memory = LazyMemoryExec::try_new(
            schema.clone(),
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(vec![]).unwrap(),
            ))],
        )
        .unwrap();
        // Required is (part, id); the input is only ordered by `id`.
        let input = SortExec::new(
            LexOrdering::new(vec![PhysicalSortExpr::new(
                col("id", &schema).unwrap(),
                SortOptions::default(),
            )])
            .unwrap(),
            Arc::new(memory),
        );

        let exec = RepartitionByRangeAndHashExec::try_new(
            Arc::new(input),
            vec![col("part", &schema).unwrap()],
            Partitioning::Hash(vec![col("id", &schema).unwrap()], 2),
            ExecutionPlanMetricsSet::new(),
        );
        assert!(
            exec.is_err(),
            "an ordering shorter than range+hash must be rejected"
        );
    }

    /// A multi-partition input that is only sorted *per partition* has an
    /// `output_ordering` (a per-partition property) that already passes the
    /// ordering-prefix check, but with `preserve_order = false` the multiple
    /// producers write to the same output channel in arrival order, so a
    /// bucket would no longer receive its rows in globally sorted order.
    /// Construction must reject such inputs; `output_ordering()` of this plan
    /// only leaks the input ordering for a single input partition.
    #[test]
    fn try_new_rejects_multi_partition_ordered_input() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = |ids: Vec<i32>| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(ids))],
            )
            .unwrap()
        };
        // Two input partitions, each one locally sorted by `id`, but their
        // interleaving across partitions is not globally sorted.
        let memory = LazyMemoryExec::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(RwLock::new(
                    InMemGenerator::try_new(vec![batch(vec![1, 3, 5, 7])]).unwrap(),
                )),
                Arc::new(RwLock::new(
                    InMemGenerator::try_new(vec![batch(vec![2, 4, 6, 8])]).unwrap(),
                )),
            ],
        )
        .unwrap();
        assert_eq!(
            memory.properties().output_partitioning().partition_count(),
            2,
            "test input must have two partitions"
        );

        // The SortExec sorts every partition locally and declares the `id`
        // ordering, even though it only holds per partition. With
        // `preserve_partitioning`, the two sorted input partitions stay
        // separate, which is the shape the range+hash repartition would
        // actually be fed by a multi-partition plan.
        let sort_input = Arc::new(
            SortExec::new(
                LexOrdering::new(vec![PhysicalSortExpr::new(
                    col("id", &schema).unwrap(),
                    SortOptions::default(),
                )])
                .unwrap(),
                Arc::new(memory),
            )
            .with_preserve_partitioning(true),
        );
        assert!(
            sort_input.properties().output_ordering().is_some(),
            "test input must expose an ordering"
        );
        // The ordering-prefix check alone would accept this input; the
        // partition-count check is what rejects it.
        let exec = RepartitionByRangeAndHashExec::try_new(
            sort_input as Arc<dyn ExecutionPlan>,
            vec![],
            Partitioning::Hash(vec![col("id", &schema).unwrap()], 1),
            ExecutionPlanMetricsSet::new(),
        );
        assert!(
            exec.is_err(),
            "a multi-partition input must be rejected, got {exec:?}"
        );
    }

    #[test]
    fn try_new_rejects_non_hash_partitioning() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input = LazyMemoryExec::try_new(
            schema,
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(vec![]).unwrap(),
            ))],
        )
        .unwrap();
        let exec = RepartitionByRangeAndHashExec::try_new(
            Arc::new(input),
            vec![],
            Partitioning::RoundRobinBatch(2),
            ExecutionPlanMetricsSet::new(),
        );
        assert!(exec.is_err());
    }
}
