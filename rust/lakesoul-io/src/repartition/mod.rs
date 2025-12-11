// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! This module provides the implementation of the repartition operator.

use std::{
    any::Any,
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_schema::SchemaRef;
use datafusion::{
    common::runtime::SpawnedTask,
    common::utils::transpose,
    execution::{
        TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
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
use datafusion_common::{DataFusionError, Result, Statistics};

use arrow_array::{ArrayRef, RecordBatch, builder::UInt64Builder};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};

use crate::{hash_utils::create_hashes, repartition::distributor_channels::channels};

use self::distributor_channels::{
    DistributionReceiver, DistributionSender, partition_aware_channels,
};

use parking_lot::Mutex;

mod distributor_channels;

type MaybeBatch = Option<Result<RecordBatch>>;
type InputPartitionsToCurrentPartitionSender = Vec<DistributionSender<MaybeBatch>>;
type InputPartitionsToCurrentPartitionReceiver = Vec<DistributionReceiver<MaybeBatch>>;

/// [`MemoryReservation`] used across query execution streams
pub(crate) type SharedMemoryReservation = Arc<Mutex<MemoryReservation>>;

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
            SharedMemoryReservation,
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
        name: String,
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
            let reservation = Arc::new(Mutex::new(
                MemoryConsumer::new(format!("{}[{partition}]", name))
                    .register(context.memory_pool()),
            ));
            channels.insert(partition, (tx, rx, reservation));
        }
        // launch one async task per *input* partition
        let mut spawned_tasks = Vec::with_capacity(num_input_partitions);
        for i in 0..num_input_partitions {
            let txs: HashMap<_, _> = channels
                .iter()
                .map(|(partition, (tx, _rx, reservation))| {
                    (*partition, (tx[i].clone(), Arc::clone(reservation)))
                })
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
                    context.clone(),
                ));

            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            let wait_for_task =
                SpawnedTask::spawn(RepartitionByRangeAndHashExec::wait_for_task(
                    input_task,
                    txs.into_iter()
                        .map(|(partition, (tx, _reservation))| (partition, tx))
                        .collect(),
                ));
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
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported repartitioning scheme {other:?}"
                )));
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
            let timer = self.timer.timer();

            let range_arrays = [range_exprs.clone()]
                .concat()
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;

            let hash_arrays = hash_exprs
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<Vec<_>>>()?;

            hash_buffer.clear();
            hash_buffer.resize(batch.num_rows(), 0);

            let mut range_buffer = vec![0; batch.num_rows()];

            create_hashes(&hash_arrays, hash_buffer)?;
            create_hashes(&range_arrays, &mut range_buffer)?;

            let mut indices: Vec<HashMap<u32, UInt64Builder>> = (0..*partitions)
                .map(|_| HashMap::new())
                // .map(|_| UInt64Builder::with_capacity(batch.num_rows()))
                .collect();

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
                    let columns = batch
                        .columns()
                        .iter()
                        .map(|c| {
                            arrow::compute::take(c.as_ref(), &indices, None).map_err(
                                |e| DataFusionError::ArrowError(Box::new(e), None),
                            )
                        })
                        .collect::<Result<Vec<ArrayRef>>>()?;

                    let batch = RecordBatch::try_new(batch.schema(), columns)?;

                    // bind timer so it drops w/ this iterator
                    let _ = &timer;

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
    ///
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
    plan_properties: PlanProperties,
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
    ) -> Result<Self> {
        let preserve_order = false;
        if let Some(ordering) = input.output_ordering() {
            let lhs = ordering
                .iter()
                .map(|sort_expr| sort_expr.expr.clone())
                .collect::<Vec<_>>();
            let rhs = [
                range_partitioning_expr.clone(),
                match &hash_partitioning {
                    Partitioning::Hash(hash_exprs, _) => hash_exprs.clone(),
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "Invalid hash_partitioning={} for RepartitionByRangeAndHashExec",
                            hash_partitioning
                        )));
                    }
                },
            ]
            .concat();

            if physical_exprs_equal(&lhs, &rhs) {
                return Ok(Self {
                    plan_properties: PlanProperties::new(
                        EquivalenceProperties::new(input.schema()),
                        hash_partitioning.clone(),
                        EmissionType::Incremental,
                        Boundedness::Bounded,
                    ),
                    input,
                    range_partitioning_expr,
                    hash_partitioning,
                    state: Default::default(),
                    metrics: ExecutionPlanMetricsSet::new(),
                    preserve_order,
                });
            }
        }
        Err(DataFusionError::Plan(format!(
            "Input ordering {:?} mismatch for RepartitionByRangeAndHashExec with range_partitioning_expr={:?}, hash_partitioning={}",
            input.output_ordering(),
            range_partitioning_expr,
            hash_partitioning,
        )))
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
        mut output_channels: HashMap<
            usize,
            (DistributionSender<MaybeBatch>, SharedMemoryReservation),
        >,
        range_partitioning: Vec<Arc<dyn PhysicalExpr>>,
        hash_partitioning: Partitioning,
        metrics: RepartitionMetrics,
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
                let size = batch.get_array_memory_size();

                let timer = metrics.send_time[partition].timer();
                // if there is still a receiver, send to it
                if let Some((tx, reservation)) = output_channels.get_mut(&partition) {
                    reservation.lock().try_grow(size)?;

                    if tx.send(Some(Ok(batch))).await.is_err() {
                        // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                        reservation.lock().shrink(size);
                        output_channels.remove(&partition);
                    }
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

        Ok(())
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
                    let err = Err(DataFusionError::Context(
                        "Join Error".to_string(),
                        Box::new(DataFusionError::External(Box::new(Arc::clone(&e)))),
                    ));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Error from running input task
            Ok(Err(e)) => {
                let e = Arc::new(e);

                for (_, tx) in txs {
                    // wrap it because need to send error to all output partitions
                    let err = Err(DataFusionError::External(Box::new(Arc::clone(&e))));
                    tx.send(Some(err)).await.ok();
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

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // We preserve ordering when input partitioning is 1
        vec![self.input().output_partitioning().partition_count() <= 1]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let repartition = RepartitionByRangeAndHashExec::try_new(
            children.swap_remove(0),
            self.range_partitioning_expr.clone(),
            self.hash_partitioning.clone(),
        )?;

        Ok(Arc::new(repartition))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // clone all data that needs to be used in async block
        let metrics = self.metrics.clone();
        let lazy_state = self.state.clone();
        let preserve_order = self.preserve_order;
        let name = self.name().to_string();
        let schema = self.schema();
        let schema_captured = Arc::clone(&schema);
        let sort_exprs = self.sort_exprs().cloned();
        let input = self.input.clone();
        let input_schema = input.schema();
        let range_partitioning_expr = self.range_partitioning_expr.clone();
        let hash_partitioning = self.hash_partitioning.clone();

        let stream = futures::stream::once(async move {
            let metrics_captured = metrics.clone();
            let name_captured = name.clone();
            let num_input_partitions = input.output_partitioning().partition_count();

            let context_captured = Arc::clone(&context);

            let state = lazy_state
                .get_or_init(|| async move {
                    Mutex::new(RepartitionByRangeAndHashExecState::new(
                        input,
                        range_partitioning_expr,
                        hash_partitioning,
                        metrics_captured,
                        preserve_order,
                        name_captured,
                        context_captured,
                    ))
                })
                .await;

            trace!(
                "Before returning stream in {}::execute for partition: {}",
                name, partition
            );

            // lock scope
            let (mut rx, reservation, abort_helper) = {
                // lock mutexes
                let mut state = state.lock();

                let (_tx, rx, reservation) = state.channels.remove(&partition).ok_or(
                    DataFusionError::Internal("partition isn't used yet".to_string()),
                )?;
                (rx, reservation, Arc::clone(&state.abort_helper))
            };

            if preserve_order {
                // Store streams from all the input partitions:
                let input_streams = rx
                    .into_iter()
                    .map(|receiver| {
                        Box::pin(PerPartitionStream {
                            schema: Arc::clone(&schema_captured),
                            receiver,
                            _drop_helper: Arc::clone(&abort_helper),
                            reservation: Arc::clone(&reservation),
                        }) as SendableRecordBatchStream
                    })
                    .collect::<Vec<_>>();
                // Note that receiver size (`rx.len()`) and `num_input_partitions` are same.

                // Merge streams (while preserving ordering) coming from
                // input partitions to this partition:
                let fetch = None;
                let merge_reservation =
                    MemoryConsumer::new(format!("{}[Merge {partition}]", name))
                        .register(context.memory_pool());
                let sort_exprs = sort_exprs.as_ref();
                let mut builder = StreamingMergeBuilder::new()
                    .with_streams(input_streams)
                    .with_schema(schema_captured)
                    .with_metrics(BaselineMetrics::new(&metrics, partition))
                    .with_batch_size(context.session_config().batch_size())
                    .with_fetch(fetch)
                    .with_reservation(merge_reservation);
                if let Some(ordering) = sort_exprs {
                    builder = builder.with_expressions(ordering);
                }
                builder.build()
            } else {
                Ok(Box::pin(RepartitionStream {
                    num_input_partitions,
                    num_input_partitions_processed: 0,
                    schema: input_schema,
                    input: rx.swap_remove(0),
                    _drop_helper: abort_helper,
                    reservation,
                }) as SendableRecordBatchStream)
            }
        })
        .try_flatten();

        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }
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

    /// Memory reservation.
    reservation: SharedMemoryReservation,
}

impl Stream for RepartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.recv().poll_unpin(cx) {
                Poll::Ready(Some(Some(v))) => {
                    if let Ok(batch) = &v {
                        self.reservation
                            .lock()
                            .shrink(batch.get_array_memory_size());
                    }

                    return Poll::Ready(Some(v));
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

    /// Memory reservation.
    reservation: SharedMemoryReservation,
}

impl Stream for PerPartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.receiver.recv().poll_unpin(cx) {
            Poll::Ready(Some(Some(v))) => {
                if let Ok(batch) = &v {
                    self.reservation
                        .lock()
                        .shrink(batch.get_array_memory_size());
                }
                Poll::Ready(Some(v))
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
