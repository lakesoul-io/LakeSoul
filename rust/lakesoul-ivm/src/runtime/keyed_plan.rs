// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Physical-plan plumbing for keyed incremental state.
//!
//! The refresh path must route a state key to the same hash bucket the writer
//! used before it performs a PK lookup.  DataFusion's default hash is not the
//! LakeSoul hash, so the bucket id is computed by a physical expression and
//! then range-repartitioned.  The resulting plan is streaming and keeps the
//! routing step inside the physical execution framework.
//!
//! The multi-aggregate state refresh uses the full chain: contributions are
//! range-repartitioned by the LakeSoul bucket expression, the lookup plan
//! resolves each bucket's state rows by primary key, and the merge plan
//! computes the next state rows without a driver-side collect or SQL merge.

use std::collections::HashMap;
use std::fmt::{self, Display};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int64Array, StringArray, UInt32Array,
    UInt32Builder, new_null_array,
};
use arrow::compute::{SortOptions, cast, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{
    DFSchema, DFSchemaRef, JoinType, NullEquality, ScalarValue,
    tree_node::TreeNodeRecursion,
};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::{
    Distribution, EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::SortMergeJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};
#[cfg(test)]
use datafusion::prelude::lit;
use datafusion::prelude::{DataFrame, SessionContext, col};
use futures::StreamExt;
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::helpers::transform::make_default_array;
use lakesoul_io::physical_plan::merge::MergeParquetExec;
use lakesoul_io::pk_locator::LookupSource;
use lakesoul_vector::IndexKey;
use rootcause::compat::boxed_error::IntoBoxedError;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use super::{
    IVM_EPOCH_COLUMN, IVM_GROUP_ROW_COLUMN, IVM_ROW_COUNT_COLUMN, IVM_ROW_KINDS_COLUMN,
};
use crate::error::Result;
use crate::table::{IvmTable, PartitionFiles, file_bucket, partition_matches};

/// Hidden column carrying the LakeSoul bucket id during a shuffle.
pub(crate) const IVM_BUCKET_COLUMN: &str = "__ivm_bucket";

#[derive(Debug)]
pub(crate) struct LakeSoulBucketExpr {
    key: Arc<dyn PhysicalExpr>,
    bucket_count: u32,
}

impl PartialEq for LakeSoulBucketExpr {
    fn eq(&self, other: &Self) -> bool {
        self.bucket_count == other.bucket_count
            && self.key.as_ref().eq(other.key.as_ref())
    }
}

impl Eq for LakeSoulBucketExpr {}

impl std::hash::Hash for LakeSoulBucketExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bucket_count.hash(state);
        self.key.as_ref().hash(state);
    }
}

impl Display for LakeSoulBucketExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lakesoul_bucket({}, {})", self.key, self.bucket_count)
    }
}

#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
impl LakeSoulBucketExpr {
    pub(crate) fn from_parts(key: Arc<dyn PhysicalExpr>, bucket_count: u32) -> Self {
        Self { key, bucket_count }
    }

    pub(crate) fn key(&self) -> &Arc<dyn PhysicalExpr> {
        &self.key
    }

    pub(crate) fn bucket_count(&self) -> u32 {
        self.bucket_count
    }
}

impl PhysicalExpr for LakeSoulBucketExpr {
    fn data_type(&self, _input_schema: &Schema) -> datafusion::error::Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(true)
    }

    fn evaluate(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        let value = self.key.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;
        let mut builder = UInt32Builder::with_capacity(array.len());
        for row in 0..array.len() {
            if array.is_null(row) {
                builder.append_null();
            } else {
                let scalar = ScalarValue::try_from_array(&array, row)?;
                let bucket = lakesoul_io::helpers::compute_scalar_hash(&scalar)
                    % self.bucket_count;
                builder.append_value(bucket);
            }
        }
        Ok(datafusion::logical_expr::ColumnarValue::Array(Arc::new(
            builder.finish(),
        )))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lakesoul_bucket({}, {})", self.key, self.bucket_count)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.key]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "lakesoul bucket expression expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            key: children[0].clone(),
            bucket_count: self.bucket_count,
        }))
    }
}

/// LakeSoul's composite hash, computed over the writer's bucket columns.
#[derive(Debug)]
pub(crate) struct LakeSoulColumnsBucketExpr {
    keys: Vec<Arc<dyn PhysicalExpr>>,
    bucket_count: u32,
}

impl PartialEq for LakeSoulColumnsBucketExpr {
    fn eq(&self, other: &Self) -> bool {
        self.bucket_count == other.bucket_count
            && self
                .keys
                .iter()
                .zip(other.keys.iter())
                .all(|(left, right)| left.as_ref().eq(right.as_ref()))
            && self.keys.len() == other.keys.len()
    }
}

impl Eq for LakeSoulColumnsBucketExpr {}

impl std::hash::Hash for LakeSoulColumnsBucketExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bucket_count.hash(state);
        for key in &self.keys {
            key.as_ref().hash(state);
        }
    }
}

impl Display for LakeSoulColumnsBucketExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "lakesoul_columns_bucket({:?}, {})",
            self.keys, self.bucket_count
        )
    }
}

#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
impl LakeSoulColumnsBucketExpr {
    pub(crate) fn from_parts(
        keys: Vec<Arc<dyn PhysicalExpr>>,
        bucket_count: u32,
    ) -> Self {
        Self { keys, bucket_count }
    }

    pub(crate) fn keys(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.keys
    }

    pub(crate) fn bucket_count(&self) -> u32 {
        self.bucket_count
    }
}

impl PhysicalExpr for LakeSoulColumnsBucketExpr {
    fn data_type(&self, _input_schema: &Schema) -> datafusion::error::Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn nullable(&self, _input_schema: &Schema) -> datafusion::error::Result<bool> {
        Ok(false)
    }

    fn evaluate(
        &self,
        batch: &RecordBatch,
    ) -> datafusion::error::Result<datafusion::logical_expr::ColumnarValue> {
        let arrays = self
            .keys
            .iter()
            .map(|key| key.evaluate(batch)?.into_array(batch.num_rows()))
            .collect::<datafusion::error::Result<Vec<ArrayRef>>>()?;
        let mut hashes = vec![0u32; batch.num_rows()];
        lakesoul_io::utils::hash::create_hashes(&arrays, &mut hashes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let buckets = hashes
            .into_iter()
            .map(|hash| hash % self.bucket_count)
            .collect::<Vec<_>>();
        Ok(datafusion::logical_expr::ColumnarValue::Array(Arc::new(
            UInt32Array::from(buckets),
        )))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.keys.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.keys.len() {
            return Err(DataFusionError::Internal(
                "composite bucket expression child count mismatch".to_string(),
            ));
        }
        Ok(Arc::new(Self {
            keys: children,
            bucket_count: self.bucket_count,
        }))
    }
}

/// The key columns a [`IvmBucketProjectionExec`] hashes.
#[derive(Debug, Clone)]
pub(crate) enum BucketKey {
    Single(String),
    Columns(Vec<String>),
}

/// Append the hidden `__ivm_bucket` column to a stream.
///
/// The bucket id is a LakeSoul-specific hash.  Keeping the hash inside a
/// dedicated execution node, instead of a `ProjectionExec` with a custom
/// physical expression, matters for distributed execution: the plan codec
/// forwards user *nodes* but not user *expressions*, so a bare custom
/// expression cannot cross the wire.  The node is rebuilt from its key
/// columns on the worker and the shuffle then hashes a plain column.
pub(crate) struct IvmBucketProjectionExec {
    input: Arc<dyn ExecutionPlan>,
    /// Local split of the contribution; the projection must not advertise a
    /// single partition, otherwise `EnsureRequirements` treats the bucket
    /// shuffle above it as redundant and drops it.
    split: Arc<dyn ExecutionPlan>,
    partitions: usize,
    key: BucketKey,
    bucket_count: u32,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl IvmBucketProjectionExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        key: BucketKey,
        bucket_count: usize,
    ) -> DataFusionResult<Self> {
        let input_schema = input.schema();
        let mut fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            IVM_BUCKET_COLUMN,
            DataType::UInt32,
            true,
        )));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let input_partitions = input.output_partitioning().partition_count();
        let partitions = input_partitions.max(2);
        let split = if input_partitions < partitions {
            Arc::new(RepartitionExec::try_new(
                Arc::clone(&input),
                Partitioning::RoundRobinBatch(partitions),
            )?) as Arc<dyn ExecutionPlan>
        } else {
            Arc::clone(&input)
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            split,
            partitions,
            key,
            bucket_count: bucket_count.max(2) as u32,
            schema,
            properties,
        })
    }

    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(crate) fn key(&self) -> &BucketKey {
        &self.key
    }

    #[cfg_attr(not(feature = "distributed"), allow(dead_code))]
    pub(crate) fn bucket_count(&self) -> u32 {
        self.bucket_count
    }

    fn bucket_expr(&self) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let schema = self.input.schema();
        Ok(match &self.key {
            BucketKey::Single(name) => {
                let index = schema.index_of(name)?;
                Arc::new(LakeSoulBucketExpr {
                    key: Arc::new(PhysicalColumn::new(name, index)),
                    bucket_count: self.bucket_count,
                }) as Arc<dyn PhysicalExpr>
            }
            BucketKey::Columns(names) => {
                let keys = names
                    .iter()
                    .map(|name| {
                        Ok(Arc::new(PhysicalColumn::new(name, schema.index_of(name)?))
                            as Arc<dyn PhysicalExpr>)
                    })
                    .collect::<DataFusionResult<Vec<_>>>()?;
                Arc::new(LakeSoulColumnsBucketExpr {
                    keys,
                    bucket_count: self.bucket_count,
                }) as Arc<dyn PhysicalExpr>
            }
        })
    }
}

impl std::fmt::Debug for IvmBucketProjectionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmBucketProjectionExec")
    }
}

impl DisplayAs for IvmBucketProjectionExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "IvmBucketProjection buckets={} partitions={} key={:?}",
            self.bucket_count, self.partitions, self.key
        )
    }
}

impl ExecutionPlanProperties for IvmBucketProjectionExec {
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

impl ExecutionPlan for IvmBucketProjectionExec {
    fn name(&self) -> &str {
        "IvmBucketProjectionExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IvmBucketProjectionExec expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.key.clone(),
            self.bucket_count as usize,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        tracing::debug!(
            target: "lakesoul_ivm::keyed_plan",
            pid = std::process::id(),
            partition,
            partitions = self.partitions,
            "executing bucket projection"
        );
        let expr = self.bucket_expr()?;
        let schema = self.schema.clone();
        let stream = self.split.execute(partition, context)?.map(move |batch| {
            let batch = batch?;
            let buckets = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            let mut columns = batch.columns().to_vec();
            columns.push(buckets);
            RecordBatch::try_new(schema.clone(), columns).map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Add a LakeSoul bucket-id column and hash-partition the stream on it.
///
/// The hidden bucket id stays in the stream.  A distributed planner rewrites
/// the hash partition count to `bucket_count * tasks`; the downstream lookup
/// recovers which buckets a `(task, partition)` owns from the same hash and
/// opens only those state files.  The lookup drops the helper column from its
/// output.
pub(crate) fn bucket_repartition(
    input: Arc<dyn ExecutionPlan>,
    key_name: &str,
    bucket_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if bucket_count < 2 {
        return Ok(input);
    }
    bucket_repartition_keys(input, BucketKey::Single(key_name.to_string()), bucket_count)
}

/// Bucket-repartition contribution rows by one or more real key columns using
/// the LakeSoul writer's composite hash.  The helper bucket column is removed
/// before the lookup plan sees the stream.
pub(crate) fn bucket_repartition_by_columns(
    input: Arc<dyn ExecutionPlan>,
    key_columns: &[String],
    bucket_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if bucket_count < 2 || key_columns.is_empty() {
        return Ok(input);
    }
    bucket_repartition_keys(
        input,
        BucketKey::Columns(key_columns.to_vec()),
        bucket_count,
    )
}

fn bucket_repartition_keys(
    input: Arc<dyn ExecutionPlan>,
    key: BucketKey,
    bucket_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if bucket_count < 2 {
        return Ok(input);
    }
    let with_bucket = Arc::new(IvmBucketProjectionExec::new(input, key, bucket_count)?)
        as Arc<dyn ExecutionPlan>;
    let bucket_index = with_bucket.schema().index_of(IVM_BUCKET_COLUMN)?;
    let bucket_expr = Arc::new(PhysicalColumn::new(IVM_BUCKET_COLUMN, bucket_index))
        as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(RepartitionExec::try_new(
        with_bucket,
        Partitioning::Hash(vec![bucket_expr], bucket_count),
    )?))
}

// ---------------------------------------------------------------------------
// Keyed state lookup and merge plans
// ---------------------------------------------------------------------------

/// Prefix of the state columns carried through the lookup plan.  State and
/// contribution schemas both contain group-key and measure names, so the
/// lookup output keeps the old state columns under a distinct prefix.
pub(crate) const IVM_STATE_COLUMN_PREFIX: &str = "__ivm_state_";

/// Above this many values a pushed-down IN list costs more to evaluate over
/// the state rows than reading the bucket and matching the exact row key in
/// memory.  The lookup plan still performs the exact composite-key match.
#[cfg(test)]
const PK_LOOKUP_IN_LIST_LIMIT: usize = 4096;

pub(crate) fn max_active_tasks() -> usize {
    crate::config::options()
        .max_active_tasks
        .unwrap_or(tokio::sync::Semaphore::MAX_PERMITS)
        .max(1)
}

/// How a lookup maps execution streams to state buckets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BucketOwnership {
    /// The input is hash-shuffled on `__ivm_bucket`; each stream owns the
    /// buckets that hash to its slot.
    Hash,
    /// The input is not bucket-shuffled; every stream probes every bucket.
    All,
}

/// The slice of a distributed stage a keyed lookup is running in.
#[derive(Clone, Copy, Debug)]
struct LookupTask {
    index: usize,
    count: usize,
}

#[cfg(feature = "distributed")]
fn lookup_task(context: &Arc<datafusion::execution::TaskContext>) -> LookupTask {
    let task = datafusion_distributed::DistributedTaskContext::from_ctx(context);
    LookupTask {
        index: task.task_index,
        count: task.task_count,
    }
}

#[cfg(not(feature = "distributed"))]
fn lookup_task(_context: &Arc<datafusion::execution::TaskContext>) -> LookupTask {
    LookupTask { index: 0, count: 1 }
}

/// Sort each bucket's contribution by the lookup key before a persistent
/// lookup consumes it.
///
/// The per-file index cursor only moves forward across key batches, and each
/// file's lookup stream must stay sorted by the state primary key for the
/// LakeSoul merge.  The sort must preserve the bucket partitioning; a plain
/// `SortExec` coalesces to one partition and would collapse all buckets into
/// a single task.
fn lookup_ordering(schema: &Schema, key: &PkLookupKey) -> DataFusionResult<LexOrdering> {
    let columns = match key {
        PkLookupKey::Columns {
            input_columns,
            table_columns,
            ..
        } => mapped_input_columns(input_columns, table_columns),
        PkLookupKey::BinaryRow { column } => vec![column.clone()],
    };
    let expressions = columns
        .iter()
        .map(|column| {
            Ok(PhysicalSortExpr {
                expr: Arc::new(PhysicalColumn::new(column, schema.index_of(column)?)),
                options: SortOptions::default(),
            })
        })
        .collect::<DataFusionResult<Vec<_>>>()?;
    LexOrdering::new(expressions).ok_or_else(|| {
        DataFusionError::Internal("persistent lookup sort key is empty".to_string())
    })
}

fn sort_lookup_input(
    input: Arc<dyn ExecutionPlan>,
    key: &PkLookupKey,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let ordering = lookup_ordering(&input.schema(), key)?;
    Ok(Arc::new(
        SortExec::new(ordering, input).with_preserve_partitioning(true),
    ))
}

/// Encode the contribution keys into the index keys the locator fetches.
fn explicit_pk_for_keys(
    key: &PkLookupKey,
    contribution_keys: &[Option<Vec<u8>>],
) -> DataFusionResult<Option<(Vec<String>, Vec<IndexKey>)>> {
    Ok(match key {
        PkLookupKey::Columns { table_columns, .. } => {
            let encoded: Vec<IndexKey> = contribution_keys
                .iter()
                .flatten()
                .map(IndexKey::new)
                .collect();
            (!encoded.is_empty()).then(|| (table_columns.clone(), encoded))
        }
        PkLookupKey::BinaryRow { column } => {
            // The join state stores an `arrow::Row` in a Binary column.  The
            // index encodes that Binary column again, so wrap each value in
            // the same one-field Row layout before probing it.
            let values = contribution_keys
                .iter()
                .map(|key| key.as_deref())
                .collect::<Vec<_>>();
            let array = arrow::array::BinaryArray::from_iter(values);
            let converter =
                arrow::row::RowConverter::new(vec![arrow::row::SortField::new(
                    DataType::Binary,
                )])
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let encoded = converter
                .convert_columns(&[Arc::new(array)])
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let keys = (0..encoded.num_rows())
                .map(|row| IndexKey::new(encoded.row(row).as_ref()))
                .collect::<Vec<_>>();
            (!keys.is_empty()).then(|| (vec![column.clone()], keys))
        }
    })
}

/// One explicit-key lookup for a bounded contribution window.
///
/// The reader opens the bucket files once, resolves every key through the pk
/// locator and merges the candidate rows once.  The exact key match is applied
/// by `lookup_batch`, so no pushed-down IN list is needed.
async fn lookup_streaming_bulk(
    table: &IvmTable,
    groups: &[PartitionFiles],
    contribution: &RecordBatch,
    state_schema: &SchemaRef,
    out_schema: &SchemaRef,
    key: &PkLookupKey,
    multi_match: bool,
    emit_unmatched: bool,
    gate: Arc<Semaphore>,
) -> DataFusionResult<(RecordBatch, usize)> {
    let contribution_keys = key_rows(contribution, key, state_schema)?;
    let explicit_pk = explicit_pk_for_keys(key, &contribution_keys)?;
    let mut state_batches = table
        .stream_partition_files_filtered(groups.to_vec(), Vec::new(), explicit_pk, gate)
        .await
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let mut batches = Vec::new();
    let mut state_rows = 0usize;
    while let Some(batch) = state_batches.recv().await {
        let batch = batch?;
        state_rows += batch.num_rows();
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    let output = lookup_batch(
        contribution,
        &batches,
        state_schema,
        out_schema,
        key,
        multi_match,
        emit_unmatched,
    )?;
    Ok((output, state_rows))
}

/// Split state file groups into the hash buckets the writer used.
///
/// A state range partition contains the files of every hash bucket.  A lookup
/// plan has one execution partition per bucket, so each partition reads only
/// its own files.  A file whose bucket cannot be parsed is repeated in every
/// bucket; the key filter still keeps the result correct, at the cost of an
/// extra read for a malformed file name.
pub(crate) fn groups_by_bucket(
    groups: Vec<PartitionFiles>,
    bucket_count: usize,
) -> Vec<Vec<PartitionFiles>> {
    let bucket_count = bucket_count.max(1);
    if bucket_count == 1 {
        return vec![groups];
    }
    let mut out: Vec<Vec<PartitionFiles>> =
        (0..bucket_count).map(|_| Vec::new()).collect();
    for group in groups {
        let mut buckets: Vec<Vec<String>> =
            (0..bucket_count).map(|_| Vec::new()).collect();
        for file in group.files {
            match file_bucket(&file) {
                Some(bucket) if (bucket as usize) < bucket_count => {
                    buckets[bucket as usize].push(file);
                }
                // A state table written with a different bucket count (or a
                // legacy file name) must not silently lose a key.
                _ => {
                    for bucket in &mut buckets {
                        bucket.push(file.clone());
                    }
                }
            }
        }
        for (bucket, files) in buckets.into_iter().enumerate() {
            if !files.is_empty() {
                out[bucket].push(PartitionFiles {
                    partition_desc: group.partition_desc.clone(),
                    files,
                });
            }
        }
    }
    out
}

/// How the contribution rows identify a row in the looked-up table.
///
/// `BinaryRow` is used by side/aggregate state tables, whose hidden key is an
/// `arrow::Row` encoding. `Columns` is used by source and MV tables, where the
/// lookup key is one or more real primary-key columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PkLookupKey {
    BinaryRow {
        column: String,
    },
    Columns {
        input_columns: Vec<String>,
        table_columns: Vec<String>,
        /// Whether a NULL key matches a NULL table row.
        ///
        /// Group keys and window partition keys may be NULL (SQL groups NULLs
        /// together), so their identity lookups are null-safe.  A strict SQL
        /// join probe (`a.k = b.k`) keeps NULLs apart, and its contribution
        /// rows with a NULL key are dropped before the lookup.
        #[serde(default)]
        null_safe: bool,
    },
}

impl PkLookupKey {
    /// Whether a NULL key matches a NULL table row.
    fn null_safe(&self) -> bool {
        matches!(
            self,
            Self::Columns {
                null_safe: true,
                ..
            }
        )
    }

    /// The same key expressed on the table side.  Contribution and table
    /// columns can have different names for a differently-named join key.
    fn for_table(&self) -> Self {
        match self {
            Self::BinaryRow { column } => Self::BinaryRow {
                column: column.clone(),
            },
            Self::Columns {
                table_columns,
                null_safe,
                ..
            } => Self::Columns {
                input_columns: table_columns.clone(),
                table_columns: table_columns.clone(),
                null_safe: *null_safe,
            },
        }
    }

    fn display(&self) -> String {
        match self {
            Self::BinaryRow { column } => format!("binary({column})"),
            Self::Columns { input_columns, .. } => {
                format!("columns({})", input_columns.join(","))
            }
        }
    }
}

#[cfg(test)]
fn state_key_filter(
    batch: &RecordBatch,
    key_column: &str,
) -> DataFusionResult<Vec<datafusion::prelude::Expr>> {
    let index = batch.schema().index_of(key_column)?;
    let keys = batch
        .column(index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "state lookup key {key_column} is not a binary column"
            ))
        })?;
    let list = keys
        .iter()
        .flatten()
        .map(|key| lit(ScalarValue::Binary(Some(key.to_vec()))))
        .collect::<Vec<_>>();
    if list.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(vec![col(key_column).in_list(list, false)])
    }
}

/// Drop the hidden bucket column from one lookup output batch.
fn project_lookup_output(
    batch: RecordBatch,
    projection: Option<&[usize]>,
) -> DataFusionResult<RecordBatch> {
    match projection {
        Some(indices) => batch.project(indices).map_err(|error| {
            DataFusionError::Execution(format!("project keyed lookup output: {error}"))
        }),
        None => Ok(batch),
    }
}

fn state_output_schema(input: &SchemaRef, state: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = input
        .fields()
        .iter()
        .map(|field| Arc::new(field.as_ref().clone()))
        .collect();
    for field in state.fields() {
        fields.push(Arc::new(Field::new(
            format!("{IVM_STATE_COLUMN_PREFIX}{}", field.name()),
            field.data_type().clone(),
            true,
        )));
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
enum PkFilters {
    /// Read each contribution chunk with its own pushed-down filter.
    Pushdown(Vec<datafusion::prelude::Expr>),
    /// At least one key column has too many values for a cheap IN list.
    /// Read the bucket once for the whole contribution batch, then use the
    /// exact Arrow row keys to match.
    ScanAll,
}

/// Order the contribution columns like the table key when both lists name the
/// same columns.  The row encoder pairs them positionally, so a join key and a
/// primary key listed in different orders would otherwise build the wrong
/// tuple.  Aliased keys (different names) keep the caller's positional order.
fn mapped_input_columns(
    input_columns: &[String],
    table_columns: &[String],
) -> Vec<String> {
    if input_columns.len() == table_columns.len()
        && table_columns
            .iter()
            .all(|table| input_columns.iter().any(|input| input == table))
    {
        table_columns
            .iter()
            .map(|table| {
                input_columns
                    .iter()
                    .find(|input| *input == table)
                    .cloned()
                    .unwrap_or_else(|| table.clone())
            })
            .collect()
    } else {
        input_columns.to_vec()
    }
}

#[cfg(test)]
fn key_filter_exprs(
    batch: &RecordBatch,
    key: &PkLookupKey,
) -> DataFusionResult<PkFilters> {
    match key {
        PkLookupKey::BinaryRow { column } => {
            Ok(PkFilters::Pushdown(state_key_filter(batch, column)?))
        }
        PkLookupKey::Columns {
            input_columns,
            table_columns,
            null_safe,
        } => {
            let input_columns = mapped_input_columns(input_columns, table_columns);
            let mut rows = Vec::new();
            let mut null_columns = vec![false; input_columns.len()];
            for row in 0..batch.num_rows() {
                let mut values = Vec::with_capacity(input_columns.len());
                for (index, column) in input_columns.iter().enumerate() {
                    let index = batch.schema().index_of(column)?;
                    let value =
                        ScalarValue::try_from_array(batch.column(index).as_ref(), row)?;
                    if value.is_null() {
                        null_columns[index] = true;
                    }
                    values.push(value);
                }
                // A strict probe never looks up a NULL key (SQL `=` does not
                // match NULL); an identity lookup includes the NULL group.
                if *null_safe || values.iter().all(|value| !value.is_null()) {
                    rows.push(values);
                }
            }
            if rows.is_empty() {
                return Ok(PkFilters::Pushdown(Vec::new()));
            }
            if table_columns.len() == 1 {
                let mut filters = Vec::new();
                if null_columns[0] {
                    filters.push(col(table_columns[0].as_str()).is_null());
                }
                let list = rows
                    .into_iter()
                    .filter_map(|mut values| {
                        let value = values.remove(0);
                        (!value.is_null()).then(|| lit(value))
                    })
                    .collect::<Vec<_>>();
                if list.len() > PK_LOOKUP_IN_LIST_LIMIT {
                    return Ok(PkFilters::ScanAll);
                }
                if !list.is_empty() {
                    filters.push(col(table_columns[0].as_str()).in_list(list, false));
                }
                return Ok(PkFilters::Pushdown(filters));
            }
            // A composite key must not become one OR term per contribution
            // row: the state reader evaluates the filter against every state
            // row, so 4k OR terms turn a small refresh into a quadratic scan.
            // Push one selective IN list instead.  It is a superset of the
            // exact composite key set; the row-key lookup in IvmPkLookupExec
            // performs the final exact match.  One list also avoids paying
            // the membership cost once per key column.
            let mut values_by_column = vec![Vec::new(); table_columns.len()];
            for values in rows {
                for (index, value) in values.into_iter().enumerate() {
                    if !value.is_null() {
                        values_by_column[index].push(value);
                    }
                }
            }
            if values_by_column
                .iter()
                .enumerate()
                .any(|(index, values)| values.is_empty() && !null_columns[index])
            {
                return Ok(PkFilters::Pushdown(vec![lit(false)]));
            }
            // If one key column already exceeds the cheap IN-list bound,
            // filtering on another low-cardinality column is usually not
            // selective enough to justify re-reading the whole bucket for
            // every contribution chunk.  Read the bucket once for the batch
            // and let the exact row-key match below do the work.
            if values_by_column
                .iter()
                .any(|values| values.len() > PK_LOOKUP_IN_LIST_LIMIT)
            {
                return Ok(PkFilters::ScanAll);
            }
            // Apply all key columns, not only the shortest IN list.  The
            // reader still performs the exact Arrow-row match, but the
            // conjunctive filters keep the state rows read for a composite
            // chunk close to the contribution cardinality.  A NULL key adds
            // an `IS NULL` term; a column with only NULL keys drops its IN
            // list.
            let mut filters = Vec::with_capacity(table_columns.len());
            for (index, values) in values_by_column.into_iter().enumerate() {
                let column = col(table_columns[index].as_str());
                let null_term = null_columns[index].then(|| column.clone().is_null());
                let list = (!values.is_empty()).then(|| {
                    column.in_list(values.into_iter().map(lit).collect(), false)
                });
                match (null_term, list) {
                    (Some(null_term), Some(list)) => filters.push(null_term.or(list)),
                    (Some(null_term), None) => filters.push(null_term),
                    (None, Some(list)) => filters.push(list),
                    (None, None) => {}
                }
            }
            Ok(PkFilters::Pushdown(filters))
        }
    }
}

fn key_rows(
    batch: &RecordBatch,
    key: &PkLookupKey,
    table_schema: &SchemaRef,
) -> DataFusionResult<Vec<Option<Vec<u8>>>> {
    match key {
        PkLookupKey::BinaryRow { column } => {
            let index = batch.schema().index_of(column)?;
            let keys = batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "lookup key {column} is not a binary column"
                    ))
                })?;
            Ok((0..keys.len())
                .map(|row| {
                    if keys.is_null(row) {
                        None
                    } else {
                        Some(keys.value(row).to_vec())
                    }
                })
                .collect())
        }
        PkLookupKey::Columns {
            input_columns,
            table_columns,
            null_safe,
        } => {
            let input_columns = mapped_input_columns(input_columns, table_columns);
            let fields = table_columns
                .iter()
                .map(|column| {
                    table_schema
                        .field_with_name(column)
                        .map(|field| SortField::new(field.data_type().clone()))
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "lookup key {column} is missing from the table: {error}"
                            ))
                        })
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            let converter = RowConverter::new(fields)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let arrays = input_columns
                .iter()
                .zip(table_columns)
                .map(|(column, table_column)| {
                    let array = batch.column_by_name(column).cloned().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "lookup key {column} is missing from the contribution"
                        ))
                    })?;
                    // A projection may hand the lookup an optimized view type
                    // (`Utf8View`) while the state table key stores `Utf8`.
                    // Encode against the state key's physical type.
                    let data_type = table_schema
                        .field_with_name(table_column)
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "lookup key {table_column} is missing from the table: {error}"
                            ))
                        })?
                        .data_type();
                    if array.data_type() == data_type {
                        Ok(array)
                    } else {
                        cast(&array, data_type).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "cast lookup key {column} to {data_type}: {error}"
                            ))
                        })
                    }
                })
                .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
            let rows = converter
                .convert_columns(&arrays)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            Ok((0..batch.num_rows())
                .map(|row| {
                    let null = arrays.iter().any(|array| array.is_null(row));
                    // A strict probe drops NULL keys: SQL `=` never matches
                    // NULL.  An identity lookup keeps the row; the Arrow row
                    // bytes encode the NULL and match the same bytes on the
                    // state side.
                    if null && !*null_safe {
                        None
                    } else {
                        Some(rows.row(row).as_ref().to_vec())
                    }
                })
                .collect())
        }
    }
}

fn lookup_batch(
    contribution: &RecordBatch,
    state_batches: &[RecordBatch],
    state_schema: &SchemaRef,
    out_schema: &SchemaRef,
    key: &PkLookupKey,
    multi_match: bool,
    emit_unmatched: bool,
) -> DataFusionResult<RecordBatch> {
    let state = if state_batches.is_empty() {
        None
    } else if state_batches.len() == 1 {
        Some(state_batches[0].clone())
    } else {
        Some(
            arrow::compute::concat_batches(state_schema, state_batches)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        )
    };
    let contribution_keys = key_rows(contribution, key, state_schema)?;
    let mut state_index: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
    if let Some(state) = state.as_ref() {
        let table_key = key.for_table();
        for (row, key) in key_rows(state, &table_key, state_schema)?
            .into_iter()
            .enumerate()
        {
            if let Some(key) = key {
                state_index.entry(key).or_default().push(row as u32);
            }
        }
    }
    let mut contribution_rows = Vec::new();
    let mut state_rows = Vec::new();
    for (row, key) in contribution_keys.into_iter().enumerate() {
        let matches = key.as_ref().and_then(|key| state_index.get(key));
        match matches {
            Some(rows) if multi_match => {
                for state_row in rows {
                    contribution_rows.push(Some(row as u32));
                    state_rows.push(Some(*state_row));
                }
            }
            Some(rows) => {
                contribution_rows.push(Some(row as u32));
                state_rows.push(rows.first().copied());
            }
            None if emit_unmatched => {
                contribution_rows.push(Some(row as u32));
                state_rows.push(None);
            }
            None => {}
        }
    }
    let contribution_rows = UInt32Array::from(contribution_rows);
    let state_rows = UInt32Array::from(state_rows);
    let mut columns =
        Vec::with_capacity(contribution.num_columns() + state_schema.fields().len());
    for index in 0..contribution.num_columns() {
        columns.push(
            take(
                contribution.column(index).as_ref(),
                &contribution_rows,
                None,
            )
            .map_err(|error| DataFusionError::Execution(error.to_string()))?,
        );
    }
    for (index, field) in state_schema.fields().iter().enumerate() {
        let array = match state.as_ref() {
            Some(state) if state.num_rows() > 0 => {
                take(state.column(index).as_ref(), &state_rows, None)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
            }
            _ => arrow::array::new_null_array(field.data_type(), contribution_rows.len()),
        };
        columns.push(array);
    }
    RecordBatch::try_new(out_schema.clone(), columns)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

/// One range-partition group's persistent lookup source and merge config.
struct GroupLookupSource {
    source: LookupSource,
    merge_config: LakeSoulIOConfig,
    defaults: Arc<HashMap<String, String>>,
}

/// Opening state is lazy: the first contribution batch opens every file and
/// index once, then the bucket runs one persistent merge and join.
enum PersistentLookupState {
    Unopened,
    Joined(SendableRecordBatchStream),
    Fallback,
}

/// Key columns and state types a persistent lookup source indexes.
fn lookup_key_spec(
    key: &PkLookupKey,
    state_schema: &SchemaRef,
) -> Option<(Vec<String>, Vec<DataType>)> {
    match key {
        PkLookupKey::Columns { table_columns, .. } => {
            let types = table_columns
                .iter()
                .map(|column| {
                    state_schema
                        .field_with_name(column)
                        .ok()
                        .map(|field| field.data_type().clone())
                })
                .collect::<Option<Vec<_>>>()?;
            Some((table_columns.clone(), types))
        }
        PkLookupKey::BinaryRow { column } => state_schema
            .field_with_name(column)
            .ok()
            .map(|field| (vec![column.clone()], vec![field.data_type().clone()])),
    }
}

async fn open_lookup_sources(
    table: &IvmTable,
    groups: &[PartitionFiles],
    key: &PkLookupKey,
    state_schema: &SchemaRef,
) -> DataFusionResult<Option<Vec<GroupLookupSource>>> {
    let Some((columns, types)) = lookup_key_spec(key, state_schema) else {
        return Ok(None);
    };
    let mut opened = Vec::with_capacity(groups.len());
    for group in groups {
        if group.files.is_empty() {
            continue;
        }
        // A data-file read failure is a query error, not a reason to fall
        // back to the regular reader path.  Only `None` (locator not
        // applicable) falls back.
        let source_and_config =
            table
                .open_lookup_source(group, &columns, &types)
                .await
                .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let Some((source, config)) = source_and_config else {
            tracing::warn!(
                table = %table.table_name,
                partition = %group.partition_desc,
                "persistent pk lookup unavailable; falling back to the reader path"
            );
            return Ok(None);
        };
        let defaults = Arc::new(config.default_column_values().clone());
        opened.push(GroupLookupSource {
            source,
            merge_config: config,
            defaults,
        });
    }
    Ok(Some(opened))
}

/// A one-partition execution plan backed by a channel.
///
/// The persistent lookup channels are unbounded.  DataFusion's piecewise
/// merge join buffers its left (state) side to the end of that stream before
/// it reads the right (contribution) side, and the per-file merge consumes
/// file streams in key order.  A bounded channel would therefore stop the
/// producer while the consumer waits for another stream to reach the same
/// key, or while the join is still collecting the state side.  The state and
/// contribution rows are materialized by the join anyway, so unbounded
/// channels do not add an asymptotic memory cost.
struct ChannelInputExec {
    schema: SchemaRef,
    receiver: Mutex<
        Option<tokio::sync::mpsc::UnboundedReceiver<DataFusionResult<RecordBatch>>>,
    >,
    properties: Arc<PlanProperties>,
}

impl ChannelInputExec {
    fn new(
        schema: SchemaRef,
        receiver: tokio::sync::mpsc::UnboundedReceiver<DataFusionResult<RecordBatch>>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            receiver: Mutex::new(Some(receiver)),
            properties,
        }
    }
}

impl std::fmt::Debug for ChannelInputExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChannelInputExec")
    }
}

impl DisplayAs for ChannelInputExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str("ChannelInputExec")
    }
}

impl ExecutionPlan for ChannelInputExec {
    fn name(&self) -> &str {
        "ChannelInputExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "ChannelInputExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let receiver = self
            .receiver
            .lock()
            .map_err(|_| {
                DataFusionError::Execution(
                    "ChannelInputExec receiver poisoned".to_string(),
                )
            })?
            .take()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "ChannelInputExec can only be executed once".to_string(),
                )
            })?;
        let schema = self.schema.clone();
        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Add the state table's range-partition constants and any missing columns so
/// every file stream has the full merge schema.
fn state_batch_with_defaults(
    batch: RecordBatch,
    defaults: &HashMap<String, String>,
    state_schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    let mut columns = Vec::with_capacity(state_schema.fields().len());
    for field in state_schema.fields() {
        let array = match batch.schema().index_of(field.name()) {
            Ok(index) => {
                let array = Arc::clone(batch.column(index));
                if array.data_type() == field.data_type() {
                    array
                } else {
                    cast(array.as_ref(), field.data_type()).map_err(|error| {
                        DataFusionError::Execution(format!(
                            "cast state column {}: {error}",
                            field.name()
                        ))
                    })?
                }
            }
            Err(_) => match defaults.get(field.name()) {
                Some(value) => {
                    make_default_array(field.data_type(), value, batch.num_rows())
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "inject state partition column {}: {error}",
                                field.name()
                            ))
                        })?
                }
                None => new_null_array(field.data_type(), batch.num_rows()),
            },
        };
        columns.push(array);
    }
    RecordBatch::try_new(state_schema.clone(), columns)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

/// Feed one bucket's sorted contribution stream through the persistent
/// per-file lookup sources into the file channels.  The contribution batch is
/// cloned into the join channel before the lookup so the join can always make
/// progress.
async fn persistent_lookup_producer(
    mut input: SendableRecordBatchStream,
    mut sources: Vec<GroupLookupSource>,
    file_txs: Vec<tokio::sync::mpsc::UnboundedSender<DataFusionResult<RecordBatch>>>,
    contribution_tx: tokio::sync::mpsc::UnboundedSender<DataFusionResult<RecordBatch>>,
    state_schema: SchemaRef,
    key: PkLookupKey,
) {
    let offsets = sources
        .iter()
        .scan(0usize, |offset, group| {
            let start = *offset;
            *offset += group.source.file_count();
            Some(start)
        })
        .collect::<Vec<_>>();
    let mut error_message: Option<String> = None;
    'batch: loop {
        let batch = match input.next().await {
            None => break,
            Some(Err(error)) => {
                error_message = Some(error.to_string());
                break;
            }
            Some(Ok(batch)) if batch.num_rows() == 0 => continue,
            Some(Ok(batch)) => batch,
        };
        if contribution_tx.send(Ok(batch.clone())).is_err() {
            return;
        }
        let contribution_keys = match key_rows(&batch, &key, &state_schema) {
            Ok(keys) => keys,
            Err(error) => {
                error_message = Some(error.to_string());
                break;
            }
        };
        let encoded = match explicit_pk_for_keys(&key, &contribution_keys) {
            Ok(Some((_, keys))) => keys,
            Ok(None) => continue,
            Err(error) => {
                error_message = Some(error.to_string());
                break;
            }
        };
        for (group_index, group) in sources.iter_mut().enumerate() {
            let file_batches = match group.source.lookup_batch(&encoded).await {
                Ok(batches) => batches,
                Err(error) => {
                    error_message = Some(error.to_string());
                    break 'batch;
                }
            };
            for file in file_batches {
                if file.batches.is_empty() {
                    continue;
                }
                let index = offsets[group_index] + file.file_index;
                for batch in file.batches {
                    let batch = match state_batch_with_defaults(
                        batch,
                        &group.defaults,
                        &state_schema,
                    ) {
                        Ok(batch) => batch,
                        Err(error) => {
                            error_message = Some(error.to_string());
                            break 'batch;
                        }
                    };
                    if file_txs[index].send(Ok(batch)).is_err() {
                        return;
                    }
                }
            }
        }
    }
    if let Some(message) = error_message {
        for tx in &file_txs {
            let _ = tx.send(Err(DataFusionError::Execution(message.clone())));
        }
        let _ = contribution_tx.send(Err(DataFusionError::Execution(message)));
    }
}

/// Build the per-bucket persistent plan: one channel input per file, one
/// long-lived `MergeParquetExec`, and one streaming sort-merge join against
/// the contribution.
#[allow(clippy::too_many_arguments)]
fn build_persistent_join(
    table: &IvmTable,
    key: &PkLookupKey,
    state_schema: &SchemaRef,
    out_schema: &SchemaRef,
    sources: Vec<GroupLookupSource>,
    input: SendableRecordBatchStream,
    context: Arc<datafusion::execution::TaskContext>,
    multi_match: bool,
    emit_unmatched: bool,
) -> DataFusionResult<SendableRecordBatchStream> {
    let contribution_schema = input.schema();
    let mut file_txs = Vec::new();
    let mut state_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    // A key may legally appear in more than one range partition (the target
    // primary key need not contain the partition column).  Merge each group
    // separately, then combine the group streams with a sort-preserving merge
    // that keeps equal keys from different groups instead of deduplicating
    // them.
    for group in &sources {
        let mut group_inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let group_files = group.merge_config.files_slice().to_vec();
        for _ in 0..group.source.file_count() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            file_txs.push(tx);
            group_inputs.push(Arc::new(ChannelInputExec::new(state_schema.clone(), rx))
                as Arc<dyn ExecutionPlan>);
        }
        if group_inputs.is_empty() {
            continue;
        }
        // Inputs already contain the group's partition constants, so the
        // merge must not inject defaults again.
        let group_config = LakeSoulIOConfigBuilder::new()
            .with_files(group_files)
            .with_schema(state_schema.clone())
            .with_primary_keys(table.primary_keys.clone())
            .with_physical_format(table.file_format)
            .build();
        let group_merge = MergeParquetExec::new_with_inputs(
            state_schema.clone(),
            group_inputs,
            group_config,
            Arc::new(HashMap::new()),
        )
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        state_plans.push(Arc::new(group_merge));
    }
    if state_plans.is_empty() {
        return Err(DataFusionError::Execution(
            "persistent lookup has no state files".to_string(),
        ));
    }
    let ordering = LexOrdering::new(
        table
            .primary_keys
            .iter()
            .map(|key| {
                Ok(PhysicalSortExpr {
                    expr: Arc::new(PhysicalColumn::new(key, state_schema.index_of(key)?)),
                    options: SortOptions::default(),
                })
            })
            .collect::<DataFusionResult<Vec<_>>>()?,
    )
    .ok_or_else(|| {
        DataFusionError::Internal("persistent lookup state ordering is empty".to_string())
    })?;
    let group_partitions: Arc<dyn ExecutionPlan> = Arc::new(BucketPartitionedExec::new(
        state_plans,
        state_schema.clone(),
    ));
    let state_stream: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(ordering, group_partitions));
    let (contribution_tx, contribution_rx) = tokio::sync::mpsc::unbounded_channel();
    let contribution_input: Arc<dyn ExecutionPlan> = Arc::new(ChannelInputExec::new(
        contribution_schema.clone(),
        contribution_rx,
    ));

    let (table_columns, input_columns) = match key {
        PkLookupKey::Columns {
            input_columns,
            table_columns,
            ..
        } => (
            table_columns.clone(),
            mapped_input_columns(input_columns, table_columns),
        ),
        PkLookupKey::BinaryRow { column } => (vec![column.clone()], vec![column.clone()]),
    };
    let mut on = Vec::with_capacity(table_columns.len());
    for (table_column, input_column) in table_columns.iter().zip(&input_columns) {
        let left = Arc::new(PhysicalColumn::new(
            table_column,
            state_schema.index_of(table_column)?,
        )) as Arc<dyn PhysicalExpr>;
        let right_column = Arc::new(PhysicalColumn::new(
            input_column,
            contribution_schema.index_of(input_column)?,
        ));
        let left_type = state_schema
            .field_with_name(table_column)?
            .data_type()
            .clone();
        let right_type = contribution_schema
            .field_with_name(input_column)?
            .data_type()
            .clone();
        let right: Arc<dyn PhysicalExpr> = if left_type == right_type {
            right_column
        } else {
            Arc::new(CastExpr::new(right_column, left_type, None))
        };
        on.push((left, right));
    }
    let sort_options = vec![SortOptions::default(); table_columns.len()];
    let join_type = if multi_match {
        JoinType::Inner
    } else if emit_unmatched {
        JoinType::Left
    } else {
        JoinType::Inner
    };
    // An identity lookup for a nullable group/partition key must match NULL
    // with NULL; a strict join probe keeps them apart.  Both inputs are sorted
    // with `SortOptions::default()` (NULLs first), the order the writer and
    // the per-file index use.
    let null_equality = if key.null_safe() {
        NullEquality::NullEqualsNull
    } else {
        NullEquality::NullEqualsNothing
    };
    let join = SortMergeJoinExec::try_new(
        state_stream,
        contribution_input,
        on,
        None,
        join_type,
        sort_options,
        null_equality,
    )?;
    let state_field_count = state_schema.fields().len();
    let contribution_field_count = contribution_schema.fields().len();
    let mut projection = Vec::with_capacity(out_schema.fields().len());
    for (index, field) in out_schema.fields().iter().enumerate() {
        if index < contribution_field_count {
            projection.push((
                Arc::new(PhysicalColumn::new(field.name(), state_field_count + index))
                    as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            ));
        } else {
            let state_index = index - contribution_field_count;
            let state_name = state_schema.field(state_index).name();
            projection.push((
                Arc::new(PhysicalColumn::new(state_name, state_index))
                    as Arc<dyn PhysicalExpr>,
                format!("{IVM_STATE_COLUMN_PREFIX}{state_name}"),
            ));
        }
    }
    let projected = ProjectionExec::try_new(projection, Arc::new(join))?;

    tokio::spawn(persistent_lookup_producer(
        input,
        sources,
        file_txs,
        contribution_tx,
        state_schema.clone(),
        key.clone(),
    ));
    projected.execute(0, context)
}

/// A physical scan of the state buckets owned by one execution stream,
/// left-joined onto the contribution rows by primary key.
///
/// The plan is intentionally made of plain table/file descriptors: the
/// execution-time concurrency gate is process-local and is not part of the
/// serializable descriptor.  Each `(task, partition)` owns the buckets that
/// hash to its slot of the bucket shuffle; [`IvmPkLookupExec::bucket_indices`]
/// derives them from the same hash the shuffle uses.
pub(crate) struct IvmPkLookupExec {
    /// The bucket shuffle this lookup consumes.  It is the plan child, so the
    /// distributed planner can split it into a worker stage.
    input: Arc<dyn ExecutionPlan>,
    /// `input` sorted by the lookup key, kept out of the plan tree.
    ///
    /// The per-file index cursor needs each partition ordered by key.  A
    /// `SortExec` in the tree is removed by `EnforceSorting` when the input
    /// has one partition, which also drops the bucket shuffle underneath it.
    /// Keeping the sort private to this node leaves the shuffle intact.
    sorted_input: Arc<dyn ExecutionPlan>,
    table: IvmTable,
    groups_by_bucket: Arc<Vec<Vec<PartitionFiles>>>,
    key: PkLookupKey,
    /// Number of state buckets (the writer's hash bucket count).
    bucket_count: usize,
    /// Partition count of the bucket shuffle the lookup consumes, or 0 when
    /// the input is not hash-partitioned on `__ivm_bucket`.
    ///
    /// `EnforceDistribution` may replace the inserted repartition with one
    /// sized by `target_partitions`; the slot mapping follows the actual
    /// shuffle count, not the writer's bucket count.
    shuffle_partitions: usize,
    schema: SchemaRef,
    gate: Arc<Semaphore>,
    multi_match: bool,
    emit_unmatched: bool,
    /// The hidden bucket column kept in the input stream so the lookup can
    /// recover which buckets its execution stream owns.
    bucket_column: Option<String>,
    /// Full lookup output, including the hidden bucket column.
    full_schema: SchemaRef,
    /// Projection that drops the hidden bucket column from the output.
    output_projection: Option<Arc<Vec<usize>>>,
    profile_io: Option<crate::observability::ProfileHandle>,
    properties: Arc<PlanProperties>,
}

#[allow(dead_code)]
impl IvmPkLookupExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        table: IvmTable,
        groups_by_bucket: Vec<Vec<PartitionFiles>>,
        key_column: impl Into<String>,
        bucket_count: usize,
    ) -> DataFusionResult<Self> {
        Self::new_with_key(
            input,
            table,
            groups_by_bucket,
            PkLookupKey::BinaryRow {
                column: key_column.into(),
            },
            bucket_count,
        )
    }

    /// Generic PK lookup by real table columns.  Contribution columns are
    /// matched to table primary-key columns with `arrow::Row`.
    pub(crate) fn new_with_columns(
        input: Arc<dyn ExecutionPlan>,
        table: IvmTable,
        groups_by_bucket: Vec<Vec<PartitionFiles>>,
        input_columns: Vec<String>,
        table_columns: Vec<String>,
        bucket_count: usize,
        null_safe: bool,
    ) -> DataFusionResult<Self> {
        Self::new_with_key(
            input,
            table,
            groups_by_bucket,
            PkLookupKey::Columns {
                input_columns,
                table_columns,
                null_safe,
            },
            bucket_count,
        )
    }

    pub(crate) fn new_with_key(
        input: Arc<dyn ExecutionPlan>,
        table: IvmTable,
        groups_by_bucket: Vec<Vec<PartitionFiles>>,
        key: PkLookupKey,
        bucket_count: usize,
    ) -> DataFusionResult<Self> {
        Self::build_with_input(input, table, groups_by_bucket, key, bucket_count)
    }

    /// Rebuild around an already shaped input.
    ///
    /// Physical optimizers and the distributed planner replace children of a
    /// plan that already carries the lookup sort.  Sorting again here would
    /// stack a second `SortExec` above the first one on every rewrite.
    fn build_with_input(
        input: Arc<dyn ExecutionPlan>,
        table: IvmTable,
        groups_by_bucket: Vec<Vec<PartitionFiles>>,
        key: PkLookupKey,
        bucket_count: usize,
    ) -> DataFusionResult<Self> {
        let bucket_count = bucket_count.max(1);
        let sorted_input = sort_lookup_input(Arc::clone(&input), &key)?;
        let input_partitions = sorted_input.output_partitioning().partition_count();
        let expected_bucket_expr: Option<Arc<dyn PhysicalExpr>> = sorted_input
            .schema()
            .index_of(IVM_BUCKET_COLUMN)
            .ok()
            .map(|index| {
                Arc::new(PhysicalColumn::new(IVM_BUCKET_COLUMN, index))
                    as Arc<dyn PhysicalExpr>
            });
        let shuffle_partitions =
            match (&expected_bucket_expr, input.output_partitioning()) {
                (Some(expected), Partitioning::Hash(expressions, count))
                    if expressions.len() == 1
                        && expressions[0].as_ref().eq(expected.as_ref()) =>
                {
                    *count
                }
                _ => 0,
            };
        let bucket_column = input
            .schema()
            .field_with_name(IVM_BUCKET_COLUMN)
            .ok()
            .map(|_| IVM_BUCKET_COLUMN.to_string());
        if bucket_count > 1 && bucket_column.is_none() && input_partitions != bucket_count
        {
            return Err(DataFusionError::Internal(format!(
                "PK lookup needs {bucket_count} input partitions, got {input_partitions}"
            )));
        }
        let output_partitions = input_partitions;
        let full_schema = state_output_schema(&sorted_input.schema(), &table.schema);
        let (schema, output_projection) = match bucket_column.as_ref() {
            Some(column) => {
                let bucket_index = full_schema.index_of(column)?;
                let keep = (0..full_schema.fields().len())
                    .filter(|index| *index != bucket_index)
                    .collect::<Vec<_>>();
                let schema = Arc::new(Schema::new(
                    keep.iter()
                        .map(|index| Arc::new(full_schema.field(*index).clone()))
                        .collect::<Vec<_>>(),
                ));
                (schema, Some(Arc::new(keep)))
            }
            None => (full_schema.clone(), None),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(output_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            sorted_input,
            table,
            groups_by_bucket: Arc::new(groups_by_bucket),
            key,
            bucket_count,
            shuffle_partitions,
            bucket_column,
            full_schema,
            output_projection,
            schema,
            gate: Arc::new(Semaphore::new(max_active_tasks())),
            multi_match: false,
            emit_unmatched: false,
            profile_io: None,
            properties,
        })
    }

    pub(crate) fn table(&self) -> &IvmTable {
        &self.table
    }

    pub(crate) fn groups_by_bucket(&self) -> &[Vec<PartitionFiles>] {
        &self.groups_by_bucket
    }

    pub(crate) fn key(&self) -> &PkLookupKey {
        &self.key
    }

    pub(crate) fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    pub(crate) fn multi_match(&self) -> bool {
        self.multi_match
    }

    pub(crate) fn emit_unmatched(&self) -> bool {
        self.emit_unmatched
    }

    pub(crate) fn with_flags(mut self, multi_match: bool, emit_unmatched: bool) -> Self {
        self.multi_match = multi_match;
        self.emit_unmatched = emit_unmatched;
        self
    }

    pub(crate) fn with_profile(
        mut self,
        profile_io: Option<crate::observability::ProfileHandle>,
    ) -> Self {
        self.profile_io = profile_io;
        self
    }

    /// Re-apply the bucket shuffle when a physical optimizer removed it.
    ///
    /// `EnforceDistribution` removes explicit repartitions and may satisfy the
    /// lookup's key requirement with a single partition, where every bucket
    /// would have to be opened.  This runs after the stock optimizers and
    /// restores the exact bucket shuffle; the distributed planner then sees a
    /// normal stage boundary.
    fn ensure_bucket_shuffle(&self) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if self.bucket_count <= 1 || self.shuffle_partitions > 0 {
            return Ok(None);
        }
        let Some(column) = self.bucket_column.as_ref() else {
            return Ok(None);
        };
        let index = self.input.schema().index_of(column)?;
        let bucket_expr =
            Arc::new(PhysicalColumn::new(column, index)) as Arc<dyn PhysicalExpr>;
        let repartition = Arc::new(RepartitionExec::try_new(
            Arc::clone(&self.input),
            Partitioning::Hash(vec![bucket_expr], self.bucket_count),
        )?) as Arc<dyn ExecutionPlan>;
        let mut plan = Self::build_with_input(
            repartition,
            self.table.clone(),
            self.groups_by_bucket.as_ref().clone(),
            self.key.clone(),
            self.bucket_count,
        )?;
        plan.gate = Arc::clone(&self.gate);
        plan.multi_match = self.multi_match;
        plan.emit_unmatched = self.emit_unmatched;
        plan.profile_io = self.profile_io.clone();
        Ok(Some(Arc::new(plan)))
    }
    /// Join-side lookup mode: emit every matching state row and drop a
    /// contribution row without a match (inner-join semantics).
    pub(crate) fn with_multi_match(mut self, multi_match: bool) -> Self {
        self.multi_match = multi_match;
        self.emit_unmatched = !multi_match;
        self
    }
}

impl std::fmt::Debug for IvmPkLookupExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmPkLookupExec")
    }
}

impl DisplayAs for IvmPkLookupExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "IvmPkLookupExec buckets={} key={}",
            self.bucket_count,
            self.key.display()
        )
    }
}

impl ExecutionPlanProperties for IvmPkLookupExec {
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

impl IvmPkLookupExec {
    /// The buckets this `(task, partition)` owns.
    ///
    /// The contribution is hash-shuffled on the deterministic bucket id.  A
    /// distributed planner scales that hash partition to
    /// `bucket_count * task_count`; consumer task `i` reads the global slots
    /// `[P*i, P*(i+1))` and its local partition `p` is slot `P*i + p`.  The
    /// same hash modulo `P * C` recovers the buckets of every local partition,
    /// so each bucket is opened by exactly one execution stream.
    /// Whether the input is actually shuffled on the hidden bucket column.
    ///
    /// `EnforceSorting` can delete an explicit bucket repartition when the
    /// input already has one partition.  In that case every execution stream
    /// sees the whole contribution and must probe every bucket; using the
    /// hash ownership would silently drop rows.  The same conservative
    /// fallback covers any repartition key the optimizer put in its place.
    fn bucket_ownership(&self) -> BucketOwnership {
        if self.shuffle_partitions > 0 {
            BucketOwnership::Hash
        } else {
            if self.bucket_count > 1 {
                tracing::debug!(
                    table = %self.table.table_name,
                    bucket_count = self.bucket_count,
                    input = %self.input.name(),
                    partitioning = %self.input.output_partitioning(),
                    "keyed lookup has no bucket shuffle; opening every state bucket"
                );
            }
            BucketOwnership::All
        }
    }

    fn bucket_indices(
        &self,
        partition: usize,
        task: LookupTask,
    ) -> DataFusionResult<Vec<usize>> {
        let buckets = self.groups_by_bucket.len().max(1);
        if self.bucket_count <= 1 || self.bucket_ownership() == BucketOwnership::All {
            return Ok((0..buckets).collect());
        }
        let task_count = task.count.max(1);
        let shuffle_partitions = self.shuffle_partitions.max(1);
        let total = shuffle_partitions.saturating_mul(task_count).max(1);
        let values: ArrayRef = Arc::new(UInt32Array::from_iter_values(
            (0..self.bucket_count as u32).collect::<Vec<_>>(),
        ));
        let mut hashes = vec![0u64; self.bucket_count];
        datafusion::common::hash_utils::create_hashes(
            [values.as_ref()],
            datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE
                .random_state(),
            &mut hashes,
        )
        .map_err(|error| {
            DataFusionError::Execution(format!("hash IVM bucket ids: {error}"))
        })?;
        Ok((0..self.bucket_count.min(buckets))
            .filter(|bucket| {
                let slot = (hashes[*bucket] % total as u64) as usize;
                slot / shuffle_partitions == task.index
                    && slot % shuffle_partitions == partition
            })
            .collect())
    }

    fn groups_for_partition(
        &self,
        partition: usize,
        task: LookupTask,
    ) -> DataFusionResult<Vec<PartitionFiles>> {
        let mut groups = Vec::new();
        for bucket in self.bucket_indices(partition, task)? {
            if let Some(files) = self.groups_by_bucket.get(bucket) {
                groups.extend(files.iter().cloned());
            }
        }
        Ok(groups)
    }

    /// Resolve one bucket through persistent per-file lookup sources.
    ///
    /// The first contribution batch opens every state file and PK index once;
    /// later batches only probe the in-memory index and fetch rows from the
    /// already-open files.  Each batch's per-file results enter one LakeSoul
    /// merge per range-partition group.
    fn execute_persistent_lookup(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let task = lookup_task(&context);
        tracing::debug!(
            target: "lakesoul_ivm::keyed_plan",
            pid = std::process::id(),
            table = %self.table.table_name,
            partition,
            task_index = task.index,
            task_count = task.count,
            "executing keyed lookup"
        );
        let input = self.sorted_input.execute(partition, context.clone())?;
        let groups =
            Arc::new(self.groups_for_partition(partition, lookup_task(&context))?);
        let table = self.table.clone();
        let key = self.key.clone();
        let state_schema = self.table.schema.clone();
        let out_schema = self.full_schema.clone();
        let output_projection = self.output_projection.clone();
        let gate = self.gate.clone();
        let multi_match = self.multi_match;
        let emit_unmatched = self.emit_unmatched;
        let stream = futures::stream::try_unfold(
            (Some(input), PersistentLookupState::Unopened),
            move |(mut input, mut state)| {
                let table = table.clone();
                let key = key.clone();
                let state_schema = state_schema.clone();
                let out_schema = out_schema.clone();
                let gate = gate.clone();
                let groups = groups.clone();
                let output_projection = output_projection.clone();
                let context = context.clone();
                async move {
                    loop {
                        match state {
                            PersistentLookupState::Unopened => {
                                let opened = open_lookup_sources(
                                    &table,
                                    groups.as_slice(),
                                    &key,
                                    &state_schema,
                                )
                                .await?;
                                let Some(sources) =
                                    opened.filter(|sources| !sources.is_empty())
                                else {
                                    state = PersistentLookupState::Fallback;
                                    continue;
                                };
                                let contribution = input.take().ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "persistent lookup lost its input".to_string(),
                                    )
                                })?;
                                let joined = build_persistent_join(
                                    &table,
                                    &key,
                                    &state_schema,
                                    &out_schema,
                                    sources,
                                    contribution,
                                    context.clone(),
                                    multi_match,
                                    emit_unmatched,
                                )?;
                                state = PersistentLookupState::Joined(joined);
                            }
                            PersistentLookupState::Joined(mut joined) => {
                                match joined.next().await {
                                    None => return Ok(None),
                                    Some(Err(error)) => return Err(error),
                                    Some(Ok(batch)) => {
                                        let batch = project_lookup_output(
                                            batch,
                                            output_projection
                                                .as_deref()
                                                .map(Vec::as_slice),
                                        )?;
                                        state = PersistentLookupState::Joined(joined);
                                        return Ok(Some((batch, (input, state))));
                                    }
                                }
                            }
                            PersistentLookupState::Fallback => {
                                let input_stream = input.as_mut().ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "fallback lookup lost its input".to_string(),
                                    )
                                })?;
                                let batch = loop {
                                    match input_stream.next().await {
                                        None => return Ok(None),
                                        Some(Err(error)) => return Err(error),
                                        Some(Ok(batch)) if batch.num_rows() == 0 => {
                                            continue;
                                        }
                                        Some(Ok(batch)) => break batch,
                                    }
                                };
                                let started = std::time::Instant::now();
                                let (output, state_rows) = lookup_streaming_bulk(
                                    &table,
                                    groups.as_slice(),
                                    &batch,
                                    &state_schema,
                                    &out_schema,
                                    &key,
                                    multi_match,
                                    emit_unmatched,
                                    gate.clone(),
                                )
                                .await?;
                                tracing::debug!(
                                    table = %table.table_name,
                                    key = %key.display(),
                                    partition,
                                    contribution_rows = batch.num_rows(),
                                    state_rows,
                                    output_rows = output.num_rows(),
                                    elapsed = ?started.elapsed(),
                                    "keyed state lookup fallback finished"
                                );
                                let output = project_lookup_output(
                                    output,
                                    output_projection.as_deref().map(Vec::as_slice),
                                )?;
                                return Ok(Some((
                                    output,
                                    (input, PersistentLookupState::Fallback),
                                )));
                            }
                        }
                    }
                }
            },
        );
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

impl ExecutionPlan for IvmPkLookupExec {
    fn name(&self) -> &str {
        "IvmPkLookupExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// The bucket repartition is part of the lookup's contract, not an
    /// optional optimization.  DataFusion strips explicit repartitions whose
    /// output distribution no parent requires, so ask for the bucket key
    /// explicitly; `EnforceDistribution` then keeps the existing shuffle
    /// instead of adding another one.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        match self.bucket_column.as_ref() {
            Some(column) => {
                match self.input.schema().index_of(column) {
                    Ok(index) => vec![Distribution::KeyPartitioned(vec![Arc::new(
                        PhysicalColumn::new(column, index),
                    )
                        as Arc<dyn PhysicalExpr>])],
                    Err(_) => vec![Distribution::UnspecifiedDistribution],
                }
            }
            None => vec![Distribution::UnspecifiedDistribution],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "keyed state lookup expects one child".to_string(),
            ));
        }
        let mut plan = Self::build_with_input(
            children[0].clone(),
            self.table.clone(),
            self.groups_by_bucket.as_ref().clone(),
            self.key.clone(),
            self.bucket_count,
        )?;
        plan.gate = self.gate.clone();
        plan.multi_match = self.multi_match;
        plan.emit_unmatched = self.emit_unmatched;
        plan.profile_io = self.profile_io.clone();
        Ok(Arc::new(plan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.execute_persistent_lookup(partition, context)
    }
}

/// The `(partition column, contribution column)` pairs a lookup can prune
/// by.  `None` means at least one partition column is not part of the lookup
/// key, so the read must keep every partition.
pub(crate) fn partition_pairs_for_key(
    key: &PkLookupKey,
    partitions: &[String],
    contribution_schema: &Schema,
) -> Option<Vec<(String, String)>> {
    if partitions.is_empty() {
        return None;
    }
    match key {
        PkLookupKey::Columns {
            input_columns,
            table_columns,
            ..
        } => partitions
            .iter()
            .map(|partition| {
                let index = table_columns
                    .iter()
                    .position(|column| column == partition)?;
                Some((partition.clone(), input_columns.get(index)?.clone()))
            })
            .collect(),
        PkLookupKey::BinaryRow { .. } => partitions
            .iter()
            .map(|partition| {
                contribution_schema.field_with_name(partition).ok()?;
                Some((partition.clone(), partition.clone()))
            })
            .collect(),
    }
}

/// The distinct partition values present in already-materialized contribution
/// batches.  Empty or unparsable input returns `None`, which keeps every
/// partition.
pub(crate) fn partition_restriction_from_batches(
    batches: &[RecordBatch],
    pairs: &[(String, String)],
) -> Option<Vec<(String, Vec<String>)>> {
    if pairs.is_empty() {
        return None;
    }
    let mut values: Vec<(String, std::collections::BTreeSet<String>)> = pairs
        .iter()
        .map(|(column, _)| (column.clone(), std::collections::BTreeSet::new()))
        .collect();
    let mut rows = 0usize;
    for batch in batches {
        for row in 0..batch.num_rows() {
            for (index, (_, input)) in pairs.iter().enumerate() {
                let array = batch.column_by_name(input)?;
                let scalar = ScalarValue::try_from_array(array, row).ok()?;
                values[index]
                    .1
                    .insert(lakesoul_io::helpers::format_scalar_value(&scalar));
            }
            rows += 1;
        }
    }
    if rows == 0 {
        return None;
    }
    Some(
        values
            .into_iter()
            .map(|(column, values)| (column, values.into_iter().collect()))
            .collect(),
    )
}

/// Test/diagnostic counters for the last keyed lookup of each state table.
static PARTITION_STATS: OnceLock<Mutex<HashMap<String, (u64, u64)>>> = OnceLock::new();
static PARTITION_STATS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Enable the partition-pruning counters for the rest of the process.
///
/// This is a test/diagnostic hook, not a production configuration: enabling
/// it costs one extra metadata listing per pruned lookup.
#[doc(hidden)]
pub fn enable_partition_stats() {
    PARTITION_STATS_ENABLED.store(true, Ordering::Relaxed);
}

pub(crate) fn partition_stats_enabled() -> bool {
    PARTITION_STATS_ENABLED.load(Ordering::Relaxed)
}

fn partition_stats() -> &'static Mutex<HashMap<String, (u64, u64)>> {
    PARTITION_STATS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn record_partition_stats(table_id: &str, total: usize, kept: usize) {
    if let Ok(mut stats) = partition_stats().lock() {
        // Several stages can narrow the same lookup.  Keep the widest total
        // seen and the narrowest kept set, so the reported pair describes the
        // whole lookup rather than only its last stage.
        let entry = stats.entry(table_id.to_string()).or_insert((0, u64::MAX));
        entry.0 = entry.0.max(total as u64);
        entry.1 = entry.1.min(kept as u64);
    }
}

/// The last keyed lookup's `(partition groups total, partition groups kept)`
/// for a state table.  Diagnostic only; the map retains one entry per table
/// id for the life of the process.
#[doc(hidden)]
pub fn state_partition_stats(table_id: &str) -> Option<(u64, u64)> {
    partition_stats()
        .lock()
        .ok()
        .and_then(|stats| stats.get(table_id).copied())
}

pub(crate) struct IvmLookupTable {
    contribution: DataFrame,
    table: IvmTable,
    groups_by_bucket: Vec<Vec<PartitionFiles>>,
    key: PkLookupKey,
    bucket_count: usize,
    route_columns: Option<Vec<String>>,
    bucket_key: Option<String>,
    multi_match: bool,
    /// The range partition columns a binary key can prune by: the plain join
    /// keys that are also partition columns of the state.
    partition_columns: Vec<String>,
    /// The caller already fetched only the matching partition groups.
    prepruned: bool,
    profile_io: Option<crate::observability::ProfileHandle>,
    schema: SchemaRef,
}

impl IvmLookupTable {
    pub(crate) fn columns(
        contribution: DataFrame,
        table: IvmTable,
        groups_by_bucket: Vec<Vec<PartitionFiles>>,
        input_columns: Vec<String>,
        table_columns: Vec<String>,
        bucket_count: usize,
        route_columns: Option<Vec<String>>,
        multi_match: bool,
    ) -> Self {
        let input_schema = Arc::new(contribution.schema().as_arrow().clone());
        let schema = state_output_schema(&input_schema, &table.schema);
        let partition_columns = table.range_partition_columns.clone();
        Self {
            contribution,
            table,
            groups_by_bucket,
            key: PkLookupKey::Columns {
                input_columns,
                table_columns,
                null_safe: false,
            },
            bucket_count,
            route_columns,
            bucket_key: None,
            multi_match,
            partition_columns,
            prepruned: false,
            profile_io: crate::observability::current_refresh_io(),
            schema,
        }
    }

    pub(crate) fn binary(
        contribution: DataFrame,
        table: IvmTable,
        groups_by_bucket: Vec<Vec<PartitionFiles>>,
        key_column: String,
        bucket_count: usize,
        multi_match: bool,
        partition_columns: Vec<String>,
    ) -> Self {
        let input_schema = Arc::new(contribution.schema().as_arrow().clone());
        let schema = state_output_schema(&input_schema, &table.schema);
        Self {
            contribution,
            table,
            groups_by_bucket,
            key: PkLookupKey::BinaryRow {
                column: key_column.clone(),
            },
            bucket_count,
            route_columns: None,
            bucket_key: (bucket_count > 1).then_some(key_column),
            multi_match,
            partition_columns,
            prepruned: false,
            profile_io: crate::observability::current_refresh_io(),
            schema,
        }
    }

    /// Mark the supplied groups as already restricted to the lookup's
    /// partitions.  The plan then skips the contribution collect and filter.
    pub(crate) fn with_prepruned_groups(mut self) -> Self {
        self.prepruned = true;
        self
    }

    /// Match NULL keys with NULL rows (a grouped/partitioned identity lookup)
    /// instead of dropping them (a strict join probe).
    pub(crate) fn with_null_safe(mut self, null_safe: bool) -> Self {
        if let PkLookupKey::Columns { null_safe: key, .. } = &mut self.key {
            *key = null_safe;
        }
        self
    }

    /// The range-partition values carried by the contribution, when the
    /// lookup key contains every partition column.
    ///
    /// Only the distinct partition values of the affected keys are collected
    /// (a handful of dates in practice), never the key frame itself.  A
    /// missing or unparsable key returns `None`, which keeps every partition:
    /// pruning is an optimization and must never drop a needed one.
    async fn partition_restriction(&self) -> Option<Vec<(String, Vec<String>)>> {
        let partitions = match &self.key {
            PkLookupKey::Columns { .. } => &self.table.range_partition_columns,
            PkLookupKey::BinaryRow { .. } => &self.partition_columns,
        };
        let pairs = partition_pairs_for_key(
            &self.key,
            partitions,
            self.contribution.schema().as_arrow(),
        )?;
        let frame = self
            .contribution
            .clone()
            .select(
                pairs
                    .iter()
                    .map(|(_, input)| col(input.as_str()))
                    .collect::<Vec<_>>(),
            )
            .ok()?
            .distinct()
            .ok()?;
        let batches = frame.collect().await.ok()?;
        partition_restriction_from_batches(&batches, &pairs)
    }

    /// Turn this provider into a keyed-lookup logical node.
    ///
    /// The contribution stays a logical child, so the session's physical
    /// planner (and the distributed planner layered on it) can split the
    /// bucket shuffle into a normal worker stage.  Only the small
    /// range-partition restriction is resolved here; the keyed read itself is
    /// planned and executed as part of the outer refresh plan.
    pub(crate) async fn into_dataframe(
        self,
        context: &SessionContext,
    ) -> DataFusionResult<DataFrame> {
        let groups_by_bucket = if self.prepruned {
            self.groups_by_bucket.clone()
        } else {
            match self.partition_restriction().await {
                Some(restriction) => {
                    let kept = self
                        .groups_by_bucket
                        .iter()
                        .map(|groups| {
                            groups
                                .iter()
                                .filter(|group| {
                                    partition_matches(
                                        &group.partition_desc,
                                        &self.table.range_partition_columns,
                                        &restriction,
                                    )
                                })
                                .cloned()
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    tracing::debug!(
                        table = %self.table.table_name,
                        partitions = self.groups_by_bucket.iter().flatten().count(),
                        kept = kept.iter().flatten().count(),
                        "keyed lookup pruned partitions"
                    );
                    kept
                }
                None => self.groups_by_bucket.clone(),
            }
        };
        record_partition_stats(
            &self.table.table_id,
            self.groups_by_bucket.iter().flatten().count(),
            groups_by_bucket.iter().flatten().count(),
        );
        let schema = self.schema.clone();
        let node = IvmKeyedLookupNode {
            spec: Arc::new(IvmKeyedLookupSpec {
                table: self.table,
                groups_by_bucket,
                key: self.key,
                bucket_count: self.bucket_count,
                route_columns: self.route_columns,
                bucket_key: self.bucket_key,
                multi_match: self.multi_match,
                profile_io: self.profile_io,
            }),
            contribution: self.contribution.logical_plan().clone(),
            schema: Arc::new(DFSchema::try_from(schema)?),
        };
        Ok(DataFrame::new(
            context.state(),
            LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            }),
        ))
    }
}

impl std::fmt::Debug for IvmLookupTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmLookupTable")
    }
}

/// One execution partition per LakeSoul hash bucket.  Each child is already a
/// single-partition plan over that bucket's state files, so the downstream
/// hash join can run every bucket concurrently without shuffling the state.
struct BucketPartitionedExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
    properties: Arc<PlanProperties>,
}

impl BucketPartitionedExec {
    fn new(children: Vec<Arc<dyn ExecutionPlan>>, schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(children.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            children,
            properties,
        }
    }
}

impl std::fmt::Debug for BucketPartitionedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BucketPartitionedExec buckets={}", self.children.len())
    }
}

impl DisplayAs for BucketPartitionedExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "BucketPartitionedExec buckets={}", self.children.len())
    }
}

impl ExecutionPlan for BucketPartitionedExec {
    fn name(&self) -> &str {
        "BucketPartitionedExec"
    }

    fn schema(&self) -> SchemaRef {
        self.properties.eq_properties.schema().clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children, self.schema())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let child = self.children.get(partition).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "bucket partition {partition} is out of range"
            ))
        })?;
        child.execute(0, context)
    }
}

/// Serializable specification of one keyed state lookup.
pub(crate) struct IvmKeyedLookupSpec {
    table: IvmTable,
    groups_by_bucket: Vec<Vec<PartitionFiles>>,
    key: PkLookupKey,
    bucket_count: usize,
    route_columns: Option<Vec<String>>,
    bucket_key: Option<String>,
    multi_match: bool,
    profile_io: Option<crate::observability::ProfileHandle>,
}

impl std::fmt::Debug for IvmKeyedLookupSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IvmKeyedLookupSpec(table={}, buckets={}, key={})",
            self.table.table_name,
            self.bucket_count,
            self.key.display(),
        )
    }
}

/// Logical node for a keyed state lookup of `contribution`.
///
/// The contribution is a logical child of this node, not a plan embedded in
/// a `TableProvider::scan`.  That distinction is what makes the bucket
/// shuffle a first-class stage: the session plans the contribution and the
/// lookup once, then the distributed planner can inject a network boundary
/// above the bucket repartition and run `IvmPkLookupExec` on workers.
///
/// A nested physical plan built inside `scan` carries its own
/// `DistributedExec`; workers can never resolve the stages of that inner
/// query, which is the deadlock this node removes.
pub(crate) struct IvmKeyedLookupNode {
    spec: Arc<IvmKeyedLookupSpec>,
    contribution: LogicalPlan,
    schema: DFSchemaRef,
}

impl std::fmt::Debug for IvmKeyedLookupNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmKeyedLookupNode")
    }
}

impl PartialEq for IvmKeyedLookupNode {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.spec, &other.spec)
    }
}

impl Eq for IvmKeyedLookupNode {}

impl std::hash::Hash for IvmKeyedLookupNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.spec) as usize).hash(state);
    }
}

impl PartialOrd for IvmKeyedLookupNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IvmKeyedLookupNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (Arc::as_ptr(&self.spec) as usize).cmp(&(Arc::as_ptr(&other.spec) as usize))
    }
}

impl UserDefinedLogicalNodeCore for IvmKeyedLookupNode {
    fn name(&self) -> &str {
        "IvmKeyedLookup"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.contribution]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IvmKeyedLookup table={} buckets={} key={}",
            self.spec.table.table_name,
            self.spec.bucket_count,
            self.spec.key.display(),
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "IvmKeyedLookup expects one input and no expressions".to_string(),
            ));
        }
        Ok(Self {
            spec: Arc::clone(&self.spec),
            contribution: inputs.into_iter().next().expect("one input"),
            schema: self.schema.clone(),
        })
    }
}

/// Serializable specification of one keyed state merge.
pub(crate) struct IvmStateMergeSpec {
    state: IvmTable,
    groups_by_bucket: Vec<Vec<PartitionFiles>>,
    bucket_count: usize,
    group_keys: Vec<String>,
    measure_columns: Vec<String>,
    epoch: i64,
}

impl std::fmt::Debug for IvmStateMergeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IvmStateMergeSpec(table={}, buckets={}, measures={})",
            self.state.table_name,
            self.bucket_count,
            self.measure_columns.join(","),
        )
    }
}

/// Logical node that turns aggregate contributions into the next state rows.
///
/// The contribution stays a logical child so the bucket shuffle and the
/// lookup are planned as normal worker stages.  The state merge itself is a
/// streaming physical node above them; it never runs on the coordinator.
pub(crate) struct IvmStateMergeNode {
    spec: Arc<IvmStateMergeSpec>,
    contribution: LogicalPlan,
    schema: DFSchemaRef,
}

impl std::fmt::Debug for IvmStateMergeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmStateMergeNode")
    }
}

impl PartialEq for IvmStateMergeNode {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.spec, &other.spec)
    }
}

impl Eq for IvmStateMergeNode {}

impl std::hash::Hash for IvmStateMergeNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.spec) as usize).hash(state);
    }
}

impl PartialOrd for IvmStateMergeNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IvmStateMergeNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (Arc::as_ptr(&self.spec) as usize).cmp(&(Arc::as_ptr(&other.spec) as usize))
    }
}

impl UserDefinedLogicalNodeCore for IvmStateMergeNode {
    fn name(&self) -> &str {
        "IvmStateMerge"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.contribution]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IvmStateMerge table={} buckets={} measures={}",
            self.spec.state.table_name,
            self.spec.bucket_count,
            self.spec.measure_columns.join(","),
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DataFusionResult<Self> {
        if !exprs.is_empty() || inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "IvmStateMerge expects one input and no expressions".to_string(),
            ));
        }
        Ok(Self {
            spec: Arc::clone(&self.spec),
            contribution: inputs.into_iter().next().expect("one input"),
            schema: self.schema.clone(),
        })
    }
}

/// Build the logical plan that applies aggregate contributions to `state`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn state_merge_dataframe(
    context: &SessionContext,
    contribution: DataFrame,
    state: IvmTable,
    groups_by_bucket: Vec<Vec<PartitionFiles>>,
    bucket_count: usize,
    group_keys: Vec<String>,
    measure_columns: Vec<String>,
    epoch: i64,
) -> DataFusionResult<DataFrame> {
    let schema = Arc::new(DFSchema::try_from(state.schema.clone())?);
    let node = IvmStateMergeNode {
        spec: Arc::new(IvmStateMergeSpec {
            state,
            groups_by_bucket,
            bucket_count,
            group_keys,
            measure_columns,
            epoch,
        }),
        contribution: contribution.logical_plan().clone(),
        schema,
    };
    Ok(DataFrame::new(
        context.state(),
        LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }),
    ))
}

/// Physical rule that restores the bucket shuffle after `EnforceDistribution`.
#[derive(Debug, Default)]
pub struct IvmKeyedLookupRule;

impl datafusion::physical_optimizer::PhysicalOptimizerRule for IvmKeyedLookupRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        use datafusion::common::tree_node::{Transformed, TreeNode};
        plan.transform_up(|node| {
            let Some(lookup) = node.downcast_ref::<IvmPkLookupExec>() else {
                return Ok(Transformed::no(node));
            };
            match lookup.ensure_bucket_shuffle()? {
                Some(replacement) => Ok(Transformed::yes(replacement)),
                None => Ok(Transformed::no(node)),
            }
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "ivm_keyed_lookup_bucket_shuffle"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Query planner for sessions that are not built by `lakesoul-datafusion`.
///
/// A plain `SessionContext` installs DataFusion's `DefaultQueryPlanner`, whose
/// physical planner does not know the IVM extension node.  Wrapping it here
/// keeps `lookup_source_by_join_keys` usable in embedded runtimes and tests.
pub struct KeyedLookupQueryPlanner {
    inner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for KeyedLookupQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyedLookupQueryPlanner")
    }
}

impl Default for KeyedLookupQueryPlanner {
    fn default() -> Self {
        Self {
            inner: DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                IvmKeyedLookupExtensionPlanner,
            )]),
        }
    }
}

#[async_trait]
impl datafusion::execution::context::QueryPlanner for KeyedLookupQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(logical_plan, session).await
    }
}

/// Physical planner for [`IvmKeyedLookupNode`].
///
/// It plans only the lookup node: the contribution arrives as an already
/// planned physical child, so there is no recursive `create_physical_plan`
/// call and no nested distributed plan.
#[derive(Debug, Default)]
pub(crate) struct IvmKeyedLookupExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for IvmKeyedLookupExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "IVM keyed node needs exactly one physical input".to_string(),
            ));
        }
        if let Some(node) = node.as_any().downcast_ref::<IvmStateMergeNode>() {
            let spec = &node.spec;
            let input = Arc::clone(&physical_inputs[0]);
            let input =
                bucket_repartition(input, IVM_GROUP_ROW_COLUMN, spec.bucket_count)
                    .map_err(|error| DataFusionError::Execution(format!("{error:?}")))?;
            let lookup = IvmPkLookupExec::new_with_key(
                input,
                spec.state.clone(),
                spec.groups_by_bucket.clone(),
                PkLookupKey::BinaryRow {
                    column: IVM_GROUP_ROW_COLUMN.to_string(),
                },
                spec.bucket_count,
            )?
            // A group with no old state row is a new group: emit the
            // contribution with a null old-state side so the merge inserts it.
            .with_multi_match(false);
            return Ok(Some(Arc::new(IvmStateMergeExec::new(
                Arc::new(lookup),
                spec.state.schema.clone(),
                spec.group_keys.clone(),
                spec.measure_columns.clone(),
                spec.epoch,
            )?)));
        }
        let Some(node) = node.as_any().downcast_ref::<IvmKeyedLookupNode>() else {
            return Ok(None);
        };
        let spec = &node.spec;
        let input = Arc::clone(&physical_inputs[0]);
        let input = if let Some(columns) = spec.route_columns.as_deref() {
            bucket_repartition_by_columns(input, columns, spec.bucket_count)
                .map_err(|error| DataFusionError::Execution(format!("{error:?}")))?
        } else if let Some(key) = spec.bucket_key.as_deref() {
            bucket_repartition(input, key, spec.bucket_count)
                .map_err(|error| DataFusionError::Execution(format!("{error:?}")))?
        } else {
            input
        };
        let lookup = match spec.key.clone() {
            PkLookupKey::BinaryRow { column } => IvmPkLookupExec::new(
                input,
                spec.table.clone(),
                spec.groups_by_bucket.clone(),
                column,
                spec.bucket_count,
            )?,
            PkLookupKey::Columns {
                input_columns,
                table_columns,
                null_safe,
            } => IvmPkLookupExec::new_with_columns(
                input,
                spec.table.clone(),
                spec.groups_by_bucket.clone(),
                input_columns,
                table_columns,
                spec.bucket_count,
                null_safe,
            )?,
        };
        Ok(Some(Arc::new(
            lookup
                .with_multi_match(spec.multi_match)
                .with_profile(spec.profile_io.clone()),
        )))
    }
}

/// Compute the next state rows from the contribution and the old state
/// columns produced by [`IvmPkLookupExec`].
///
/// This is a streaming physical node: it has no driver-side collect and no
/// SQL string captured from the view.  The only dynamic values are the state
/// schema, the group-key/measure names and the epoch, all of which are plain
/// plan configuration.
pub(crate) struct IvmStateMergeExec {
    input: Arc<dyn ExecutionPlan>,
    state_schema: SchemaRef,
    group_keys: Vec<String>,
    measure_columns: Vec<String>,
    epoch: i64,
    properties: Arc<PlanProperties>,
}

#[cfg_attr(not(feature = "distributed"), allow(dead_code))]
impl IvmStateMergeExec {
    pub(crate) fn state_schema(&self) -> &SchemaRef {
        &self.state_schema
    }

    pub(crate) fn group_keys(&self) -> &[String] {
        &self.group_keys
    }

    pub(crate) fn measure_columns(&self) -> &[String] {
        &self.measure_columns
    }

    pub(crate) fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Number of output partitions; one per input bucket.
    pub(crate) fn output_partition_count(&self) -> usize {
        self.properties.partitioning.partition_count()
    }

    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        state_schema: SchemaRef,
        group_keys: Vec<String>,
        measure_columns: Vec<String>,
        epoch: i64,
    ) -> DataFusionResult<Self> {
        let partitions = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(state_schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            state_schema,
            group_keys,
            measure_columns,
            epoch,
            properties,
        })
    }
}

impl std::fmt::Debug for IvmStateMergeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmStateMergeExec")
    }
}

impl DisplayAs for IvmStateMergeExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "IvmStateMergeExec measures={}",
            self.measure_columns.join(",")
        )
    }
}

impl ExecutionPlanProperties for IvmStateMergeExec {
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

impl ExecutionPlan for IvmStateMergeExec {
    fn name(&self) -> &str {
        "IvmStateMergeExec"
    }

    fn schema(&self) -> SchemaRef {
        self.state_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "state merge expects one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.state_schema.clone(),
            self.group_keys.clone(),
            self.measure_columns.clone(),
            self.epoch,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        tracing::debug!(
            target: "lakesoul_ivm::keyed_plan",
            pid = std::process::id(),
            partition,
            "executing state merge"
        );
        let input = self.input.execute(partition, context)?;
        let state_schema = self.state_schema.clone();
        let group_keys = self.group_keys.clone();
        let measures = self.measure_columns.clone();
        let epoch = self.epoch;
        let output_schema = self.state_schema.clone();
        let stream = input.map(move |batch| {
            let batch = batch?;
            merge_state_batch(&batch, &state_schema, &group_keys, &measures, epoch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }
}

fn add_accumulator(
    batch: &RecordBatch,
    prefix: &str,
    measure: &str,
    data_type: &DataType,
    already_applied: &[bool],
) -> DataFusionResult<ArrayRef> {
    let old = batch
        .column_by_name(&format!("{prefix}{measure}"))
        .ok_or_else(|| {
            DataFusionError::Execution(format!("missing old state {measure}"))
        })?;
    let delta = batch
        .column_by_name(&format!("d_{measure}"))
        .ok_or_else(|| DataFusionError::Execution(format!("missing delta {measure}")))?;
    let old = cast(old.as_ref(), data_type)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let delta = cast(delta.as_ref(), data_type)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    match data_type {
        DataType::Int64 => {
            let old = old.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("old state is not bigint".to_string())
            })?;
            let delta = delta.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                DataFusionError::Execution("delta is not bigint".to_string())
            })?;
            Ok(Arc::new(Int64Array::from_iter_values((0..old.len()).map(
                |row| {
                    let value = if old.is_null(row) { 0 } else { old.value(row) };
                    if already_applied[row] {
                        // The window's delta is already in this state row.
                        return value;
                    }
                    let change = if delta.is_null(row) {
                        0
                    } else {
                        delta.value(row)
                    };
                    value.wrapping_add(change)
                },
            ))))
        }
        DataType::Decimal128(precision, scale) => {
            let old =
                old.as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("old state is not decimal".to_string())
                    })?;
            let delta = delta
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    DataFusionError::Execution("delta is not decimal".to_string())
                })?;
            let values = (0..old.len())
                .map(|row| {
                    let value = if old.is_null(row) { 0 } else { old.value(row) };
                    if already_applied[row] {
                        return value;
                    }
                    let change = if delta.is_null(row) {
                        0
                    } else {
                        delta.value(row)
                    };
                    value.wrapping_add(change)
                })
                .collect::<Vec<_>>();
            let array = Decimal128Array::from_iter_values(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            Ok(Arc::new(array))
        }
        other => Err(DataFusionError::Execution(format!(
            "unsupported state accumulator type {other}"
        ))),
    }
}

fn merge_state_batch(
    batch: &RecordBatch,
    state_schema: &SchemaRef,
    group_keys: &[String],
    measures: &[String],
    epoch: i64,
) -> DataFusionResult<RecordBatch> {
    let rows = batch.num_rows();
    let old_count = batch
        .column_by_name(&format!("{IVM_STATE_COLUMN_PREFIX}{IVM_ROW_COUNT_COLUMN}"))
        .ok_or_else(|| DataFusionError::Execution("missing old row count".to_string()))?;
    let delta_count = batch.column_by_name("d_rows").ok_or_else(|| {
        DataFusionError::Execution("missing delta row count".to_string())
    })?;
    let old_count = cast(old_count.as_ref(), &DataType::Int64)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let delta_count = cast(delta_count.as_ref(), &DataType::Int64)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let old_count = old_count
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("old row count is not bigint".to_string())
        })?;
    let delta_count = delta_count
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("delta row count is not bigint".to_string())
        })?;
    let old_epoch = batch
        .column_by_name(&format!("{IVM_STATE_COLUMN_PREFIX}{IVM_EPOCH_COLUMN}"))
        .ok_or_else(|| {
            DataFusionError::Execution("missing old state epoch".to_string())
        })?;
    let old_epoch = cast(old_epoch.as_ref(), &DataType::Int64)
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    let old_epoch = old_epoch
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Execution("old state epoch is not bigint".to_string())
        })?;
    // A state row written at this epoch (or a later one) already includes
    // the window's delta.  Replaying the same window must not add it again.
    let already_applied = (0..rows)
        .map(|row| old_epoch.is_valid(row) && old_epoch.value(row) >= epoch)
        .collect::<Vec<bool>>();
    let counts = (0..rows)
        .map(|row| {
            let old = if old_count.is_null(row) {
                0
            } else {
                old_count.value(row)
            };
            if already_applied[row] {
                return old;
            }
            let delta = if delta_count.is_null(row) {
                0
            } else {
                delta_count.value(row)
            };
            old.wrapping_add(delta)
        })
        .collect::<Vec<_>>();
    let kinds = StringArray::from_iter_values(
        counts
            .iter()
            .map(|count| if *count > 0 { "insert" } else { "delete" }),
    );
    let epochs = Int64Array::from(
        (0..rows)
            .map(|row| {
                if already_applied[row] && old_epoch.is_valid(row) {
                    old_epoch.value(row)
                } else {
                    epoch
                }
            })
            .collect::<Vec<_>>(),
    );
    let mut columns = Vec::with_capacity(state_schema.fields().len());
    for field in state_schema.fields() {
        let name = field.name();
        let array: ArrayRef =
            if name == IVM_GROUP_ROW_COLUMN || group_keys.iter().any(|key| key == name) {
                let column = batch.column_by_name(name).ok_or_else(|| {
                    DataFusionError::Execution(format!("missing group column {name}"))
                })?;
                cast(column.as_ref(), field.data_type())
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
            } else if name == IVM_ROW_COUNT_COLUMN {
                Arc::new(Int64Array::from(counts.clone()))
            } else if name == IVM_ROW_KINDS_COLUMN {
                Arc::new(kinds.clone())
            } else if name == IVM_EPOCH_COLUMN {
                Arc::new(epochs.clone())
            } else if measures.iter().any(|measure| measure == name) {
                add_accumulator(
                    batch,
                    IVM_STATE_COLUMN_PREFIX,
                    name,
                    field.data_type(),
                    &already_applied,
                )?
            } else {
                return Err(DataFusionError::Execution(format!(
                    "state merge does not know how to write {name}"
                )));
            };
        columns.push(array);
    }
    RecordBatch::try_new(state_schema.clone(), columns)
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, StringViewArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::SessionContext;

    #[test]
    fn splits_state_files_into_writer_buckets() {
        let groups = vec![PartitionFiles {
            partition_desc: "-5".to_string(),
            files: vec![
                "file:///tmp/part-a_0000.parquet".to_string(),
                "file:///tmp/part-b_0003.parquet".to_string(),
            ],
        }];
        let split = groups_by_bucket(groups, 4);
        assert_eq!(split.len(), 4);
        assert_eq!(split[0][0].files.len(), 1);
        assert_eq!(split[3][0].files.len(), 1);
        assert!(split[1].is_empty());
        assert!(split[2].is_empty());
    }

    #[test]
    fn maps_permuted_key_columns_to_table_order() {
        let input = ["order_item_id", "order_no", "src_sys"].map(str::to_string);
        let table = ["src_sys", "order_no", "order_item_id"].map(str::to_string);
        assert_eq!(mapped_input_columns(&input, &table), table);
    }

    #[test]
    fn keeps_an_alias_mapping_positional() {
        let input = ["left_key"].map(str::to_string);
        let table = ["right_key"].map(str::to_string);
        assert_eq!(mapped_input_columns(&input, &table), input);
    }

    #[test]
    fn lookup_key_casts_an_optimized_string_view() {
        let table_schema =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let contribution_schema = Arc::new(Schema::new(vec![Field::new(
            "k",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(
            contribution_schema,
            vec![Arc::new(StringViewArray::from(vec!["a"]))],
        )
        .unwrap();
        let key = PkLookupKey::Columns {
            input_columns: vec!["k".to_string()],
            table_columns: vec!["k".to_string()],
            null_safe: false,
        };
        let rows = key_rows(&batch, &key, &table_schema).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_some());
    }

    #[test]
    fn a_null_safe_key_encodes_the_null_group() {
        let table_schema =
            Arc::new(Schema::new(vec![Field::new("g", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            table_schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                Some("a"),
                None,
                Some("a"),
                None,
            ]))],
        )
        .unwrap();

        let strict = PkLookupKey::Columns {
            input_columns: vec!["g".to_string()],
            table_columns: vec!["g".to_string()],
            null_safe: false,
        };
        let rows = key_rows(&batch, &strict, &table_schema).unwrap();
        assert_eq!(rows.len(), 4);
        assert!(rows[0].is_some());
        assert!(rows[1].is_none());
        assert!(rows[3].is_none());

        let null_safe = PkLookupKey::Columns {
            input_columns: vec!["g".to_string()],
            table_columns: vec!["g".to_string()],
            null_safe: true,
        };
        let rows = key_rows(&batch, &null_safe, &table_schema).unwrap();
        assert!(rows.iter().all(Option::is_some));
        assert_eq!(rows[0], rows[2]);
        assert_eq!(rows[1], rows[3]);
        assert_ne!(rows[0], rows[1]);
    }

    #[test]
    fn adds_decimal_state_accumulators() {
        let data_type = DataType::Decimal128(10, 2);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                format!("{IVM_STATE_COLUMN_PREFIX}v"),
                data_type.clone(),
                true,
            ),
            Field::new("d_v", data_type.clone(), true),
        ]));
        let old = Decimal128Array::from(vec![Some(100), None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let delta = Decimal128Array::from(vec![Some(50), Some(25)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(old), Arc::new(delta)]).unwrap();
        let out = add_accumulator(
            &batch,
            IVM_STATE_COLUMN_PREFIX,
            "v",
            &data_type,
            &[false, false],
        )
        .unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 150);
        assert_eq!(out.value(1), 25);
    }

    #[test]
    fn merge_state_batch_keeps_an_already_applied_epoch() {
        let state_schema = Arc::new(Schema::new(vec![
            Field::new(IVM_GROUP_ROW_COLUMN, DataType::Binary, false),
            Field::new("k", DataType::Int64, false),
            Field::new("v_sum", DataType::Int64, true),
            Field::new(IVM_ROW_COUNT_COLUMN, DataType::Int64, false),
            Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
            Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
        ]));
        let input_schema = Arc::new(Schema::new(vec![
            Field::new(IVM_GROUP_ROW_COLUMN, DataType::Binary, false),
            Field::new("k", DataType::Int64, false),
            Field::new(
                format!("{IVM_STATE_COLUMN_PREFIX}{IVM_ROW_COUNT_COLUMN}"),
                DataType::Int64,
                true,
            ),
            Field::new(
                format!("{IVM_STATE_COLUMN_PREFIX}v_sum"),
                DataType::Int64,
                true,
            ),
            Field::new(
                format!("{IVM_STATE_COLUMN_PREFIX}{IVM_EPOCH_COLUMN}"),
                DataType::Int64,
                true,
            ),
            Field::new("d_rows", DataType::Int64, false),
            Field::new("d_v_sum", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(BinaryArray::from(vec![
                    Some(b"a".as_slice()),
                    Some(b"b".as_slice()),
                    Some(b"c".as_slice()),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![2, 3, 4])),
                Arc::new(Int64Array::from(vec![20, 25, 30])),
                // Row 0 is pre-window, row 1 was written by this epoch and
                // row 2 by a later epoch.
                Arc::new(Int64Array::from(vec![1, 3, 5])),
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![5, 5, 5])),
            ],
        )
        .unwrap();
        let out = merge_state_batch(
            &batch,
            &state_schema,
            &["k".to_string()],
            &["v_sum".to_string()],
            3,
        )
        .unwrap();
        let counts = out
            .column_by_name(IVM_ROW_COUNT_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let measures = out
            .column_by_name("v_sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let epochs = out
            .column_by_name(IVM_EPOCH_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(counts.values(), &[3, 3, 4]);
        assert_eq!(measures.values(), &[25, 25, 30]);
        assert_eq!(epochs.values(), &[3, 3, 5]);
    }

    #[test]
    fn composite_key_filters_use_all_key_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();
        let key = PkLookupKey::Columns {
            input_columns: vec!["a".to_string(), "b".to_string()],
            table_columns: vec!["a".to_string(), "b".to_string()],
            null_safe: false,
        };
        let PkFilters::Pushdown(filters) = key_filter_exprs(&batch, &key).unwrap() else {
            panic!("small composite key should use pushed-down filters");
        };
        assert_eq!(filters.len(), 2);
        assert!(
            filters
                .iter()
                .all(|filter| format!("{filter}").contains("IN"))
        );
    }

    #[test]
    fn large_composite_key_sets_scan_the_bucket_once() {
        let rows = PK_LOOKUP_IN_LIST_LIMIT + 1;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
                Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            ],
        )
        .unwrap();
        let key = PkLookupKey::Columns {
            input_columns: vec!["a".to_string(), "b".to_string()],
            table_columns: vec!["a".to_string(), "b".to_string()],
            null_safe: false,
        };
        assert!(matches!(
            key_filter_exprs(&batch, &key).unwrap(),
            PkFilters::ScanAll
        ));
    }

    #[test]
    fn matches_composite_pk_with_arrow_row() {
        let contribution_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("d", DataType::Int32, false),
        ]));
        let contribution = RecordBatch::try_new(
            contribution_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["x", "y", "x"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let state_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let state = RecordBatch::try_new(
            state_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let out_schema = state_output_schema(&contribution_schema, &state_schema);
        let output = lookup_batch(
            &contribution,
            &[state],
            &state_schema,
            &out_schema,
            &PkLookupKey::Columns {
                input_columns: vec!["a".into(), "b".into()],
                table_columns: vec!["a".into(), "b".into()],
                null_safe: false,
            },
            false,
            true,
        )
        .unwrap();
        assert_eq!(output.num_rows(), 3);
        let values = output
            .column_by_name(&format!("{IVM_STATE_COLUMN_PREFIX}v"))
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 100);
        assert_eq!(values.value(1), 200);
        assert!(values.is_null(2));
    }

    /// The execution partition a bucket lands in after the hash shuffle.
    fn bucket_partition(bucket: u32, partitions: usize) -> usize {
        let values: ArrayRef = Arc::new(UInt32Array::from(vec![bucket]));
        let mut hashes = [0u64];
        datafusion::common::hash_utils::create_hashes(
            [values.as_ref()],
            datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE
                .random_state(),
            &mut hashes,
        )
        .unwrap();
        (hashes[0] % partitions as u64) as usize
    }

    #[tokio::test]
    async fn routes_composite_keys_with_the_writer_hash() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "x", "z"])),
            ],
        )?;
        let context = SessionContext::new();
        let frame = context.read_batch(batch.clone())?;
        let input = frame.create_physical_plan().await?;
        let plan =
            bucket_repartition_by_columns(input, &["a".to_string(), "b".to_string()], 4)?;
        let task_context = context.task_ctx();
        let mut seen = 0;
        for partition in 0..plan.output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, task_context.clone())?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                for row in 0..batch.num_rows() {
                    let a = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let b = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let mut hashes = vec![0u32; 1];
                    let arrays: Vec<ArrayRef> = vec![
                        Arc::new(Int64Array::from(vec![a.value(row)])),
                        Arc::new(StringArray::from(vec![b.value(row)])),
                    ];
                    lakesoul_io::utils::hash::create_hashes(&arrays, &mut hashes)
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                    assert_eq!(partition, bucket_partition(hashes[0] % 4, 4));
                    seen += 1;
                }
            }
        }
        assert_eq!(seen, 4);
        Ok(())
    }

    #[tokio::test]
    async fn routes_each_key_to_its_writer_bucket() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )?;
        let context = SessionContext::new();
        let frame = context.read_batch(batch)?;
        let input = frame.create_physical_plan().await?;
        let plan = bucket_repartition(input, "k", 4)?;
        let task_context = context.task_ctx();
        let mut seen = 0;
        for partition in 0..plan.output_partitioning().partition_count() {
            let mut stream = plan.execute(partition, task_context.clone())?;
            use futures::StreamExt;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                seen += batch.num_rows();
                let key = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..key.len() {
                    let scalar = ScalarValue::Utf8(Some(key.value(row).to_string()));
                    let bucket = lakesoul_io::helpers::compute_scalar_hash(&scalar) % 4;
                    assert_eq!(partition, bucket_partition(bucket, 4));
                }
            }
        }
        assert_eq!(seen, 5);
        Ok(())
    }
}

#[cfg(test)]
mod bucket_ownership_tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn table() -> IvmTable {
        IvmTable {
            table_id: "state".to_string(),
            table_name: "state".to_string(),
            namespace: "default".to_string(),
            table_path: "file:///tmp/state".to_string(),
            schema: Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, true),
            ])),
            primary_keys: vec!["k".to_string()],
            range_partition_columns: Vec::new(),
            internal_columns: Vec::new(),
            bucket_columns: vec!["k".to_string()],
            hash_bucket_num: "4".to_string(),
            cdc_column: None,
            file_format: lakesoul_io::file_format::PhysicalFormat::Vortex,
            allow_fallback: true,
        }
    }

    fn lookup(bucket_count: usize) -> IvmPkLookupExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new(IVM_BUCKET_COLUMN, DataType::UInt32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
            ],
        )
        .unwrap();
        let context = SessionContext::new();
        let frame = context.read_batch(batch).unwrap();
        let input = futures::executor::block_on(frame.create_physical_plan()).unwrap();
        let input = bucket_repartition(input, "k", bucket_count).unwrap();
        let groups = (0..bucket_count)
            .map(|bucket| {
                vec![PartitionFiles {
                    partition_desc: "-5".to_string(),
                    files: vec![format!("bucket-{bucket}.vortex")],
                }]
            })
            .collect();
        IvmPkLookupExec::new_with_key(
            input,
            table(),
            groups,
            PkLookupKey::BinaryRow {
                column: "k".to_string(),
            },
            bucket_count,
        )
        .unwrap()
    }

    /// Every bucket is owned by exactly one `(task, partition)`.
    #[test]
    fn bucket_ownership_is_complete_and_disjoint() {
        for bucket_count in [1usize, 4, 16] {
            for task_count in [1usize, 2, 3, 8] {
                let lookup = lookup(bucket_count);
                let mut owners = vec![Vec::new(); bucket_count];
                for task_index in 0..task_count {
                    for partition in 0..bucket_count {
                        for bucket in lookup
                            .bucket_indices(
                                partition,
                                LookupTask {
                                    index: task_index,
                                    count: task_count,
                                },
                            )
                            .unwrap()
                        {
                            owners[bucket].push((task_index, partition));
                        }
                    }
                }
                for (bucket, owner) in owners.iter().enumerate() {
                    if bucket_count == 1 {
                        // Without a bucket shuffle every task reads all
                        // contribution partitions and must open every bucket.
                        assert_eq!(owner.len(), task_count);
                    } else {
                        assert_eq!(
                            owner.len(),
                            1,
                            "bucket {bucket} (P={bucket_count}, C={task_count}) has owners {owner:?}"
                        );
                    }
                }
            }
        }
    }

    /// The rows a shuffle slot delivers to `(task, partition)` all belong to
    /// the buckets that partition opens.
    #[test]
    fn shuffle_slots_contain_only_their_buckets() {
        for bucket_count in [4usize, 16] {
            for task_count in [1usize, 2, 3] {
                let lookup = lookup(bucket_count);
                let total = bucket_count * task_count;
                let values: ArrayRef = Arc::new(UInt32Array::from_iter_values(
                    (0..bucket_count as u32).collect::<Vec<_>>(),
                ));
                let mut hashes = vec![0u64; bucket_count];
                datafusion::common::hash_utils::create_hashes(
                    [values.as_ref()],
                    datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE
                        .random_state(),
                    &mut hashes,
                )
                .unwrap();
                for (bucket, &hash) in hashes.iter().enumerate() {
                    let slot = (hash % total as u64) as usize;
                    let task = LookupTask {
                        index: slot / bucket_count,
                        count: task_count,
                    };
                    let owned = lookup.bucket_indices(slot % bucket_count, task).unwrap();
                    assert!(
                        owned.contains(&bucket),
                        "bucket {bucket} missing from slot {slot}                          (P={bucket_count}, C={task_count}, owned={owned:?})"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn repartition_partition_matches_bucket_owner() -> DataFusionResult<()> {
        for bucket_count in [4usize, 8] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new(IVM_BUCKET_COLUMN, DataType::UInt32, true),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from_iter_values(0..bucket_count as i64)),
                    Arc::new(UInt32Array::from_iter_values(0..bucket_count as u32)),
                ],
            )?;
            let context = SessionContext::new();
            let input = context.read_batch(batch)?.create_physical_plan().await?;
            let plan = bucket_repartition(input, "k", bucket_count)
                .map_err(|error| DataFusionError::Execution(format!("{error:?}")))?;
            let lookup = lookup(bucket_count);
            for partition in 0..bucket_count {
                let mut stream = plan.execute(partition, context.task_ctx())?;
                let mut seen = Vec::new();
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    let index = batch.schema().index_of(IVM_BUCKET_COLUMN)?;
                    let buckets = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap();
                    seen.extend(
                        (0..buckets.len()).map(|row| buckets.value(row) as usize),
                    );
                }
                seen.sort_unstable();
                let owned = lookup
                    .bucket_indices(partition, LookupTask { index: 0, count: 1 })?;
                assert_eq!(seen, owned, "partition {partition} of {bucket_count}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn the_physical_optimizer_keeps_the_bucket_shuffle() -> DataFusionResult<()> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;
        let context = crate::runtime::install_keyed_lookup_planner(SessionContext::new());
        let contribution = context.read_batch(batch)?;
        let provider = IvmLookupTable::columns(
            contribution,
            table(),
            Vec::new(),
            vec!["k".to_string()],
            vec!["k".to_string()],
            8,
            Some(vec!["k".to_string()]),
            true,
        );
        let frame = provider.into_dataframe(&context).await?;
        let plan = frame.create_physical_plan().await?;
        fn find_lookup(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
            if plan.downcast_ref::<IvmPkLookupExec>().is_some() {
                return Some(Arc::clone(plan));
            }
            plan.children()
                .into_iter()
                .find_map(|child| find_lookup(child))
        }

        // `EnforceDistribution` may replace the inserted repartition with one
        // sized by `target_partitions`; either way the lookup must still see a
        // hash shuffle on `__ivm_bucket` and use the hash ownership.
        let lookup = find_lookup(&plan).expect("lookup node");
        let input = lookup.children()[0];
        assert_eq!(
            input.name(),
            "RepartitionExec",
            "lookup input was {}",
            input.name()
        );
        match input.output_partitioning() {
            Partitioning::Hash(expressions, count) => {
                assert!(*count > 1);
                assert_eq!(expressions.len(), 1);
            }
            other => panic!("lookup input is not hash partitioned: {other:?}"),
        }
        let lookup = lookup
            .downcast_ref::<IvmPkLookupExec>()
            .expect("lookup exec");
        assert_eq!(lookup.bucket_ownership(), BucketOwnership::Hash);
        assert!(lookup.shuffle_partitions > 1);
        assert_eq!(lookup.bucket_count, 8);
        Ok(())
    }

    #[test]
    fn a_single_task_opens_every_bucket_once_across_partitions() {
        let lookup = lookup(4);
        let task = LookupTask { index: 0, count: 1 };
        let mut seen = Vec::new();
        for partition in 0..4 {
            let buckets = lookup.bucket_indices(partition, task).unwrap();
            assert_eq!(
                lookup.groups_for_partition(partition, task).unwrap().len(),
                buckets.len()
            );
            seen.extend(buckets);
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }
}
