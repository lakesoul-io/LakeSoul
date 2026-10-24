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
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, Decimal128Array, Int64Array, StringArray, UInt32Array,
    UInt32Builder,
};
use arrow::compute::{SortOptions, cast, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion::common::{ScalarValue, tree_node::TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::physical_plan::{RangePartitioning, SplitPoint};
use datafusion::prelude::{col, lit};
use futures::StreamExt;
use tokio::sync::Semaphore;

use super::{
    IVM_EPOCH_COLUMN, IVM_GROUP_ROW_COLUMN, IVM_ROW_COUNT_COLUMN, IVM_ROW_KINDS_COLUMN,
};
use crate::error::Result;
use crate::table::{IvmTable, PartitionFiles, file_bucket};

/// Hidden column carrying the LakeSoul bucket id during a shuffle.
pub(crate) const IVM_BUCKET_COLUMN: &str = "__ivm_bucket";

#[derive(Debug)]
struct LakeSoulBucketExpr {
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
struct LakeSoulColumnsBucketExpr {
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

/// Add a LakeSoul bucket-id column, range-repartition into exactly one
/// execution partition per bucket, then remove the helper column.
pub(crate) fn bucket_repartition(
    input: Arc<dyn ExecutionPlan>,
    key_name: &str,
    bucket_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if bucket_count < 2 {
        return Ok(input);
    }
    let input_schema = input.schema();
    let key_index = input_schema.index_of(key_name)?;
    let key = Arc::new(PhysicalColumn::new(key_name, key_index)) as Arc<dyn PhysicalExpr>;
    let bucket = Arc::new(LakeSoulBucketExpr {
        key,
        bucket_count: bucket_count as u32,
    }) as Arc<dyn PhysicalExpr>;

    let mut projected = Vec::with_capacity(input_schema.fields().len() + 1);
    for (index, field) in input_schema.fields().iter().enumerate() {
        projected.push((
            Arc::new(PhysicalColumn::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));
    }
    projected.push((bucket, IVM_BUCKET_COLUMN.to_string()));
    let with_bucket = Arc::new(ProjectionExec::try_new(projected, input)?);

    let bucket_index = with_bucket.schema().index_of(IVM_BUCKET_COLUMN)?;
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(PhysicalColumn::new(IVM_BUCKET_COLUMN, bucket_index)),
        SortOptions::default(),
    )])
    .ok_or_else(|| DataFusionError::Internal("empty bucket ordering".to_string()))?;
    let split_points = (1..bucket_count)
        .map(|bucket| SplitPoint::new(vec![ScalarValue::UInt32(Some(bucket as u32))]))
        .collect();
    let range = RangePartitioning::try_new(ordering, split_points)?;
    let repartitioned = Arc::new(RepartitionExec::try_new(
        with_bucket,
        Partitioning::Range(range),
    )?);

    let mut out = Vec::with_capacity(input_schema.fields().len());
    for (index, field) in input_schema.fields().iter().enumerate() {
        out.push((
            Arc::new(PhysicalColumn::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));
    }
    Ok(Arc::new(ProjectionExec::try_new(out, repartitioned)?))
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
    let input_schema = input.schema();
    let keys = key_columns
        .iter()
        .map(|column| {
            let index = input_schema.index_of(column)?;
            Ok(Arc::new(PhysicalColumn::new(column, index)) as Arc<dyn PhysicalExpr>)
        })
        .collect::<Result<Vec<_>>>()?;
    let bucket = Arc::new(LakeSoulColumnsBucketExpr {
        keys,
        bucket_count: bucket_count as u32,
    }) as Arc<dyn PhysicalExpr>;
    bucket_repartition_with_expr(input, bucket, bucket_count)
}

fn bucket_repartition_with_expr(
    input: Arc<dyn ExecutionPlan>,
    bucket: Arc<dyn PhysicalExpr>,
    bucket_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if bucket_count < 2 {
        return Ok(input);
    }
    let input_schema = input.schema();
    let mut projected = Vec::with_capacity(input_schema.fields().len() + 1);
    for (index, field) in input_schema.fields().iter().enumerate() {
        projected.push((
            Arc::new(PhysicalColumn::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));
    }
    projected.push((bucket, IVM_BUCKET_COLUMN.to_string()));
    let with_bucket = Arc::new(ProjectionExec::try_new(projected, input)?);

    let bucket_index = with_bucket.schema().index_of(IVM_BUCKET_COLUMN)?;
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(PhysicalColumn::new(IVM_BUCKET_COLUMN, bucket_index)),
        SortOptions::default(),
    )])
    .ok_or_else(|| DataFusionError::Internal("empty bucket ordering".to_string()))?;
    let split_points = (1..bucket_count)
        .map(|bucket| SplitPoint::new(vec![ScalarValue::UInt32(Some(bucket as u32))]))
        .collect();
    let range = RangePartitioning::try_new(ordering, split_points)?;
    let repartitioned = Arc::new(RepartitionExec::try_new(
        with_bucket,
        Partitioning::Range(range),
    )?);

    let mut out = Vec::with_capacity(input_schema.fields().len());
    for (index, field) in input_schema.fields().iter().enumerate() {
        out.push((
            Arc::new(PhysicalColumn::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        ));
    }
    Ok(Arc::new(ProjectionExec::try_new(out, repartitioned)?))
}

// ---------------------------------------------------------------------------
// Keyed state lookup and merge plans
// ---------------------------------------------------------------------------

/// Prefix of the state columns carried through the lookup plan.  State and
/// contribution schemas both contain group-key and measure names, so the
/// lookup output keeps the old state columns under a distinct prefix.
pub(crate) const IVM_STATE_COLUMN_PREFIX: &str = "__ivm_state_";

/// Number of state reads that may execute concurrently in one plan.  The
/// physical plan is partitioned by hash bucket, which can be more than the
/// four active reader/state tasks the pilot is allowed to run.
/// Maximum number of contribution keys in one PK lookup read.  Chunking keeps
/// the reader filter/index request bounded instead of materializing one huge
/// IN expression.
pub(crate) const PK_LOOKUP_CHUNK_ROWS: usize = 4096;

pub(crate) fn max_active_tasks() -> usize {
    std::env::var("LAKESOUL_IVM_MAX_ACTIVE_TASKS")
        .or_else(|_| std::env::var("LAKESOUL_IVM_MAX_ACTIVE_LOOKUPS"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4)
        .max(1)
}

async fn read_lookup_chunk(
    table: IvmTable,
    groups: Vec<PartitionFiles>,
    filters: Vec<datafusion::prelude::Expr>,
    gate: Arc<Semaphore>,
) -> DataFusionResult<Vec<RecordBatch>> {
    if groups.is_empty() || filters.is_empty() {
        return Ok(Vec::new());
    }
    let permit = gate.acquire_owned().await.map_err(|error| {
        DataFusionError::Execution(format!("state lookup gate closed: {error}"))
    })?;
    let joined = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        lakesoul_io::session::GLOBAL_RUNTIME
            .block_on(table.read_partition_files_filtered(groups, filters))
    })
    .await
    .map_err(|error| {
        DataFusionError::Execution(format!("state lookup task failed: {error}"))
    })?;
    joined.map_err(|error| DataFusionError::Execution(error.to_string()))
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PkLookupKey {
    BinaryRow {
        column: String,
    },
    Columns {
        input_columns: Vec<String>,
        table_columns: Vec<String>,
    },
}

impl PkLookupKey {
    /// The same key expressed on the table side.  Contribution and table
    /// columns can have different names for a differently-named join key.
    fn for_table(&self) -> Self {
        match self {
            Self::BinaryRow { column } => Self::BinaryRow {
                column: column.clone(),
            },
            Self::Columns { table_columns, .. } => Self::Columns {
                input_columns: table_columns.clone(),
                table_columns: table_columns.clone(),
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

fn key_filter_exprs(
    batch: &RecordBatch,
    key: &PkLookupKey,
) -> DataFusionResult<Vec<datafusion::prelude::Expr>> {
    match key {
        PkLookupKey::BinaryRow { column } => state_key_filter(batch, column),
        PkLookupKey::Columns {
            input_columns,
            table_columns,
        } => {
            let mut rows = Vec::new();
            for row in 0..batch.num_rows() {
                let mut values = Vec::with_capacity(input_columns.len());
                let mut null = false;
                for column in input_columns {
                    let index = batch.schema().index_of(column)?;
                    if batch.column(index).is_null(row) {
                        null = true;
                        break;
                    }
                    values.push(ScalarValue::try_from_array(
                        batch.column(index).as_ref(),
                        row,
                    )?);
                }
                if !null {
                    rows.push(values);
                }
            }
            if rows.is_empty() {
                return Ok(Vec::new());
            }
            if table_columns.len() == 1 {
                let list = rows
                    .into_iter()
                    .map(|mut values| lit(values.remove(0)))
                    .collect();
                return Ok(vec![col(table_columns[0].as_str()).in_list(list, false)]);
            }
            let mut filter: Option<datafusion::prelude::Expr> = None;
            for values in rows {
                let mut conjunction =
                    col(table_columns[0].as_str()).eq(lit(values[0].clone()));
                for (column, value) in table_columns.iter().zip(values).skip(1) {
                    conjunction = conjunction.and(col(column.as_str()).eq(lit(value)));
                }
                filter = Some(match filter {
                    Some(existing) => existing.or(conjunction),
                    None => conjunction,
                });
            }
            Ok(filter.into_iter().collect())
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
        } => {
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
                .map(|column| {
                    batch.column_by_name(column).cloned().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "lookup key {column} is missing from the contribution"
                        ))
                    })
                })
                .collect::<DataFusionResult<Vec<ArrayRef>>>()?;
            let nulls = (0..batch.num_rows())
                .map(|row| arrays.iter().any(|array| array.is_null(row)))
                .collect::<Vec<_>>();
            let rows = converter
                .convert_columns(&arrays)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            Ok((0..batch.num_rows())
                .map(|row| {
                    if nulls[row] {
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

/// A physical scan of one bucket's state files, left-joined onto the
/// contribution rows by group-row primary key.
///
/// The plan is intentionally made of plain table/file descriptors: the
/// execution-time concurrency gate is process-local and is not part of the
/// serializable descriptor.  One execution partition corresponds to one
/// LakeSoul bucket after [`bucket_repartition`].
pub(crate) struct IvmPkLookupExec {
    input: Arc<dyn ExecutionPlan>,
    table: IvmTable,
    groups_by_bucket: Arc<Vec<Vec<PartitionFiles>>>,
    key: PkLookupKey,
    bucket_count: usize,
    schema: SchemaRef,
    gate: Arc<Semaphore>,
    multi_match: bool,
    emit_unmatched: bool,
    properties: Arc<PlanProperties>,
}

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
    ) -> DataFusionResult<Self> {
        Self::new_with_key(
            input,
            table,
            groups_by_bucket,
            PkLookupKey::Columns {
                input_columns,
                table_columns,
            },
            bucket_count,
        )
    }

    fn new_with_key(
        input: Arc<dyn ExecutionPlan>,
        table: IvmTable,
        groups_by_bucket: Vec<Vec<PartitionFiles>>,
        key: PkLookupKey,
        bucket_count: usize,
    ) -> DataFusionResult<Self> {
        let bucket_count = bucket_count.max(1);
        let input_partitions = input.output_partitioning().partition_count();
        if bucket_count > 1 && input_partitions != bucket_count {
            return Err(DataFusionError::Internal(format!(
                "PK lookup needs {bucket_count} input partitions, got {input_partitions}"
            )));
        }
        // An unbucketed table is read by every contribution partition; its
        // one group is shared rather than duplicated.
        let output_partitions = if bucket_count == 1 {
            input_partitions
        } else {
            bucket_count
        };
        let schema = state_output_schema(&input.schema(), &table.schema);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(output_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            table,
            groups_by_bucket: Arc::new(groups_by_bucket),
            key,
            bucket_count,
            schema,
            gate: Arc::new(Semaphore::new(max_active_tasks())),
            multi_match: false,
            emit_unmatched: false,
            properties,
        })
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "keyed state lookup expects one child".to_string(),
            ));
        }
        let mut plan = Self::new_with_key(
            children[0].clone(),
            self.table.clone(),
            self.groups_by_bucket.as_ref().clone(),
            self.key.clone(),
            self.bucket_count,
        )?;
        plan.gate = self.gate.clone();
        plan.multi_match = self.multi_match;
        plan.emit_unmatched = self.emit_unmatched;
        Ok(Arc::new(plan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let groups = if self.bucket_count == 1 {
            self.groups_by_bucket.first().cloned().unwrap_or_default()
        } else {
            self.groups_by_bucket
                .get(partition)
                .cloned()
                .unwrap_or_default()
        };
        let table = self.table.clone();
        let key = self.key.clone();
        let state_schema = self.table.schema.clone();
        let out_schema = self.schema.clone();
        let gate = self.gate.clone();
        let multi_match = self.multi_match;
        let emit_unmatched = self.emit_unmatched;
        let stream =
            futures::stream::try_unfold((input, groups), move |(mut input, groups)| {
                let table = table.clone();
                let key = key.clone();
                let state_schema = state_schema.clone();
                let out_schema = out_schema.clone();
                let gate = gate.clone();
                async move {
                    loop {
                        let batch = match input.next().await {
                            None => return Ok::<_, DataFusionError>(None),
                            Some(Err(error)) => return Err(error),
                            Some(Ok(batch)) if batch.num_rows() == 0 => continue,
                            Some(Ok(batch)) => batch,
                        };
                        let started = std::time::Instant::now();
                        let mut outputs = Vec::new();
                        let mut state_rows = 0usize;
                        for offset in (0..batch.num_rows()).step_by(PK_LOOKUP_CHUNK_ROWS)
                        {
                            let length =
                                PK_LOOKUP_CHUNK_ROWS.min(batch.num_rows() - offset);
                            let chunk = batch.slice(offset, length);
                            let filters = key_filter_exprs(&chunk, &key)?;
                            let state_batches = read_lookup_chunk(
                                table.clone(),
                                groups.clone(),
                                filters,
                                gate.clone(),
                            )
                            .await?;
                            state_rows += state_batches
                                .iter()
                                .map(RecordBatch::num_rows)
                                .sum::<usize>();
                            outputs.push(lookup_batch(
                                &chunk,
                                &state_batches,
                                &state_schema,
                                &out_schema,
                                &key,
                                multi_match,
                                emit_unmatched,
                            )?);
                        }
                        let output = match outputs.len() {
                            0 => RecordBatch::new_empty(out_schema.clone()),
                            1 => outputs.pop().expect("one lookup output"),
                            _ => arrow::compute::concat_batches(&out_schema, &outputs)?,
                        };
                        tracing::debug!(
                            partition,
                            contribution_rows = batch.num_rows(),
                            state_rows,
                            elapsed = ?started.elapsed(),
                            "keyed state lookup batch finished"
                        );
                        return Ok(Some((output, (input, groups))));
                    }
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
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

impl IvmStateMergeExec {
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
    let counts = (0..rows)
        .map(|row| {
            let old = if old_count.is_null(row) {
                0
            } else {
                old_count.value(row)
            };
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
    let epochs = Int64Array::from_iter_values(std::iter::repeat_n(epoch, rows));
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
                add_accumulator(batch, IVM_STATE_COLUMN_PREFIX, name, field.data_type())?
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
    use arrow::array::{Int32Array, StringArray};
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
        let out =
            add_accumulator(&batch, IVM_STATE_COLUMN_PREFIX, "v", &data_type).unwrap();
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), 150);
        assert_eq!(out.value(1), 25);
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
                    assert_eq!(partition, (hashes[0] % 4) as usize);
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
                    let expected = lakesoul_io::helpers::compute_scalar_hash(&scalar) % 4;
                    assert_eq!(partition, expected as usize);
                }
            }
        }
        assert_eq!(seen, 5);
        Ok(())
    }
}
