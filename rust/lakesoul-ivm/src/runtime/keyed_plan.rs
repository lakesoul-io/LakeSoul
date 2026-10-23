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
//! The first consumer is being wired in follow-up changes; keep the plan
//! helpers compiled and unit-tested until then.
#![allow(dead_code)]

use std::fmt::{self, Display};
use std::sync::Arc;

use arrow::array::{Array, UInt32Builder};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{RangePartitioning, SplitPoint};

use crate::error::Result;

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
                let bucket =
                    lakesoul_io::helpers::compute_scalar_hash(&scalar) % self.bucket_count;
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
        .map(|bucket| {
            SplitPoint::new(vec![ScalarValue::UInt32(Some(bucket as u32))])
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::SessionContext;

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
                let key = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                for row in 0..key.len() {
                    let scalar = ScalarValue::Utf8(Some(key.value(row).to_string()));
                    let expected =
                        lakesoul_io::helpers::compute_scalar_hash(&scalar) % 4;
                    assert_eq!(partition, expected as usize);
                }
            }
        }
        assert_eq!(seen, 5);
        Ok(())
    }
}
