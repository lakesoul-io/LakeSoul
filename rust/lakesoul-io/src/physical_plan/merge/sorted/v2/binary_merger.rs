// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::Result;
use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::v2::batch_range::{
    BatchRange, InProgressPkGroup, InProgressRow,
};
use crate::physical_plan::merge::sorted::v2::record_batch_builder::{
    ColumnMapping, build_record_batch_from_pk_groups,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use smallvec::smallvec;
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct BinaryMerger<'a, C: CursorValues> {
    ranges: Vec<&'a mut BatchRange<C>>,
    pk_groups: Vec<InProgressPkGroup>,
    schema: SchemaRef,
    target_batch_size: usize,
    remaining_rows_threshold: usize,
    column_mapping: Arc<ColumnMapping>,
}

impl<'a, C: CursorValues> BinaryMerger<'a, C> {
    pub fn new(
        ranges: Vec<&'a mut BatchRange<C>>,
        target_batch_size: usize,
        schema: SchemaRef,
        column_mapping: Arc<ColumnMapping>,
    ) -> Self {
        assert_eq!(ranges.len(), 2, "BinaryMerger expects exactly two ranges");

        let remaining_rows_threshold = 32;
        BinaryMerger {
            ranges,
            pk_groups: Vec::with_capacity(target_batch_size + 1),
            target_batch_size,
            schema,
            remaining_rows_threshold,
            column_mapping,
        }
    }

    pub async fn merge(&mut self, tx: &Sender<Result<RecordBatch>>) -> Result<()> {
        let mut left_valid = self.ranges[0].has_more_rows();
        let mut right_valid = self.ranges[1].has_more_rows();

        while left_valid && right_valid {
            let cmp = C::compare(
                self.ranges[0].cursor(),
                self.ranges[0].begin_row(),
                self.ranges[1].cursor(),
                self.ranges[1].begin_row(),
            );
            match cmp {
                Ordering::Less => {
                    self.add_row_to_pk_group(0);
                    left_valid = self.advance(0);
                }
                Ordering::Equal => {
                    self.add_row_to_pk_group(0);
                    let right_row = InProgressRow {
                        range_idx: 1,
                        row_idx: self.ranges[1].begin_row(),
                    };
                    self.pk_groups.last_mut().unwrap().push(right_row);
                    left_valid = self.advance(0);
                    right_valid = self.advance(1);
                }
                Ordering::Greater => {
                    self.add_row_to_pk_group(1);
                    right_valid = self.advance(1);
                }
            }
            if self.pk_groups.len() >= self.target_batch_size {
                self.flush_pk_groups(tx).await?;
            }
        }

        if !left_valid && !right_valid {
            self.flush_pk_groups(tx).await?;
            return Ok(());
        }

        if left_valid {
            self.process_remaining_rows(0, tx).await?;
        }

        if right_valid {
            self.process_remaining_rows(1, tx).await?;
        }

        Ok(())
    }

    fn add_row_to_pk_group(&mut self, range_idx: usize) {
        let row = InProgressRow {
            range_idx,
            row_idx: self.ranges[range_idx].begin_row(),
        };

        let is_same_pk = self.pk_groups.last().map_or(false, |group| {
            let first = &group[0];
            C::eq(
                self.ranges[first.range_idx].cursor(),
                first.row_idx,
                self.ranges[row.range_idx].cursor(),
                row.row_idx,
            )
        });

        if is_same_pk {
            self.pk_groups.last_mut().unwrap().push(row);
        } else {
            self.pk_groups.push(smallvec![row]);
        }
    }

    fn advance(&mut self, idx: usize) -> bool {
        self.ranges[idx].advance();
        self.ranges[idx].has_more_rows()
    }

    async fn process_remaining_rows(
        &mut self,
        range_idx: usize,
        tx: &Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let remaining = self.ranges[range_idx].remaining_rows();
        if remaining <= self.remaining_rows_threshold && !self.pk_groups.is_empty() {
            let remaining_end = self.ranges[range_idx].end_row_for_merge;
            for _i in self.ranges[range_idx].begin_row..=remaining_end {
                self.add_row_to_pk_group(range_idx);
                self.ranges[range_idx].advance();
            }
            self.flush_pk_groups(tx).await?;
        } else {
            self.flush_pk_groups(tx).await?;

            let range = &mut self.ranges[range_idx];
            let batch = range.slice_remaining_and_advance();
            tx.send(Ok(batch)).await?;
        }
        Ok(())
    }

    async fn flush_pk_groups(&mut self, tx: &Sender<Result<RecordBatch>>) -> Result<()> {
        if self.pk_groups.is_empty() {
            return Ok(());
        }

        let ranges_ref: Vec<&BatchRange<C>> = self.ranges.iter().map(|r| &**r).collect();
        let batch = build_record_batch_from_pk_groups(
            &self.pk_groups,
            &ranges_ref,
            &self.schema,
            &self.column_mapping,
        )?;
        self.pk_groups.clear();
        tx.send(Ok(batch)).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::merge::sorted::cursor::RowValues;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::compute::SortOptions;
    use arrow::row::{RowConverter, SortField};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn create_batch(values: &[i32]) -> RecordBatch {
        let arr: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        RecordBatch::try_from_iter(vec![("a", arr)]).unwrap()
    }

    fn create_range(values: &[i32], stream_idx: usize) -> BatchRange<RowValues> {
        let batch = create_batch(values);
        if batch.num_rows() == 0 {
            panic!("create_range does not support empty batch in this test helper");
        }

        let converter = Arc::new(
            RowConverter::new(vec![SortField::new_with_options(
                batch.schema().field(0).data_type().clone(),
                SortOptions::default(),
            )])
            .unwrap(),
        );
        let cols = vec![batch.column(0).clone()];
        let rows = converter.convert_columns(&cols).unwrap();

        let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024)) as _;
        let consumer = MemoryConsumer::new("batch_range_test").register(&pool);
        let reservation = consumer.new_empty();
        reservation.try_grow(rows.size()).unwrap();
        let cursor = RowValues::new(rows, converter, reservation);
        let end_row_for_merge = if batch.num_rows() == 0 {
            0
        } else {
            batch.num_rows() - 1
        };
        BatchRange::new(cursor, batch, stream_idx, 0, end_row_for_merge)
    }

    async fn collect_merge_results(
        mut left: BatchRange<RowValues>,
        mut right: BatchRange<RowValues>,
        target_batch_size: usize,
    ) -> Vec<RecordBatch> {
        let schema = left.batch().schema();
        let (tx, mut rx) = mpsc::channel(10);

        let handle = tokio::spawn(async move {
            let cm = Arc::new(ColumnMapping::from_fields_map(&[vec![0]], 1));
            let mut merger = BinaryMerger::new(
                vec![&mut left, &mut right],
                target_batch_size,
                schema,
                cm,
            );
            merger.merge(&tx).await.unwrap();
        });

        let mut results = Vec::new();
        while let Some(result) = rx.recv().await {
            results.push(result.unwrap());
        }
        handle.await.unwrap();
        results
    }

    #[tokio::test]
    async fn test_merge_basic() {
        let left = create_range(&[1, 3, 5], 0);
        let right = create_range(&[2, 4, 6], 1);
        let target_batch_size = 10;

        let results = collect_merge_results(left, right, target_batch_size).await;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values().as_ref(), &[1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn test_merge_with_batch_size() {
        let left = create_range(&[1, 3, 5, 7, 9], 0);
        let right = create_range(&[2, 4, 6, 8, 10], 1);
        let target_batch_size = 6;

        let results = collect_merge_results(left, right, target_batch_size).await;

        assert_eq!(results.len(), 2);

        let batch1 = &results[0];
        assert_eq!(batch1.num_rows(), 6);
        let col1 = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col1.values(), &[1, 2, 3, 4, 5, 6]);

        let batch2 = &results[1];
        assert_eq!(batch2.num_rows(), 4);
        let col2 = batch2
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col2.values(), &[7, 8, 9, 10]);
    }

    #[tokio::test]
    async fn test_merge_with_empty_left() {
        let mut left = create_range(&[1], 0);
        left.advance();
        let right = create_range(&[2, 4, 6], 1);
        let target_batch_size = 10;

        let results = collect_merge_results(left, right, target_batch_size).await;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values(), &[2, 4, 6]);
    }

    #[tokio::test]
    async fn test_merge_with_empty_right() {
        let left = create_range(&[1, 3, 5], 0);
        let mut right = create_range(&[2], 1);
        right.advance();
        let target_batch_size = 10;

        let results = collect_merge_results(left, right, target_batch_size).await;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values(), &[1, 3, 5]);
    }

    #[tokio::test]
    async fn test_merge_with_equal_values() {
        let left = create_range(&[1, 2, 3], 0);
        let right = create_range(&[2, 3, 4], 1);
        let target_batch_size = 10;

        let results = collect_merge_results(left, right, target_batch_size).await;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values(), &[1, 2, 3, 4]);
    }
}
