// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::Result;
use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::v2::batch_range::{BatchRange, InProgressRow};
use arrow::compute::interleave;
use arrow::record_batch::RecordBatch;
use arrow_array::ArrayRef;
use arrow_schema::SchemaRef;
use std::cmp::Ordering;
use tokio::sync::mpsc::Sender;

pub struct BinaryMerger<'a, C: CursorValues> {
    ranges: Vec<&'a mut BatchRange<C>>,
    in_progress_row: Vec<InProgressRow>,
    schema: SchemaRef,
    target_batch_size: usize,
    remaining_rows_threshold: usize,
}

impl<'a, C: CursorValues> BinaryMerger<'a, C> {
    pub fn new(
        ranges: Vec<&'a mut BatchRange<C>>,
        target_batch_size: usize,
        schema: SchemaRef,
    ) -> Self {
        // 确保只有两个 ranges
        assert_eq!(ranges.len(), 2, "BinaryMerger expects exactly two ranges");

        let remaining_rows_threshold = 32;
        BinaryMerger {
            ranges,
            in_progress_row: Vec::with_capacity(target_batch_size + 1),
            target_batch_size,
            schema,
            remaining_rows_threshold,
        }
    }

    pub async fn merge(
        &mut self,
        tx: Sender<Result<RecordBatch>>,
    ) -> Result<Sender<Result<RecordBatch>>> {
        let mut left_valid = self.ranges[0].has_more_rows();
        let mut right_valid = self.ranges[1].has_more_rows();

        while left_valid && right_valid {
            // we cannot use left.cmp(right) because it will compare the stream idx
            // instead, we need to compare the cursor values at the current positions
            let cmp = C::compare(
                self.ranges[0].cursor(),
                self.ranges[0].begin_row(),
                self.ranges[1].cursor(),
                self.ranges[1].begin_row(),
            );
            match cmp {
                Ordering::Less => {
                    self.in_progress_row.push(InProgressRow {
                        range_idx: 0,
                        row_idx: self.ranges[0].begin_row(),
                    });
                    left_valid = self.advance(0);
                }
                Ordering::Equal => {
                    self.in_progress_row.push(InProgressRow {
                        range_idx: 1,
                        row_idx: self.ranges[1].begin_row(),
                    });
                    left_valid = self.advance(0);
                    right_valid = self.advance(1);
                }
                Ordering::Greater => {
                    self.in_progress_row.push(InProgressRow {
                        range_idx: 1,
                        row_idx: self.ranges[1].begin_row(),
                    });
                    right_valid = self.advance(1);
                }
            }
            if self.in_progress_row.len() >= self.target_batch_size {
                self.build_record_batch(&tx).await?;
            }
        }

        if !left_valid && !right_valid {
            self.build_record_batch(&tx).await?;
            return Ok(tx);
        }

        if left_valid {
            self.process_remaining_rows(0, &tx).await?;
        }

        if right_valid {
            self.process_remaining_rows(1, &tx).await?;
        }

        Ok(tx)
    }

    fn advance(&mut self, idx: usize) -> bool {
        self.ranges[idx].advance();
        self.ranges[idx].has_more_rows()
    }

    /// 处理剩余行的逻辑
    async fn process_remaining_rows(
        &mut self,
        range_idx: usize,
        tx: &Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        let remaining = self.ranges[range_idx].remaining_rows();
        if remaining <= self.remaining_rows_threshold && !self.in_progress_row.is_empty()
        {
            let range = &mut self.ranges[range_idx];
            // 剩余行数低于阈值且有未处理的行，将剩余行添加到 in_progress_row
            for i in range.begin_row..=range.end_row_for_merge {
                self.in_progress_row.push(InProgressRow {
                    range_idx,
                    row_idx: i,
                });
                self.advance(range_idx); // 移动到末尾
            }
            self.build_record_batch(&tx).await?;
        } else {
            self.build_record_batch(&tx).await?;

            let range = &mut self.ranges[range_idx];
            // 剩余行数高于阈值，直接 slice 输出
            let batch = range.slice_remaining_and_advance();
            tx.send(Ok(batch)).await?;
        }
        Ok(())
    }

    async fn build_record_batch(
        &mut self,
        tx: &Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        if self.in_progress_row.is_empty() {
            return Ok(());
        }
        let mut indices = Vec::with_capacity(self.in_progress_row.len());
        for row in self.in_progress_row.iter() {
            indices.push((row.range_idx, row.row_idx));
        }
        let column_num = self.ranges[0].batch().num_columns();
        let result = (0..column_num)
            .into_iter()
            .map(|i| {
                let columns = [
                    self.ranges[0].batch().column(i).as_ref(),
                    self.ranges[1].batch().column(i).as_ref(),
                ];
                let col = interleave(columns.as_slice(), indices.as_slice());
                col
            })
            .collect::<arrow::error::Result<Vec<ArrayRef>>>()?;
        self.in_progress_row.clear();
        tx.send(Ok(RecordBatch::try_new(self.schema.clone(), result)?))
            .await?;
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
            // RowValues::new 要求至少 1 行；空批次在当前测试中不需要构造 cursor
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
        let mut reservation = consumer.new_empty();
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
        left: &mut BatchRange<RowValues>,
        right: &mut BatchRange<RowValues>,
        target_batch_size: usize,
    ) -> Vec<RecordBatch> {
        let schema = left.batch().schema();
        let (tx, mut rx) = mpsc::channel(10);

        let mut merger = BinaryMerger::new(vec![left, right], target_batch_size, schema);
        merger.merge(tx).await.unwrap();

        let mut results = Vec::new();
        while let Some(result) = rx.recv().await {
            results.push(result.unwrap());
        }
        results
    }

    #[tokio::test]
    async fn test_merge_basic() {
        let mut left = create_range(&[1, 3, 5], 0);
        let mut right = create_range(&[2, 4, 6], 1);
        let target_batch_size = 10;

        let results =
            collect_merge_results(&mut left, &mut right, target_batch_size).await;

        // 验证结果
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
        let mut left = create_range(&[1, 3, 5, 7, 9], 0);
        let mut right = create_range(&[2, 4, 6, 8, 10], 1);
        let target_batch_size = 6;

        let results =
            collect_merge_results(&mut left, &mut right, target_batch_size).await;

        // 验证结果被分成多个批次
        assert_eq!(results.len(), 2);

        // 第一个批次应该有 6 行
        let batch1 = &results[0];
        assert_eq!(batch1.num_rows(), 6);
        let col1 = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col1.values(), &[1, 2, 3, 4, 5, 6]);

        // 第二个批次应该有 4 行
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
        left.advance(); // 移动到末尾，使其没有更多行
        let mut right = create_range(&[2, 4, 6], 1);
        let target_batch_size = 10;

        let results =
            collect_merge_results(&mut left, &mut right, target_batch_size).await;
        println!("{:?}", results);

        // 验证结果只包含右侧的数据
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
        let mut left = create_range(&[1, 3, 5], 0);
        let mut right = create_range(&[2], 1);
        right.advance(); // 移动到末尾，使其没有更多行
        let target_batch_size = 10;

        let results =
            collect_merge_results(&mut left, &mut right, target_batch_size).await;

        // 验证结果只包含左侧的数据
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
        let mut left = create_range(&[1, 2, 3], 0);
        let mut right = create_range(&[2, 3, 4], 1);
        let target_batch_size = 10;

        let results =
            collect_merge_results(&mut left, &mut right, target_batch_size).await;

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
