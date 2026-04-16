// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::physical_plan::merge::sorted::cursor::CursorValues;
use arrow_array::RecordBatch;
use std::cmp::Ordering;

pub struct BatchRange<C: CursorValues> {
    cursor: C,
    batch: RecordBatch,
    stream_idx: usize,
    pub(crate) begin_row: usize,
    batch_idx: usize,
    // end row is inclusive
    pub(crate) end_row_for_merge: usize,
}

impl<C: CursorValues> BatchRange<C> {
    pub fn new(
        cursor: C,
        batch: RecordBatch,
        stream_idx: usize,
        batch_idx: usize,
        end_row_for_merge: usize,
    ) -> Self {
        Self {
            cursor,
            batch,
            stream_idx,
            begin_row: 0,
            batch_idx,
            end_row_for_merge,
        }
    }

    // produce this range from begin to num_rows - 1
    pub fn slice_remaining_and_advance(&mut self) -> RecordBatch {
        let sliced = self
            .batch
            .slice(self.begin_row, self.end_row_for_merge - self.begin_row + 1);
        self.begin_row = self.end_row_for_merge + 1;
        sliced
    }

    /// Find the starting index in this BatchRange where the value is greater than or equal to
    /// the value at the specified row in another BatchRange
    ///
    /// # Arguments
    ///
    /// * `other` - Another BatchRange to compare against
    /// * `other_row_idx` - The row index in the other BatchRange to compare against
    ///
    /// # Returns
    ///
    /// The starting index in this BatchRange where values are >= the value at `other_row_idx` in `other`,
    /// or None if all values in this range are less than the target value
    #[allow(dead_code)]
    pub fn find_ge_start_index(
        &self,
        other: &BatchRange<C>,
        other_row_idx: usize,
    ) -> Option<usize> {
        if other_row_idx > other.end_row_for_merge() {
            return None;
        }

        if self.batch.num_rows() == 0 || self.begin_row > self.end_row_for_merge {
            return None;
        }

        // 在当前可 merge 区间 [begin_row, end_row_for_merge] 内，
        // 查找第一个满足 self[idx] >= other[other_row_idx] 的位置（lower_bound）
        let mut left = self.begin_row;
        let mut right = self.end_row_for_merge + 1; // 开区间

        while left < right {
            let mid = left + (right - left) / 2;

            let comparison = C::compare(&self.cursor, mid, &other.cursor, other_row_idx);

            if comparison.is_lt() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left <= self.end_row_for_merge {
            Some(left)
        } else {
            None
        }
    }

    /// Find the starting index in this BatchRange where the value is less than or equal to
    /// the value at the specified row in another BatchRange
    ///
    /// # Arguments
    ///
    /// * `other` - Another BatchRange to compare against
    /// * `other_row_idx` - The row index in the other BatchRange to compare against
    ///
    /// # Returns
    ///
    /// The ending index in this BatchRange where values are <= the value at `other_row_idx` in `other`,
    /// or None if all values in this range are greater than the target value
    pub fn find_le_start_index(
        &self,
        other: &BatchRange<C>,
        other_row_idx: usize,
    ) -> Option<usize> {
        if other_row_idx > other.end_row_for_merge() {
            return None;
        }

        if self.batch.num_rows() == 0 || self.begin_row > self.end_row_for_merge {
            return None;
        }

        // 在当前可 merge 区间 [begin_row, end_row_for_merge] 内，
        // 查找最后一个满足 self[idx] <= other[other_row_idx] 的位置（upper_bound - 1）
        let mut left = self.begin_row;
        let mut right = self.end_row_for_merge + 1; // 开区间

        while left < right {
            let mid = left + (right - left) / 2;

            let comparison = C::compare(&self.cursor, mid, &other.cursor, other_row_idx);

            if comparison.is_lt() || comparison.is_eq() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left == self.begin_row {
            return None;
        }
        Some(left - 1)
    }

    /// Get the cursor associated with this batch range
    pub fn cursor(&self) -> &C {
        &self.cursor
    }

    /// Get the batch associated with this range
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Get the stream index
    pub fn stream_idx(&self) -> usize {
        self.stream_idx
    }

    /// Get the begin row
    pub fn begin_row(&self) -> usize {
        self.begin_row
    }

    /// Get the batch index
    pub fn batch_idx(&self) -> usize {
        self.batch_idx
    }

    /// Get the end row for merge
    pub fn end_row_for_merge(&self) -> usize {
        self.end_row_for_merge
    }

    pub fn set_end_row_for_merge(&mut self, end_row_for_merge: usize) {
        self.end_row_for_merge = end_row_for_merge;
    }

    /// Check if this range has more rows to process
    pub fn has_more_rows(&self) -> bool {
        self.begin_row <= self.end_row_for_merge
    }

    /// Advance to next row
    pub fn advance(&mut self) {
        self.begin_row += 1;
    }

    pub fn reset(&mut self) {
        self.end_row_for_merge = self.batch.num_rows() - 1;
    }

    pub fn remaining_rows(&self) -> usize {
        self.end_row_for_merge - self.begin_row + 1
    }
}

impl<C: CursorValues> PartialEq for BatchRange<C> {
    fn eq(&self, other: &Self) -> bool {
        C::eq(&self.cursor, self.begin_row, &other.cursor, other.begin_row)
    }
}

impl<C: CursorValues> Eq for BatchRange<C> {}

impl<C: CursorValues> PartialOrd for BatchRange<C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<C: CursorValues> Ord for BatchRange<C> {
    fn cmp(&self, other: &Self) -> Ordering {
        C::compare(&self.cursor, self.begin_row, &other.cursor, other.begin_row)
            .then_with(|| self.stream_idx.cmp(&other.stream_idx))
            .then_with(|| self.batch_idx.cmp(&other.batch_idx))
    }
}

pub struct InProgressRow {
    pub(crate) range_idx: usize,
    pub(crate) row_idx: usize,
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

    fn create_batch(values: &[i32]) -> RecordBatch {
        let arr: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        RecordBatch::try_from_iter(vec![("a", arr)]).unwrap()
    }

    fn create_range(values: &[i32]) -> BatchRange<RowValues> {
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
        BatchRange::new(cursor, batch, 0, 0, end_row_for_merge)
    }

    #[test]
    fn test_find_ge_start_index_basic_and_none() {
        let r = create_range(&[1, 3, 5, 7]);

        // target = 3 => first >= 3 is idx 1
        let target_eq = create_range(&[3]);
        assert_eq!(r.find_ge_start_index(&target_eq, 0), Some(1));

        // target = 4 => first >= 4 is idx 3 (value 5)
        let target_between = create_range(&[4]);
        assert_eq!(r.find_ge_start_index(&target_between, 0), Some(2));

        // target = 8 => none
        let target_big = create_range(&[8]);
        assert_eq!(r.find_ge_start_index(&target_big, 0), None);
    }

    #[test]
    fn test_find_ge_start_index_with_begin_row() {
        let mut r = create_range(&[1, 3, 5, 7]);
        r.advance();
        r.advance();
        // begin_row = 2, target = 3, first >= 3 in [2..] is 2 (value 5)
        let target = create_range(&[3]);
        assert_eq!(r.find_ge_start_index(&target, 0), Some(2));
    }

    #[test]
    fn test_find_le_start_index_basic_and_none() {
        let r = create_range(&[1, 3, 5, 7]);

        // target = 1 => last <= 1 is idx 0
        let target_eq = create_range(&[1]);
        assert_eq!(r.find_le_start_index(&target_eq, 0), Some(0));

        // target = 3 => last <= 3 is idx 1
        let target_eq = create_range(&[3]);
        assert_eq!(r.find_le_start_index(&target_eq, 0), Some(1));

        // target = 4 => last <= 4 is idx 1 (value 3)
        let target_between = create_range(&[4]);
        assert_eq!(r.find_le_start_index(&target_between, 0), Some(1));

        // target = 7 => last <= 7 is idx 3 (value 7)
        let target_between = create_range(&[7]);
        assert_eq!(r.find_le_start_index(&target_between, 0), Some(3));

        // target = 0 => none
        let target_small = create_range(&[0]);
        assert_eq!(r.find_le_start_index(&target_small, 0), None);
    }

    #[test]
    fn test_find_le_start_index_respect_end_row_for_merge() {
        let mut r = create_range(&[1, 3, 5, 7, 7]);
        // restrict merge window to [0..=1]
        r.set_end_row_for_merge(1);
        let target = create_range(&[10]);
        // although 5/7 <= 10, they are outside merge window, so answer is 1
        assert_eq!(r.find_le_start_index(&target, 0), Some(1));

        let target = create_range(&[3]);
        assert_eq!(r.find_le_start_index(&target, 0), Some(1));

        // reset after slice_remaining_and_advance
        r.slice_remaining_and_advance();
        r.reset();
        let target = create_range(&[5]);
        assert_eq!(r.find_le_start_index(&target, 0), Some(2));

        r.advance();
        let target = create_range(&[7]);
        assert_eq!(r.find_le_start_index(&target, 0), Some(4));
    }
}
