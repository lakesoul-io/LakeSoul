use crate::physical_plan::merge::sorted::cursor::CursorValues;
use arrow_array::RecordBatch;
use std::cmp::Ordering;

pub struct BatchRange<C: CursorValues> {
    cursor: C,
    batch: RecordBatch,
    stream_idx: usize,
    begin_row: usize,
    batch_idx: usize,
    end_row_for_merge: usize,
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
    pub fn slice_range_remaining(&mut self) {
        if self.begin_row > 0 {
            self.batch = self
                .batch
                .slice(self.begin_row, self.batch.num_rows() - self.begin_row);
        }
        self.begin_row = self.batch.num_rows();
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
    pub fn find_ge_start_index(
        &self,
        other: &BatchRange<C>,
        other_row_idx: usize,
    ) -> Option<usize> {
        if other_row_idx >= other.batch.num_rows() {
            return None;
        }

        let self_batch_rows = self.batch.num_rows();
        if self_batch_rows == 0 {
            return None;
        }

        // Perform binary search to find the first position where self.value >= other.value
        let mut left = self.begin_row;
        let mut right =
            std::cmp::min(self.begin_row + self_batch_rows, self.batch.num_rows());

        // Get the value from the other batch to compare against
        let target_value_position = other_row_idx;

        while left < right {
            let mid = left + (right - left) / 2;

            // Compare the value at mid in self with the value at other_row_idx in other
            let comparison =
                C::compare(&self.cursor, mid, &other.cursor, target_value_position);

            if comparison.is_lt() {
                // self[mid] < other[other_row_idx], so we need to search in the right half
                left = mid + 1;
            } else {
                // self[mid] >= other[other_row_idx], so this could be our answer
                // but we continue searching in the left half to find the first occurrence
                right = mid;
            }
        }

        // After the loop, left is the first position where self[left] >= other[other_row_idx]
        if left < self.batch.num_rows() {
            Some(left)
        } else {
            None // No value in self is >= the target value
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
    /// The starting index in this BatchRange where values are <= the value at `other_row_idx` in `other`,
    /// or None if all values in this range are greater than the target value
    pub fn find_le_start_index(
        &self,
        other: &BatchRange<C>,
        other_row_idx: usize,
    ) -> Option<usize> {
        if other_row_idx >= other.batch.num_rows() {
            return None;
        }

        let self_batch_rows = self.batch.num_rows();
        if self_batch_rows == 0 {
            return None;
        }

        // Perform binary search to find the first position where self.value <= other.value
        let mut left = self.begin_row;
        let mut right =
            std::cmp::min(self.begin_row + self_batch_rows, self.batch.num_rows());

        // Get the value from the other batch to compare against
        let target_value_position = other_row_idx;

        while left < right {
            let mid = left + (right - left) / 2;

            // Compare the value at mid in self with the value at other_row_idx in other
            let comparison =
                C::compare(&self.cursor, mid, &other.cursor, target_value_position);

            if comparison.is_lt() {
                // self[mid] < other[other_row_idx], so we need to search in the right half
                left = mid + 1;
            } else {
                // self[mid] >= other[other_row_idx], so this could be our answer
                // but we continue searching in the left half to find the first occurrence
                right = mid;
            }
        }

        // After the loop, left is the first position where self[left] <= other[other_row_idx]
        if left < self.batch.num_rows() {
            Some(left)
        } else {
            None // No value in self is <= the target value
        }
    }

    /// Get the number of rows in this batch range
    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
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

    /// Get the current row value for comparison
    pub fn current_row_value(&self) -> Option<&C> {
        if self.has_more_rows() {
            Some(&self.cursor)
        } else {
            None
        }
    }

    /// Advance to next row
    pub fn advance(&mut self) {
        self.begin_row += 1;
    }

    pub fn reset(&mut self) {
        self.begin_row = self.end_row_for_merge + 1;
        self.end_row_for_merge = self.batch.num_rows() - 1;
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
    }
}
