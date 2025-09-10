// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! This module provides definition and utilities for sort key ranges.

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::{Row, Rows},
};
use arrow_array::Array;
use arrow_cast::pretty::pretty_format_batches;
use smallvec::{SmallVec, smallvec};

/// A range in one arrow::record_batch::RecordBatch with same sorted primary key
/// The minimum unit at SortedStreamMerger
pub struct SortKeyBatchRange {
    /// The begin row in this batch, included
    pub(crate) begin_row: usize,
    /// The end row in this batch, not included
    pub(crate) end_row: usize,
    /// The stream index
    pub(crate) stream_idx: usize,
    /// The batch index counting at SortedStreamMerger
    pub(crate) batch_idx: usize,
    /// The source reference batch
    pub(crate) batch: Arc<RecordBatch>,
    /// The reference of rows in this batch
    pub(crate) rows: Arc<Rows>,
}

impl SortKeyBatchRange {
    /// Create a new SortKeyBatchRange
    pub fn new(
        begin_row: usize,
        end_row: usize,
        stream_idx: usize,
        batch_idx: usize,
        batch: Arc<RecordBatch>,
        rows: Arc<Rows>,
    ) -> Self {
        SortKeyBatchRange {
            begin_row,
            end_row,
            stream_idx,
            batch_idx,
            batch,
            rows,
        }
    }

    /// Create a new SortKeyBatchRange and initialize it
    pub fn new_and_init(
        begin_row: usize,
        stream_idx: usize,
        batch_idx: usize,
        batch: Arc<RecordBatch>,
        rows: Arc<Rows>,
    ) -> Self {
        let mut range = SortKeyBatchRange {
            begin_row,
            end_row: begin_row,
            batch_idx,
            stream_idx,
            batch,
            rows,
        };
        range.advance();
        range
    }

    /// Returns the [`Schema`](arrow_schema::Schema) of the record batch.
    pub fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    /// Return the number of columns in this batch
    pub fn columns(&self) -> usize {
        self.batch.num_columns()
    }

    /// Return the current row of this range
    pub(crate) fn current(&self) -> Row<'_> {
        self.rows.row(self.begin_row)
    }

    #[inline(always)]
    /// Return the stream index of this range
    pub fn stream_idx(&self) -> usize {
        self.stream_idx
    }

    #[inline(always)]
    /// Return true if the range has reached the end of batch
    pub fn is_finished(&self) -> bool {
        self.begin_row >= self.batch.num_rows()
    }

    #[inline(always)]
    /// Returns the cloned current batch range, and advances the current range to the next range with next sort key
    pub fn advance(&mut self) -> SortKeyBatchRange {
        let current = self.clone();
        self.begin_row = self.end_row;
        if !self.is_finished() {
            while self.end_row < self.batch.num_rows() {
                // check if next row in this batch has same sort key
                if self.rows.row(self.end_row) == self.rows.row(self.begin_row) {
                    self.end_row += 1;
                } else {
                    break;
                }
            }
        }
        current
    }

    /// create a SortKeyArrayRange of specific column index of SortKeyBatchRange
    pub fn column(&self, idx: usize) -> SortKeyArrayRange {
        SortKeyArrayRange {
            begin_row: self.begin_row,
            end_row: self.end_row,
            stream_idx: self.stream_idx,
            batch_idx: self.batch_idx,
            array: self.batch.column(idx).clone(),
        }
    }

    /// Return the reference of array of specific column index of SortKeyBatchRange
    pub fn array(&self, idx: usize) -> ArrayRef {
        unsafe { self.batch.columns().get_unchecked(idx).clone() }
    }

    /// Return the reference of batch
    pub fn batch(&self) -> Arc<RecordBatch> {
        self.batch.clone()
    }
}

impl Debug for SortKeyBatchRange {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SortKeyBatchRange: \nbegin_row: {}, end_row: {}, stream_idx: {}, batch_idx: {}\n",
            self.begin_row, self.end_row, self.stream_idx, self.batch_idx
        )?;
        write!(
            f,
            "batch: \n{}",
            &pretty_format_batches(&[self
                .batch
                .slice(self.begin_row, self.end_row - self.begin_row)])
            .unwrap()
        )
    }
}

impl Clone for SortKeyBatchRange {
    fn clone(&self) -> Self {
        SortKeyBatchRange::new(
            self.begin_row,
            self.end_row,
            self.stream_idx,
            self.batch_idx,
            self.batch.clone(),
            self.rows.clone(),
        )
    }
}

impl PartialEq for SortKeyBatchRange {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for SortKeyBatchRange {}

impl PartialOrd for SortKeyBatchRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKeyBatchRange {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current()
            .cmp(&other.current())
            .then_with(|| self.stream_idx.cmp(&other.stream_idx))
    }
}

/// A range in one arrow::array::Array with same sorted primary key
#[derive(Debug)]
pub struct SortKeyArrayRange {
    /// The begin row in this batch, included
    pub(crate) begin_row: usize,
    /// The end row in this batch, not included
    pub(crate) end_row: usize,
    /// The stream index
    pub(crate) stream_idx: usize,
    /// The batch index counting at SortedStreamMerger
    pub(crate) batch_idx: usize,
    /// The reference of array
    pub(crate) array: ArrayRef,
}

impl SortKeyArrayRange {
    pub fn array(&self) -> ArrayRef {
        self.array.clone()
    }
}

impl Clone for SortKeyArrayRange {
    fn clone(&self) -> Self {
        SortKeyArrayRange {
            begin_row: self.begin_row,
            end_row: self.end_row,
            stream_idx: self.stream_idx,
            batch_idx: self.batch_idx,
            array: self.array.clone(),
        }
    }
}

pub(crate) type SortKeyArrayRangeVec = SmallVec<[SortKeyArrayRange; 16]>;

/// Multiple ranges with same sorted primary key from variant source record_batch.
/// These ranges will be merged into ONE row of target record_batch finally.
#[derive(Debug, Clone)]
pub struct SortKeyBatchRanges {
    /// vector with length=column_num
    /// each element of this vector is a collection corresponding to the specific column of SortKeyArrayRange to be merged
    pub(crate) sort_key_array_ranges: Vec<SortKeyArrayRangeVec>,

    /// fields_index_map from source schemas to target schema which vector index = stream_idx
    fields_map: Arc<Vec<Vec<usize>>>,

    /// The schema of the record batch
    pub(crate) schema: SchemaRef,

    /// The current batch range for collecting SortKeyArrayRange of current primary key
    pub(crate) batch_range: Option<SortKeyBatchRange>,
}

impl SortKeyBatchRanges {
    pub fn new(
        schema: SchemaRef,
        fields_map: Arc<Vec<Vec<usize>>>,
    ) -> SortKeyBatchRanges {
        SortKeyBatchRanges {
            sort_key_array_ranges: vec![smallvec![]; schema.fields().len()],
            fields_map,
            schema: schema.clone(),
            batch_range: None,
        }
    }

    /// Returns the [`Schema`](arrow_schema::Schema) of the record batch.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn column(&self, column_idx: usize) -> &SortKeyArrayRangeVec {
        &self.sort_key_array_ranges[column_idx]
    }

    /// add one SortKeyBatchRange into SortKeyBatchRanges, collect SortKeyArrayRange of each column into sort_key_array_ranges
    pub fn add_range_in_batch(&mut self, range: SortKeyBatchRange) {
        if self.is_empty() {
            self.set_batch_range(Some(range.clone()));
        }
        let schema = range.schema();
        for column_idx in 0..schema.fields().len() {
            let range_col = range.column(column_idx);
            let target_schema_idx = self.fields_map[range.stream_idx()][column_idx];
            self.sort_key_array_ranges[target_schema_idx].push(range_col);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.batch_range.is_none()
    }

    /// update the current batch range
    pub fn set_batch_range(&mut self, batch_range: Option<SortKeyBatchRange>) {
        self.batch_range = batch_range
    }

    /// check if the current batch range matches the given range
    pub fn match_row(&self, range: &SortKeyBatchRange) -> bool {
        match &self.batch_range {
            // return true if no current batch range
            None => true,
            Some(batch_range) => batch_range.current() == range.current(),
        }
    }
}

/// A range in one arrow::array::Array with same sorted primary key, which is used for UseLast MergeOperator
#[derive(Debug)]
pub struct UseLastSortKeyArrayRange {
    /// The row index in the batch
    pub(crate) row_idx: usize,
    /// The batch index
    pub(crate) batch_idx: usize,
    /// The reference of batch
    pub(crate) batch: Arc<RecordBatch>,
    /// The column index
    pub(crate) column_idx: usize,
    /// The stream index
    pub(crate) stream_idx: usize,
}

impl UseLastSortKeyArrayRange {
    #[inline]
    pub fn array(&self) -> ArrayRef {
        unsafe { self.batch.columns().get_unchecked(self.column_idx).clone() }
    }

    #[inline]
    pub fn array_ref(&self) -> &dyn Array {
        unsafe { self.batch.columns().get_unchecked(self.column_idx).as_ref() }
    }

    #[inline]
    pub fn array_ref_by_col(&self, column_idx: usize) -> ArrayRef {
        unsafe { self.batch.columns().get_unchecked(column_idx).clone() }
    }
}

impl Clone for UseLastSortKeyArrayRange {
    fn clone(&self) -> Self {
        UseLastSortKeyArrayRange {
            row_idx: self.row_idx,
            batch_idx: self.batch_idx,
            batch: self.batch.clone(),
            column_idx: self.column_idx,
            stream_idx: self.stream_idx,
        }
    }
}

/// Multiple ranges with same sorted primary key from variant source record_batch.
/// These ranges will be merged into ONE row of target record_batch finally.
/// This is used for case of UseLast MergeOperator.
#[derive(Debug, Clone, Default)]
pub struct UseLastSortKeyBatchRanges {
    /// The current batch range for collecting UseLastSortKeyArrayRange of current primary key
    current_batch_range: Option<SortKeyBatchRange>,

    /// UseLastSortKeyArrayRange for each column of source schema
    last_index_of_array: Vec<Option<UseLastSortKeyArrayRange>>,

    /// whether the current batch range is partial merge
    is_partial_merge: bool,
}

impl UseLastSortKeyBatchRanges {
    pub fn new(field_num: usize, is_partial_merge: bool) -> UseLastSortKeyBatchRanges {
        let last_index_of_array = if is_partial_merge {
            vec![None; field_num]
        } else {
            vec![None; 1]
        };
        UseLastSortKeyBatchRanges {
            current_batch_range: None,
            last_index_of_array,
            is_partial_merge,
        }
    }

    #[inline]
    pub fn match_row(&self, range: &SortKeyBatchRange) -> bool {
        match &self.current_batch_range {
            // return true if no current batch range
            None => true,
            Some(batch_range) => batch_range.current() == range.current(),
        }
    }

    /// add one SortKeyBatchRange into UseLastSortKeyBatchRanges,
    /// collect UseLastSortKeyArrayRange of each column into last_index_of_array
    pub fn add_range_in_batch(
        &mut self,
        range: &SortKeyBatchRange,
        fields_map: &Vec<Vec<usize>>,
    ) {
        if self.is_empty() {
            self.set_batch_range(Some(range.clone()));
        }
        unsafe {
            if self.is_partial_merge {
                let range_col = fields_map.get_unchecked(range.stream_idx());
                for column_idx in 0..range.columns() {
                    let target_schema_idx = range_col.get_unchecked(column_idx);
                    *self
                        .last_index_of_array
                        .get_unchecked_mut(*target_schema_idx) =
                        Some(UseLastSortKeyArrayRange {
                            row_idx: range.end_row - 1,
                            batch_idx: range.batch_idx,
                            batch: range.batch(),
                            column_idx,
                            stream_idx: range.stream_idx(),
                        });
                }
            } else {
                // full column merge. we just need to record batch idx of this row
                *self.last_index_of_array.get_unchecked_mut(0) =
                    Some(UseLastSortKeyArrayRange {
                        row_idx: range.end_row - 1,
                        batch_idx: range.batch_idx,
                        batch: range.batch(),
                        column_idx: 0,
                        stream_idx: range.stream_idx(),
                    });
            }
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.current_batch_range.is_none()
    }

    /// update the current batch range
    #[inline]
    pub fn set_batch_range(&mut self, batch_range: Option<SortKeyBatchRange>) {
        self.current_batch_range = batch_range
    }

    /// return the UseLastSortKeyArrayRange of specific column
    #[inline]
    pub fn column(&self, column_idx: usize) -> &Option<UseLastSortKeyArrayRange> {
        unsafe {
            if self.is_partial_merge {
                self.last_index_of_array.get_unchecked(column_idx)
            } else {
                self.last_index_of_array.get_unchecked(0)
            }
        }
    }
}

pub type SortKeyBatchRangesRef = Arc<SortKeyBatchRanges>;
pub type UseLastSortKeyBatchRangesRef = Arc<UseLastSortKeyBatchRanges>;
