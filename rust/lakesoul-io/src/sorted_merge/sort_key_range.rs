// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::{
    array::ArrayRef,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
    row::{Row, Rows},
};
use smallvec::{smallvec, SmallVec};

// A range in one arrow::record_batch::RecordBatch with same sorted primary key
// This is the unit to be sorted in min heap
pub struct SortKeyBatchRange {
    pub(crate) begin_row: usize, // begin row in this batch, included
    pub(crate) end_row: usize,   // not included
    pub(crate) stream_idx: usize,
    pub(crate) batch_idx: usize,
    pub(crate) batch: Arc<RecordBatch>,
    pub(crate) rows: Arc<Rows>,
}

impl SortKeyBatchRange {
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
    /// Returns the current batch range, and advances the next range with next sort key
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

    //create a SortKeyArrayRange with specific column index of SortKeyBatchRange
    pub fn column(&self, idx: usize) -> SortKeyArrayRange {
        SortKeyArrayRange {
            begin_row: self.begin_row,
            end_row: self.end_row,
            stream_idx: self.stream_idx,
            batch_idx: self.batch_idx,
            array: self.batch.column(idx).clone(),
        }
    }
}

impl Debug for SortKeyBatchRange {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyBatchRange")
            .field("begin_row", &self.begin_row)
            .field("end_row", &self.end_row)
            .field("batch", &self.batch)
            .finish()
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

// A range in one arrow::array::Array with same sorted primary key
#[derive(Debug)]
pub struct SortKeyArrayRange {
    pub(crate) begin_row: usize, // begin row in this batch, included
    pub(crate) end_row: usize,   // not included
    pub(crate) stream_idx: usize,
    pub(crate) batch_idx: usize,
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

// Multiple ranges with same sorted primary key from variant source record_batch. These ranges will be merged into ONE row of target record_batch finnally.
#[derive(Debug)]
pub struct SortKeyBatchRanges {
    // vector with length=column_num that holds a Vector of SortKeyArrayRange to be merged for each column
    pub(crate) sort_key_array_ranges: Vec<SmallVec<[SortKeyArrayRange; 4]>>,

    // fields_index_map from source schemas to target schema which vector index = stream_idx
    fields_map: Arc<Vec<Vec<usize>>>,

    pub(crate) schema: SchemaRef,

    pub(crate) batch_range: Option<SortKeyBatchRange>,
}

impl SortKeyBatchRanges {
    pub fn new(schema: SchemaRef, fields_map: Arc<Vec<Vec<usize>>>) -> SortKeyBatchRanges {
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

    pub fn column(&self, column_idx: usize) -> &SmallVec<[SortKeyArrayRange; 4]> {
        &self.sort_key_array_ranges[column_idx]
    }

    // insert one SortKeyBatchRange into SortKeyArrayRanges
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

    pub fn set_batch_range(&mut self, batch_range: Option<SortKeyBatchRange>) {
        self.batch_range = batch_range
    }

    pub fn match_row(&self, range: &SortKeyBatchRange) -> bool {
        match &self.batch_range {
            None => true,
            Some(batch_range) => batch_range.current() == range.current(),
        }
    }
}

pub type SortKeyBatchRangesRef = Arc<SortKeyBatchRanges>;
