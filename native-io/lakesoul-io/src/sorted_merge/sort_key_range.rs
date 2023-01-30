use std::sync::Arc;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};


use arrow::{
    record_batch::RecordBatch,
    datatypes::SchemaRef,
    row::{Row, Rows},
    array::ArrayRef,
};

// A range in one record_batch with same primary key
pub struct SortKeyBatchRange {
    pub(crate) begin_row: usize, // begin row in this batch, included
    pub(crate) end_row: usize,   // not included
    pub(crate) stream_idx: usize,
    pub(crate) batch: Arc<RecordBatch>,
    pub(crate) rows: Arc<Rows>,
}

impl SortKeyBatchRange {
    pub fn new(begin_row: usize, end_row: usize, stream_idx: usize, batch: Arc<RecordBatch>, rows: Arc<Rows>) -> Self {
        SortKeyBatchRange {
            begin_row,
            end_row,
            stream_idx,
            batch,
            rows,
        }
    }

    pub fn new_and_init(begin_row: usize, stream_idx: usize, batch: Arc<RecordBatch>, rows: Arc<Rows>) -> Self {
        let mut range =SortKeyBatchRange {
            begin_row,
            end_row: begin_row,
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
    /// Return true if the stream is finished
    pub fn is_finished(&self) -> bool {
        self.begin_row >= self.rows.num_rows()
    }


    #[inline(always)]
    /// Returns the cursor's current row, and advances the cursor to the next row
    pub fn advance(&mut self) -> SortKeyBatchRange {
        // assert!(!self.is_finished());
        // let t = self.cur_row;
        // self.cur_row += 1;
        // t
        let current = self.clone();
        self.begin_row = self.end_row;
        if !self.is_finished() {
            while self.end_row < self.rows.num_rows() {
                // check if next row in this batch has same sort key
                if self.rows.row(self.end_row) == self.rows.row(self.begin_row) {
                    self.end_row = self.end_row + 1;
                } else {
                    break;
                }
            }
        }
        current
    }

    pub fn column(&self, idx: usize) -> SortKeyArrayRange {
        SortKeyArrayRange {
            begin_row: self.begin_row,
            end_row: self.end_row,
            stream_idx: self.stream_idx,
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
        SortKeyBatchRange::new(self.begin_row, self.end_row, self.stream_idx, self.batch.clone(), self.rows.clone())
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

#[derive(Debug)]
pub struct SortKeyArrayRange {
    pub(crate) begin_row: usize, // begin row in this batch, included
    pub(crate) end_row: usize,   // not included
    pub(crate) stream_idx: usize,
    pub(crate) array: ArrayRef,
}

impl SortKeyArrayRange {
    pub fn array(&self) -> ArrayRef {
        self.array.clone()
    }
}

impl Clone for SortKeyArrayRange {
    fn clone(&self) -> Self {
        SortKeyArrayRange{
            begin_row: self.begin_row,
            end_row: self.end_row,
            stream_idx: self.stream_idx,
            array: self.array.clone(),
        }
    }
}


// Multiple ranges in consecutive batches of ONE stream with same primary key
// This is the unit to be sorted in min heap
#[derive(Debug)]
pub struct SortKeyArrayRanges {
    pub(crate) sort_key_array_ranges: Vec<Vec<SortKeyArrayRange>>,

    pub(crate) schema: SchemaRef,

    pub(crate) batch_range: Option<SortKeyBatchRange>,
}

impl SortKeyArrayRanges {
    pub fn new(schema: SchemaRef) -> SortKeyArrayRanges {
        SortKeyArrayRanges {
            sort_key_array_ranges: (0..schema.fields().len()).map(|_| vec![]).collect(),
            schema: schema.clone(),
            batch_range: None,
        }
    }

    /// Returns the [`Schema`](arrow_schema::Schema) of the record batch.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn column(&self, column_idx: usize) -> Vec<SortKeyArrayRange> {
        self.sort_key_array_ranges[column_idx].clone()
    }

    pub fn add_range_in_batch(&mut self, range: SortKeyBatchRange) {
        if self.is_empty() {
            self.set_batch_range(Some(range.clone()));
        }
        let schema = self.schema();
        for column_idx in 0..schema.fields().len() {
            let name = schema.field(column_idx).name();

            range
                .schema()
                .column_with_name(name)
                .map(|(idx, feild)| self.sort_key_array_ranges[column_idx].push(range.column(idx)));
        };
    }

    pub fn is_empty(&self) -> bool{
        self.batch_range.is_none()
    }

    pub fn set_batch_range(&mut self, batch_range: Option<SortKeyBatchRange>) {
        self.batch_range = match batch_range {
            None => None,
            Some(batch_range) => Some(batch_range.clone()),
        }
    }

    pub fn match_row(&self, range: &SortKeyBatchRange) -> bool {
        match &self.batch_range {
            None => true,
            Some(batch_range) => batch_range.current() == range.current()
        }
    }

}

impl Clone for SortKeyArrayRanges {
    fn clone(&self) -> Self {
        SortKeyArrayRanges {
            sort_key_array_ranges: self.sort_key_array_ranges.clone(),
            schema: self.schema.clone(),
            batch_range: match &self.batch_range {
                None => None,
                Some(batch_range) => Some(batch_range.clone()),
            },
        }
    }
}
