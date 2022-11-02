use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::Schema;
use heapless::binary_heap::{BinaryHeap, Max};

use std::sync::Arc;
use std::iter::Iterator;

use crate::merge_logic::index::ResultIndex;
use crate::merge_logic::merge_heap::MergeHeap;
use crate::merge_logic::merge_partitioned_file::RecordBatchIterWithFile;

struct MergeBatchRow
{
    batch_iter: Vec<RecordBatchIterWithFile>,
    // flatten column vector of record batch from batch_iter
    flatten_columns: Vec<ArrayRef>,
    // flatten schema of batch_iter
    flatten_schema: Schema,
    // schema of result_rows
    result_schema: Schema,
    // accumulate result_row for yield RecordBatch
    result_rows: Vec<Row>,

    // todo: fix MergeHeap type
    merge_heap: MergeHeap<usize, Max, 8>,
}

pub struct Row{
    schema: Box<Schema>,
    fields: Vec<ArrayRef>,

    // index that mapping result_schema and flatten schema
    result_index: Vec<usize>,
    // reference of flatten columns and flatten schema from MergeBatchRow
    flatten_columns_ref: Box<Vec<ArrayRef>>,
    flatten_schema_ref: Box<Schema>,
}

impl MergeBatchRow {
    fn init(&mut self) {
        self.merge_heap = MergeHeap::new();
        
    }
}

impl Iterator for MergeBatchRow{
    type Item = RecordBatch;
    fn next(&mut self) -> Option<RecordBatch> {
        None
    }
}