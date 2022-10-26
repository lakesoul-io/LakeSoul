use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::result;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::error::ArrowError;
use arrow::row::{Row, Rows};
use arrow::{array::make_array as make_arrow_array, error::Result as ArrowResult, record_batch::RecordBatch};
use arrow::compute::sort;
use datafusion::arrow::array::MutableArrayData;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::error::Result;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::metrics::MemTrackingMetrics;
use datafusion::physical_plan::sorts::RowIndex;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{Buffered, Fuse, FusedStream, Peekable};
use futures::{Stream, StreamExt};
use smallvec::SmallVec;
use async_stream::stream;

// A range in one record batch with same primary key
pub struct SortKeyRangeInBatch {
    batch_id: usize,
    begin_row: usize, // begin row in this batch, included
    end_row: usize,   // included
    num_rows_in_batch: usize,
}

impl SortKeyRangeInBatch {
    pub fn new(batch_id: usize, begin_row: usize, end_row: usize, num_rows_in_batch: usize) -> Self {
        SortKeyRangeInBatch {
            batch_id,
            begin_row,
            end_row,
            num_rows_in_batch,
        }
    }

    fn current(&self) -> Row<'_> {
        self.rows.row(self.begin_row)
    }
}

// Multiple ranges in consecutive batches of ONE stream with same primary key
// This is the unit to be sorted in min heap
pub struct SortKeyRange {
    // use small vector to avoid allocation on every row
    sort_key_ranges: SmallVec<[SortKeyRangeInBatch; 2]>,

    stream_idx: usize,
}

impl SortKeyRange {
    pub fn new(stream_idx: usize) -> SortKeyRange {
        SortKeyRange {
            sort_key_ranges: SmallVec::new(),
            stream_idx,
        }
    }

    pub fn add_range_in_batch(&mut self, range: SortKeyRangeInBatch) {
        self.sort_key_ranges.push(range)
    }

    fn current(&self) -> Row<'_> {
        self.sort_key_ranges.first().unwrap().current()
    }
}

impl PartialEq for SortKeyRange {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for SortKeyRange {}

impl PartialOrd for SortKeyRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKeyRange {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current()
            .cmp(&other.current())
            .then_with(|| self.stream_idx.cmp(&other.stream_idx))
    }
}

impl Debug for SortKeyRangeInBatch {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyRangeInBatch")
            .field("begin_row", &self.begin_row)
            .field("end_row", &self.end_row)
            .field("num_rows", &self.num_rows_in_batch)
            .field("batch_id", &self.batch_id)
            .finish()
    }
}

impl Debug for SortKeyRange {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("SortKeyRange")
            .field("stream_idx", &self.stream_idx)
            .finish()?;
        f.debug_list().entries(self.sort_key_ranges.iter()).finish()
    }
}

pub(crate) struct SortedStream {
    stream: SendableRecordBatchStream,
    mem_used: usize,
}

impl Debug for SortedStream {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InMemSorterStream")
    }
}

impl SortedStream {
    pub(crate) fn new(stream: SendableRecordBatchStream, mem_used: usize) -> Self {
        Self { stream, mem_used }
    }
}

struct MergingStreams {
    /// The sorted input streams to merge together
    streams: Vec<Fuse<Peekable<Buffered<SendableRecordBatchStream>>>>,
    /// number of streams
    num_streams: usize,
}

impl Debug for MergingStreams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergingStreams")
            .field("num_streams", &self.num_streams)
            .finish()
    }
}

impl MergingStreams {
    fn new(input_streams: Vec<Fuse<Peekable<Buffered<SendableRecordBatchStream>>>>) -> Self {
        Self {
            num_streams: input_streams.len(),
            streams: input_streams,
        }
    }

    fn num_streams(&self) -> usize {
        self.num_streams
    }
}

#[derive(Debug)]
pub(crate) struct SortPreservingMergeStream {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    streams: MergingStreams,

    /// For each input stream maintain a dequeue of RecordBatches
    ///
    /// Exhausted batches will be popped off the front once all
    /// their rows have been yielded to the output
    batches: Vec<VecDeque<(RecordBatch, Rows)>>,

    /// The row index for each stream to identify next range begin row id
    in_progress_ranges: Vec<RowIndex>,

    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,

    /// used to record execution metrics
    tracking_metrics: MemTrackingMetrics,

    heap: BinaryHeap<Reverse<SortKeyRange>>,

    /// target batch size
    batch_size: usize,

    /// row converter
    row_converter: RowConverter,

    waiting_ranges_to_merge: Vec<SmallVec<[SortKeyRange; 4]>>,
}

impl SortPreservingMergeStream {
    pub(crate) fn new_from_streams(
        streams: Vec<SortedStream>,
        schema: SchemaRef,
        expressions: &[PhysicalSortExpr],
        tracking_metrics: MemTrackingMetrics,
        batch_size: usize,
    ) -> Result<Self> {
        let stream_count = streams.len();
        let batches = (0..stream_count).into_iter().map(|_| VecDeque::new()).collect();
        tracking_metrics.init_mem_used(streams.iter().map(|s| s.mem_used).sum());
        let wrappers = streams.into_iter().map(|s| s.stream.buffered(2).peekable()).collect();
        let default_row_index = RowIndex {
            stream_idx: 0,
            batch_idx: 0,
            row_idx: 0
        };

        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(&schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;
        let row_converter = RowConverter::new(sort_fields);

        Ok(Self {
            schema,
            batches,
            streams: MergingStreams::new(wrappers),
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            tracking_metrics,
            in_progress_ranges: vec![default_row_index; stream_count],
            heap: BinaryHeap::with_capacity(stream_count),
            batch_size,
            row_converter,
            waiting_ranges_to_merge: Vec::with_capacity(batch_size),
        })
    }

    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    async fn fetch_next_batch_from_stream(&mut self, idx: usize) -> ArrowResult<()> {
        let stream = &mut self.streams.streams[idx];
        if stream.is_terminated() {
            return Ok(());
        }

        let batch = stream.next().await?;
        // Fetch a new input record and create a cursor from it
        if batch.num_rows() > 0 {
            let cols = self
                .column_expressions
                .iter()
                .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;

            let rows = self.row_converter.convert_columns(&cols)?;
            let batch_idx = self.batches[idx].len();
            self.batches[idx].push_back((batch, rows));
            let in_progress_row_index = self.in_progress_ranges.get_mut(idx).unwrap();
            in_progress_row_index.row_idx = 0;
            in_progress_row_index.batch_idx = batch_idx;
            in_progress_row_index.stream_idx = idx;
            Ok(())
        } else {
            self.maybe_poll_stream(idx)
        }
    }

    async fn fetch_sort_key_range_from_stream(&mut self, idx: usize) -> ArrowResult<()> {
        if self.batches[idx].is_empty() {
            self.fetch_next_batch_from_stream(idx).await?;
            if self.batches[idx].is_empty() {
                return Ok(());
            }
        }

        let mut sort_key_range = SortKeyRange::new(idx);
        loop {
            // get current in progress(next) begin row for this stream
            let in_progress_row_index = self.in_progress_ranges.get_mut(idx).unwrap();
            let mut i = 0;
            let rows = &self.batches[idx][in_progress_row_index.batch_idx].1;
            while i < rows.num_rows() {
                if i < rows.num_rows() - 1 {
                    if rows.row(i + 1) == rows.row(i) {
                        i = i + 1;
                        continue;
                    }
                    // construct a sort key in batch
                    let sort_key_in_batch = SortKeyRangeInBatch::new(
                        in_progress_row_index.batch_idx,
                        in_progress_row_index.row_idx,
                        i,
                        rows.num_rows(),
                    );
                    sort_key_range.sort_key_ranges.push(sort_key_in_batch);
                    in_progress_row_index.row_idx += 1;
                }
            }
            if i >= rows.num_rows() {
                // see if next batch has same row
                self.fetch_next_batch_from_stream(idx)?;
                if !sort_key_range.sort_key_ranges.last().is_some_and(|sort_key_last_batch| {
                    sort_key_last_batch.current() == self.batches.last().unwrap().1
                }) {
                    break;
                }
            } else {
                break;
            }
        }
        self.heap.push(Reverse(sort_key_range));
        return Ok(());
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        Err(ArrowError::DivideByZero)
    }

    async fn pop_heap(&mut self) -> Option<SortKeyRange> {
        match self.heap.pop() {
            Some(Reverse(range)) => {
                self.fetch_sort_key_range_from_stream(range.stream_idx).await?;
                Some(range)
            }
            _ => None
        }
    }

    async fn maybe_merge_waiting_ranges(&mut self, new_range: SmallVec<[SortKeyRange; 4]>) -> Option<RecordBatch> {
        self.waiting_ranges_to_merge.push(new_range);
        if self.waiting_ranges_to_merge.len() >= self.batch_size {
            // execute merge for currently collected ranges
            
        }
        None
    }

    pub async fn get_stream(&mut self) /*-> ArrowResult<SendableRecordBatchStream>*/ {
        for i in 0..self.streams.num_streams() {
            self.fetch_sort_key_range_from_stream(i).await?;
        }
        loop {
            let mut ranges: Vec<SmallVec<[SortKeyRange; 4]>> = Vec::with_capacity(self.streams.num_streams());
            let sort_key_range = self.pop_heap().await;
            match sort_key_range {
                Some(range) => {
                    let mut same_sort_key_ranges = SmallVec::<[SortKeyRange; 4]>::new();
                    same_sort_key_ranges.push(range);
                    loop {
                        // check if next range (in another stream) has same row key
                        let next_range = self.heap.peek();
                        if next_range.is_some_and(|range| {
                            range == next_range
                        }) {
                            let sort_key_range = self.pop_heap().await.unwrap();
                            same_sort_key_ranges.push(sort_key_range);
                        } else {
                            break;
                        }
                    }
                    ranges.push(same_sort_key_ranges);
                }
                _ => break
            }
        }
    }
}