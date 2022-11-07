use std::borrow::BorrowMut;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::future::Ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::error::ArrowError;
use arrow::row::{Row, Rows};
use arrow::{array::make_array as make_arrow_array, error::Result as ArrowResult, record_batch::RecordBatch};
use datafusion::arrow::array::MutableArrayData;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::error::Result;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::metrics::MemTrackingMetrics;
use datafusion::physical_plan::sorts::RowIndex;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{Buffered, Fuse, FusedStream, Map};
use futures::{Stream, StreamExt};
use smallvec::SmallVec;
use async_stream::{AsyncStream, stream, try_stream};
use pin_project_lite::pin_project;

// A range in one record batch with same primary key
pub struct SortKeyRangeInBatch {
    batch_id: usize,
    begin_row: usize, // begin row in this batch, included
    end_row: usize,   // included
    rows: Arc<Rows>,
}

impl SortKeyRangeInBatch {
    pub fn new(batch_id: usize, begin_row: usize, end_row: usize, rows: Arc<Rows>) -> Self {
        SortKeyRangeInBatch {
            batch_id,
            begin_row,
            end_row,
            rows,
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

pub type BufferedRecordBatchStream =
    Buffered<Map<SendableRecordBatchStream, fn(ArrowResult<RecordBatch>) -> Ready<ArrowResult<RecordBatch>>>>;

struct MergingStreams {
    /// The sorted input streams to merge together
    streams: Vec<Fuse<BufferedRecordBatchStream>>,
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
    fn new(input_streams: Vec<Fuse<BufferedRecordBatchStream>>) -> Self {
        Self {
            num_streams: input_streams.len(),
            streams: input_streams,
        }
    }

    fn num_streams(&self) -> usize {
        self.num_streams
    }
}

pin_project! {
    pub struct MergedStream<St> {
        #[pin]
        inner: St,
        schema: SchemaRef,
    }
}

impl <St> MergedStream<St>
where St: Stream<Item = ArrowResult<RecordBatch>>
{
    pub fn new(schema: SchemaRef, stream: St) -> Self {
        MergedStream {
            inner: stream,
            schema,
        }
    }
}

impl <St> Stream for MergedStream<St>
where
    St: Stream<Item = ArrowResult<RecordBatch>>
{
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.as_mut().poll_next(cx)
    }
}

impl <St> RecordBatchStream for MergedStream<St>
where St: Stream<Item = ArrowResult<RecordBatch>>
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
    batches: Vec<VecDeque<(RecordBatch, Arc<Rows>)>>,

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
        let mut wrappers = Vec::with_capacity(stream_count);
        streams.into_iter().for_each(|s| {
            let ready_futures = s.stream.map(std::future::ready as fn(ArrowResult<RecordBatch>) -> Ready<ArrowResult<RecordBatch>>);
            let bufferred = ready_futures.buffered(2);
            wrappers.push(bufferred.fuse());
        });
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

        // Fetch a new input record and create a cursor from it
        loop {
            let batch = stream.next().await.unwrap()?;
            if batch.num_rows() > 0 {
                let cols = self
                    .column_expressions
                    .iter()
                    .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows())))
                    .collect::<Result<Vec<_>>>()?;

                let rows = self.row_converter.convert_columns(&cols)?;
                let batch_idx = self.batches[idx].len();
                self.batches[idx].push_back((batch, Arc::new(rows)));
                let in_progress_row_index = self.in_progress_ranges.get_mut(idx).unwrap();
                in_progress_row_index.row_idx = 0;
                in_progress_row_index.batch_idx = batch_idx;
                in_progress_row_index.stream_idx = idx;
                return Ok(());
            } else {
                continue;
            }
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
            let mut i = in_progress_row_index.row_idx;
            let rows= self.batches[idx][in_progress_row_index.batch_idx].1.clone();
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
                        rows.clone(),
                    );
                    sort_key_range.sort_key_ranges.push(sort_key_in_batch);
                    in_progress_row_index.row_idx += 1;
                }
            }
            // reach the end of current batch
            if i >= rows.num_rows() {
                // see if next batch has same row
                self.fetch_next_batch_from_stream(idx).await?;
                if !sort_key_range.sort_key_ranges.last().is_some_and(|sort_key_last_batch| {
                    let next_batch_and_row = self.batches[idx].back().unwrap();
                    sort_key_last_batch.current() == (*next_batch_and_row).1.row(0)
                }) {
                    // if next batch's first row doesn't match, break the loop.
                    // otherwise we'll continue to next batch
                    break;
                }
            } else {
                break;
            }
        }
        self.heap.push(Reverse(sort_key_range));
        return Ok(());
    }

    async fn pop_heap(&mut self) -> ArrowResult<Option<SortKeyRange>> {
        match self.heap.pop() {
            Some(Reverse(range)) => {
                self.fetch_sort_key_range_from_stream(range.stream_idx).await?;
                Ok(Some(range))
            }
            _ => Ok(None)
        }
    }

    async fn maybe_merge_waiting_ranges(&mut self, new_range: SmallVec<[SortKeyRange; 4]>) -> Option<RecordBatch> {
        self.waiting_ranges_to_merge.push(new_range);
        if self.waiting_ranges_to_merge.len() >= self.batch_size {
            // execute merge for currently collected ranges
            
        }
        None
    }

    pub async fn get_stream(mut self) -> ArrowResult<SendableRecordBatchStream> {
        for i in 0..self.streams.num_streams() {
            self.fetch_sort_key_range_from_stream(i).await?;
        }
        let schema = self.schema.clone();
        let stream = try_stream! {
            loop {
                let mut ranges: SmallVec<[SortKeyRange; 4]> = SmallVec::<[SortKeyRange; 4]>::new();
                let sort_key_range = self.pop_heap().await?;
                match sort_key_range {
                    Some(range) => {
                        ranges.push(range);
                        loop {
                            // check if next range (in another stream) has same row key
                            let next_range = self.heap.peek();
                            if next_range.is_some_and(|nr| {
                                match nr {
                                    Reverse(r) => r == ranges.last().unwrap(),
                                }
                            }) {
                                let sort_key_range_next = self.pop_heap().await?;
                                match sort_key_range_next {
                                    Some(range_next) => {
                                        ranges.push(range_next);
                                    },
                                    _ => break,
                                };
                            } else {
                                break;
                            }
                        }
                    },
                    _ => break
                }
                match self.maybe_merge_waiting_ranges(ranges).await {
                    Some(rb) => yield rb,
                    None => continue,
                }
            }
        };
        Ok(Box::pin(MergedStream::new(schema.clone(), stream)))
    }
}