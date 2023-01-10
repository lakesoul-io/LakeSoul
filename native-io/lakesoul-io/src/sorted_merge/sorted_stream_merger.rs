use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::future::Ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::cmp::Reverse;
use std::collections::VecDeque;


use crate::sorted_merge::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
use crate::sorted_merge::combiner::RangeCombiner;
use crate::sorted_merge::fetcher::{RangeFetcher, NonUniqueSortKeyRangeFetcher};

use arrow::error::ArrowError;
use arrow::error::ArrowError::DivideByZero;
use arrow::row::{Row, Rows, RowConverter, SortField};
use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};

use async_stream::try_stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::physical_expr::{PhysicalSortExpr, PhysicalExpr};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{Buffered, Map};
use futures::{Stream, StreamExt};
use futures::stream::{Fuse, FusedStream};

use pin_project_lite::pin_project;
use smallvec::SmallVec;

// A range in one record batch with same primary key
pub struct SortKeyRangeInBatch {
    pub(crate) begin_row: usize, // begin row in this batch, included
    pub(crate) end_row: usize,   // included
    pub(crate) batch: Arc<RecordBatch>,
    pub(crate) rows: Arc<Rows>,
}

impl SortKeyRangeInBatch {
    pub fn new(begin_row: usize, end_row: usize, batch: Arc<RecordBatch>, rows: Arc<Rows>) -> Self {
        SortKeyRangeInBatch {
            begin_row,
            end_row,
            batch,
            rows,
        }
    }

    pub(crate) fn current(&self) -> Row<'_> {
        self.rows.row(self.begin_row)
    }
}

impl Clone for SortKeyRangeInBatch {
    fn clone(&self) -> Self {
        SortKeyRangeInBatch::new(self.begin_row, self.end_row, self.batch.clone(), self.rows.clone())
    }
}

// Multiple ranges in consecutive batches of ONE stream with same primary key
// This is the unit to be sorted in min heap
pub struct SortKeyRange {
    // use small vector to avoid allocation on every row
    pub(crate) sort_key_ranges: SmallVec<[SortKeyRangeInBatch; 2]>,

    pub(crate) stream_idx: usize,
}

impl SortKeyRange {
    pub fn new(stream_idx: usize) -> SortKeyRange {
        SortKeyRange {
            sort_key_ranges: SmallVec::new(),
            stream_idx,
        }
    }

    pub fn clone_from(source: &SortKeyRange) -> SortKeyRange {
        let new_range = SortKeyRange {
            sort_key_ranges: SmallVec::new(),
            stream_idx: source.stream_idx,
        };
        // source.sort_key_ranges
        //     .into_iter()
        //     .map(|range_in_batch| {
        //         new_range.add_range_in_batch(range_in_batch.clone())
        //     });
        new_range
    }

    pub fn add_range_in_batch(&mut self, range: SortKeyRangeInBatch) {
        self.sort_key_ranges.push(range)
    }

    fn current(&self) -> Row<'_> {
        self.sort_key_ranges.first().unwrap().current()
    }

    pub fn is_empty(&self) -> bool {
        todo!()
    }
}

impl Clone for SortKeyRange {
    fn clone(&self) -> Self {
        SortKeyRange::clone_from(self)
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
            .field("batch", &self.batch)
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
}

impl Debug for SortedStream {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InMemSorterStream")
    }
}

impl SortedStream {
    pub(crate) fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

pub type BufferedRecordBatchStream =
    Buffered<Map<SendableRecordBatchStream, fn(ArrowResult<RecordBatch>) -> Ready<ArrowResult<RecordBatch>>>>;

struct MergingStreams {
    /// The sorted input streams to merge together
    streams: Vec<Fuse<SendableRecordBatchStream>>,
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
    fn new(input_streams: Vec<Fuse<SendableRecordBatchStream>>) -> Self {
        Self {
            num_streams: input_streams.len(),
            streams: input_streams,
        }
    }

    fn num_streams(&self) -> usize {
        self.num_streams
    }
}

struct MergingRangeFetchers {
    /// The sorted input streams to merge together
    fetchers: Vec<RangeFetcher>,
    /// number of streams
    num_fetchers: usize,
}

impl Debug for MergingRangeFetchers {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergingRangeFetchers")
            .field("num_fetchers", &self.num_fetchers)
            .finish()
    }
}

impl MergingRangeFetchers {
    fn new(input_fetchers: Vec<RangeFetcher>) -> Self {
        Self {
            num_fetchers: input_fetchers.len(),
            fetchers: input_fetchers,
        }
    }

    fn num_fetchers(&self) -> usize {
        self.num_fetchers
    }
}

pin_project! {
    pub struct MergedStream<St> {
        #[pin]
        inner: St,
        schema: SchemaRef,
    }
}


#[derive(Debug)]
pub(crate) struct SortedStreamMerger
{
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    // streams: MergingStreams,

    range_fetchers: MergingRangeFetchers,

    /// For each input stream maintain a dequeue of RecordBatches
    ///
    /// Exhausted batches will be popped off the front once all
    /// their rows have been yielded to the output
    batches: Vec<VecDeque<RecordBatch>>,

    /// Maintain a flag for each stream denoting if the current cursor
    /// has finished and needs to poll from the stream
    range_finished: Vec<bool>,

    // /// The accumulated row indexes for the next record batch
    // in_progress: Vec<RowIndex>,

    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,

    /// target batch size
    batch_size: usize,

    range_combiner: RangeCombiner,

    /// If the stream has encountered an error
    aborted: bool,

    /// An id to uniquely identify the input stream batch
    next_batch_id: usize,

    /// Vector that holds all [`SortKeyCursor`]s
    ranges: Vec<Option<SortKeyRangeInBatch>>,

    /// row converter
    row_converter: RowConverter,

}

impl SortedStreamMerger
{
    pub(crate) fn new_from_streams(
        streams: Vec<SortedStream>,
        schema: SchemaRef,
        expressions: &[PhysicalSortExpr],
        batch_size: usize,
    ) -> Result<Self> {
        let stream_count = streams.len();
        let batches = (0..stream_count)
            .into_iter()
            .map(|_| VecDeque::new())
            .collect();
        let wrappers:Vec<Fuse<SendableRecordBatchStream>> = streams.into_iter().map(|s| s.stream.fuse()).collect();

        let sort_fields = expressions
        .iter()
        .map(|expr| {
            let data_type = expr.expr.data_type(&schema)?;
            Ok(SortField::new_with_options(data_type, expr.options))
        })
        .collect::<Result<Vec<_>>>()?;
        let row_converter = RowConverter::new(sort_fields);


        let range_fetchers = (0..stream_count)
            .into_iter()
            .zip(wrappers)
            .map(|(stream_idx, stream)| RangeFetcher::new(stream_idx, stream, expressions, schema.clone()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema,
            batches,
            range_finished: vec![true; stream_count],
            // streams: MergingStreams::new(wrappers),
            range_fetchers: MergingRangeFetchers::new(range_fetchers),
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            aborted: false,
            // in_progress: vec![],
            next_batch_id: 0,
            ranges: (0..stream_count).into_iter().map(|_| None).collect(),
            range_combiner: RangeCombiner::new(),
            batch_size,
            row_converter,
        })
    }

    // fetch batches from all stream concurrently
    // used in initializing merger
    // after initialization, each stream would be prefetched(buffered)
    async fn init(&mut self) -> Result<()> {
        // self.range_combiner.init().await
        Ok(())
    }

    // async fn maybe_merge_waiting_ranges(&mut self, new_range: SmallVec<[SortKeyRange; 4]>) -> Option<RecordBatch> {
    //     self.waiting_ranges_to_merge.push(new_range);
    //     if self.waiting_ranges_to_merge.len() >= self.batch_size {
    //         // execute merge for currently collected ranges
    //     }
    //     None
    // }

    // pub async fn get_stream(&mut self) -> ArrowResult<SendableRecordBatchStream> {
    //     self.init().await?;

    //     let schema = self.schema.clone();
    //     // let stream = try_stream! {
    //     loop {
    //         let ranges_opt = self.range_combiner.next().await?;
    //         if let Some(ranges) = ranges_opt {
    //             match self.maybe_merge_waiting_ranges(ranges).await {
    //                 Some(rb) => {
    //                     println!("{:?}", rb);
    //                     break;
    //                 }
    //                 None => continue,
    //             };
    //         } else {
    //             break;
    //         }
    //     }
    //     // };
    //     // Ok(Box::pin(MergedStream::new(schema.clone(), stream)))
    //     Err(DivideByZero)
    // }


    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    fn maybe_poll_fetcher(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<ArrowResult<()>> {
        if !self.range_finished[idx] {
            // Cursor is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }
        let mut empty_range = false;
        {
            let fetcher = &mut self.range_fetchers.fetchers[idx];
            if fetcher.is_terminated() {
                return Poll::Ready(Ok(()));
            }

            // Fetch a new input record and create a RecordBatchRanges from it
            match futures::ready!(fetcher.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(())),
                Some(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Some(Ok(sort_key_range)) => {
                    if sort_key_range.is_empty() {
                        empty_range = true;
                    } else {
                        self.range_combiner.push(sort_key_range)
                    }
                    // if batch.num_rows() > 0 {
                    //     let cols = self
                    //         .column_expressions
                    //         .iter()
                    //         .map(|expr| {
                    //             Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()))
                    //         })
                    //         .collect::<Result<Vec<_>>>()?;

                    //     let rows = match self.row_converter.convert_columns(&cols) {
                    //         Ok(rows) => rows,
                    //         Err(e) => {
                    //             return Poll::Ready(Err(ArrowError::ExternalError(
                    //                 Box::new(e),
                    //             )));
                    //         }
                    //     };

                    //     let cursor = SortKeyCursor::new(
                    //         idx,
                    //         self.next_batch_id, // assign this batch an ID
                    //         rows,
                    //     );
                    //     self.next_batch_id += 1;
                    //     self.heap.push(Reverse(cursor));
                    //     self.cursor_finished[idx] = false;
                    //     self.batches[idx].push_back(batch)
                    // } else {
                    //     empty_batch = true;
                    // }

                }
            }
        }

        if empty_range {
            self.maybe_poll_fetcher(cx, idx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    /// Drains the in_progress SortKeyRangeInBatch, and builds a new RecordBatch from them
    ///
    /// Will then drop any batches for which all rows have been yielded to the output
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        // Mapping from stream index to the index of the first buffer from that stream

        // Merge all SortKeyRangeInBatch with specific MergeOp(RangeCombiner)
        todo!()
    }

}

impl SortedStreamMerger
{
    #[inline]
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        // Ensure all non-exhausted fetchers have a cursor from which
        // rows can be pulled
        for i in 0..self.range_fetchers.num_fetchers() {
            match futures::ready!(self.maybe_poll_fetcher(cx, i)) {
                Ok(_) => {}
                Err(e) => {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
            todo!()
        }

        
        // refer by https://docs.rs/datafusion/13.0.0/src/datafusion/physical_plan/sorts/sort_preserving_merge.rs.html#567-608
        loop {
            todo!()
        }
    }

}

impl Stream for SortedStreamMerger
{
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx)
    }
}



#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::collections::BTreeMap;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    
    use datafusion::prelude::SessionContext;
    use datafusion::physical_expr::PhysicalSortExpr;    
    use datafusion::physical_plan::expressions::col;


    use crate::sorted_merge::sorted_stream_merger::{SortedStream, SortedStreamMerger};
    use crate::sorted_merge::combiner::MinHeapSortKeyRangeCombiner;
    use crate::sorted_merge::fetcher::NonUniqueSortKeyRangeFetcher;

    #[tokio::test]
    async fn test_multi_file_merger() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let files:Vec<String> = vec![
            "/Users/ceng/PycharmProjects/write_parquet/small_1.parquet".to_string(),
            "/Users/ceng/PycharmProjects/write_parquet/small_2.parquet".to_string(),
        ];
        let mut streams = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            let mut stream = session_ctx
                .read_parquet(files[i].as_str(), Default::default())
                .await
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            streams.push(SortedStream::new(
                stream,
            ));
        }

        let schema = get_test_schema();
        let sort = vec![PhysicalSortExpr {
            expr: col("int0", &schema).unwrap(),
            options: SortOptions::default(),
        }];


        let merge_stream = SortedStreamMerger::new_from_streams(
            streams,
            schema,
            sort.as_slice(),
            1024,
        ).unwrap();

    }

    pub fn get_test_schema() -> SchemaRef {
        let schema = Schema::new(vec![
            Field::new("str0", DataType::Utf8, false),
            Field::new("str1", DataType::Utf8, false),
            Field::new("str2", DataType::Utf8, false),
            Field::new("str3", DataType::Utf8, false),
            Field::new("str4", DataType::Utf8, false),
            Field::new("str5", DataType::Utf8, false),
            Field::new("str6", DataType::Utf8, false),
            Field::new("str7", DataType::Utf8, false),
            Field::new("str8", DataType::Utf8, false),
            Field::new("str9", DataType::Utf8, false),
            Field::new("str10", DataType::Utf8, false),
            Field::new("str11", DataType::Utf8, false),
            Field::new("str12", DataType::Utf8, false),
            Field::new("str13", DataType::Utf8, false),
            Field::new("str14", DataType::Utf8, false),
            Field::new("int0", DataType::Int64, false),
            Field::new("int1", DataType::Int64, false),
            Field::new("int2", DataType::Int64, false),
            Field::new("int3", DataType::Int64, false),
            Field::new("int4", DataType::Int64, false),
            Field::new("int5", DataType::Int64, false),
            Field::new("int6", DataType::Int64, false),
            Field::new("int7", DataType::Int64, false),
            Field::new("int8", DataType::Int64, false),
            Field::new("int9", DataType::Int64, false),
            Field::new("int10", DataType::Int64, false),
            Field::new("int11", DataType::Int64, false),
            Field::new("int12", DataType::Int64, false),
            Field::new("int13", DataType::Int64, false),
            Field::new("int14", DataType::Int64, false),

        ]);
    
        Arc::new(schema)
    }
}