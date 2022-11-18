use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::future::Ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
use arrow::error::ArrowError::DivideByZero;
use arrow::row::{Row, Rows};
use arrow::{error::Result as ArrowResult, record_batch::RecordBatch};
use async_stream::try_stream;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{Buffered, Map};
use futures::{Stream, StreamExt};
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
    streams: Vec<BufferedRecordBatchStream>,
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
    fn new(input_streams: Vec<BufferedRecordBatchStream>) -> Self {
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

impl<St> MergedStream<St>
where
    St: Stream<Item = ArrowResult<RecordBatch>>,
{
    pub fn new(schema: SchemaRef, stream: St) -> Self {
        MergedStream { inner: stream, schema }
    }
}

impl<St> Stream for MergedStream<St>
where
    St: Stream<Item = ArrowResult<RecordBatch>>,
{
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.as_mut().poll_next(cx)
    }
}

impl<St> RecordBatchStream for MergedStream<St>
where
    St: Stream<Item = ArrowResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub(crate) struct SortedStreamMerger<RangeCombiner> {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// target batch size
    batch_size: usize,

    waiting_ranges_to_merge: Vec<SmallVec<[SortKeyRange; 4]>>,

    range_combiner: RangeCombiner,
}

impl<RangeCombiner> SortedStreamMerger<RangeCombiner>
where
    RangeCombiner: StreamSortKeyRangeCombiner + Send + 'static,
{
    pub(crate) fn new_from_streams(
        streams: Vec<SortedStream>,
        schema: SchemaRef,
        expressions: &[PhysicalSortExpr],
        batch_size: usize,
    ) -> Result<Self> {
        let stream_count = streams.len();
        let mut wrappers: Vec<BufferedRecordBatchStream> = Vec::with_capacity(stream_count);
        streams.into_iter().for_each(|s| {
            let ready_futures = s
                .stream
                .map(std::future::ready as fn(ArrowResult<RecordBatch>) -> Ready<ArrowResult<RecordBatch>>);
            let bufferred = ready_futures.buffered(2);
            wrappers.push(bufferred);
        });

        let range_fetchers = (0..stream_count)
            .into_iter()
            .zip(wrappers)
            .map(|(stream_idx, stream)| RangeCombiner::Fetcher::new(stream_idx, stream, expressions, schema.clone()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema,
            batch_size,
            waiting_ranges_to_merge: Vec::with_capacity(batch_size),
            range_combiner: RangeCombiner::with_fetchers(range_fetchers),
        })
    }

    // fetch batches from all stream concurrently
    // used in initializing merger
    // after initialization, each stream would be prefetched(buffered)
    async fn init(&mut self) -> Result<()> {
        self.range_combiner.init().await
    }

    async fn maybe_merge_waiting_ranges(&mut self, new_range: SmallVec<[SortKeyRange; 4]>) -> Option<RecordBatch> {
        self.waiting_ranges_to_merge.push(new_range);
        if self.waiting_ranges_to_merge.len() >= self.batch_size {
            // execute merge for currently collected ranges
        }
        None
    }

    pub async fn get_stream(&mut self) -> ArrowResult<SendableRecordBatchStream> {
        self.init().await?;

        let schema = self.schema.clone();
        // let stream = try_stream! {
        loop {
            let ranges_opt = self.range_combiner.next().await?;
            if let Some(ranges) = ranges_opt {
                match self.maybe_merge_waiting_ranges(ranges).await {
                    Some(rb) => {
                        println!("{:?}", rb);
                        break;
                    }
                    None => continue,
                };
            } else {
                break;
            }
        }
        // };
        // Ok(Box::pin(MergedStream::new(schema.clone(), stream)))
        Err(DivideByZero)
    }
}
