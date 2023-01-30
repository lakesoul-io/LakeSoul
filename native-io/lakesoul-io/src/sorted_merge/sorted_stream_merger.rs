use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::future::Ready;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::cmp::Reverse;
use std::collections::VecDeque;


use crate::sorted_merge::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
use crate::sorted_merge::combiner::{RangeCombiner, RangeCombinerResult};
use crate::sorted_merge::fetcher::{RangeFetcher, NonUniqueSortKeyRangeFetcher};
use crate::sorted_merge::sort_key_range::SortKeyBatchRange;

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


// Multiple ranges in consecutive batches of ONE stream with same primary key
// This is the unit to be sorted in min heap
pub struct SortKeyRange {
    // use small vector to avoid allocation on every row
    pub(crate) sort_key_ranges: SmallVec<[SortKeyBatchRange; 2]>,

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
        let mut new_range = SortKeyRange {
            sort_key_ranges: SmallVec::new(),
            stream_idx: source.stream_idx,
        };
        for i in 0..source.sort_key_ranges.len() {
            let batch_range = source.sort_key_ranges[i].clone();
            new_range.add_range_in_batch(batch_range);
        }
        new_range
    }

    pub fn add_range_in_batch(&mut self, range: SortKeyBatchRange) {
        self.sort_key_ranges.push(range)
    }

    pub fn current(&self) -> Row<'_> {
        self.sort_key_ranges.first().unwrap().current()
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

    streams: MergingStreams,

    /// For each input stream maintain a dequeue of RecordBatches
    ///
    /// Exhausted batches will be popped off the front once all
    /// their rows have been yielded to the output
    batches: Vec<VecDeque<RecordBatch>>,

    /// Maintain a flag for each stream denoting if the current cursor
    /// has finished and needs to poll from the stream
    window_finished: Vec<bool>,

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
    ranges: Vec<Option<SortKeyBatchRange>>,

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
        let streams_num = streams.len();
        let batches = (0..streams_num)
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


        // let range_fetchers = (0..stream_count)
        //     .into_iter()
        //     .zip(wrappers)
        //     .map(|(stream_idx, stream)| RangeFetcher::new(stream_idx, stream, expressions, schema.clone()))
        //     .collect::<Result<Vec<_>>>()?;

        let combiner = RangeCombiner::new(schema.clone(), streams_num, batch_size);

        Ok(Self {
            schema,
            batches,
            window_finished: vec![true; streams_num],
            streams: MergingStreams::new(wrappers),
            // range_fetchers: MergingRangeFetchers::new(range_fetchers),
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            aborted: false,
            // in_progress: vec![],
            next_batch_id: 0,
            ranges: (0..streams_num).into_iter().map(|_| None).collect(),
            range_combiner: combiner,
            batch_size,
            row_converter,
        })
    }

    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    fn maybe_poll_stream(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<ArrowResult<()>> {
        if !self.window_finished[idx] {
            // Cursor is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }
        let mut empty_batch = false;
        {
            let stream = &mut self.streams.streams[idx];
            if stream.is_terminated() {
                return Poll::Ready(Ok(()));
            }

            // Fetch a new input record and create a RecordBatchRanges from it
            match futures::ready!(stream.poll_next_unpin(cx)) {
                None => return Poll::Ready(Ok(())),
                Some(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        let cols = self
                            .column_expressions
                            .iter()
                            .map(|expr| {
                                Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let rows = match self.row_converter.convert_columns(&cols) {
                            Ok(rows) => rows,
                            Err(e) => {
                                return Poll::Ready(Err(ArrowError::ExternalError(
                                    Box::new(e),
                                )));
                            }
                        };
                        self.next_batch_id += 1;
                        self.batches[idx].push_back(batch.clone());
                        
                        let (batch, rows) = (Arc::new(batch), Arc::new(rows));
                        let range = SortKeyBatchRange::new_and_init(0, idx, batch.clone(), rows.clone());

                        self.window_finished[idx] = false;

                        self.range_combiner.push_range(Reverse(range));
                        
                    } else {
                        empty_batch = true;
                    }
                }
            }
        }

        if empty_batch {
            self.maybe_poll_stream(cx, idx)
        } else {
            Poll::Ready(Ok(()))
        }
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
        for i in 0..self.streams.num_streams() {
            match futures::ready!(self.maybe_poll_stream(cx, i)) {
                Ok(_) => {}
                Err(e) => {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
        
        // refer by https://docs.rs/datafusion/13.0.0/src/datafusion/physical_plan/sorts/sort_preserving_merge.rs.html#567-608
        loop {
            match self.range_combiner.poll_result() {
                RangeCombinerResult::Err(e) => return Poll::Ready(Some(Err(e))),
                RangeCombinerResult::None => return Poll::Ready(None),
                RangeCombinerResult::Range(Reverse(mut range)) => {
                    let stream_idx = range.stream_idx();
                    let batch = Arc::new(self.batches[stream_idx].back().unwrap());
                    let next_range = range.advance();

                    let mut window_finished = false;
                    if !range.is_finished() {
                        self.range_combiner.push_range(Reverse(range))
                    } else {
                        self.window_finished[stream_idx] = true;
                        match futures::ready!(self.maybe_poll_stream(cx, stream_idx)) {
                            Ok(_) => {}
                            Err(e) => {
                                self.aborted = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }

                    }
                },                
                RangeCombinerResult::RecordBatch(batch) => {
                     return Poll::Ready(Some(batch))
                }
                    
            }
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

impl RecordBatchStream for SortedStreamMerger {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}



#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::ops::Index;
    use std::collections::BTreeMap;

    use futures::stream::Fuse;
    use futures::StreamExt;
    use futures::TryStreamExt;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use arrow::array::{Int32Array, StringArray};
    use arrow::array::ArrayRef;
    use arrow::util::pretty::print_batches;
    use arrow::util::display::array_value_to_string;
    use arrow::array::as_primitive_array;
    use arrow::datatypes::Int64Type;

    use datafusion::error::Result;
    use datafusion::from_slice::FromSlice;
    use datafusion::prelude::{SessionContext, SessionConfig};
    use datafusion::execution::context::TaskContext;
    use datafusion::physical_expr::PhysicalSortExpr;    
    use datafusion::physical_plan::expressions::col;
    use datafusion::logical_plan::col as logical_col;
    use datafusion::assert_batches_eq;
    use datafusion::physical_plan::{SendableRecordBatchStream, memory::MemoryExec, ExecutionPlan, common};

    use comfy_table::{Cell, Table};



    use crate::sorted_merge::sorted_stream_merger::{SortedStream, SortedStreamMerger};
    use crate::sorted_merge::combiner::MinHeapSortKeyRangeCombiner;
    use crate::sorted_merge::fetcher::NonUniqueSortKeyRangeFetcher;

    #[tokio::test]
    async fn test_multi_file_merger() {
        let session_config = SessionConfig::default().with_batch_size(32);
        let session_ctx = SessionContext::with_config(session_config);
        let task_ctx = session_ctx.task_ctx();
        let files:Vec<String> = vec![
            "/Users/ceng/PycharmProjects/write_parquet/small_0.parquet".to_string(),
            "/Users/ceng/PycharmProjects/write_parquet/small_1.parquet".to_string(),
            "/Users/ceng/PycharmProjects/write_parquet/small_2.parquet".to_string(),
        ];
        let mut streams = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            let mut stream = session_ctx
                .read_parquet(files[i].as_str(), Default::default())
                .await
                .unwrap()
                .sort(vec![logical_col("int0").sort(true, true)])
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            streams.push(SortedStream::new(
                stream,
            ));
        }

        let schema = get_test_file_schema();
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
        let merged_result = common::collect(Box::pin(merge_stream)).await.unwrap();

        let mut all_rb = Vec::new();
        for i in 0..files.len() {
            let mut stream = session_ctx
                .read_parquet(files[i].as_str(), Default::default())
                .await
                .unwrap()
                .sort(vec![logical_col("int0").sort(true, true)])
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            let rb = common::collect(stream).await.unwrap();
            print_batches(&rb.clone());
            all_rb.extend(rb);
        }


        let expected_table = merge_with_use_last(&all_rb).unwrap();
        let expected_lines = expected_table.lines()
            .map(|line| String::from(line.trim_end()))
            .collect::<Vec<_>>();

        let formatted = arrow::util::pretty::pretty_format_batches(&merged_result)
            .unwrap()
            .to_string();

        let actual_lines: Vec<&str> = formatted.trim().lines().collect();
        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    
    }

    ///! merge a series of record batches into a table using use_last
    fn merge_with_use_last(results: &[RecordBatch]) -> Result<Table> {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        if results.is_empty() {
            return Ok(table);
        }

        let schema = results[0].schema();

        let mut header = Vec::new();
        for field in schema.fields() {
            header.push(Cell::new(field.name()));
        }
        table.set_header(header);
        
        let mut rows = Vec::new();
        for batch in results {
            for row in 0..batch.num_rows() {
                let mut cells = Vec::new();
                for col in 0..batch.num_columns() {
                    let column = batch.column(col);
                    let arr = as_primitive_array::<Int64Type>(column);
                    cells.push(arr.value(row));
                }
                rows.push(cells);
                // table.add_row(cells);
            }
        }

        rows.sort_by_key(|k| k[0]);

        for row_idx in  0..rows.len() {
            if row_idx == rows.len() - 1 || rows.index(row_idx)[0] != rows.index(row_idx + 1)[0]{
                table.add_row(rows.index(row_idx));
            }
        }

        Ok(table)
    }

    pub fn get_test_file_schema() -> SchemaRef {
        let schema = Schema::new(vec![
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

    use crate::sorted_merge::sort_key_range::SortKeyBatchRange;
    use crate::sorted_merge::combiner::RangeCombiner;

    fn create_batch_one_col_i32(name: &str, vec: &[i32]) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from_slice(vec));
        RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
    }

    async fn create_stream(
        stream_idx: usize,
        batches: Vec<RecordBatch>,
        context: Arc<TaskContext>,
        sort_fields: Vec<&str>,
    ) -> Result<SortedStream> {
        let schema = batches[0].schema();
        let sort_exprs: Vec<_> = sort_fields
            .into_iter()
            .map(|field| PhysicalSortExpr {
                expr: col(field, &schema).unwrap(),
                options: Default::default(),
            })
            .collect();
        let exec = MemoryExec::try_new(&[batches], schema.clone(), None).unwrap();
        let stream = exec.execute(0, context.clone()).unwrap();
        Ok(SortedStream::new(stream))
        // NonUniqueSortKeyRangeFetcher::new(stream_idx, fused, &sort_exprs, schema)
    }

    #[tokio::test]
    async fn test_sorted_stream_merger() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let s1b1 = create_batch_one_col_i32("a", &[1, 1, 3, 3, 4]);
        let schema = s1b1.schema();
        
        let s1b2 = create_batch_one_col_i32("a", &[4, 5]);
        let s1b3 = create_batch_one_col_i32("a", &[]);
        let s1b4 = create_batch_one_col_i32("a", &[5]);
        let s1b5 = create_batch_one_col_i32("a", &[5, 6, 6]);
        let s1 = create_stream(
            0,
            vec![s1b1, s1b2, s1b3, s1b4, s1b5],
            task_ctx.clone(),
            vec!["a"],
        ).await.unwrap();

        let s2b1 = create_batch_one_col_i32("a", &[3, 4]);
        let s2b2 = create_batch_one_col_i32("a", &[4, 5]);
        let s2b3 = create_batch_one_col_i32("a", &[]);
        let s2b4 = create_batch_one_col_i32("a", &[5]);
        let s2b5 = create_batch_one_col_i32("a", &[5, 7]);
        let s2 = create_stream(
            1,
            vec![s2b1, s2b2, s2b3, s2b4, s2b5],
            task_ctx.clone(),
            vec!["a"],
        ).await.unwrap();

        let s3b1 = create_batch_one_col_i32("a", &[]);
        let s3b2 = create_batch_one_col_i32("a", &[5]);
        let s3b3 = create_batch_one_col_i32("a", &[5, 7]);
        let s3b4 = create_batch_one_col_i32("a", &[7, 9]);
        let s3b5 = create_batch_one_col_i32("a", &[]);
        let s3b6 = create_batch_one_col_i32("a", &[10]);
        let s3 = create_stream(
            2,
            vec![s3b1, s3b2, s3b3, s3b4, s3b5, s3b6],
            task_ctx.clone(),
            vec!["a"],
        ).await.unwrap();

        let sort_fields = vec!["a"];
        let sort_exprs: Vec<_> = sort_fields
            .into_iter()
            .map(|field| PhysicalSortExpr {
                expr: col(field, &schema).unwrap(),
                options: Default::default(),
            })
            .collect();

        
        let merge_stream = SortedStreamMerger::new_from_streams(
            vec![s1, s2, s3],
            schema,
            &sort_exprs, 
            2).unwrap();
        let merged = common::collect(Box::pin(merge_stream)).await.unwrap();
        assert_batches_eq!(
            &[
                "+----+",
                "| a  |",
                "+----+",
                "| 1  |",
                "| 3  |",
                "| 4  |",
                "| 5  |",
                "| 6  |",
                "| 7  |",
                "| 9  |",
                "| 10 |",
                "+----+",
            ]
            , &merged);
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values = values.into_iter().map(|vec| Arc::new(Int32Array::from_slice(vec))as ArrayRef).collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).collect::<Vec<_>>();
        RecordBatch::try_from_iter(iter).unwrap()
    }

    fn create_batch_string(names: Vec<&str>, id_value: &[i32], str_value: Vec<&str>) -> RecordBatch {
        let mut values: Vec<ArrayRef> = vec![];
        values.push(Arc::new(Int32Array::from_slice(id_value)) as ArrayRef);
        values.push(Arc::new(StringArray::from(str_value)) as ArrayRef);
        let iter = names.into_iter().zip(values).collect::<Vec<_>>();
        RecordBatch::try_from_iter(iter).unwrap()
    }

    #[tokio::test]
    async fn test_sorted_stream_merger_multi_columns() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let s1b1 = create_batch_i32(vec!["id", "a"], vec![&[1, 1, 3, 3, 4], &[10001,10002,10003,10004,10005]]);
        let s1b2 = create_batch_i32(vec!["id", "a"], vec![&[4, 5], &[10006, 10007]]);
        let s1b3 = create_batch_i32(vec!["id", "a"], vec![&[], &[]]);
        let s1b4 = create_batch_i32(vec!["id", "a"], vec![&[5], &[10008]]);
        let s1b5 = create_batch_i32(vec!["id", "a"], vec![&[5, 5, 6], &[10009, 10010, 10011]]);
        assert_batches_eq!(
            &[
                "+----+-------+",
                "| id | a     |",
                "+----+-------+",
                "| 1  | 10001 |",
                "| 1  | 10002 |",
                "| 3  | 10003 |",
                "| 3  | 10004 |",
                "| 4  | 10005 |",
                "| 4  | 10006 |",
                "| 5  | 10007 |",
                "| 5  | 10008 |",
                "| 5  | 10009 |",
                "| 5  | 10010 |",
                "| 6  | 10011 |",
                "+----+-------+",
            ]
            , &[s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()]);

        
        let s2b1 = create_batch_i32(vec!["id", "b"], vec![&[3, 4], &[20001, 20002]]);
        let s2b2 = create_batch_i32(vec!["id", "b"], vec![&[4, 5], &[20003, 20004]]);
        let s2b3 = create_batch_i32(vec!["id", "b"], vec![&[], &[]]);
        let s2b4 = create_batch_i32(vec!["id", "b"], vec![&[5], &[20005]]);
        let s2b5 = create_batch_i32(vec!["id", "b"], vec![&[5, 7], &[20006, 20007]]);
        assert_batches_eq!(
            &[
                "+----+-------+",
                "| id | b     |",
                "+----+-------+",
                "| 3  | 20001 |",
                "| 4  | 20002 |",
                "| 4  | 20003 |",
                "| 5  | 20004 |",
                "| 5  | 20005 |",
                "| 5  | 20006 |",
                "| 7  | 20007 |",
                "+----+-------+",
            ]
            , &[s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()]);
        let s3b1 = create_batch_i32(vec!["id", "c"], vec![&[], &[]]);
        let s3b2 = create_batch_i32(vec!["id", "c"], vec![&[5, 5], &[30001, 30002]]);
        let s3b3 = create_batch_i32(vec!["id", "c"], vec![&[5, 7], &[30003, 30004]]);
        let s3b4 = create_batch_i32(vec!["id", "c"], vec![&[], &[]]);
        let s3b5 = create_batch_i32(vec!["id", "c"], vec![&[7, 9], &[30005, 30006]]);
        let s3b6 = create_batch_i32(vec!["id", "c"], vec![&[10], &[30007]]);
        assert_batches_eq!(
            &[
                "+----+-------+",
                "| id | c     |",
                "+----+-------+",
                "| 5  | 30001 |",
                "| 5  | 30002 |",
                "| 5  | 30003 |",
                "| 7  | 30004 |",
                "| 7  | 30005 |",
                "| 9  | 30006 |",
                "| 10 | 30007 |",
                "+----+-------+",
            ]
            , &[s3b1.clone(), s3b2.clone(), s3b3.clone(), s3b4.clone(), s3b5.clone(), s3b6.clone()]);
        
        let s1 = create_stream(
            1,
            vec![s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()],
            task_ctx.clone(),
            vec!["id"],
        ).await.unwrap();
        let s2 = create_stream(
            2,
            vec![s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()],
            task_ctx.clone(),
            vec!["id"],
        ).await.unwrap();
        let s3 = create_stream(
            3,
            vec![s3b1.clone(), s3b2.clone(), s3b3.clone(), s3b4.clone(), s3b5.clone(), s3b6.clone()],
            task_ctx.clone(),
            vec!["id"],
        ).await.unwrap();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false), 
            Field::new("a", DataType::Int32, true), 
            Field::new("b", DataType::Int32, true), 
            Field::new("c", DataType::Int32, true), ]);

        let sort_fields = vec!["id"];
        let sort_exprs: Vec<_> = sort_fields
            .into_iter()
            .map(|field| PhysicalSortExpr {
                expr: col(field, &schema).unwrap(),
                options: Default::default(),
            })
            .collect();

        
        let merge_stream = SortedStreamMerger::new_from_streams(
            vec![s1, s2, s3],
            Arc::new(schema),
            &sort_exprs, 
            2)
            .unwrap();
        let merged = common::collect(Box::pin(merge_stream)).await.unwrap();
        assert_batches_eq!(
            &[
                "+----+-------+-------+-------+",
                "| id | a     | b     | c     |",
                "+----+-------+-------+-------+",
                "| 1  | 10002 |       |       |",
                "| 3  | 10004 | 20001 |       |",
                "| 4  | 10006 | 20003 |       |",
                "| 5  | 10010 | 20006 | 30003 |",
                "| 6  | 10011 |       |       |",
                "| 7  |       | 20007 | 30005 |",
                "| 9  |       |       | 30006 |",
                "| 10 |       |       | 30007 |",
                "+----+-------+-------+-------+",
            ], 
            &merged);
    }

    #[tokio::test]
    async fn test_sorted_stream_merger_with_string() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let s1b1 = create_batch_string(vec!["id", "a"], &[1, 1, 3, 3, 4], vec!["1001", "102", "10003", "10004", "15"]);
        let s1b2 = create_batch_string(vec!["id", "a"], &[4, 5], vec!["1006", "10007"]);
        let s1b3 = create_batch_string(vec!["id", "a"], &[], vec![]);
        let s1b4 = create_batch_string(vec!["id", "a"], &[5], vec!["100008"]);
        let s1b5 = create_batch_string(vec!["id", "a"], &[5, 5, 6], vec!["10009", "10010", "10011"]);
        assert_batches_eq!(
            &[
                "+----+--------+",
                "| id | a      |",
                "+----+--------+",
                "| 1  | 1001   |",
                "| 1  | 102    |",
                "| 3  | 10003  |",
                "| 3  | 10004  |",
                "| 4  | 15     |",
                "| 4  | 1006   |",
                "| 5  | 10007  |",
                "| 5  | 100008 |",
                "| 5  | 10009  |",
                "| 5  | 10010  |",
                "| 6  | 10011  |",
                "+----+--------+",
            ]
            , &[s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()]);

        
        let s2b1 = create_batch_string(vec!["id", "b"], &[3, 4], vec!["201", "200002"]);
        let s2b2 = create_batch_string(vec!["id", "b"], &[4, 5], vec!["20003", "20004"]);
        let s2b3 = create_batch_string(vec!["id", "b"], &[], vec![]);
        let s2b4 = create_batch_string(vec!["id", "b"], &[5], vec!["20005"]);
        let s2b5 = create_batch_string(vec!["id", "b"], &[5, 7], vec!["20006", "20007"]);
        assert_batches_eq!(
            &[
                "+----+--------+",
                "| id | b      |",
                "+----+--------+",
                "| 3  | 201    |",
                "| 4  | 200002 |",
                "| 4  | 20003  |",
                "| 5  | 20004  |",
                "| 5  | 20005  |",
                "| 5  | 20006  |",
                "| 7  | 20007  |",
                "+----+--------+",
            ]
            , &[s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()]);
        let s3b1 = create_batch_string(vec!["id", "c"], &[], vec![]);
        let s3b2 = create_batch_string(vec!["id", "c"], &[5, 5], vec!["30001", "30002"]);
        let s3b3 = create_batch_string(vec!["id", "c"], &[5, 7], vec!["33", "30004"]);
        let s3b4 = create_batch_string(vec!["id", "c"], &[], vec![]);
        let s3b5 = create_batch_string(vec!["id", "c"], &[7, 9], vec!["30005", "30006"]);
        let s3b6 = create_batch_string(vec!["id", "c"], &[10], vec!["300007"]);
        assert_batches_eq!(
            &[
                "+----+--------+",
                "| id | c      |",
                "+----+--------+",
                "| 5  | 30001  |",
                "| 5  | 30002  |",
                "| 5  | 33     |",
                "| 7  | 30004  |",
                "| 7  | 30005  |",
                "| 9  | 30006  |",
                "| 10 | 300007 |",
                "+----+--------+",
            ]
            , &[s3b1.clone(), s3b2.clone(), s3b3.clone(), s3b4.clone(), s3b5.clone(), s3b6.clone()]);
        
        let s1 = create_stream(
            1,
            vec![s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()],
            task_ctx.clone(),
            vec!["id"],
        ).await.unwrap();
        let s2 = create_stream(
            2,
            vec![s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()],
            task_ctx.clone(),
            vec!["id"],
        ).await.unwrap();
        let s3 = create_stream(
            3,
            vec![s3b1.clone(), s3b2.clone(), s3b3.clone(), s3b4.clone(), s3b5.clone(), s3b6.clone()],
            task_ctx.clone(),
            vec!["id"],
        ).await.unwrap();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false), 
            Field::new("a", DataType::Utf8, true), 
            Field::new("b", DataType::Utf8, true), 
            Field::new("c", DataType::Utf8, true), ]);

        let sort_fields = vec!["id"];
        let sort_exprs: Vec<_> = sort_fields
            .into_iter()
            .map(|field| PhysicalSortExpr {
                expr: col(field, &schema).unwrap(),
                options: Default::default(),
            })
            .collect();

        
        let merge_stream = SortedStreamMerger::new_from_streams(
            vec![s1, s2, s3],
            Arc::new(schema),
            &sort_exprs, 
            2)
            .unwrap();
        let merged = common::collect(Box::pin(merge_stream)).await.unwrap();
        assert_batches_eq!(
            &[
                "+----+-------+-------+--------+",
                "| id | a     | b     | c      |",
                "+----+-------+-------+--------+",
                "| 1  | 102   |       |        |",
                "| 3  | 10004 | 201   |        |",
                "| 4  | 1006  | 20003 |        |",
                "| 5  | 10010 | 20006 | 33     |",
                "| 6  | 10011 |       |        |",
                "| 7  |       | 20007 | 30005  |",
                "| 9  |       |       | 30006  |",
                "| 10 |       |       | 300007 |",
                "+----+-------+-------+--------+",
            ], 
            &merged);
    }
}