// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Reverse;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::sorted_merge::combiner::{RangeCombiner, RangeCombinerResult};
use crate::sorted_merge::merge_operator::MergeOperator;
use crate::sorted_merge::sort_key_range::SortKeyBatchRange;

use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{expressions::col, RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::DataFusionError::ArrowError;
use futures::stream::{Fuse, FusedStream};
use futures::{Stream, StreamExt};

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

#[derive(Debug)]
pub(crate) struct SortedStreamMerger {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    // streams: MergingStreams,
    streams: MergingStreams,

    /// Maintain a flag for each stream denoting if the current range
    /// has finished and needs to poll from the stream
    range_finished: Vec<bool>,

    // /// The accumulated row indexes for the next record batch
    // in_progress: Vec<RowIndex>,
    // The physical expressions to sort by
    column_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    range_combiner: RangeCombiner,

    // If the stream has encountered an error
    aborted: bool,

    // row converter
    row_converters: Vec<RowConverter>,

    batch_idx_counter: usize,

    initialized: Vec<bool>,
}

impl SortedStreamMerger {
    pub(crate) fn new_from_streams(
        streams: Vec<SortedStream>,
        target_schema: SchemaRef,
        primary_keys: Vec<String>,
        batch_size: usize,
        merge_operator: Vec<MergeOperator>,
    ) -> Result<Self> {
        let streams_num = streams.len();

        let expressions = streams
            .iter()
            .map(|stream| {
                let schema = stream.stream.schema();
                primary_keys
                    .iter()
                    .map(move |pk| col(pk.as_str(), &schema.clone()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        let row_converters = streams
            .iter()
            .map(|stream| {
                let schema = stream.stream.schema();
                let sort_fields = primary_keys
                    .iter()
                    .map(move |pk| {
                        let data_type = schema.field_with_name(pk.as_str()).unwrap().data_type().clone();
                        SortField::new(data_type)
                    })
                    .collect::<Vec<_>>();
                RowConverter::new(sort_fields).unwrap()
            })
            .collect::<Vec<_>>();

        let fields_map = streams
            .iter()
            .map(|s| {
                s.stream
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| target_schema.index_of(f.name()).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let fields_map = Arc::new(fields_map);

        let wrappers: Vec<Fuse<SendableRecordBatchStream>> = streams.into_iter().map(|s| s.stream.fuse()).collect();

        let combiner = RangeCombiner::new(
            target_schema.clone(),
            streams_num,
            fields_map,
            batch_size,
            merge_operator,
        );

        Ok(Self {
            schema: target_schema,
            range_finished: vec![true; streams_num],
            streams: MergingStreams::new(wrappers),
            column_expressions: expressions,
            aborted: false,
            range_combiner: combiner,
            row_converters,
            batch_idx_counter: 0,
            initialized: vec![false; streams_num],
        })
    }

    /// If the stream at the given index is not exhausted, and the last batch range for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// batch range for the stream from the returned result
    fn maybe_poll_stream(&mut self, cx: &mut Context<'_>, idx: usize) -> Poll<Result<()>> {
        if !self.range_finished[idx] {
            // Range is not finished - don't need a new RecordBatch yet
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
                        self.initialized[idx] = true;
                        let cols = self.column_expressions[idx]
                            .iter()
                            .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows())))
                            .collect::<Result<Vec<_>>>()?;
                        let rows = match self.row_converters[idx].convert_columns(&cols) {
                            Ok(rows) => rows,
                            Err(e) => {
                                return Poll::Ready(Err(ArrowError(e)));
                            }
                        };

                        self.batch_idx_counter += 1;
                        let (batch, rows) = (Arc::new(batch), Arc::new(rows));
                        let range = SortKeyBatchRange::new_and_init(0, idx, self.batch_idx_counter, batch, rows);

                        self.range_finished[idx] = false;

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

impl SortedStreamMerger {
    #[inline]
    fn poll_next_inner(self: &mut Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        // Ensure all non-exhausted streams have a range from which
        // rows can be pulled
        let mut pending = false;
        for i in 0..self.streams.num_streams() {
            if !self.initialized[i] {
                match self.maybe_poll_stream(cx, i) {
                    Poll::Ready(r) => match r {
                        Ok(_) => {}
                        Err(e) => {
                            self.aborted = true;
                            return Poll::Ready(Some(Err(e)));
                        }
                    },
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            // not all streams have been initialized, we have to wait
            return Poll::Pending;
        }

        // refer by https://docs.rs/datafusion/13.0.0/src/datafusion/physical_plan/sorts/sort_preserving_merge.rs.html#567-608
        loop {
            match self.range_combiner.poll_result() {
                RangeCombinerResult::Err(e) => {
                    return Poll::Ready(Some(Err(ArrowError(e))));
                }
                RangeCombinerResult::None => {
                    return Poll::Ready(None);
                }
                RangeCombinerResult::Range(Reverse(mut range)) => {
                    let stream_idx = range.stream_idx();
                    range.advance();

                    if !range.is_finished() {
                        self.range_combiner.push_range(Reverse(range))
                    } else {
                        // we should mark this stream uninitalized
                        // since its polling may return pending
                        self.initialized[stream_idx] = false;
                        self.range_finished[stream_idx] = true;
                        match futures::ready!(self.maybe_poll_stream(cx, stream_idx)) {
                            Ok(_) => {}
                            Err(e) => {
                                self.aborted = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                }
                RangeCombinerResult::RecordBatch(batch) => return Poll::Ready(Some(batch.map_err(ArrowError))),
            }
        }
    }
}

impl Stream for SortedStreamMerger {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
    use std::ops::Index;
    use std::sync::Arc;

    use arrow::array::as_primitive_array;
    use arrow::array::ArrayRef;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::Int64Type;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use arrow_array::Float64Array;
    use datafusion::assert_batches_eq;
    use datafusion::error::Result;
    use datafusion::execution::context::TaskContext;
    use datafusion::logical_expr::col as logical_col;
    use datafusion::physical_plan::{common, memory::MemoryExec, ExecutionPlan};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use comfy_table::{Cell, Table};

    use crate::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use crate::lakesoul_reader::LakeSoulReader;
    use crate::sorted_merge::merge_operator::MergeOperator;
    use crate::sorted_merge::sorted_stream_merger::{SortedStream, SortedStreamMerger};

    #[tokio::test]
    async fn test_multi_file_merger() {
        let session_config = SessionConfig::default().with_batch_size(32);
        let session_ctx = SessionContext::with_config(session_config);
        let project_dir = std::env::current_dir().unwrap();
        let files: Vec<String> = vec![
            project_dir
                .join("../../python/small_0.parquet")
                .into_os_string()
                .into_string()
                .unwrap(),
            project_dir
                .join("../../python/small_1.parquet")
                .into_os_string()
                .into_string()
                .unwrap(),
            project_dir
                .join("../../python/small_2.parquet")
                .into_os_string()
                .into_string()
                .unwrap(),
        ];
        let mut streams = Vec::with_capacity(files.len());
        for i in 0..files.len() {
            let stream = session_ctx
                .read_parquet(files[i].as_str(), Default::default())
                .await
                .unwrap()
                .sort(vec![logical_col("int0").sort(true, true)])
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            streams.push(SortedStream::new(stream));
        }

        let schema = get_test_file_schema();

        let merge_stream =
            SortedStreamMerger::new_from_streams(streams, schema, vec![String::from("int0")], 1024, vec![]).unwrap();
        let merged_result = common::collect(Box::pin(merge_stream)).await.unwrap();

        let mut all_rb = Vec::new();
        for i in 0..files.len() {
            let stream = session_ctx
                .read_parquet(files[i].as_str(), Default::default())
                .await
                .unwrap()
                .sort(vec![logical_col("int0").sort(true, true)])
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            let rb = common::collect(stream).await.unwrap();
            print_batches(&rb.clone()).unwrap();
            all_rb.extend(rb);
        }

        let expected_table = merge_with_use_last(&all_rb).unwrap();
        let expected_lines = expected_table
            .lines()
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

        for row_idx in 0..rows.len() {
            if row_idx == rows.len() - 1 || rows.index(row_idx)[0] != rows.index(row_idx + 1)[0] {
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

    fn create_batch_one_col_i32(name: &str, vec: &[i32]) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from(Vec::from(vec)));
        RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
    }

    async fn create_stream(batches: Vec<RecordBatch>, context: Arc<TaskContext>) -> Result<SortedStream> {
        let schema = batches[0].schema();
        let exec = MemoryExec::try_new(&[batches], schema.clone(), None).unwrap();
        let stream = exec.execute(0, context.clone()).unwrap();
        Ok(SortedStream::new(stream))
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
        let s1 = create_stream(vec![s1b1, s1b2, s1b3, s1b4, s1b5], task_ctx.clone())
            .await
            .unwrap();

        let s2b1 = create_batch_one_col_i32("a", &[3, 4]);
        let s2b2 = create_batch_one_col_i32("a", &[4, 5]);
        let s2b3 = create_batch_one_col_i32("a", &[]);
        let s2b4 = create_batch_one_col_i32("a", &[5]);
        let s2b5 = create_batch_one_col_i32("a", &[5, 7]);
        let s2 = create_stream(vec![s2b1, s2b2, s2b3, s2b4, s2b5], task_ctx.clone())
            .await
            .unwrap();

        let s3b1 = create_batch_one_col_i32("a", &[]);
        let s3b2 = create_batch_one_col_i32("a", &[5]);
        let s3b3 = create_batch_one_col_i32("a", &[5, 7]);
        let s3b4 = create_batch_one_col_i32("a", &[7, 9]);
        let s3b5 = create_batch_one_col_i32("a", &[]);
        let s3b6 = create_batch_one_col_i32("a", &[10]);
        let s3 = create_stream(vec![s3b1, s3b2, s3b3, s3b4, s3b5, s3b6], task_ctx.clone())
            .await
            .unwrap();

        let merge_stream =
            SortedStreamMerger::new_from_streams(vec![s1, s2, s3], schema, vec![String::from("a")], 2, vec![]).unwrap();
        let merged = common::collect(Box::pin(merge_stream)).await.unwrap();
        assert_batches_eq!(
            &[
                "+----+", "| a  |", "+----+", "| 1  |", "| 3  |", "| 4  |", "| 5  |", "| 6  |", "| 7  |", "| 9  |",
                "| 10 |", "+----+",
            ],
            &merged
        );
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).collect::<Vec<_>>();
        RecordBatch::try_from_iter(iter).unwrap()
    }

    fn create_batch(
        names: Vec<&str>,
        first_col_value: &[i32],
        second_col_value: &[i32],
        third_col_value: Vec<Option<f64>>,
        fourth_col_value: Vec<&str>,
    ) -> RecordBatch {
        let mut values: Vec<ArrayRef> = vec![];
        values.push(Arc::new(Int32Array::from(Vec::from(first_col_value))) as ArrayRef);
        values.push(Arc::new(Int32Array::from(Vec::from(second_col_value))) as ArrayRef);
        values.push(Arc::new(Float64Array::from(third_col_value)) as ArrayRef);
        values.push(Arc::new(StringArray::from(fourth_col_value)) as ArrayRef);
        let iter = names.into_iter().zip(values).collect::<Vec<_>>();
        RecordBatch::try_from_iter(iter).unwrap()
    }

    #[tokio::test]
    async fn test_sorted_stream_merger_multi_columns() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let s1b1 = create_batch_i32(
            vec!["id", "a"],
            vec![&[1, 1, 3, 3, 4], &[10001, 10002, 10003, 10004, 10005]],
        );
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
            ],
            &[s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()]
        );

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
            ],
            &[s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()]
        );
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
            ],
            &[
                s3b1.clone(),
                s3b2.clone(),
                s3b3.clone(),
                s3b4.clone(),
                s3b5.clone(),
                s3b6.clone()
            ]
        );

        let s1 = create_stream(
            vec![s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()],
            task_ctx.clone(),
        )
        .await
        .unwrap();
        let s2 = create_stream(
            vec![s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()],
            task_ctx.clone(),
        )
        .await
        .unwrap();
        let s3 = create_stream(
            vec![
                s3b1.clone(),
                s3b2.clone(),
                s3b3.clone(),
                s3b4.clone(),
                s3b5.clone(),
                s3b6.clone(),
            ],
            task_ctx.clone(),
        )
        .await
        .unwrap();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);

        let sort_fields = vec!["id"];

        let merge_stream = SortedStreamMerger::new_from_streams(
            vec![s1, s2, s3],
            Arc::new(schema),
            vec![String::from("id")],
            2,
            vec![],
        )
        .unwrap();
        let merged = common::collect(Box::pin(merge_stream)).await.unwrap();
        print_batches(&merged).unwrap();
    }

    #[tokio::test]
    async fn test_sorted_stream_merger_with_sum_and_last() {
        let session_config = SessionConfig::default().with_batch_size(2);
        let session_ctx = SessionContext::with_config(session_config);
        let task_ctx = session_ctx.task_ctx();
        let s1b1 = create_batch(
            vec!["id", "a", "b", "c"],
            &[1, 1, 3, 3, 4],
            &[1, 9, 3, 4, 9],
            vec![Some(1.2), Some(2.0), None, Some(4.8), Some(4.8)],
            vec!["1001", "102", "10003", "10004", "15"],
        );
        let s1b2 = create_batch(
            vec!["id", "a", "b", "c"],
            &[4, 5],
            &[9, 3],
            vec![Some(1.2), Some(2.3)],
            vec!["1006", "10007"],
        );
        let s1b3 = create_batch(vec!["id", "a", "b", "c"], &[], &[], vec![], vec![]);
        let s1b4 = create_batch(vec!["id", "a", "b", "c"], &[5], &[5], vec![Some(4.4)], vec!["100008"]);
        let s1b5 = create_batch(
            vec!["id", "a", "b", "c"],
            &[5, 5, 6],
            &[5, 5, 60],
            vec![Some(1.51), Some(1.52), Some(1.61)],
            vec!["10009", "10010", "10011"],
        );
        assert_batches_eq!(
            &[
                "+----+----+------+--------+",
                "| id | a  | b    | c      |",
                "+----+----+------+--------+",
                "| 1  | 1  | 1.2  | 1001   |",
                "| 1  | 9  | 2    | 102    |",
                "| 3  | 3  |      | 10003  |",
                "| 3  | 4  | 4.8  | 10004  |",
                "| 4  | 9  | 4.8  | 15     |",
                "| 4  | 9  | 1.2  | 1006   |",
                "| 5  | 3  | 2.3  | 10007  |",
                "| 5  | 5  | 4.4  | 100008 |",
                "| 5  | 5  | 1.51 | 10009  |",
                "| 5  | 5  | 1.52 | 10010  |",
                "| 6  | 60 | 1.61 | 10011  |",
                "+----+----+------+--------+",
            ],
            &[s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()]
        );

        let s2b1 = create_batch(
            vec!["id", "a", "b", "c"],
            &[3, 4],
            &[23, 13],
            vec![None, Some(3.5)],
            vec!["201", "200002"],
        );
        let s2b2 = create_batch(
            vec!["id", "a", "b", "c"],
            &[4, 5],
            &[9, 5],
            vec![Some(1.2), Some(2.3)],
            vec!["20003", "20004"],
        );
        let s2b3 = create_batch(vec!["id", "a", "b", "c"], &[], &[], vec![], vec![]);
        let s2b4 = create_batch(vec!["id", "a", "b", "c"], &[5], &[5], vec![Some(4.4)], vec!["20005"]);
        let s2b5 = create_batch(
            vec!["id", "a", "b", "c"],
            &[5, 7],
            &[5, 55],
            vec![Some(1.51), None],
            vec!["20006", "20007"],
        );
        assert_batches_eq!(
            &[
                "+----+----+------+--------+",
                "| id | a  | b    | c      |",
                "+----+----+------+--------+",
                "| 3  | 23 |      | 201    |",
                "| 4  | 13 | 3.5  | 200002 |",
                "| 4  | 9  | 1.2  | 20003  |",
                "| 5  | 5  | 2.3  | 20004  |",
                "| 5  | 5  | 4.4  | 20005  |",
                "| 5  | 5  | 1.51 | 20006  |",
                "| 7  | 55 |      | 20007  |",
                "+----+----+------+--------+",
            ],
            &[s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()]
        );

        let s3b1 = create_batch(vec!["id", "a", "b", "d"], &[], &[], vec![], vec![]);
        let s3b2 = create_batch(
            vec!["id", "a", "b", "d"],
            &[5, 5],
            &[5, 8],
            vec![Some(3.2), Some(3.2)],
            vec!["30001", "30002"],
        );
        let s3b3 = create_batch(
            vec!["id", "a", "b", "d"],
            &[5, 7],
            &[4, 10],
            vec![None, None],
            vec!["33", "30004"],
        );
        let s3b4 = create_batch(vec!["id", "a", "b", "d"], &[], &[], vec![], vec![]);
        let s3b5 = create_batch(
            vec!["id", "a", "b", "d"],
            &[7, 9],
            &[5, 90],
            vec![None, None],
            vec!["30005", "30006"],
        );
        let s3b6 = create_batch(
            vec!["id", "a", "b", "d"],
            &[10],
            &[100],
            vec![Some(1.51)],
            vec!["300007"],
        );
        assert_batches_eq!(
            &[
                "+----+-----+------+--------+",
                "| id | a   | b    | d      |",
                "+----+-----+------+--------+",
                "| 5  | 5   | 3.2  | 30001  |",
                "| 5  | 8   | 3.2  | 30002  |",
                "| 5  | 4   |      | 33     |",
                "| 7  | 10  |      | 30004  |",
                "| 7  | 5   |      | 30005  |",
                "| 9  | 90  |      | 30006  |",
                "| 10 | 100 | 1.51 | 300007 |",
                "+----+-----+------+--------+",
            ],
            &[
                s3b1.clone(),
                s3b2.clone(),
                s3b3.clone(),
                s3b4.clone(),
                s3b5.clone(),
                s3b6.clone()
            ]
        );

        let s1 = create_stream(
            vec![s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()],
            task_ctx.clone(),
        )
        .await
        .unwrap();
        let s2 = create_stream(
            vec![s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()],
            task_ctx.clone(),
        )
        .await
        .unwrap();
        let s3 = create_stream(
            vec![
                s3b1.clone(),
                s3b2.clone(),
                s3b3.clone(),
                s3b4.clone(),
                s3b5.clone(),
                s3b6.clone(),
            ],
            task_ctx.clone(),
        )
        .await
        .unwrap();

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]);

        let merge_stream = SortedStreamMerger::new_from_streams(
            vec![s1, s2, s3],
            Arc::new(schema),
            vec![String::from("id")],
            2,
            vec![
                MergeOperator::UseLast,
                MergeOperator::Sum,
                MergeOperator::UseLastNotNull,
                MergeOperator::UseLast,
                MergeOperator::UseLast,
            ],
        )
        .unwrap();
        let merged = common::collect(Box::pin(merge_stream)).await.unwrap();
        assert_batches_eq!(
            &[
                "+----+-----+------+-------+--------+",
                "| id | a   | b    | c     | d      |",
                "+----+-----+------+-------+--------+",
                "| 1  | 10  | 2    | 102   |        |",
                "| 3  | 30  | 4.8  | 201   |        |",
                "| 4  | 40  | 1.2  | 20003 |        |",
                "| 5  | 50  | 3.2  | 20006 | 33     |",
                "| 6  | 60  | 1.61 | 10011 |        |",
                "| 7  | 70  |      | 20007 | 30005  |",
                "| 9  | 90  |      |       | 30006  |",
                "| 10 | 100 | 1.51 |       | 300007 |",
                "+----+-----+------+-------+--------+",
            ],
            &merged
        );
    }

    #[tokio::test]
    async fn test_s3_file_merge() {
        let schema = Schema::new(vec![
            Field::new("uuid", DataType::Utf8, false),
            Field::new("ip", DataType::Utf8, true),
            Field::new("hostname", DataType::Utf8, true),
            Field::new("requests", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
            Field::new("job", DataType::Utf8, true),
            Field::new("phonenum", DataType::Utf8, true),
        ]);
        let conf = LakeSoulIOConfigBuilder::new()
            .with_primary_keys(vec!["uuid".to_string()])
            .with_files(vec![
                "s3://lakesoul-test-bucket/datalake_table/part-00000-c2c3071b-e566-4b2f-a67c-6648c311b9f5_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-bde11c74-f264-40cc-aa71-be7d61c2ed78_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-84dd2e3f-3bce-4dd3-b612-81791cbc701f_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-5813797f-8d93-420f-af9f-75ebeb655af9_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-659b7074-3547-43cb-b858-3867624c0236_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-ab29b003-5438-4b19-ba9d-067c0bf82800_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-c6efa765-91cc-4393-a7ef-6a53f1f0d777_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-391c2a2d-7c8c-4193-9539-ec881e046a23_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-44b1bdd7-6501-4056-aa1b-1851a40192ac_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-2c7f8088-cf5b-4418-94f1-41aece595c6b_00000.c000.parquet".to_string(),
                "s3://lakesoul-test-bucket/datalake_table/part-00000-4c90050a-6d97-423e-a90b-3385872a03a9_00000.c000.parquet".to_string(),
            ])
            .with_schema(Arc::new(schema))
            .with_thread_num(2)
            .with_batch_size(8192)
            .with_max_row_group_size(250000)
            .with_object_store_option("fs.s3a.access.key".to_string(), "minioadmin1".to_string())
            .with_object_store_option("fs.s3a.secret.key".to_string(), "minioadmin1".to_string())
            .with_object_store_option("fs.s3a.endpoint".to_string(), "http://localhost:9000".to_string())
            .build();
        let mut reader = LakeSoulReader::new(conf).unwrap();
        reader.start().await.unwrap();
        let mut len = 0;
        while let Some(rb) = reader.next_rb().await {
            let rb = rb.unwrap();
            len += rb.num_rows();
        }
        println!("total rows: {}", len);
    }

    #[tokio::test]
    async fn parquet_viewer() {
        let session_config = SessionConfig::default().with_batch_size(2);
        let session_ctx = SessionContext::with_config(session_config);
        let stream = session_ctx
            .read_parquet(
                "part-00000-58928ac0-5640-486e-bb94-8990262a1797_00000.c000.parquet",
                Default::default(),
            )
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();
        let rb = common::collect(stream).await.unwrap();
        println!(
            "{}",
            &rb.iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<usize>>()
                .iter()
                .sum::<usize>()
        );
        print_batches(&rb.clone()).expect("");
    }
}
