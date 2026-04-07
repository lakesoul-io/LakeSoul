// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! This module provides functionality for sorted stream merger.
//! Which is referred by `SortPreservingMergeExec` in DataFusion.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::*;
use arrow::record_batch::RecordBatch;
use arrow_array::{
    BinaryArray, LargeBinaryArray, LargeStringArray, StringArray, StringViewArray,
    downcast_primitive,
};
use arrow_schema::{DataType, SortOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::{
    RecordBatchStream, SendableRecordBatchStream, expressions::col,
};
use datafusion_common::DataFusionError;
use futures::stream::{Fuse, FusedStream};
use futures::{Stream, StreamExt};
use rootcause::compat::boxed_error::IntoBoxedError;
use tokio::sync::Mutex;

use super::combiner::*;
use super::cursor::{ArrayValues, CursorArray, CursorValues, RowValues};
use super::merge_operator::MergeOperator;
use super::sort_key_range::SortKeyBatchRange;
use super::v2::window_sliding_merger::WindowSlidingMerger;
use crate::Result;
use crate::stream::{
    FieldCursorStream, RowCursorStream, default_column::DefaultColumnStream,
};

/// A wrapper of sorted stream.
pub(crate) struct SortedStream {
    stream: SendableRecordBatchStream,
}

impl Debug for SortedStream {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InMemSortedStream")
    }
}

impl SortedStream {
    pub(crate) fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

impl Stream for SortedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

pub(crate) type CursorStream<C> =
    Pin<Box<dyn Stream<Item = Result<(C, RecordBatch)>> + Send>>;

/// A wrapper of sorted input streams to merge together.
struct MergingStreams<C: CursorValues> {
    /// The sorted input streams to merge together
    streams: Vec<Fuse<CursorStream<C>>>,
    /// number of streams
    num_streams: usize,
}

impl<C: CursorValues> Debug for MergingStreams<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergingStreams")
            .field("num_streams", &self.num_streams)
            .finish()
    }
}

impl<C: CursorValues> MergingStreams<C> {
    fn new(input_streams: Vec<Fuse<CursorStream<C>>>) -> Self {
        Self {
            num_streams: input_streams.len(),
            streams: input_streams,
        }
    }

    fn num_streams(&self) -> usize {
        self.num_streams
    }
}

/// Struct of sorted stream merger.
#[derive(Debug)]
pub(crate) struct SortedStreamMerger<C: CursorValues, R: RangeCombinerTrait<C>> {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    // streams: MergingStreams,
    streams: MergingStreams<C>,

    /// Maintain a flag for each stream denoting if the current range
    /// has finished and needs to poll from the stream
    range_finished: Vec<bool>,

    /// The [`RangeCombiner`] of sorted stream
    range_combiner: R,

    /// If the stream has encountered an error
    aborted: bool,

    /// The accumulated indexes for the next record batch
    batch_idx_counter: usize,

    /// The initialized flag for each stream
    initialized: Vec<bool>,
}

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(PrimitiveArray<$t>, $($v),+)
    };
}

macro_rules! merge_helper {
    ($t:ty, $streams:ident, $col_name:ident, $physical_schema:ident, $merged_schema:ident, $batch_size:ident, $default_column_value:ident, $reservation:ident, $merge_operator:ident, $fields_map:ident, $is_compacted:ident) => {{
        let streams = $streams
            .into_iter()
            .map(|s| {
                let stream = s.stream;
                let schema = stream.schema();
                let col_expr = col($col_name, schema.as_ref())?;
                let stream = FieldCursorStream::<$t>::new(
                    PhysicalSortExpr::new(col_expr, SortOptions::default()),
                    stream,
                    $reservation.new_empty(),
                );
                let stream: CursorStream<ArrayValues<<$t as CursorArray>::Values>> =
                    Box::pin(stream);
                Ok(stream)
            })
            .collect::<Result<Vec<_>>>()?;
        create_merger!(
            ArrayValues<<$t as CursorArray>::Values>,
            streams,
            $physical_schema,
            $merged_schema,
            $fields_map,
            $batch_size,
            $merge_operator,
            $default_column_value,
            $is_compacted
        );
    }};
}

macro_rules! create_merger {
    ($t:ty, $streams:ident, $physical_schema:ident, $merged_schema:ident, $fields_map:ident, $batch_size:ident, $merge_operator:ident, $default_column_value:ident, $is_compacted:ident) => {{
        let streams_num = $streams.len();
        if $merge_operator.is_empty()
            || $merge_operator
                .iter()
                .all(|op| *op == MergeOperator::UseLast)
        {
            let is_partial_merge = $fields_map
                .iter()
                .enumerate()
                .any(|(i, f)| {
                    let is_partial_merge = f.len() != $physical_schema.fields().len();
                    if is_partial_merge {
                        info!("{}th stream is partial merge, field count: {}, physical schema count: {}",
                            i, f.len(), $physical_schema.fields().len());
                    }
                    is_partial_merge
                });
            info!("lakesoul is_partial_merge: {}", is_partial_merge);
            if is_partial_merge {
                let combiner = UseLastRangeCombiner::<$t, true>::new(
                    $physical_schema.clone(),
                    streams_num,
                    $fields_map,
                    $batch_size,
                );
                let merge_stream = SortedStreamMerger::new_from_streams(
                    $streams,
                    $physical_schema,
                    combiner,
                )?;
                return Ok(Box::pin(
                    DefaultColumnStream::new_from_streams_with_default(
                        vec![Box::pin(merge_stream)],
                        $merged_schema,
                        $default_column_value,
                    ),
                ));
            } else {
                let use_v2_merge: bool = std::env::var("LAKESOUL_IO_USE_V2_MERGE")
                    .unwrap_or("false".into())
                    .parse()
                    .unwrap_or(false);
                info!("lakesoul use_v2_merge: {}", use_v2_merge);
                if !use_v2_merge {
                    info!("lakesoul using old row wise combiner");
                    let combiner = UseLastRangeCombiner::<$t, false>::new(
                        $physical_schema.clone(),
                        streams_num,
                        $fields_map,
                        $batch_size,
                    );
                    let merge_stream = SortedStreamMerger::new_from_streams(
                        $streams,
                        $physical_schema,
                        combiner,
                    )?;
                    return Ok(Box::pin(
                        DefaultColumnStream::new_from_streams_with_default(
                            vec![Box::pin(merge_stream)],
                            $merged_schema,
                            $default_column_value,
                        ),
                    ));
                } else {
                    info!("lakesoul using new batch wise combiner");
                    let combiner = Arc::new(Mutex::new(WindowSlidingMerger::new(
                        $streams
                            .into_iter()
                            .enumerate()
                            .map(|(i, s)| (s, !$is_compacted[i]))
                            .collect(),
                        $physical_schema,
                        streams_num,
                        $batch_size,
                    )?));
                    let merge_stream =
                        WindowSlidingMerger::build_merged_stream(combiner)?;
                    return Ok(Box::pin(
                        DefaultColumnStream::new_from_streams_with_default(
                            vec![merge_stream],
                            $merged_schema,
                            $default_column_value,
                        ),
                    ));
                }
            }
        } else {
            let combiner = MinHeapSortKeyBatchRangeCombiner::new(
                $physical_schema.clone(),
                streams_num,
                $fields_map,
                $batch_size,
                $merge_operator,
            );
            let merge_stream = SortedStreamMerger::new_from_streams(
                $streams,
                $physical_schema,
                combiner,
            )?;
            return Ok(Box::pin(
                DefaultColumnStream::new_from_streams_with_default(
                    vec![Box::pin(merge_stream)],
                    $merged_schema,
                    $default_column_value,
                ),
            ));
        }
    }};
}

pub(crate) fn build_sorted_stream_merger(
    streams: Vec<SortedStream>,
    primary_keys: Arc<Vec<String>>,
    physical_schema: SchemaRef,
    merged_schema: SchemaRef,
    batch_size: usize,
    default_column_value: Arc<HashMap<String, String>>,
    merge_operator: Vec<MergeOperator>,
    reservation: MemoryReservation,
    is_compacted: Vec<bool>,
) -> Result<SendableRecordBatchStream> {
    let fields_map = streams
        .iter()
        .map(|s| {
            s.stream
                .schema()
                .fields()
                .iter()
                .map(|f| Ok(physical_schema.index_of(f.name())?))
                .collect::<Result<Vec<usize>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let fields_map = Arc::new(fields_map);

    // for single column pk with primitive data type,
    // use FieldCursorStream to avoid RowConverter overhead
    if primary_keys.len() == 1 {
        let col_name = primary_keys[0].as_str();
        let data_type = physical_schema.field_with_name(col_name)?.data_type();
        downcast_primitive! {
            data_type => (primitive_merge_helper, streams, col_name, physical_schema, merged_schema, batch_size, default_column_value, reservation, merge_operator, fields_map, is_compacted),
            DataType::Utf8 => merge_helper!(StringArray, streams, col_name, physical_schema, merged_schema, batch_size, default_column_value, reservation, merge_operator, fields_map, is_compacted),
            DataType::Utf8View => merge_helper!(StringViewArray, streams, col_name, physical_schema, merged_schema, batch_size, default_column_value, reservation, merge_operator, fields_map, is_compacted),
            DataType::LargeUtf8 => merge_helper!(LargeStringArray, streams, col_name, physical_schema, merged_schema, batch_size, default_column_value, reservation, merge_operator, fields_map, is_compacted),
            DataType::Binary => merge_helper!(BinaryArray, streams, col_name, physical_schema, merged_schema, batch_size, default_column_value, reservation, merge_operator, fields_map, is_compacted),
            DataType::LargeBinary => merge_helper!(LargeBinaryArray, streams, col_name, physical_schema, merged_schema, batch_size, default_column_value, reservation, merge_operator, fields_map, is_compacted),
            _ => {}
        }
    }

    // For single column pk with unsupport data type,
    // or multi-column pk, use RowCusorStream
    let streams = streams
        .into_iter()
        .map(|s| {
            let stream = s.stream;
            let schema = stream.schema();
            let sort_exprs = primary_keys
                .iter()
                .map(|k| {
                    let col_expr = col(k.as_str(), &schema)?;
                    Ok(PhysicalSortExpr::new(col_expr, SortOptions::default()))
                })
                .collect::<Result<Vec<_>>>()?;
            let stream = RowCursorStream::try_new(
                schema.as_ref(),
                &LexOrdering::new(sort_exprs)
                    .ok_or(DataFusionError::Execution("empty sort expr".into()))?,
                stream,
                reservation.new_empty(),
            )?;
            let stream: CursorStream<RowValues> = Box::pin(stream);
            Ok(stream)
        })
        .collect::<Result<Vec<_>>>()?;
    create_merger!(
        RowValues,
        streams,
        physical_schema,
        merged_schema,
        fields_map,
        batch_size,
        merge_operator,
        default_column_value,
        is_compacted
    );
}

impl<C: CursorValues, R: RangeCombinerTrait<C>> SortedStreamMerger<C, R> {
    /// Create a new sorted stream merger from a list of sorted streams.
    ///
    /// # Arguments
    ///
    /// * `streams` - A list of sorted streams to merge.
    /// * `target_schema` - The schema of the RecordBatches yielded by this stream.
    /// * `primary_keys` - The primary keys of the RecordBatches.
    /// * `batch_size` - The batch size of the RecordBatches.
    /// * `merge_operator` - The merge operator to use.
    pub(crate) fn new_from_streams(
        streams: Vec<CursorStream<C>>,
        target_schema: SchemaRef,
        range_combiner: R,
    ) -> Result<Self> {
        let streams_num = streams.len();

        Ok(Self {
            schema: target_schema,
            range_finished: vec![true; streams_num],
            streams: MergingStreams::new(streams.into_iter().map(|s| s.fuse()).collect()),
            aborted: false,
            range_combiner,
            batch_idx_counter: 0,
            initialized: vec![false; streams_num],
        })
    }

    /// If the stream at the given index is not exhausted, and the last batch range for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// batch range for the stream from the returned result
    #[instrument(skip(self, cx))]
    fn maybe_poll_stream(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<Result<()>> {
        if !self.range_finished[idx] {
            // Range is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }
        let mut empty_batch = false;
        {
            let stream = &mut self.streams.streams[idx];
            if stream.is_terminated() {
                debug!("stream[{idx}] terminated");
                return Poll::Ready(Ok(()));
            }

            // Fetch a new input record and create a RecordBatchRanges from it
            match futures::ready!(stream.poll_next_unpin(cx)) {
                None => {
                    return {
                        debug!("stream[{idx}] exhausted");
                        Poll::Ready(Ok(()))
                    };
                }
                Some(Err(e)) => {
                    error!("{e}");
                    return Poll::Ready(Err(e));
                }
                Some(Ok(batch)) => {
                    let (cursor_values, batch) = batch;
                    if batch.num_rows() > 0 {
                        self.initialized[idx] = true;

                        self.batch_idx_counter += 1;
                        let range = SortKeyBatchRange::new_and_init(
                            0,
                            idx,
                            self.batch_idx_counter,
                            Arc::new(batch),
                            Arc::new(cursor_values),
                        );

                        self.range_finished[idx] = false;
                        debug!("push range in {}:{}", file!(), line!());
                        self.range_combiner.push_range(range);
                    } else {
                        debug!("empty batch");
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

impl<C: CursorValues, R: RangeCombinerTrait<C>> SortedStreamMerger<C, R> {
    #[inline]
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            debug!("inner stream aborted");
            return Poll::Ready(None);
        }

        // Ensure all non-exhausted streams have a range from which
        // rows can be pulled
        let mut pending = false;
        for i in 0..self.streams.num_streams() {
            if !self.initialized[i] {
                debug!("uninitialized stream[{}]", i);
                match self.maybe_poll_stream(cx, i) {
                    Poll::Ready(r) => match r {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{}", e);
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
                    error!("{}", e);
                    return Poll::Ready(Some(Err(e)));
                }
                RangeCombinerResult::None => {
                    return Poll::Ready(None);
                }
                RangeCombinerResult::Range(range) => {
                    let stream_idx = range.stream_idx();

                    if !range.is_finished() {
                        self.range_combiner.push_range(range)
                    } else {
                        // we should mark this stream uninitialized
                        // since its polling may return pending
                        self.initialized[stream_idx] = false;
                        self.range_finished[stream_idx] = true;
                        match futures::ready!(self.maybe_poll_stream(cx, stream_idx)) {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{}", e);
                                self.aborted = true;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                    // here we don't return Poll::Pending to let combiner
                    // continue to produce range
                }
                RangeCombinerResult::RecordBatch(batch) => {
                    return Poll::Ready(Some(batch));
                }
            }
        }
    }
}

impl<C: CursorValues, R: RangeCombinerTrait<C>> Stream for SortedStreamMerger<C, R> {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_inner(cx).map(|inner| {
            inner.map(|res| {
                res.map_err(|e| {
                    error!("{}", e);
                    DataFusionError::External(e.into_boxed_error())
                })
            })
        })
    }
}

impl<C: CursorValues, R: RangeCombinerTrait<C>> RecordBatchStream
    for SortedStreamMerger<C, R>
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use arrow_array::Float64Array;
    use datafusion::assert_batches_eq;
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::physical_plan::{ExecutionPlan, common};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use parking_lot::lock_api::RwLock;

    use crate::Result;
    use crate::helpers::InMemGenerator;
    use crate::physical_plan::merge::sorted::merge_operator::MergeOperator;
    use crate::physical_plan::merge::sorted::sorted_stream_merger::{
        SortedStream, build_sorted_stream_merger,
    };

    fn create_batch_one_col_i32(name: &str, vec: &[i32]) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from(Vec::from(vec)));
        RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
    }

    async fn create_stream(
        batches: Vec<RecordBatch>,
        context: Arc<TaskContext>,
    ) -> Result<SortedStream> {
        let schema = batches[0].schema();
        let exec = LazyMemoryExec::try_new(
            schema.clone(),
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(batches).unwrap(),
            ))],
        )?;
        let stream = exec.execute(0, context.clone())?;
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
        let s3 =
            create_stream(vec![s3b1, s3b2, s3b3, s3b4, s3b5, s3b6], task_ctx.clone())
                .await
                .unwrap();

        let primary_keys = vec!["a".to_string()];
        let pool = Arc::new(GreedyMemoryPool::new(100 * 1024 * 1024)) as _;
        let a1 = MemoryConsumer::new("a1").register(&pool);

        let merge_stream = build_sorted_stream_merger(
            vec![s1, s2, s3],
            Arc::from(primary_keys),
            schema.clone(),
            schema.clone(),
            2,
            Arc::new(HashMap::new()),
            vec![],
            a1,
            vec![false, false, false],
        )
        .unwrap();
        let merged = common::collect(merge_stream).await.unwrap();
        assert_batches_eq!(
            &[
                "+----+", "| a  |", "+----+", "| 1  |", "| 3  |", "| 4  |", "| 5  |",
                "| 6  |", "| 7  |", "| 9  |", "| 10 |", "+----+",
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
        let s1b5 =
            create_batch_i32(vec!["id", "a"], vec![&[5, 5, 6], &[10009, 10010, 10011]]);
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
            &[
                s1b1.clone(),
                s1b2.clone(),
                s1b3.clone(),
                s1b4.clone(),
                s1b5.clone()
            ]
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
            &[
                s2b1.clone(),
                s2b2.clone(),
                s2b3.clone(),
                s2b4.clone(),
                s2b5.clone()
            ]
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
            vec![
                s1b1.clone(),
                s1b2.clone(),
                s1b3.clone(),
                s1b4.clone(),
                s1b5.clone(),
            ],
            task_ctx.clone(),
        )
        .await
        .unwrap();
        let s2 = create_stream(
            vec![
                s2b1.clone(),
                s2b2.clone(),
                s2b3.clone(),
                s2b4.clone(),
                s2b5.clone(),
            ],
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

        let primary_keys = vec!["id".to_string()];
        let pool = Arc::new(GreedyMemoryPool::new(100 * 1024 * 1024)) as _;
        let a1 = MemoryConsumer::new("a1").register(&pool);

        let schema = Arc::new(schema);
        let merge_stream = build_sorted_stream_merger(
            vec![s1, s2, s3],
            Arc::from(primary_keys),
            schema.clone(),
            schema.clone(),
            2,
            Arc::new(HashMap::new()),
            vec![],
            a1,
            vec![false, false, false],
        )
        .unwrap();
        let merged = common::collect(merge_stream).await.unwrap();
        print_batches(&merged).unwrap();
    }

    #[tokio::test]
    async fn test_sorted_stream_merger_with_sum_and_last() {
        let session_config = SessionConfig::default().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(session_config);
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
        let s1b4 = create_batch(
            vec!["id", "a", "b", "c"],
            &[5],
            &[5],
            vec![Some(4.4)],
            vec!["100008"],
        );
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
                "| 1  | 9  | 2.0  | 102    |",
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
            &[
                s1b1.clone(),
                s1b2.clone(),
                s1b3.clone(),
                s1b4.clone(),
                s1b5.clone()
            ]
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
        let s2b4 = create_batch(
            vec!["id", "a", "b", "c"],
            &[5],
            &[5],
            vec![Some(4.4)],
            vec!["20005"],
        );
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
            &[
                s2b1.clone(),
                s2b2.clone(),
                s2b3.clone(),
                s2b4.clone(),
                s2b5.clone()
            ]
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
            vec![
                s1b1.clone(),
                s1b2.clone(),
                s1b3.clone(),
                s1b4.clone(),
                s1b5.clone(),
            ],
            task_ctx.clone(),
        )
        .await
        .unwrap();
        let s2 = create_stream(
            vec![
                s2b1.clone(),
                s2b2.clone(),
                s2b3.clone(),
                s2b4.clone(),
                s2b5.clone(),
            ],
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

        let primary_keys = vec!["id".to_string()];
        let pool = Arc::new(GreedyMemoryPool::new(100 * 1024 * 1024)) as _;
        let a1 = MemoryConsumer::new("a1").register(&pool);

        let schema = Arc::new(schema);
        let merge_stream = build_sorted_stream_merger(
            vec![s1, s2, s3],
            Arc::from(primary_keys),
            schema.clone(),
            schema.clone(),
            2,
            Arc::new(HashMap::new()),
            vec![
                MergeOperator::UseLast,
                MergeOperator::SumAll,
                MergeOperator::UseLastNotNull,
                MergeOperator::UseLast,
                MergeOperator::UseLast,
            ],
            a1,
            vec![false, false, false],
        )
        .unwrap();
        let merged = common::collect(merge_stream).await.unwrap();
        assert_batches_eq!(
            &[
                "+----+-----+------+-------+--------+",
                "| id | a   | b    | c     | d      |",
                "+----+-----+------+-------+--------+",
                "| 1  | 10  | 2.0  | 102   |        |",
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
}
