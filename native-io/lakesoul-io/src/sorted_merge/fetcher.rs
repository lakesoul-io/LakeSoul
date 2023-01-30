use crate::lakesoul_reader::ArrowResult;
use crate::sorted_merge::merge_traits::StreamSortKeyRangeFetcher;
use crate::sorted_merge::sorted_stream_merger::{BufferedRecordBatchStream, SortKeyRange};
use crate::sorted_merge::sort_key_range::SortKeyBatchRange;


use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows, SortField};
use async_trait::async_trait;
use datafusion::error::DataFusionError::{ArrowError, Execution};
use datafusion::error::Result;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::SendableRecordBatchStream;

use futures::{Stream, StreamExt};
use futures_util::stream::{Fuse, FilterMap, FusedStream, Peekable};
use std::pin::Pin;
use std::sync::Arc;
use std::fmt::{Debug, Formatter};
use std::task::{Context, Poll};



#[derive(Debug)]
pub enum RangeFetcher {
    NonUniqueSortKeyRangeFetcher(NonUniqueSortKeyRangeFetcher),
}

impl Stream for RangeFetcher { 
    type Item = ArrowResult<SortKeyBatchRange>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match this {
            RangeFetcher::NonUniqueSortKeyRangeFetcher(fetcher) => fetcher.poll_next_unpin(cx)
        }
    }
}


// #[async_trait]
impl  RangeFetcher {
    pub fn new(
        stream_idx: usize,
        stream: Fuse<SendableRecordBatchStream>,
        expressions: &[PhysicalSortExpr],
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(RangeFetcher::NonUniqueSortKeyRangeFetcher(NonUniqueSortKeyRangeFetcher::new(stream_idx, stream, expressions, schema.clone()).unwrap()))
    }

    // async fn init_batch(&mut self) -> Result<()> {
    //     match self {
    //         RangeFetcher::NonUniqueSortKeyRangeFetcher(fetcher) => fetcher.init_batch()
    //     };
    //     Ok(())
    // }

    pub fn is_terminated(&self) -> bool {
        match self {
            RangeFetcher::NonUniqueSortKeyRangeFetcher(fetcher) => fetcher.is_terminated()
        }
    }

}

pub type PeekableBatchRowsStream = Peekable<
    FilterMap<
        Fuse<SendableRecordBatchStream>,
        std::future::Ready<Option<Result<(RecordBatch, Rows)>>>,
        Box<
            dyn FnMut(ArrowResult<RecordBatch>) -> std::future::Ready<Option<Result<(RecordBatch, Rows)>>>
                + Send
                + Unpin,
        >,
    >,
>;

pub struct NonUniqueSortKeyRangeFetcher {
    stream_idx: usize,
    stream: PeekableBatchRowsStream,
    current_range: Option<SortKeyRange>,
}

impl Debug for NonUniqueSortKeyRangeFetcher {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NonUniqueSortKeyRangeFetcher")
    }
}

impl Stream for NonUniqueSortKeyRangeFetcher { 
    type Item = ArrowResult<SortKeyBatchRange>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

impl NonUniqueSortKeyRangeFetcher {
    async fn fetch_next_batch(&mut self) -> Result<Option<(RecordBatch, Rows)>> {
        if self.stream.is_terminated() {
            return Ok(None);
        }
        let batch_opt = self.stream.next().await;
        match batch_opt {
            None => Ok(None),
            Some(batch_result) => {
                let batch = batch_result?;
                Ok(Some(batch))
            }
        }
    }
}

#[async_trait]
impl StreamSortKeyRangeFetcher for NonUniqueSortKeyRangeFetcher {
    fn new(
        stream_idx: usize,
        stream: Fuse<SendableRecordBatchStream>,
        expressions: &[PhysicalSortExpr],
        schema: SchemaRef,
    ) -> Result<Self> {
        let sort_fields = expressions
            .iter()
            .map(|expr| {
                let data_type = expr.expr.data_type(&schema)?;
                Ok(SortField::new_with_options(data_type, expr.options))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut row_converter = RowConverter::new(sort_fields);
        let column_expressions: Vec<Arc<dyn PhysicalExpr>> = expressions.iter().map(|x| x.expr.clone()).collect();
        let map_fn: Box<
            dyn FnMut(ArrowResult<RecordBatch>) -> std::future::Ready<Option<Result<(RecordBatch, Rows)>>>
                + Send
                + Unpin,
        > = Box::new(move |batch_result| match batch_result {
            Ok(batch) => {
                if batch.num_rows() > 0 {
                    let cols_result = column_expressions
                        .iter()
                        .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows())))
                        .collect::<Result<Vec<_>>>();
                    match cols_result {
                        Ok(cols) => match row_converter.convert_columns(&cols) {
                            // convert to rows
                            Ok(rows) => std::future::ready(Some(Ok((batch, rows)))),
                            Err(e) => std::future::ready(Some(Err(ArrowError(e)))),
                        },
                        Err(e) => std::future::ready(Some(Err(e))),
                    }
                } else {
                    // skip empty batch
                    std::future::ready(None)
                }
            }
            Err(e) => std::future::ready(Some(Err(ArrowError(e)))),
        });
        let stream = stream.filter_map(map_fn).peekable();
        Ok(NonUniqueSortKeyRangeFetcher { stream_idx, stream, current_range: None })
    }

    async fn init_batch(&mut self) -> Result<()> {
        if let Some(peeked) = Pin::new(&mut self.stream).peek().await {
            return match peeked {
                Err(e) => Err(Execution(e.to_string())),
                _ => Ok(()),
            };
        }
        Ok(())
    }

    fn is_terminated(&self) -> bool {
        self.stream.is_terminated()
    }

    async fn fetch_sort_key_range_from_stream(
        &mut self,
        current_range: Option<&SortKeyRange>,
    ) -> Result<Option<SortKeyRange>> {
        let mut sort_key_range = SortKeyRange::new(self.stream_idx);
        // get current in progress(next) begin row for this stream
        let (mut begin, mut batch, mut rows) = if let Some(current_range) = current_range {
            let sort_key_in_batch = current_range.sort_key_ranges.last().unwrap();
            (
                sort_key_in_batch.end_row + 1,
                sort_key_in_batch.batch.clone(),
                sort_key_in_batch.rows.clone(),
            )
        } else {
            if let Some((batch, rows)) = self.fetch_next_batch().await? {
                (0usize, Arc::new(batch), Arc::new(rows))
            } else {
                return Ok(None);
            }
        };
        loop {
            let mut i = begin;
            while i < rows.num_rows() {
                if i < rows.num_rows() - 1 {
                    // check if next row in this batch has same sort key
                    if rows.row(i + 1) == rows.row(i) {
                        i = i + 1;
                        continue;
                    }
                }
                // construct a sort key in batch
                let sort_key_in_batch = SortKeyBatchRange::new(begin, i, self.stream_idx, batch.clone(), rows.clone());
                sort_key_range.sort_key_ranges.push(sort_key_in_batch);
                i = i + 1;
                break; // while
            }
            // reach the end of current batch
            // current ranges in batch could be empty
            if i >= rows.num_rows() {
                // current range in batches is empty, directly go to next batch
                let mut need_next = sort_key_range.sort_key_ranges.is_empty();
                if !need_next {
                    // current range in batches non empty, peek to see if next batch has same row
                    if let Some(peeked_result) = Pin::new(&mut self.stream).peek().await {
                        match peeked_result {
                            Ok((_, next_rows)) => {
                                // last batch must exist, so we unwrap it directly
                                let last_batch = sort_key_range.sort_key_ranges.last().unwrap();
                                if last_batch.current() == next_rows.row(0) {
                                    need_next = true;
                                }
                            }
                            Err(e) => return Err(Execution(e.to_string())),
                        }
                    }
                }
                if need_next {
                    if let Some(batch_rows) = self.fetch_next_batch().await? {
                        batch = Arc::new(batch_rows.0);
                        rows = Arc::new(batch_rows.1);
                        begin = 0usize;
                        continue;
                    }
                }
            }
            break; // loop
        }
        return Ok(Some(sort_key_range));
    }
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_reader::ArrowResult;
    use crate::sorted_merge::merge_traits::StreamSortKeyRangeFetcher;
    use crate::sorted_merge::fetcher::NonUniqueSortKeyRangeFetcher;
    use crate::sorted_merge::sort_key_range::SortKeyBatchRange;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion::execution::context::TaskContext;
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use futures_util::StreamExt;
    use std::future::Ready;
    use std::sync::Arc;

    async fn create_stream_fetcher(
        batches: Vec<RecordBatch>,
        context: Arc<TaskContext>,
        sort_fields: Vec<&str>,
    ) -> Result<NonUniqueSortKeyRangeFetcher> {
        let schema = batches[0].schema();
        let sort_exprs: Vec<_> = sort_fields
            .into_iter()
            .map(|field| PhysicalSortExpr {
                expr: col(field, &schema).unwrap(),
                options: Default::default(),
            })
            .collect();
        let batches = [batches];
        let exec = MemoryExec::try_new(&batches, schema.clone(), None).unwrap();
        let stream = exec.execute(0, context.clone()).unwrap();
        let fused = stream.fuse();
        NonUniqueSortKeyRangeFetcher::new(0, fused, &sort_exprs, schema)
    }

    fn create_batch_one_col_i32(name: &str, vec: &[i32]) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from_slice(vec));
        RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
    }

    fn assert_sort_key_range_in_batch(sk: &SortKeyBatchRange, begin_row: usize, end_row: usize) {
        assert_eq!(sk.begin_row, begin_row);
        assert_eq!(sk.end_row, end_row);
    }

    #[tokio::test]
    async fn test_single_stream_range() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let b1 = create_batch_one_col_i32("a", &[1, 1, 3, 3, 4]);
        let b2 = create_batch_one_col_i32("a", &[4, 5]);
        let b3 = create_batch_one_col_i32("a", &[]);
        let b4 = create_batch_one_col_i32("a", &[5]);
        let b5 = create_batch_one_col_i32("a", &[5, 6, 6]);
        let b6 = create_batch_one_col_i32("a", &[7]);
        let mut fetcher = create_stream_fetcher(
            vec![b1.clone(), b2.clone(), b3.clone(), b4.clone(), b5.clone(), b6.clone()],
            task_ctx,
            vec!["a"],
        )
        .await
        .unwrap();
        fetcher.init_batch().await.unwrap();

        // first range, [1, 1] in batch 0
        let range = fetcher.fetch_sort_key_range_from_stream(None).await.unwrap().unwrap();
        assert_eq!(range.sort_key_ranges.len(), 1usize);
        assert_sort_key_range_in_batch(&range.sort_key_ranges[0], 0, 1);
        assert_eq!(range.sort_key_ranges[0].batch.column(0), b1.column(0));

        // second range, [3, 3] in batch 0
        let range = fetcher
            .fetch_sort_key_range_from_stream(Some(&range))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(range.sort_key_ranges.len(), 1usize);
        assert_sort_key_range_in_batch(&range.sort_key_ranges[0], 2, 3);
        assert_eq!(range.sort_key_ranges[0].batch.column(0), b1.column(0));

        // third range, [4] in batch 0 and [4] in batch 1
        let range = fetcher
            .fetch_sort_key_range_from_stream(Some(&range))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(range.sort_key_ranges.len(), 2usize);
        assert_sort_key_range_in_batch(&range.sort_key_ranges[0], 4, 4);
        assert_eq!(range.sort_key_ranges[0].batch.column(0), b1.column(0));
        assert_sort_key_range_in_batch(&range.sort_key_ranges[1], 0, 0);
        assert_eq!(range.sort_key_ranges[1].batch.column(0), b2.column(0));

        // fourth range, [5] in batch 1 and [5] in batch 2(b4) and [5] in batch 3(b5)
        let range = fetcher
            .fetch_sort_key_range_from_stream(Some(&range))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(range.sort_key_ranges.len(), 3usize);
        assert_sort_key_range_in_batch(&range.sort_key_ranges[0], 1, 1);
        assert_eq!(range.sort_key_ranges[0].batch.column(0), b2.column(0));
        assert_sort_key_range_in_batch(&range.sort_key_ranges[1], 0, 0);
        assert_eq!(range.sort_key_ranges[1].batch.column(0), b4.column(0));
        assert_sort_key_range_in_batch(&range.sort_key_ranges[2], 0, 0);
        assert_eq!(range.sort_key_ranges[2].batch.column(0), b5.column(0));

        // fifth range, [6, 6] in batch 3(b5)
        let range = fetcher
            .fetch_sort_key_range_from_stream(Some(&range))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(range.sort_key_ranges.len(), 1usize);
        assert_sort_key_range_in_batch(&range.sort_key_ranges[0], 1, 2);
        assert_eq!(range.sort_key_ranges[0].batch.column(0), b5.column(0));

        // sixth range, [7] in batch 4
        let range = fetcher
            .fetch_sort_key_range_from_stream(Some(&range))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(range.sort_key_ranges.len(), 1usize);
        assert_sort_key_range_in_batch(&range.sort_key_ranges[0], 0, 0);
        assert_eq!(range.sort_key_ranges[0].batch.column(0), b6.column(0));

        // terminated
        assert!(fetcher.is_terminated());
    }
}
