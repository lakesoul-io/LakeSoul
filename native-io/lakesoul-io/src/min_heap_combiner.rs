use crate::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
use crate::sorted_stream_merger::SortKeyRange;
use async_trait::async_trait;
use dary_heap::DaryHeap;
use datafusion::error::Result;
use futures::future::try_join_all;
use smallvec::SmallVec;
use std::cmp::Reverse;

pub struct MinHeapSortKeyRangeCombiner<Fetcher: StreamSortKeyRangeFetcher + Send, const D: usize> {
    fetchers: Vec<Fetcher>,
    heap: DaryHeap<Reverse<SortKeyRange>, D>,
}

impl<Fetcher: StreamSortKeyRangeFetcher + Send, const D: usize> MinHeapSortKeyRangeCombiner<Fetcher, D> {
    async fn next_range_of_fetcher(
        fetcher: &mut Fetcher,
        current_range: Option<&SortKeyRange>,
        heap: &mut DaryHeap<Reverse<SortKeyRange>, D>,
    ) -> Result<()> {
        if fetcher.is_terminated() {
            return Ok(());
        }

        if let Some(sort_key_range) = fetcher.fetch_sort_key_range_from_stream(current_range).await? {
            heap.push(Reverse(sort_key_range));
        }
        Ok(())
    }

    async fn fetch_next_range(&mut self, stream_idx: usize, current_range: Option<&SortKeyRange>) -> Result<()> {
        let fetcher = &mut self.fetchers[stream_idx];
        MinHeapSortKeyRangeCombiner::<Fetcher, D>::next_range_of_fetcher(fetcher, current_range, &mut self.heap).await
    }
}

#[async_trait]
impl<F, const D: usize> StreamSortKeyRangeCombiner for MinHeapSortKeyRangeCombiner<F, D>
where
    F: StreamSortKeyRangeFetcher + Send,
{
    type Fetcher = F;

    fn with_fetchers(fetchers: Vec<Self::Fetcher>) -> Self {
        let n = fetchers.len();
        MinHeapSortKeyRangeCombiner {
            fetchers,
            heap: DaryHeap::with_capacity(n),
        }
    }

    async fn init(&mut self) -> Result<()> {
        let fetcher_iter_mut = self.fetchers.iter_mut();
        let futures = fetcher_iter_mut.map(|fetcher| async { fetcher.init_batch().await });
        let _ = try_join_all(futures).await?;
        let stream_count = self.fetchers.len();
        for i in 0..stream_count {
            self.fetch_next_range(i, None).await?;
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<SmallVec<[SortKeyRange; 4]>>> {
        let mut ranges: SmallVec<[SortKeyRange; 4]> = SmallVec::<[SortKeyRange; 4]>::new();
        let sort_key_range = self.heap.pop();
        if sort_key_range.is_some() {
            let range = sort_key_range.unwrap().0;
            self.fetch_next_range(range.stream_idx, Some(&range)).await?;
            ranges.push(range);
            loop {
                // check if next range (maybe in another stream) has same row key
                let next_range = self.heap.peek();
                if next_range.is_some_and(|nr| nr.0 == *ranges.last().unwrap()) {
                    let range_next = self.heap.pop().unwrap().0;
                    self.fetch_next_range(range_next.stream_idx, Some(&range_next)).await?;
                    ranges.push(range_next);
                } else {
                    break;
                }
            }
        };
        if ranges.is_empty() {
            Ok(None)
        } else {
            Ok(Some(ranges))
        }
    }
}
#[cfg(test)]
mod tests {
    use crate::lakesoul_reader::ArrowResult;
    use crate::merge_traits::StreamSortKeyRangeFetcher;
    use crate::non_unique_fetcher::NonUniqueSortKeyRangeFetcher;
    use crate::sorted_stream_merger::SortKeyRangeInBatch;
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
        let ready_futures =
            stream.map(std::future::ready as fn(ArrowResult<RecordBatch>) -> Ready<ArrowResult<RecordBatch>>);
        let bufferred = ready_futures.buffered(2);
        NonUniqueSortKeyRangeFetcher::new(0, bufferred, &sort_exprs, schema)
    }

    fn create_batch_one_col_i32(name: &str, vec: &[i32]) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from_slice(vec));
        RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
    }

    fn assert_sort_key_range_in_batch(sk: &SortKeyRangeInBatch, begin_row: usize, end_row: usize) {
        assert_eq!(sk.begin_row, begin_row);
        assert_eq!(sk.end_row, end_row);
    }

    #[tokio::test]
    async fn test_multi_streams_combine() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let s1b1 = create_batch_one_col_i32("a", &[1, 1, 3, 3, 4]);
        let s1b2 = create_batch_one_col_i32("a", &[4, 5]);
        let s1b3 = create_batch_one_col_i32("a", &[]);
        let s1b4 = create_batch_one_col_i32("a", &[5]);
        let s1b5 = create_batch_one_col_i32("a", &[5, 6, 6]);
        let mut s1fetcher = create_stream_fetcher(
            vec![s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()],
            task_ctx.clone(),
            vec!["a"],
        )
        .await
        .unwrap();
        let s2b1 = create_batch_one_col_i32("a", &[1, 1, 3, 3, 4]);
        let s2b2 = create_batch_one_col_i32("a", &[4, 5]);
        let s2b3 = create_batch_one_col_i32("a", &[]);
        let s2b4 = create_batch_one_col_i32("a", &[5]);
        let s2b5 = create_batch_one_col_i32("a", &[5, 6, 6]);
        let mut s2fetcher = create_stream_fetcher(
            vec![s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()],
            task_ctx.clone(),
            vec!["a"],
        )
        .await
        .unwrap();
    }
}
