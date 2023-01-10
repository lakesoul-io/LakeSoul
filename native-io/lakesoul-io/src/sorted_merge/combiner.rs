use crate::sorted_merge::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
use crate::sorted_merge::fetcher::NonUniqueSortKeyRangeFetcher;
use crate::sorted_merge::sorted_stream_merger::SortKeyRange;
use async_trait::async_trait;
use dary_heap::DaryHeap;
use datafusion::error::Result;
use futures::future::try_join_all;
use smallvec::SmallVec;

use std::fmt::{Debug, Formatter};
use std::cmp::Reverse;
use std::sync::Arc;
use std::ops::Deref;
use std::borrow::Borrow;

#[derive(Debug)]
pub enum RangeCombiner {
    MinHeapSortKeyRangeCombiner(MinHeapSortKeyRangeCombiner::<NonUniqueSortKeyRangeFetcher, 2>)
}

impl RangeCombiner {
    pub fn new() -> Self {
        RangeCombiner::MinHeapSortKeyRangeCombiner(MinHeapSortKeyRangeCombiner::<NonUniqueSortKeyRangeFetcher, 2>::with_fetchers(vec![]))
    }

    pub fn push(&mut self, range: SortKeyRange) {
        match self {
            RangeCombiner::MinHeapSortKeyRangeCombiner(combiner) => combiner.push(range)
        };
    }
}

#[derive(Debug)]
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

    fn fetcher_num(self) -> usize {
        self.fetchers.len()
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

    async fn next(&mut self) -> Result<Option<SmallVec<[Box<SortKeyRange>; 4]>>> {
        let mut ranges: SmallVec<[Box<SortKeyRange>; 4]> = SmallVec::<[Box<SortKeyRange>; 4]>::new();
        let sort_key_range = self.heap.pop();
        if sort_key_range.is_some() {
            let range = Box::new(sort_key_range.unwrap().0);
            self.fetch_next_range(range.stream_idx, Some(range.borrow())).await?;
            ranges.push(range);
            loop {
                // check if next range (maybe in another stream) has same row key
                let next_range = self.heap.peek();
                if next_range.is_some_and(|nr| nr.0 == **ranges.last().unwrap()) {
                    let range_next = Box::new(self.heap.pop().unwrap().0);
                    self.fetch_next_range(range_next.stream_idx, Some(range_next.borrow())).await?;
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

    fn push(&mut self, sort_key_range: SortKeyRange) {
        self.heap.push(Reverse(sort_key_range));
    }
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_reader::ArrowResult;
    use crate::sorted_merge::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
    use crate::sorted_merge::combiner::MinHeapSortKeyRangeCombiner;
    use crate::sorted_merge::fetcher::NonUniqueSortKeyRangeFetcher;
    use crate::sorted_merge::sorted_stream_merger::SortKeyRangeInBatch;
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
        stream_idx: usize,
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

        NonUniqueSortKeyRangeFetcher::new(stream_idx, fused, &sort_exprs, schema)
    }

    fn create_batch_one_col_i32(name: &str, vec: &[i32]) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from_slice(vec));
        RecordBatch::try_from_iter(vec![(name, a)]).unwrap()
    }

    fn assert_sort_key_range_in_batch(sk: &SortKeyRangeInBatch, begin_row: usize, end_row: usize, batch: &RecordBatch) {
        assert_eq!(sk.begin_row, begin_row);
        assert_eq!(sk.end_row, end_row);
        assert_eq!(sk.batch.as_ref(), batch);
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
        let s1fetcher = create_stream_fetcher(
            0,
            vec![s1b1.clone(), s1b2.clone(), s1b3.clone(), s1b4.clone(), s1b5.clone()],
            task_ctx.clone(),
            vec!["a"],
        )
        .await
        .unwrap();
        let s2b1 = create_batch_one_col_i32("a", &[3, 4]);
        let s2b2 = create_batch_one_col_i32("a", &[4, 5]);
        let s2b3 = create_batch_one_col_i32("a", &[]);
        let s2b4 = create_batch_one_col_i32("a", &[5]);
        let s2b5 = create_batch_one_col_i32("a", &[5, 7]);
        let s2fetcher = create_stream_fetcher(
            1,
            vec![s2b1.clone(), s2b2.clone(), s2b3.clone(), s2b4.clone(), s2b5.clone()],
            task_ctx.clone(),
            vec!["a"],
        )
        .await
        .unwrap();
        let s3b1 = create_batch_one_col_i32("a", &[]);
        let s3b2 = create_batch_one_col_i32("a", &[5]);
        let s3b3 = create_batch_one_col_i32("a", &[5, 7]);
        let s3b4 = create_batch_one_col_i32("a", &[7, 9]);
        let s3b5 = create_batch_one_col_i32("a", &[]);
        let s3b6 = create_batch_one_col_i32("a", &[10]);
        let s3fetcher = create_stream_fetcher(
            2,
            vec![
                s3b1.clone(),
                s3b2.clone(),
                s3b3.clone(),
                s3b4.clone(),
                s3b5.clone(),
                s3b6.clone(),
            ],
            task_ctx.clone(),
            vec!["a"],
        )
        .await
        .unwrap();

        let mut combiner = MinHeapSortKeyRangeCombiner::<NonUniqueSortKeyRangeFetcher, 2>::with_fetchers(vec![
            s1fetcher, s2fetcher, s3fetcher,
        ]);
        combiner.init().await.unwrap();
        // [1, 1] from s0
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].stream_idx, 0);
        assert_eq!(ranges[0].sort_key_ranges.len(), 1);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 0, 1, &s1b1);
        // [3, 3] from s0, [3] from s1
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].stream_idx, 0);
        assert_eq!(ranges[1].stream_idx, 1);
        assert_eq!(ranges[0].sort_key_ranges.len(), 1);
        assert_eq!(ranges[1].sort_key_ranges.len(), 1);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 2, 3, &s1b1);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[0], 0, 0, &s2b1);
        // [4, 4] from s0, [4, 4] from s1
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].stream_idx, 0);
        assert_eq!(ranges[1].stream_idx, 1);
        assert_eq!(ranges[0].sort_key_ranges.len(), 2);
        assert_eq!(ranges[1].sort_key_ranges.len(), 2);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 4, 4, &s1b1);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[1], 0, 0, &s1b2);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[0], 1, 1, &s2b1);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[1], 0, 0, &s2b2);
        // [5, 5, 5] from s0, [5, 5, 5] from s1, [5, 5] from s2
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].stream_idx, 0);
        assert_eq!(ranges[1].stream_idx, 1);
        assert_eq!(ranges[2].stream_idx, 2);
        assert_eq!(ranges[0].sort_key_ranges.len(), 3);
        assert_eq!(ranges[1].sort_key_ranges.len(), 3);
        assert_eq!(ranges[2].sort_key_ranges.len(), 2);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 1, 1, &s1b2);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[1], 0, 0, &s1b4);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[2], 0, 0, &s1b5);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[0], 1, 1, &s2b2);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[1], 0, 0, &s2b4);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[2], 0, 0, &s2b5);
        assert_sort_key_range_in_batch(&ranges[2].sort_key_ranges[0], 0, 0, &s3b2);
        assert_sort_key_range_in_batch(&ranges[2].sort_key_ranges[1], 0, 0, &s3b3);
        // [6, 6] from s0
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].stream_idx, 0);
        assert_eq!(ranges[0].sort_key_ranges.len(), 1);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 1, 2, &s1b5);
        // [7] from s1, [7, 7] from s2
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].stream_idx, 1);
        assert_eq!(ranges[1].stream_idx, 2);
        assert_eq!(ranges[0].sort_key_ranges.len(), 1);
        assert_eq!(ranges[1].sort_key_ranges.len(), 2);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 1, 1, &s2b5);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[0], 1, 1, &s3b3);
        assert_sort_key_range_in_batch(&ranges[1].sort_key_ranges[1], 0, 0, &s3b4);
        // [9] from s2
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].stream_idx, 2);
        assert_eq!(ranges[0].sort_key_ranges.len(), 1);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 1, 1, &s3b4);
        // [10] from s2
        let ranges = combiner.next().await.unwrap().unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].stream_idx, 2);
        assert_eq!(ranges[0].sort_key_ranges.len(), 1);
        assert_sort_key_range_in_batch(&ranges[0].sort_key_ranges[0], 0, 0, &s3b6);
        // end
        let ranges = combiner.next().await.unwrap();
        assert!(ranges.is_none());
    }
}
