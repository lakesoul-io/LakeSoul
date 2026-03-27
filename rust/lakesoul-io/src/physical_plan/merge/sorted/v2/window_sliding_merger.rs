// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! This module provides the implementation of a batch-wise range combiner that
//! optimizes the sorted merge by checking range overlaps using batch min/max values
//! before doing row-level comparisons.

use crate::Result;
use crate::cache::disk_cache::lru_cache::CountableMeter;
use crate::physical_plan::merge::sorted::cursor::CursorValues;
use crate::physical_plan::merge::sorted::sorted_stream_merger::CursorStream;
use crate::physical_plan::merge::sorted::v2::batch_range::BatchRange;
use crate::physical_plan::merge::sorted::v2::binary_merger::BinaryMerger;
use crate::physical_plan::merge::sorted::v2::deduplicate::DeduplicateStream;
use crate::physical_plan::merge::sorted::v2::loser_tree_merger::LoserTreeRangeMerge;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use atomic_refcell::AtomicRefCell;
use datafusion_common::DataFusionError;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::stream::{FusedStream, FuturesUnordered, Peekable};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use nohash::BuildNoHashHasher;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::{Report, report};
use std::collections::HashMap;
use std::future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;

pub(crate) type CursorStreamWithId<C> =
    Pin<Box<dyn Stream<Item = Result<(C, RecordBatch, usize)>> + Send + 'static>>;

/// A sorted runs merger that uses batch-wise(range) min/max values to determine range overlaps before
/// doing row-level merge operations. This can significantly optimize the merge
/// process because its highly likelybatches have non-overlapping ranges.
pub struct WindowSlidingMerger<C: CursorValues> {
    streams: Vec<Arc<Mutex<Peekable<CursorStreamWithId<C>>>>>,

    /// The schema of the record batch.
    schema: SchemaRef,

    /// Target batch size for generated record batch
    target_batch_size: usize,

    /// Number of active streams
    num_streams: usize,

    /// Counter for tracking initialized ranges
    ranges_counter: usize,

    ranges: HashMap<usize, BatchRange<C>, BuildNoHashHasher<usize>>,
}

impl<C: CursorValues + Send + Sync + 'static> WindowSlidingMerger<C> {
    /// Create a new WindowSlidingMerger
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the record batch
    /// * `streams_num` - The number of streams to merge
    /// * `fields_map` - The fields map from source schemas to target schema
    /// * `target_batch_size` - The target batch size for generated record batch
    pub fn new(
        streams: Vec<(CursorStream<C>, bool)>,
        schema: SchemaRef,
        streams_num: usize,
        target_batch_size: usize,
    ) -> Result<Self> {
        info!(
            "Create WindowSlidingMerger with {} streams, target_batch_size: {}",
            streams_num, target_batch_size
        );
        let streams: Vec<CursorStreamWithId<C>> = streams
            .into_iter()
            .enumerate()
            .map(|(idx, (stream, require_dedup))| {
                let stream = if require_dedup {
                    let dedup_stream =
                        DeduplicateStream::new_from_stream(stream, schema.clone())?;
                    dedup_stream.create_deduplicated_stream()
                } else {
                    stream
                };
                Ok(Box::pin(
                    stream
                        .map(move |e| {
                            if let Ok((c, b)) = e {
                                Ok((c, b, idx))
                            } else {
                                Err(e.err().unwrap())
                            }
                        })
                        .map(|e| future::ready(e))
                        .buffered(4),
                ) as CursorStreamWithId<C>)
            })
            .collect::<Result<Vec<_>>>()?;
        let streams = streams
            .into_iter()
            .map(|stream: CursorStreamWithId<C>| Arc::new(Mutex::new(stream.peekable())))
            .collect::<Vec<_>>();
        Ok(Self {
            streams,
            schema,
            target_batch_size,
            num_streams: streams_num,
            ranges_counter: 0,
            ranges: HashMap::<usize, BatchRange<C>, BuildNoHashHasher<usize>>::with_capacity_and_hasher(
                streams_num * 2,
                BuildNoHashHasher::default(),
            ),
        })
    }

    // Algorithm to merge current batches in `self.ranges`:
    // 1. Find the range with the smallest first-row value, as `r_star`, its min value is r_start
    // 2. Find all ranges whose r.min is lte to `r_star.max`, store the set in `overlap_ranges`
    // 3. Find the minimum value of r.max in `overlap_ranges`, as `window_end`
    // 4. Merge all ranges in `overlap_ranges` whose r.min is lte to `window_end`
    //   4.1. For each r in `overlap_ranges`: merge its range in [r.min, min(r.max, window_end)]
    //   4.2. Merge these ranges row-by-row with loser tree
    // 5. If no overlap ranges found for [r_star.min, r_star.max], produce r_star directly as next batch
    // 6. For all merged ranges (either fully or partially), update their begin row to end + 1, and end to num_rows - 1
    // 7. For all ranges having begin > end, pull from streams
    // 8. repeat until ranges is empty
    //
    // Use sender-receiver as this function may produce multiple batches
    pub async fn merge_next_batch<'a>(
        &'a mut self,
        mut tx: Sender<Result<RecordBatch>>,
    ) -> Result<Sender<Result<RecordBatch>>> {
        if self.ranges.is_empty() {
            return Ok(tx);
        }
        // Fast path 1: only one range, produce it directly
        if self.ranges.len() == 1 {
            let range = self.ranges.values_mut().next().unwrap();
            let batch = range.slice_remaining_and_advance();
            // println!(
            //     "Sending 1 batch\n{}",
            //     pretty_format_batches(&[batch.clone()])?
            // );
            tx.send(Ok(batch)).await?;
            return Ok(tx);
        }
        // Step 1. Find the two ranges with the smallest first-row values
        let mut r_star_range_idx = *self.ranges.iter().next().unwrap().0;
        let mut second_smallest_range = None;

        self.ranges.iter().skip(1).for_each(|(batch_idx, range)| {
            let r_star_range = self.ranges.get(&r_star_range_idx).unwrap();
            let cmp_with_first = C::compare(
                &range.cursor(),
                range.begin_row(),
                &r_star_range.cursor(),
                r_star_range.begin_row(),
            );
            if cmp_with_first.is_lt() {
                // Current range is smaller than the first, so shift first to second and update first
                second_smallest_range = Some(r_star_range);
                r_star_range_idx = *batch_idx;
            } else if second_smallest_range.is_none()
                || C::compare(
                    &range.cursor(),
                    range.begin_row(),
                    &second_smallest_range.unwrap().cursor(),
                    second_smallest_range.unwrap().begin_row(),
                )
                .is_lt()
            {
                // Current range is larger than first but smaller than second
                second_smallest_range = Some(range);
            }
        });
        let r_star_end = self
            .ranges
            .get(&r_star_range_idx)
            .unwrap()
            .end_row_for_merge();
        // println!(
        //     "r_star_range: \n{}, idx: {}, end row {}",
        //     pretty_format_batches(&[self
        //         .ranges
        //         .get(&r_star_range_idx)
        //         .unwrap()
        //         .batch()
        //         .clone()])?,
        //     r_star_range_idx,
        //     r_star_end
        // );
        // Step 2.
        let mut overlap_ranges = Vec::with_capacity(self.ranges.len());
        for (batch_idx, range) in self.ranges.iter() {
            let cmp = C::compare(
                &range.cursor(),
                range.begin_row(),
                self.ranges.get(&r_star_range_idx).unwrap().cursor(),
                r_star_end,
            );
            if cmp.is_lt() || cmp.is_eq() {
                overlap_ranges.push(*batch_idx);
            }
        }
        // Fast path 2: if overlap ranges contains only one range, produce it directly
        if overlap_ranges.len() == 1 {
            let range = self.ranges.get_mut(&overlap_ranges.pop().unwrap()).unwrap();
            let batch = range.slice_remaining_and_advance();
            // println!(
            //     "Sending 1 batch\n{}",
            //     pretty_format_batches(&[batch.clone()])?
            // );
            tx.send(Ok(batch)).await?;
            return Ok(tx);
        }
        // Step 3. Find the minimum value of r.max in `overlap_ranges`, as `window_end`
        let overlap_range_iter = overlap_ranges.iter();
        let mut window_end_range = *overlap_ranges.iter().next().unwrap();
        overlap_range_iter.for_each(|range| {
            let cmp = C::compare(
                &self.ranges.get(&range).unwrap().cursor(),
                self.ranges.get(&range).unwrap().end_row_for_merge(),
                &self.ranges.get(&window_end_range).unwrap().cursor(),
                self.ranges
                    .get(&window_end_range)
                    .unwrap()
                    .end_row_for_merge(),
            );
            if cmp.is_lt() {
                window_end_range = *range;
            }
        });
        let mut r_star_begin_num = 0;
        // update overlapped ranges' begin_row and end_row_for_merge
        let mut new_overlap_range_idx = Vec::with_capacity(overlap_ranges.len());
        // Step 4.1
        for range in overlap_ranges.iter() {
            if C::compare(
                &self.ranges.get(&range).unwrap().cursor(),
                self.ranges.get(&range).unwrap().begin_row(),
                &self.ranges.get(&r_star_range_idx).unwrap().cursor(),
                self.ranges.get(&r_star_range_idx).unwrap().begin_row(),
            )
            .is_eq()
            {
                r_star_begin_num += 1;
            }
            // update this range's end_row_for_merge to be lte window_end
            if let Some(end_row_for_merge) =
                self.ranges.get(&range).unwrap().find_le_start_index(
                    &self.ranges.get(&window_end_range).unwrap(),
                    self.ranges
                        .get(&window_end_range)
                        .unwrap()
                        .end_row_for_merge(),
                )
            {
                // println!(
                //     "Updating range \n{}'s end_row_for_merge to {}",
                //     pretty_format_batches(&[self
                //         .ranges
                //         .get(&range)
                //         .unwrap()
                //         .batch()
                //         .clone()])
                //     .unwrap(),
                //     end_row_for_merge
                // );
                self.ranges
                    .get_mut(&range)
                    .unwrap()
                    .set_end_row_for_merge(end_row_for_merge);
                new_overlap_range_idx.push(*range);
            }
        }
        // new_overlap_range should have at least two elements
        if new_overlap_range_idx.len() < 2 {
            tx.send(Err(report!("Not enough overlap ranges"))).await?;
            return Ok(tx);
        }
        // Step 5. Merge all ranges in `new_overlap_ranges`
        // let mut new_overlap_ranges: Vec<&'a mut BatchRange<C>> = Vec::with_capacity(new_overlap_range_idx.len());
        let new_overlap_ranges: Vec<&mut BatchRange<C>> = self
            .ranges
            .iter_mut()
            .filter(|(batch_idx, _)| new_overlap_range_idx.contains(batch_idx))
            .map(|(_, range)| range)
            .sorted_by(|a, b| {
                a.stream_idx()
                    .cmp(&b.stream_idx())
                    .then_with(|| a.batch_idx().cmp(&b.batch_idx()))
            })
            .collect();
        // println!(
        //     "Merging multiple ranges {:?}",
        //     new_overlap_ranges
        //         .iter()
        //         .map(|r| (
        //             format!("\n{}", pretty_format_batches(&[r.batch().clone()]).unwrap()),
        //             r.begin_row(),
        //             r.end_row_for_merge()
        //         ))
        //         .collect::<Vec<_>>()
        // );
        // Fast path 3: if overlap ranges contains only two ranges, merge them directly
        if overlap_ranges.len() == 2 {
            // println!(
            //     "Merging two ranges\n{:?}",
            //     new_overlap_ranges
            //         .iter()
            //         .map(|r| (
            //             format!(
            //                 "\n{}",
            //                 pretty_format_batches(&[r.batch().clone()]).unwrap()
            //             ),
            //             r.begin_row(),
            //             r.end_row_for_merge()
            //         ))
            //         .collect::<Vec<_>>()
            // );
            let mut merger = BinaryMerger::new(
                new_overlap_ranges,
                self.target_batch_size,
                self.schema.clone(),
            );
            tx = merger.merge(tx).await?;
            return Ok(tx);
        } else {
            let mut merger = LoserTreeRangeMerge::new(
                self.schema.clone(),
                new_overlap_ranges,
                self.target_batch_size,
            );
            tx = merger.merge(tx).await?;
        }
        Ok(tx)
    }

    pub fn build_merged_stream(
        this: Arc<Mutex<Self>>,
    ) -> Result<SendableRecordBatchStream> {
        let (tx, rx) = mpsc::channel(10);
        let self_cloned = this.clone();
        let schema = futures::executor::block_on(async move {
            // initialize ranges
            let self_ref = self_cloned.lock().await;
            self_ref.schema.clone()
        });
        let self_cloned = this.clone();
        // ignore JoinHandle, as when caller needs to cancel, it could just drop the receiver
        tokio::spawn(async move {
            let mut self_ref = self_cloned.lock().await;
            let stream_num = self_ref.num_streams;
            self_ref.pull_all_from_streams(0..stream_num).await?;
            let mut tx = tx;
            while !self_ref.ranges.is_empty() && !tx.is_closed() {
                tx = self_ref.merge_next_batch(tx).await?;
                // update ranges
                let mut to_pull_stream_indices =
                    Vec::with_capacity(self_ref.ranges.len());
                let mut to_remove_range_indices =
                    Vec::with_capacity(self_ref.ranges.len());
                self_ref.ranges.iter_mut().for_each(|(batch_idx, range)| {
                    // reset end to last row so it can continue to be merged
                    range.reset();
                    // if this range has ended, need to remove from ranges and pull from streams
                    if !range.has_more_rows() {
                        to_remove_range_indices.push(*batch_idx);
                        to_pull_stream_indices.push(range.stream_idx())
                    }
                });
                // pull from streams
                to_remove_range_indices.into_iter().for_each(|batch_idx| {
                    self_ref.ranges.remove(&batch_idx);
                });
                self_ref
                    .pull_all_from_streams(to_pull_stream_indices.into_iter())
                    .await?;
            }
            Ok::<(), Report>(())
        });
        Ok(Box::pin(MergedStream {
            stream: ReceiverStream::new(rx),
            schema,
        }))
    }

    async fn pull_all_from_streams(
        &mut self,
        stream_idx: impl Iterator<Item = usize>,
    ) -> Result<()> {
        let mut futures = FuturesUnordered::new();
        for idx in stream_idx {
            futures.push(Self::pull_one_stream(self.streams[idx].clone()));
        }
        while let Some(result) = futures.next().await {
            if let Some(batch) = result {
                self.add_batch(batch)?;
            }
        }
        Ok(())
    }

    async fn pull_one_stream(
        stream: Arc<Mutex<Peekable<CursorStreamWithId<C>>>>,
    ) -> Option<Result<(C, RecordBatch, usize)>> {
        let mut s = stream.lock().await;
        s.next().await
    }

    fn add_batch(&mut self, batch: Result<(C, RecordBatch, usize)>) -> Result<()> {
        let batch = batch?;
        if batch.1.num_rows() > 0 {
            let end_row_for_merge = batch.1.num_rows() - 1;
            self.ranges.insert(
                self.ranges_counter,
                BatchRange::new(
                    batch.0,
                    batch.1,
                    batch.2,
                    self.ranges_counter,
                    end_row_for_merge,
                ),
            );
            self.ranges_counter += 1;
        }
        Ok(())
    }
}

struct MergedStream {
    stream: ReceiverStream<Result<RecordBatch>>,
    schema: SchemaRef,
}

impl Stream for MergedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let p = self.stream.poll_next_unpin(cx);
        match p {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(
                batch.map_err(|e| DataFusionError::External(e.into_boxed_error())),
            )),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for MergedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::Result;
    use crate::helpers::InMemGenerator;
    use crate::physical_plan::merge::sorted::cursor::RowValues;
    use crate::physical_plan::merge::sorted::sorted_stream_merger::CursorStream;
    use crate::physical_plan::merge::sorted::v2::window_sliding_merger::WindowSlidingMerger;
    use crate::stream::RowCursorStream;
    use arrow::array::ArrayRef;
    use arrow::array::Int32Array;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::SortOptions;
    use datafusion::assert_batches_eq;
    use datafusion::execution::context::TaskContext;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::memory::LazyMemoryExec;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DataFusionError;
    use datafusion_execution::memory_pool::MemoryReservation;
    use datafusion_physical_expr::expressions::col;
    use futures::TryStreamExt;
    use parking_lot::lock_api::RwLock;
    use tokio::sync::Mutex;

    fn create_batch_two_col_i32(
        name1: &str,
        vec1: &[i32],
        name2: &str,
        vec2: &[i32],
    ) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from(Vec::from(vec1)));
        let b: ArrayRef = Arc::new(Int32Array::from(Vec::from(vec2)));
        RecordBatch::try_from_iter(vec![(name1, a), (name2, b)]).unwrap()
    }

    async fn create_stream(
        batches: Vec<RecordBatch>,
        context: Arc<TaskContext>,
        primary_keys: Vec<String>,
        reservation: MemoryReservation,
    ) -> Result<CursorStream<RowValues>> {
        let schema = batches[0].schema();
        let exec = LazyMemoryExec::try_new(
            schema.clone(),
            vec![Arc::new(RwLock::new(
                InMemGenerator::try_new(batches).unwrap(),
            ))],
        )?;
        let stream = exec.execute(0, context.clone())?;
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
    }

    #[tokio::test]
    async fn test_sorted_stream_merger() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        // 创建两列数据，第一列是排序键 "a"，第二列 "b" 用于验证 merge 结果
        let s1b1 = create_batch_two_col_i32(
            "a",
            &[1, 1, 3, 3, 4],
            "b",
            &[101, 102, 103, 104, 105],
        );
        let schema = s1b1.schema();
        let primary_keys = vec!["a".to_string()];

        let pool = Arc::new(GreedyMemoryPool::new(100 * 1024 * 1024)) as _;
        let a1 = MemoryConsumer::new("a1").register(&pool);

        let s1b2 = create_batch_two_col_i32("a", &[4, 5], "b", &[106, 107]);
        let s1b3 = create_batch_two_col_i32("a", &[], "b", &[]);
        let s1b4 = create_batch_two_col_i32("a", &[5], "b", &[108]);
        let s1b5 = create_batch_two_col_i32("a", &[5, 6, 6], "b", &[109, 110, 111]);
        let s1 = create_stream(
            vec![s1b1, s1b2, s1b3, s1b4, s1b5],
            task_ctx.clone(),
            primary_keys.clone(),
            a1.new_empty(),
        )
        .await?;

        let s2b1 = create_batch_two_col_i32("a", &[3, 4], "b", &[201, 202]);
        let s2b2 = create_batch_two_col_i32("a", &[4, 5], "b", &[203, 204]);
        let s2b3 = create_batch_two_col_i32("a", &[], "b", &[]);
        let s2b4 = create_batch_two_col_i32("a", &[5], "b", &[205]);
        let s2b5 = create_batch_two_col_i32("a", &[5, 7], "b", &[206, 207]);
        let s2 = create_stream(
            vec![s2b1, s2b2, s2b3, s2b4, s2b5],
            task_ctx.clone(),
            primary_keys.clone(),
            a1.new_empty(),
        )
        .await?;

        let s3b1 = create_batch_two_col_i32("a", &[], "b", &[]);
        let s3b2 = create_batch_two_col_i32("a", &[5], "b", &[301]);
        let s3b3 = create_batch_two_col_i32("a", &[5, 7], "b", &[302, 303]);
        let s3b4 = create_batch_two_col_i32("a", &[7, 9], "b", &[304, 305]);
        let s3b5 = create_batch_two_col_i32("a", &[], "b", &[]);
        let s3b6 = create_batch_two_col_i32("a", &[10], "b", &[306]);
        let s3 = create_stream(
            vec![s3b1, s3b2, s3b3, s3b4, s3b5, s3b6],
            task_ctx.clone(),
            primary_keys.clone(),
            a1.new_empty(),
        )
        .await?;
        let combiner = Arc::new(Mutex::new(WindowSlidingMerger::new(
            vec![(s1, true), (s2, true), (s3, true)],
            schema,
            3,
            10,
        )?));
        let stream = WindowSlidingMerger::build_merged_stream(combiner)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| a  | b   |",
                "+----+-----+",
                "| 1  | 102 |",
                "| 3  | 201 |",
                "| 4  | 203 |",
                "| 5  | 302 |",
                "| 6  | 111 |",
                "| 7  | 304 |",
                "| 9  | 305 |",
                "| 10 | 306 |",
                "+----+-----+",
            ],
            &batches
        );
        Ok(())
    }
}
