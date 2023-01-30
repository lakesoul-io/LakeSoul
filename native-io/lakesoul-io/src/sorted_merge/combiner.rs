use crate::sorted_merge::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
use crate::sorted_merge::fetcher::NonUniqueSortKeyRangeFetcher;
use crate::sorted_merge::sorted_stream_merger::SortKeyRange;
use crate::sorted_merge::sort_key_range::{SortKeyBatchRange, SortKeyArrayRange, SortKeyArrayRanges};
use crate::sorted_merge::record_batch_builder::MergedArrayData;
use crate::sorted_merge::merge_operator::MergeOperator;


use async_trait::async_trait;
use dary_heap::DaryHeap;
use datafusion::error::Result;
use futures::future::try_join_all;
use smallvec::SmallVec;

use arrow::{error::Result as ArrowResult, 
    error::ArrowError,  
    record_batch::RecordBatch, 
    datatypes::{SchemaRef, DataType, ArrowPrimitiveType, ArrowNativeType},
    array::{
        make_array as make_arrow_array, ArrayData, Array, ArrayRef, Int16Builder, PrimitiveBuilder,
        BooleanBuilder, OffsetSizeTrait, GenericStringBuilder
    },
    buffer::Buffer,
};
use arrow_array::types::*;


use std::fmt::{Debug, Formatter};
use std::cmp::Reverse;
use std::sync::Arc;
use std::ops::Deref;
use std::borrow::Borrow;
use std::collections::BinaryHeap;
use std::pin::Pin;


#[derive(Debug)]
pub enum RangeCombiner {
    MinHeapSortKeyBatchRangeCombiner(MinHeapSortKeyBatchRangeCombiner),
}

impl RangeCombiner {
    pub fn new(
        schema: SchemaRef,
        streams_num:usize,
        target_batch_size: usize) -> Self {
        RangeCombiner::MinHeapSortKeyBatchRangeCombiner(MinHeapSortKeyBatchRangeCombiner::new(schema, streams_num, target_batch_size))
    }

    pub fn push_range(&mut self, range: Reverse<SortKeyBatchRange>) {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(combiner) => combiner.push(range)
        };
    }

    pub fn poll_result(&mut self) -> RangeCombinerResult {
        match self {
            RangeCombiner::MinHeapSortKeyBatchRangeCombiner(combiner) => combiner.poll_result()
        }
    }
}

#[derive(Debug)]
pub enum RangeCombinerResult {
    None,
    Err(ArrowError),
    Range(Reverse<SortKeyBatchRange>),
    RecordBatch(ArrowResult<RecordBatch>),
}

#[derive(Debug)]
pub struct MinHeapSortKeyBatchRangeCombiner{
    schema: SchemaRef,
    heap: BinaryHeap<Reverse<SortKeyBatchRange>>,
    in_progress: Vec<SortKeyArrayRanges>,
    target_batch_size: usize,
    current_sort_key_range: SortKeyArrayRanges,
    merge_operator: MergeOperator,
}

impl MinHeapSortKeyBatchRangeCombiner{
    pub fn new(
        schema: SchemaRef,
        streams_num: usize, 
        target_batch_size: usize) -> Self {
        let new_range = SortKeyArrayRanges::new(schema.clone());
        MinHeapSortKeyBatchRangeCombiner{
            schema: schema.clone(),
            heap: BinaryHeap::with_capacity(streams_num),
            in_progress: vec![],
            target_batch_size: target_batch_size,
            current_sort_key_range: new_range,
            merge_operator: MergeOperator::UseLast,
        }
    }

    pub fn push(&mut self, range: Reverse<SortKeyBatchRange>) {
        self.heap.push(range)
    }

    pub fn poll_result(&mut self) -> RangeCombinerResult  {
        if self.in_progress.len() == self.target_batch_size {
            RangeCombinerResult::RecordBatch(self.build_record_batch())
        } else {
            match self.heap.pop() {
                Some(Reverse(range)) => {
                    if self.current_sort_key_range.match_row(&range) {
                        self.current_sort_key_range.add_range_in_batch(range.clone());
                    } else {
                        self.in_progress.push(self.current_sort_key_range.clone());
                        self.current_sort_key_range = SortKeyArrayRanges::new(self.schema.clone());
                        self.current_sort_key_range.add_range_in_batch(range.clone());
                    }
                    RangeCombinerResult::Range(Reverse(range)) 
                }
                None => {
                    if self.current_sort_key_range.is_empty() && self.in_progress.is_empty() {
                        RangeCombinerResult::None
                    } else {
                        if !self.current_sort_key_range.is_empty() {
                            self.in_progress.push(self.current_sort_key_range.clone());
                            self.current_sort_key_range.set_batch_range(None);
                        }
                        RangeCombinerResult::RecordBatch(self.build_record_batch())
                    }
                }
            }
        }
    }
 
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, field)| {
                let capacity = self.in_progress.len();
                let data_type = (*field.data_type()).clone();
                let ranges_per_col:Vec<Vec<SortKeyArrayRange>> = self.in_progress
                    .iter()
                    .map(|ranges_per_row| ranges_per_row.column(column_idx))
                    .collect::<Vec<_>>();

                match data_type {
                    DataType::Int16 => merge_sort_key_array_ranges_with_primitive::<Int16Type>(capacity, &ranges_per_col, &self.merge_operator),
                    DataType::Int32 => merge_sort_key_array_ranges_with_primitive::<Int32Type>(capacity, &ranges_per_col, &self.merge_operator),
                    // Note: If the maximum length (in bytes) of the stored string exceeds the maximum value of i32, we need to update i32 to i64
                    DataType::Utf8 => merge_sort_key_array_ranges_with_utf8::<i32>(capacity, &ranges_per_col, &self.merge_operator),
                    DataType::Int64 => merge_sort_key_array_ranges_with_primitive::<Int64Type>(capacity, &ranges_per_col, &self.merge_operator),
                    DataType::Boolean => merge_sort_key_array_ranges_with_boolean(capacity, &ranges_per_col, &self.merge_operator),
                    _ => todo!()
                }
                
            })
            .collect();

        self.in_progress.clear();

        // todo!("drain exhausted batch");

        RecordBatch::try_new(self.schema.clone(), columns)
    }


   

}

fn merge_sort_key_array_ranges_with_primitive<T:ArrowPrimitiveType>(capacity:usize, ranges:&Vec<Vec<SortKeyArrayRange>>, merge_operator:&MergeOperator) ->ArrayRef {
    let mut array_data_builder = PrimitiveBuilder::<T>::with_capacity(capacity);
    for i in 0..ranges.len() {
        let ranges_pre_row = ranges[i].clone();
        let res = merge_operator.merge_primitive::<T>(&ranges_pre_row);
        match res.is_some() {
            true => array_data_builder.append_value(res.unwrap()),
            false => array_data_builder.append_null()
        }
    }

    make_arrow_array(array_data_builder.finish().into_data())
}

fn merge_sort_key_array_ranges_with_utf8<OffsetSize: OffsetSizeTrait>(capacity:usize, ranges:&Vec<Vec<SortKeyArrayRange>>, merge_operator:&MergeOperator) ->ArrayRef {
    let mut array_data_builder = GenericStringBuilder::<OffsetSize>::with_capacity(capacity, capacity);
    for range in ranges.iter() {
        let res = merge_operator.merge_utf8(range);
        match res.is_some() {
            true => array_data_builder.append_value(res.unwrap()),
            false => array_data_builder.append_null()
        }
    }

    make_arrow_array(array_data_builder.finish().into_data())
}

fn merge_sort_key_array_ranges_with_boolean(capacity:usize, ranges:&Vec<Vec<SortKeyArrayRange>>, merge_operator:&MergeOperator) ->ArrayRef {
    let mut array_data_builder = BooleanBuilder::with_capacity(capacity);
    for range in ranges.iter() {
        let res = merge_operator.merge_boolean(range);
        match res.is_some() {
            true => array_data_builder.append_value(res.unwrap()),
            false => array_data_builder.append_null()
        }
    }

    make_arrow_array(array_data_builder.finish().into_data())
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
    use crate::sorted_merge::record_batch_builder::MergedArrayData;
    use crate::sorted_merge::merge_traits::{StreamSortKeyRangeCombiner, StreamSortKeyRangeFetcher};
    use crate::sorted_merge::combiner::MinHeapSortKeyRangeCombiner;
    use crate::sorted_merge::fetcher::NonUniqueSortKeyRangeFetcher;
    use crate::sorted_merge::sorted_stream_merger::{SortKeyRange};
    use crate::sorted_merge::sort_key_range::{SortKeyBatchRange, SortKeyArrayRange, SortKeyArrayRanges};
    use crate::sorted_merge::merge_operator::MergeOperator;
    use arrow::array::{Array, ArrayRef, Int32Array, UInt16Array, BooleanArray, UInt32Array};
    use arrow::array::as_primitive_array;
    use arrow::datatypes::{UInt16Type, Int32Type, ArrowPrimitiveType, ArrowNativeType, UInt32Type};
    use arrow::array::{Int32Builder, PrimitiveBuilder, BooleanBuilder};
    use arrow::array::{make_array as make_arrow_array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};

    use datafusion::arrow::util::pretty::print_batches;
    use datafusion::error::Result;
    use datafusion::execution::context::TaskContext;
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion::assert_batches_eq;

    use futures_util::StreamExt;
    use parquet::data_type;
    use std::future::Ready;
    use std::sync::Arc;
    use smallvec::SmallVec;

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

    fn create_batch_two_col_i32_uint16(col_one_name: &str, id_vec: &[i32], col_two_name: &str, num_vec: Vec<Option<u16>>) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from_slice(id_vec));
        let b: ArrayRef = Arc::new(UInt16Array::from(num_vec));
        RecordBatch::try_from_iter_with_nullable(vec![
            (col_one_name, a, false),
            (col_two_name, b, true),
        ]).unwrap()
    }

    fn assert_sort_key_range_in_batch(sk: &SortKeyBatchRange, begin_row: usize, end_row: usize, batch: &RecordBatch) {
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

    /**
    In product environment, we can use this function definition
    fn merge_multi_sort_key_ranges(merged_array_data: Vec<&mut MergedArrayData>, result_schema: SchemaRef, sort_key_ranges: SmallVec::<[Box<SortKeyRange>; 4]>,
            merge_operator: Vec<merge_op>) {
        for i in 0..col_nums {
            merge_one_column(merged_array_data[i], result_schema.fields[i].data_type(), sort_key_ranges)
        }
    }
    */
    fn merge_one_column(merged_array_data: &mut MergedArrayData, dt: &DataType, sort_key_ranges: SmallVec::<[Box<SortKeyRange>; 4]>) {
        let mut result = 0i32;
        for i in 0..sort_key_ranges.len() {
            let last_range_in_batch = sort_key_ranges[i].sort_key_ranges.last().unwrap();
            
            let arr = as_primitive_array::<Int32Type>(last_range_in_batch.batch.column(0));
            // .as_any()
            //         .downcast_ref::<Int32Array>()
            //         .expect("Failed to downcast");
            let p = arr.value(last_range_in_batch.end_row);
            result += p;
        }
        merged_array_data.push_non_null_item(result);
    }

    fn merge_sort_key_array_ranges_with_primitive<T:ArrowPrimitiveType>(capacity:usize, ranges:&Vec<Vec<SortKeyArrayRange>>, merge_operator:&MergeOperator) ->ArrayRef {
        let mut array_data_builder = PrimitiveBuilder::<T>::with_capacity(capacity);
        for i in 0..ranges.len() {
            let ranges_pre_row = ranges[i].clone();
            match merge_operator {
                MergeOperator::UseLast => {
                    // match ranges_pre_row.last() {
                    //     None => None,
                    //     Some(range) => todo!()
                    // }
                    let range = ranges_pre_row.last().unwrap();
                    if range.array().as_ref().is_valid(range.end_row) {
                        array_data_builder.append_value(as_primitive_array::<T>(range.array().as_ref()).value(range.end_row));
                    } else {
                        array_data_builder.append_null();
                    }
                },
                MergeOperator::Sum => {
                    match T::DATA_TYPE {
                        DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Int8
                        | DataType::Int16 => { // todo: Int8 and Int16 may be wrong here
                            let mut res = T::default_value().as_usize();
                            let mut is_none = true;
                            for i in 0..ranges_pre_row.len() {
                                let range = ranges_pre_row[i].clone();
                                if i < ranges_pre_row.len() - 1 && range.stream_idx == ranges_pre_row[i + 1].stream_idx { continue; }
                                if range.array().as_ref().is_valid(range.end_row) {
                                    is_none = false;
                                    res += as_primitive_array::<T>(range.array().as_ref()).value(range.end_row).as_usize();
                                }
                            }
                            if is_none {
                                array_data_builder.append_null();
                            } else {
                                array_data_builder.append_value(T::Native::from_usize(res).unwrap());
                            }
                        },
                        DataType::Int32
                        | DataType::Int64 => {
                            let mut res = T::default_value().to_isize().unwrap();
                            let mut is_none = true;
                            for range in ranges_pre_row {
                                if range.array().as_ref().is_valid(range.end_row) {
                                    is_none = false;
                                    res += as_primitive_array::<T>(range.array().as_ref()).value(range.end_row).to_isize().unwrap();
                                }
                            }
                            if is_none {
                                todo!()
                            } else {
                                todo!()
                            }
                        },
                        DataType::Float16
                        | DataType::Float32
                        | DataType::Float64 => todo!(),
                        _ => panic!("{} is not PrimitiveType", T::DATA_TYPE)
                    }
    
                }
            }
        }
    
        make_arrow_array(array_data_builder.finish().into_data())
    }

    fn merge_sort_key_array_ranges_with_boolean(capacity:usize, ranges:&Vec<Vec<SortKeyArrayRange>>, merge_operator:&MergeOperator) ->ArrayRef {
        let mut array_data_builder = BooleanBuilder::with_capacity(capacity);
        for range in ranges.iter() {
            let res = merge_operator.merge_boolean(range);
            match res.is_some() {
                true => array_data_builder.append_value(res.unwrap()),
                false => array_data_builder.append_null()
            }
        }

        make_arrow_array(array_data_builder.finish().into_data())
    }

    fn build_record_batch(in_progress: &Vec<SortKeyArrayRanges>, schema: SchemaRef, merge_operator: &MergeOperator) -> ArrowResult<RecordBatch> {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, field)| {
                let capacity = in_progress.len();
                let data_type = (*field.data_type()).clone();
                let ranges_per_col:Vec<Vec<SortKeyArrayRange>> = in_progress
                    .iter()
                    .map(|ranges_per_row| ranges_per_row.column(column_idx))
                    .collect::<Vec<_>>();

                match data_type {
                    DataType::UInt16 => merge_sort_key_array_ranges_with_primitive::<UInt16Type>(capacity, &ranges_per_col, merge_operator),
                    DataType::UInt32 => merge_sort_key_array_ranges_with_primitive::<UInt32Type>(capacity, &ranges_per_col, merge_operator),
                    DataType::Boolean => merge_sort_key_array_ranges_with_boolean(capacity, &ranges_per_col, merge_operator),
                    DataType::Int32 => merge_sort_key_array_ranges_with_primitive::<Int32Type>(capacity, &ranges_per_col, &MergeOperator::UseLast),
                    _ => todo!()
                }

            })
            .collect();
        RecordBatch::try_new(schema.clone(), columns)
    }

    #[tokio::test]
    async fn test_multi_streams_combine_and_merge() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let rb1 = create_batch_one_col_i32("id", &[1, 1, 3, 3, 4]);
        let rb2 = create_batch_one_col_i32("id", &[1, 1, 1, 2, 3, 3, 4]);
        let rb3 = create_batch_one_col_i32("id", &[0, 1, 2, 3, 3, 4, 4, 5]);
        let s1fetcher = create_stream_fetcher(
            0,
            vec![rb1.clone()],
            task_ctx.clone(),
            vec!["id"],
        )
        .await
        .unwrap();

        let s2fetcher = create_stream_fetcher(
            1,
            vec![rb2.clone()],
            task_ctx.clone(),
            vec!["id"],
        )
        .await
        .unwrap();
        let s3fetcher = create_stream_fetcher(
            2,
            vec![
                rb3.clone()
            ],
            task_ctx.clone(),
            vec!["id"],
        )
        .await
        .unwrap();


        let mut combiner = MinHeapSortKeyRangeCombiner::<NonUniqueSortKeyRangeFetcher, 2>::with_fetchers(vec![
            s1fetcher, s2fetcher, s3fetcher,
        ]);
        combiner.init().await.unwrap();

        let field = Field::new("id", DataType::Int32, false);
        let mut array_data = MergedArrayData::new(&field, 6);

        let dt = DataType::Int32;

        // [0] from s3
        let ranges = combiner.next().await.unwrap().unwrap();
        merge_one_column(&mut array_data, &dt,ranges);
        // [1, 1] from s1, [1, 1, 1] from s2, [1, 1] from s3
        let ranges = combiner.next().await.unwrap().unwrap();
        merge_one_column(&mut array_data, &dt,ranges);
        // [2] from s2, [2] from s3
        let ranges = combiner.next().await.unwrap().unwrap();
        merge_one_column(&mut array_data, &dt,ranges);
        // [3, 3] from s1, [3, 3] from s2, [3, 3] from s3
        let ranges = combiner.next().await.unwrap().unwrap();
        merge_one_column(&mut array_data, &dt,ranges);
        // [4] from s1, [4] from s2, [4, 4] from s3
        let ranges = combiner.next().await.unwrap().unwrap();
        merge_one_column(&mut array_data, &dt,ranges);
        // [5] from s3
        let ranges = combiner.next().await.unwrap().unwrap();
        merge_one_column(&mut array_data, &dt,ranges);
        // end
        let ranges = combiner.next().await.unwrap();
        assert!(ranges.is_none());

        let ad = array_data.freeze();
        let column = make_arrow_array(ad);
        let schema = Schema::new(vec![field]);
        let rb = RecordBatch::try_new(std::sync::Arc::new(schema), vec![column]).unwrap();
        assert_batches_eq!(
            &[
                "+----+",
                "| id |",
                "+----+",
                "| 0  |",
                "| 3  |",
                "| 4  |",
                "| 9  |",
                "| 12 |",
                "| 5  |",
                "+----+",
            ]
            , &[rb]);
    }

    #[tokio::test]
    async fn test_multi_streams_with_same_schema_combine_and_merge() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let s1rb1 = create_batch_two_col_i32_uint16("id", &[1, 1, 2, 3, 3, 4],
            "salary", vec![Some(1u16), None, Some(2), Some(2u16), Some(4u16), None]);
        let s1rb2 = create_batch_two_col_i32_uint16("id", &[4, 5, 6, 6, 7, 9],
            "salary", vec![Some(4u16), None, Some(6u16), Some(4u16), None, Some(9)]);
        let s2rb1 = create_batch_two_col_i32_uint16("id", &[1, 1, 1, 2, 3, 3, 4],
            "salary", vec![None, Some(1u16), Some(3u16), None, Some(4u16), None, Some(8u16)]);
        let s2rb2 = create_batch_two_col_i32_uint16("id", &[4, 4, 5, 6, 8, 8],
            "salary", vec![Some(5u16), Some(2u16), Some(3u16), Some(7u16), None, Some(8u16)]);
        let s3rb1 = create_batch_two_col_i32_uint16("id", &[0, 1, 3, 3, 4, 4, 5],
            "salary", vec![None, Some(6u16), Some(5u16), Some(6u16), None, None, Some(7u16)]);
        let s3rb2 = create_batch_two_col_i32_uint16("id", &[5, 7, 8],
            "salary", vec![None, Some(2u16), None]);

        let s1fetcher = create_stream_fetcher(
            0,
            vec![s1rb1.clone(), s1rb2.clone()],
            task_ctx.clone(),
            vec!["id"],
        )
        .await.
        unwrap();
        let s2fetcher = create_stream_fetcher(
            1,
            vec![s2rb1.clone(), s2rb2.clone()],
            task_ctx.clone(),
            vec!["id"],
        )
        .await
        .unwrap();
        let s3fetcher = create_stream_fetcher(
            2,
            vec![s3rb1.clone(), s3rb2.clone()],
            task_ctx.clone(),
            vec!["id"],
        )
        .await
        .unwrap();

        let mut combiner = MinHeapSortKeyRangeCombiner::<NonUniqueSortKeyRangeFetcher, 2>::with_fetchers(vec![
            s1fetcher, s2fetcher, s3fetcher,
        ]);
        combiner.init().await.unwrap();

        let field_a = Field::new("id", DataType::Int32, false);
        let field_b = Field::new("salary", DataType::UInt16, true);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let mut in_progrss: Vec<SortKeyArrayRanges> = vec![];
        for i in 0..10 {
            let ranges = combiner.next().await.unwrap().unwrap();
            let mut current_sort_key_range = SortKeyArrayRanges::new(schema.clone());
            for j in 0..ranges.len() {
                let range = ranges[j].clone();
                for k in 0..range.sort_key_ranges.len() {
                    let batch_range = range.sort_key_ranges[k].clone();
                    current_sort_key_range.add_range_in_batch(batch_range);
                }
            }
            in_progrss.push(current_sort_key_range);

        }
        let ranges = combiner.next().await.unwrap();
        assert!(ranges.is_none());

        // The order of SortKeyRange is as follows:
        // {[0], [None]} from s3
        // {[1, 1], [1, None]} from s1, {[1, 1, 1], [None, 1, 3]} from s2, {[1], [6]} from s3
        // {[2], [2]} from s1, {[2], [None]} from s2
        // {[3, 3], [2, 4]} from s1, {[3, 3], [4, None]} from s2, {[3, 3], [5, 6]} from s3
        // {[4, 4], [None, 4]} from s1, {[4, 4, 4], [8, 5, 2]} from s2, {[4, 4], [None, None]} from s3
        // {[5], [None]} from s1, {[5], [3]} from s2, {[5, 5], [7, None]} from s3
        // {[6, 6], [6, 4]} from s1, {[6], [7]} from s2
        // {[7], [None]} from s1, {[7], [2]} from s3
        // {[8, 8], [None, 8]} from s2, {[8], [None]} from s3
        // {[9], [9]} from s1
        let rb = build_record_batch(&in_progrss, schema.clone(), &MergeOperator::UseLast).unwrap();
        assert_batches_eq!(
            &[
                "+----+--------+",
                "| id | salary |",
                "+----+--------+",
                "| 0  |        |",
                "| 1  | 6      |",
                "| 2  |        |",
                "| 3  | 6      |",
                "| 4  |        |",
                "| 5  |        |",
                "| 6  | 7      |",
                "| 7  | 2      |",
                "| 8  |        |",
                "| 9  | 9      |",
                "+----+--------+",
            ]
            , &[rb]);
        let rb = build_record_batch(&in_progrss, schema.clone(), &MergeOperator::Sum).unwrap();
        assert_batches_eq!(
            &[
                "+----+--------+",
                "| id | salary |",
                "+----+--------+",
                "| 0  |        |",
                "| 1  | 9      |",
                "| 2  | 2      |",
                "| 3  | 10     |",
                "| 4  | 6      |",
                "| 5  | 3      |",
                "| 6  | 11     |",
                "| 7  | 2      |",
                "| 8  | 8      |",
                "| 9  | 9      |",
                "+----+--------+",
            ]
            , &[rb]);

    }

    #[test]
    fn test_array_data_builder() {
        let mut primitive_array_builder = Int32Builder::with_capacity(100);

        // Append an individual primitive value
        primitive_array_builder.append_value(55);
    
        // Append a null value
        primitive_array_builder.append_null();
    
        // Append a slice of primitive values
        primitive_array_builder.append_slice(&[39, 89, 12]);
    
        // Append lots of values
        primitive_array_builder.append_null();
        primitive_array_builder.append_slice(&(25..50).collect::<Vec<i32>>());
    
        // Build the `PrimitiveArray`
        let primitive_array = primitive_array_builder.finish();
        // Long arrays will have an ellipsis printed in the middle
        println!("{:?}", primitive_array);

        let field = Field::new("id", DataType::Int32, true);
        let schema = Schema::new(vec![field]);
        let rb = RecordBatch::try_new(std::sync::Arc::new(schema), vec![Arc::new(primitive_array)]).unwrap();
    }
}
