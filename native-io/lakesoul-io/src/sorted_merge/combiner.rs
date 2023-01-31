use std::fmt::Debug;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::sorted_merge::sort_key_range::{SortKeyBatchRange, SortKeyArrayRange, SortKeyArrayRanges};
use crate::sorted_merge::merge_operator::MergeOperator;

use arrow::{error::Result as ArrowResult, 
    error::ArrowError,  
    record_batch::RecordBatch, 
    datatypes::{SchemaRef, DataType, ArrowPrimitiveType},
    array::{
        make_array as make_arrow_array, Array, ArrayRef, PrimitiveBuilder,
        BooleanBuilder, OffsetSizeTrait, GenericStringBuilder
    },
};
use arrow_array::types::*;


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

        RecordBatch::try_new(self.schema.clone(), columns)
    }


   

}

fn merge_sort_key_array_ranges_with_primitive<T:ArrowPrimitiveType>(capacity:usize, ranges:&Vec<Vec<SortKeyArrayRange>>, merge_operator:&MergeOperator) ->ArrayRef {
    let mut array_data_builder = PrimitiveBuilder::<T>::with_capacity(capacity);
    for i in 0..ranges.len() {
        let ranges_of_row = ranges[i].clone();
        let res = merge_operator.merge_primitive::<T>(&ranges_of_row);
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
