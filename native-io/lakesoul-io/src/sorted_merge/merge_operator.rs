use std::fmt::Debug;

use arrow_array::{ArrowPrimitiveType, Array, types::*};
use arrow::array::{
        as_primitive_array, UInt8Builder, ArrayBuilder,
        UInt16Builder, UInt32Builder, UInt64Builder, 
        Int8Builder, Int16Builder, Int32Builder, 
        Int64Builder, Float32Builder, Float64Builder
    };
use arrow_schema::DataType;

use crate::sorted_merge::sort_key_range::SortKeyArrayRange;
use crate::sum_and_append_value;


#[derive(Debug, Clone, PartialEq)]
pub enum MergeOperator {
    UseLast,
    Sum,
}

pub enum MergeResult {
    AppendNull,
    AppendValue,
    Extend(usize, usize),
}

impl MergeOperator{
    pub fn merge(&self, data_type: DataType, ranges: &Vec<SortKeyArrayRange>, append_array_data_builder:&mut Box<dyn ArrayBuilder>) -> MergeResult {
        match &ranges.len() {
            0 => MergeResult::AppendNull,
            1 => {
                match self {
                    MergeOperator::UseLast => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    MergeOperator::Sum => {
                        match ranges[0].end_row - ranges[0].begin_row {
                            1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                            _ => {
                                let is_none = self.sum_according_to_data_type(data_type, ranges, append_array_data_builder);
                                match is_none {
                                    true => MergeResult::AppendNull,
                                    false => MergeResult::AppendValue
                                }
                            }
                        }
                    }
                }
            }

            

            _ => match self {
                MergeOperator::UseLast => {
                    let range = ranges.last().unwrap();
                    MergeResult::Extend(range.batch_idx, range.end_row - 1) 
                },
                MergeOperator::Sum => {
                    let is_none = self.sum_according_to_data_type(data_type, ranges, append_array_data_builder);
                    match is_none {
                        true => MergeResult::AppendNull,
                        false => MergeResult::AppendValue
                    }
                }
                _ => panic!("Only MergeOperator::UseLast and MergeOperator::Sum is supported")
            }
        }
    }

    fn sum_according_to_data_type(&self, dt: DataType, ranges: &Vec<SortKeyArrayRange>, append_array_data_builder:&mut Box<dyn ArrayBuilder>) -> bool {
        assert!(*self == MergeOperator::Sum);
        match dt {
            DataType::UInt8 => sum_and_append_value!(UInt8Type, UInt8Builder, append_array_data_builder, ranges),
            DataType::UInt16 => sum_and_append_value!(UInt16Type, UInt16Builder, append_array_data_builder, ranges),
            DataType::UInt32 => sum_and_append_value!(UInt32Type, UInt32Builder, append_array_data_builder, ranges),
            DataType::UInt64 => sum_and_append_value!(UInt64Type, UInt64Builder, append_array_data_builder, ranges),
            DataType::Int8 => sum_and_append_value!(Int8Type, Int8Builder, append_array_data_builder, ranges),
            DataType::Int16 => sum_and_append_value!(Int16Type, Int16Builder, append_array_data_builder, ranges),
            DataType::Int32 => sum_and_append_value!(Int32Type, Int32Builder, append_array_data_builder, ranges),
            DataType::Int64 => sum_and_append_value!(Int64Type, Int64Builder, append_array_data_builder, ranges),
            DataType::Float32 => sum_and_append_value!(Float32Type, Float32Builder, append_array_data_builder, ranges),
            DataType::Float64 => sum_and_append_value!(Float64Type, Float64Builder, append_array_data_builder, ranges),
            _ => panic!("{} doesn't support MergeOperator::Sum", dt)
        }
    }
}

#[macro_export]
macro_rules! sum_and_append_value {
    ($name:ident, $actual_builder_type:ty, $builder:ident, $ranges:expr) => (
        {
            let mut is_none = true;
            let mut res = <$name>::default_value();
            for range in $ranges.iter() {
                let array = range.array();
                let arr = as_primitive_array::<$name>(array.as_ref());
                let values = arr.values();
                let null_bitmap = arr.data_ref().null_bitmap();
                let offset = arr.data_ref().offset();
                if is_none {
                    match null_bitmap.is_some() {
                        true => {
                            for i in offset+range.begin_row..offset+range.end_row {
                                if null_bitmap.unwrap().is_set(i) {
                                    is_none = false;
                                    break;
                                }
                            }
                        },
                        false => is_none = false
                    }
                }
                for i in range.begin_row..range.end_row {
                    res += values[i];
                }
            }
            match is_none {
                true => $builder.as_any_mut().downcast_mut::<$actual_builder_type>().unwrap().append_null(),
                false => $builder.as_any_mut().downcast_mut::<$actual_builder_type>().unwrap().append_value(res),
            }
            is_none
        }
    );
}
