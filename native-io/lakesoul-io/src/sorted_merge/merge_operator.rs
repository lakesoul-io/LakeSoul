use std::fmt::{Debug, Formatter};

use arrow_schema::{DataType};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::array::as_primitive_array;

use crate::sorted_merge::sort_key_range::{SortKeyBatchRange, SortKeyArrayRange};


#[derive(Debug)]
pub enum MergeOperator {
    UseLast,
}

impl MergeOperator{
    pub fn merge_primitive<T:ArrowPrimitiveType>(&self, ranges: &Vec<SortKeyArrayRange>) -> T::Native {
        match self {
            MergeOperator::UseLast => {
                let range = ranges.last().unwrap();
                as_primitive_array::<T>(range.array().as_ref()).value(range.end_row - 1)
            }        
        }
    }
}