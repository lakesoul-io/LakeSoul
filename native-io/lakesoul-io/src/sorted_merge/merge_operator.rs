use core::panic;
use std::fmt::{Debug, Formatter};

use arrow_buffer::ArrowNativeType;
use arrow_schema::{DataType};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::array::{as_primitive_array, as_string_array};
use arrow_array::cast::as_boolean_array;
use datafusion::physical_plan::DisplayFormatType::Default;

use crate::sorted_merge::sort_key_range::{SortKeyBatchRange, SortKeyArrayRange};


#[derive(Debug)]
pub enum MergeOperator {
    UseLast,
    Sum,
}

impl MergeOperator{
    pub fn merge_primitive<T:ArrowPrimitiveType>(&self, ranges: &Vec<SortKeyArrayRange>) -> Option<T::Native> {
        match self {
            MergeOperator::UseLast => {
                match ranges.last() {
                    None => None,
                    Some(range) => {
                        if range.array().as_ref().is_valid(range.end_row - 1) {
                            Some(as_primitive_array::<T>(range.array().as_ref()).value(range.end_row - 1))
                        } else {
                            None
                        }
                    }
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
                        for i in 0..ranges.len() {
                            let range = ranges[i].clone();
                            if i < ranges.len() - 1 && range.stream_idx == ranges[i + 1].stream_idx { continue; }
                            if range.array().as_ref().is_valid(range.end_row - 1) {
                                is_none = false;
                                res += as_primitive_array::<T>(range.array().as_ref()).value(range.end_row - 1).as_usize();
                            }
                        }
                        if is_none {
                            None
                        } else {
                            T::Native::from_usize(res)
                        }
                    },
                    DataType::Int32
                    | DataType::Int64 => {
                        let mut res = T::default_value().to_isize().unwrap();
                        let mut is_none = true;
                        ranges.iter().map( |range| {
                            if range.array().as_ref().is_valid(range.end_row - 1) {
                                is_none = false;
                                res += as_primitive_array::<T>(range.array().as_ref()).value(range.end_row - 1).to_isize().unwrap();
                            }
                        });
                        if is_none {
                            None
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

    pub fn merge_utf8(&self, ranges: &Vec<SortKeyArrayRange>) -> Option<String> {
        match self {
            MergeOperator::UseLast => {
                match ranges.last() {
                    None => None,
                    Some(range) => {
                        if range.array().as_ref().is_valid(range.end_row - 1) {
                            Some(as_string_array(range.array().as_ref()).value(range.end_row - 1).to_string())
                        } else {
                            None
                        }
                    }
                }
            },
            _ => todo!()     
        }
    }

    pub fn merge_boolean(&self, ranges: &Vec<SortKeyArrayRange>) -> Option<bool> {
        match self {
            MergeOperator::UseLast => {
                match ranges.last() {
                    None => None,
                    Some(range) => {
                        if range.array().as_ref().is_valid(range.end_row - 1) {
                            Some(as_boolean_array(range.array().as_ref()).value(range.end_row - 1))
                        } else {
                            None
                        }
                    }
                }
            },
            _ => todo!()     
        }
    }
}