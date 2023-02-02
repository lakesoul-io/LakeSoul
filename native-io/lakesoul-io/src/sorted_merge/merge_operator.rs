use std::fmt::Debug;

use arrow_schema::{DataType};
use arrow::array::ArrayBuilder;

use crate::sorted_merge::sort_key_range::SortKeyArrayRange;


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
            1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
            _ => match self {
                MergeOperator::UseLast => {
                    let range = ranges.last().unwrap();
                    MergeResult::Extend(range.batch_idx, range.end_row - 1) 
                },
                MergeOperator::Sum => {
                    match data_type {
                        _ => panic!("{} is not PrimitiveType", data_type)
                    }
                }
                _ => panic!("Only MergeOperator::UseLast and MergeOperator::Sum is supported")
            }
        }
    }
}