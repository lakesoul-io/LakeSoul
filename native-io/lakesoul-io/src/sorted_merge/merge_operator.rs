/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::fmt::Debug;

use arrow::array::{as_primitive_array, as_string_array, ArrayBuilder, UInt8Builder};
use arrow_array::{builder::*, types::*, Array, ArrowPrimitiveType};
use arrow_schema::DataType;

use crate::sorted_merge::sort_key_range::SortKeyArrayRange;
use crate::sum_with_primitive_type_and_append_value;

#[derive(Debug, Clone, PartialEq)]
pub enum MergeOperator {
    UseLast,
    Sum,
    Concat,
}

impl Default for MergeOperator{
    fn default() -> Self {
        MergeOperator::UseLast
    }
}

pub enum MergeResult {
    AppendNull,
    AppendValue(usize),
    Extend(usize, usize),
}

impl MergeOperator {
    pub fn from_name(name: &str)-> Self{
        match name {
            "UseLast" => MergeOperator::UseLast,
            "Sum" => MergeOperator::Sum,
            "Concat" => MergeOperator::Concat,
            _ => panic!("Invalid MergeOperator name")
        }
    }

    pub fn merge(
        &self,
        data_type: DataType,
        ranges: &Vec<SortKeyArrayRange>,
        append_array_data_builder: &mut Box<dyn ArrayBuilder>,
    ) -> MergeResult {
        match &ranges.len() {
            0 => MergeResult::AppendNull,
            1 => match self {
                MergeOperator::UseLast => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                MergeOperator::Sum => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => self.sum_with_primitive_type(data_type, ranges, append_array_data_builder),
                },
                MergeOperator::Concat => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => self.concat_with_string_type(ranges, append_array_data_builder),
                },
            },
            _ => match self {
                MergeOperator::UseLast => {
                    let range = ranges.last().unwrap();
                    MergeResult::Extend(range.batch_idx, range.end_row - 1)
                }
                MergeOperator::Sum => self.sum_with_primitive_type(data_type, ranges, append_array_data_builder),
                MergeOperator::Concat => self.concat_with_string_type(ranges, append_array_data_builder),
            },
        }
    }

    fn sum_with_primitive_type(
        &self,
        dt: DataType,
        ranges: &Vec<SortKeyArrayRange>,
        append_array_data_builder: &mut Box<dyn ArrayBuilder>,
    ) -> MergeResult {
        match dt {
            DataType::UInt8 => {
                sum_with_primitive_type_and_append_value!(UInt8Type, UInt8Builder, append_array_data_builder, ranges)
            }
            DataType::UInt16 => {
                sum_with_primitive_type_and_append_value!(UInt16Type, UInt16Builder, append_array_data_builder, ranges)
            }
            DataType::UInt32 => {
                sum_with_primitive_type_and_append_value!(UInt32Type, UInt32Builder, append_array_data_builder, ranges)
            }
            DataType::UInt64 => {
                sum_with_primitive_type_and_append_value!(UInt64Type, UInt64Builder, append_array_data_builder, ranges)
            }
            DataType::Int8 => {
                sum_with_primitive_type_and_append_value!(Int8Type, Int8Builder, append_array_data_builder, ranges)
            }
            DataType::Int16 => {
                sum_with_primitive_type_and_append_value!(Int16Type, Int16Builder, append_array_data_builder, ranges)
            }
            DataType::Int32 => {
                sum_with_primitive_type_and_append_value!(Int32Type, Int32Builder, append_array_data_builder, ranges)
            }
            DataType::Int64 => {
                sum_with_primitive_type_and_append_value!(Int64Type, Int64Builder, append_array_data_builder, ranges)
            }
            DataType::Float32 => sum_with_primitive_type_and_append_value!(
                Float32Type,
                Float32Builder,
                append_array_data_builder,
                ranges
            ),
            DataType::Float64 => sum_with_primitive_type_and_append_value!(
                Float64Type,
                Float64Builder,
                append_array_data_builder,
                ranges
            ),
            _ => panic!("{} doesn't support MergeOperator::Sum", dt),
        }
    }

    fn concat_with_string_type(
        &self,
        ranges: &Vec<SortKeyArrayRange>,
        append_array_data_builder: &mut Box<dyn ArrayBuilder>,
    ) -> MergeResult {
        let mut is_none = true;
        let mut res = String::new();
        for range in ranges.iter() {
            let array = range.array();
            let arr = as_string_array(array.as_ref());
            for i in range.begin_row..range.end_row {
                if !arr.is_null(i) {
                    if !is_none {
                        res.push(',');
                    }
                    is_none = false;
                    res.push_str(arr.value(i));
                }
            }
        }
        match is_none {
            true => MergeResult::AppendNull,
            false => {
                append_array_data_builder
                    .as_any_mut()
                    .downcast_mut::<StringBuilder>()
                    .unwrap()
                    .append_value(res);
                MergeResult::AppendValue(append_array_data_builder.len() - 1)
            }
        }
    }
}

#[macro_export]
macro_rules! sum_with_primitive_type_and_append_value {
    ($primitive_type_name:ty, $primitive_builder_type:ty, $builder:ident, $ranges:ident) => {{
        let mut is_none = true;
        let mut res = <$primitive_type_name>::default_value();
        for range in $ranges.iter() {
            let array = range.array();
            let arr = as_primitive_array::<$primitive_type_name>(array.as_ref());
            let values = arr.values();
            let null_bitmap = arr.data_ref().null_bitmap();
            let offset = arr.data_ref().offset();
            if is_none {
                match null_bitmap.is_some() {
                    true => {
                        for i in offset + range.begin_row..offset + range.end_row {
                            if null_bitmap.unwrap().is_set(i) {
                                is_none = false;
                                break;
                            }
                        }
                    }
                    false => is_none = false,
                }
            }
            for i in range.begin_row..range.end_row {
                res += values[i];
            }
        }
        match is_none {
            true => MergeResult::AppendNull,
            false => {
                $builder
                    .as_any_mut()
                    .downcast_mut::<$primitive_builder_type>()
                    .unwrap()
                    .append_value(res);
                MergeResult::AppendValue($builder.len() - 1)
            }
        }
    }};
}
