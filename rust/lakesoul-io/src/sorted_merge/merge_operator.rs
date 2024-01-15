// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;

use arrow::array::{as_primitive_array, as_string_array, ArrayBuilder, UInt8Builder};
use arrow_array::{builder::*, types::*, Array, ArrowPrimitiveType};
use arrow_schema::DataType;
use smallvec::SmallVec;

use crate::sorted_merge::sort_key_range::SortKeyArrayRange;
use crate::{sum_all_with_primitive_type_and_append_value, sum_last_with_primitive_type_and_append_value};

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub enum MergeOperator {
    #[default]
    UseLast,
    UseLastNotNull,
    SumAll,
    SumLast,
    JoinedLastByComma,
    JoinedLastBySemicolon,
    JoinedAllByComma,
    JoinedAllBySemicolon,
}

pub enum MergeResult {
    AppendNull,
    AppendValue(usize),
    Extend(usize, usize),
}

impl MergeOperator {
    pub fn from_name(name: &str) -> Self {
        match name {
            "UseLast" => MergeOperator::UseLast,
            "UseLastNotNull" => MergeOperator::UseLastNotNull,
            "SumAll" => MergeOperator::SumAll,
            "SumLast" => MergeOperator::SumLast,
            "JoinedLastByComma" => MergeOperator::JoinedLastByComma,
            "JoinedLastBySemicolon" => MergeOperator::JoinedLastBySemicolon,
            "JoinedAllByComma" => MergeOperator::JoinedAllByComma,
            "JoinedAllBySemicolon" => MergeOperator::JoinedAllBySemicolon,
            _ => panic!("Invalid MergeOperator name"),
        }
    }

    pub fn merge(
        &self,
        data_type: DataType,
        ranges: &SmallVec<[SortKeyArrayRange; 4]>,
        append_array_data_builder: &mut Box<dyn ArrayBuilder>,
    ) -> MergeResult {
        match &ranges.len() {
            0 => MergeResult::AppendNull,
            1 => match self {
                MergeOperator::UseLast => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                MergeOperator::UseLastNotNull => last_non_null(ranges),
                MergeOperator::SumAll => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => sum_all_with_primitive_type(data_type, ranges, append_array_data_builder),
                },
                MergeOperator::SumLast => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => sum_last_with_primitive_type(data_type, ranges, append_array_data_builder),
                },
                MergeOperator::JoinedLastByComma => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => concat_last_with_string_type(ranges, append_array_data_builder, ','),
                },
                MergeOperator::JoinedLastBySemicolon => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => concat_last_with_string_type(ranges, append_array_data_builder, ';'),
                },
                MergeOperator::JoinedAllByComma => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => concat_all_with_string_type(ranges, append_array_data_builder, ','),
                },
                MergeOperator::JoinedAllBySemicolon => match ranges[0].end_row - ranges[0].begin_row {
                    1 => MergeResult::Extend(ranges[0].batch_idx, ranges[0].end_row - 1),
                    _ => concat_all_with_string_type(ranges, append_array_data_builder, ';'),
                },
            },
            _ => match self {
                MergeOperator::UseLast => {
                    MergeResult::Extend(ranges.last().unwrap().batch_idx, ranges.last().unwrap().end_row - 1)
                }
                MergeOperator::UseLastNotNull => last_non_null(ranges),
                MergeOperator::SumAll => sum_all_with_primitive_type(data_type, ranges, append_array_data_builder),
                MergeOperator::SumLast => sum_last_with_primitive_type(data_type, ranges, append_array_data_builder),
                MergeOperator::JoinedLastByComma => concat_last_with_string_type(ranges, append_array_data_builder, ','),
                MergeOperator::JoinedLastBySemicolon => concat_last_with_string_type(ranges, append_array_data_builder, ';'),
                MergeOperator::JoinedAllByComma => concat_all_with_string_type(ranges, append_array_data_builder, ','),
                MergeOperator::JoinedAllBySemicolon => concat_all_with_string_type(ranges, append_array_data_builder, ';'),
            },
        }
    }
}

fn last_non_null(ranges: &SmallVec<[SortKeyArrayRange; 4]>) -> MergeResult {
    let mut is_none = true;
    let mut non_null_row_idx = 0;
    let mut batch_idx = 0;
    let len = ranges.len();
    for range_idx in 0..ranges.len() {
        let range = &ranges[len - range_idx - 1];
        let array = range.array();
        for row_idx in 0..range.end_row - range.begin_row {
            if !array.is_null(range.end_row - row_idx - 1) {
                is_none = false;
                non_null_row_idx = range.end_row - row_idx - 1;
                batch_idx = range.batch_idx;
                break;
            }
        }
        if !is_none {
            break;
        }
    }

    match is_none {
        true => MergeResult::AppendNull,
        false => MergeResult::Extend(batch_idx, non_null_row_idx),
    }
}

fn sum_all_with_primitive_type(
    dt: DataType,
    ranges: &SmallVec<[SortKeyArrayRange; 4]>,
    append_array_data_builder: &mut Box<dyn ArrayBuilder>,
) -> MergeResult {
    match dt {
        DataType::UInt8 => {
            sum_all_with_primitive_type_and_append_value!(UInt8Type, u8, UInt8Builder, append_array_data_builder, ranges)
        }
        DataType::UInt16 => {
            sum_all_with_primitive_type_and_append_value!(UInt16Type, u16, UInt16Builder, append_array_data_builder, ranges)
        }
        DataType::UInt32 => {
            sum_all_with_primitive_type_and_append_value!(UInt32Type, u32, UInt32Builder, append_array_data_builder, ranges)
        }
        DataType::UInt64 => {
            sum_all_with_primitive_type_and_append_value!(UInt64Type, u64, UInt64Builder, append_array_data_builder, ranges)
        }
        DataType::Int8 => {
            sum_all_with_primitive_type_and_append_value!(Int8Type, i8, Int8Builder, append_array_data_builder, ranges)
        }
        DataType::Int16 => {
            sum_all_with_primitive_type_and_append_value!(Int16Type, i16, Int16Builder, append_array_data_builder, ranges)
        }
        DataType::Int32 => {
            sum_all_with_primitive_type_and_append_value!(Int32Type, i32, Int32Builder, append_array_data_builder, ranges)
        }
        DataType::Int64 => {
            sum_all_with_primitive_type_and_append_value!(Int64Type, i64, Int64Builder, append_array_data_builder, ranges)
        }
        DataType::Float32 => sum_all_with_primitive_type_and_append_value!(
            Float32Type,
            f32,
            Float32Builder,
            append_array_data_builder,
            ranges
        ),
        DataType::Float64 => sum_all_with_primitive_type_and_append_value!(
            Float64Type,
            f64,
            Float64Builder,
            append_array_data_builder,
            ranges
        ),
        _ => panic!("{} doesn't support MergeOperator::Sum", dt),
    }
}

fn sum_last_with_primitive_type(
    dt: DataType,
    ranges: &SmallVec<[SortKeyArrayRange; 4]>,
    append_array_data_builder: &mut Box<dyn ArrayBuilder>,
) -> MergeResult {
    match dt {
        DataType::UInt8 => {
            sum_last_with_primitive_type_and_append_value!(UInt8Type, u8, UInt8Builder, append_array_data_builder, ranges)
        }
        DataType::UInt16 => {
            sum_last_with_primitive_type_and_append_value!(UInt16Type, u16, UInt16Builder, append_array_data_builder, ranges)
        }
        DataType::UInt32 => {
            sum_last_with_primitive_type_and_append_value!(UInt32Type, u32, UInt32Builder, append_array_data_builder, ranges)
        }
        DataType::UInt64 => {
            sum_last_with_primitive_type_and_append_value!(UInt64Type, u64, UInt64Builder, append_array_data_builder, ranges)
        }
        DataType::Int8 => {
            sum_last_with_primitive_type_and_append_value!(Int8Type, i8, Int8Builder, append_array_data_builder, ranges)
        }
        DataType::Int16 => {
            sum_last_with_primitive_type_and_append_value!(Int16Type, i16, Int16Builder, append_array_data_builder, ranges)
        }
        DataType::Int32 => {
            sum_last_with_primitive_type_and_append_value!(Int32Type, i32, Int32Builder, append_array_data_builder, ranges)
        }
        DataType::Int64 => {
            sum_last_with_primitive_type_and_append_value!(Int64Type, i64, Int64Builder, append_array_data_builder, ranges)
        }
        DataType::Float32 => sum_last_with_primitive_type_and_append_value!(
            Float32Type,
            f32,
            Float32Builder,
            append_array_data_builder,
            ranges
        ),
        DataType::Float64 => sum_last_with_primitive_type_and_append_value!(
            Float64Type,
            f64,
            Float64Builder,
            append_array_data_builder,
            ranges
        ),
        _ => panic!("{} doesn't support MergeOperator::Sum", dt),
    }
}


fn concat_all_with_string_type(
    ranges: &SmallVec<[SortKeyArrayRange; 4]>,
    append_array_data_builder: &mut Box<dyn ArrayBuilder>,
    delim: char,
) -> MergeResult {
    let mut is_none = false;
    let mut first = true;
    let mut res = String::new();
    for range in ranges.iter() {
        let array = range.array();
        let arr = as_string_array(array.as_ref());
        for i in range.begin_row..range.end_row {
            if !arr.is_null(i) {
                if !first {
                    res.push(delim);
                } else {
                    first = true;
                }
                res.push_str(arr.value(i));
            } else {
                is_none = true;
                break;
            }
        }
        if is_none {
            break;
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

fn concat_last_with_string_type(
    ranges: &SmallVec<[SortKeyArrayRange; 4]>,
    append_array_data_builder: &mut Box<dyn ArrayBuilder>,
    delim: char,
) -> MergeResult {
    let mut is_none = false;
    let mut first = true;
    let mut res = String::new();
    let num_ranges = ranges.len();
    for (idx, range) in ranges.iter().enumerate() {
        let array = range.array();
        let arr = as_string_array(array.as_ref());
        if range.end_row == range.array().len() {
            if idx == num_ranges - 1 || ranges[idx + 1].stream_idx != ranges[idx].stream_idx {
                if !arr.is_null(range.end_row - 1) {
                    if !first {
                        res.push(delim);
                    } else {
                        first = false;
                    }
                    res.push_str(arr.value(range.end_row - 1));
                } else {
                    is_none = true;
                    break;
                }
            }
        } else {
            if !arr.is_null(range.end_row - 1) {
                if !first {
                    res.push(delim);
                } else {
                    first = false;
                }
                res.push_str(arr.value(range.end_row - 1));
            } else {
                is_none = true;
                break;
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


#[macro_export]
macro_rules! sum_all_with_primitive_type_and_append_value {
    ($primitive_type_name:ty, $native_ty:ty, $primitive_builder_type:ty, $builder:ident, $ranges:ident) => {{
        let mut is_none = false;
        let mut res = <$primitive_type_name>::default_value();
        for range in $ranges.iter() {
            let array = range.array();
            let arr = as_primitive_array::<$primitive_type_name>(array.as_ref());
            let values = arr.values();
            let null_buffer = arr.nulls();
            match null_buffer {
                Some(buffer) => {
                    let offset = arr.offset();
                    let null_buf_range = buffer.slice(offset + range.begin_row, range.end_row - range.begin_row);
                    // the entire range is null
                    if null_buf_range.null_count() > 0 {
                        is_none = true;
                        break;
                    }
                }
                None => {}
            }
            res += values[range.begin_row..range.end_row].iter().sum::<$native_ty>();
        }
        match is_none {
            // sum result is null if null value exists
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

#[macro_export]
macro_rules! sum_last_with_primitive_type_and_append_value {
    ($primitive_type_name:ty, $native_ty:ty, $primitive_builder_type:ty, $builder:ident, $ranges:ident) => {{
        let mut is_none = false;
        let mut res = <$primitive_type_name>::default_value();
        let num_ranges = $ranges.len();
        for (idx, range) in $ranges.iter().enumerate() {
            let array = range.array();
            let arr = as_primitive_array::<$primitive_type_name>(array.as_ref());
            let values = arr.values();

            if range.end_row == range.array().len() {
                if idx == num_ranges - 1 || $ranges[idx + 1].stream_idx != $ranges[idx].stream_idx {
                    if !arr.is_null(range.end_row - 1) {
                        res += values[range.end_row - 1]
                    } else {
                        is_none = true;
                        break;
                    }
                }
            } else {
                if !arr.is_null(range.end_row - 1) {
                    res += values[range.end_row - 1]
                } else {
                    is_none = true;
                    break;
                }
            }
        }
        match is_none {
            // sum result is null if null value exists
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


#[cfg(test)]
mod tests {
    use arrow::array::{PrimitiveArray, TimestampMillisecondArray};
    use arrow::datatypes::TimestampMillisecondType;
    #[test]
    fn test_timestamp_with_fixed_offset_tz_fmt_debug() {
        let arr: PrimitiveArray<TimestampMillisecondType> =
            TimestampMillisecondArray::from(vec![1546214400000, 1546214400000, -1546214400000])
                .with_timezone("America/Denver".to_string());
        assert_eq!(
            "PrimitiveArray<Timestamp(Millisecond, Some(\"+08:00\"))>\n[\n  2018-12-31T08:00:00+08:00,\n  2018-12-31T08:00:00+08:00,\n  1921-01-02T08:00:00+08:00,\n]",
            format!("{arr:?}")
        );
    }
}
