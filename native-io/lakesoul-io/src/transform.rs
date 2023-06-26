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
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{as_primitive_array, as_struct_array, make_array, Array};
use arrow::record_batch::RecordBatch;
use arrow_array::{new_null_array, types::*, ArrayRef, PrimitiveArray, RecordBatchOptions, StringArray, StructArray, BooleanArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

pub fn uniform_schema(orig_schema: SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        orig_schema
            .fields()
            .iter()
            .map(|field| {
                let data_type = field.data_type();
                match data_type {
                    DataType::Timestamp(unit, Some(_)) => Field::new(
                        field.name(),
                        DataType::Timestamp(unit.clone(), Some(String::from(crate::constant::LAKESOUL_TIMEZONE))),
                        field.is_nullable(),
                    ),
                    _ => field.clone(),
                }
            })
            .collect::<Vec<_>>(),
    ))
}

pub fn uniform_record_batch(batch: RecordBatch) -> RecordBatch {
    transform_record_batch(
        uniform_schema(batch.schema()),
        batch,
        false,
        Arc::new(Default::default()),
    )
}

pub fn transform_schema(target_schema: SchemaRef, schema: SchemaRef, use_default: bool) -> SchemaRef {
    if use_default {
        target_schema
    } else {
        Arc::new(Schema::new(
            target_schema
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(_, target_field)| {
                    schema
                        .column_with_name(target_field.name())
                        .map(|_| target_field.clone())
                })
                .collect::<Vec<_>>(),
        ))
    }
}

pub fn transform_record_batch(
    target_schema: SchemaRef,
    batch: RecordBatch,
    use_default: bool,
    default_column_value: Arc<HashMap<String, String>>,
) -> RecordBatch {
    let num_rows = batch.num_rows();
    let orig_schema = batch.schema();
    let mut transform_arrays = Vec::new();
    let transform_schema = Arc::new(Schema::new(
        target_schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(
                |(_, target_field)| match orig_schema.column_with_name(target_field.name()) {
                    Some((idx, _)) => {
                        let data_type = target_field.data_type();
                        let transformed_array = transform_array(
                            target_field.name().to_string(),
                            data_type.clone(),
                            batch.column(idx).clone(),
                            num_rows,
                            use_default,
                            default_column_value.clone(),
                        );
                        transform_arrays.push(transformed_array);
                        Some(target_field.clone())
                    }
                    None if use_default => {
                        let default_value_array = match default_column_value.get(target_field.name()) {
                            Some(value) => make_default_array(&target_field.data_type().clone(), value, num_rows),
                            _ => new_null_array(&target_field.data_type().clone(), num_rows),
                        };
                        transform_arrays.push(default_value_array);
                        Some(target_field.clone())
                    }
                    _ => None,
                },
            )
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new_with_options(
        transform_schema,
        transform_arrays,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .unwrap()
}

pub fn transform_array(
    name: String,
    target_datatype: DataType,
    array: ArrayRef,
    num_rows: usize,
    use_default: bool,
    default_column_value: Arc<HashMap<String, String>>,
) -> ArrayRef {
    match target_datatype {
        DataType::Timestamp(target_unit, Some(target_tz)) => make_array(match &target_unit {
            TimeUnit::Second => as_primitive_array::<TimestampSecondType>(&array)
                .with_timezone_opt(Some(target_tz))
                .into_data(),
            TimeUnit::Microsecond => as_primitive_array::<TimestampMicrosecondType>(&array)
                .with_timezone_opt(Some(target_tz))
                .into_data(),
            TimeUnit::Millisecond => as_primitive_array::<TimestampMillisecondType>(&array)
                .with_timezone_opt(Some(target_tz))
                .into_data(),
            TimeUnit::Nanosecond => as_primitive_array::<TimestampNanosecondType>(&array)
                .with_timezone_opt(Some(target_tz))
                .into_data(),
        }),
        DataType::Struct(target_child_fileds) => {
            let orig_array = as_struct_array(&array);
            let child_array = target_child_fileds
                .iter()
                .filter_map(|field| match orig_array.column_by_name(field.name()) {
                    Some(array) => Some((
                        field.clone(),
                        transform_array(
                            name.as_str().to_owned() + "." + field.name(),
                            field.data_type().clone(),
                            array.clone(),
                            num_rows,
                            use_default,
                            default_column_value.clone(),
                        ),
                    )),
                    None if use_default => {
                        let default_value_array = match default_column_value.get(field.name()) {
                            Some(value) => make_default_array(&field.data_type().clone(), value, num_rows),
                            _ => new_null_array(&field.data_type().clone(), num_rows),
                        };
                        Some((field.clone(), default_value_array))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            match orig_array.data().null_buffer() {
                Some(buffer) => Arc::new(StructArray::from((child_array, buffer.clone()))),
                None => Arc::new(StructArray::from(child_array)),
            }
        }
        target_datatype => {
            if target_datatype != *array.data_type() {
                panic!(
                    "Parquet column cannot be converted in file. Column: [{}], Expected: {}, Found: {}",
                    name,
                    target_datatype,
                    array.data_type()
                )
            }
            array.clone()
        }
    }
}

pub fn make_default_array(datatype: &DataType, value: &String, num_rows: usize) -> ArrayRef {
    if value == "null" || value == "NULL" {
        return new_null_array(datatype, num_rows);
    }
    match datatype {
        DataType::Utf8 => Arc::new(StringArray::from(vec![value.as_str(); num_rows])),
        DataType::Int32 => Arc::new(PrimitiveArray::<Int32Type>::from(vec![
            value
                .as_str()
                .parse::<i32>()
                .unwrap();
            num_rows
        ])),
        DataType::Int64 => Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            value
                .as_str()
                .parse::<i64>()
                .unwrap();
            num_rows
        ])),
        DataType::Date32 => Arc::new(PrimitiveArray::<Date32Type>::from(vec![
            value
                .as_str()
                .parse::<i32>()
                .unwrap();
            num_rows
        ])),
        DataType::Boolean => Arc::new(BooleanArray::from(vec![
            value
                .as_str()
                .parse::<bool>()
                .unwrap();
            num_rows
        ])),
        _ => {
            println!(
                "make_default_array() datatype not match, datatype={:?}, value={:?}",
                datatype, value
            );
            new_null_array(datatype, num_rows)
        }
    }
}
