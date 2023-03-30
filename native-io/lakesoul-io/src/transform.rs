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
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow::array::{as_primitive_array, as_struct_array, Array, make_array};
use arrow_array::{types::*, ArrayRef, StructArray, new_null_array, RecordBatchOptions};
use arrow_schema::{ArrowError, SchemaRef, Schema, DataType, Field, TimeUnit};
use arrow::ffi;

use crate::constant::ConstNullArray;


pub fn uniform_schema(orig_schema: SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        orig_schema.fields().iter().enumerate().map(|(idx, field)| {
            let data_type = field.data_type();
            match data_type {
                DataType::Timestamp(unit, Some(tz)) => Field::new(field.name(), DataType::Timestamp(unit.clone(), Some(String::from(crate::constant::LAKESOUL_TIMEZONE))), field.is_nullable()),
                _ => field.clone()
            }
        })    
        .collect::<Vec<_>>()
    ))
}

pub fn uniform_record_batch(batch: RecordBatch) -> RecordBatch {
    transform_record_batch(uniform_schema(batch.schema()), batch, false)
}

pub fn transform_schema(target_schema: SchemaRef, schema: SchemaRef, fill_null_array:bool) -> SchemaRef {
    if fill_null_array {
        target_schema.clone()
    } else {
    Arc::new(
        Schema::new(
    target_schema.fields().iter().enumerate()
            .filter_map(|(_, target_field)| {
                match schema.column_with_name(target_field.name()) {
                    Some(_) => Some(target_field.clone()),
                    None => None
                }
            }).collect::<Vec<_>>()
        ))  
    }
}


pub fn transform_record_batch(target_schema: SchemaRef, batch: RecordBatch, fill_null_array:bool) -> RecordBatch {
    let num_rows = batch.num_rows();
    let orig_schema = batch.schema();
    let mut transform_arrays = Vec::new();
    let transform_schema = Arc::new(Schema::new(
        target_schema.fields().iter().enumerate()
            .filter_map(|(_, target_field)| {
                match orig_schema.column_with_name(target_field.name()) {
                    Some((idx, orig_field)) => {
                        let data_type = target_field.data_type();
                        let transformed_array = transform_array(data_type.clone(), batch.column(idx).clone(), num_rows, fill_null_array);                    
                        transform_arrays.push(transformed_array);
                        Some(target_field.clone())
                    }
                    None if fill_null_array => {
                        let null_array = new_null_array(&target_field.data_type().clone(), num_rows);
                        transform_arrays.push(null_array);
                        Some(target_field.clone())
                    }
                    None => None
                }
            })    
            .collect::<Vec<_>>()
    ));
    RecordBatch::try_new_with_options(transform_schema, transform_arrays, &RecordBatchOptions::new().with_row_count(Some(num_rows))).unwrap()
}

pub fn transform_array(target_datatype: DataType, array: ArrayRef, num_rows:usize, fill_null_array:bool) -> ArrayRef {
    match target_datatype {
        DataType::Timestamp(target_unit, Some(target_tz)) => 
            make_array(
                match &target_unit {
                    TimeUnit::Second =>  as_primitive_array::<TimestampSecondType>(&array).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                    TimeUnit::Microsecond => as_primitive_array::<TimestampMicrosecondType>(&array).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                    TimeUnit::Millisecond => as_primitive_array::<TimestampMillisecondType>(&array).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                    TimeUnit::Nanosecond =>  as_primitive_array::<TimestampNanosecondType>(&array).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                }
            ),
        DataType::Struct(target_child_fileds) => {
            let orig_array = as_struct_array(&array);
            let child_array = 
                target_child_fileds
                    .iter()
                    .filter_map(|field| 
                        match orig_array.column_by_name(field.name()) {
                            Some(array) => Some((field.clone(), transform_array(field.data_type().clone(), array.clone(), num_rows, fill_null_array))),
                            None if fill_null_array => {
                                let null_array = new_null_array(&field.data_type().clone(), num_rows);
                                Some((field.clone(), null_array))
                            }
                            None => None
                        })
                    .collect::<Vec<_>>();
            match orig_array.data().null_buffer() {
                Some(buffer) => {
                    Arc::new(StructArray::from((child_array, buffer.clone())))
                }
                None => Arc::new(StructArray::from(child_array))
            }
            
        }
        _ => array.clone()
    }
}
