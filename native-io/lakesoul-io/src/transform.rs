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
use arrow::array::{as_primitive_array, Array, make_array};
use arrow_array::types::*;
use arrow_schema::{ArrowError, SchemaRef, Schema, DataType, Field, TimeUnit};


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
    transform_record_batch(uniform_schema(batch.schema()), batch)
}

pub fn transform_record_batch(target_schema: SchemaRef, batch: RecordBatch) -> RecordBatch {
    let orig_schema = batch.schema();
    let mut transform_arrays = Vec::new();
    let transform_schema = Arc::new(Schema::new(
        orig_schema.fields().iter().enumerate().map(|(idx, field)| {
            let data_type = field.data_type();
            match data_type {
                DataType::Timestamp(_, Some(_)) => {
                    if let DataType::Timestamp(target_unit, Some(target_tz)) = target_schema.field_with_name(field.name()).unwrap().data_type() {
                        let orig_array = batch.column(idx).clone();

                        let transform_array = make_array(
                            match &target_unit {
                                TimeUnit::Second =>  as_primitive_array::<TimestampSecondType>(orig_array.as_ref()).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                                TimeUnit::Microsecond => as_primitive_array::<TimestampMicrosecondType>(orig_array.as_ref()).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                                TimeUnit::Millisecond => as_primitive_array::<TimestampMillisecondType>(orig_array.as_ref()).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                                TimeUnit::Nanosecond =>  as_primitive_array::<TimestampNanosecondType>(orig_array.as_ref()).with_timezone_opt(Some(target_tz.to_string())).into_data(),
                            }
                        );
                        transform_arrays.push(transform_array);
                        Field::new(field.name(), DataType::Timestamp(target_unit.clone(), Some(target_tz.to_string())), field.is_nullable())
                    } else {
                        transform_arrays.push(batch.column(idx).clone());
                        field.clone()
                    }
                },
                _ => {
                    transform_arrays.push(batch.column(idx).clone());
                    field.clone()
                }
            }
        })    
        .collect::<Vec<_>>()
    ));
    
    
    RecordBatch::try_new(transform_schema, transform_arrays).unwrap()
}

