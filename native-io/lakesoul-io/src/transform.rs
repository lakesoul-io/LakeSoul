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
use arrow_array::{types::*, ArrayRef, StructArray};
use arrow_schema::{ArrowError, SchemaRef, Schema, DataType, Field, TimeUnit};
use arrow::ffi;


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
            match target_schema.field_with_name(field.name()) {
                Ok(target_field) => {
                    let data_type = target_field.data_type();
                    let transformed_array = transform_array(data_type.clone(), batch.column(idx).clone());                    
                    transform_arrays.push(transformed_array);
                    target_field.clone()
                }
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

pub fn transform_array(target_datatype: DataType, array: ArrayRef) -> ArrayRef {
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
            let child_array = target_child_fileds.iter().map(|field| (field.clone(), orig_array.column_by_name(field.name()).unwrap().clone())).collect::<Vec<_>>();
            match orig_array.data().null_buffer() {
                Some(buffer) => {
                    println!("buffer.capacity() {}", buffer.capacity());
                    Arc::new(StructArray::from((child_array, buffer.clone())))
                }
                None => Arc::new(StructArray::from(child_array))
            }
            
        }
        _ => array.clone()
    }
}

// ArrayData { 
//     data_type: Struct([
//         Field { name: "x", data_type: Struct([
//             Field { name: "a", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, 
//             Field { name: "b", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), 
//             nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} 
//         }, 
//         Field { name: "y", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} 
//     }]), 
//     len: 1, 
//     null_count: 0, 
//     offset: 0, 
//     buffers: [], 
//     child_data: [
//         ArrayData { 
//             data_type: Struct([Field { name: "a", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: "b", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }]), 
//             len: 1, 
//             null_count: 1, 
//             offset: 0, 
//             buffers: [], 
//             child_data: [
//                 ArrayData { 
//                     data_type: Int64, 
//                     len: 1, 
//                     null_count: 1, 
//                     offset: 0, 
//                     buffers: [Buffer { data: Bytes { ptr: 0x7ff4b883d780, len: 8, data: [0, 0, 0, 0, 0, 0, 0, 0] }, offset: 0, length: 8 }], 
//                     child_data: [], 
//                     null_bitmap: Some(Bitmap { bits: Buffer { data: Bytes { ptr: 0x7ff4b883d900, len: 1, data: [0] }, offset: 0, length: 1 } }) }, 
//                 ArrayData { 
//                     data_type: Utf8, 
//                     len: 1, 
//                     null_count: 1, 
//                     offset: 0, 
//                     buffers: [
//                         Buffer { data: Bytes { ptr: 0x7ff4b883c680, len: 8, data: [0, 0, 0, 0, 0, 0, 0, 0] }, offset: 0, length: 8 }, 
//                         Buffer { data: Bytes { ptr: 0x80, len: 0, data: [] }, offset: 0, length: 0 }
//                     ], 
//                     child_data: [], 
//                     null_bitmap: Some(Bitmap { bits: Buffer { data: Bytes { ptr: 0x7ff4b883da00, len: 1, data: [0] }, offset: 0, length: 1 } }) 
//                 }], 
//             null_bitmap: Some(Bitmap { bits: Buffer { data: Bytes { ptr: 0x7ff4b883e300, len: 1, data: [0] }, offset: 0, length: 1 } }) 
//         }, 
//         ArrayData { 
//             data_type: Int64, len: 1, null_count: 0, offset: 0, 
//             buffers: [Buffer { data: Bytes { ptr: 0x7ff4b883db80, len: 8, data: [3, 0, 0, 0, 0, 0, 0, 0] }, offset: 0, length: 8 }], 
//             child_data: [], 
//             null_bitmap: None 
//         }], 
//     null_bitmap: None 
// }
