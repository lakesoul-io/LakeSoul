// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{as_primitive_array, as_struct_array, make_array, Array};
use arrow::compute::kernels::cast::cast_with_options;
use arrow::record_batch::RecordBatch;
use arrow_array::{
    new_null_array, types::*, ArrayRef, BooleanArray, PrimitiveArray, RecordBatchOptions, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaBuilder, SchemaRef, TimeUnit};
use datafusion::error::Result;
use datafusion_common::DataFusionError::{ArrowError, External, Internal};

use crate::constant::{
    ARROW_CAST_OPTIONS, FLINK_TIMESTAMP_FORMAT, LAKESOUL_EMPTY_STRING, LAKESOUL_NULL_STRING,
    TIMESTAMP_MICROSECOND_FORMAT, TIMESTAMP_MILLSECOND_FORMAT, TIMESTAMP_NANOSECOND_FORMAT, TIMESTAMP_SECOND_FORMAT,
};
use crate::helpers::{date_str_to_epoch_days, timestamp_str_to_unix_time};

/// adjust time zone to UTC
pub fn uniform_field(orig_field: &FieldRef) -> FieldRef {
    let data_type = orig_field.data_type();
    match data_type {
        DataType::Timestamp(unit, Some(_)) => Arc::new(Field::new(
            orig_field.name(),
            DataType::Timestamp(unit.clone(), Some(Arc::from(crate::constant::LAKESOUL_TIMEZONE))),
            orig_field.is_nullable(),
        )),
        DataType::Struct(fields) => Arc::new(Field::new(
            orig_field.name(),
            DataType::Struct(Fields::from(fields.iter().map(uniform_field).collect::<Vec<_>>())),
            orig_field.is_nullable(),
        )),
        _ => orig_field.clone(),
    }
}

/// adjust time zone to UTC
pub fn uniform_schema(orig_schema: SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        orig_schema.fields().iter().map(uniform_field).collect::<Vec<_>>(),
    ))
}

pub fn uniform_record_batch(batch: RecordBatch) -> Result<RecordBatch> {
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
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let orig_schema = batch.schema();
    let mut transform_arrays = Vec::new();
    let mut fields = vec![];
    target_schema
        .fields()
        .iter()
        .enumerate()
        .try_for_each(|(_, target_field)| -> Result<()> {
            match orig_schema.column_with_name(target_field.name()) {
                Some((idx, _)) => {
                    let data_type = target_field.data_type();
                    let transformed_array = transform_array(
                        target_field.name().to_string(),
                        data_type.clone(),
                        batch.column(idx).clone(),
                        num_rows,
                        use_default,
                        default_column_value.clone(),
                    )?;
                    transform_arrays.push(transformed_array);
                    fields.push(target_field.clone());
                    Ok(())
                }
                None if use_default => {
                    let default_value_array = match default_column_value.get(target_field.name()) {
                        Some(value) => make_default_array(&target_field.data_type().clone(), value, num_rows)?,
                        _ => new_null_array(&target_field.data_type().clone(), num_rows),
                    };
                    transform_arrays.push(default_value_array);
                    fields.push(target_field.clone());
                    Ok(())
                }
                _ => Ok(()),
            }
        })?;
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        transform_arrays,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .map_err(ArrowError)
}

pub fn transform_array(
    name: String,
    target_datatype: DataType,
    array: ArrayRef,
    num_rows: usize,
    use_default: bool,
    default_column_value: Arc<HashMap<String, String>>,
) -> Result<ArrayRef> {
    Ok(match target_datatype {
        DataType::Timestamp(target_unit, Some(target_tz)) => make_array(match &target_unit {
            TimeUnit::Second => as_primitive_array::<TimestampSecondType>(&array)
                .clone()
                .with_timezone_opt(Some(target_tz))
                .into_data(),
            TimeUnit::Microsecond => as_primitive_array::<TimestampMicrosecondType>(&array)
                .clone()
                .with_timezone_opt(Some(target_tz))
                .into_data(),
            TimeUnit::Millisecond => as_primitive_array::<TimestampMillisecondType>(&array)
                .clone()
                .with_timezone_opt(Some(target_tz))
                .into_data(),
            TimeUnit::Nanosecond => as_primitive_array::<TimestampNanosecondType>(&array)
                .clone()
                .with_timezone_opt(Some(target_tz))
                .into_data(),
        }),
        DataType::Struct(target_child_fields) => {
            let orig_array = as_struct_array(&array);
            let mut child_array = vec![];
            target_child_fields.iter().try_for_each(|field| -> Result<()> {
                match orig_array.column_by_name(field.name()) {
                    Some(array) => {
                        child_array.push((
                            field.clone(),
                            transform_array(
                                name.as_str().to_owned() + "." + field.name(),
                                field.data_type().clone(),
                                array.clone(),
                                num_rows,
                                use_default,
                                default_column_value.clone(),
                            )?,
                        ));
                        Ok(())
                    }
                    None if use_default => {
                        let default_value_array = match default_column_value.get(field.name()) {
                            Some(value) => make_default_array(&field.data_type().clone(), value, num_rows)?,
                            _ => new_null_array(&field.data_type().clone(), num_rows),
                        };
                        child_array.push((field.clone(), default_value_array));
                        Ok(())
                    }
                    _ => Ok(()),
                }
            })?;
            let (schema, arrays): (SchemaBuilder, _) = child_array.into_iter().unzip();
            match orig_array.nulls() {
                Some(buffer) => Arc::new(StructArray::new(schema.finish().fields, arrays, Some(buffer.clone()))),
                None => Arc::new(StructArray::new(schema.finish().fields, arrays, None)),
            }
        }
        target_datatype => {
            if target_datatype != *array.data_type() {
                cast_with_options(&array, &target_datatype, &ARROW_CAST_OPTIONS).map_err(ArrowError)?
            } else {
                array.clone()
            }
        }
    })
}

pub fn make_default_array(datatype: &DataType, value: &String, num_rows: usize) -> Result<ArrayRef> {
    if value == LAKESOUL_NULL_STRING {
        return Ok(new_null_array(datatype, num_rows));
    }
    Ok(match datatype {
        DataType::Utf8 => {
            if value == LAKESOUL_EMPTY_STRING {
                Arc::new(StringArray::from(vec![""; num_rows]))
            } else {
                Arc::new(StringArray::from(vec![value.as_str(); num_rows]))
            }
        }
        DataType::Int32 => Arc::new(PrimitiveArray::<Int32Type>::from(vec![
            value
                .as_str()
                .parse::<i32>()
                .map_err(|e| External(
                    Box::new(e)
                ))?;
            num_rows
        ])),
        DataType::Int64 => Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            value
                .as_str()
                .parse::<i64>()
                .map_err(|e| External(
                    Box::new(e)
                ))?;
            num_rows
        ])),
        DataType::Date32 => Arc::new(PrimitiveArray::<Date32Type>::from(vec![
            // first try parsing epoch day int32 (for spark)
            if let Ok(epoch_days) = value.as_str().parse::<i32>() {
                epoch_days
            } else {
            // then try parsing string timestamp to epoch seconds (for flink)
                date_str_to_epoch_days(value.as_str())?
            };
            num_rows
        ])),
        DataType::Timestamp(unit, timezone) => {
            match unit {
                TimeUnit::Second => Arc::new(
                    PrimitiveArray::<TimestampSecondType>::from(vec![
                        if let Ok(unix_time) = value.as_str().parse::<i64>() {
                            unix_time
                        } else if let Ok(duration) = timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT) {
                            duration.num_seconds()
                        } else {
                            timestamp_str_to_unix_time(value, TIMESTAMP_SECOND_FORMAT)?.num_seconds()
                        };
                        num_rows
                    ])
                    .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Millisecond => Arc::new(
                    PrimitiveArray::<TimestampMillisecondType>::from(vec![
                        if let Ok(unix_time) = value.as_str().parse::<i64>() {
                            unix_time
                        } else if let Ok(duration) = timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT) {
                            duration.num_milliseconds()
                        } else {
                            // then try parsing string timestamp to epoch seconds (for flink)
                            timestamp_str_to_unix_time(value, TIMESTAMP_MILLSECOND_FORMAT)?.num_milliseconds()
                        };
                        num_rows
                    ])
                    .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Microsecond => Arc::new(
                    PrimitiveArray::<TimestampMicrosecondType>::from(vec![
                        if let Ok(unix_time) = value.as_str().parse::<i64>() {
                            unix_time
                        } else if let Ok(duration) = timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT) {
                            match duration.num_microseconds() {
                                Some(microsecond) => microsecond,
                                None => return Err(Internal("microsecond is out of range".to_string())),
                            }
                        } else {
                            match timestamp_str_to_unix_time(value, TIMESTAMP_MICROSECOND_FORMAT)?.num_microseconds() {
                                Some(microsecond) => microsecond,
                                None => return Err(Internal("microsecond is out of range".to_string())),
                            }
                        };
                        num_rows
                    ])
                    .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Nanosecond => Arc::new(
                    PrimitiveArray::<TimestampNanosecondType>::from(vec![
                        if let Ok(unix_time) = value.as_str().parse::<i64>() {
                            unix_time
                        } else if let Ok(duration) = timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT) {
                            match duration.num_nanoseconds() {
                                Some(nanosecond) => nanosecond,
                                None => return Err(Internal("nanosecond is out of range".to_string())),
                            }
                        } else {
                            match timestamp_str_to_unix_time(value, TIMESTAMP_NANOSECOND_FORMAT)?.num_nanoseconds() {
                                Some(nanosecond) => nanosecond,
                                None => return Err(Internal("nanoseconds is out of range".to_string())),
                            }
                        };
                        num_rows
                    ])
                    .with_timezone_opt(timezone.clone()),
                ),
            }
        }
        DataType::Boolean => Arc::new(BooleanArray::from(vec![
            value
                .as_str()
                .parse::<bool>()
                .map_err(|e| External(Box::new(e)))?;
            num_rows
        ])),
        _ => {
            println!(
                "make_default_array() datatype not match, datatype={:?}, value={:?}",
                datatype, value
            );
            new_null_array(datatype, num_rows)
        }
    })
}
