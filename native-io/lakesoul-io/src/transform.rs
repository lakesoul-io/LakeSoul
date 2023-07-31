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
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit};
use datafusion::error::Result;
use datafusion_common::DataFusionError::{ArrowError, External};

use crate::constant::{ARROW_CAST_OPTIONS, LAKESOUL_EMPTY_STRING, LAKESOUL_NULL_STRING};

pub fn uniform_schema(orig_schema: SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        orig_schema
            .fields()
            .iter()
            .map(|field| {
                let data_type = field.data_type();
                match data_type {
                    DataType::Timestamp(unit, Some(_)) => Arc::new(Field::new(
                        field.name(),
                        DataType::Timestamp(unit.clone(), Some(Arc::from(crate::constant::LAKESOUL_TIMEZONE))),
                        field.is_nullable(),
                    )),
                    _ => field.clone(),
                }
            })
            .collect::<Vec<_>>(),
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
        DataType::Struct(target_child_fileds) => {
            let orig_array = as_struct_array(&array);
            let mut child_array = vec![];
            target_child_fileds.iter().try_for_each(|field| -> Result<()> {
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
            value
                .as_str()
                // first try parsing epoch days integer (for spark)
                .parse::<i32>()
                .map_err(|e| External(Box::new(e)))
                // then try parsing string date to epoch days (for flink)
                .or(
                    date_str_to_epoch_days(value.as_str())
                )?;
            num_rows
        ])),
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

fn date_str_to_epoch_days(value: &str) -> Result<i32> {
    let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|e| External(Box::new(e)))?;
    let datetime = date.and_hms_opt(12, 12, 12).unwrap();
    let epoch_time = chrono::NaiveDateTime::from_timestamp_millis(0).unwrap();
    Ok(datetime.signed_duration_since(epoch_time).num_days() as i32)
}
