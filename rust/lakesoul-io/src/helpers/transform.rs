// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Transform Module for uniformizing schema and record batches

use std::collections::HashMap;
use std::sync::Arc;

use crate::Result;
use crate::constant::{
    ARROW_CAST_OPTIONS, FLINK_TIMESTAMP_FORMAT, LAKESOUL_EMPTY_STRING,
    LAKESOUL_NULL_STRING, TIMESTAMP_MICROSECOND_FORMAT, TIMESTAMP_MILLSECOND_FORMAT,
    TIMESTAMP_NANOSECOND_FORMAT, TIMESTAMP_SECOND_FORMAT,
};
use crate::helpers::{
    column_with_name_and_name2index, date_str_to_epoch_days, into_scalar_value,
    timestamp_str_to_unix_time,
};
use arrow::array::{Array, as_primitive_array, as_struct_array, make_array};
use arrow::compute::kernels::cast::cast_with_options;
use arrow::record_batch::RecordBatch;
use arrow_array::cast::as_string_array;
use arrow_array::{
    ArrayRef, BooleanArray, PrimitiveArray, RecordBatchOptions, StringArray, StructArray,
    new_null_array, types::*,
};
use arrow_schema::{
    DataType, Field, FieldRef, Fields, Schema, SchemaBuilder, SchemaRef, TimeUnit,
    UnionFields,
};
use datafusion_common::DataFusionError;
use rootcause::{bail, report};

/// adjust time zone to UTC
pub fn uniform_field(orig_field: &FieldRef) -> FieldRef {
    let data_type = orig_field.data_type();
    match data_type {
        DataType::Timestamp(unit, Some(_)) => Arc::new(Field::new(
            orig_field.name(),
            DataType::Timestamp(
                *unit,
                Some(Arc::from(crate::constant::LAKESOUL_TIMEZONE)),
            ),
            orig_field.is_nullable(),
        )),
        DataType::Struct(fields) => Arc::new(Field::new(
            orig_field.name(),
            DataType::Struct(Fields::from(
                fields.iter().map(uniform_field).collect::<Vec<_>>(),
            )),
            orig_field.is_nullable(),
        )),
        _ => orig_field.clone(),
    }
}

/// adjust time zone to UTC
pub fn uniform_schema(orig_schema: SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        orig_schema
            .fields()
            .iter()
            .map(uniform_field)
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

pub fn normalize_data_type_for_java(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::ListView(field) => DataType::List(normalize_field_for_java(field)),
        DataType::LargeListView(field) => {
            DataType::LargeList(normalize_field_for_java(field))
        }
        DataType::List(field) => DataType::List(normalize_field_for_java(field)),
        DataType::LargeList(field) => {
            DataType::LargeList(normalize_field_for_java(field))
        }
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(normalize_field_for_java(field), *size)
        }
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(normalize_field_for_java)
                .collect::<Vec<_>>(),
        )),
        DataType::Map(field, sorted) => {
            DataType::Map(normalize_field_for_java(field), *sorted)
        }
        DataType::Dictionary(key, value) => DataType::Dictionary(
            Box::new(normalize_data_type_for_java(key)),
            Box::new(normalize_data_type_for_java(value)),
        ),
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(type_id, field)| (type_id, normalize_field_for_java(field)))
                .collect::<UnionFields>(),
            *mode,
        ),
        DataType::RunEndEncoded(run_ends, values) => DataType::RunEndEncoded(
            normalize_field_for_java(run_ends),
            normalize_field_for_java(values),
        ),
        _ => data_type.clone(),
    }
}

pub fn normalize_field_for_java(field: &FieldRef) -> FieldRef {
    Arc::new(
        Field::new(
            field.name(),
            normalize_data_type_for_java(field.data_type()),
            field.is_nullable(),
        )
        .with_metadata(field.metadata().clone()),
    )
}

pub fn normalize_schema_for_java(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(normalize_field_for_java)
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

fn data_type_needs_java_normalization(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8View
        | DataType::BinaryView
        | DataType::ListView(_)
        | DataType::LargeListView(_) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            data_type_needs_java_normalization(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_needs_java_normalization(field.data_type())),
        DataType::Dictionary(key, value) => {
            data_type_needs_java_normalization(key)
                || data_type_needs_java_normalization(value)
        }
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| data_type_needs_java_normalization(field.data_type())),
        DataType::RunEndEncoded(run_ends, values) => {
            data_type_needs_java_normalization(run_ends.data_type())
                || data_type_needs_java_normalization(values.data_type())
        }
        _ => false,
    }
}

pub fn schema_needs_java_normalization(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| data_type_needs_java_normalization(field.data_type()))
}

pub fn normalize_record_batch_for_java(batch: RecordBatch) -> Result<RecordBatch> {
    if !schema_needs_java_normalization(batch.schema().as_ref()) {
        return Ok(batch);
    }

    let num_rows = batch.num_rows();
    let normalized_schema = normalize_schema_for_java(batch.schema().as_ref());
    let columns = batch
        .columns()
        .iter()
        .zip(normalized_schema.fields().iter())
        .map(|(array, field)| {
            transform_array(
                field.name().to_string(),
                field.data_type().clone(),
                array.clone(),
                num_rows,
                false,
                Arc::new(HashMap::new()),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new_with_options(
        normalized_schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

pub fn transform_schema(
    target_schema: SchemaRef,
    schema: SchemaRef,
    use_default: bool,
) -> SchemaRef {
    if use_default {
        target_schema
    } else {
        // O(nm) n = schema.fields().len(), m = target_schema.fields().len()
        Arc::new(Schema::new(
            target_schema
                .fields()
                .iter()
                .filter_map(|target_field| {
                    schema
                        .column_with_name(target_field.name())
                        .map(|_| target_field.clone())
                })
                .collect::<Vec<_>>(),
        ))
    }
}

/// Transforms an input `RecordBatch` to match a target `merged_schema`.
///
/// This handles:
/// 1. Column reordering/alignment with the target schema.
/// 2. Filling missing columns with default values or Nulls.
/// 3. Recursive type casting for nested structures (e.g., Structs).
#[instrument(skip(batch))]
pub fn transform_record_batch(
    merged_schema: SchemaRef,
    batch: RecordBatch,
    use_default: bool, // Flag to enable/disable default value filling
    default_column_value: Arc<HashMap<String, String>>, // Mapping of field names to their string-represented default values
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let batch_schema = batch.schema(); // file schema?

    // Optimization: Use a HashMap for O(1) field lookup if the schema is large (exceeds threshold),
    // preventing the O(N*M) complexity of linear scanning.
    let name_to_index =
        if batch_schema.fields().len() > crate::constant::NUM_COLUMN_OPTIMIZE_THRESHOLD {
            Some(HashMap::<String, usize>::from_iter(
                batch_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| (field.name().clone(), idx)),
            ))
        } else {
            None
        };
    let mut transform_arrays = Vec::new();
    let mut fields = vec![];

    // Iterate through each field in the target schema to align the input batch
    merged_schema.fields().iter().enumerate().try_for_each(
        |(_, target_field)| -> Result<()> {
            match column_with_name_and_name2index(
                &batch_schema,
                target_field.name(),
                &name_to_index,
            ) {
                // Case A: The column exists in the current batch
                Some((idx, _)) => {
                    let data_type = target_field.data_type();
                    // Perform recursive transformation (type casting, nested struct alignment)
                    let transformed_array = transform_array(
                        target_field.name().to_string(),
                        data_type.clone(),
                        batch.column(idx).clone(),
                        num_rows,
                        use_default,
                        default_column_value.clone(),
                    )?;
                    fields.push(Arc::new(Field::new(
                        target_field.name(),
                        transformed_array.data_type().clone(),
                        target_field.is_nullable(),
                    )));
                    transform_arrays.push(transformed_array);
                    Ok(())
                }

                // Case B: The column is missing but default values are enabled
                None if use_default => {
                    let default_value_array = match default_column_value
                        .get(target_field.name())
                    {
                        // Generate a constant array with the provided default value
                        Some(value) => make_default_array(
                            &target_field.data_type().clone(),
                            value,
                            num_rows,
                        )?,
                        // Default to a Null array if no specific value is provided
                        _ => new_null_array(&target_field.data_type().clone(), num_rows),
                    };
                    fields.push(Arc::new(Field::new(
                        target_field.name(),
                        default_value_array.data_type().clone(),
                        target_field.is_nullable(),
                    )));
                    transform_arrays.push(default_value_array);
                    Ok(())
                }
                // Case C: Column is missing and no default is used; it will be excluded from the output
                _ => Ok(()),
            }
        },
    )?;

    // Construct the final aligned RecordBatch
    Ok(RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        transform_arrays,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

/// Transforms a single `ArrayRef` to a target `DataType`.
///
/// Specifically handles:
/// - Timestamp coercion from Int64, Utf8, or different precision Timestamps.
/// - Recursive transformation for nested `StructArray` fields.
pub fn transform_array(
    name: String,
    target_datatype: DataType,
    array: ArrayRef,
    num_rows: usize,
    use_default: bool,
    default_column_value: Arc<HashMap<String, String>>,
) -> Result<ArrayRef> {
    Ok(match target_datatype {
        // 1. Specialized Timestamp Handling: Coerce from various sources (Int, String, other Timestamps)
        DataType::Timestamp(target_unit, Some(target_tz)) => {
            let array = match array.data_type() {
                // Extract underlying data for primitive timestamps
                DataType::Timestamp(TimeUnit::Second, _) => {
                    as_primitive_array::<TimestampSecondType>(&array)
                        .clone()
                        .into_data()
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    as_primitive_array::<TimestampMillisecondType>(&array)
                        .clone()
                        .into_data()
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    as_primitive_array::<TimestampMicrosecondType>(&array)
                        .clone()
                        .into_data()
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    as_primitive_array::<TimestampNanosecondType>(&array)
                        .clone()
                        .into_data()
                }
                DataType::Int64 => {
                    as_primitive_array::<Int64Type>(&array).clone().into_data()
                }
                DataType::Utf8 => as_string_array(&array).clone().into_data(),
                _ => {
                    bail!(
                        "cannot cast to timestamp from unsupported type {:?}",
                        array.data_type()
                    );
                }
            };
            let array_ref = make_array(array);
            let source_datatype = array_ref.data_type();
            let target_datatype =
                DataType::Timestamp(target_unit, Some(target_tz.clone()));

            // Utilize arrow-cast for final unit conversion and timezone application
            (cast_with_options(
                &array_ref,
                &DataType::Timestamp(target_unit, Some(target_tz.clone())),
                &ARROW_CAST_OPTIONS,
            )
            .map_err(|_| {
                report!(
                    "Failed to cast timestamp type from {} to {}",
                    source_datatype,
                    target_datatype
                )
            })?) as _
        }
        // 2. Nested Struct Handling: Recursively transform each child field within the Struct
        DataType::Struct(target_child_fields) => {
            let orig_array = as_struct_array(&array);
            let mut child_array = vec![];
            target_child_fields
                .iter()
                .try_for_each(|field| -> Result<()> {
                    match orig_array.column_by_name(field.name()) {
                        // Child exists: recurse to transform the nested array
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
                        // Child missing: apply default value logic at the nested level
                        None if use_default => {
                            let default_value_array = match default_column_value
                                .get(field.name())
                            {
                                Some(value) => make_default_array(
                                    &field.data_type().clone(),
                                    value,
                                    num_rows,
                                )?,
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
                // Reconstruct the StructArray, preserving the original null bitmap
                Some(buffer) => Arc::new(StructArray::new(
                    schema.finish().fields,
                    arrays,
                    Some(buffer.clone()),
                )),
                None => Arc::new(StructArray::new(schema.finish().fields, arrays, None)),
            }
        }
        // 3. General Transformation Path
        target_datatype => {
            if target_datatype != *array.data_type() {
                cast_with_options(&array, &target_datatype, &ARROW_CAST_OPTIONS).map_err(
                    |e| {
                        DataFusionError::ArrowError(
                            Box::new(e),
                            Some(format!(
                                "Failed to cast type from {} to {}",
                                array.data_type(),
                                target_datatype
                            )),
                        )
                    },
                )?
            } else {
                // Types match exactly, return a shallow clone (increment RefCount)
                array.clone()
            }
        }
    })
}

pub fn make_default_array(
    datatype: &DataType,
    value: &String,
    num_rows: usize,
) -> Result<ArrayRef> {
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
                .parse::<i32>(
                )?;
            num_rows
        ])),
        DataType::Int64 => Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            value
                .as_str()
                .parse::<i64>(
                )?;
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
                        } else if let Ok(duration) =
                            timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT)
                        {
                            duration.num_seconds()
                        } else {
                            timestamp_str_to_unix_time(value, TIMESTAMP_SECOND_FORMAT)?
                                .num_seconds()
                        };
                        num_rows
                    ])
                    .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Millisecond => Arc::new(
                    PrimitiveArray::<TimestampMillisecondType>::from(vec![
                        if let Ok(unix_time) = value.as_str().parse::<i64>() {
                            unix_time
                        } else if let Ok(duration) =
                            timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT)
                        {
                            duration.num_milliseconds()
                        } else {
                            // then try parsing string timestamp to epoch seconds (for flink)
                            timestamp_str_to_unix_time(
                                value,
                                TIMESTAMP_MILLSECOND_FORMAT,
                            )?
                            .num_milliseconds()
                        };
                        num_rows
                    ])
                    .with_timezone_opt(timezone.clone()),
                ),
                TimeUnit::Microsecond => Arc::new(
                    PrimitiveArray::<TimestampMicrosecondType>::from(vec![
                        if let Ok(unix_time) = value.as_str().parse::<i64>() {
                            unix_time
                        } else if let Ok(duration) =
                            timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT)
                        {
                            match duration.num_microseconds() {
                                Some(microsecond) => microsecond,
                                None => {
                                    bail!("microsecond is out of range");
                                }
                            }
                        } else {
                            match timestamp_str_to_unix_time(
                                value,
                                TIMESTAMP_MICROSECOND_FORMAT,
                            )?
                            .num_microseconds()
                            {
                                Some(microsecond) => microsecond,
                                None => {
                                    bail!("microsecond is out of range");
                                }
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
                        } else if let Ok(duration) =
                            timestamp_str_to_unix_time(value, FLINK_TIMESTAMP_FORMAT)
                        {
                            match duration.num_nanoseconds() {
                                Some(nanosecond) => nanosecond,
                                None => {
                                    bail!("nanosecond is out of range");
                                }
                            }
                        } else {
                            match timestamp_str_to_unix_time(
                                value,
                                TIMESTAMP_NANOSECOND_FORMAT,
                            )?
                            .num_nanoseconds()
                            {
                                Some(nanosecond) => nanosecond,
                                None => {
                                    bail!("nanosecond is out of range");
                                }
                            }
                        };
                        num_rows
                    ])
                    .with_timezone_opt(timezone.clone()),
                ),
            }
        }
        DataType::Boolean => {
            Arc::new(BooleanArray::from(vec![
                value.as_str().parse::<bool>()?;
                num_rows
            ]))
        }
        data_type => match into_scalar_value(value, data_type) {
            Ok(scalar) => scalar.to_array_of_size(num_rows)?,
            Err(_) => {
                error!(
                    "make_default_array() datatype not match, datatype={:?}, value={:?}",
                    datatype, value
                );
                new_null_array(datatype, num_rows)
            }
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{ListViewBuilder, StringViewBuilder};
    use arrow_array::{ListArray, StringViewArray};

    #[test]
    fn transform_record_batch_materializes_utf8_view_as_utf8() {
        let target_schema =
            Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8View,
            true,
        )]));
        let input_array: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("aa"),
            Some("bbb"),
            None,
            Some("long string over twelve bytes"),
        ]));
        let input_batch = RecordBatch::try_new(input_schema, vec![input_array]).unwrap();

        let transformed = transform_record_batch(
            target_schema,
            input_batch,
            false,
            Arc::new(HashMap::new()),
        )
        .unwrap();

        assert_eq!(transformed.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(transformed.column(0).data_type(), &DataType::Utf8);
        let values = transformed
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "aa");
        assert_eq!(values.value(1), "bbb");
        assert!(values.is_null(2));
        assert_eq!(values.value(3), "long string over twelve bytes");
    }

    #[test]
    fn normalize_record_batch_for_java_materializes_nested_view_types() {
        let top_text: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("abc"),
            Some("database-system"),
        ]));

        let nested_text: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("nested"),
            Some("long nested string"),
        ]));
        let nested_fields =
            Fields::from(vec![Arc::new(Field::new("name", DataType::Utf8View, true))]);
        let nested_struct: ArrayRef = Arc::new(StructArray::new(
            nested_fields.clone(),
            vec![nested_text],
            None,
        ));

        let item_field = Arc::new(Field::new("item", DataType::Utf8View, true));
        let mut list_builder =
            ListViewBuilder::new(StringViewBuilder::new()).with_field(item_field.clone());
        list_builder.append_value([Some("x"), Some("long list value")]);
        list_builder.append_null();
        let list_view: ArrayRef = Arc::new(list_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("top_text", DataType::Utf8View, true),
            Field::new("nested", DataType::Struct(nested_fields), true),
            Field::new("items", DataType::ListView(item_field), true),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![top_text, nested_struct, list_view])
                .unwrap();

        let normalized = normalize_record_batch_for_java(batch).unwrap();

        assert_eq!(normalized.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(normalized.column(0).data_type(), &DataType::Utf8);
        let top_values = normalized
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(top_values.value_offsets(), &[0, 3, 18]);
        assert_eq!(top_values.value_data().as_ref(), b"abcdatabase-system");

        match normalized.schema().field(1).data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields[0].data_type(), &DataType::Utf8);
            }
            other => panic!("expected struct, got {other:?}"),
        }
        let struct_values = normalized
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(struct_values.column(0).data_type(), &DataType::Utf8);

        match normalized.schema().field(2).data_type() {
            DataType::List(field) => {
                assert_eq!(field.data_type(), &DataType::Utf8);
            }
            other => panic!("expected list, got {other:?}"),
        }
        let list_values = normalized
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(
            list_values.data_type(),
            normalized.schema().field(2).data_type()
        );
        assert!(list_values.is_null(1));
        assert_eq!(list_values.value(0).data_type(), &DataType::Utf8);
    }
}
