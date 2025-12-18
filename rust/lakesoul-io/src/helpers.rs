//
// SPDX-License-Identifier: Apache-2.0

//! Helper Module for LakeSoul IO
//!
//! This module provides helper functions for LakeSoul IO operations.
//! It includes functions for formatting scalar values, converting partition descriptions,
//! and applying partition filters.

use arrow::datatypes::UInt32Type;
use arrow_array::{Array, RecordBatch, UInt32Array};
use arrow_buffer::i256;
use arrow_schema::{
    ArrowError, DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit,
};
use chrono::{DateTime, Duration};
use datafusion::physical_expr::create_physical_sort_expr;
use datafusion::physical_plan::memory::LazyBatchGenerator;
use datafusion::{
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
    },
    execution::context::{SessionContext, SessionState},
    logical_expr::col,
    physical_expr::{PhysicalSortExpr, create_physical_expr},
    physical_plan::PhysicalExpr,
};
use datafusion_common::DataFusionError::{External, Internal};
use datafusion_common::{
    DFSchema, DataFusionError, Result, ScalarValue, cast::as_primitive_array,
};
use datafusion_substrait::substrait::proto::Plan;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectMeta;
use object_store::path::Path;
use parquet::file::metadata::ParquetMetaData;
use proto::proto::entity::JniWrapper;
use rand::distr::SampleString;
use std::collections::VecDeque;
use std::fmt::{Debug, Display, Formatter};
use std::iter::zip;
use std::{collections::HashMap, fmt, sync::Arc};
use tokio::runtime::Builder;
use url::Url;

use crate::{
    constant::{
        DATE32_FORMAT, FLINK_TIMESTAMP_FORMAT, LAKESOUL_COMMA, LAKESOUL_EMPTY_STRING,
        LAKESOUL_EQ, LAKESOUL_NULL_STRING, TIMESTAMP_MICROSECOND_FORMAT,
        TIMESTAMP_MILLSECOND_FORMAT, TIMESTAMP_NANOSECOND_FORMAT,
        TIMESTAMP_SECOND_FORMAT,
    },
    filter::parser::Parser,
    lakesoul_io_config::LakeSoulIOConfig,
    transform::uniform_schema,
};

/// Converts column names to [`datafusion::physical_expr::PhysicalSortExpr`].
///
/// # Arguments
///
/// * `columns` - A slice of column names to convert
/// * `input_dfschema` - The input DataFusion schema
/// * `session_state` - The session state
///
/// # Returns
///
/// Returns a vector of [`datafusion::physical_expr::PhysicalSortExpr`]
pub fn column_names_to_physical_sort_expr(
    columns: &[String],
    input_dfschema: &DFSchema,
    session_state: &SessionState,
) -> Result<Vec<PhysicalSortExpr>> {
    columns
        .iter()
        .map(|column| {
            create_physical_sort_expr(
                &col(column).sort(true, true),
                input_dfschema,
                session_state.execution_props(),
            )
        })
        .collect::<Result<Vec<_>>>()
}

/// Converts column names to [`datafusion::physical_expr::PhysicalExpr`].
///
/// # Arguments
///
/// * `columns` - A slice of column names to convert
/// * `input_dfschema` - The input DataFusion schema
/// * `session_state` - The session state
///
/// # Returns
///
/// Returns a vector of [`datafusion::physical_expr::PhysicalExpr`]
pub fn column_names_to_physical_expr(
    columns: &[String],
    input_dfschema: &DFSchema,
    session_state: &SessionState,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let runtime_expr = columns
        .iter()
        .map(|column| {
            create_physical_expr(
                &col(column),
                input_dfschema,
                session_state.execution_props(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(runtime_expr)
}

/// Converts range partitions to partition columns of (Column Name, [`arrow::datatypes::DataType`]).
///
/// # Arguments
///
/// * `schema` - The schema of the table
/// * `range_partitions` - The range partitions
///
/// # Returns
///
/// Returns a vector of partition columns of (Column Name, [`arrow::datatypes::DataType`])
pub fn range_partition_to_partition_cols(
    schema: SchemaRef,
    range_partitions: &[String],
) -> Result<Vec<(String, DataType)>> {
    range_partitions
        .iter()
        .map(|col| {
            Ok((
                col.clone(),
                schema.field_with_name(col)?.data_type().clone(),
            ))
        })
        .collect::<Result<Vec<_>>>()
}

/// Gets the [`datafusion::scalar::ScalarValue`] of the partition columns from a record batch.
///
/// # Arguments
///
/// * `batch` - The record batch
/// * `range_partitions` - The range partitions
///
/// # Returns
///
/// Returns a vector of (Column Name, [`datafusion::scalar::ScalarValue`])
pub fn get_columnar_values(
    batch: &RecordBatch,
    range_partitions: Arc<Vec<String>>,
) -> Result<Vec<(String, ScalarValue)>> {
    range_partitions
        .iter()
        .map(|range_col| {
            if let Some(array) = batch.column_by_name(range_col) {
                match ScalarValue::try_from_array(array, 0) {
                    Ok(scalar) => Ok((range_col.clone(), scalar)),
                    Err(e) => Err(e),
                }
            } else {
                Err(External(
                    format!("Invalid partition desc of {}", range_col).into(),
                ))
            }
        })
        .collect::<Result<Vec<_>>>()
}

/// Formats a [`datafusion::scalar::ScalarValue`] to a string.
///
/// # Arguments
///
/// * `v` - The [`datafusion::scalar::ScalarValue`] to format
///
/// # Returns
///
/// Returns a string representation of the [`datafusion::scalar::ScalarValue`]
pub fn format_scalar_value(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Date32(Some(days)) => format!(
            "{}",
            chrono::NaiveDate::from_num_days_from_ce_opt(*days + 719163)
                .unwrap()
                .format(DATE32_FORMAT)
        ),
        ScalarValue::Null => LAKESOUL_NULL_STRING.to_string(),
        ScalarValue::Utf8(Some(s)) => {
            if s.is_empty() {
                LAKESOUL_EMPTY_STRING.to_string()
            } else {
                s.replace(',', LAKESOUL_EQ).replace(',', LAKESOUL_COMMA)
            }
        }
        ScalarValue::TimestampSecond(Some(s), _) => {
            let secs = *s;
            let nsecs = 0;
            format!(
                "{}",
                DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .format(TIMESTAMP_SECOND_FORMAT)
            )
        }
        ScalarValue::TimestampMillisecond(Some(s), _) => {
            let secs = *s / 1000;
            let nsecs = u32::try_from(*s % 1000).unwrap() * 1000000;
            format!(
                "{}",
                DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .format(TIMESTAMP_MILLSECOND_FORMAT)
            )
        }
        ScalarValue::TimestampMicrosecond(Some(s), _) => {
            let secs = *s / 1000000;
            let nsecs = u32::try_from(*s % 1000000).unwrap() * 1000;
            format!(
                "{}",
                DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .format(TIMESTAMP_MICROSECOND_FORMAT)
            )
        }
        ScalarValue::TimestampNanosecond(Some(s), _) => {
            let secs = *s / 1000000000;
            let nsecs = u32::try_from(*s % 1000000000).unwrap();
            format!(
                "{}",
                DateTime::from_timestamp(secs, nsecs)
                    .unwrap()
                    .format(TIMESTAMP_NANOSECOND_FORMAT)
            )
        }
        ScalarValue::Decimal128(Some(s), _, _) => format!("{}", s),
        ScalarValue::Decimal256(Some(s), _, _) => format!("{}", s),
        ScalarValue::Binary(e)
        | ScalarValue::FixedSizeBinary(_, e)
        | ScalarValue::LargeBinary(e) => match e {
            Some(bytes) => hex::encode(bytes),
            None => LAKESOUL_NULL_STRING.to_string(),
        },
        other => other.to_string(),
    }
}

/// Converts a string to a [`datafusion::scalar::ScalarValue`].
///
/// # Arguments
///
/// * `val` - The string to convert
/// * `data_type` - The data type of the [`arrow::datatypes::DataType`]
///
/// # Returns
///
/// Returns a [`datafusion::scalar::ScalarValue`] of the given string
pub fn into_scalar_value(val: &str, data_type: &DataType) -> Result<ScalarValue> {
    if val.eq(LAKESOUL_NULL_STRING) {
        match data_type {
            DataType::Date32 => Ok(ScalarValue::Date32(None)),
            DataType::Utf8 => Ok(ScalarValue::Utf8(None)),
            DataType::Timestamp(unit, timezone) => match unit {
                TimeUnit::Second => {
                    Ok(ScalarValue::TimestampSecond(None, timezone.clone()))
                }
                TimeUnit::Millisecond => {
                    Ok(ScalarValue::TimestampMillisecond(None, timezone.clone()))
                }
                TimeUnit::Microsecond => {
                    Ok(ScalarValue::TimestampMicrosecond(None, timezone.clone()))
                }
                TimeUnit::Nanosecond => {
                    Ok(ScalarValue::TimestampNanosecond(None, timezone.clone()))
                }
            },
            DataType::Decimal128(p, s) => Ok(ScalarValue::Decimal128(None, *p, *s)),
            DataType::Decimal256(p, s) => Ok(ScalarValue::Decimal256(None, *p, *s)),
            DataType::Binary => Ok(ScalarValue::Binary(None)),
            DataType::FixedSizeBinary(size) => {
                Ok(ScalarValue::FixedSizeBinary(*size, None))
            }
            DataType::LargeBinary => Ok(ScalarValue::LargeBinary(None)),
            _ => Ok(ScalarValue::Null),
        }
    } else {
        match data_type {
            DataType::Date32 => {
                Ok(ScalarValue::Date32(Some(date_str_to_epoch_days(val)?)))
            }
            DataType::Utf8 => {
                if val.eq(LAKESOUL_EMPTY_STRING) {
                    Ok(ScalarValue::Utf8(Some("".to_string())))
                } else {
                    Ok(ScalarValue::Utf8(Some(
                        val.replace(LAKESOUL_EQ, "=").replace(LAKESOUL_COMMA, ","),
                    )))
                }
            }
            DataType::Timestamp(unit, timezone) => match unit {
                TimeUnit::Second => {
                    let secs = if let Ok(unix_time) = val.parse::<i64>() {
                        unix_time
                    } else if let Ok(duration) =
                        timestamp_str_to_unix_time(val, FLINK_TIMESTAMP_FORMAT)
                    {
                        duration.num_seconds()
                    } else {
                        timestamp_str_to_unix_time(val, TIMESTAMP_SECOND_FORMAT)?
                            .num_seconds()
                    };
                    Ok(ScalarValue::TimestampSecond(Some(secs), timezone.clone()))
                }
                TimeUnit::Millisecond => {
                    let millsecs = if let Ok(unix_time) = val.parse::<i64>() {
                        unix_time
                    } else if let Ok(duration) =
                        timestamp_str_to_unix_time(val, FLINK_TIMESTAMP_FORMAT)
                    {
                        duration.num_milliseconds()
                    } else {
                        // then try parsing string timestamp to epoch seconds (for flink)
                        timestamp_str_to_unix_time(val, TIMESTAMP_MILLSECOND_FORMAT)?
                            .num_milliseconds()
                    };
                    Ok(ScalarValue::TimestampMillisecond(
                        Some(millsecs),
                        timezone.clone(),
                    ))
                }
                TimeUnit::Microsecond => {
                    let microsecs = if let Ok(unix_time) = val.parse::<i64>() {
                        unix_time
                    } else if let Ok(duration) =
                        timestamp_str_to_unix_time(val, FLINK_TIMESTAMP_FORMAT)
                    {
                        match duration.num_microseconds() {
                            Some(microsecond) => microsecond,
                            None => {
                                return Err(Internal(
                                    "microsecond is out of range".to_string(),
                                ));
                            }
                        }
                    } else {
                        match timestamp_str_to_unix_time(
                            val,
                            TIMESTAMP_MICROSECOND_FORMAT,
                        )?
                        .num_microseconds()
                        {
                            Some(microsecond) => microsecond,
                            None => {
                                return Err(Internal(
                                    "microsecond is out of range".to_string(),
                                ));
                            }
                        }
                    };
                    Ok(ScalarValue::TimestampMicrosecond(
                        Some(microsecs),
                        timezone.clone(),
                    ))
                }
                TimeUnit::Nanosecond => {
                    let nanosecs = if let Ok(unix_time) = val.parse::<i64>() {
                        unix_time
                    } else if let Ok(duration) =
                        timestamp_str_to_unix_time(val, FLINK_TIMESTAMP_FORMAT)
                    {
                        match duration.num_nanoseconds() {
                            Some(nanosecond) => nanosecond,
                            None => {
                                return Err(Internal(
                                    "nanosecond is out of range".to_string(),
                                ));
                            }
                        }
                    } else {
                        match timestamp_str_to_unix_time(
                            val,
                            TIMESTAMP_NANOSECOND_FORMAT,
                        )?
                        .num_nanoseconds()
                        {
                            Some(nanosecond) => nanosecond,
                            None => {
                                return Err(Internal(
                                    "nanoseconds is out of range".to_string(),
                                ));
                            }
                        }
                    };
                    Ok(ScalarValue::TimestampNanosecond(
                        Some(nanosecs),
                        timezone.clone(),
                    ))
                }
            },
            DataType::Decimal128(p, s) => Ok(ScalarValue::Decimal128(
                Some(val.parse::<i128>().unwrap()),
                *p,
                *s,
            )),
            DataType::Decimal256(p, s) => Ok(ScalarValue::Decimal256(
                Some(i256::from_string(val).unwrap()),
                *p,
                *s,
            )),
            DataType::Binary => Ok(ScalarValue::Binary(Some(hex::decode(val).unwrap()))),
            DataType::FixedSizeBinary(size) => Ok(ScalarValue::FixedSizeBinary(
                *size,
                Some(hex::decode(val).unwrap()),
            )),
            DataType::LargeBinary => {
                Ok(ScalarValue::LargeBinary(Some(hex::decode(val).unwrap())))
            }
            _ => ScalarValue::try_from_string(val.to_string(), data_type),
        }
    }
}

/// Converts a vector of (Column Name, [`datafusion::scalar::ScalarValue`]) to a sub path.
///
/// # Arguments
///
/// * `columnar_values` - The vector of (Column Name, [`datafusion::scalar::ScalarValue`])
///
/// # Returns
///
/// Returns a sub path by concatenating the column names and [`datafusion::scalar::ScalarValue`]
pub fn columnar_values_to_sub_path(columnar_values: &[(String, ScalarValue)]) -> String {
    if columnar_values.is_empty() {
        "/".to_string()
    } else {
        format!(
            "/{}/",
            columnar_values
                .iter()
                .map(|(k, v)| format!("{}={}", k, format_scalar_value(v)))
                .collect::<Vec<_>>()
                .join("/")
        )
    }
}

/// Converts a vector of (Column Name, [`datafusion::scalar::ScalarValue`]) to a partition description.
///
/// # Arguments
///
/// * `columnar_values` - The vector of (Column Name, [`datafusion::scalar::ScalarValue`])
///
/// # Returns
///
/// Returns a partition description by concatenating the column names and [`datafusion::scalar::ScalarValue`]
pub fn columnar_values_to_partition_desc(
    columnar_values: &[(String, ScalarValue)],
) -> String {
    if columnar_values.is_empty() {
        "-5".to_string()
    } else {
        columnar_values
            .iter()
            .map(|(k, v)| format!("{}={}", k, format_scalar_value(v)))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Converts a partition description to a vector of scalar values.
///
/// # Arguments
///
/// * `schema` - The schema of the table
/// * `partition_desc` - The partition description
///
/// # Returns
///
/// Returns a vector of [`datafusion::scalar::ScalarValue`] of the given partition description
pub fn partition_desc_to_scalar_values(
    schema: SchemaRef,
    partition_desc: String,
) -> Result<Vec<ScalarValue>> {
    if partition_desc == "-5" {
        Ok(vec![])
    } else {
        let mut part_values = Vec::with_capacity(schema.fields().len());
        for part in partition_desc.split(',') {
            match part.split_once('=') {
                Some((name, val)) => {
                    part_values.push((name, val));
                }
                _ => {
                    return Err(External(
                        format!("Invalid partition_desc: {}", partition_desc).into(),
                    ));
                }
            }
        }
        let mut scalar_values = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            for (name, val) in part_values.iter() {
                if field.name() == name {
                    let scalar = into_scalar_value(val, field.data_type())?;
                    scalar_values.push(scalar);
                    break;
                }
            }
        }
        Ok(scalar_values)
    }
}

/// Extracts a partition description and a map of column names to file paths from a file scan config.
///
/// # Arguments
///
/// * `conf` - The [`datafusion::datasource::physical_plan::FileScanConfig`]
///
/// # Returns
///
/// Returns a tuple of (Partition Description, Map of Column Names to File Paths)
pub fn partition_desc_from_file_scan_config(
    conf: &FileScanConfig,
) -> Result<(String, HashMap<String, String>)> {
    if conf.table_partition_cols().is_empty() {
        Ok(("-5".to_string(), HashMap::default()))
    } else {
        match conf.file_groups.first().and_then(|g| g.files().first()) {
            Some(file) => Ok((
                conf.table_partition_cols()
                    .iter()
                    .enumerate()
                    .map(|(idx, col)| {
                        format!("{}={}", col.name().clone(), file.partition_values[idx])
                    })
                    .collect::<Vec<_>>()
                    .join(","),
                HashMap::from_iter(conf.table_partition_cols().iter().enumerate().map(
                    |(idx, col)| {
                        (col.name().clone(), file.partition_values[idx].to_string())
                    },
                )),
            )),
            None => Err(External(
                format!("Invalid file_group {:?}", conf.file_groups).into(),
            )),
        }
    }
}

/// Creates a [`datafusion::datasource::listing::ListingTable`] from a [`LakeSoulIOConfig`].
///
/// # Arguments
///
/// * `session_state` - The session state
/// * `lakesoul_io_config` - The [`LakeSoulIOConfig`]
/// * `file_format` - The file format
/// * `as_sink` - Whether to create a sink
///
/// # Returns
///
/// Returns a tuple of (Option<[`arrow::datatypes::SchemaRef`]>, Arc<[`datafusion::datasource::listing::ListingTable`]>)
pub async fn listing_table_from_lakesoul_io_config(
    session_state: &SessionState,
    lakesoul_io_config: LakeSoulIOConfig,
    file_format: Arc<dyn FileFormat>,
    as_sink: bool,
) -> Result<(Option<SchemaRef>, Arc<ListingTable>)> {
    let config = match as_sink {
        false => {
            // Parse the path
            let table_paths = lakesoul_io_config
                .files
                .iter()
                .map(ListingTableUrl::parse)
                .collect::<Result<Vec<_>>>()?;
            let object_metas = get_file_object_meta(session_state, &table_paths).await?;
            let (table_paths, object_metas): (Vec<_>, Vec<_>) =
                zip(table_paths, object_metas)
                    .filter(|(_, obj_meta)| {
                        let valid = obj_meta.size >= 8;
                        if !valid {
                            println!(
                                "File {}, size {}, is invalid",
                                obj_meta.location, obj_meta.size
                            );
                        }
                        valid
                    })
                    .unzip();
            // Resolve the schema
            let resolved_schema = infer_schema(
                session_state,
                &table_paths,
                &object_metas,
                Arc::clone(&file_format),
            )
            .await?;

            let target_schema = if lakesoul_io_config.inferring_schema {
                SchemaRef::new(Schema::empty())
            } else {
                uniform_schema(lakesoul_io_config.target_schema())
            };

            let table_partition_cols = range_partition_to_partition_cols(
                target_schema.clone(),
                lakesoul_io_config.range_partitions_slice(),
            )?;
            let listing_options = ListingOptions::new(file_format.clone())
                .with_file_extension(".parquet")
                .with_table_partition_cols(table_partition_cols);

            let mut builder = SchemaBuilder::from(target_schema.fields());
            // O(n^2), n = target_schema.fields().len()
            for field in resolved_schema.fields() {
                if target_schema.field_with_name(field.name()).is_err() {
                    builder.push(field.clone());
                }
            }

            ListingTableConfig::new_with_multi_paths(table_paths)
                .with_listing_options(listing_options)
                // .with_schema(Arc::new(builder.finish()))
                .with_schema(resolved_schema)
        }
        true => {
            let target_schema = uniform_schema(lakesoul_io_config.target_schema());
            let table_partition_cols = range_partition_to_partition_cols(
                target_schema.clone(),
                lakesoul_io_config.range_partitions_slice(),
            )?;

            let listing_options = ListingOptions::new(file_format.clone())
                .with_file_extension(".parquet")
                .with_table_partition_cols(table_partition_cols);
            let prefix = ListingTableUrl::parse(lakesoul_io_config.prefix.clone())?;

            ListingTableConfig::new(prefix)
                .with_listing_options(listing_options)
                .with_schema(target_schema)
        }
    };

    Ok((
        config.file_schema.clone(),
        Arc::new(ListingTable::try_new(config)?),
    ))
}

/// Gets the [`object_store::ObjectMetadata`] for a list of table paths.
///
/// # Arguments
///
/// * `sc` - The session state
/// * `table_paths` - The list of table paths
///
/// # Returns
///
/// Returns a vector of [`object_store::ObjectMetadata`]
pub async fn get_file_object_meta(
    sc: &SessionState,
    table_paths: &[ListingTableUrl],
) -> Result<Vec<ObjectMeta>> {
    let object_store_url = table_paths
        .first()
        .ok_or(Internal("no table path".to_string()))?
        .object_store();
    let store = sc.runtime_env().object_store(object_store_url.clone())?;
    futures::stream::iter(table_paths)
        .map(|path| {
            let store = store.clone();
            async move {
                let path = Path::from_url_path(
                    <ListingTableUrl as AsRef<Url>>::as_ref(path).path(),
                )
                .map_err(object_store::Error::from)?;
                store.head(&path).await
            }
        })
        .boxed()
        .buffered(sc.config_options().execution.meta_fetch_concurrency)
        .try_collect()
        .await
        .map_err(DataFusionError::from)
}

/// Infers the schema of files from a list of [`ListingTableUrl`] and [`ObjectMeta`].
///
/// # Arguments
///
/// * `sc` - The session state
/// * `table_paths` - The list of table paths
/// * `object_metas` - The list of object metadata
/// * `file_format` - The file format
///
/// # Returns
///
/// Returns the inferred schema
pub async fn infer_schema(
    sc: &SessionState,
    table_paths: &[ListingTableUrl],
    object_metas: &[ObjectMeta],
    file_format: Arc<dyn FileFormat>,
) -> Result<SchemaRef> {
    let object_store_url = table_paths
        .first()
        .ok_or(Internal("no table path".to_string()))?
        .object_store();
    let store = sc.runtime_env().object_store(object_store_url.clone())?;

    // Resolve the schema
    file_format.infer_schema(sc, &store, object_metas).await
}

/// Applies a partition filter to a [`JniWrapper`].
///
/// # Arguments
///
/// * `wrapper` - The [`JniWrapper`]
/// * `schema` - The [`arrow::datatypes::SchemaRef`] of the table
/// * `filter` - The [`datafusion_substrait::substrait::proto::Plan`]
///
/// # Returns
///
/// Returns the [`JniWrapper`] of filtered partition info
pub fn apply_partition_filter(
    wrapper: JniWrapper,
    schema: SchemaRef,
    filter: Plan,
) -> Result<JniWrapper> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .build()
        .map_err(|e| External(Box::new(e)))?;
    runtime.block_on(async {
        let context = SessionContext::default();
        let index_filed_name =
            rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 8);
        let index_filed = Field::new(index_filed_name, DataType::UInt32, false);
        let schema_len = schema.fields().len();
        let batch = batch_from_partition(&wrapper, schema, index_filed)?;

        let dataframe = context.read_batch(batch)?;
        let df_filter = Parser::parse_substrait_plan(filter, dataframe.schema())?;

        let results = dataframe.filter(df_filter)?.collect().await?;

        let mut partition_info = vec![];
        for result_batch in results {
            for index in
                as_primitive_array::<UInt32Type>(result_batch.column(schema_len))?
                    .values()
                    .iter()
            {
                partition_info.push(wrapper.partition_info[*index as usize].clone());
            }
        }

        Ok(JniWrapper {
            partition_info,
            ..Default::default()
        })
    })
}

/// Creates a [`RecordBatch`] of partition info from a [`JniWrapper`].
///
/// # Arguments
///
/// * `wrapper` - The [`JniWrapper`]
/// * `schema` - The [`arrow::datatypes::SchemaRef`] of the table
/// * `index_field` - The [`arrow::datatypes::Field`] of the index field
///
/// # Returns
///
/// Returns a [`RecordBatch`] of partition info for partition filter
fn batch_from_partition(
    wrapper: &JniWrapper,
    schema: SchemaRef,
    index_field: Field,
) -> Result<RecordBatch> {
    let scalar_values = wrapper
        .partition_info
        .iter()
        .map(|partition_info| {
            partition_desc_to_scalar_values(
                schema.clone(),
                partition_info.partition_desc.clone(),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let mut columns = vec![vec![]; schema.fields().len()];

    for values in scalar_values.iter() {
        values.iter().enumerate().for_each(|(index, value)| {
            columns[index].push(value.clone());
        })
    }
    let mut columns = columns
        .iter()
        .map(|values| ScalarValue::iter_to_array(values.clone()))
        .collect::<Result<Vec<_>>>()?;

    // Add index column
    let mut fields_with_index = schema
        .flattened_fields()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    fields_with_index.push(index_field);
    let schema_with_index = SchemaRef::new(Schema::new(fields_with_index));
    columns.push(Arc::new(UInt32Array::from(
        (0..wrapper.partition_info.len() as u32).collect::<Vec<_>>(),
    )));

    Ok(RecordBatch::try_new(schema_with_index, columns)?)
}

/// Converts a date string to epoch days.
pub fn date_str_to_epoch_days(value: &str) -> Result<i32> {
    let date = chrono::NaiveDate::parse_from_str(value, DATE32_FORMAT)
        .map_err(|e| External(Box::new(e)))?;
    let datetime = date
        .and_hms_opt(12, 12, 12)
        .ok_or(Internal("invalid h/m/s".to_string()))?;
    let epoch_time = DateTime::from_timestamp_millis(0).ok_or(Internal(
        "the number of milliseconds is out of range for a NaiveDateTim".to_string(),
    ))?;

    Ok(datetime
        .signed_duration_since(epoch_time.naive_utc())
        .num_days() as i32)
}

/// Converts a timestamp string to unix time.
pub fn timestamp_str_to_unix_time(value: &str, fmt: &str) -> Result<Duration> {
    let datetime = chrono::NaiveDateTime::parse_from_str(value, fmt)
        .map_err(|e| External(Box::new(e)))?;
    let epoch_time = DateTime::from_timestamp_millis(0).ok_or(Internal(
        "the number of milliseconds is out of range for a NaiveDateTim".to_string(),
    ))?;

    Ok(datetime.signed_duration_since(epoch_time.naive_utc()))
}

/// Gets the column index and field from a schema.
pub fn column_with_name_and_name2index<'a>(
    schema: &'a SchemaRef,
    name: &str,
    name_to_index: &Option<HashMap<String, usize>>,
) -> Option<(usize, &'a Field)> {
    if let Some(name_to_index) = name_to_index {
        name_to_index
            .get(name)
            .map(|index| (*index, schema.field(*index)))
    } else {
        schema.column_with_name(name)
    }
}

/// Gets the memory size of a [`RecordBatch`].
pub fn get_batch_memory_size(batch: &RecordBatch) -> Result<usize> {
    Ok(batch
        .columns()
        .iter()
        .map(|array| array.to_data().get_slice_memory_size())
        .collect::<std::result::Result<Vec<usize>, ArrowError>>()?
        .into_iter()
        .sum())
}

/// Gets the file exist columns of a [`ParquetMetaData`].
pub fn get_file_exist_col(metadata: &ParquetMetaData) -> String {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|col| col.name().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Extracts the hash bucket ID from a file path.
///
/// File paths are formatted as: "{prefix}/part-{random_string}_{hash_bucket_id:0>4}.parquet"
/// This function extracts and returns the hash bucket ID as a usize.
///
/// # Arguments
///
/// * `file_path` - The file path to extract the hash bucket ID from
///
/// # Returns
///
/// Returns the hash bucket ID as a usize, or None if the path doesn't match the expected format.
///
/// # Examples
///
/// ```
/// use lakesoul_io::helpers::extract_hash_bucket_id;
///
/// let path = "/some/prefix/part-AbCdEfGhIjKlMnOp_0042.parquet";
/// assert_eq!(extract_hash_bucket_id(path), Some(42));
/// ```
pub fn extract_hash_bucket_id(file_path: &str) -> Option<u32> {
    use regex::Regex;

    // Get the file name from the path
    let file_name = file_path.split('/').next_back()?;

    // Regex pattern to extract the hash bucket id between the last underscore and any suffix
    // This pattern matches filenames starting with "part-" and containing an underscore
    // followed by digits before any optional suffix
    let re = Regex::new(r"part-.*_(\d+)(?:\..+)?$").ok()?;

    if let Some(captures) = re.captures(file_name)
        && let Some(id_match) = captures.get(1)
    {
        return id_match.as_str().parse::<u32>().ok();
    }

    None
}

/// Represents a column equality comparison (column_name = scalar_value)
#[derive(Debug)]
pub struct ColumnEquality {
    pub column_name: String,
    pub scalar_value: ScalarValue,
}

/// Checks if an expression is an OR-conjunctive expression.
pub fn is_or_conjunctive(expr: &datafusion::logical_expr::Expr) -> bool {
    match expr {
        datafusion::logical_expr::Expr::BinaryExpr(binary_expr) => {
            binary_expr.op == datafusion::logical_expr::Operator::Or
        }
        _ => false,
    }
}

/// Collects filter expressions that can be optimized.
///
/// This function looks for OR-conjunctive expressions where all components
/// are equality comparisons on primary key columns.
pub fn collect_or_conjunctive_filter_expressions(
    filters: &[datafusion::logical_expr::Expr],
    primary_keys: &[String],
) -> Vec<ColumnEquality> {
    let mut result = Vec::new();

    for filter in filters {
        if is_or_conjunctive(filter) {
            // Collect all equality expressions from this OR-conjunctive expression
            let mut equalities = Vec::new();
            collect_column_equalities(filter, &mut equalities);

            // Check if all collected equalities are on primary key columns
            let all_primary = equalities
                .iter()
                .all(|eq| primary_keys.contains(&eq.column_name));

            if all_primary && !equalities.is_empty() {
                result.extend(equalities);
            }
        }
    }

    result
}

/// Collects column equality comparisons from an expression.
///
/// Recursively traverses OR expressions to find all column = value comparisons.
pub fn collect_column_equalities(
    expr: &datafusion::logical_expr::Expr,
    equalities: &mut Vec<ColumnEquality>,
) {
    use datafusion::logical_expr::{Expr, Operator};

    match expr {
        // If it's an OR expression, process both sides
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Or => {
            collect_column_equalities(&binary_expr.left, equalities);
            collect_column_equalities(&binary_expr.right, equalities);
        }
        // If it's an equality comparison with a literal, extract the column name and scalar value
        Expr::BinaryExpr(binary_expr) if binary_expr.op == Operator::Eq => {
            if let (Expr::Column(col), Expr::Literal(scalar, _)) =
                (&binary_expr.left.as_ref(), &binary_expr.right.as_ref())
            {
                equalities.push(ColumnEquality {
                    column_name: col.name.clone(),
                    scalar_value: scalar.clone(),
                });
            } else if let (Expr::Literal(scalar, _), Expr::Column(col)) =
                (&binary_expr.left.as_ref(), &binary_expr.right.as_ref())
            {
                equalities.push(ColumnEquality {
                    column_name: col.name.clone(),
                    scalar_value: scalar.clone(),
                });
            }
        }
        _ => {}
    }
}

/// Computes the hash value for a ScalarValue.
///
/// # Arguments
///
/// * `scalar` - The scalar value to hash
/// * `seed` - The seed to use for hashing
///
/// # Returns
///
/// Returns the hash value as a u32
pub fn compute_scalar_hash(scalar: &ScalarValue) -> u32 {
    use crate::hash_utils::{HASH_SEED, HashValue};

    match scalar {
        ScalarValue::Int8(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::Int16(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::Int32(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::Int64(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::UInt8(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::UInt16(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::UInt32(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::UInt64(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::Float32(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::Float64(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        ScalarValue::Utf8(Some(v)) => HashValue::hash_one(v.as_bytes(), HASH_SEED),
        ScalarValue::LargeUtf8(Some(v)) => HashValue::hash_one(v.as_bytes(), HASH_SEED),
        ScalarValue::Binary(Some(v)) => HashValue::hash_one(v.as_slice(), HASH_SEED),
        ScalarValue::LargeBinary(Some(v)) => HashValue::hash_one(v.as_slice(), HASH_SEED),
        ScalarValue::Boolean(Some(v)) => HashValue::hash_one(v, HASH_SEED),
        // For other types or None values, use a default hash
        _ => HASH_SEED, // Use seed itself as fallback
    }
}

/// Extracts the scalar value from a ColumnEquality.
///
/// # Arguments
///
/// * `equality` - The ColumnEquality to extract the scalar value from
///
/// # Returns
///
/// Returns Some(scalar_value) if successful, None otherwise
pub fn extract_scalar_value_from_expr(equality: &ColumnEquality) -> Option<&ScalarValue> {
    Some(&equality.scalar_value)
}

#[derive(Debug)]
pub struct InMemGenerator {
    batches: VecDeque<RecordBatch>,
}

impl InMemGenerator {
    pub fn try_new(batches: Vec<RecordBatch>) -> Result<Self> {
        Ok(Self {
            batches: VecDeque::from(batches),
        })
    }
}

impl Display for InMemGenerator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("InMemGenerator: {}", self.batches.len()))
    }
}

impl LazyBatchGenerator for InMemGenerator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.pop_front())
    }
}
