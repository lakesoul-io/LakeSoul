// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::UInt32Type;
use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit};
use chrono::{DateTime, Duration};
use datafusion::{
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
    },
    execution::context::{SessionContext, SessionState},
    logical_expr::col,
    physical_expr::{create_physical_expr, PhysicalSortExpr},
    physical_plan::PhysicalExpr,
    physical_planner::create_physical_sort_expr,
};
use datafusion_common::DataFusionError::{External, Internal};
use datafusion_common::{cast::as_primitive_array, DFSchema, DataFusionError, Result, ScalarValue};

use datafusion_substrait::substrait::proto::Plan;
use object_store::path::Path;
use proto::proto::entity::JniWrapper;
use rand::distributions::DistString;
use url::Url;

use crate::{
    constant::{
        DATE32_FORMAT, LAKESOUL_EMPTY_STRING, LAKESOUL_NULL_STRING, TIMESTAMP_MICROSECOND_FORMAT,
        TIMESTAMP_MILLSECOND_FORMAT, TIMESTAMP_NANOSECOND_FORMAT, TIMESTAMP_SECOND_FORMAT,
    },
    filter::parser::Parser,
    lakesoul_io_config::LakeSoulIOConfig,
    transform::uniform_schema,
};

pub fn column_names_to_physical_sort_expr(
    columns: &[String],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<Vec<PhysicalSortExpr>> {
    columns
        .iter()
        .map(|column| {
            create_physical_sort_expr(
                &col(column).sort(true, true),
                input_dfschema,
                input_schema,
                session_state.execution_props(),
            )
        })
        .collect::<Result<Vec<_>>>()
}

pub fn column_names_to_physical_expr(
    columns: &[String],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let runtime_expr = columns
        .iter()
        .map(|column| {
            create_physical_expr(
                &col(column),
                input_dfschema,
                input_schema,
                session_state.execution_props(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(runtime_expr)
}

fn range_partition_to_partition_cols(
    schema: SchemaRef,
    range_partitions: &[String],
) -> Result<Vec<(String, DataType)>> {
    range_partitions
        .iter()
        .map(|col| Ok((col.clone(), schema.field_with_name(col)?.data_type().clone())))
        .collect::<Result<Vec<_>>>()
}

pub fn get_columnar_values(
    batch: &RecordBatch,
    range_partitions: Arc<Vec<String>>,
) -> datafusion::error::Result<Vec<(String, ScalarValue)>> {
    range_partitions
        .iter()
        .map(|range_col| {
            if let Some(array) = batch.column_by_name(range_col) {
                match ScalarValue::try_from_array(array, 0) {
                    Ok(scalar) => Ok((range_col.clone(), scalar)),
                    Err(e) => Err(e),
                }
            } else {
                Err(datafusion::error::DataFusionError::External(
                    format!("Invalid partition desc of {}", range_col).into(),
                ))
            }
        })
        .collect::<datafusion::error::Result<Vec<_>>>()
}

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
                s.clone()
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
        other => other.to_string(),
    }
}

pub fn into_scalar_value(val: &str, data_type: &DataType) -> Result<ScalarValue> {
    if val.eq(LAKESOUL_NULL_STRING) {
        Ok(ScalarValue::Null)
    } else {
        match data_type {
            DataType::Date32 => Ok(ScalarValue::Date32(Some(date_str_to_epoch_days(val)?))),
            DataType::Utf8 => {
                if val.eq(LAKESOUL_EMPTY_STRING) {
                    Ok(ScalarValue::Utf8(Some("".to_string())))
                } else {
                    Ok(ScalarValue::Utf8(Some(val.to_string())))
                }
            }
            DataType::Timestamp(unit, timezone) => match unit {
                TimeUnit::Second => {
                    let secs = timestamp_str_to_unix_time(val, TIMESTAMP_SECOND_FORMAT)?.num_seconds();
                    Ok(ScalarValue::TimestampSecond(Some(secs), timezone.clone()))
                }
                TimeUnit::Millisecond => {
                    let millsecs = timestamp_str_to_unix_time(val, TIMESTAMP_MILLSECOND_FORMAT)?.num_milliseconds();
                    Ok(ScalarValue::TimestampMillisecond(Some(millsecs), timezone.clone()))
                }
                TimeUnit::Microsecond => {
                    let microsecs = timestamp_str_to_unix_time(val, TIMESTAMP_MICROSECOND_FORMAT)?.num_microseconds();
                    Ok(ScalarValue::TimestampMicrosecond(microsecs, timezone.clone()))
                }
                TimeUnit::Nanosecond => {
                    let nanosecs = timestamp_str_to_unix_time(val, TIMESTAMP_NANOSECOND_FORMAT)?.num_nanoseconds();
                    Ok(ScalarValue::TimestampNanosecond(nanosecs, timezone.clone()))
                }
            },
            _ => ScalarValue::try_from_string(val.to_string(), data_type),
        }
    }
}

pub fn columnar_values_to_sub_path(columnar_values: &Vec<(String, ScalarValue)>) -> String {
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

pub fn columnar_values_to_partition_desc(columnar_values: &Vec<(String, ScalarValue)>) -> String {
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

pub fn partition_desc_to_scalar_values(schema: SchemaRef, partition_desc: String) -> Result<Vec<ScalarValue>> {
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
                    return Err(datafusion::error::DataFusionError::External(
                        format!("Invalid partition_desc: {}", partition_desc).into(),
                    ))
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

pub fn partition_desc_from_file_scan_config(conf: &FileScanConfig) -> Result<(String, HashMap<String, String>)> {
    if conf.table_partition_cols.is_empty() {
        Ok(("-5".to_string(), HashMap::default()))
    } else {
        match conf.file_groups.first().unwrap().first() {
            Some(file) => Ok((
                conf.table_partition_cols
                    .iter()
                    .enumerate()
                    .map(|(idx, col)| format!("{}={}", col.name().clone(), file.partition_values[idx]))
                    .collect::<Vec<_>>()
                    .join(","),
                HashMap::from_iter(
                    conf.table_partition_cols
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| (col.name().clone(), file.partition_values[idx].to_string())),
                ),
            )),
            None => Err(DataFusionError::External(
                format!("Invalid file_group {:?}", conf.file_groups).into(),
            )),
        }
    }
}

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
            // Resolve the schema
            let resolved_schema = infer_schema(session_state, &table_paths, Arc::clone(&file_format)).await?;

            let target_schema = uniform_schema(lakesoul_io_config.target_schema());

            let table_partition_cols =
                range_partition_to_partition_cols(target_schema.clone(), lakesoul_io_config.range_partitions_slice())?;
            let listing_options = ListingOptions::new(file_format.clone())
                .with_file_extension(".parquet")
                .with_table_partition_cols(table_partition_cols);

            let mut builder = SchemaBuilder::from(target_schema.fields());
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
            let table_partition_cols =
                range_partition_to_partition_cols(target_schema.clone(), lakesoul_io_config.range_partitions_slice())?;

            let listing_options = ListingOptions::new(file_format.clone())
                .with_file_extension(".parquet")
                .with_table_partition_cols(table_partition_cols)
                .with_insert_mode(datafusion::datasource::listing::ListingTableInsertMode::AppendNewFiles);
            let prefix = ListingTableUrl::parse_create_local_if_not_exists(lakesoul_io_config.prefix.clone(), true)?;

            ListingTableConfig::new(prefix)
                .with_listing_options(listing_options)
                .with_schema(target_schema)
        }
    };

    Ok((config.file_schema.clone(), Arc::new(ListingTable::try_new(config)?)))
}

pub async fn infer_schema(
    sc: &SessionState,
    table_paths: &[ListingTableUrl],
    file_format: Arc<dyn FileFormat>,
) -> Result<SchemaRef> {
    // Create default parquet options
    let object_store_url = table_paths
        .first()
        .ok_or(DataFusionError::Internal("no table path".to_string()))?
        .object_store();
    let store = sc.runtime_env().object_store(object_store_url.clone())?;
    let mut objects = vec![];

    for url in table_paths {
        objects.push(
            store
                .head(&Path::from_url_path(
                    <ListingTableUrl as AsRef<Url>>::as_ref(url).path(),
                )?)
                .await?,
        );
    }

    // Resolve the schema
    file_format.infer_schema(sc, &store, &objects).await
}

pub fn apply_partition_filter(wrapper: JniWrapper, schema: SchemaRef, filter: Plan) -> Result<JniWrapper> {
    tokio::runtime::Runtime::new()?.block_on(async {
        let context = SessionContext::default();
        let index_filed_name = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 8);
        let index_filed = Field::new(index_filed_name, DataType::UInt32, false);
        let schema_len = schema.fields().len();
        let batch = batch_from_partition(&wrapper, schema, index_filed)?;

        let dataframe = context.read_batch(batch)?;
        let df_filter = Parser::parse_proto(&filter, dataframe.schema())?;

        let results = dataframe.filter(df_filter)?.collect().await?;

        let mut partition_info = vec![];
        for result_batch in results {
            for index in as_primitive_array::<UInt32Type>(result_batch.column(schema_len))?
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

fn batch_from_partition(wrapper: &JniWrapper, schema: SchemaRef, index_field: Field) -> Result<RecordBatch> {
    let scalar_values = wrapper
        .partition_info
        .iter()
        .map(|partition_info| partition_desc_to_scalar_values(schema.clone(), partition_info.partition_desc.clone()))
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
    let mut fields_with_index = schema.all_fields().into_iter().cloned().collect::<Vec<_>>();
    fields_with_index.push(index_field);
    let schema_with_index = SchemaRef::new(Schema::new(fields_with_index));
    columns.push(Arc::new(UInt32Array::from(
        (0..wrapper.partition_info.len() as u32).collect::<Vec<_>>(),
    )));

    Ok(RecordBatch::try_new(schema_with_index, columns)?)
}

pub fn date_str_to_epoch_days(value: &str) -> Result<i32> {
    let date = chrono::NaiveDate::parse_from_str(value, DATE32_FORMAT).map_err(|e| External(Box::new(e)))?;
    let datetime = date
        .and_hms_opt(12, 12, 12)
        .ok_or(Internal("invalid h/m/s".to_string()))?;
    let epoch_time = DateTime::from_timestamp_millis(0).ok_or(Internal(
        "the number of milliseconds is out of range for a NaiveDateTim".to_string(),
    ))?;

    Ok(datetime.signed_duration_since(epoch_time.naive_utc()).num_days() as i32)
}

pub fn timestamp_str_to_unix_time(value: &str, fmt: &str) -> Result<Duration> {
    let datetime = chrono::NaiveDateTime::parse_from_str(value, fmt).map_err(|e| External(Box::new(e)))?;
    let epoch_time = DateTime::from_timestamp_millis(0).ok_or(Internal(
        "the number of milliseconds is out of range for a NaiveDateTim".to_string(),
    ))?;

    Ok(datetime.signed_duration_since(epoch_time.naive_utc()))
}
