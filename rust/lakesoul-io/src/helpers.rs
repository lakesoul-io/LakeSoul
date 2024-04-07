// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{DataType, Schema, SchemaBuilder, SchemaRef};
use datafusion::{
    datasource::{file_format::FileFormat, listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl}, physical_plan::FileScanConfig}, execution::context::SessionState, logical_expr::col, physical_expr::{create_physical_expr, PhysicalSortExpr}, physical_plan::PhysicalExpr, physical_planner::create_physical_sort_expr,
};
use datafusion_common::{DataFusionError, DFSchema, Result};
use object_store::path::Path;
use url::Url;

use crate::{lakesoul_io_config::LakeSoulIOConfig, transform::uniform_schema};

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

pub fn partition_desc_from_file_scan_config(
    conf: &FileScanConfig
) -> Result<(String, HashMap<String, String>)> {
    if conf.table_partition_cols.is_empty() {
        Ok(("-5".to_string(), HashMap::default()))
    } else {
        match conf.file_groups.first().unwrap().first() {
            Some(file) => Ok(
                (conf
                     .table_partition_cols
                     .iter()
                     .enumerate()
                     .map(|(idx, col)| {
                         format!("{}={}", col.name().clone(), file.partition_values[idx].to_string())
                     })
                     .collect::<Vec<_>>()
                     .join(","),
                 HashMap::from_iter(
                     conf
                         .table_partition_cols
                         .iter()
                         .enumerate()
                         .map(|(idx, col)| {
                             (col.name().clone(), file.partition_values[idx].to_string())
                         })
                 ))
            ),
            None => Err(DataFusionError::External(format!("Invalid file_group {:?}", conf.file_groups).into())),
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

            let target_schema = uniform_schema(lakesoul_io_config.schema());

            let table_partition_cols = range_partition_to_partition_cols(target_schema.clone(), lakesoul_io_config.range_partitions_slice())?;
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
            let target_schema = uniform_schema(lakesoul_io_config.schema());
            let table_partition_cols = range_partition_to_partition_cols(target_schema.clone(), lakesoul_io_config.range_partitions_slice())?;

            let listing_options = ListingOptions::new(file_format.clone())
                .with_file_extension(".parquet")
                .with_table_partition_cols(table_partition_cols)
                .with_insert_mode(datafusion::datasource::listing::ListingTableInsertMode::AppendNewFiles);
            let prefix =
                ListingTableUrl::parse_create_local_if_not_exists(lakesoul_io_config.prefix.clone(), true)?;

            ListingTableConfig::new(prefix)
                .with_listing_options(listing_options)
                .with_schema(target_schema)
        }
    };

    Ok((config.file_schema.clone(), Arc::new(ListingTable::try_new(config)?)))
}

pub async fn infer_schema(sc: &SessionState, table_paths: &[ListingTableUrl], file_format: Arc<dyn FileFormat>) -> Result<SchemaRef> {
    // Create default parquet options
    let object_store_url = table_paths
        .first()
        .ok_or(DataFusionError::Internal("no table path".to_string()))?
        .object_store();
    let store = sc.runtime_env().object_store(object_store_url.clone())?;
    let mut objects = vec![];

    for url in table_paths {
        objects.push(store.head(&Path::from_url_path(<ListingTableUrl as AsRef<Url>>::as_ref(url).path())?).await?);
    }

    // Resolve the schema
    file_format.infer_schema(sc, &store, &objects).await
}

