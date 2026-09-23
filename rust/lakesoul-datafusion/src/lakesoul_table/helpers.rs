// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The utilities for LakeSoul table.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, AsArray, StringBuilder},
    compute::prep_null_mask_filter,
    datatypes::{DataType, Field, Fields, Schema},
    record_batch::RecordBatch,
};
use arrow_arith::boolean::and;
use arrow_cast::cast;
use datafusion::{
    common::DFSchema, error::DataFusionError, execution::context::ExecutionProps,
    logical_expr::Expr, logical_expr::physical_planning_context::PhysicalPlanningContext,
    physical_expr::create_physical_expr,
};
use lakesoul_io::config::{
    LakeSoulIOConfigBuilder, OPTION_KEY_CDC_COLUMN, OPTION_KEY_STABLE_SORT,
};
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata_proto::entity::{PartitionInfo, TableInfo};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};
use rootcause::report;
use url::Url;

use crate::Result;
use lakesoul_common::ser::arrow_java::schema_from_table_info_metadata;

use crate::catalog::{LakeSoulTableProperty, parse_table_info_partitions};

/// Create a [`LakeSoulIOConfigBuilder`] from the table info.
pub(crate) fn create_io_config_builder_from_table_info(
    table_info: Arc<TableInfo>,
    options: HashMap<String, String>,
    object_store_options: HashMap<String, String>,
) -> Result<LakeSoulIOConfigBuilder> {
    let (range_partitions, hash_partitions) =
        parse_table_info_partitions(&table_info.partitions)?;
    let properties =
        serde_json::from_str::<LakeSoulTableProperty>(&table_info.properties)?;
    let use_cdc = properties
        .use_cdc
        .map_or("false".to_string(), |use_cdc| use_cdc.clone());
    let cdc_column = properties
        .cdc_change_column
        .map_or("".to_string(), |cdc_column| cdc_column.clone());
    let dynamic_partition = hash_partitions.len() + range_partitions.len() > 0;

    let physical_format = crate::catalog::table_file_format(&table_info.properties)?;
    // Internal IVM tables may bucket by a prefix of the merge key. The property
    // is only honoured for tables explicitly marked internal, and the bucket
    // columns must be a prefix of the primary keys so merge-on-read keeps
    // working on the bucket-ordered files.
    let bucket_columns = match properties.ivm_bucket_columns.as_deref() {
        Some(columns) => {
            if properties.ivm_internal.as_deref() != Some("true") {
                return Err(report!(
                    "lakesoul.ivm.bucket_columns requires lakesoul.ivm.internal=true"
                ));
            }
            let columns = columns
                .split(',')
                .map(str::trim)
                .filter(|column| !column.is_empty())
                .map(String::from)
                .collect::<Vec<_>>();
            let is_prefix = columns.len() <= hash_partitions.len()
                && columns
                    .iter()
                    .zip(hash_partitions.iter())
                    .all(|(bucket, key)| bucket == key);
            if !is_prefix {
                return Err(report!(
                    "lakesoul.ivm.bucket_columns {:?} must be a prefix of the primary keys {:?}",
                    columns,
                    hash_partitions
                ));
            }
            columns
        }
        None => Vec::new(),
    };
    // Vector index columns get small row blocks on write so the candidate
    // rows can later be fetched by row index cheaply.
    let vector_columns = crate::vector_index::parse_vector_index_columns(
        properties.vector_index_columns.as_deref(),
    )
    .unwrap_or_default()
    .into_iter()
    .map(|config| config.column)
    .collect::<Vec<_>>();
    let mut builder = LakeSoulIOConfigBuilder::new()
        .with_schema(Arc::new(schema_from_table_info_metadata(
            &table_info.table_schema,
            &table_info.table_schema_arrow_ipc,
            &table_info.table_schema_arrow_ipc_json_hash,
        )?))
        .with_prefix(table_info.table_path.clone())
        .with_physical_format(physical_format)
        .with_primary_keys(hash_partitions)
        .with_hash_partitioning_columns(bucket_columns)
        .with_vector_columns(vector_columns)
        .with_range_partitions(range_partitions)
        .with_hash_bucket_num(properties.hash_bucket_num.unwrap_or(String::from("1")))
        .set_dynamic_partition(dynamic_partition)
        .with_option(OPTION_KEY_STABLE_SORT, use_cdc)
        .with_option(OPTION_KEY_CDC_COLUMN, cdc_column);

    for (key, value) in options {
        builder = builder.with_option(key, value);
    }

    for (key, value) in object_store_options {
        builder = builder.with_object_store_option(key, value);
    }

    Ok(builder)
}

/// Prune the partition infos according to the filters.
pub async fn prune_partitions(
    all_partition_info: Vec<PartitionInfo>,
    filters: &[Expr],
    partition_cols: &[(String, DataType)],
) -> Result<Vec<PartitionInfo>> {
    // Without partition columns there is nothing to prune, and without
    // filters there is no predicate to evaluate on partition values. Both
    // cases return every partition untouched.
    if filters.is_empty() || partition_cols.is_empty() {
        return Ok(all_partition_info);
    }

    let mut builders: Vec<_> = (0..partition_cols.len())
        .map(|_| {
            StringBuilder::with_capacity(
                all_partition_info.len(),
                all_partition_info.len() * 10,
            )
        })
        .collect();

    for partition in &all_partition_info {
        let cols = partition_cols.iter().map(|x| x.0.as_str());
        let parsed = parse_partitions_for_partition_desc(&partition.partition_desc, cols)
            .unwrap_or_default();

        let mut builders = builders.iter_mut();
        for (p, b) in parsed.iter().zip(&mut builders) {
            b.append_value(p);
        }
        builders.for_each(|b| b.append_null());
    }

    let arrays = partition_cols
        .iter()
        .zip(builders)
        .map(|((_, d), mut builder)| {
            let array = builder.finish();
            cast(&array, d)
        })
        .collect::<Result<_, _>>()?;

    let fields: Fields = partition_cols
        .iter()
        .map(|(n, d)| Field::new(n, d.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let df_schema = DFSchema::new_with_metadata(
        partition_cols
            .iter()
            .map(|(n, d)| (None, Arc::new(Field::new(n, d.clone(), true))))
            .collect(),
        Default::default(),
    )?;

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    // TODO: Plumb this down
    let props = ExecutionProps::new();

    // Applies `filter` to `batch` returning `None` on error
    let do_filter = |filter| -> Option<ArrayRef> {
        let expr = create_physical_expr(
            filter,
            &df_schema,
            &props,
            &PhysicalPlanningContext::default(),
        )
        .ok()?;
        expr.evaluate(&batch)
            .ok()?
            .into_array(all_partition_info.len())
            .ok()
    };

    //.Compute the conjunction of the filters, ignoring errors
    let mask = filters
        .iter()
        .fold(None, |acc, filter| match (acc, do_filter(filter)) {
            (Some(a), Some(b)) => Some(and(&a, b.as_boolean()).unwrap_or(a)),
            (None, Some(r)) => Some(r.as_boolean().clone()),
            (r, None) => r,
        });

    let mask = match mask {
        Some(mask) => mask,
        None => return Ok(all_partition_info),
    };

    // Don't retain partitions that evaluated to null
    let prepared = match mask.null_count() {
        0 => mask,
        _ => prep_null_mask_filter(&mask),
    };

    // Sanity check
    assert_eq!(prepared.len(), all_partition_info.len());

    let filtered = all_partition_info
        .into_iter()
        .zip(prepared.values())
        .filter_map(|(p, f)| f.then_some(p))
        .collect();

    Ok(filtered)
}

/// Parse the partition description and the table partition columns.
pub fn parse_partitions_for_partition_desc<'a, I>(
    partition_desc: &'a str,
    table_partition_cols: I,
) -> Option<Vec<&'a str>>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut part_values = vec![];
    for (part, pn) in partition_desc.split(',').zip(table_partition_cols) {
        match part.split_once('=') {
            Some((name, val)) if name == pn => part_values.push(val),
            _ => {
                debug!(
                    "Ignoring file: partition_desc='{}', part='{}', partition_col='{}'",
                    partition_desc, part, pn,
                );
                return None;
            }
        }
    }
    Some(part_values)
}

/// Listing the partition info and the files from the metadata client.
pub async fn listing_partition_info(
    partition_info: PartitionInfo,
    store: &dyn ObjectStore,
    client: MetaDataClientRef,
) -> datafusion::error::Result<(PartitionInfo, Vec<ObjectMeta>)> {
    info!("Listing partition {:?}", partition_info);
    let paths = client
        .get_data_files_of_single_partition(&partition_info)
        .await
        .map_err(|_| DataFusionError::External("listing partition info failed".into()))?;
    let mut files = Vec::new();
    for path in paths {
        let result = store
            .head(&Path::from_url_path(
                Url::parse(path.as_str())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .path(),
            )?)
            .await?;
        files.push(result);
    }
    Ok((partition_info, files))
}

/// Case fold the table name.
pub fn case_fold_table_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Case fold the column name.
pub fn case_fold_column_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ivm_table_info(properties: &str) -> Arc<TableInfo> {
        let schema = Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("row_id", DataType::Int64, false),
        ]);
        Arc::new(TableInfo {
            table_id: "table_ivm".to_string(),
            table_schema: lakesoul_common::ser::arrow_java::schema_to_metadata_str(
                &schema,
            ),
            properties: properties.to_string(),
            partitions: ";k,row_id".to_string(),
            ..Default::default()
        })
    }

    fn ivm_io_config(properties: &str) -> Result<lakesoul_io::config::LakeSoulIOConfig> {
        Ok(create_io_config_builder_from_table_info(
            ivm_table_info(properties),
            HashMap::new(),
            HashMap::new(),
        )?
        .build())
    }

    #[test]
    fn bucket_columns_prefix_is_applied() {
        let config = ivm_io_config(
            r#"{"lakesoul.ivm.internal":"true","lakesoul.ivm.bucket_columns":"k"}"#,
        )
        .unwrap();

        assert_eq!(
            config.primary_keys_slice(),
            &["k".to_string(), "row_id".to_string()]
        );
        assert_eq!(config.hash_partitioning_columns_slice(), &["k".to_string()]);
    }

    #[test]
    fn bucket_columns_default_to_primary_keys() {
        let config = ivm_io_config("{}").unwrap();

        assert_eq!(
            config.hash_partitioning_columns_slice(),
            config.primary_keys_slice()
        );
    }

    #[test]
    fn bucket_columns_must_be_a_prefix_of_primary_keys() {
        let error = ivm_io_config(
            r#"{"lakesoul.ivm.internal":"true","lakesoul.ivm.bucket_columns":"row_id"}"#,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("must be a prefix"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn bucket_columns_require_internal_table() {
        let error = ivm_io_config(r#"{"lakesoul.ivm.bucket_columns":"k"}"#).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires lakesoul.ivm.internal=true"),
            "unexpected error: {error}"
        );
    }
}
