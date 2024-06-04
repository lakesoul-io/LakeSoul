// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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
    common::{DFField, DFSchema},
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::Expr,
    physical_expr::create_physical_expr,
};
use lakesoul_metadata::MetaDataClientRef;
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tracing::{debug, trace};
use url::Url;

use crate::error::Result;
use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
use proto::proto::entity::{PartitionInfo, TableInfo};

use crate::{
    catalog::{parse_table_info_partitions, LakeSoulTableProperty},
    serialize::arrow_java::schema_from_metadata_str,
};

pub(crate) fn create_io_config_builder_from_table_info(table_info: Arc<TableInfo>) -> Result<LakeSoulIOConfigBuilder> {
    let (range_partitions, hash_partitions) = parse_table_info_partitions(table_info.partitions.clone())?;
    let properties = serde_json::from_str::<LakeSoulTableProperty>(&table_info.properties)?;
    Ok(LakeSoulIOConfigBuilder::new()
        .with_schema(schema_from_metadata_str(&table_info.table_schema))
        .with_prefix(table_info.table_path.clone())
        .with_primary_keys(hash_partitions)
        .with_range_partitions(range_partitions)
        .with_hash_bucket_num(properties.hash_bucket_num.unwrap_or(1)))
}

pub async fn prune_partitions(
    all_partition_info: Vec<PartitionInfo>,
    filters: &[Expr],
    partition_cols: &[(String, DataType)],
) -> Result<Vec<PartitionInfo>> {
    if filters.is_empty() {
        return Ok(all_partition_info);
    }

    let mut builders: Vec<_> = (0..partition_cols.len())
        .map(|_| StringBuilder::with_capacity(all_partition_info.len(), all_partition_info.len() * 10))
        .collect();

    for partition in &all_partition_info {
        let cols = partition_cols.iter().map(|x| x.0.as_str());
        let parsed = parse_partitions_for_partition_desc(&partition.partition_desc, cols).unwrap_or_default();

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
            .map(|(n, d)| DFField::new_unqualified(n, d.clone(), true))
            .collect(),
        Default::default(),
    )?;

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    // TODO: Plumb this down
    let props = ExecutionProps::new();

    // Applies `filter` to `batch` returning `None` on error
    let do_filter = |filter| -> Option<ArrayRef> {
        let expr = create_physical_expr(filter, &df_schema, &schema, &props).ok()?;
        expr.evaluate(&batch).ok()?.into_array(all_partition_info.len()).ok()
    };

    //.Compute the conjunction of the filters, ignoring errors
    let mask = filters.iter().fold(None, |acc, filter| match (acc, do_filter(filter)) {
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

pub async fn listing_partition_info(
    partition_info: PartitionInfo,
    store: &dyn ObjectStore,
    client: MetaDataClientRef,
) -> datafusion::error::Result<(PartitionInfo, Vec<ObjectMeta>)> {
    trace!("Listing partition {:?}", partition_info);
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
