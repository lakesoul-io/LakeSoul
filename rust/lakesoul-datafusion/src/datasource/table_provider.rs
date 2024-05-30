// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Schema, SchemaRef};

use async_trait::async_trait;

use datafusion::common::{project_schema, FileTypeWriterOptions, Statistics, ToDFSchema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};

use datafusion::optimizer::utils::conjunction;
use datafusion::physical_expr::{create_physical_expr, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion::{execution::context::SessionState, logical_expr::Expr};

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use lakesoul_io::helpers::listing_table_from_lakesoul_io_config;
use lakesoul_io::lakesoul_io_config::LakeSoulIOConfig;
use lakesoul_metadata::MetaDataClientRef;
use proto::proto::entity::TableInfo;

use crate::catalog::parse_table_info_partitions;
use crate::lakesoul_table::helpers::{listing_partition_info, parse_partitions_for_partition_desc, prune_partitions};
use crate::serialize::arrow_java::schema_from_metadata_str;

use super::file_format::LakeSoulMetaDataParquetFormat;

/// Reads data from LakeSoul
///
/// # Features
///
/// 1. Merges schemas if the files have compatible but not identical schemas
///
/// 2. Hive-style partitioning support, where a path such as
/// `/files/date=1/1/2022/data.parquet` is injected as a `date` column.
///
/// 3. Projection pushdown for formats that support it such as
/// Parquet
///
/// ```
pub struct LakeSoulTableProvider {
    listing_table: Arc<ListingTable>,
    client: MetaDataClientRef,
    table_info: Arc<TableInfo>,
    table_schema: SchemaRef,
    file_schema: SchemaRef,
    primary_keys: Vec<String>,
    range_partitions: Vec<String>,
}

impl LakeSoulTableProvider {
    pub async fn try_new(
        session_state: &SessionState,
        client: MetaDataClientRef,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
        as_sink: bool,
    ) -> crate::error::Result<Self> {
        let table_schema = schema_from_metadata_str(&table_info.table_schema);
        let (range_partitions, hash_partitions) = parse_table_info_partitions(table_info.partitions.clone())?;
        let mut range_partition_projection = Vec::with_capacity(range_partitions.len());
        let mut file_schema_projection = Vec::with_capacity(table_schema.fields().len() - range_partitions.len());
        for (idx, field) in table_schema.fields().iter().enumerate() {
            match range_partitions.contains(field.name()) {
                false => file_schema_projection.push(idx),
                true => range_partition_projection.push(idx),
            };
        }

        let file_schema = Arc::new(table_schema.project(&file_schema_projection)?);
        let table_schema =
            Arc::new(table_schema.project(&[file_schema_projection, range_partition_projection].concat())?);

        let file_format: Arc<dyn FileFormat> = Arc::new(
            LakeSoulMetaDataParquetFormat::new(
                client.clone(),
                Arc::new(ParquetFormat::new()),
                table_info.clone(),
                lakesoul_io_config.clone(),
            )
            .await?,
        );

        let (_, listing_table) =
            listing_table_from_lakesoul_io_config(session_state, lakesoul_io_config.clone(), file_format, as_sink)
                .await?;

        Ok(Self {
            listing_table,
            client,
            table_info,
            table_schema,
            file_schema,
            primary_keys: hash_partitions,
            range_partitions,
        })
    }

    fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    fn table_name(&self) -> &str {
        &self.table_info.table_name
    }

    fn table_namespace(&self) -> &str {
        &self.table_info.table_namespace
    }

    fn table_id(&self) -> &str {
        &self.table_info.table_id
    }

    fn is_partition_filter(&self, f: &Expr) -> bool {
        if let Ok(cols) = f.to_columns() {
            cols.iter().all(|col| self.range_partitions.contains(&col.name))
        } else {
            false
        }
    }

    pub fn options(&self) -> &ListingOptions {
        self.listing_table.options()
    }

    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        self.listing_table.table_paths()
    }

    pub fn file_schema(&self) -> SchemaRef {
        self.file_schema.clone()
    }

    pub fn table_partition_cols(&self) -> &[(String, DataType)] {
        &self.options().table_partition_cols
    }

    /// If file_sort_order is specified, creates the appropriate physical expressions
    pub fn try_create_output_ordering(&self) -> Result<Vec<LexOrdering>> {
        let mut all_sort_orders = vec![];

        for exprs in &self.options().file_sort_order {
            // Construct PhsyicalSortExpr objects from Expr objects:
            let sort_exprs = exprs
                .iter()
                .map(|expr| {
                    if let Expr::Sort(Sort { expr, asc, nulls_first }) = expr {
                        if let Expr::Column(col) = expr.as_ref() {
                            let expr = datafusion::physical_plan::expressions::col(&col.name, self.schema().as_ref())?;
                            Ok(PhysicalSortExpr {
                                expr,
                                options: SortOptions {
                                    descending: !asc,
                                    nulls_first: *nulls_first,
                                },
                            })
                        } else {
                            Err(DataFusionError::Plan(
                                // Return an error if schema of the input query does not match with the table schema.
                                format!("Expected single column references in output_ordering, got {}", expr),
                            ))
                        }
                    } else {
                        Err(DataFusionError::Plan(format!(
                            "Expected Expr::Sort in output_ordering, but got {}",
                            expr
                        )))
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            all_sort_orders.push(sort_exprs);
        }
        Ok(all_sort_orders)
    }

    async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a SessionState,
        filters: &'a [Expr],
        _limit: Option<usize>,
    ) -> Result<(Vec<Vec<PartitionedFile>>, Statistics)> {
        let store = if let Some(url) = self.table_paths().first() {
            ctx.runtime_env().object_store(url)?
        } else {
            return Ok((vec![], Statistics::new_unknown(&self.file_schema())));
        };

        let all_partition_info = self.client.get_all_partition_info(self.table_id()).await.map_err(|_| {
            DataFusionError::External(
                format!(
                    "get all partition_info of table {} failed",
                    &self.table_info().table_name
                )
                .into(),
            )
        })?;

        let prune_partition_info = prune_partitions(all_partition_info, filters, self.table_partition_cols())
            .await
            .map_err(|_| {
                DataFusionError::External(
                    format!(
                        "get all partition_info of table {} failed",
                        &self.table_info().table_name
                    )
                    .into(),
                )
            })?;

        let mut futures = FuturesUnordered::new();
        for partition in prune_partition_info {
            futures.push(listing_partition_info(partition, store.as_ref(), self.client()))
        }

        let mut file_groups = Vec::new();

        while let Some((partition, object_metas)) = futures.next().await.transpose()? {
            let cols = self.table_partition_cols().iter().map(|x| x.0.as_str());
            let parsed = parse_partitions_for_partition_desc(&partition.partition_desc, cols);

            let partition_values = parsed
                .into_iter()
                .flatten()
                .zip(self.table_partition_cols())
                .map(|(parsed, (_, datatype))| ScalarValue::try_from_string(parsed.to_string(), datatype))
                .collect::<Result<Vec<_>>>()?;

            let files = object_metas
                .into_iter()
                .map(|object_meta| PartitionedFile {
                    object_meta,
                    partition_values: partition_values.clone(),
                    range: None,
                    extensions: None,
                })
                .collect::<Vec<_>>();
            file_groups.push(files)
        }

        Ok((file_groups, Statistics::new_unknown(self.schema().deref())))
    }
}

#[async_trait]
impl TableProvider for LakeSoulTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (partitioned_file_lists, _) = self.list_files_for_scan(state, filters, limit).await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let schema = self.schema();
            let projected_schema = project_schema(&schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        // extract types of partition columns
        let table_partition_cols = self
            .listing_table
            .options()
            .table_partition_cols
            .iter()
            .map(|col| Ok(self.schema().field_with_name(&col.0)?.clone()))
            .collect::<Result<Vec<_>>>()?;

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.schema().as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(&expr, &table_df_schema, &self.schema(), state.execution_props())?;
            Some(filters)
        } else {
            None
        };

        let object_store_url = if let Some(url) = self.listing_table.table_paths().first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        // create the execution plan
        self.listing_table
            .options()
            .format
            .create_physical_plan(
                state,
                FileScanConfig {
                    object_store_url,
                    file_schema: Arc::clone(&self.file_schema()),
                    file_groups: partitioned_file_lists,
                    statistics: Statistics::new_unknown(self.schema().deref()),
                    projection: projection.cloned(),
                    limit,
                    output_ordering: self.try_create_output_ordering()?,
                    table_partition_cols,
                    infinite_source: false,
                },
                filters.as_ref(),
            )
            .await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| {
                if self.is_partition_filter(f) {
                    Ok(TableProviderFilterPushDown::Exact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .collect()
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table_path = &self.listing_table.table_paths()[0];
        // Get the object store for the table path.
        let _store = state.runtime_env().object_store(table_path)?;

        let file_format = self.options().format.as_ref();

        let file_type_writer_options = match &self.options().file_type_write_options {
            Some(opt) => opt.clone(),
            None => FileTypeWriterOptions::build_default(&file_format.file_type(), state.config_options())?,
        };

        // Sink related option, apart from format
        let config = FileSinkConfig {
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_groups: vec![],
            output_schema: self.schema(),
            table_partition_cols: self.options().table_partition_cols.clone(),
            writer_mode: datafusion::datasource::file_format::write::FileWriterMode::PutMultipart,
            // A plan can produce finite number of rows even if it has unbounded sources, like LIMIT
            // queries. Thus, we can check if the plan is streaming to ensure file sink input is
            // unbounded. When `unbounded_input` flag is `true` for sink, we occasionally call `yield_now`
            // to consume data at the input. When `unbounded_input` flag is `false` (e.g. non-streaming data),
            // all the data at the input is sink after execution finishes. See discussion for rationale:
            // https://github.com/apache/arrow-datafusion/pull/7610#issuecomment-1728979918
            unbounded_input: false,
            single_file_output: self.options().single_file,
            overwrite,
            file_type_writer_options,
        };

        let unsorted: Vec<Vec<Expr>> = vec![];
        let order_requirements = if self.options().file_sort_order != unsorted {
            todo!()
        } else {
            None
        };

        self.options()
            .format
            .create_writer_physical_plan(input, state, config, order_requirements)
            .await
    }
}
