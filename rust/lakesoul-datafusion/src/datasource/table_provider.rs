// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::datasource::TableProvider`] implementation for LakeSoul table.

use std::any::Any;
use std::env;
use std::ops::Deref;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{project_schema, Constraint, Statistics, ToDFSchema};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{CreateExternalTable, TableProviderFilterPushDown, TableType};
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
use log::info;
use proto::proto::entity::TableInfo;

use crate::catalog::{format_table_info_partitions, parse_table_info_partitions, LakeSoulTableProperty};
use crate::lakesoul_table::helpers::{
    case_fold_column_name, case_fold_table_name, listing_partition_info, parse_partitions_for_partition_desc,
    prune_partitions,
};
use crate::serialize::arrow_java::{schema_from_metadata_str, ArrowJavaSchema};

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
/// 3. Projection pushdown for formats that support
///
#[derive(Debug)]
pub struct LakeSoulTableProvider {
    pub(crate) listing_options: ListingOptions,
    pub(crate) listing_table_paths: Vec<ListingTableUrl>,
    pub(crate) client: MetaDataClientRef,
    pub(crate) table_info: Arc<TableInfo>,
    // table schema is the normalized schema of TableProvider
    pub(crate) table_schema: SchemaRef,
    pub(crate) file_schema: SchemaRef,
    pub(crate) primary_keys: Vec<String>,
    pub(crate) range_partitions: Vec<String>,
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
        // O(nm), n = number of table fields, m = number of range partitions
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

        let listing_options = listing_table.options().clone();
        let listing_table_paths = listing_table.table_paths().clone();
        Ok(Self {
            listing_options,
            listing_table_paths,
            client,
            table_info,
            table_schema,
            file_schema,
            primary_keys: hash_partitions,
            range_partitions,
        })
    }

    pub async fn new_from_create_external_table(
        _session_state: &dyn Session,
        client: MetaDataClientRef,
        cmd: &CreateExternalTable,
    ) -> crate::error::Result<Self> {
        let primary_keys = cmd
            .constraints
            .iter()
            .flat_map(|constraint| match constraint {
                Constraint::PrimaryKey(pk) => pk
                    .iter()
                    .map(|col| cmd.schema.as_ref().field(*col).name().to_string())
                    .collect::<Vec<_>>(),
                _ => vec![],
            })
            .collect::<Vec<_>>();

        let range_partitions = cmd.table_partition_cols.clone();

        info!(
            "LakeSoulTableProvider::new_from_create_external_table cmd.options: {:?}",
            cmd.options
        );

        let mut schema_builder = SchemaBuilder::new();
        for field in cmd.schema.as_ref().fields() {
            schema_builder.push(Field::new(
                case_fold_column_name(field.name()),
                field.data_type().clone(),
                field.is_nullable() && !primary_keys.contains(&field.name()),
            ));
        }

        let (cdc_column, use_cdc) = if cmd.options.contains_key("format.use_cdc") {
            let cdc_column = cmd
                .options
                .get("format.cdc_column")
                .unwrap_or(&"rowKinds".to_string())
                .to_string();
            let use_cdc = cmd
                .options
                .get("format.use_cdc")
                .unwrap_or(&"false".to_string())
                .to_string();
            let cdc_field = Arc::new(Field::new(cdc_column.clone(), DataType::Utf8, true));
            schema_builder.try_merge(&cdc_field)?;
            (Some(cdc_column), Some(use_cdc))
        } else {
            (None, None)
        };

        let table_schema = Arc::new(schema_builder.finish());

        let file_schema: SchemaRef = table_schema.clone();

        let table_info = Arc::new(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_namespace: cmd.name.schema().unwrap_or("default").to_string(),
            table_name: case_fold_table_name(cmd.name.table()),
            table_schema: serde_json::to_string::<ArrowJavaSchema>(&table_schema.clone().into())?,
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: if primary_keys.is_empty() { None } else { Some(4) },
                datafusion_properties: Some(cmd.options.clone()),
                cdc_change_column: cdc_column,
                use_cdc,
                ..Default::default()
            })
            .unwrap(),
            partitions: format_table_info_partitions(&range_partitions, &primary_keys),
            table_path: if cmd.location.is_empty() {
                format!(
                    "file://{}/{}/{}",
                    env::current_dir().unwrap().to_str().unwrap(),
                    cmd.name.schema().unwrap_or("default"),
                    cmd.name.table()
                )
            } else {
                cmd.location.to_string()
            },
            domain: "public".to_string(),
        });
        Ok(Self {
            listing_options: LakeSoulMetaDataParquetFormat::default_listing_options().await?,
            listing_table_paths: vec![],
            client,
            table_info,
            table_schema,
            file_schema,
            primary_keys,
            range_partitions,
        })
    }

    fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
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
        info!("is_partition_filter: {:?}", f);
        // O(nm), n = number of expr fields, m = number of range partitions
        f.column_refs()
            .iter()
            .all(|col| self.range_partitions.contains(&col.name))
    }

    pub fn options(&self) -> &ListingOptions {
        &self.listing_options
    }

    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        &self.listing_table_paths
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
                .map(|sort| {
                    let Sort { expr, asc, nulls_first } = sort;
                    if let Expr::Column(col) = expr {
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
                })
                .collect::<Result<Vec<_>>>()?;
            all_sort_orders.push(LexOrdering::new(sort_exprs));
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

        let all_partition_info = self.client.get_all_partition_info(self.table_id()).await.map_err(|e| {
            DataFusionError::External(
                format!(
                    "get all partition_info of table {} failed: {}",
                    &self.table_info().table_name,
                    e
                )
                .into(),
            )
        })?;
        let partition_filters = filters
            .iter()
            .filter(|f| self.is_partition_filter(f))
            .cloned()
            .collect::<Vec<Expr>>();

        let prune_partition_info = prune_partitions(
            all_partition_info,
            partition_filters.as_slice(),
            self.table_partition_cols(),
        )
        .await
        .map_err(|_| {
            DataFusionError::External(
                format!(
                    "prune partitions for all partitions of table {} failed",
                    &self.table_info().table_name
                )
                .into(),
            )
        })?;

        info!("prune_partition_info: {:?}", prune_partition_info);

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
                    statistics: None,
                    extensions: None,
                    metadata_size_hint: None,
                })
                .collect::<Vec<_>>();
            file_groups.push(files)
        }
        info!("file_groups: {:?}", file_groups);

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
        session_state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session_state = session_state.as_any().downcast_ref::<SessionState>().unwrap();
        let (partitioned_file_lists, _) = self.list_files_for_scan(session_state, filters, limit).await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let schema = self.schema();
            let projected_schema = project_schema(&schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // extract types of partition columns
        // O(nm), n = number of partitions, m = number of columns
        let table_partition_cols = self
            .options()
            .table_partition_cols
            .iter()
            .map(|col| Ok(self.schema().field_with_name(&col.0)?.clone()))
            .collect::<Result<Vec<_>>>()?;

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.schema().as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(&expr, &table_df_schema, session_state.execution_props())?;
            Some(filters)
        } else {
            None
        };

        let object_store_url = if let Some(url) = self.table_paths().first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        // create the execution plan
        self.options()
            .format
            .create_physical_plan(
                session_state,
                FileScanConfig {
                    object_store_url,
                    file_schema: self.schema(),
                    file_groups: partitioned_file_lists,
                    constraints: Default::default(),
                    statistics: Statistics::new_unknown(self.schema().deref()),
                    // projection for Table instead of File
                    projection: projection.cloned(),
                    limit,
                    output_ordering: self.try_create_output_ordering()?,
                    table_partition_cols,
                },
                filters.as_ref(),
            )
            .await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        info!("supports_filters_pushdown: {:?}", filters);
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
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Get the object store for the table path.
        // let url = Url::parse(table_path.as_str()).unwrap();
        // let _store = state.runtime_env().object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?);
        // dbg!(&_store);
        let state = state.as_any().downcast_ref::<SessionState>().unwrap();

        // let file_type_writer_options = match &self.options().file_type_write_options {
        //     Some(opt) => opt.clone(),
        //     None => FileTypeWriterOptions::build_default(&file_format.file_type(), state.config_options())?,
        // };

        // Sink related option, apart from format
        let config = FileSinkConfig {
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_groups: vec![],
            output_schema: self.schema(),
            table_partition_cols: self.options().table_partition_cols.clone(),
            insert_op,
            keep_partition_by_columns: false,
            file_extension: "".to_string(),
        };

        let _unsorted: Vec<Vec<Expr>> = vec![];
        // todo: fix this
        let order_requirements = None;

        self.options()
            .format
            .create_writer_physical_plan(input, state, config, order_requirements)
            .await
    }
}
