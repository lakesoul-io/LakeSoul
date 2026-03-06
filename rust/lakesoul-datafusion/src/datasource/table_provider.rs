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
use arrow::error::ArrowError;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraint, Statistics, ToDFSchema, project_schema};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, FileSinkConfig,
};
use datafusion::datasource::source::DataSource;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    CreateExternalTable, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, create_physical_expr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::scalar::ScalarValue;
use datafusion::{execution::context::SessionState, logical_expr::Expr};
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::helpers::listing_table_from_lakesoul_io_config;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::utils::qualify_path;
use proto::proto::entity::TableInfo;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::report;

use crate::Result;
use crate::catalog::{
    LakeSoulTableProperty, format_table_info_partitions, parse_table_info_partitions,
};
use crate::lakesoul_table::helpers::{
    case_fold_column_name, case_fold_table_name, listing_partition_info,
    parse_partitions_for_partition_desc, prune_partitions,
};
use crate::serialize::arrow_java::{ArrowJavaSchema, schema_from_metadata_str};

use super::file_format::LakeSoulMetaDataParquetFormat;

/// Reads data from LakeSoul
///
/// # Features
///
/// 1. Merge schemas if the files have compatible but not identical schemas
///
/// 2. Hive-style partitioning support, where a path such as
///    `/files/date=1/1/2022/data.parquet` is injected as a `date` column.
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
    pub(crate) pushdown_filters: bool,
}
impl LakeSoulTableProvider {
    pub async fn try_new(
        session_state: &SessionState,
        client: MetaDataClientRef,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
        as_sink: bool,
    ) -> Result<Self> {
        let table_schema = schema_from_metadata_str(&table_info.table_schema);
        let (range_partitions, hash_partitions) =
            parse_table_info_partitions(&table_info.partitions)?;
        let mut range_partition_projection = Vec::with_capacity(range_partitions.len());
        let mut file_schema_projection =
            Vec::with_capacity(table_schema.fields().len() - range_partitions.len());
        // O(nm), n = number of table fields, m = number of range partitions
        for (idx, field) in table_schema.fields().iter().enumerate() {
            match range_partitions.contains(field.name()) {
                true => range_partition_projection.push(idx),
                false => file_schema_projection.push(idx),
            };
        }

        let file_schema = Arc::new(table_schema.project(&file_schema_projection)?);
        let table_schema =
            Arc::new(table_schema.project(
                &[file_schema_projection, range_partition_projection].concat(),
            )?);

        let file_format: Arc<dyn FileFormat> = Arc::new(
            LakeSoulMetaDataParquetFormat::new(
                client.clone(),
                Arc::new(
                    ParquetFormat::new().with_force_view_types(
                        session_state
                            .config_options()
                            .execution
                            .parquet
                            .schema_force_view_types,
                    ),
                ),
                table_info.clone(),
                lakesoul_io_config.clone(),
            )
            .await?,
        );

        let (_, listing_table) = listing_table_from_lakesoul_io_config(
            session_state,
            lakesoul_io_config.clone(),
            file_format,
            as_sink,
        )
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
            pushdown_filters: lakesoul_io_config.parquet_pushdown_filters(), // TODO after add more format
        })
    }

    pub async fn new_from_create_external_table(
        session_state: &dyn Session,
        client: MetaDataClientRef,
        cmd: &CreateExternalTable,
    ) -> Result<Self> {
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

        debug!(
            "LakeSoulTableProvider::new_from_create_external_table cmd.options: {:#?}",
            cmd.options
        );

        let mut schema_builder = SchemaBuilder::new();
        for field in cmd.schema.as_ref().fields() {
            schema_builder.push(Field::new(
                case_fold_column_name(field.name()),
                field.data_type().clone(),
                field.is_nullable() && !primary_keys.contains(field.name()),
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
            let cdc_field =
                Arc::new(Field::new(cdc_column.clone(), DataType::Utf8, true));
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
            table_schema: serde_json::to_string::<ArrowJavaSchema>(
                &table_schema.clone().into(),
            )?,
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: if primary_keys.is_empty() {
                    None
                } else {
                    // TODO: 4 should be parameter
                    Some(
                        cmd.options
                            .get("hash_bucket_num")
                            .cloned()
                            .unwrap_or(String::from("4")),
                    )
                },
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
                // hdfs is not checked
                qualify_path(&cmd.location)?
            },
            domain: "public".to_string(),
        });
        Ok(Self {
            listing_options: LakeSoulMetaDataParquetFormat::default_listing_options()
                .await?,
            listing_table_paths: vec![],
            client,
            table_info,
            table_schema,
            file_schema,
            primary_keys,
            range_partitions,
            pushdown_filters: session_state
                .config_options()
                .execution
                .parquet
                .pushdown_filters, // TODO after more format
        })
    }

    fn client(&self) -> MetaDataClientRef {
        self.client.clone()
    }

    fn _primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    fn _table_name(&self) -> &str {
        &self.table_info.table_name
    }

    fn _table_namespace(&self) -> &str {
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
                            // Return an error if the schema of the input query does not match with the table schema.
                            format!("Expected single column references in output_ordering, got {}", expr),
                        ))
                    }
                })
                .collect::<Result<Vec<_>,DataFusionError>>()?;
            if let Some(ordering) = LexOrdering::new(sort_exprs) {
                all_sort_orders.push(ordering);
            }
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

        let all_partition_info = self
            .client
            .get_all_partition_info(self.table_id())
            .await
            .map_err(|e| report!(e).attach(self.table_info().table_name.clone()))?;
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
        .map_err(|report| report.attach(self.table_info().table_name.clone()))?;

        info!("prune_partition_info: {:?}", prune_partition_info);

        let mut futures = FuturesUnordered::new();
        for partition in prune_partition_info {
            futures.push(listing_partition_info(
                partition,
                store.as_ref(),
                self.client(),
            ))
        }

        let mut file_groups = Vec::new();

        while let Some((partition, object_metas)) = futures.next().await.transpose()? {
            let cols = self.table_partition_cols().iter().map(|x| x.0.as_str());
            let parsed =
                parse_partitions_for_partition_desc(&partition.partition_desc, cols);

            let partition_values = parsed
                .into_iter()
                .flatten()
                .zip(self.table_partition_cols())
                .map(|(parsed, (_, datatype))| {
                    ScalarValue::try_from_string(parsed.to_string(), datatype)
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;

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
        debug!("file_groups: {:#?}", file_groups);

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
        filters: &[Expr], // TODO
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let session_state = session_state
            .as_any()
            .downcast_ref::<SessionState>()
            .unwrap();
        let (partitioned_file_lists, statistics) = self
            .list_files_for_scan(session_state, filters, limit)
            .await
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;

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
            .map(|col| Ok(Arc::new(self.schema().field_with_name(&col.0)?.clone())))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        // TODO change logic when datafusion 52
        let table_schema =
            TableSchema::new(self.file_schema.clone(), table_partition_cols.clone());
        let statistics = Arc::new(statistics);
        let file_source = self
            .options()
            .format
            .file_source()
            .with_statistics(Statistics::new_unknown(&self.schema()))
            .with_schema(table_schema.clone());

        let object_store_url = if let Some(url) = self.table_paths().first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let mut scan_config = FileScanConfigBuilder::new(
            object_store_url,
            table_schema.file_schema().clone(), // TODO: change logic when datafusion 52
            file_source,
        )
        .with_file_groups(
            partitioned_file_lists
                .into_iter()
                .map(|files| FileGroup::new(files).with_statistics(statistics.clone()))
                .collect(),
        )
        .with_projection_indices(projection.cloned())
        .with_limit(limit)
        .with_newlines_in_values(false)
        .with_output_ordering(
            self.try_create_output_ordering()
                .map_err(|report| DataFusionError::External(report.into_boxed_error()))?,
        )
        .with_table_partition_cols(
            table_partition_cols
                .into_iter()
                .map(|field_ref| field_ref.as_ref().clone())
                .collect(),
        )
        .with_file_compression_type(FileCompressionType::ZSTD) // TODO CONF;
        .build();

        if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.schema().as_ref().clone().to_dfschema()?;
            let filter = create_physical_expr(
                &expr,
                &table_df_schema,
                session_state.execution_props(),
            )?;

            let res = scan_config
                .try_pushdown_filters(vec![filter], session_state.config_options())?;
            match res.updated_node {
                Some(sc) => {
                    debug!("apply new scan config");
                    debug!("filters: {:?}", res.filters);
                    scan_config = sc
                        .as_any()
                        .downcast_ref::<FileScanConfig>()
                        .ok_or(DataFusionError::Internal(
                            "Failed to downcast FileScanConfig".into(),
                        ))?
                        .clone();
                }
                None => {
                    debug!("no updated node")
                }
            }
        }
        // create the execution plan
        self.options()
            .format
            .create_physical_plan(session_state, scan_config)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        info!("supports_filters_pushdown: {:?}", filters);

        if !self.pushdown_filters {
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }

        if self.primary_keys.is_empty() {
            // TODO session config -> io_config / config.parquet support filter pushdown
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        } else {
            // O(nml), n = number of filters, m = number of primary keys, l = number of columns
            filters
                .iter()
                .map(|f| {
                    let cols = f.column_refs();
                    if cols.iter().all(|col| self.primary_keys.contains(&col.name)) {
                        // use primary key
                        Ok(TableProviderFilterPushDown::Inexact)
                    } else {
                        Ok(TableProviderFilterPushDown::Unsupported)
                    }
                })
                .collect()
        }
    }

    #[instrument(skip(self, state))]
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let state = state.as_any().downcast_ref::<SessionState>().unwrap();
        // Sink related option, apart from format
        let config = FileSinkConfig {
            original_url: "".to_string(),
            object_store_url: self.table_paths()[0].object_store(),
            table_paths: self.table_paths().clone(),
            file_group: FileGroup::new(vec![]),
            output_schema: self.schema(),
            table_partition_cols: self.options().table_partition_cols.clone(),
            insert_op,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_string(),
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
