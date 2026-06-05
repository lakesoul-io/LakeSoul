// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::datasource::TableProvider`] implementation for LakeSoul table.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::env;
use std::ops::Deref;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use arrow::error::ArrowError;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{Constraint, Statistics, ToDFSchema, project_schema};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormat;
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
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::scalar::ScalarValue;
use datafusion::{
    execution::{context::SessionState, object_store::ObjectStoreUrl},
    logical_expr::Expr,
};
use datafusion_datasource::file_sink_config::FileOutputMode;
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::file_format::{
    LakeSoulFormatRegistry, PhysicalFormat, compute_project_column_indices,
    flatten_file_scan_config_for_format,
};
use lakesoul_io::helpers::{
    listing_table_from_lakesoul_io_config, partition_desc_from_file_scan_config,
};
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::utils::qualify_path;
use proto::proto::entity::TableInfo;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::prelude::ResultExt;
use rootcause::report;

use crate::Result;
use crate::catalog::{
    LakeSoulTableProperty, format_table_info_partitions, parse_table_info_partitions,
};
use crate::lakesoul_table::helpers::{
    case_fold_column_name, case_fold_table_name,
    create_io_config_builder_from_table_info, listing_partition_info,
    parse_partitions_for_partition_desc, prune_partitions,
};
use crate::serialize::arrow_java::{ArrowJavaSchema, schema_from_metadata_str};

use super::file_format::LakeSoulMetaDataParquetFormat;

struct FormatScanGroup {
    object_store_url: ObjectStoreUrl,
    physical_format: PhysicalFormat,
    partitioned_file_lists: Vec<Vec<PartitionedFile>>,
}

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
    // logical schema keeps metadata / SQL column order
    pub(crate) logical_schema: SchemaRef,
    // scan schema keeps file columns followed by partition columns
    pub(crate) scan_schema: SchemaRef,
    pub(crate) file_schema: SchemaRef,
    pub(crate) primary_keys: Vec<String>,
    pub(crate) range_partitions: Vec<String>,
    pub(crate) pushdown_filters: bool,
    pub(crate) io_config: LakeSoulIOConfig,
    pub(crate) format_registry: Arc<LakeSoulFormatRegistry>,
}
impl LakeSoulTableProvider {
    fn needs_output_projection(
        target_schema: &SchemaRef,
        merged_schema: &SchemaRef,
    ) -> bool {
        if target_schema.fields().len() != merged_schema.fields().len() {
            return true;
        }

        target_schema
            .fields()
            .iter()
            .zip(merged_schema.fields().iter())
            .any(|(target, merged)| target.name() != merged.name())
    }

    fn split_schemas(
        logical_schema: SchemaRef,
        range_partitions: &[String],
    ) -> Result<(SchemaRef, SchemaRef)> {
        let mut range_partition_projection = Vec::with_capacity(range_partitions.len());
        let mut file_schema_projection =
            Vec::with_capacity(logical_schema.fields().len() - range_partitions.len());
        // O(nm), n = number of table fields, m = number of range partitions
        for (idx, field) in logical_schema.fields().iter().enumerate() {
            match range_partitions.contains(field.name()) {
                true => range_partition_projection.push(idx),
                false => file_schema_projection.push(idx),
            };
        }

        let file_schema = Arc::new(logical_schema.project(&file_schema_projection)?);
        let scan_schema =
            Arc::new(logical_schema.project(
                &[file_schema_projection, range_partition_projection].concat(),
            )?);
        Ok((file_schema, scan_schema))
    }

    fn logical_projection_to_scan_indices(
        projection: Option<&Vec<usize>>,
        logical_schema: &SchemaRef,
        scan_schema: &SchemaRef,
        project_full_schema: bool,
    ) -> DFResult<Option<Vec<usize>>> {
        if projection.is_none() && !project_full_schema {
            return Ok(None);
        }

        let requested_projection = projection
            .cloned()
            .unwrap_or_else(|| (0..logical_schema.fields().len()).collect());
        let projection_indices = requested_projection
            .into_iter()
            .map(|logical_idx| {
                let field = logical_schema.field(logical_idx);
                scan_schema
                    .index_of(field.name())
                    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(projection_indices))
    }

    fn scan_projection(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<Option<Vec<usize>>> {
        Self::logical_projection_to_scan_indices(
            projection,
            &self.logical_schema,
            &self.scan_schema,
            !self.range_partitions.is_empty(),
        )
    }

    pub async fn try_new(
        session_state: &SessionState,
        client: MetaDataClientRef,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
        as_sink: bool,
    ) -> Result<Self> {
        let logical_schema = schema_from_metadata_str(&table_info.table_schema);
        let (range_partitions, hash_partitions) =
            parse_table_info_partitions(&table_info.partitions)?;
        let (file_schema, scan_schema) =
            Self::split_schemas(logical_schema.clone(), &range_partitions)?;

        let parquet_force_view_types = session_state
            .config_options()
            .execution
            .parquet
            .schema_force_view_types;
        let format_registry = Arc::new(LakeSoulFormatRegistry::new(
            lakesoul_io_config.clone(),
            parquet_force_view_types,
        )?);
        let file_format: Arc<dyn FileFormat> = Arc::new(
            LakeSoulMetaDataParquetFormat::new(
                client.clone(),
                Arc::new(
                    ParquetFormat::new().with_force_view_types(parquet_force_view_types),
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
            logical_schema,
            scan_schema,
            file_schema,
            primary_keys: hash_partitions,
            range_partitions,
            pushdown_filters: lakesoul_io_config.file_filter_pushdown(),
            io_config: lakesoul_io_config,
            format_registry,
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

        let logical_schema = Arc::new(schema_builder.finish());
        let (file_schema, scan_schema) =
            Self::split_schemas(logical_schema.clone(), &range_partitions)?;

        let table_info = Arc::new(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_namespace: cmd.name.schema().unwrap_or("default").to_string(),
            table_name: case_fold_table_name(cmd.name.table()),
            table_schema: serde_json::to_string::<ArrowJavaSchema>(
                &logical_schema.clone().into(),
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
        let io_config = create_io_config_builder_from_table_info(
            table_info.clone(),
            cmd.options.clone(),
            HashMap::new(),
        )?
        .build();
        let format_registry = Arc::new(LakeSoulFormatRegistry::new(
            io_config.clone(),
            session_state
                .config_options()
                .execution
                .parquet
                .schema_force_view_types,
        )?);
        Ok(Self {
            listing_options: LakeSoulMetaDataParquetFormat::default_listing_options()
                .await?,
            listing_table_paths: vec![],
            client,
            table_info,
            logical_schema,
            scan_schema,
            file_schema,
            primary_keys,
            range_partitions,
            pushdown_filters: session_state
                .config_options()
                .execution
                .parquet
                .pushdown_filters, // TODO after more format
            io_config,
            format_registry,
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

    pub fn scan_schema(&self) -> SchemaRef {
        self.scan_schema.clone()
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
            ctx.runtime_env()
                .object_store(url)
                .attach(format!("{:?}", ctx.runtime_env().object_store_registry))?
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
                    ordering: None,
                    extensions: None,
                    metadata_size_hint: None,
                })
                .collect::<Vec<_>>();
            file_groups.push(files)
        }
        debug!("file_groups: {:#?}", file_groups);

        Ok((file_groups, Statistics::new_unknown(self.schema().deref())))
    }

    fn format_scan_groups(
        format_registry: &LakeSoulFormatRegistry,
        object_store_url: ObjectStoreUrl,
        partitioned_file_lists: Vec<Vec<PartitionedFile>>,
    ) -> Result<Vec<FormatScanGroup>> {
        let mut groups: Vec<FormatScanGroup> = vec![];

        for partitioned_files in partitioned_file_lists {
            let mut files_by_format: Vec<(PhysicalFormat, Vec<PartitionedFile>)> = vec![];

            for file in partitioned_files {
                let physical_format = format_registry
                    .physical_format_for_path(file.object_meta.location.as_ref())?;

                if let Some((_, files)) = files_by_format
                    .iter_mut()
                    .find(|(format, _)| *format == physical_format)
                {
                    files.push(file);
                } else {
                    files_by_format.push((physical_format, vec![file]));
                }
            }

            for (physical_format, files) in files_by_format {
                if let Some(group) = groups.iter_mut().find(|group| {
                    group.object_store_url == object_store_url
                        && group.physical_format == physical_format
                }) {
                    group.partitioned_file_lists.push(files);
                } else {
                    groups.push(FormatScanGroup {
                        object_store_url: object_store_url.clone(),
                        physical_format,
                        partitioned_file_lists: vec![files],
                    });
                }
            }
        }

        Ok(groups)
    }
}

#[async_trait]
impl TableProvider for LakeSoulTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.logical_schema.clone()
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
            .map(|col| {
                Ok(Arc::new(
                    self.scan_schema().field_with_name(&col.0)?.clone(),
                ))
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        // TODO change logic when datafusion 52
        let table_schema =
            TableSchema::new(self.file_schema.clone(), table_partition_cols.clone());
        let statistics = Arc::new(statistics);

        let object_store_url = if let Some(url) = self.table_paths().first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        let projection_indices = self.scan_projection(projection)?;
        let scan_schema =
            project_schema(table_schema.table_schema(), projection_indices.as_ref())?;
        let merged_projection = compute_project_column_indices(
            table_schema.table_schema().clone(),
            scan_schema.clone(),
            self.primary_keys.as_slice(),
            &self.io_config.cdc_column(),
        );
        let merged_schema =
            project_schema(table_schema.table_schema(), merged_projection.as_ref())?;

        let filter = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.schema().as_ref().clone().to_dfschema()?;
            Some(create_physical_expr(
                &expr,
                &table_df_schema,
                session_state.execution_props(),
            )?)
        } else {
            None
        };

        let partition_schema = Arc::new(Schema::new(table_partition_cols));
        let output_ordering = self
            .try_create_output_ordering()
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
        let format_groups = Self::format_scan_groups(
            self.format_registry.as_ref(),
            object_store_url,
            partitioned_file_lists,
        )
        .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;

        let mut flatten_configs = vec![];
        for group in format_groups {
            let file_format = self.format_registry.file_format(group.physical_format);
            let file_source = file_format.file_source(table_schema.clone());
            let mut scan_config =
                FileScanConfigBuilder::new(group.object_store_url, file_source)
                    .with_file_groups(
                        group
                            .partitioned_file_lists
                            .into_iter()
                            .map(|files| {
                                FileGroup::new(files).with_statistics(statistics.clone())
                            })
                            .collect(),
                    )
                    .with_statistics((*statistics).clone())
                    .with_projection_indices(projection_indices.clone())?
                    .with_limit(limit)
                    .with_output_ordering(output_ordering.clone())
                    .with_file_compression_type(
                        self.format_registry
                            .file_compression_type(group.physical_format),
                    )
                    .build();

            if let Some(filter) = &filter {
                let res = scan_config.try_pushdown_filters(
                    vec![filter.clone()],
                    session_state.config_options(),
                )?;
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

            let group_flatten_configs = flatten_file_scan_config_for_format(
                session_state,
                file_format,
                scan_config,
                self.primary_keys.as_slice(),
                &self.io_config.cdc_column(),
                partition_schema.clone(),
                scan_schema.clone(),
            )
            .await
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
            flatten_configs.extend(group_flatten_configs);
        }

        let mut inputs_map: HashMap<
            String,
            (
                Arc<HashMap<String, String>>,
                (Vec<Arc<dyn ExecutionPlan>>, Vec<String>),
            ),
        > = HashMap::new();
        let mut column_nullable = HashSet::<String>::new();

        for config in flatten_configs {
            let (partition_desc, partition_values) =
                partition_desc_from_file_scan_config(&config).map_err(|report| {
                    DataFusionError::External(report.into_boxed_error())
                })?;
            let partition_values = Arc::new(partition_values);
            let file_path = config.file_groups[0].files()[0].path().to_string();
            let input = DataSourceExec::from_data_source(config);
            for field in input.schema().fields() {
                if field.is_nullable() {
                    column_nullable.insert(field.name().clone());
                }
            }

            if let Some((_, inputs)) = inputs_map.get_mut(&partition_desc) {
                inputs.0.push(input);
                inputs.1.push(file_path);
            } else {
                inputs_map.insert(
                    partition_desc,
                    (partition_values, (vec![input], vec![file_path])),
                );
            }
        }

        let merged_schema = Arc::new(Schema::new(
            merged_schema
                .fields()
                .iter()
                .map(|field| {
                    Field::new(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable() | column_nullable.contains(field.name()),
                    )
                })
                .collect::<Vec<_>>(),
        ));

        let mut partitioned_execs = Vec::new();
        for (_, (partition_values, (inputs, file_paths))) in inputs_map {
            let mut io_config = self.io_config.clone();
            io_config.set_files(file_paths);
            let merge_exec = Arc::new(
                lakesoul_io::physical_plan::MergeParquetExec::new_with_inputs(
                    merged_schema.clone(),
                    inputs,
                    io_config,
                    partition_values,
                )
                .map_err(|report| DataFusionError::External(report.into_boxed_error()))?,
            ) as Arc<dyn ExecutionPlan>;
            partitioned_execs.push(merge_exec);
        }

        let merge_exec = if partitioned_execs.len() > 1 {
            UnionExec::try_new(partitioned_execs)?
        } else {
            partitioned_execs.first().unwrap().clone()
        };

        if Self::needs_output_projection(&scan_schema, &merged_schema) {
            let mut projection_expr = vec![];
            for field in scan_schema.fields() {
                projection_expr.push((
                    datafusion::physical_expr::expressions::col(
                        field.name(),
                        &merged_schema,
                    )?,
                    field.name().clone(),
                ));
            }
            Ok(Arc::new(ProjectionExec::try_new(
                projection_expr,
                merge_exec,
            )?))
        } else {
            Ok(merge_exec)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        info!("supports_filters_pushdown: {:?}", filters);
        Self::classify_filter_pushdown(self.pushdown_filters, &self.primary_keys, filters)
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
            file_output_mode: FileOutputMode::Automatic,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use object_store::{ObjectMeta, path::Path};

    fn object_meta(location: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(location),
            last_modified: chrono::Utc.timestamp_nanos(0),
            size: 100,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn logical_projection_maps_to_scan_schema_order() {
        let logical_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
        ]));
        let scan_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c3", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
        ]));

        let projection = vec![0, 1, 2];
        let mapped = LakeSoulTableProvider::logical_projection_to_scan_indices(
            Some(&projection),
            &logical_schema,
            &scan_schema,
            true,
        )
        .unwrap();

        assert_eq!(mapped, Some(vec![0, 2, 1]));
    }

    #[test]
    fn output_projection_is_needed_when_schema_order_differs() {
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Int32, true),
        ]));
        let merged_schema = Arc::new(Schema::new(vec![
            Field::new("c2", DataType::Int32, true),
            Field::new("c1", DataType::Int32, true),
        ]));

        assert!(LakeSoulTableProvider::needs_output_projection(
            &target_schema,
            &merged_schema
        ));
    }

    #[test]
    fn format_scan_groups_split_mixed_physical_formats() {
        let registry =
            LakeSoulFormatRegistry::new(LakeSoulIOConfig::default(), false).unwrap();
        let object_store_url = ObjectStoreUrl::parse("file://").unwrap();
        let groups = LakeSoulTableProvider::format_scan_groups(
            &registry,
            object_store_url,
            vec![vec![
                PartitionedFile::from(object_meta("part-000.parquet")),
                PartitionedFile::from(object_meta("part-001.vortex")),
            ]],
        )
        .unwrap();

        assert_eq!(groups.len(), 2);
        assert_eq!(
            groups
                .iter()
                .find(|group| group.physical_format == PhysicalFormat::Parquet)
                .unwrap()
                .partitioned_file_lists[0]
                .len(),
            1
        );
        assert_eq!(
            groups
                .iter()
                .find(|group| group.physical_format == PhysicalFormat::Vortex)
                .unwrap()
                .partitioned_file_lists[0]
                .len(),
            1
        );
    }
}
