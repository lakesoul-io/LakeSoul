// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::datasource::TableProvider`] implementation for LakeSoul table.

use std::collections::HashMap;
use std::env;
use std::ops::Deref;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use arrow::error::ArrowError;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::{Constraint, DFSchema, Statistics, ToDFSchema, project_schema};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, FileSinkConfig,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    CreateExternalTable, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalSortExpr, create_physical_expr,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::prelude::{ident, lit};
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
    LakeSoulFormatRegistry, PhysicalFormat, append_only_scan_exec,
    compute_project_column_indices, flatten_file_scan_config_for_format,
};
use lakesoul_io::helpers::{
    listing_sink_table_from_lakesoul_io_config,
    listing_source_table_from_lakesoul_io_config, partition_desc_from_file_scan_config,
};
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::utils::qualify_path;
use lakesoul_metadata_proto::entity::TableInfo;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::prelude::ResultExt;
use rootcause::report;

use crate::Result;
use crate::catalog::{
    LakeSoulProviderOptions, LakeSoulTableProperty, format_table_info_partitions,
    parse_table_info_partitions,
};
use crate::lakesoul_table::helpers::{
    case_fold_column_name, case_fold_table_name,
    create_io_config_builder_from_table_info, listing_partition_info,
    parse_partitions_for_partition_desc, prune_partitions,
};
use lakesoul_common::ser::arrow_java::{
    schema_from_table_info_metadata, schema_to_metadata_parts,
};

use super::file_format::LakeSoulMetaDataFormat;

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
    /// Whether row-level predicates may be forwarded to the `FileSource`s
    /// (e.g. Parquet page/row-group pruning). Range-partition metadata
    /// pruning is independent of this flag.
    pub(crate) pushdown_filters: bool,
    pub(crate) io_config: LakeSoulIOConfig,
    /// Vector index configurations declared by the table's
    /// `vector_index_columns` property (empty when the table has none).
    pub(crate) vector_index_configs: Vec<crate::vector_index::VectorIndexTableConfig>,
    /// Text index configurations declared by the table's
    /// `text_index_columns` property (empty when the table has none).
    pub(crate) text_index_configs: Vec<crate::text_index::TextIndexTableConfig>,
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

    /// Build a source provider using the current session for object-store
    /// access and file-schema inference.
    pub async fn try_new(
        session: &dyn Session,
        client: MetaDataClientRef,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
    ) -> Result<Self> {
        Self::try_new_inner(
            Some(session),
            LakeSoulProviderOptions::from_session(session),
            client,
            lakesoul_io_config,
            table_info,
        )
        .await
    }

    /// Build a sink provider without retaining or requiring a session.
    pub async fn try_new_as_sink(
        provider_options: LakeSoulProviderOptions,
        client: MetaDataClientRef,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
    ) -> Result<Self> {
        Self::try_new_inner(
            None,
            provider_options,
            client,
            lakesoul_io_config,
            table_info,
        )
        .await
    }

    async fn try_new_inner(
        source_session: Option<&dyn Session>,
        provider_options: LakeSoulProviderOptions,
        client: MetaDataClientRef,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
    ) -> Result<Self> {
        let logical_schema = Arc::new(schema_from_table_info_metadata(
            &table_info.table_schema,
            &table_info.table_schema_arrow_ipc,
            &table_info.table_schema_arrow_ipc_json_hash,
        )?);
        let (range_partitions, hash_partitions) =
            parse_table_info_partitions(&table_info.partitions)?;
        let (file_schema, scan_schema) =
            Self::split_schemas(logical_schema.clone(), &range_partitions)?;

        let parquet_force_view_types = provider_options.parquet_force_view_types;
        let format_registry = Arc::new(LakeSoulFormatRegistry::new(
            lakesoul_io_config.clone(),
            parquet_force_view_types,
        )?);
        let file_format: Arc<dyn FileFormat> = Arc::new(LakeSoulMetaDataFormat::new(
            client.clone(),
            table_info.clone(),
            lakesoul_io_config.clone(),
            format_registry.clone(),
        )?);

        let (_, listing_table) = match source_session {
            Some(session) => {
                listing_source_table_from_lakesoul_io_config(
                    session,
                    lakesoul_io_config.clone(),
                    file_format,
                )
                .await?
            }
            None => listing_sink_table_from_lakesoul_io_config(
                lakesoul_io_config.clone(),
                file_format,
            )?,
        };

        let listing_options = listing_table.options().clone();
        let listing_table_paths = listing_table.table_paths().clone();
        let vector_index_configs =
            crate::vector_index::parse_vector_index_from_table_properties(
                &table_info.properties,
            )
            .unwrap_or_default();
        let text_index_configs =
            crate::text_index::parse_text_index_from_table_properties(
                &table_info.properties,
            )
            .unwrap_or_default();
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
            pushdown_filters: provider_options.pushdown_filters,
            io_config: lakesoul_io_config,
            vector_index_configs,
            text_index_configs,
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

        // The `vector_index_columns` option declares vector indexes at
        // creation time (JSON array, as in the Python SDK); it is validated
        // against the schema and stored as a table property so writes
        // auto-build the indexes.
        // Table-factory options arrive with a `format.` prefix (like
        // `format.use_cdc`); accept both forms.
        let vector_index_columns = cmd
            .options
            .get("format.vector_index_columns")
            .or_else(|| cmd.options.get("vector_index_columns"))
            .cloned();
        if let Some(raw) = &vector_index_columns {
            let configs = crate::vector_index::parse_vector_index_columns(Some(raw))
                .map_err(|report| {
                    report!("invalid vector_index_columns option: {report}")
                })?;
            crate::vector_index::validate_vector_index_configs(
                &configs,
                logical_schema.as_ref(),
                &primary_keys,
            )
            .map_err(|report| report!("invalid vector_index_columns option: {report}"))?;
        }

        // The `text_index_columns` option declares text indexes at creation
        // time (JSON array, as in the Python SDK); it is validated against
        // the schema and stored as a table property so writes auto-build the
        // indexes.  Accept both the bare and the `format.`-prefixed key,
        // like the vector option above.
        let text_index_columns = cmd
            .options
            .get("format.text_index_columns")
            .or_else(|| cmd.options.get("text_index_columns"))
            .cloned();
        if let Some(raw) = &text_index_columns {
            let configs = crate::text_index::parse_text_index_columns(Some(raw))
                .map_err(|report| {
                    report!("invalid text_index_columns option: {report}")
                })?;
            crate::text_index::validate_text_index_configs(
                &configs,
                logical_schema.as_ref(),
                &primary_keys,
            )
            .map_err(|report| report!("invalid text_index_columns option: {report}"))?;
        }

        // Optional file format ("parquet", "vortex" or "vortex-compact"),
        // named `file_format` like the Spark/Flink connectors and stored as
        // a table property for the sink.
        let file_format = cmd
            .options
            .get("format.file_format")
            .or_else(|| cmd.options.get("file_format"))
            .cloned();
        if let Some(raw) = &file_format {
            raw.parse::<lakesoul_io::file_format::PhysicalFormat>()
                .map_err(|report| {
                    report!("invalid file_format option '{raw}': {report}")
                })?;
        }

        let (table_schema, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
            schema_to_metadata_parts(logical_schema.as_ref());

        let table_info = Arc::new(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_namespace: cmd.name.schema().unwrap_or("default").to_string(),
            table_name: case_fold_table_name(cmd.name.table()),
            table_schema,
            table_schema_arrow_ipc,
            table_schema_arrow_ipc_json_hash,
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: if primary_keys.is_empty() {
                    None
                } else {
                    // `hashBucketNum`, matching the Spark/Flink connectors.
                    // DataFusion lower-cases option keys and prefixes
                    // namespace-less keys with `format.` (so the SQL spelling
                    // `'hashBucketNum'` arrives as `format.hashbucketnum`).
                    Some(
                        cmd.options
                            .get("format.hashbucketnum")
                            .or_else(|| cmd.options.get("hashbucketnum"))
                            .or_else(|| cmd.options.get("format.hash_bucket_num"))
                            .or_else(|| cmd.options.get("hash_bucket_num"))
                            .cloned()
                            .unwrap_or(String::from("4")),
                    )
                },
                cdc_change_column: cdc_column,
                use_cdc,
                vector_index_columns,
                text_index_columns,
                file_format,
                ..Default::default()
            })
            .unwrap(),
            partitions: format_table_info_partitions(&range_partitions, &primary_keys),
            table_path: if cmd.locations.is_empty() {
                format!(
                    "file://{}/{}/{}",
                    env::current_dir().unwrap().to_str().unwrap(),
                    cmd.name.schema().unwrap_or("default"),
                    cmd.name.table()
                )
            } else {
                // hdfs is not checked
                qualify_path(cmd.locations.first().ok_or_else(|| {
                    DataFusionError::Internal("missing location".to_string())
                })?)?
            },
            domain: "public".to_string(),
        });
        // The object-store options captured on the session (`fs.s3a.*`, …)
        // must reach the table's io config: stores built outside the session
        // runtime — the vector search scan's native reader and the vector
        // index auto-build after a commit — construct them from this map.
        let object_store_options =
            LakeSoulProviderOptions::from_session(session_state).object_store_options;
        let io_config = create_io_config_builder_from_table_info(
            table_info.clone(),
            cmd.options.clone(),
            object_store_options,
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
        let vector_index_configs =
            crate::vector_index::parse_vector_index_from_table_properties(
                &table_info.properties,
            )
            .unwrap_or_default();
        let text_index_configs =
            crate::text_index::parse_text_index_from_table_properties(
                &table_info.properties,
            )
            .unwrap_or_default();
        Ok(Self {
            listing_options: ListingOptions::new(Arc::new(LakeSoulMetaDataFormat::new(
                client.clone(),
                table_info.clone(),
                io_config.clone(),
                format_registry.clone(),
            )?)),
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
            vector_index_configs,
            text_index_configs,
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

    /// Lists the work units that survive `partition_filters`.
    ///
    /// The caller hands in predicates that classify as
    /// [`FilterRole::partition_pruning`]: they are evaluated on the partition
    /// values, so a work unit whose values cannot match is left out before
    /// any file is opened. Row-level pushdown is a separate decision
    /// ([`ClassifiedFilters::pre_merge_pushdown`]); a predicate reaching this
    /// function is not thereby allowed below the merge.
    pub(crate) async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a SessionState,
        partition_filters: &'a [Expr],
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
        let prune_partition_info = prune_partitions(
            all_partition_info,
            partition_filters,
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
                .map(|object_meta| {
                    PartitionedFile::new_from_meta(object_meta)
                        .with_partition_values(partition_values.clone())
                })
                .collect::<Vec<_>>();
            file_groups.push(files)
        }
        debug!("file_groups: {:#?}", file_groups);

        Ok((file_groups, Statistics::new_unknown(self.schema().deref())))
    }

    /// Build the vector-index candidate scan for a marker request.
    ///
    /// Returns `Ok(None)` when the request cannot be served — no primary
    /// key, no `vector_index_columns` table property declaring the searched
    /// column, an unsupported metric, a non-vector column, or an empty
    /// scan — in which case the caller runs the regular full scan instead.
    async fn try_build_vector_search_exec(
        &self,
        session_state: &SessionState,
        request: crate::udf::vector_search_marker::VectorSearchRequest,
        partition_filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if self.primary_keys.is_empty() {
            return Ok(None);
        }
        if !matches!(request.metric.as_str(), "L2" | "IP") {
            return Ok(None);
        }
        // The table must declare the searched column in its
        // `vector_index_columns` property (and with a compatible metric).
        let Some(declared) = self
            .vector_index_configs
            .iter()
            .find(|c| c.column == request.vec_column)
        else {
            return Ok(None);
        };
        if declared.params.metric.to_uppercase().as_str() != request.metric {
            return Ok(None);
        }
        let vec_field = match self.file_schema.field_with_name(&request.vec_column) {
            Ok(field) => field,
            Err(_) => {
                return Ok(None);
            }
        };
        if !is_vector_type(vec_field.data_type()) {
            return Ok(None);
        }
        let (partitioned_file_lists, _) = self
            .list_files_for_scan(session_state, partition_filters, limit)
            .await
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
        if partitioned_file_lists.is_empty() {
            return Ok(None);
        }

        let mut groups = Vec::with_capacity(partitioned_file_lists.len());
        let mut partition_values = Vec::with_capacity(partitioned_file_lists.len());
        for files in &partitioned_file_lists {
            // The native reader searches the index shard of *one* hash
            // bucket per reader, so split each partition's files by bucket.
            let mut by_bucket: std::collections::BTreeMap<u32, Vec<PartitionedFile>> =
                std::collections::BTreeMap::new();
            for file in files {
                let bucket = lakesoul_io::helpers::extract_hash_bucket_id(
                    file.object_meta.location.as_ref(),
                )
                .unwrap_or(0);
                by_bucket.entry(bucket).or_default().push(file.clone());
            }
            for bucket_files in by_bucket.into_values() {
                let values = bucket_files
                    .first()
                    .map(|f| f.partition_values.clone())
                    .unwrap_or_default();
                groups.push(bucket_files);
                partition_values.push(values);
            }
        }

        let exec = crate::datasource::file_format::LakeSoulVectorSearchExec::try_new(
            self.scan_schema.clone(),
            self.file_schema.clone(),
            self.table_partition_cols().to_vec(),
            groups,
            partition_values,
            self.table_paths()[0].object_store(),
            self.primary_keys.clone(),
            self.io_config.object_store_options().clone(),
            self.io_config.cdc_column(),
            request,
            self.client.vector_index_catalog(),
        )?;
        Ok(Some(Arc::new(exec)))
    }

    /// Build the text-index candidate scan for a marker request.
    ///
    /// Returns `Ok(None)` when the request cannot be served — no primary
    /// key, no `text_index_columns` entry for the column, a non-text column,
    /// or an empty scan — in which case the caller runs the regular full
    /// scan and the exact `text_match` predicate filters it.
    async fn try_build_text_search_exec(
        &self,
        session_state: &SessionState,
        request: crate::udf::text_search_marker::TextSearchRequest,
        partition_filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if self.primary_keys.is_empty() {
            return Ok(None);
        }
        let Some(declared) = self
            .text_index_configs
            .iter()
            .find(|c| c.column == request.column)
        else {
            return Ok(None);
        };
        let Ok(field) = self.file_schema.field_with_name(&request.column) else {
            return Ok(None);
        };
        if !matches!(
            field.data_type(),
            arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::LargeUtf8
                | arrow::datatypes::DataType::Utf8View
        ) {
            return Ok(None);
        }
        let (partitioned_file_lists, _) = self
            .list_files_for_scan(session_state, partition_filters, limit)
            .await
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?;
        if partitioned_file_lists.is_empty() {
            return Ok(None);
        }

        let mut groups = Vec::with_capacity(partitioned_file_lists.len());
        let mut partition_values = Vec::with_capacity(partitioned_file_lists.len());
        for files in &partitioned_file_lists {
            let mut by_bucket: std::collections::BTreeMap<u32, Vec<PartitionedFile>> =
                std::collections::BTreeMap::new();
            for file in files {
                let bucket = lakesoul_io::helpers::extract_hash_bucket_id(
                    file.object_meta.location.as_ref(),
                )
                .unwrap_or(0);
                by_bucket.entry(bucket).or_default().push(file.clone());
            }
            for bucket_files in by_bucket.into_values() {
                let values = bucket_files
                    .first()
                    .map(|f| f.partition_values.clone())
                    .unwrap_or_default();
                groups.push(bucket_files);
                partition_values.push(values);
            }
        }

        let catalog = self.client.index_catalog::<lakesoul_text::TextSplitEntry>(
            lakesoul_common::IndexKind::Text,
        );
        let exec = crate::datasource::file_format::LakeSoulTextSearchExec::try_new(
            self.scan_schema.clone(),
            self.file_schema.clone(),
            self.table_partition_cols().to_vec(),
            groups,
            partition_values,
            self.table_paths()[0].object_store(),
            self.primary_keys.clone(),
            self.io_config.object_store_options().clone(),
            self.io_config.cdc_column(),
            declared.to_text_index_config(),
            request,
            catalog,
        )?;
        Ok(Some(Arc::new(exec)))
    }

    /// The file format a work unit's files are read with.
    ///
    /// `format_scan_groups` derived it from the first file's extension when it
    /// built the work unit's per-file scan configs, and a work unit holds files
    /// of one format group.
    fn work_unit_format(
        &self,
        configs: &[FileScanConfig],
    ) -> DFResult<Option<Arc<dyn FileFormat>>> {
        let Some(path) = configs
            .first()
            .and_then(|config| config.file_groups.first())
            .and_then(|group| group.files().first())
            .map(|file| file.object_meta.location.as_ref())
        else {
            return Ok(None);
        };
        Ok(Some(
            self.format_registry.file_format(
                self.format_registry
                    .physical_format_for_path(path)
                    .map_err(|report| {
                        DataFusionError::External(report.into_boxed_error())
                    })?,
            ),
        ))
    }

    fn format_scan_groups(
        format_registry: &LakeSoulFormatRegistry,
        object_store_url: ObjectStoreUrl,
        partitioned_file_lists: Vec<Vec<PartitionedFile>>,
    ) -> Result<Vec<FormatScanGroup>> {
        let mut groups: Vec<FormatScanGroup> = vec![];

        for partitioned_files in partitioned_file_lists {
            let mut current_format = None;
            let mut current_files = vec![];

            for file in partitioned_files {
                let physical_format = format_registry
                    .physical_format_for_path(file.object_meta.location.as_ref())?;

                match current_format {
                    Some(format) if format == physical_format => {
                        current_files.push(file);
                    }
                    Some(format) => {
                        Self::push_format_scan_group(
                            &mut groups,
                            object_store_url.clone(),
                            format,
                            std::mem::take(&mut current_files),
                        );
                        current_format = Some(physical_format);
                        current_files.push(file);
                    }
                    None => {
                        current_format = Some(physical_format);
                        current_files.push(file);
                    }
                }
            }

            if let Some(physical_format) = current_format {
                Self::push_format_scan_group(
                    &mut groups,
                    object_store_url.clone(),
                    physical_format,
                    current_files,
                );
            }
        }

        Ok(groups)
    }

    fn push_format_scan_group(
        groups: &mut Vec<FormatScanGroup>,
        object_store_url: ObjectStoreUrl,
        physical_format: PhysicalFormat,
        files: Vec<PartitionedFile>,
    ) {
        if files.is_empty() {
            return;
        }

        if let Some(group) = groups.last_mut().filter(|group| {
            group.object_store_url == object_store_url
                && group.physical_format == physical_format
        }) {
            group.partitioned_file_lists.push(files);
        } else {
            groups.push(FormatScanGroup {
                object_store_url,
                physical_format,
                partitioned_file_lists: vec![files],
            });
        }
    }

    fn build_partitioned_exec(
        partitioned_execs: Vec<Arc<dyn ExecutionPlan>>,
        empty_schema: SchemaRef,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match partitioned_execs.len() {
            0 => Ok(Arc::new(EmptyExec::new(empty_schema))),
            1 => Ok(partitioned_execs[0].clone()),
            _ => UnionExec::try_new(partitioned_execs),
        }
    }

    fn empty_partitioned_exec_schema(
        _scan_schema: SchemaRef,
        merged_schema: SchemaRef,
    ) -> SchemaRef {
        merged_schema
    }

    fn merged_schema_with_input_nullability(
        merged_schema: SchemaRef,
        inputs: &[Arc<dyn ExecutionPlan>],
        partition_columns: &[String],
    ) -> SchemaRef {
        Arc::new(Schema::new(
            merged_schema
                .fields()
                .iter()
                .map(|field| {
                    // The logical schema is the contract for nullability:
                    // DataFusion's physical planner rejects scan outputs whose
                    // fields are *more* nullable than the logical schema, so a
                    // file-level nullable inference (parquet always infers
                    // nullable) must not widen a logically non-null column.
                    // Widen only when a non-partition column is genuinely
                    // absent from some input (schema evolution), where merged
                    // rows can be null; partition columns are projected
                    // constants supplied by the merge exec.
                    let missing_in_some_input = !partition_columns.contains(field.name())
                        && inputs.iter().any(|input| {
                            input.schema().column_with_name(field.name()).is_none()
                        });
                    Field::new(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable() || missing_in_some_input,
                    )
                })
                .collect::<Vec<_>>(),
        ))
    }
}

/// What one predicate may be used for when scanning a LakeSoul table.
///
/// The roles are independent by design. A predicate can be safe for metadata
/// pruning while being unsafe to evaluate per file, and a predicate can be
/// safe to push below the merge while being useless for pruning:
///
/// - `partition_pruning`: every referenced column is a range-partition
///   column, so the predicate can be evaluated exactly on the partition
///   values and whole work units whose values cannot match can be dropped
///   before any file is opened. This does *not* imply the predicate may be
///   evaluated below the merge, and it does not depend on the table having
///   primary keys or on row-level pushdown being enabled.
/// - `pre_merge_pushdown`: the predicate is row-invariant across every
///   version of a merge key (primary-key columns) or the table has no merge
///   at all, *and* row-level/`FileSource` pushdown is enabled. Only such
///   predicates may be forwarded to `FileSource::try_pushdown_filters`.
///   `pushdown_filters` gates this role; it never gates `partition_pruning`.
/// - `pk_candidates`: the predicate references only primary-key columns and
///   may feed the primary-key candidate / row-locator path. Like pruning,
///   this is independent of row-level pushdown.
///
/// A predicate with none of the roles stays above the scan; for a
/// merge-on-read table it is evaluated by the `FilterExec` DataFusion keeps
/// because `supports_filters_pushdown` never returns `Exact`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FilterRole {
    partition_pruning: bool,
    pre_merge_pushdown: bool,
    pk_candidates: bool,
}

/// Whether every column referenced by `expr` is one of `columns`.
///
/// A predicate without columns (a constant) is not classified: it cannot
/// prune partitions and carries no version semantics, so DataFusion's own
/// `FilterExec` handles it.
fn columns_within(expr: &Expr, columns: &[String]) -> bool {
    let refs = expr.column_refs();
    !refs.is_empty() && refs.iter().all(|col| columns.contains(&col.name))
}

/// Classifies one predicate against the table's key layout and the
/// row-level pushdown option.
///
/// Callers pass the *conjuncts* of their filter list: DataFusion splits
/// top-level `AND` predicates before asking `supports_filters_pushdown`, and
/// `LakeSoulTableProvider::scan` splits them again so that a direct caller of
/// the trait method gets the same independent treatment (`part = 0 AND v > 3`
/// must prune by `part` while leaving `v > 3` above the merge).
fn classify_filter(
    pushdown_filters: bool,
    primary_keys: &[String],
    range_partitions: &[String],
    expr: &Expr,
) -> FilterRole {
    let range_only = columns_within(expr, range_partitions);
    let pk_only = columns_within(expr, primary_keys);
    FilterRole {
        // Metadata pruning is independent of the primary keys and of the
        // row-level pushdown option.
        partition_pruning: range_only,
        // Safety: without a merge the per-file scans are concatenated, so
        // every predicate is safe below the scan; with a merge only
        // primary-key predicates are row-invariant across versions. Policy:
        // row-level/FileSource pushdown must also be enabled. A range
        // predicate may drop whole work units, but it is deliberately *not*
        // pushed into the per-file scans — "safe to prune" and "safe to
        // evaluate on one version of a row" are different questions.
        pre_merge_pushdown: pushdown_filters && (primary_keys.is_empty() || pk_only),
        // The row locator is a separate optimization from FileSource
        // pushdown, so it is not gated by `pushdown_filters`.
        pk_candidates: pk_only,
    }
}

/// A filter list split into its independent uses for one scan.
///
/// The predicates not selected for any role remain for DataFusion's own
/// `FilterExec` above the scan; `LakeSoulTableProvider::scan` never forwards
/// the complete filter list to a `FileSource`.
#[derive(Debug, Default)]
struct ClassifiedFilters {
    /// Predicates that may be evaluated on range-partition values to drop
    /// whole work units before any file is opened.
    partition_pruning: Vec<Expr>,
    /// Predicates that may be forwarded to `FileSource::try_pushdown_filters`
    /// (already gated by `pushdown_filters`): row-invariant predicates below
    /// `MergeParquetExec`, or every predicate when there is no merge.
    pre_merge_pushdown: Vec<Expr>,
    /// Predicates that may feed the primary-key candidate / row locator.
    pk_candidates: Vec<Expr>,
}

impl ClassifiedFilters {
    /// Splits `filters` into conjuncts and classifies each one on its own, so
    /// a conjunction never forces one predicate's safety onto another.
    fn classify(
        pushdown_filters: bool,
        filters: &[Expr],
        primary_keys: &[String],
        range_partitions: &[String],
    ) -> Self {
        let mut classified = Self::default();
        for expr in filters.iter().flat_map(|filter| split_conjunction(filter)) {
            let role =
                classify_filter(pushdown_filters, primary_keys, range_partitions, expr);
            if role.partition_pruning {
                classified.partition_pruning.push(expr.clone());
            }
            if role.pre_merge_pushdown {
                classified.pre_merge_pushdown.push(expr.clone());
            }
            if role.pk_candidates {
                classified.pk_candidates.push(expr.clone());
            }
        }
        classified
    }
}

/// The DataFusion verdict for one predicate.
///
/// `Inexact` predicates are added to `TableScan.filters` (so `scan` can use
/// them for pruning, the row locator, or an enabled row-level pushdown) while
/// DataFusion keeps its own `FilterExec` for correctness. `Unsupported`
/// predicates never reach `scan`; DataFusion evaluates them above the scan.
/// Nothing is ever `Exact`: the file-level filters are best-effort, so
/// DataFusion must always re-check.
fn filter_pushdown_verdict(
    pushdown_filters: bool,
    primary_keys: &[String],
    range_partitions: &[String],
    expr: &Expr,
) -> TableProviderFilterPushDown {
    let role = classify_filter(pushdown_filters, primary_keys, range_partitions, expr);
    // Anything `scan` can use must reach it, including range predicates used
    // only for metadata pruning and primary-key predicates used only by the
    // row locator. DataFusion does not pass `Unsupported` predicates to
    // `TableProvider::scan`.
    if role.partition_pruning || role.pre_merge_pushdown || role.pk_candidates {
        TableProviderFilterPushDown::Inexact
    } else {
        TableProviderFilterPushDown::Unsupported
    }
}

#[async_trait]
impl TableProvider for LakeSoulTableProvider {
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

        // Both index pushdowns are optimizations: they only need the
        // partition-pruning half of the filters to enumerate candidate work
        // units, so they receive `classified.partition_pruning` below.
        let vector_search =
            crate::udf::vector_search_marker::parse_vector_search_request(filters);
        let filters: Vec<Expr> = filters
            .iter()
            .filter(|f| !crate::udf::vector_search_marker::is_marker_expr(f))
            .cloned()
            .collect();
        let text_search =
            crate::udf::text_search_marker::parse_text_search_request(&filters);
        let filters: Vec<Expr> = filters
            .iter()
            .filter(|f| !crate::udf::text_search_marker::is_marker_expr(f))
            .cloned()
            .collect();

        // Split the filters into their independent uses before touching the
        // metadata: range predicates prune whole work units, primary-key
        // predicates are safety-pushable below the merge (and feed the row
        // locator), and everything else stays for DataFusion's `FilterExec`
        // above the scan. See `FilterRole`.
        let classified = ClassifiedFilters::classify(
            self.pushdown_filters,
            &filters,
            &self.primary_keys,
            &self.range_partitions,
        );

        // Vector search pushdown: when the optimizer annotated the scan
        // with the vector-search marker, read only the index candidates
        // through the native reader.  The `Sort` + `Limit` above the scan
        // then compute the exact global top-k.
        if let Some(request) = vector_search
            && let Some(exec) = self
                .try_build_vector_search_exec(
                    session_state,
                    request,
                    &classified.partition_pruning,
                    limit,
                )
                .await?
        {
            return Ok(exec);
        }

        // Text search pushdown: the marker switches the scan to the text
        // index candidates; the rewritten `text_match` predicate above the
        // scan verifies them exactly.
        if let Some(request) = text_search
            && let Some(exec) = self
                .try_build_text_search_exec(
                    session_state,
                    request,
                    &classified.partition_pruning,
                    limit,
                )
                .await?
        {
            return Ok(exec);
        }

        // Finite primary-key predicates let the scan fetch only the matching
        // rows (vortex + integer pk only); the optimizer keeps re-applying
        // the filter above the scan, so a superset candidate set is safe.
        let pk_candidates = if self.primary_keys.len() == 1 {
            let pk = &self.primary_keys[0];
            self.file_schema.field_with_name(pk).ok().and_then(|field| {
                lakesoul_io::pk_locator::extract_pk_candidates(
                    &classified.pk_candidates,
                    pk,
                    field.data_type(),
                )
            })
        } else {
            None
        };

        let (partitioned_file_lists, statistics) = self
            .list_files_for_scan(session_state, &classified.partition_pruning, limit)
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

        // Only the pre-merge subset reaches the file sources, and only when
        // row-level pushdown is enabled (the classification already applied
        // the flag). A non-key predicate must never be evaluated on a single
        // version below the merge: filtering `v = 1` before the merge can
        // keep an old `(id=1, v=1)` file while the new `(id=1, v=100)`
        // version is dropped, resurrecting a superseded row. DataFusion's
        // upper `FilterExec` evaluates those predicates on the merged rows.
        //
        // The conjuncts are offered to the source one by one. A file source
        // judges every predicate on its own, and the common `part = .. AND
        // v = ..` shape mixes a partition column - which has no file-schema
        // entry - with file columns, so a conjunction judged as a whole is
        // refused outright and takes the file columns' pushdown with it.
        let pushed_filters: Vec<Arc<dyn PhysicalExpr>> =
            if classified.pre_merge_pushdown.is_empty() {
                Vec::new()
            } else {
                // Filters are evaluated before projection and may reference
                // partition columns or columns omitted from the output
                // projection.
                let table_df_schema =
                    table_schema.table_schema().as_ref().clone().to_dfschema()?;
                classified
                    .pre_merge_pushdown
                    .iter()
                    .map(|expr| {
                        create_physical_expr(
                            expr,
                            &table_df_schema,
                            session_state.execution_props(),
                            &PhysicalPlanningContext::default(),
                        )
                    })
                    .collect::<DFResult<Vec<_>>>()?
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
            let mut file_source = file_format.file_source(table_schema.clone());
            if !pushed_filters.is_empty() {
                let result = file_source.try_pushdown_filters(
                    pushed_filters.clone(),
                    session_state.config_options(),
                )?;
                if let Some(updated_source) = result.updated_node {
                    file_source = updated_source;
                }
            }

            let scan_config =
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

        let candidate_inputs = match pk_candidates {
            Some(candidates) if !candidates.is_empty() => {
                lakesoul_io::pk_locator::try_build_pk_inputs(
                    session_state,
                    &self.io_config,
                    &flatten_configs,
                    &candidates,
                )
                .await
            }
            _ => None,
        };

        let mut inputs_map: HashMap<
            String,
            (
                Arc<HashMap<String, String>>,
                (
                    Vec<Arc<dyn ExecutionPlan>>,
                    Vec<String>,
                    Vec<FileScanConfig>,
                ),
            ),
        > = HashMap::new();
        let mut all_inputs = Vec::<Arc<dyn ExecutionPlan>>::new();

        for (index, config) in flatten_configs.into_iter().enumerate() {
            let (partition_desc, partition_values) =
                partition_desc_from_file_scan_config(&config).map_err(|report| {
                    DataFusionError::External(report.into_boxed_error())
                })?;
            let partition_values = Arc::new(partition_values);
            let file_path = config.file_groups[0].files()[0].path().to_string();
            let input: Arc<dyn ExecutionPlan> = match &candidate_inputs {
                Some(inputs) => Arc::clone(&inputs[index]),
                None => DataSourceExec::from_data_source(config.clone()),
            };
            all_inputs.push(input.clone());
            if let Some((_, inputs)) = inputs_map.get_mut(&partition_desc) {
                inputs.0.push(input);
                inputs.1.push(file_path);
                inputs.2.push(config);
            } else {
                inputs_map.insert(
                    partition_desc,
                    (
                        partition_values,
                        (vec![input], vec![file_path], vec![config]),
                    ),
                );
            }
        }

        let merged_schema = Self::merged_schema_with_input_nullability(
            merged_schema,
            &all_inputs,
            &self.range_partitions,
        );

        let mut partitioned_execs = Vec::new();
        for (_, (partition_values, (inputs, file_paths, configs))) in inputs_map {
            // A work unit of a table without primary keys never merges
            // (`merge_stream` concatenates), so it is served by a plain scan
            // leaf: same rows, but the distributed planner can split it over
            // worker tasks, which a `MergeParquetExec` cannot allow. Only a
            // work unit whose files cannot be read by one leaf keeps the merge
            // operator.
            let leaf = if self.io_config.primary_keys_slice().is_empty() {
                match self.work_unit_format(&configs)? {
                    Some(format) => append_only_scan_exec(
                        session_state,
                        format.as_ref(),
                        &configs,
                        &merged_schema,
                    )
                    .map_err(|report| {
                        DataFusionError::External(report.into_boxed_error())
                    })?,
                    None => None,
                }
            } else {
                None
            };
            let work_unit_exec = match leaf {
                Some(exec) => exec,
                None => {
                    let mut io_config = self.io_config.clone();
                    io_config.set_files(file_paths);
                    Arc::new(
                        lakesoul_io::physical_plan::MergeParquetExec::new_with_inputs(
                            merged_schema.clone(),
                            inputs,
                            io_config,
                            partition_values,
                        )
                        .map_err(|report| {
                            DataFusionError::External(report.into_boxed_error())
                        })?,
                    ) as Arc<dyn ExecutionPlan>
                }
            };
            partitioned_execs.push(work_unit_exec);
        }

        let empty_exec_schema = Self::empty_partitioned_exec_schema(
            scan_schema.clone(),
            merged_schema.clone(),
        );
        let exec = Self::build_partitioned_exec(partitioned_execs, empty_exec_schema)?;

        let cdc_column = self.io_config.cdc_column();
        let exec = if !cdc_column.is_empty() {
            let dfschema = DFSchema::try_from(exec.schema().as_ref().clone())?;
            let cdc_filter = ident(cdc_column).not_eq(lit("delete"));
            let expr = create_physical_expr(
                &cdc_filter,
                &dfschema,
                session_state.execution_props(),
                &PhysicalPlanningContext::default(),
            )?;

            Arc::new(FilterExec::try_new(expr, exec)?) as Arc<dyn ExecutionPlan>
        } else {
            exec
        };

        if Self::needs_output_projection(&scan_schema, &merged_schema) {
            let mut projection_expr = vec![];
            for field in scan_schema.fields() {
                projection_expr.push((
                    datafusion::physical_expr::expressions::col(
                        field.name(),
                        exec.schema().as_ref(),
                    )?,
                    field.name().clone(),
                ));
            }
            Ok(Arc::new(ProjectionExec::try_new(projection_expr, exec)?))
        } else {
            Ok(exec)
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        info!("supports_filters_pushdown: {:?}", filters);
        Ok(filters
            .iter()
            .map(|filter| {
                filter_pushdown_verdict(
                    self.pushdown_filters,
                    &self.primary_keys,
                    &self.range_partitions,
                    filter,
                )
            })
            .collect())
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
            // The table's own format decides the sink's file extension; the
            // LakeSoul sink writes one file per input partition itself.
            // Actually, we don't need to set `file_extension` here, as the
            // LakeSoul sink will set it to the table's format extension.
            file_extension: self.options().format.get_ext(),
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

/// True for list-of-numbers types accepted by the vector index.
fn is_vector_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        }
        DataType::FixedSizeList(field, _) => {
            matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
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

    /// The filter roles are independent: a range predicate prunes partitions
    /// regardless of the primary keys and of the row-level pushdown flag; a
    /// primary-key predicate may feed the row locator and, when pushdown is
    /// enabled, reach the file source; a value predicate does neither on a
    /// merge-on-read table.
    #[test]
    fn filter_roles_separate_pruning_from_pre_merge_pushdown() {
        use datafusion::prelude::col;

        let primary_keys = vec![String::from("id")];
        let range_partitions = vec![String::from("part")];
        let pk_filter = col("id").eq(lit(1i32));
        let range_filter = col("part").eq(lit(0i32));
        let value_filter = col("v").gt(lit(3i32));

        // Merge-on-read table, row-level pushdown enabled.
        assert_eq!(
            classify_filter(true, &primary_keys, &range_partitions, &pk_filter),
            FilterRole {
                partition_pruning: false,
                pre_merge_pushdown: true,
                pk_candidates: true,
            }
        );
        assert_eq!(
            classify_filter(true, &primary_keys, &range_partitions, &range_filter),
            FilterRole {
                partition_pruning: true,
                pre_merge_pushdown: false,
                pk_candidates: false,
            }
        );
        assert_eq!(
            classify_filter(true, &primary_keys, &range_partitions, &value_filter),
            FilterRole::default(),
        );

        // Metadata pruning is independent of the row-level flag; the pk
        // locator is too, but pk row-level pushdown is not.
        assert_eq!(
            classify_filter(false, &primary_keys, &range_partitions, &range_filter),
            FilterRole {
                partition_pruning: true,
                pre_merge_pushdown: false,
                pk_candidates: false,
            }
        );
        assert_eq!(
            classify_filter(false, &primary_keys, &range_partitions, &pk_filter),
            FilterRole {
                partition_pruning: false,
                pre_merge_pushdown: false,
                pk_candidates: true,
            }
        );

        // Append-only table: every predicate is safe below the scan, but
        // only row-level pushdown enabled sends it there; pruning still
        // works with the flag off.
        let no_primary_keys: Vec<String> = vec![];
        assert_eq!(
            classify_filter(true, &no_primary_keys, &range_partitions, &value_filter),
            FilterRole {
                partition_pruning: false,
                pre_merge_pushdown: true,
                pk_candidates: false,
            }
        );
        assert_eq!(
            classify_filter(false, &no_primary_keys, &range_partitions, &value_filter),
            FilterRole::default(),
        );
        assert_eq!(
            classify_filter(false, &no_primary_keys, &range_partitions, &range_filter),
            FilterRole {
                partition_pruning: true,
                pre_merge_pushdown: false,
                pk_candidates: false,
            }
        );

        // A conjunction must not bind the safe predicate to the unsafe one:
        // `part = 0` keeps pruning while `v > 3` stays out of the file source
        // even with row-level pushdown disabled.
        let classified = ClassifiedFilters::classify(
            false,
            &[range_filter.and(value_filter)],
            &primary_keys,
            &range_partitions,
        );
        assert_eq!(
            classified.partition_pruning,
            vec![col("part").eq(lit(0i32))]
        );
        assert!(classified.pre_merge_pushdown.is_empty());
        assert!(classified.pk_candidates.is_empty());
    }

    /// The DataFusion verdict lets every predicate `scan` can use reach it —
    /// pruning, the pk locator, or an enabled row-level pushdown — and keeps
    /// the rest above, independently of the table shape. It never returns
    /// `Exact`: file-level filters are best-effort, so DataFusion must always
    /// keep its own `FilterExec`.
    #[test]
    fn filter_pushdown_verdict_never_returns_exact() {
        use datafusion::prelude::col;

        let primary_keys = vec![String::from("id")];
        let range_partitions = vec![String::from("part")];
        let pk_filter = col("id").eq(lit(1i32));
        let range_filter = col("part").eq(lit(0i32));
        let value_filter = col("v").gt(lit(3i32));

        // Merge-on-read table: the value predicate stays above in both
        // modes; pk and range predicates reach `scan` in both modes.
        for pushdown_filters in [true, false] {
            let verdict = |expr: &Expr| {
                filter_pushdown_verdict(
                    pushdown_filters,
                    &primary_keys,
                    &range_partitions,
                    expr,
                )
            };
            assert_eq!(verdict(&pk_filter), TableProviderFilterPushDown::Inexact);
            assert_eq!(verdict(&range_filter), TableProviderFilterPushDown::Inexact);
            assert_eq!(
                verdict(&value_filter),
                TableProviderFilterPushDown::Unsupported
            );
        }

        // Append-only table: range predicates reach `scan` for metadata
        // pruning in both modes; ordinary predicates only when row-level
        // pushdown is enabled.
        let no_primary_keys: Vec<String> = vec![];
        assert_eq!(
            filter_pushdown_verdict(
                true,
                &no_primary_keys,
                &range_partitions,
                &range_filter
            ),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            filter_pushdown_verdict(
                false,
                &no_primary_keys,
                &range_partitions,
                &range_filter
            ),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            filter_pushdown_verdict(
                true,
                &no_primary_keys,
                &range_partitions,
                &value_filter
            ),
            TableProviderFilterPushDown::Inexact
        );
        assert_eq!(
            filter_pushdown_verdict(
                false,
                &no_primary_keys,
                &range_partitions,
                &value_filter
            ),
            TableProviderFilterPushDown::Unsupported
        );
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

    #[test]
    fn format_scan_groups_preserve_non_adjacent_format_order() {
        let registry =
            LakeSoulFormatRegistry::new(LakeSoulIOConfig::default(), false).unwrap();
        let object_store_url = ObjectStoreUrl::parse("file://").unwrap();
        let groups = LakeSoulTableProvider::format_scan_groups(
            &registry,
            object_store_url,
            vec![vec![
                PartitionedFile::from(object_meta("part-000.parquet")),
                PartitionedFile::from(object_meta("part-001.vortex")),
                PartitionedFile::from(object_meta("part-002.parquet")),
            ]],
        )
        .unwrap();

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].physical_format, PhysicalFormat::Parquet);
        assert_eq!(groups[1].physical_format, PhysicalFormat::Vortex);
        assert_eq!(groups[2].physical_format, PhysicalFormat::Parquet);
        assert_eq!(
            groups[2].partitioned_file_lists[0][0]
                .object_meta
                .location
                .as_ref(),
            "part-002.parquet"
        );
    }

    #[test]
    fn build_partitioned_exec_returns_empty_exec_for_empty_partitions() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));

        let exec = LakeSoulTableProvider::build_partitioned_exec(vec![], schema.clone())
            .unwrap();

        assert!(exec.is::<EmptyExec>());
        assert_eq!(exec.schema(), schema);
    }

    #[test]
    fn build_empty_partitioned_exec_preserves_cdc_column_for_filtering() {
        let scan_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let merged_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("op", DataType::Utf8, true),
        ]));

        let empty_schema = LakeSoulTableProvider::empty_partitioned_exec_schema(
            scan_schema,
            merged_schema.clone(),
        );
        let exec =
            LakeSoulTableProvider::build_partitioned_exec(vec![], empty_schema).unwrap();

        assert!(exec.schema().field_with_name("op").is_ok());
        assert_eq!(exec.schema(), merged_schema);
    }

    #[test]
    fn partitioned_merge_execs_share_nullable_schema_after_evolution() {
        let merged_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("evolved", DataType::Utf8, false),
        ]));
        let old_file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let new_file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("evolved", DataType::Utf8, false),
        ]));
        let old_input =
            Arc::new(EmptyExec::new(old_file_schema)) as Arc<dyn ExecutionPlan>;
        let new_input =
            Arc::new(EmptyExec::new(new_file_schema)) as Arc<dyn ExecutionPlan>;
        let merged_schema = LakeSoulTableProvider::merged_schema_with_input_nullability(
            merged_schema,
            &[old_input.clone(), new_input.clone()],
            &[],
        );

        let old_partition = Arc::new(
            lakesoul_io::physical_plan::MergeParquetExec::new_with_inputs(
                merged_schema.clone(),
                vec![old_input],
                LakeSoulIOConfig::default(),
                Arc::new(HashMap::new()),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;
        let new_partition = Arc::new(
            lakesoul_io::physical_plan::MergeParquetExec::new_with_inputs(
                merged_schema,
                vec![new_input],
                LakeSoulIOConfig::default(),
                Arc::new(HashMap::new()),
            )
            .unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        assert!(
            old_partition
                .schema()
                .field_with_name("evolved")
                .unwrap()
                .is_nullable()
        );
        assert!(
            new_partition
                .schema()
                .field_with_name("evolved")
                .unwrap()
                .is_nullable(),
            "all partition execs must advertise the globally nullable schema"
        );
        assert_eq!(old_partition.schema(), new_partition.schema());
    }
}
