// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::HashMap;
use std::iter::zip;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaBuilder, SchemaRef};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::prelude::SessionContext;
use datafusion_common::{
    DFSchema, DataFusionError, Result, Statistics, ToDFSchema, config::TableOptions,
};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSource;
use datafusion_datasource::{ListingTableUrl, PartitionedFile, TableSchema};
use datafusion_datasource_parquet::ParquetFormat;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::{TaskContext, runtime_env::RuntimeEnv};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_session::Session;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use rootcause::{Report, report};
use url::Url;

use crate::config::LakeSoulIOConfig;
use crate::datasource::file_format::LakeSoulParquetFormat;
use crate::helpers::{get_file_object_meta, infer_schema};
use crate::transform::uniform_schema;
use crate::utils::random_str;

/// Creates a new session context
///
/// # Arguments
///
/// * `config` - A mutable reference to the LakeSoulIOConfig instance
///
/// # Returns
///
/// A new SessionContext instance
#[deprecated(since = "3.1.0", note = "Use lakesoul session")]
pub fn create_session_context(
    config: &mut LakeSoulIOConfig,
) -> Result<SessionContext, Report> {
    create_session_context_with_planner(config, None)
}

/// Creates a new session context with a specific query planner
///
/// # Arguments
///
/// * `config` - A mutable reference to the LakeSoulIOConfig instance
/// * `planner` - An optional Arc<dyn QueryPlanner + Send + Sync> instance
///
/// # Returns
///
/// A new SessionContext instance
#[deprecated(since = "3.1.0", note = "Use lakesoul session")]
pub fn create_session_context_with_planner(
    config: &mut LakeSoulIOConfig,
    planner: Option<Arc<dyn QueryPlanner + Send + Sync>>,
) -> Result<SessionContext, Report> {
    let mut sess_conf = SessionConfig::default()
        .with_batch_size(config.batch_size)
        .with_parquet_pruning(true)
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(true);

    sess_conf
        .options_mut()
        .optimizer
        .enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
    sess_conf.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
    // sess_conf.options_mut().execution.parquet.pushdown_filters =
    //     config.parquet_filter_pushdown;
    sess_conf.options_mut().execution.target_partitions = 1;
    sess_conf.options_mut().execution.parquet.dictionary_enabled = Some(false);
    sess_conf
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;

    let mut runtime_conf = RuntimeEnvBuilder::new();
    if let Some(pool_size) = config.pool_size() {
        let memory_pool = FairSpillPool::new(pool_size);
        runtime_conf = runtime_conf.with_memory_pool(Arc::new(memory_pool));
    }
    let runtime = runtime_conf.build()?;

    // firstly, parse default fs if exist
    let default_fs = config
        .object_store_options
        .get("fs.defaultFS")
        .or_else(|| config.object_store_options.get("fs.default.name"))
        .cloned();
    if let Some(fs) = default_fs {
        config.default_fs = fs.clone();
        info!("NativeIO register default fs {}", fs);
        crate::object_store::register_object_store(&fs, config, &runtime)?;
    };

    if !config.prefix.is_empty() {
        let prefix = config.prefix.clone();
        info!("NativeIO register prefix fs {}", prefix);
        let normalized_prefix =
            crate::object_store::register_object_store(&prefix, config, &runtime)?;
        config.prefix = normalized_prefix;
    } else if let Ok(warehouse_prefix) = std::env::var("LAKESOUL_WAREHOUSE_PREFIX") {
        info!("NativeIO register warehouse prefix {}", warehouse_prefix);
        let normalized_prefix = crate::object_store::register_object_store(
            &warehouse_prefix,
            config,
            &runtime,
        )?;
        config.prefix = normalized_prefix;
    }
    debug!("{}", &config.prefix);

    // register object store(s) for input/output files' path
    // and replace file names with default fs concatenated if exist
    let files = config.files.clone();
    let normalized_filenames = files
        .into_iter()
        .map(|file_name| {
            crate::object_store::register_object_store(&file_name, config, &runtime)
        })
        .collect::<Result<Vec<String>, Report>>()?;
    config.files = normalized_filenames;
    info!("NativeIO normalized file names: {:?}", config.files);
    info!("NativeIO final config: {:?}", config);

    let builder = SessionStateBuilder::new()
        .with_config(sess_conf)
        .with_runtime_env(Arc::new(runtime));
    let builder = if let Some(planner) = planner {
        builder.with_query_planner(planner)
    } else {
        builder
    };
    // create session context
    // only keep projection/filter rules as others are unnecessary
    let state = builder
        .with_analyzer_rules(vec![Arc::new(TypeCoercion {})])
        .with_optimizer_rules(vec![
            Arc::new(PushDownFilter {}),
            Arc::new(OptimizeProjections {}),
            Arc::new(SimplifyExpressions {}),
        ])
        .with_physical_optimizer_rules(vec![Arc::new(ProjectionPushdown {})])
        .build();

    Ok(SessionContext::new_with_state(state))
}
pub struct LakeSoulIOSession {
    // mutable part
    io_config: LakeSoulIOConfig,
    execution_props: ExecutionProps,
    inner: Arc<IOSessionInner>,
}

/// Immutable part of `LakeSoulIOSession`
struct IOSessionInner {
    pub session_id: String,
    pub session_config: SessionConfig,
    pub runtime_env: Arc<RuntimeEnv>,
}

impl LakeSoulIOSession {
    pub fn try_new(mut config: LakeSoulIOConfig) -> Result<Self, Report> {
        let mut sess_conf = SessionConfig::default()
            .with_batch_size(config.batch_size)
            .with_parquet_pruning(true)
            .with_information_schema(true)
            .with_create_default_catalog_and_schema(true);
        sess_conf
            .options_mut()
            .optimizer
            .enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
        sess_conf.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
        sess_conf.options_mut().execution.target_partitions = 1;
        sess_conf.options_mut().execution.parquet.dictionary_enabled = Some(false);
        sess_conf
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = false;
        let mut runtime_conf = RuntimeEnvBuilder::new();
        if let Some(pool_size) = config.pool_size() {
            let memory_pool = FairSpillPool::new(pool_size);
            runtime_conf = runtime_conf.with_memory_pool(Arc::new(memory_pool));
        }
        let runtime = runtime_conf.build()?;
        // firstly, parse default fs if exist
        let default_fs = config
            .object_store_options
            .get("fs.defaultFS")
            .or_else(|| config.object_store_options.get("fs.default.name"))
            .cloned();
        if let Some(fs) = default_fs {
            config.default_fs = fs.clone();
            info!("NativeIO register default fs {}", fs);
            crate::object_store::register_object_store(&fs, &mut config, &runtime)?;
        };

        if !config.prefix.is_empty() {
            let prefix = config.prefix.clone();
            info!("NativeIO register prefix fs {}", prefix);
            let normalized_prefix = crate::object_store::register_object_store(
                &prefix,
                &mut config,
                &runtime,
            )?;
            config.prefix = normalized_prefix;
        } else if let Ok(warehouse_prefix) = std::env::var("LAKESOUL_WAREHOUSE_PREFIX") {
            info!("NativeIO register warehouse prefix {}", warehouse_prefix);
            let normalized_prefix = crate::object_store::register_object_store(
                &warehouse_prefix,
                &mut config,
                &runtime,
            )?;
            config.prefix = normalized_prefix;
        }
        debug!("{}", &config.prefix);

        // register object store(s) for input/output files' path
        // and replace file names with default fs concatenated if exist
        let files = config.files.clone();
        let normalized_filenames = files
            .into_iter()
            .map(|file_name| {
                crate::object_store::register_object_store(
                    &file_name,
                    &mut config,
                    &runtime,
                )
            })
            .collect::<Result<Vec<String>, Report>>()?;
        config.files = normalized_filenames;
        info!("NativeIO normalized file names: {:?}", config.files);
        info!("NativeIO final config\n{:#?}", config);
        let inner = IOSessionInner {
            session_id: random_str(8),
            session_config: sess_conf,
            runtime_env: Arc::new(runtime),
        };
        Ok(Self {
            io_config: config,
            inner: Arc::new(inner),
            execution_props: ExecutionProps::new(),
        })
    }
    pub fn io_config(&self) -> &LakeSoulIOConfig {
        &self.io_config
    }

    pub fn io_config_mut(&mut self) -> &mut LakeSoulIOConfig {
        &mut self.io_config
    }

    pub fn execution_props_mut(&mut self) -> &mut ExecutionProps {
        &mut self.execution_props
    }

    pub async fn get_table_schema(&self) -> Result<TableSchema, Report> {
        let table_paths = self
            .io_config
            .files
            .iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<_>>>()?;
        let object_metas = get_file_object_meta(self.task_ctx(), &table_paths).await?;
        let (listing_table_paths, object_metas): (Vec<_>, Vec<_>) =
            zip(table_paths, object_metas)
                .filter(|(_, obj_meta)| {
                    let valid = obj_meta.size >= 8;
                    if !valid {
                        error!(
                            "File {}, size {}, is invalid",
                            obj_meta.location, obj_meta.size
                        );
                    }
                    valid
                })
                .unzip();

        if listing_table_paths.is_empty() {
            warn!("No valid files found");
            return Ok(TableSchema::from_file_schema(Arc::new(Schema::empty())));
        }
        let file_format = Arc::new(LakeSoulParquetFormat::new(
            Arc::new(
                ParquetFormat::new().with_force_view_types(
                    self.config_options()
                        .execution
                        .parquet
                        .schema_force_view_types,
                ),
            ),
            self.io_config().clone(),
        ));
        // Resolve the schema (all files schema)
        let file_schema = infer_schema(
            self,
            &listing_table_paths,
            &object_metas,
            file_format.clone(),
        )
        .await?;

        self.compute_table_schema(file_schema)
    }

    /// Return projected table schema
    pub fn compute_table_schema(
        &self,
        file_schema: SchemaRef,
    ) -> Result<TableSchema, Report> {
        let target_schema = if self.io_config.inferring_schema {
            SchemaRef::new(Schema::empty())
        } else {
            uniform_schema(self.io_config.target_schema())
        };

        let mut builder = SchemaBuilder::from(target_schema.fields());
        // O(n^2), n is the number of fields in the file_schema
        for field in file_schema.fields() {
            if target_schema.field_with_name(field.name()).is_err() {
                // in file schema but not in target schema
                builder.try_merge(field)?;
            }
        }

        let table_partition_cols = self
            .io_config
            .range_partitions
            .iter()
            .map(|col| {
                Ok(
                    Arc::new(target_schema.field_with_name(col)?.clone()), // in schema
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let file_schema = Arc::new(builder.finish());
        Ok(TableSchema::new(file_schema, table_partition_cols))
    }

    pub async fn build_physical_plan(
        &mut self,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>, Report> {
        let table_paths = self
            .io_config
            .files
            .iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<_>>>()?;
        let object_metas = get_file_object_meta(self.task_ctx(), &table_paths).await?;
        let (listing_table_paths, object_metas): (Vec<_>, Vec<_>) =
            zip(table_paths, object_metas)
                .filter(|(_, obj_meta)| {
                    let valid = obj_meta.size >= 8;
                    if !valid {
                        error!(
                            "File {}, size {}, is invalid",
                            obj_meta.location, obj_meta.size
                        );
                    }
                    valid
                })
                .unzip();

        let object_store_url = if let Some(url) = listing_table_paths.first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };
        let file_format = Arc::new(LakeSoulParquetFormat::new(
            Arc::new(
                ParquetFormat::new().with_force_view_types(
                    self.config_options()
                        .execution
                        .parquet
                        .schema_force_view_types,
                ),
            ),
            self.io_config().clone(),
        ));
        // Resolve the schema (all files schema)
        let file_schema = infer_schema(
            self,
            &listing_table_paths,
            &object_metas,
            file_format.clone(),
        )
        .await?;

        let table_schema = self.compute_table_schema(file_schema)?;

        let statistics = Statistics::new_unknown(table_schema.table_schema());

        let store = self.runtime_env().object_store(&object_store_url)?;

        let partition_files: Result<Vec<PartitionedFile>, Report> =
            futures::stream::iter(&listing_table_paths)
                .map(|url| {
                    let store = store.clone();
                    async move {
                        Ok(PartitionedFile::from(
                            store
                                .head(&Path::from_url_path(
                                    <ListingTableUrl as AsRef<Url>>::as_ref(url).path(),
                                )?)
                                .await?,
                        ))
                    }
                })
                .boxed()
                .buffered(self.config_options().execution.meta_fetch_concurrency)
                .try_collect()
                .await;

        let source = file_format.file_source();
        let mut scan_config = FileScanConfigBuilder::new(
            object_store_url,
            table_schema.file_schema().clone(), // this is weird
            source,
        )
        .with_file_groups(vec![
            FileGroup::new(partition_files?)
                .with_statistics(Arc::new(statistics.clone())),
        ])
        .with_file_compression_type(FileCompressionType::ZSTD)
        .with_newlines_in_values(false)
        .with_statistics(statistics)
        .with_table_partition_cols(
            table_schema
                .table_partition_cols()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect(),
        ) // this is weird
        .build();

        if let Some(expr) = conjunction(filters) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = table_schema.table_schema().clone().to_dfschema()?;
            let filter = datafusion_physical_expr::create_physical_expr(
                &expr,
                &table_df_schema,
                self.execution_props(),
            )?;
            let res =
                scan_config.try_pushdown_filters(vec![filter], self.config_options())?;
            match res.updated_node {
                Some(sc) => {
                    debug!("apply new scan config");
                    debug!("filters: {:?}", res.filters);
                    scan_config = sc
                        .as_any()
                        .downcast_ref::<FileScanConfig>()
                        .ok_or(report!("Failed to downcast FileScanConfig"))?
                        .clone();
                }
                None => {
                    debug!("no updated node")
                }
            }
        }

        let exec = file_format.create_physical_plan(self, scan_config).await?;
        Ok(exec)
    }

    /// create a new session with a new io config
    /// io_config must valid
    pub fn with_io_config(&self, io_config: LakeSoulIOConfig) -> Self {
        Self {
            io_config,
            execution_props: self.execution_props.clone(),
            inner: self.inner.clone(),
        }
    }
}

#[async_trait::async_trait]
impl Session for LakeSoulIOSession {
    fn session_id(&self) -> &str {
        &self.inner.session_id
    }

    fn config(&self) -> &SessionConfig {
        &self.inner.session_config
    }

    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "lakesoul session unsupported".into(),
        ))
    }
    fn create_physical_expr(
        &self,
        _expr: Expr,
        _df_schema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Err(DataFusionError::NotImplemented(
            "lakesoul session unsupported".into(),
        ))
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        unimplemented!("lakesoul session unsupported")
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        unimplemented!("lakesoul session unsupported")
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        unimplemented!("lakesoul session unsupported")
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.inner.runtime_env
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_options(&self) -> &TableOptions {
        unimplemented!("lakesoul session unsupported")
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        unimplemented!("lakesoul session unsupported")
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        Arc::new(TaskContext::from(self))
    }
}

impl From<&LakeSoulIOSession> for TaskContext {
    fn from(value: &LakeSoulIOSession) -> Self {
        TaskContext::new(
            None,
            value.session_id().to_string(),
            value.config().clone(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Arc::clone(value.runtime_env()),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::config::LakeSoulIOConfigBuilder;

    use super::*;

    #[test]
    fn test_path_normalize() {
        let conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![
                "file:///some/absolute/local/file1",
                "/some/absolute/local/file2",
            ])
            .build();
        let session = LakeSoulIOSession::try_new(conf).unwrap();
        let io_config = session.io_config();
        assert_eq!(
            io_config.files,
            vec![
                "file:///some/absolute/local/file1".to_string(),
                "file:///some/absolute/local/file2".to_string(),
            ]
        );
        assert_eq!(io_config.max_file_size, None);
        assert_eq!(io_config.max_row_group_size, 250000);
        assert_eq!(io_config.max_row_group_num_values, 2147483647);
        assert_eq!(io_config.prefetch_size, 1);
        assert_eq!(io_config.parquet_filter_pushdown, false);
    }
}
