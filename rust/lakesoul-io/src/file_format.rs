// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource::file_format] implementation of LakeSoul.

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use ::vortex::{VortexSessionDefault, session::VortexSession};
use arrow::datatypes::SchemaRef;
use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, FieldRef, Fields, Schema, SchemaBuilder};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, parquet::ParquetFormat};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
    ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::{
    DataFusionError, Statistics, error::Result as DFResult, project_schema,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::parquet_to_arrow_schema;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::{Report, bail, report};
use vortex_datafusion::{VortexFormat, VortexTableOptions};

use crate::Result;
use crate::config::LakeSoulIOConfig;
use crate::physical_plan::merge::MergeParquetExec;

pub(crate) mod vortex;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalFormat {
    Parquet,
    Vortex,
    /// A variant of `Vortex` (use zstd/pcodec)
    #[default]
    VortexCompact,
}

impl PhysicalFormat {
    pub fn name(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Vortex => "vortex",
            Self::VortexCompact => "vortex-compact",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Vortex | Self::VortexCompact => "vortex",
        }
    }

    pub fn from_extension(path: &str) -> Result<Self> {
        let path_without_query = path.split_once('?').map_or(path, |(path, _)| path);
        let extension = path_without_query
            .rsplit('/')
            .next()
            .and_then(|file_name| file_name.rsplit_once('.').map(|(_, ext)| ext))
            .ok_or(report!("No physical format found").attach(path.to_string()))?;
        Self::from_str(extension)
    }
}

impl Display for PhysicalFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for PhysicalFormat {
    type Err = Report;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "vortex" => Ok(Self::Vortex),
            "vortex-compact" => Ok(Self::VortexCompact),
            other => bail!("Unsupported physical format: {other}"),
        }
    }
}

#[derive(Debug)]
pub struct LakeSoulFormatRegistry {
    parquet: Arc<LakeSoulParquetFormat>,
    vortex: Arc<LakeSoulVortexFormat>,
}

impl LakeSoulFormatRegistry {
    pub fn new(
        io_config: LakeSoulIOConfig,
        parquet_force_view_types: bool,
    ) -> Result<Self> {
        let parquet = Arc::new(LakeSoulParquetFormat::new(
            Arc::new(
                ParquetFormat::new().with_force_view_types(parquet_force_view_types),
            ),
            io_config.clone(),
        ));
        let opts = {
            let mut opts = VortexTableOptions::default();
            opts.projection_pushdown = true;
            opts.predicate_pushdown = true;
            opts
        };

        let vortex = Arc::new(LakeSoulVortexFormat::new(
            Arc::new(VortexFormat::new_with_options(
                VortexSession::default(),
                opts,
            )),
            io_config,
        ));

        Ok(Self { parquet, vortex })
    }

    pub fn physical_format_for_path(&self, path: &str) -> Result<PhysicalFormat> {
        PhysicalFormat::from_extension(path)
    }

    pub fn file_format(&self, physical_format: PhysicalFormat) -> Arc<dyn FileFormat> {
        match physical_format {
            PhysicalFormat::Parquet => self.parquet.clone(),
            PhysicalFormat::Vortex | PhysicalFormat::VortexCompact => self.vortex.clone(),
        }
    }

    pub fn file_compression_type(
        &self,
        physical_format: PhysicalFormat,
    ) -> FileCompressionType {
        match physical_format {
            PhysicalFormat::Parquet => FileCompressionType::ZSTD,
            PhysicalFormat::Vortex => FileCompressionType::UNCOMPRESSED,
            PhysicalFormat::VortexCompact => FileCompressionType::ZSTD,
        }
    }
}

/// LakeSoul `FileFormat` implementation for supporting Apache Parquet
///
/// Note it is recommended these are instead configured on the [`ConfigOptions`]
/// associated with the [`SessionState`] instead of overridden on a format-basis
///
/// TODO: Deprecate and remove overrides
/// <https://github.com/apache/arrow-datafusion/issues/4349>
#[derive(Debug, Default)]
pub struct LakeSoulParquetFormat {
    parquet_format: Arc<ParquetFormat>,
    io_config: LakeSoulIOConfig, // TODO try Arc
}

impl LakeSoulParquetFormat {
    pub fn new(parquet_format: Arc<ParquetFormat>, conf: LakeSoulIOConfig) -> Self {
        Self {
            parquet_format,
            io_config: conf,
        }
    }

    pub fn primary_keys(&self) -> Arc<Vec<String>> {
        Arc::new(self.io_config.primary_keys.clone())
    }

    pub fn default_column_value(&self) -> Arc<HashMap<String, String>> {
        Arc::new(self.io_config.default_column_value.clone())
    }

    pub fn merge_operators(&self) -> Arc<HashMap<String, String>> {
        Arc::new(self.io_config.merge_operators.clone())
    }
}

#[async_trait]
impl FileFormat for LakeSoulParquetFormat {
    fn get_ext(&self) -> String {
        self.parquet_format.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        self.parquet_format
            .get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.parquet_format.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        let schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| {
                fetch_schema(
                    store.as_ref(),
                    object,
                    self.parquet_format.metadata_size_hint(),
                )
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        merge_schemas_with_cast(schemas)
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        self.parquet_format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        mut conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table_schema = Arc::clone(conf.file_source.table_schema().table_schema());

        // adapted file source with metadata size hint
        let mut parquet_source = conf
            .file_source
            .downcast_ref::<ParquetSource>()
            .ok_or(DataFusionError::Internal("file source".into()))?
            .clone();
        if let Some(metadata_size_hint) = self.parquet_format.metadata_size_hint() {
            parquet_source = parquet_source.with_metadata_size_hint(metadata_size_hint);
        }

        conf.file_source = Arc::new(parquet_source);

        // projection for Table Schema instead of File Schema
        let projection = conf.file_column_projection_indices();
        let target_schema = project_schema(&table_schema, projection.as_ref())?;

        // merge cdc and pks
        let merged_projection = compute_project_column_indices(
            table_schema.clone(),
            target_schema.clone(),
            self.io_config.primary_keys_slice(),
            &self.io_config.cdc_column(),
        );
        let merged_schema = project_schema(&table_schema, merged_projection.as_ref())?;

        // files to read
        let flatten_conf = flatten_file_scan_config(
            state,
            self.parquet_format.clone(),
            conf,
            self.io_config.primary_keys_slice(),
            &self.io_config.cdc_column(),
            self.io_config.partition_schema(),
            target_schema.clone(),
        )
        .await?;

        // merge on read files
        let merge_exec = Arc::new(
            MergeParquetExec::new(
                merged_schema.clone(),
                flatten_conf,
                self.io_config.clone(),
            )
            .map_err(|e| {
                error!("{e}");
                e.into_boxed_error()
            })?,
        );

        if target_schema.fields().len() < merged_schema.fields().len() {
            let mut projection_expr = vec![];
            for field in target_schema.fields() {
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

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.parquet_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.parquet_format.file_source(table_schema)
    }
}

/// LakeSoul `FileFormat` implementation for Vortex.
///
/// The upstream Vortex `infer_schema` uses Arrow's strict `Schema::try_merge`.
/// LakeSoul's Parquet path has historically allowed schemas whose fields can be
/// cast to each other (for example `Int32` and `Int64`). Keep the Vortex path
/// aligned with that behavior by inferring every file schema independently and
/// merging with `CanCastSchemaBuilder`.
#[derive(Debug, Clone)]
pub struct LakeSoulVortexFormat {
    vortex_format: Arc<VortexFormat>,
    io_config: LakeSoulIOConfig,
}

impl LakeSoulVortexFormat {
    pub fn new(vortex_format: Arc<VortexFormat>, io_config: LakeSoulIOConfig) -> Self {
        Self {
            vortex_format,
            io_config,
        }
    }
}

#[async_trait]
impl FileFormat for LakeSoulVortexFormat {
    fn get_ext(&self) -> String {
        self.vortex_format.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> DFResult<String> {
        self.vortex_format
            .get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.vortex_format.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> DFResult<SchemaRef> {
        let schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| {
                let vortex_format = self.vortex_format.clone();
                let store = store.clone();
                let object = object.clone();
                async move {
                    vortex_format
                        .infer_schema(state, &store, std::slice::from_ref(&object))
                        .await
                }
            })
            .boxed()
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        merge_schemas_with_cast(schemas.into_iter().map(|schema| schema.as_ref().clone()))
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> DFResult<Statistics> {
        self.vortex_format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table_schema = Arc::clone(conf.file_source.table_schema().table_schema());
        let projection = conf.file_column_projection_indices();
        let target_schema = project_schema(&table_schema, projection.as_ref())?;

        let merged_projection = compute_project_column_indices(
            table_schema.clone(),
            target_schema.clone(),
            self.io_config.primary_keys_slice(),
            &self.io_config.cdc_column(),
        );
        let merged_schema = project_schema(&table_schema, merged_projection.as_ref())?;

        let format: Arc<dyn FileFormat> = Arc::new(self.clone());
        let flatten_conf = flatten_file_scan_config_for_format(
            state,
            format,
            conf,
            self.io_config.primary_keys_slice(),
            &self.io_config.cdc_column(),
            self.io_config.partition_schema(),
            target_schema.clone(),
        )
        .await
        .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;

        let merge_exec = Arc::new(
            MergeParquetExec::new(
                merged_schema.clone(),
                flatten_conf,
                self.io_config.clone(),
            )
            .map_err(|e| {
                error!("{e}");
                e.into_boxed_error()
            })?,
        );

        if target_schema.fields().len() < merged_schema.fields().len() {
            let mut projection_expr = vec![];
            for field in target_schema.fields() {
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

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.vortex_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.vortex_format.file_source(table_schema)
    }
}

async fn fetch_schema(
    store: &dyn ObjectStore,
    obj_meta: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> DFResult<Schema> {
    // TODO add cryption
    let metadata = DFParquetMetadata::new(store, obj_meta)
        .with_metadata_size_hint(metadata_size_hint)
        .fetch_metadata()
        .await?;

    let file_metadata = metadata.file_metadata();
    let schema = parquet_to_arrow_schema(
        file_metadata.schema_descr(),
        file_metadata.key_value_metadata(),
    )?;
    Ok(schema)
}

fn clear_metadata(
    schemas: impl IntoIterator<Item = Schema>,
) -> impl Iterator<Item = Schema> {
    schemas.into_iter().map(|schema| {
        let fields = schema
            .fields()
            .iter()
            .map(|field| {
                field.as_ref().clone().with_metadata(Default::default()) // clear meta
            })
            .collect::<Fields>();
        Schema::new(fields)
    })
}

/// Merge file schemas using LakeSoul's schema evolution semantics.
///
/// Arrow's default `Schema::try_merge` requires the same field to have exactly
/// the same data type in every file. LakeSoul's Parquet path has historically
/// been more permissive and accepts fields whose types are castable by Arrow
/// (for example `Int32` and `Int64`). Reuse the same merge rule for all native
/// formats so Vortex and Parquet behave consistently during schema inference.
///
/// Field-level metadata is cleared before merging to avoid metadata-only
/// differences between files from causing schema conflicts. Schema metadata is
/// merged conservatively: the same key must have the same value in every file.
fn merge_schemas_with_cast(
    schemas: impl IntoIterator<Item = Schema>,
) -> DFResult<SchemaRef> {
    let mut out_meta = HashMap::new();
    let mut out_fields = CanCastSchemaBuilder::new();
    for schema in clear_metadata(schemas) {
        let Schema { metadata, fields } = schema;

        // merge metadata
        for (key, value) in metadata.into_iter() {
            if let Some(old_val) = out_meta.get(&key)
                && old_val != &value
            {
                return Err(DataFusionError::ArrowError(
                    Box::new(ArrowError::SchemaError(format!(
                        "Fail to merge schema due to conflicting metadata. \
                                     Key '{key}' has different values '{old_val}' and '{value}'"
                    ))),
                    None,
                ));
            }
            out_meta.insert(key, value);
        }

        // merge fields
        fields.iter().try_for_each(|x| out_fields.try_merge(x))?
    }
    Ok(Arc::new(out_fields.finish().with_metadata(out_meta)))
}

#[derive(Debug, Default)]
pub struct CanCastSchemaBuilder {
    fields: Vec<FieldRef>,
}

impl CanCastSchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, field: impl Into<FieldRef>) {
        self.fields.push(field.into())
    }

    fn merge(e: &mut FieldRef, field: &FieldRef) -> Result<(), ArrowError> {
        if can_cast_types(e.data_type(), field.data_type()) {
            *e = field.clone();
            Ok(())
        } else {
            Err(ArrowError::SchemaError(format!(
                "Fail to merge schema field '{}' because the from data_type = {} does not equal {}",
                field.name(),
                e.data_type(),
                field.data_type()
            )))
        }
    }

    pub fn try_merge(&mut self, field: &FieldRef) -> Result<(), ArrowError> {
        // This could potentially be sped up with a HashMap or similar
        let existing = self.fields.iter_mut().find(|f| f.name() == field.name());
        match existing {
            Some(e) => {
                Self::merge(e, field)?;
            }
            None => self.fields.push(field.clone()),
        }
        Ok(())
    }

    pub fn finish(self) -> Schema {
        Schema::new(self.fields)
    }
}

pub async fn flatten_file_scan_config(
    state: &dyn Session,
    format: Arc<ParquetFormat>,
    conf: FileScanConfig,
    primary_keys: &[String],
    cdc_column: &str,
    partition_schema: SchemaRef, // use less
    target_schema: SchemaRef,
) -> DFResult<Vec<FileScanConfig>> {
    debug!("partition schema: {}", partition_schema);
    // TODO remove clone
    let store = state.runtime_env().object_store(&conf.object_store_url)?;
    let file_groups = conf.file_groups.clone();
    let flatten_configs = futures::stream::iter(file_groups)
        .map(|files| {
            let store = store.clone();
            let format = format.clone();
            let partition_schema = partition_schema.clone();
            let target_schema = target_schema.clone();
            let conf = conf.clone();
            async move {
                let configs: Vec<FileScanConfig> = futures::stream::iter(files.files())
                    .map(|file| {
                        let store = store.clone();
                        let format = format.clone();
                        let partition_schema = partition_schema.clone();
                        let target_schema = target_schema.clone();
                        let conf = conf.clone();
                        async move {
                            let objects = std::slice::from_ref(&file.object_meta);
                            let files = vec![file.clone()];
                            // TODO use schema adapter rewriter?
                            let file_schema =
                                format.infer_schema(state, &store, objects).await?;
                            let file_schema = {
                                let mut builder = SchemaBuilder::new();
                                // O(nm), n = number of fields, m = number of partition columns
                                for field in file_schema.fields() {
                                    // in file not in partition
                                    if partition_schema
                                        .field_with_name(field.name())
                                        .is_err()
                                    {
                                        builder.push(field.clone());
                                    }
                                }
                                SchemaRef::new(builder.finish())
                            };
                            debug!("file schema:{}", file_schema);
                            let cols = conf.table_partition_cols().clone();
                            debug!("partition cols: {:?}", cols);
                            debug!("flatten: file_schema: {}", file_schema);
                            // only file schema
                            let table_schema =
                                TableSchema::new(file_schema.clone(), cols);
                            let file_statistics = format
                                .infer_stats(
                                    state,
                                    &store,
                                    file_schema.clone(),
                                    &file.object_meta,
                                )
                                .await?;
                            let statistics =
                                table_statistics(file_statistics, &table_schema);
                            let physical_target_schema = strip_partition_columns(
                                target_schema.clone(),
                                partition_schema.clone(),
                            );
                            let projection_indices = compute_project_column_indices(
                                table_schema.file_schema().clone(),
                                physical_target_schema,
                                primary_keys,
                                cdc_column,
                            );
                            let mut parquet_source = format
                                .file_source(table_schema)
                                .downcast_ref::<ParquetSource>()
                                .ok_or(DataFusionError::Internal("file source".into()))?
                                .clone();
                            if let Some(predicate) = conf.file_source.filter() {
                                parquet_source = parquet_source.with_predicate(predicate);
                            }
                            if let Some(metadata_size_hint) = format.metadata_size_hint()
                            {
                                parquet_source = parquet_source
                                    .with_metadata_size_hint(metadata_size_hint);
                            }
                            if let Some(reader_factory) = conf
                                .file_source
                                .downcast_ref::<ParquetSource>()
                                .and_then(|source| {
                                    source.parquet_file_reader_factory().cloned()
                                })
                            {
                                parquet_source = parquet_source
                                    .with_parquet_file_reader_factory(reader_factory);
                            }
                            let config = FileScanConfigBuilder::from(conf)
                                .with_source(Arc::new(parquet_source))
                                .with_file_groups(vec![
                                    FileGroup::new(files)
                                        .with_statistics(Arc::new(statistics.clone())),
                                ])
                                .with_statistics(statistics)
                                .with_projection_indices(projection_indices)?
                                .build();

                            Ok::<FileScanConfig, DataFusionError>(config)
                        }
                    })
                    .boxed()
                    .buffered(4)
                    .try_collect::<Vec<_>>()
                    .await?;
                Ok::<Vec<FileScanConfig>, DataFusionError>(configs)
            }
        })
        .boxed()
        .buffered(4)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(flatten_configs.into_iter().flatten().collect())
}

pub async fn flatten_file_scan_config_for_format(
    state: &dyn Session,
    format: Arc<dyn FileFormat>,
    conf: FileScanConfig,
    primary_keys: &[String],
    cdc_column: &str,
    partition_schema: SchemaRef,
    target_schema: SchemaRef,
) -> Result<Vec<FileScanConfig>> {
    debug!("partition schema: {}", partition_schema);
    let store = state.runtime_env().object_store(&conf.object_store_url)?;
    let file_groups = conf.file_groups.clone();
    let flatten_configs = futures::stream::iter(file_groups)
        .map(|files| {
            let store = store.clone();
            let format = format.clone();
            let partition_schema = partition_schema.clone();
            let target_schema = target_schema.clone();
            let conf = conf.clone();
            async move {
                let configs: Vec<FileScanConfig> = futures::stream::iter(files.files())
                    .map(|file| {
                        let store = store.clone();
                        let format = format.clone();
                        let partition_schema = partition_schema.clone();
                        let target_schema = target_schema.clone();
                        let conf = conf.clone();
                        async move {
                            let objects = std::slice::from_ref(&file.object_meta);
                            let files = vec![file.clone()];
                            let file_schema =
                                format.infer_schema(state, &store, objects).await?;
                            let file_schema = strip_partition_columns(
                                file_schema,
                                partition_schema.clone(),
                            );
                            debug!("file schema:{}", file_schema);
                            let cols = conf.table_partition_cols().clone();
                            debug!("partition cols: {:?}", cols);
                            debug!("flatten: file_schema: {}", file_schema);
                            let table_schema = TableSchema::new(file_schema, cols);
                            let file_statistics = format
                                .infer_stats(
                                    state,
                                    &store,
                                    table_schema.file_schema().clone(),
                                    &file.object_meta,
                                )
                                .await?;
                            let statistics =
                                table_statistics(file_statistics, &table_schema);
                            let physical_target_schema = strip_partition_columns(
                                target_schema.clone(),
                                partition_schema.clone(),
                            );
                            let projection_indices = compute_project_column_indices(
                                table_schema.file_schema().clone(),
                                physical_target_schema,
                                primary_keys,
                                cdc_column,
                            );
                            let mut source = format.file_source(table_schema);
                            source = adapt_file_source_for_single_file(
                                source, &format, &conf,
                            )?;
                            if let Some(predicate) = conf.file_source.filter() {
                                let result = source.try_pushdown_filters(
                                    vec![predicate],
                                    state.config_options(),
                                )?;
                                if let Some(updated_source) = result.updated_node {
                                    source = updated_source;
                                }
                            }
                            let config = FileScanConfigBuilder::from(conf)
                                .with_source(source)
                                .with_file_groups(vec![
                                    FileGroup::new(files)
                                        .with_statistics(Arc::new(statistics.clone())),
                                ])
                                .with_statistics(statistics)
                                .with_projection_indices(projection_indices)?
                                .build();

                            Ok::<FileScanConfig, DataFusionError>(config)
                        }
                    })
                    .boxed()
                    .buffered(4)
                    .try_collect::<Vec<_>>()
                    .await?;
                Ok::<Vec<FileScanConfig>, DataFusionError>(configs)
            }
        })
        .boxed()
        .buffered(4)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(flatten_configs.into_iter().flatten().collect())
}

pub fn merge_schema_refs(
    schemas: impl IntoIterator<Item = SchemaRef>,
) -> Result<SchemaRef> {
    let mut out_fields = CanCastSchemaBuilder::new();
    for schema in schemas {
        for field in schema.fields() {
            out_fields.try_merge(field)?;
        }
    }
    Ok(Arc::new(out_fields.finish()))
}

fn strip_partition_columns(
    file_schema: SchemaRef,
    partition_schema: SchemaRef,
) -> SchemaRef {
    let mut builder = SchemaBuilder::new();
    for field in file_schema.fields() {
        if partition_schema.field_with_name(field.name()).is_err() {
            builder.push(field.clone());
        }
    }
    SchemaRef::new(builder.finish())
}

fn table_statistics(
    file_statistics: Statistics,
    table_schema: &TableSchema,
) -> Statistics {
    let mut statistics = Statistics::new_unknown(table_schema.table_schema());
    statistics.num_rows = file_statistics.num_rows;
    statistics.total_byte_size = file_statistics.total_byte_size;
    for (idx, column_statistics) in
        file_statistics.column_statistics.into_iter().enumerate()
    {
        if idx < statistics.column_statistics.len() {
            statistics.column_statistics[idx] = column_statistics;
        }
    }
    statistics
}

fn adapt_file_source_for_single_file(
    source: Arc<dyn FileSource>,
    format: &Arc<dyn FileFormat>,
    conf: &FileScanConfig,
) -> DFResult<Arc<dyn FileSource>> {
    let Some(format) = format.downcast_ref::<LakeSoulParquetFormat>() else {
        return Ok(source);
    };

    let mut parquet_source = source
        .downcast_ref::<ParquetSource>()
        .ok_or(DataFusionError::Internal("file source".into()))?
        .clone();

    if let Some(metadata_size_hint) = format.parquet_format.metadata_size_hint() {
        parquet_source = parquet_source.with_metadata_size_hint(metadata_size_hint);
    }

    if let Some(reader_factory) = conf
        .file_source
        .downcast_ref::<ParquetSource>()
        .and_then(|source| source.parquet_file_reader_factory().cloned())
    {
        parquet_source = parquet_source.with_parquet_file_reader_factory(reader_factory);
    }

    Ok(Arc::new(parquet_source))
}

pub fn compute_project_column_indices(
    schema: SchemaRef,
    projected_schema: SchemaRef,
    primary_keys: &[String],
    cdc_column: &str,
) -> Option<Vec<usize>> {
    // O(nm), n = number of fields, m = number of projected columns
    Some(
        schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if projected_schema.field_with_name(field.name()).is_ok()
                    || primary_keys.contains(field.name()) // linear
                    || field.name().eq(&cdc_column)
                {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};

    #[test]
    fn merge_schema_refs_allows_castable_field_types() {
        let int32_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let int64_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));

        let merged = merge_schema_refs([int32_schema, int64_schema]).unwrap();

        assert_eq!(
            merged.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
    }
}
