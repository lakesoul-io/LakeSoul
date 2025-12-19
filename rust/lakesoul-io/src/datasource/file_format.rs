// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource::file_format] implementation of LakeSoul.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, FieldRef, Fields, Schema, SchemaBuilder};

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, parquet::ParquetFormat};
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
    ParquetSource,
};

use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::{DataFusionError, Result, Statistics, project_schema};

use object_store::{ObjectMeta, ObjectStore};

use crate::datasource::{
    listing::LakeSoulTableProvider, physical_plan::MergeParquetExec,
};
use crate::lakesoul_io_config::LakeSoulIOConfig;
use async_trait::async_trait;
use datafusion::catalog::Session;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::parquet_to_arrow_schema;

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
    conf: LakeSoulIOConfig,
}

impl LakeSoulParquetFormat {
    pub fn new(parquet_format: Arc<ParquetFormat>, conf: LakeSoulIOConfig) -> Self {
        Self {
            parquet_format,
            conf,
        }
    }

    pub fn primary_keys(&self) -> Arc<Vec<String>> {
        Arc::new(self.conf.primary_keys.clone())
    }

    pub fn default_column_value(&self) -> Arc<HashMap<String, String>> {
        Arc::new(self.conf.default_column_value.clone())
    }

    pub fn merge_operators(&self) -> Arc<HashMap<String, String>> {
        Arc::new(self.conf.merge_operators.clone())
    }
}

async fn fetch_schema(
    store: &dyn ObjectStore,
    obj_meta: &ObjectMeta,
    metadata_size_hint: Option<usize>,
) -> Result<Schema> {
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

#[async_trait]
impl FileFormat for LakeSoulParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.parquet_format.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.parquet_format
            .get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
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

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.parquet_format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        mut conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let table_schema = LakeSoulTableProvider::compute_table_schema(
            conf.table_schema.file_schema().clone(), // todo try table schema
            &self.conf,
        )?;

        // adapted file source with metadata size hint
        let mut parquet_source = conf
            .file_source
            .as_any()
            .downcast_ref::<ParquetSource>()
            .ok_or(DataFusionError::Internal("file source".into()))?
            .clone();
        if let Some(metadata_size_hint) = self.parquet_format.metadata_size_hint() {
            parquet_source = parquet_source.with_metadata_size_hint(metadata_size_hint);
        }

        conf.file_source = Arc::new(parquet_source);

        // projection for Table instead of File
        let projection = conf.file_column_projection_indices();
        let target_schema = project_schema(&table_schema, projection.as_ref())?;

        let merged_projection = compute_project_column_indices(
            table_schema.clone(),
            target_schema.clone(),
            self.conf.primary_keys_slice(),
            &self.conf.cdc_column(),
        );
        let merged_schema = project_schema(&table_schema, merged_projection.as_ref())?;

        // files to read
        let flatten_conf = flatten_file_scan_config(
            state,
            self.parquet_format.clone(),
            conf,
            self.conf.primary_keys_slice(),
            &self.conf.cdc_column(),
            self.conf.partition_schema(),
            target_schema.clone(),
        )
        .await?;

        // merge on read files
        let merge_exec = Arc::new(MergeParquetExec::new(
            merged_schema.clone(),
            flatten_conf,
            self.conf.clone(),
        )?);

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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.parquet_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        self.parquet_format
            .file_source()
            .with_statistics(Statistics::default())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.parquet_format.compression_type()
    }
}

pub async fn flatten_file_scan_config(
    state: &dyn Session,
    format: Arc<ParquetFormat>,
    conf: FileScanConfig,
    primary_keys: &[String],
    cdc_column: &str,
    partition_schema: SchemaRef,
    target_schema: SchemaRef,
) -> Result<Vec<FileScanConfig>> {
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
                            let file_schema = {
                                let mut builder = SchemaBuilder::new();
                                // O(nm), n = number of fields, m = number of partition columns
                                for field in file_schema.fields() {
                                    if partition_schema
                                        .field_with_name(field.name())
                                        .is_err()
                                    {
                                        builder.push(field.clone());
                                    }
                                }
                                SchemaRef::new(builder.finish())
                            };
                            let statistics = format
                                .infer_stats(
                                    state,
                                    &store,
                                    file_schema.clone(),
                                    &file.object_meta,
                                )
                                .await?;
                            let projection_indices = compute_project_column_indices(
                                file_schema.clone(),
                                target_schema.clone(),
                                primary_keys,
                                cdc_column,
                            );
                            let config = FileScanConfigBuilder::from(conf.clone())
                                .with_file_groups(vec![
                                    FileGroup::new(files)
                                        .with_statistics(Arc::new(statistics.clone())),
                                ])
                                .with_projection_indices(projection_indices)
                                .with_source(conf.file_source.with_statistics(statistics))
                                .build();
                            // flatten_configs.push(config);
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
                    || primary_keys.contains(field.name())
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
