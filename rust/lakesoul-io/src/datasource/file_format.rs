// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_cast::can_cast_types;
use arrow_schema::{ArrowError, FieldRef, Fields, Schema, SchemaBuilder};

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use datafusion::execution::context::SessionState;

use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_common::{project_schema, DataFusionError, Result, Statistics};

use object_store::{ObjectMeta, ObjectStore};

use crate::datasource::{listing::LakeSoulTableProvider, physical_plan::MergeParquetExec};
use crate::lakesoul_io_config::LakeSoulIOConfig;
use async_trait::async_trait;
use datafusion::datasource::file_format::parquet::fetch_parquet_metadata;
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
        Self { parquet_format, conf }
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

async fn fetch_schema(store: &dyn ObjectStore, file: &ObjectMeta, metadata_size_hint: Option<usize>) -> Result<Schema> {
    let metadata = fetch_parquet_metadata(store, file, metadata_size_hint).await?;
    let file_metadata = metadata.file_metadata();
    let schema = parquet_to_arrow_schema(file_metadata.schema_descr(), file_metadata.key_value_metadata())?;
    Ok(schema)
}

fn clear_metadata(schemas: impl IntoIterator<Item = Schema>) -> impl Iterator<Item = Schema> {
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

    fn get_ext_with_compression(&self, file_compression_type: &FileCompressionType) -> Result<String> {
        self.parquet_format.get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let schemas: Vec<_> = futures::stream::iter(objects)
            .map(|object| fetch_schema(store.as_ref(), object, self.parquet_format.metadata_size_hint()))
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
                if let Some(old_val) = out_meta.get(&key) {
                    if old_val != &value {
                        return Err(DataFusionError::ArrowError(
                            ArrowError::SchemaError(format!(
                                "Fail to merge schema due to conflicting metadata. \
                                         Key '{key}' has different values '{old_val}' and '{value}'"
                            )),
                            None,
                        ));
                    }
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
        state: &SessionState,
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
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        let predicate = self.parquet_format.enable_pruning().then(|| filters.cloned()).flatten();

        let table_schema = LakeSoulTableProvider::compute_table_schema(conf.file_schema.clone(), &self.conf)?;
        // projection for Table instead of File
        let projection = conf.projection.clone();
        let target_schema = project_schema(&table_schema, projection.as_ref())?;

        let merged_projection = compute_project_column_indices(
            table_schema.clone(),
            target_schema.clone(),
            self.conf.primary_keys_slice(),
        );
        let merged_schema = project_schema(&table_schema, merged_projection.as_ref())?;

        // files to read
        let flatten_conf = flatten_file_scan_config(
            state,
            self.parquet_format.clone(),
            conf,
            self.conf.primary_keys_slice(),
            self.conf.partition_schema(),
            target_schema.clone(),
        )
        .await?;

        // merge on read files
        let merge_exec = Arc::new(MergeParquetExec::new(
            merged_schema.clone(),
            flatten_conf,
            predicate,
            self.parquet_format.metadata_size_hint(),
            self.conf.clone(),
        )?);

        if target_schema.fields().len() < merged_schema.fields().len() {
            let mut projection_expr = vec![];
            for field in target_schema.fields() {
                projection_expr.push((
                    datafusion::physical_expr::expressions::col(field.name(), &merged_schema)?,
                    field.name().clone(),
                ));
            }
            Ok(Arc::new(ProjectionExec::try_new(projection_expr, merge_exec)?))
        } else {
            Ok(merge_exec)
        }
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.parquet_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }
}

pub async fn flatten_file_scan_config(
    state: &SessionState,
    format: Arc<ParquetFormat>,
    conf: FileScanConfig,
    primary_keys: &[String],
    partition_schema: SchemaRef,
    target_schema: SchemaRef,
) -> Result<Vec<FileScanConfig>> {
    let object_store_url = conf.object_store_url.clone();
    let store = state.runtime_env().object_store(object_store_url.clone())?;

    let mut flatten_configs = vec![];
    for i in 0..conf.file_groups.len() {
        let files = &conf.file_groups[i];
        for file in files {
            let objects = &[file.object_meta.clone()];
            let file_groups = vec![vec![file.clone()]];
            let file_schema = format.infer_schema(state, &store, objects).await?;
            let file_schema = {
                let mut builder = SchemaBuilder::new();
                // O(nm), n = number of fields, m = number of partition columns
                for field in file_schema.fields() {
                    if partition_schema.field_with_name(field.name()).is_err() {
                        builder.push(field.clone());
                    }
                }
                SchemaRef::new(builder.finish())
            };
            let statistics = format
                .infer_stats(state, &store, file_schema.clone(), &file.object_meta)
                .await?;
            let projection = compute_project_column_indices(file_schema.clone(), target_schema.clone(), primary_keys);
            let limit = conf.limit;
            let table_partition_cols = conf.table_partition_cols.clone();
            let output_ordering = conf.output_ordering.clone();
            let config = FileScanConfig {
                object_store_url: object_store_url.clone(),
                file_schema,
                file_groups,
                constraints: Default::default(),
                statistics,
                projection,
                limit,
                table_partition_cols,
                output_ordering,
            };
            flatten_configs.push(config);
        }
    }
    Ok(flatten_configs)
}

pub fn compute_project_column_indices(
    schema: SchemaRef,
    projected_schema: SchemaRef,
    primary_keys: &[String],
) -> Option<Vec<usize>> {
    // O(nm), n = number of fields, m = number of projected columns
    Some(
        schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if projected_schema.field_with_name(field.name()).is_ok() | primary_keys.contains(field.name()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    )
}
