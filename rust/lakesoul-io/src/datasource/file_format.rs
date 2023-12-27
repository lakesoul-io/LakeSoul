// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, SchemaBuilder, SchemaRef};

use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use datafusion::execution::context::SessionState;

use datafusion::physical_expr::PhysicalSortRequirement;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_common::{project_schema, FileType, Result, Statistics};

use object_store::{ObjectMeta, ObjectStore};

use async_trait::async_trait;

use crate::datasource::physical_plan::MergeParquetExec;
use crate::lakesoul_io_config::LakeSoulIOConfig;

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

#[async_trait]
impl FileFormat for LakeSoulParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.parquet_format.infer_schema(state, store, objects).await
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
        let predicate = self
            .parquet_format
            .enable_pruning(state.config_options())
            .then(|| filters.cloned())
            .flatten();

        let file_schema = conf.file_schema.clone();
        let mut builder = SchemaBuilder::from(file_schema.fields());
        for field in &conf.table_partition_cols {
            builder.push(Field::new(field.name(), field.data_type().clone(), false));
        }
        let (summary_conf, flatten_conf) =
            flatten_file_scan_config(state, self.parquet_format.clone(), conf, self.conf.primary_keys_slice()).await?;
        let projection = summary_conf.projection.clone();
        let merge_schema = Arc::new(builder.finish());

        let merge_exec = Arc::new(MergeParquetExec::new(
            merge_schema.clone(),
            summary_conf,
            flatten_conf,
            predicate,
            self.parquet_format.metadata_size_hint(state.config_options()),
            self.conf.clone(),
        ));
        if let Some(projection) = projection {
            let mut projection_expr = vec![];
            for idx in projection {
                projection_expr.push((
                    datafusion::physical_expr::expressions::col(merge_schema.field(idx).name(), &merge_schema)?,
                    merge_schema.field(idx).name().clone(),
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
        order_requirements: Option<Vec<PhysicalSortRequirement>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.parquet_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_type(&self) -> FileType {
        FileType::PARQUET
    }
}

async fn flatten_file_scan_config(
    state: &SessionState,
    format: Arc<ParquetFormat>,
    conf: FileScanConfig,
    primary_keys: &[String],
) -> Result<(FileScanConfig, Vec<FileScanConfig>)> {
    let summary_conf = conf.clone();
    let object_store_url = conf.object_store_url.clone();
    let store = state.runtime_env().object_store(object_store_url.clone())?;
    let projected_schema = project_schema(&conf.file_schema.clone(), conf.projection.as_ref())?;

    let mut flatten_configs = vec![];
    for i in 0..conf.file_groups.len() {
        let files = &conf.file_groups[i];
        for file in files {
            let objects = &[file.object_meta.clone()];
            let file_groups = vec![vec![file.clone()]];
            let file_schema = format.infer_schema(state, &store, objects).await?;
            let statistics = format
                .infer_stats(state, &store, file_schema.clone(), &file.object_meta)
                .await?;
            let projection =
                compute_project_column_indices(file_schema.clone(), projected_schema.clone(), primary_keys);
            let limit = conf.limit;
            let table_partition_cols = vec![];
            let output_ordering = conf.output_ordering.clone();
            let infinite_source = conf.infinite_source;
            let config = FileScanConfig {
                object_store_url: object_store_url.clone(),
                file_schema,
                file_groups,
                statistics,
                projection,
                limit,
                table_partition_cols,
                output_ordering,
                infinite_source,
            };
            flatten_configs.push(config);
        }
    }
    Ok((summary_conf, flatten_configs))
}

fn compute_project_column_indices(
    schema: SchemaRef,
    project_schema: SchemaRef,
    primary_keys: &[String],
) -> Option<Vec<usize>> {
    Some(
        schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if project_schema.field_with_name(field.name()).is_ok() | primary_keys.contains(field.name()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>(),
    )
}
