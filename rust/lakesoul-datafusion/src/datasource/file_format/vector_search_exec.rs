// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Physical execution plan that reads the vector-index candidates for a
//! table.
//!
//! The plan is produced by [`LakeSoulTableProvider::scan`] when the logical
//! plan carries the vector-search marker (see
//! [`VectorSearchPushdownRule`](crate::planner::vector_search_rule::VectorSearchPushdownRule)).
//! It reads each partition/bucket through the native [`LakeSoulReader`]
//! configured with the `vector_search_*` options, so the reader runs the
//! ANN search against that bucket's IVF+RaBitQ index and returns only the
//! candidate rows.  The `Sort` + `Limit` nodes above the scan then compute
//! the exact global top-k over the (small) candidate set.

use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    stream::RecordBatchStreamAdapter,
};
use object_store::path::Path as StorePath;
use rootcause::compat::boxed_error::IntoBoxedError;

use lakesoul_io::config::{
    LakeSoulIOConfig, LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_COLUMN,
    OPTION_KEY_VECTOR_SEARCH_METRIC, OPTION_KEY_VECTOR_SEARCH_NPROBE,
    OPTION_KEY_VECTOR_SEARCH_QUERY, OPTION_KEY_VECTOR_SEARCH_TOP_K,
};
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};

use crate::udf::vector_search_marker::{
    LakeSoulVectorSearchOptions, VectorSearchRequest,
};

/// Execution plan for the vector-index candidate scan.
#[derive(Debug)]
pub struct LakeSoulVectorSearchExec {
    /// Output schema: file columns followed by partition columns.
    schema: SchemaRef,
    /// File columns (partition columns excluded).
    file_schema: SchemaRef,
    /// (name, type) of the range partition columns.
    partition_cols: Vec<(String, DataType)>,
    /// One group of files (one partition/bucket) per partition of the scan.
    file_groups: Vec<Vec<PartitionedFile>>,
    /// Values of the partition columns for each file group (aligned with
    /// `partition_cols`).
    partition_values: Vec<Vec<ScalarValue>>,
    /// Object store URL used to reconstruct reader file URIs.
    object_store_url: ObjectStoreUrl,
    /// Primary key columns (the vector index returns primary key ids).
    primary_keys: Vec<String>,
    /// Object store configuration options (e.g. S3 credentials).
    object_store_options: HashMap<String, String>,
    /// Vector search parameters.
    vector_search: VectorSearchRequest,
    /// Runtime metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties.
    properties: Arc<PlanProperties>,
}

impl LakeSoulVectorSearchExec {
    /// Create a new vector-search scan plan.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: SchemaRef,
        file_schema: SchemaRef,
        partition_cols: Vec<(String, DataType)>,
        file_groups: Vec<Vec<PartitionedFile>>,
        partition_values: Vec<Vec<ScalarValue>>,
        object_store_url: ObjectStoreUrl,
        primary_keys: Vec<String>,
        object_store_options: HashMap<String, String>,
        vector_search: VectorSearchRequest,
    ) -> DFResult<Self> {
        Ok(Self {
            schema: Arc::clone(&schema),
            file_schema,
            partition_cols,
            file_groups,
            partition_values,
            object_store_url,
            primary_keys,
            object_store_options,
            vector_search,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }

    /// Build the native reader configuration for one file group.
    fn reader_config(
        &self,
        files: &[PartitionedFile],
        partition_values: &[ScalarValue],
        nprobe: usize,
    ) -> DFResult<LakeSoulIOConfig> {
        let file_uris = files
            .iter()
            .map(|f| file_uri(&self.object_store_url, &f.object_meta.location))
            .collect::<Vec<_>>();
        let first = file_uris.first().cloned().ok_or_else(|| {
            DataFusionError::Internal("empty vector-search file group".into())
        })?;

        let mut builder = LakeSoulIOConfigBuilder::default()
            .with_files(file_uris)
            .with_primary_keys(self.primary_keys.clone())
            .with_schema(Arc::clone(&self.file_schema))
            .with_prefix(derive_prefix(&first));

        if !self.partition_cols.is_empty() {
            let partition_schema = Arc::new(Schema::new(
                self.partition_cols
                    .iter()
                    .map(|(name, ty)| {
                        arrow::datatypes::Field::new(name, ty.clone(), true)
                    })
                    .collect::<Vec<_>>(),
            ));
            builder = builder.with_partition_schema(partition_schema);
            for ((name, _), value) in self.partition_cols.iter().zip(partition_values) {
                builder = builder
                    .with_default_column_value(name.clone(), scalar_to_string(value));
            }
        }

        let mut has_path_style_config = false;
        for (k, v) in &self.object_store_options {
            if k == "fs.s3a.path.style.access" {
                has_path_style_config = true;
            }
            builder = builder.with_object_store_option(k.clone(), v.clone());
        }
        if !has_path_style_config {
            builder = builder.with_object_store_option(
                "fs.s3a.path.style.access".to_string(),
                "true".to_string(),
            );
        }

        builder = builder
            .with_option(
                OPTION_KEY_VECTOR_SEARCH_COLUMN,
                self.vector_search.vec_column.clone(),
            )
            .with_option(
                OPTION_KEY_VECTOR_SEARCH_QUERY,
                self.vector_search.query_csv.clone(),
            )
            // Fetch extra candidates per bucket so the exact global sort
            // above has recall margin against the approximate within-shard
            // ranking; the physical `Sort` + `Limit` trims back to top_k.
            .with_option(
                OPTION_KEY_VECTOR_SEARCH_TOP_K,
                self.vector_search
                    .top_k
                    .saturating_mul(10)
                    .max(100)
                    .to_string(),
            )
            .with_option(OPTION_KEY_VECTOR_SEARCH_NPROBE, nprobe.to_string())
            .with_option(
                OPTION_KEY_VECTOR_SEARCH_METRIC,
                self.vector_search.metric.clone(),
            );
        Ok(builder.build())
    }
}

impl ExecutionPlanProperties for LakeSoulVectorSearchExec {
    fn output_partitioning(&self) -> &Partitioning {
        &Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties.equivalence_properties()
    }
}

impl ExecutionPlan for LakeSoulVectorSearchExec {
    fn name(&self) -> &str {
        "LakeSoulVectorSearchExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "LakeSoulVectorSearchExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LakeSoulVectorSearchExec only supports 1 partition, got {partition}"
            )));
        }
        let nprobe = context
            .session_config()
            .get_extension::<LakeSoulVectorSearchOptions>()
            .map(|o| o.nprobe)
            .unwrap_or(64);

        let mut configs = Vec::with_capacity(self.file_groups.len());
        for (group, values) in self.file_groups.iter().zip(&self.partition_values) {
            configs.push(self.reader_config(group, values, nprobe)?);
        }
        let schema = Arc::clone(&self.schema);

        // Read every bucket's candidates through the native reader.  The
        // reader's async API is not `Send`, so it is driven through the
        // blocking wrapper on the global runtime; the candidate set is
        // bounded by top_k × bucket_count and cheap to collect.
        let batches = tokio::task::block_in_place(|| {
            let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
            for config in configs {
                let reader = LakeSoulReader::new(config)
                    .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
                let mut sync_reader =
                    SyncSendableMutableLakeSoulReader::new_with_global_runtime(reader);
                sync_reader
                    .start_blocked()
                    .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
                while let Some(batch) = sync_reader.next_rb_blocked() {
                    batches.push(
                        batch.map_err(|e| {
                            DataFusionError::External(e.into_boxed_error())
                        })?,
                    );
                }
            }
            Ok::<_, DataFusionError>(batches)
        })?;
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream))
            as SendableRecordBatchStream)
    }
}

impl DisplayAs for LakeSoulVectorSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "LakeSoulVectorSearchExec(vec={}, top_k={}, metric={})",
            self.vector_search.vec_column,
            self.vector_search.top_k,
            self.vector_search.metric
        )
    }
}

/// Reconstruct a full file URI for the native reader from the object store
/// URL and the store-relative location.
fn file_uri(object_store_url: &ObjectStoreUrl, location: &StorePath) -> String {
    let url = object_store_url.to_string();
    if url.starts_with("file:") {
        format!("file:///{}", location.as_ref())
    } else {
        format!("{}/{}", url.trim_end_matches('/'), location.as_ref())
    }
}

/// Derive the store prefix (parent directory) from a file URI, preserving
/// the scheme and authority.
fn derive_prefix(first_file: &str) -> String {
    let (scheme, rest) = match first_file.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest),
        None => ("".to_string(), first_file),
    };
    let parent = std::path::Path::new(rest.trim_end_matches('/'))
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    if scheme.is_empty() {
        parent
    } else {
        format!("{scheme}{parent}")
    }
}

/// Render a partition value as the string form used by the native reader.
fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(Some(s)) => s.clone(),
        ScalarValue::Utf8View(Some(s)) => s.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Date32(Some(v)) => v.to_string(),
        _ => value.to_string(),
    }
}
