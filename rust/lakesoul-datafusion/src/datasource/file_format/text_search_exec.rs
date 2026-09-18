// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Physical execution plan that reads text-index candidates for a table.
//!
//! Produced by [`LakeSoulTableProvider::scan`] when the logical plan carries
//! the text-search marker (see
//! [`TextSearchPushdownRule`](crate::planner::text_search_rule::TextSearchPushdownRule)).
//! Each partition/bucket is read through the native [`LakeSoulReader`]
//! configured with the `text_search_*` options, so the reader searches that
//! bucket's Tantivy splits and returns candidate rows.  The exact
//! `text_match` predicate above the scan removes stale candidates, and the
//! `Limit` trims the result.

use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    stream::RecordBatchStreamAdapter,
};
use rootcause::compat::boxed_error::IntoBoxedError;

use lakesoul_common::IndexKind;
use lakesoul_io::config::{
    LakeSoulIOConfig, LakeSoulIOConfigBuilder, OPTION_KEY_FILE_FILTER_PUSHDOWN,
    OPTION_KEY_TEXT_SEARCH_COLUMN, OPTION_KEY_TEXT_SEARCH_QUERY,
    OPTION_KEY_TEXT_SEARCH_TOP_K, OPTION_KEY_TEXT_SEARCH_VERIFY,
};
use lakesoul_io::index::IndexLease;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_metadata::index_catalog::IndexCatalog;
use lakesoul_text::{TextIndexConfig, TextSplitEntry};

use super::vector_search_exec::{
    derive_prefix, file_uri, lease_owner, lease_ttl, scalar_to_string,
};
use crate::udf::text_search_marker::TextSearchRequest;

/// Execution plan for the text-index candidate scan.
#[derive(Debug)]
pub struct LakeSoulTextSearchExec {
    /// Output schema: file columns followed by partition columns.
    schema: SchemaRef,
    /// File columns (partition columns excluded).
    file_schema: SchemaRef,
    /// (name, type) of the range partition columns.
    partition_cols: Vec<(String, DataType)>,
    /// One group of files (one partition/bucket) per partition of the scan.
    file_groups: Vec<Vec<PartitionedFile>>,
    /// Values of the partition columns for each file group.
    partition_values: Vec<Vec<ScalarValue>>,
    /// Object store URL used to reconstruct reader file URIs.
    object_store_url: ObjectStoreUrl,
    /// Primary key columns (the text index returns primary key ids).
    primary_keys: Vec<String>,
    /// Object store configuration options (e.g. S3 credentials).
    object_store_options: HashMap<String, String>,
    /// CDC change column; when set, delete tombstones are dropped after the
    /// merge-on-read merge.
    cdc_column: String,
    /// Analyzer configuration recovered from the table property.
    config: TextIndexConfig,
    /// Text search parameters.
    text_search: TextSearchRequest,
    /// Catalog used to resolve index commits and hold reader leases.
    catalog: IndexCatalog<TextSplitEntry>,
    /// Runtime metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties.
    properties: Arc<PlanProperties>,
}

impl LakeSoulTextSearchExec {
    /// Create a new text-search scan plan.
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
        cdc_column: String,
        config: TextIndexConfig,
        text_search: TextSearchRequest,
        catalog: IndexCatalog<TextSplitEntry>,
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
            cdc_column,
            config,
            text_search,
            catalog,
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
    async fn reader_config(
        &self,
        store: &Arc<dyn object_store::ObjectStore>,
        files: &[PartitionedFile],
        partition_values: &[ScalarValue],
    ) -> DFResult<LakeSoulIOConfig> {
        let file_uris = files
            .iter()
            .map(|f| file_uri(&self.object_store_url, &f.object_meta.location))
            .collect::<Vec<_>>();
        let first = file_uris.first().cloned().ok_or_else(|| {
            DataFusionError::Internal("empty text-search file group".into())
        })?;

        let table_prefix = derive_prefix(&first);
        let index_prefixes = lakesoul_io::index::prefix::derive_index_prefixes(
            &file_uris,
            &table_prefix,
            IndexKind::Text,
            &self.config.column_name,
        );
        let mut resolved_shards = Vec::with_capacity(index_prefixes.len());
        let mut leases = Vec::with_capacity(index_prefixes.len());
        for (index_prefix, _bucket) in index_prefixes {
            let view =
                self.catalog
                    .resolve_cached(&index_prefix)
                    .await
                    .map_err(|error| {
                        DataFusionError::External(
                            rootcause::report!(
                                "failed to resolve text index at '{}': {}",
                                index_prefix,
                                error
                            )
                            .into_boxed_error(),
                        )
                    })?;
            let Some(view) = view else {
                continue;
            };
            if !lakesoul_io::index::cache::is_loaded(
                store,
                IndexKind::Text,
                index_prefix.trim_end_matches('/'),
                view.commit_id,
            )
            .await
            {
                let lease = self
                    .catalog
                    .acquire_lease(&index_prefix, lease_ttl(), &lease_owner())
                    .await
                    .map_err(|error| {
                        DataFusionError::External(
                            rootcause::report!(
                                "failed to lease text index at '{}': {}",
                                index_prefix,
                                error
                            )
                            .into_boxed_error(),
                        )
                    })?;
                if let Some(handle) = lease {
                    leases.push(Arc::new(IndexLease::new(handle)));
                }
            }
            let segments = serde_json::to_value(&view.segments).map_err(|error| {
                DataFusionError::External(
                    rootcause::report!(
                        "failed to serialize text index splits: {}",
                        error
                    )
                    .into_boxed_error(),
                )
            })?;
            resolved_shards.push(ResolvedIndex {
                kind: IndexKind::Text,
                index_prefix,
                commit_id: view.commit_id,
                generation: view.generation,
                version: view.version,
                header: view.header,
                segments,
            });
        }

        let mut builder = LakeSoulIOConfigBuilder::default()
            .with_files(file_uris)
            .with_primary_keys(self.primary_keys.clone())
            .with_schema(Arc::clone(&self.file_schema))
            .with_prefix(derive_prefix(&first))
            .with_option(OPTION_KEY_FILE_FILTER_PUSHDOWN, "true");
        if !self.cdc_column.is_empty() {
            builder = builder.with_option(
                lakesoul_io::config::OPTION_KEY_CDC_COLUMN,
                self.cdc_column.clone(),
            );
        }

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
            .with_resolved_index_shards(resolved_shards)
            .with_index_leases(leases)
            .with_option(
                OPTION_KEY_TEXT_SEARCH_COLUMN,
                self.text_search.column.clone(),
            )
            .with_option(OPTION_KEY_TEXT_SEARCH_QUERY, self.text_search.query.clone())
            // Fetch extra candidates so the exact predicate above and the
            // final LIMIT have margin against stale index entries.
            .with_option(
                OPTION_KEY_TEXT_SEARCH_TOP_K,
                self.text_search
                    .top_k
                    .saturating_mul(10)
                    .max(100)
                    .to_string(),
            )
            // The exact `text_match` predicate above the scan verifies, so
            // the reader's own pass is redundant here.
            .with_option(OPTION_KEY_TEXT_SEARCH_VERIFY, "false");
        Ok(builder.build())
    }
}

impl ExecutionPlanProperties for LakeSoulTextSearchExec {
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

impl ExecutionPlan for LakeSoulTextSearchExec {
    fn name(&self) -> &str {
        "LakeSoulTextSearchExec"
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "LakeSoulTextSearchExec has no children".to_string(),
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
                "LakeSoulTextSearchExec only supports 1 partition, got {partition}"
            )));
        }
        let store = context
            .runtime_env()
            .object_store(self.object_store_url.clone())
            .map_err(|error| {
                DataFusionError::External(
                    rootcause::report!("failed to get object store: {}", error)
                        .into_boxed_error(),
                )
            })?;
        let mut configs = Vec::with_capacity(self.file_groups.len());
        for (group, values) in self.file_groups.iter().zip(&self.partition_values) {
            let config = tokio::task::block_in_place(|| {
                lakesoul_io::session::GLOBAL_RUNTIME
                    .block_on(self.reader_config(&store, group, values))
            })?;
            configs.push(config);
        }
        let schema = Arc::clone(&self.schema);

        fn read_bucket(
            config: LakeSoulIOConfig,
        ) -> DFResult<Vec<arrow::record_batch::RecordBatch>> {
            let reader = LakeSoulReader::new(config)
                .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
            let mut sync_reader =
                SyncSendableMutableLakeSoulReader::new_with_global_runtime(reader);
            sync_reader
                .start_blocked()
                .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
            let mut batches = Vec::new();
            while let Some(batch) = sync_reader.next_rb_blocked() {
                let batch =
                    batch.map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
                batches.push(batch);
            }
            Ok(batches)
        }

        let batches = tokio::task::block_in_place(|| {
            if configs.len() == 1 {
                let config = configs.pop().expect("one config");
                return read_bucket(config);
            }
            std::thread::scope(|scope| {
                let handles: Vec<_> = configs
                    .into_iter()
                    .map(|config| scope.spawn(move || read_bucket(config)))
                    .collect();
                let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
                for handle in handles {
                    let mut bucket_batches = handle.join().map_err(|_| {
                        DataFusionError::Execution(
                            "text search bucket reader panicked".to_string(),
                        )
                    })??;
                    batches.append(&mut bucket_batches);
                }
                Ok::<_, DataFusionError>(batches)
            })
        })?;
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream))
            as SendableRecordBatchStream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for LakeSoulTextSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "LakeSoulTextSearchExec(column={}, query={:?}, top_k={})",
            self.text_search.column, self.text_search.query, self.text_search.top_k
        )
    }
}
