// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource::listing] implementation of LakeSoul.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_schema::SchemaBuilder;
use async_trait::async_trait;

use crate::helpers::listing_table_from_lakesoul_io_config;
use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::transform::uniform_schema;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource,
};
use datafusion::datasource::source::DataSource;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::{datasource::TableProvider, logical_expr::Expr};
use datafusion_common::{DataFusionError, Result, Statistics, ToDFSchema};
use futures::future;
use object_store::path::Path;
use tracing::debug;
use url::Url;

pub struct LakeSoulTableProvider {
    listing_table: Arc<ListingTable>,
    lakesoul_io_config: LakeSoulIOConfig,
    table_schema: SchemaRef,
    listing_options: ListingOptions,
    listing_table_paths: Vec<ListingTableUrl>,
    file_source: Arc<dyn FileSource>,
}

impl Debug for LakeSoulTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulListingTable{..}").finish()
    }
}

impl LakeSoulTableProvider {
    pub async fn new_with_config_and_format(
        session_state: &SessionState,
        lakesoul_io_config: LakeSoulIOConfig,
        file_format: Arc<dyn FileFormat>,
        as_sink: bool,
    ) -> Result<Self> {
        let file_source = file_format.file_source();
        let (file_schema, listing_table) = listing_table_from_lakesoul_io_config(
            session_state,
            lakesoul_io_config.clone(),
            file_format,
            as_sink,
        )
        .await?;
        let file_schema = file_schema
            .ok_or_else(|| DataFusionError::Internal("No schema provided.".into()))?;
        let table_schema = Self::compute_table_schema(file_schema, &lakesoul_io_config)?;
        let listing_options = listing_table.options().clone();
        let listing_table_paths = listing_table.table_paths().clone();

        Ok(Self {
            listing_table,
            lakesoul_io_config,
            table_schema,
            listing_options,
            listing_table_paths,
            file_source,
        })
    }

    pub fn options(&self) -> &ListingOptions {
        self.listing_table.options()
    }

    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        self.listing_table.table_paths()
    }

    pub fn compute_table_schema(
        file_schema: SchemaRef,
        config: &LakeSoulIOConfig,
    ) -> Result<SchemaRef> {
        let target_schema = if config.inferring_schema {
            SchemaRef::new(Schema::empty())
        } else {
            uniform_schema(config.target_schema())
        };
        let mut builder = SchemaBuilder::from(target_schema.fields());
        // O(n^2), n is the number of fields in file_schema and config.partition_schema
        for field in file_schema.fields() {
            if target_schema.field_with_name(field.name()).is_err() {
                builder.try_merge(field)?;
            }
        }
        for field in config.partition_schema().fields() {
            if target_schema.field_with_name(field.name()).is_err() {
                builder.try_merge(field)?;
            }
        }
        Ok(Arc::new(builder.finish()))
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("listing scan start");
        let object_store_url = if let Some(url) = self.listing_table_paths.first() {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };
        let statistics = Statistics::new_unknown(&self.schema());
        let source = self.file_source.clone();

        let store = state.runtime_env().object_store(object_store_url.clone())?;
        let partition_files: Result<Vec<PartitionedFile>> =
            future::try_join_all(self.listing_table_paths.iter().map(|url| {
                let store = store.clone();
                async move {
                    Ok(PartitionedFile::from(
                        store
                            .head(&Path::from_url_path(
                                <ListingTableUrl as AsRef<Url>>::as_ref(url).path(),
                            )?)
                            .await
                            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?,
                    ))
                }
            }))
            .await;

        let mut scan_config =
            FileScanConfigBuilder::new(object_store_url, self.schema().clone(), source)
                .with_file_groups(vec![
                    FileGroup::new(partition_files?)
                        .with_statistics(Arc::new(statistics)),
                ])
                .with_projection_indices(projection.cloned())
                .with_limit(limit)
                .with_file_compression_type(FileCompressionType::ZSTD)
                .with_newlines_in_values(false)
                .build();

        if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
            let filter = datafusion::physical_expr::create_physical_expr(
                &expr,
                &table_df_schema,
                state.execution_props(),
            )?;
            let res =
                scan_config.try_pushdown_filters(vec![filter], state.config_options())?;
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

        self.listing_options
            .format
            .create_physical_plan(state, scan_config)
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.lakesoul_io_config.primary_keys.is_empty() {
            if self.lakesoul_io_config.parquet_filter_pushdown {
                Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
            } else {
                Ok(vec![
                    TableProviderFilterPushDown::Unsupported;
                    filters.len()
                ])
            }
        } else {
            // O(nml), n = number of filters, m = number of primary keys, l = number of columns
            filters
                .iter()
                .map(|f| {
                    let cols = f.column_refs();
                    if self.lakesoul_io_config.parquet_filter_pushdown
                        && cols.iter().all(|col| {
                            self.lakesoul_io_config.primary_keys.contains(&col.name)
                        })
                    {
                        // use primary key
                        Ok(TableProviderFilterPushDown::Inexact)
                    } else {
                        Ok(TableProviderFilterPushDown::Unsupported)
                    }
                })
                .collect()
        }
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table
            .insert_into(state, input, overwrite)
            .await
    }
}
