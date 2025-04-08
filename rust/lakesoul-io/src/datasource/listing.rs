// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource::listing] implementation of LakeSoul.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_schema::SchemaBuilder;
use async_trait::async_trait;

use arrow::datatypes::{Schema, SchemaRef};

use crate::helpers::listing_table_from_lakesoul_io_config;
use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::transform::uniform_schema;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{datasource::TableProvider, logical_expr::Expr};
use datafusion_common::DataFusionError::ObjectStore;
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
        let (file_schema, listing_table) =
            listing_table_from_lakesoul_io_config(session_state, lakesoul_io_config.clone(), file_format, as_sink)
                .await?;
        let file_schema = file_schema.ok_or_else(|| DataFusionError::Internal("No schema provided.".into()))?;
        let table_schema = Self::compute_table_schema(file_schema, &lakesoul_io_config)?;
        let listing_options = listing_table.options().clone();
        let listing_table_paths = listing_table.table_paths().clone();

        Ok(Self {
            listing_table,
            lakesoul_io_config,
            table_schema,
            listing_options,
            listing_table_paths,
        })
    }

    pub fn options(&self) -> &ListingOptions {
        self.listing_table.options()
    }

    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        self.listing_table.table_paths()
    }

    pub fn compute_table_schema(file_schema: SchemaRef, config: &LakeSoulIOConfig) -> Result<SchemaRef> {
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
        let object_store_url = if let Some(url) = self.listing_table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };
        let statistics = Statistics::new_unknown(&self.schema());
        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
            let filters =
                datafusion::physical_expr::create_physical_expr(&expr, &table_df_schema, state.execution_props())?;
            Some(filters)
        } else {
            None
        };
        let session_state = state.as_any().downcast_ref::<SessionState>().unwrap();
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
                            .map_err(ObjectStore)?,
                    ))
                }
            }))
            .await;
        self.listing_options
            .format
            .create_physical_plan(
                session_state,
                FileScanConfig {
                    object_store_url,
                    file_schema: Arc::clone(&self.schema()),
                    file_groups: vec![partition_files?],
                    constraints: Default::default(),
                    statistics,
                    projection: projection.cloned(),
                    limit,
                    output_ordering: vec![],
                    table_partition_cols: vec![],
                },
                filters.as_ref(),
            )
            .await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.lakesoul_io_config.primary_keys.is_empty() {
            if self.lakesoul_io_config.parquet_filter_pushdown {
                Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
            } else {
                Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
            }
        } else {
            // O(nml), n = number of filters, m = number of primary keys, l = number of columns
            filters
                .iter()
                .map(|f| {
                    let cols = f.column_refs();
                    if self.lakesoul_io_config.parquet_filter_pushdown
                        && cols
                            .iter()
                            .all(|col| self.lakesoul_io_config.primary_keys.contains(&col.name))
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
        self.listing_table.insert_into(state, input, overwrite).await
    }
}
