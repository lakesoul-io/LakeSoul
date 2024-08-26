// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_schema::SchemaBuilder;
use async_trait::async_trait;

use arrow::datatypes::{Schema, SchemaRef};

use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{datasource::TableProvider, logical_expr::Expr};

use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion_common::{DataFusionError, Result, Statistics, ToDFSchema};
use object_store::path::Path;
use tracing::{debug, instrument};
use url::Url;

use crate::helpers::listing_table_from_lakesoul_io_config;
use crate::lakesoul_io_config::LakeSoulIOConfig;
use crate::transform::uniform_schema;

pub struct LakeSoulListingTable {
    listing_table: Arc<ListingTable>,
    lakesoul_io_config: LakeSoulIOConfig,
    table_schema: SchemaRef,
}

impl Debug for LakeSoulListingTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulListingTable{..}").finish()
    }
}

impl LakeSoulListingTable {
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

        Ok(Self {
            listing_table,
            lakesoul_io_config,
            table_schema,
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
impl TableProvider for LakeSoulListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[instrument]
    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("listing scan start");
        let object_store_url = if let Some(url) = self.table_paths().get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };
        let statistics = Statistics::new_unknown(&self.schema());

        let filters = if let Some(expr) = datafusion::optimizer::utils::conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
            let filters = datafusion::physical_expr::create_physical_expr(
                &expr,
                &table_df_schema,
                &self.table_schema,
                state.execution_props(),
            )?;
            Some(filters)
        } else {
            None
        };

        let store = state.runtime_env().object_store(object_store_url.clone())?;
        
        let mut partition_files : Vec<PartitionedFile> = vec![];
        for url in self.table_paths() {
            partition_files.push(
                PartitionedFile::from(
                store
                    .head(&Path::from_url_path(
                        <ListingTableUrl as AsRef<Url>>::as_ref(url).path(),
                    )?)
                    .await?,
            ));
        }
        self.options().format.create_physical_plan(
            state,
            FileScanConfig {
                object_store_url,
                file_schema: Arc::clone(&self.schema()),
                file_groups: vec![self.table_paths().iter().map(|url|PartitionedFile::new(url.to_string(), 0)).collect::<Vec<_>>()],
                statistics,
                projection: projection.cloned(),
                limit,
                output_ordering: vec![],
                table_partition_cols: vec![],
                infinite_source: false,
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
                    if let Ok(cols) = f.to_columns() {
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
                    } else {
                        Ok(TableProviderFilterPushDown::Unsupported)
                    }
                })
                .collect()
        }
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table.insert_into(state, input, overwrite).await
    }
}
