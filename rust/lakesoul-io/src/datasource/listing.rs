// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_schema::SchemaBuilder;
use async_trait::async_trait;

use arrow::datatypes::SchemaRef;

use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableUrl};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{datasource::TableProvider, logical_expr::Expr};

use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion_common::{DataFusionError, Result};
use tracing::{debug, instrument};

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
        let table_schema = Self::compute_table_schema(file_schema, &lakesoul_io_config);

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

    pub fn compute_table_schema(file_schema: SchemaRef, config: &LakeSoulIOConfig) -> SchemaRef {
        let target_schema = uniform_schema(config.target_schema());
        let mut builder = SchemaBuilder::from(target_schema.fields());
        for field in file_schema.fields() {
            if target_schema.field_with_name(field.name()).is_err() {
                builder.push(field.clone());
            }
        }
        for field in config.partition_schema().fields() {
            if target_schema.field_with_name(field.name()).is_err() {
                builder.push(field.clone());
            }
        }
        Arc::new(builder.finish())
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
        self.listing_table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        if self.lakesoul_io_config.primary_keys.is_empty() {
            if self.lakesoul_io_config.parquet_filter_pushdown {
                Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
            } else {
                Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
            }
        } else {
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
