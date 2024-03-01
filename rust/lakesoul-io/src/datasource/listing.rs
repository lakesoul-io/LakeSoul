// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;

use arrow::datatypes::SchemaRef;

use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableUrl};
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{datasource::TableProvider, logical_expr::Expr};

use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion_common::Result;
use tracing::{debug, instrument};

use crate::helpers::listing_table_from_lakesoul_io_config;
use crate::lakesoul_io_config::LakeSoulIOConfig;

pub struct LakeSoulListingTable {
    listing_table: Arc<ListingTable>,
    lakesoul_io_config: LakeSoulIOConfig,
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

        let listing_table = Arc::new(listing_table_from_lakesoul_io_config(session_state, lakesoul_io_config.clone(), file_format, as_sink).await?);

        Ok(Self {
            listing_table,
            lakesoul_io_config,
        })
    }

    pub fn options(&self) -> &ListingOptions {
        self.listing_table.options()
    }

    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        self.listing_table.table_paths()
    }

}

#[async_trait]
impl TableProvider for LakeSoulListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.listing_table.schema()
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
