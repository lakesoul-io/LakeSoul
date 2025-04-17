// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::catalog::TableProviderFactory`] implementation for LakeSoul table.
//!
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::logical_plan::CreateExternalTable;
use lakesoul_metadata::MetaDataClientRef;
use log::info;
use std::sync::Arc;

use crate::datasource::table_provider::LakeSoulTableProvider;

#[derive(Debug, Clone)]
pub struct LakeSoulTableProviderFactory {
    metadata_client: MetaDataClientRef,
    warehouse_prefix: Option<String>,
}

impl LakeSoulTableProviderFactory {
    pub fn new(metadata_client: MetaDataClientRef, warehouse_prefix: Option<String>) -> Self {
        Self {
            metadata_client,
            warehouse_prefix,
        }
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }
}

#[async_trait::async_trait]
impl TableProviderFactory for LakeSoulTableProviderFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        info!(
            "LakeSoulTableProviderFactory::create: {:?}, {:?}, {:?}, {:?}, {:?}",
            cmd.name, cmd.location, cmd.schema, cmd.constraints, cmd.options
        );

        let mut cmd = cmd.clone();
        if let Some(warehouse_prefix) = &self.warehouse_prefix {
            let schema = cmd.name.schema().unwrap_or("default");
            let table_name = cmd.name.table();
            cmd.location = format!("{}/{}/{}", warehouse_prefix, schema, table_name);
        }
        Ok(Arc::new(
            LakeSoulTableProvider::new_from_create_external_table(state, self.metadata_client(), &cmd)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        ))
    }
}
