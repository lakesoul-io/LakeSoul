// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::catalog::LakeSoulNamespace;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::prelude::SessionContext;
use lakesoul_metadata::MetaDataClientRef;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::runtime::Handle;

/// A metadata wrapper
pub struct LakeSoulCatalog {
    metadata_client: MetaDataClientRef,
    context: Arc<SessionContext>,
}

impl Debug for LakeSoulCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulCatalog{..}").finish()
    }
}

impl LakeSoulCatalog {
    pub fn new(meta_data_client_ref: MetaDataClientRef, context: Arc<SessionContext>) -> Self {
        Self {
            metadata_client: meta_data_client_ref,
            context,
        }
    }
    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }
    pub fn context(&self) -> Arc<SessionContext> {
        self.context.clone()
    }
}

impl CatalogProvider for LakeSoulCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let client = self.metadata_client.clone();
        futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move { client.get_all_namespace().await.unwrap() })
                .await
                .unwrap()
                .into_iter()
                .map(|t| t.namespace)
                .collect()
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if self.schema_names().contains(&name.to_string()) {
            Some(Arc::new(LakeSoulNamespace::new(
                self.metadata_client.clone(),
                self.context.clone(),
                name,
            )))
        } else {
            None
        }
    }

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    ///
    /// By default returns a "Not Implemented" error
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> lakesoul_io::lakesoul_io_config::Result<Option<Arc<dyn SchemaProvider>>> {
        // the type info of dyn schema is not enough, nothing to use
        let _ = name;
        let _ = schema;
        unimplemented!("Registering new schemas is not supported")
    }

    /// Removes a schema from this catalog. Implementations of this method should return
    /// errors if the schema exists but cannot be dropped. For example, in DataFusion's
    /// default in-memory catalog, [`MemoryCatalogProvider`], a non-empty schema
    /// will only be successfully dropped when `cascade` is true.
    /// This is equivalent to how DROP SCHEMA works in PostgreSQL.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    ///
    /// By default returns a "Not Implemented" error
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> lakesoul_io::lakesoul_io_config::Result<Option<Arc<dyn SchemaProvider>>> {
        // the type info of dyn schema is not enough, nothing to use
        unimplemented!("Deregistering new schemas is not supported")
    }
}
