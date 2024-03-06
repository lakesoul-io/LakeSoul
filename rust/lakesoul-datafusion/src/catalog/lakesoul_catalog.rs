// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::catalog::LakeSoulNamespace;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::MetaDataClientRef;
use proto::proto::entity::Namespace;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, RwLock};
use tokio::runtime::Handle;

/// A metadata wrapper
/// may need a lock
pub struct LakeSoulCatalog {
    metadata_client: MetaDataClientRef,
    context: Arc<SessionContext>,
    catalog_lock: RwLock<()>,
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
            catalog_lock: RwLock::new(()),
        }
    }
    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }
    pub fn context(&self) -> Arc<SessionContext> {
        self.context.clone()
    }

    fn get_all_namespace(&self) -> crate::error::Result<Vec<Namespace>> {
        let client = self.metadata_client.clone();
        futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move { Ok(client.get_all_namespace().await?) })
                .await?
        })
    }
}

impl CatalogProvider for LakeSoulCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let _guard = self.catalog_lock.read();
        if let Ok(v) = self.get_all_namespace() {
            v.into_iter().map(|np| np.namespace).collect()
        } else {
            vec![]
        }
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let _guard = self.catalog_lock.read();
        match self.get_all_namespace() {
            Ok(v) if v.iter().any(|np| np.namespace == name) => Some(Arc::new(LakeSoulNamespace::new(
                self.metadata_client.clone(),
                self.context.clone(),
                name,
            ))),
            _ => None,
        }
    }

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    fn register_schema(&self, name: &str, _schema: Arc<dyn SchemaProvider>) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let _guard = self.catalog_lock.write();
        let client = self.metadata_client.clone();
        let schema: Option<Arc<dyn SchemaProvider>> = {
            match self.get_all_namespace() {
                Ok(v) if v.iter().any(|np| np.namespace == name) => Some(Arc::new(LakeSoulNamespace::new(
                    self.metadata_client.clone(),
                    self.context.clone(),
                    name,
                ))),
                _ => None,
            }
        };
        // use default value
        let np = Namespace {
            namespace: name.into(),
            properties: "{}".into(),
            comment: "created by lakesoul-datafusion".into(),
            domain: "public".into(),
        };
        let _ = futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move { client.create_namespace(np).await })
                .await
                .map_err(|e| LakeSoulMetaDataError::Other(Box::new(e)))?
        });
        Ok(schema)
    }

    /// Removes a schema from this catalog. Implementations of this method should return
    /// errors if the schema exists but cannot be dropped. For example, in DataFusion's
    /// default in-memory catalog, [`MemoryCatalogProvider`], a non-empty schema
    /// will only be successfully dropped when `cascade` is true.
    /// This is equivalent to how DROP SCHEMA works in PostgreSQL.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    fn deregister_schema(&self, _name: &str, _cascade: bool) -> Result<Option<Arc<dyn SchemaProvider>>> {
        // Not supported
        // let _guard = self.catalog_lock.write();
        // let client = self.metadata_client.clone();
        // let schema: Option<Arc<dyn SchemaProvider>> = {
        //     match self.get_all_namespace() {
        //         Ok(v) if v.iter().any(|np| np.namespace == name) => Some(Arc::new(LakeSoulNamespace::new(
        //             self.metadata_client.clone(),
        //             self.context.clone(),
        //             name,
        //         ))),
        //         _ => None,
        //     }
        // };
        // let namespace = name.to_string();
        // if let Some(s) = schema {
        //     if !s.table_names().is_empty() && !cascade {
        //         return Err(DataFusionError::External("can not delete".into()));
        //     }
        //     // delete all tables
        //     return Ok(Some(s));
        // }
        // return Ok(None);
        Err(DataFusionError::NotImplemented("Not supported".into()))
    }
}
