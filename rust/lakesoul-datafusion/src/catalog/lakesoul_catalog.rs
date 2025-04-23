// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::catalog::CatalogProvider`] implementation for the LakeSoul.

use crate::catalog::LakeSoulNamespace;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::SchemaProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::MetaDataClientRef;
use proto::proto::entity::Namespace;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::runtime::Handle;

/// A metadata wrapper for LakeSoul metadata and DataFusion catalog.
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

    fn get_all_namespace(&self) -> crate::error::Result<Vec<Namespace>> {
        let client = self.metadata_client.clone();
        futures::executor::block_on(async move { Ok(client.get_all_namespace().await?) })
    }
}

impl CatalogProvider for LakeSoulCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        tokio::task::block_in_place(|| {
            match futures::executor::block_on(async { self.metadata_client.get_all_namespace().await }) {
                Ok(v) => v.into_iter().map(|np| np.namespace).collect(),
                Err(_) => vec![],
            }
        })
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!("schema: {:?}", name);
        tokio::task::block_in_place(|| {
            match futures::executor::block_on(async { self.metadata_client.get_all_namespace().await }) {
                Ok(v) => {
                    if v.iter().any(|np| np.namespace == name) {
                        Some(Arc::new(LakeSoulNamespace::new(
                            self.metadata_client.clone(),
                            self.context.clone(),
                            name,
                        )) as Arc<dyn SchemaProvider>)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
    }

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    fn register_schema(&self, name: &str, _schema: Arc<dyn SchemaProvider>) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let client = self.metadata_client.clone();
        let schema: Option<Arc<dyn SchemaProvider>> = {
            match self.get_all_namespace() {
                Ok(v) if v.iter().any(|np| np.namespace == name) => Some(Arc::new(LakeSoulNamespace::new(
                    self.metadata_client.clone(),
                    self.context.clone(),
                    name,
                )) as Arc<dyn SchemaProvider>),
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
        Err(DataFusionError::NotImplemented("Not supported".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LakeSoulQueryPlanner;
    use datafusion::arrow::util::pretty::print_batches;
    use datafusion::{
        execution::{
            context::{SessionContext, SessionState},
            runtime_env::RuntimeEnv,
        },
        prelude::SessionConfig,
    };
    use lakesoul_metadata::MetaDataClient;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_show_tables() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let config = SessionConfig::default().with_information_schema(true);
        let planner = LakeSoulQueryPlanner::new_ref();
        let state =
            SessionState::new_with_config_rt(config, Arc::new(RuntimeEnv::default())).with_query_planner(planner);

        let ctx = Arc::new(SessionContext::new_with_state(state));
        let catalog = LakeSoulCatalog::new(client.clone(), ctx.clone());
        ctx.register_catalog("LAKESOUL".to_string(), Arc::new(catalog));

        // // 创建测试用的namespace
        // let test_namespace = "test_namespace";
        // let schema = Arc::new(LakeSoulNamespace::new(client.clone(), ctx.clone(), test_namespace));
        // catalog.register_schema(test_namespace, schema)?;

        // 执行show tables命令
        // let sql = "SHOW CATALOGS";
        let sql = "SHOW TABLES";
        // let sql = "CREATE SCHEMA LAKESOUL.DEFAULT";
        let df = ctx.sql(sql).await?;
        // print_batches(&df.clone().explain(true, false)?.collect().await?);
        let results = df.collect().await?;
        let _ = print_batches(&results);

        // 验证结果
        // assert!(!results.is_empty());

        Ok(())
    }
}
