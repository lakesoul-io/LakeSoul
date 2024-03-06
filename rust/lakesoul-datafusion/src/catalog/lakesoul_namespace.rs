// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::catalog::create_io_config_builder;
use async_trait::async_trait;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use lakesoul_io::datasource::file_format::LakeSoulParquetFormat;
use lakesoul_io::datasource::listing::LakeSoulListingTable;
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::MetaDataClientRef;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::field::debug;

/// A [`SchemaProvider`] that query pg to automatically discover tables.
/// Due to the restriction of datafusion 's api, "CREATE [EXTERNAL] Table ... " is not supported.
/// May have race condition
pub struct LakeSoulNamespace {
    metadata_client: MetaDataClientRef,
    context: Arc<SessionContext>,
    // primary key
    namespace: String,
    namespace_lock: Arc<RwLock<()>>,
}

impl LakeSoulNamespace {
    pub fn new(meta_data_client_ref: MetaDataClientRef, context: Arc<SessionContext>, namespace: &str) -> Self {
        Self {
            metadata_client: meta_data_client_ref,
            context,
            namespace: namespace.to_string(),
            namespace_lock: Arc::new(RwLock::new(())),
        }
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }

    pub fn context(&self) -> Arc<SessionContext> {
        self.context.clone()
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Dangerous
    /// Should use transaction?
    fn _delete_all_tables(&self) -> Result<()> {
        unimplemented!()
    }
}

impl Debug for LakeSoulNamespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulNamespace{...}").finish()
    }
}

#[async_trait]
impl SchemaProvider for LakeSoulNamespace {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// query table_name_id by namespace
    fn table_names(&self) -> Vec<String> {
        let client = self.metadata_client.clone();
        let np = self.namespace.clone();
        let lock = self.namespace_lock.clone();
        futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move {
                    let _guard = lock.read().await;
                    client
                        .get_all_table_name_id_by_namespace(&np)
                        .await
                        .expect("get all table name failed")
                })
                .await
                .expect("spawn failed")
        })
        .into_iter()
        .map(|v| v.table_name)
        .collect()
    }

    /// Search table by name
    /// return LakeSoulListing table
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let _guard = self.namespace_lock.read().await;
        if self
            .metadata_client
            .get_table_info_by_table_name(name, &self.namespace)
            .await
            .is_ok()
        {
            debug!("call table() on table: {}.{}", &self.namespace, name);
            let config;
            if let Ok(config_builder) =
                create_io_config_builder(self.metadata_client.clone(), Some(name), true, self.namespace()).await
            {
                config = config_builder.build();
            } else {
                return None;
            }
            // Maybe should change
            let file_format = Arc::new(LakeSoulParquetFormat::new(
                Arc::new(ParquetFormat::new()),
                config.clone(),
            ));
            if let Ok(table_provider) = LakeSoulListingTable::new_with_config_and_format(
                &self.context.state(),
                config,
                file_format,
                // care this
                false,
            )
            .await
            {
                debug!("get table provider success");
                return Some(Arc::new(table_provider));
            }
            debug("get table provider fail");
            return None;
        } else {
            debug("get table provider fail");
            None
        }
    }

    /// If supported by the implementation, adds a new table to this schema.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    #[allow(unused_variables)]
    fn register_table(&self, name: String, table: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>> {
        // the type info of dyn TableProvider is not enough or use AST??????
        unimplemented!("schema provider does not support registering tables")
    }
    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let client = self.metadata_client.clone();
        let table_name = name.to_string();
        let np = self.namespace.clone();
        let cxt = self.context.clone();
        let lock = self.namespace_lock.clone();
        futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move {
                    // get table info
                    let _guard = lock.write().await;
                    match client.get_table_info_by_table_name(&table_name, &np).await {
                        Ok(table_info) => {
                            let config;
                            if let Ok(config_builder) =
                                create_io_config_builder(client.clone(), Some(&table_name), true, &np).await
                            {
                                config = config_builder.build();
                            } else {
                                return Err(DataFusionError::External("get table provider config failed".into()));
                            }
                            // Maybe should change
                            let file_format = Arc::new(LakeSoulParquetFormat::new(
                                Arc::new(ParquetFormat::new()),
                                config.clone(),
                            ));
                            if let Ok(table_provider) = LakeSoulListingTable::new_with_config_and_format(
                                &cxt.state(),
                                config,
                                file_format,
                                // care this
                                false,
                            )
                            .await
                            {
                                debug!("get table provider success");
                                client
                                    .delete_table_by_table_info_cascade(&table_info)
                                    .await
                                    .map_err(|_| DataFusionError::External("delete table info failed".into()))?;
                                return Ok(Some(Arc::new(table_provider) as Arc<dyn TableProvider>));
                            }
                            debug("get table provider fail");
                            Err(DataFusionError::External("get table provider failed".into()))
                        }
                        Err(e) => match e {
                            LakeSoulMetaDataError::NotFound(_) => Ok(None),
                            _ => Err(DataFusionError::External("get table info failed".into())),
                        },
                    }
                })
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        })
    }

    fn table_exist(&self, name: &str) -> bool {
        // table name is primary key for `table_name_id`
        let client = self.metadata_client.clone();
        let np = self.namespace.clone();
        let lock = self.namespace_lock.clone();
        futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move {
                    let _guard = lock.read().await;
                    client
                        .get_all_table_name_id_by_namespace(&np)
                        .await
                        .expect("get table name failed")
                })
                .await
                .expect("spawn failed")
        })
        .into_iter()
        .map(|v| v.table_name)
        .any(|s| s == name)
    }
}
