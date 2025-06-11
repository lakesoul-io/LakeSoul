// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::catalog::SchemaProvider`] implementation for the LakeSoul.

use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::lakesoul_table::helpers::case_fold_table_name;
use crate::lakesoul_table::LakeSoulTable;
use crate::LakeSoulError;
use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::MetaDataClientRef;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio::runtime::Handle;

/// A [`SchemaProvider`] that query from LakeSoul metadata.
pub struct LakeSoulNamespace {
    metadata_client: MetaDataClientRef,
    context: Arc<SessionContext>,
    namespace: String,
}

impl LakeSoulNamespace {
    pub fn new(meta_data_client_ref: MetaDataClientRef, context: Arc<SessionContext>, namespace: &str) -> Self {
        debug!("LakeSoulNamespace::new - Creating new namespace: {}", namespace);
        Self {
            metadata_client: meta_data_client_ref,
            context,
            namespace: namespace.to_string(),
        }
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        debug!("LakeSoulNamespace::metadata_client - Getting metadata client");
        self.metadata_client.clone()
    }

    pub fn context(&self) -> Arc<SessionContext> {
        debug!("LakeSoulNamespace::context - Getting session context");
        self.context.clone()
    }

    pub fn namespace(&self) -> &str {
        debug!("LakeSoulNamespace::namespace - Getting namespace: {}", &self.namespace);
        &self.namespace
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
        debug!("LakeSoulNamespace::as_any called");
        self
    }

    /// query table_name_id by namespace
    fn table_names(&self) -> Vec<String> {
        debug!(
            "LakeSoulNamespace::table_names - Getting all tables for namespace: {}",
            &self.namespace
        );
        let client = self.metadata_client.clone();
        let np = self.namespace.clone();
        futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move {
                    let table_name_ids = client
                        .get_all_table_name_id_by_namespace(&np)
                        .await
                        .expect("get all table name failed");
                    debug!("table_name_ids: {:?}", table_name_ids);
                    table_name_ids
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
    #[instrument(skip(self))]
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        debug!(
            "LakeSoulNamespace::table - Looking up table '{}' in namespace '{}'",
            name, &self.namespace
        );
        let name = case_fold_table_name(name);
        debug!("try to query table: {}.{}", &self.namespace, name);
        let table =
            match LakeSoulTable::for_namespace_and_name(&self.namespace, &name, Some(self.metadata_client())).await {
                Ok(t) => t,
                Err(e) => {
                    debug!("table {}.{} not found: {:?}", self.namespace, name, e);
                    return Ok(None);
                }
            };
        debug!("find table: {} {}, table {:?}", name, &self.namespace, table);
        Ok(table.as_sink_provider(&self.context.state()).await.ok())
    }

    /// If supported by the implementation, adds a new table to this schema.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    #[allow(unused_variables)]
    fn register_table(&self, name: String, table: Arc<dyn TableProvider>) -> Result<Option<Arc<dyn TableProvider>>> {
        debug!(
            "LakeSoulNamespace::register_table - Registering table '{}' in namespace '{}'",
            name, &self.namespace
        );
        // 获取表的 schema
        let schema = table.schema();

        let lakesoul_table = table
            .as_any()
            .downcast_ref::<LakeSoulTableProvider>()
            .ok_or_else(|| DataFusionError::Internal("Table is not a LakeSoulTableProvider".to_string()))?;

        // 调用 create_table 创建表
        let client = self.metadata_client.clone();
        tokio::task::block_in_place(|| {
            match futures::executor::block_on(async move {
                client
                    .create_table(lakesoul_table.table_info().as_ref().clone())
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            }) {
                Ok(_) => Ok(None),
                Err(e) => Err(e),
            }
        })
    }
    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        debug!(
            "LakeSoulNamespace::deregister_table - Deregistering table '{}' from namespace '{}'",
            name, &self.namespace
        );
        let name = case_fold_table_name(name);
        info!("deregister_table: {:?} {:?}", name, &self.namespace);
        let client = self.metadata_client.clone();
        let table_name = name.to_string();
        let namespace = self.namespace.clone();
        let ctx = self.context.clone();
        tokio::task::block_in_place(|| {
            futures::executor::block_on(async move {
                Handle::current()
                    .spawn(async move {
                        match LakeSoulTable::for_namespace_and_name(&namespace, &table_name, Some(client.clone())).await
                        {
                            Ok(table) => {
                                debug!("get table provider success");
                                let _ = client
                                    .delete_table_by_table_info_cascade(&table.table_info())
                                    .await
                                    .map_err(|_| DataFusionError::External("delete table info failed".into()))?;
                                Ok(Some(
                                    table
                                        .as_provider()
                                        .await
                                        .map_err(|e| DataFusionError::External(Box::new(e)))?,
                                ))
                            }
                            Err(e) => match e {
                                LakeSoulError::MetaDataError(LakeSoulMetaDataError::NotFound(_)) => Ok(None),
                                _ => Err(DataFusionError::External("get table info failed".into())),
                            },
                        }
                    })
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            })
        })
    }

    /// Check if the table exists in the namespace.
    fn table_exist(&self, name: &str) -> bool {
        debug!(
            "LakeSoulNamespace::table_exist - Checking existence of table '{}' in namespace '{}'",
            name, &self.namespace
        );
        info!("table_exist: {:?} {:?}", name, &self.namespace);
        // table name is primary key for `table_name_id`
        let client = self.metadata_client.clone();
        let np = self.namespace.clone();
        let name = name.to_string();
        futures::executor::block_on(async move {
            Handle::current()
                .spawn(async move {
                    let table_name_ids = client
                        .get_all_table_name_id_by_namespace(&np)
                        .await
                        .expect("get table name failed");
                    table_name_ids
                        .into_iter()
                        .map(|v| v.table_name)
                        .any(|s| s.eq_ignore_ascii_case(&name))
                })
                .await
                .expect("spawn failed")
        })
    }
}
