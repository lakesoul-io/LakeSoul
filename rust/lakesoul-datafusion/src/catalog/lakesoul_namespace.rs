// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::catalog::SchemaProvider`] implementation for the LakeSoul.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::error::LakeSoulMetaDataError;
use rootcause::compat::boxed_error::IntoBoxedError;

use crate::catalog::LakeSoulProviderOptions;
use crate::catalog::snapshot::{
    CatalogSnapshot, DEFAULT_CATALOG_REFRESH_INTERVAL, wait_on_runtime,
};
use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::lakesoul_table::LakeSoulTable;
use crate::lakesoul_table::helpers::case_fold_table_name;

/// A [`SchemaProvider`] that query from LakeSoul metadata.
pub struct LakeSoulNamespace {
    metadata_client: MetaDataClientRef,
    provider_options: LakeSoulProviderOptions,
    namespace: String,
    /// Namespace/table view backing the synchronous listing methods.
    snapshot: Arc<CatalogSnapshot>,
}

impl LakeSoulNamespace {
    /// Builds a namespace backed by its own metadata snapshot, refreshed in
    /// the background every [`DEFAULT_CATALOG_REFRESH_INTERVAL`].
    ///
    /// Prefer [`Self::with_snapshot`] when several namespaces belong to one
    /// catalog: they then share a single refresher.
    pub fn new(
        meta_data_client_ref: MetaDataClientRef,
        provider_options: LakeSoulProviderOptions,
        namespace: &str,
    ) -> Self {
        let snapshot = CatalogSnapshot::new(
            Arc::clone(&meta_data_client_ref),
            DEFAULT_CATALOG_REFRESH_INTERVAL,
        );
        Self::with_snapshot(meta_data_client_ref, provider_options, namespace, snapshot)
    }

    /// Builds a namespace listing its tables from `snapshot`.
    ///
    /// The catalog adapters use this to share the factory's snapshot.
    pub fn with_snapshot(
        meta_data_client_ref: MetaDataClientRef,
        provider_options: LakeSoulProviderOptions,
        namespace: &str,
        snapshot: Arc<CatalogSnapshot>,
    ) -> Self {
        debug!(
            "LakeSoulNamespace::new - Creating new namespace: {}",
            namespace
        );
        Self {
            metadata_client: meta_data_client_ref,
            provider_options,
            namespace: namespace.to_string(),
            snapshot,
        }
    }

    /// Refreshes the snapshot this namespace lists its tables from.
    pub async fn refresh(&self) -> Result<(), DataFusionError> {
        self.snapshot.refresh().await
    }

    /// The metadata view this namespace lists from, shared with the other
    /// namespaces of the same catalog.
    pub fn snapshot(&self) -> &Arc<CatalogSnapshot> {
        &self.snapshot
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        debug!("LakeSoulNamespace::metadata_client - Getting metadata client");
        self.metadata_client.clone()
    }

    pub fn namespace(&self) -> &str {
        debug!(
            "LakeSoulNamespace::namespace - Getting namespace: {}",
            &self.namespace
        );
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
    /// query table_name_id by namespace
    fn table_names(&self) -> Vec<String> {
        debug!(
            "LakeSoulNamespace::table_names - Getting all tables for namespace: {}",
            &self.namespace
        );
        // Answered from the catalog snapshot: this trait method is synchronous
        // and BI clients call it concurrently, so it must not block on a
        // metadata query.
        self.snapshot.ensure_loaded();
        if !self.snapshot.has_namespace(&self.namespace) {
            self.snapshot.spawn_refresh_if_stale();
        }
        self.snapshot.tables(&self.namespace)
    }

    /// Search table by name
    /// return LakeSoulListing table
    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        debug!(
            "LakeSoulNamespace::table - Looking up table '{}' in namespace '{}'",
            name, &self.namespace
        );
        let name = case_fold_table_name(name);
        let table = match LakeSoulTable::for_namespace_and_name(
            &self.namespace,
            &name,
            Some(self.metadata_client()),
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                debug!("table {}.{} not found: {:?}", self.namespace, name, e);
                return Ok(None);
            }
        };
        info!("found table: {}::{}", &self.namespace, table);

        Ok(Some(
            table
                .as_sink_provider(self.provider_options)
                .await
                .map_err(|e| DataFusionError::External(e.into_boxed_error()))?,
        ))
    }

    /// If supported by the implementation, adds a new table to this schema.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        debug!(
            "LakeSoulNamespace::register_table - Registering table '{}' in namespace '{}'",
            name, &self.namespace
        );
        // 获取表的 schema
        let _schema = table.schema();

        let lakesoul_table =
            table
                .downcast_ref::<LakeSoulTableProvider>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Table is not a LakeSoulTableProvider".to_string(),
                    )
                })?;

        let client = self.metadata_client.clone();
        let table_info = lakesoul_table.table_info();
        let table_name = table_info.table_name.clone();
        let table_info = table_info.as_ref().clone();
        // Authoritative existence check: the view can be stale, and creating
        // unconditionally would fail with a duplicate key — after partial
        // metadata inserts — for `CREATE EXTERNAL TABLE IF NOT EXISTS`.
        let existed = {
            let client = client.clone();
            let namespace = self.namespace.clone();
            let table_name = table_name.clone();
            wait_on_runtime(async move {
                match LakeSoulTable::for_namespace_and_name(
                    &namespace,
                    &table_name,
                    Some(client),
                )
                .await
                {
                    Ok(_) => Ok(true),
                    // Only a missing table means "create it": a metadata
                    // failure or an unreadable existing table must not be
                    // turned into a create attempt.
                    Err(report) => {
                        if matches!(
                            report.current_context(),
                            LakeSoulMetaDataError::NotFound(_)
                        ) {
                            Ok(false)
                        } else {
                            Err(DataFusionError::External(report.into_boxed_error()))
                        }
                    }
                }
            })?
        };
        if !existed {
            wait_on_runtime(async move {
                client
                    .create_table(table_info)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })?;
        }
        // Published to the view: the write must be visible to the next listing,
        // while a listing failure must never turn an already committed write
        // into an error for the caller.
        self.snapshot.refresh_after_write();
        Ok(existed.then(|| Arc::clone(&table)))
    }
    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        debug!(
            "LakeSoulNamespace::deregister_table - Deregistering table '{}' from namespace '{}'",
            name, &self.namespace
        );
        let name = case_fold_table_name(name);
        info!("deregister_table: {:?} {:?}", name, &self.namespace);
        let client = self.metadata_client.clone();
        let table_name = name.to_string();
        let namespace = self.namespace.clone();
        let pushdown_filters = self.provider_options.pushdown_filters;
        let provider: Option<Arc<dyn TableProvider>> = wait_on_runtime(async move {
            match LakeSoulTable::for_namespace_and_name(
                &namespace,
                &table_name,
                Some(client.clone()),
            )
            .await
            {
                Ok(table) => {
                    debug!("get table provider success");
                    client
                        .delete_table_by_table_info_cascade(&table.table_info())
                        .await
                        .map_err(|_| {
                            DataFusionError::External("delete table info failed".into())
                        })?;
                    Ok(Some(table.as_provider(pushdown_filters).await.map_err(
                        |e| DataFusionError::External(e.into_boxed_error()),
                    )?))
                }
                Err(report) => match report.current_context() {
                    LakeSoulMetaDataError::NotFound(_) => Ok(None),
                    _ => Err(DataFusionError::External("get table info failed".into())),
                },
            }
        })?;
        // Published to the view: the write must be visible to the next listing,
        // while a listing failure must never turn an already committed write
        // into an error for the caller.
        self.snapshot.refresh_after_write();
        Ok(provider)
    }

    /// Check if the table exists in the namespace.
    fn table_exist(&self, name: &str) -> bool {
        debug!(
            "LakeSoulNamespace::table_exist - Checking existence of table '{}' in namespace '{}'",
            name, &self.namespace
        );
        info!("table_exist: {:?} {:?}", name, &self.namespace);
        // Answered from the snapshot; see `table_names`.
        self.snapshot.ensure_loaded();
        let exists = self.snapshot.table_exists(&self.namespace, name);
        if !exists {
            // Unknown namespace *or* a table created since the last refresh:
            // converge in the background instead of reporting a permanent miss.
            self.snapshot.spawn_refresh_if_stale();
        }
        exists
    }
}
