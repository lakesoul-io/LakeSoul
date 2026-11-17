// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::catalog::SchemaProvider`] implementation for the LakeSoul.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock, Mutex};

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata_proto::entity::TableInfo;
use rootcause::compat::boxed_error::IntoBoxedError;
use rootcause::prelude::ResultExt;
use rootcause::report;

use crate::Result;
use crate::catalog::LakeSoulProviderOptions;
use crate::catalog::snapshot::{
    CatalogSnapshot, DEFAULT_CATALOG_REFRESH_INTERVAL, wait_on_runtime,
};
use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::error::df_external_err;
use crate::lakesoul_table::LakeSoulTable;
use crate::lakesoul_table::helpers::case_fold_table_name;

/// A built table provider together with the exact metadata it was built from.
///
/// Building the provider parses the table schema, the table properties and the
/// IO configuration, so a workload that resolves the same table on every
/// statement pays that cost per statement.  The provider is deterministic in
/// the table metadata (and the catalog's provider options), so it can be
/// reused as long as both are unchanged; the table metadata itself is still
/// read from the catalog on every lookup, so a schema or property change is
/// picked up immediately.
struct CachedTableProvider {
    table_info: TableInfo,
    provider_options: String,
    /// Identity of the metadata client the provider was built from.  A
    /// provider holds that client (and its read-replica routing), so it must
    /// never be reused by a session bound to another client.
    client: usize,
    provider: Arc<dyn TableProvider>,
}

type TableProviderCache = Mutex<HashMap<(String, String, usize), CachedTableProvider>>;

static TABLE_PROVIDER_CACHE: LazyLock<TableProviderCache> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const TABLE_PROVIDER_CACHE_ENTRIES: usize = 256;

/// Whether the provider cache is enabled (`LAKESOUL_TABLE_PROVIDER_CACHE=0`
/// turns it off for A/B runs).
fn table_provider_cache_enabled() -> bool {
    !std::env::var("LAKESOUL_TABLE_PROVIDER_CACHE").is_ok_and(|value| value == "0")
}

/// Fingerprint of the catalog options that go into a provider.
fn provider_options_fingerprint(options: &LakeSoulProviderOptions) -> String {
    let mut object_store_options: Vec<(String, String)> = options
        .object_store_options
        .clone()
        .into_inner()
        .into_iter()
        .collect();
    object_store_options.sort();
    format!(
        "{}|{}|{:?}|{:?}|deferred={:?}",
        options.parquet_force_view_types,
        options.pushdown_filters,
        object_store_options,
        options.vector_field_metadata,
        // A transaction block's deferred-commit handle is part of the
        // provider's behaviour (staged rows must stay invisible to index
        // shards), so a provider built for a block may not be reused by a
        // statement outside it.
        options
            .deferred_commit
            .as_ref()
            .map(|commit| Arc::as_ptr(commit) as *const () as usize),
    )
}

fn cached_table_provider(
    namespace: &str,
    name: &str,
    table_info: &TableInfo,
    provider_options: &str,
    client: usize,
) -> Option<Arc<dyn TableProvider>> {
    let cache = TABLE_PROVIDER_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = cache.get(&(namespace.to_string(), name.to_string(), client))?;
    if &entry.table_info != table_info
        || entry.provider_options != provider_options
        || entry.client != client
    {
        return None;
    }
    Some(Arc::clone(&entry.provider))
}

fn store_table_provider(
    namespace: &str,
    name: &str,
    table_info: TableInfo,
    provider_options: String,
    client: usize,
    provider: Arc<dyn TableProvider>,
) {
    let mut cache = TABLE_PROVIDER_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if cache.len() >= TABLE_PROVIDER_CACHE_ENTRIES {
        cache.clear();
    }
    cache.insert(
        (namespace.to_string(), name.to_string(), client),
        CachedTableProvider {
            table_info,
            provider_options,
            client,
            provider,
        },
    );
}

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
    pub async fn refresh(&self) -> Result<()> {
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

    /// Removes table `name` when `allowed` accepts its current definition,
    /// and returns it; `Ok(None)` when no such table exists or `allowed`
    /// refuses it, which leaves it untouched.
    ///
    /// `allowed` sees the exact row that is deleted: the table is resolved
    /// once and deleted by its `table_id`, so a table that replaced the name
    /// since a caller's earlier, possibly stale, check is judged on its own
    /// definition, and one that replaces it during this call is not deleted.
    pub fn deregister_table_if<F>(
        &self,
        name: &str,
        allowed: F,
    ) -> DFResult<Option<Arc<dyn TableProvider>>>
    where
        F: FnOnce(&TableInfo) -> bool + Send + 'static,
    {
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
                    let table_info = table.table_info();
                    if !allowed(&table_info) {
                        info!(
                            "deregister_table: {namespace}.{table_name} ({}) refused",
                            table_info.table_id
                        );
                        return Ok(None);
                    }
                    let deleted = client
                        .delete_table_by_table_id_cascade_if_exists(
                            &table_info.table_id,
                            &table_info.table_path,
                        )
                        .await
                        .context("delete table info failed")?;
                    if !deleted {
                        return Ok(None);
                    }
                    Ok(Some(table.as_provider(pushdown_filters).await?))
                }
                Err(report) => match report.current_context() {
                    LakeSoulMetaDataError::NotFound(_) => Ok(None),
                    _ => Err(report!("get table info failed")),
                },
            }
        })
        .map_err(df_external_err)?;
        // Published to the view: the write must be visible to the next listing,
        // while a listing failure must never turn an already committed write
        // into an error for the caller. After a refusal it lets a stale view
        // converge.
        self.snapshot.refresh_after_write();
        Ok(provider)
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
        let profile = std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok();
        let t_lookup = std::time::Instant::now();
        let table = match super::metadata_cache::table(
            super::metadata_cache::table_key(
                &self.metadata_client(),
                &self.namespace,
                &name,
            ),
            LakeSoulTable::for_namespace_and_name(
                &self.namespace,
                &name,
                Some(self.metadata_client()),
            ),
        )
        .await
        {
            Ok(t) => t,
            Err(e) => {
                debug!("table {}.{} not found: {}", self.namespace, name, e);
                return Ok(None);
            }
        };
        if let Some(sink) = self
            .provider_options
            .deferred_commit
            .as_ref()
            .and_then(|handle| handle.get())
        {
            sink.lock_table(&self.metadata_client, &table.table_info())
                .await?;
        }
        info!(
            "found table: {}.{} table_id: {}",
            &self.namespace,
            table,
            table.table_info().table_id
        );

        let table_info_ms = t_lookup.elapsed().as_secs_f64() * 1000.0;
        // Building the provider is deterministic in the table metadata and the
        // catalog options, both of which are validated here, so a statement
        // resolving an unchanged table reuses it.
        let cache_enabled = table_provider_cache_enabled();
        let options_fingerprint =
            cache_enabled.then(|| provider_options_fingerprint(&self.provider_options));
        if let Some(options) = options_fingerprint.as_deref()
            && let Some(provider) = cached_table_provider(
                &self.namespace,
                &name,
                table.table_info().as_ref(),
                options,
                Arc::as_ptr(&self.metadata_client) as *const () as usize,
            )
        {
            if profile {
                debug!(
                    table_info_ms,
                    total_ms = t_lookup.elapsed().as_secs_f64() * 1000.0,
                    cached = true,
                    "lakesoul catalog table lookup"
                );
            }
            return Ok(Some(provider));
        }
        let t_provider = std::time::Instant::now();
        let provider = table
            .as_provider_without_session(self.provider_options.clone())
            .await
            .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
        if let Some(options) = options_fingerprint {
            store_table_provider(
                &self.namespace,
                &name,
                table.table_info().as_ref().clone(),
                options,
                Arc::as_ptr(&self.metadata_client) as *const () as usize,
                Arc::clone(&provider),
            );
        }
        if profile {
            debug!(
                table_info_ms,
                provider_ms = t_provider.elapsed().as_secs_f64() * 1000.0,
                total_ms = t_lookup.elapsed().as_secs_f64() * 1000.0,
                cached = false,
                "lakesoul catalog table lookup"
            );
        }
        Ok(Some(provider))
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
        let table_info = lakesoul_table.table_info().as_ref().clone();
        // Let PostgreSQL's unique constraint arbitrate concurrent creators.
        // A check followed by create is racy even when the check reads live
        // metadata: two sessions can both observe the table as absent.
        let created = wait_on_runtime(async move {
            client
                .create_table_if_not_exists(table_info)
                .await
                .map_err(|e| report!(e).into_dynamic())
        })
        .map_err(|rep| DataFusionError::External(rep.into_boxed_error()))?;
        let existed = !created;
        // Published to the view: the write must be visible to the next listing,
        // while a listing failure must never turn an already committed write
        // into an error for the caller.
        self.snapshot.refresh_after_write();
        Ok(existed.then(|| Arc::clone(&table)))
    }
    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.deregister_table_if(name, |_| true)
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
        let exists = self.snapshot.is_table_exists(&self.namespace, name);
        if !exists {
            // Unknown namespace *or* a table created since the last refresh:
            // converge in the background instead of reporting a permanent miss.
            self.snapshot.spawn_refresh_if_stale();
        }
        exists
    }
}
