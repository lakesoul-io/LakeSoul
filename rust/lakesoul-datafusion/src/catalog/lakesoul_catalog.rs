// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The [`datafusion::catalog::CatalogProvider`] implementation for the LakeSoul.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use datafusion::catalog::{CatalogProvider, SchemaProvider, Session};
use datafusion::error::{DataFusionError, Result};
use lakesoul_metadata::MetaDataClientRef;
use lakesoul_metadata_proto::entity::Namespace;
use rootcause::report;

use crate::catalog::LakeSoulNamespace;
use crate::catalog::snapshot::{
    CatalogSnapshot, DEFAULT_CATALOG_REFRESH_INTERVAL, wait_on_runtime,
};
use crate::error::df_external_err;

#[derive(Debug, Default, Clone, Copy)]
pub struct LakeSoulProviderOptions {
    pub parquet_force_view_types: bool,
    pub pushdown_filters: bool,
}

impl LakeSoulProviderOptions {
    pub fn from_session(session: &dyn Session) -> Self {
        let parquet = &session.config_options().execution.parquet;
        Self {
            parquet_force_view_types: parquet.schema_force_view_types,
            pushdown_filters: parquet.pushdown_filters,
        }
    }
}

/// A metadata wrapper for LakeSoul metadata and DataFusion catalog.
pub struct LakeSoulCatalog {
    metadata_client: MetaDataClientRef,
    provider_options: LakeSoulProviderOptions,
    /// Namespace/table view backing the synchronous catalog listing methods.
    snapshot: Arc<CatalogSnapshot>,
}

impl Debug for LakeSoulCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulCatalog{..}").finish()
    }
}

impl LakeSoulCatalog {
    /// Builds a catalog whose metadata view refreshes every
    /// [`DEFAULT_CATALOG_REFRESH_INTERVAL`].
    pub fn new(
        meta_data_client_ref: MetaDataClientRef,
        provider_options: LakeSoulProviderOptions,
    ) -> Self {
        Self::with_refresh_interval(
            meta_data_client_ref,
            provider_options,
            DEFAULT_CATALOG_REFRESH_INTERVAL,
        )
    }

    /// Builds a catalog whose metadata view is refreshed every
    /// `refresh_interval` by a background task.
    pub fn with_refresh_interval(
        meta_data_client_ref: MetaDataClientRef,
        provider_options: LakeSoulProviderOptions,
        refresh_interval: Duration,
    ) -> Self {
        let snapshot =
            CatalogSnapshot::new(Arc::clone(&meta_data_client_ref), refresh_interval);
        Self::with_snapshot(meta_data_client_ref, provider_options, snapshot)
    }

    /// Builds a catalog listing from a shared `snapshot`.
    ///
    /// Sessions of one factory share a single snapshot, so a cluster of
    /// connections runs one metadata refresher instead of one per connection.
    pub fn with_snapshot(
        meta_data_client_ref: MetaDataClientRef,
        provider_options: LakeSoulProviderOptions,
        snapshot: Arc<CatalogSnapshot>,
    ) -> Self {
        Self {
            metadata_client: meta_data_client_ref,
            provider_options,
            snapshot,
        }
    }

    pub fn metadata_client(&self) -> MetaDataClientRef {
        self.metadata_client.clone()
    }
    pub fn provider_options(&self) -> &LakeSoulProviderOptions {
        &self.provider_options
    }

    /// The catalog's metadata view.
    ///
    /// The listing methods answer from it; call
    /// [`CatalogSnapshot::refresh`] to refresh it on demand.
    pub fn snapshot(&self) -> &Arc<CatalogSnapshot> {
        &self.snapshot
    }
}

impl CatalogProvider for LakeSoulCatalog {
    fn schema_names(&self) -> Vec<String> {
        // Answered from the snapshot: BI clients call this (and the other
        // listing methods) concurrently and often, and blocking each call on a
        // metadata round trip parked runtime worker threads.
        self.snapshot.ensure_loaded();
        if self.snapshot.is_never_refreshed() {
            self.snapshot.spawn_refresh_if_stale();
        }
        self.snapshot.namespaces()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        info!("schema: {}", name);
        // Existence comes from the view: DataFusion consults `schema()` for
        // name resolution and for `CREATE SCHEMA`'s existence check, and a
        // live metadata read here would block every call. A miss triggers a
        // background refresh and is visible to the following call; catalog
        // *writes* never rely on this answer (see `register_schema`).
        self.snapshot.ensure_loaded();
        if !self.snapshot.has_namespace(name) {
            self.snapshot.spawn_refresh_if_stale();
            return None;
        }
        Some(Arc::new(LakeSoulNamespace::with_snapshot(
            self.metadata_client.clone(),
            self.provider_options,
            name,
            Arc::clone(&self.snapshot),
        )) as Arc<dyn SchemaProvider>)
    }

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    fn register_schema(
        &self,
        name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        // Authoritative existence check. `schema()` answers from a view that
        // can be up to one refresh interval stale, so creating unconditionally
        // would surface a duplicate-key error for `CREATE SCHEMA IF NOT
        // EXISTS` when the namespace was created out of band; the trait
        // contract asks for the replaced provider instead.
        let client = self.metadata_client.clone();
        let existed = {
            let client = client.clone();
            let name = name.to_string();
            wait_on_runtime(async move {
                client
                    .get_namespace_by_namespace(&name)
                    .await
                    .map(|namespace| namespace.is_some())
                    .map_err(|e| report!(e).into_dynamic())
            })
            .map_err(df_external_err)?
        };
        if !existed {
            // use default value
            let np = Namespace {
                namespace: name.into(),
                properties: "{}".into(),
                comment: "created by lakesoul-datafusion".into(),
                domain: "public".into(),
            };
            wait_on_runtime(async move {
                client
                    .create_namespace(np)
                    .await
                    .map_err(|e| report!(e).into_dynamic())
            })
            .map_err(df_external_err)?;
        }
        // Published to the view: the write must be visible to the next listing,
        // while a listing failure must never turn an already committed write
        // into an error for the caller.
        self.snapshot.refresh_after_write();
        Ok(existed.then(|| {
            Arc::new(LakeSoulNamespace::with_snapshot(
                self.metadata_client.clone(),
                self.provider_options,
                name,
                Arc::clone(&self.snapshot),
            )) as Arc<dyn SchemaProvider>
        }))
    }

    /// Removes a schema from this catalog. Implementations of this method should return
    /// errors if the schema exists but cannot be dropped. For example, in DataFusion's
    /// default in-memory catalog, [`MemoryCatalogProvider`], a non-empty schema
    /// will only be successfully dropped when `cascade` is true.
    /// This is equivalent to how DROP SCHEMA works in PostgreSQL.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::NotImplemented("Not supported".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::LakeSoulQueryPlanner;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::util::pretty::print_batches;
    use datafusion::catalog::MemorySchemaProvider;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::{execution::context::SessionContext, prelude::SessionConfig};
    use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
    use lakesoul_metadata::MetaDataClient;
    use tokio::runtime::Runtime;

    use crate::lakesoul_table::LakeSoulTable;

    /// Minimal append-only table schema used by the snapshot tests.
    fn table_config() -> LakeSoulIOConfig {
        LakeSoulIOConfigBuilder::new()
            .with_schema(Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Int32,
                false,
            )])))
            .build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_show_tables() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let config = SessionConfig::default().with_information_schema(true);
        let planner = LakeSoulQueryPlanner::new_ref();
        let state = SessionStateBuilder::default()
            .with_config(config)
            .with_query_planner(planner)
            .build();

        let ctx = Arc::new(SessionContext::new_with_state(state));
        let catalog = LakeSoulCatalog::new(
            client.clone(),
            LakeSoulProviderOptions::from_session(&ctx.state()),
        );
        ctx.register_catalog("lakesoul".to_string(), Arc::new(catalog));

        // // 创建测试用的namespace
        // let test_namespace = "test_namespace";
        // let schema = Arc::new(LakeSoulNamespace::new(
        //     client.clone(),
        //     LakeSoulProviderOptions::from_session(&ctx.state()),
        //     test_namespace,
        // ));
        // catalog.register_schema(test_namespace, schema)?;

        // 执行show tables命令
        // let sql = "SHOW CATALOGS";
        let sql = "SHOW TABLES";
        // let sql = "CREATE SCHEMA lakesoul.DEFAULT";
        let df = ctx.sql(sql).await?;
        // print_batches(&df.clone().explain(true, false)?.collect().await?);
        let results = df.collect().await?;
        let _ = print_batches(&results);

        // 验证结果
        // assert!(!results.is_empty());

        Ok(())
    }

    /// Listing methods answer from the snapshot without a tokio runtime.
    ///
    /// They used to block on a metadata query: `Handle::current()` panicked
    /// here, and `block_in_place` parked a runtime worker thread when a
    /// runtime was present.
    #[test]
    fn listing_methods_answer_without_a_runtime() {
        let rt = Runtime::new().unwrap();
        let client =
            rt.block_on(async { Arc::new(MetaDataClient::from_env().await.unwrap()) });
        let catalog =
            LakeSoulCatalog::new(client.clone(), LakeSoulProviderOptions::default());

        // Built outside the runtime: the view is cold and stays empty until
        // somebody refreshes it from an async context.
        assert!(catalog.snapshot().is_never_refreshed());
        assert!(catalog.schema_names().is_empty());
        // Cold view: existence checks answer "no schema" instead of blocking
        // (or panicking) on a metadata query.
        assert!(catalog.schema("default").is_none());
        let namespace =
            LakeSoulNamespace::new(client, LakeSoulProviderOptions::default(), "default");
        assert!(namespace.table_names().is_empty());
        assert!(!namespace.table_exist("any_table"));
    }

    /// A refresh exposes namespaces and tables created in metadata meanwhile,
    /// while the async table lookup keeps reading live metadata: a stale
    /// listing never hides a table from a query.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn refreshed_snapshot_lists_tables_and_lookup_stays_live() {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        // Long interval: only the explicit refresh below changes the view, so
        // the staleness of the listing is deterministic.
        let catalog = LakeSoulCatalog::with_refresh_interval(
            client.clone(),
            LakeSoulProviderOptions::default(),
            Duration::from_secs(3600),
        );

        let suffix = rand::random::<u32>();
        let listed = format!("snapshot_listed_{suffix}");
        let live = format!("snapshot_live_{suffix}");
        crate::tests::create_table(client.clone(), &listed, table_config())
            .await
            .unwrap();

        catalog.snapshot().refresh().await.unwrap();
        assert!(
            catalog
                .schema_names()
                .contains(&crate::session::DEFAULT_SCHEMA.to_string()),
            "refreshed snapshot must list the default namespace"
        );
        let schema = catalog.schema(crate::session::DEFAULT_SCHEMA).unwrap();
        assert!(schema.table_names().contains(&listed));
        assert!(schema.table_exist(&listed));
        assert!(!schema.table_exist("missing_table"));

        // Created after the last refresh: the listing is stale, the lookup is
        // not.
        crate::tests::create_table(client.clone(), &live, table_config())
            .await
            .unwrap();
        assert!(schema.table(&live).await.unwrap().is_some());
    }

    /// Metadata writes publish into the view: a listing right after a write is
    /// correct without waiting for a refresh, and a namespace that already
    /// exists is returned instead of re-created (the `CREATE SCHEMA IF NOT
    /// EXISTS` path with a stale view).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writes_publish_into_the_view() {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let catalog = LakeSoulCatalog::with_refresh_interval(
            client,
            LakeSoulProviderOptions::default(),
            Duration::from_secs(3600),
        );
        let namespace = format!("write_through_{}", rand::random::<u32>());
        let schema_provider =
            Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;

        assert!(catalog.schema(&namespace).is_none());
        assert!(
            catalog
                .register_schema(&namespace, schema_provider.clone())
                .unwrap()
                .is_none()
        );

        assert!(catalog.schema_names().contains(&namespace));
        let schema = catalog
            .schema(&namespace)
            .expect("the write must publish the namespace");
        assert!(schema.table_names().is_empty());

        let existing = catalog
            .register_schema(&namespace, schema_provider)
            .expect("registering an existing namespace is not an error");
        assert!(existing.is_some());
    }

    /// A table created behind the catalog's back is not re-created by
    /// `register_table`, even while the view is stale:
    /// `CREATE EXTERNAL TABLE IF NOT EXISTS` must not surface a duplicate-key
    /// error, and a listing miss must converge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn registering_an_existing_table_is_idempotent() {
        let client = Arc::new(MetaDataClient::from_env().await.unwrap());
        let options = LakeSoulProviderOptions::default();
        // A long interval keeps the staleness deterministic: only the load
        // below touches the view.
        let catalog = LakeSoulCatalog::with_refresh_interval(
            client.clone(),
            options,
            Duration::from_secs(3600),
        );
        catalog.snapshot().load().await.unwrap();

        let table_name = format!("stale_table_{}", rand::random::<u32>());
        crate::tests::create_table(client.clone(), &table_name, table_config())
            .await
            .unwrap();

        let schema = catalog
            .schema(crate::session::DEFAULT_SCHEMA)
            .expect("the default namespace is listed");
        assert!(!schema.table_exist(&table_name), "the view is stale");

        let lakehouse_table = LakeSoulTable::for_namespace_and_name(
            crate::session::DEFAULT_SCHEMA,
            &table_name,
            Some(client.clone()),
        )
        .await
        .unwrap();
        let provider = lakehouse_table.as_sink_provider(options).await.unwrap();

        let existing = schema
            .register_table(table_name.clone(), provider)
            .expect("registering an existing table must not fail");
        assert!(existing.is_some());
        assert!(
            schema.table_exist(&table_name),
            "the write path refreshes the view"
        );
    }
}
