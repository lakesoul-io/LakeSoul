// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Connection-local PG catalog layer.
//!
//! PostgreSQL maps a database to exactly one LakeSoul namespace. A
//! connection therefore sees:
//!
//! - a real [`PgLakeSoulCatalog`] for the current database, exposing the
//!   `public` schema (backed by the mapped LakeSoul namespace) and the
//!   `pg_catalog` virtual schema;
//! - an empty marker catalog for every *other* visible namespace, so that
//!   `pg_catalog.pg_database` lists them while `pg_namespace`/`pg_class`
//!   stay scoped to the current database, matching PostgreSQL's
//!   no-cross-database-query semantics.
//!
//! The namespace list is snapshotted once during connection startup from the
//! async metadata client; [`CatalogProviderList::catalog_names`] is a sync
//! interface, so it reads that snapshot instead of blocking on metadata.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use lakesoul_datafusion::catalog::{
    CatalogSnapshot, LakeSoulNamespace, LakeSoulProviderOptions,
};
use lakesoul_datafusion::datasource::table_provider::{
    LakeSoulTableProvider, TableReadSnapshot,
};
use lakesoul_metadata::MetaDataClientRef;
use parking_lot::RwLock;

use crate::acl::AclEntry;
use crate::auth::DomainAccess;
use crate::federation::FederationRuntime;
use crate::pg_settings::{PgParameters, TIME_TRAVEL_PARAMETER};

/// The single user-visible schema of a PG database: `public` maps to the
/// LakeSoul namespace named by the database connection parameter.
pub const PUBLIC_SCHEMA: &str = "public";

/// The real catalog backing one PG database.
///
/// Schemas are fixed: `public` (the mapped LakeSoul namespace) plus whatever
/// the `pg_catalog` setup registers as a virtual schema. LakeSoul namespaces
/// other than the mapped one are intentionally invisible here — they are
/// separate PG databases, not schemas.
#[derive(Debug)]
pub struct PgLakeSoulCatalog {
    public_schema: Arc<PgPublicSchema>,
    virtual_schemas: RwLock<BTreeMap<String, Arc<dyn SchemaProvider>>>,
}

impl PgLakeSoulCatalog {
    pub fn new(public_schema: Arc<PgPublicSchema>) -> Self {
        Self {
            public_schema,
            virtual_schemas: RwLock::new(BTreeMap::new()),
        }
    }
}

/// `public` schema of one PostgreSQL database: LakeSoul tables plus foreign
/// tables defined for that database.
///
/// Under domain enforcement (`access` is set) a LakeSoul table outside the
/// connection's domain does not exist for it: [`SchemaProvider::table`]
/// checks the domain of the table it just read from metadata, and the
/// synchronous methods consult the domains the catalog snapshot holds. A
/// table the snapshot does not know yet (created by another process since
/// the last refresh) is therefore listed, `table_exist`-ed and dropped only
/// after the next refresh, while queries resolve it at once. Foreign tables
/// keep their own per-user rules.
pub struct PgPublicSchema {
    local: Arc<LakeSoulNamespace>,
    federation: Option<Arc<FederationRuntime>>,
    database: String,
    user: String,
    access: Option<DomainAccess>,
    acl: Option<Arc<AclEntry>>,
    /// Connection-local session parameters; `lakesoul.read.time_travel` pins
    /// every local table scan resolved through this schema.
    parameters: Arc<PgParameters>,
    /// Pending DDL of the connection's open transaction block.
    transaction: crate::transaction::SharedTransaction,
}

impl std::fmt::Debug for PgPublicSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgPublicSchema")
            .field("database", &self.database)
            .field("user", &self.user)
            .field("federation", &self.federation.is_some())
            .field("access", &self.access)
            .field("acl", &self.acl.is_some())
            .finish()
    }
}

impl PgPublicSchema {
    pub fn new(
        local: Arc<LakeSoulNamespace>,
        federation: Option<Arc<FederationRuntime>>,
        database: &str,
        user: &str,
        access: Option<DomainAccess>,
        acl: Option<Arc<AclEntry>>,
        parameters: Arc<PgParameters>,
        transaction: crate::transaction::SharedTransaction,
    ) -> Self {
        Self {
            local,
            federation,
            database: database.to_string(),
            user: user.to_string(),
            access,
            acl,
            parameters,
            transaction,
        }
    }

    /// Pin a local table to the session's `lakesoul.read.time_travel`.
    ///
    /// Foreign tables are not LakeSoul providers and keep reading their
    /// current state. An invalid value can only come from `set_config` (SET
    /// validates it); leave the table at latest rather than failing the whole
    /// resolution, and let the scan report the bad parameter if it matters.
    fn pin_session_read(&self, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        let Some(raw) = self.parameters.get(TIME_TRAVEL_PARAMETER) else {
            return table;
        };
        let raw = raw.trim();
        if raw.is_empty() {
            return table;
        }
        let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(raw) else {
            return table;
        };
        let provider: &dyn Any = table.as_ref();
        let Some(provider) = provider.downcast_ref::<LakeSoulTableProvider>() else {
            return table;
        };
        Arc::new(
            provider.snapshot_view_requiring_data(TableReadSnapshot::AsOfMs(
                parsed.timestamp_millis(),
            )),
        )
    }

    /// The block's pending provider or hidden marker for `name`, if any.
    fn pending(&self, name: &str) -> Option<Option<Arc<dyn TableProvider>>> {
        let transaction = self.transaction.lock();
        let state = transaction.as_ref()?;
        if let Some(table) = state.catalog.table(name) {
            return Some(Some(table));
        }
        state.catalog.is_hidden(name).then_some(None)
    }

    #[cfg(test)]
    pub fn snapshot(&self) -> &Arc<CatalogSnapshot> {
        self.local.snapshot()
    }

    /// Whether the snapshot places `table` in a domain this connection may
    /// see. Always true without domain enforcement; false for a table the
    /// snapshot does not know.
    fn visible_in_snapshot(&self, table: &str) -> bool {
        if let Some(acl) = &self.acl {
            return acl.view().table_is_visible(self.local.namespace(), table);
        }
        let Some(access) = &self.access else {
            return true;
        };
        self.local
            .snapshot()
            .table_domain(self.local.namespace(), table)
            .is_some_and(|domain| access.allows(&domain))
    }

    /// Whether a table read from metadata is visible to this connection.
    fn visible_provider(&self, table: &Arc<dyn TableProvider>) -> bool {
        if let Some(acl) = &self.acl {
            return table.downcast_ref::<LakeSoulTableProvider>().is_some_and(
                |provider| {
                    let info = provider.table_info();
                    acl.view()
                        .table_is_visible(&info.table_namespace, &info.table_name)
                },
            );
        }
        let Some(access) = &self.access else {
            return true;
        };
        // Fail closed: the local namespace only builds LakeSoul tables, so
        // anything else has no domain to check.
        table
            .downcast_ref::<LakeSoulTableProvider>()
            .is_some_and(|provider| access.allows(&provider.table_info().domain))
    }
}

#[async_trait]
impl SchemaProvider for PgPublicSchema {
    fn table_names(&self) -> Vec<String> {
        let mut names = self.local.table_names();
        if self.access.is_some() || self.acl.is_some() {
            names.retain(|name| self.visible_in_snapshot(name));
        }
        if let Some(federation) = &self.federation {
            let acl = self.acl.as_ref().map(|acl| acl.view());
            for name in federation.visible_foreign_names(
                &self.database,
                &self.user,
                self.access.as_ref().map(DomainAccess::domain),
                acl.as_deref(),
            ) {
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        let transaction = self.transaction.lock();
        match transaction.as_ref() {
            Some(state) => state.catalog.table_names(names),
            None => names,
        }
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(pending) = self.pending(name) {
            return Ok(pending);
        }
        if let Some(acl) = &self.acl
            && !acl.view().table_is_visible(self.local.namespace(), name)
        {
            // A table created by another engine after this session's last ACL
            // refresh is not in the view yet. Resolve that one table live
            // before reporting it missing; no full metadata scan is needed.
            let live = acl
                .live_table_access(self.local.namespace(), name)
                .await
                .map_err(|error| {
                    datafusion::error::DataFusionError::External(Box::new(error))
                })?;
            if let Some(live) = live
                && live.privilege.can_read()
            {
                let table = self.local.table(name).await?;
                return Ok(table.map(|table| self.pin_session_read(table)));
            }
        }
        if let Some(federation) = &self.federation
            && federation.is_foreign(&self.database, name)
        {
            if let Some(acl) = &self.acl
                && !federation
                    .foreign_table_privilege(
                        &self.database,
                        name,
                        &self.user,
                        self.access.as_ref().map(DomainAccess::domain),
                        Some(&acl.view()),
                    )
                    .can_read()
            {
                return Ok(None);
            }
            let acl_view = self.acl.as_ref().map(|acl| acl.view());
            if let Some(table) = federation
                .provider(
                    &self.database,
                    &self.user,
                    name,
                    self.access.as_ref().map(DomainAccess::domain),
                    acl_view.as_deref(),
                )
                .map_err(|error| DataFusionError::External(error.into()))?
            {
                return Ok(Some(table));
            }
        }
        let table = self.local.table(name).await?;
        Ok(table
            .filter(|table| self.visible_provider(table))
            .map(|table| self.pin_session_read(table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        if let Some(pending) = self.pending(name) {
            return pending.is_some();
        }
        if let Some(federation) = &self.federation
            && federation.is_foreign(&self.database, name)
        {
            return self.acl.as_ref().is_none_or(|acl| {
                federation
                    .foreign_table_privilege(
                        &self.database,
                        name,
                        &self.user,
                        self.access.as_ref().map(DomainAccess::domain),
                        Some(&acl.view()),
                    )
                    .can_read()
            });
        }
        self.local.table_exist(name) && self.visible_in_snapshot(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.local.register_table(name, table)
    }

    /// A table hidden by the snapshot, or one it does not know yet, is
    /// reported as absent, like `table_exist` reports it. The snapshot may
    /// be stale, though: the name it allowed can have been dropped and
    /// recreated in another domain since. The deletion is therefore
    /// authorized again on the exact row it deletes.
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(acl) = self.acl.clone() {
            let view = acl.view();
            if !view.table_is_visible(self.local.namespace(), name) {
                return Ok(None);
            }
            return self.local.deregister_table_if(name, move |table_info| {
                view.table_access(&table_info.table_namespace, &table_info.table_name)
                    .is_some_and(|access| {
                        access.table_id == table_info.table_id && access.can_ddl()
                    })
            });
        }
        let Some(access) = self.access.clone() else {
            return self.local.deregister_table(name);
        };
        if !self.visible_in_snapshot(name) {
            return Ok(None);
        }
        self.local.deregister_table_if(name, move |table_info| {
            access.allows(&table_info.domain)
        })
    }
}

impl CatalogProvider for PgLakeSoulCatalog {
    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> =
            self.virtual_schemas.read().keys().cloned().collect();
        names.push(PUBLIC_SCHEMA.to_string());
        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == PUBLIC_SCHEMA {
            return Some(Arc::clone(&self.public_schema) as Arc<dyn SchemaProvider>);
        }
        self.virtual_schemas
            .read()
            .get(name)
            .map(|schema| Arc::clone(schema) as Arc<dyn SchemaProvider>)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        if name == "pg_catalog" {
            Ok(self.virtual_schemas.write().insert(name.into(), schema))
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "cannot create schema {name:?} in a read-only LakeSoul session"
            )))
        }
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::NotImplemented(
            "cannot drop schemas in a read-only LakeSoul session".to_string(),
        ))
    }
}

/// Placeholder for a LakeSoul namespace that is visible in `pg_database` but
/// is not this connection's database.
#[derive(Debug)]
struct EmptyDatabaseCatalog;

impl CatalogProvider for EmptyDatabaseCatalog {
    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        None
    }

    fn register_schema(
        &self,
        name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::NotImplemented(format!(
            "cannot create schema {name:?}: cross-database objects are not supported"
        )))
    }

    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Err(DataFusionError::NotImplemented(
            "cannot drop schemas in a read-only LakeSoul session".to_string(),
        ))
    }
}

/// Catalog list that turns every visible LakeSoul namespace into a PG
/// database entry.
///
/// `pg_catalog.pg_database` traverses `catalog_names()` only, while
/// `pg_namespace` and `pg_class` additionally walk each catalog's schemas
/// and tables. Listing the other namespaces as empty catalogs makes them
/// visible as databases while keeping object visibility scoped to the
/// current database.
///
/// The pinned `datafusion-postgres` fork (branch `pg-catalog-master`) restores
/// `PgCatalogOptions::include_synthetic_postgres_database`, which LakeSoul
/// sets to `false`: no synthetic `postgres` row is appended to
/// `pg_catalog.pg_database`, so listings show exactly the LakeSoul
/// namespaces. (Upstream master since 3ef93e6 always adds the row.)
#[derive(Debug)]
pub struct PgDatabaseCatalogList {
    /// The database selected by this connection at startup.
    current_database: String,
    /// Namespaces visible to this connection (startup snapshot).
    visible_databases: RwLock<BTreeSet<String>>,
    /// Real catalog backing `current_database`.
    current_catalog: RwLock<Option<Arc<dyn CatalogProvider>>>,
    marker_catalog: Arc<dyn CatalogProvider>,
}

impl PgDatabaseCatalogList {
    pub fn new(
        current_database: String,
        visible_databases: BTreeSet<String>,
        current_catalog: Arc<dyn CatalogProvider>,
    ) -> Self {
        Self {
            current_database,
            visible_databases: RwLock::new(visible_databases),
            current_catalog: RwLock::new(Some(current_catalog)),
            marker_catalog: Arc::new(EmptyDatabaseCatalog),
        }
    }

    /// Databases visible in `pg_catalog.pg_database`.
    pub fn database_names(&self) -> Vec<String> {
        self.visible_databases.read().iter().cloned().collect()
    }

    /// Replace the visible database set after an ACL refresh.
    pub fn replace_visible_databases(&self, databases: BTreeSet<String>) {
        *self.visible_databases.write() = databases;
    }

    /// Make a database created by this connection visible in its catalog.
    pub(crate) fn register_database(&self, name: String) {
        self.visible_databases.write().insert(name);
    }
}

impl CatalogProviderList for PgDatabaseCatalogList {
    /// Only the current database may install its real catalog; registering
    /// into a marker database is a no-op.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        if name == self.current_database {
            self.current_catalog.write().replace(catalog)
        } else {
            None
        }
    }

    fn catalog_names(&self) -> Vec<String> {
        self.database_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        if name == self.current_database {
            self.current_catalog.read().clone()
        } else if self.visible_databases.read().contains(name) {
            Some(Arc::clone(&self.marker_catalog))
        } else {
            None
        }
    }
}

/// Builds the real catalog for the current PG database.
///
/// `snapshot` is the session factory's metadata view: every connection shares
/// it, so one process runs one background refresher rather than one per
/// connection.
#[cfg(test)]
pub fn pg_lakesoul_catalog(
    client: MetaDataClientRef,
    provider_options: LakeSoulProviderOptions,
    namespace: &str,
    snapshot: Arc<CatalogSnapshot>,
) -> Arc<PgLakeSoulCatalog> {
    pg_lakesoul_catalog_with_federation(
        client,
        provider_options,
        namespace,
        snapshot,
        None,
        "",
        None,
        None,
        Arc::new(PgParameters::default()),
        crate::transaction::shared(),
    )
}

pub fn pg_lakesoul_catalog_with_federation(
    client: MetaDataClientRef,
    provider_options: LakeSoulProviderOptions,
    namespace: &str,
    snapshot: Arc<CatalogSnapshot>,
    federation: Option<Arc<FederationRuntime>>,
    user: &str,
    access: Option<DomainAccess>,
    acl: Option<Arc<AclEntry>>,
    parameters: Arc<PgParameters>,
    transaction: crate::transaction::SharedTransaction,
) -> Arc<PgLakeSoulCatalog> {
    let local = Arc::new(LakeSoulNamespace::with_snapshot(
        client,
        provider_options,
        namespace,
        snapshot,
    ));
    let public_schema = Arc::new(PgPublicSchema::new(
        local,
        federation,
        namespace,
        user,
        access,
        acl,
        parameters,
        transaction,
    ));
    Arc::new(PgLakeSoulCatalog::new(public_schema))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
    use datafusion::error::{DataFusionError, Result};
    use lakesoul_datafusion::catalog::{
        DEFAULT_CATALOG_REFRESH_INTERVAL, LakeSoulProviderOptions,
    };
    use lakesoul_metadata::MetaDataClient;

    use super::*;

    /// Stand-in catalog used by the struct-logic tests; these never touch
    /// PostgreSQL metadata, so no real LakeSoul catalog is required.
    #[derive(Debug)]
    struct FakeCatalog;

    impl CatalogProvider for FakeCatalog {
        fn schema_names(&self) -> Vec<String> {
            vec![PUBLIC_SCHEMA.to_string()]
        }

        fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            (name == PUBLIC_SCHEMA)
                .then(|| Arc::new(FakeSchema) as Arc<dyn SchemaProvider>)
        }

        fn register_schema(
            &self,
            _name: &str,
            _schema: Arc<dyn SchemaProvider>,
        ) -> Result<Option<Arc<dyn SchemaProvider>>> {
            Ok(None)
        }
    }

    #[derive(Debug)]
    struct FakeSchema;

    #[async_trait::async_trait]
    impl SchemaProvider for FakeSchema {
        fn table_names(&self) -> Vec<String> {
            vec![]
        }

        async fn table(
            &self,
            _name: &str,
        ) -> Result<Option<Arc<dyn datafusion::catalog::TableProvider>>> {
            Ok(None)
        }

        fn table_exist(&self, _name: &str) -> bool {
            false
        }
    }

    fn fake_catalog() -> Arc<dyn CatalogProvider> {
        Arc::new(FakeCatalog)
    }

    fn fake_schema() -> Arc<dyn SchemaProvider> {
        Arc::new(FakeSchema)
    }

    fn namespaces(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// Constructs the real catalog backed by live metadata.
    fn real_catalog(namespace: &str) -> Arc<PgLakeSoulCatalog> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let client = Arc::new(
            runtime
                .block_on(async { MetaDataClient::from_env().await })
                .expect("metadata client"),
        );
        let snapshot =
            CatalogSnapshot::new(Arc::clone(&client), DEFAULT_CATALOG_REFRESH_INTERVAL);
        pg_lakesoul_catalog(
            client,
            LakeSoulProviderOptions::default(),
            namespace,
            snapshot,
        )
    }

    #[test]
    fn catalog_lists_from_the_given_snapshot() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let client = Arc::new(
            runtime
                .block_on(async { MetaDataClient::from_env().await })
                .expect("metadata client"),
        );
        let snapshot =
            CatalogSnapshot::new(Arc::clone(&client), DEFAULT_CATALOG_REFRESH_INTERVAL);
        let catalog = pg_lakesoul_catalog(
            client,
            LakeSoulProviderOptions::default(),
            "default",
            Arc::clone(&snapshot),
        );
        assert!(Arc::ptr_eq(catalog.public_schema.snapshot(), &snapshot));
    }

    #[test]
    fn catalog_names_list_all_visible_databases() {
        let list = PgDatabaseCatalogList::new(
            "sales".to_string(),
            namespaces(&["sales", "finance", "default"]),
            fake_catalog(),
        );
        assert_eq!(
            list.catalog_names(),
            vec![
                "default".to_string(),
                "finance".to_string(),
                "sales".to_string()
            ]
        );
    }

    #[test]
    fn current_database_resolves_real_catalog_others_marker() {
        let list = PgDatabaseCatalogList::new(
            "sales".to_string(),
            namespaces(&["sales", "finance"]),
            fake_catalog(),
        );

        let current = list.catalog("sales").expect("current catalog");
        assert_eq!(current.schema_names(), vec![PUBLIC_SCHEMA.to_string()]);

        let marker = list.catalog("finance").expect("marker catalog");
        assert!(
            marker.is::<EmptyDatabaseCatalog>(),
            "other databases resolve to the marker"
        );
        assert!(marker.schema_names().is_empty());
        assert!(list.catalog("unknown").is_none());
    }

    #[test]
    fn register_catalog_only_accepts_current_database() {
        let list = PgDatabaseCatalogList::new(
            "sales".to_string(),
            namespaces(&["sales", "finance"]),
            fake_catalog(),
        );

        assert!(
            list.register_catalog("finance".to_string(), fake_catalog())
                .is_none(),
            "registering into a marker database must be ignored"
        );
        assert!(
            list.register_catalog("sales".to_string(), fake_catalog())
                .is_some()
        );
    }

    #[test]
    fn marker_catalog_rejects_schema_writes() {
        let marker = EmptyDatabaseCatalog;
        assert!(marker.schema_names().is_empty());
        assert!(marker.schema(PUBLIC_SCHEMA).is_none());
        let err = marker
            .register_schema(PUBLIC_SCHEMA, fake_schema())
            .unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
    }

    #[test]
    fn pg_lakesoul_catalog_pins_public_schema_and_rejects_others() {
        let catalog = real_catalog("sales");

        assert_eq!(catalog.schema_names(), vec![PUBLIC_SCHEMA.to_string()]);
        assert!(catalog.schema(PUBLIC_SCHEMA).is_some());
        // Other namespaces are separate PG databases, not schemas.
        assert!(catalog.schema("finance").is_none());

        let err = catalog
            .register_schema(PUBLIC_SCHEMA, fake_schema())
            .unwrap_err();
        assert!(matches!(err, DataFusionError::NotImplemented(_)), "{err}");
        assert!(catalog.deregister_schema(PUBLIC_SCHEMA, true).is_err());
    }

    #[test]
    fn pg_catalog_registers_as_virtual_schema() {
        let catalog = real_catalog("sales");
        catalog
            .register_schema("pg_catalog", fake_schema())
            .expect("pg_catalog registration");
        assert_eq!(
            catalog.schema_names(),
            vec!["pg_catalog".to_string(), PUBLIC_SCHEMA.to_string()]
        );
        assert!(catalog.schema("pg_catalog").is_some());
    }
}
