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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::error::{DataFusionError, Result};
use lakesoul_datafusion::catalog::{LakeSoulNamespace, LakeSoulProviderOptions};
use lakesoul_metadata::MetaDataClientRef;
use parking_lot::RwLock;

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
    public_schema: Arc<LakeSoulNamespace>,
    virtual_schemas: RwLock<BTreeMap<String, Arc<dyn SchemaProvider>>>,
}

impl PgLakeSoulCatalog {
    pub fn new(public_schema: Arc<LakeSoulNamespace>) -> Self {
        Self {
            public_schema,
            virtual_schemas: RwLock::new(BTreeMap::new()),
        }
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
pub fn pg_lakesoul_catalog(
    client: MetaDataClientRef,
    provider_options: LakeSoulProviderOptions,
    namespace: &str,
) -> Arc<PgLakeSoulCatalog> {
    let public_schema =
        Arc::new(LakeSoulNamespace::new(client, provider_options, namespace));
    Arc::new(PgLakeSoulCatalog::new(public_schema))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
    use datafusion::error::{DataFusionError, Result};
    use lakesoul_datafusion::catalog::LakeSoulProviderOptions;
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

    /// Constructs the real catalog backed by live metadata; skipped without
    /// a PostgreSQL instance (`LAKESOUL_PG_URL` / `lakesoul_home`).
    fn meta_available() -> bool {
        std::env::var("LAKESOUL_PG_URL").is_ok() || std::env::var("lakesoul_home").is_ok()
    }

    fn real_catalog(namespace: &str) -> Arc<PgLakeSoulCatalog> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let client = runtime
            .block_on(async { MetaDataClient::from_env().await })
            .expect("metadata client");
        pg_lakesoul_catalog(
            Arc::new(client),
            LakeSoulProviderOptions::default(),
            namespace,
        )
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
        if !meta_available() {
            return;
        }
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
        if !meta_available() {
            return;
        }
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
