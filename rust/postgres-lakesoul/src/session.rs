// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use jiff::tz::TimeZone;
use parking_lot::RwLock;
use rootcause::bail;

use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::datafusion_pg_catalog::{
    PgCatalogOptions, setup_pg_catalog_with_options,
};
use lakesoul_datafusion::catalog::LakeSoulProviderOptions;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use lakesoul_metadata::MetaDataClientRef;
use rootcause::Report;
use tracing::info;

use crate::catalog::{PUBLIC_SCHEMA, PgDatabaseCatalogList, pg_lakesoul_catalog};

/// Identity resolved for one PostgreSQL connection during startup.
#[derive(Debug, Clone, Default)]
pub struct SessionIdentity {
    pub user: String,
    pub database: String,
}

/// PostgreSQL connection-local settings.
#[derive(Debug, Clone)]
pub struct SessionSettings {
    /// PostgreSQL search path, most significant schema first.
    pub search_path: Vec<String>,
    /// Session time zone applied to DataFusion execution.
    pub time_zone: TimeZone,
    pub statement_timeout: Option<Duration>,
}

impl Default for SessionSettings {
    fn default() -> Self {
        // default is utc
        let tz = TimeZone::try_system().unwrap_or(TimeZone::UTC);
        Self {
            search_path: vec![PUBLIC_SCHEMA.to_string(), "pg_catalog".to_string()],
            time_zone: tz,
            statement_timeout: None,
        }
    }
}

impl SessionSettings {
    /// Initial DataFusion schema derived from the PostgreSQL search path.
    /// Full multi-schema resolution is handled by the PG query layer.
    pub fn initial_schema(&self) -> &str {
        self.search_path
            .iter()
            .map(String::as_str)
            .find(|schema| *schema != "pg_catalog")
            .unwrap_or(PUBLIC_SCHEMA)
    }
}

/// State owned by one PostgreSQL connection.
pub struct PgSession {
    pub identity: SessionIdentity,
    pub context: Arc<SessionContext>,
    pub settings: RwLock<SessionSettings>,
}

impl PgSession {
    pub fn new(
        identity: SessionIdentity,
        context: Arc<SessionContext>,
        settings: SessionSettings,
    ) -> Self {
        Self {
            identity,
            context,
            settings: RwLock::new(settings),
        }
    }
}

type Result<T, E = Report> = std::result::Result<T, E>;

/// PostgreSQL-specific adapter around the generic LakeSoul session factory.
///
/// It maps connection-local PG settings to DataFusion session options and
/// installs the PostgreSQL database semantics:
///
/// - the PG database parameter names the LakeSoul namespace the connection
///   attaches to; the session's default catalog and schema are that
///   database and its `public` schema;
/// - a connection-level [`PgDatabaseCatalogList`] lists every visible
///   namespace as a database (empty marker catalogs outside the current
///   one), so `pg_catalog.pg_database` reports them without exposing
///   cross-database objects;
/// - `pg_catalog` plus the PostgreSQL compatibility functions are installed
///   only into the current database's catalog.
pub struct PgSessionFactory {
    meta_client: MetaDataClientRef,
    base: LakeSoulSessionFactory,
    auth_manager: Arc<AuthManager>,
    catalog_options: PgCatalogOptions,
}

impl PgSessionFactory {
    pub fn new(
        meta_client: MetaDataClientRef,
        args: &CoreArgs,
        auth_manager: Arc<AuthManager>,
    ) -> Result<Self> {
        let base = LakeSoulSessionFactory::new(Arc::clone(&meta_client), args)?;
        Ok(Self {
            meta_client,
            base,
            auth_manager,
            catalog_options: PgCatalogOptions {
                include_synthetic_postgres_database: false,
            },
        })
    }

    pub async fn create_session(
        &self,
        identity: SessionIdentity,
        settings: &SessionSettings,
    ) -> Result<Arc<PgSession>> {
        // Namespace visibility is snapshotted once per connection; the sync
        // catalog interfaces later read this snapshot without blocking.
        let namespaces = self.meta_client.get_all_namespace().await?;
        let visible_databases: BTreeSet<String> = namespaces
            .into_iter()
            .map(|namespace| namespace.namespace)
            .collect();
        if !visible_databases.contains(&identity.database) {
            bail!("database \"{}\" does not exist", identity.database);
        }

        let context = self.base.create_session(&LakeSoulSessionOptions {
            default_schema: settings.initial_schema().to_string(),
            time_zone: settings.time_zone.iana_name().map(str::to_owned),
        })?;

        let state = context.state();
        let catalog = pg_lakesoul_catalog(
            Arc::clone(&self.meta_client),
            LakeSoulProviderOptions::from_session(&state),
            &identity.database,
        );
        let catalog_list = Arc::new(PgDatabaseCatalogList::new(
            identity.database.clone(),
            visible_databases,
            catalog,
        ));

        let config = state
            .config()
            .clone()
            .with_default_catalog_and_schema(
                identity.database.clone(),
                PUBLIC_SCHEMA.to_string(),
            )
            .with_create_default_catalog_and_schema(false);
        let context = Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new_from_existing(state)
                .with_config(config)
                .with_catalog_list(catalog_list)
                .build(),
        ));

        setup_pg_catalog_with_options(
            &context,
            &identity.database,
            Arc::clone(&self.auth_manager),
            self.catalog_options,
        )?;

        let session = Arc::new(PgSession::new(identity, context, settings.clone()));
        {
            let settings = session.settings.read();
            info!(
                user = %session.identity.user,
                database = %session.identity.database,
                time_zone = ?settings.time_zone.iana_name().unwrap_or("None"),
                statement_timeout = ?settings.statement_timeout,
                "created PostgreSQL session"
            );
        }
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_schema_skips_pg_catalog() {
        let settings = SessionSettings {
            search_path: vec!["pg_catalog".to_string(), "tenant_a".to_string()],
            ..Default::default()
        };
        assert_eq!(settings.initial_schema(), "tenant_a");
    }

    #[test]
    fn initial_schema_falls_back_to_public() {
        let settings = SessionSettings {
            search_path: vec!["pg_catalog".to_string()],
            ..Default::default()
        };
        assert_eq!(settings.initial_schema(), PUBLIC_SCHEMA);
    }
}
