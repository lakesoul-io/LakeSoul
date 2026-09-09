// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use lakesoul_datafusion::session::DEFAULT_SCHEMA;
use parking_lot::RwLock;

use datafusion::catalog::CatalogProvider;
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::datafusion_pg_catalog::{
    PgCatalogOptions, setup_pg_catalog_with_options,
};
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::session::{
    DEFAULT_CATALOG, LakeSoulSessionFactory, LakeSoulSessionOptions,
};
use lakesoul_metadata::MetaDataClientRef;
use rootcause::Report;
use tracing::info;

use crate::catalog::PgLakeSoulCatalog;

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
    pub time_zone: String,
    pub statement_timeout: Option<Duration>,
}

impl Default for SessionSettings {
    fn default() -> Self {
        Self {
            search_path: vec![DEFAULT_SCHEMA.to_string(), "pg_catalog".to_string()],
            time_zone: "UTC".to_string(),
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
            .unwrap_or(DEFAULT_SCHEMA)
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
/// It maps connection-local PG settings to DataFusion session options, wraps
/// the LakeSoul catalog with PostgreSQL virtual-schema support and installs
/// `pg_catalog` plus the PostgreSQL compatibility functions.
pub struct PgSessionFactory {
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
        let base = LakeSoulSessionFactory::new(meta_client, args)?
            .with_catalog_decorator(|catalog| {
                Arc::new(PgLakeSoulCatalog::new(catalog)) as Arc<dyn CatalogProvider>
            });
        Ok(Self {
            base,
            auth_manager,
            catalog_options: PgCatalogOptions {
                include_synthetic_postgres_database: false,
            },
        })
    }

    pub fn create_session(
        &self,
        identity: SessionIdentity,
        settings: &SessionSettings,
    ) -> Result<Arc<PgSession>> {
        let context = self.base.create_session(&LakeSoulSessionOptions {
            default_schema: settings.initial_schema().to_string(),
            time_zone: Some(settings.time_zone.clone()),
        })?;
        setup_pg_catalog_with_options(
            &context,
            DEFAULT_CATALOG,
            Arc::clone(&self.auth_manager),
            self.catalog_options,
        )?;

        let session = Arc::new(PgSession::new(identity, context, settings.clone()));
        {
            let settings = session.settings.read();
            info!(
                user = %session.identity.user,
                database = %session.identity.database,
                time_zone = %settings.time_zone,
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
    fn initial_schema_falls_back_to_default() {
        let settings = SessionSettings {
            search_path: vec!["pg_catalog".to_string()],
            ..Default::default()
        };
        assert_eq!(settings.initial_schema(), DEFAULT_SCHEMA);
    }
}
