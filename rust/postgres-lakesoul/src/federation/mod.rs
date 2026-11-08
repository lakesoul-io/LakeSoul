// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process-wide foreign-server registry and ADBC table providers.

mod metadata;
mod parser;

pub(crate) use parser::{FederationStatementHook, rewrite};

use std::collections::BTreeMap;
use std::sync::Arc;

use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Driver, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};
use arrow_array::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use datafusion_table_providers_adbc::pool::ADBCPool;
use datafusion_table_providers_common::sql::db_connection_pool::DbConnectionPool;
use datafusion_table_providers_common::sql::sql_provider_datafusion::SqlTable;
use lakesoul_metadata::MetaDataClientRef;
use parking_lot::RwLock;
use r2d2_adbc::AdbcConnectionManager;
use rootcause::{bail, report};
use url::Url;

use crate::Result;
use metadata::{
    FederationData, FederationMetadata, ServerDefinition, UserMappingDefinition,
};

type AdbcPool = ADBCPool<ManagedDatabase>;
type AdbcPoolDyn = dyn DbConnectionPool<
        r2d2::PooledConnection<AdbcConnectionManager<ManagedDatabase>>,
        RecordBatch,
    > + Send
    + Sync;

#[derive(Default)]
struct RuntimeState {
    data: FederationData,
    /// Pools are keyed by server and PostgreSQL user because the
    /// user-mapping credentials are part of the ADBC URI.
    pools: BTreeMap<(String, String), Arc<AdbcPool>>,
}

/// Shared federation catalog and ADBC pool cache.
///
/// DDL handlers update the metadata and call [`refresh`](Self::refresh) so the
/// next lookup in every connection sees the new server or table. A server
/// definition contains credentials, so plans built from foreign tables carry
/// only the server name.
pub struct FederationRuntime {
    metadata: FederationMetadata,
    state: RwLock<RuntimeState>,
}

impl FederationRuntime {
    pub async fn new(meta: &MetaDataClientRef) -> Result<Self> {
        let pool = meta.pooled_client();
        let pool = pool.lock().await.clone();
        let runtime = Self {
            metadata: FederationMetadata::new(pool),
            state: RwLock::new(RuntimeState::default()),
        };
        runtime.load().await?;
        Ok(runtime)
    }

    pub async fn load(&self) -> Result<()> {
        self.metadata.init_schema().await?;
        let data = self.metadata.load().await?;
        self.state.write().data = data;
        Ok(())
    }

    /// Reload definitions and drop cached pools so changed credentials or
    /// server options take effect on the next query.
    pub async fn refresh(&self) -> Result<()> {
        let data = self.metadata.load().await?;
        let mut state = self.state.write();
        state.data = data;
        state.pools.clear();
        Ok(())
    }

    pub fn metadata(&self) -> &FederationMetadata {
        &self.metadata
    }

    pub fn foreign_names(&self, database: &str) -> Vec<String> {
        self.state
            .read()
            .data
            .tables
            .keys()
            .filter(|(db, schema, _)| db == database && schema == "public")
            .map(|(_, _, name)| name.clone())
            .collect()
    }

    pub fn is_foreign(&self, database: &str, name: &str) -> bool {
        self.state.read().data.tables.contains_key(&(
            database.to_string(),
            "public".to_string(),
            name.to_string(),
        ))
    }

    pub fn provider(
        &self,
        database: &str,
        user: &str,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let (table, server, mapping) = {
            let state = self.state.read();
            let key = (database.to_string(), "public".to_string(), name.to_string());
            let Some(table) = state.data.tables.get(&key).cloned() else {
                return Ok(None);
            };
            let Some(server) = state.data.servers.get(&table.server).cloned() else {
                bail!(
                    "foreign table {}.{} references unknown server {:?}",
                    table.local_schema,
                    table.local_name,
                    table.server
                );
            };
            let mapping = state
                .data
                .mappings
                .get(&(server.name.clone(), user.to_string()))
                .or_else(|| {
                    state
                        .data
                        .mappings
                        .get(&(server.name.clone(), "PUBLIC".to_string()))
                })
                .cloned();
            (table, server, mapping)
        };
        let pool = Self::pool_for(
            &mut self.state.write().pools,
            &server,
            mapping.as_ref(),
            user,
        )?;
        let pool: Arc<AdbcPoolDyn> = pool;
        let remote = format!("{}.{}", table.remote_schema, table.remote_table);
        let sql_table = SqlTable::new_with_schema(
            "federation",
            &pool,
            Arc::clone(&table.schema),
            TableReference::from(remote.as_str()),
        )
        .with_dialect(Arc::new(PostgreSqlDialect {}));
        Ok(Some(Arc::new(sql_table)))
    }

    fn pool_for(
        pools: &mut BTreeMap<(String, String), Arc<AdbcPool>>,
        server: &ServerDefinition,
        mapping: Option<&UserMappingDefinition>,
        user: &str,
    ) -> Result<Arc<AdbcPool>> {
        let key = (server.name.clone(), user.to_string());
        if let Some(pool) = pools.get(&key) {
            return Ok(Arc::clone(pool));
        }
        let uri = server_uri(server, mapping)?;
        if server.driver.contains('/') || server.driver.contains('\\') {
            bail!(
                "federation server {:?} has an unsafe driver name {:?}; \
                 configure the ADBC driver by name, not by path",
                server.name,
                server.driver
            );
        }
        let mut driver = ManagedDriver::load_from_name(
            &server.driver,
            None,
            AdbcVersion::default(),
            LOAD_FLAG_DEFAULT,
            None,
        )
        .map_err(|error| {
            report!(
                "load ADBC driver {} for federation server {}: {error}",
                server.driver,
                server.name
            )
        })?;
        let database = driver
            .new_database_with_opts([(OptionDatabase::Uri, uri.into())])
            .map_err(|error| {
                report!(
                    "open ADBC database for federation server {}: {error}",
                    server.name
                )
            })?;
        let pool = Arc::new(AdbcPool::new(database, None).map_err(|error| {
            report!(
                "create ADBC connection pool for federation server {}: {error}",
                server.name
            )
        })?);
        pools.insert(key, Arc::clone(&pool));
        Ok(pool)
    }
}

fn server_uri(
    server: &ServerDefinition,
    mapping: Option<&UserMappingDefinition>,
) -> Result<String> {
    let mut options = server.options.clone();
    if let Some(mapping) = mapping {
        options.extend(mapping.options.clone());
    }
    if let Some(uri) = options.remove("uri") {
        return Ok(uri);
    }
    let host = options
        .remove("host")
        .ok_or_else(|| report!("federation server {} has no host option", server.name))?;
    let dbname = options.remove("dbname").ok_or_else(|| {
        report!("federation server {} has no dbname option", server.name)
    })?;
    let port = options.remove("port").unwrap_or_else(|| "5432".to_string());
    let port = port.parse::<u16>().map_err(|_| {
        report!(
            "federation server {} has invalid port {:?}",
            server.name,
            port
        )
    })?;
    let user = options
        .remove("user")
        .ok_or_else(|| report!("federation server {} has no user option", server.name))?;
    let password = options.remove("password");
    let mut url = Url::parse("postgresql://localhost")
        .map_err(|error| report!("build federation URI: {error}"))?;
    url.set_host(Some(&host))
        .map_err(|error| report!("federation server {} host: {error}", server.name))?;
    url.set_port(Some(port))
        .map_err(|_| report!("federation server {} has invalid port", server.name))?;
    url.set_path(&format!("/{dbname}"));
    url.set_username(&user)
        .map_err(|_| report!("federation user mapping has an invalid user"))?;
    if let Some(password) = password {
        url.set_password(Some(&password))
            .map_err(|_| report!("federation user mapping has an invalid password"))?;
    }
    Ok(url.to_string())
}
