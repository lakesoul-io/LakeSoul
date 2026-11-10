// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process-wide foreign-server registry and ADBC table providers.

mod metadata;
mod parser;

pub(crate) use parser::{FederationStatementHook, rewrite};

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use lakesoul_datafusion::federation::external_file::{
    ExternalFileStoreConfig, ExternalFileTableProvider,
};
use lakesoul_datafusion::federation::{
    FederationPool, clear_pools, open_pool, register_pool, table_provider,
};
use lakesoul_metadata::MetaDataClientRef;
use parking_lot::RwLock;
use rootcause::bail;

use crate::Result;
use metadata::{
    FederationData, FederationMetadata, ServerBackend, ServerDefinition,
    UserMappingDefinition,
};

#[derive(Default)]
struct RuntimeState {
    data: FederationData,
    /// Pools are keyed by server and PostgreSQL user because the
    /// user-mapping credentials are part of the ADBC connection.
    pools: BTreeMap<(String, String), FederationPool>,
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
        drop(state);
        clear_pools();
        Ok(())
    }

    pub fn metadata(&self) -> &FederationMetadata {
        &self.metadata
    }

    pub fn server_backend(&self, name: &str) -> Option<ServerBackend> {
        self.state
            .read()
            .data
            .servers
            .get(name)
            .map(|server| server.backend)
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
        let (table, server, mapping, mapping_user) = {
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
            let (mapping, mapping_user) =
                match mapping_for_user(&state.data, &server.name, user) {
                    Some((mapping, key)) => (Some(mapping), key),
                    None => (None, user.to_string()),
                };
            (table, server, mapping, mapping_user)
        };
        if server.backend == ServerBackend::ExternalFile {
            let store_config = ExternalFileStoreConfig::from_options(&server.options)
                .map_err(|error| {
                    rootcause::report!(
                        "configure external file server {}: {error}",
                        server.name
                    )
                })?;
            let provider = ExternalFileTableProvider::try_new(
                table.local_name.clone(),
                &table.options,
                Arc::clone(&table.schema),
                store_config,
            )
            .map_err(|error| {
                rootcause::report!(
                    "configure external file table {}.{}: {error}",
                    table.local_schema,
                    table.local_name
                )
            })?;
            return Ok(Some(Arc::new(provider)));
        }
        let dialect = server.dialect;
        let pool = Self::pool_for(
            &mut self.state.write().pools,
            &server,
            mapping.as_ref(),
            &mapping_user,
        )?;
        let server_key = format!("{}:{}", server.name, mapping_user);
        register_pool(&server_key, Arc::clone(&pool));
        let remote = format!("{}.{}", table.remote_schema, table.remote_table);
        Ok(Some(table_provider(
            server_key,
            pool,
            dialect,
            Arc::clone(&table.schema),
            TableReference::from(remote.as_str()),
            table.options.clone(),
        )))
    }

    fn pool_for(
        pools: &mut BTreeMap<(String, String), FederationPool>,
        server: &ServerDefinition,
        mapping: Option<&UserMappingDefinition>,
        mapping_user: &str,
    ) -> Result<FederationPool> {
        let key = (server.name.clone(), mapping_user.to_string());
        if let Some(pool) = pools.get(&key) {
            return Ok(Arc::clone(pool));
        }
        let pool = open_pool(
            &server.name,
            &server.driver,
            &server.options,
            mapping.map(|mapping| &mapping.options),
        )?;
        pools.insert(key, Arc::clone(&pool));
        Ok(pool)
    }
}

/// Find the user mapping that applies to `user`, and the key it is registered
/// under. `PUBLIC` is a shared mapping, so its key is `PUBLIC` rather than the
/// query user; workers load pools by exactly the keys found in metadata.
fn mapping_for_user(
    data: &FederationData,
    server: &str,
    user: &str,
) -> Option<(UserMappingDefinition, String)> {
    if let Some(mapping) = data.mappings.get(&(server.to_string(), user.to_string())) {
        return Some((mapping.clone(), user.to_string()));
    }
    data.mappings
        .get(&(server.to_string(), "PUBLIC".to_string()))
        .map(|mapping| (mapping.clone(), "PUBLIC".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_mapping_uses_its_own_registry_key() {
        let mut data = FederationData::default();
        data.mappings.insert(
            ("app".to_string(), "PUBLIC".to_string()),
            UserMappingDefinition {
                options: BTreeMap::new(),
            },
        );
        let (_, key) = mapping_for_user(&data, "app", "alice").expect("public mapping");
        assert_eq!(key, "PUBLIC");

        data.mappings.insert(
            ("app".to_string(), "alice".to_string()),
            UserMappingDefinition {
                options: BTreeMap::new(),
            },
        );
        let (_, key) = mapping_for_user(&data, "app", "alice").expect("exact mapping");
        assert_eq!(key, "alice");
    }
}
