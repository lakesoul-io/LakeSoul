// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent definitions for PostgreSQL foreign tables backed by ADBC.
//!
//! PostgreSQL stores foreign servers, user mappings and foreign tables in its
//! own catalogs. The PostgreSQL front end has no such catalogs, so it keeps
//! the same three objects in the metadata PostgreSQL under `federation`.
//! Credentials stay in the metadata database and are referenced by a server
//! name at query time; plan payloads only carry that name.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use chrono::Utc;
use lakesoul_common::ser::arrow_java::{
    schema_from_metadata_str, schema_to_metadata_str,
};
use lakesoul_datafusion::federation::DialectKind;
use lakesoul_metadata::{PooledClient, QueryType};
use rootcause::report;

use crate::Result;

/// Idempotent runtime DDL. Production deployments also apply
/// `script/metadata-migrations/V4000003__federation.sql` and
/// `V4000004__federation_dialect.sql`.
const FEDERATION_SCHEMA_DDL: &str = r#"
create schema if not exists federation;

create table if not exists federation.servers
(
    server_name text primary key,
    backend     text not null default 'adbc',
    driver      text not null default 'postgresql',
    dialect     text not null default 'postgres',
    options     text not null default '{}',
    created_at  bigint not null,
    updated_at  bigint not null
);

alter table federation.servers
    add column if not exists backend text not null default 'adbc';

alter table federation.servers
    add column if not exists dialect text not null default 'postgres';

-- Backfill rows created before the dialect column existed. The old code
-- always generated PostgreSQL SQL, so anything else was unusable anyway.
update federation.servers
set dialect = case driver
    when 'mysql' then 'mysql'
    when 'sqlite' then 'sqlite'
    when 'duckdb' then 'duckdb'
    when 'bigquery' then 'bigquery'
    when 'snowflake' then 'snowflake'
    when 'trino' then 'generic'
    else dialect
end
where dialect = 'postgres' and driver not in ('postgresql', 'postgres');

create table if not exists federation.user_mappings
(
    server_name text not null references federation.servers (server_name) on delete cascade,
    pg_user     text not null,
    options     text not null default '{}',
    updated_at  bigint not null,
    primary key (server_name, pg_user)
);

create table if not exists federation.foreign_tables
(
    database       text not null,
    local_schema   text not null,
    local_name     text not null,
    server_name    text not null references federation.servers (server_name) on delete cascade,
    remote_schema  text not null,
    remote_table   text not null,
    schema_json    text not null,
    options        text not null default '{}',
    created_at     bigint not null,
    updated_at     bigint not null,
    primary key (database, local_schema, local_name)
);
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerBackend {
    Adbc,
    ExternalFile,
}

impl ServerBackend {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "adbc" => Some(Self::Adbc),
            "external_file" => Some(Self::ExternalFile),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Adbc => "adbc",
            Self::ExternalFile => "external_file",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerDefinition {
    pub name: String,
    pub backend: ServerBackend,
    pub driver: String,
    pub dialect: DialectKind,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct UserMappingDefinition {
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ForeignTableDefinition {
    pub database: String,
    pub local_schema: String,
    pub local_name: String,
    pub server: String,
    pub remote_schema: String,
    pub remote_table: String,
    pub schema: SchemaRef,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct FederationData {
    pub servers: BTreeMap<String, ServerDefinition>,
    pub mappings: BTreeMap<(String, String), UserMappingDefinition>,
    pub tables: BTreeMap<(String, String, String), ForeignTableDefinition>,
}

#[derive(Clone)]
pub struct FederationMetadata {
    client: PooledClient,
}

impl FederationMetadata {
    pub fn new(client: PooledClient) -> Self {
        Self { client }
    }

    pub async fn init_schema(&self) -> Result<()> {
        self.client
            .batch_execute(FEDERATION_SCHEMA_DDL, QueryType::RW)
            .await
            .map_err(|error| report!("create federation metadata schema: {error}"))
    }

    pub async fn load(&self) -> Result<FederationData> {
        let server_rows = self
            .client
            .query(
                "select server_name, backend, driver, dialect, options \
                 from federation.servers order by server_name",
                QueryType::RW,
                &[],
            )
            .await
            .map_err(|error| report!("read federation servers: {error}"))?;
        let mut servers = BTreeMap::new();
        for row in server_rows {
            let name: String = row.get(0);
            let backend_name: String = row.get(1);
            let backend = ServerBackend::parse(&backend_name).ok_or_else(|| {
                report!(
                    "federation server {name} has unknown backend {backend_name:?}; \
                     supported backends: adbc, external_file"
                )
            })?;
            let driver: String = row.get(2);
            let dialect_name: String = row.get(3);
            let dialect = DialectKind::parse(&dialect_name).ok_or_else(|| {
                report!(
                    "federation server {name} has unknown dialect \
                     {dialect_name:?}; supported dialects: {}",
                    DialectKind::supported_names()
                )
            })?;
            let options: String = row.get(4);
            servers.insert(
                name.clone(),
                ServerDefinition {
                    name,
                    backend,
                    driver,
                    dialect,
                    options: parse_options(&options)?,
                },
            );
        }

        let mapping_rows = self
            .client
            .query(
                "select server_name, pg_user, options from federation.user_mappings",
                QueryType::RW,
                &[],
            )
            .await
            .map_err(|error| report!("read federation user mappings: {error}"))?;
        let mut mappings = BTreeMap::new();
        for row in mapping_rows {
            let server: String = row.get(0);
            let user: String = row.get(1);
            let options: String = row.get(2);
            mappings.insert(
                (server, user),
                UserMappingDefinition {
                    options: parse_options(&options)?,
                },
            );
        }

        let table_rows = self
            .client
            .query(
                "select database, local_schema, local_name, server_name, \
                        remote_schema, remote_table, schema_json, options \
                 from federation.foreign_tables \
                 order by database, local_schema, local_name",
                QueryType::RW,
                &[],
            )
            .await
            .map_err(|error| report!("read federation foreign tables: {error}"))?;
        let mut tables = BTreeMap::new();
        for row in table_rows {
            let database: String = row.get(0);
            let local_schema: String = row.get(1);
            let local_name: String = row.get(2);
            let server: String = row.get(3);
            let remote_schema: String = row.get(4);
            let remote_table: String = row.get(5);
            let schema_json: String = row.get(6);
            let options: String = row.get(7);
            let schema = schema_from_metadata_str(&schema_json)
                .map_err(|error| report!("parse foreign table schema: {error}"))?;
            tables.insert(
                (database.clone(), local_schema.clone(), local_name.clone()),
                ForeignTableDefinition {
                    database,
                    local_schema,
                    local_name,
                    server,
                    remote_schema,
                    remote_table,
                    schema: Arc::new(schema),
                    options: parse_options(&options)?,
                },
            );
        }

        Ok(FederationData {
            servers,
            mappings,
            tables,
        })
    }

    pub async fn create_server(
        &self,
        name: &str,
        backend: ServerBackend,
        driver: &str,
        dialect: DialectKind,
        options: &BTreeMap<String, String>,
        if_not_exists: bool,
    ) -> Result<bool> {
        let now = now_ms();
        // The driver and dialect have their own columns; keeping them in the
        // JSON would also forward them to the ADBC driver as unknown options.
        let mut options = options.clone();
        options.remove("driver");
        options.remove("dialect");
        let options = serde_json::to_string(&options)
            .map_err(|error| report!("serialize server options: {error}"))?;
        let backend = backend.name();
        let dialect = dialect.name();
        let sql = if if_not_exists {
            "insert into federation.servers \
                 (server_name, backend, driver, dialect, options, created_at, updated_at) \
             values ($1, $2, $3, $4, $5, $6, $6) on conflict (server_name) do nothing"
        } else {
            "insert into federation.servers \
                 (server_name, backend, driver, dialect, options, created_at, updated_at) \
             values ($1, $2, $3, $4, $5, $6, $6)"
        };
        let inserted = self
            .client
            .execute(
                sql,
                QueryType::RW,
                &[&name, &backend, &driver, &dialect, &options, &now],
            )
            .await
            .map_err(|error| report!("create federation server {name}: {error}"))?;
        Ok(inserted > 0)
    }

    pub async fn drop_server(&self, name: &str) -> Result<bool> {
        let deleted = self
            .client
            .execute(
                "delete from federation.servers where server_name = $1",
                QueryType::RW,
                &[&name],
            )
            .await
            .map_err(|error| report!("drop federation server {name}: {error}"))?;
        Ok(deleted > 0)
    }

    pub async fn create_user_mapping(
        &self,
        server: &str,
        user: &str,
        options: &BTreeMap<String, String>,
        if_not_exists: bool,
    ) -> Result<bool> {
        let now = now_ms();
        let options = serde_json::to_string(options)
            .map_err(|error| report!("serialize user mapping options: {error}"))?;
        let sql = if if_not_exists {
            "insert into federation.user_mappings \
                 (server_name, pg_user, options, updated_at) \
             values ($1, $2, $3, $4) \
             on conflict (server_name, pg_user) do nothing"
        } else {
            "insert into federation.user_mappings \
                 (server_name, pg_user, options, updated_at) \
             values ($1, $2, $3, $4)"
        };
        let inserted = self
            .client
            .execute(sql, QueryType::RW, &[&server, &user, &options, &now])
            .await
            .map_err(|error| report!("create federation user mapping: {error}"))?;
        Ok(inserted > 0)
    }

    pub async fn drop_user_mapping(&self, server: &str, user: &str) -> Result<bool> {
        let deleted = self
            .client
            .execute(
                "delete from federation.user_mappings \
                 where server_name = $1 and pg_user = $2",
                QueryType::RW,
                &[&server, &user],
            )
            .await
            .map_err(|error| report!("drop federation user mapping: {error}"))?;
        Ok(deleted > 0)
    }

    pub async fn create_foreign_table(
        &self,
        table: &ForeignTableDefinition,
        if_not_exists: bool,
    ) -> Result<bool> {
        let now = now_ms();
        let schema_json = schema_to_metadata_str(&table.schema);
        let options = serde_json::to_string(&table.options)
            .map_err(|error| report!("serialize foreign table options: {error}"))?;
        let sql = if if_not_exists {
            "insert into federation.foreign_tables \
                 (database, local_schema, local_name, server_name, remote_schema, \
                  remote_table, schema_json, options, created_at, updated_at) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9) \
             on conflict (database, local_schema, local_name) do nothing"
        } else {
            "insert into federation.foreign_tables \
                 (database, local_schema, local_name, server_name, remote_schema, \
                  remote_table, schema_json, options, created_at, updated_at) \
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)"
        };
        let inserted = self
            .client
            .execute(
                sql,
                QueryType::RW,
                &[
                    &table.database,
                    &table.local_schema,
                    &table.local_name,
                    &table.server,
                    &table.remote_schema,
                    &table.remote_table,
                    &schema_json,
                    &options,
                    &now,
                ],
            )
            .await
            .map_err(|error| {
                report!(
                    "create foreign table {}.{}.{}: {error}",
                    table.database,
                    table.local_schema,
                    table.local_name
                )
            })?;
        Ok(inserted > 0)
    }

    pub async fn drop_foreign_table(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<bool> {
        let deleted = self
            .client
            .execute(
                "delete from federation.foreign_tables \
                 where database = $1 and local_schema = $2 and local_name = $3",
                QueryType::RW,
                &[&database, &schema, &name],
            )
            .await
            .map_err(|error| report!("drop foreign table {schema}.{name}: {error}"))?;
        Ok(deleted > 0)
    }

    pub async fn foreign_tables_for_server(
        &self,
        server: &str,
    ) -> Result<Vec<(String, String, String)>> {
        let rows = self
            .client
            .query(
                "select database, local_schema, local_name \
                 from federation.foreign_tables where server_name = $1",
                QueryType::RW,
                &[&server],
            )
            .await
            .map_err(|error| {
                report!("read tables for federation server {server}: {error}")
            })?;
        Ok(rows
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect())
    }
}

fn parse_options(text: &str) -> Result<BTreeMap<String, String>> {
    serde_json::from_str(text)
        .map_err(|error| report!("federation options are not a JSON object: {error}"))
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}
