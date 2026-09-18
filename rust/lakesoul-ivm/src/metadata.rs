// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL metadata owned by the IVM runtime (schema `ivm`).
//!
//! The tables live next to the LakeSoul metadata in the same database but are
//! created and used only by this crate:
//!
//! * `ivm.views` stores the serialized [`crate::ViewSpec`] of every view;
//! * `ivm.cursors` stores, per `(view, source, partition)`, the last source
//!   version consumed by a refresh. Cursors are version-based so commits that
//!   share a millisecond cannot be skipped or read twice.

use lakesoul_metadata::{PooledClient, QueryType, create_connection, pg_config_from_env};
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// DDL for the IVM metadata schema. Idempotent and safe to run concurrently
/// (PostgreSQL `IF NOT EXISTS` is not race free, so each statement tolerates
/// the duplicate created by a concurrent initializer).
const IVM_SCHEMA_DDL: &str = "
do $$ begin
    create schema if not exists ivm;
exception when duplicate_schema then null;
end $$;

do $$ begin
    create table if not exists ivm.views (
        view_id             text primary key,
        spec                jsonb not null,
        refresh_interval_ms bigint not null default 0,
        status              text not null default 'active',
        created_at          bigint not null
    );
exception when duplicate_table then null;
end $$;

do $$ begin
    create table if not exists ivm.cursors (
        view_id         text not null,
        source_table_id text not null,
        partition_desc  text not null,
        last_version    bigint not null,
        last_timestamp  bigint not null,
        primary key (view_id, source_table_id, partition_desc)
    );
exception when duplicate_table then null;
end $$;
";

/// The persisted cursor of one source partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// The source table id the cursor belongs to.
    pub source_table_id: String,
    /// The source partition description.
    pub partition_desc: String,
    /// The last partition version consumed by the view.
    pub last_version: i64,
    /// `partition_info.timestamp` of `last_version`.
    pub last_timestamp: i64,
}

/// The `ivm` schema access layer.
pub struct IvmMetadata {
    client: PooledClient,
}

impl IvmMetadata {
    /// Connect using the same `LAKESOUL_PG_*` configuration as the metadata
    /// client.
    pub async fn from_env() -> Result<Self> {
        let config = pg_config_from_env(
            lakesoul_metadata::PRIMARY_URL_PROP_KEY,
            lakesoul_metadata::PRIMARY_URL_ENV_KEY,
        )?;
        let client = create_connection(config, None).await?;
        Ok(Self { client })
    }

    /// Create the `ivm` schema and tables if they do not exist.
    pub async fn init_schema(&self) -> Result<()> {
        self.client
            .batch_execute(IVM_SCHEMA_DDL, QueryType::RW)
            .await?;
        Ok(())
    }

    /// Insert or update a registered view.
    pub async fn upsert_view(
        &self,
        view_id: &str,
        spec: &serde_json::Value,
        refresh_interval_ms: i64,
    ) -> Result<()> {
        let (conn, statement) = self
            .client
            .prepare_cached(
                "insert into ivm.views(view_id, spec, refresh_interval_ms, created_at)
                 values ($1::TEXT, $2::JSONB, $3::BIGINT, $4::BIGINT)
                 on conflict (view_id) do update
                 set spec = excluded.spec,
                     refresh_interval_ms = excluded.refresh_interval_ms",
                QueryType::RW,
            )
            .await?;
        conn.execute(
            &statement,
            &[&view_id, spec, &refresh_interval_ms, &crate::now_ms()],
        )
        .await?;
        Ok(())
    }

    /// Read a registered view spec.
    pub async fn get_view_spec(
        &self,
        view_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let row = self
            .client
            .query_opt(
                "select spec from ivm.views where view_id = $1::TEXT",
                QueryType::RO,
                &[&view_id],
            )
            .await?;
        Ok(row.map(|row| row.get(0)))
    }

    /// Insert or update the cursor of one source partition.
    pub async fn upsert_cursor(
        &self,
        view_id: &str,
        source_table_id: &str,
        partition_desc: &str,
        last_version: i64,
        last_timestamp: i64,
    ) -> Result<()> {
        let (conn, statement) = self
            .client
            .prepare_cached(
                "insert into ivm.cursors(
                     view_id, source_table_id, partition_desc, last_version, last_timestamp)
                 values ($1::TEXT, $2::TEXT, $3::TEXT, $4::BIGINT, $5::BIGINT)
                 on conflict (view_id, source_table_id, partition_desc) do update
                 set last_version = excluded.last_version,
                     last_timestamp = excluded.last_timestamp",
                QueryType::RW,
            )
            .await?;
        conn.execute(
            &statement,
            &[
                &view_id,
                &source_table_id,
                &partition_desc,
                &last_version,
                &last_timestamp,
            ],
        )
        .await?;
        Ok(())
    }

    /// List all cursors of a view.
    pub async fn list_cursors(&self, view_id: &str) -> Result<Vec<Cursor>> {
        let rows = self
            .client
            .query(
                "select source_table_id, partition_desc, last_version, last_timestamp
                 from ivm.cursors where view_id = $1::TEXT",
                QueryType::RO,
                &[&view_id],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| Cursor {
                source_table_id: row.get(0),
                partition_desc: row.get(1),
                last_version: row.get(2),
                last_timestamp: row.get(3),
            })
            .collect())
    }

    /// Delete a view and its cursors.
    pub async fn delete_view(&self, view_id: &str) -> Result<()> {
        self.client
            .execute(
                "delete from ivm.cursors where view_id = $1::TEXT",
                QueryType::RW,
                &[&view_id],
            )
            .await?;
        self.client
            .execute(
                "delete from ivm.views where view_id = $1::TEXT",
                QueryType::RW,
                &[&view_id],
            )
            .await?;
        Ok(())
    }
}
