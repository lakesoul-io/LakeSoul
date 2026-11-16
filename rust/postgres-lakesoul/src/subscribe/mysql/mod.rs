// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Native MySQL binlog subscriber.

use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::memory_pool::MemoryPool;
use lakesoul_metadata::MetaDataClientRef;
use url::Url;

use super::{SchemaEvolution, SnapshotMode, TableMapping, create_memory_pool};
use crate::Result;
use crate::subscribe::state::{MySqlStateRecord, SubscriptionRecord};

pub(crate) mod binlog;
pub(crate) mod catalog;
pub(crate) mod plan;
pub(crate) mod runner;
pub(crate) mod sink;
pub(crate) mod snapshot;
pub(crate) mod types;

pub use catalog::GtidMode;
pub use snapshot::SnapshotLocking;

#[derive(Debug, Clone)]
pub struct MySqlSubscribeConfig {
    pub subscription_id: String,
    pub source_url: String,
    pub source_database: String,
    pub server_id: u32,
    pub gtid_mode: GtidMode,
    pub snapshot: SnapshotMode,
    pub snapshot_locking: SnapshotLocking,
    pub tables: Vec<TableMapping>,
    pub auto_create: bool,
    pub schema_evolution: SchemaEvolution,
    pub warehouse_prefix: Option<String>,
    pub cdc_column: String,
    pub commit_rows: usize,
    pub commit_interval: Duration,
    pub snapshot_batch_rows: usize,
    pub snapshot_parallelism: usize,
    pub memory_pool: Arc<dyn MemoryPool>,
}

impl MySqlSubscribeConfig {
    pub fn source_table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.source_table.clone())
            .collect()
    }
}

pub async fn run_from_record(
    record: &SubscriptionRecord,
    state: &MySqlStateRecord,
    meta: MetaDataClientRef,
) -> Result<()> {
    let table = record
        .tables
        .first()
        .ok_or_else(|| rootcause::report!("MySQL subscription has no table mappings"))?;
    let snapshot = match table.snapshot_mode.as_str() {
        "none" => SnapshotMode::None,
        "managed" => SnapshotMode::Managed,
        other => {
            return Err(rootcause::report!(
                "MySQL subscription has unknown snapshot mode {other:?}"
            ));
        }
    };
    let snapshot_locking =
        SnapshotLocking::parse(&state.snapshot_locking).ok_or_else(|| {
            rootcause::report!(
                "MySQL subscription has unknown snapshot_locking {:?}",
                state.snapshot_locking
            )
        })?;
    let gtid_mode = GtidMode::parse(&state.gtid_mode).ok_or_else(|| {
        rootcause::report!(
            "MySQL subscription has unknown gtid_mode {:?}",
            state.gtid_mode
        )
    })?;
    let schema_evolution = match record.schema_evolution.as_str() {
        "none" => SchemaEvolution::None,
        "auto" => SchemaEvolution::Auto,
        other => {
            return Err(rootcause::report!(
                "MySQL subscription has unknown schema_evolution {other:?}"
            ));
        }
    };
    let memory_pool = create_memory_pool(record.snapshot_memory_bytes);
    let tables = record
        .tables
        .iter()
        .map(|table| TableMapping {
            source_schema: table.source_schema.clone(),
            source_table: table.source_table.clone(),
            target_namespace: table.target_namespace.clone(),
            target_table: table.target_table.clone(),
            primary_key: table.primary_key.clone(),
        })
        .collect();
    let config = MySqlSubscribeConfig {
        subscription_id: record.subscription_id.clone(),
        source_url: record.source_url.clone(),
        source_database: state.source_database.clone(),
        server_id: state.server_id,
        gtid_mode,
        snapshot,
        snapshot_locking,
        tables,
        auto_create: record.auto_create,
        schema_evolution,
        warehouse_prefix: record.warehouse_prefix.clone(),
        cdc_column: record.cdc_column.clone(),
        commit_rows: record.commit_rows,
        commit_interval: Duration::from_millis(record.commit_interval_ms),
        snapshot_batch_rows: record.snapshot_batch_rows,
        snapshot_parallelism: record.snapshot_parallelism,
        memory_pool,
    };
    runner::run(config, state.clone(), meta).await
}

pub fn database_from_url(url: &str) -> Result<String> {
    let parsed = Url::parse(url)
        .map_err(|error| rootcause::report!("invalid MySQL connection URL: {error}"))?;
    let database = parsed.path().trim_start_matches('/').trim_end_matches('/');
    if database.is_empty() {
        return Err(rootcause::report!(
            "MySQL connection URL must name a database, e.g. mysql://host/db"
        ));
    }
    Ok(database.to_string())
}
