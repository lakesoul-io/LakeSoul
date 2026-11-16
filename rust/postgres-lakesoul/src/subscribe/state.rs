// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Subscription definition and progress in the metadata PostgreSQL.
//!
//! The target table's commit ids remain the authoritative data watermark
//! (they are written atomically with the data). These rows add:
//!
//! * the source/target identity, so reusing a slot with another table or a
//!   recreated table fails instead of silently skipping data;
//! * `processed_lsn`, which also records transactions that touched no mapped
//!   table and therefore produced no target commit.

use std::collections::HashSet;

use chrono::Utc;
use lakesoul_metadata::{PooledClient, QueryType};
use rootcause::{bail, report};

use crate::Result;

#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub source_schema: String,
    pub source_table: String,
    pub source_relation_oid: u32,
    pub target_namespace: String,
    pub target_table: String,
    pub target_table_id: String,
    pub primary_key: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SubscriptionDefinition {
    pub subscription_id: String,
    pub source_kind: String,
    pub slot: String,
    pub publication: String,
    pub source_url: String,
    pub snapshot_mode: String,
    pub auto_create: bool,
    pub schema_evolution: String,
    pub warehouse_prefix: Option<String>,
    pub cdc_column: String,
    pub supervised: bool,
    pub commit_rows: usize,
    pub commit_interval_ms: u64,
    pub snapshot_batch_rows: usize,
    pub snapshot_batch_bytes: usize,
    pub snapshot_parallelism: usize,
    pub snapshot_memory_bytes: usize,
    pub tables: Vec<TableDefinition>,
}

#[derive(Debug, Clone)]
pub struct TableRecord {
    pub source_schema: String,
    pub source_table: String,
    pub source_relation_oid: u32,
    pub target_namespace: String,
    pub target_table: String,
    pub target_table_id: String,
    pub primary_key: Vec<String>,
    pub data_lsn: u64,
    pub state: String,
    pub snapshot_state: String,
    pub snapshot_mode: String,
    pub snapshot_lsn: Option<u64>,
    pub snapshot_started_at: Option<i64>,
    pub snapshot_completed_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SubscriptionRecord {
    pub subscription_id: String,
    pub source_kind: String,
    pub slot: String,
    pub publication: String,
    pub source_url: String,
    pub state: String,
    pub enabled: bool,
    pub auto_create: bool,
    pub schema_evolution: String,
    pub warehouse_prefix: Option<String>,
    pub cdc_column: String,
    pub supervised: bool,
    pub commit_rows: usize,
    pub commit_interval_ms: u64,
    pub snapshot_batch_rows: usize,
    pub snapshot_batch_bytes: usize,
    pub snapshot_parallelism: usize,
    pub snapshot_memory_bytes: usize,
    pub supervisor_pid: Option<i32>,
    pub log_path: Option<String>,
    pub last_error: Option<String>,
    pub tables: Vec<TableRecord>,
    pub processed_lsn: u64,
}

impl SubscriptionRecord {
    pub fn validate(&self, definition: &SubscriptionDefinition) -> Result<()> {
        if self.source_kind != definition.source_kind {
            bail!(
                "subscription state is for source kind {:?}, not {:?}",
                self.source_kind,
                definition.source_kind
            );
        }
        if definition.source_kind == "postgres"
            && (self.slot != definition.slot
                || self.publication != definition.publication)
        {
            bail!(
                "subscription state belongs to slot {:?} / publication {:?}, not {:?} / {:?}",
                self.slot,
                self.publication,
                definition.slot,
                definition.publication
            );
        }
        if definition.source_kind == "mysql" && self.source_url != definition.source_url {
            bail!(
                "subscription state belongs to source URL {:?}, not {:?}",
                self.source_url,
                definition.source_url
            );
        }
        if definition.tables.is_empty() {
            bail!("subscription definition has no table mappings");
        }
        if self.tables.len() != definition.tables.len() {
            bail!(
                "subscription state has {} table mappings, but the definition has {}",
                self.tables.len(),
                definition.tables.len()
            );
        }
        if self
            .tables
            .iter()
            .any(|table| table.snapshot_mode != definition.snapshot_mode)
        {
            bail!(
                "subscription state has a snapshot mode that does not match {:?}",
                definition.snapshot_mode
            );
        }
        if self.schema_evolution != definition.schema_evolution {
            bail!(
                "subscription state has schema_evolution={:?}, not {:?}",
                self.schema_evolution,
                definition.schema_evolution
            );
        }
        if self.cdc_column != definition.cdc_column {
            bail!(
                "subscription state has cdc_column={:?}, not {:?}",
                self.cdc_column,
                definition.cdc_column
            );
        }

        for expected in &definition.tables {
            let table = self
                .tables
                .iter()
                .find(|table| {
                    table.source_schema == expected.source_schema
                        && table.source_table == expected.source_table
                })
                .ok_or_else(|| {
                    report!(
                        "subscription state has no mapping for {}.{}",
                        expected.source_schema,
                        expected.source_table
                    )
                })?;
            if table.source_relation_oid != expected.source_relation_oid {
                bail!(
                    "source table {}.{} was recreated (relation oid {} -> {}); \
                     resnapshot is required",
                    expected.source_schema,
                    expected.source_table,
                    table.source_relation_oid,
                    expected.source_relation_oid
                );
            }
            if table.target_namespace != expected.target_namespace
                || table.target_table != expected.target_table
            {
                bail!(
                    "subscription state targets {}.{}, not {}.{}",
                    table.target_namespace,
                    table.target_table,
                    expected.target_namespace,
                    expected.target_table
                );
            }
            if table.target_table_id != expected.target_table_id {
                bail!(
                    "target table {}.{} was recreated (table id {} -> {}); \
                     resnapshot is required",
                    expected.target_namespace,
                    expected.target_table,
                    table.target_table_id,
                    expected.target_table_id
                );
            }
            if table.primary_key != expected.primary_key {
                bail!(
                    "subscription state primary key {:?} does not match {:?}",
                    table.primary_key,
                    expected.primary_key
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MySqlStateRecord {
    pub subscription_id: String,
    pub server_id: u32,
    pub gtid_mode: String,
    pub snapshot_locking: String,
    pub source_database: String,
    pub stream_offset: Option<serde_json::Value>,
    pub stream_sequence: u64,
    pub snapshot_boundary: Option<serde_json::Value>,
    pub snapshot_rows: u64,
    pub snapshot_state: String,
}

#[derive(Clone)]
pub struct SubscriptionMetadata {
    client: PooledClient,
}

impl SubscriptionMetadata {
    pub fn from_pool(client: PooledClient) -> Self {
        Self { client }
    }

    /// Verify that the subscription metadata schema is installed. The schema
    /// is owned by `script/meta_init.sql` and the metadata migrations; the
    /// server does not create it at runtime.
    pub async fn init_schema(&self) -> Result<()> {
        let installed: Option<bool> = self
            .client
            .query_opt(
                "select to_regclass('subscription.subscriptions') is not null",
                QueryType::RW,
                &[],
            )
            .await
            .map_err(|error| report!("check subscription metadata schema: {error}"))?
            .map(|row| row.get(0));
        if installed != Some(true) {
            return Err(report!(
                "the subscription metadata schema is missing; initialize the \
                 metadata database with script/meta_init.sql and run \
                 script/metadata_migrate.py migrate"
            ));
        }
        Ok(())
    }

    pub async fn load_by_id(
        &self,
        subscription_id: &str,
    ) -> Result<Option<SubscriptionRecord>> {
        self.load_where("subscription_id = $1", subscription_id)
            .await
    }

    pub async fn load_by_slot(&self, slot: &str) -> Result<Option<SubscriptionRecord>> {
        self.load_where("slot_name = $1", slot).await
    }

    pub async fn load_mysql_state(
        &self,
        subscription_id: &str,
    ) -> Result<Option<MySqlStateRecord>> {
        let row = self
            .client
            .query_opt(
                "select subscription_id, server_id, gtid_mode, snapshot_locking, \
                        source_database, stream_offset, stream_sequence, \
                        snapshot_boundary, snapshot_rows, snapshot_state \
                 from subscription.mysql_state where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id],
            )
            .await
            .map_err(|error| {
                report!("read MySQL subscription state for {subscription_id}: {error}")
            })?;
        let Some(row) = row else {
            return Ok(None);
        };
        let server_id: i32 = row.get(1);
        let stream_sequence: i64 = row.get(6);
        let snapshot_rows: i64 = row.get(8);
        Ok(Some(MySqlStateRecord {
            subscription_id: row.get(0),
            server_id: u32::try_from(server_id)
                .map_err(|_| report!("invalid MySQL server_id {server_id}"))?,
            gtid_mode: row.get(2),
            snapshot_locking: row.get(3),
            source_database: row.get(4),
            stream_offset: row.get(5),
            stream_sequence: u64::try_from(stream_sequence).map_err(|_| {
                report!("invalid MySQL stream_sequence {stream_sequence}")
            })?,
            snapshot_boundary: row.get(7),
            snapshot_rows: u64::try_from(snapshot_rows)
                .map_err(|_| report!("invalid MySQL snapshot_rows {snapshot_rows}"))?,
            snapshot_state: row.get(9),
        }))
    }

    pub async fn register_mysql_state(
        &self,
        subscription_id: &str,
        server_id: u32,
        gtid_mode: &str,
        snapshot_locking: &str,
        source_database: &str,
    ) -> Result<()> {
        let server_id = i32::try_from(server_id)
            .map_err(|_| report!("MySQL server_id is too large"))?;
        self.client
            .execute(
                "insert into subscription.mysql_state \
                     (subscription_id, server_id, gtid_mode, snapshot_locking, \
                      source_database, snapshot_state, updated_at) \
                 values ($1, $2, $3, $4, $5, 'pending', $6) \
                 on conflict (subscription_id) do update set \
                     server_id = excluded.server_id, \
                     gtid_mode = excluded.gtid_mode, \
                     snapshot_locking = excluded.snapshot_locking, \
                     source_database = excluded.source_database, \
                     updated_at = excluded.updated_at",
                QueryType::RW,
                &[
                    &subscription_id,
                    &server_id,
                    &gtid_mode,
                    &snapshot_locking,
                    &source_database,
                    &now_ms(),
                ],
            )
            .await
            .map_err(|error| {
                report!("register MySQL subscription state {subscription_id}: {error}")
            })?;
        Ok(())
    }

    pub async fn set_mysql_snapshot_boundary(
        &self,
        subscription_id: &str,
        boundary: &serde_json::Value,
        rows: u64,
        state: &str,
    ) -> Result<()> {
        let rows = i64::try_from(rows)
            .map_err(|_| report!("MySQL snapshot row count is too large"))?;
        self.client
            .execute(
                "update subscription.mysql_state set snapshot_boundary = $2, \
                        snapshot_rows = $3, snapshot_state = $4, updated_at = $5 \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id, boundary, &rows, &state, &now_ms()],
            )
            .await
            .map_err(|error| {
                report!("store MySQL snapshot boundary for {subscription_id}: {error}")
            })?;
        Ok(())
    }

    pub async fn set_mysql_snapshot_state(
        &self,
        subscription_id: &str,
        state: &str,
    ) -> Result<()> {
        self.client
            .execute(
                "update subscription.mysql_state set snapshot_state = $2, \
                        updated_at = $3 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id, &state, &now_ms()],
            )
            .await
            .map_err(|error| {
                report!("store MySQL snapshot state for {subscription_id}: {error}")
            })?;
        Ok(())
    }

    pub async fn update_mysql_stream_offset(
        &self,
        subscription_id: &str,
        offset: &serde_json::Value,
        sequence: u64,
    ) -> Result<()> {
        let sequence = i64::try_from(sequence)
            .map_err(|_| report!("MySQL stream sequence is too large"))?;
        self.client
            .execute(
                "update subscription.mysql_state set stream_offset = $2, \
                        stream_sequence = $3, updated_at = $4 \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id, offset, &sequence, &now_ms()],
            )
            .await
            .map_err(|error| {
                report!("store MySQL stream offset for {subscription_id}: {error}")
            })?;
        Ok(())
    }

    async fn load_where(
        &self,
        predicate: &str,
        value: &str,
    ) -> Result<Option<SubscriptionRecord>> {
        let query = format!(
            "select subscription_id, slot_name, publication, source_url, state, \
                    enabled, supervised, commit_rows, commit_interval_ms, \
                    snapshot_batch_rows, snapshot_batch_bytes, \
                    snapshot_parallelism, snapshot_memory_bytes, auto_create, \
                    schema_evolution, warehouse_prefix, cdc_column, supervisor_pid, \
                    log_path, last_error, source_kind \
             from subscription.subscriptions where {predicate}"
        );
        let subscription = self
            .client
            .query_opt(&query, QueryType::RW, &[&value])
            .await
            .map_err(|error| report!("read subscription {value}: {error}"))?;
        let Some(subscription) = subscription else {
            return Ok(None);
        };
        let subscription_id: String = subscription.get(0);
        let slot: String = subscription.get(1);
        let publication: String = subscription.get(2);
        let source_url: String = subscription.get(3);
        let state: String = subscription.get(4);
        let enabled: bool = subscription.get(5);
        let supervised: bool = subscription.get(6);
        let commit_rows: i64 = subscription.get(7);
        let commit_interval_ms: i64 = subscription.get(8);
        let snapshot_batch_rows: i64 = subscription.get(9);
        let snapshot_batch_bytes: i64 = subscription.get(10);
        let snapshot_parallelism: i64 = subscription.get(11);
        let snapshot_memory_bytes: i64 = subscription.get(12);
        let auto_create: bool = subscription.get(13);
        let schema_evolution: String = subscription.get(14);
        let warehouse_prefix: Option<String> = subscription.get(15);
        let cdc_column: String = subscription.get(16);
        let supervisor_pid: Option<i32> = subscription.get(17);
        let log_path: Option<String> = subscription.get(18);
        let last_error: Option<String> = subscription.get(19);
        let source_kind: String = subscription.get(20);

        let table_rows = self
            .client
            .query(
                "select source_schema, source_table, source_relation_oid, \
                        target_namespace, target_table, target_table_id, \
                        primary_key, data_lsn::text, state, snapshot_state, \
                        snapshot_mode, snapshot_lsn::text, snapshot_started_at, \
                        snapshot_completed_at, last_error \
                 from subscription.tables \
                 where subscription_id = $1 \
                 order by source_schema, source_table",
                QueryType::RW,
                &[&subscription_id],
            )
            .await
            .map_err(|error| report!("read subscription tables for {slot}: {error}"))?;
        let tables = table_rows
            .iter()
            .map(|row| {
                let oid: i64 = row.get(2);
                let data_lsn: String = row.get(7);
                let snapshot_lsn: Option<String> = row.get(11);
                Ok(TableRecord {
                    source_schema: row.get(0),
                    source_table: row.get(1),
                    source_relation_oid: u32::try_from(oid).map_err(|_| {
                        report!("source relation oid {oid} does not fit in u32")
                    })?,
                    target_namespace: row.get(3),
                    target_table: row.get(4),
                    target_table_id: row.get(5),
                    primary_key: split_primary_key(&row.get::<_, String>(6)),
                    data_lsn: parse_lsn_text(&data_lsn)?,
                    state: row.get(8),
                    snapshot_state: row.get(9),
                    snapshot_mode: row.get(10),
                    snapshot_lsn: parse_optional_lsn(snapshot_lsn)?,
                    snapshot_started_at: row.get(12),
                    snapshot_completed_at: row.get(13),
                    last_error: row.get(14),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let progress = self
            .client
            .query_opt(
                "select processed_lsn::text from subscription.progress \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id],
            )
            .await
            .map_err(|error| report!("read subscription progress for {slot}: {error}"))?;
        let processed_lsn = match progress {
            Some(row) => {
                let value: String = row.get(0);
                parse_lsn_text(&value)?
            }
            None => 0,
        };

        Ok(Some(SubscriptionRecord {
            subscription_id,
            source_kind,
            slot,
            publication,
            source_url,
            state,
            enabled,
            auto_create,
            schema_evolution,
            warehouse_prefix,
            cdc_column,
            supervised,
            commit_rows: usize::try_from(commit_rows)
                .map_err(|_| report!("invalid commit_rows {commit_rows}"))?,
            commit_interval_ms: u64::try_from(commit_interval_ms).map_err(|_| {
                report!("invalid commit_interval_ms {commit_interval_ms}")
            })?,
            snapshot_batch_rows: usize::try_from(snapshot_batch_rows).map_err(|_| {
                report!("invalid snapshot_batch_rows {snapshot_batch_rows}")
            })?,
            snapshot_batch_bytes: usize::try_from(snapshot_batch_bytes).map_err(
                |_| report!("invalid snapshot_batch_bytes {snapshot_batch_bytes}"),
            )?,
            snapshot_parallelism: usize::try_from(snapshot_parallelism).map_err(
                |_| report!("invalid snapshot_parallelism {snapshot_parallelism}"),
            )?,
            snapshot_memory_bytes: usize::try_from(snapshot_memory_bytes).map_err(
                |_| report!("invalid snapshot_memory_bytes {snapshot_memory_bytes}"),
            )?,
            supervisor_pid,
            log_path,
            last_error,
            tables,
            processed_lsn,
        }))
    }

    /// Insert the definition and zero progress if this slot has no state yet.
    /// A concurrent initializer can win; the caller reloads and validates.
    /// Returns whether the subscription row itself was inserted.
    pub async fn register(&self, definition: &SubscriptionDefinition) -> Result<bool> {
        let now = now_ms();
        let snapshot_state = if definition.snapshot_mode == "none" {
            "done"
        } else {
            "pending"
        };
        let mut sources = HashSet::new();
        let mut targets = HashSet::new();
        for table in &definition.tables {
            if !sources
                .insert((table.source_schema.as_str(), table.source_table.as_str()))
            {
                bail!(
                    "duplicate source table {}.{} in subscription definition",
                    table.source_schema,
                    table.source_table
                );
            }
            if !targets
                .insert((table.target_namespace.as_str(), table.target_table.as_str()))
            {
                bail!(
                    "duplicate target table {}.{} in subscription definition",
                    table.target_namespace,
                    table.target_table
                );
            }
        }
        let mut connection = self
            .client
            .get(QueryType::RW)
            .await
            .map_err(|error| report!("check out metadata connection: {error}"))?;
        let transaction = connection
            .transaction()
            .await
            .map_err(|error| report!("begin subscription registration: {error}"))?;
        let commit_rows = i64::try_from(definition.commit_rows)
            .map_err(|_| report!("commit_rows is too large"))?;
        let commit_interval_ms = i64::try_from(definition.commit_interval_ms)
            .map_err(|_| report!("commit_interval_ms is too large"))?;
        let snapshot_batch_rows = i64::try_from(definition.snapshot_batch_rows)
            .map_err(|_| report!("snapshot_batch_rows is too large"))?;
        let snapshot_batch_bytes = i64::try_from(definition.snapshot_batch_bytes)
            .map_err(|_| report!("snapshot_batch_bytes is too large"))?;
        let snapshot_parallelism = i64::try_from(definition.snapshot_parallelism)
            .map_err(|_| report!("snapshot_parallelism is too large"))?;
        let snapshot_memory_bytes = i64::try_from(definition.snapshot_memory_bytes)
            .map_err(|_| report!("snapshot_memory_bytes is too large"))?;
        let inserted = transaction
            .execute(
                "insert into subscription.subscriptions \
                     (subscription_id, slot_name, publication, source_url, state, \
                      enabled, supervised, auto_create, schema_evolution, \
                      warehouse_prefix, cdc_column, commit_rows, commit_interval_ms, \
                      snapshot_batch_rows, snapshot_batch_bytes, \
                      snapshot_parallelism, snapshot_memory_bytes, \
                      supervisor_pid, log_path, last_error, source_kind, \
                      created_at, updated_at) \
                 values ($1, $2, $3, $4, 'starting', true, $5, $6, $7, $8, $9, \
                         $10, $11, $12, $13, $14, $15, null, null, null, $16, \
                         $17, $17) \
                 on conflict (subscription_id) do nothing",
                &[
                    &definition.subscription_id,
                    &definition.slot,
                    &definition.publication,
                    &definition.source_url,
                    &definition.supervised,
                    &definition.auto_create,
                    &definition.schema_evolution,
                    &definition.warehouse_prefix,
                    &definition.cdc_column,
                    &commit_rows,
                    &commit_interval_ms,
                    &snapshot_batch_rows,
                    &snapshot_batch_bytes,
                    &snapshot_parallelism,
                    &snapshot_memory_bytes,
                    &definition.source_kind,
                    &now,
                ],
            )
            .await
            .map_err(|error| report!("insert subscription definition: {error}"))?;
        if inserted == 0 {
            // The subscription already existed; leave its rows untouched.
            transaction
                .rollback()
                .await
                .map_err(|error| report!("rollback duplicate subscription: {error}"))?;
            return Ok(false);
        }
        for table in &definition.tables {
            let primary_key = table.primary_key.join(",");
            transaction
                .execute(
                    "insert into subscription.tables \
                         (subscription_id, source_schema, source_table, source_relation_oid, \
                          target_namespace, target_table, target_table_id, primary_key, \
                          data_lsn, state, snapshot_state, snapshot_mode, \
                          snapshot_lsn, snapshot_started_at, snapshot_completed_at, \
                          snapshot_dump_path, last_error, created_at, updated_at) \
                     values ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'init', $9, $10, \
                             null, null, null, null, null, $11, $11) \
                     on conflict (subscription_id, source_schema, source_table) do nothing",
                    &[
                        &definition.subscription_id,
                        &table.source_schema,
                        &table.source_table,
                        &i64::from(table.source_relation_oid),
                        &table.target_namespace,
                        &table.target_table,
                        &table.target_table_id,
                        &primary_key,
                        &snapshot_state,
                        &definition.snapshot_mode,
                        &now,
                    ],
                )
                .await
                .map_err(|error| report!("insert subscription table definition: {error}"))?;
        }
        transaction
            .execute(
                "insert into subscription.progress \
                     (subscription_id, processed_lsn, updated_at) \
                 values ($1, 0, $2) \
                 on conflict (subscription_id) do nothing",
                &[&definition.subscription_id, &now],
            )
            .await
            .map_err(|error| report!("insert subscription progress: {error}"))?;
        transaction
            .commit()
            .await
            .map_err(|error| report!("commit subscription registration: {error}"))?;
        Ok(true)
    }

    pub async fn exists(&self, subscription_id: &str) -> Result<bool> {
        let row = self
            .client
            .query_opt(
                "select 1 from subscription.subscriptions \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id],
            )
            .await
            .map_err(|error| report!("check subscription {subscription_id}: {error}"))?;
        Ok(row.is_some())
    }

    pub async fn target_in_use(
        &self,
        target_namespace: &str,
        target_table: &str,
    ) -> Result<bool> {
        let row = self
            .client
            .query_opt(
                "select 1 from subscription.tables \
                 where target_namespace = $1 and target_table = $2",
                QueryType::RW,
                &[&target_namespace, &target_table],
            )
            .await
            .map_err(|error| {
                report!("check target table {target_namespace}.{target_table}: {error}")
            })?;
        Ok(row.is_some())
    }

    pub async fn list_enabled_ids(&self) -> Result<Vec<String>> {
        let rows = self
            .client
            .query(
                "select subscription_id from subscription.subscriptions \
                 where enabled and supervised order by subscription_id",
                QueryType::RW,
                &[],
            )
            .await
            .map_err(|error| report!("list enabled subscriptions: {error}"))?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    pub async fn set_process(
        &self,
        subscription_id: &str,
        pid: u32,
        log_path: &str,
    ) -> Result<()> {
        let pid =
            i32::try_from(pid).map_err(|_| report!("child pid {pid} is too large"))?;
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.subscriptions \
                 set supervisor_pid = $2, log_path = $3, updated_at = $4 \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id, &pid, &log_path, &now],
            )
            .await
            .map_err(|error| report!("record subscription process: {error}"))?;
        if updated == 0 {
            bail!("subscription {subscription_id} disappeared");
        }
        Ok(())
    }

    pub async fn clear_process(&self, subscription_id: &str) -> Result<()> {
        let now = now_ms();
        self.client
            .execute(
                "update subscription.subscriptions \
                 set supervisor_pid = null, updated_at = $2 \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id, &now],
            )
            .await
            .map_err(|error| report!("clear subscription process: {error}"))?;
        Ok(())
    }

    pub async fn set_enabled(&self, subscription_id: &str, enabled: bool) -> Result<()> {
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.subscriptions \
                 set enabled = $2, updated_at = $3 \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id, &enabled, &now],
            )
            .await
            .map_err(|error| report!("set subscription enabled: {error}"))?;
        if updated == 0 {
            bail!("subscription {subscription_id} disappeared");
        }
        Ok(())
    }

    pub async fn unregister(&self, subscription_id: &str) -> Result<bool> {
        let deleted = self
            .client
            .execute(
                "delete from subscription.subscriptions where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id],
            )
            .await
            .map_err(|error| report!("delete subscription {subscription_id}: {error}"))?;
        Ok(deleted > 0)
    }

    /// Record a child process that exited without the subscriber marking
    /// itself stopped (for example a crash or SIGKILL).
    pub async fn record_unexpected_exit(
        &self,
        subscription_id: &str,
        message: &str,
    ) -> Result<()> {
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.subscriptions \
                 set state = 'error', last_error = $2, supervisor_pid = null, \
                     updated_at = $3 \
                 where subscription_id = $1 \
                   and state not in ('stopped', 'disabled', 'error')",
                QueryType::RW,
                &[&subscription_id, &message, &now],
            )
            .await
            .map_err(|error| report!("record subscription exit: {error}"))?;
        if updated > 0 {
            self.client
                .execute(
                    "update subscription.tables \
                     set state = 'error', last_error = $2, updated_at = $3 \
                     where subscription_id = $1",
                    QueryType::RW,
                    &[&subscription_id, &message, &now],
                )
                .await
                .map_err(|error| report!("record subscription table exit: {error}"))?;
        }
        Ok(())
    }

    pub async fn set_snapshot_started(
        &self,
        subscription_id: &str,
        source_schema: &str,
        source_table: &str,
        lsn: u64,
    ) -> Result<()> {
        let lsn = lsn.to_string();
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.tables \
                 set snapshot_state = 'copying', \
                     snapshot_lsn = $4::text::numeric, \
                     snapshot_started_at = $5, \
                     snapshot_completed_at = null, \
                     last_error = null, updated_at = $5 \
                 where subscription_id = $1 and source_schema = $2 \
                   and source_table = $3",
                QueryType::RW,
                &[&subscription_id, &source_schema, &source_table, &lsn, &now],
            )
            .await
            .map_err(|error| report!("start subscription snapshot: {error}"))?;
        if updated == 0 {
            bail!(
                "subscription table definition {source_schema}.{source_table} disappeared"
            );
        }
        Ok(())
    }

    pub async fn set_snapshot_done(
        &self,
        subscription_id: &str,
        source_schema: &str,
        source_table: &str,
        lsn: u64,
    ) -> Result<()> {
        let lsn = lsn.to_string();
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.tables \
                 set snapshot_state = 'done', \
                     snapshot_lsn = $4::text::numeric, \
                     snapshot_completed_at = $5, \
                     last_error = null, updated_at = $5 \
                 where subscription_id = $1 and source_schema = $2 \
                   and source_table = $3",
                QueryType::RW,
                &[&subscription_id, &source_schema, &source_table, &lsn, &now],
            )
            .await
            .map_err(|error| report!("finish subscription snapshot: {error}"))?;
        if updated == 0 {
            bail!(
                "subscription table definition {source_schema}.{source_table} disappeared"
            );
        }
        Ok(())
    }

    pub async fn set_snapshot_failed(
        &self,
        subscription_id: &str,
        source_schema: &str,
        source_table: &str,
        error: &str,
    ) -> Result<()> {
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.tables \
                 set snapshot_state = 'failed', last_error = $4, updated_at = $5 \
                 where subscription_id = $1 and source_schema = $2 \
                   and source_table = $3",
                QueryType::RW,
                &[
                    &subscription_id,
                    &source_schema,
                    &source_table,
                    &error,
                    &now,
                ],
            )
            .await
            .map_err(|error| report!("record subscription snapshot failure: {error}"))?;
        if updated == 0 {
            bail!(
                "subscription table definition {source_schema}.{source_table} disappeared"
            );
        }
        Ok(())
    }

    pub async fn update_table_data_lsn(
        &self,
        subscription_id: &str,
        source_schema: &str,
        source_table: &str,
        lsn: u64,
    ) -> Result<()> {
        let lsn = lsn.to_string();
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.tables \
                 set data_lsn = greatest(data_lsn, $4::text::numeric), \
                     updated_at = $5 \
                 where subscription_id = $1 and source_schema = $2 \
                   and source_table = $3",
                QueryType::RW,
                &[&subscription_id, &source_schema, &source_table, &lsn, &now],
            )
            .await
            .map_err(|error| report!("update subscription table data_lsn: {error}"))?;
        if updated == 0 {
            bail!(
                "subscription table definition {source_schema}.{source_table} disappeared"
            );
        }
        Ok(())
    }

    pub async fn update_progress(&self, subscription_id: &str, lsn: u64) -> Result<()> {
        let lsn = lsn.to_string();
        let now = now_ms();
        let updated = self
            .client
            .execute(
                "update subscription.progress \
                 set processed_lsn = greatest(processed_lsn, $2::text::numeric), \
                     updated_at = $3 \
                 where subscription_id = $1",
                QueryType::RW,
                &[&subscription_id, &lsn, &now],
            )
            .await
            .map_err(|error| report!("update subscription progress: {error}"))?;
        if updated == 0 {
            bail!("subscription progress row for {subscription_id} disappeared");
        }
        Ok(())
    }

    pub async fn set_state(
        &self,
        subscription_id: &str,
        state: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let error = error.map(str::to_string);
        let now = now_ms();
        let mut connection = self
            .client
            .get(QueryType::RW)
            .await
            .map_err(|error| report!("check out metadata connection: {error}"))?;
        let transaction = connection
            .transaction()
            .await
            .map_err(|error| report!("begin subscription state update: {error}"))?;
        let updated = transaction
            .execute(
                "update subscription.subscriptions \
                 set state = $2, last_error = $3, updated_at = $4 \
                 where subscription_id = $1",
                &[&subscription_id, &state, &error, &now],
            )
            .await
            .map_err(|error| report!("update subscription state: {error}"))?;
        if updated == 0 {
            bail!("subscription {subscription_id} disappeared");
        }
        transaction
            .execute(
                "update subscription.tables \
                 set state = $2, last_error = $3, updated_at = $4 \
                 where subscription_id = $1",
                &[&subscription_id, &state, &error, &now],
            )
            .await
            .map_err(|error| report!("update subscription table state: {error}"))?;
        transaction
            .commit()
            .await
            .map_err(|error| report!("commit subscription state: {error}"))?;
        Ok(())
    }
}

fn split_primary_key(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn parse_lsn_text(value: &str) -> Result<u64> {
    value.parse::<u64>().map_err(|error| {
        report!("invalid LSN {value:?} in subscription metadata: {error}")
    })
}

fn parse_optional_lsn(value: Option<String>) -> Result<Option<u64>> {
    value.map(|value| parse_lsn_text(&value)).transpose()
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition() -> SubscriptionDefinition {
        SubscriptionDefinition {
            subscription_id: "slot".to_string(),
            source_kind: "postgres".to_string(),
            slot: "slot".to_string(),
            publication: "pub".to_string(),
            source_url: "postgresql://source/db".to_string(),
            snapshot_mode: "managed".to_string(),
            auto_create: false,
            schema_evolution: "auto".to_string(),
            warehouse_prefix: None,
            cdc_column: "rowKinds".to_string(),
            supervised: true,
            commit_rows: 50_000,
            commit_interval_ms: 1_000,
            snapshot_batch_rows: 50_000,
            snapshot_batch_bytes: 64 * 1024 * 1024,
            snapshot_parallelism: 4,
            snapshot_memory_bytes: 1024 * 1024 * 1024,
            tables: vec![TableDefinition {
                source_schema: "public".to_string(),
                source_table: "users".to_string(),
                source_relation_oid: 42,
                target_namespace: "ns".to_string(),
                target_table: "users".to_string(),
                target_table_id: "table-1".to_string(),
                primary_key: vec!["id".to_string()],
            }],
        }
    }

    fn sample_record() -> SubscriptionRecord {
        SubscriptionRecord {
            subscription_id: "slot".to_string(),
            source_kind: "postgres".to_string(),
            slot: "slot".to_string(),
            publication: "pub".to_string(),
            source_url: "postgresql://source/db".to_string(),
            state: "streaming".to_string(),
            enabled: true,
            auto_create: false,
            schema_evolution: "auto".to_string(),
            warehouse_prefix: None,
            cdc_column: "rowKinds".to_string(),
            supervised: true,
            commit_rows: 50_000,
            commit_interval_ms: 1_000,
            snapshot_batch_rows: 50_000,
            snapshot_batch_bytes: 64 * 1024 * 1024,
            snapshot_parallelism: 4,
            snapshot_memory_bytes: 1024 * 1024 * 1024,
            supervisor_pid: None,
            log_path: None,
            last_error: None,
            tables: vec![TableRecord {
                source_schema: "public".to_string(),
                source_table: "users".to_string(),
                source_relation_oid: 42,
                target_namespace: "ns".to_string(),
                target_table: "users".to_string(),
                target_table_id: "table-1".to_string(),
                primary_key: vec!["id".to_string()],
                data_lsn: 10,
                state: "streaming".to_string(),
                snapshot_state: "done".to_string(),
                snapshot_mode: "managed".to_string(),
                snapshot_lsn: Some(10),
                snapshot_started_at: Some(1),
                snapshot_completed_at: Some(2),
                last_error: None,
            }],
            processed_lsn: 12,
        }
    }

    #[test]
    fn matching_identity_passes() {
        sample_record().validate(&definition()).unwrap();
    }

    #[test]
    fn recreated_source_or_target_is_rejected() {
        let mut record = sample_record();
        record.tables[0].source_relation_oid = 43;
        assert!(record.validate(&definition()).is_err());

        let mut record = sample_record();
        record.tables[0].target_table_id = "table-2".to_string();
        assert!(record.validate(&definition()).is_err());
    }

    #[test]
    fn different_primary_key_is_rejected() {
        let mut record = sample_record();
        record.tables[0].primary_key = vec!["other".to_string()];
        assert!(record.validate(&definition()).is_err());
    }
}
