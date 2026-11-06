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

use chrono::Utc;
use lakesoul_metadata::{PooledClient, QueryType};
use rootcause::{bail, report};

use crate::Result;

/// Idempotent runtime DDL. Production deployments should also apply
/// `script/metadata-migrations/V4000002__subscription.sql`.
const SUBSCRIPTION_SCHEMA_DDL: &str = r#"
do $$ begin
    create schema if not exists subscription;
exception when duplicate_schema or unique_violation then null;
end $$;

do $$ begin
    create table if not exists subscription.subscriptions (
        subscription_id       text primary key,
        slot_name             text not null unique,
        publication           text not null,
        source_url            text not null,
        state                 text not null default 'init',
        enabled               boolean not null default true,
        -- Only SQL-created subscriptions are restarted by the server
        -- supervisor; a manually started CLI subscriber must not be
        -- duplicated.
        supervised            boolean not null default false,
        commit_rows           bigint not null default 50000,
        commit_interval_ms    bigint not null default 1000,
        snapshot_batch_rows   bigint not null default 50000,
        snapshot_batch_bytes  bigint not null default 67108864,
        snapshot_parallelism  bigint not null default 4,
        snapshot_memory_bytes bigint not null default 1073741824,
        supervisor_pid        integer,
        log_path              text,
        last_error            text,
        created_at            bigint not null,
        updated_at            bigint not null
    );
exception when duplicate_table or unique_violation then null;
end $$;

do $$ begin
    create table if not exists subscription.tables (
        subscription_id     text   not null references subscription.subscriptions (subscription_id) on delete cascade,
        source_schema       text   not null,
        source_table        text   not null,
        source_relation_oid bigint not null,
        target_namespace    text   not null,
        target_table        text   not null,
        target_table_id     text   not null,
        primary_key         text   not null,
        data_lsn            numeric(20, 0) not null default 0,
        state               text   not null default 'init',
        snapshot_state      text   not null default 'pending',
        snapshot_mode       text   not null default 'managed',
        snapshot_lsn        numeric(20, 0),
        snapshot_started_at bigint,
        snapshot_completed_at bigint,
        snapshot_dump_path  text,
        last_error          text,
        created_at          bigint not null,
        updated_at          bigint not null,
        primary key (subscription_id, source_schema, source_table),
        unique (subscription_id, target_namespace, target_table),
        -- Two subscriber processes must not write the same target table.
        unique (target_namespace, target_table)
    );
exception when duplicate_table or unique_violation then null;
end $$;

do $$ begin
    create table if not exists subscription.progress (
        subscription_id  text primary key references subscription.subscriptions (subscription_id) on delete cascade,
        processed_lsn    numeric(20, 0) not null default 0,
        updated_at       bigint not null,
        owner            text,
        lease_expires_at bigint
    );
exception when duplicate_table or unique_violation then null;
end $$;
"#;

#[derive(Debug, Clone)]
pub struct SubscriptionDefinition {
    pub subscription_id: String,
    pub slot: String,
    pub publication: String,
    pub source_url: String,
    pub source_schema: String,
    pub source_table: String,
    pub source_relation_oid: u32,
    pub target_namespace: String,
    pub target_table: String,
    pub target_table_id: String,
    pub primary_key: Vec<String>,
    pub snapshot_mode: String,
    pub supervised: bool,
    pub commit_rows: usize,
    pub commit_interval_ms: u64,
    pub snapshot_batch_rows: usize,
    pub snapshot_batch_bytes: usize,
    pub snapshot_parallelism: usize,
    pub snapshot_memory_bytes: usize,
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
    pub slot: String,
    pub publication: String,
    pub source_url: String,
    pub state: String,
    pub enabled: bool,
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
        if self.subscription_id != definition.subscription_id
            || self.slot != definition.slot
            || self.publication != definition.publication
        {
            bail!(
                "subscription state belongs to slot {:?} / publication {:?}, not {:?} / {:?}",
                self.slot,
                self.publication,
                definition.slot,
                definition.publication
            );
        }
        if self.tables.len() != 1 {
            bail!(
                "P0 supports exactly one source table per subscription, but the metadata \
                 has {} table mappings",
                self.tables.len()
            );
        }
        let table = &self.tables[0];
        if table.source_schema != definition.source_schema
            || table.source_table != definition.source_table
        {
            bail!(
                "subscription state maps {}.{}, not {}.{}",
                table.source_schema,
                table.source_table,
                definition.source_schema,
                definition.source_table
            );
        }
        if table.source_relation_oid != definition.source_relation_oid {
            bail!(
                "source table {}.{} was recreated (relation oid {} -> {}); \
                 resnapshot is required",
                definition.source_schema,
                definition.source_table,
                table.source_relation_oid,
                definition.source_relation_oid
            );
        }
        if table.target_namespace != definition.target_namespace
            || table.target_table != definition.target_table
        {
            bail!(
                "subscription state targets {}.{}, not {}.{}",
                table.target_namespace,
                table.target_table,
                definition.target_namespace,
                definition.target_table
            );
        }
        if table.target_table_id != definition.target_table_id {
            bail!(
                "target table {}.{} was recreated (table id {} -> {}); \
                 resnapshot is required",
                definition.target_namespace,
                definition.target_table,
                table.target_table_id,
                definition.target_table_id
            );
        }
        if table.primary_key != definition.primary_key {
            bail!(
                "subscription state primary key {:?} does not match {:?}",
                table.primary_key,
                definition.primary_key
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SubscriptionMetadata {
    client: PooledClient,
}

impl SubscriptionMetadata {
    pub fn from_pool(client: PooledClient) -> Self {
        Self { client }
    }

    pub async fn init_schema(&self) -> Result<()> {
        self.client
            .batch_execute(SUBSCRIPTION_SCHEMA_DDL, QueryType::RW)
            .await
            .map_err(|error| report!("create subscription metadata schema: {error}"))
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

    async fn load_where(
        &self,
        predicate: &str,
        value: &str,
    ) -> Result<Option<SubscriptionRecord>> {
        let query = format!(
            "select subscription_id, slot_name, publication, source_url, state, \
                    enabled, supervised, commit_rows, commit_interval_ms, \
                    snapshot_batch_rows, snapshot_batch_bytes, \
                    snapshot_parallelism, snapshot_memory_bytes, supervisor_pid, \
                    log_path, last_error \
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
        let supervisor_pid: Option<i32> = subscription.get(13);
        let log_path: Option<String> = subscription.get(14);
        let last_error: Option<String> = subscription.get(15);

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
            slot,
            publication,
            source_url,
            state,
            enabled,
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
        let primary_key = definition.primary_key.join(",");
        let snapshot_state = if definition.snapshot_mode == "none" {
            "done"
        } else {
            "pending"
        };
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
                      enabled, supervised, commit_rows, commit_interval_ms, \
                      snapshot_batch_rows, snapshot_batch_bytes, \
                      snapshot_parallelism, snapshot_memory_bytes, \
                      supervisor_pid, log_path, last_error, created_at, updated_at) \
                 values ($1, $2, $3, $4, 'starting', true, $5, $6, $7, $8, $9, \
                         $10, $11, null, null, null, $12, $12) \
                 on conflict (subscription_id) do nothing",
                &[
                    &definition.subscription_id,
                    &definition.slot,
                    &definition.publication,
                    &definition.source_url,
                    &definition.supervised,
                    &commit_rows,
                    &commit_interval_ms,
                    &snapshot_batch_rows,
                    &snapshot_batch_bytes,
                    &snapshot_parallelism,
                    &snapshot_memory_bytes,
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
                    &definition.source_schema,
                    &definition.source_table,
                    &i64::from(definition.source_relation_oid),
                    &definition.target_namespace,
                    &definition.target_table,
                    &definition.target_table_id,
                    &primary_key,
                    &snapshot_state,
                    &definition.snapshot_mode,
                    &now,
                ],
            )
            .await
            .map_err(|error| report!("insert subscription table definition: {error}"))?;
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
            slot: "slot".to_string(),
            publication: "pub".to_string(),
            source_url: "postgresql://source/db".to_string(),
            source_schema: "public".to_string(),
            source_table: "users".to_string(),
            source_relation_oid: 42,
            target_namespace: "ns".to_string(),
            target_table: "users".to_string(),
            target_table_id: "table-1".to_string(),
            primary_key: vec!["id".to_string()],
            snapshot_mode: "managed".to_string(),
            supervised: true,
            commit_rows: 50_000,
            commit_interval_ms: 1_000,
            snapshot_batch_rows: 50_000,
            snapshot_batch_bytes: 64 * 1024 * 1024,
            snapshot_parallelism: 4,
            snapshot_memory_bytes: 1024 * 1024 * 1024,
        }
    }

    fn sample_record() -> SubscriptionRecord {
        SubscriptionRecord {
            subscription_id: "slot".to_string(),
            slot: "slot".to_string(),
            publication: "pub".to_string(),
            source_url: "postgresql://source/db".to_string(),
            state: "streaming".to_string(),
            enabled: true,
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
