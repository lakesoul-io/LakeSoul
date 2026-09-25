// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL metadata owned by the IVM runtime (schema `ivm`).
//!
//! The tables live next to the LakeSoul metadata in the same database but are
//! created and used only by this crate:
//!
//! * `ivm.views` stores the serialized [`crate::ViewSpec`] of every view, its
//!   monotonic epoch counter and its rebuild generation;
//! * `ivm.cursors` stores, per `(view, source, partition)`, the last source
//!   version consumed by a refresh. Cursors are version-based so commits that
//!   share a millisecond cannot be skipped or read twice;
//! * `ivm.epochs` records every refresh window (see `EPOCH.md`): one row per
//!   `(view, generation, window_key)` with its monotonic epoch, the consumed
//!   source ranges, and the MV partition versions before/after the write. A
//!   refresh uses it to skip windows that are already applied without reading
//!   the MV data again;
//! * `ivm.states` binds every `(view, role)` to the internal LakeSoul table
//!   that holds the state (`mv` for the output, `state` for the value-count
//!   table), so internal tables are discoverable and a view id cannot silently
//!   switch to a different state table;
//! * `ivm.consumers` tracks the epoch each consumer still needs, so committed
//!   epoch rows below `min(last_epoch) - grace` can be garbage collected
//!   without stranding a consumer.

use std::collections::HashMap;
use std::time::Duration;

use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::{PooledClient, QueryType, create_connection, pg_config_from_env};
use serde::{Deserialize, Serialize};
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;

use crate::error::Result;

/// DDL for the IVM metadata schema. Idempotent and safe to run concurrently
/// (PostgreSQL `IF NOT EXISTS` is not race free, so each statement tolerates
/// the duplicate created by a concurrent initializer).
const IVM_SCHEMA_DDL: &str = "
do $$ begin
    create schema if not exists ivm;
exception when duplicate_schema or unique_violation then null;
end $$;

do $$ begin
    create table if not exists ivm.views (
        view_id             text primary key,
        spec                jsonb not null,
        refresh_interval_ms bigint not null default 0,
        status              text not null default 'active',
        last_epoch          bigint not null default 0,
        generation          bigint not null default 0,
        created_at          bigint not null
    );
exception when duplicate_table or unique_violation then null;
end $$;

do $$ begin
    alter table ivm.views add column if not exists last_epoch bigint not null default 0;
exception when duplicate_column or unique_violation then null;
end $$;

do $$ begin
    alter table ivm.views add column if not exists generation bigint not null default 0;
exception when duplicate_column or unique_violation then null;
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
exception when duplicate_table or unique_violation then null;
end $$;

do $$ begin
    create table if not exists ivm.epochs (
        view_id            text   not null,
        generation         bigint not null,
        epoch              bigint not null,
        window_key         text   not null,
        status             text   not null,
        to_versions        jsonb  not null,
        mv_versions_before jsonb  not null default '[]'::jsonb,
        mv_versions        jsonb  not null default '[]'::jsonb,
        created_at         bigint not null,
        committed_at       bigint,
        primary key (view_id, generation, epoch)
    );
exception when duplicate_table or unique_violation then null;
end $$;

do $$ begin
    create unique index if not exists ivm_epochs_window_key
        on ivm.epochs (view_id, generation, window_key);
exception when duplicate_table or unique_violation then null;
end $$;

do $$ begin
    create table if not exists ivm.states (
        view_id    text   not null,
        role       text   not null,
        table_id   text   not null,
        table_name text   not null,
        namespace  text   not null default 'default',
        table_path text   not null,
        created_at bigint not null,
        primary key (view_id, role)
    );
exception when duplicate_table or unique_violation then null;
end $$;

do $$ begin
    create table if not exists ivm.consumers (
        view_id     text   not null,
        consumer_id text   not null,
        last_epoch  bigint not null,
        updated_at  bigint not null,
        primary key (view_id, consumer_id)
    );
exception when duplicate_table or unique_violation then null;
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

/// The version of one partition of a table at a point in time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionVersion {
    /// The partition description.
    pub partition_desc: String,
    /// The partition version.
    pub version: i64,
}

/// The source range one refresh consumed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceVersionRange {
    /// The source table id.
    pub source_table_id: String,
    /// The source partition description.
    pub partition_desc: String,
    /// The exclusive lower bound (`-1` when the partition started here).
    pub from_version: i64,
    /// The inclusive upper bound.
    pub to_version: i64,
}

/// The lifecycle state of a refresh epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochStatus {
    /// The epoch was allocated and the data may or may not be written yet.
    Pending,
    /// The data is committed and the epoch is visible to consumers.
    Committed,
}

impl EpochStatus {
    fn as_str(self) -> &'static str {
        match self {
            EpochStatus::Pending => "pending",
            EpochStatus::Committed => "committed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(EpochStatus::Pending),
            "committed" => Ok(EpochStatus::Committed),
            other => Err(rootcause::report!("unknown epoch status {other:?}")),
        }
    }
}

/// The role an internal table plays for a view in `ivm.states`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateRole {
    /// The materialized view output (which is also the state for views that
    /// keep their accumulator in the output, e.g. SUM/COUNT).
    Mv,
    /// An auxiliary state table (the value-count table of MIN/MAX and
    /// DISTINCT views).
    State,
}

impl StateRole {
    /// The persisted role name.
    pub fn as_str(self) -> &'static str {
        match self {
            StateRole::Mv => "mv",
            StateRole::State => "state",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "mv" => Ok(StateRole::Mv),
            "state" => Ok(StateRole::State),
            other => Err(rootcause::report!("unknown state role {other:?}")),
        }
    }
}

/// One row of `ivm.states`: the internal table bound to a `(view, role)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateTable {
    /// The view the table belongs to.
    pub view_id: String,
    /// The role of the table in the view.
    pub role: StateRole,
    /// The LakeSoul table id.
    pub table_id: String,
    /// The LakeSoul table name.
    pub table_name: String,
    /// The namespace of the table.
    pub namespace: String,
    /// The table root path.
    pub table_path: String,
    /// The registration time (unix milliseconds).
    pub created_at: i64,
}

/// One row of `ivm.consumers`: a consumer watermark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Consumer {
    /// The view the consumer reads.
    pub view_id: String,
    /// The consumer identity.
    pub consumer_id: String,
    /// The oldest epoch the consumer still needs.
    pub last_epoch: i64,
    /// The last update time (unix milliseconds).
    pub updated_at: i64,
}

/// One row of `ivm.epochs`: a refresh window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochRecord {
    /// The view the window belongs to.
    pub view_id: String,
    /// The rebuild generation of the view.
    pub generation: i64,
    /// The monotonic epoch of the window.
    pub epoch: i64,
    /// The canonical window identity.
    pub window_key: String,
    /// The lifecycle state.
    pub status: EpochStatus,
    /// The consumed source ranges.
    pub to_versions: Vec<SourceVersionRange>,
    /// The MV partition versions before the window was applied.
    pub mv_versions_before: Vec<PartitionVersion>,
    /// The MV partition versions after the window was applied.
    pub mv_versions: Vec<PartitionVersion>,
    /// The allocation time (unix milliseconds).
    pub created_at: i64,
    /// The commit time (unix milliseconds).
    pub committed_at: Option<i64>,
}

/// The outcome of [`IvmMetadata::begin_epoch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginEpoch {
    /// A new pending epoch was allocated; the caller must apply the window and
    /// then call [`IvmMetadata::mark_epoch_committed`].
    Created(EpochRecord),
    /// A previous attempt left this window pending; the caller must compare the
    /// current MV versions with `mv_versions_before` to decide whether the data
    /// is already written, then mark it committed.
    Pending(EpochRecord),
    /// The window is already committed; the caller skips the data write.
    Committed(EpochRecord),
}

impl BeginEpoch {
    /// The epoch row in all three cases.
    pub fn record(&self) -> &EpochRecord {
        match self {
            BeginEpoch::Created(record)
            | BeginEpoch::Pending(record)
            | BeginEpoch::Committed(record) => record,
        }
    }
}

const EPOCH_COLUMNS: &str = "view_id, generation, epoch, window_key, status, \
     to_versions, mv_versions_before, mv_versions, created_at, committed_at";

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
        self.batch_execute_rw(IVM_SCHEMA_DDL).await?;
        Ok(())
    }

    /// How many times a statement is retried on serialization failures and
    /// deadlocks. The docker-compose test environment runs PostgreSQL at
    /// `serializable`, where concurrent refreshes abort each other's
    /// statements; retrying with backoff makes the metadata writes robust
    /// there and at the default `read committed` everywhere else.
    const MAX_RETRY_ATTEMPTS: u32 = 10;

    /// Whether a PostgreSQL error is a retryable concurrency conflict.
    fn is_retryable(error: &LakeSoulMetaDataError) -> bool {
        let code = match error {
            LakeSoulMetaDataError::PostgresError(error) => error.code(),
            _ => None,
        };
        matches!(
            code,
            Some(&SqlState::T_R_SERIALIZATION_FAILURE)
                | Some(&SqlState::T_R_DEADLOCK_DETECTED)
        )
    }

    /// Exponential backoff with jitter for one retry attempt.
    async fn retry_backoff(attempt: u32) {
        let base = 1u64 << attempt.min(5);
        let jitter = crate::now_ms().unsigned_abs() % (base + 1);
        tokio::time::sleep(Duration::from_millis(base + jitter)).await;
    }

    /// Run a read/write statement, retrying concurrency conflicts.
    async fn execute_rw(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.client.execute(sql, QueryType::RW, params).await {
                Ok(rows) => return Ok(rows),
                Err(error)
                    if attempt < Self::MAX_RETRY_ATTEMPTS
                        && Self::is_retryable(&error) =>
                {
                    Self::retry_backoff(attempt).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Run a read/write `query_opt`, retrying concurrency conflicts.
    async fn query_opt_rw(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<tokio_postgres::Row>> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.client.query_opt(sql, QueryType::RW, params).await {
                Ok(row) => return Ok(row),
                Err(error)
                    if attempt < Self::MAX_RETRY_ATTEMPTS
                        && Self::is_retryable(&error) =>
                {
                    Self::retry_backoff(attempt).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Run a DDL batch, retrying concurrency conflicts (idempotent DDL).
    async fn batch_execute_rw(&self, sql: &str) -> Result<()> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match self.client.batch_execute(sql, QueryType::RW).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if attempt < Self::MAX_RETRY_ATTEMPTS
                        && Self::is_retryable(&error) =>
                {
                    Self::retry_backoff(attempt).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Insert or update a registered view.
    pub async fn upsert_view(
        &self,
        view_id: &str,
        spec: &serde_json::Value,
        refresh_interval_ms: i64,
    ) -> Result<()> {
        self.execute_rw(
            "insert into ivm.views(view_id, spec, refresh_interval_ms, created_at)
                 values ($1::TEXT, $2::JSONB, $3::BIGINT, $4::BIGINT)
                 on conflict (view_id) do update
                 set spec = excluded.spec,
                     refresh_interval_ms = excluded.refresh_interval_ms",
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
        self.execute_rw(
                "insert into ivm.cursors(
                     view_id, source_table_id, partition_desc, last_version, last_timestamp)
                 values ($1::TEXT, $2::TEXT, $3::TEXT, $4::BIGINT, $5::BIGINT)
                 on conflict (view_id, source_table_id, partition_desc) do update
                 set last_version = excluded.last_version,
                     last_timestamp = excluded.last_timestamp",
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

    /// The rebuild generation of a view.
    pub async fn view_generation(&self, view_id: &str) -> Result<i64> {
        let row = self
            .client
            .query_opt(
                "select generation from ivm.views where view_id = $1::TEXT",
                QueryType::RO,
                &[&view_id],
            )
            .await?;
        row.map(|row| row.get(0))
            .ok_or_else(|| rootcause::report!("view {view_id} is not registered"))
    }

    /// Set the lifecycle status of a view (`active` / `rebuilding`).
    pub async fn set_view_status(&self, view_id: &str, status: &str) -> Result<()> {
        self.execute_rw(
            "update ivm.views set status = $2::TEXT where view_id = $1::TEXT",
            &[&view_id, &status],
        )
        .await?;
        Ok(())
    }

    /// The lifecycle status of a view, if it is registered.
    pub async fn view_status(&self, view_id: &str) -> Result<Option<String>> {
        let row = self
            .client
            .query_opt(
                "select status from ivm.views where view_id = $1::TEXT",
                QueryType::RO,
                &[&view_id],
            )
            .await?;
        Ok(row.map(|row| row.get(0)))
    }

    /// Start a rebuild: bump the generation and return it. Epochs of older
    /// generations stay for auditing but can no longer be matched.
    pub async fn bump_generation(&self, view_id: &str) -> Result<i64> {
        let row = self
            .query_opt_rw(
                "update ivm.views set generation = generation + 1
                 where view_id = $1::TEXT returning generation",
                &[&view_id],
            )
            .await?;
        row.map(|row| row.get(0))
            .ok_or_else(|| rootcause::report!("view {view_id} is not registered"))
    }

    /// Delete every cursor of a view (a rebuild re-baselines them).
    pub async fn delete_cursors(&self, view_id: &str) -> Result<()> {
        self.execute_rw(
            "delete from ivm.cursors where view_id = $1::TEXT",
            &[&view_id],
        )
        .await?;
        Ok(())
    }

    /// The latest committed epoch of a view generation.
    pub async fn latest_committed_epoch(
        &self,
        view_id: &str,
        generation: i64,
    ) -> Result<Option<EpochRecord>> {
        let row = self
            .client
            .query_opt(
                &format!(
                    "select {EPOCH_COLUMNS} from ivm.epochs
                     where view_id = $1::TEXT and generation = $2::BIGINT
                       and status = 'committed'
                     order by epoch desc limit 1"
                ),
                QueryType::RO,
                &[&view_id, &generation],
            )
            .await?;
        row.map(|row| epoch_record_from_row(&row)).transpose()
    }

    /// Look up one window by its canonical key.
    pub async fn get_epoch(
        &self,
        view_id: &str,
        generation: i64,
        window_key: &str,
    ) -> Result<Option<EpochRecord>> {
        let row = self
            .client
            .query_opt(
                &format!(
                    "select {EPOCH_COLUMNS} from ivm.epochs
                     where view_id = $1::TEXT and generation = $2::BIGINT
                       and window_key = $3::TEXT"
                ),
                QueryType::RO,
                &[&view_id, &generation, &window_key],
            )
            .await?;
        row.map(|row| epoch_record_from_row(&row)).transpose()
    }

    /// Begin the refresh window `window_key`.
    ///
    /// Returns the existing epoch row (pending or committed) if this window was
    /// attempted before, otherwise allocates the next monotonic epoch from
    /// `ivm.views.last_epoch` and inserts it as `pending`. The caller is
    /// responsible for applying the window and calling
    /// [`IvmMetadata::mark_epoch_committed`].
    pub async fn begin_epoch(
        &self,
        view_id: &str,
        window_key: &str,
        to_versions: &[SourceVersionRange],
        mv_versions_before: &[PartitionVersion],
    ) -> Result<BeginEpoch> {
        let generation = self.view_generation(view_id).await?;
        if let Some(record) = self.get_epoch(view_id, generation, window_key).await? {
            return Ok(match record.status {
                EpochStatus::Committed => BeginEpoch::Committed(record),
                EpochStatus::Pending => BeginEpoch::Pending(record),
            });
        }

        let epoch: i64 = self
            .query_opt_rw(
                "update ivm.views set last_epoch = last_epoch + 1
                 where view_id = $1::TEXT returning last_epoch",
                &[&view_id],
            )
            .await?
            .ok_or_else(|| rootcause::report!("view {view_id} is not registered"))?
            .get(0);

        self.execute_rw(
            "insert into ivm.epochs(
                     view_id, generation, epoch, window_key, status,
                     to_versions, mv_versions_before, mv_versions, created_at)
                 values ($1::TEXT, $2::BIGINT, $3::BIGINT, $4::TEXT, 'pending',
                     $5::JSONB, $6::JSONB, '[]'::jsonb, $7::BIGINT)
                 on conflict (view_id, generation, window_key) do nothing",
            &[
                &view_id,
                &generation,
                &epoch,
                &window_key,
                &serde_json::to_value(to_versions)?,
                &serde_json::to_value(mv_versions_before)?,
                &crate::now_ms(),
            ],
        )
        .await?;

        // A concurrent attempt may have won the insert for this window.
        let record = self
            .get_epoch(view_id, generation, window_key)
            .await?
            .ok_or_else(|| {
                rootcause::report!("epoch row for {view_id}/{window_key} disappeared")
            })?;
        Ok(match record.status {
            EpochStatus::Committed => BeginEpoch::Committed(record),
            EpochStatus::Pending if record.epoch == epoch => BeginEpoch::Created(record),
            EpochStatus::Pending => BeginEpoch::Pending(record),
        })
    }

    /// Mark an epoch committed and record the MV versions it produced.
    pub async fn mark_epoch_committed(
        &self,
        record: &EpochRecord,
        mv_versions: &[PartitionVersion],
    ) -> Result<()> {
        self.execute_rw(
                "update ivm.epochs
                 set status = 'committed', mv_versions = $4::JSONB, committed_at = $5::BIGINT
                 where view_id = $1::TEXT and generation = $2::BIGINT and epoch = $3::BIGINT",
            &[
                &record.view_id,
                &record.generation,
                &record.epoch,
                &serde_json::to_value(mv_versions)?,
                &crate::now_ms(),
            ],
        )
        .await?;
        Ok(())
    }

    /// All committed epochs of a view generation, ordered by epoch.
    pub async fn list_committed_epochs(
        &self,
        view_id: &str,
        generation: i64,
    ) -> Result<Vec<EpochRecord>> {
        let rows = self
            .client
            .query(
                &format!(
                    "select {EPOCH_COLUMNS} from ivm.epochs
                     where view_id = $1::TEXT and generation = $2::BIGINT
                       and status = 'committed'
                     order by epoch"
                ),
                QueryType::RO,
                &[&view_id, &generation],
            )
            .await?;
        rows.iter().map(epoch_record_from_row).collect()
    }

    /// The highest committed `to_version` per `(source_table_id,
    /// partition_desc)` of a view generation.
    pub async fn max_committed_to_versions(
        &self,
        view_id: &str,
        generation: i64,
    ) -> Result<HashMap<(String, String), i64>> {
        let mut max = HashMap::new();
        for record in self.list_committed_epochs(view_id, generation).await? {
            for range in record.to_versions {
                let entry = max
                    .entry((range.source_table_id, range.partition_desc))
                    .or_insert(range.to_version);
                *entry = (*entry).max(range.to_version);
            }
        }
        Ok(max)
    }

    /// Crash-recovery/test helper: move a committed epoch back to pending.
    #[doc(hidden)]
    pub async fn set_epoch_pending(
        &self,
        view_id: &str,
        generation: i64,
        epoch: i64,
    ) -> Result<()> {
        self.execute_rw(
                "update ivm.epochs set status = $4::TEXT, committed_at = null
                 where view_id = $1::TEXT and generation = $2::BIGINT and epoch = $3::BIGINT",
                &[&view_id, &generation, &epoch, &EpochStatus::Pending.as_str()],
        )
        .await?;
        Ok(())
    }

    /// Register the internal table a view uses for one state role.
    ///
    /// Registration is idempotent for the same table but refuses to bind an
    /// existing `(view, role)` to a different one, so a view id cannot silently
    /// switch or share its state.
    pub async fn register_state(
        &self,
        view_id: &str,
        role: StateRole,
        table: &crate::table::IvmTable,
    ) -> Result<()> {
        self.execute_rw(
                "insert into ivm.states(
                     view_id, role, table_id, table_name, namespace, table_path, created_at)
                 values ($1::TEXT, $2::TEXT, $3::TEXT, $4::TEXT, $5::TEXT, $6::TEXT, $7::BIGINT)
                 on conflict (view_id, role) do nothing",
            &[
                &view_id,
                &role.as_str(),
                &table.table_id,
                &table.table_name,
                &table.namespace,
                &table.table_path,
                &crate::now_ms(),
            ],
        )
        .await?;

        if let Some(existing) = self.get_state(view_id, role).await?
            && existing.table_id != table.table_id
        {
            return Err(rootcause::report!(
                "view {view_id} already uses table {} as its {} state",
                existing.table_name,
                role.as_str()
            ));
        }
        Ok(())
    }

    /// The internal table registered for one `(view, role)`.
    pub async fn get_state(
        &self,
        view_id: &str,
        role: StateRole,
    ) -> Result<Option<StateTable>> {
        let row = self
            .client
            .query_opt(
                "select view_id, role, table_id, table_name, namespace, table_path, created_at
                 from ivm.states where view_id = $1::TEXT and role = $2::TEXT",
                QueryType::RO,
                &[&view_id, &role.as_str()],
            )
            .await?;
        row.map(|row| state_table_from_row(&row)).transpose()
    }

    /// All internal tables registered for a view.
    pub async fn list_states(&self, view_id: &str) -> Result<Vec<StateTable>> {
        let rows = self
            .client
            .query(
                "select view_id, role, table_id, table_name, namespace, table_path, created_at
                 from ivm.states where view_id = $1::TEXT order by role, table_name",
                QueryType::RO,
                &[&view_id],
            )
            .await?;
        rows.iter().map(state_table_from_row).collect()
    }

    /// Insert or update a consumer watermark.
    ///
    /// `last_epoch` is the oldest epoch the consumer still needs; it may move
    /// backwards when a consumer resets its state.
    pub async fn upsert_consumer(
        &self,
        view_id: &str,
        consumer_id: &str,
        last_epoch: i64,
    ) -> Result<()> {
        self.execute_rw(
            "insert into ivm.consumers(view_id, consumer_id, last_epoch, updated_at)
                 values ($1::TEXT, $2::TEXT, $3::BIGINT, $4::BIGINT)
                 on conflict (view_id, consumer_id) do update
                 set last_epoch = excluded.last_epoch,
                     updated_at = excluded.updated_at",
            &[&view_id, &consumer_id, &last_epoch, &crate::now_ms()],
        )
        .await?;
        Ok(())
    }

    /// The consumers of a view.
    pub async fn list_consumers(&self, view_id: &str) -> Result<Vec<Consumer>> {
        let rows = self
            .client
            .query(
                "select view_id, consumer_id, last_epoch, updated_at
                 from ivm.consumers where view_id = $1::TEXT order by consumer_id",
                QueryType::RO,
                &[&view_id],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| Consumer {
                view_id: row.get(0),
                consumer_id: row.get(1),
                last_epoch: row.get(2),
                updated_at: row.get(3),
            })
            .collect())
    }

    /// Remove a consumer.
    pub async fn delete_consumer(&self, view_id: &str, consumer_id: &str) -> Result<()> {
        self.execute_rw(
                "delete from ivm.consumers where view_id = $1::TEXT and consumer_id = $2::TEXT",
                &[&view_id, &consumer_id],
        )
        .await?;
        Ok(())
    }

    /// The oldest epoch any consumer of the view still needs, if any.
    pub async fn consumer_watermark(&self, view_id: &str) -> Result<Option<i64>> {
        let row = self
            .client
            .query_opt(
                "select min(last_epoch) from ivm.consumers where view_id = $1::TEXT",
                QueryType::RO,
                &[&view_id],
            )
            .await?;
        Ok(row.and_then(|row| row.get(0)))
    }

    /// Delete committed epochs below the consumer watermark minus `grace`.
    ///
    /// Nothing is deleted while the view has no consumers, so a consumer can
    /// always catch up from the retained history. Pending epochs are never
    /// deleted: they still have to be recovered or inspected.
    pub async fn gc_epochs(&self, view_id: &str, grace: i64) -> Result<u64> {
        let Some(watermark) = self.consumer_watermark(view_id).await? else {
            return Ok(0);
        };
        let before = watermark.saturating_sub(grace);
        let deleted = self
            .execute_rw(
                "delete from ivm.epochs
                 where view_id = $1::TEXT and status = 'committed' and epoch < $2::BIGINT",
                &[&view_id, &before],
            )
            .await?;
        Ok(deleted)
    }

    /// Delete a view and its cursors.
    pub async fn delete_view(&self, view_id: &str) -> Result<()> {
        self.execute_rw(
            "delete from ivm.consumers where view_id = $1::TEXT",
            &[&view_id],
        )
        .await?;
        self.execute_rw(
            "delete from ivm.states where view_id = $1::TEXT",
            &[&view_id],
        )
        .await?;
        self.execute_rw(
            "delete from ivm.epochs where view_id = $1::TEXT",
            &[&view_id],
        )
        .await?;
        self.execute_rw(
            "delete from ivm.cursors where view_id = $1::TEXT",
            &[&view_id],
        )
        .await?;
        self.execute_rw(
            "delete from ivm.views where view_id = $1::TEXT",
            &[&view_id],
        )
        .await?;
        Ok(())
    }
}

fn state_table_from_row(row: &tokio_postgres::Row) -> Result<StateTable> {
    Ok(StateTable {
        view_id: row.get(0),
        role: StateRole::parse(row.get::<_, String>(1).as_str())?,
        table_id: row.get(2),
        table_name: row.get(3),
        namespace: row.get(4),
        table_path: row.get(5),
        created_at: row.get(6),
    })
}

fn epoch_record_from_row(row: &tokio_postgres::Row) -> Result<EpochRecord> {
    Ok(EpochRecord {
        view_id: row.get(0),
        generation: row.get(1),
        epoch: row.get(2),
        window_key: row.get(3),
        status: EpochStatus::parse(row.get::<_, String>(4).as_str())?,
        to_versions: serde_json::from_value(row.get(5))?,
        mv_versions_before: serde_json::from_value(row.get(6))?,
        mv_versions: serde_json::from_value(row.get(7))?,
        created_at: row.get(8),
        committed_at: row.get(9),
    })
}
