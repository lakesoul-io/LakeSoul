// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Detached query jobs: durable definition, progress and result metadata.
//!
//! A detached query is one statement submitted by a PostgreSQL client and run
//! by the server outside that client's connection. The connection that
//! submitted it only writes one row here (and later reads status/result); the
//! execution belongs to the job runner.
//!
//! The tables live next to the other server-side metadata schemas (`ivm`,
//! `subscription`). `script/metadata-migrations/V4000007__query_jobs.sql`
//! carries the same DDL for deployments that apply migrations up front; the
//! `create ... if not exists` statements here keep a fresh test database
//! usable without the migration tool.

use lakesoul_metadata::{PooledClient, QueryType};
use rootcause::report;
use uuid::Uuid;

use crate::Result;

const JOBS_SCHEMA_DDL: &str = r#"
create schema if not exists job;

create table if not exists job.queries (
    query_id           uuid primary key,
    job_key            text,
    user_name          text   not null,
    database_name      text   not null,
    sql_text           text   not null,
    settings           jsonb  not null,
    state              text   not null default 'queued',
    attempt            integer not null default 0,
    max_attempts       integer not null default 3,
    retry_on_restart   boolean not null default true,
    timeout_ms         bigint,
    created_at         bigint not null,
    started_at         bigint,
    finished_at        bigint,
    lease_expires_at   bigint,
    owner              text,
    cancel_requested   boolean not null default false,
    command_tag        text,
    rows_produced      bigint not null default 0,
    bytes_produced     bigint not null default 0,
    error_sqlstate     text,
    error_message      text,
    result_kind        text   not null default 'none',
    result_rows        bigint,
    result_bytes       bigint,
    result_location    text,
    result_schema      jsonb,
    result_sets        jsonb,
    result_expires_at  bigint
);

alter table if exists job.queries
    add column if not exists result_schema jsonb;
alter table if exists job.queries
    add column if not exists result_sets jsonb;
alter table if exists job.queries
    add column if not exists attempt integer not null default 0;
alter table if exists job.queries
    add column if not exists max_attempts integer not null default 3;
alter table if exists job.queries
    add column if not exists retry_on_restart boolean not null default true;
alter table if exists job.queries
    add column if not exists timeout_ms bigint;

create index if not exists job_queries_state_idx
    on job.queries (state, created_at);

create index if not exists job_queries_user_idx
    on job.queries (user_name, created_at desc);

drop index if exists job.job_queries_user_key_idx;

create unique index if not exists job_queries_scope_key_idx
    on job.queries (database_name, user_name, job_key);
"#;

/// One row of `job.queries` as the server writes it.
pub struct NewJob<'a> {
    pub query_id: Uuid,
    pub job_key: Option<&'a str>,
    pub user_name: &'a str,
    pub database_name: &'a str,
    pub sql_text: &'a str,
    pub settings: &'a serde_json::Value,
    pub created_at: i64,
    /// The server process that accepted the job. A restart uses the same id
    /// (address) to reconcile only its own unfinished rows.
    pub owner: &'a str,
    pub timeout_ms: Option<i64>,
    pub max_attempts: i32,
    pub retry_on_restart: bool,
}

/// The terminal state one job attempt ends in.
pub enum FinishState {
    Succeeded,
    Failed,
    Canceled,
    Lost,
}

impl FinishState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Lost => "lost",
        }
    }
}

/// The durable half of the detached-query service.
pub struct JobMetadata {
    client: PooledClient,
}

impl JobMetadata {
    pub fn from_pool(client: PooledClient) -> Self {
        Self { client }
    }

    pub async fn init_schema(&self) -> Result<()> {
        self.client
            .batch_execute(JOBS_SCHEMA_DDL, QueryType::RW)
            .await
            .map_err(|error| report!("create query job metadata schema: {error}"))
    }

    /// Inserts a queued job unless the user already submitted one with the
    /// same `job_key`. Returns the existing id in that case.
    pub async fn insert_job(&self, job: &NewJob<'_>) -> Result<Option<Uuid>> {
        let row = self
            .client
            .query_opt(
                "insert into job.queries
                    (query_id, job_key, user_name, database_name, sql_text,
                     settings, state, created_at, owner, timeout_ms,
                     max_attempts, retry_on_restart)
                 values ($1, $2, $3, $4, $5, $6, 'queued', $7, $8, $9, $10, $11)
                 on conflict (database_name, user_name, job_key) do nothing
                 returning query_id",
                QueryType::RW,
                &[
                    &job.query_id,
                    &job.job_key,
                    &job.user_name,
                    &job.database_name,
                    &job.sql_text,
                    &job.settings,
                    &job.created_at,
                    &job.owner,
                    &job.timeout_ms,
                    &job.max_attempts,
                    &job.retry_on_restart,
                ],
            )
            .await
            .map_err(|error| report!("insert query job: {error}"))?;
        Ok(row.map(|row| row.get(0)))
    }

    /// Looks up the job a repeated `job_key` refers to.
    pub async fn job_by_key(
        &self,
        database: &str,
        user: &str,
        key: &str,
    ) -> Result<Option<Uuid>> {
        let row = self
            .client
            .query_opt(
                "select query_id from job.queries \
                 where database_name = $1 and user_name = $2 and job_key = $3",
                QueryType::RW,
                &[&database, &user, &key],
            )
            .await
            .map_err(|error| report!("look up query job by key: {error}"))?;
        Ok(row.map(|row| row.get(0)))
    }

    /// Takes the queued job for one runner. A job that was canceled or
    /// claimed meanwhile is not taken.
    pub async fn mark_running(
        &self,
        query_id: Uuid,
        owner: &str,
        lease_expires_at: i64,
        started_at: i64,
    ) -> Result<Option<i32>> {
        let row = self
            .client
            .query_opt(
                "update job.queries
                 set state = 'running', owner = $2, lease_expires_at = $3,
                     started_at = $4, attempt = attempt + 1
                 where query_id = $1 and state = 'queued'
                 returning attempt",
                QueryType::RW,
                &[&query_id, &owner, &lease_expires_at, &started_at],
            )
            .await
            .map_err(|error| report!("mark query job running: {error}"))?;
        Ok(row.map(|row| row.get(0)))
    }

    /// Renews the lease and records progress. Returns false when the runner
    /// no longer owns a running job (canceled, lost or finished elsewhere).
    pub async fn heartbeat(
        &self,
        query_id: Uuid,
        owner: &str,
        lease_expires_at: i64,
        rows_produced: i64,
        bytes_produced: i64,
    ) -> Result<bool> {
        let rows = self
            .client
            .execute(
                "update job.queries
                 set lease_expires_at = $3, rows_produced = $4,
                     bytes_produced = $5
                 where query_id = $1 and owner = $2 and state = 'running'
                   and not cancel_requested",
                QueryType::RW,
                &[
                    &query_id,
                    &owner,
                    &lease_expires_at,
                    &rows_produced,
                    &bytes_produced,
                ],
            )
            .await
            .map_err(|error| report!("heartbeat query job: {error}"))?;
        Ok(rows > 0)
    }

    /// Records the terminal state. `result_*` are only meaningful for a
    /// successful job; callers pass empty values otherwise.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish(
        &self,
        query_id: Uuid,
        state: FinishState,
        finished_at: i64,
        rows_produced: i64,
        bytes_produced: i64,
        command_tag: Option<&str>,
        error_sqlstate: Option<&str>,
        error_message: Option<&str>,
        result_kind: &str,
        result_rows: Option<i64>,
        result_bytes: Option<i64>,
        result_location: Option<&str>,
        result_schema: Option<&serde_json::Value>,
        result_sets: Option<&serde_json::Value>,
        result_expires_at: Option<i64>,
    ) -> Result<()> {
        self.client
            .execute(
                "update job.queries
                 set state = $2, finished_at = $3,
                     rows_produced = $4, bytes_produced = $5,
                     command_tag = $6, error_sqlstate = $7,
                     error_message = $8, result_kind = $9,
                     result_rows = $10, result_bytes = $11,
                     result_location = $12, result_schema = $13,
                     result_sets = $14, result_expires_at = $15,
                     lease_expires_at = null
                 where query_id = $1 and state in ('queued', 'running')",
                QueryType::RW,
                &[
                    &query_id,
                    &state.as_str(),
                    &finished_at,
                    &rows_produced,
                    &bytes_produced,
                    &command_tag,
                    &error_sqlstate,
                    &error_message,
                    &result_kind,
                    &result_rows,
                    &result_bytes,
                    &result_location,
                    &result_schema,
                    &result_sets,
                    &result_expires_at,
                ],
            )
            .await
            .map_err(|error| report!("finish query job: {error}"))?;
        Ok(())
    }

    /// Asks a queued or running job to stop.
    pub async fn request_cancel(&self, query_id: Uuid) -> Result<bool> {
        let rows = self
            .client
            .execute(
                "update job.queries set cancel_requested = true
                 where query_id = $1 and state in ('queued', 'running')",
                QueryType::RW,
                &[&query_id],
            )
            .await
            .map_err(|error| report!("cancel query job: {error}"))?;
        Ok(rows > 0)
    }

    /// Running jobs whose runner stopped renewing its lease.
    pub async fn expired_running(&self, now: i64) -> Result<Vec<Uuid>> {
        let rows = self
            .client
            .query(
                "select query_id from job.queries
                 where state = 'running' and lease_expires_at < $1",
                QueryType::RW,
                &[&now],
            )
            .await
            .map_err(|error| report!("list expired query jobs: {error}"))?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    /// Marks one lease-expired job lost.
    pub async fn mark_lost(&self, query_id: Uuid, now: i64) -> Result<bool> {
        let rows = self
            .client
            .execute(
                "update job.queries
                 set state = 'lost', finished_at = $2,
                     error_message = 'query runner stopped renewing its lease',
                     lease_expires_at = null
                 where query_id = $1 and state = 'running'",
                QueryType::RW,
                &[&query_id, &now],
            )
            .await
            .map_err(|error| report!("mark query job lost: {error}"))?;
        Ok(rows > 0)
    }

    /// Spooled results whose retention window elapsed.
    pub async fn expired_results(&self, now: i64) -> Result<Vec<(Uuid, String)>> {
        let rows = self
            .client
            .query(
                "select query_id, result_location from job.queries
                 where result_kind = 'spool' and result_location is not null
                   and result_expires_at < $1",
                QueryType::RW,
                &[&now],
            )
            .await
            .map_err(|error| report!("list expired query results: {error}"))?;
        Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
    }

    /// Records that a spooled result was cleaned up.
    pub async fn mark_result_expired(&self, query_id: Uuid) -> Result<()> {
        self.client
            .execute(
                "update job.queries
                 set result_kind = 'expired', result_location = null,
                     result_rows = null, result_bytes = null,
                     result_schema = null, result_sets = null
                 where query_id = $1",
                QueryType::RW,
                &[&query_id],
            )
            .await
            .map_err(|error| report!("mark query result expired: {error}"))?;
        Ok(())
    }

    /// Deletes job history past the record retention window.
    pub async fn delete_old(&self, finished_before: i64) -> Result<u64> {
        let rows = self
            .client
            .execute(
                "delete from job.queries
                 where finished_at is not null and finished_at < $1",
                QueryType::RW,
                &[&finished_before],
            )
            .await
            .map_err(|error| report!("delete old query jobs: {error}"))?;
        Ok(rows)
    }
}

/// An unfinished job this server owned before a restart.
pub struct RecoveredJob {
    pub query_id: Uuid,
    pub job_key: Option<String>,
    pub user_name: String,
    pub database_name: String,
    pub sql_text: String,
    pub settings: serde_json::Value,
    pub state: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_ms: Option<i64>,
    pub created_at: i64,
    pub cancel_requested: bool,
}

/// A finished SELECT result that can still be served after a server restart.
pub struct RecoveredSpool {
    pub query_id: Uuid,
    pub job_key: Option<String>,
    pub user_name: String,
    pub database_name: String,
    pub sql_text: String,
    pub settings: serde_json::Value,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub command_tag: Option<String>,
    pub attempt: i32,
    pub rows: i64,
    pub bytes: i64,
    pub location: String,
    pub schema: Option<serde_json::Value>,
    pub result_sets: Option<serde_json::Value>,
    pub expires_at: i64,
}

impl JobMetadata {
    /// Unfinished jobs accepted by this server id. They are re-queued (or
    /// finished as canceled/lost) by [`JobService`] at startup.
    pub async fn recoverable_jobs(&self, owner: &str) -> Result<Vec<RecoveredJob>> {
        let rows = self
            .client
            .query(
                "select query_id, job_key, user_name, database_name, sql_text,
                        settings, state, attempt, max_attempts,
                        timeout_ms, created_at, cancel_requested
                 from job.queries
                 where state in ('queued', 'running') and owner = $1
                 order by created_at",
                QueryType::RW,
                &[&owner],
            )
            .await
            .map_err(|error| report!("load recoverable query jobs: {error}"))?;
        Ok(rows
            .iter()
            .map(|row| RecoveredJob {
                query_id: row.get(0),
                job_key: row.get(1),
                user_name: row.get(2),
                database_name: row.get(3),
                sql_text: row.get(4),
                settings: row.get(5),
                state: row.get(6),
                attempt: row.get(7),
                max_attempts: row.get(8),
                timeout_ms: row.get(9),
                created_at: row.get(10),
                cancel_requested: row.get(11),
            })
            .collect())
    }

    /// Puts an unfinished job back in the queue without counting a new
    /// attempt; the attempt is incremented when a runner claims it.
    pub async fn requeue_job(&self, query_id: Uuid, owner: &str) -> Result<()> {
        self.client
            .execute(
                "update job.queries
                 set state = 'queued', started_at = null, finished_at = null,
                     lease_expires_at = null, command_tag = null,
                     error_sqlstate = null, error_message = null,
                     result_kind = 'none', result_rows = null,
                     result_bytes = null, result_location = null,
                     result_schema = null, result_sets = null,
                     result_expires_at = null
                 where query_id = $1 and state in ('queued', 'running')
                   and owner = $2",
                QueryType::RW,
                &[&query_id, &owner],
            )
            .await
            .map_err(|error| report!("requeue query job: {error}"))?;
        Ok(())
    }

    /// Completed SELECT results whose 24h retention window is still open.
    /// Running jobs are deliberately not recovered: DataFusion distributed has
    /// no mid-query checkpoint, so they are marked lost instead.
    pub async fn recoverable_spools(&self, now: i64) -> Result<Vec<RecoveredSpool>> {
        let rows = self
            .client
            .query(
                "select query_id, job_key, user_name, database_name, sql_text,
                        settings, created_at, started_at, finished_at, command_tag,
                        attempt, result_rows, result_bytes, result_location,
                        result_schema, result_sets, result_expires_at
                 from job.queries
                 where state = 'succeeded' and result_kind = 'spool'
                   and result_expires_at > $1",
                QueryType::RW,
                &[&now],
            )
            .await
            .map_err(|error| report!("load recoverable query results: {error}"))?;
        Ok(rows
            .iter()
            .map(|row| RecoveredSpool {
                query_id: row.get(0),
                job_key: row.get(1),
                user_name: row.get(2),
                database_name: row.get(3),
                sql_text: row.get(4),
                settings: row.get(5),
                created_at: row.get(6),
                started_at: row.get(7),
                finished_at: row.get(8),
                command_tag: row.get(9),
                attempt: row.get(10),
                rows: row.get(11),
                bytes: row.get(12),
                location: row.get(13),
                schema: row.get(14),
                result_sets: row.get(15),
                expires_at: row.get(16),
            })
            .collect())
    }
}
