// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Detached query jobs.
//!
//! A job is one SQL statement that runs outside the PostgreSQL connection that
//! submitted it. The connection is only a control channel: it writes a
//! `job.queries` row, returns the query id, and later polls status or reads the
//! result. Losing the connection therefore does not touch the execution.
//!
//! P1 runs jobs in this process. A server restart marks the unfinished jobs
//! `lost` instead of resuming them: DataFusion distributed has no mid-query
//! checkpoint, so resuming would silently restart the statement from scratch.

pub mod metadata;
mod result;
mod runner;
mod sql;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use arrow_schema::SchemaRef;
use chrono::Utc;
use datafusion::arrow::datatypes::Schema;
use futures::StreamExt;
use lakesoul_common::misc::TimeZone;
use lakesoul_metadata::MetaDataClientRef;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::Result;
use crate::cancel::QueryToken;
use crate::session::SessionSettings;
use metadata::{FinishState, JobMetadata, NewJob};

pub use result::{
    QueriesFunction, QueryResultFunction, QueryResultSetFunction,
    QueryResultSetsFunction, QueryStatusFunction,
};
pub use runner::JobRunner;
pub(crate) use sql::JobIdentityGuard;
pub use sql::JobStatementHook;
pub(crate) use sql::register_control_stubs;

pub(crate) use runner::spool_scan;

/// The result one job's statement produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultKind {
    /// A script with only `SET` statements or no result.
    None,
    /// A write: the result is the target table's commit, not a row set.
    Table,
    /// A read: the rows are in one Parquet spool file.
    Spool,
    /// The result existed but its retention window elapsed.
    Expired,
}

impl ResultKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Table => "table",
            Self::Spool => "spool",
            Self::Expired => "expired",
        }
    }
}

/// One job's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Canceled,
    Lost,
}

impl JobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Lost => "lost",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Canceled | Self::Lost
        )
    }
}

/// The connection-local settings a job inherits at submit time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobSettings {
    pub search_path: Vec<String>,
    pub time_zone: Option<String>,
    pub ivm_enabled: bool,
    pub application_name: Option<String>,
}

impl JobSettings {
    pub fn from_session(
        settings: &SessionSettings,
        ivm_enabled: bool,
        application_name: Option<String>,
    ) -> Self {
        Self {
            search_path: settings.search_path.clone(),
            time_zone: settings.time_zone.iana_name().map(str::to_owned),
            ivm_enabled,
            application_name,
        }
    }

    /// A fresh session configuration. `statement_timeout` is deliberately not
    /// inherited: a job has its own timeout, and a 30s interactive timeout
    /// must not kill an ETL.
    pub fn to_session_settings(&self) -> SessionSettings {
        let time_zone = self
            .time_zone
            .as_deref()
            .and_then(|name| TimeZone::get(name).ok())
            .unwrap_or(TimeZone::UTC);
        SessionSettings {
            search_path: self.search_path.clone(),
            time_zone,
            statement_timeout: None,
        }
    }
}

/// A spooled result file.
#[derive(Debug, Clone)]
pub struct JobFile {
    pub url: String,
    pub size: u64,
}

/// One SELECT result inside a script.
#[derive(Debug, Clone)]
pub struct JobResultSet {
    /// 0-based index among the script's SELECTs.
    pub set_index: usize,
    /// 0-based index of the statement that produced it.
    pub statement_index: usize,
    pub schema: SchemaRef,
    pub file: JobFile,
    pub rows: u64,
    pub bytes: u64,
}

/// The durable form of one result set in `job.queries.result_sets`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredResultSet {
    pub set_index: usize,
    pub statement_index: usize,
    pub location: String,
    pub schema: serde_json::Value,
    pub rows: u64,
    pub bytes: u64,
}

/// The result a finished job exposes to later readers.
#[derive(Debug, Clone)]
pub struct JobResult {
    pub kind: ResultKind,
    /// Every SELECT result, in script order.
    pub sets: Vec<JobResultSet>,
    /// The last SELECT's schema, used by the single-result SQL function.
    pub schema: SchemaRef,
    /// Total result rows (SELECT rows plus written rows).
    pub rows: u64,
    /// Total spooled bytes.
    pub bytes: u64,
    pub expires_at: Option<i64>,
    pub command_tag: Option<String>,
    /// The spool directory (`.../attempt-<n>/`) for retention cleanup.
    pub location: Option<String>,
}

impl JobResult {
    pub fn script(
        sets: Vec<JobResultSet>,
        insert_rows: u64,
        command_tag: Option<String>,
        expires_at: Option<i64>,
        location: Option<String>,
    ) -> Self {
        let bytes = sets.iter().map(|set| set.bytes).sum();
        let set_rows: u64 = sets.iter().map(|set| set.rows).sum();
        let last = sets.last();
        let kind = if !sets.is_empty() {
            ResultKind::Spool
        } else if command_tag.is_some() || insert_rows > 0 {
            ResultKind::Table
        } else {
            ResultKind::None
        };
        Self {
            kind,
            schema: last
                .map(|set| set.schema.clone())
                .unwrap_or_else(|| Arc::new(Schema::empty())),
            sets,
            rows: set_rows.saturating_add(insert_rows),
            bytes,
            expires_at,
            command_tag,
            location,
        }
    }
}

/// The mutable status of one job.
#[derive(Debug, Clone)]
pub struct JobStatus {
    pub state: JobState,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub command_tag: Option<String>,
    pub error_sqlstate: Option<String>,
    pub error_message: Option<String>,
    pub result_kind: Option<ResultKind>,
    pub result: Option<Arc<JobResult>>,
    pub result_expires_at: Option<i64>,
    pub cancel_requested: bool,
}

impl Default for JobStatus {
    fn default() -> Self {
        Self {
            state: JobState::Queued,
            started_at: None,
            finished_at: None,
            command_tag: None,
            error_sqlstate: None,
            error_message: None,
            result_kind: None,
            result: None,
            result_expires_at: None,
            cancel_requested: false,
        }
    }
}

/// What a cancel request found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The job had already finished.
    Finished,
    /// The job was still queued and is now canceled.
    Queued,
    /// The job is running; its token was fired.
    Running,
}

/// One in-memory job. The durable copy is the `job.queries` row; this is the
/// low-latency view status polls read and the place the runner publishes its
/// progress.
pub struct JobHandle {
    pub id: Uuid,
    pub job_key: Option<String>,
    pub user: String,
    pub database: String,
    pub sql: String,
    pub settings: JobSettings,
    pub created_at: i64,
    pub timeout_ms: Option<i64>,
    /// The attempt currently running; 0 until the first `mark_running`.
    pub attempt: AtomicU32,
    pub token: QueryToken,
    pub rows: AtomicU64,
    pub bytes: AtomicU64,
    status: RwLock<JobStatus>,
}

impl JobHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Uuid,
        job_key: Option<String>,
        user: String,
        database: String,
        sql: String,
        settings: JobSettings,
        created_at: i64,
        timeout_ms: Option<i64>,
        attempt: u32,
    ) -> Self {
        Self {
            id,
            job_key,
            user,
            database,
            sql,
            settings,
            created_at,
            timeout_ms,
            attempt: AtomicU32::new(attempt),
            token: QueryToken::new(),
            rows: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            status: RwLock::new(JobStatus::default()),
        }
    }

    pub fn set_attempt(&self, attempt: u32) {
        self.attempt.store(attempt, Ordering::Relaxed);
    }

    pub fn state(&self) -> JobState {
        self.status.read().state
    }

    pub fn status(&self) -> JobStatus {
        self.status.read().clone()
    }

    pub fn note_progress(&self, rows: u64, bytes: u64) {
        self.rows.store(rows, Ordering::Relaxed);
        self.bytes.store(bytes, Ordering::Relaxed);
    }

    /// Takes the job from queued to running. False when the job was canceled
    /// before it started or was already claimed.
    pub fn begin_running(&self) -> bool {
        let mut status = self.status.write();
        if status.state != JobState::Queued || status.cancel_requested {
            return false;
        }
        status.state = JobState::Running;
        status.started_at = Some(now_ms());
        true
    }

    /// Marks a cancel request. A queued job is canceled here; a running one
    /// is left to its runner, which watches the token.
    pub fn request_cancel(&self) -> CancelOutcome {
        let outcome = {
            let mut status = self.status.write();
            if status.state.is_terminal() {
                CancelOutcome::Finished
            } else {
                status.cancel_requested = true;
                if status.state == JobState::Queued {
                    status.state = JobState::Canceled;
                    status.finished_at = Some(now_ms());
                    status.error_sqlstate = Some("57014".to_string());
                    status.error_message =
                        Some("canceling statement due to user request".to_string());
                    CancelOutcome::Queued
                } else {
                    CancelOutcome::Running
                }
            }
        };
        self.token.cancel();
        outcome
    }

    pub fn set_succeeded(&self, result: JobResult) {
        let mut status = self.status.write();
        if status.state != JobState::Running {
            return;
        }
        status.state = JobState::Succeeded;
        status.finished_at = Some(now_ms());
        status.result_kind = Some(result.kind);
        status.result_expires_at = result.expires_at;
        status.command_tag = result.command_tag.clone();
        status.result = Some(Arc::new(result));
    }

    /// Rebuilds a succeeded job from its durable row after a restart. The
    /// result file and schema are still valid; this makes a completed SELECT
    /// readable for the rest of its retention window.
    pub fn restore_succeeded(
        &self,
        started_at: Option<i64>,
        finished_at: Option<i64>,
        result: JobResult,
    ) {
        let rows = result.rows;
        let bytes = result.bytes;
        let mut status = self.status.write();
        status.state = JobState::Succeeded;
        status.started_at = started_at;
        status.finished_at = finished_at;
        status.result_kind = Some(result.kind);
        status.result_expires_at = result.expires_at;
        status.command_tag = result.command_tag.clone();
        status.result = Some(Arc::new(result));
        drop(status);
        self.note_progress(rows, bytes);
    }

    pub fn set_failed(&self, sqlstate: &str, message: String) {
        let mut status = self.status.write();
        if status.state != JobState::Running {
            return;
        }
        status.state = JobState::Failed;
        status.finished_at = Some(now_ms());
        status.error_sqlstate = Some(sqlstate.to_string());
        status.error_message = Some(message);
    }

    pub fn set_canceled(&self, message: String) {
        let mut status = self.status.write();
        if status.state.is_terminal() {
            return;
        }
        status.state = JobState::Canceled;
        status.finished_at = Some(now_ms());
        status.error_sqlstate = Some("57014".to_string());
        status.error_message = Some(message);
    }

    pub fn set_lost(&self, message: String) {
        let mut status = self.status.write();
        if status.state.is_terminal() {
            return;
        }
        status.state = JobState::Lost;
        status.finished_at = Some(now_ms());
        status.error_message = Some(message);
    }

    pub fn expire_result(&self) {
        let mut status = self.status.write();
        if matches!(status.result_kind, Some(ResultKind::Spool)) {
            status.result_kind = Some(ResultKind::Expired);
            status.result = None;
            status.result_expires_at = None;
        }
    }

    pub fn snapshot(&self) -> JobSnapshot {
        let status = self.status.read();
        let expired = status
            .result_expires_at
            .is_some_and(|expires_at| now_ms() >= expires_at);
        JobSnapshot {
            id: self.id,
            job_key: self.job_key.clone(),
            user: self.user.clone(),
            database: self.database.clone(),
            sql: self.sql.clone(),
            state: status.state,
            attempt: self.attempt.load(Ordering::Relaxed),
            created_at: self.created_at,
            started_at: status.started_at,
            finished_at: status.finished_at,
            rows: self.rows.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            command_tag: status.command_tag.clone(),
            error_sqlstate: status.error_sqlstate.clone(),
            error_message: status.error_message.clone(),
            result_kind: if expired {
                Some(ResultKind::Expired)
            } else {
                status.result_kind
            },
            result_rows: if expired {
                None
            } else {
                status.result.as_ref().map(|result| result.rows)
            },
            result_bytes: if expired {
                None
            } else {
                status.result.as_ref().map(|result| result.bytes)
            },
            result_set_count: status
                .result
                .as_ref()
                .map(|result| result.sets.len())
                .unwrap_or(0),
            result_expires_at: status.result_expires_at,
        }
    }
}

/// An immutable view of one job for the status table functions.
#[derive(Debug, Clone)]
pub struct JobSnapshot {
    pub id: Uuid,
    pub job_key: Option<String>,
    pub user: String,
    pub database: String,
    pub sql: String,
    pub state: JobState,
    pub attempt: u32,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub rows: u64,
    pub bytes: u64,
    pub command_tag: Option<String>,
    pub error_sqlstate: Option<String>,
    pub error_message: Option<String>,
    pub result_kind: Option<ResultKind>,
    pub result_rows: Option<u64>,
    pub result_bytes: Option<u64>,
    pub result_set_count: usize,
    pub result_expires_at: Option<i64>,
}

/// Limits and retention for detached jobs.
#[derive(Debug, Clone)]
pub struct JobConfig {
    pub max_jobs: usize,
    pub result_max_bytes: u64,
    pub result_ttl_ms: i64,
    pub record_ttl_ms: i64,
    pub lease_ms: i64,
    pub heartbeat_ms: u64,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            max_jobs: 4,
            result_max_bytes: 1 << 30,
            result_ttl_ms: 24 * 60 * 60 * 1000,
            record_ttl_ms: 3 * 24 * 60 * 60 * 1000,
            lease_ms: 60_000,
            heartbeat_ms: 5_000,
        }
    }
}

/// A validation or metadata failure, carrying the PostgreSQL SQLSTATE the
/// control statement should report.
#[derive(Debug, Clone)]
pub struct JobError {
    pub sqlstate: String,
    pub message: String,
}

impl JobError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            sqlstate: "0A000".to_string(),
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            sqlstate: "XX000".to_string(),
            message: message.into(),
        }
    }
}

impl From<rootcause::Report> for JobError {
    fn from(value: rootcause::Report) -> Self {
        Self::internal(value.format_current_context_unhooked().to_string())
    }
}

/// Everything the submit path needs.
pub struct SubmitRequest {
    pub sql: String,
    pub job_key: Option<String>,
    pub timeout_ms: Option<i64>,
    pub retry_on_restart: bool,
    pub user: String,
    pub database: String,
    pub settings: JobSettings,
}

const MAX_SQL_BYTES: usize = 1024 * 1024;
const MAX_JOB_KEY_BYTES: usize = 256;
/// A detached job may run for a week at most; this keeps `Instant + timeout`
/// inside its representable range and catches typos in milliseconds.
const MAX_JOB_TIMEOUT_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// A detached job is retried from scratch at most this many times after a
/// server restart (when retry_on_restart is enabled).
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// The process-wide detached-query service.
pub struct JobService {
    meta: JobMetadata,
    warehouse: Option<String>,
    config: JobConfig,
    instance: String,
    records: RwLock<HashMap<Uuid, Arc<JobHandle>>>,
    queue_tx: mpsc::UnboundedSender<Uuid>,
    queue_rx: Mutex<Option<mpsc::UnboundedReceiver<Uuid>>>,
    shutting_down: AtomicBool,
}

impl JobService {
    pub async fn new(
        meta_client: MetaDataClientRef,
        warehouse: Option<String>,
        config: JobConfig,
        server_id: String,
    ) -> Result<Arc<Self>> {
        let pool = meta_client.pooled_client();
        let pool = pool.lock().await.clone();
        let meta = JobMetadata::from_pool(pool);
        meta.init_schema().await?;
        let (queue_tx, queue_rx) = mpsc::unbounded_channel();
        let records = RwLock::new(HashMap::new());
        let mut requeued = Vec::new();
        match meta.recoverable_jobs(&server_id).await {
            Ok(jobs) => {
                let mut handles = Vec::with_capacity(jobs.len());
                for job in jobs {
                    let query_id = job.query_id;
                    let canceled = job.cancel_requested;
                    let out_of_attempts =
                        job.state == "running" && job.attempt >= job.max_attempts.max(1);
                    if canceled || out_of_attempts {
                        let (message, sqlstate) = if canceled {
                            ("canceling statement due to user request", "57014")
                        } else {
                            ("query runner exhausted its restart attempts", "57014")
                        };
                        let _ = meta
                            .finish(
                                query_id,
                                if canceled {
                                    FinishState::Canceled
                                } else {
                                    FinishState::Lost
                                },
                                now_ms(),
                                0,
                                0,
                                None,
                                Some(sqlstate),
                                Some(message),
                                "none",
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                            )
                            .await;
                        let handle = recovered_handle(job);
                        if canceled {
                            handle.set_canceled(message.to_string());
                        } else {
                            handle.set_lost(message.to_string());
                        }
                        handles.push(Arc::new(handle));
                    } else {
                        if let Err(error) = meta.requeue_job(query_id, &server_id).await {
                            tracing::warn!(
                                query_id = %query_id,
                                %error,
                                "failed to requeue a query job after restart"
                            );
                            continue;
                        }
                        let handle = recovered_handle(job);
                        handles.push(Arc::new(handle));
                        requeued.push(query_id);
                    }
                }
                let mut map = records.write();
                for handle in handles {
                    map.insert(handle.id, Arc::clone(&handle));
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to recover unfinished query jobs");
            }
        }
        for query_id in requeued {
            let _ = queue_tx.send(query_id);
        }
        match meta.recoverable_spools(now_ms()).await {
            Ok(spools) => {
                let mut map = records.write();
                for spool in spools {
                    if let Some(handle) = restore_spool(spool) {
                        map.insert(handle.id, Arc::new(handle));
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to load retained query results");
            }
        }
        Ok(Arc::new(Self {
            meta,
            warehouse,
            config,
            instance: server_id,
            records,
            queue_tx,
            queue_rx: Mutex::new(Some(queue_rx)),
            shutting_down: AtomicBool::new(false),
        }))
    }

    pub fn config(&self) -> &JobConfig {
        &self.config
    }

    /// The durable side, used by the runner for leases and terminal states.
    pub fn metadata(&self) -> &JobMetadata {
        &self.meta
    }

    pub fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    pub fn instance(&self) -> &str {
        &self.instance
    }

    pub fn shutdown_requested(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire)
    }

    /// Validates a script without running it. A detached job accepts one or
    /// more `SELECT`, `INSERT ... SELECT` and `SET` statements; the SETs affect
    /// every statement after them in the same job session.
    pub fn classify(&self, sql: &str) -> Result<(), JobError> {
        if sql.len() > MAX_SQL_BYTES {
            return Err(JobError::invalid(format!(
                "detached query text exceeds the {} byte limit",
                MAX_SQL_BYTES
            )));
        }
        let statements = parse_script(sql)?;
        if statements.is_empty() {
            return Err(JobError::invalid(
                "a detached query must contain at least one statement".to_string(),
            ));
        }
        let mut has_select = false;
        for statement in &statements {
            match statement {
                datafusion::sql::sqlparser::ast::Statement::Query(_) => {
                    has_select = true;
                }
                datafusion::sql::sqlparser::ast::Statement::Insert(_) => {}
                datafusion::sql::sqlparser::ast::Statement::Set(set) => {
                    validate_set(set)?;
                }
                other => {
                    return Err(JobError::invalid(format!(
                        "only SELECT, INSERT and SET can run in a detached query, not {}",
                        statement_name(other)
                    )));
                }
            }
        }
        if has_select && self.warehouse.is_none() {
            return Err(JobError::invalid(
                "a detached SELECT needs a configured warehouse prefix for its result"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub async fn submit(&self, request: SubmitRequest) -> Result<Uuid, JobError> {
        if self.shutdown_requested() {
            return Err(JobError::internal("the server is shutting down"));
        }
        self.classify(&request.sql)?;
        if request.user.is_empty() {
            return Err(JobError::internal("a detached query needs a user"));
        }
        if let Some(key) = &request.job_key
            && key.len() > MAX_JOB_KEY_BYTES
        {
            return Err(JobError::invalid(format!(
                "job_key exceeds the {} byte limit",
                MAX_JOB_KEY_BYTES
            )));
        }
        let timeout_ms = match request.timeout_ms {
            Some(ms) if ms <= 0 => None,
            Some(ms) if ms > MAX_JOB_TIMEOUT_MS => {
                return Err(JobError::invalid(format!(
                    "timeout_ms must be at most {MAX_JOB_TIMEOUT_MS} (seven days)"
                )));
            }
            Some(ms) => Some(ms),
            None => None,
        };
        let query_id = Uuid::new_v4();
        let created_at = now_ms();
        let settings = serde_json::to_value(&request.settings).map_err(|error| {
            JobError::internal(format!("serialize job settings: {error}"))
        })?;
        let inserted = self
            .meta
            .insert_job(&NewJob {
                query_id,
                job_key: request.job_key.as_deref(),
                user_name: &request.user,
                database_name: &request.database,
                sql_text: &request.sql,
                settings: &settings,
                created_at,
                owner: self.instance(),
                timeout_ms,
                max_attempts: if request.retry_on_restart {
                    DEFAULT_MAX_ATTEMPTS as i32
                } else {
                    1
                },
                retry_on_restart: request.retry_on_restart,
            })
            .await
            .map_err(JobError::from)?;
        if inserted.is_none() {
            let key = request.job_key.as_deref().ok_or_else(|| {
                JobError::internal("job insert conflicted without a job_key")
            })?;
            let existing = self
                .meta
                .job_by_key(&request.database, &request.user, key)
                .await
                .map_err(JobError::from)?
                .ok_or_else(|| {
                    JobError::internal("job_key conflict without a job row")
                })?;
            // A key from a previous server run has a durable row but no
            // in-memory handle (or, for a completed SELECT, its result was
            // reloaded at startup). Returning an id nothing can serve would
            // leave the client polling a ghost; ask for a new key instead.
            if !self.records.read().contains_key(&existing) {
                return Err(JobError::invalid(format!(
                    "job_key {key:?} belongs to a job from a previous server run; \
                     choose a new job_key"
                )));
            }
            return Ok(existing);
        }
        let handle = Arc::new(JobHandle::new(
            query_id,
            request.job_key,
            request.user,
            request.database,
            request.sql,
            request.settings,
            created_at,
            timeout_ms,
            0,
        ));
        self.records.write().insert(query_id, Arc::clone(&handle));
        metrics::counter!("lakesoul_jobs_submitted_total").increment(1);
        if self.queue_tx.send(query_id).is_err() {
            handle.set_failed("XX000", "query job runner is not available".to_string());
            let _ = self
                .meta
                .finish(
                    query_id,
                    FinishState::Failed,
                    now_ms(),
                    0,
                    0,
                    None,
                    Some("XX000"),
                    Some("query job runner is not available"),
                    "none",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await;
            return Err(JobError::internal("query job runner is not available"));
        }
        Ok(query_id)
    }

    pub fn handle(&self, query_id: Uuid) -> Option<Arc<JobHandle>> {
        self.records.read().get(&query_id).cloned()
    }

    /// The queue the runner drains. Called once by [`JobRunner`].
    pub fn take_queue(&self) -> Option<mpsc::UnboundedReceiver<Uuid>> {
        self.queue_rx.lock().take()
    }

    pub fn snapshot(
        &self,
        query_id: Uuid,
        user: &str,
        database: &str,
    ) -> Option<JobSnapshot> {
        self.handle(query_id)
            .filter(|handle| handle.user == user && handle.database == database)
            .map(|handle| handle.snapshot())
    }

    pub fn snapshots(&self, user: &str, database: &str) -> Vec<JobSnapshot> {
        let mut snapshots: Vec<_> = self
            .records
            .read()
            .values()
            .filter(|handle| handle.user == user && handle.database == database)
            .map(|handle| handle.snapshot())
            .collect();
        snapshots.sort_by_key(|job| std::cmp::Reverse(job.created_at));
        snapshots
    }

    /// Cancels a job owned by `user`. Returns the outcome: a job that already
    /// finished is not an error, it just cannot be canceled.
    pub async fn cancel(
        &self,
        query_id: Uuid,
        user: &str,
        database: &str,
    ) -> Result<CancelOutcome, JobError> {
        let Some(handle) = self.handle(query_id) else {
            return Err(JobError::invalid(format!(
                "query {query_id} does not exist"
            )));
        };
        if handle.user != user || handle.database != database {
            return Err(JobError::invalid(format!(
                "query {query_id} does not exist"
            )));
        }
        let outcome = handle.request_cancel();
        match outcome {
            CancelOutcome::Finished => Ok(outcome),
            CancelOutcome::Queued => {
                let status = handle.status();
                let _ = self
                    .meta
                    .finish(
                        query_id,
                        FinishState::Canceled,
                        status.finished_at.unwrap_or_else(now_ms),
                        0,
                        0,
                        None,
                        Some("57014"),
                        Some("canceling statement due to user request"),
                        "none",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .await;
                Ok(outcome)
            }
            CancelOutcome::Running => {
                let _ = self.meta.request_cancel(query_id).await;
                Ok(outcome)
            }
        }
    }

    /// Waits up to `timeout_ms` for a terminal state. A timeout returns the
    /// state the job is in; it does not cancel the job.
    pub async fn wait(
        &self,
        query_id: Uuid,
        user: &str,
        database: &str,
        timeout_ms: i64,
    ) -> Result<JobState, JobError> {
        let handle = self
            .handle(query_id)
            .filter(|handle| handle.user == user && handle.database == database)
            .ok_or_else(|| {
                JobError::invalid(format!("query {query_id} does not exist"))
            })?;
        let deadline = now_ms().saturating_add(timeout_ms.max(0));
        loop {
            let state = handle.state();
            if state.is_terminal() || now_ms() >= deadline {
                return Ok(state);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The result a finished job exposes. Expired results report the same
    /// error as a missing one: the object is gone, not merely hidden.
    pub fn result(
        &self,
        query_id: Uuid,
        user: &str,
        database: &str,
    ) -> Result<Arc<JobResult>, JobError> {
        let handle = self
            .handle(query_id)
            .filter(|handle| handle.user == user && handle.database == database)
            .ok_or_else(|| {
                JobError::invalid(format!("query {query_id} does not exist"))
            })?;
        let status = handle.status();
        match status.state {
            JobState::Succeeded => {}
            state => {
                return Err(JobError::invalid(format!(
                    "query {query_id} is {}, not succeeded",
                    state.as_str()
                )));
            }
        }
        let Some(result) = status.result else {
            return Err(JobError::invalid(format!(
                "query {query_id} has no result (it may have expired)"
            )));
        };
        if let Some(expires_at) = result.expires_at
            && now_ms() >= expires_at
        {
            return Err(JobError::invalid(format!(
                "the result of query {query_id} has expired"
            )));
        }
        Ok(result)
    }

    /// Records a successful job and publishes its result.
    pub async fn publish_success(
        &self,
        handle: &JobHandle,
        result: JobResult,
    ) -> Result<()> {
        let rows = result.rows as i64;
        let bytes = result.bytes as i64;
        let kind = result.kind.as_str();
        let expires_at = result.expires_at;
        let command_tag = result.command_tag.clone();
        let stored_sets: Vec<StoredResultSet> = result
            .sets
            .iter()
            .map(|set| StoredResultSet {
                set_index: set.set_index,
                statement_index: set.statement_index,
                location: set.file.url.clone(),
                schema: serde_json::to_value(set.schema.as_ref())
                    .unwrap_or(serde_json::Value::Null),
                rows: set.rows,
                bytes: set.bytes,
            })
            .collect();
        let sets_json = (!stored_sets.is_empty())
            .then(|| serde_json::to_value(&stored_sets).ok())
            .flatten();
        let schema = if result.kind == ResultKind::Spool {
            serde_json::to_value(result.schema.as_ref())
                .map_err(|error| {
                    tracing::warn!(%error, "failed to store a query result schema");
                    error
                })
                .ok()
        } else {
            None
        };
        let location = result.location.clone();
        handle.note_progress(rows as u64, bytes as u64);
        handle.set_succeeded(result);
        self.meta
            .finish(
                handle.id,
                FinishState::Succeeded,
                now_ms(),
                rows,
                bytes,
                command_tag.as_deref(),
                None,
                None,
                kind,
                Some(rows),
                Some(bytes),
                location.as_deref(),
                schema.as_ref(),
                sets_json.as_ref(),
                expires_at,
            )
            .await?;
        metrics::counter!("lakesoul_jobs_finished_total", "state" => "succeeded")
            .increment(1);
        Ok(())
    }

    /// Records a terminal failure. `state` distinguishes a user cancel from a
    /// statement error; both keep their PostgreSQL SQLSTATE.
    pub async fn publish_failure(
        &self,
        handle: &JobHandle,
        state: JobState,
        sqlstate: &str,
        message: String,
    ) -> Result<()> {
        match state {
            JobState::Canceled => handle.set_canceled(message.clone()),
            JobState::Lost => handle.set_lost(message.clone()),
            _ => handle.set_failed(sqlstate, message.clone()),
        }
        let finish = match state {
            JobState::Canceled => FinishState::Canceled,
            JobState::Lost => FinishState::Lost,
            _ => FinishState::Failed,
        };
        self.meta
            .finish(
                handle.id,
                finish,
                now_ms(),
                handle.rows.load(Ordering::Relaxed) as i64,
                handle.bytes.load(Ordering::Relaxed) as i64,
                None,
                Some(sqlstate),
                Some(&message),
                "none",
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        metrics::counter!(
            "lakesoul_jobs_finished_total",
            "state" => state.as_str()
        )
        .increment(1);
        Ok(())
    }

    /// The reaper's view: jobs whose runner stopped renewing its lease.
    pub async fn expire_stale(&self) -> Result<()> {
        let now = now_ms();
        let expired = self.meta.expired_running(now).await?;
        for query_id in expired {
            if self.meta.mark_lost(query_id, now).await.unwrap_or(false)
                && let Some(handle) = self.handle(query_id)
            {
                handle.set_lost("query runner stopped renewing its lease".to_string());
            }
        }
        Ok(())
    }

    /// Removes spooled results past their retention window and deletes old
    /// job history. The object store is supplied by the caller so this module
    /// does not need a session of its own.
    pub async fn expire_results(&self, store: Arc<dyn ObjectStore>) -> Result<()> {
        let now = now_ms();
        for (query_id, location) in self.meta.expired_results(now).await? {
            let path = match url::Url::parse(&location)
                .ok()
                .and_then(|url| ObjectPath::from_url_path(url.path()).ok())
            {
                Some(path) => path,
                None => {
                    tracing::warn!(
                        query_id = %query_id,
                        location = %location,
                        "cannot parse an expired query result location"
                    );
                    self.meta.mark_result_expired(query_id).await?;
                    if let Some(handle) = self.handle(query_id) {
                        handle.expire_result();
                    }
                    continue;
                }
            };
            let mut objects = store.list(Some(&path));
            let mut failed = false;
            while let Some(meta) = objects.next().await {
                match meta {
                    Ok(meta) => {
                        if let Err(error) = store.delete(&meta.location).await {
                            failed = true;
                            tracing::warn!(
                                %error,
                                query_id = %query_id,
                                "failed to remove an expired query result"
                            );
                        }
                    }
                    Err(error) => {
                        failed = true;
                        tracing::warn!(
                            %error,
                            query_id = %query_id,
                            "failed to list an expired query result"
                        );
                    }
                }
            }
            if !failed {
                self.meta.mark_result_expired(query_id).await?;
                if let Some(handle) = self.handle(query_id) {
                    handle.expire_result();
                }
            }
        }
        let record_before = now.saturating_sub(self.config.record_ttl_ms);
        self.meta.delete_old(record_before).await?;
        let live: Vec<Uuid> = self
            .records
            .read()
            .values()
            .filter(|handle| {
                handle
                    .status()
                    .finished_at
                    .is_some_and(|at| at < record_before)
            })
            .map(|handle| handle.id)
            .collect();
        if !live.is_empty() {
            let mut records = self.records.write();
            for query_id in live {
                records.remove(&query_id);
            }
        }
        Ok(())
    }

    /// Stops every job this process owns. The runner tasks mark their jobs
    /// canceled as they observe the tokens; queued jobs are marked here so a
    /// graceful shutdown does not leave rows that look alive.
    pub async fn shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        let handles: Vec<Arc<JobHandle>> =
            self.records.read().values().cloned().collect();
        for handle in handles {
            if handle.state().is_terminal() {
                continue;
            }
            let outcome = handle.request_cancel();
            if outcome == CancelOutcome::Queued {
                let status = handle.status();
                let _ = self
                    .meta
                    .finish(
                        handle.id,
                        FinishState::Canceled,
                        status.finished_at.unwrap_or_else(now_ms),
                        0,
                        0,
                        None,
                        Some("57014"),
                        Some("canceling statement due to server shutdown"),
                        "none",
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .await;
            } else {
                let _ = self.meta.request_cancel(handle.id).await;
            }
        }
    }
}

/// Parses a job script into statements. Submit-time validation and execution
/// use the same parser, so a job cannot pass validation and then fail to parse.
pub(crate) fn parse_script(
    sql: &str,
) -> Result<Vec<datafusion::sql::sqlparser::ast::Statement>, JobError> {
    datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser::new()
        .parse(sql)
        .map_err(|error| JobError::invalid(format!("could not parse query: {error}")))
}

fn set_names_identity<'a>(
    variables: impl IntoIterator<Item = &'a datafusion::sql::sqlparser::ast::ObjectName>,
) -> bool {
    variables.into_iter().any(|variable| {
        variable
            .0
            .first()
            .and_then(|part| part.as_ident())
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case("lakesoul_pg_identity"))
    })
}

/// Rejects the `SET` forms a detached script cannot honour.
pub(crate) fn validate_set(
    set: &datafusion::sql::sqlparser::ast::Set,
) -> Result<(), JobError> {
    use datafusion::sql::sqlparser::ast::{ContextModifier, Set};
    match set {
        Set::SetTimeZone { .. } => Ok(()),
        Set::SingleAssignment {
            scope, variable, ..
        } => {
            if matches!(
                scope,
                Some(ContextModifier::Local) | Some(ContextModifier::Global)
            ) {
                return Err(JobError::invalid(
                    "SET LOCAL and SET GLOBAL are not supported in a detached query"
                        .to_string(),
                ));
            }
            if set_names_identity(std::iter::once(variable)) {
                return Err(JobError::invalid(
                    "permission denied: the query job identity is read-only".to_string(),
                ));
            }
            Ok(())
        }
        Set::ParenthesizedAssignments { variables, .. } => {
            if set_names_identity(variables) {
                return Err(JobError::invalid(
                    "permission denied: the query job identity is read-only".to_string(),
                ));
            }
            Ok(())
        }
        Set::MultipleAssignments { assignments } => {
            if assignments
                .iter()
                .any(|assignment| set_names_identity(std::iter::once(&assignment.name)))
            {
                return Err(JobError::invalid(
                    "permission denied: the query job identity is read-only".to_string(),
                ));
            }
            Ok(())
        }
        Set::SetRole { .. }
        | Set::SetSessionAuthorization(_)
        | Set::SetSessionParam(_) => Err(JobError::invalid(
            "SET ROLE, SET SESSION AUTHORIZATION and SET SESSION are not supported \
             in a detached query"
                .to_string(),
        )),
        _ => Err(JobError::invalid(
            "this SET form is not supported in a detached query".to_string(),
        )),
    }
}

fn statement_name(
    statement: &datafusion::sql::sqlparser::ast::Statement,
) -> &'static str {
    use datafusion::sql::sqlparser::ast::Statement;
    match statement {
        Statement::Query(_) => "SELECT",
        Statement::Insert(_) => "INSERT",
        Statement::Update(_) => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::Set(_) => "SET",
        _ => "this statement",
    }
}

/// Rebuilds an unfinished job's in-memory handle from its durable row. The
/// caller decides whether it is requeued, canceled or lost.
fn recovered_handle(job: metadata::RecoveredJob) -> JobHandle {
    let settings: JobSettings = serde_json::from_value(job.settings).unwrap_or_default();
    JobHandle::new(
        job.query_id,
        job.job_key,
        job.user_name,
        job.database_name,
        job.sql_text,
        settings,
        job.created_at,
        job.timeout_ms,
        job.attempt.max(0) as u32,
    )
}

/// Rebuilds one recovered SELECT result. A row whose schema or settings no
/// longer parse is skipped rather than failing server startup.
fn restore_spool(spool: metadata::RecoveredSpool) -> Option<JobHandle> {
    let settings: JobSettings =
        serde_json::from_value(spool.settings).unwrap_or_default();
    let stored: Vec<StoredResultSet> = spool
        .result_sets
        .as_ref()
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    let mut sets = Vec::with_capacity(stored.len());
    for set in stored {
        let schema: arrow_schema::Schema = serde_json::from_value(set.schema).ok()?;
        sets.push(JobResultSet {
            set_index: set.set_index,
            statement_index: set.statement_index,
            schema: Arc::new(schema),
            file: JobFile {
                url: set.location,
                size: set.bytes,
            },
            rows: set.rows,
            bytes: set.bytes,
        });
    }
    if sets.is_empty() {
        // Rows written before result_sets existed carry one schema/location.
        let schema: arrow_schema::Schema = serde_json::from_value(spool.schema?).ok()?;
        sets.push(JobResultSet {
            set_index: 0,
            statement_index: 0,
            schema: Arc::new(schema),
            file: JobFile {
                url: spool.location.clone(),
                size: spool.bytes.max(0) as u64,
            },
            rows: spool.rows.max(0) as u64,
            bytes: spool.bytes.max(0) as u64,
        });
    }
    sets.sort_by_key(|set| set.set_index);
    let set_rows: u64 = sets.iter().map(|set| set.rows).sum();
    let insert_rows = (spool.rows.max(0) as u64).saturating_sub(set_rows);
    let result = JobResult::script(
        sets,
        insert_rows,
        spool.command_tag,
        Some(spool.expires_at),
        Some(spool.location),
    );
    let handle = JobHandle::new(
        spool.query_id,
        spool.job_key,
        spool.user_name,
        spool.database_name,
        spool.sql_text,
        settings,
        spool.created_at,
        None,
        spool.attempt.max(0) as u32,
    );
    handle.restore_succeeded(spool.started_at, spool.finished_at, result);
    Some(handle)
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn describe_metrics_once() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        metrics::describe_counter!(
            "lakesoul_jobs_submitted_total",
            "Detached queries accepted by lakesoul_submit"
        );
        metrics::describe_counter!(
            "lakesoul_jobs_finished_total",
            "Detached queries that reached a terminal state, grouped by state"
        );
        metrics::describe_gauge!(
            "lakesoul_jobs_running",
            "Detached queries holding an execution slot"
        );
        metrics::describe_histogram!(
            "lakesoul_jobs_result_bytes",
            metrics::Unit::Bytes,
            "Encoded size of a detached SELECT result"
        );
    });
}

/// Exports the job metrics at zero so the first series is not missed by
/// `increase()`/`rate()`. Call once at startup.
pub fn register_metrics() {
    describe_metrics_once();
    metrics::counter!("lakesoul_jobs_submitted_total").increment(0);
    for state in ["succeeded", "failed", "canceled", "lost"] {
        metrics::counter!("lakesoul_jobs_finished_total", "state" => state).increment(0);
    }
    metrics::gauge!("lakesoul_jobs_running").set(0.0);
    let _ = metrics::histogram!("lakesoul_jobs_result_bytes");
}

#[cfg(test)]
mod script_tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::Statement;

    #[test]
    fn scripts_accept_set_select_and_insert() {
        let statements =
            parse_script("SET search_path = public; SELECT 1; INSERT INTO t SELECT 2")
                .expect("parse");
        assert_eq!(statements.len(), 3);
        assert!(matches!(statements[0], Statement::Set(_)));
        assert!(matches!(statements[1], Statement::Query(_)));
        assert!(matches!(statements[2], Statement::Insert(_)));
    }

    #[test]
    fn identity_and_local_settings_are_rejected() {
        let identity =
            parse_script("SET lakesoul_pg_identity.user = 'other'").expect("parse");
        let Statement::Set(set) = &identity[0] else {
            panic!("SET");
        };
        assert!(validate_set(set).is_err());

        let local = parse_script("SET LOCAL statement_timeout = '5s'").expect("parse");
        let Statement::Set(set) = &local[0] else {
            panic!("SET");
        };
        assert!(validate_set(set).is_err());
    }

    #[test]
    fn a_script_splits_its_result_sets() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![datafusion::arrow::datatypes::Field::new(
                "n",
                datafusion::arrow::datatypes::DataType::Int64,
                false,
            )]));
        let set = JobResultSet {
            set_index: 0,
            statement_index: 1,
            schema: Arc::clone(&schema),
            file: JobFile {
                url: "file:///tmp/result.parquet".to_string(),
                size: 10,
            },
            rows: 2,
            bytes: 10,
        };
        let result = JobResult::script(
            vec![set],
            3,
            Some("INSERT 0 3".to_string()),
            Some(123),
            Some("file:///tmp/attempt-1".to_string()),
        );
        assert_eq!(result.kind, ResultKind::Spool);
        assert_eq!(result.rows, 5);
        assert_eq!(result.bytes, 10);
        assert_eq!(result.sets.len(), 1);
        assert_eq!(result.schema, schema);
    }
}
