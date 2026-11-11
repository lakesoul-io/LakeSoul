// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The in-process runner for detached queries.
//!
//! One job is one spawned task. It owns a fresh [`PgSession`] built from the
//! settings captured at submit time, so nothing it does depends on the
//! connection that submitted it. The task renews a metadata lease while it
//! runs; a lease that expires without a heartbeat is marked `lost` by the
//! reaper instead of staying `running` forever.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow_schema::SchemaRef;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{Expr, Set, Statement, Value};
use datafusion_postgres::pgwire::error::PgWireError;
use futures::StreamExt;
use lakesoul_common::misc::TimeZone;
use object_store::ObjectStoreExt;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::parquet_to_arrow_schema;
use rootcause::Report;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::jobs::{
    JobFile, JobHandle, JobResult, JobResultSet, JobService, JobState, now_ms,
    validate_set,
};
use crate::session::{PgSession, PgSessionFactory, SessionIdentity};
use crate::session_settings::write_config;

/// The process-wide detached-query runner.
pub struct JobRunner {
    jobs: Arc<JobService>,
    factory: Arc<PgSessionFactory>,
    /// `None` means no server-wide concurrency limit.
    semaphore: Option<Arc<Semaphore>>,
}

struct ExecError {
    state: JobState,
    sqlstate: String,
    message: String,
}

impl ExecError {
    fn failed(sqlstate: &str, message: impl Into<String>) -> Self {
        Self {
            state: JobState::Failed,
            sqlstate: sqlstate.to_string(),
            message: message.into(),
        }
    }

    fn canceled() -> Self {
        Self {
            state: JobState::Canceled,
            sqlstate: "57014".to_string(),
            message: "canceling statement due to user request".to_string(),
        }
    }

    fn timeout() -> Self {
        Self {
            state: JobState::Failed,
            sqlstate: "57014".to_string(),
            message: "canceling statement due to statement timeout".to_string(),
        }
    }
}

/// Keeps the running gauge balanced even when a job returns early.
struct RunningGauge;

impl Drop for RunningGauge {
    fn drop(&mut self) {
        metrics::gauge!("lakesoul_jobs_running").decrement(1.0);
    }
}

impl JobRunner {
    pub fn new(jobs: Arc<JobService>, factory: Arc<PgSessionFactory>) -> Self {
        let permits = jobs.config().max_jobs;
        Self {
            jobs,
            factory,
            semaphore: (permits > 0).then(|| Arc::new(Semaphore::new(permits))),
        }
    }

    /// Starts the queue consumer and the lease/retention reaper.
    pub fn spawn(self: Arc<Self>) {
        let queue = Arc::clone(&self);
        tokio::spawn(async move { queue.run_loop().await });
        let reaper = Arc::clone(&self);
        tokio::spawn(async move { reaper.reaper_loop().await });
    }

    async fn run_loop(self: Arc<Self>) {
        let Some(mut queue) = self.jobs.take_queue() else {
            return;
        };
        while let Some(query_id) = queue.recv().await {
            if self.jobs.shutdown_requested() {
                break;
            }
            let permit = match &self.semaphore {
                Some(semaphore) => match Arc::clone(semaphore).acquire_owned().await {
                    Ok(permit) => Some(permit),
                    Err(_) => break,
                },
                None => None,
            };
            let runner = Arc::clone(&self);
            tokio::spawn(async move {
                runner.run_one(query_id).await;
                drop(permit);
            });
        }
    }

    async fn run_one(&self, query_id: Uuid) {
        let Some(handle) = self.jobs.handle(query_id) else {
            return;
        };
        if handle.state().is_terminal() {
            return;
        }
        if !handle.begin_running() {
            return;
        }
        metrics::gauge!("lakesoul_jobs_running").increment(1.0);
        let _running = RunningGauge;
        let lease = now_ms().saturating_add(self.jobs.config().lease_ms);
        let attempt = match self
            .jobs
            .metadata()
            .mark_running(query_id, self.jobs.instance(), lease, now_ms())
            .await
        {
            Ok(Some(attempt)) => attempt.max(1) as u32,
            Ok(None) => {
                handle.set_lost("the query was no longer queued".to_string());
                return;
            }
            Err(error) => {
                self.fail(
                    &handle,
                    ExecError::failed("XX000", format!("claim the query job: {error}")),
                )
                .await;
                return;
            }
        };
        handle.set_attempt(attempt);

        let statements = match crate::jobs::parse_script(&handle.sql) {
            Ok(statements) if !statements.is_empty() => statements,
            Ok(_) => {
                self.fail(
                    &handle,
                    ExecError::failed("0A000", "the query job has no statements"),
                )
                .await;
                return;
            }
            Err(error) => {
                self.fail(&handle, ExecError::failed("42601", error.message))
                    .await;
                return;
            }
        };

        let session = match self
            .factory
            .create_session(
                SessionIdentity {
                    user: handle.user.clone(),
                    database: handle.database.clone(),
                },
                &handle.settings.to_session_settings(),
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                self.fail(
                    &handle,
                    ExecError::failed(
                        "XX000",
                        format!("create the job session: {error}"),
                    ),
                )
                .await;
                return;
            }
        };
        if let Err(error) =
            crate::ivm::set_enabled(&session.context, handle.settings.ivm_enabled)
        {
            self.fail(
                &handle,
                ExecError::failed(
                    "XX000",
                    format!("restore lakesoul.ivm.enabled: {error}"),
                ),
            )
            .await;
            return;
        }

        // A retry starts a fresh attempt; its spool lives under attempt-<n>.
        self.cleanup_attempts(&session, &handle, attempt).await;
        let outcome = self
            .execute_with_heartbeat(&handle, &session, statements, attempt)
            .await;
        match outcome {
            Ok(result) => {
                if let Err(error) = self.jobs.publish_success(&handle, result).await {
                    warn!(query_id = %query_id, %error, "failed to record a succeeded query job");
                }
            }
            Err(error) => {
                if let Ok(prefix) = self.spool_prefix(&handle, attempt) {
                    self.cleanup_prefix(&session, &prefix).await;
                }
                self.fail(&handle, error).await;
            }
        }
    }

    async fn fail(&self, handle: &JobHandle, error: ExecError) {
        if let Err(error) = self
            .jobs
            .publish_failure(handle, error.state, &error.sqlstate, error.message)
            .await
        {
            warn!(
                query_id = %handle.id,
                %error,
                "failed to record a failed query job"
            );
        }
    }

    async fn execute_with_heartbeat(
        &self,
        handle: &JobHandle,
        session: &PgSession,
        statements: Vec<Statement>,
        attempt: u32,
    ) -> Result<JobResult, ExecError> {
        let deadline = handle
            .timeout_ms
            .map(|ms| Instant::now() + Duration::from_millis(ms.max(1) as u64));
        let token = handle.token.clone();
        let mut heartbeat = tokio::time::interval(Duration::from_millis(
            self.jobs.config().heartbeat_ms.max(1),
        ));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let execution = self.execute_script(handle, session, statements, attempt);
        tokio::pin!(execution);
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    return Err(ExecError::canceled());
                }
                _ = sleep_until(deadline) => {
                    return Err(ExecError::timeout());
                }
                _ = heartbeat.tick() => {
                    let lease = now_ms().saturating_add(self.jobs.config().lease_ms);
                    let rows = handle.rows.load(Ordering::Relaxed) as i64;
                    let bytes = handle.bytes.load(Ordering::Relaxed) as i64;
                    match self
                        .jobs
                        .metadata()
                        .heartbeat(handle.id, self.jobs.instance(), lease, rows, bytes)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => token.cancel(),
                        Err(error) => {
                            debug!(
                                query_id = %handle.id,
                                %error,
                                "query job heartbeat failed"
                            );
                        }
                    }
                }
                result = &mut execution => {
                    return result;
                }
            }
        }
    }

    /// Executes the script in order. `SET` updates the job session before the
    /// next statement; every SELECT becomes its own spooled result set.
    async fn execute_script(
        &self,
        handle: &JobHandle,
        session: &PgSession,
        statements: Vec<Statement>,
        attempt: u32,
    ) -> Result<JobResult, ExecError> {
        let cap = self.jobs.config().result_max_bytes;
        let mut sets: Vec<JobResultSet> = Vec::new();
        let mut insert_rows = 0u64;
        let mut command_tag = None;
        let mut progress_rows = 0u64;
        let mut progress_bytes = 0u64;
        for (statement_index, statement) in statements.into_iter().enumerate() {
            if handle.token.is_cancelled() {
                return Err(ExecError::canceled());
            }
            let statement_timeout = session.settings.read().statement_timeout;
            match statement {
                Statement::Set(set) => {
                    apply_set(session, &set)?;
                }
                Statement::Query(_) => {
                    let set_index = sets.len();
                    let remaining = cap.saturating_sub(progress_bytes);
                    let set = self
                        .run_with_timeout(
                            handle,
                            statement_timeout,
                            self.spool_statement(
                                handle,
                                &session.context,
                                statement,
                                attempt,
                                statement_index,
                                set_index,
                                remaining,
                            ),
                        )
                        .await?;
                    progress_rows = progress_rows.saturating_add(set.rows);
                    progress_bytes = progress_bytes.saturating_add(set.bytes);
                    handle.note_progress(progress_rows, progress_bytes);
                    sets.push(set);
                }
                Statement::Insert(insert) => {
                    let (rows, tag) = self
                        .run_with_timeout(handle, statement_timeout, async {
                            crate::insert::run_for_job(
                                &insert,
                                &session.context,
                                &handle.database,
                            )
                            .await
                            .map_err(pg_error)
                        })
                        .await?;
                    insert_rows = insert_rows.saturating_add(rows as u64);
                    command_tag = Some(tag);
                    progress_rows = progress_rows.saturating_add(rows as u64);
                    handle.note_progress(progress_rows, progress_bytes);
                }
                _ => {
                    return Err(ExecError::failed(
                        "0A000",
                        "only SELECT, INSERT and SET can run in a detached query",
                    ));
                }
            }
        }
        let location = if sets.is_empty() {
            None
        } else {
            Some(self.spool_prefix(handle, attempt)?)
        };
        let expires_at =
            sets.is_empty()
                .then_some(())
                .map(|_| None)
                .unwrap_or_else(|| {
                    Some(now_ms().saturating_add(self.jobs.config().result_ttl_ms))
                });
        handle.note_progress(progress_rows, progress_bytes);
        Ok(JobResult::script(
            sets,
            insert_rows,
            command_tag,
            expires_at,
            location,
        ))
    }

    async fn run_with_timeout<F, T>(
        &self,
        handle: &JobHandle,
        timeout: Option<Duration>,
        future: F,
    ) -> Result<T, ExecError>
    where
        F: std::future::Future<Output = Result<T, ExecError>>,
    {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let token = handle.token.clone();
        tokio::pin!(future);
        tokio::select! {
            _ = token.cancelled() => Err(ExecError::canceled()),
            _ = sleep_until(deadline) => Err(ExecError::failed(
                "57014",
                "canceling statement due to statement timeout",
            )),
            result = &mut future => result,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn spool_statement(
        &self,
        handle: &JobHandle,
        context: &SessionContext,
        statement: Statement,
        attempt: u32,
        statement_index: usize,
        set_index: usize,
        remaining_bytes: u64,
    ) -> Result<JobResultSet, ExecError> {
        let path = format!(
            "{}/stmt-{statement_index}/part-0.parquet",
            self.spool_prefix(handle, attempt)?
        );
        let state = context.state();
        let plan = state
            .statement_to_plan(DfStatement::Statement(Box::new(statement)))
            .await
            .map_err(|error| {
                ExecError::failed("XX000", format!("plan the detached query: {error}"))
            })?;
        let dataframe = context.execute_logical_plan(plan).await.map_err(|error| {
            ExecError::failed("XX000", format!("execute the detached query: {error}"))
        })?;
        let mut stream = dataframe.execute_stream().await.map_err(|error| {
            ExecError::failed("XX000", format!("stream the detached query: {error}"))
        })?;
        let stream_schema = stream.schema();
        let (store_url, object_path) = split_location(&path)?;
        let store = context
            .runtime_env()
            .object_store(&store_url)
            .map_err(|error| {
                ExecError::failed(
                    "XX000",
                    format!("open the query result object store: {error}"),
                )
            })?;
        let writer = BufWriter::new(Arc::clone(&store), object_path.clone());
        let mut writer =
            AsyncArrowWriter::try_new(writer, stream_schema, None).map_err(|error| {
                ExecError::failed(
                    "XX000",
                    format!("create the query result file: {error}"),
                )
            })?;
        let mut rows = 0u64;
        loop {
            if handle.token.is_cancelled() {
                return Err(ExecError::canceled());
            }
            let Some(batch) = stream.next().await else {
                break;
            };
            let batch = batch.map_err(|error| {
                ExecError::failed(
                    "XX000",
                    format!("read the detached query result: {error}"),
                )
            })?;
            writer.write(&batch).await.map_err(|error| {
                ExecError::failed(
                    "XX000",
                    format!("write the query result file: {error}"),
                )
            })?;
            rows += batch.num_rows() as u64;
            let written = (writer.bytes_written() + writer.in_progress_size()) as u64;
            if written > remaining_bytes {
                return Err(ExecError::failed(
                    "54000",
                    format!(
                        "detached result exceeds the configured maximum of {} bytes; \
                         write the rows into a table with INSERT ... SELECT instead",
                        self.jobs.config().result_max_bytes
                    ),
                ));
            }
        }
        let metadata = writer.close().await.map_err(|error| {
            ExecError::failed("XX000", format!("finish the query result file: {error}"))
        })?;
        let schema = parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )
        .map_err(|error| {
            ExecError::failed("XX000", format!("read the query result schema: {error}"))
        })?;
        let size = store
            .head(&object_path)
            .await
            .map_err(|error| {
                ExecError::failed("XX000", format!("stat the query result file: {error}"))
            })?
            .size;
        Ok(JobResultSet {
            set_index,
            statement_index,
            schema: Arc::new(schema),
            file: JobFile { url: path, size },
            rows,
            bytes: size,
        })
    }

    fn spool_prefix(
        &self,
        handle: &JobHandle,
        attempt: u32,
    ) -> Result<String, ExecError> {
        let warehouse = self.jobs.warehouse().ok_or_else(|| {
            ExecError::failed("0A000", "no warehouse prefix is configured for results")
        })?;
        Ok(format!(
            "{}/_lakesoul_jobs/{}/attempt-{attempt}",
            warehouse.trim_end_matches('/'),
            handle.id
        ))
    }

    async fn cleanup_attempts(
        &self,
        session: &PgSession,
        handle: &JobHandle,
        attempt: u32,
    ) {
        for previous in 1..attempt {
            if let Ok(prefix) = self.spool_prefix(handle, previous) {
                self.cleanup_prefix(session, &prefix).await;
            }
        }
    }

    async fn cleanup_prefix(&self, session: &PgSession, prefix: &str) {
        let Ok((store_url, path)) = split_location_raw(prefix) else {
            return;
        };
        let Ok(store) = session.context.runtime_env().object_store(&store_url) else {
            return;
        };
        let mut stream = store.list(Some(&path));
        while let Some(meta) = stream.next().await {
            let Ok(meta) = meta else {
                continue;
            };
            if let Err(error) = store.delete(&meta.location).await {
                debug!(%error, path = %meta.location, "failed to remove a query result");
            }
        }
    }

    async fn reaper_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut rounds = 0u32;
        loop {
            tick.tick().await;
            if self.jobs.shutdown_requested() {
                return;
            }
            if let Err(error) = self.jobs.expire_stale().await {
                warn!(%error, "failed to reap expired query jobs");
            }
            rounds = rounds.wrapping_add(1);
            if rounds.is_multiple_of(20) {
                self.collect_garbage().await;
            }
        }
    }

    async fn collect_garbage(&self) {
        let Some(warehouse) = self.jobs.warehouse() else {
            return;
        };
        let session = match self.factory.create_template_session() {
            Ok(session) => session,
            Err(error) => {
                warn!(%error, "failed to create a session for query result cleanup");
                return;
            }
        };
        let store_url = match ListingTableUrl::parse(warehouse) {
            Ok(url) => url.object_store(),
            Err(error) => {
                warn!(%error, "invalid warehouse prefix for query result cleanup");
                return;
            }
        };
        let store = match session.runtime_env().object_store(&store_url) {
            Ok(store) => store,
            Err(error) => {
                warn!(%error, "failed to open the query result object store");
                return;
            }
        };
        if let Err(error) = self.jobs.expire_results(store).await {
            warn!(%error, "failed to clean up expired query results");
        }
    }
}

/// Builds the result scan plan for one spool file.
pub(crate) fn spool_scan(
    schema: SchemaRef,
    path: &str,
    size: u64,
    offset: usize,
    limit: usize,
    projection: Option<&Vec<usize>>,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let url = url::Url::parse(path).map_err(|error| {
        datafusion::common::exec_datafusion_err!("invalid result url '{path}': {error}")
    })?;
    let store_url: ObjectStoreUrl = ListingTableUrl::parse(path)
        .map_err(|error| {
            datafusion::common::exec_datafusion_err!(
                "invalid result object store url '{path}': {error}"
            )
        })?
        .object_store();
    let object_path = ObjectPath::from_url_path(url.path()).map_err(|error| {
        datafusion::common::exec_datafusion_err!("invalid result path '{path}': {error}")
    })?;
    let source = ParquetSource::new(TableSchema::new(schema.clone(), Vec::new()));
    let file = PartitionedFile::new(object_path.to_string(), size);
    let config = FileScanConfigBuilder::new(store_url, Arc::new(source))
        .with_file_groups(vec![FileGroup::new(vec![file])])
        .with_projection_indices(projection.cloned())?
        .build();
    let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    Ok(Arc::new(SpoolScanExec::new(plan, offset, limit)))
}

/// Applies the result page's offset and limit inside the scan.
///
/// A plain `GlobalLimitExec` is not enough: when the caller sorts the page,
/// DataFusion's limit/sort pushdown rewrites it across the `SortExec` and can
/// turn the fetch into zero. An opaque scan keeps the page's boundaries fixed
/// while the outer query keeps its own ordering and limit.
#[derive(Debug)]
struct SpoolScanExec {
    input: Arc<dyn ExecutionPlan>,
    offset: usize,
    limit: usize,
    properties: Arc<PlanProperties>,
}

impl SpoolScanExec {
    fn new(input: Arc<dyn ExecutionPlan>, offset: usize, limit: usize) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            input,
            offset,
            limit,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for SpoolScanExec {
    fn fmt_as(
        &self,
        format: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SpoolScanExec: offset={}, limit={}",
                    self.offset, self.limit
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "offset={}, limit={}", self.offset, self.limit)
            }
        }
    }
}

impl ExecutionPlan for SpoolScanExec {
    fn name(&self) -> &'static str {
        "SpoolScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let Some(child) = children.pop() else {
            return datafusion::common::internal_err!("SpoolScanExec requires one child");
        };
        Ok(Arc::new(Self::new(child, self.offset, self.limit)))
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> datafusion::common::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::common::Result<datafusion::common::tree_node::TreeNodeRecursion>
    {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        Ok(Box::pin(SpoolStream {
            input,
            offset: self.offset,
            remaining: (self.limit > 0).then_some(self.limit),
            schema: self.input.schema(),
        }))
    }
}

struct SpoolStream {
    input: SendableRecordBatchStream,
    offset: usize,
    remaining: Option<usize>,
    schema: SchemaRef,
}

impl std::fmt::Debug for SpoolStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpoolStream")
            .field("offset", &self.offset)
            .field("remaining", &self.remaining)
            .finish()
    }
}

impl RecordBatchStream for SpoolStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl futures::Stream for SpoolStream {
    type Item = datafusion::common::Result<arrow_array::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use futures::StreamExt;
        if matches!(self.remaining, Some(0)) {
            return std::task::Poll::Ready(None);
        }
        loop {
            let next = match self.input.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(batch)) => batch,
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            };
            let batch = match next {
                Ok(batch) => batch,
                Err(error) => return std::task::Poll::Ready(Some(Err(error))),
            };
            let rows = batch.num_rows();
            if self.offset >= rows {
                self.offset -= rows;
                continue;
            }
            let start = self.offset;
            self.offset = 0;
            let mut batch = batch.slice(start, rows - start);
            if let Some(remaining) = &mut self.remaining {
                let take = (*remaining).min(batch.num_rows());
                batch = batch.slice(0, take);
                *remaining -= take;
            }
            if batch.num_rows() == 0 {
                if matches!(self.remaining, Some(0)) {
                    return std::task::Poll::Ready(None);
                }
                continue;
            }
            return std::task::Poll::Ready(Some(Ok(batch)));
        }
    }
}

fn pg_error(error: PgWireError) -> ExecError {
    match error {
        PgWireError::UserError(info) => ExecError::failed(&info.code, info.message),
        other => ExecError::failed("XX000", other.to_string()),
    }
}

fn split_location(path: &str) -> Result<(ObjectStoreUrl, ObjectPath), ExecError> {
    split_location_raw(path).map_err(|error| {
        ExecError::failed("XX000", format!("invalid query result location: {error}"))
    })
}

fn split_location_raw(path: &str) -> Result<(ObjectStoreUrl, ObjectPath), Report> {
    let url = url::Url::parse(path)?;
    let store_url = ListingTableUrl::parse(path)?.object_store();
    let object_path = ObjectPath::from_url_path(url.path())?;
    Ok((store_url, object_path))
}

/// Applies one `SET` to the job session. The next statement in the script sees
/// the new time zone, search path, statement timeout or extension option.
fn apply_set(session: &PgSession, set: &Set) -> Result<(), ExecError> {
    use datafusion::sql::sqlparser::ast::{ContextModifier, Set};
    validate_set(set)
        .map_err(|error| ExecError::failed(&error.sqlstate, error.message))?;
    match set {
        Set::SetTimeZone { value, .. } => {
            set_time_zone(session, value)?;
        }
        Set::SingleAssignment {
            scope,
            variable,
            values,
            ..
        } => {
            if matches!(
                scope,
                Some(ContextModifier::Local) | Some(ContextModifier::Global)
            ) {
                return Err(ExecError::failed(
                    "0A000",
                    "SET LOCAL and SET GLOBAL are not supported in a detached query",
                ));
            }
            apply_assignment(session, &variable.to_string(), values)?;
        }
        Set::ParenthesizedAssignments { variables, values } => {
            for (variable, value) in variables.iter().zip(values.iter()) {
                apply_assignment(
                    session,
                    &variable.to_string(),
                    std::slice::from_ref(value),
                )?;
            }
        }
        Set::MultipleAssignments { assignments } => {
            for assignment in assignments {
                apply_assignment(
                    session,
                    &assignment.name.to_string(),
                    std::slice::from_ref(&assignment.value),
                )?;
            }
        }
        _ => {
            return Err(ExecError::failed(
                "0A000",
                "this SET form is not supported in a detached query",
            ));
        }
    }
    Ok(())
}

fn set_time_zone(session: &PgSession, value: &Expr) -> Result<(), ExecError> {
    let raw = set_value(value)
        .ok_or_else(|| ExecError::failed("22023", "invalid value for TimeZone"))?;
    let zone = TimeZone::get(raw.trim()).map_err(|_| {
        ExecError::failed("22023", format!("invalid value for TimeZone: {raw:?}"))
    })?;
    let iana = zone
        .iana_name()
        .map(str::to_owned)
        .ok_or_else(|| ExecError::failed("22023", "the time zone has no IANA name"))?;
    session.settings.write().time_zone = zone;
    write_config(&session.context, |config| {
        config.options_mut().execution.time_zone = Some(iana);
        Ok(())
    })
    .map_err(|error| ExecError::failed("22023", format!("set TimeZone: {error}")))?;
    Ok(())
}

fn apply_assignment(
    session: &PgSession,
    key: &str,
    values: &[Expr],
) -> Result<(), ExecError> {
    let key = key.to_ascii_lowercase();
    if key.starts_with("lakesoul_pg_identity") {
        return Err(ExecError::failed(
            "42501",
            "permission denied: the query job identity is read-only",
        ));
    }
    let raw = values
        .iter()
        .filter_map(set_value)
        .collect::<Vec<_>>()
        .join(", ");
    match key.as_str() {
        "search_path" => {
            let paths: Vec<String> = values
                .iter()
                .filter_map(set_value)
                .flat_map(|value| {
                    value
                        .split(',')
                        .map(|part| part.trim().to_string())
                        .collect::<Vec<_>>()
                })
                .filter(|part| !part.is_empty())
                .collect();
            let Some(first) = paths.first().cloned() else {
                return Err(ExecError::failed("22023", "search_path cannot be empty"));
            };
            session.settings.write().search_path = paths;
            write_config(&session.context, move |config| {
                config.options_mut().catalog.default_schema = first;
                Ok(())
            })
            .map_err(|error| {
                ExecError::failed("22023", format!("set search_path: {error}"))
            })?;
        }
        "statement_timeout" => {
            let timeout = parse_timeout(&raw).ok_or_else(|| {
                ExecError::failed(
                    "22023",
                    format!("invalid value for statement_timeout: {raw:?}"),
                )
            })?;
            session.settings.write().statement_timeout = timeout;
        }
        "lakesoul.ivm.enabled" => {
            let enabled = parse_bool(&raw).ok_or_else(|| {
                ExecError::failed(
                    "22023",
                    format!("invalid value for lakesoul.ivm.enabled: {raw:?}"),
                )
            })?;
            crate::ivm::set_enabled(&session.context, enabled).map_err(|error| {
                ExecError::failed("22023", format!("set lakesoul.ivm.enabled: {error}"))
            })?;
        }
        "timezone" | "time zone" => {
            let value = values.first().ok_or_else(|| {
                ExecError::failed("22023", "invalid value for TimeZone")
            })?;
            set_time_zone(session, value)?;
        }
        // Connection-only settings have no meaning for a detached session.
        "application_name"
        | "client_encoding"
        | "datestyle"
        | "intervalstyle"
        | "standard_conforming_strings"
        | "extra_float_digits"
        | "bytea_output"
        | "client_min_messages" => {}
        _ => {
            write_config(&session.context, |config| {
                config.options_mut().set(&key, &raw)
            })
            .map_err(|error| {
                ExecError::failed(
                    "42704",
                    format!("unrecognized configuration parameter {key:?}: {error}"),
                )
            })?;
        }
    }
    Ok(())
}

fn set_value(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Value(value) => match &value.value {
            Value::Null => None,
            Value::Number(number, _) => Some(number.clone()),
            Value::Boolean(value) => Some(value.to_string()),
            other => other.clone().into_string(),
        },
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => Some(
            parts
                .iter()
                .map(|part| part.value.clone())
                .collect::<Vec<_>>()
                .join("."),
        ),
        Expr::Nested(expr) => set_value(expr),
        _ => None,
    }
}

fn parse_timeout(raw: &str) -> Option<Option<Duration>> {
    let raw = raw.trim().trim_matches('\'').trim_matches('"');
    if raw.is_empty() || raw.eq_ignore_ascii_case("default") || raw == "0" {
        return Some(None);
    }
    let split = raw
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(raw.len());
    let (number, unit) = raw.split_at(split);
    let number: f64 = number.parse().ok()?;
    if !number.is_finite() || number < 0.0 {
        return None;
    }
    let millis = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "ms" => number,
        "s" | "sec" | "secs" | "second" | "seconds" => number * 1000.0,
        "m" | "min" | "mins" | "minute" | "minutes" => number * 60_000.0,
        "h" | "hour" | "hours" => number * 3_600_000.0,
        "d" | "day" | "days" => number * 86_400_000.0,
        _ => return None,
    };
    if millis <= 0.0 {
        Some(None)
    } else {
        Some(Some(Duration::from_millis(millis as u64)))
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "on" | "true" | "1" | "yes" => Some(true),
        "off" | "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}
