// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cancellation and statement timeouts for one PostgreSQL connection.
//!
//! A PostgreSQL client cancels a statement either by sending a `CancelRequest`
//! on a second connection or by setting `statement_timeout`; a client that
//! simply stops reading should also stop the work it no longer consumes. The
//! upstream service only bounds plan construction with the timeout and, when a
//! cancel request arrives, aborts the `do_query` future it is awaiting. Neither
//! covers the dominant case here — a query that spends its time *producing
//! rows* — and neither reaches the distributed stages that a cancelled
//! statement leaves running.
//!
//! So the adapter owns cancellation end to end:
//!
//! - every statement gets a [`QueryToken`] and an absolute deadline from its
//!   connection's [`QueryCancellation`] *before* the upstream service is
//!   called, so planning and stage start-up are covered too
//!   ([`with_cancellation`]);
//! - the rows it returns are wrapped in [`CancellableRows`], which keeps
//!   checking the same token and the remaining time while they stream;
//! - a `CancelRequest` fires that token, so a query blocked anywhere
//!   (metadata, planning or a full result buffer) stops;
//! - the token also fires when the connection goes away, because the guard
//!   that removes it from the registry does so on drop — a disconnected client
//!   stops its work instead of leaving it running;
//! - on cancellation or deadline the wrapper yields SQLSTATE `57014` and drops
//!   the wrapped stream. Dropping a DataFusion stream stops its execution: the
//!   distributed coordinator treats the end of its stream as an abort signal,
//!   so worker tasks and their object-store reads stop as well.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Instant;

use async_trait::async_trait;
use datafusion_postgres::pgwire;
use futures::Stream;
use parking_lot::Mutex;
use pgwire::api::ClientInfo;
use pgwire::api::cancel::CancelHandler;
use pgwire::api::results::{QueryResponse, Response, SendableRowStream};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::data::DataRow;
use tokio::sync::watch;
use tracing::debug;

/// `57014 query_canceled`, the SQLSTATE PostgreSQL reports for both a
/// cancelled statement and an expired `statement_timeout`.
const QUERY_CANCELED: &str = "57014";

/// The client metadata key the upstream service reads `statement_timeout`
/// from (its `client.rs` keeps the constant private, so the name is repeated
/// here). The adapter writes it from `SET statement_timeout` and reads it back
/// to bound the whole statement.
pub const STATEMENT_TIMEOUT_KEY: &str = "statement_timeout_ms";

fn query_canceled(message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        QUERY_CANCELED.to_string(),
        message.to_string(),
    )))
}

pub fn canceled_error() -> PgWireError {
    query_canceled("canceling statement due to user request")
}

pub fn timeout_error() -> PgWireError {
    query_canceled("canceling statement due to statement timeout")
}

/// The cancellation of one statement.
#[derive(Debug, Clone)]
pub struct QueryToken {
    cancelled: watch::Sender<bool>,
}

impl QueryToken {
    fn new() -> Self {
        let (cancelled, _) = watch::channel(false);
        Self { cancelled }
    }

    /// Cancels the statement. Idempotent, and never fails: the sender keeps a
    /// receiver alive.
    pub fn cancel(&self) {
        self.cancelled.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    /// Resolves once the statement is cancelled, or immediately when it
    /// already was.
    pub async fn cancelled(&self) {
        let mut receiver = self.cancelled.subscribe();
        if *receiver.borrow() {
            return;
        }
        // The receiver is subscribed before the flag is read, so a
        // cancellation in between only makes `changed` resolve at once.
        let _ = receiver.changed().await;
    }
}

/// The execution of one statement: its cancellation token and the deadline it
/// must be done by.
///
/// Shared between the connection (which can address and cancel it) and the row
/// wrapper (which enforces it while the rows stream).
#[derive(Debug)]
pub struct ExecutionState {
    execution: Mutex<Execution>,
}

/// One execution of a statement.
#[derive(Debug)]
struct Execution {
    token: QueryToken,
    /// Bumped by every execution, so a row wrapper can tell that the statement
    /// started a new one and watch the new token.
    generation: u64,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
}

impl ExecutionState {
    fn new(timeout: Option<Duration>) -> Arc<Self> {
        let state = Arc::new(Self {
            execution: Mutex::new(Execution {
                token: QueryToken::new(),
                generation: 0,
                timeout,
                deadline: None,
            }),
        });
        state.restart(timeout);
        state
    }

    /// The token of the current execution.
    pub fn token(&self) -> QueryToken {
        self.execution.lock().token.clone()
    }

    /// The token to watch and the execution it belongs to.
    fn watched(&self) -> (QueryToken, u64) {
        let execution = self.execution.lock();
        (execution.token.clone(), execution.generation)
    }

    /// When this execution must be done, or `None` when the connection has no
    /// `statement_timeout`.
    pub fn deadline(&self) -> Option<Instant> {
        self.execution.lock().deadline
    }

    /// Starts a new execution: a fresh token - a cancel request that arrived
    /// while the statement was not running must not reach what runs next - the
    /// `statement_timeout` in force at this boundary, and a deadline from now.
    fn restart(&self, timeout: Option<Duration>) {
        let mut execution = self.execution.lock();
        execution.token = QueryToken::new();
        execution.generation = execution.generation.wrapping_add(1);
        execution.timeout = timeout;
        execution.deadline = timeout.map(|timeout| Instant::now() + timeout);
    }
}

/// The cancellation state of one connection.
#[derive(Debug, Default)]
pub struct QueryCancellation {
    /// The statement whose rows are being produced or sent right now, which is
    /// the one a `CancelRequest` refers to.
    current: Mutex<Option<Arc<ExecutionState>>>,
    /// The statement each extended-protocol portal is running, so an `Execute`
    /// that resumes a suspended portal can reach it again.
    portals: Mutex<HashMap<String, Arc<ExecutionState>>>,
}

impl QueryCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts a statement and makes it the one this connection is running.
    ///
    /// `portal` names the extended-protocol portal it belongs to, so later
    /// `Execute` messages can find it again.
    pub fn begin_statement(
        &self,
        timeout: Option<Duration>,
        portal: Option<&str>,
    ) -> Arc<ExecutionState> {
        let state = ExecutionState::new(timeout);
        if let Some(portal) = portal {
            self.portals
                .lock()
                .insert(portal.to_string(), Arc::clone(&state));
        }
        *self.current.lock() = Some(Arc::clone(&state));
        state
    }

    /// The `Execute` boundary of `portal`: a new execution of that statement,
    /// so its deadline starts now and it becomes the statement this connection
    /// is running - even if another one ran in between.
    ///
    /// The timeout is read at every execution, so a `SET statement_timeout`
    /// that happened while the portal was suspended applies to the fetch that
    /// resumes it. Called before the portal is fetched, so a cancel request
    /// that arrives during the fetch refers to it.
    pub fn begin_execution(
        &self,
        portal: &str,
        timeout: Option<Duration>,
    ) -> Arc<ExecutionState> {
        let mut portals = self.portals.lock();
        let state = portals
            .entry(portal.to_string())
            .or_insert_with(|| ExecutionState::new(None))
            .clone();
        drop(portals);
        state.restart(timeout);
        *self.current.lock() = Some(Arc::clone(&state));
        state
    }

    /// The statement a portal is currently executing, when one exists.
    ///
    /// `Execute` creates it before the portal is fetched; the handler that
    /// creates the plan reuses it instead of registering a second statement,
    /// which would leave the fetch watching one token while a cancel request
    /// reached another.
    pub fn execution_for(&self, portal: &str) -> Option<Arc<ExecutionState>> {
        self.portals.lock().get(portal).cloned()
    }

    /// How many portals this connection has statements for: tests assert that
    /// the count stays bounded by the portals that exist.
    #[cfg(test)]
    pub fn portal_count(&self) -> usize {
        self.portals.lock().len()
    }

    /// A statement execution is over: the connection has nothing in flight
    /// until the next statement starts, so a cancel request that arrives in
    /// between must not touch a statement that already finished.
    pub fn finish_execution(&self, state: &Arc<ExecutionState>) {
        let mut current = self.current.lock();
        if current.as_ref().is_some_and(|it| Arc::ptr_eq(it, state)) {
            *current = None;
        }
    }

    /// Forgets a portal pgwire has closed: its statement (and its token) must
    /// not be kept for the lifetime of the connection.
    pub fn forget_portal(&self, portal: &str) {
        let removed = self.portals.lock().remove(portal);
        if let Some(state) = removed {
            self.finish_execution(&state);
        }
    }

    /// Forgets every portal: SQL `CLOSE ALL` closes cursors and named portals
    /// alike, and each of them must stop keeping its statement.
    pub fn forget_all_portals(&self) {
        let removed: Vec<Arc<ExecutionState>> =
            self.portals.lock().drain().map(|(_, s)| s).collect();
        for state in removed {
            self.finish_execution(&state);
        }
    }

    /// Marks the statement this connection is running as cancelled. Returns
    /// whether there is one.
    pub fn cancel(&self) -> bool {
        match self.current.lock().as_ref() {
            Some(state) => {
                state.token().cancel();
                true
            }
            None => false,
        }
    }
}

/// How PostgreSQL addresses one backend: its process id and the secret key
/// handed to the client at startup.
type ConnectionKey = (i32, Vec<u8>);

/// The connections a `CancelRequest` may target.
#[derive(Debug, Default)]
pub struct CancelRegistry {
    connections: Mutex<HashMap<ConnectionKey, Arc<QueryCancellation>>>,
}

impl CancelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publishes a connection until the returned guard is dropped.
    pub fn register(
        self: &Arc<Self>,
        pid: i32,
        secret_key: &[u8],
        cancellation: Arc<QueryCancellation>,
    ) -> CancelRegistration {
        self.connections
            .lock()
            .insert((pid, secret_key.to_vec()), cancellation);
        CancelRegistration {
            registry: Arc::clone(self),
            key: (pid, secret_key.to_vec()),
        }
    }

    /// Delivers a cancel request to the addressed connection.
    ///
    /// Returns whether the connection exists; whether a statement was actually
    /// in flight is the connection's business (a cancel request that arrives
    /// between two statements is still a delivered request).
    pub fn cancel(&self, pid: i32, secret_key: &[u8]) -> bool {
        let cancellation = self
            .connections
            .lock()
            .get(&(pid, secret_key.to_vec()))
            .cloned();
        match cancellation {
            Some(cancellation) => {
                cancellation.cancel();
                true
            }
            None => false,
        }
    }
}

/// Removes a connection from the registry, cancelling its statement: a
/// connection that goes away must not leave its work running.
#[derive(Debug)]
pub struct CancelRegistration {
    registry: Arc<CancelRegistry>,
    key: ConnectionKey,
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        if let Some(cancellation) = self.registry.connections.lock().remove(&self.key) {
            cancellation.cancel();
        }
    }
}

/// Handles `CancelRequest` for every connection of this server.
///
/// The statement's token is what stops a query: every execution entry point
/// (planning, streaming rows, the socket writes) runs under
/// `with_cancellation`, which drops the underlying future once the token
/// fires. There is no second mechanism on purpose: pgwire's connection
/// manager cancels whatever statement the connection runs *when the request
/// is processed*, which — the token having already finished the current
/// statement — can be a newer pipelined one, so one request would cancel two
/// statements.
#[derive(Debug)]
pub struct LakeSoulCancelHandler {
    registry: Arc<CancelRegistry>,
}

impl LakeSoulCancelHandler {
    pub fn new(registry: Arc<CancelRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl CancelHandler for LakeSoulCancelHandler {
    async fn on_cancel_request(&self, cancel_request: CancelRequest) {
        let key = cancel_request.secret_key.to_bytes();
        let cancelled = self.registry.cancel(cancel_request.pid, &key);
        debug!(
            pid = cancel_request.pid,
            statement_in_flight = cancelled,
            "received a cancel request"
        );
    }
}

/// Bounds one statement's rows by a cancellation token and, optionally, a
/// deadline.
///
/// Yields `57014` once and then ends, dropping the wrapped stream — which is
/// what stops the underlying execution, including the distributed stages and
/// their object-store reads.
pub struct CancellableRows {
    rows: SendableRowStream,
    state: Arc<ExecutionState>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    /// The deadline the sleep below was built for, so a deadline that a new
    /// `Execute` restarted replaces the sleep.
    sleeping_until: Option<Instant>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    /// The execution the cancellation future below watches.
    watched_generation: u64,
    finished: bool,
}

impl CancellableRows {
    fn new(rows: SendableRowStream, state: Arc<ExecutionState>) -> Self {
        let (token, watched_generation) = state.watched();
        let cancelled = Box::pin(async move { token.cancelled().await });
        Self {
            rows,
            state,
            cancelled,
            sleeping_until: None,
            sleep: None,
            watched_generation,
            finished: false,
        }
    }

    /// The deadline this stream is currently sleeping until, rebuilt when the
    /// statement's deadline changed (a resumed portal starts a new one).
    /// Ends the stream, dropping what it wraps.
    ///
    /// Dropping is what stops the execution behind it, so it must happen even
    /// when the error is only reported to the client: a portal can keep
    /// holding this wrapper after the fetch returned.
    fn finish(&mut self) {
        self.finished = true;
        self.rows = Box::pin(futures::stream::empty());
    }

    fn sleep_until(
        &mut self,
        deadline: Option<Instant>,
    ) -> Option<Pin<&mut tokio::time::Sleep>> {
        if self.sleeping_until != deadline {
            self.sleeping_until = deadline;
            self.sleep =
                deadline.map(|deadline| Box::pin(tokio::time::sleep_until(deadline)));
        }
        self.sleep.as_mut().map(|sleep| sleep.as_mut())
    }
}

impl Stream for CancellableRows {
    type Item = PgWireResult<DataRow>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        // A new execution of the same statement (a resumed portal) has a new
        // token to watch and a new deadline.
        let (token, generation) = this.state.watched();
        if generation != this.watched_generation {
            this.watched_generation = generation;
            let watched = token.clone();
            this.cancelled = Box::pin(async move { watched.cancelled().await });
            // Both, not just the marker: a sleep from the previous execution
            // has already expired and would report its deadline immediately.
            this.sleeping_until = None;
            this.sleep = None;
        }
        if token.is_cancelled() || this.cancelled.as_mut().poll(cx).is_ready() {
            this.finish();
            return Poll::Ready(Some(Err(canceled_error())));
        }
        // The deadline is read from the statement, never inferred from how
        // often this stream happens to be polled: an `Execute` that resumes a
        // portal restarts it explicitly.
        let deadline = this.state.deadline();
        if let Some(mut sleep) = this.sleep_until(deadline)
            && sleep.as_mut().poll(cx).is_ready()
        {
            this.finish();
            return Poll::Ready(Some(Err(timeout_error())));
        }
        match this.rows.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(row))) => Poll::Ready(Some(Ok(row))),
            Poll::Ready(Some(Err(error))) => {
                this.finish();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Applies `token` and `timeout` to the rows of a response.
///
/// Only queries stream rows; every other response is returned unchanged.
pub fn guard_response(response: Response, state: &Arc<ExecutionState>) -> Response {
    match response {
        Response::Query(query) => {
            // `QueryResponse` is non-exhaustive, so its fields are moved out
            // and back rather than rebuilt in a struct literal.
            let QueryResponse {
                command_tag,
                row_schema,
                data_rows,
                ..
            } = query;
            let mut guarded = QueryResponse::new(
                row_schema,
                Box::pin(CancellableRows::new(data_rows, Arc::clone(state))),
            );
            guarded.command_tag = command_tag;
            Response::Query(guarded)
        }
        other => other,
    }
}

/// Starts a statement for `client`: the deadline comes from the connection's
/// `statement_timeout`, which the upstream `SET statement_timeout` hook records
/// in the client metadata.
///
/// Called *before* the upstream service runs the statement, so planning and
/// stage start-up are covered by it and count against the deadline.
pub fn begin_statement<C>(
    client: &C,
    cancellation: &QueryCancellation,
    portal: Option<&str>,
) -> Arc<ExecutionState>
where
    C: ClientInfo + ?Sized,
{
    cancellation.begin_statement(statement_timeout(client), portal)
}

/// Runs `future` under the statement's cancellation and deadline.
///
/// Abandoning the future is what stops work that has not produced rows yet -
/// planning, metadata reads, stage start-up - which the row wrapper cannot
/// reach.
pub async fn with_cancellation<T, F>(
    state: &Arc<ExecutionState>,
    future: F,
) -> PgWireResult<T>
where
    F: Future<Output = T>,
{
    let token = state.token();
    // A limit that fired while the statement was being parsed or planned must
    // win over an immediately-completing future: a statement that finishes on
    // its first poll would otherwise still run its side effects (a `SET`, a
    // portal close) after the cancel request or timeout had already arrived.
    // The checks here cover a limit that fired before the select even starts;
    // `biased` below covers the window between these checks and the first
    // poll, where `cancelled` is polled before the future can run.
    if token.is_cancelled() {
        return Err(canceled_error());
    }
    if let Some(deadline) = state.deadline() {
        if Instant::now() >= deadline {
            return Err(timeout_error());
        }
    }
    let cancelled = token.cancelled();
    tokio::pin!(cancelled);
    match state.deadline() {
        Some(deadline) => {
            tokio::select! {
                biased;
                _ = &mut cancelled => Err(canceled_error()),
                _ = tokio::time::sleep_until(deadline) => Err(timeout_error()),
                output = future => Ok(output),
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = &mut cancelled => Err(canceled_error()),
                output = future => Ok(output),
            }
        }
    }
}

/// Reads the connection's statement timeout, as set through
/// `SET statement_timeout`.
pub fn statement_timeout<C>(client: &C) -> Option<Duration>
where
    C: ClientInfo + ?Sized,
{
    client
        .metadata()
        .get(STATEMENT_TIMEOUT_KEY)
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use pgwire::api::results::{FieldInfo, QueryResponse};
    use std::task::Poll;

    fn rows(count: usize) -> SendableRowStream {
        Box::pin(futures::stream::iter(
            (0..count).map(|_| Ok(DataRow::default())),
        ))
    }

    async fn collect(stream: &mut SendableRowStream) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => out.push("ok".to_string()),
                Err(error) => {
                    out.push(match error {
                        PgWireError::UserError(info) => {
                            format!("err:{}:{}", info.code, info.message)
                        }
                        other => format!("other:{other}"),
                    });
                    break;
                }
            }
        }
        out
    }

    fn response(rows: SendableRowStream) -> Response {
        Response::Query(QueryResponse::new(Arc::new(Vec::<FieldInfo>::new()), rows))
    }

    /// Starts a statement the way the router does.
    fn statement(
        cancellation: &QueryCancellation,
        timeout: Option<Duration>,
    ) -> Arc<ExecutionState> {
        cancellation.begin_statement(timeout, None)
    }

    fn wrapped(
        rows: SendableRowStream,
        state: &Arc<ExecutionState>,
    ) -> SendableRowStream {
        match guard_response(response(rows), state) {
            Response::Query(query) => query.data_rows,
            _ => panic!("a query response stays a query response"),
        }
    }

    async fn poll_once(
        stream: &mut SendableRowStream,
    ) -> Poll<Option<PgWireResult<DataRow>>> {
        futures::future::poll_fn(|cx| Poll::Ready(stream.as_mut().poll_next(cx))).await
    }

    /// The `changed()` await in `cancelled()`: a receiver that subscribed and
    /// read the flag before the flip still resolves through `changed()`, so
    /// the subscribe-before-read window is safe when the cancellation lands
    /// after the first poll.
    #[test]
    fn cancelled_resolves_through_changed_when_the_flag_flips_later() {
        let token = QueryToken::new();
        let mut cancelled = Box::pin(token.cancelled());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());

        // Subscribed, flag read as false, waiting on `changed()`.
        assert!(matches!(cancelled.as_mut().poll(&mut cx), Poll::Pending));
        token.cancel();
        assert!(matches!(cancelled.as_mut().poll(&mut cx), Poll::Ready(())));
    }

    #[test]
    fn cancelled_resolves_immediately_for_an_already_cancelled_token() {
        let token = QueryToken::new();
        token.cancel();
        let mut cancelled = Box::pin(token.cancelled());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(matches!(cancelled.as_mut().poll(&mut cx), Poll::Ready(())));
    }

    #[tokio::test]
    async fn rows_pass_through_when_nothing_cancels() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, None);
        let mut stream = wrapped(rows(3), &state);
        assert_eq!(collect(&mut stream).await, vec!["ok", "ok", "ok"]);
    }

    /// A limit that fired before the statement runs must stop a future that
    /// completes on its first poll: select! polls its branches in random
    /// order, so without the pre-checks such a future could win the select
    /// and run its side effects (a `SET`, a portal close) after the limit
    /// had already arrived.
    #[tokio::test]
    async fn a_pre_fired_cancel_beats_an_instantly_completing_future() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, None);
        state.token().cancel();
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let result = with_cancellation(&state, async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
        assert!(
            matches!(&result, Err(PgWireError::UserError(info)) if info.code == "57014"),
            "the pre-fired cancel must stop the statement, got {result:?}"
        );
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "a statement cancelled before it runs must not run its side effect"
        );
    }

    #[tokio::test]
    async fn an_already_expired_deadline_beats_an_instantly_completing_future() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, Some(Duration::from_millis(0)));
        // The deadline is now + 0ms: by the time the statement runs it is past.
        tokio::time::sleep(Duration::from_millis(2)).await;
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let result = with_cancellation(&state, async move {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
        assert!(
            matches!(&result, Err(PgWireError::UserError(info))
                if info.code == "57014" && info.message.contains("statement timeout")),
            "an expired deadline must stop the statement, got {result:?}"
        );
        assert!(
            !ran.load(std::sync::atomic::Ordering::SeqCst),
            "a statement past its deadline must not run its side effect"
        );
    }

    #[tokio::test]
    async fn an_already_cancelled_statement_yields_57014_before_any_row() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, None);
        state.token().cancel();
        let mut stream = wrapped(rows(3), &state);
        assert_eq!(
            collect(&mut stream).await,
            vec!["err:57014:canceling statement due to user request"]
        );
    }

    #[tokio::test]
    async fn cancelling_mid_stream_stops_the_rows() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, None);
        // One row, then a producer that never ends: only the token can stop
        // the stream, and the rows before it must still arrive.
        let rows =
            futures::stream::iter(vec![Ok::<DataRow, PgWireError>(DataRow::default())])
                .chain(futures::stream::pending());
        let mut stream = wrapped(Box::pin(rows), &state);

        assert!(stream.next().await.is_some_and(|item| item.is_ok()));
        state.token().cancel();
        assert_eq!(
            collect(&mut stream).await,
            vec!["err:57014:canceling statement due to user request"]
        );
    }

    /// An error from the wrapped rows is passed through verbatim, and the
    /// upstream is dropped at once: rows that were queued after the error
    /// must not appear, and no later poll resurrects the stream.
    #[tokio::test]
    async fn an_error_from_the_rows_ends_the_stream_and_drops_what_it_wraps() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, None);
        let rows = futures::stream::iter(vec![
            Err::<DataRow, PgWireError>(canceled_error()),
            Ok(DataRow::default()), // must never surface: finish() dropped the upstream
        ]);
        let mut stream = wrapped(Box::pin(rows), &state);
        assert_eq!(
            collect(&mut stream).await,
            vec!["err:57014:canceling statement due to user request"]
        );
        assert!(matches!(poll_once(&mut stream).await, Poll::Ready(None)));
    }

    #[tokio::test]
    async fn an_expired_statement_timeout_yields_57014() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, Some(Duration::from_millis(1)));
        let pending = futures::stream::pending::<PgWireResult<DataRow>>();
        let mut stream = wrapped(Box::pin(pending), &state);
        assert_eq!(
            collect(&mut stream).await,
            vec!["err:57014:canceling statement due to statement timeout"]
        );
    }

    /// The deadline belongs to the statement: polling the stream, however
    /// rarely, must not extend it.
    #[tokio::test]
    async fn a_pending_stream_is_not_kept_alive_by_polling() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, Some(Duration::from_millis(30)));
        let pending = futures::stream::pending::<PgWireResult<DataRow>>();
        let mut stream = wrapped(Box::pin(pending), &state);

        for _ in 0..8 {
            // Longer than any pause the stream could be mistaken for.
            tokio::time::sleep(Duration::from_millis(60)).await;
            if let Poll::Ready(Some(item)) = poll_once(&mut stream).await {
                match item {
                    Err(PgWireError::UserError(info)) => {
                        assert_eq!(info.code, "57014");
                        assert!(
                            info.message.contains("statement timeout"),
                            "{}",
                            info.message
                        );
                        return;
                    }
                    other => panic!("unexpected item: {other:?}"),
                }
            }
        }
        panic!("a pending statement must time out instead of being extended by polls");
    }

    /// An `Execute` that resumes a portal is a new statement execution: the
    /// deadline starts again and the statement becomes the connection's current
    /// one, so a cancel request reaches it even after another statement ran in
    /// between.
    #[tokio::test]
    async fn an_execute_restarts_the_deadline_and_makes_the_statement_current() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state =
            cancellation.begin_statement(Some(Duration::from_millis(40)), Some("c"));
        let pending = futures::stream::pending::<PgWireResult<DataRow>>();
        let mut stream = wrapped(Box::pin(pending), &state);
        assert!(matches!(poll_once(&mut stream).await, Poll::Pending));

        // The portal sits suspended past its deadline, and another statement
        // runs on the connection meanwhile.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let other = cancellation.begin_statement(None, None);
        assert!(cancellation.cancel(), "the last statement is cancellable");
        other.token().cancel();
        assert!(!state.token().is_cancelled());

        // A new Execute of the same portal: not a stale timeout, and the
        // statement is the connection's current one again.
        cancellation.begin_execution("c", None);
        assert!(matches!(poll_once(&mut stream).await, Poll::Pending));
        assert!(cancellation.cancel());
        assert!(state.token().is_cancelled());
        assert_eq!(
            collect(&mut stream).await,
            vec!["err:57014:canceling statement due to user request"]
        );
    }

    /// The plan builder reuses the statement the execution boundary registered
    /// for the portal (`execution_for`), whether that was `begin_statement`
    /// at bind time or `begin_execution` at execute time. Registering a second
    /// statement instead would leave the fetch watching one token while a
    /// cancel request reached another.
    #[test]
    fn execution_for_returns_the_statement_the_boundary_registered() {
        let cancellation = QueryCancellation::new();

        // Bound at bind time (the extended-protocol handler registers the
        // portal's statement before the planner runs).
        let parsed = cancellation.begin_statement(None, Some("p"));
        assert!(
            cancellation
                .execution_for("p")
                .is_some_and(|it| Arc::ptr_eq(&it, &parsed)),
            "the planner must reuse the statement begin_statement registered"
        );

        // Bound at execute time.
        let executed = cancellation.begin_execution("c", None);
        let reused = cancellation
            .execution_for("c")
            .expect("the execute registered the portal's statement");
        assert!(Arc::ptr_eq(&executed, &reused));
        // The cancel path fires the very token the fetch watches.
        assert!(cancellation.cancel());
        assert!(executed.token().is_cancelled());

        // A portal name with no statement resolves to none.
        assert!(cancellation.execution_for("other").is_none());
    }

    /// A cancel request that arrives while the statement is not running must
    /// not reach the execution that follows: an idle connection has no
    /// statement, and the next `Execute` is a new execution with a new token.
    #[test]
    fn a_cancel_between_two_executions_does_not_poison_the_portal() {
        let cancellation = QueryCancellation::new();
        let state = cancellation.begin_execution("c", Some(Duration::from_millis(50)));
        let first = state.token();
        assert!(cancellation.cancel(), "the execution is cancellable");
        assert!(first.is_cancelled());

        cancellation.finish_execution(&state);
        assert!(
            !cancellation.cancel(),
            "an idle connection runs no statement"
        );

        let next = cancellation.begin_execution("c", Some(Duration::from_millis(50)));
        assert!(Arc::ptr_eq(&state, &next), "the portal keeps its statement");
        assert!(
            !next.token().is_cancelled(),
            "the earlier cancel must not reach the next execution"
        );
    }

    /// Every `Execute` reads `statement_timeout` again, so a `SET` that
    /// happened while the portal was suspended applies to the fetch that
    /// resumes it.
    #[test]
    fn a_resumed_execution_reads_the_timeout_in_force() {
        let cancellation = QueryCancellation::new();
        let state = cancellation.begin_execution("c", None);
        assert_eq!(state.deadline(), None);

        cancellation.begin_execution("c", Some(Duration::from_millis(20)));
        assert!(
            state.deadline().is_some(),
            "the timeout set while the portal was suspended applies now"
        );

        cancellation.begin_execution("c", None);
        assert_eq!(state.deadline(), None, "and turning it off applies too");
    }

    /// A closed portal must not be kept for the lifetime of the connection.
    #[test]
    fn closing_a_portal_forgets_its_statement() {
        let cancellation = QueryCancellation::new();
        let state = cancellation.begin_execution("c", None);
        cancellation.forget_portal("c");
        assert!(!cancellation.cancel(), "the closed portal runs nothing");

        let next = cancellation.begin_execution("c", None);
        assert!(
            !Arc::ptr_eq(&state, &next),
            "a name reused after close gets a fresh statement"
        );
    }

    /// `CLOSE ALL` forgets every portal, and each of them stops keeping its
    /// statement: closing the same name again starts a fresh statement.
    #[test]
    fn forget_all_portals_frees_every_portal() {
        let cancellation = QueryCancellation::new();
        let a = cancellation.begin_execution("a", None);
        let b = cancellation.begin_execution("b", None);
        cancellation.forget_all_portals();
        assert_eq!(cancellation.portal_count(), 0);
        assert!(!cancellation.cancel(), "no portal keeps a statement");
        let next_a = cancellation.begin_execution("a", None);
        assert!(
            !Arc::ptr_eq(&a, &next_a),
            "a name closed by CLOSE ALL gets a fresh statement"
        );
        let next_b = cancellation.begin_execution("b", None);
        assert!(!Arc::ptr_eq(&b, &next_b));
    }

    #[tokio::test]
    async fn other_responses_are_not_wrapped() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, None);
        assert!(matches!(
            guard_response(Response::EmptyQuery, &state),
            Response::EmptyQuery
        ));
    }

    #[tokio::test]
    async fn a_statement_finishing_inside_its_deadline_is_not_interrupted() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, Some(Duration::from_secs(30)));
        let output = with_cancellation(&state, async { 7 })
            .await
            .expect("no cancel");
        assert_eq!(output, 7);
    }

    #[tokio::test]
    async fn cancelling_reaches_work_that_has_not_produced_rows_yet() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, None);
        let token = state.token().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            token.cancel();
        });
        let error = with_cancellation(&state, futures::future::pending::<i32>())
            .await
            .expect_err("a cancelled statement must not complete");
        assert!(matches!(
            error,
            PgWireError::UserError(info) if info.code == "57014"
                && info.message.contains("user request")
        ));
    }

    #[tokio::test]
    async fn the_deadline_covers_work_that_has_not_produced_rows_yet() {
        let cancellation = Arc::new(QueryCancellation::new());
        let state = statement(&cancellation, Some(Duration::from_millis(5)));
        let error = with_cancellation(&state, futures::future::pending::<i32>())
            .await
            .expect_err("a statement past its deadline must not complete");
        assert!(matches!(
            error,
            PgWireError::UserError(info) if info.code == "57014"
                && info.message.contains("statement timeout")
        ));
    }

    #[test]
    fn the_cancel_registry_addresses_a_connection_by_pid_and_secret() {
        let registry = Arc::new(CancelRegistry::new());
        let cancellation = Arc::new(QueryCancellation::new());
        let _registration = registry.register(7, b"secret", Arc::clone(&cancellation));

        assert!(!cancellation.cancel(), "no statement in flight yet");
        let state = cancellation.begin_statement(None, None);
        assert!(!state.token().is_cancelled());

        assert!(registry.cancel(7, b"secret"));
        assert!(state.token().is_cancelled());
        assert!(
            !registry.cancel(7, b"other-secret"),
            "a wrong secret key must not reach a connection"
        );
    }

    #[test]
    fn a_dropped_registration_cancels_the_statement() {
        let registry = Arc::new(CancelRegistry::new());
        let cancellation = Arc::new(QueryCancellation::new());
        let state = cancellation.begin_statement(None, None);
        let registration = registry.register(9, b"secret", Arc::clone(&cancellation));
        drop(registration);
        assert!(
            state.token().is_cancelled(),
            "a disconnected client must not leave a statement running"
        );
        assert!(!registry.cancel(9, b"secret"));
    }

    #[test]
    fn a_new_statement_replaces_the_cancellable_one() {
        let cancellation = QueryCancellation::new();
        let first = cancellation.begin_statement(None, None);
        let second = cancellation.begin_statement(None, None);
        assert!(cancellation.cancel());
        assert!(
            !first.token().is_cancelled(),
            "only the statement in flight is cancelled"
        );
        assert!(second.token().is_cancelled());
    }

    /// The deadline comes from the metadata key the upstream `SET
    /// statement_timeout` hook writes; reading a different key would silently
    /// disable timeouts.
    #[test]
    fn the_statement_timeout_is_read_from_the_upstream_metadata_key() {
        let mut client = datafusion_postgres::testing::MockClient::new();
        assert_eq!(statement_timeout(&client), None);
        client
            .metadata_mut()
            .insert(STATEMENT_TIMEOUT_KEY.to_string(), "1500".to_string());
        assert_eq!(
            statement_timeout(&client),
            Some(Duration::from_millis(1500))
        );
        client.metadata_mut().remove(STATEMENT_TIMEOUT_KEY);
        assert_eq!(statement_timeout(&client), None);
    }
}
