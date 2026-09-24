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
//!   so worker tasks and their object-store reads stop as well;
//! - a statement is reported when its rows have been *sent*, not when they were
//!   produced. Pgwire collects the rows of an extended fetch before it writes
//!   any of them, so a send that a slow client pushes into a cancel or a
//!   deadline is the ending the client sees — and the ending the report
//!   carries ([`ExecutionState::end_after_send`]).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
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
use pgwire::messages::Message as _;
use pgwire::messages::cancel::CancelRequest;
use pgwire::messages::data::DataRow;
use tokio::sync::watch;
use tracing::{debug, info};

use crate::limits::{ServerLimits, StatementOutcome, StatementPermit};

/// Numbers the statements of one process, so a log line can be tied to the
/// statement it describes.
static NEXT_STATEMENT_ID: AtomicU64 = AtomicU64::new(1);

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

/// One prepared statement: the identity every execution of it reports under.
///
/// A prepared statement can be executed many times - different parameters,
/// different portals - so this holds only what those executions share: the id
/// the logs use and the moment planning started. The slot, the counters and the
/// report belong to an execution, not to this.
#[derive(Debug)]
pub struct StatementIdentity {
    id: u64,
    user: String,
    started: Instant,
}

impl StatementIdentity {
    fn new(user: &str) -> Arc<Self> {
        Arc::new(Self {
            id: NEXT_STATEMENT_ID.fetch_add(1, Ordering::Relaxed),
            user: user.to_string(),
            started: Instant::now(),
        })
    }
}

/// The result a portal has produced so far, across every `Execute` that has
/// fetched it.
///
/// The result limits bound the whole result, not one fetch of it: a client that
/// resumes a suspended portal one row per `Execute` must not be able to grow
/// the result past `max_result_rows` or `max_result_bytes`. The counters live
/// on the statement, which outlives the per-execution report, because a new
/// `Execute` replaces that report.
#[derive(Debug, Default)]
struct ResultBudget {
    rows: AtomicU64,
    /// Encoded bytes of those rows.
    bytes: AtomicU64,
}

impl ResultBudget {
    /// Charges one row, refused - and rolled back - when the result would pass
    /// a limit.
    fn charge(
        &self,
        server: &Arc<ServerLimits>,
        bytes: usize,
    ) -> Result<(), PgWireError> {
        let rows = self.rows.fetch_add(1, Ordering::Relaxed) + 1;
        let total = self.bytes.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
        match server.check_result_rows(rows as usize, total as usize) {
            Ok(()) => Ok(()),
            // The row is refused, not sent, so it is not part of the result.
            Err(error) => {
                self.rows.fetch_sub(1, Ordering::Relaxed);
                self.bytes.fetch_sub(bytes as u64, Ordering::Relaxed);
                Err(error)
            }
        }
    }
}

/// One execution of a statement: what it used, and the single log line it
/// produces when it ends.
///
/// Every `Execute` of a portal starts an execution of its own and with it a
/// report of its own, so a finished report can never keep a later execution's
/// slot from being released and one execution's numbers cannot inflate
/// another's. The result limits are not here: they bound the portal's whole
/// result and live on the statement, which a new `Execute` does not replace.
#[derive(Debug)]
pub struct StatementReport {
    identity: Arc<StatementIdentity>,
    started: Instant,
    /// Rows handed to the sending layer for the client.
    delivered: AtomicU64,
    outcome: AtomicU8,
    /// Set by whichever path ends the report first.
    finished: AtomicBool,
}

impl StatementReport {
    fn new(identity: Arc<StatementIdentity>) -> Arc<Self> {
        Arc::new(Self {
            identity,
            started: Instant::now(),
            delivered: AtomicU64::new(0),
            outcome: AtomicU8::new(StatementOutcome::Completed as u8),
            finished: AtomicBool::new(false),
        })
    }

    /// Rows handed to the sending layer for the client.
    fn note_delivered(&self, rows: u64) {
        self.delivered.fetch_add(rows, Ordering::Relaxed);
    }

    fn note_outcome(&self, outcome: StatementOutcome) {
        self.outcome.store(outcome as u8, Ordering::Relaxed);
    }

    /// Whether the report has been written.
    #[cfg(test)]
    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }

    /// Rows the client received, as far as the sending layer counted them.
    #[cfg(test)]
    pub fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn outcome(&self) -> StatementOutcome {
        match self.outcome.load(Ordering::Relaxed) {
            1 => StatementOutcome::Canceled,
            2 => StatementOutcome::TimedOut,
            3 => StatementOutcome::ResultLimit,
            4 => StatementOutcome::Failed,
            5 => StatementOutcome::Abandoned,
            _ => StatementOutcome::Completed,
        }
    }

    /// Ends the execution: reports it, once.
    fn finish(&self, outcome: StatementOutcome) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        self.note_outcome(outcome);
        info!(
            query_id = self.identity.id,
            user = %self.identity.user,
            rows = self.delivered.load(Ordering::Relaxed),
            // This execution, and the same query since its planning started.
            elapsed_ms = self.started.elapsed().as_millis() as u64,
            since_parse_ms =
                self.started.duration_since(self.identity.started).as_millis() as u64,
            outcome = outcome.as_str(),
            "statement finished"
        );
    }
}

/// The lifetime of one portal (or of one simple-protocol statement): the slot
/// it holds and the execution running in it.
///
/// The slot lives here, not on the report: a portal that is suspended with rows
/// still to fetch keeps holding it, while each `Execute` that fetches more rows
/// reports separately.
#[derive(Debug)]
pub struct ExecutionState {
    server: Arc<ServerLimits>,
    identity: Arc<StatementIdentity>,
    /// Held from the first execution until the work ends: the result is
    /// exhausted, the portal is closed, or the connection goes away.
    permit: Mutex<Option<StatementPermit>>,
    /// The report of the execution running now, `None` while the statement is
    /// only being planned.
    report: Mutex<Option<Arc<StatementReport>>>,
    /// What this portal's whole result has produced, charged across every
    /// `Execute` that fetches it, so a resumed portal cannot start the result
    /// limits over.
    produced: ResultBudget,
    /// Whether this execution ever produced a row stream. A statement that
    /// answered without one (`SET`, a transaction command) is over as soon as
    /// its response was sent, and only the caller knows that.
    stream_started: AtomicBool,
    /// The outcome the execution that ran last ended its result on, while the
    /// rows of that result are on their way to the client. The report is
    /// written when the send is over ([`ExecutionState::end_after_send`]), and
    /// the send is the caller's (see [`CancellableRows::terminate`]), so the
    /// wrapper only records what the result itself ended on.
    result_outcome: Mutex<Option<StatementOutcome>>,
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
    fn new(
        server: &Arc<ServerLimits>,
        identity: Arc<StatementIdentity>,
        timeout: Option<Duration>,
    ) -> Arc<Self> {
        Arc::new(Self {
            server: Arc::clone(server),
            identity,
            permit: Mutex::new(None),
            report: Mutex::new(None),
            produced: ResultBudget::default(),
            stream_started: AtomicBool::new(false),
            result_outcome: Mutex::new(None),
            execution: Mutex::new(Execution {
                token: QueryToken::new(),
                generation: 0,
                timeout,
                deadline: None,
            }),
        })
    }

    /// The report of the execution running now, if any.
    #[cfg(test)]
    pub fn report(&self) -> Option<Arc<StatementReport>> {
        self.report.lock().clone()
    }

    /// The query id this statement reports under.
    #[cfg(test)]
    pub fn query_id(&self) -> u64 {
        self.identity.id
    }

    /// Notes that this execution produced rows: its end is its rows' end.
    pub fn note_stream_started(&self) {
        self.stream_started.store(true, Ordering::Relaxed);
    }

    /// Whether this execution produced a row stream at all.
    pub fn ran_a_stream(&self) -> bool {
        self.stream_started.load(Ordering::Relaxed)
    }

    /// The identity this statement reports under.
    pub fn identity(&self) -> Arc<StatementIdentity> {
        Arc::clone(&self.identity)
    }

    /// Whether the execution that ran last produced its whole result and the
    /// slot may be given back once its rows have been sent.
    pub fn result_ended(&self) -> bool {
        self.result_outcome.lock().is_some()
    }

    /// The outcome the execution that ran last ended its result on, once its
    /// result ended. The result's own ending, separate from the statement's:
    /// the statement ends - and reports - once its rows have been sent.
    pub fn result_outcome(&self) -> Option<StatementOutcome> {
        *self.result_outcome.lock()
    }

    /// Notes the outcome the execution's result ended on. The slot stays held
    /// and nothing is reported yet: the rows pgwire collected from the stream
    /// may still be on their way to the client, and a send that a cancel or a
    /// deadline cuts short is the ending the client sees.
    fn note_result_ended(&self, outcome: StatementOutcome) {
        *self.result_outcome.lock() = Some(outcome);
    }

    /// Takes the statement's slot, unless the portal already holds it.
    fn ensure_permit(&self) -> Result<(), PgWireError> {
        let mut permit = self.permit.lock();
        if permit.is_none() {
            *permit = Some(
                self.server
                    .admit_statement(&self.identity.user)
                    .map_err(|error| error.into_statement())?,
            );
        }
        Ok(())
    }

    /// Releases the statement's slot. Called when the work behind it is over,
    /// whatever ended it.
    pub fn release_permit(&self) {
        let _permit = self.permit.lock().take();
    }

    /// Charged for planning, without a report: a `Parse` that is never executed
    /// produces no rows and must not leave a log line behind, but it may be
    /// slow enough to be worth a slot - and to be worth cancelling.
    pub fn admit_for_planning(
        &self,
        timeout: Option<Duration>,
    ) -> Result<(), PgWireError> {
        self.ensure_permit()?;
        self.arm(timeout);
        Ok(())
    }

    /// Starts an execution: a fresh token - a cancel request that arrived while
    /// the statement was not running must not reach what runs next - the
    /// `statement_timeout` in force at this boundary, a deadline from now, and
    /// a report of its own. The slot is taken unless the portal already holds
    /// it, so a statement fetched over several `Execute`es keeps one slot.
    fn restart(&self, timeout: Option<Duration>) -> Result<(), PgWireError> {
        self.ensure_permit()?;
        // The previous execution of this portal is over (a new `Execute`
        // started); report it before replacing it.
        self.finish(StatementOutcome::Completed);
        *self.report.lock() = Some(StatementReport::new(Arc::clone(&self.identity)));
        self.stream_started.store(false, Ordering::Relaxed);
        self.arm(timeout);
        Ok(())
    }

    fn arm(&self, timeout: Option<Duration>) {
        let mut execution = self.execution.lock();
        execution.token = QueryToken::new();
        execution.generation = execution.generation.wrapping_add(1);
        execution.timeout = timeout;
        execution.deadline = timeout.map(|timeout| Instant::now() + timeout);
        // A new execution: whatever the last one produced, its result is over.
        *self.result_outcome.lock() = None;
    }

    /// A row handed to the sending layer, with its encoded size.
    ///
    /// Charged against the portal's whole result, not the execution: the
    /// counter outlives the report that a new `Execute` replaces.
    pub fn note_row(&self, bytes: usize) -> Result<(), PgWireError> {
        // Planning produces no rows, so only an execution with a report
        // charges the portal's budget.
        if self.report.lock().is_none() {
            return Ok(());
        }
        self.produced.charge(&self.server, bytes)
    }

    /// Rows handed to the sending layer for the client.
    pub fn note_delivered(&self, rows: u64) {
        if let Some(report) = self.report.lock().as_ref() {
            report.note_delivered(rows);
        }
    }

    /// Ends the current execution: writes its report, once. The slot stays held
    /// - the portal may still have rows to fetch.
    pub fn finish(&self, outcome: StatementOutcome) {
        if let Some(report) = self.report.lock().as_ref() {
            report.finish(outcome);
        }
    }

    /// Ends the current execution *and* the statement's hold on its slot: the
    /// work behind the portal is over (its rows ended, it was closed, or the
    /// connection went away).
    pub fn end(&self, outcome: StatementOutcome) {
        self.finish(outcome);
        self.release_permit();
    }

    /// Ends the statement once the rows of its result have been sent.
    /// `send_ending` is the ending the send itself had (completed, failed, or
    /// the interruption that cut it short).
    ///
    /// A result that ended on its own - the result limit, a failing row, a
    /// cancel the row wrapper observed - keeps that ending: it is what ended
    /// the work. Only `Completed` is provisional, because a result is complete
    /// once its rows reach the client, which is after this point.
    pub fn end_after_send(&self, send_ending: StatementOutcome) {
        let ending = match self.result_outcome() {
            Some(ended) if ended != StatementOutcome::Completed => ended,
            _ => send_ending,
        };
        self.end(ending);
    }

    /// Gives up an execution that never ran a statement: its slot returns and
    /// it writes no report, like a `Parse` that was never executed. Used when a
    /// batch is registered before its text is read and the text turns out to
    /// hold no statement at all.
    pub fn discard(&self) {
        *self.report.lock() = None;
        self.release_permit();
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
}

/// A statement nobody ended still reports and returns its slot: dropping the
/// state means its connection is gone (or the statement was replaced).
impl Drop for ExecutionState {
    fn drop(&mut self) {
        self.end(StatementOutcome::Abandoned);
    }
}

/// The cancellation state of one connection.
#[derive(Debug)]
pub struct QueryCancellation {
    /// The limits this connection's statements are charged against.
    server: Arc<ServerLimits>,
    user: String,
    /// The statement whose rows are being produced or sent right now, which is
    /// the one a `CancelRequest` refers to.
    current: Mutex<Option<Arc<ExecutionState>>>,
    /// The statement each extended-protocol portal is running, so an `Execute`
    /// that resumes a suspended portal can reach it again.
    portals: Mutex<HashMap<String, Arc<ExecutionState>>>,
    /// The identity behind each prepared statement name, so the `Execute`s that
    /// follow a `Parse` report under the same query id.
    statements: Mutex<HashMap<String, Arc<StatementIdentity>>>,
}

impl QueryCancellation {
    pub fn new(server: Arc<ServerLimits>, user: &str) -> Self {
        Self {
            server,
            user: user.to_string(),
            current: Mutex::new(None),
            portals: Mutex::new(HashMap::new()),
            statements: Mutex::new(HashMap::new()),
        }
    }

    /// Starts the planning of a prepared statement.
    ///
    /// Planning is charged against the connection's statement limit while it
    /// runs and released when it returns: a statement that is planned and never
    /// executed has no rows and must not keep a slot. Its identity is not
    /// registered here - [`commit_parse`](Self::commit_parse) does that once the
    /// parse succeeded - so a `Parse` that fails leaves no entry behind, and a
    /// name parsed again gets the new statement's identity rather than the
    /// replaced one's query id and parse time.
    pub fn begin_parse(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Arc<ExecutionState>, PgWireError> {
        let state = ExecutionState::new(
            &self.server,
            StatementIdentity::new(&self.user),
            timeout,
        );
        state.admit_for_planning(timeout)?;
        *self.current.lock() = Some(Arc::clone(&state));
        Ok(state)
    }

    /// Registers the identity of a parse that succeeded: the `Execute`s of
    /// `name` report under it. A name parsed again is a new statement, so its
    /// identity replaces the one of the statement it replaced.
    pub fn commit_parse(&self, name: &str, execution: &Arc<ExecutionState>) {
        self.statements
            .lock()
            .insert(name.to_string(), execution.identity());
    }

    /// Starts a statement that is not a prepared one (a SimpleQuery statement).
    pub fn begin_statement(
        &self,
        timeout: Option<Duration>,
        portal: Option<&str>,
    ) -> Result<Arc<ExecutionState>, PgWireError> {
        let state = ExecutionState::new(
            &self.server,
            StatementIdentity::new(&self.user),
            timeout,
        );
        state.restart(timeout)?;
        if let Some(portal) = portal {
            self.portals
                .lock()
                .insert(portal.to_string(), Arc::clone(&state));
        }
        *self.current.lock() = Some(Arc::clone(&state));
        Ok(state)
    }

    /// The `Execute` boundary of `portal`: a new execution of the statement
    /// that portal is bound to.
    ///
    /// The timeout is read at every execution, so a `SET statement_timeout`
    /// that happened while the portal was suspended applies to the fetch that
    /// resumes it. Re-executing an existing portal continues the statement and
    /// keeps its slot; a portal that is executed for the first time starts a
    /// statement of its own - the same prepared statement executed twice is two
    /// executions, with two reports and two slots.
    pub fn begin_execution(
        &self,
        portal: &str,
        statement: &str,
        timeout: Option<Duration>,
    ) -> Result<Arc<ExecutionState>, PgWireError> {
        // The lookup happens in its own statement: a `match` scrutinee's guard
        // lives for the whole match, and parking_lot mutexes are not reentrant,
        // so locking again in the `None` arm would deadlock.
        let existing = self.portals.lock().get(portal).cloned();
        let state = match existing {
            Some(state) => state,
            None => {
                let identity = {
                    let mut statements = self.statements.lock();
                    statements
                        .entry(statement.to_string())
                        .or_insert_with(|| StatementIdentity::new(&self.user))
                        .clone()
                };
                let state = ExecutionState::new(&self.server, identity, timeout);
                self.portals
                    .lock()
                    .insert(portal.to_string(), Arc::clone(&state));
                state
            }
        };
        state.restart(timeout)?;
        *self.current.lock() = Some(Arc::clone(&state));
        Ok(state)
    }

    /// The statement a portal is currently executing, when one exists.
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
    /// until the next statement starts. The statement itself may still be
    /// unfinished (a suspended portal), so it keeps its slot and its report.
    pub fn finish_execution(&self, state: &Arc<ExecutionState>) {
        let mut current = self.current.lock();
        if current.as_ref().is_some_and(|it| Arc::ptr_eq(it, state)) {
            *current = None;
        }
    }

    /// Forgets a portal pgwire has closed, ending the statement it holds.
    ///
    /// The prepared statement it was bound to keeps its identity: closing a
    /// portal does not close the statement, and the next `Execute` of that
    /// prepared statement is a new execution of it.
    pub fn forget_portal(&self, portal: &str) {
        if let Some(state) = self.portals.lock().remove(portal) {
            self.finish_execution(&state);
            state.end(StatementOutcome::Abandoned);
        }
    }

    /// Forgets a prepared statement pgwire has closed or replaced: nothing
    /// should report under its identity again.
    pub fn forget_statement(&self, statement: &str) {
        self.statements.lock().remove(statement);
    }

    /// Forgets every portal: SQL `CLOSE ALL` closes cursors and named portals
    /// alike, and each of them must stop keeping its statement.
    pub fn forget_all_portals(&self) {
        let removed: Vec<Arc<ExecutionState>> =
            self.portals.lock().drain().map(|(_, s)| s).collect();
        for state in removed {
            self.finish_execution(&state);
            state.end(StatementOutcome::Abandoned);
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

    /// Ends every statement this connection still holds: called when the
    /// connection goes away, so their slots are released and their reports are
    /// written.
    pub fn finish_all(&self) {
        self.statements.lock().clear();
        let portals: Vec<_> = self.portals.lock().drain().map(|(_, it)| it).collect();
        for state in portals {
            state.end(StatementOutcome::Abandoned);
        }
    }
}

/// A connection that goes away ends its statements: their slots must return
/// and their reports must still be written.
impl Drop for QueryCancellation {
    fn drop(&mut self) {
        self.finish_all();
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
        state.note_stream_started();
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

    /// Ends the result behind this stream: the outcome it ended on is recorded
    /// and the wrapped stream is dropped, so the execution behind it stops.
    ///
    /// The statement is not reported and its slot stays held. pgwire collects
    /// the rows of an extended fetch into a buffer before writing any of them
    /// to the client, so the work ending here is not the send ending: reporting
    /// now would leave a send that a slow client pushes into a cancel or a
    /// deadline reported as completed, and releasing the slot now would let a
    /// connection run more statements than its limit allows while it is still
    /// sending the rows it already collected. The caller ends the statement
    /// once the send is over, seeing [`ExecutionState::result_ended`] and
    /// [`ExecutionState::end_after_send`].
    fn terminate(&mut self, outcome: StatementOutcome) {
        self.state.note_result_ended(outcome);
        self.finish();
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
            this.terminate(StatementOutcome::Canceled);
            return Poll::Ready(Some(Err(canceled_error())));
        }
        // The deadline is read from the statement, never inferred from how
        // often this stream happens to be polled: an `Execute` that resumes a
        // portal restarts it explicitly.
        let deadline = this.state.deadline();
        if let Some(mut sleep) = this.sleep_until(deadline)
            && sleep.as_mut().poll(cx).is_ready()
        {
            this.terminate(StatementOutcome::TimedOut);
            return Poll::Ready(Some(Err(timeout_error())));
        }
        match this.rows.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(row))) => {
                // The row is charged its full wire size: pgwire sends a type
                // byte, a length and a field count around the values.
                match this.state.note_row(row.message_length() + 1) {
                    Ok(()) => {
                        // Counted as it is handed out: this is the last point
                        // at which the row can still be attributed to the
                        // execution that produced it. pgwire buffers the rows
                        // of an extended fetch and writes them afterwards, so
                        // a later send failure is not observable here - and
                        // deferring the count would credit the row to whatever
                        // execution comes next.
                        this.state.note_delivered(1);
                        Poll::Ready(Some(Ok(row)))
                    }
                    Err(error) => {
                        this.terminate(StatementOutcome::ResultLimit);
                        Poll::Ready(Some(Err(error)))
                    }
                }
            }
            Poll::Ready(Some(Err(error))) => {
                this.terminate(StatementOutcome::Failed);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.terminate(StatementOutcome::Completed);
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

/// Why an execution ended before its work did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interruption {
    Canceled,
    TimedOut,
}

impl Interruption {
    pub fn error(self) -> PgWireError {
        match self {
            Self::Canceled => canceled_error(),
            Self::TimedOut => timeout_error(),
        }
    }

    pub fn outcome(self) -> StatementOutcome {
        match self {
            Self::Canceled => StatementOutcome::Canceled,
            Self::TimedOut => StatementOutcome::TimedOut,
        }
    }
}

/// Runs `future` under the statement's cancellation and deadline.
///
/// Abandoning the future is what stops work that has not produced rows yet -
/// planning, metadata reads, stage start-up, a blocked socket write - which the
/// row wrapper cannot reach. The caller ends the statement on both paths, so
/// its slot and its report are never left behind.
pub async fn with_cancellation<T, F>(
    state: &Arc<ExecutionState>,
    future: F,
) -> Result<T, Interruption>
where
    F: Future<Output = T>,
{
    let token = state.token();
    // Checked before the work is polled: a token that is already cancelled, or
    // a deadline that has already passed, must win over a future that is ready
    // immediately. Otherwise a statement that finishes at once (a fast `SET`)
    // would take effect after the cancellation or timeout that already ended
    // it. The row wrapper this must stay consistent with checks the same two
    // conditions in the same order.
    if token.is_cancelled() {
        return Err(Interruption::Canceled);
    }
    let deadline = state.deadline();
    if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
        return Err(Interruption::TimedOut);
    }
    let cancelled = token.cancelled();
    tokio::pin!(cancelled);
    match deadline {
        Some(deadline) => {
            // `biased` with the interruption branches first: when the work is
            // ready in the same poll as a cancellation or the deadline, the
            // interruption is what the statement ended on.
            tokio::select! {
                biased;
                _ = &mut cancelled => Err(Interruption::Canceled),
                _ = tokio::time::sleep_until(deadline) => Err(Interruption::TimedOut),
                output = future => Ok(output),
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = &mut cancelled => Err(Interruption::Canceled),
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
    use crate::limits::Limits;
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

    /// A cancellation whose statement limits are the defaults.
    fn cancellation() -> Arc<QueryCancellation> {
        Arc::new(QueryCancellation::new(
            ServerLimits::new(Limits::default()),
            "user_a",
        ))
    }

    /// A cancellation with the given statement limits.
    fn cancellation_with(limits: Limits) -> (Arc<ServerLimits>, Arc<QueryCancellation>) {
        let server = ServerLimits::new(limits);
        let cancellation =
            Arc::new(QueryCancellation::new(Arc::clone(&server), "user_a"));
        (server, cancellation)
    }

    /// Starts a statement the way the router does.
    fn statement(
        cancellation: &QueryCancellation,
        timeout: Option<Duration>,
    ) -> Arc<ExecutionState> {
        cancellation
            .begin_statement(timeout, None)
            .expect("statement admitted")
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
        let cancellation = cancellation();
        let state = statement(&cancellation, None);
        let mut stream = wrapped(rows(3), &state);
        assert_eq!(collect(&mut stream).await, vec!["ok", "ok", "ok"]);
    }

    #[tokio::test]
    async fn an_already_cancelled_statement_yields_57014_before_any_row() {
        let cancellation = cancellation();
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
        let cancellation = cancellation();
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
        let cancellation = cancellation();
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
        let cancellation = cancellation();
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
        let cancellation = cancellation();
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
        let cancellation = cancellation();
        let state = cancellation
            .begin_statement(Some(Duration::from_millis(40)), Some("c"))
            .expect("statement admitted");
        let pending = futures::stream::pending::<PgWireResult<DataRow>>();
        let mut stream = wrapped(Box::pin(pending), &state);
        assert!(matches!(poll_once(&mut stream).await, Poll::Pending));

        // The portal sits suspended past its deadline, and another statement
        // runs on the connection meanwhile.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let other = statement(&cancellation, None);
        assert!(cancellation.cancel(), "the last statement is cancellable");
        other.token().cancel();
        assert!(!state.token().is_cancelled());

        // A new Execute of the same portal: not a stale timeout, and the
        // statement is the connection's current one again.
        cancellation
            .begin_execution("c", "s", None)
            .expect("statement admitted");
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
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");

        // Bound at bind time (the extended-protocol handler registers the
        // portal's statement before the planner runs).
        let parsed = cancellation
            .begin_statement(None, Some("p"))
            .expect("statement admitted");
        assert!(
            cancellation
                .execution_for("p")
                .is_some_and(|it| Arc::ptr_eq(&it, &parsed)),
            "the planner must reuse the statement begin_statement registered"
        );

        // Bound at execute time.
        let executed = cancellation
            .begin_execution("c", "s", None)
            .expect("statement admitted");
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
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");
        let state = cancellation
            .begin_execution("c", "s", Some(Duration::from_millis(50)))
            .expect("statement admitted");
        let first = state.token();
        assert!(cancellation.cancel(), "the execution is cancellable");
        assert!(first.is_cancelled());

        cancellation.finish_execution(&state);
        assert!(
            !cancellation.cancel(),
            "an idle connection runs no statement"
        );

        let next = cancellation
            .begin_execution("c", "s", Some(Duration::from_millis(50)))
            .expect("statement admitted");
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
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");
        let state = cancellation
            .begin_execution("c", "s", None)
            .expect("statement admitted");
        assert_eq!(state.deadline(), None);

        cancellation
            .begin_execution("c", "s", Some(Duration::from_millis(20)))
            .expect("statement admitted");
        assert!(
            state.deadline().is_some(),
            "the timeout set while the portal was suspended applies now"
        );

        cancellation
            .begin_execution("c", "s", None)
            .expect("statement admitted");
        assert_eq!(state.deadline(), None, "and turning it off applies too");
    }

    /// A closed portal must not be kept for the lifetime of the connection.
    #[test]
    fn closing_a_portal_forgets_its_statement() {
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");
        let state = cancellation
            .begin_execution("c", "s", None)
            .expect("statement admitted");
        cancellation.forget_portal("c");
        assert!(!cancellation.cancel(), "the closed portal runs nothing");

        let next = cancellation
            .begin_execution("c", "s", None)
            .expect("statement admitted");
        assert!(
            !Arc::ptr_eq(&state, &next),
            "a name reused after close gets a fresh statement"
        );
    }

    /// `CLOSE ALL` forgets every portal, and each of them stops keeping its
    /// statement: closing the same name again starts a fresh statement.
    #[test]
    fn forget_all_portals_frees_every_portal() {
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");
        let a = cancellation
            .begin_execution("a", "sa", None)
            .expect("statement admitted");
        let b = cancellation
            .begin_execution("b", "sb", None)
            .expect("statement admitted");
        cancellation.forget_all_portals();
        assert_eq!(cancellation.portal_count(), 0);
        assert!(!cancellation.cancel(), "no portal keeps a statement");
        let next_a = cancellation
            .begin_execution("a", "sa", None)
            .expect("statement admitted");
        assert!(
            !Arc::ptr_eq(&a, &next_a),
            "a name closed by CLOSE ALL gets a fresh statement"
        );
        let next_b = cancellation
            .begin_execution("b", "sb", None)
            .expect("statement admitted");
        assert!(!Arc::ptr_eq(&b, &next_b));
    }

    /// The statement a portal runs holds one slot from the first `Execute`
    /// until its result ends, however many times pgwire fetches it, and one
    /// identity: a `Parse` and the `Execute`s that follow are one statement.
    #[tokio::test]
    async fn a_portal_holds_one_slot_and_one_identity_across_executions() {
        let (_server, cancellation) = cancellation_with(Limits {
            max_queries_per_user: 1,
            ..Limits::default()
        });
        let parse = cancellation.begin_parse(None).expect("parse admitted");
        let id = parse.query_id();
        // A parse registers its identity only once it succeeded, which is what
        // the `Execute`s that follow report under.
        cancellation.commit_parse("s", &parse);
        // Planning holds the slot while it runs and gives it back when it
        // returns: a statement that is never executed has no work to charge.
        assert!(parse.report().is_none(), "planning writes no report");
        assert!(cancellation.begin_statement(None, None).is_err());
        parse.release_permit();
        cancellation.finish_execution(&parse);
        let probe = cancellation
            .begin_statement(None, None)
            .expect("the slot came back");
        probe.end(StatementOutcome::Completed);

        // The first Execute takes the slot; a second statement is refused.
        let first = cancellation
            .begin_execution("p", "s", None)
            .expect("first execute admitted");
        assert!(cancellation.begin_statement(None, None).is_err());

        // Suspending the portal does not give the slot back: the result still
        // has rows to fetch, and the next `Execute` reports its own fetch.
        let first_report = first.report().expect("first fetch");
        cancellation.finish_execution(&first);
        assert!(cancellation.begin_statement(None, None).is_err());

        // Resuming is the same statement: no second admission, same identity.
        let resumed = cancellation
            .begin_execution("p", "s", None)
            .expect("resuming takes no second slot");
        assert!(Arc::ptr_eq(&first, &resumed));
        assert_eq!(resumed.query_id(), id);
        assert!(
            !Arc::ptr_eq(&first_report, &resumed.report().expect("second fetch")),
            "each Execute reports its own fetch"
        );

        // The result ends: the slot returns.
        resumed.end(StatementOutcome::Completed);
        assert!(cancellation.begin_statement(None, None).is_ok());
    }

    /// A parse registers its identity only once it succeeded: a `Parse` that
    /// failed leaves no entry behind, so the next `Execute` of the name is a
    /// statement of its own rather than the reporting of another one.
    #[test]
    fn a_failed_parse_leaves_no_identity_behind() {
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");
        let parse = cancellation.begin_parse(None).expect("planning admitted");
        parse.release_permit();
        cancellation.finish_execution(&parse);

        let executed = cancellation
            .begin_execution("p", "unparsed", None)
            .expect("executed");
        assert_ne!(
            executed.query_id(),
            parse.query_id(),
            "a name whose parse failed must not report under it"
        );
    }

    /// A name parsed again is a new statement, not the one it replaced - even
    /// when the name is the unnamed statement clients reuse for every query.
    /// Its executions report the new query id and parse time.
    #[test]
    fn reparsing_a_name_gives_its_executions_the_new_identity() {
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");

        let first = cancellation.begin_parse(None).expect("first parse");
        cancellation.commit_parse("s", &first);
        let first_execution = cancellation
            .begin_execution("p1", "s", None)
            .expect("first execution");
        assert_eq!(first_execution.query_id(), first.query_id());

        let second = cancellation.begin_parse(None).expect("second parse");
        cancellation.commit_parse("s", &second);
        let second_execution = cancellation
            .begin_execution("p2", "s", None)
            .expect("second execution");
        assert_eq!(
            second_execution.query_id(),
            second.query_id(),
            "the executions of the new statement report its identity"
        );
        assert_ne!(
            second_execution.query_id(),
            first_execution.query_id(),
            "and not the replaced statement's"
        );
    }

    /// The result ending is not the send ending: pgwire collects the rows of an
    /// extended fetch before it writes any of them, so the wrapper keeps the
    /// slot when the stream runs out and reports nothing yet - the caller ends
    /// the statement once the rows have been sent.
    #[tokio::test]
    async fn the_result_ending_holds_the_slot_until_the_caller_releases_it() {
        let (_server, cancellation) = cancellation_with(Limits {
            max_queries_per_user: 1,
            ..Limits::default()
        });
        let state = statement(&cancellation, None);
        let mut stream = wrapped(rows(1), &state);
        assert!(stream.next().await.expect("a row").is_ok());
        assert!(stream.next().await.is_none(), "the result ended");
        assert!(state.result_ended(), "the caller is told the result ended");
        assert_eq!(
            state.result_outcome(),
            Some(StatementOutcome::Completed),
            "a whole result is what the wrapper ended on"
        );
        assert!(
            !state.report().expect("report").is_finished(),
            "the send has not ended, so the statement is not reported yet"
        );
        assert!(
            cancellation.begin_statement(None, None).is_err(),
            "the slot is held while the collected rows are being sent"
        );

        state.end_after_send(StatementOutcome::Completed);
        assert!(state.report().expect("report").is_finished());
        assert!(
            cancellation.begin_statement(None, None).is_ok(),
            "the slot returns when the caller releases it"
        );
    }

    /// A result that completed is only reported as completed once its rows have
    /// been sent: a send that a slow client pushes into a deadline is the
    /// ending the client saw, and the ending the statement reports.
    #[tokio::test]
    async fn a_send_cut_short_reports_the_send_ending_not_the_result() {
        let (_server, cancellation) = cancellation_with(Limits::default());
        let state = statement(&cancellation, None);
        let mut stream = wrapped(rows(2), &state);
        assert_eq!(
            collect(&mut stream).await,
            vec!["ok", "ok"],
            "the result completed while pgwire was collecting it"
        );
        assert_eq!(state.result_outcome(), Some(StatementOutcome::Completed));
        assert!(!state.report().expect("report").is_finished());

        // The send of those collected rows ran into the statement's deadline.
        state.end(StatementOutcome::TimedOut);
        assert_eq!(
            state.report().expect("report").outcome(),
            StatementOutcome::TimedOut,
            "the interruption is not lost to an earlier completed"
        );
        assert_eq!(
            state.report().expect("report").delivered(),
            2,
            "the ending does not change what was counted"
        );
    }

    /// Rows are credited to the execution that produced them: a fetch that
    /// suspends with a row in hand (one row per `Execute`) must not push that
    /// row into the next execution's report.
    #[tokio::test]
    async fn a_row_is_credited_to_the_execution_that_produced_it() {
        let (_server, cancellation) = cancellation_with(Limits::default());
        let state = cancellation
            .begin_execution("c", "s", None)
            .expect("executed");
        let mut stream = wrapped(rows(2), &state);
        assert!(stream.next().await.expect("a row").is_ok());
        let first = state.report().expect("first report");
        assert_eq!(first.delivered(), 1);

        // The portal is resumed: a new execution, with a report of its own.
        cancellation
            .begin_execution("c", "s", None)
            .expect("resumed");
        let second = state.report().expect("second report");
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(
            second.delivered(),
            0,
            "the earlier execution's row is not credited again"
        );
        assert!(stream.next().await.expect("a row").is_ok());
        assert_eq!(second.delivered(), 1);
    }

    /// Delivered rows are what the report shows: the sending layer counts a
    /// row as it hands it to the transport.
    #[test]
    fn delivered_rows_are_counted_for_the_report() {
        let handle = StatementReport::new(StatementIdentity::new("user_a"));
        handle.note_delivered(3);
        assert_eq!(handle.delivered(), 3);
        handle.finish(StatementOutcome::Completed);
        assert_eq!(handle.delivered(), 3);
    }

    /// A statement is reported once, whichever path ends it first.
    #[test]
    fn a_statement_is_reported_once() {
        let handle = StatementReport::new(StatementIdentity::new("user_a"));
        assert!(!handle.is_finished());
        handle.finish(StatementOutcome::ResultLimit);
        handle.finish(StatementOutcome::Completed);
        assert!(handle.is_finished());
        assert_eq!(
            handle.outcome(),
            StatementOutcome::ResultLimit,
            "the first ending is the one reported"
        );
    }

    /// A user cannot run more statements at once than the server allows: the
    /// statement is refused, and the slot returns when the running one ends.
    #[tokio::test]
    async fn a_statement_cannot_exceed_its_user_limit() {
        let (_server, cancellation) = cancellation_with(Limits {
            max_queries_per_user: 1,
            ..Limits::default()
        });
        let running = statement(&cancellation, None);
        let error = cancellation
            .begin_statement(None, None)
            .expect_err("the user is already running a statement");
        assert!(matches!(
            error,
            PgWireError::UserError(info) if info.code == "53400"
                && info.message.contains("user_a")
        ));

        running.end(StatementOutcome::Completed);
        assert!(
            cancellation.begin_statement(None, None).is_ok(),
            "the slot returns when the statement ends"
        );
    }

    /// A prepared statement executed twice is two executions: each reports on
    /// its own - its own rows, its own report - and the first one's numbers
    /// cannot inflate the second's.
    #[tokio::test]
    async fn each_execution_of_a_prepared_statement_reports_separately() {
        let (_server, cancellation) = cancellation_with(Limits {
            max_queries_per_user: 1,
            ..Limits::default()
        });
        let first = cancellation
            .begin_execution("p1", "s", None)
            .expect("first execution admitted");
        first.note_row(8).unwrap();
        first.note_delivered(1);
        let first_report = first.report().expect("first report");
        first.end(StatementOutcome::Completed);
        assert_eq!(first_report.delivered(), 1);

        // The same prepared statement, a new portal: a fresh execution with a
        // fresh report and a slot of its own (the first one returned its own).
        let second = cancellation
            .begin_execution("p2", "s", None)
            .expect("second execution admitted");
        let second_report = second.report().expect("second report");
        assert!(
            !Arc::ptr_eq(&first_report, &second_report),
            "a later execution does not reuse the earlier report"
        );
        assert_eq!(second.query_id(), first.query_id());
        assert_eq!(
            second_report.delivered(),
            0,
            "the earlier execution's rows are not counted again"
        );

        // Ending the first execution twice must not release the second's slot.
        first.finish(StatementOutcome::Abandoned);
        assert!(
            cancellation.begin_statement(None, None).is_err(),
            "the running execution still holds the slot"
        );
        second.end(StatementOutcome::Completed);
        assert!(cancellation.begin_statement(None, None).is_ok());
    }

    /// Rows are charged their wire size, not just their values: the message
    /// type, the length and the field count are bytes the client receives too.
    #[tokio::test]
    async fn rows_are_charged_their_wire_size() {
        // Seven bytes of overhead per row: an empty row fits a ten-byte
        // budget, two do not.
        let (_server, cancellation) = cancellation_with(Limits {
            max_result_bytes: 10,
            ..Limits::default()
        });
        let state = statement(&cancellation, None);
        let mut stream = wrapped(rows(2), &state);
        assert!(stream.next().await.expect("a row").is_ok());
        let refused = stream.next().await.expect("a second row");
        assert!(
            refused.is_err(),
            "two empty rows exceed a ten-byte budget once framed"
        );
        assert_eq!(
            state.result_outcome(),
            Some(StatementOutcome::ResultLimit),
            "the result ended on the limit"
        );
        state.end_after_send(StatementOutcome::Completed);
        assert_eq!(
            state.report().expect("report").outcome(),
            StatementOutcome::ResultLimit,
            "and that is the ending the sent statement reports"
        );
    }

    /// A row is counted as delivered when the stream hands it out: that is the
    /// last point at which it can be attributed to the execution that produced
    /// it, because pgwire collects the rows of an extended fetch before it
    /// writes any of them.
    #[tokio::test]
    async fn rows_are_counted_as_they_are_handed_out() {
        let (_server, cancellation) = cancellation_with(Limits::default());
        let state = statement(&cancellation, None);
        let mut stream = wrapped(rows(3), &state);

        for expected in 1..=3 {
            assert!(stream.next().await.expect("a row").is_ok());
            assert_eq!(
                state.report().expect("report").delivered(),
                expected,
                "the row that was just handed out is the one counted"
            );
        }
        assert!(stream.next().await.is_none());
        assert_eq!(state.report().expect("report").delivered(), 3);
    }

    /// The result-row limit ends the statement, and reports it: the rows
    /// before the limit still arrive.
    #[tokio::test]
    async fn a_result_beyond_the_configured_maximum_is_refused() {
        let (_server, cancellation) = cancellation_with(Limits {
            max_result_rows: 2,
            ..Limits::default()
        });
        let state = statement(&cancellation, None);
        let mut stream = wrapped(rows(5), &state);
        assert_eq!(
            collect(&mut stream).await,
            vec![
                "ok",
                "ok",
                "err:53400:result exceeds the configured maximum of 2 rows"
            ]
        );
        assert_eq!(
            state.result_outcome(),
            Some(StatementOutcome::ResultLimit),
            "the result ended on the limit"
        );
        state.end_after_send(StatementOutcome::Completed);
        assert_eq!(
            state.report().expect("report").outcome(),
            StatementOutcome::ResultLimit,
            "and that is the ending the sent statement reports"
        );
    }

    /// The result limits bound the portal's whole result, not one fetch of it:
    /// fetching a row per `Execute` must not let the result grow past the limit
    /// the earlier fetches already spent.
    #[tokio::test]
    async fn a_resumed_portal_keeps_the_result_limit_it_already_spent() {
        let (_server, cancellation) = cancellation_with(Limits {
            max_result_rows: 2,
            ..Limits::default()
        });
        let state = cancellation
            .begin_execution("c", "s", None)
            .expect("first execution admitted");
        let mut stream = wrapped(rows(5), &state);
        assert!(stream.next().await.expect("a row").is_ok());
        assert!(stream.next().await.expect("a row").is_ok());

        // The next `Execute` of the same portal: a new execution and a new
        // report, but the same result.
        cancellation
            .begin_execution("c", "s", None)
            .expect("the portal is resumed");
        assert_eq!(
            collect(&mut stream).await,
            vec!["err:53400:result exceeds the configured maximum of 2 rows"],
            "the rows the earlier fetches produced are still charged"
        );
        assert_eq!(
            state.result_outcome(),
            Some(StatementOutcome::ResultLimit),
            "the result ended on the limit"
        );
        state.end_after_send(StatementOutcome::Completed);
        assert_eq!(
            state.report().expect("report").outcome(),
            StatementOutcome::ResultLimit,
            "and that is the ending the sent statement reports"
        );
    }

    #[tokio::test]
    async fn other_responses_are_not_wrapped() {
        let cancellation = cancellation();
        let state = statement(&cancellation, None);
        assert!(matches!(
            guard_response(Response::EmptyQuery, &state),
            Response::EmptyQuery
        ));
    }

    #[tokio::test]
    async fn a_statement_finishing_inside_its_deadline_is_not_interrupted() {
        let cancellation = cancellation();
        let state = statement(&cancellation, Some(Duration::from_secs(30)));
        let output = with_cancellation(&state, async { 7 })
            .await
            .expect("no cancel");
        assert_eq!(output, 7);
    }

    #[tokio::test]
    async fn cancelling_reaches_work_that_has_not_produced_rows_yet() {
        let cancellation = cancellation();
        let state = statement(&cancellation, None);
        let token = state.token().clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            token.cancel();
        });
        let interruption = with_cancellation(&state, futures::future::pending::<i32>())
            .await
            .expect_err("a cancelled statement must not complete");
        assert_eq!(interruption, Interruption::Canceled);
        assert_eq!(interruption.outcome(), StatementOutcome::Canceled);
        assert!(matches!(
            interruption.error(),
            PgWireError::UserError(info) if info.code == "57014"
                && info.message.contains("user request")
        ));
    }

    #[tokio::test]
    async fn the_deadline_covers_work_that_has_not_produced_rows_yet() {
        let cancellation = cancellation();
        let state = statement(&cancellation, Some(Duration::from_millis(5)));
        let interruption = with_cancellation(&state, futures::future::pending::<i32>())
            .await
            .expect_err("a statement past its deadline must not complete");
        assert_eq!(interruption, Interruption::TimedOut);
        assert!(matches!(
            interruption.error(),
            PgWireError::UserError(info) if info.code == "57014"
                && info.message.contains("statement timeout")
        ));
    }

    /// The work is made ready immediately, so only checking the token before
    /// polling it - and before the work - keeps a fast statement from running
    /// after the cancellation that already ended it.
    #[tokio::test]
    async fn a_ready_future_does_not_outrun_an_already_cancelled_statement() {
        let cancellation = cancellation();
        let state = statement(&cancellation, None);
        state.token().cancel();
        let ran = Arc::new(AtomicBool::new(false));
        let work = {
            let ran = Arc::clone(&ran);
            async move {
                ran.store(true, Ordering::Relaxed);
                7
            }
        };
        let interruption = with_cancellation(&state, work)
            .await
            .expect_err("a cancelled statement must not complete");
        assert_eq!(interruption, Interruption::Canceled);
        assert!(!ran.load(Ordering::Relaxed), "the work must not have run");
    }

    /// Same race on the other condition: a deadline that has already passed
    /// must beat work that is ready at once.
    #[tokio::test]
    async fn a_ready_future_does_not_outrun_an_expired_deadline() {
        let cancellation = cancellation();
        let state = statement(&cancellation, Some(Duration::ZERO));
        let ran = Arc::new(AtomicBool::new(false));
        let work = {
            let ran = Arc::clone(&ran);
            async move {
                ran.store(true, Ordering::Relaxed);
                7
            }
        };
        let interruption = with_cancellation(&state, work)
            .await
            .expect_err("a statement past its deadline must not complete");
        assert_eq!(interruption, Interruption::TimedOut);
        assert!(!ran.load(Ordering::Relaxed), "the work must not have run");
    }

    /// Cancel and deadline can both be set on the same pass; the row wrapper
    /// reports the cancellation, so the statement wrapper must agree.
    #[tokio::test]
    async fn a_cancelled_statement_wins_over_its_expired_deadline() {
        let cancellation = cancellation();
        let state = statement(&cancellation, Some(Duration::ZERO));
        state.token().cancel();
        let interruption = with_cancellation(&state, futures::future::ready(7))
            .await
            .expect_err("a cancelled statement must not complete");
        assert_eq!(interruption, Interruption::Canceled);
    }

    #[test]
    fn the_cancel_registry_addresses_a_connection_by_pid_and_secret() {
        let registry = Arc::new(CancelRegistry::new());
        let cancellation = cancellation();
        let _registration = registry.register(7, b"secret", Arc::clone(&cancellation));

        assert!(!cancellation.cancel(), "no statement in flight yet");
        let state = cancellation
            .begin_statement(None, None)
            .expect("statement admitted");
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
        let cancellation = cancellation();
        let state = cancellation
            .begin_statement(None, None)
            .expect("statement admitted");
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
        let cancellation =
            QueryCancellation::new(ServerLimits::new(Limits::default()), "user_a");
        let first = cancellation
            .begin_statement(None, None)
            .expect("statement admitted");
        let second = cancellation
            .begin_statement(None, None)
            .expect("statement admitted");
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
