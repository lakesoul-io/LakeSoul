// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Connection-local session routing for the PostgreSQL wire server.
//!
//! [`PgSessionFactory`] is globally shared, but `create_session()` runs once
//! per TCP connection during the startup phase. The resulting [`PgSession`]
//! and its [`DfSessionService`] are stored in pgwire's per-connection
//! `SessionExtensions`, so they are released automatically when the
//! connection closes. Every query is routed to the service that belongs to
//! the current connection — never to a globally shared `SessionContext`.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::pgwire;
use datafusion_postgres::{DfSessionService, Parser};
use futures::{Sink, SinkExt};
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::cancel::CancelHandler;
use pgwire::api::copy;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{
    ExtendedQueryHandler, SimpleQueryHandler, send_execution_response,
    send_query_response, send_ready_for_query,
};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionManager, DEFAULT_NAME, ErrorHandler,
    METADATA_DATABASE, METADATA_USER, PgWireConnectionState, PgWireServerHandlers, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::{
    Bind, Close, Execute, Parse, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL,
};
use pgwire::messages::response::{EmptyQueryResponse, TransactionStatus};
use pgwire::messages::simplequery::Query;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use rootcause::Report;
use tracing::{info, warn};

use crate::cancel::{
    CancelRegistration, CancelRegistry, ExecutionState, LakeSoulCancelHandler,
    QueryCancellation, guard_response, statement_timeout, with_cancellation,
};
use crate::limits::{ConnectionPermit, ServerLimits, StatementOutcome};
use crate::read_only::{rejected_startup_isolation, statement_hooks};
use crate::session::{PgSession, PgSessionFactory, SessionIdentity, SessionSettings};

/// Drops the response a failed `Execute` would otherwise leave in its portal,
/// and the statement registered for that portal.
///
/// The response holds the row stream, so dropping it is what releases the
/// execution behind it - and the statement must not outlive the portal either.
fn release_portal<C>(
    client: &mut C,
    cancellation: &QueryCancellation,
    portal: &str,
    execution: &Arc<ExecutionState>,
) where
    C: ClientPortalStore,
    C::PortalStore: PortalStore,
{
    client.portal_store().rm_portal(portal);
    cancellation.finish_execution(execution);
    // `forget_portal` ends the statement if it had not ended yet (it reports
    // it as abandoned), and drops the mapping, so nothing is kept for a portal
    // pgwire no longer has.
    cancellation.forget_portal(portal);
}

/// State owned by one PostgreSQL connection.
pub struct ConnectionSession {
    /// Connection-local session (identity + settings). Used by hooks/tests
    /// that need per-connection settings beyond the `SessionContext`.
    #[allow(dead_code)]
    pub session: Arc<PgSession>,
    pub service: Arc<DfSessionService>,
    /// The statement this connection is running, so a `CancelRequest` (or the
    /// connection going away) stops it.
    pub cancellation: Arc<QueryCancellation>,
    /// The connection's slot: released when this state is dropped.
    #[allow(dead_code)]
    pub permit: ConnectionPermit,
}

/// Resolve the connection-local session state from pgwire's
/// `SessionExtensions`.
fn connection_session<C>(client: &C) -> PgWireResult<Arc<ConnectionSession>>
where
    C: ClientInfo,
{
    client
        .session_extensions()
        .get::<ConnectionSession>()
        .ok_or_else(session_not_initialized)
}

fn session_not_initialized() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_string(),
        "XX000".to_string(),
        "session not initialized for this connection".to_string(),
    )))
}

fn fatal_startup_error(error: Report) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_string(),
        "XX000".to_string(),
        error.format_current_context_unhooked().to_string(),
    )))
}

impl LakeSoulQueryRouter {
    /// Executes the statements of a SimpleQuery one at a time and sends each
    /// statement's rows before starting the next: every statement is a
    /// PostgreSQL statement execution of its own, with its own cancellation
    /// token, its own deadline and its own result on the wire.
    async fn run_statements<C>(
        &self,
        client: &mut C,
        query_text: String,
    ) -> PgWireResult<()>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if !matches!(client.state(), PgWireConnectionState::ReadyForQuery) {
            return Err(PgWireError::NotReadyForQuery);
        }
        let state = connection_session(client)?;
        let mut transaction_status = client.transaction_status();
        client.set_state(PgWireConnectionState::QueryInProgress);

        // The batch's first statement is registered before the untrusted text
        // is parsed, so a cancel request during parsing finds a statement in
        // flight instead of being discarded. The token aborts the first
        // statement before its work is polled. Later statements get their own
        // registration and deadline. Empty batches give this registration up
        // below without a report.
        //
        // If registration is refused, inspect only small inputs: long inputs
        // are refused without parsing. The parser distinguishes empty batches,
        // including comment-only text, from requests that carry a statement.
        let mut pending = match state
            .cancellation
            .begin_statement(statement_timeout(client), None)
        {
            Ok(execution) => Some(execution),
            Err(refused) => {
                // A refused request has no cancellation registration or
                // statement deadline. Only inspect a small input here; long
                // inputs are refused without scanning or parsing them.
                if query_text.len() > 8192
                    || !split_statements(&query_text)
                        .is_some_and(|statements| statements.is_empty())
                {
                    return Err(refused);
                }
                None
            }
        };

        // PostgreSQL stops a batch at the first failing statement: the rest is
        // never executed, and the client still gets its `ReadyForQuery`.
        // Whatever ends the batch - success, a failing statement, a cancelled
        // or interrupted send - the status the statements that already ran
        // left behind becomes the client's before the error is reported:
        // pgwire's error handling derives the reported status from the
        // client's, so persisting only after a fully successful batch would
        // let an error after a `COMMIT` resurrect the ended transaction as
        // failed.
        let mut failed = false;
        let outcome: PgWireResult<()> = async {
            let statements =
                split_statements(&query_text).unwrap_or_else(|| vec![query_text.clone()]);
            if statements.is_empty() {
                client
                    .feed(PgWireBackendMessage::EmptyQueryResponse(
                        EmptyQueryResponse::new(),
                    ))
                    .await?;
            }
            for statement in statements {
                if is_empty_query(&statement) {
                    client
                        .feed(PgWireBackendMessage::EmptyQueryResponse(
                            EmptyQueryResponse::new(),
                        ))
                        .await?;
                    continue;
                }
                let execution = match pending.take() {
                    Some(execution) => execution,
                    None => state
                        .cancellation
                        .begin_statement(statement_timeout(client), None)?,
                };
                let planned = run_under_limit(
                    &execution,
                    <DfSessionService as SimpleQueryHandler>::do_query(
                        state.service.as_ref(),
                        client,
                        &statement,
                    ),
                )
                .await;
                let responses = match planned {
                    Ok(responses) => responses,
                    Err(error) => {
                        // `run_under_limit` already ended an interrupted
                        // statement; a planning error ends it as a failure.
                        execution.end_after_send(StatementOutcome::Failed);
                        state.cancellation.finish_execution(&execution);
                        return Err(error);
                    }
                };
                for response in responses {
                    let response = guard_response(response, &execution);
                    // The socket write is bounded as well: a client that stops
                    // reading must not keep the statement - and the execution
                    // behind its rows - alive behind a full send buffer.
                    let sent = run_under_limit(
                        &execution,
                        send_response(
                            client,
                            response,
                            &mut transaction_status,
                            &mut failed,
                        ),
                    )
                    .await;
                    if let Err(error) = sent {
                        execution.end_after_send(StatementOutcome::Failed);
                        state.cancellation.finish_execution(&execution);
                        return Err(error);
                    }
                }
                // The statement is over once its rows have been sent: a
                // response that carried no row stream (a `SET`, a transaction
                // command) ends completed, and one whose rows ended on their
                // own - at the result limit, at a failing row, at a cancel the
                // wrapper observed while producing them - keeps that ending,
                // which the wrapper recorded.
                execution.end_after_send(StatementOutcome::Completed);
                state.cancellation.finish_execution(&execution);
                // The statements after this one read the client's status to
                // decide what they may run, so every statement's transitions
                // must reach the client before the next one runs: a batch like
                // `ROLLBACK; SELECT 1` would otherwise run the SELECT against
                // the stale pre-ROLLBACK status, and `COMMIT; BEGIN` would
                // begin nothing. The persist after the batch below still covers
                // the error paths, which return early.
                client.set_transaction_status(transaction_status);
                if failed {
                    break;
                }
            }
            // Every statement of the batch was empty (a text that is only
            // comments, say): the statement registered before the text was read
            // never ran, so it gives up its slot here instead of staying in
            // flight until the connection goes away - and writes no report,
            // like a `Parse` that is never executed.
            if let Some(execution) = pending {
                execution.discard();
                state.cancellation.finish_execution(&execution);
            }
            Ok(())
        }
        .await;
        client.set_transaction_status(transaction_status);
        outcome?;

        client.set_state(PgWireConnectionState::ReadyForQuery);
        send_ready_for_query(client, transaction_status).await?;
        Ok(())
    }
}

/// Runs one phase of a statement under its cancellation and deadline.
///
/// An interruption ends the statement - its slot and its report - before the
/// error is returned, whichever phase it interrupted; a phase that completes
/// leaves the ending to the caller. A phase that *sends* rows leaves it to the
/// row wrapper too, which records what the result ended on - the rows running
/// out, the result limit, a failing row - for the caller to report once the
/// send is over.
async fn run_under_limit<T, F>(
    execution: &Arc<ExecutionState>,
    future: F,
) -> PgWireResult<T>
where
    F: Future<Output = PgWireResult<T>>,
{
    match with_cancellation(execution, future).await {
        Ok(result) => result,
        Err(interruption) => {
            execution.finish(interruption.outcome());
            Err(interruption.error())
        }
    }
}

/// Sends one response of a SimpleQuery, mirroring the upstream service.
async fn send_response<C>(
    client: &mut C,
    response: Response,
    transaction_status: &mut TransactionStatus,
    failed: &mut bool,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: std::fmt::Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    match response {
        Response::EmptyQuery => {
            client
                .feed(PgWireBackendMessage::EmptyQueryResponse(
                    EmptyQueryResponse::new(),
                ))
                .await?;
        }
        Response::Query(results) => {
            send_query_response(client, results, true).await?;
        }
        Response::Execution(tag) => {
            send_execution_response(client, tag).await?;
        }
        Response::TransactionStart(tag) => {
            send_execution_response(client, tag).await?;
            *transaction_status = transaction_status.to_in_transaction_state();
        }
        Response::TransactionEnd(tag) => {
            send_execution_response(client, tag).await?;
            *transaction_status = transaction_status.to_idle_state();
        }
        Response::Error(error) => {
            client
                .feed(PgWireBackendMessage::ErrorResponse((*error).into()))
                .await?;
            *transaction_status = transaction_status.to_error_state();
            *failed = true;
        }
        Response::CopyIn(result) => {
            copy::send_copy_in_response(client, result).await?;
        }
        Response::CopyOut(result) => {
            copy::send_copy_out_response(client, result).await?;
        }
        Response::CopyBoth(result) => {
            copy::send_copy_both_response(client, result).await?;
        }
    }
    Ok(())
}
fn is_empty_query(sql: &str) -> bool {
    sql.chars().all(|c| c == ';' || c.is_whitespace())
}

/// Splits a SimpleQuery into its statements the way the upstream service
/// parses them, so each one gets its own cancellation token and deadline.
///
/// Returns `None` when there is nothing to split - a single statement, or text
/// this parser cannot handle - in which case the query is passed through
/// unchanged and the upstream service reports its own error.
fn split_statements(sql: &str) -> Option<Vec<String>> {
    let statements = PostgresCompatibilityParser::new().parse(sql).ok()?;
    if statements.is_empty() {
        return Some(Vec::new());
    }
    if statements.len() == 1 {
        return None;
    }
    Some(statements.iter().map(ToString::to_string).collect())
}
/// Startup handler: creates one session per connection.
///
/// `post_startup` runs after the startup parameters have been written to the
/// client metadata and before `ReadyForQuery` is sent, which makes it the
/// right place to build the connection-local session. A failure here is
/// reported as a `FATAL` error so the connection never reaches
/// `ReadyForQuery`.
pub struct LakeSoulStartupHandler {
    session_factory: Arc<PgSessionFactory>,
    cancel_registry: Arc<CancelRegistry>,
    connection_manager: Arc<ConnectionManager>,
    server_limits: Arc<ServerLimits>,
}

impl LakeSoulStartupHandler {
    pub(crate) fn new(
        session_factory: Arc<PgSessionFactory>,
        cancel_registry: Arc<CancelRegistry>,
        connection_manager: Arc<ConnectionManager>,
        server_limits: Arc<ServerLimits>,
    ) -> Self {
        Self {
            session_factory,
            cancel_registry,
            connection_manager,
            server_limits,
        }
    }
}

#[async_trait]
impl NoopStartupHandler for LakeSoulStartupHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }

    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // The session's isolation semantics are fixed (READ COMMITTED, no
        // multi-statement snapshot). A client that requests a stronger
        // default at startup would otherwise connect under a false
        // assumption, so the connection is rejected before a session is
        // ever created.
        if let Some(error) = rejected_startup_isolation(client.metadata()) {
            return Err(error);
        }
        let user = client
            .metadata()
            .get(METADATA_USER)
            .cloned()
            .unwrap_or_else(|| "postgres".to_string());
        let database = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| user.clone());
        // The connection takes its slot before any session work: a server at
        // its limit refuses the client here, as a FATAL, instead of running
        // one more session past the bound.
        let permit = self.server_limits.admit_connection()?;
        let session = self
            .session_factory
            .create_session(
                SessionIdentity {
                    user: user.clone(),
                    database,
                },
                &SessionSettings::default(),
            )
            .await
            .map_err(fatal_startup_error)?;
        info!(
            user = %session.identity.user,
            database = %session.identity.database,
            "created connection-local session"
        );

        let service = Arc::new(DfSessionService::new_with_hooks(
            Arc::clone(&session.context),
            statement_hooks(),
        ));
        let cancellation = Arc::new(QueryCancellation::new(
            Arc::clone(&self.server_limits),
            &user,
        ));
        // Reachable by `CancelRequest` until the connection ends; the guard
        // also cancels the statement in flight when the client disconnects.
        let (pid, secret_key) = client.pid_and_secret_key();
        let registration = self.cancel_registry.register(
            pid,
            &secret_key.to_bytes(),
            Arc::clone(&cancellation),
        );
        client
            .session_extensions()
            .insert::<CancelRegistration>(registration);
        client.session_extensions().insert(ConnectionSession {
            session,
            service,
            cancellation,
            permit,
        });
        Ok(())
    }
}

/// Connection-aware parser facade.
///
/// `parse_sql` routes to the parser bound to the current connection's
/// `SessionContext`. `get_parameter_types` / `get_result_schema` are pure
/// plan-level functions and delegate to the fallback parser; the fallback's
/// session context is never used for planning.
///
/// Public because it appears as `ExtendedQueryHandler::QueryParser` for the
/// [`LakeSoulQueryRouter`], but it is not part of the crate's public API
/// surface otherwise.
pub struct __ConnectionParser {
    fallback: Arc<Parser>,
}

#[async_trait]
impl QueryParser for __ConnectionParser {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Option<Self::Statement>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let state = connection_session(client)?;
        let parser = state.service.query_parser();
        parser.parse_sql(client, sql, types).await
    }

    fn get_parameter_types(&self, stmt: &Self::Statement) -> PgWireResult<Vec<Type>> {
        self.fallback.get_parameter_types(stmt)
    }

    fn get_result_schema(
        &self,
        stmt: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        self.fallback.get_result_schema(stmt, column_format)
    }
}

/// Stateless router that forwards every query to the connection-local
/// [`DfSessionService`].
pub struct LakeSoulQueryRouter {
    parser: Arc<__ConnectionParser>,
}

impl LakeSoulQueryRouter {
    fn new() -> Self {
        // Throwaway context only backs the fallback parser's plan-level
        // helpers (parameter/result schema). Parsing is always routed to the
        // connection-local parser.
        let fallback = DfSessionService::new(Arc::new(SessionContext::new()));
        let fallback =
            <DfSessionService as ExtendedQueryHandler>::query_parser(&fallback);
        Self {
            parser: Arc::new(__ConnectionParser { fallback }),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for LakeSoulQueryRouter {
    /// Runs a multi-statement SimpleQuery statement by statement, sending each
    /// statement's rows before the next one starts.
    ///
    /// The route through `do_query` builds every response first and lets pgwire
    /// send them afterwards, which would leave one statement's cancellation
    /// token and deadline covering the sending of the earlier ones: a cancel
    /// request during the first result would hit the last statement's token,
    /// and a later statement's deadline would already be running. Single
    /// statements keep the upstream flow.
    async fn on_query<C>(&self, client: &mut C, query: Query) -> PgWireResult<()>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Every query goes through the statement-by-statement path, including
        // a single statement: that is what bounds the sending of its rows, not
        // only their production. The text is split inside `run_statements`,
        // after the statement that bounds the parse is registered.
        self.run_statements(client, query.query).await
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        // The whole text in one call: `on_query` hands a statement of a batch
        // - or a single statement - to every bounded execution, and this call
        // has no batch to split.
        let execution = state
            .cancellation
            .begin_statement(statement_timeout(client), None)?;
        let responses = match run_under_limit(
            &execution,
            <DfSessionService as SimpleQueryHandler>::do_query(
                state.service.as_ref(),
                client,
                query,
            ),
        )
        .await
        {
            Ok(responses) => responses,
            Err(error) => {
                execution.finish(StatementOutcome::Failed);
                state.cancellation.finish_execution(&execution);
                return Err(error);
            }
        };
        Ok(responses
            .into_iter()
            .map(|response| guard_response(response, &execution))
            .collect())
    }
}

#[async_trait]
impl ExtendedQueryHandler for LakeSoulQueryRouter {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;
    type QueryParser = __ConnectionParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        // A statement is planned here, not in `Execute`: the upstream parser
        // resolves tables and reads metadata, so this phase is bounded the same
        // way, or a cancel request and the statement timeout would both miss
        // it. The statement it starts is the one the `Execute` that follows
        // reports, so planning and execution share one identity.
        let name = message
            .name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_string());
        let execution = state.cancellation.begin_parse(statement_timeout(client))?;
        let result = run_under_limit(
            &execution,
            <DfSessionService as ExtendedQueryHandler>::on_parse(
                state.service.as_ref(),
                client,
                message,
            ),
        )
        .await;
        match result {
            // Planned, not run: the connection has nothing in flight, though
            // the statement keeps its identity (and its log line) for the
            // `Execute` that follows, or ends at Close, or when the connection
            // goes away. The identity is registered now, not before the
            // planning: a name whose parse failed has no statement behind it,
            // so it must leave no entry - and a name parsed again must not
            // report the replaced statement's query id and parse time.
            Ok(()) => {
                state.cancellation.commit_parse(&name, &execution);
                // Planning is over: its slot returns, and a statement that is
                // never executed leaves no report behind. The identity stays,
                // so the `Execute` that follows reports under it.
                execution.release_permit();
                state.cancellation.finish_execution(&execution);
                Ok(())
            }
            Err(error) => {
                execution.end(StatementOutcome::Failed);
                state.cancellation.finish_execution(&execution);
                Err(error)
            }
        }
    }

    /// A `Bind` replaces whatever portal the name held: pgwire keeps one portal
    /// per name and PostgreSQL drops the previous unnamed portal at the next
    /// `Bind`, so the statement that portal was running ends here. Its result
    /// is not the result the new binding will fetch, and the rows it produced
    /// must not be charged against the new one.
    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        let portal = message
            .portal_name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_string());
        let result = <DfSessionService as ExtendedQueryHandler>::on_bind(
            state.service.as_ref(),
            client,
            message,
        )
        .await;
        // Forgotten only once the binding replaced the portal: a `Bind` that
        // failed (an unknown statement) leaves the existing portal, and the
        // statement running it, in place.
        if result.is_ok() {
            state.cancellation.forget_portal(&portal);
        }
        result
    }

    /// Every `Execute` is a statement execution of its own: when it resumes a
    /// suspended portal, the portal's statement becomes the one this connection
    /// is running again and its deadline starts now. pgwire fetches a suspended
    /// portal itself, without calling the handler that created it, so this is
    /// the only place that boundary is visible.
    async fn on_execute<C>(&self, client: &mut C, message: Execute) -> PgWireResult<()>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // An unnamed portal is addressed by pgwire's default name, not by
        // "none".
        let state = connection_session(client)?;
        let portal = message
            .name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_string());
        // Only an existing portal gets a statement: registering one for a name
        // that does not exist would leave an entry nothing ever cleans up,
        // because pgwire never had that portal to drop.
        let execution = client
            .portal_store()
            .get_portal(&portal)
            .and_then(|entry| match entry {
                pgwire::api::store::Entry::Value(portal) => Some(portal),
                pgwire::api::store::Entry::Empty => None,
            })
            .map(|portal| {
                state.cancellation.begin_execution(
                    &portal.name,
                    &portal.statement.id,
                    statement_timeout(client),
                )
            })
            .transpose()?;
        let Some(execution) = execution else {
            return self._on_execute(client, message).await;
        };
        // Fetching the portal and sending its rows happen inside this future:
        // bounding it is what stops a statement whose client has stopped
        // reading, instead of leaving the execution alive behind a blocked
        // socket write. When the limit ends it, the portal must not keep the
        // response it borrows - dropping that response is what releases the
        // execution behind its rows.
        let result = match with_cancellation(
            &execution,
            self._on_execute(client, message),
        )
        .await
        {
            Ok(result) => result,
            Err(interruption) => {
                execution.end(interruption.outcome());
                release_portal(client, &state.cancellation, &portal, &execution);
                return Err(interruption.error());
            }
        };
        // The execution is over (a suspended portal runs nothing until the next
        // `Execute`), so a cancel request arriving now must not reach this
        // portal's token. A suspended portal keeps its statement - and its
        // slot: it still holds rows - until its result ends, is closed, or the
        // connection goes away.
        state.cancellation.finish_execution(&execution);
        if result.is_err() {
            // The send failed, or a row failed while pgwire was collecting it:
            // a result that ended on its own (the result limit, a failing row)
            // keeps that ending, a completed one takes the failure.
            execution.end_after_send(StatementOutcome::Failed);
            release_portal(client, &state.cancellation, &portal, &execution);
        } else if !execution.ran_a_stream() {
            // A response without rows (`SET`, a transaction command) has no
            // stream to end the statement, so the `Execute` that sent it does:
            // otherwise a named portal would hold the connection's statement
            // slot until it is closed.
            execution.end(StatementOutcome::Completed);
        } else if execution.result_ended() {
            // The result ended while pgwire was collecting its rows, and those
            // rows have been sent by now (`_on_execute` returned only after
            // writing them): this is where the statement ends, so a send that a
            // slow client pushes into a cancel or a deadline is what gets
            // reported, and the collection - which can run far ahead of that
            // client - does not free the connection's quota early.
            execution.end_after_send(StatementOutcome::Completed);
        }
        result
    }

    /// Closing a portal drops its statement: pgwire frees the portal, so the
    /// connection must not keep its execution (nor its token) either.
    async fn on_close<C>(&self, client: &mut C, message: Close) -> PgWireResult<()>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match message.target_type {
            TARGET_TYPE_BYTE_PORTAL => state.cancellation.forget_portal(name),
            // A closed prepared statement: pgwire drops it, so the statement it
            // was charged for goes with it.
            _ => state.cancellation.forget_statement(name),
        }
        <DfSessionService as ExtendedQueryHandler>::on_close(
            state.service.as_ref(),
            client,
            message,
        )
        .await
    }

    /// `Sync` ends the implicit transaction and lets pgwire drop the unnamed
    /// portal, so its statement goes with it.
    async fn on_sync<C>(&self, client: &mut C, message: PgSync) -> PgWireResult<()>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        state.cancellation.forget_portal(DEFAULT_NAME);
        <DfSessionService as ExtendedQueryHandler>::on_sync(
            state.service.as_ref(),
            client,
            message,
        )
        .await
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        // The limit starts before the upstream service runs the statement, so
        // planning and stage start-up are covered; the row wrapper then keeps
        // the same token and the remaining time. A portal that pgwire resumes
        // later is re-registered by the wrapper itself.
        // `on_execute` already started this portal's statement before the
        // fetch: reuse it, so the token a cancel request reaches is the one the
        // fetch and the sending watch. Only a direct call (a test) starts one.
        let execution = match state.cancellation.execution_for(&portal.name) {
            Some(execution) => execution,
            None => state
                .cancellation
                .begin_statement(statement_timeout(client), Some(&portal.name))?,
        };
        let response = match run_under_limit(
            &execution,
            <DfSessionService as ExtendedQueryHandler>::do_query(
                state.service.as_ref(),
                client,
                portal,
                max_rows,
            ),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                execution.end(StatementOutcome::Failed);
                return Err(error);
            }
        };
        Ok(guard_response(response, &execution))
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        <DfSessionService as ExtendedQueryHandler>::do_describe_statement(
            state.service.as_ref(),
            client,
            target,
        )
        .await
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo
            + ClientPortalStore
            + Sink<PgWireBackendMessage>
            + Unpin
            + Send
            + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = connection_session(client)?;
        <DfSessionService as ExtendedQueryHandler>::do_describe_portal(
            state.service.as_ref(),
            client,
            target,
        )
        .await
    }
}

/// Top-level pgwire handler set.
///
/// Globally shared state: `PgSessionFactory` (and transitively the
/// `MetaDataClient`, `AuthManager` and config template), the registry that
/// makes connections reachable by `CancelRequest`, and pgwire's connection
/// manager. No `SessionContext` is retained here.
pub struct LakeSoulHandlers {
    router: Arc<LakeSoulQueryRouter>,
    startup_handler: Arc<LakeSoulStartupHandler>,
    cancel_handler: Arc<LakeSoulCancelHandler>,
}

impl LakeSoulHandlers {
    /// Builds the handler set. `server_limits` bounds connections and
    /// statements.
    pub fn new(
        session_factory: Arc<PgSessionFactory>,
        server_limits: Arc<ServerLimits>,
    ) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new());
        let cancel_registry = Arc::new(CancelRegistry::new());
        Self {
            router: Arc::new(LakeSoulQueryRouter::new()),
            startup_handler: Arc::new(LakeSoulStartupHandler::new(
                session_factory,
                Arc::clone(&cancel_registry),
                connection_manager,
                server_limits,
            )),
            cancel_handler: Arc::new(LakeSoulCancelHandler::new(cancel_registry)),
        }
    }
}

impl PgWireServerHandlers for LakeSoulHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.router.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.router.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup_handler.clone()
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        self.cancel_handler.clone()
    }
}

struct LoggingErrorHandler;

impl ErrorHandler for LoggingErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        warn!("Sending error: {error}")
    }
}

#[cfg(test)]
mod tests;
