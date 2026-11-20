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

use crate::auth::DomainAccess;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::scalar_subquery::ScalarSubqueryCardinalityError;
use datafusion::prelude::SessionContext;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::pgwire;
use datafusion_postgres::{DfSessionService, Parser};
use futures::{Sink, SinkExt};
use lakesoul_metadata::JwtServer;
use pgwire::api::auth::{
    DefaultServerParameterProvider, ServerParameterProvider, StartupHandler,
    protocol_negotiation, save_startup_parameters_to_metadata,
};
use pgwire::api::cancel::CancelHandler;
use pgwire::api::copy;
use pgwire::api::portal::{Format, Portal, PortalExecutionState};
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
    ClientInfo, ClientPortalStore, ConnectionGuard, ConnectionHandle, ConnectionManager,
    DEFAULT_NAME, ErrorHandler, METADATA_DATABASE, METADATA_USER, PgWireConnectionState,
    PgWireServerHandlers, PidSecretKeyGenerator, RandomPidSecretKeyGenerator, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::{
    Bind, Close, Execute, Parse, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL,
};
use pgwire::messages::response::{EmptyQueryResponse, ReadyForQuery, TransactionStatus};
use pgwire::messages::simplequery::Query;
use pgwire::messages::startup::{Authentication, BackendKeyData, ParameterStatus};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use rootcause::Report;
use tracing::{debug, info, warn};

use crate::cancel::{
    CancelRegistration, CancelRegistry, ExecutionState, LakeSoulCancelHandler,
    QueryCancellation, guard_response, statement_timeout, with_cancellation,
};
use crate::limits::{ConnectionPermit, ServerLimits, StatementOutcome};
use crate::read_only::{rejected_startup_isolation, statement_hooks_with};
use crate::session::{PgSession, PgSessionFactory, SessionIdentity, SessionSettings};
use crate::session_settings::{
    SessionDefaults, SettingsGeneration, initialize_client_metadata,
};

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
    /// The open transaction block, shared with the write path.
    pub transaction: crate::transaction::SharedTransaction,
    /// The metadata client a `COMMIT` publishes through.
    pub meta_client: lakesoul_metadata::MetaDataClientRef,
    /// The connection's slot: released when this state is dropped.
    #[allow(dead_code)]
    pub permit: ConnectionPermit,
    /// Startup settings `RESET`/`DISCARD ALL` restore.
    pub defaults: SessionDefaults,
    /// Session-setting changes, and the generation each prepared statement
    /// was planned under: `Execute` replans a statement whose plan predates a
    /// change.
    pub settings: Arc<SettingsGeneration>,
    /// The server limits, for the transaction block's staged-file accounting.
    pub limits: crate::limits::Limits,
}

/// The result format requested by the current extended-protocol `Bind`.
///
/// pgwire sends `RowDescription` from the portal's `result_column_format` at
/// Describe time, while a query hook returns already-encoded `DataRow`s. A
/// hook that produces rows itself must encode them with the same format.
#[derive(Default)]
pub(crate) struct ResultFormatCell(pub(crate) parking_lot::Mutex<Format>);

impl Drop for ConnectionSession {
    /// A connection that goes away with an open block leaves its staged files
    /// unreferenced: delete them instead of waiting for GC.
    ///
    /// A `COMMIT` in flight owns its files through `StagedFiles` and the block
    /// state is already gone, so this only removes what no statement is
    /// publishing.
    fn drop(&mut self) {
        let Some(state) = self.transaction.lock().take() else {
            return;
        };
        if state.pending.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let context = self.session.context.clone();
        runtime.spawn(async move {
            crate::transaction::discard_staged_files(&context, state.pending).await;
        });
    }
}

/// Resolve the connection-local session state from pgwire's
/// `SessionExtensions`.
pub(crate) fn connection_session<C>(client: &C) -> PgWireResult<Arc<ConnectionSession>>
where
    C: ClientInfo + ?Sized,
{
    client
        .session_extensions()
        .get::<ConnectionSession>()
        .ok_or_else(session_not_initialized)
}

/// Rewrite DDL sqlparser cannot parse to the sentinel SELECT its hook claims.
fn rewrite_special_statements(sql: &str) -> PgWireResult<String> {
    // `ALTER TABLE ... RESET (...)` is not in sqlparser either; it becomes the
    // `SET (k = NULL)` form `DdlStatementHook` reads as a reset.
    let sql = crate::alter::rewrite_reset_options(sql).unwrap_or_else(|| sql.to_string());
    // `REINDEX` is not in sqlparser either; it becomes the sentinel SELECT
    // `IndexStatementHook` claims.
    let sql = crate::index_ddl::rewrite(&sql)?.unwrap_or(sql);
    let sql = crate::federation::rewrite(&sql)?.unwrap_or(sql);
    let sql = crate::user::rewrite_create_user(&sql).unwrap_or(sql);
    let sql = crate::subscribe::sql::rewrite(&sql)?.unwrap_or(sql);
    let sql = crate::snapshot::rewrite(&sql).unwrap_or(sql);
    // psycopg2 and other libpq clients send bytea parameters as text
    // literals (`'\\x...'::bytea`). Decode them before DataFusion's raw
    // Utf8 -> Binary cast can store the literal characters.
    crate::pg_compat::rewrite_bytea_literals(&sql)
        .map(|rewritten| rewritten.unwrap_or(sql))
}

pub(crate) fn session_not_initialized() -> PgWireError {
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

/// Refuse a startup that did not negotiate TLS when `--require-tls` is on.
///
/// PostgreSQL's `hostssl` equivalent: the listener still answers an
/// `SSLRequest`, and only a client that completed the TLS handshake reaches
/// authentication. The check runs before the startup parameters are stored
/// and before any session is built, so a plaintext client holds no slot.
fn reject_plaintext_startup<C>(
    client: &C,
    auth: &crate::auth::AuthSettings,
) -> PgWireResult<()>
where
    C: ClientInfo + ?Sized,
{
    if auth.require_tls && !client.is_secure() {
        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_string(),
            "28000".to_string(),
            "TLS is required: connect with sslmode=require (or verify-full)".to_string(),
        ))));
    }
    Ok(())
}

/// PostgreSQL `cardinality_violation`.
fn cardinality_violation_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "21000".to_string(),
        "more than one row returned by a subquery used as an expression".to_string(),
    )))
}

/// Maps execution errors that have a PostgreSQL SQLSTATE equivalent.
///
/// The match is on the typed error emitted by `ScalarSubqueryExec`, not on
/// error text, so unrelated execution failures keep the generic `XX000`
/// mapping.
fn map_statement_error(error: PgWireError) -> PgWireError {
    let PgWireError::ApiError(source) = &error else {
        return error;
    };
    // Native repartition/write operators may add rootcause wrappers between
    // DataFusion errors. Follow the typed source chain, not only DataFusion's
    // Context/SharedResult variants or the displayed error text.
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(source.as_ref());
    while let Some(cause) = current {
        if cause.is::<ScalarSubqueryCardinalityError>()
            || cause
                .downcast_ref::<DataFusionError>()
                .is_some_and(|error| {
                    matches!(
                        error.find_root(),
                        DataFusionError::External(inner)
                            if inner.is::<ScalarSubqueryCardinalityError>()
                    )
                })
        {
            return cardinality_violation_error();
        }
        if let Some(incompatible) = incompatible_column_type(cause) {
            return incompatible_file_type_error(incompatible);
        }
        if let Some(missing) = missing_table_error(cause) {
            return missing;
        }
        current = cause.source();
    }
    error
}

/// The typed schema-merge failure, also when DataFusion wrapped it in
/// variants whose `source()` does not lead to it.
fn incompatible_column_type<'a>(
    cause: &'a (dyn std::error::Error + 'static),
) -> Option<&'a lakesoul_io::file_format::IncompatibleColumnType> {
    if let Some(typed) = cause.downcast_ref() {
        return Some(typed);
    }
    match cause.downcast_ref::<DataFusionError>()?.find_root() {
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            datafusion::arrow::error::ArrowError::ExternalError(inner) => {
                inner.downcast_ref()
            }
            _ => None,
        },
        DataFusionError::External(inner) => inner.downcast_ref(),
        _ => None,
    }
}

/// DataFusion reports a catalog miss as a planning error. PostgreSQL reports
/// the same case as `42P01`; the pg-wire catalog hides ACL-invisible tables by
/// returning no provider, so those reads must not look like `XX000`.
fn missing_table_error(cause: &(dyn std::error::Error + 'static)) -> Option<PgWireError> {
    let message = cause.to_string();
    // DataFusion phrases a catalog miss as `table '…' not found` (and some
    // paths as `No table named '…'`). Match only those forms: other errors
    // mention a table name too, and must keep their own SQLSTATE.
    let (needle, found) = if message.contains("No table named '") {
        ("No table named '", true)
    } else if message.contains("table '") && message.contains("not found") {
        ("table '", true)
    } else {
        ("", false)
    };
    if !found {
        return None;
    }
    let relation = message
        .split(needle)
        .nth(1)
        .and_then(|rest| rest.split('\'').next())
        .filter(|name| !name.is_empty())
        .unwrap_or("?");
    let table = relation.rsplit('.').next().unwrap_or(relation);
    Some(PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42P01".to_string(),
        format!("relation \"{table}\" does not exist"),
    ))))
}

/// PostgreSQL `datatype_mismatch`: the table's data files disagree on a column
/// type no reader can reconcile (written by an engine with a looser schema
/// evolution rule than LakeSoul's), so every scan of it fails.
fn incompatible_file_type_error(
    incompatible: &lakesoul_io::file_format::IncompatibleColumnType,
) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42804".to_string(),
        incompatible.to_string(),
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
        // A `COPY ... FROM STDIN` hands the connection to the copy
        // sub-protocol; after its `CopyInResponse` the batch has no
        // `ReadyForQuery` and no further statement runs.
        let mut copy_started = false;
        let outcome: PgWireResult<()> = async {
            // `RESET TIME ZONE` is a multi-word PostgreSQL parameter name that
            // sqlparser cannot parse; a single-statement form is rewritten
            // before the batch is split (batches containing it stay a syntax
            // error). The rewrite runs after the first statement is registered,
            // so a cancel that arrives while the text is still being inspected
            // finds a statement in flight.
            let query_text =
                crate::session_settings::rewrite_reset_time_zone(&query_text)
                    .unwrap_or(query_text);
            // Subscription and federation DDL are not in sqlparser. Rewrite a
            // single such statement to the internal sentinel SELECT its hook
            // claims; a mixed batch is rejected here.
            let query_text = rewrite_special_statements(&query_text)?;
            // COPY takes over the connection until `CopyDone`; the rest of a
            // batch would never run. The check runs on the text because the
            // parser reads everything after `COPY ... FROM STDIN;` as inline
            // copy data and would hide the statements that follow.
            crate::copy_in::refuse_copy_in_batch(&query_text)?;
            let statements =
                split_statements(&query_text).unwrap_or_else(|| vec![query_text.clone()]);
            if statements.is_empty() {
                client
                    .feed(PgWireBackendMessage::EmptyQueryResponse(
                        EmptyQueryResponse::new(),
                    ))
                    .await?;
            }
            // A batch PostgreSQL would run in an implicit transaction cannot
            // contain DDL: the metadata commit cannot be rolled back.
            crate::ddl::refuse_ddl_in_batch(&statements)?;
            // Refused at the INSERT itself so statements before it still run
            // and stream their rows, as PostgreSQL does before the failure.
            let insert_position = crate::insert::first_insert_in_batch(&statements);
            let update_position = crate::update::first_update_in_batch(&statements);
            let truncate_position = crate::truncate::first_truncate_in_batch(&statements);
            let delete_position = crate::delete::first_delete_in_batch(&statements);
            for (index, statement) in statements.into_iter().enumerate() {
                if insert_position == Some(index) {
                    return Err(crate::insert::refuse_insert_in_batch());
                }
                if update_position == Some(index) {
                    return Err(crate::update::refuse_update_in_batch());
                }
                if truncate_position == Some(index) {
                    return Err(crate::truncate::refuse_truncate_in_batch());
                }
                if delete_position == Some(index) {
                    return Err(crate::delete::refuse_delete_in_batch());
                }
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
                let _statement = state
                    .session
                    .label_statement(execution.query_id(), &execution.span());
                // A block whose `BEGIN` response never reached the client is
                // not a transaction: undo it (and delete what it staged)
                // before this statement can stage into it.
                crate::transaction::reconcile(&state, client.transaction_status()).await;
                // The SQL text is only known here: `RUST_LOG=info,lakesoul_sql=debug`
                // turns it on without enabling debug for every target.
                debug!(
                    target: "lakesoul_sql",
                    query_id = execution.query_id(),
                    sql = %statement,
                    "executing SQL (simple query)"
                );
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
                        // A statement hook may have ended the block itself (a
                        // `COMMIT` that fails closes the transaction): the
                        // status it left on the client is the one pgwire
                        // reports in `ReadyForQuery`, so it must not be
                        // overwritten with this batch's stale copy.
                        transaction_status = client.transaction_status();
                        // `run_under_limit` already ended an interrupted
                        // statement; a planning error ends it as a failure.
                        execution.end_after_send(StatementOutcome::Failed);
                        state.cancellation.finish_execution(&execution);
                        return Err(error);
                    }
                };
                // A transaction hook may have changed the client's status: a
                // `COMMIT` reports the block idle before it publishes and
                // before the cancellable index build. Take that status into
                // this batch's copy, so an interruption while the response is
                // sent cannot write the stale pre-hook one back.
                transaction_status = client.transaction_status();
                for response in responses {
                    let response = guard_response(response, &execution);
                    let is_copy_in = matches!(response, Response::CopyIn(_));
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
                    if is_copy_in {
                        // The client now streams CopyData; pgwire owns the
                        // state transition and sends `ReadyForQuery` after
                        // `CopyDone`.
                        execution.end_after_send(StatementOutcome::Completed);
                        state.cancellation.finish_execution(&execution);
                        client.set_state(PgWireConnectionState::CopyInProgress(false));
                        copy_started = true;
                        break;
                    }
                }
                if copy_started {
                    break;
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
            if let Some(execution) = pending.take() {
                execution.discard();
                state.cancellation.finish_execution(&execution);
            }
            Ok(())
        }
        .await;
        // An error before the first statement runs (a mixed subscription
        // batch, for example) returns from the async block without reaching
        // its empty-batch cleanup. Give the pre-registered slot back here.
        if let Some(execution) = pending.take() {
            execution.discard();
            state.cancellation.finish_execution(&execution);
        }
        client.set_transaction_status(transaction_status);
        outcome?;
        if copy_started {
            // `send_response` already sent `CopyInResponse`; the protocol
            // continues with CopyData frames, not another query.
            return Ok(());
        }

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
pub(crate) async fn run_under_limit<T, F>(
    execution: &Arc<ExecutionState>,
    future: F,
) -> PgWireResult<T>
where
    F: Future<Output = PgWireResult<T>>,
{
    let future = tracing::Instrument::instrument(future, execution.span());
    match with_cancellation(execution, future).await {
        Ok(result) => result.map_err(map_statement_error),
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
    // The compatibility parser rewrites PostgreSQL into what DataFusion
    // accepts, and constructing it parses every entry of its blacklist with
    // sqlparser (tens of statements, ~0.3 ms).  That cost was paid per
    // SimpleQuery message and dominated a short statement: a client sending
    // `SELECT 1` spent 0.68 ms of a 0.79 ms statement here.  The parser is
    // immutable after construction, so one is built per process, and a text
    // without a statement separator cannot be a batch: it is answered without
    // parsing at all (the caller treats `None` as one statement).
    static PARSER: std::sync::LazyLock<PostgresCompatibilityParser> =
        std::sync::LazyLock::new(PostgresCompatibilityParser::new);
    // An empty query carries no statement at all: report it as an empty batch
    // so the statement quota is not charged for it.
    if is_empty_query(sql) {
        return Some(Vec::new());
    }
    if !sql.contains(';') {
        return None;
    }
    let statements = PARSER.parse(sql).ok()?;
    if statements.is_empty() {
        return Some(Vec::new());
    }
    if statements.len() == 1 {
        return None;
    }
    Some(statements.iter().map(ToString::to_string).collect())
}
/// Startup handler: authenticates the connection, then creates its session.
///
/// Without JWT authentication the `Startup` message completes the handshake
/// at once, as it always has. With it, the server answers `Startup` with a
/// cleartext password request and completes the handshake only when the
/// password is a valid LakeSoul token for the startup user (see
/// [`crate::auth`]); anything else is `FATAL 28P01` and no session is ever
/// built.
///
/// Completing the handshake sends `AuthenticationOk`, the server parameters
/// and `BackendKeyData`, builds the connection-local session, and only then
/// sends `ReadyForQuery`. pgwire's own helpers for that sequence are
/// crate-private, and its public `finish_authentication` sends
/// `ReadyForQuery` before the session could exist, so the sequence is
/// written out here.
pub struct LakeSoulStartupHandler {
    session_factory: Arc<PgSessionFactory>,
    cancel_registry: Arc<CancelRegistry>,
    connection_manager: Arc<ConnectionManager>,
    server_limits: Arc<ServerLimits>,
    pid_secret_key_generator: RandomPidSecretKeyGenerator,
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
            pid_secret_key_generator: RandomPidSecretKeyGenerator::default(),
        }
    }

    /// Checks the password sent for the startup user against the token
    /// verifier, and returns the domain the connection is confined to when
    /// domain enforcement is on. The reason for a refusal is logged, never
    /// sent.
    async fn authenticate<C: ClientInfo>(
        &self,
        client: &C,
        jwt: &JwtServer,
        password: &str,
    ) -> PgWireResult<Option<DomainAccess>> {
        let Some(user) = client.metadata().get(METADATA_USER).cloned() else {
            return Err(PgWireError::UserNameRequired);
        };
        let claims = match crate::auth::verify_token(jwt, &user, password) {
            Ok(claims) => claims,
            Err(failure) => {
                crate::auth::record_failure(&failure);
                warn!(
                    user = %user,
                    peer = %client.socket_addr(),
                    reason = failure.reason(),
                    detail = %failure,
                    "token authentication failed"
                );
                return Err(PgWireError::InvalidPassword(user));
            }
        };
        let settings = self.session_factory.auth();
        let listed = if settings.rbac {
            match settings.user_directory() {
                Some(directory) => match directory.is_user(&claims.group, &user).await {
                    Ok(listed) => listed,
                    Err(error) => {
                        // Fail closed, but keep the cause in the log: the
                        // client still only sees a generic password failure.
                        warn!(
                            user = %user,
                            group = %claims.group,
                            %error,
                            "user directory lookup failed"
                        );
                        false
                    }
                },
                None => true,
            }
        } else {
            true
        };
        if !listed {
            let failure = crate::auth::AuthFailure::UnknownUser {
                user: user.clone(),
                group: claims.group.clone(),
            };
            crate::auth::record_failure(&failure);
            warn!(
                user = %user,
                group = %claims.group,
                peer = %client.socket_addr(),
                reason = failure.reason(),
                detail = %failure,
                "token authentication failed"
            );
            return Err(PgWireError::InvalidPassword(user));
        }
        info!(user = %user, group = %claims.group, "token authenticated");
        Ok(settings.rbac.then(|| DomainAccess::new(claims.group)))
    }

    /// Ends a successful handshake: registers the connection, sends
    /// `AuthenticationOk`, the server parameters and `BackendKeyData`, builds
    /// the session, then sends `ReadyForQuery`.
    async fn complete_startup<C>(
        &self,
        client: &mut C,
        access: Option<DomainAccess>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (pid, secret_key) = self.pid_secret_key_generator.generate(client);
        client.set_pid_and_secret_key(pid, secret_key.clone());
        let (handle, guard) = self.connection_manager.register(pid, secret_key.clone());
        client
            .session_extensions()
            .insert::<Arc<ConnectionHandle>>(handle);
        client.session_extensions().insert::<ConnectionGuard>(guard);

        client
            .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
            .await?;
        if let Some(parameters) = server_parameter_provider().server_parameters(client) {
            for (name, value) in parameters {
                client
                    .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                        name, value,
                    )))
                    .await?;
            }
        }
        client
            .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
                pid, secret_key,
            )))
            .await?;

        self.establish_session(client, access).await?;

        client
            .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                TransactionStatus::Idle,
            )))
            .await?;
        client.set_state(PgWireConnectionState::ReadyForQuery);
        Ok(())
    }

    /// Builds the connection-local session and installs it in the client's
    /// session extensions.
    ///
    /// Runs after the startup parameters have been written to the client
    /// metadata and before `ReadyForQuery` is sent. A failure is reported as
    /// a `FATAL` error so the connection never reaches `ReadyForQuery`.
    ///
    /// `access` confines the session to one domain; `None` without `--rbac`.
    pub(crate) async fn establish_session<C>(
        &self,
        client: &mut C,
        access: Option<DomainAccess>,
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
                    access,
                },
                &SessionSettings::default(),
            )
            .await
            .map_err(fatal_startup_error)?;
        info!(
            user = %session.identity.user,
            database = %session.identity.database,
            domain = session
                .identity
                .access
                .as_ref()
                .map_or("-", DomainAccess::domain),
            "created connection-local session"
        );

        // Startup parameters the client sent are part of the session's
        // parameters: `SHOW application_name` and `current_setting` must see
        // them, not just the later `SET` path.
        for name in ["application_name", "client_encoding"] {
            if let Some(value) = client.metadata().get(name) {
                session.parameters.set_default(name, value);
            }
        }
        // Seed the metadata the `SHOW`/`SET` paths read from the settings the
        // session was actually created with, and report the real time zone:
        // pgwire's own startup `ParameterStatus` is a fixed `Etc/UTC`.
        let time_zone = initialize_client_metadata(&session, client.metadata_mut());
        client
            .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                "TimeZone".to_string(),
                time_zone,
            )))
            .await?;
        let defaults = SessionDefaults::capture(&session.context, client.metadata());
        let service = Arc::new(DfSessionService::new_with_hooks(
            Arc::clone(&session.context),
            connection_statement_hooks(&self.session_factory, &session),
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
            transaction: crate::transaction::shared(),
            meta_client: Arc::clone(self.session_factory.meta_client()),
            permit,
            defaults,
            settings: Arc::new(SettingsGeneration::default()),
            limits: self.server_limits.limits(),
        });
        Ok(())
    }
}

#[async_trait]
impl StartupHandler for LakeSoulStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                reject_plaintext_startup(client, self.session_factory.auth())?;
                save_startup_parameters_to_metadata(client, startup);
                if self.session_factory.auth().jwt.is_some() {
                    client.set_state(PgWireConnectionState::AuthenticationInProgress);
                    client
                        .send(PgWireBackendMessage::Authentication(
                            Authentication::CleartextPassword,
                        ))
                        .await?;
                } else {
                    self.complete_startup(client, None).await?;
                }
            }
            // Only reachable after this handler asked for a password: pgwire
            // routes startup-phase messages here, and without JWT the
            // handshake has already completed on `Startup`.
            PgWireFrontendMessage::PasswordMessageFamily(password) => {
                let Some(jwt) = self.session_factory.auth().jwt.as_deref() else {
                    return Ok(());
                };
                let password = password.into_password()?;
                let access = self.authenticate(client, jwt, &password.password).await?;
                self.complete_startup(client, access).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// Statement hooks for one connection.
///
/// The pgvector GUC hook claims `ivfflat.probes` before the upstream
/// `SetShowHook` offers the generic DataFusion config path, behind the
/// transaction hook like every other `SET` hook. The DDL hook claims
/// `CREATE TABLE`/`DROP TABLE`, which DataFusion would otherwise plan as its
/// own dialect-specific DDL.
pub(crate) fn connection_statement_hooks(
    factory: &PgSessionFactory,
    session: &PgSession,
) -> Vec<Arc<dyn datafusion_postgres::QueryHook>> {
    let identity = &session.identity;
    let database = identity.database.as_str();
    let mut hooks = crate::pg_vector::statement_hooks();
    let cluster_domain = session.cluster_domain().map(str::to_owned).or_else(|| {
        identity
            .access
            .as_ref()
            .map(|access| access.domain().to_string())
    });
    hooks.push(Arc::new(crate::acl::AclRefreshHook));
    if let Some(federation) = factory.federation() {
        let session_domain = identity
            .access
            .as_ref()
            .map(|access| access.domain().to_string());
        hooks.push(Arc::new(crate::federation::FederationStatementHook::new(
            Arc::clone(federation),
            database.to_string(),
            identity.user.clone(),
            cluster_domain.clone(),
            session_domain,
        )));
    }
    hooks.push(Arc::new(crate::user::UserStatementHook::new(
        Arc::clone(factory.meta_client()),
        identity.user.clone(),
    )));
    hooks.push(Arc::new(crate::grant::AclStatementHook::new(
        Arc::clone(factory.meta_client()),
        database.to_string(),
        identity.user.clone(),
    )));
    hooks.push(Arc::new(crate::snapshot::SnapshotDdlHook::new(
        Arc::clone(factory.meta_client()),
        database.to_string(),
    )));
    hooks.push(Arc::new(crate::ddl::DdlStatementHook::new(
        Arc::clone(factory.meta_client()),
        database.to_string(),
        factory.warehouse().map(str::to_owned),
        cluster_domain,
        identity.user.clone(),
    )));
    hooks.push(Arc::new(crate::index_ddl::IndexStatementHook::new(
        Arc::clone(factory.meta_client()),
        database.to_string(),
        factory.object_store_options().clone(),
    )));
    hooks.push(Arc::new(crate::ivm::IvmSettingsHook));
    hooks.push(Arc::new(crate::ivm::IvmInsertHook));
    hooks.push(Arc::new(crate::insert::InsertStatementHook));
    // COPY runs the copy sub-protocol: the hook answers the statement with
    // `CopyInResponse` and the router's `CopyHandler` consumes the frames.
    hooks.push(Arc::new(crate::copy_in::CopyStatementHook));
    if let Some(jobs) = factory.jobs() {
        hooks.push(Arc::new(crate::jobs::JobStatementHook::new(Arc::clone(
            jobs,
        ))));
        hooks.push(Arc::new(crate::jobs::JobIdentityGuard));
    }
    if let Some(supervisor) = factory.subscriptions() {
        hooks.push(Arc::new(
            crate::subscribe::sql::SubscriptionStatementHook::new(
                Arc::clone(factory.meta_client()),
                database.to_string(),
                factory.warehouse().map(str::to_owned),
                Arc::clone(supervisor),
            ),
        ));
    }
    hooks.push(Arc::new(crate::update::UpdateStatementHook));
    hooks.push(Arc::new(crate::truncate::TruncateStatementHook));
    hooks.push(Arc::new(crate::delete::DeleteStatementHook));
    statement_hooks_with(hooks)
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
        let rewritten = crate::session_settings::rewrite_reset_time_zone(sql);
        let sql = rewritten.as_deref().unwrap_or(sql);
        let sql = crate::federation::rewrite(sql)?.unwrap_or_else(|| sql.to_string());
        let sql = crate::user::rewrite_create_user(&sql).unwrap_or(sql);
        let sql =
            crate::subscribe::sql::rewrite(&sql)?.unwrap_or_else(|| sql.to_string());
        let sql = crate::snapshot::rewrite(&sql).unwrap_or(sql);
        let state = connection_session(client)?;
        let parser = state.service.query_parser();
        parser.parse_sql(client, &sql, types).await
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

    /// Plan the portal's statement again when the session settings changed
    /// since it was planned.
    ///
    /// A logical plan embeds session settings - `CAST(x AS TIMESTAMPTZ)`
    /// resolves its target type with the time zone current at plan time - so a
    /// statement parsed before a `SET` would otherwise execute with the old
    /// value. PostgreSQL runs a prepared statement with the settings current
    /// at execution.
    async fn replan_if_stale<C>(
        &self,
        client: &C,
        state: &ConnectionSession,
        portal: &Portal<<DfSessionService as ExtendedQueryHandler>::Statement>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore<
            Statement = <DfSessionService as ExtendedQueryHandler>::Statement,
        >,
    {
        let statement = &portal.statement;
        // This runs before the generation check: a REVOKE or GRANT made in
        // another session may have changed this connection's visibility since
        // the portal was planned. Refresh first, then make a changed view
        // invalidate that plan.
        state
            .session
            .refresh_acl_if_stale()
            .await
            .map_err(|error| {
                crate::ddl::user_error(
                    "XX000",
                    format!("refreshing table ACLs failed: {error}"),
                )
            })?;
        // The ACL entry is shared per (user, domains). Another connection may
        // have consumed the refresh, so the local `changed` result is not
        // enough: compare the view version recorded at plan time as well.
        if let Some(acl) = state.session.acl()
            && state.settings.acl_changed(&statement.id, acl.version())
        {
            state.settings.bump();
        }
        if !state.settings.needs_replan(&statement.id) {
            return Ok(());
        }
        // A portal that already started keeps the plan its result came from:
        // replacing it would restart the rows the client is fetching.
        if !matches!(*portal.state.lock().await, PortalExecutionState::Initial) {
            return Ok(());
        }
        let (sql, planned) = &statement.statement;
        // A table this plan bound may have been dropped by this connection's
        // own `DROP TABLE`: replanning would then fail as a generic catalog
        // resolution error (`XX000`), while PostgreSQL reports `42P01` for a
        // relation that no longer exists. Report the missing binding before
        // planning again, so same-connection DDL behaves like PostgreSQL.
        if let Some((_, plan)) = planned
            && let Some(table) = missing_bound_table(&state.session.context, plan)
        {
            return Err(undefined_relation(&table));
        }
        // Capture the version before planning: if a peer publishes a new view
        // while the replacement plan is being built, recording the post-plan
        // version would hide that change from the next Execute.
        let acl_version_before_plan = state.session.acl().map(|acl| acl.version());
        let Some(replanned) = self
            .parser
            .parse_sql(client, sql, &statement.parameter_types)
            .await?
        else {
            return Ok(());
        };
        // The client may already hold this statement's row description
        // (Describe after Parse) and decodes every row with it. A replan that
        // changes the result columns - `ALTER TABLE ... TYPE BIGINT` on a
        // selected `INT` column, or DROP + CREATE with other types - would
        // send rows the client misreads, so it is refused the way PostgreSQL
        // refuses it. The statement keeps its stale generation: every later
        // Execute is refused too, until the client prepares it again.
        if let Some((_, plan)) = planned
            && let (_, Some((_, new_plan))) = &replanned
            && result_columns(plan)? != result_columns(new_plan)?
        {
            return Err(changed_result_type());
        }
        let planned = replanned;
        let mut replacement = Portal::default();
        replacement.name = portal.name.clone();
        replacement.statement = Arc::new(StoredStatement::new(
            statement.id.clone(),
            planned,
            statement.parameter_types.clone(),
        ));
        replacement.parameter_format = portal.parameter_format.clone();
        replacement.parameters = portal.parameters.clone();
        replacement.result_column_format = portal.result_column_format.clone();
        replacement.state =
            Arc::new(tokio::sync::Mutex::new(PortalExecutionState::Initial));
        client.portal_store().put_portal(Arc::new(replacement));
        state.settings.record(&statement.id);
        if let Some(version) = acl_version_before_plan {
            state.settings.record_acl(&statement.id, version);
        }
        Ok(())
    }
}

/// The first table a stale plan scans that this connection's catalog no
/// longer holds.
///
/// Only called before replanning a portal whose plan predates a session
/// generation change. A same-connection `DROP TABLE` bumps that generation, so
/// a scan bound before the drop is reported as `42P01` at `Execute`, as
/// PostgreSQL does, instead of the generic catalog-resolution `XX000`.
fn missing_bound_table(context: &SessionContext, plan: &LogicalPlan) -> Option<String> {
    let state = context.state();
    let catalog_options = &state.config_options().catalog;
    let mut missing = None;
    let _ = plan.apply(|node| {
        if missing.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::TableScan(scan) = node {
            let resolved = scan.table_name.clone().resolve(
                &catalog_options.default_catalog,
                &catalog_options.default_schema,
            );
            let exists = state
                .catalog_list()
                .catalog(resolved.catalog.as_ref())
                .and_then(|catalog| catalog.schema(resolved.schema.as_ref()))
                .is_some_and(|schema| schema.table_exist(resolved.table.as_ref()));
            if !exists {
                missing = Some(resolved.table.to_string());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    missing
}

/// A plan's result columns as the client sees them: names and PostgreSQL
/// types, mapped exactly as `Describe` maps them. Arrow-level differences the
/// client cannot observe (`Utf8` vs `Utf8View`, nullability) do not count.
fn result_columns(plan: &LogicalPlan) -> PgWireResult<Vec<(String, Type)>> {
    Ok(
        datafusion_postgres::arrow_pg::datatypes::arrow_schema_to_pg_fields(
            plan.schema().as_arrow(),
            &Format::UnifiedText,
            None,
        )?
        .into_iter()
        .map(|field| (field.name().to_string(), field.datatype().clone()))
        .collect(),
    )
}

/// PostgreSQL's refusal to run a prepared statement whose result columns
/// changed since it was planned.
fn changed_result_type() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        "cached plan must not change result type".to_string(),
    )))
}

/// PostgreSQL `undefined_table` for a binding the catalog lost.
fn undefined_relation(name: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "42P01".to_string(),
        format!("relation \"{name}\" does not exist"),
    )))
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
        // `on_query` rewrites the batch before splitting; this direct entry
        // point (used by tests and embedded callers) does the same.
        let query = rewrite_special_statements(query)?;
        let query = query.as_str();
        // The whole text in one call: `on_query` hands a statement of a batch
        // - or a single statement - to every bounded execution, and this call
        // has no batch to split.
        let execution = state
            .cancellation
            .begin_statement(statement_timeout(client), None)?;
        let _statement = state
            .session
            .label_statement(execution.query_id(), &execution.span());
        debug!(
            target: "lakesoul_sql",
            query_id = execution.query_id(),
            sql = %query,
            "executing SQL (simple query)"
        );
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

/// Arrow result types of the portal's statement, in output order.
///
/// `None` when the statement has no logical plan (a protocol-level empty
/// query); the response is then left untouched.
fn plan_column_types(
    portal: &Portal<<DfSessionService as ExtendedQueryHandler>::Statement>,
) -> Option<Vec<DataType>> {
    let (_, plan) = portal.statement.statement.1.as_ref()?;
    Some(
        plan.schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect(),
    )
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
        let _statement = state
            .session
            .label_statement(execution.query_id(), &execution.span());
        // The extended protocol keeps no SQL text past the parse: the portal
        // only holds the parsed statement, so this is the only place the text
        // can be logged.
        debug!(
            target: "lakesoul_sql",
            query_id = execution.query_id(),
            statement_name = %name,
            sql = %message.query,
            "planning SQL (extended query)"
        );
        // `RESET TIME ZONE` is a multi-word PostgreSQL parameter name the
        // upstream parser cannot read; the statement stored here is the one
        // `Execute` plans from, so the rewrite must happen before it is parsed
        // (the log line above kept the text the client sent).
        let mut message = message;
        if let Some(rewritten) =
            crate::session_settings::rewrite_reset_time_zone(&message.query)
        {
            message.query = rewritten;
        }
        let rewrite = match rewrite_special_statements(&message.query) {
            Ok(rewritten) => {
                message.query = rewritten;
                Ok(())
            }
            Err(error) => Err(error),
        };
        // Capture the ACL version before planning; a peer may publish a new
        // view while `on_parse` runs, and the plan must remember the version it
        // was actually built against.
        let acl_version_before_plan = state.session.acl().map(|acl| acl.version());
        let result = match rewrite {
            Ok(()) => {
                run_under_limit(
                    &execution,
                    <DfSessionService as ExtendedQueryHandler>::on_parse(
                        state.service.as_ref(),
                        client,
                        message,
                    ),
                )
                .await
            }
            Err(error) => Err(error),
        };
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
                // The plan just made belongs to the current settings; a later
                // `SET` bumps past it and makes the next `Execute` replan.
                state.settings.record(&name);
                if let Some(version) = acl_version_before_plan {
                    state.settings.record_acl(&name, version);
                }
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
            // Remember the result format at Bind time. Hooks that build a
            // response before `Execute` (INSERT ... RETURNING) need it to know
            // whether to encode text or binary; waiting for `on_execute` is too
            // late for them.
            if let Some(pgwire::api::store::Entry::Value(portal)) =
                client.portal_store().get_portal(&portal)
            {
                *client
                    .session_extensions()
                    .get_or_insert_with(ResultFormatCell::default)
                    .0
                    .lock() = portal.result_column_format.clone();
            }
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
        // See the simple-query path: a block the client never learned about
        // must not receive this statement.
        crate::transaction::reconcile(&state, client.transaction_status()).await;
        let portal = message
            .name
            .clone()
            .unwrap_or_else(|| DEFAULT_NAME.to_string());
        // Only an existing portal gets a statement: registering one for a name
        // that does not exist would leave an entry nothing ever cleans up,
        // because pgwire never had that portal to drop.
        let stored =
            client
                .portal_store()
                .get_portal(&portal)
                .and_then(|entry| match entry {
                    pgwire::api::store::Entry::Value(portal) => Some(portal),
                    pgwire::api::store::Entry::Empty => None,
                });
        if let Some(portal) = stored.as_ref() {
            *client
                .session_extensions()
                .get_or_insert_with(ResultFormatCell::default)
                .0
                .lock() = portal.result_column_format.clone();
        }
        let execution = stored
            .as_ref()
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
        // The stored plan was made at `Parse`; a `SET` since then leaves it
        // stale, and PostgreSQL runs a prepared statement with the settings
        // current at execution. Bounded like the parse it repeats.
        if let Some(portal) = stored.as_ref() {
            run_under_limit(
                &execution,
                self.replan_if_stale(client, state.as_ref(), portal),
            )
            .await?;
        }
        // Fetching the portal and sending its rows happen inside this future:
        // bounding it is what stops a statement whose client has stopped
        // reading, instead of leaving the execution alive behind a blocked
        // socket write. When the limit ends it, the portal must not keep the
        // response it borrows - dropping that response is what releases the
        // execution behind its rows.
        let execute = tracing::Instrument::instrument(
            self._on_execute(client, message),
            execution.span(),
        );
        let result = match with_cancellation(&execution, execute).await {
            Ok(result) => result.map_err(map_statement_error),
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
            _ => {
                state.settings.forget(name);
                state.cancellation.forget_statement(name)
            }
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
        let _statement = state
            .session
            .label_statement(execution.query_id(), &execution.span());
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
        // A binary DECIMAL cell carries only its value: the column scale lives
        // in the statement's Arrow schema. Re-encode zero cells with it so a
        // typed client decodes `0.00` as scale 2, as PostgreSQL sends it.
        let response = match plan_column_types(portal) {
            Some(types) => {
                match crate::pg_numeric::rewrite_decimal_scale(response, &types) {
                    Ok(response) => response,
                    Err(error) => {
                        execution.end(StatementOutcome::Failed);
                        return Err(error);
                    }
                }
            }
            None => response,
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

/// The startup parameters every connection sees.
///
/// The version must agree with `version()` and the session parameters:
/// clients gate SQL features on it, and advertising pgwire's own version
/// would claim support for a newer PostgreSQL than the engine implements.
fn server_parameter_provider() -> DefaultServerParameterProvider {
    let mut provider = DefaultServerParameterProvider::default();
    provider.server_version = crate::pg_settings::PG_COMPAT_VERSION.to_string();
    provider.search_path = "public, pg_catalog".to_string();
    provider
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

    fn copy_handler(&self) -> Arc<impl pgwire::api::copy::CopyHandler> {
        self.router.clone()
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
