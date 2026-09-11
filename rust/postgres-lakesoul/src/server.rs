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
use datafusion_postgres::pgwire;
use datafusion_postgres::{DfSessionService, Parser};
use futures::Sink;
use pgwire::api::auth::StartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionManager, ErrorHandler, METADATA_DATABASE,
    METADATA_USER, PgWireServerHandlers, Type,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::Parse;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use rootcause::Report;
use tracing::{info, warn};

use crate::read_only::statement_hooks;
use crate::session::{PgSession, PgSessionFactory, SessionIdentity, SessionSettings};

/// State owned by one PostgreSQL connection.
pub struct ConnectionSession {
    /// Connection-local session (identity + settings). Used by hooks/tests
    /// that need per-connection settings beyond the `SessionContext`.
    #[allow(dead_code)]
    pub session: Arc<PgSession>,
    pub service: Arc<DfSessionService>,
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

/// Startup handler: creates one session per connection.
///
/// `post_startup` runs after the startup parameters have been written to the
/// client metadata and before `ReadyForQuery` is sent, which makes it the
/// right place to build the connection-local session. A failure here is
/// reported as a `FATAL` error so the connection never reaches
/// `ReadyForQuery`.
pub struct LakeSoulStartupHandler {
    session_factory: Arc<PgSessionFactory>,
    connection_manager: Arc<ConnectionManager>,
}

impl LakeSoulStartupHandler {
    pub(crate) fn new(
        session_factory: Arc<PgSessionFactory>,
        connection_manager: Arc<ConnectionManager>,
    ) -> Self {
        Self {
            session_factory,
            connection_manager,
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

        let session = self
            .session_factory
            .create_session(
                SessionIdentity { user, database },
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
        client
            .session_extensions()
            .insert(ConnectionSession { session, service });
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
    ) -> PgWireResult<Self::Statement>
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
        <DfSessionService as SimpleQueryHandler>::do_query(
            state.service.as_ref(),
            client,
            query,
        )
        .await
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
        <DfSessionService as ExtendedQueryHandler>::on_parse(
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
        <DfSessionService as ExtendedQueryHandler>::do_query(
            state.service.as_ref(),
            client,
            portal,
            max_rows,
        )
        .await
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
/// `MetaDataClient`, `AuthManager` and config template) plus the cancel
/// connection manager. No `SessionContext` is retained here.
pub struct LakeSoulHandlers {
    router: Arc<LakeSoulQueryRouter>,
    startup_handler: Arc<LakeSoulStartupHandler>,
    cancel_handler: Arc<DefaultCancelHandler>,
}

impl LakeSoulHandlers {
    pub fn new(session_factory: Arc<PgSessionFactory>) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new());
        Self {
            router: Arc::new(LakeSoulQueryRouter::new()),
            startup_handler: Arc::new(LakeSoulStartupHandler::new(
                session_factory,
                Arc::clone(&connection_manager),
            )),
            cancel_handler: Arc::new(DefaultCancelHandler::new(connection_manager)),
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
