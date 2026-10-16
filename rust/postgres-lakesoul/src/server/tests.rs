// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for connection-local session routing.
//!
//! These tests require the metadata PostgreSQL the metadata client resolves —
//! `LAKESOUL_PG_URL`, or its built-in local default when the variable is
//! absent (see `script/meta_init_for_local_test.sh`). There is no
//! availability gate on purpose: a missing database fails the tests loudly
//! instead of silently skipping them.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::limits::Limits;
use datafusion::prelude::SessionContext;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::pgwire;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{FieldInfo, Response, SendableRowStream};
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore};
use datafusion_postgres::pgwire::messages::data::DataRow;
use datafusion_postgres::testing::MockClient;
use futures::{Sink, StreamExt};
use lakesoul_metadata::MetaDataClient;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::stmt::QueryParser;
use pgwire::api::{
    METADATA_DATABASE, METADATA_USER, PgWireConnectionState, SessionExtensions,
};
use pgwire::error::PgWireError;
use pgwire::messages::extendedquery::{
    Bind, Close, Execute, Parse, TARGET_TYPE_BYTE_PORTAL,
};
use pgwire::messages::simplequery::Query;
use pgwire::messages::{
    PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
    extendedquery::Sync as PgSync, response::TransactionStatus, startup::SecretKey,
};

use super::{ConnectionSession, LakeSoulQueryRouter, LakeSoulStartupHandler};
use crate::session::{PgSessionFactory, SessionIdentity, SessionSettings};

/// Time zone configured by `SessionSettings::default()`.
const DEFAULT_TIME_ZONE: &str = "UTC";

async fn test_factory() -> Arc<PgSessionFactory> {
    let meta_client = Arc::new(
        MetaDataClient::from_env()
            .await
            .expect("PostgreSQL required"),
    );
    Arc::new(
        PgSessionFactory::new(
            meta_client,
            &lakesoul_datafusion::cli::CoreArgs::from_env(),
            Arc::new(datafusion_postgres::auth::AuthManager::new()),
        )
        .expect("failed to create session factory"),
    )
}

/// Simulate one connection startup: create a session and attach it to a
/// mock client's `SessionExtensions`, mirroring
/// `LakeSoulStartupHandler::post_startup`.
async fn attach_session(
    factory: &PgSessionFactory,
    client: &mut MockClient,
    user: &str,
) -> Arc<ConnectionSession> {
    attach_session_with_limits(factory, client, user, crate::limits::Limits::default())
        .await
}

/// The same, with the statement limits a test wants to exercise.
async fn attach_session_with_limits(
    factory: &PgSessionFactory,
    client: &mut MockClient,
    user: &str,
    limits: crate::limits::Limits,
) -> Arc<ConnectionSession> {
    // `default` is the namespace guaranteed by the metadata schema init;
    // PG maps the database parameter to a LakeSoul namespace.
    let session = factory
        .create_session(
            SessionIdentity {
                user: user.to_string(),
                database: "default".to_string(),
            },
            &SessionSettings::default(),
        )
        .await
        .expect("create_session");
    // The production service runs the crate's statement hooks; the mock must
    // behave like the real connection, or SQL paths they implement (SQL
    // `CLOSE`, `SET`, transactions) diverge from what tests exercise.
    let service = Arc::new(DfSessionService::new_with_hooks(
        Arc::clone(&session.context),
        crate::server::connection_statement_hooks(),
    ));
    let server = crate::limits::ServerLimits::new(limits);
    let state = Arc::new(ConnectionSession {
        session,
        service,
        cancellation: Arc::new(crate::cancel::QueryCancellation::new(
            Arc::clone(&server),
            user,
        )),
        permit: server
            .admit_connection()
            .expect("the test server admits a connection"),
    });
    client.session_extensions().insert(ConnectionSession {
        session: Arc::clone(&state.session),
        service: Arc::clone(&state.service),
        cancellation: Arc::clone(&state.cancellation),
        // The copy inserted for the client owns a slot of its own: a
        // `ConnectionPermit` is not cloneable, and the mock connection lives
        // as long as the test's client does.
        permit: server
            .admit_connection()
            .expect("the test server admits the inserted connection"),
    });
    state
}

/// A `MockClient` whose transaction status is really tracked.
///
/// `MockClient::set_transaction_status` is a no-op, so neither the transaction
/// hook — which reads the client's status to answer `BEGIN`/`COMMIT` — nor a
/// test can observe a status a batch persisted. pgwire reports a failing
/// statement against the client's status, which is what the tests below
/// observe.
#[derive(Debug)]
struct TrackingClient {
    inner: MockClient,
    transaction_status: TransactionStatus,
}

impl TrackingClient {
    fn new(inner: MockClient) -> Self {
        Self {
            inner,
            transaction_status: TransactionStatus::Idle,
        }
    }
}

impl ClientInfo for TrackingClient {
    fn socket_addr(&self) -> std::net::SocketAddr {
        self.inner.socket_addr()
    }

    fn is_secure(&self) -> bool {
        self.inner.is_secure()
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.inner.protocol_version()
    }

    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.inner.set_protocol_version(version);
    }

    fn pid_and_secret_key(&self) -> (i32, SecretKey) {
        self.inner.pid_and_secret_key()
    }

    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey) {
        self.inner.set_pid_and_secret_key(pid, secret_key);
    }

    fn state(&self) -> PgWireConnectionState {
        self.inner.state()
    }

    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.inner.set_state(new_state);
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    fn set_transaction_status(&mut self, new_status: TransactionStatus) {
        self.transaction_status = new_status;
    }

    fn metadata(&self) -> &HashMap<String, String> {
        self.inner.metadata()
    }

    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        self.inner.metadata_mut()
    }

    fn session_extensions(&self) -> &SessionExtensions {
        self.inner.session_extensions()
    }

    fn sni_server_name(&self) -> Option<&str> {
        self.inner.sni_server_name()
    }

    fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
        self.inner.client_certificates()
    }
}

impl ClientPortalStore for TrackingClient {
    type PortalStore = <MockClient as ClientPortalStore>::PortalStore;

    fn portal_store(&self) -> &Self::PortalStore {
        self.inner.portal_store()
    }
}

impl Sink<PgWireBackendMessage> for TrackingClient {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_ready(cx)
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: PgWireBackendMessage,
    ) -> Result<(), Self::Error> {
        Pin::new(&mut self.get_mut().inner).start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

fn execution_time_zone(context: &SessionContext) -> Option<String> {
    context
        .state()
        .config()
        .options()
        .execution
        .time_zone
        .clone()
}

#[tokio::test]
async fn sessions_are_connection_local() {
    let factory = test_factory().await;
    let session_a = factory
        .create_session(
            SessionIdentity {
                user: "user_a".to_string(),
                database: "default".to_string(),
            },
            &SessionSettings::default(),
        )
        .await
        .expect("create_session A");
    let session_b = factory
        .create_session(
            SessionIdentity {
                user: "user_b".to_string(),
                database: "default".to_string(),
            },
            &SessionSettings::default(),
        )
        .await
        .expect("create_session B");

    assert!(!Arc::ptr_eq(&session_a.context, &session_b.context));
}

#[tokio::test]
async fn simple_query_routing_is_isolated() {
    let factory = test_factory().await;
    let mut client_a = MockClient::new();
    let mut client_b = MockClient::new();
    let state_a = attach_session(&factory, &mut client_a, "user_a").await;
    let state_b = attach_session(&factory, &mut client_b, "user_b").await;

    let router = LakeSoulQueryRouter::new();
    let responses = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client_a,
        "SET datafusion.execution.time_zone = 'America/New_York'",
    )
    .await
    .expect("SET on connection A");
    assert!(!responses.is_empty());

    // Connection A picked up the new time zone...
    assert_eq!(
        execution_time_zone(&state_a.session.context).as_deref(),
        Some("America/New_York")
    );
    // ...while connection B is unaffected.
    assert_eq!(
        execution_time_zone(&state_b.session.context).as_deref(),
        Some(DEFAULT_TIME_ZONE)
    );
}

#[tokio::test]
async fn extended_protocol_uses_connection_local_context() {
    let factory = test_factory().await;
    let mut client_a = MockClient::new();
    let mut client_b = MockClient::new();
    let state_a = attach_session(&factory, &mut client_a, "user_a").await;
    let state_b = attach_session(&factory, &mut client_b, "user_b").await;

    let router = LakeSoulQueryRouter::new();

    // Parse/Bind/Execute on connection A only.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client_a,
        Parse::new(
            None,
            "SET datafusion.execution.time_zone = 'Asia/Shanghai'".to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse on connection A");

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client_a,
        Bind::new(None, None, vec![], vec![], vec![]),
    )
    .await
    .expect("bind on connection A");

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client_a,
        Execute::new(None, 0),
    )
    .await
    .expect("execute on connection A");

    // Connection A picked up the new time zone through the extended protocol,
    // proving both the parser and the executed context are connection-local.
    assert_eq!(
        execution_time_zone(&state_a.session.context).as_deref(),
        Some("Asia/Shanghai")
    );
    // Connection B is unaffected.
    assert_eq!(
        execution_time_zone(&state_b.session.context).as_deref(),
        Some(DEFAULT_TIME_ZONE)
    );
}

#[tokio::test]
async fn query_without_session_fails_fatal() {
    let factory = test_factory().await;
    // Factory exists but the (mock) connection never went through startup.
    let _ = factory;

    let router = LakeSoulQueryRouter::new();
    let mut client = MockClient::new();
    let result = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        "SELECT 1",
    )
    .await;
    assert!(result.is_err(), "query must fail before startup");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("session not initialized"), "{err}");
}

#[tokio::test]
async fn connection_close_releases_session() {
    let factory = test_factory().await;
    let session = factory
        .create_session(
            SessionIdentity {
                user: "user_a".to_string(),
                database: "default".to_string(),
            },
            &SessionSettings::default(),
        )
        .await
        .expect("create_session");
    let context = Arc::clone(&session.context);
    let baseline = Arc::strong_count(&context);

    let client = MockClient::new();
    let service = Arc::new(DfSessionService::new(Arc::clone(&session.context)));
    let server = crate::limits::ServerLimits::new(crate::limits::Limits::default());
    client.session_extensions().insert(ConnectionSession {
        session,
        service,
        cancellation: Arc::new(crate::cancel::QueryCancellation::new(
            Arc::clone(&server),
            "user_a",
        )),
        permit: server
            .admit_connection()
            .expect("the test server admits a connection"),
    });
    assert!(Arc::strong_count(&context) > baseline);

    // Dropping the client releases its SessionExtensions, which drops the
    // connection-local state: both the `ConnectionSession` and the `PgSession`
    // (which holds its own `Arc<SessionContext>` reference) are released.
    drop(client);
    assert_eq!(Arc::strong_count(&context), baseline - 1);
}

#[tokio::test]
async fn startup_handler_installs_session() {
    let factory = test_factory().await;
    let handler = LakeSoulStartupHandler::new(
        Arc::clone(&factory),
        Arc::new(crate::cancel::CancelRegistry::new()),
        Arc::new(pgwire::api::ConnectionManager::new()),
        crate::limits::ServerLimits::new(crate::limits::Limits::default()),
    );
    let mut client = MockClient::new();
    client
        .metadata_mut()
        .insert(METADATA_USER.to_string(), "lakesoul_user".to_string());
    client
        .metadata_mut()
        .insert(METADATA_DATABASE.to_string(), "default".to_string());

    let message = PgWireFrontendMessage::Sync(PgSync::new());
    NoopStartupHandler::post_startup(&handler, &mut client, message)
        .await
        .expect("post_startup");

    let state = client
        .session_extensions()
        .get::<ConnectionSession>()
        .expect("connection session installed");
    assert_eq!(state.session.identity.user, "lakesoul_user");
    assert_eq!(state.session.identity.database, "default");
}

#[tokio::test]
async fn startup_rejects_unsupported_default_isolation() {
    let factory = test_factory().await;
    let handler = LakeSoulStartupHandler::new(
        Arc::clone(&factory),
        Arc::new(crate::cancel::CancelRegistry::new()),
        Arc::new(pgwire::api::ConnectionManager::new()),
        crate::limits::ServerLimits::new(crate::limits::Limits::default()),
    );
    let sync = || PgWireFrontendMessage::Sync(PgSync::new());

    // A startup GUC that promises a stronger isolation level is refused
    // before any session is created, so the client never believes the
    // connection honors it.
    let mut client = MockClient::new();
    client.metadata_mut().insert(
        "default_transaction_isolation".to_string(),
        "serializable".to_string(),
    );
    let error = NoopStartupHandler::post_startup(&handler, &mut client, sync())
        .await
        .expect_err("conflicting startup GUC must fail the connection");
    let info = match error {
        pgwire::error::PgWireError::UserError(info) => info,
        other => panic!("unexpected error: {other:?}"),
    };
    assert_eq!(info.severity, "FATAL");
    assert_eq!(info.code, "0A000");

    // The same request hidden inside libpq's `options` string must not
    // slip past the startup check either.
    let mut client = MockClient::new();
    client.metadata_mut().insert(
        "options".to_string(),
        "-c statement_timeout=5 -c default_transaction_isolation=2".to_string(),
    );
    let error = NoopStartupHandler::post_startup(&handler, &mut client, sync())
        .await
        .expect_err("conflicting libpq option must fail the connection");
    let info = match error {
        pgwire::error::PgWireError::UserError(info) => info,
        other => panic!("unexpected error: {other:?}"),
    };
    assert_eq!(info.severity, "FATAL");
    assert_eq!(info.code, "0A000");

    // The server's own default stays connectable.
    let mut client = MockClient::new();
    client.metadata_mut().insert(
        "default_transaction_isolation".to_string(),
        "read committed".to_string(),
    );
    client
        .metadata_mut()
        .insert(METADATA_USER.to_string(), "lakesoul_user".to_string());
    client
        .metadata_mut()
        .insert(METADATA_DATABASE.to_string(), "default".to_string());
    NoopStartupHandler::post_startup(&handler, &mut client, sync())
        .await
        .expect("read committed default must be accepted");
}

#[tokio::test]
async fn session_scopes_namespaces_as_databases() {
    let factory = test_factory().await;
    let session = factory
        .create_session(
            SessionIdentity {
                user: "user_a".to_string(),
                database: "default".to_string(),
            },
            &SessionSettings::default(),
        )
        .await
        .expect("create_session");

    let meta = MetaDataClient::from_env().await.expect("metadata client");
    let expected: std::collections::BTreeSet<String> = meta
        .get_all_namespace()
        .await
        .expect("namespaces")
        .into_iter()
        .map(|namespace| namespace.namespace)
        .collect();
    let state = session.context.state();
    let catalog_list = state.catalog_list();
    let listed = catalog_list.catalog_names();
    assert_eq!(listed.len(), expected.len());
    for namespace in &expected {
        assert!(listed.contains(namespace), "{listed:?} missing {namespace}");
    }

    // The current database is backed by the real catalog with pg_catalog
    // installed; other databases resolve to empty markers.
    assert_eq!(state.config().options().catalog.default_catalog, "default");
    assert_eq!(state.config().options().catalog.default_schema, "public");
    let current = catalog_list.catalog("default").expect("current catalog");
    assert!(current.schema("public").is_some());
    assert!(current.schema("pg_catalog").is_some());
    assert_eq!(current.schema_names(), vec!["pg_catalog", "public"]);

    let others: Vec<_> = listed
        .iter()
        .filter(|name| *name != "default")
        .filter_map(|name| catalog_list.catalog(name))
        .collect();
    for marker in others {
        assert!(marker.schema_names().is_empty());
    }
}

/// psql's `\d` sends the relation oid as a quoted literal
/// (`pg_relation_is_publishable('16495')`), which the upstream oid-only
/// signature cannot coerce.
#[tokio::test]
async fn publishable_shim_accepts_quoted_oid() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    for sql in [
        "SELECT pg_catalog.pg_relation_is_publishable('16495')",
        "SELECT pg_catalog.pg_relation_is_publishable(16495)",
    ] {
        let responses = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
            &router,
            &mut client,
            sql,
        )
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"));
        assert!(!responses.is_empty(), "{sql}");
    }
}

#[tokio::test]
async fn connections_share_the_factory_catalog_snapshot() {
    let factory = test_factory().await;
    let session = factory
        .create_session(
            SessionIdentity {
                user: "user_a".to_string(),
                database: "default".to_string(),
            },
            &SessionSettings::default(),
        )
        .await
        .expect("create_session");

    assert!(
        Arc::ptr_eq(session.catalog_snapshot(), factory.catalog_snapshot()),
        "connections must share the factory metadata view"
    );
}
/// A statement the server is cancelled out of before it produces any row.
#[tokio::test]
async fn a_cancelled_statement_produces_no_rows_and_does_not_leak() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    let mut responses = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        "select 42",
    )
    .await
    .expect("plan the statement");
    // The cancel request arrives after the statement started and before the
    // client reads any row - the case the upstream service cannot handle,
    // because it only guards plan construction.
    assert!(state.cancellation.cancel(), "a statement was in flight");

    let mut stream = query_rows(&mut responses);
    match futures::StreamExt::next(&mut stream).await {
        Some(Err(PgWireError::UserError(info))) => {
            assert_eq!(info.code, "57014");
            assert!(info.message.contains("user request"), "{}", info.message);
        }
        other => panic!("a cancelled statement must report 57014, got {other:?}"),
    }

    // The next statement on the same connection is not affected: cancellation
    // applies to the statement that was in flight.
    let mut responses = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        "select 1",
    )
    .await
    .expect("plan the next statement");
    let mut stream = query_rows(&mut responses);
    assert!(
        futures::StreamExt::next(&mut stream).await.is_some(),
        "the connection keeps working after a cancelled statement"
    );
}

/// A statement that outlives `statement_timeout` is interrupted with `57014`.
///
/// Either enforcement point may report it (the upstream service bounds plan
/// construction, this adapter bounds the rows while they stream); what the
/// client observes is the same, and that is what the test pins.
#[tokio::test]
async fn a_statement_timeout_interrupts_a_slow_statement() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    client.metadata_mut().insert(
        crate::cancel::STATEMENT_TIMEOUT_KEY.to_string(),
        "1".to_string(),
    );
    let router = LakeSoulQueryRouter::new();

    let error = match <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        "select count(*) from pg_catalog.pg_class a, pg_catalog.pg_class b, pg_catalog.pg_class c",
    )
    .await
    {
        Err(error) => error,
        Ok(mut responses) => {
            let mut stream = query_rows(&mut responses);
            // Let the deadline pass before the first row is read: reading is
            // what the deadline bounds here, and sleeping keeps the test free
            // of timing guesswork about how long planning took.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            futures::StreamExt::next(&mut stream)
                .await
                .expect("a query response yields at least one item")
                .expect_err("a statement past its deadline must not yield rows")
        }
    };
    match error {
        PgWireError::UserError(info) => {
            assert_eq!(info.code, "57014");
            assert!(
                info.message.contains("statement timeout"),
                "{}",
                info.message
            );
        }
        other => panic!("a timed out statement must report 57014, got {other:?}"),
    }
}

/// A `SET statement_timeout` in the same batch applies to the statements that
/// follow it: the hook writes the timeout into the connection metadata, and the
/// next statement's deadline comes from there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_set_statement_timeout_bounds_the_next_statement() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    // The statement after the `SET` streams from a source whose size the test
    // controls: how long a scan of `pg_catalog` streams depends on the
    // environment, and the 1ms deadline must be what ends this one. The
    // runtime is multi-threaded because the deadline has to be polled while
    // the rows are being sent.
    let error = router
        .run_statements(
            &mut client,
            "SET statement_timeout = '1ms'; \
             select x from generate_series(1, 200000) as g(x)"
                .to_string(),
        )
        .await
        .expect_err("the statement after the SET must be timed out");
    match error {
        PgWireError::UserError(info) => {
            assert_eq!(info.code, "57014");
            assert!(
                info.message.contains("statement timeout"),
                "{}",
                info.message
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

/// Planning happens at Parse, so that phase is its own bounded execution:
/// while it runs the statement is the connection's current one, and when it
/// returns nothing is in flight any more.
#[tokio::test]
async fn parse_is_a_bounded_execution_of_its_own() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(None, "select 42".to_string(), vec![]),
    )
    .await
    .expect("parse");

    assert!(
        !state.cancellation.cancel(),
        "a finished Parse leaves no statement to cancel"
    );
}

/// A cancel request during a batch hits the statement that is being executed
/// and sent, not the last statement of the batch, and the statements after it
/// never run.
///
/// Needs a multi-thread runtime: sending a ready row stream never yields, so on
/// a current-thread runtime the canceller would only run once the statement is
/// done (the server itself runs on a multi-thread runtime).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_during_a_batch_hits_the_statement_being_sent() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let cancellation = Arc::clone(&state.cancellation);
    let router = Arc::new(LakeSoulQueryRouter::new());

    // A streaming source with a row count the test controls: how long a scan of
    // `pg_catalog` streams depends on the environment (an empty local database
    // has a handful of rows, CI may have preset data). The session is
    // read-only, so the batch selects from the `generate_series` table function
    // instead of creating a table. The first statement streams for long enough
    // to be cancelled while its rows are being sent; the `SET` that follows it
    // must never run.
    let batch = "select x from generate_series(1, 200000) as g(x); \
                 set statement_timeout = '7s'";
    let runner = Arc::clone(&router);
    let batch_task = tokio::spawn(async move {
        let result = <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
            runner.as_ref(),
            &mut client,
            Query::new(batch.to_string()),
        )
        .await;
        (result, client)
    });

    // Poll until the first statement is in flight instead of sleeping a fixed
    // delay: how fast the session initializes varies, and a fixed wait could
    // land before the statement is registered at all.
    let mut cancelled = false;
    for _ in 0..500 {
        if cancellation.cancel() {
            cancelled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(cancelled, "the first statement is in flight");
    let (result, client) = batch_task.await.expect("batch task");

    let error = result.expect_err("a cancelled statement fails the batch");
    match error {
        PgWireError::UserError(info) => {
            assert_eq!(info.code, "57014");
            assert!(info.message.contains("user request"), "{}", info.message);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        client.metadata().get(crate::cancel::STATEMENT_TIMEOUT_KEY),
        None,
        "the statements after the cancelled one must not have run"
    );
}

/// The `Execute` of a portal runs one statement: the one the boundary started,
/// and it is over when the Execute returns.
#[tokio::test]
async fn an_execute_leaves_no_statement_in_flight() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(None, "select 1".to_string(), vec![]),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(None, None, vec![], vec![], vec![]),
    )
    .await
    .expect("bind");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(None, 0),
    )
    .await
    .expect("execute");

    assert!(
        !state.cancellation.cancel(),
        "the Execute is over: nothing is in flight, and the statement it ran \
         must not be left registered as current"
    );
}

/// An `Execute` of a statement that answers without rows (`SET`, a transaction
/// command) ends the statement it ran: a named portal that keeps such a
/// statement open must not hold the connection's statement slot, or a single
/// prepared `SET` would block every later statement of that connection.
#[tokio::test]
async fn an_execute_without_rows_releases_its_slot() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session_with_limits(
        &factory,
        &mut client,
        "user_a",
        Limits {
            max_queries_per_user: 1,
            ..Limits::default()
        },
    )
    .await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(
            Some("p".to_string()),
            "set statement_timeout = '7s'".to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(
            Some("p".to_string()),
            Some("p".to_string()),
            vec![],
            vec![],
            vec![],
        ),
    )
    .await
    .expect("bind");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(Some("p".to_string()), 0),
    )
    .await
    .expect("execute");

    assert!(
        client
            .metadata()
            .get(crate::cancel::STATEMENT_TIMEOUT_KEY)
            .is_some(),
        "the SET ran"
    );
    assert!(
        state.cancellation.begin_statement(None, None).is_ok(),
        "a statement that answered without rows has released its slot"
    );
}

/// An `Execute` that the limit ends must release the portal: a client that does
/// not close it afterwards must not leave the execution - and its distributed
/// stages - alive behind the response the portal still held.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupted_execute_releases_its_portal() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    client.metadata_mut().insert(
        crate::cancel::STATEMENT_TIMEOUT_KEY.to_string(),
        "100".to_string(),
    );
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(
            Some("p".to_string()),
            // Streams for far longer than the deadline, and yields between
            // batches, so the deadline is what ends it.
            "select * from pg_catalog.pg_class a, pg_catalog.pg_class b, \
             pg_catalog.pg_class c"
                .to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(
            Some("p".to_string()),
            Some("p".to_string()),
            vec![],
            vec![],
            vec![],
        ),
    )
    .await
    .expect("bind");

    let error = <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(Some("p".to_string()), 0),
    )
    .await
    .expect_err("the deadline must end this Execute");
    assert!(
        matches!(error, PgWireError::UserError(ref info) if info.code == "57014"),
        "unexpected error: {error:?}"
    );
    assert!(
        client.portal_store().get_portal("p").is_none(),
        "an interrupted Execute must not leave the portal holding its stream"
    );
}

/// A suspended portal still holds rows, so it keeps the statement's slot: a
/// client cannot park results and run more statements than its limit allows.
#[tokio::test]
async fn a_suspended_portal_holds_its_slot() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session_with_limits(
        &factory,
        &mut client,
        "user_a",
        crate::limits::Limits {
            max_queries_per_user: 1,
            ..Default::default()
        },
    )
    .await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(
            Some("s".to_string()),
            "select * from pg_catalog.pg_class".to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(
            Some("p".to_string()),
            Some("s".to_string()),
            vec![],
            vec![],
            vec![],
        ),
    )
    .await
    .expect("bind");
    // One row suspends the portal, with the rest of its result still to fetch.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(Some("p".to_string()), 1),
    )
    .await
    .expect("first fetch");
    assert_eq!(sent_data_rows(&client), 1);

    let refused = <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(None, "select 1".to_string(), vec![]),
    )
    .await
    .expect_err("the suspended portal still holds the user's only slot");
    match refused {
        PgWireError::UserError(info) => assert_eq!(info.code, "53400"),
        other => panic!("unexpected error: {other:?}"),
    }

    // Closing the portal ends the statement and returns the slot.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_close(
        &router,
        &mut client,
        Close::new(TARGET_TYPE_BYTE_PORTAL, Some("p".to_string())),
    )
    .await
    .expect("close");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(None, "select 1".to_string(), vec![]),
    )
    .await
    .expect("the slot returns with the portal");

    assert!(!state.cancellation.cancel(), "nothing is in flight");
}

/// A statement the server cancels must give its slot back: otherwise a
/// connection would be stuck at its own limit until it disconnects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_statement_does_not_keep_its_slot() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session_with_limits(
        &factory,
        &mut client,
        "user_a",
        crate::limits::Limits {
            max_queries_per_user: 1,
            ..Default::default()
        },
    )
    .await;
    let cancellation = Arc::clone(&state.cancellation);
    let router = Arc::new(LakeSoulQueryRouter::new());

    // A streaming source whose row count the test controls, for the same reason
    // as above: the environment decides how long a `pg_catalog` scan streams,
    // so the statement must not rely on it.
    let batch = "select x from generate_series(1, 200000) as g(x); \
                 set statement_timeout = '7s'";
    let runner = Arc::clone(&router);
    let batch_task = tokio::spawn(async move {
        let result = <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
            runner.as_ref(),
            &mut client,
            Query::new(batch.to_string()),
        )
        .await;
        (result, client)
    });
    // Poll until the statement is registered: a fixed wait could land before
    // it is, and a cancel with nothing in flight would be discarded.
    let mut cancelled = false;
    for _ in 0..500 {
        if cancellation.cancel() {
            cancelled = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(cancelled, "a statement is in flight");
    let (result, mut client) = batch_task.await.expect("batch task");
    assert!(result.is_err(), "the cancelled statement fails the batch");

    // The slot came back with the cancelled statement.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(None, "select 1".to_string(), vec![]),
    )
    .await
    .expect("the connection is not stuck at its limit");
}

/// An `Execute` for a portal that does not exist must not register anything: a
/// long-lived connection sending such messages must not grow the state it keeps
/// per statement.
#[tokio::test]
async fn an_execute_of_an_unknown_portal_registers_nothing() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    for index in 0..64 {
        let name = format!("missing-{index}");
        <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
            &router,
            &mut client,
            Execute::new(Some(name), 0),
        )
        .await
        .expect_err("the portal does not exist");
    }

    assert_eq!(
        state.cancellation.portal_count(),
        0,
        "no statement may be kept for portals that do not exist"
    );
    assert!(!state.cancellation.cancel());
}

/// A suspended extended-protocol portal is resumed by pgwire itself, without
/// going through this handler; the resumed fetch is a new execution, so it must
/// not be killed by the deadline of the execution that suspended it.
#[tokio::test]
async fn a_suspended_portal_resumes_after_its_deadline_has_passed() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    client.metadata_mut().insert(
        crate::cancel::STATEMENT_TIMEOUT_KEY.to_string(),
        "60".to_string(),
    );
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(
            None,
            "select * from pg_catalog.pg_class limit 200".to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(None, None, vec![], vec![], vec![]),
    )
    .await
    .expect("bind");
    // One row suspends the portal.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(None, 1),
    )
    .await
    .expect("first fetch");
    let first = sent_data_rows(&client);
    assert_eq!(first, 1, "the first fetch returns the requested row");

    // The portal stays suspended past both the idle gap and the deadline.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // Resuming it is a new execution: it returns rows instead of the timeout
    // the suspended execution would have left behind.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(None, 0),
    )
    .await
    .expect("resumed fetch");
    assert!(
        !client
            .sent_messages()
            .iter()
            .any(|message| matches!(message, PgWireBackendMessage::ErrorResponse(_))),
        "a resumed fetch must not inherit the previous execution's deadline"
    );
    assert!(
        sent_data_rows(&client) > first,
        "the resumed fetch must produce the remaining rows"
    );
}

fn sent_data_rows(client: &MockClient) -> usize {
    client
        .sent_messages()
        .iter()
        .filter(|message| matches!(message, PgWireBackendMessage::DataRow(_)))
        .count()
}

/// A batch is bounded statement by statement: the deadline of an earlier
/// statement is captured before a later `SET statement_timeout` clears it, and
/// each statement's rows are read under their own limit.
#[tokio::test]
async fn every_statement_of_a_batch_gets_its_own_deadline() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    client.metadata_mut().insert(
        crate::cancel::STATEMENT_TIMEOUT_KEY.to_string(),
        "1".to_string(),
    );
    let router = LakeSoulQueryRouter::new();

    let mut responses = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        "select 1; set statement_timeout = 0",
    )
    .await
    .expect("plan the batch");
    assert_eq!(responses.len(), 2, "one response per statement");

    // The `SET` cleared the connection's timeout, but the first statement was
    // already bounded by the value in force when it started.
    assert_eq!(
        client.metadata().get(crate::cancel::STATEMENT_TIMEOUT_KEY),
        None,
        "the SET applied to the connection"
    );
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let mut stream = query_rows(&mut responses);
    match futures::StreamExt::next(&mut stream).await {
        Some(Err(PgWireError::UserError(info))) => {
            assert_eq!(info.code, "57014");
            assert!(
                info.message.contains("statement timeout"),
                "{}",
                info.message
            );
        }
        other => panic!("the first statement kept its own deadline, got {other:?}"),
    }
}

/// The transaction hook reads the client's status once per statement, so every
/// statement's transitions must reach the client before the next statement of
/// the batch runs.
///
/// - `ROLLBACK; SELECT 1` starting from the aborted status must execute the
///   SELECT: the hook gates statements against the status it reads, and a
///   stale pre-ROLLBACK status would reject the SELECT with `25P01`.
/// - `COMMIT; BEGIN` must end the batch inside the transaction the `BEGIN`
///   opened: a stale post-COMMIT status would make the hook treat the BEGIN as
///   a nested one and begin nothing.
///
/// The test starts from the status a real server's error path leaves behind
/// (pgwire derives it from the client's status after reporting a failed
/// statement), because the router is driven directly here.
#[tokio::test]
async fn a_batch_persists_transaction_transitions_between_statements() {
    let factory = test_factory().await;
    let mut mock = MockClient::new();
    attach_session(&factory, &mut mock, "user_a").await;
    let mut client = TrackingClient::new(mock);
    let router = LakeSoulQueryRouter::new();

    // What pgwire's error handling leaves behind after a failed statement: the
    // current status derived to its error state.
    client.set_transaction_status(TransactionStatus::Error);

    <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        &router,
        &mut client,
        Query::new("rollback; select 1".to_string()),
    )
    .await
    .expect("the ROLLBACK must end the aborted transaction and let SELECT run");
    assert_eq!(
        client.transaction_status(),
        TransactionStatus::Idle,
        "ROLLBACK; SELECT 1 leaves the connection idle"
    );

    <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        &router,
        &mut client,
        Query::new("commit; begin".to_string()),
    )
    .await
    .expect("commit ends nothing and begin opens a transaction");
    assert_eq!(
        client.transaction_status(),
        TransactionStatus::Transaction,
        "the batch's BEGIN opened a transaction"
    );
}

/// pgwire reports a failing statement against the client's transaction
/// status, so the status the statements that already ran left behind must
/// reach the client before the error is reported: an error after a `COMMIT`
/// in the same batch must not resurrect the transaction the `COMMIT` ended
/// as failed.
#[tokio::test]
async fn a_failed_statement_reports_the_status_the_batch_left_behind() {
    let factory = test_factory().await;
    let mut mock = MockClient::new();
    attach_session(&factory, &mut mock, "user_a").await;
    let mut client = TrackingClient::new(mock);
    let router = LakeSoulQueryRouter::new();

    // The connection enters the next batch inside a transaction; the
    // transaction hook answers `BEGIN` from the client's status, so that
    // status must really be there.
    <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        &router,
        &mut client,
        Query::new("begin".to_string()),
    )
    .await
    .expect("begin");
    assert_eq!(
        client.transaction_status(),
        TransactionStatus::Transaction,
        "the batch's status reaches the client when the batch is over"
    );

    // The `COMMIT` ends the transaction, the missing table fails the batch.
    <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        &router,
        &mut client,
        Query::new("commit; select * from missing_table_xyz".to_string()),
    )
    .await
    .expect_err("the missing table fails the batch");
    assert_eq!(
        client.transaction_status(),
        TransactionStatus::Idle,
        "the `COMMIT`'s status must reach the client before the error is reported"
    );
}

/// SQL `CLOSE` removes a portal without a protocol `Close` message: the
/// upstream cursor hook only drops the portal from pgwire's store, so the
/// cleanup hook must forget the statement the closed portal kept in the
/// cancellation map.
#[tokio::test]
async fn a_sql_close_forgots_the_portal_s_statement() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(Some("stmt".to_string()), "select 1".to_string(), vec![]),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(
            Some("p".to_string()),
            Some("stmt".to_string()),
            vec![],
            vec![],
            vec![],
        ),
    )
    .await
    .expect("bind");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(Some("p".to_string()), 0),
    )
    .await
    .expect("execute");
    assert_eq!(
        state.cancellation.portal_count(),
        1,
        "the executed portal keeps its statement"
    );

    <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        &router,
        &mut client,
        Query::new("close p".to_string()),
    )
    .await
    .expect("close");
    assert_eq!(
        state.cancellation.portal_count(),
        0,
        "the SQL close must forget the portal's statement"
    );
}

/// `CLOSE ALL` covers every portal: none of them may keep a statement.
#[tokio::test]
async fn a_sql_close_all_forgots_every_portal_s_statement() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(Some("stmt".to_string()), "select 1".to_string(), vec![]),
    )
    .await
    .expect("parse");
    for portal in ["p1", "p2"] {
        <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
            &router,
            &mut client,
            Bind::new(
                Some(portal.to_string()),
                Some("stmt".to_string()),
                vec![],
                vec![],
                vec![],
            ),
        )
        .await
        .expect("bind");
        <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
            &router,
            &mut client,
            Execute::new(Some(portal.to_string()), 0),
        )
        .await
        .expect("execute");
    }
    assert_eq!(
        state.cancellation.portal_count(),
        2,
        "both portals keep their statements"
    );

    <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        &router,
        &mut client,
        Query::new("close all".to_string()),
    )
    .await
    .expect("close all");
    assert_eq!(
        state.cancellation.portal_count(),
        0,
        "CLOSE ALL must forget every portal's statement"
    );
}

/// A portal's result is what its current `Bind` fetches: re-binding the name
/// starts a new result, and the rows the previous one produced are not charged
/// against it. Without the bind boundary the second result would be refused by
/// a limit the first one had already spent.
#[tokio::test]
async fn rebinding_a_portal_starts_the_result_limit_over() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session_with_limits(
        &factory,
        &mut client,
        "user_a",
        Limits {
            max_result_rows: 1,
            ..Limits::default()
        },
    )
    .await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(
            Some("stmt".to_string()),
            "select x from generate_series(1, 1) as g(x)".to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse");

    for fetch in 1..=2 {
        <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
            &router,
            &mut client,
            Bind::new(
                Some("p".to_string()),
                Some("stmt".to_string()),
                vec![],
                vec![],
                vec![],
            ),
        )
        .await
        .expect("bind");
        <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
            &router,
            &mut client,
            Execute::new(Some("p".to_string()), 0),
        )
        .await
        .unwrap_or_else(|error| {
            panic!("fetch {fetch} must stay inside the result limit: {error}")
        });
    }
    assert_eq!(
        state.cancellation.portal_count(),
        1,
        "the last binding is the one the portal keeps"
    );
}

/// Replacing a suspended portal under the same name gives its slot back: a
/// `Bind` drops the result the old portal was fetching, so the statement
/// behind it is over even if the replacement is never executed.
#[tokio::test]
async fn rebinding_a_suspended_portal_releases_its_slot() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session_with_limits(
        &factory,
        &mut client,
        "user_a",
        Limits {
            max_queries_per_user: 1,
            ..Limits::default()
        },
    )
    .await;
    let router = LakeSoulQueryRouter::new();

    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(
            Some("stmt".to_string()),
            "select x from generate_series(1, 10) as g(x)".to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(
            Some("p".to_string()),
            Some("stmt".to_string()),
            vec![],
            vec![],
            vec![],
        ),
    )
    .await
    .expect("bind");
    // One row per fetch: the portal suspends with the rest of its result
    // still to fetch, and keeps the connection's only slot.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(Some("p".to_string()), 1),
    )
    .await
    .expect("execute");
    assert!(
        state.cancellation.begin_statement(None, None).is_err(),
        "the suspended portal holds the slot"
    );

    // The same name bound again replaces the portal: the replaced statement
    // ends and its slot returns.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(
            Some("p".to_string()),
            Some("stmt".to_string()),
            vec![],
            vec![],
            vec![],
        ),
    )
    .await
    .expect("rebind");
    assert!(
        state.cancellation.begin_statement(None, None).is_ok(),
        "the replaced portal must not keep holding the slot"
    );
}

/// A cancel request received while the untrusted query text is still being
/// parsed must reach the statement it targets: the batch's current execution
/// is created before `split_statements` parses the text, so the request is
/// not discarded and no statement of the batch runs on its behalf.
///
/// Needs a multi-thread runtime: the batch task must be parsing while the
/// canceller runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_received_while_parsing_stops_the_whole_batch() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let cancellation = Arc::clone(&state.cancellation);
    let router = Arc::new(LakeSoulQueryRouter::new());

    // A batch whose text takes far longer to parse than any of its statements
    // takes to run: the first statement is trivial, so a cancel that reaches
    // the connection within the parse can only be answered by the
    // registration that precedes the parse.
    let batch = "select 1; ".repeat(60_000) + "select 2";
    let runner = Arc::clone(&router);
    let batch_task = tokio::spawn(async move {
        let result = <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
            runner.as_ref(),
            &mut client,
            Query::new(batch),
        )
        .await;
        (result, client)
    });

    // The statement is registered before parsing starts, so this lands while
    // the parse is still running.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        cancellation.cancel(),
        "the batch must have a statement in flight while it parses"
    );
    let (result, client) = batch_task.await.expect("batch task");

    let error = result.expect_err("a batch cancelled during its parse fails");
    match error {
        PgWireError::UserError(info) => {
            assert_eq!(info.code, "57014");
            assert!(info.message.contains("user request"), "{}", info.message);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        sent_data_rows(&client),
        0,
        "no statement of a batch cancelled during its parse may produce rows"
    );
}

/// The same parse-window cancel as above, but the first statement is a `SET`,
/// which completes without producing rows: the pre-fired limit must stop it
/// before its side effect reaches the session, not merely fail the batch
/// afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_received_while_parsing_prevents_the_first_set_from_taking_effect() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let cancellation = Arc::clone(&state.cancellation);
    let router = Arc::new(LakeSoulQueryRouter::new());

    // The first statement is the side effect; the text after it only makes
    // the parse outlast the cancel, so the token fires before the `SET` runs.
    let batch = format!(
        "set statement_timeout = '99s'; {}",
        "select 1; ".repeat(60_000)
    );
    let runner = Arc::clone(&router);
    let batch_task = tokio::spawn(async move {
        let result = <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
            runner.as_ref(),
            &mut client,
            Query::new(batch),
        )
        .await;
        (result, client)
    });

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        cancellation.cancel(),
        "the batch must have a statement in flight while it parses"
    );
    let (result, client) = batch_task.await.expect("batch task");

    let error = result.expect_err("a batch cancelled during its parse fails");
    match error {
        PgWireError::UserError(info) => {
            assert_eq!(info.code, "57014");
            assert!(info.message.contains("user request"), "{}", info.message);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        client.metadata().get(crate::cancel::STATEMENT_TIMEOUT_KEY),
        None,
        "the cancelled statement's side effect must not have taken effect"
    );
}

/// The registration covers reading the text, not only parsing it: a batch that
/// opens with a long run of whitespace is scanned (`is_empty_query`) before its
/// first statement exists, and a cancel request that arrives during that scan
/// must not be discarded - the `set` at the end must not take effect.
///
/// The prefix is sized so that the scan, not the parse, is what is still
/// running when the cancel below arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_received_while_the_text_is_scanned_prevents_the_first_set() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let cancellation = Arc::clone(&state.cancellation);
    let router = Arc::new(LakeSoulQueryRouter::new());

    // Whitespace before the `SET` and nothing else: scanning it costs time,
    // and the statement is at the end, where the scan finds it.
    let batch = format!("{}set statement_timeout = '99s'", " ".repeat(8_000_000));
    let runner = Arc::clone(&router);
    let batch_task = tokio::spawn(async move {
        let result = <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
            runner.as_ref(),
            &mut client,
            Query::new(batch),
        )
        .await;
        (result, client)
    });

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        cancellation.cancel(),
        "the batch must have a statement in flight while its text is scanned"
    );
    let (result, client) = batch_task.await.expect("batch task");

    let error = result.expect_err("a batch cancelled during its scan fails");
    match error {
        PgWireError::UserError(info) => {
            assert_eq!(info.code, "57014");
            assert!(info.message.contains("user request"), "{}", info.message);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(
        client.metadata().get(crate::cancel::STATEMENT_TIMEOUT_KEY),
        None,
        "the cancelled statement's side effect must not have taken effect"
    );
}

/// A batch that carries no statement at all - only separators - registers a
/// statement before its text is read and gives it up again: it answers the
/// empty query and leaves the connection's slot free for what runs next.
#[tokio::test]
async fn a_batch_with_no_statement_gives_its_registration_back() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = Arc::new(LakeSoulQueryRouter::new());

    <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        router.as_ref(),
        &mut client,
        Query::new(";;  ;".to_string()),
    )
    .await
    .expect("a batch with no statement answers");

    assert!(
        client.sent_messages().iter().any(|message| matches!(
            message,
            PgWireBackendMessage::EmptyQueryResponse(_)
        )),
        "an empty query is answered as one"
    );
    assert_eq!(sent_data_rows(&client), 0);
    assert!(
        state.cancellation.begin_statement(None, None).is_ok(),
        "the registration the batch took before reading its text is given back"
    );
}

/// A request that carries no statement runs no work, so the statement quota
/// must not turn its empty answer into an error: with the user's only slot held
/// by a portal suspended with rows still to fetch, an empty query still answers
/// as one - and a statement still reports the refusal.
#[tokio::test]
async fn an_empty_query_is_not_refused_by_the_statement_quota() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session_with_limits(
        &factory,
        &mut client,
        "user_a",
        crate::limits::Limits {
            max_queries_per_user: 1,
            ..Default::default()
        },
    )
    .await;
    let router = LakeSoulQueryRouter::new();

    // The user's only slot: a portal suspended with rows still to fetch.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(
            Some("s".to_string()),
            "select * from pg_catalog.pg_class".to_string(),
            vec![],
        ),
    )
    .await
    .expect("parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(
            Some("p".to_string()),
            Some("s".to_string()),
            vec![],
            vec![],
            vec![],
        ),
    )
    .await
    .expect("bind");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(Some("p".to_string()), 1),
    )
    .await
    .expect("first fetch");
    assert_eq!(sent_data_rows(&client), 1);

    for empty in ["", ";", "  ;  ", "  \n\t  "] {
        let before = client.sent_messages().len();
        <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
            &router,
            &mut client,
            Query::new(empty.to_string()),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "{empty:?} carries no statement, so the quota cannot refuse it: {error:?}"
            )
        });
        let sent = &client.sent_messages()[before..];
        assert!(
            sent.iter().any(|message| matches!(
                message,
                PgWireBackendMessage::EmptyQueryResponse(_)
            )),
            "{empty:?} is answered as an empty query: {sent:?}"
        );
        assert_eq!(sent_data_rows(&client), 1, "no statement ran");
    }

    // The empty queries neither took a slot of their own nor gave the held one
    // back: a statement still reports the refusal.
    let refused = <LakeSoulQueryRouter as SimpleQueryHandler>::on_query(
        &router,
        &mut client,
        Query::new("select 1".to_string()),
    )
    .await
    .expect_err("a statement is refused while the only slot is held");
    match refused {
        PgWireError::UserError(info) => assert_eq!(info.code, "53400"),
        other => panic!("unexpected error: {other:?}"),
    }
}

/// A cancel request reaches the connection it addresses - and only that one.
#[tokio::test]
async fn a_cancel_request_reaches_the_connection_it_addresses() {
    let factory = test_factory().await;
    let registry = Arc::new(crate::cancel::CancelRegistry::new());
    let handler = Arc::new(super::LakeSoulStartupHandler::new(
        Arc::clone(&factory),
        Arc::clone(&registry),
        Arc::new(pgwire::api::ConnectionManager::new()),
        crate::limits::ServerLimits::new(crate::limits::Limits::default()),
    ));
    let mut client = MockClient::new();
    client
        .metadata_mut()
        .insert(METADATA_USER.to_string(), "user_a".to_string());
    client
        .metadata_mut()
        .insert(METADATA_DATABASE.to_string(), "default".to_string());
    let message = PgWireFrontendMessage::Sync(PgSync::new());
    NoopStartupHandler::post_startup(handler.as_ref(), &mut client, message)
        .await
        .expect("install the connection");
    let (pid, secret_key) = client.pid_and_secret_key();

    let state = client
        .session_extensions()
        .get::<ConnectionSession>()
        .expect("connection session");
    let execution = state
        .cancellation
        .begin_statement(None, None)
        .expect("statement admitted");
    let token = execution.token();
    assert!(
        !registry.cancel(pid, b"a-different-key"),
        "a wrong secret key must not cancel anything"
    );
    assert!(
        !token.is_cancelled(),
        "a cancel request with the wrong key must not reach the statement"
    );
    assert!(
        registry.cancel(pid, &secret_key.to_bytes()),
        "the connection is registered under its own pid and key"
    );
    assert!(
        token.is_cancelled(),
        "a cancel request reaches the statement of the connection it addresses"
    );
}

fn query_rows(responses: &mut [Response]) -> &mut SendableRowStream {
    match responses.first_mut().expect("one response") {
        Response::Query(query) => &mut query.data_rows,
        other => panic!("expected a query response, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// pgvector
// ---------------------------------------------------------------------------

/// The `FLOAT[]` value [`setup_vector_table`] writes for `value`; the SQL
/// insert computes the same expression.
fn generated_vector(value: i64) -> [f32; 8] {
    [
        (value as f64).sin() as f32,
        (value as f64).cos() as f32,
        // Same arithmetic as the insert: f64, then narrowed to the stored
        // Float32 (an f32 product would round differently).
        (value as f64 * 0.1) as f32,
        0.5,
        -0.5,
        1.0,
        -1.0,
        // Strictly increasing in `value`: the sin/cos and `value * 0.1`
        // terms are symmetric around a query on the corpus, so without this
        // the nearest rows would tie to within f32 noise.
        (value as f64 * 0.01) as f32,
    ]
}

/// The pgvector text form of a vector (`[1,2,3]`).
fn vector_text(values: &[f32]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn vector_norm(values: &[f32]) -> f64 {
    values
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt()
}

fn l2_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(a, b)| (*a as f64 - *b as f64).powi(2))
        .sum::<f64>()
        .sqrt()
}

/// The exact `<->`-nearest rows of the generated corpus.
///
/// Asserts that the corpus separates the requested prefix, so the expected
/// order is unambiguous (the generated vectors are symmetric around the
/// query, so a perturbed query is used).
fn exact_top_k(query: &[f32], k: usize, rows: i64) -> Vec<i64> {
    let mut scored: Vec<(f64, i64)> = (0..rows)
        .map(|id| (l2_distance(&generated_vector(id), query), id))
        .collect();
    scored.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap());
    // Every consecutive pair of the prefix (and the row after it) must be
    // separated, or the expected order is not well defined.
    for window in scored.windows(2).take(k) {
        let gap = window[1].0 - window[0].0;
        assert!(
            gap > 1e-5,
            "the corpus must separate every top-{k} row: {gap} in {scored:?}"
        );
    }
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// A query vector that is not one of the generated rows and is not the
/// mirror point of the corpus: `generated_vector(7)` sits exactly between the
/// `value * 0.1` and `value * 0.01` terms of mirror rows (`i` and `14 - i`),
/// so those components are moved off the midpoint.
fn perturbed_query() -> [f32; 8] {
    let mut query = generated_vector(7);
    query[2] += 0.03;
    query[3] += 0.13;
    query[5] -= 0.07;
    query[7] -= 0.015;
    query
}

/// Create a LakeSoul table with one indexed `FLOAT[]` column and `rows`
/// generated rows; the insert auto-builds the vector index.
async fn setup_vector_table(table: &str, rows: i64) -> Arc<MetaDataClient> {
    let client = Arc::new(
        MetaDataClient::from_env()
            .await
            .expect("PostgreSQL required"),
    );
    let _ = client.drop_table(table, "default").await;
    let location = std::env::current_dir().unwrap().join("default").join(table);
    let _ = std::fs::remove_dir_all(&location);
    let ctx = lakesoul_datafusion::create_lakesoul_session_ctx(
        Arc::clone(&client),
        &lakesoul_datafusion::cli::CoreArgs::from_env(),
    )
    .expect("lakesoul session");
    let config = r#"[{"column":"vec","dim":8,"nlist":4,"total_bits":7,"metric":"L2"}]"#;
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL LOCATION '{}'
         OPTIONS ('vector_index_columns' '{config}', 'hashBucketNum' '4')",
        location.display()
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();
    let insert_sql = format!(
        "INSERT INTO \"lakesoul\".default.{table}
         SELECT CAST(g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value * 0.1, 0.5, -0.5, 1.0, -1.0, g.value * 0.01]
         FROM generate_series(0, {}) AS g(value)",
        rows - 1
    );
    ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();
    client
}

/// Drop a test table and its data directory.
async fn cleanup_vector_table(client: &MetaDataClient, table: &str) {
    let _ = client.drop_table(table, "default").await;
    let _ = std::fs::remove_dir_all(
        std::env::current_dir().unwrap().join("default").join(table),
    );
}

type QueryResult = (Arc<Vec<FieldInfo>>, Vec<Vec<Option<String>>>);

/// Decode a text-format `DataRow`: one `i32` length per cell, `-1` for NULL.
fn decode_text_row(row: &DataRow) -> Vec<Option<String>> {
    let mut cells = Vec::new();
    let mut offset = 0usize;
    while offset + 4 <= row.data.len() {
        let length = i32::from_be_bytes(row.data[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if length < 0 {
            cells.push(None);
            continue;
        }
        let length = length as usize;
        cells.push(Some(
            String::from_utf8_lossy(&row.data[offset..offset + length]).to_string(),
        ));
        offset += length;
    }
    cells
}

/// Run one statement through the router and collect the text rows of every
/// query response, with each response's row description.
async fn run_query(
    router: &LakeSoulQueryRouter,
    client: &mut MockClient,
    sql: &str,
) -> Vec<QueryResult> {
    let responses =
        <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(router, client, sql)
            .await
            .unwrap_or_else(|error| panic!("query failed: {error}\nsql: {sql}"));
    let mut results = Vec::new();
    for response in responses {
        if let Response::Query(mut query) = response {
            let mut rows = Vec::new();
            while let Some(row) = query.data_rows.next().await {
                rows.push(decode_text_row(&row.expect("row")));
            }
            results.push((query.row_schema, rows));
        }
    }
    results
}

/// The first column of every row, in order.
fn first_column(results: &[QueryResult]) -> Vec<Option<String>> {
    results
        .iter()
        .flat_map(|(_, rows)| rows.iter().map(|row| row[0].clone()))
        .collect()
}

/// The concatenated text of an `EXPLAIN` (every column: the plan type and
/// the plan itself).
fn explain_text(results: &[QueryResult]) -> String {
    results
        .iter()
        .flat_map(|(_, rows)| rows.iter())
        .flat_map(|row| row.iter().flatten().cloned())
        .collect::<Vec<_>>()
        .join("\n")
}

fn as_f64(cell: &Option<String>) -> f64 {
    cell.as_deref().expect("value").parse().expect("number")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgvector_distance_operators_use_the_vector_index() {
    let table = "pgvector_ops";
    let meta = setup_vector_table(table, 50).await;
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    let query = generated_vector(7);
    let literal = vector_text(&query);

    // `<->` is the Euclidean distance: the distance of the row it points at
    // is zero, and the indexed top-k matches the exact top-k.
    let sql = format!("SELECT vec <-> '{literal}' FROM {table} WHERE id = 7");
    let rows = run_query(&router, &mut client, &sql).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1.len(), 1, "{rows:?}");
    assert!(as_f64(&rows[0].1[0][0]).abs() < 1e-5, "{rows:?}");

    // The generated corpus is symmetric around row 7, so the top-k assertion
    // uses a perturbed query with well-separated distances.
    let probe = perturbed_query();
    let probe_text = vector_text(&probe);
    let sql = format!("SELECT id FROM {table} ORDER BY vec <-> '{probe_text}' LIMIT 3");
    let ids: Vec<i64> = first_column(&run_query(&router, &mut client, &sql).await)
        .iter()
        .map(|cell| as_f64(cell) as i64)
        .collect();
    assert_eq!(ids, exact_top_k(&probe, 3, 50));

    // `<#>` is the *negative* inner product; `<=>` is the cosine distance.
    let unit = [1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
    let unit_text = vector_text(&unit);
    let stored = generated_vector(7);
    let rows = run_query(
        &router,
        &mut client,
        &format!("SELECT vec <#> '{unit_text}' FROM {table} WHERE id = 7"),
    )
    .await;
    let expected = -(stored[0] as f64);
    assert!(
        (as_f64(&rows[0].1[0][0]) - expected).abs() < 1e-5,
        "{rows:?}"
    );

    let rows = run_query(
        &router,
        &mut client,
        &format!("SELECT vec <=> '{unit_text}' FROM {table} WHERE id = 7"),
    )
    .await;
    let expected = 1.0 - stored[0] as f64 / vector_norm(&stored);
    assert!(
        (as_f64(&rows[0].1[0][0]) - expected).abs() < 1e-5,
        "{rows:?}"
    );

    // The rewrite keeps the vector-index pushdown ...
    let sql = format!("SELECT id FROM {table} ORDER BY vec <-> '{literal}' LIMIT 3");
    let explain = explain_text(
        &run_query(&router, &mut client, &format!("EXPLAIN VERBOSE {sql}")).await,
    );
    assert!(explain.contains("LakeSoulVectorSearchExec"), "{explain}");

    // ... and the session option switches it to index-only ordering.
    run_query(
        &router,
        &mut client,
        "SET lakesoul_vector_search.rerank = 'index'",
    )
    .await;
    let explain = explain_text(
        &run_query(&router, &mut client, &format!("EXPLAIN VERBOSE {sql}")).await,
    );
    assert!(explain.contains("order_by=true"), "{explain}");
    assert!(!explain.contains("SortExec"), "{explain}");

    cleanup_vector_table(&meta, table).await;
}

/// Run one prepared statement with one pgvector parameter (declared the way
/// a driver does: the vector OID in `Parse`, the text form in `Bind`) and
/// return the rows it produced.
async fn run_prepared_vector_query(
    router: &LakeSoulQueryRouter,
    client: &mut MockClient,
    sql: &str,
    parameter: &[f32],
) -> Vec<Vec<Option<String>>> {
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        router,
        client,
        Parse::new(
            None,
            sql.to_string(),
            vec![datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID],
        ),
    )
    .await
    .expect("on_parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        router,
        client,
        Bind::new(
            None,
            None,
            vec![0],
            vec![Some(bytes::Bytes::from(
                vector_text(parameter).into_bytes(),
            ))],
            vec![],
        ),
    )
    .await
    .expect("on_bind");
    let before = client
        .sent_messages()
        .iter()
        .filter(|message| matches!(message, PgWireBackendMessage::DataRow(_)))
        .count();
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        router,
        client,
        Execute::new(None, 0),
    )
    .await
    .expect("on_execute");
    client
        .sent_messages()
        .iter()
        .filter_map(|message| match message {
            PgWireBackendMessage::DataRow(row) => Some(decode_text_row(row)),
            _ => None,
        })
        .skip(before)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgvector_prepared_parameter_binds_a_vector() {
    let table = "pgvector_prepared";
    let meta = setup_vector_table(table, 20).await;
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    let query = generated_vector(9);
    let sql = format!("SELECT id FROM {table} ORDER BY vec <-> $1::vector(8) LIMIT 1");

    // The parameter is reported as the pgvector type, so a driver knows to
    // bind it with its vector adapter.
    let parser = <LakeSoulQueryRouter as ExtendedQueryHandler>::query_parser(&router);
    let statement = parser
        .parse_sql(&client, &sql, &[])
        .await
        .expect("parse")
        .expect("statement");
    let types = parser.get_parameter_types(&statement).expect("types");
    assert_eq!(types.len(), 1, "{types:?}");
    assert_eq!(
        types[0].oid(),
        datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID,
        "{types:?}"
    );

    let rows = run_prepared_vector_query(&router, &mut client, &sql, &query).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(as_f64(&rows[0][0]) as i64, 9);

    // The shape a driver sends when it relies on the server for the
    // dimension: a bare `$1`, whose type and dimension come from the column
    // on the other side of the operator.
    let uncast = format!("SELECT id FROM {table} ORDER BY vec <-> $1 LIMIT 1");
    let statement = parser
        .parse_sql(&client, &uncast, &[])
        .await
        .expect("parse")
        .expect("statement");
    let types = parser.get_parameter_types(&statement).expect("types");
    assert_eq!(
        types[0].oid(),
        datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID,
        "{types:?}"
    );
    let rows = run_prepared_vector_query(&router, &mut client, &uncast, &query).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(as_f64(&rows[0][0]) as i64, 9);

    cleanup_vector_table(&meta, table).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgvector_parameter_and_edge_cases() {
    let table = "pgvector_edges";
    let meta = setup_vector_table(table, 20).await;
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    // A `vector(n)` literal that disagrees with its own text is rejected
    // instead of comparing only the shared prefix.
    let outcome = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        &format!("SELECT vec <-> '[1,2]'::vector(8) FROM {table} WHERE id = 7"),
    )
    .await;
    let message = format!("{outcome:?}");
    assert!(
        message.contains("does not match the declared dimension"),
        "{message}"
    );

    // `NULL <-> vector` is NULL, not a type error.
    let rows = run_query(
        &router,
        &mut client,
        &format!("SELECT NULL <-> '[1,2,3,4,5,6,7,8]' FROM {table} WHERE id = 7"),
    )
    .await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].1[0][0], None, "{rows:?}");

    // A malformed literal keeps the upstream error.
    let outcome = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        &format!("SELECT vec <-> 'oops' FROM {table} WHERE id = 7"),
    )
    .await;
    assert!(outcome.is_err(), "{outcome:?}");

    cleanup_vector_table(&meta, table).await;
}

/// The stored vector columns are reported as the pgvector `vector` type, so a
/// driver registered for it decodes results as vectors.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pgvector_results_report_the_vector_type() {
    let table = "pgvector_types";
    let meta = setup_vector_table(table, 5).await;
    let factory = test_factory().await;
    let mut client = MockClient::new();
    attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    let pg_vector_oid = datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_TYPE_OID;
    let results = run_query(
        &router,
        &mut client,
        &format!("SELECT id, vec FROM {table} WHERE id = 3"),
    )
    .await;
    let (schema, rows) = &results[0];
    assert_eq!(schema.len(), 2, "{schema:?}");
    assert_eq!(
        schema[0].datatype().oid(),
        datafusion_postgres::pgwire::api::Type::INT8.oid(),
        "other columns keep their type: {schema:?}"
    );
    assert_eq!(
        schema[1].datatype().oid(),
        pg_vector_oid,
        "the vector column is reported as the pgvector type: {schema:?}"
    );
    // The value is encoded in the pgvector text form; a registered driver
    // parses it back into the stored vector.
    let text = rows[0][1].as_deref().expect("vector value");
    let values: Vec<f32> = text
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|value| value.parse().expect("number"))
        .collect();
    assert_eq!(values, generated_vector(3).to_vec(), "{text}");

    // `SELECT *` keeps the same report.
    let results = run_query(
        &router,
        &mut client,
        &format!("SELECT * FROM {table} WHERE id = 3"),
    )
    .await;
    let (schema, _) = &results[0];
    let vec_index = schema
        .iter()
        .position(|field| field.name() == "vec")
        .expect("the vec column is in the result");
    assert_eq!(
        schema[vec_index].datatype().oid(),
        pg_vector_oid,
        "{schema:?}"
    );

    // The index-backed search still works with the marker in place.
    let probe = vector_text(&generated_vector(1));
    let results = run_query(
        &router,
        &mut client,
        &format!("SELECT id FROM {table} ORDER BY vec <-> '{probe}' LIMIT 1"),
    )
    .await;
    assert_eq!(as_f64(&first_column(&results)[0]) as i64, 1);

    cleanup_vector_table(&meta, table).await;
}

/// The connection's configured vector-search probes.
fn vector_search_nprobe(context: &SessionContext) -> Option<usize> {
    context
        .state()
        .config()
        .options()
        .extensions
        .get::<lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchConfig>()
        .map(|options| options.nprobe)
}

/// pgvector's `ivfflat.probes` GUC is served from the connection's
/// vector-search `nprobe`: `SET` writes it, `SHOW` reads it back, and the SQL
/// option the scan uses is the same one.
#[tokio::test]
async fn pgvector_probes_guc_maps_to_nprobe() {
    let factory = test_factory().await;
    let mut client = MockClient::new();
    let state = attach_session(&factory, &mut client, "user_a").await;
    let router = LakeSoulQueryRouter::new();

    // The session default.
    let results = run_query(&router, &mut client, "SHOW ivfflat.probes").await;
    assert_eq!(first_column(&results), vec![Some("64".to_string())]);

    // `SET` applies to this connection's vector-search options ...
    run_query(&router, &mut client, "SET ivfflat.probes = 7").await;
    assert_eq!(vector_search_nprobe(&state.session.context), Some(7));
    let results = run_query(&router, &mut client, "SHOW ivfflat.probes").await;
    assert_eq!(
        results[0].0[0].name(),
        "ivfflat.probes",
        "SHOW reports the GUC name: {results:?}"
    );
    assert_eq!(first_column(&results), vec![Some("7".to_string())]);

    // ... which is the option the LakeSoul SQL surface sets too.
    run_query(
        &router,
        &mut client,
        "SET lakesoul_vector_search.nprobe = 9",
    )
    .await;
    assert_eq!(vector_search_nprobe(&state.session.context), Some(9));

    // Out-of-range values are rejected the way pgvector does.
    let outcome = <LakeSoulQueryRouter as SimpleQueryHandler>::do_query(
        &router,
        &mut client,
        "SET ivfflat.probes = 0",
    )
    .await;
    let message = format!("{outcome:?}");
    assert!(outcome.is_err(), "{message}");
    assert!(message.contains("expects 1..32768"), "{message}");
    assert_eq!(vector_search_nprobe(&state.session.context), Some(9));

    // The extended protocol — what drivers use — goes through the same hook.
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_parse(
        &router,
        &mut client,
        Parse::new(None, "SET ivfflat.probes = 5".to_string(), vec![]),
    )
    .await
    .expect("on_parse");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_bind(
        &router,
        &mut client,
        Bind::new(None, None, vec![], vec![], vec![]),
    )
    .await
    .expect("on_bind");
    <LakeSoulQueryRouter as ExtendedQueryHandler>::on_execute(
        &router,
        &mut client,
        Execute::new(None, 0),
    )
    .await
    .expect("on_execute");
    assert_eq!(vector_search_nprobe(&state.session.context), Some(5));
}
