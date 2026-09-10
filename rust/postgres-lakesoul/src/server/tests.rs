// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for connection-local session routing.
//!
//! These tests need a live PostgreSQL instance configured through
//! `LAKESOUL_PG_URL` (see `script/meta_init_for_local_test.sh`); they are
//! skipped otherwise.

use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::pgwire;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::testing::MockClient;
use lakesoul_metadata::MetaDataClient;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::{METADATA_DATABASE, METADATA_USER};
use pgwire::messages::extendedquery::{Bind, Execute, Parse};
use pgwire::messages::{PgWireFrontendMessage, extendedquery::Sync as PgSync};

use super::{ConnectionSession, LakeSoulQueryRouter, LakeSoulStartupHandler};
use crate::session::{PgSessionFactory, SessionIdentity, SessionSettings};

/// Time zone configured by `SessionSettings::default()`.
const DEFAULT_TIME_ZONE: &str = "UTC";

fn pg_available() -> bool {
    std::env::var("LAKESOUL_PG_URL").is_ok() || std::env::var("lakesoul_home").is_ok()
}

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
    let service = Arc::new(DfSessionService::new(Arc::clone(&session.context)));
    let state = Arc::new(ConnectionSession { session, service });
    client.session_extensions().insert(ConnectionSession {
        session: Arc::clone(&state.session),
        service: Arc::clone(&state.service),
    });
    state
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
    if !pg_available() {
        return;
    }
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
    if !pg_available() {
        return;
    }
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
    if !pg_available() {
        return;
    }
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
    if !pg_available() {
        return;
    }
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
    if !pg_available() {
        return;
    }
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
    client
        .session_extensions()
        .insert(ConnectionSession { session, service });
    assert!(Arc::strong_count(&context) > baseline);

    // Dropping the client releases its SessionExtensions, which drops the
    // connection-local state: both the `ConnectionSession` and the `PgSession`
    // (which holds its own `Arc<SessionContext>` reference) are released.
    drop(client);
    assert_eq!(Arc::strong_count(&context), baseline - 1);
}

#[tokio::test]
async fn startup_handler_installs_session() {
    if !pg_available() {
        return;
    }
    let factory = test_factory().await;
    let handler = LakeSoulStartupHandler::new(
        Arc::clone(&factory),
        Arc::new(pgwire::api::ConnectionManager::new()),
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
async fn session_scopes_namespaces_as_databases() {
    if !pg_available() {
        return;
    }
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
