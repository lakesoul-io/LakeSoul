// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Read-only statement policy for the PostgreSQL wire server.
//!
//! The server exposes LakeSoul tables for analytical queries only. Anything
//! that is not a query, an `EXPLAIN` of a query, or a session/transaction
//! control statement is rejected with SQLSTATE `0A000`
//! (feature_not_supported) before it reaches DataFusion execution, so a
//! write or DDL can never run against LakeSoul metadata or data files.
//!
//! [`ReadOnlyStatementGuard`] is installed as the *last* statement hook of
//! the connection-local `DfSessionService`, after the upstream
//! cursor/set-show/transaction hooks: those keep handling their statements
//! and can still short-circuit (e.g. with PostgreSQL's "current transaction
//! is aborted" error) before the guard sees anything else. Rejections happen
//!
//! - in the simple protocol: per statement, before `SessionContext::sql`
//!   executes it;
//! - in the extended protocol: at `Parse` time, so clients get one
//!   consistent read-only error instead of DataFusion's "unsupported
//!   statement" planning error.
//!
//! A simple-protocol batch runs its statements before the first rejected
//! one; those can only be reads or session-local control statements, so no
//! persistent state changes before the error.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::hooks::cursor::CursorStatementHook;
use datafusion_postgres::hooks::set_show::SetShowHook;
use datafusion_postgres::hooks::transactions::TransactionStatementHook;
use datafusion_postgres::pgwire;
use pgwire::api::ClientInfo;
use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

/// SQLSTATE for rejected statements (read-only error mapping).
const FEATURE_NOT_SUPPORTED: &str = "0A000";

/// Statement hooks executed by every connection-local service.
///
/// The read-only guard runs last so the upstream hooks keep first claim on
/// cursor, `SET`/`SHOW` and transaction statements.
pub(crate) fn statement_hooks() -> Vec<Arc<dyn QueryHook>> {
    vec![
        Arc::new(CursorStatementHook),
        Arc::new(SetShowHook),
        Arc::new(TransactionStatementHook),
        Arc::new(ReadOnlyStatementGuard), // new
    ]
}

/// Rejects every statement the read-only server does not support.
struct ReadOnlyStatementGuard;

impl ReadOnlyStatementGuard {
    /// Statements the server accepts: queries, `DESCRIBE <table>`,
    /// session-local `SET`/`SHOW`, transaction control, and cursor
    /// statements (which the upstream hooks handle as reads). `EXPLAIN` is
    /// allowed only for a query, so `EXPLAIN INSERT` is rejected like the
    /// statement it wraps.
    fn allow(statement: &Statement) -> bool {
        match statement {
            Statement::Query(_)
            | Statement::ExplainTable { .. }
            | Statement::Set(_)
            | Statement::ShowVariable { .. }
            | Statement::ShowStatus { .. }
            | Statement::ShowVariables { .. }
            | Statement::ShowCatalogs { .. }
            | Statement::ShowTables { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowFunctions { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowDatabases { .. }
            | Statement::ShowSchemas { .. }
            | Statement::ShowObjects { .. }
            | Statement::ShowViews { .. }
            | Statement::ShowProcessList { .. }
            | Statement::ShowCharset { .. }
            | Statement::ShowCollation { .. }
            | Statement::StartTransaction { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. }
            | Statement::Declare { .. }
            | Statement::Fetch { .. }
            | Statement::Close { .. } => true,
            Statement::Explain {
                statement: inner, ..
            } => {
                matches!(&**inner, Statement::Query(_))
            }
            _ => false,
        }
    }
}

fn reject_message(statement: &Statement) -> String {
    let verb = match statement {
        Statement::Insert { .. } => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete { .. } => "DELETE",
        Statement::Merge { .. } => "MERGE",
        Statement::Copy { .. } | Statement::CopyIntoSnowflake { .. } => "COPY",
        Statement::Truncate { .. } => "TRUNCATE",
        Statement::Prepare { .. } => "PREPARE",
        Statement::Execute { .. } => "EXECUTE",
        Statement::Deallocate { .. } => "DEALLOCATE",
        Statement::Savepoint { .. } => "SAVEPOINT",
        Statement::ReleaseSavepoint { .. } => "RELEASE SAVEPOINT",
        Statement::Grant { .. } | Statement::Revoke { .. } | Statement::Deny { .. } => {
            "GRANT/REVOKE"
        }
        Statement::Drop { .. } => "DROP",
        Statement::CreateTable { .. } | Statement::CreateVirtualTable { .. } => {
            "CREATE TABLE"
        }
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::CreateSchema { .. } => "CREATE SCHEMA",
        Statement::CreateDatabase { .. } => "CREATE DATABASE",
        Statement::CreateIndex { .. } => "CREATE INDEX",
        Statement::AlterTable { .. } => "ALTER TABLE",
        _ => "this statement",
    };
    format!("cannot execute {verb} in a read-only LakeSoul session")
}

fn reject_error(statement: &Statement) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        FEATURE_NOT_SUPPORTED.to_string(),
        reject_message(statement),
    )))
}

#[async_trait]
impl QueryHook for ReadOnlyStatementGuard {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if Self::allow(statement) {
            return None;
        }
        Some(Err(reject_error(statement)))
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // Fail at Parse time: the client gets the read-only error before any
        // Bind/Execute instead of a DataFusion planning error.
        if Self::allow(statement) {
            return None;
        }
        Some(Err(reject_error(statement)))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // Defense in depth: unreachable while the parse hook rejects first,
        // but keeps the execute phase safe if hook ordering ever changes.
        if Self::allow(statement) {
            return None;
        }
        Some(Err(reject_error(statement)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
    use datafusion::prelude::SessionContext;
    use datafusion_postgres::DfSessionService;
    use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
    use datafusion_postgres::pgwire;
    use datafusion_postgres::testing::MockClient;
    use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
    use pgwire::api::results::Response;
    use pgwire::api::stmt::QueryParser;
    use pgwire::error::PgWireError;

    use super::*;

    fn service() -> DfSessionService {
        DfSessionService::new_with_hooks(
            Arc::new(SessionContext::new()),
            statement_hooks(),
        )
    }

    fn dummy_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    fn sql_state(error: &PgWireError) -> Option<&str> {
        match error {
            PgWireError::UserError(info) => Some(info.code.as_str()),
            _ => None,
        }
    }

    async fn parse(service: &DfSessionService, sql: &str) -> PgWireResult<()> {
        let parser = <DfSessionService as ExtendedQueryHandler>::query_parser(service);
        let client = MockClient::new();
        parser.parse_sql(&client, sql, &[]).await.map(|_| ())
    }

    const REJECTED: &[&str] = &[
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET a = 1",
        "COPY t TO STDOUT",
        "COPY t FROM STDIN",
        "TRUNCATE t",
        "CREATE TABLE t (a INT)",
        "CREATE TABLE t AS SELECT 1",
        "CREATE VIEW v AS SELECT 1",
        "CREATE SCHEMA s",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN b INT",
        "GRANT SELECT ON t TO u",
        "REVOKE SELECT ON t FROM u",
        "PREPARE p AS SELECT 1",
        "EXPLAIN INSERT INTO t VALUES (1)",
        "SAVEPOINT sp",
    ];

    const ALLOWED: &[&str] = &[
        "SELECT 1",
        "SELECT 1 WHERE 1 = 0",
        "WITH x AS (SELECT 1 AS a) SELECT * FROM x",
        "VALUES (1)",
        "EXPLAIN SELECT 1",
        "EXPLAIN ANALYZE SELECT 1",
        "SET a = 1",
        "SET TIME ZONE 'UTC'",
        "SHOW server_version",
        "BEGIN",
        "BEGIN TRANSACTION READ ONLY",
        "COMMIT",
        "ROLLBACK",
        "DECLARE c CURSOR FOR SELECT 1",
        "FETCH 1 FROM c",
        "CLOSE c",
    ];

    #[tokio::test]
    async fn extended_parse_rejects_writes_and_ddl() {
        let service = service();
        for sql in REJECTED {
            let error = parse(&service, sql).await.expect_err(sql);
            assert_eq!(sql_state(&error), Some("0A000"), "sql: {sql}");
        }
    }

    #[tokio::test]
    async fn extended_parse_allows_queries_and_session_control() {
        let service = service();
        for sql in ALLOWED {
            parse(&service, sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }

    #[tokio::test]
    async fn simple_protocol_rejects_writes_before_execution() {
        let service = service();
        let mut client = MockClient::new();
        // The trailing statement of a batch is rejected as well, so no
        // statement after the first rejected one can ever execute.
        for sql in [
            "INSERT INTO t VALUES (1)",
            "CREATE TABLE t (a INT)",
            "SELECT 1; DELETE FROM t",
        ] {
            let error = <DfSessionService as SimpleQueryHandler>::do_query(
                &service,
                &mut client,
                sql,
            )
            .await
            .expect_err(sql);
            assert_eq!(sql_state(&error), Some("0A000"), "sql: {sql}");
        }
    }

    #[tokio::test]
    async fn simple_protocol_allows_queries() {
        let service = service();
        let mut client = MockClient::new();
        let responses = <DfSessionService as SimpleQueryHandler>::do_query(
            &service,
            &mut client,
            "SELECT 1; SELECT 2",
        )
        .await
        .expect("SELECT statements should run");
        assert_eq!(responses.len(), 2);
        assert!(matches!(responses[0], Response::Query(_)));
    }

    #[tokio::test]
    async fn extended_execute_phase_rejects_writes() {
        // Second line of defense behind the parse hook: feed a write
        // statement straight into the execute-phase hook.
        let statements = PostgresCompatibilityParser::new()
            .parse("INSERT INTO t VALUES (1)")
            .expect("parse");
        let statement = statements.first().expect("statement");
        let mut client = MockClient::new();
        let context = SessionContext::new();

        let result = ReadOnlyStatementGuard
            .handle_extended_query(
                statement,
                &dummy_plan(),
                &ParamValues::List(vec![]),
                &context,
                &mut client,
            )
            .await
            .expect("write statement must be rejected");
        assert_eq!(sql_state(&result.unwrap_err()), Some("0A000"));
    }
}
