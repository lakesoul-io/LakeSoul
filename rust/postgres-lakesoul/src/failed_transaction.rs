// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL's failed-transaction statement gate.
//!
//! Once a statement fails inside an explicit transaction block, PostgreSQL
//! ignores every later statement of that block except `COMMIT` and `ROLLBACK`,
//! reporting SQLSTATE `25P02` (in_failed_sql_transaction). The upstream
//! [`TransactionStatementHook`] enforces the same rule but reports `25P01`
//! (no_active_sql_transaction), so [`FailedTransactionGuard`] sits immediately
//! in front of it, reads the same per-connection state
//! (`ClientInfo::transaction_status`) and answers first with PostgreSQL's
//! code. That makes the upstream `25P01` branch unreachable for the statements
//! this guard sees; nothing is matched on error text and nothing rewrites an
//! error afterwards.
//!
//! `COMMIT` and `ROLLBACK` pass through so a failed block still ends the way
//! PostgreSQL ends it - `COMMIT` is answered with the `ROLLBACK` tag and ends
//! the transaction - and every statement in a healthy transaction or in
//! autocommit mode is untouched, as are errors produced by actual execution.
//! Entering and leaving the failed state is left to the server's error path and
//! to those two statements.
//!
//! Both protocol paths gate where PostgreSQL gates: the simple path per
//! statement, the extended path at `Parse` (after the SQL text parsed, before
//! analysis, so a syntax error still wins, as in PostgreSQL) and at `Execute`
//! for a statement that was prepared while the block was healthy.
//!
//! [`TransactionStatementHook`]: datafusion_postgres::hooks::transactions::TransactionStatementHook

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use pgwire::api::ClientInfo;
use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;

/// PostgreSQL `in_failed_sql_transaction`.
const IN_FAILED_SQL_TRANSACTION: &str = "25P02";

/// Rejects statements issued in a failed transaction block with `25P02`.
pub(crate) struct FailedTransactionGuard;

impl FailedTransactionGuard {
    /// Whether PostgreSQL ignores `statement` in a block that already failed.
    ///
    /// The status is the one pgwire keeps for the connection and the upstream
    /// transaction hook reads; `COMMIT` and `ROLLBACK` are the statements that
    /// still run in such a block.
    fn rejects(transaction_status: TransactionStatus, statement: &Statement) -> bool {
        transaction_status == TransactionStatus::Error
            && !matches!(
                statement,
                Statement::Commit { .. } | Statement::Rollback { .. }
            )
    }
}

/// PostgreSQL's wording for the failed-transaction rejection.
fn failed_transaction_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        IN_FAILED_SQL_TRANSACTION.to_string(),
        "current transaction is aborted, commands ignored until end of \
         transaction block"
            .to_string(),
    )))
}

#[async_trait]
impl QueryHook for FailedTransactionGuard {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if Self::rejects(client.transaction_status(), statement) {
            return Some(Err(failed_transaction_error()));
        }
        None
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // PostgreSQL rejects `Parse` in a failed block as well, with the same
        // code; parsing the text already happened, so a syntax error keeps
        // precedence.
        if Self::rejects(client.transaction_status(), statement) {
            return Some(Err(failed_transaction_error()));
        }
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // A statement prepared while the block was healthy must still be
        // refused when it executes after the block failed.
        if Self::rejects(client.transaction_status(), statement) {
            return Some(Err(failed_transaction_error()));
        }
        None
    }
}
