// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared assertions for the phase-two session/state contracts.
//!
//! Server rejections and client-side parameter rejections are deliberately
//! separated: only the former carries a PostgreSQL SQLSTATE, the latter is the
//! mature `tokio-postgres` driver refusing to serialize before any bytes reach
//! the server. The phase-two fixtures are never modified; every refused
//! statement is followed by a read and by an exact authoritative snapshot
//! comparison.

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::Client;
use tokio_postgres::types::Type;

use crate::support::{Harness, compare};

/// The base fixture as both servers see it.
///
/// NULL stays distinct from the empty string, the UTF-8 label keeps its
/// surrounding spaces, and the duplicate key remains two physical rows.
pub fn items_rows() -> compare::Rows {
    [
        (Some("1"), Some("10"), Some("a")),
        (Some("1"), None, Some("")),
        (Some("2"), Some("20"), Some(" β ")),
        (None, None, None),
    ]
    .into_iter()
    .map(|(id, val, label)| {
        vec![
            id.map(str::to_owned),
            val.map(str::to_owned),
            label.map(str::to_owned),
        ]
    })
    .collect()
}

/// Assert that `error` is a server `ErrorResponse` with exactly `expected`.
///
/// The full message is not part of the contract; the SQLSTATE and the fact
/// that the failure came from the server are.
pub fn ensure_sqlstate(
    error: &tokio_postgres::Error,
    expected: &str,
    context: &str,
) -> Result<()> {
    let db = error.as_db_error().with_context(|| {
        format!(
            "{context}: expected server ErrorResponse SQLSTATE {expected}, \
             actual client-side rejection (no SQLSTATE): {error}"
        )
    })?;
    ensure!(
        db.code().code() == expected,
        "{context}: expected SQLSTATE {expected}, actual {} ({})",
        db.code().code(),
        db.message()
    );
    Ok(())
}

/// Assert that the driver refused locally, before sending Bind/Parse.
///
/// `as_db_error()` being `None` is the observable distinction from a server
/// binding/cast failure; the connection must still be usable afterwards.
pub fn ensure_client_rejection(
    error: &tokio_postgres::Error,
    context: &str,
) -> Result<()> {
    ensure!(
        error.as_db_error().is_none(),
        "{context}: expected a client-side serialization/parameter rejection, \
         actual server SQLSTATE {:?}: {error}",
        error.code().map(|code| code.code())
    );
    Ok(())
}

/// Take the authoritative no-side-effect snapshot for one phase of a case.
pub async fn snapshot(h: &Harness, context: &str) -> Result<String> {
    h.authoritative_snapshot()
        .await
        .with_context(|| format!("{context}: read authoritative snapshot"))
}

/// Compare two snapshots without printing their contents.
///
/// The snapshot includes exact warehouse file bytes and can be large; the
/// failure names the case and reports only the sizes and first differing byte.
pub fn ensure_snapshot_unchanged(before: &str, after: &str, context: &str) -> Result<()> {
    if before == after {
        return Ok(());
    }
    let first_difference = before
        .bytes()
        .zip(after.bytes())
        .position(|(before, after)| before != after);
    bail!(
        "{context}: authoritative snapshot changed \
         (before {} bytes, after {} bytes, first difference at byte {:?})",
        before.len(),
        after.len(),
        first_difference
    )
}

/// Read the base fixture through the read-only server.
///
/// This is the first subsequent query after a rejection and also proves the
/// extended-protocol statement cache/connection is still healthy. It asserts
/// every row and column type rather than a count, so an UPDATE/DELETE/replace
/// hidden behind a refusal cannot pass.
pub async fn assert_items_intact(client: &Client, context: &str) -> Result<()> {
    let sql = "SELECT id, val, label FROM items";
    let actual = compare::query_binary(client, sql)
        .await
        .with_context(|| format!("{context}: read fixture with SQL={sql}"))?;
    compare::check_columns(
        &actual,
        &[
            ("id", Type::INT4),
            ("val", Type::INT4),
            ("label", Type::TEXT),
        ],
    )
    .with_context(|| format!("{context}: fixture columns for SQL={sql}"))?;
    compare::compare_rows(&actual.rows, &items_rows(), false)
        .with_context(|| format!("{context}: fixture rows for SQL={sql}"))?;
    Ok(())
}

/// Read the `items` row count through a simple query, preserving exact text.
pub async fn assert_items_count(
    client: &Client,
    expected: i64,
    context: &str,
) -> Result<()> {
    let sql = "SELECT COUNT(*) AS n FROM items";
    let actual = compare::simple(client, sql)
        .await
        .with_context(|| format!("{context}: simple query SQL={sql}"))?;
    ensure!(
        actual.names == vec!["n".to_owned()],
        "{context}: column names for SQL={sql}: expected [\"n\"], actual {:?}",
        actual.names
    );
    let expected_rows = vec![vec![Some(expected.to_string())]];
    ensure!(
        actual.rows == expected_rows,
        "{context}: SQL={sql} expected {expected_rows:?}, actual {:?}",
        actual.rows
    );
    Ok(())
}
