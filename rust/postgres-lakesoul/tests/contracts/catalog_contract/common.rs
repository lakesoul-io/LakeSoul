// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::{Client, types::Type};

use crate::support::compare;

pub fn rows(cells: &[&[Option<&str>]]) -> compare::Rows {
    cells
        .iter()
        .map(|row| row.iter().map(|cell| cell.map(str::to_owned)).collect())
        .collect()
}

/// These are LakeSoul-specific namespace/database contracts, not comparisons
/// with the reference PG connection's unrelated database and fixture schema.
pub async fn assert_query(
    client: &Client,
    id: &str,
    bug: &str,
    sql: &str,
    columns: &[(&str, Type)],
    expected: &compare::Rows,
    ordered: bool,
) -> Result<()> {
    for binary in [false, true] {
        let protocol = if binary {
            "extended-binary"
        } else {
            "simple-text"
        };
        let context = || {
            format!(
                "case={id} classification=lakesoul-database-public bug={bug} SQL={sql} \
             params=[] protocol={protocol} expected_columns={columns:?} ordered={ordered}"
            )
        };
        let actual = if binary {
            compare::query_binary(client, sql).await
        } else {
            compare::simple(client, sql).await
        }
        .with_context(context)?;
        compare::check_columns(&actual, columns).with_context(context)?;
        compare::compare_rows(&actual.rows, expected, ordered).with_context(context)?;
        eprintln!(
            "PASS {} rows={} oid_check=extended-only",
            context(),
            actual.rows.len()
        );
    }
    Ok(())
}

/// Reject at simple execution / extended Parse, then immediately read the
/// caller's own public relation. No intervening probe or reconnect can mask
/// a poisoned session or a leaked foreign database identity.
pub async fn assert_rejected(
    client: &Client,
    id: &str,
    sql: &str,
    expected_sqlstate: &str,
    recovery_expected: &compare::Rows,
) -> Result<()> {
    let recovery_sql = "SELECT id, origin FROM public.mapping_items ORDER BY id";
    let columns = [("id", Type::INT4), ("origin", Type::TEXT)];
    for binary in [false, true] {
        let protocol = if binary {
            "extended-parse"
        } else {
            "simple-text"
        };
        let context = || {
            format!(
                "case={id} classification=lakesoul-database-isolation SQL={sql} params=[] \
             protocol={protocol} expected_server_sqlstate={expected_sqlstate}"
            )
        };
        let result = if binary {
            client.prepare(sql).await.map(drop)
        } else {
            client.simple_query(sql).await.map(drop)
        };
        let error = match result {
            Ok(()) => bail!(
                "{}: foreign/missing object access unexpectedly accepted",
                context()
            ),
            Err(error) => error,
        };
        let db = error.as_db_error().with_context(|| {
            format!(
                "{}: expected server ErrorResponse, actual client failure: {error:?}",
                context(),
            )
        })?;
        ensure!(
            db.code().code() == expected_sqlstate,
            "{}: actual SQLSTATE={} message={}",
            context(),
            db.code().code(),
            db.message()
        );
        let actual = if binary {
            compare::query_binary(client, recovery_sql).await
        } else {
            compare::simple(client, recovery_sql).await
        }
        .with_context(|| format!("{} first recovery SQL={recovery_sql}", context()))?;
        compare::check_columns(&actual, &columns).with_context(context)?;
        compare::compare_rows(&actual.rows, recovery_expected, true).with_context(
            || format!("{} first recovery SQL={recovery_sql}", context()),
        )?;
        eprintln!(
            "PASS {} first_read=own-public-relation recovery_columns={columns:?}",
            context()
        );
    }
    Ok(())
}
