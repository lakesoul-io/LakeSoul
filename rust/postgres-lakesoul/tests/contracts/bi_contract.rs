// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! BI / database-browser metadata contracts against the real server.
//!
//! These are the queries Superset, Metabase, DBeaver and psql make before
//! they run a report: role and session discovery, `SHOW`/`current_setting`
//! values, the `pg_settings` relation, and the date formatting reports use.
//! The pinned catalog's `pg_roles` provider used to recurse until the server
//! process overflowed its stack; the first case pins that it answers instead.

use crate::support;

use crate::support::Harness;
use anyhow::{Context, Result, ensure};
use tokio_postgres::SimpleQueryMessage;

macro_rules! ensure_eq {
    ($left:expr, $right:expr $(, $($arg:tt)+)?) => {
        ensure!($left == $right $(, $($arg)+)?)
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bi_metadata_contract() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            roles_and_session_identity(h).await?;
            psql_publication_probe(h).await?;
            show_reports_postgres_values(h).await?;
            settings_are_readable_and_writable(h).await?;
            report_date_formatting(h).await?;
            Ok(())
        })
    })
    .await
}

/// `pg_roles` answers without taking the process down, and the session
/// functions report the connected user rather than the hard-coded default.
async fn roles_and_session_identity(h: &Harness) -> Result<()> {
    let roles: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pg_catalog.pg_roles", &[])
        .await
        .context("pg_roles must answer")?
        .get(0);
    ensure!(roles >= 0, "pg_roles count {roles}");

    let current: String = h
        .lake
        .query_one("SELECT current_user", &[])
        .await
        .context("current_user")?
        .get(0);
    let session: String = h
        .lake
        .query_one("SELECT session_user", &[])
        .await
        .context("session_user")?
        .get(0);
    ensure_eq!(
        current,
        "sql_compat",
        "current_user reports the connection user"
    );
    ensure_eq!(
        session,
        "sql_compat",
        "session_user reports the connection user"
    );
    eprintln!("PASS case=bi-roles roles={roles} user={current}");
    Ok(())
}

/// psql 18 asks a pre-15 server for a table's publications with two
/// unaliased `NULL` projections in a `UNION ALL`. DataFusion refuses the
/// duplicate expression names, so the server must answer the probe itself.
async fn psql_publication_probe(h: &Harness) -> Result<()> {
    let sql = "SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p \
               JOIN pg_catalog.pg_publication_rel pr ON p.oid = pr.prpubid \
               WHERE pr.prrelid = '0' \
               UNION ALL \
               SELECT pubname, NULL, NULL FROM pg_catalog.pg_publication p \
               WHERE p.puballtables \
                 AND pg_catalog.pg_relation_is_publishable('0') \
               ORDER BY 1";
    let statement = h
        .lake
        .prepare(sql)
        .await
        .context("prepare the psql publication probe")?;
    ensure_eq!(
        statement.columns().len(),
        3,
        "publication probe column count"
    );
    let rows = h
        .lake
        .query(&statement, &[])
        .await
        .context("execute the psql publication probe")?;
    ensure_eq!(rows.len(), 0, "LakeSoul has no publications");
    let messages = h
        .lake
        .simple_query(sql)
        .await
        .context("simple psql publication probe")?;
    ensure!(
        !messages
            .iter()
            .any(|message| matches!(message, SimpleQueryMessage::Row(_))),
        "the publication probe must return no rows"
    );
    eprintln!("PASS case=bi-psql-publication rows=0 columns=3");
    Ok(())
}

/// `SHOW` must report the real session values, not empty strings, and the
/// multi-word `SHOW transaction isolation level` spelling must agree with
/// `SHOW transaction_isolation`.
async fn show_reports_postgres_values(h: &Harness) -> Result<()> {
    for (sql, expected) in [
        ("SHOW server_version", "14.0"),
        ("SHOW server_version_num", "140000"),
        ("SHOW standard_conforming_strings", "on"),
        ("SHOW client_encoding", "UTF8"),
        ("SHOW DateStyle", "ISO, YMD"),
        ("SHOW max_identifier_length", "63"),
        ("SHOW transaction_isolation", "read committed"),
        ("SHOW transaction isolation level", "read committed"),
        ("SHOW search_path", "public, pg_catalog"),
    ] {
        let value: String = h
            .lake
            .query_one(sql, &[])
            .await
            .with_context(|| format!("{sql} must answer"))?
            .get(0);
        ensure_eq!(value, expected, "{sql}");
    }

    let rows = h.lake.query("SHOW ALL", &[]).await.context("SHOW ALL")?;
    ensure!(rows.len() > 20, "SHOW ALL returned {} rows", rows.len());
    ensure_eq!(rows[0].columns()[0].name(), "name", "SHOW ALL column name");
    ensure_eq!(
        rows[0].columns()[1].name(),
        "setting",
        "SHOW ALL setting column"
    );
    ensure_eq!(
        rows[0].columns()[2].name(),
        "description",
        "SHOW ALL description column"
    );
    eprintln!(
        "PASS case=bi-show parameters=9 show-all-rows={}",
        rows.len()
    );
    Ok(())
}

/// `current_setting` / `set_config` and the `pg_settings` relation share the
/// connection's values.
async fn settings_are_readable_and_writable(h: &Harness) -> Result<()> {
    let version: String = h
        .lake
        .query_one("SELECT current_setting('server_version_num')", &[])
        .await
        .context("current_setting")?
        .get(0);
    ensure_eq!(version, "140000", "current_setting server_version_num");

    let missing: Option<String> = h
        .lake
        .query_one("SELECT current_setting('no_such_setting', true)", &[])
        .await
        .context("current_setting(name, true)")?
        .get(0);
    ensure_eq!(missing, None, "unknown parameter with missing_ok");

    let written: String = h
        .lake
        .query_one(
            "SELECT set_config('application_name', 'bi_probe', false)",
            &[],
        )
        .await
        .context("set_config")?
        .get(0);
    ensure_eq!(written, "bi_probe", "set_config result");
    let read: String = h
        .lake
        .query_one("SELECT current_setting('application_name')", &[])
        .await
        .context("current_setting after set_config")?
        .get(0);
    ensure_eq!(read, "bi_probe", "current_setting after set_config");
    let shown: String = h
        .lake
        .query_one("SHOW application_name", &[])
        .await
        .context("SHOW after set_config")?
        .get(0);
    ensure_eq!(shown, "bi_probe", "SHOW after set_config");

    // The BI tools send these through the extended protocol with bound
    // parameters, not as literals.
    let written: String = h
        .lake
        .query_one(
            "SELECT set_config($1, $2, false)",
            &[&"application_name", &"param_probe"],
        )
        .await
        .context("parameterized set_config")?
        .get(0);
    ensure_eq!(written, "param_probe", "parameterized set_config");
    let read: String = h
        .lake
        .query_one("SELECT current_setting($1)", &[&"application_name"])
        .await
        .context("parameterized current_setting")?
        .get(0);
    ensure_eq!(read, "param_probe", "parameterized current_setting");
    // The upstream `SET` path stores some values in connection metadata;
    // `SHOW` must merge them, and `RESET` must drop the `set_config` override.
    h.lake
        .batch_execute("RESET application_name")
        .await
        .context("RESET application_name")?;
    h.lake
        .batch_execute("SET application_name = 'set_probe'")
        .await
        .context("SET application_name")?;
    let shown: String = h
        .lake
        .query_one("SHOW application_name", &[])
        .await
        .context("SHOW after SET application_name")?
        .get(0);
    ensure_eq!(shown, "set_probe", "SHOW after SET application_name");

    let version: String = h
        .lake
        .query_one(
            "SELECT setting FROM pg_catalog.pg_settings WHERE name = $1",
            &[&"server_version_num"],
        )
        .await
        .context("parameterized pg_settings")?
        .get(0);
    ensure_eq!(version, "140000", "parameterized pg_settings");

    let rows = h
        .lake
        .query("SELECT name, setting FROM pg_catalog.pg_settings", &[])
        .await
        .context("pg_settings")?;
    ensure!(rows.len() > 20, "pg_settings rows: {}", rows.len());
    let count: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pg_catalog.pg_settings", &[])
        .await?
        .get(0);
    ensure!(count > 20, "pg_settings count: {count}");
    let value: String = h
        .lake
        .query_one(
            "SELECT setting FROM pg_catalog.pg_settings \
             WHERE name = 'server_version_num'",
            &[],
        )
        .await
        .context("pg_settings filter")?
        .get(0);
    ensure_eq!(value, "140000", "pg_settings filter");

    // `SET TIME ZONE` is reflected by `current_setting`, not only by SHOW.
    h.lake
        .batch_execute("SET TIME ZONE 'Asia/Shanghai'")
        .await
        .context("SET TIME ZONE")?;
    let zone: String = h
        .lake
        .query_one("SELECT current_setting('TimeZone')", &[])
        .await
        .context("current_setting TimeZone")?
        .get(0);
    ensure_eq!(zone, "Asia/Shanghai", "current_setting after SET TIME ZONE");
    h.lake
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .context("restore TimeZone")?;
    eprintln!("PASS case=bi-settings rows={count}");
    Ok(())
}

/// PostgreSQL date patterns, not DataFusion's strftime patterns.
async fn report_date_formatting(h: &Harness) -> Result<()> {
    for (sql, expected) in [
        (
            "SELECT to_char(TIMESTAMP '2024-01-02 03:04:05', \
             'YYYY-MM-DD HH24:MI:SS')",
            "2024-01-02 03:04:05",
        ),
        (
            "SELECT to_char(DATE '2024-01-02', 'DD Mon YYYY')",
            "02 Jan 2024",
        ),
        (
            "SELECT to_char(DATE '2024-01-02', 'FMMM/FMDD/YYYY')",
            "1/2/2024",
        ),
    ] {
        let value: String = h
            .lake
            .query_one(sql, &[])
            .await
            .with_context(|| format!("{sql} must format"))?
            .get(0);
        ensure_eq!(value, expected, "{sql}");
    }

    // Numeric formatting is not implemented; it must be an explicit error,
    // never the format string copied through as a value.
    let error = h
        .lake
        .query_one("SELECT to_char(1234.5, '9999.99')", &[])
        .await
        .expect_err("numeric to_char is not implemented");
    let message = error
        .as_db_error()
        .map(|database| database.message().to_string())
        .unwrap_or_else(|| error.to_string());
    ensure!(
        message.contains("to_char currently supports date and timestamp"),
        "unexpected numeric to_char error: {message}"
    );
    eprintln!("PASS case=bi-to-char patterns=3 numeric=explicit-error");
    Ok(())
}
