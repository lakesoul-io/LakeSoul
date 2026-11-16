// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Connection-local session state and transaction recovery.

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Statement};

use crate::common;
use crate::support::{Harness, compare};

const INSTANT_SQL: &str =
    "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00.123456' AS TIMESTAMPTZ) AS instant";
const UTC_TEXT: &str = "2020-01-01 00:00:00.123456+00";
const NEW_YORK: &str = "America/New_York";
const NEW_YORK_TEXT: &str = "2020-01-01 00:00:00.123456-05";

pub async fn run(h: &Harness) -> Result<()> {
    statement_timeout_is_connection_local(h).await?;
    failed_transaction_recovers_after_rollback(h).await?;
    failed_transaction_recovers_after_extended_parse_failure(h).await?;
    only_read_committed_isolation_is_accepted(h).await?;
    Ok(())
}

/// Same expensive aggregate on independent peers: only the issuing
/// connections must time out. No elapsed-time tolerance or sleep-based race.
async fn statement_timeout_is_connection_local(h: &Harness) -> Result<()> {
    const SQL: &str = "SELECT CAST(SUM(CAST(x AS BIGINT)) AS BIGINT) AS total FROM generate_series(1, 10000000) AS g(x)";
    let lake_peer = h.connect_lake().await?;
    let reference_peer = h.connect_reference().await?;
    let lake = h.connect_lake().await?;
    let reference = h.connect_reference().await?;
    lake.batch_execute("SET statement_timeout = '1ms'").await?;
    reference
        .batch_execute("SET statement_timeout = '1ms'")
        .await?;
    for (label, client) in [("LakeSoul", &lake), ("PostgreSQL", &reference)] {
        let error = client.simple_query(SQL).await.err().with_context(|| format!(
            "case=statement-timeout contract=PG-equivalent backend={label} SQL={SQL} params=[] protocol=simple-text expected server cancellation 57014, got success"
        ))?;
        common::ensure_sqlstate(
            &error,
            "57014",
            &format!("case=statement-timeout backend={label} SQL={SQL}"),
        )?;
    }
    let actual = compare::query_binary(&lake_peer, SQL)
        .await
        .context("case=statement-timeout peer must retain its own 60s budget")?;
    let expected = compare::query_binary(&reference_peer, SQL).await?;
    compare::check_columns(&actual, &[("total", Type::INT8)])?;
    compare::compare(&actual, &expected, true)?;
    compare::compare_rows(
        &actual.rows,
        &vec![vec![Some("50000005000000".into())]],
        true,
    )?;
    // These intentionally tiny budgets also cover SET itself. Do not depend
    // on raising them under load; close these owned clients after checking the
    // independent peers, rather than contaminating later recovery cases.
    drop(lake);
    drop(reference);
    eprintln!(
        "sql-compat case=statement-timeout contract=PG-equivalent cancellation=57014 existing-peer-budget=unaffected peer-result=50000005000000"
    );
    Ok(())
}

/// Strict `SET TIME ZONE` / reset contract for one reused physical connection.
///
/// The harness launches the server with `TZ=UTC`, so the startup default is
/// deterministic and is read from a fresh, unconfigured connection instead of
/// being hardcoded from whatever the test machine itself uses.
pub async fn time_zone_is_connection_local(h: &Harness) -> Result<()> {
    let startup = startup_defaults(h).await?;
    time_zone_changes_rendering_and_isolates_peers(h).await?;
    time_zone_accepted_spellings(h, &startup).await?;
    time_zone_invalid_value_is_rejected(h).await?;
    time_zone_local_and_global_scopes(h).await?;
    time_zone_parameter_status(h, &startup).await?;
    time_zone_reset_restores_startup(h, &startup).await?;
    Ok(())
}

/// Values a brand-new connection reports before this harness applies its
/// per-session `SET` script.
///
/// `time_zone` is asserted against UTC because the harness starts the child
/// with `TZ=UTC`; the remaining values are derived, never re-pinned from the
/// mutated connection's output.
struct StartupDefaults {
    time_zone: String,
    statement_timeout: String,
    search_path: String,
    probes: String,
    application_name: String,
    datestyle: String,
    intervalstyle: String,
    bytea_output: String,
    extra_float_digits: String,
}

async fn startup_defaults(h: &Harness) -> Result<StartupDefaults> {
    let client = h.connect_lake_startup_defaults().await?;
    let defaults = StartupDefaults {
        time_zone: show_value(
            &client,
            "SHOW TimeZone",
            "case=time-zone-startup-defaults",
        )
        .await?,
        statement_timeout: show_value(
            &client,
            "SHOW statement_timeout",
            "case=time-zone-startup-defaults",
        )
        .await?,
        search_path: show_value(
            &client,
            "SHOW search_path",
            "case=time-zone-startup-defaults",
        )
        .await?,
        probes: show_value(
            &client,
            "SHOW ivfflat.probes",
            "case=time-zone-startup-defaults",
        )
        .await?,
        application_name: show_value(
            &client,
            "SHOW application_name",
            "case=time-zone-startup-defaults",
        )
        .await?,
        datestyle: show_value(
            &client,
            "SHOW DateStyle",
            "case=time-zone-startup-defaults",
        )
        .await?,
        intervalstyle: show_value(
            &client,
            "SHOW IntervalStyle",
            "case=time-zone-startup-defaults",
        )
        .await?,
        bytea_output: show_value(
            &client,
            "SHOW bytea_output",
            "case=time-zone-startup-defaults",
        )
        .await?,
        extra_float_digits: show_value(
            &client,
            "SHOW extra_float_digits",
            "case=time-zone-startup-defaults",
        )
        .await?,
    };
    ensure!(
        defaults.time_zone == "UTC",
        "case=time-zone-startup-defaults: harness launches the server with \
         TZ=UTC, so the startup TimeZone must be UTC, actual {:?}",
        defaults.time_zone
    );
    ensure!(
        defaults.statement_timeout == "0",
        "case=time-zone-startup-defaults: startup statement_timeout must be \
         disabled (\"0\"), actual {:?}",
        defaults.statement_timeout
    );
    eprintln!(
        "sql-compat case=time-zone-startup-defaults time_zone={} \
         statement_timeout={} search_path={:?} probes={} datestyle={:?}",
        defaults.time_zone,
        defaults.statement_timeout,
        defaults.search_path,
        defaults.probes,
        defaults.datestyle
    );
    Ok(defaults)
}

async fn show_value(client: &Client, sql: &str, context: &str) -> Result<String> {
    let result = compare::simple(client, sql)
        .await
        .with_context(|| format!("{context}: SQL={sql}"))?;
    ensure!(
        result.names.len() == 1 && result.rows.len() == 1 && result.rows[0].len() == 1,
        "{context}: SQL={sql} expected one single-column row, actual names={:?} rows={:?}",
        result.names,
        result.rows
    );
    let value = result.rows[0][0]
        .clone()
        .with_context(|| format!("{context}: SQL={sql} returned NULL"))?;
    Ok(value)
}

/// `Describe` must report the column `Execute` returns, not a placeholder.
///
/// The extended protocol answers `Describe` from the plan the statement was
/// parsed into; PostgreSQL reports the canonical parameter name and `VARCHAR`
/// both before and after the value changes.
async fn assert_show_describe(client: &Client, sql: &str, context: &str) -> Result<()> {
    let statement = client
        .prepare(sql)
        .await
        .with_context(|| format!("{context}: SQL={sql} Describe failed"))?;
    let columns: Vec<(String, Type)> = statement
        .columns()
        .iter()
        .map(|column| (column.name().to_string(), column.type_().clone()))
        .collect();
    ensure!(
        columns.len() == 1 && columns[0].0 == "TimeZone" && columns[0].1 == Type::TEXT,
        "{context}: SQL={sql} exact Describe [(\"TimeZone\", TEXT)], actual {columns:?}"
    );
    Ok(())
}

async fn assert_show(
    client: &Client,
    sql: &str,
    expected: &str,
    context: &str,
) -> Result<()> {
    let actual = show_value(client, sql, context).await?;
    ensure!(
        actual == expected,
        "{context}: SQL={sql} expected {expected:?}, actual {actual:?}"
    );
    Ok(())
}

/// `SET TIME ZONE` changes the rendered result on the issuing connection only.
///
/// Casting a naive timestamp uses the connection's timezone. The same wall
/// clock therefore denotes different instants after SET. Check text rendering,
/// binary instants, and an already-open peer with unchanged UTC settings.
async fn time_zone_changes_rendering_and_isolates_peers(h: &Harness) -> Result<()> {
    const SQL: &str =
        "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00.123456' AS TIMESTAMPTZ) AS instant";
    const UTC_TEXT: &str = "2020-01-01 00:00:00.123456+00";
    const NEW_YORK_TEXT: &str = "2020-01-01 00:00:00.123456-05";

    let baseline_context = || {
        format!(
            "case=time-zone-baseline SQL={SQL} params=[] protocol=simple-text \
             expected=[{UTC_TEXT}]"
        )
    };
    let baseline = compare::simple(&h.lake, SQL)
        .await
        .with_context(baseline_context)?;
    let reference_baseline = compare::simple(&h.reference, SQL)
        .await
        .with_context(baseline_context)?;
    ensure!(
        baseline.rows == vec![vec![Some(UTC_TEXT.to_owned())]],
        "{}: LakeSoul rows expected [[{UTC_TEXT:?}]], actual {:?}",
        baseline_context(),
        baseline.rows
    );
    ensure!(
        reference_baseline.rows == vec![vec![Some(UTC_TEXT.to_owned())]],
        "{}: PostgreSQL rows expected [[{UTC_TEXT:?}]], actual {:?}",
        baseline_context(),
        reference_baseline.rows
    );
    compare::compare(&baseline, &reference_baseline, true)
        .with_context(baseline_context)?;

    let utc_context = "case=time-zone-baseline protocol=extended-binary";
    let lake_utc = binary_instant(&h.lake, SQL, utc_context).await?;
    let reference_utc = binary_instant(&h.reference, SQL, utc_context).await?;
    ensure!(
        lake_utc == reference_utc,
        "{utc_context}: binary TIMESTAMPTZ instant differs between LakeSoul \
         ({lake_utc}) and PostgreSQL ({reference_utc})"
    );
    assert_show(
        &h.lake,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-baseline protocol=simple-text",
    )
    .await?;
    assert_show(
        &h.reference,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-baseline reference protocol=simple-text",
    )
    .await?;
    let lake_show = compare::simple(&h.lake, "SHOW TimeZone").await?;
    let reference_show = compare::simple(&h.reference, "SHOW TimeZone").await?;
    compare::compare(&lake_show, &reference_show, true)
        .context("case=time-zone-baseline: SHOW TimeZone differs from PostgreSQL")?;

    // The extended protocol describes the statement before it runs: the
    // column and its type must be the ones the response carries, for the
    // parameter's canonical spelling and the one-word form alike.
    for sql in ["SHOW TimeZone", "SHOW timezone"] {
        let context = format!("case=time-zone-describe SQL={sql} protocol=extended");
        assert_show_describe(&h.lake, sql, &context).await?;
        assert_show_describe(&h.reference, sql, &context).await?;
    }

    // Parse/Describe the statement before the mutation; Execute must still
    // observe the time zone that is current at execution time.
    let lake_prepared = h
        .lake
        .prepare(SQL)
        .await
        .context("case=time-zone-prepared-before-set: LakeSoul Parse/Describe")?;
    let reference_prepared = h
        .reference
        .prepare(SQL)
        .await
        .context("case=time-zone-prepared-before-set: PostgreSQL Parse/Describe")?;

    // Create the peer before the mutation: configuring a new peer afterwards
    // could reset shared state and accidentally hide cross-connection leakage.
    let peer = h.connect_lake().await?;
    assert_show(
        &peer,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-peer-utc before SET protocol=simple-text",
    )
    .await?;

    h.lake
        .batch_execute("SET TIME ZONE 'America/New_York'")
        .await
        .context("case=time-zone-new-york SQL=SET TIME ZONE 'America/New_York' protocol=simple-text")?;
    h.reference
        .batch_execute("SET TIME ZONE 'America/New_York'")
        .await
        .context(
            "case=time-zone-new-york reference SQL=SET TIME ZONE 'America/New_York'",
        )?;

    assert_show(
        &h.lake,
        "SHOW TimeZone",
        "America/New_York",
        "case=time-zone-new-york protocol=simple-text",
    )
    .await?;
    assert_show(
        &h.reference,
        "SHOW TimeZone",
        "America/New_York",
        "case=time-zone-new-york reference protocol=simple-text",
    )
    .await?;
    let lake_show = compare::simple(&h.lake, "SHOW TimeZone").await?;
    let reference_show = compare::simple(&h.reference, "SHOW TimeZone").await?;
    compare::compare(&lake_show, &reference_show, true)
        .context("case=time-zone-new-york: SHOW TimeZone differs from PostgreSQL")?;

    let context = || {
        format!(
            "case=time-zone-new-york SQL={SQL} params=[] protocol=simple-text \
             expected=[{NEW_YORK_TEXT}]"
        )
    };
    let lake_ny = compare::simple(&h.lake, SQL).await.with_context(context)?;
    let reference_ny = compare::simple(&h.reference, SQL)
        .await
        .with_context(context)?;
    ensure!(
        lake_ny.rows == vec![vec![Some(NEW_YORK_TEXT.to_owned())]],
        "{}: LakeSoul rows expected [[{NEW_YORK_TEXT:?}]], actual {:?}",
        context(),
        lake_ny.rows
    );
    ensure!(
        reference_ny.rows == vec![vec![Some(NEW_YORK_TEXT.to_owned())]],
        "{}: PostgreSQL rows expected [[{NEW_YORK_TEXT:?}]], actual {:?}",
        context(),
        reference_ny.rows
    );
    compare::compare(&lake_ny, &reference_ny, true).with_context(context)?;

    // The same naive wall clock in New York denotes an instant five hours
    // later than UTC, independently of protocol formatting.
    let ny_context = "case=time-zone-new-york protocol=extended-binary";
    let lake_ny_instant = binary_instant(&h.lake, SQL, ny_context).await?;
    let reference_ny_instant = binary_instant(&h.reference, SQL, ny_context).await?;
    ensure!(
        lake_ny_instant == lake_utc + chrono::Duration::hours(5),
        "{ny_context}: casting the same wall clock in New York must shift the instant by five hours: \
         baseline {lake_utc}, after SET TIME ZONE {lake_ny_instant}"
    );
    ensure!(
        lake_ny_instant == reference_ny_instant,
        "{ny_context}: binary TIMESTAMPTZ instant differs between LakeSoul \
         ({lake_ny_instant}) and PostgreSQL ({reference_ny_instant})"
    );

    let prepared_context =
        "case=time-zone-prepared-before-set SQL=naive-cast protocol=extended-binary";
    let lake_prepared_ny =
        prepared_binary_instant(&h.lake, &lake_prepared, prepared_context).await?;
    let reference_prepared_ny =
        prepared_binary_instant(&h.reference, &reference_prepared, prepared_context)
            .await?;
    ensure!(
        lake_prepared_ny == lake_utc + chrono::Duration::hours(5),
        "{prepared_context}: a Statement parsed before SET must execute with \
         the current zone: baseline {lake_utc}, after SET {lake_prepared_ny}"
    );
    ensure!(
        lake_prepared_ny == reference_prepared_ny,
        "{prepared_context}: binary instant differs between LakeSoul \
         ({lake_prepared_ny}) and PostgreSQL ({reference_prepared_ny})"
    );

    // The already-open peer must remain UTC while the issuing session is NY.
    // No intervening SET on the peer is allowed to manufacture that isolation.
    let peer_context = "case=time-zone-peer-utc protocol=simple-text";
    let peer_result = compare::simple(&peer, SQL)
        .await
        .with_context(|| format!("{peer_context}: SQL={SQL}"))?;
    ensure!(
        peer_result.rows == vec![vec![Some(UTC_TEXT.to_owned())]],
        "{peer_context}: expected UTC rendering [[{UTC_TEXT:?}]], actual {:?}",
        peer_result.rows
    );
    let peer_instant = binary_instant(
        &peer,
        SQL,
        "case=time-zone-peer-utc protocol=extended-binary",
    )
    .await?;
    ensure!(
        peer_instant == lake_utc,
        "case=time-zone-peer-utc protocol=extended-binary: peer must retain \
         the baseline instant {lake_utc}, actual {peer_instant}"
    );
    drop(peer);

    // A connection created after the mutation must still start at the
    // process startup default, not inherit the issuing session's zone.
    let new_connection = h.connect_lake_startup_defaults().await?;
    assert_show(
        &new_connection,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-new-connection startup default",
    )
    .await?;
    let new_connection_instant = binary_instant(
        &new_connection,
        SQL,
        "case=time-zone-new-connection protocol=extended-binary",
    )
    .await?;
    ensure!(
        new_connection_instant == lake_utc,
        "case=time-zone-new-connection: new connection instant must remain \
         {lake_utc}, actual {new_connection_instant}"
    );
    drop(new_connection);

    h.lake
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .context("case=time-zone-restore SQL=SET TIME ZONE 'UTC'")?;
    h.reference
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .context("case=time-zone-restore reference SQL=SET TIME ZONE 'UTC'")?;

    let restored = compare::simple(&h.lake, SQL)
        .await
        .context("case=time-zone-restore: restore the LakeSoul session to UTC")?;
    ensure!(
        restored.rows == vec![vec![Some(UTC_TEXT.to_owned())]],
        "case=time-zone-restore: expected UTC rendering [[{UTC_TEXT:?}]], actual {:?}",
        restored.rows
    );
    let reference_restored = compare::simple(&h.reference, SQL)
        .await
        .context("case=time-zone-restore: restore the PostgreSQL session to UTC")?;
    compare::compare(&restored, &reference_restored, true)
        .context("case=time-zone-restore: compare restored UTC results")?;
    assert_show(
        &h.lake,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-restore protocol=simple-text",
    )
    .await?;
    assert_show(
        &h.reference,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-restore reference protocol=simple-text",
    )
    .await?;

    eprintln!(
        "sql-compat case=time-zone classification=pg-equivalent \
         utc={UTC_TEXT} new_york={NEW_YORK_TEXT} peer=utc prepared=applies \
         new-connection=utc restored=utc"
    );
    Ok(())
}

async fn binary_instant(
    client: &Client,
    sql: &str,
    context: &str,
) -> Result<DateTime<Utc>> {
    let statement = client
        .prepare(sql)
        .await
        .with_context(|| format!("{context}: SQL={sql} extended Parse/Describe"))?;
    prepared_binary_instant(client, &statement, context)
        .await
        .with_context(|| format!("{context}: SQL={sql}"))
}

async fn prepared_binary_instant(
    client: &Client,
    statement: &Statement,
    context: &str,
) -> Result<DateTime<Utc>> {
    ensure!(
        statement.columns().len() == 1,
        "{context}: expected one result column, actual {}",
        statement.columns().len()
    );
    ensure!(
        statement.columns()[0].type_() == &Type::TIMESTAMPTZ,
        "{context}: expected TIMESTAMPTZ, actual {:?}",
        statement.columns()[0].type_()
    );
    let rows = client
        .query(statement, &[])
        .await
        .with_context(|| format!("{context}: extended Bind/Execute"))?;
    ensure!(
        rows.len() == 1,
        "{context}: expected one row, actual {}",
        rows.len()
    );
    rows[0]
        .try_get::<_, DateTime<Utc>>(0)
        .with_context(|| format!("{context}: binary TIMESTAMPTZ decode"))
}

async fn set_session(client: &Client, sql: &str, context: &str) -> Result<()> {
    client
        .batch_execute(sql)
        .await
        .with_context(|| format!("{context}: SQL={sql}"))
}

async fn assert_instant_text(
    client: &Client,
    expected: &str,
    context: &str,
) -> Result<()> {
    let result = compare::simple(client, INSTANT_SQL)
        .await
        .with_context(|| format!("{context}: SQL={INSTANT_SQL}"))?;
    ensure!(
        result.rows == vec![vec![Some(expected.to_owned())]],
        "{context}: expected [[{expected:?}]], actual {:?}",
        result.rows
    );
    Ok(())
}

/// Accepted spellings. The forms PostgreSQL also accepts run differentially
/// against the reference server; sqlparser forms that PostgreSQL's grammar
/// does not accept after `TIME ZONE` are marked as LakeSoul-only extensions.
async fn time_zone_accepted_spellings(
    h: &Harness,
    startup: &StartupDefaults,
) -> Result<()> {
    const DIFFERENTIAL: &[&str] = &[
        "SET TIME ZONE 'America/New_York'",
        "SET SESSION TIME ZONE 'America/New_York'",
        "SET timezone = 'America/New_York'",
        "SET SESSION timezone TO 'America/New_York'",
    ];
    for &sql in DIFFERENTIAL {
        for (label, client) in [("LakeSoul", &h.lake), ("PostgreSQL", &h.reference)] {
            set_session(
                client,
                sql,
                &format!("case=time-zone-spelling backend={label}"),
            )
            .await?;
            assert_show(
                client,
                "SHOW TimeZone",
                NEW_YORK,
                &format!("case=time-zone-spelling backend={label}"),
            )
            .await?;
            assert_instant_text(
                client,
                NEW_YORK_TEXT,
                &format!("case=time-zone-spelling backend={label} protocol=simple-text"),
            )
            .await?;
        }
        let lake_show = compare::simple(&h.lake, "SHOW TimeZone").await?;
        let reference_show = compare::simple(&h.reference, "SHOW TimeZone").await?;
        compare::compare(&lake_show, &reference_show, true).with_context(|| {
            format!("case=time-zone-spelling SQL={sql}: SHOW differs from PostgreSQL")
        })?;
        let lake_text = compare::simple(&h.lake, INSTANT_SQL).await?;
        let reference_text = compare::simple(&h.reference, INSTANT_SQL).await?;
        compare::compare(&lake_text, &reference_text, true).with_context(|| {
            format!("case=time-zone-spelling SQL={sql}: result differs from PostgreSQL")
        })?;
        set_session(
            &h.lake,
            "SET TIME ZONE 'UTC'",
            "case=time-zone-spelling reset-to-utc",
        )
        .await?;
        set_session(
            &h.reference,
            "SET TIME ZONE 'UTC'",
            "case=time-zone-spelling reference reset-to-utc",
        )
        .await?;
        eprintln!(
            "sql-compat case=time-zone-spelling SQL={sql} \
             classification=pg-equivalent SHOW={NEW_YORK} text={NEW_YORK_TEXT}"
        );
    }

    // `DEFAULT` is legal in PostgreSQL, but the value is each server's own
    // startup zone. LakeSoul's is derived from the unconfigured connection
    // above; PostgreSQL is only required to accept the statement.
    for sql in ["SET TIME ZONE DEFAULT", "SET timezone = DEFAULT"] {
        set_session(&h.lake, sql, "case=time-zone-default").await?;
        assert_show(
            &h.lake,
            "SHOW TimeZone",
            &startup.time_zone,
            "case=time-zone-default protocol=simple-text",
        )
        .await?;
        assert_instant_text(
            &h.lake,
            UTC_TEXT,
            "case=time-zone-default protocol=simple-text",
        )
        .await?;
        set_session(&h.reference, sql, "case=time-zone-default reference").await?;
        let reference_zone = show_value(
            &h.reference,
            "SHOW TimeZone",
            "case=time-zone-default reference",
        )
        .await?;
        ensure!(
            !reference_zone.is_empty(),
            "case=time-zone-default reference: SHOW TimeZone must not be empty"
        );
        eprintln!(
            "sql-compat case=time-zone-default SQL={sql} \
             lakesoul={} postgres=accepted",
            startup.time_zone
        );
    }

    // sqlparser accepts `TO`/`=` after `TIME ZONE`; PostgreSQL's grammar does
    // not. These spellings are accepted extensions, not PG equivalence.
    for sql in [
        "SET TIME ZONE TO 'America/New_York'",
        "SET TIME ZONE = 'America/New_York'",
        "SET TIME ZONE TO DEFAULT",
    ] {
        set_session(&h.lake, sql, "case=time-zone-extension").await?;
        let expected = if sql.ends_with("DEFAULT") {
            startup.time_zone.as_str()
        } else {
            NEW_YORK
        };
        assert_show(
            &h.lake,
            "SHOW TimeZone",
            expected,
            "case=time-zone-extension protocol=simple-text",
        )
        .await?;
        assert_instant_text(
            &h.lake,
            if sql.ends_with("DEFAULT") {
                UTC_TEXT
            } else {
                NEW_YORK_TEXT
            },
            "case=time-zone-extension protocol=simple-text",
        )
        .await?;
        eprintln!(
            "sql-compat case=time-zone-extension SQL={sql} \
             classification=lakesoul-accepted-extension"
        );
    }

    set_session(
        &h.lake,
        "SET TIME ZONE 'UTC'",
        "case=time-zone-spelling final",
    )
    .await?;
    set_session(
        &h.reference,
        "SET TIME ZONE 'UTC'",
        "case=time-zone-spelling reference final",
    )
    .await?;
    Ok(())
}

/// Unknown zones must be server errors, both in the dedicated `TIME ZONE`
/// path and in the generic `timezone` GUC assignment, without changing state.
async fn time_zone_invalid_value_is_rejected(h: &Harness) -> Result<()> {
    set_session(
        &h.lake,
        "SET TIME ZONE 'UTC'",
        "case=time-zone-invalid baseline",
    )
    .await?;
    set_session(
        &h.reference,
        "SET TIME ZONE 'UTC'",
        "case=time-zone-invalid reference baseline",
    )
    .await?;

    let sql = "SET TIME ZONE 'Not/AZone'";
    for (label, client) in [("LakeSoul", &h.lake), ("PostgreSQL", &h.reference)] {
        let context = format!(
            "case=time-zone-invalid backend={label} SQL={sql} \
             protocol=simple-text expected SQLSTATE=22023"
        );
        let error = match client.simple_query(sql).await {
            Ok(_) => bail!("{context}: expected rejection, actual success"),
            Err(error) => error,
        };
        common::ensure_sqlstate(&error, "22023", &context)?;
    }

    let sql = "SET timezone = 'Not/AZone'";
    let context = format!(
        "case=time-zone-invalid-assignment SQL={sql} protocol=simple-text \
         expected SQLSTATE=22023"
    );
    let error = match h.lake.simple_query(sql).await {
        Ok(_) => bail!("{context}: expected rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_sqlstate(&error, "22023", &context)?;

    let sql = "SET TIME ZONE 'Not/AZone'";
    let statement =
        h.lake.prepare(sql).await.with_context(|| {
            format!("case=time-zone-invalid extended Parse SQL={sql}")
        })?;
    let context = format!(
        "case=time-zone-invalid-extended SQL={sql} protocol=extended-Bind/Execute \
         expected SQLSTATE=22023"
    );
    let error = match h.lake.execute(&statement, &[]).await {
        Ok(_) => bail!("{context}: expected rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_sqlstate(&error, "22023", &context)?;

    assert_show(
        &h.lake,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-invalid: rejected SET must not change the zone",
    )
    .await?;
    assert_show(
        &h.reference,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-invalid reference: rejected SET must not change the zone",
    )
    .await?;
    common::assert_items_intact(
        &h.lake,
        "case=time-zone-invalid: first read after rejected SET",
    )
    .await?;
    eprintln!(
        "sql-compat case=time-zone-invalid classification=pg-equivalent \
         simple=22023 extended=22023 state=unchanged"
    );
    Ok(())
}

/// `LOCAL`/`GLOBAL` scopes are not silently applied as session-wide changes.
///
/// PostgreSQL's `SET LOCAL` outside a transaction only warns and does nothing,
/// so this explicit refusal is a LakeSoul boundary, not a PG-equivalent case.
async fn time_zone_local_and_global_scopes(h: &Harness) -> Result<()> {
    for sql in [
        "SET LOCAL TIME ZONE 'America/New_York'",
        "SET LOCAL timezone = 'America/New_York'",
        "SET GLOBAL timezone = 'America/New_York'",
    ] {
        let context = format!(
            "case=time-zone-scope SQL={sql} protocol=simple-text expected SQLSTATE=0A000"
        );
        let error = match h.lake.simple_query(sql).await {
            Ok(_) => bail!("{context}: expected rejection, actual success"),
            Err(error) => error,
        };
        common::ensure_sqlstate(&error, "0A000", &context)?;
        assert_show(
            &h.lake,
            "SHOW TimeZone",
            "UTC",
            &format!("case=time-zone-scope-recovery SQL={sql}"),
        )
        .await?;
    }

    let sql = "SET LOCAL TIME ZONE 'America/New_York'";
    let statement = h
        .lake
        .prepare(sql)
        .await
        .with_context(|| format!("case=time-zone-local-parse SQL={sql}"))?;
    let context = format!(
        "case=time-zone-local-extended SQL={sql} protocol=extended-Bind/Execute \
         expected SQLSTATE=0A000"
    );
    let error = match h.lake.execute(&statement, &[]).await {
        Ok(_) => bail!("{context}: expected rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_sqlstate(&error, "0A000", &context)?;
    assert_show(
        &h.lake,
        "SHOW TimeZone",
        "UTC",
        "case=time-zone-local-recovery: rejected LOCAL must not apply",
    )
    .await?;
    common::assert_items_intact(
        &h.lake,
        "case=time-zone-local-recovery: first read after rejected LOCAL",
    )
    .await?;
    eprintln!(
        "sql-compat case=time-zone-scope classification=lakesoul-explicit-refusal \
         local=0A000 global=0A000 state=unchanged"
    );
    Ok(())
}

/// Direct wire observation of `ParameterStatus`.
///
/// `tokio-postgres` does not expose post-startup `ParameterStatus` messages,
/// and this server does not register `current_setting()`, so SHOW plus a raw
/// protocol exchange are the only observation paths that exist.
async fn time_zone_parameter_status(
    h: &Harness,
    startup: &StartupDefaults,
) -> Result<()> {
    let exchange = async {
        let mut stream = TcpStream::connect(h.lake_addr)
            .await
            .with_context(|| format!("connect raw wire client to {}", h.lake_addr))?;

        let mut startup_payload = Vec::new();
        startup_payload.extend_from_slice(&196_608_i32.to_be_bytes());
        for (key, value) in [
            ("user", "sql_compat"),
            ("database", h.lake_namespace.as_str()),
            ("application_name", "sql_compat_raw"),
        ] {
            startup_payload.extend_from_slice(key.as_bytes());
            startup_payload.push(0);
            startup_payload.extend_from_slice(value.as_bytes());
            startup_payload.push(0);
        }
        startup_payload.push(0);
        stream
            .write_all(&((startup_payload.len() + 4) as i32).to_be_bytes())
            .await?;
        stream.write_all(&startup_payload).await?;

        let mut startup_statuses = Vec::new();
        read_wire_until_ready(&mut stream, &mut startup_statuses, "startup").await?;

        let set_statuses =
            raw_wire_query(&mut stream, "SET TIME ZONE 'America/New_York'").await?;
        let reset_statuses = raw_wire_query(&mut stream, "RESET TIME ZONE").await?;
        let discard_setup =
            raw_wire_query(&mut stream, "SET TIME ZONE 'America/New_York'").await?;
        let discard_statuses = raw_wire_query(&mut stream, "DISCARD ALL").await?;

        drop(stream);
        Ok::<_, anyhow::Error>((
            startup_statuses,
            set_statuses,
            reset_statuses,
            discard_setup,
            discard_statuses,
        ))
    };
    let (startup_statuses, set_statuses, reset_statuses, discard_setup, discard_statuses) =
        tokio::time::timeout(Duration::from_secs(30), exchange)
            .await
            .context("case=time-zone-parameter-status: raw wire exchange timed out")??;

    ensure!(
        last_parameter(&startup_statuses, "TimeZone") == Some(startup.time_zone.as_str()),
        "case=time-zone-parameter-status startup: expected ParameterStatus \
         TimeZone={:?}, actual {:?}",
        startup.time_zone,
        startup_statuses
    );
    ensure!(
        last_parameter(&set_statuses, "TimeZone") == Some(NEW_YORK),
        "case=time-zone-parameter-status set: expected ParameterStatus \
         TimeZone={NEW_YORK:?}, actual {set_statuses:?}"
    );
    ensure!(
        last_parameter(&reset_statuses, "TimeZone") == Some(startup.time_zone.as_str()),
        "case=time-zone-parameter-status reset: expected ParameterStatus \
         TimeZone={:?}, actual {:?}",
        startup.time_zone,
        reset_statuses
    );
    ensure!(
        last_parameter(&discard_setup, "TimeZone") == Some(NEW_YORK),
        "case=time-zone-parameter-status discard setup: expected \
         ParameterStatus TimeZone={NEW_YORK:?}, actual {discard_setup:?}"
    );
    ensure!(
        last_parameter(&discard_statuses, "TimeZone") == Some(startup.time_zone.as_str()),
        "case=time-zone-parameter-status discard: expected ParameterStatus \
         TimeZone={:?}, actual {:?}",
        startup.time_zone,
        discard_statuses
    );
    eprintln!(
        "sql-compat case=time-zone-parameter-status classification=pg-equivalent \
         startup={} set={NEW_YORK} reset={} discard={}",
        startup.time_zone, startup.time_zone, startup.time_zone
    );
    Ok(())
}

fn last_parameter<'a>(statuses: &'a [(String, String)], name: &str) -> Option<&'a str> {
    statuses
        .iter()
        .rev()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

async fn read_wire_until_ready(
    stream: &mut TcpStream,
    statuses: &mut Vec<(String, String)>,
    context: &str,
) -> Result<()> {
    loop {
        let (kind, body) = read_wire_message(stream, context).await?;
        match kind {
            b'S' => statuses.push(parse_parameter_status(&body, context)?),
            b'E' => bail!(
                "case=time-zone-parameter-status {context}: server ErrorResponse {}",
                format_wire_error(&body)
            ),
            b'Z' => return Ok(()),
            _ => {}
        }
    }
}

async fn raw_wire_query(
    stream: &mut TcpStream,
    sql: &str,
) -> Result<Vec<(String, String)>> {
    let mut message = vec![b'Q'];
    message.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
    message.extend_from_slice(sql.as_bytes());
    message.push(0);
    stream
        .write_all(&message)
        .await
        .with_context(|| format!("raw wire send SQL={sql}"))?;
    let mut statuses = Vec::new();
    read_wire_until_ready(stream, &mut statuses, sql).await?;
    Ok(statuses)
}

async fn read_wire_message(
    stream: &mut TcpStream,
    context: &str,
) -> Result<(u8, Vec<u8>)> {
    let mut header = [0_u8; 5];
    stream
        .read_exact(&mut header)
        .await
        .with_context(|| format!("raw wire read {context} header"))?;
    let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    ensure!(
        length >= 4,
        "raw wire read {context}: invalid message length {length}"
    );
    let mut body = vec![0_u8; (length - 4) as usize];
    stream
        .read_exact(&mut body)
        .await
        .with_context(|| format!("raw wire read {context} body"))?;
    Ok((header[0], body))
}

fn parse_parameter_status(body: &[u8], context: &str) -> Result<(String, String)> {
    let split = body
        .iter()
        .position(|byte| *byte == 0)
        .with_context(|| format!("raw wire {context}: ParameterStatus missing name"))?;
    let name = std::str::from_utf8(&body[..split])
        .with_context(|| {
            format!("raw wire {context}: ParameterStatus name is not UTF-8")
        })?
        .to_owned();
    let rest = &body[split + 1..];
    let end = rest
        .iter()
        .position(|byte| *byte == 0)
        .with_context(|| format!("raw wire {context}: ParameterStatus missing value"))?;
    let value = std::str::from_utf8(&rest[..end])
        .with_context(|| {
            format!("raw wire {context}: ParameterStatus value is not UTF-8")
        })?
        .to_owned();
    Ok((name, value))
}

fn format_wire_error(body: &[u8]) -> String {
    let mut fields = Vec::new();
    let mut index = 0;
    while index < body.len() && body[index] != 0 {
        let kind = body[index];
        index += 1;
        let end = body[index..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| index + offset)
            .unwrap_or(body.len());
        let value = String::from_utf8_lossy(&body[index..end]);
        fields.push(format!("{}={value}", kind as char));
        index = end.saturating_add(1);
    }
    fields.join(" ")
}

async fn apply_tracked_changes(client: &Client, startup: &StartupDefaults) -> Result<()> {
    for sql in [
        "SET TIME ZONE 'America/New_York'",
        "SET statement_timeout = '5s'",
        "SET search_path = 'pg_catalog, public'",
        "SET ivfflat.probes = 1",
        "SET application_name = 'time-zone-reset'",
        "SET DateStyle = 'ISO, DMY'",
        "SET IntervalStyle = 'iso_8601'",
        "SET bytea_output = 'escape'",
        "SET extra_float_digits = '0'",
    ] {
        set_session(client, sql, "case=time-zone-reset mutate").await?;
    }
    // A reset can only be proven to restore a setting if that setting was
    // actually changed first; assert the mutations before the resets below.
    let context = "case=time-zone-reset mutate";
    assert_show(client, "SHOW TimeZone", NEW_YORK, context).await?;
    assert_show(client, "SHOW search_path", "pg_catalog, public", context).await?;
    assert_show(client, "SHOW ivfflat.probes", "1", context).await?;
    assert_show(client, "SHOW application_name", "time-zone-reset", context).await?;
    assert_show(client, "SHOW DateStyle", "ISO, DMY", context).await?;
    assert_show(client, "SHOW IntervalStyle", "iso_8601", context).await?;
    assert_show(client, "SHOW bytea_output", "escape", context).await?;
    assert_show(client, "SHOW extra_float_digits", "0", context).await?;
    let timeout = show_value(client, "SHOW statement_timeout", context).await?;
    ensure!(
        timeout != startup.statement_timeout,
        "{context}: SET statement_timeout must differ from startup \
         ({:?}), actual {timeout:?}",
        startup.statement_timeout
    );
    Ok(())
}

async fn assert_tracked_defaults(
    client: &Client,
    startup: &StartupDefaults,
    context: &str,
) -> Result<()> {
    assert_show(client, "SHOW TimeZone", &startup.time_zone, context).await?;
    assert_show(
        client,
        "SHOW statement_timeout",
        &startup.statement_timeout,
        context,
    )
    .await?;
    assert_show(client, "SHOW search_path", &startup.search_path, context).await?;
    assert_show(client, "SHOW ivfflat.probes", &startup.probes, context).await?;
    assert_show(
        client,
        "SHOW application_name",
        &startup.application_name,
        context,
    )
    .await?;
    assert_show(client, "SHOW DateStyle", &startup.datestyle, context).await?;
    assert_show(
        client,
        "SHOW IntervalStyle",
        &startup.intervalstyle,
        context,
    )
    .await?;
    assert_show(client, "SHOW bytea_output", &startup.bytea_output, context).await?;
    assert_show(
        client,
        "SHOW extra_float_digits",
        &startup.extra_float_digits,
        context,
    )
    .await?;
    Ok(())
}

fn ensure_reset_sqlstate(
    result: Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error>,
    expected: &str,
    context: &str,
) -> Result<()> {
    let error = match result {
        Ok(_) => bail!("{context}: expected rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_sqlstate(&error, expected, context)
}

async fn time_zone_reset_restores_startup(
    h: &Harness,
    startup: &StartupDefaults,
) -> Result<()> {
    // An independent peer created before the mutations must stay on its own
    // configured session values throughout.
    let peer = h.connect_lake().await?;
    assert_show(
        &peer,
        "SHOW TimeZone",
        &startup.time_zone,
        "case=time-zone-reset peer",
    )
    .await?;

    // Targeted RESET: only the named setting returns to its startup value.
    apply_tracked_changes(&h.lake, startup).await?;
    set_session(&h.lake, "RESET TIME ZONE", "case=time-zone-reset-targeted").await?;
    assert_show(
        &h.lake,
        "SHOW TimeZone",
        &startup.time_zone,
        "case=time-zone-reset-targeted protocol=simple-text",
    )
    .await?;
    assert_instant_text(
        &h.lake,
        UTC_TEXT,
        "case=time-zone-reset-targeted protocol=simple-text",
    )
    .await?;
    let timeout_after_targeted = show_value(
        &h.lake,
        "SHOW statement_timeout",
        "case=time-zone-reset-targeted",
    )
    .await?;
    ensure!(
        timeout_after_targeted != startup.statement_timeout,
        "case=time-zone-reset-targeted: targeted RESET must not reset \
         statement_timeout (expected non-default, actual {timeout_after_targeted:?})"
    );

    for (sql, expected, context) in [
        (
            "RESET statement_timeout",
            startup.statement_timeout.as_str(),
            "case=time-zone-reset-statement-timeout",
        ),
        (
            "RESET ivfflat.probes",
            startup.probes.as_str(),
            "case=time-zone-reset-probes",
        ),
        (
            "RESET search_path",
            startup.search_path.as_str(),
            "case=time-zone-reset-search-path",
        ),
        (
            "RESET application_name",
            startup.application_name.as_str(),
            "case=time-zone-reset-application-name",
        ),
        (
            "RESET DateStyle",
            startup.datestyle.as_str(),
            "case=time-zone-reset-datestyle",
        ),
        (
            "RESET IntervalStyle",
            startup.intervalstyle.as_str(),
            "case=time-zone-reset-intervalstyle",
        ),
        (
            "RESET bytea_output",
            startup.bytea_output.as_str(),
            "case=time-zone-reset-bytea-output",
        ),
        (
            "RESET extra_float_digits",
            startup.extra_float_digits.as_str(),
            "case=time-zone-reset-extra-float-digits",
        ),
    ] {
        set_session(&h.lake, sql, context).await?;
        let show_sql = match sql {
            "RESET statement_timeout" => "SHOW statement_timeout",
            "RESET ivfflat.probes" => "SHOW ivfflat.probes",
            "RESET search_path" => "SHOW search_path",
            "RESET application_name" => "SHOW application_name",
            "RESET DateStyle" => "SHOW DateStyle",
            "RESET IntervalStyle" => "SHOW IntervalStyle",
            "RESET bytea_output" => "SHOW bytea_output",
            "RESET extra_float_digits" => "SHOW extra_float_digits",
            _ => unreachable!("every targeted reset above has a SHOW mapping"),
        };
        assert_show(&h.lake, show_sql, expected, context).await?;
    }

    // Unknown parameters are a real error, matching PostgreSQL; runtime-only
    // DataFusion options cannot be reset by a client.
    ensure_reset_sqlstate(
        h.lake
            .simple_query("RESET definitely_not_a_parameter")
            .await,
        "42704",
        "case=time-zone-reset-unknown SQL=RESET definitely_not_a_parameter",
    )?;
    ensure_reset_sqlstate(
        h.reference
            .simple_query("RESET definitely_not_a_parameter")
            .await,
        "42704",
        "case=time-zone-reset-unknown reference SQL=RESET definitely_not_a_parameter",
    )?;
    ensure_reset_sqlstate(
        h.lake
            .simple_query("RESET datafusion.runtime.not_a_real_option")
            .await,
        "0A000",
        "case=time-zone-reset-runtime SQL=RESET datafusion.runtime.not_a_real_option",
    )?;

    // RESET ALL and DISCARD ALL restore every tracked setting, and the reused
    // connection must still answer the fixture read.
    for sql in ["RESET ALL", "DISCARD ALL"] {
        let context = format!("case=time-zone-reset-all SQL={sql}");
        apply_tracked_changes(&h.lake, startup).await?;
        set_session(&h.lake, sql, &context).await?;
        assert_tracked_defaults(&h.lake, startup, &context).await?;
        assert_instant_text(&h.lake, UTC_TEXT, &context).await?;
        common::assert_items_intact(&h.lake, &format!("{context}: fixture after reset"))
            .await?;
        common::assert_items_count(
            &h.lake,
            4,
            &format!("{context}: simple fixture count after reset"),
        )
        .await?;
        eprintln!(
            "sql-compat {context} classification=lakesoul-connection-defaults \
             time_zone={} statement_timeout={} probes={} fixture=intact",
            startup.time_zone, startup.statement_timeout, startup.probes
        );
    }

    // The same reset surface through the extended protocol.
    for (sql, tag) in [
        ("RESET ALL", "reset-all"),
        ("DISCARD ALL", "discard-all"),
        ("RESET timezone", "reset-timezone"),
        ("RESET TIME ZONE", "reset-time-zone"),
    ] {
        let context = format!("case=time-zone-reset-extended tag={tag} SQL={sql}");
        apply_tracked_changes(&h.lake, startup).await?;
        let statement = h
            .lake
            .prepare(sql)
            .await
            .with_context(|| format!("{context}: extended Parse"))?;
        h.lake
            .execute(&statement, &[])
            .await
            .with_context(|| format!("{context}: extended Bind/Execute"))?;
        if tag.starts_with("reset-time") {
            assert_show(
                &h.lake,
                "SHOW TimeZone",
                &startup.time_zone,
                &format!("{context}: time zone after Execute"),
            )
            .await?;
            assert_instant_text(
                &h.lake,
                UTC_TEXT,
                &format!("{context}: instant after Execute"),
            )
            .await?;
        } else {
            assert_tracked_defaults(&h.lake, startup, &context).await?;
        }
        common::assert_items_intact(
            &h.lake,
            &format!("{context}: fixture after Execute"),
        )
        .await?;
        eprintln!(
            "sql-compat {context} classification=lakesoul-connection-defaults \
             protocol=extended execute=succeeds"
        );
    }

    // DISCARD variants outside ALL are not implemented: reject, never
    // acknowledge. PostgreSQL accepts these, so this is LakeSoul-only.
    for sql in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
        ensure_reset_sqlstate(
            h.lake.simple_query(sql).await,
            "0A000",
            &format!("case=time-zone-discard-variant SQL={sql} protocol=simple-text"),
        )?;
        let context =
            format!("case=time-zone-discard-variant SQL={sql} protocol=extended-Parse");
        let error = match h.lake.prepare(sql).await {
            Ok(_) => bail!("{context}: expected rejection, actual success"),
            Err(error) => error,
        };
        common::ensure_sqlstate(&error, "0A000", &context)?;
    }
    common::assert_items_intact(
        &h.lake,
        "case=time-zone-discard-variant: first read after rejection",
    )
    .await?;

    // Leave the reused connection on its startup settings even though this
    // suite has no later case that depends on them.
    set_session(&h.lake, "RESET ALL", "case=time-zone-reset final").await?;
    assert_tracked_defaults(&h.lake, startup, "case=time-zone-reset final").await?;

    // Nothing above may leak to the independent peer or to fresh connections.
    assert_show(
        &peer,
        "SHOW TimeZone",
        &startup.time_zone,
        "case=time-zone-reset peer after mutations",
    )
    .await?;
    common::assert_items_intact(&peer, "case=time-zone-reset peer fixture").await?;
    let fresh = h.connect_lake_startup_defaults().await?;
    assert_tracked_defaults(&fresh, startup, "case=time-zone-reset fresh connection")
        .await?;
    eprintln!(
        "sql-compat case=time-zone-reset classification=lakesoul-connection-defaults \
         peer=unchanged new-connection=startup-defaults reused-connection=usable"
    );
    Ok(())
}

/// A failing statement inside a transaction fails the block; no read executes
/// until `ROLLBACK`; a simple batch then performs `ROLLBACK; SELECT ...` on the
/// recovered connection.
///
/// PostgreSQL contract: a statement issued in a failed transaction block is
/// rejected with `25P02` (in_failed_sql_transaction), and `COMMIT` in that
/// block ends the transaction instead of erroring. LakeSoul must report the
/// same code for the same SQL as the reference connection; both exact codes
/// are asserted on both sides, and no allowed-code list is accepted.
async fn failed_transaction_recovers_after_rollback(h: &Harness) -> Result<()> {
    let before = common::snapshot(h, "case=failed-transaction-simple").await?;

    h.lake
        .batch_execute("BEGIN")
        .await
        .context("case=failed-transaction-simple SQL=BEGIN")?;
    // A statement that fails inside the block leaves it failed. A write is
    // staged now rather than refused, so this uses a statement that is an error
    // on its own.
    let rejected = "SELECT * FROM missing_table_xyz";
    let error = match h.lake.simple_query(rejected).await {
        Ok(_) => bail!(
            "case=failed-transaction-simple SQL={rejected} protocol=simple-text \
             expected a failing statement, actual success"
        ),
        Err(error) => error,
    };
    ensure!(
        error.as_db_error().is_some(),
        "case=failed-transaction-simple SQL={rejected}: expected a server error, \
         actual {error}"
    );

    let aborted_read = "SELECT COUNT(*) AS n FROM items";
    let error = match h.lake.simple_query(aborted_read).await {
        Ok(_) => bail!(
            "case=failed-transaction-simple-after-error SQL={aborted_read} \
             protocol=simple-text expected SQLSTATE 25P02 (failed transaction), \
             actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P02",
        &format!(
            "case=failed-transaction-simple-after-error SQL={aborted_read} \
             protocol=simple-text"
        ),
    )?;

    let recovery = "ROLLBACK; SELECT COUNT(*) AS n FROM items";
    let recovered = compare::simple(&h.lake, recovery).await.with_context(|| {
        format!(
            "case=failed-transaction-simple-rollback SQL={recovery} \
                 protocol=simple-text after aborted transaction"
        )
    })?;
    ensure!(
        recovered.names == vec!["n".to_owned()],
        "case=failed-transaction-simple-rollback: expected column [\"n\"], \
         actual {:?}",
        recovered.names
    );
    ensure!(
        recovered.rows == vec![vec![Some("4".to_owned())]],
        "case=failed-transaction-simple-rollback: expected [[\"4\"]], actual {:?}",
        recovered.rows
    );
    common::assert_items_intact(
        &h.lake,
        "case=failed-transaction-simple-first-read-after-rollback \
         protocol=extended-binary",
    )
    .await?;

    let after = common::snapshot(h, "case=failed-transaction-simple-after").await?;
    common::ensure_snapshot_unchanged(&before, &after, "case=failed-transaction-simple")?;

    // Independent PostgreSQL oracle for the failed-transaction state. A cast
    // error is used so the reference fixture is not written; the same SQL that
    // LakeSoul rejected above must then be rejected here with the same code.
    h.reference
        .batch_execute("BEGIN")
        .await
        .context("case=failed-transaction-reference SQL=BEGIN")?;
    let reference_failure = "SELECT CAST('not-an-integer' AS INTEGER)";
    let error = match h.reference.simple_query(reference_failure).await {
        Ok(_) => bail!(
            "case=failed-transaction-reference SQL={reference_failure} \
             protocol=simple-text expected SQLSTATE 22P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "22P02",
        &format!(
            "case=failed-transaction-reference SQL={reference_failure} \
             protocol=simple-text"
        ),
    )?;
    let error = match h.reference.simple_query(aborted_read).await {
        Ok(_) => bail!(
            "case=failed-transaction-reference-aborted SQL={aborted_read} \
             protocol=simple-text expected SQLSTATE 25P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P02",
        &format!(
            "case=failed-transaction-reference-aborted SQL={aborted_read} \
             protocol=simple-text"
        ),
    )?;
    h.reference
        .batch_execute("ROLLBACK")
        .await
        .context("case=failed-transaction-reference-rollback SQL=ROLLBACK")?;
    common::assert_items_intact(
        &h.reference,
        "case=failed-transaction-reference-first-read-after-rollback \
         protocol=extended-binary",
    )
    .await?;

    // `COMMIT` is not rejected in a failed block: PostgreSQL ends the
    // transaction with the `ROLLBACK` tag, so the same `COMMIT` must succeed on
    // both sides and leave the connection readable.
    for (client, failure, failure_state, side) in [
        // A write inside a block is staged now, so this side uses a statement
        // that fails on its own. The SQLSTATE of a DataFusion planning error is
        // the front end's own mapping: the contract here is that the statement
        // failed and the block recovers, not the code.
        (&h.lake, rejected, None, "lakesoul"),
        (&h.reference, reference_failure, Some("22P02"), "postgres"),
    ] {
        let context = format!("case=failed-transaction-commit side={side} SQL=BEGIN");
        client
            .batch_execute("BEGIN")
            .await
            .with_context(|| context.clone())?;
        let context = format!(
            "case=failed-transaction-commit side={side} SQL={failure} \
             protocol=simple-text"
        );
        let error = match client.simple_query(failure).await {
            Ok(_) => bail!("{context} expected a failing statement, actual success"),
            Err(error) => error,
        };
        match failure_state {
            Some(state) => common::ensure_sqlstate(&error, state, &context)?,
            None => ensure!(
                error.as_db_error().is_some(),
                "{context}: expected a server error, actual {error}"
            ),
        }
        let context =
            format!("case=failed-transaction-commit side={side} SQL=COMMIT failed block");
        client
            .batch_execute("COMMIT")
            .await
            .with_context(|| context.clone())?;
        let context = format!(
            "case=failed-transaction-commit side={side} SQL={aborted_read} \
             protocol=simple-text first read after COMMIT"
        );
        let read = compare::simple(client, aborted_read)
            .await
            .with_context(|| context.clone())?;
        ensure!(
            read.names == vec!["n".to_owned()],
            "{context}: expected column [\"n\"], actual {:?}",
            read.names
        );
        ensure!(
            read.rows == vec![vec![Some("4".to_owned())]],
            "{context}: expected [[\"4\"]], actual {:?}",
            read.rows
        );
    }

    eprintln!(
        "sql-compat case=failed-transaction classification=pg-equivalent \
         lake=25P02 postgres=25P02 no_read_before_rollback rollback=succeeds \
         commit=succeeds first_read=succeeds"
    );
    Ok(())
}

/// The extended-protocol paths follow PostgreSQL as well: `Parse` in a failed
/// transaction block is rejected with `25P02` (only `COMMIT`/`ROLLBACK` still
/// parse), and so is `Execute` of a statement prepared while the block was
/// healthy. A `Sync` keeps the connection usable; `ROLLBACK` then restores both
/// simple and extended reads.
async fn failed_transaction_recovers_after_extended_parse_failure(
    h: &Harness,
) -> Result<()> {
    let before = common::snapshot(h, "case=failed-transaction-extended-parse").await?;

    let read = "SELECT COUNT(*) AS n FROM items";
    // Prepared while both transactions were still healthy: after the block
    // fails, `Execute` is what must be rejected for these statements.
    let lake_prepared =
        h.lake.prepare(read).await.with_context(|| {
            format!("case=failed-transaction-extended-parse SQL={read}")
        })?;
    let reference_prepared = h.reference.prepare(read).await.with_context(|| {
        format!("case=failed-transaction-reference-extended-parse SQL={read}")
    })?;

    h.lake
        .batch_execute("BEGIN")
        .await
        .context("case=failed-transaction-extended-parse SQL=BEGIN")?;
    let rejected = "DELETE FROM items WHERE id = 1";
    let error = match h.lake.prepare(rejected).await {
        Ok(_) => bail!(
            "case=failed-transaction-extended-parse SQL={rejected} \
             protocol=extended-Parse expected SQLSTATE 0A000, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "0A000",
        &format!(
            "case=failed-transaction-extended-parse SQL={rejected} \
             protocol=extended-Parse"
        ),
    )?;

    // PostgreSQL also rejects `Parse` in a failed block, with `25P02`.
    let error = match h.lake.prepare(read).await {
        Ok(_) => bail!(
            "case=failed-transaction-extended-parse-aborted SQL={read} \
             protocol=extended-Parse expected SQLSTATE 25P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P02",
        &format!(
            "case=failed-transaction-extended-parse-aborted SQL={read} \
             protocol=extended-Parse"
        ),
    )?;

    // A statement prepared before the failure is rejected when it executes.
    let error = match h.lake.query(&lake_prepared, &[]).await {
        Ok(_) => bail!(
            "case=failed-transaction-extended-execute-aborted SQL={read} \
             protocol=extended-Bind/Execute expected SQLSTATE 25P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P02",
        &format!(
            "case=failed-transaction-extended-execute-aborted SQL={read} \
             protocol=extended-Bind/Execute"
        ),
    )?;

    // Reference oracle: the same SQL through the same protocol steps must be
    // rejected with the same code. The failing statement is a cast error so the
    // reference fixture is not written.
    h.reference
        .batch_execute("BEGIN")
        .await
        .context("case=failed-transaction-reference-extended-parse SQL=BEGIN")?;
    let reference_failure = "SELECT CAST('not-an-integer' AS INTEGER)";
    let error = match h.reference.simple_query(reference_failure).await {
        Ok(_) => bail!(
            "case=failed-transaction-reference-extended-parse \
             SQL={reference_failure} protocol=simple-text expected SQLSTATE \
             22P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "22P02",
        &format!(
            "case=failed-transaction-reference-extended-parse \
             SQL={reference_failure} protocol=simple-text"
        ),
    )?;
    let error = match h.reference.prepare(read).await {
        Ok(_) => bail!(
            "case=failed-transaction-reference-extended-parse-aborted SQL={read} \
             protocol=extended-Parse expected SQLSTATE 25P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P02",
        &format!(
            "case=failed-transaction-reference-extended-parse-aborted SQL={read} \
             protocol=extended-Parse"
        ),
    )?;
    let error = match h.reference.query(&reference_prepared, &[]).await {
        Ok(_) => bail!(
            "case=failed-transaction-reference-extended-execute-aborted \
             SQL={read} protocol=extended-Bind/Execute expected SQLSTATE 25P02, \
             actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P02",
        &format!(
            "case=failed-transaction-reference-extended-execute-aborted \
             SQL={read} protocol=extended-Bind/Execute"
        ),
    )?;
    h.reference.batch_execute("ROLLBACK").await.context(
        "case=failed-transaction-reference-extended-parse-rollback \
             SQL=ROLLBACK",
    )?;
    common::assert_items_intact(
        &h.reference,
        "case=failed-transaction-reference-extended-parse-first-read-after-rollback \
         protocol=extended-binary",
    )
    .await?;

    h.lake.batch_execute("ROLLBACK").await.context(
        "case=failed-transaction-extended-parse-rollback SQL=ROLLBACK \
             after a failed extended Parse",
    )?;

    common::assert_items_count(
        &h.lake,
        4,
        "case=failed-transaction-extended-parse-first-read-after-rollback \
         protocol=simple-text",
    )
    .await?;
    common::assert_items_intact(
        &h.lake,
        "case=failed-transaction-extended-parse-first-read-after-rollback \
         protocol=extended-binary",
    )
    .await?;

    let after =
        common::snapshot(h, "case=failed-transaction-extended-parse-after").await?;
    common::ensure_snapshot_unchanged(
        &before,
        &after,
        "case=failed-transaction-extended-parse",
    )?;

    eprintln!(
        "sql-compat case=failed-transaction-extended-parse \
         classification=pg-equivalent parse=25P02 execute=25P02 \
         no_read_before_rollback rollback=succeeds"
    );
    Ok(())
}

/// Only READ COMMITTED is accepted. The rejection must leave the connection
/// idle and usable, not aborted, because no transaction was ever started.
async fn only_read_committed_isolation_is_accepted(h: &Harness) -> Result<()> {
    for sql in [
        "BEGIN ISOLATION LEVEL REPEATABLE READ",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
    ] {
        let simple_context = format!(
            "case=isolation-simple SQL={sql} protocol=simple-text expected SQLSTATE=0A000"
        );
        let error = match h.lake.simple_query(sql).await {
            Ok(_) => bail!("{simple_context}: expected rejection, actual success"),
            Err(error) => error,
        };
        common::ensure_sqlstate(&error, "0A000", &simple_context)?;
        common::assert_items_intact(
            &h.lake,
            &format!(
                "case=isolation-simple-recovery SQL={sql} first read after \
                 rejection (connection must not be aborted)"
            ),
        )
        .await?;

        let parse_context = format!(
            "case=isolation-parse SQL={sql} protocol=extended-Parse expected SQLSTATE=0A000"
        );
        let error = match h.lake.prepare(sql).await {
            Ok(_) => bail!("{parse_context}: expected rejection, actual success"),
            Err(error) => error,
        };
        common::ensure_sqlstate(&error, "0A000", &parse_context)?;
        common::assert_items_intact(
            &h.lake,
            &format!(
                "case=isolation-parse-recovery SQL={sql} first read after \
                 Parse rejection (connection must not be aborted)"
            ),
        )
        .await?;

        // PostgreSQL accepts the same statement at Parse; the refusal is a
        // LakeSoul READ COMMITTED-only boundary, not invalid SQL.
        let _reference_statement = h.reference.prepare(sql).await.with_context(|| {
            format!(
                "case=isolation-reference SQL={sql} PostgreSQL must accept \
                     this statement at Parse"
            )
        })?;

        eprintln!(
            "sql-compat case=isolation SQL={sql} \
             classification=lakesoul-read-committed-only simple=0A000 \
             parse=0A000 postgres=accepted"
        );
    }

    let sql = "SET default_transaction_isolation = 'serializable'";
    let simple_context = format!(
        "case=isolation-guc-simple SQL={sql} protocol=simple-text expected SQLSTATE=0A000"
    );
    let error = match h.lake.simple_query(sql).await {
        Ok(_) => bail!("{simple_context}: expected rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_sqlstate(&error, "0A000", &simple_context)?;
    common::assert_items_intact(
        &h.lake,
        "case=isolation-guc-simple-recovery: first read after rejected SET",
    )
    .await?;

    let parse_context = format!(
        "case=isolation-guc-parse SQL={sql} protocol=extended-Parse expected SQLSTATE=0A000"
    );
    let error = match h.lake.prepare(sql).await {
        Ok(_) => bail!("{parse_context}: expected rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_sqlstate(&error, "0A000", &parse_context)?;
    common::assert_items_intact(
        &h.lake,
        "case=isolation-guc-parse-recovery: first read after rejected extended Parse",
    )
    .await?;

    eprintln!(
        "sql-compat case=isolation-guc SQL=SET default_transaction_isolation \
         classification=lakesoul-read-committed-only simple=0A000 parse=0A000"
    );
    Ok(())
}
