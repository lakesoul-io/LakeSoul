// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Connection-local session state and transaction recovery.

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use tokio_postgres::Client;
use tokio_postgres::types::Type;

use crate::common;
use crate::support::{Harness, compare};

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

/// `SET TIME ZONE` changes the rendered result on the issuing connection only.
///
/// Casting a naive timestamp uses the connection's timezone. The same wall
/// clock therefore denotes different instants after SET. Check text rendering,
/// binary instants, and an already-open peer with unchanged UTC settings.
pub async fn time_zone_is_connection_local(h: &Harness) -> Result<()> {
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

    // Create the peer before the mutation: configuring a new peer afterwards
    // could reset shared state and accidentally hide cross-connection leakage.
    let peer = h.connect_lake().await?;

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
    drop(peer);

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

    eprintln!(
        "sql-compat case=time-zone classification=pg-equivalent \
         utc={UTC_TEXT} new_york={NEW_YORK_TEXT} peer=utc restored=utc"
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
    ensure!(
        statement.columns().len() == 1,
        "{context}: SQL={sql} expected one result column, actual {}",
        statement.columns().len()
    );
    ensure!(
        statement.columns()[0].type_() == &Type::TIMESTAMPTZ,
        "{context}: SQL={sql} expected TIMESTAMPTZ, actual {:?}",
        statement.columns()[0].type_()
    );
    let rows = client
        .query(&statement, &[])
        .await
        .with_context(|| format!("{context}: SQL={sql} extended Bind/Execute"))?;
    ensure!(
        rows.len() == 1,
        "{context}: SQL={sql} expected one row, actual {}",
        rows.len()
    );
    rows[0]
        .try_get::<_, DateTime<Utc>>(0)
        .with_context(|| format!("{context}: SQL={sql} binary TIMESTAMPTZ decode"))
}

/// A refused write inside a transaction aborts it; no read executes until
/// `ROLLBACK`; a simple batch then performs `ROLLBACK; SELECT ...` on the
/// recovered connection.
///
/// Known non-PostgreSQL defect boundary: LakeSoul reports `25P01`
/// (no_active_sql_transaction) while PostgreSQL reports `25P02`
/// (in_failed_sql_transaction) for the same aborted state. Both exact codes are
/// asserted; this is a documented defect, not claimed PG compatibility, and no
/// allowed-code list is accepted.
async fn failed_transaction_recovers_after_rollback(h: &Harness) -> Result<()> {
    let before = common::snapshot(h, "case=failed-transaction-simple").await?;

    h.lake
        .batch_execute("BEGIN")
        .await
        .context("case=failed-transaction-simple SQL=BEGIN")?;
    let rejected = "INSERT INTO items (id, val, label) VALUES (97, 97, 'aborted')";
    let error = match h.lake.simple_query(rejected).await {
        Ok(_) => bail!(
            "case=failed-transaction-simple SQL={rejected} protocol=simple-text \
             expected SQLSTATE 0A000, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "0A000",
        &format!("case=failed-transaction-simple SQL={rejected} protocol=simple-text"),
    )?;

    let aborted_read = "SELECT COUNT(*) AS n FROM items";
    let error = match h.lake.simple_query(aborted_read).await {
        Ok(_) => bail!(
            "case=failed-transaction-simple-after-error SQL={aborted_read} \
             protocol=simple-text expected SQLSTATE 25P01 (aborted transaction), \
             actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P01",
        &format!(
            "case=failed-transaction-simple-after-error SQL={aborted_read} \
             protocol=simple-text; known non-PG SQLSTATE defect (PostgreSQL: 25P02)"
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
    // error is used so the reference fixture is not written; PostgreSQL must
    // reject the next command with its own exact code, 25P02.
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
    let error = match h.reference.simple_query("SELECT 1").await {
        Ok(_) => bail!(
            "case=failed-transaction-reference-aborted SQL=SELECT 1 \
             protocol=simple-text expected SQLSTATE 25P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P02",
        "case=failed-transaction-reference-aborted SQL=SELECT 1 protocol=simple-text",
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

    eprintln!(
        "sql-compat case=failed-transaction classification=non-pg-sqlstate-defect \
         lake=25P01 postgres=25P02 no_read_before_rollback rollback=succeeds \
         first_read=succeeds"
    );
    Ok(())
}

/// The extended-protocol Parse failure must be followed by a real `Sync`
/// recovery: the transaction remains aborted, a later Execute is rejected, and
/// `ROLLBACK` restores both simple and extended reads.
async fn failed_transaction_recovers_after_extended_parse_failure(
    h: &Harness,
) -> Result<()> {
    let before = common::snapshot(h, "case=failed-transaction-extended-parse").await?;

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

    // Parse of a read is not blocked while aborted; the following Execute is.
    let read = "SELECT COUNT(*) AS n FROM items";
    let statement = h.lake.prepare(read).await.with_context(|| {
        format!(
            "case=failed-transaction-extended-parse-recovery SQL={read} \
                 protocol=extended-Parse: connection must be usable after the \
                 failed Parse and its Sync"
        )
    })?;
    let error = match h.lake.query(&statement, &[]).await {
        Ok(_) => bail!(
            "case=failed-transaction-extended-parse-aborted SQL={read} \
             protocol=extended-Bind/Execute expected SQLSTATE 25P01, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "25P01",
        &format!(
            "case=failed-transaction-extended-parse-aborted SQL={read} \
             protocol=extended-Bind/Execute; known non-PG SQLSTATE defect \
             (PostgreSQL: 25P02)"
        ),
    )?;

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
         classification=non-pg-sqlstate-defect \
         parse=0A000 abort=25P01 no_read_before_rollback rollback=succeeds"
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
