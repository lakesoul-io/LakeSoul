// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Read-only policy refusals and parameter-error boundaries.

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::types::{ToSql, Type};

use crate::common;
use crate::support::{Harness, compare};

pub async fn run(h: &Harness) -> Result<()> {
    refused_writes_and_ddl(h).await?;
    refused_write_in_simple_batch(h).await?;
    parameter_error_boundaries(h).await?;
    Ok(())
}

struct Refusal {
    id: &'static str,
    sql: &'static str,
}

/// Every statement targets the live fixture or its schema, so an absent guard
/// would be observable as a changed row set, table, or warehouse file rather
/// than accidentally succeed against a missing object.
const REFUSALS: &[Refusal] = &[
    Refusal {
        id: "update",
        sql: "UPDATE items SET val = -1 WHERE id = 1",
    },
    Refusal {
        id: "delete",
        sql: "DELETE FROM items WHERE id = 1",
    },
    Refusal {
        id: "create-table-as",
        sql: "CREATE TABLE items_shadow AS SELECT id, val, label FROM items",
    },
    Refusal {
        id: "drop-view",
        sql: "DROP VIEW items",
    },
    Refusal {
        id: "truncate",
        sql: "TRUNCATE items",
    },
    Refusal {
        id: "copy-out",
        sql: "COPY items TO STDOUT",
    },
];

async fn refused_writes_and_ddl(h: &Harness) -> Result<()> {
    for case in REFUSALS {
        let before =
            common::snapshot(h, &format!("case=read-only-{} phase=before", case.id))
                .await?;

        let simple_context = format!(
            "case=read-only-{} SQL={} protocol=simple-text expected SQLSTATE=0A000 \
             before any side effect",
            case.id, case.sql
        );
        let error = match h.lake.simple_query(case.sql).await {
            Ok(_) => bail!("{simple_context}: expected rejection, actual success"),
            Err(error) => error,
        };
        common::ensure_sqlstate(&error, "0A000", &simple_context)?;
        common::assert_items_intact(
            &h.lake,
            &format!(
                "case=read-only-{} phase=after-simple protocol=extended-binary \
                 first query after rejection",
                case.id
            ),
        )
        .await?;
        let after_simple = common::snapshot(
            h,
            &format!("case=read-only-{} phase=after-simple", case.id),
        )
        .await?;
        common::ensure_snapshot_unchanged(&before, &after_simple, &simple_context)?;

        let parse_context = format!(
            "case=read-only-{} SQL={} protocol=extended-Parse expected SQLSTATE=0A000 \
             before Bind/Execute",
            case.id, case.sql
        );
        let error = match h.lake.prepare(case.sql).await {
            Ok(_) => bail!("{parse_context}: expected rejection, actual success"),
            Err(error) => error,
        };
        common::ensure_sqlstate(&error, "0A000", &parse_context)?;
        common::assert_items_intact(
            &h.lake,
            &format!(
                "case=read-only-{} phase=after-parse protocol=extended-binary \
                 first query after rejected Parse",
                case.id
            ),
        )
        .await?;
        let after_parse =
            common::snapshot(h, &format!("case=read-only-{} phase=after-parse", case.id))
                .await?;
        common::ensure_snapshot_unchanged(&before, &after_parse, &parse_context)?;

        // Reconnection is supplementary evidence; the authoritative snapshot
        // above is what proves the absence of a persistent side effect.
        let peer = h.connect_lake().await.with_context(|| {
            format!(
                "case=read-only-{} reconnect: open a fresh LakeSoul client",
                case.id
            )
        })?;
        common::assert_items_intact(
            &peer,
            &format!(
                "case=read-only-{} reconnect-read protocol=extended-binary",
                case.id
            ),
        )
        .await?;
        drop(peer);

        eprintln!(
            "sql-compat case=read-only-{} SQL={} classification=rejected \
             simple=0A000 parse=0A000 snapshot=unchanged reconnect=items-intact",
            case.id, case.sql
        );
    }
    Ok(())
}

/// A streamed batch proves the leading read ran before refusal. A trailing
/// supported timeout mutation is a visible sentinel: it must not run after error.
async fn refused_write_in_simple_batch(h: &Harness) -> Result<()> {
    const SQL: &str = "SELECT COUNT(*) AS n FROM items; \
                       INSERT INTO items (id, val, label) VALUES (98, 98, 'batch'); \
                       SET statement_timeout = '1ms'";
    let before = common::snapshot(h, "case=read-only-batch").await?;
    let budget_before = compare::simple(&h.lake, "SHOW statement_timeout").await?;
    let context = format!(
        "case=read-only-batch SQL={SQL} protocol=simple-text expected SQLSTATE=0A000 \
         after the leading read; the trailing SET must not run"
    );
    let (error, prefix_names, prefix_rows) = {
        let mut stream = std::pin::pin!(h.lake.simple_query_raw(SQL).await?);
        let mut names = Vec::new();
        let mut rows = Vec::new();
        let error = loop {
            match futures::StreamExt::next(&mut stream).await {
                Some(Ok(tokio_postgres::SimpleQueryMessage::RowDescription(columns))) => {
                    names = columns
                        .iter()
                        .map(|column| column.name().to_owned())
                        .collect::<Vec<_>>();
                }
                Some(Ok(tokio_postgres::SimpleQueryMessage::Row(row))) => {
                    rows.push(
                        (0..row.len())
                            .map(|i| row.get(i).map(str::to_owned))
                            .collect::<Vec<_>>(),
                    );
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => break error,
                None => bail!("{context}: expected rejection, actual success"),
            }
        };
        (error, names, rows)
    };
    ensure!(
        prefix_names == ["n"] && prefix_rows == vec![vec![Some("4".to_owned())]],
        "{context}: leading read must execute before refusal, actual names={prefix_names:?} rows={prefix_rows:?}"
    );
    common::ensure_sqlstate(&error, "0A000", &context)?;
    common::assert_items_intact(
        &h.lake,
        "case=read-only-batch-after protocol=extended-binary first query after batch",
    )
    .await?;
    let budget_after = compare::simple(&h.lake, "SHOW statement_timeout").await?;
    compare::compare(&budget_after, &budget_before, true)
        .with_context(|| format!("{context}: trailing SET changed the query budget"))?;
    let after = common::snapshot(h, "case=read-only-batch-after").await?;
    common::ensure_snapshot_unchanged(&before, &after, &context)?;

    let peer = h
        .connect_lake()
        .await
        .context("case=read-only-batch reconnect: open a fresh LakeSoul client")?;
    common::assert_items_intact(
        &peer,
        "case=read-only-batch reconnect-read protocol=extended-binary",
    )
    .await?;
    drop(peer);

    eprintln!(
        "sql-compat case=read-only-batch classification=rejected simple=0A000 \
         snapshot=unchanged reconnect=items-intact"
    );
    Ok(())
}

/// The mature client rejects wrong arity and wrong Rust parameter types before
/// sending Bind; the server rejects an explicit text-to-integer cast with its
/// own error mapping. Wrong-type *bytes* are not forged on the wire: the
/// distinction under test is exactly what a normal typed client can observe.
async fn parameter_error_boundaries(h: &Harness) -> Result<()> {
    let before = common::snapshot(h, "case=parameter-errors").await?;

    let sql =
        "SELECT id, val, label FROM items WHERE id = $1 ORDER BY val NULLS LAST, label";
    let statement = h
        .lake
        .prepare(sql)
        .await
        .context("case=parameter-client SQL={sql} extended Parse/Describe")?;
    ensure!(
        statement.params() == [Type::INT4],
        "case=parameter-client SQL={sql}: expected inferred parameter [INT4], actual {:?}",
        statement.params()
    );

    let no_params: &[&(dyn ToSql + Sync)] = &[];
    let context = "case=parameter-count-mismatch SQL=SELECT id, val, label FROM items WHERE id = $1 \
         params=[] protocol=extended-Bind expected client-side arity rejection";
    let error = match h.lake.query(&statement, no_params).await {
        Ok(_) => bail!("{context}: expected client-side rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_client_rejection(&error, context)?;
    common::assert_items_intact(
        &h.lake,
        "case=parameter-count-mismatch-after protocol=extended-binary connection intact",
    )
    .await?;

    let wrong_type_params: &[&(dyn ToSql + Sync)] = &[&"not-an-integer"];
    let context = "case=parameter-type-mismatch SQL=SELECT id, val, label FROM items WHERE id = $1 \
         params=[TEXT \"not-an-integer\"] for INT4 protocol=extended-Bind expected \
         client-side serialization rejection";
    let error = match h.lake.query(&statement, wrong_type_params).await {
        Ok(_) => bail!("{context}: expected client-side rejection, actual success"),
        Err(error) => error,
    };
    common::ensure_client_rejection(&error, context)?;
    common::assert_items_intact(
        &h.lake,
        "case=parameter-type-mismatch-after protocol=extended-binary connection intact",
    )
    .await?;

    // Explicit server-side cast error: the parameter really is TEXT, so the
    // driver serializes it and the server fails while executing the cast.
    let cast_sql = "SELECT CAST($1 AS INTEGER) AS n";
    let cast = h
        .lake
        .prepare_typed(cast_sql, &[Type::TEXT])
        .await
        .context(
            "case=parameter-server-cast SQL=SELECT CAST($1 AS INTEGER) extended Parse",
        )?;
    let params: &[&(dyn ToSql + Sync)] = &[&"not-an-integer"];
    let context = "case=parameter-server-cast SQL=SELECT CAST($1 AS INTEGER) AS n \
         params=[TEXT \"not-an-integer\"] protocol=extended-Bind/Execute expected \
         server error, not client rejection";
    let error = match h.lake.query(&cast, params).await {
        Ok(_) => bail!("{context}: expected server error, actual success"),
        Err(error) => error,
    };
    let db = error.as_db_error().with_context(|| {
        format!("{context}: expected a server ErrorResponse, actual client-side rejection: {error}")
    })?;
    ensure!(
        db.code().code() == "XX000",
        "{context}: LakeSoul maps DataFusion execution errors to XX000, \
         actual SQLSTATE {} ({})",
        db.code().code(),
        db.message()
    );
    common::assert_items_intact(
        &h.lake,
        "case=parameter-server-cast-after protocol=extended-binary connection intact",
    )
    .await?;

    // PostgreSQL uses 22P02 for the same input; the mapping is a LakeSoul
    // boundary, not claimed PG equivalence.
    let reference_cast = h
        .reference
        .prepare_typed(cast_sql, &[Type::TEXT])
        .await
        .context("case=parameter-server-cast-reference SQL=SELECT CAST($1 AS INTEGER) extended Parse")?;
    let error = match h.reference.query(&reference_cast, params).await {
        Ok(_) => bail!(
            "case=parameter-server-cast-reference SQL={cast_sql} params=[TEXT \
             \"not-an-integer\"] expected SQLSTATE 22P02, actual success"
        ),
        Err(error) => error,
    };
    common::ensure_sqlstate(
        &error,
        "22P02",
        "case=parameter-server-cast-reference SQL=SELECT CAST($1 AS INTEGER) \
         params=[TEXT \"not-an-integer\"]",
    )?;
    common::assert_items_intact(
        &h.reference,
        "case=parameter-server-cast-reference-after protocol=extended-binary \
         PostgreSQL connection intact",
    )
    .await?;

    let after = common::snapshot(h, "case=parameter-errors-after").await?;
    common::ensure_snapshot_unchanged(&before, &after, "case=parameter-errors")?;

    eprintln!(
        "sql-compat case=parameter-client classification=client-side-no-sqlstate \
         arity-intact type-intact; case=parameter-server-cast \
         classification=non-equivalent-sqlstate lake=XX000 postgres=22P02"
    );
    Ok(())
}
