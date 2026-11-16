// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#[path = "sql_compat/codecs.rs"]
mod codecs;
#[path = "sql_compat/semantics.rs"]
mod semantics;
pub mod support;

use anyhow::{Context, Result, ensure};
use support::{Harness, compare};
use tokio_postgres::types::{ToSql, Type};

struct Case {
    id: &'static str,
    sql: &'static str,
    columns: Vec<(&'static str, Type)>,
    ordered: bool,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_compat_phase_one() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            public_sql(h).await?;
            literal_type_boundary(h).await?;
            parameter_binding(h).await?;
            merge_across_commits(h).await?;
            codecs::run(h).await?;
            server_error_and_recovery(h).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_compat_phase_two() -> Result<()> {
    support::run_suite(|h| Box::pin(semantics::run(h))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decimal_zero_binary_scale_matches_postgres() -> Result<()> {
    support::run_suite(|h| Box::pin(codecs::zero_binary_scale(h))).await
}

/// Projection scalar subqueries count rows: zero -> typed NULL, one -> the
/// value, more than one -> execution-time SQLSTATE 21000. Explicit user
/// LIMIT 1 remains legal; prepared binds, cross-batch execution and explicit
/// transactions are covered in `semantics::projection_scalar_subquery_cardinality`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projection_scalar_subquery_cardinality() -> Result<()> {
    support::run_suite(|h| Box::pin(semantics::projection_scalar_subquery_cardinality(h)))
        .await
}

// Explicitly a support boundary, not PG type equivalence: DataFusion's bare
// integer literal widens COALESCE to INT8; PostgreSQL keeps this input INT4.
// The equivalent case above uses the same explicit INTEGER cast on both sides.
async fn literal_type_boundary(h: &Harness) -> Result<()> {
    let sql = "SELECT COALESCE(val, -1) AS filled FROM items";
    let context = || {
        format!(
            "case=literal-type-boundary SQL={sql} params=[] protocol=extended-binary expected LakeSoul=[INT8] PostgreSQL=[INT4]"
        )
    };
    let actual = compare::query_binary(&h.lake, sql)
        .await
        .with_context(context)?;
    let reference = compare::query_binary(&h.reference, sql)
        .await
        .with_context(context)?;
    compare::check_columns(&actual, &[("filled", Type::INT8)]).with_context(context)?;
    compare::check_columns(&reference, &[("filled", Type::INT4)])
        .with_context(context)?;
    compare::compare_rows(&actual.rows, &reference.rows, false).with_context(context)?;
    eprintln!(
        "PASS case=literal-type-boundary classification=non-equivalent-types lake=INT8 postgres=INT4"
    );
    Ok(())
}

async fn public_sql(h: &Harness) -> Result<()> {
    let cases = [
        Case {
            id: "null-expressions",
            sql: "SELECT id, val, label, val IS NULL AS missing, COALESCE(val, CAST(-1 AS INTEGER)) AS filled, CASE WHEN val IS NULL THEN 'missing' ELSE 'present' END AS status FROM items",
            columns: vec![
                ("id", Type::INT4),
                ("val", Type::INT4),
                ("label", Type::TEXT),
                ("missing", Type::BOOL),
                ("filled", Type::INT4),
                ("status", Type::TEXT),
            ],
            ordered: false,
        },
        Case {
            id: "null-three-valued-logic",
            sql: "SELECT id, (val = 10) AS equal_ten, (val = 10 OR val IS NULL) AS nullable_or, (val = 10 AND val IS NULL) AS nullable_and, id NOT IN (1, NULL) AS not_in FROM items",
            columns: vec![
                ("id", Type::INT4),
                ("equal_ten", Type::BOOL),
                ("nullable_or", Type::BOOL),
                ("nullable_and", Type::BOOL),
                ("not_in", Type::BOOL),
            ],
            ordered: false,
        },
        Case {
            id: "empty-aggregate",
            sql: "SELECT COUNT(*) AS all_count, COUNT(val) AS value_count, SUM(val) AS total, MIN(val) AS minimum, MAX(val) AS maximum FROM empty_items",
            columns: vec![
                ("all_count", Type::INT8),
                ("value_count", Type::INT8),
                ("total", Type::INT8),
                ("minimum", Type::INT4),
                ("maximum", Type::INT4),
            ],
            ordered: true,
        },
        Case {
            id: "all-null-aggregate",
            sql: "SELECT COUNT(*) AS all_count, COUNT(val) AS value_count, SUM(val) AS total FROM items WHERE val IS NULL",
            columns: vec![
                ("all_count", Type::INT8),
                ("value_count", Type::INT8),
                ("total", Type::INT8),
            ],
            ordered: true,
        },
        Case {
            id: "duplicate-null-key-join",
            sql: "SELECT l.id, l.label AS left_label, r.label AS right_label FROM items l JOIN items r ON l.id = r.id",
            columns: vec![
                ("id", Type::INT4),
                ("left_label", Type::TEXT),
                ("right_label", Type::TEXT),
            ],
            ordered: false,
        },
        Case {
            id: "duplicate-output-multiplicity",
            sql: "SELECT l.id FROM items l JOIN items r ON l.id = r.id",
            columns: vec![("id", Type::INT4)],
            ordered: false,
        },
    ];
    for case in cases {
        for mode in ["simple-text", "extended-binary"] {
            let context = || {
                format!(
                    "case={} SQL={} params=[] protocol={mode} expected columns={:?}",
                    case.id, case.sql, case.columns
                )
            };
            let (actual, expected) = if mode == "simple-text" {
                (
                    compare::simple(&h.lake, case.sql)
                        .await
                        .with_context(context)?,
                    compare::simple(&h.reference, case.sql)
                        .await
                        .with_context(context)?,
                )
            } else {
                (
                    compare::query_binary(&h.lake, case.sql)
                        .await
                        .with_context(context)?,
                    compare::query_binary(&h.reference, case.sql)
                        .await
                        .with_context(context)?,
                )
            };
            compare::check_columns(&actual, &case.columns).with_context(context)?;
            compare::check_columns(&expected, &case.columns).with_context(context)?;
            compare::compare(&actual, &expected, case.ordered).with_context(context)?;
            eprintln!("PASS case={} protocol={mode}", case.id);
        }
    }
    Ok(())
}

async fn parameter_binding(h: &Harness) -> Result<()> {
    // No cast on $1: Parse must infer INT4 from the persisted column.
    let sql =
        "SELECT id, val, label FROM items WHERE id = $1 ORDER BY val NULLS LAST, label";
    for explicit in [false, true] {
        let label = if explicit { "typed" } else { "inferred" };
        let context = || {
            format!(
                "case=parameter-{label} SQL={sql} protocol=extended-binary expected params=[INT4] columns=[INT4, INT4, TEXT]"
            )
        };
        let (lake, reference) = if explicit {
            (
                h.lake
                    .prepare_typed(sql, &[Type::INT4])
                    .await
                    .with_context(context)?,
                h.reference
                    .prepare_typed(sql, &[Type::INT4])
                    .await
                    .with_context(context)?,
            )
        } else {
            (
                h.lake.prepare(sql).await.with_context(context)?,
                h.reference.prepare(sql).await.with_context(context)?,
            )
        };
        ensure!(
            lake.params() == [Type::INT4],
            "{} actual params={:?}",
            context(),
            lake.params()
        );
        ensure!(
            reference.params() == [Type::INT4],
            "{} reference params={:?}",
            context(),
            reference.params()
        );
        // Same server-side statement, repeated binds including NULL and a
        // return to the first value: catches stale bound plans/results.
        for value in [Some(1_i32), Some(2), None, Some(1)] {
            let params: &[&(dyn ToSql + Sync)] = &[&value];
            let context = || {
                format!(
                    "case=parameter-{label} SQL={sql} params=[{value:?}] protocol=extended-binary expected columns=[INT4, INT4, TEXT]"
                )
            };
            let actual_rows = h.lake.query(&lake, params).await.with_context(context)?;
            let expected_rows = h
                .reference
                .query(&reference, params)
                .await
                .with_context(context)?;
            let actual = compare::binary(&lake, &actual_rows).with_context(context)?;
            let expected =
                compare::binary(&reference, &expected_rows).with_context(context)?;
            compare::check_columns(
                &actual,
                &[
                    ("id", Type::INT4),
                    ("val", Type::INT4),
                    ("label", Type::TEXT),
                ],
            )
            .with_context(context)?;
            compare::compare(&actual, &expected, true).with_context(context)?;
        }
        eprintln!(
            "PASS case=parameter-{label} protocol=extended-binary binds=1,2,NULL,1"
        );
    }
    Ok(())
}

async fn merge_across_commits(h: &Harness) -> Result<()> {
    let sql = "SELECT id, val, label FROM pk_items ORDER BY id";
    let mut final_rows: compare::Rows = [
        ("1", "99", "new"),
        ("2", "20", "keep"),
        ("3", "30", "added"),
    ]
    .into_iter()
    .map(|(id, val, label)| vec![Some(id.into()), Some(val.into()), Some(label.into())])
    .collect();
    if std::env::var("LAKESOUL_SQL_COMPAT_FAULT").as_deref() == Ok("expectation") {
        // Acceptance probe: corrupt the expected latest value, not the query
        // or an unconditional synthetic failure. The real comparator must fail.
        final_rows[0][1] = Some("10".into());
    }
    for mode in ["simple-text", "extended-binary"] {
        let context = || {
            format!(
                "case=pk-cross-commit SQL={sql} params=[] protocol={mode} expected columns=[INT8, INT8, TEXT]"
            )
        };
        let (actual, expected) = if mode == "simple-text" {
            (
                compare::simple(&h.lake, sql).await.with_context(context)?,
                compare::simple(&h.reference, sql)
                    .await
                    .with_context(context)?,
            )
        } else {
            (
                compare::query_binary(&h.lake, sql)
                    .await
                    .with_context(context)?,
                compare::query_binary(&h.reference, sql)
                    .await
                    .with_context(context)?,
            )
        };
        compare::check_columns(
            &actual,
            &[
                ("id", Type::INT8),
                ("val", Type::INT8),
                ("label", Type::TEXT),
            ],
        )
        .with_context(context)?;
        compare::compare(&actual, &expected, true).with_context(context)?;
        compare::compare_rows(&actual.rows, &final_rows, true).with_context(context)?;
        eprintln!("PASS case=pk-cross-commit protocol={mode}");
    }
    Ok(())
}

async fn server_error_and_recovery(h: &Harness) -> Result<()> {
    // No writes to fixtures: unsupported COPY is rejected before execution.
    // Full no-side-effect/write-policy coverage belongs to phase two.
    let sql = "COPY items TO STDOUT";
    let error = h.lake.simple_query(sql).await.err()
        .context("case=server-sqlstate SQL=COPY items TO STDOUT protocol=simple-text expected rejection, actual success")?;
    let db = error.as_db_error().context("case=server-sqlstate protocol=simple-text expected server ErrorResponse, not client rejection")?;
    ensure!(
        db.code().code() == "0A000",
        "case=server-sqlstate SQL={sql} params=[] protocol=simple-text expected SQLSTATE=0A000 actual={}",
        db.code().code()
    );
    let recovery_sql = "SELECT COUNT(*) AS n FROM items";
    let actual = compare::query_binary(&h.lake, recovery_sql)
        .await
        .context("case=server-sqlstate first query after error")?;
    compare::compare_rows(&actual.rows, &vec![vec![Some("4".into())]], true)?;
    eprintln!("PASS case=server-sqlstate SQLSTATE=0A000 first-query recovery=4");
    Ok(())
}
