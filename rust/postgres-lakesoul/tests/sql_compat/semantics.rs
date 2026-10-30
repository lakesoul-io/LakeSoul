// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Phase-two public SQL semantics over the real socket.
//!
//! Every case runs the *same explicit SQL text* against the LakeSoul server and
//! the PostgreSQL reference, once through the simple query protocol (server
//! text output) and once through the extended protocol (Describe OIDs plus
//! binary result decoding in [`compare`]). Fixtures come from the shared
//! `items`/`empty_items` tables, so this suite describes public SQL behavior
//! rather than codec reach.
//!
//! Contract classes used below:
//!
//! * `PG-equivalent`: identical rows, column names and OIDs on both servers.
//! * `support-boundary`: type/error mappings asserted separately from PG
//!   equivalence. Phase two adds the window `UInt64 -> numeric` mapping
//!   and DataFusion's generic `XX000` error mapping.
//! * Known semantic defects retain strict PG expectations in separately named,
//!   explicitly ignored reproductions, never assertions blessing bad values.
//!
//! No case mutates fixtures, skips a protocol, or accepts actual output as the
//! expected one.

use std::str::FromStr;

use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Error};

use crate::support::{Harness, compare};

/// One differential case: identical SQL, expected column names and OIDs, and
/// optionally the exact rows the contract requires.
struct Case {
    id: &'static str,
    sql: &'static str,
    columns: Vec<(&'static str, Type)>,
    ordered: bool,
    expected: Option<compare::Rows>,
}

pub async fn run(h: &Harness) -> Result<()> {
    aggregate_groups(h).await?;
    joins(h).await?;
    ordering_and_limits(h).await?;
    integers(h).await?;
    decimal_arithmetic(h).await?;
    cte_and_subqueries(h).await?;
    set_operations(h).await?;
    windows(h).await?;
    strings(h).await?;
    temporal(h).await?;
    arrays(h).await?;
    quoted_identifiers(h).await?;
    error_contracts(h).await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum Mode {
    Simple,
    Extended,
}

impl Mode {
    const BOTH: [Mode; 2] = [Mode::Simple, Mode::Extended];

    fn label(self) -> &'static str {
        match self {
            Mode::Simple => "simple-text",
            Mode::Extended => "extended-binary",
        }
    }
}

async fn fetch(client: &Client, sql: &str, mode: Mode) -> Result<compare::ResultSet> {
    match mode {
        Mode::Simple => compare::simple(client, sql).await,
        Mode::Extended => compare::query_binary(client, sql).await,
    }
}

/// Build the exact-rows expectation used by cases whose semantics must not be
/// inferred from the reference alone.
fn rows(cells: &[&[Option<&str>]]) -> compare::Rows {
    cells
        .iter()
        .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
        .collect()
}

/// Differential runner: both protocols, both servers, exact names and OIDs,
/// multiset comparison unless `ordered`, and an optional exact-row assertion.
async fn assert_sql(
    h: &Harness,
    id: &str,
    sql: &str,
    columns: &[(&str, Type)],
    ordered: bool,
    expected: Option<&compare::Rows>,
) -> Result<()> {
    for mode in Mode::BOTH {
        let context = || {
            format!(
                "case={id} SQL={sql} params=[] protocol={} expected columns={columns:?}",
                mode.label()
            )
        };
        let actual = fetch(&h.lake, sql, mode).await.with_context(context)?;
        let reference = fetch(&h.reference, sql, mode).await.with_context(context)?;
        compare::check_columns(&actual, columns).with_context(context)?;
        compare::check_columns(&reference, columns).with_context(context)?;
        compare::compare(&actual, &reference, ordered).with_context(context)?;
        if let Some(expected) = expected {
            compare::compare_rows(&actual.rows, expected, ordered)
                .with_context(context)?;
        }
        eprintln!("PASS case={id} protocol={}", mode.label());
    }
    Ok(())
}

async fn run_cases(h: &Harness, cases: Vec<Case>) -> Result<()> {
    for case in cases {
        assert_sql(
            h,
            case.id,
            case.sql,
            &case.columns,
            case.ordered,
            case.expected.as_ref(),
        )
        .await?;
    }
    Ok(())
}

async fn aggregate_groups(h: &Harness) -> Result<()> {
    run_cases(
        h,
        vec![
            Case {
                id: "group-null-key-having",
                sql: "SELECT val, COUNT(*) AS n FROM items \
                      GROUP BY val HAVING COUNT(*) > 1 ORDER BY val NULLS FIRST",
                columns: vec![("val", Type::INT4), ("n", Type::INT8)],
                ordered: true,
                expected: Some(rows(&[&[None, Some("2")]])),
            },
            Case {
                id: "group-null-aggregate-having",
                sql: "SELECT val, SUM(val) AS total FROM items \
                      GROUP BY val HAVING SUM(val) IS NOT NULL ORDER BY val NULLS FIRST",
                columns: vec![("val", Type::INT4), ("total", Type::INT8)],
                ordered: true,
                expected: Some(rows(&[
                    &[Some("10"), Some("10")],
                    &[Some("20"), Some("20")],
                ])),
            },
        ],
    )
    .await
}

async fn joins(h: &Harness) -> Result<()> {
    run_cases(
        h,
        vec![
            // The right side is derived from `items` (keys 2 and 20) so both
            // unmatched-left and unmatched-right rows are genuine.
            Case {
                id: "full-outer-join-unmatched",
                sql: "SELECT l.id AS lid, r.rid FROM items l \
                      FULL OUTER JOIN (\
                        SELECT id AS rid FROM items WHERE id = 2 \
                        UNION ALL \
                        SELECT val AS rid FROM items WHERE val = 20\
                      ) r ON l.id = r.rid \
                      ORDER BY lid NULLS FIRST, rid NULLS LAST",
                columns: vec![("lid", Type::INT4), ("rid", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[
                    &[None, Some("20")],
                    &[None, None],
                    &[Some("1"), None],
                    &[Some("1"), None],
                    &[Some("2"), Some("2")],
                ])),
            },
            // ON keeps every left row; WHERE would drop the unmatched ones.
            Case {
                id: "left-join-on-keeps-unmatched",
                sql: "SELECT l.id AS lid, r.rid FROM items l \
                      LEFT JOIN (SELECT id AS rid FROM items WHERE id = 2) r \
                        ON l.id = r.rid \
                      ORDER BY lid NULLS FIRST, rid NULLS LAST",
                columns: vec![("lid", Type::INT4), ("rid", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[
                    &[None, None],
                    &[Some("1"), None],
                    &[Some("1"), None],
                    &[Some("2"), Some("2")],
                ])),
            },
            Case {
                id: "left-join-where-filters-unmatched",
                sql: "SELECT l.id AS lid, r.rid FROM items l \
                      LEFT JOIN (SELECT id AS rid FROM items WHERE id = 2) r \
                        ON l.id = r.rid \
                      WHERE r.rid = 2 \
                      ORDER BY lid NULLS FIRST, rid NULLS LAST",
                columns: vec![("lid", Type::INT4), ("rid", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[&[Some("2"), Some("2")]])),
            },
        ],
    )
    .await
}

async fn ordering_and_limits(h: &Harness) -> Result<()> {
    let base = "SELECT id, val, label FROM items \
                ORDER BY id NULLS FIRST, val NULLS LAST, label NULLS FIRST";
    let columns = vec![
        ("id", Type::INT4),
        ("val", Type::INT4),
        ("label", Type::TEXT),
    ];
    let full = rows(&[
        &[None, None, None],
        &[Some("1"), Some("10"), Some("a")],
        &[Some("1"), None, Some("")],
        &[Some("2"), Some("20"), Some(" β ")],
    ]);
    assert_sql(
        h,
        "order-nulls-first-last",
        base,
        &columns,
        true,
        Some(&full),
    )
    .await?;

    let variants: [(&str, &str, compare::Rows); 4] = [
        ("limit-zero", " LIMIT 0", rows(&[])),
        ("offset-zero", " OFFSET 0", full.clone()),
        (
            "limit-offset",
            " LIMIT 2 OFFSET 1",
            rows(&[
                &[Some("1"), Some("10"), Some("a")],
                &[Some("1"), None, Some("")],
            ]),
        ),
        ("offset-past-end", " OFFSET 99", rows(&[])),
    ];
    for (id, suffix, expected) in &variants {
        let sql = format!("{base}{suffix}");
        assert_sql(h, id, &sql, &columns, true, Some(expected)).await?;
    }
    Ok(())
}

async fn integers(h: &Harness) -> Result<()> {
    assert_sql(
        h,
        "integer-division-and-bounds",
        "SELECT CAST(-7 AS INTEGER) / CAST(2 AS INTEGER) AS neg_quot, \
                CAST(-7 AS INTEGER) % CAST(2 AS INTEGER) AS neg_rem, \
                CAST(7 AS INTEGER) / CAST(-2 AS INTEGER) AS pos_neg_quot, \
                CAST(7 AS INTEGER) % CAST(-2 AS INTEGER) AS pos_neg_rem, \
                CAST(-2147483648 AS INTEGER) AS min_int4, \
                CAST(2147483647 AS INTEGER) AS max_int4, \
                CAST(-9223372036854775807 AS BIGINT) AS min_int8, \
                CAST(9223372036854775807 AS BIGINT) AS max_int8",
        &[
            ("neg_quot", Type::INT4),
            ("neg_rem", Type::INT4),
            ("pos_neg_quot", Type::INT4),
            ("pos_neg_rem", Type::INT4),
            ("min_int4", Type::INT4),
            ("max_int4", Type::INT4),
            ("min_int8", Type::INT8),
            ("max_int8", Type::INT8),
        ],
        true,
        Some(&rows(&[&[
            Some("-3"),
            Some("-1"),
            Some("-3"),
            Some("1"),
            Some("-2147483648"),
            Some("2147483647"),
            Some("-9223372036854775807"),
            Some("9223372036854775807"),
        ]])),
    )
    .await
}

const DECIMAL_SQL: &str = "SELECT \
     CAST('1.20' AS DECIMAL(6,2)) + CAST('2.30' AS DECIMAL(6,2)) AS sum_scale, \
     CAST('1.20' AS DECIMAL(6,2)) * CAST('2.30' AS DECIMAL(6,2)) AS product_scale, \
     CAST('123456.78' AS DECIMAL(10,2)) - CAST('0.08' AS DECIMAL(10,2)) AS diff_scale, \
     CAST('1.20' AS DECIMAL(6,2)) + CAST(NULL AS DECIMAL(6,2)) AS null_sum";

async fn decimal_arithmetic(h: &Harness) -> Result<()> {
    // Representative precision/scale arithmetic. The operands are nonzero on
    // purpose: the zero-scale binary defect is reproduced separately by the
    // ignored `decimal_zero_binary_scale_matches_postgres` phase-one case.
    assert_sql(
        h,
        "decimal-arithmetic",
        DECIMAL_SQL,
        &[
            ("sum_scale", Type::NUMERIC),
            ("product_scale", Type::NUMERIC),
            ("diff_scale", Type::NUMERIC),
            ("null_sum", Type::NUMERIC),
        ],
        true,
        Some(&rows(&[&[
            Some("3.50"),
            Some("2.7600"),
            Some("123456.70"),
            None,
        ]])),
    )
    .await?;

    // Exact decimal values *and* scales, decoded independently through
    // `rust_decimal` on both servers.
    let expected = [
        Some(Decimal::from_str("3.50")?),
        Some(Decimal::from_str("2.7600")?),
        Some(Decimal::from_str("123456.70")?),
        None,
    ];
    let mut decoded = Vec::new();
    for (side, client) in [("lake", &h.lake), ("reference", &h.reference)] {
        let context = format!(
            "case=decimal-arithmetic SQL={DECIMAL_SQL} params=[] protocol=extended-binary side={side} expected types=[NUMERIC; 4]"
        );
        let statement = client
            .prepare(DECIMAL_SQL)
            .await
            .with_context(|| format!("{context} extended Parse"))?;
        for column in statement.columns() {
            ensure!(
                column.type_() == &Type::NUMERIC,
                "{context}: column {} OID: expected NUMERIC, actual {:?}",
                column.name(),
                column.type_()
            );
        }
        let row = client
            .query_one(&statement, &[])
            .await
            .with_context(|| format!("{context} Bind/Execute"))?;
        let mut values = Vec::new();
        for index in 0..expected.len() {
            values.push(
                row.try_get::<_, Option<Decimal>>(index)
                    .with_context(|| format!("{context} decode column {index}"))?,
            );
        }
        ensure!(
            values == expected,
            "{context}: expected {expected:?}, actual {values:?}"
        );
        for (index, value) in values.iter().enumerate() {
            if let (Some(value), Some(expected)) = (value, expected[index]) {
                ensure!(
                    value.scale() == expected.scale(),
                    "{context}: column {index} scale: expected {}, actual {} (value {value})",
                    expected.scale(),
                    value.scale()
                );
            }
        }
        decoded.push(values);
    }
    ensure!(
        decoded[0] == decoded[1],
        "case=decimal-arithmetic value mismatch: lake={:?} reference={:?}",
        decoded[0],
        decoded[1]
    );
    eprintln!("PASS case=decimal-arithmetic decimal-scale=2,4,2,None");
    Ok(())
}

async fn cte_and_subqueries(h: &Harness) -> Result<()> {
    run_cases(
        h,
        vec![
            Case {
                id: "cte-grouped",
                sql: "WITH grouped AS (\
                        SELECT id, COUNT(*) AS n FROM items GROUP BY id\
                      ) \
                      SELECT id, n FROM grouped ORDER BY id NULLS FIRST",
                columns: vec![("id", Type::INT4), ("n", Type::INT8)],
                ordered: true,
                expected: Some(rows(&[
                    &[None, Some("1")],
                    &[Some("1"), Some("2")],
                    &[Some("2"), Some("1")],
                ])),
            },
            Case {
                id: "correlated-exists",
                sql: "SELECT i.id, i.val FROM items i \
                      WHERE EXISTS (\
                        SELECT 1 FROM items j \
                        WHERE j.id = i.id AND j.val IS NOT NULL\
                      ) \
                      ORDER BY i.id NULLS FIRST, i.val NULLS FIRST",
                columns: vec![("id", Type::INT4), ("val", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[
                    &[Some("1"), None],
                    &[Some("1"), Some("10")],
                    &[Some("2"), Some("20")],
                ])),
            },
            Case {
                id: "scalar-subquery-zero-rows",
                sql: "SELECT id, (SELECT val FROM items WHERE val > 1000) AS near \
                      FROM items ORDER BY id NULLS FIRST",
                columns: vec![("id", Type::INT4), ("near", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[
                    &[None, None],
                    &[Some("1"), None],
                    &[Some("1"), None],
                    &[Some("2"), None],
                ])),
            },
        ],
    )
    .await
}

async fn set_operations(h: &Harness) -> Result<()> {
    run_cases(
        h,
        vec![
            Case {
                id: "union-distinct",
                sql: "SELECT val FROM items UNION SELECT val FROM items ORDER BY val NULLS FIRST",
                columns: vec![("val", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[&[None], &[Some("10")], &[Some("20")]])),
            },
            Case {
                id: "union-all-duplicates",
                sql: "SELECT val FROM items UNION ALL SELECT val FROM items \
                      ORDER BY val NULLS FIRST",
                columns: vec![("val", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[
                    &[None],
                    &[None],
                    &[None],
                    &[None],
                    &[Some("10")],
                    &[Some("10")],
                    &[Some("20")],
                    &[Some("20")],
                ])),
            },
            Case {
                id: "intersect-null",
                sql: "SELECT val FROM items \
                      INTERSECT \
                      SELECT val FROM items WHERE val IS NULL OR val >= 20 \
                      ORDER BY val NULLS FIRST",
                columns: vec![("val", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[&[None], &[Some("20")]])),
            },
            Case {
                id: "except-null",
                sql: "SELECT val FROM items \
                      EXCEPT \
                      SELECT val FROM items WHERE val IS NOT NULL \
                      ORDER BY val NULLS FIRST",
                columns: vec![("val", Type::INT4)],
                ordered: true,
                expected: Some(rows(&[&[None]])),
            },
        ],
    )
    .await
}

async fn windows(h: &Harness) -> Result<()> {
    // Explicit `ROWS` frame, RANK ties on the duplicated `id = 1` key, and a
    // unique tie breaker for ROW_NUMBER. Rank/row_number are cast to BIGINT on
    // both sides so this case is a pure semantics check.
    assert_sql(
        h,
        "window-rank-frame",
        "SELECT id, val, \
                CAST(RANK() OVER (ORDER BY id NULLS LAST) AS BIGINT) AS rank_by_id, \
                CAST(ROW_NUMBER() OVER (ORDER BY id NULLS LAST, val NULLS LAST) AS BIGINT) AS row_number_unique, \
                CAST(SUM(val) OVER (\
                  PARTITION BY id ORDER BY val NULLS LAST \
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\
                ) AS BIGINT) AS running \
         FROM items ORDER BY id NULLS FIRST, val NULLS FIRST",
        &[
            ("id", Type::INT4),
            ("val", Type::INT4),
            ("rank_by_id", Type::INT8),
            ("row_number_unique", Type::INT8),
            ("running", Type::INT8),
        ],
        true,
        Some(&rows(&[
            &[None, None, Some("4"), Some("4"), None],
            &[Some("1"), None, Some("1"), Some("2"), Some("10")],
            &[Some("1"), Some("10"), Some("1"), Some("1"), Some("10")],
            &[Some("2"), Some("20"), Some("3"), Some("3"), Some("20")],
        ])),
    )
    .await?;

    // The uncast window counters expose the wire-type boundary: DataFusion's
    // window UDFs return UInt64, which arrow-pg reports as NUMERIC, while
    // PostgreSQL reports bigint. Values still have to agree.
    let sql = "SELECT id, val, \
                      RANK() OVER (ORDER BY id NULLS LAST) AS rank_native, \
                      ROW_NUMBER() OVER (ORDER BY id NULLS LAST, val NULLS LAST) AS row_number_native \
               FROM items ORDER BY id NULLS FIRST, val NULLS FIRST";
    for mode in Mode::BOTH {
        let context = || {
            format!(
                "case=window-native-types SQL={sql} params=[] protocol={} expected LakeSoul=[INT4, INT4, NUMERIC, NUMERIC] PostgreSQL=[INT4, INT4, INT8, INT8]",
                mode.label()
            )
        };
        let actual = fetch(&h.lake, sql, mode).await.with_context(context)?;
        let reference = fetch(&h.reference, sql, mode).await.with_context(context)?;
        compare::check_columns(
            &actual,
            &[
                ("id", Type::INT4),
                ("val", Type::INT4),
                ("rank_native", Type::NUMERIC),
                ("row_number_native", Type::NUMERIC),
            ],
        )
        .with_context(context)?;
        compare::check_columns(
            &reference,
            &[
                ("id", Type::INT4),
                ("val", Type::INT4),
                ("rank_native", Type::INT8),
                ("row_number_native", Type::INT8),
            ],
        )
        .with_context(context)?;
        compare::compare_rows(&actual.rows, &reference.rows, true)
            .with_context(context)?;
    }
    eprintln!(
        "PASS case=window-native-types classification=support-boundary lake=NUMERIC postgres=INT8"
    );
    Ok(())
}

async fn strings(h: &Harness) -> Result<()> {
    run_cases(
        h,
        vec![
            Case {
                id: "string-literals",
                sql: r"SELECT 'it''s' AS quoted, 'a\b' AS backslash, '' AS empty,
                       'β' AS utf8, ' line ' AS padded",
                columns: vec![
                    ("quoted", Type::TEXT),
                    ("backslash", Type::TEXT),
                    ("empty", Type::TEXT),
                    ("utf8", Type::TEXT),
                    ("padded", Type::TEXT),
                ],
                ordered: true,
                expected: Some(rows(&[&[
                    Some("it's"),
                    Some(r"a\b"),
                    Some(""),
                    Some("β"),
                    Some(" line "),
                ]])),
            },
            Case {
                id: "string-operations-on-fixture",
                sql: "SELECT id, label, \
                            char_length(label) AS characters, \
                            substr(label, 1, 1) AS first_char, \
                            replace(label, 'a', 'A') AS replaced, \
                            label || '|' AS concatenated \
                      FROM items ORDER BY id NULLS FIRST, label NULLS FIRST",
                columns: vec![
                    ("id", Type::INT4),
                    ("label", Type::TEXT),
                    ("characters", Type::INT4),
                    ("first_char", Type::TEXT),
                    ("replaced", Type::TEXT),
                    ("concatenated", Type::TEXT),
                ],
                ordered: true,
                expected: Some(rows(&[
                    &[None, None, None, None, None, None],
                    &[
                        Some("1"),
                        Some(""),
                        Some("0"),
                        Some(""),
                        Some(""),
                        Some("|"),
                    ],
                    &[
                        Some("1"),
                        Some("a"),
                        Some("1"),
                        Some("a"),
                        Some("A"),
                        Some("a|"),
                    ],
                    &[
                        Some("2"),
                        Some(" β "),
                        Some("3"),
                        Some(" "),
                        Some(" β "),
                        Some(" β |"),
                    ],
                ])),
            },
        ],
    )
    .await
}

async fn temporal(h: &Harness) -> Result<()> {
    // Microseconds (`.123456`) make the reference's ISO text output identical to
    // pgwire's date-style formatting, so the simple protocol compares raw server
    // output; the extended protocol decodes the same instants through chrono.
    // The timestamptz value is built by casting a naive timestamp, which both
    // servers interpret in the session time zone (`UTC` on both sides here), so
    // no server-specific offset-string parsing is involved.
    assert_sql(
        h,
        "temporal-literals",
        "SELECT DATE '2024-02-29' AS d, \
                TIMESTAMP '2024-02-29 12:34:56.123456' AS ts, \
                CAST(TIMESTAMP '2024-02-29 12:34:56.123456' AS TIMESTAMPTZ) AS tstz",
        &[
            ("d", Type::DATE),
            ("ts", Type::TIMESTAMP),
            ("tstz", Type::TIMESTAMPTZ),
        ],
        true,
        None,
    )
    .await
}

async fn arrays(h: &Harness) -> Result<()> {
    assert_sql(
        h,
        "array-shapes",
        "SELECT CAST(ARRAY[] AS INTEGER[]) AS empty_arr, \
                CAST(NULL AS INTEGER[]) AS null_arr, \
                CAST(ARRAY[1, NULL, 3] AS INTEGER[]) AS mixed, \
                (CAST(ARRAY[1, NULL, 3] AS INTEGER[]))[1] AS first, \
                (CAST(ARRAY[1, NULL, 3] AS INTEGER[]))[2] AS second, \
                (CAST(ARRAY[1, NULL, 3] AS INTEGER[]))[10] AS missing",
        &[
            ("empty_arr", Type::INT4_ARRAY),
            ("null_arr", Type::INT4_ARRAY),
            ("mixed", Type::INT4_ARRAY),
            ("first", Type::INT4),
            ("second", Type::INT4),
            ("missing", Type::INT4),
        ],
        true,
        Some(&rows(&[&[
            Some("{}"),
            None,
            Some("{1,NULL,3}"),
            Some("1"),
            None,
            None,
        ]])),
    )
    .await
}

async fn quoted_identifiers(h: &Harness) -> Result<()> {
    // Quoted aliases and a quoted unqualified table name. Mapping a database
    // to `public` or a different schema is phase-three catalog work; the
    // reference fixture resolves `"items"` through its own search_path.
    assert_sql(
        h,
        "quoted-identifiers",
        "SELECT i.\"id\" AS \"Id\", i.\"val\" AS \"Val\", i.\"label\" AS \"Label\" \
         FROM \"items\" AS i \
         ORDER BY \"Id\" NULLS FIRST, \"Label\" NULLS FIRST",
        &[
            ("Id", Type::INT4),
            ("Val", Type::INT4),
            ("Label", Type::TEXT),
        ],
        true,
        Some(&rows(&[
            &[None, None, None],
            &[Some("1"), None, Some("")],
            &[Some("1"), Some("10"), Some("a")],
            &[Some("2"), Some("20"), Some(" β ")],
        ])),
    )
    .await
}

/// Assert one statement is rejected by the *server* with `expected` SQLSTATE.
fn server_sqlstate(error: &Error, context: &str) -> Result<String> {
    let db = error.as_db_error().with_context(|| {
        format!("{context}: expected a server ErrorResponse, actual client-side error: {error}")
    })?;
    Ok(db.code().code().to_owned())
}

async fn assert_simple_error(
    client: &Client,
    sql: &str,
    expected: &str,
    context: &str,
) -> Result<()> {
    let error = client.simple_query(sql).await.err().with_context(|| {
        format!("{context}: expected server rejection, actual success")
    })?;
    let code = server_sqlstate(&error, context)?;
    ensure!(
        code == expected,
        "{context}: expected SQLSTATE={expected}, actual={code}"
    );
    Ok(())
}

async fn assert_extended_error(
    client: &Client,
    sql: &str,
    expected: &str,
    context: &str,
) -> Result<()> {
    // Parse/Describe must accept the statement: the failure is a planning or
    // execution error, not a syntax error.
    let statement = client.prepare(sql).await.with_context(|| {
        format!("{context}: extended Parse rejected the statement, actual server rejection at parse")
    })?;
    let error = client.query(&statement, &[]).await.err().with_context(|| {
        format!("{context}: expected execution rejection, actual success")
    })?;
    let code = server_sqlstate(&error, context)?;
    ensure!(
        code == expected,
        "{context}: expected SQLSTATE={expected}, actual={code}"
    );
    Ok(())
}

async fn error_contracts(h: &Harness) -> Result<()> {
    invalid_conversion(h).await?;
    scalar_subquery_multirow(h).await?;
    Ok(())
}

/// `CAST(... AS INTEGER)` over row data containing `'12abc'`: both servers
/// reject at execution; LakeSoul surfaces the DataFusion failure as `XX000`
/// while PostgreSQL uses `22P02`. The cast is row-dependent on purpose so it
/// cannot be constant folded at plan time (DataFusion folds failing literal
/// casts during planning, PostgreSQL defers the cast function to execution).
async fn invalid_conversion(h: &Harness) -> Result<()> {
    let sql = "SELECT CAST(CASE WHEN id = 1 THEN '12abc' END AS INTEGER) AS bad \
               FROM items";
    for mode in Mode::BOTH {
        let lake = format!(
            "case=invalid-conversion SQL={sql} params=[] protocol={} expected LakeSoul rejection SQLSTATE=XX000 reference=22P02",
            mode.label()
        );
        let reference = format!(
            "case=invalid-conversion SQL={sql} params=[] protocol={} expected PostgreSQL rejection SQLSTATE=22P02",
            mode.label()
        );
        match mode {
            Mode::Simple => {
                assert_simple_error(&h.lake, sql, "XX000", &lake).await?;
                assert_simple_error(&h.reference, sql, "22P02", &reference).await?;
            }
            Mode::Extended => {
                assert_extended_error(&h.lake, sql, "XX000", &lake).await?;
                assert_extended_error(&h.reference, sql, "22P02", &reference).await?;
            }
        }
    }
    let recovery = compare::query_binary(&h.lake, "SELECT COUNT(*) AS n FROM items")
        .await
        .context("case=invalid-conversion first query after error")?;
    compare::compare_rows(&recovery.rows, &rows(&[&[Some("4")]]), true)?;
    eprintln!("PASS case=invalid-conversion lake=XX000 postgres=22P02 recovery=4");
    Ok(())
}

/// A scalar subquery in `WHERE` returning three rows: both servers reject;
/// LakeSoul reports `XX000`, PostgreSQL `21000` (cardinality_violation).
async fn scalar_subquery_multirow(h: &Harness) -> Result<()> {
    let sql = "SELECT id FROM items \
               WHERE id = (SELECT id FROM items WHERE id IS NOT NULL) ORDER BY id";
    for mode in Mode::BOTH {
        let lake = format!(
            "case=scalar-subquery-multirow SQL={sql} params=[] protocol={} expected LakeSoul rejection SQLSTATE=XX000 reference=21000",
            mode.label()
        );
        let reference = format!(
            "case=scalar-subquery-multirow SQL={sql} params=[] protocol={} expected PostgreSQL rejection SQLSTATE=21000",
            mode.label()
        );
        match mode {
            Mode::Simple => {
                assert_simple_error(&h.lake, sql, "XX000", &lake).await?;
                assert_simple_error(&h.reference, sql, "21000", &reference).await?;
            }
            Mode::Extended => {
                assert_extended_error(&h.lake, sql, "XX000", &lake).await?;
                assert_extended_error(&h.reference, sql, "21000", &reference).await?;
            }
        }
    }
    let recovery = compare::query_binary(&h.lake, "SELECT COUNT(*) AS n FROM items")
        .await
        .context("case=scalar-subquery-multirow first query after error")?;
    compare::compare_rows(&recovery.rows, &rows(&[&[Some("4")]]), true)?;
    eprintln!("PASS case=scalar-subquery-multirow lake=XX000 postgres=21000 recovery=4");
    Ok(())
}

/// Strict PG contract: a projection scalar subquery must not silently truncate
/// multiple rows. The upstream LIMIT 1 rewrite currently violates this.
pub async fn projection_scalar_subquery_rejects_multiple_rows(h: &Harness) -> Result<()> {
    let sql =
        "SELECT (SELECT val FROM items WHERE val IS NOT NULL ORDER BY val) AS first_val";
    let mut failures = Vec::new();
    for mode in Mode::BOTH {
        let context = format!(
            "case=projection-scalar-subquery contract=PG-equivalent SQL={sql} params=[] protocol={} expected rejection SQLSTATE=21000",
            mode.label()
        );
        let result = match mode {
            Mode::Simple => {
                assert_simple_error(&h.reference, sql, "21000", &context).await?;
                assert_simple_error(&h.lake, sql, "21000", &context).await
            }
            Mode::Extended => {
                assert_extended_error(&h.reference, sql, "21000", &context).await?;
                assert_extended_error(&h.lake, sql, "21000", &context).await
            }
        };
        if let Err(error) = result {
            failures.push(format!("{error:#}"));
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
