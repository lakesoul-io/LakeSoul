// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Phase-one codec feasibility probes.
//!
//! Two contracts are exercised through the real pgwire connection:
//!
//! * `DECIMAL(p,s)` / `NUMERIC(p,s)` results are reported as PostgreSQL
//!   `numeric` (OID 1700) and the extended-protocol binary payload decodes on
//!   the client to the exact value *and* scale for the nonzero inputs below;
//!   the same SQL is run against PostgreSQL. Zero's binary scale has a known
//!   upstream defect, isolated in `decimal_zero_binary_scale_matches_postgres`.
//! * a pgvector `vector` result type is discovered dynamically: the client
//!   resolves the unknown result OID through `pg_catalog.pg_type` and reports
//!   the type as name `vector` in schema `pg_catalog` with `Kind::Simple`, and
//!   the official pgvector client codec decodes the binary result. The OID is
//!   read from the catalog, not hard-coded, because a real pgvector install
//!   picks its own OID.
//!
//! Decimal fixtures stay inside rust_decimal's 96-bit mantissa / 28-digit
//! scale (the server encodes a NUMERIC through `rust_decimal::Decimal`) and
//! are written as string casts (`CAST('...' AS DECIMAL(p,s))`), so both sides
//! parse the exact decimal text instead of a Float64 literal; the assertions
//! describe the codec rather than literal parsing or decimal overflow.

use std::str::FromStr;

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;
use tokio_postgres::Client;
use tokio_postgres::types::{Kind, Type};

use crate::support::{Harness, compare};

/// One decimal result column: alias, `CAST` expression, exact expected text
/// (`None` = SQL NULL) and the scale declared by the target type.
///
/// Precision never exceeds 20 and scale never exceeds 4, so every value is
/// exactly representable by `rust_decimal`.
const DECIMAL_COLUMNS: &[(&str, &str, Option<&str>, u32)] = &[
    (
        "pos",
        "CAST('1234567.8901' AS DECIMAL(18,4))",
        Some("1234567.8901"),
        4,
    ),
    (
        "neg",
        "CAST('-1234567.8901' AS DECIMAL(18,4))",
        Some("-1234567.8901"),
        4,
    ),
    (
        "small_neg",
        "CAST('-0.01' AS DECIMAL(10,2))",
        Some("-0.01"),
        2,
    ),
    ("missing", "CAST(NULL AS DECIMAL(6,3))", None, 3),
    // Widest declared precision/scale pair: numeric keyword instead of the
    // DECIMAL synonym, every digit significant.
    (
        "wide",
        "CAST('9999999999999999.9999' AS NUMERIC(20,4))",
        Some("9999999999999999.9999"),
        4,
    ),
    // Integral boundary of the largest fixture, no fractional digits at all.
    (
        "integer_edge",
        "CAST('-9223372036854775807' AS DECIMAL(19,0))",
        Some("-9223372036854775807"),
        0,
    ),
];

fn decimal_sql() -> String {
    let mut sql = String::from("SELECT ");
    for (index, (name, expression, _, _)) in DECIMAL_COLUMNS.iter().enumerate() {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str(expression);
        sql.push_str(" AS ");
        sql.push_str(name);
    }
    sql
}

/// pgvector result probe.
///
/// The bracket literal `'[1,2,3]'::vector(3)` is only rewritten when it is an
/// operand of a distance operator (see `postgres-lakesoul/src/pg_vector.rs`);
/// a standalone one would need an Arrow `Utf8 -> FixedSizeList` cast that does
/// not exist. The `ARRAY[...]` form is the supported literal shape: it is a
/// `List(Float64)` cast to the planned `FixedSizeList(Float32, 3)`.
const VECTOR_SQL: &str = "SELECT ARRAY[1.0, 2.5, -3.0]::vector(3) AS v";
/// pgvector's text form of the vector above (`f32` shortest round-trip).
const VECTOR_TEXT: &str = "[1,2.5,-3]";
const VECTOR_OID_SQL: &str = "SELECT t.oid FROM pg_catalog.pg_type t \
     JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
     WHERE t.typname = 'vector' AND n.nspname = 'pg_catalog'";

/// Number of digits after the decimal point in a PostgreSQL numeric text
/// value. PostgreSQL prints exactly the value's scale digits (no exponent),
/// so this is the wire-visible scale.
fn text_scale(text: &str) -> usize {
    text.split_once('.')
        .map_or(0, |(_, fraction)| fraction.len())
}

pub async fn run(h: &Harness) -> Result<()> {
    decimal_binary(h).await.context(
        "case=decimal-codec params=[] protocol=extended-binary expected types=[NUMERIC]",
    )?;
    eprintln!(
        "PASS case=decimal-codec protocol=extended-binary exact-value-and-scale=true"
    );
    decimal_text(h).await.context(
        "case=decimal-codec params=[] protocol=simple-text expected numeric text",
    )?;
    eprintln!("PASS case=decimal-codec protocol=simple-text exact-value-and-scale=true");
    vector_result(h)
        .await
        .context("case=vector-discovery params=[] expected type=pg_catalog.vector")?;
    eprintln!("PASS case=vector-discovery dynamic-type=true official-client-decode=true");
    Ok(())
}

/// Strict reproduction of the upstream zero-scale defect. Never normalize the
/// scale or assert the broken value as a supported result.
pub async fn zero_binary_scale(h: &Harness) -> Result<()> {
    let sql = "SELECT CAST('0' AS DECIMAL(10,2)) AS amount";
    let context = || {
        format!(
            "case=decimal-zero-scale SQL={sql} params=[] protocol=extended-binary expected type=NUMERIC scale=2"
        )
    };
    let reference = h
        .reference
        .query_one(sql, &[])
        .await
        .with_context(context)?;
    let actual = h.lake.query_one(sql, &[]).await.with_context(context)?;
    for row in [&reference, &actual] {
        ensure!(
            row.columns()[0].type_() == &Type::NUMERIC,
            "{} actual type={:?}",
            context(),
            row.columns()[0].type_()
        );
    }
    let expected: Decimal = reference.try_get(0).with_context(context)?;
    let decoded: Decimal = actual.try_get(0).with_context(context)?;
    ensure!(
        expected == Decimal::ZERO && expected.scale() == 2,
        "{} unexpected reference={expected} scale={}",
        context(),
        expected.scale()
    );
    ensure!(
        decoded == expected,
        "{} expected value={expected} actual={decoded}",
        context()
    );
    ensure!(
        decoded.scale() == expected.scale(),
        "{} PostgreSQL scale={} LakeSoul scale={}",
        context(),
        expected.scale(),
        decoded.scale()
    );
    Ok(())
}

/// Extended protocol: prepare, then decode every NUMERIC cell as
/// `rust_decimal::Decimal` from the binary result format.
async fn decimal_binary(h: &Harness) -> Result<()> {
    let sql = decimal_sql();
    let reference = load_decimal_row(&h.reference, &sql, "reference").await?;
    let lake = load_decimal_row(&h.lake, &sql, "lake").await?;
    ensure!(
        lake == reference,
        "DECIMAL binary values differ from PostgreSQL for the same SQL: SQL={sql} reference={reference:?} lake={lake:?}"
    );
    Ok(())
}

async fn load_decimal_row(
    client: &Client,
    sql: &str,
    side: &str,
) -> Result<Vec<Option<Decimal>>> {
    let context = || format!("side={side} SQL={sql} protocol=extended-binary");
    let statement = client.prepare(sql).await.with_context(context)?;
    ensure!(
        statement.columns().len() == DECIMAL_COLUMNS.len(),
        "{}: expected {} result columns, actual {}",
        context(),
        DECIMAL_COLUMNS.len(),
        statement.columns().len()
    );

    for (index, (name, _, _, _)) in DECIMAL_COLUMNS.iter().enumerate() {
        let column = &statement.columns()[index];
        ensure!(
            column.name() == *name,
            "{}: column {index} name: expected {name}, actual {}",
            context(),
            column.name()
        );
        ensure!(
            column.type_() == &Type::NUMERIC,
            "{}: column {index} ({name}) type: expected NUMERIC (OID {}), actual {:?}",
            context(),
            Type::NUMERIC.oid(),
            column.type_()
        );
    }

    let rows = client.query(&statement, &[]).await.with_context(context)?;
    ensure!(
        rows.len() == 1,
        "{}: expected exactly one row, actual {}",
        context(),
        rows.len()
    );
    let row = &rows[0];

    let mut values = Vec::with_capacity(DECIMAL_COLUMNS.len());
    for (index, (name, _, expected, scale)) in DECIMAL_COLUMNS.iter().enumerate() {
        let decoded: Option<Decimal> = row.try_get(index).with_context(|| {
            format!(
                "{}: column {index} ({name}) NUMERIC binary decode",
                context()
            )
        })?;
        match (*expected, decoded) {
            (None, None) => {}
            (None, Some(value)) => bail!(
                "{}: column {index} ({name}) expected NULL, decoded {value}",
                context()
            ),
            (Some(text), None) => bail!(
                "{}: column {index} ({name}) decoded NULL, expected {text}",
                context()
            ),
            (Some(text), Some(value)) => {
                let want = Decimal::from_str(text).with_context(|| {
                    format!("{}: fixing the fixture value {text:?}", context())
                })?;
                ensure!(
                    value == want,
                    "{}: column {index} ({name}) value: expected {want}, decoded {value}",
                    context()
                );
                ensure!(
                    value.scale() == *scale,
                    "{}: column {index} ({name}) scale: expected {scale}, decoded value {value} has scale {}",
                    context(),
                    value.scale()
                );
            }
        }
        values.push(decoded);
    }
    Ok(values)
}

/// Simple text protocol: compare every cell against PostgreSQL for the same
/// SQL, then assert the numeric value and the scale rendered as digits.
async fn decimal_text(h: &Harness) -> Result<()> {
    let sql = decimal_sql();
    let lake = compare::simple(&h.lake, &sql)
        .await
        .with_context(|| format!("side=lake SQL={sql} protocol=simple-text"))?;
    let reference = compare::simple(&h.reference, &sql)
        .await
        .with_context(|| format!("side=reference SQL={sql} protocol=simple-text"))?;

    let names: Vec<String> = DECIMAL_COLUMNS
        .iter()
        .map(|(name, _, _, _)| (*name).to_owned())
        .collect();
    ensure!(
        lake.names == names,
        "simple-text column names: expected {names:?}, actual {:?} (SQL={sql})",
        lake.names
    );
    ensure!(
        lake.rows.len() == 1,
        "simple-text expected exactly one row, actual {} (SQL={sql})",
        lake.rows.len()
    );
    ensure!(
        lake.names == reference.names,
        "simple-text column names differ from PostgreSQL: SQL={sql} reference={:?} lake={:?}",
        reference.names,
        lake.names
    );
    ensure!(
        lake.rows == reference.rows,
        "simple-text cells differ from PostgreSQL: SQL={sql} reference={:?} lake={:?}",
        reference.rows,
        lake.rows
    );

    let row = &lake.rows[0];
    ensure!(
        row.len() == DECIMAL_COLUMNS.len(),
        "simple-text row has {} cells, expected {} (SQL={sql})",
        row.len(),
        DECIMAL_COLUMNS.len()
    );
    for (index, (name, _, expected, scale)) in DECIMAL_COLUMNS.iter().enumerate() {
        match (*expected, &row[index]) {
            (None, None) => {}
            (None, Some(text)) => bail!(
                "simple-text column {index} ({name}) expected NULL, actual {text:?} (SQL={sql})"
            ),
            (Some(text), None) => bail!(
                "simple-text column {index} ({name}) returned NULL, expected {text} (SQL={sql})"
            ),
            (Some(want_text), Some(text)) => {
                let text = text.as_str();
                ensure!(
                    text_scale(text) == *scale as usize,
                    "simple-text column {index} ({name}) scale: expected {scale} digits after the point, actual {text:?} (SQL={sql})"
                );
                let got = Decimal::from_str(text).with_context(|| {
                    format!(
                        "simple-text column {index} ({name}) value {text:?} is not a decimal (SQL={sql})"
                    )
                })?;
                let want = Decimal::from_str(want_text)
                    .with_context(|| format!("fixing the fixture value {want_text:?}"))?;
                ensure!(
                    got == want,
                    "simple-text column {index} ({name}) value: expected {want}, actual {got} (SQL={sql})"
                );
            }
        }
    }
    Ok(())
}

/// pgvector feasibility: dynamic result-type discovery plus binary decode.
///
/// LakeSoul-specific (the reference PostgreSQL has no `vector` type), so no
/// cross-instance result comparison.
async fn vector_result(h: &Harness) -> Result<()> {
    let context = || format!("SQL={VECTOR_SQL} protocol=extended-binary");
    let statement = h.lake.prepare(VECTOR_SQL).await.with_context(|| {
        format!("{}: Parse/Describe + vector type discovery", context())
    })?;
    ensure!(
        statement.columns().len() == 1,
        "{}: expected one result column, actual {}",
        context(),
        statement.columns().len()
    );
    let column = &statement.columns()[0];
    ensure!(
        column.name() == "v",
        "{}: result column name: expected v, actual {}",
        context(),
        column.name()
    );

    // tokio-postgres resolved the unknown result OID through pg_catalog; the
    // identity below is what a typed driver (psycopg + register_vector) sees.
    let vector_type = column.type_();
    ensure!(
        vector_type.name() == "vector",
        "{}: result type name: expected vector, actual {:?} (OID {})",
        context(),
        vector_type.name(),
        vector_type.oid()
    );
    ensure!(
        vector_type.schema() == "pg_catalog",
        "{}: result type schema: expected pg_catalog, actual {:?}",
        context(),
        vector_type.schema()
    );
    ensure!(
        vector_type.kind() == &Kind::Simple,
        "{}: result type kind: expected Kind::Simple, actual {:?}",
        context(),
        vector_type.kind()
    );

    // The OID is whatever this server's pg_type row says; it must not be a
    // hard-coded constant duplicated in the test.
    let oid_row = h
        .lake
        .query_opt(VECTOR_OID_SQL, &[])
        .await
        .with_context(|| format!("{}: reading pg_catalog.pg_type vector oid", context()))?
        .with_context(|| {
            format!(
                "{}: pg_catalog.pg_type has no vector row for schema pg_catalog",
                context()
            )
        })?;
    let catalog_oid: u32 = oid_row
        .try_get(0)
        .with_context(|| format!("{}: decoding catalog vector oid", context()))?;
    ensure!(
        vector_type.oid() == catalog_oid,
        "{}: result type OID: expected catalog oid {catalog_oid}, actual {}",
        context(),
        vector_type.oid()
    );

    let rows = h
        .lake
        .query(&statement, &[])
        .await
        .with_context(|| format!("{}: Bind/Execute", context()))?;
    ensure!(
        rows.len() == 1,
        "{}: expected exactly one row, actual {}",
        context(),
        rows.len()
    );

    let returned = rows[0].columns()[0].type_();
    ensure!(
        returned.name() == "vector" && returned.oid() == catalog_oid,
        "{}: returned row column type: expected vector OID {catalog_oid}, actual {} OID {}",
        context(),
        returned.name(),
        returned.oid()
    );

    // The official pgvector client codec accepts the type by name and decodes
    // the binary result; reusing it keeps this test to the type-discovery
    // contract instead of a second protocol implementation.
    let decoded: pgvector::Vector = rows[0]
        .try_get(0)
        .with_context(|| format!("{}: pgvector binary decode", context()))?;
    let expected = vec![1.0f32, 2.5, -3.0];
    ensure!(
        decoded.to_vec() == expected,
        "{}: decoded vector: expected {expected:?}, actual {:?}",
        context(),
        decoded.to_vec()
    );

    // The text protocol must render the pgvector form, not a float4 array.
    let text = compare::simple(&h.lake, VECTOR_SQL)
        .await
        .with_context(|| format!("SQL={VECTOR_SQL} protocol=simple-text"))?;
    ensure!(
        text.names == vec!["v".to_owned()],
        "simple-text column names: expected [\"v\"], actual {:?} (SQL={VECTOR_SQL})",
        text.names
    );
    ensure!(
        text.rows == vec![vec![Some(VECTOR_TEXT.to_owned())]],
        "simple-text vector: expected [[Some({VECTOR_TEXT:?})]], actual {:?} (SQL={VECTOR_SQL})",
        text.rows
    );
    Ok(())
}
