// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector codec, distance and parameter-binding contract.
//!
//! All checks run against the real `postgres-lakesoul` child over the
//! PostgreSQL wire protocol and use the official `pgvector` client codec; the
//! reference PostgreSQL server is never asked for the `vector` extension.
//!
//! The fixture table `vector_values` is deliberately created without a vector
//! index, so its physical column is a PostgreSQL `float4[]` (`List(Float32)`)
//! and the session's index-driven `pg.vector` marker does not apply to a bare
//! `SELECT vec`. Its codec surface is therefore projected explicitly as
//! `vec::vector(8)`: the cast keeps the stored values and gives the result the
//! server's dynamic pgvector type. The indexed `vector_ann` fixture's physical
//! column carries the marker, so its stored `vec` column is also read directly
//! (no cast) to prove the production indexed stored-result surface over the
//! wire.
//!
//! Oracles are independent `f64` arithmetic over the exact `f32` inputs. Every
//! floating case names its own absolute tolerance; there is no global float
//! tolerance and no result normalization.

use anyhow::{Context, Result, anyhow, bail, ensure};
use pgvector::Vector;
use tokio_postgres::types::{Kind, ToSql, Type};
use tokio_postgres::{Client, Statement};

use crate::common;
use crate::support::{
    Harness, compare,
    vector::{ANN_TABLE, DIM, VALUES_TABLE, corpus_vector},
};

/// Absolute tolerance for L2 distance results. Every fixture component is an
/// exact binary fraction and both sides compute in `f64`, so the expected
/// round-off is far below this bound.
const L2_ABS_TOLERANCE: f64 = 1e-9;
/// Absolute tolerance for the negative inner product (`<#>`) results.
const NEGATIVE_INNER_PRODUCT_ABS_TOLERANCE: f64 = 1e-9;
/// Absolute tolerance for cosine distance (`<=>`) results.
const COSINE_DISTANCE_ABS_TOLERANCE: f64 = 1e-12;

/// The first independent fixed vector: exact decimal components only.
const FIXED_A: [f32; DIM] = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
/// A fixed query whose inner product with [`FIXED_A`] is nonzero and negative
/// (`-1.75`), so `<#>` must flip the sign.
const FIXED_B: [f32; DIM] = [-2.0, 1.0, 0.5, -0.5, 1.0, -1.0, 0.25, -0.25];
/// The second stored row of `vector_values`, independent of [`FIXED_B`].
const FIXED_STORED_B: [f32; DIM] = [-2.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];

const VECTOR_CATALOG_SQL: &str = "SELECT t.oid, t.typname, n.nspname \
     FROM pg_catalog.pg_type t \
     JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
     WHERE t.typname = 'vector' AND n.nspname = 'pg_catalog'";

/// Rows of `vector_values` in id order: two fixed vectors and SQL NULL.
fn stored_vectors() -> [Option<Vec<f32>>; 3] {
    [Some(FIXED_A.to_vec()), Some(FIXED_STORED_B.to_vec()), None]
}

fn opposite_b() -> [f32; DIM] {
    let mut values = FIXED_B;
    for value in &mut values {
        *value = -*value;
    }
    values
}

/// pgvector's text form, rendered the same way the server renders `f32`.
fn vector_literal(values: &[f32]) -> String {
    let mut text = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            text.push(',');
        }
        text.push_str(&value.to_string());
    }
    text.push(']');
    text
}

fn l2_oracle(left: &[f32], right: &[f32]) -> f64 {
    assert_eq!(
        left.len(),
        right.len(),
        "distance oracle inputs must have the same dimension"
    );
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let difference = f64::from(*left) - f64::from(*right);
            difference * difference
        })
        .sum::<f64>()
        .sqrt()
}

fn negative_inner_product_oracle(left: &[f32], right: &[f32]) -> f64 {
    assert_eq!(
        left.len(),
        right.len(),
        "distance oracle inputs must have the same dimension"
    );
    -left
        .iter()
        .zip(right)
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum::<f64>()
}

fn cosine_distance_oracle(left: &[f32], right: &[f32]) -> f64 {
    assert_eq!(
        left.len(),
        right.len(),
        "distance oracle inputs must have the same dimension"
    );
    let mut dot = 0.0;
    let mut left_squares = 0.0;
    let mut right_squares = 0.0;
    for (left, right) in left.iter().zip(right) {
        let left = f64::from(*left);
        let right = f64::from(*right);
        dot += left * right;
        left_squares += left * left;
        right_squares += right * right;
    }
    assert!(
        left_squares != 0.0 && right_squares != 0.0,
        "cosine oracle vectors must have a nontrivial norm"
    );
    1.0 - dot / (left_squares.sqrt() * right_squares.sqrt())
}

fn assert_close(case: &str, actual: f64, expected: f64, tolerance: f64) -> Result<()> {
    let difference = (actual - expected).abs();
    ensure!(
        difference <= tolerance,
        "case={case}: expected={expected:.17e} actual={actual:.17e} \
         abs_error={difference:.17e} tolerance={tolerance:.1e}"
    );
    eprintln!(
        "PASS case={case} expected={expected:.17e} actual={actual:.17e} \
         tolerance={tolerance:.1e}"
    );
    Ok(())
}

fn text_float(cell: Option<&str>, case: &str, column: &str) -> Result<f64> {
    let text =
        cell.ok_or_else(|| anyhow!("case={case}: column {column} is unexpectedly NULL"))?;
    text.parse::<f64>()
        .with_context(|| format!("case={case}: column {column} is not an f64: {text:?}"))
}

/// The unknown `vector` OID as this server's catalog reports it. No extension
/// OID is hard-coded: the client's Describe result and the catalog must agree.
async fn discover_vector_type(h: &Harness) -> Result<Type> {
    let rows = h
        .lake
        .query(VECTOR_CATALOG_SQL, &[])
        .await
        .context("case=vector-type-discovery: query pg_catalog.pg_type")?;
    ensure!(
        rows.len() == 1,
        "case=vector-type-discovery: expected one pg_catalog vector row, actual {}",
        rows.len()
    );
    let oid: u32 = rows[0].try_get(0)?;
    let name: String = rows[0].try_get(1)?;
    let schema: String = rows[0].try_get(2)?;
    ensure!(
        name == "vector",
        "case=vector-type-discovery: catalog typname: expected vector, actual {name:?}"
    );
    ensure!(
        schema == "pg_catalog",
        "case=vector-type-discovery: catalog namespace: expected pg_catalog, actual {schema:?}"
    );
    let vector_type = Type::new(name, oid, Kind::Simple, schema);
    eprintln!(
        "PASS case=vector-type-discovery name=vector schema=pg_catalog oid={} \
         source=pg_catalog.pg_type",
        vector_type.oid()
    );
    Ok(vector_type)
}

fn assert_vector_identity(case: &str, actual: &Type, expected: &Type) -> Result<()> {
    ensure!(
        actual.name() == expected.name(),
        "case={case}: type name: expected {:?}, actual {:?}",
        expected.name(),
        actual.name()
    );
    ensure!(
        actual.oid() == expected.oid(),
        "case={case}: type OID: expected {}, actual {}",
        expected.oid(),
        actual.oid()
    );
    ensure!(
        actual.schema() == expected.schema(),
        "case={case}: type schema: expected {:?}, actual {:?}",
        expected.schema(),
        actual.schema()
    );
    ensure!(
        actual.kind() == expected.kind(),
        "case={case}: type kind: expected {:?}, actual {:?}",
        expected.kind(),
        actual.kind()
    );
    eprintln!(
        "PASS case={case} type=vector oid={} schema={} kind={:?}",
        actual.oid(),
        actual.schema(),
        actual.kind()
    );
    Ok(())
}

fn require_column(
    statement: &Statement,
    index: usize,
    name: &str,
    expected_type: &Type,
    case: &str,
) -> Result<()> {
    let column = statement.columns().get(index).ok_or_else(|| {
        anyhow!(
            "case={case}: missing result column {index} ({name}), Describe has {} columns",
            statement.columns().len()
        )
    })?;
    ensure!(
        column.name() == name,
        "case={case}: column {index} name: expected {name}, actual {} (all={:?})",
        column.name(),
        statement.columns()
    );
    ensure!(
        column.type_() == expected_type,
        "case={case}: column {index} ({name}) type/OID: expected {expected_type:?}, \
         actual {:?}",
        column.type_()
    );
    Ok(())
}

pub async fn run(h: &Harness) -> Result<()> {
    let vector_type = discover_vector_type(h).await?;
    stored_vector_codecs(h, &vector_type).await?;
    indexed_stored_column(h, &vector_type).await?;
    distance_codecs(h).await?;
    null_operands(h).await?;
    stored_null_operand(h).await?;
    parameter_statement(
        h,
        "vector-parameter-explicit",
        &explicit_parameter_sql(),
        true,
        &vector_type,
    )
    .await?;
    parameter_statement(
        h,
        "vector-parameter-bare",
        &bare_parameter_sql(),
        false,
        &vector_type,
    )
    .await?;
    rejection_cases(h, &vector_type).await
}

/// Stored `vector_values` rows read through the dynamic vector type: official
/// binary decode of both fixed vectors and NULL, plus the exact simple-text
/// form derived from the known `f32` inputs.
async fn stored_vector_codecs(h: &Harness, vector_type: &Type) -> Result<()> {
    // The physical column has no index-driven pg.vector marker, so the cast is
    // the explicit vector codec surface (see the module comment).
    let sql = format!(
        "SELECT id, vec::vector({DIM}) AS vec FROM public.{VALUES_TABLE} ORDER BY id"
    );
    let statement =
        h.lake.prepare(&sql).await.with_context(|| {
            format!("case=vector-stored-binary stage=parse SQL={sql}")
        })?;
    ensure!(
        statement.columns().len() == 2,
        "case=vector-stored-binary: expected 2 result columns, actual {:?}",
        statement.columns()
    );
    require_column(&statement, 0, "id", &Type::INT8, "vector-stored-binary")?;
    assert_vector_identity(
        "vector-stored-binary-vec-type",
        statement.columns()[1].type_(),
        vector_type,
    )?;

    let rows =
        h.lake.query(&statement, &[]).await.with_context(|| {
            format!("case=vector-stored-binary stage=execute SQL={sql}")
        })?;
    ensure!(
        rows.len() == 3,
        "case=vector-stored-binary: expected 3 rows, actual {}",
        rows.len()
    );
    let expected = stored_vectors();
    for (index, row) in rows.iter().enumerate() {
        let expected_id = (index + 1) as i64;
        let actual_id: i64 = row.try_get(0)?;
        ensure!(
            actual_id == expected_id,
            "case=vector-stored-binary: row {index} id: expected {expected_id}, \
             actual {actual_id}"
        );
        let decoded: Option<Vector> = row.try_get(1).with_context(|| {
            format!("case=vector-stored-binary: row {index} official pgvector decode")
        })?;
        ensure!(
            decoded.as_ref().map(|vector| vector.to_vec()).as_ref()
                == expected[index].as_ref(),
            "case=vector-stored-binary: row {index} vector: expected {:?}, actual {:?}",
            expected[index],
            decoded
        );
    }
    eprintln!(
        "PASS case=vector-stored-binary rows=3 nullable=true codec=official-pgvector \
         projection=vec::vector({DIM})"
    );

    let simple = compare::simple(&h.lake, &sql)
        .await
        .with_context(|| format!("case=vector-stored-text SQL={sql}"))?;
    compare::check_columns(&simple, &[("id", Type::INT8), ("vec", vector_type.clone())])?;
    let text_a = vector_literal(&FIXED_A);
    let text_b = vector_literal(&FIXED_STORED_B);
    let expected = common::rows(&[
        &[Some("1"), Some(text_a.as_str())],
        &[Some("2"), Some(text_b.as_str())],
        &[Some("3"), None],
    ]);
    compare::compare_rows(&simple.rows, &expected, true)?;
    eprintln!(
        "PASS case=vector-stored-text rows=3 exact_text=true nullable=true \
         expected_a={text_a} expected_b={text_b}"
    );
    Ok(())
}

/// The indexed corpus table's stored `vec` column is reported to drivers as
/// the pgvector `vector` type **without** a projection cast, because the
/// fixture's committed `vector_index_columns` marks the physical column. Row
/// `id = 0` is the fixed exact corpus vector, so this proves the production
/// indexed stored-result surface over the wire rather than only the cast used
/// for the unindexed `vector_values` table.
async fn indexed_stored_column(h: &Harness, vector_type: &Type) -> Result<()> {
    let sql = format!("SELECT id, vec FROM public.{ANN_TABLE} WHERE id = 0");
    let expected = corpus_vector(0).to_vec();
    let statement = h.lake.prepare(&sql).await.with_context(|| {
        format!("case=vector-indexed-stored-binary stage=parse SQL={sql}")
    })?;
    ensure!(
        statement.columns().len() == 2,
        "case=vector-indexed-stored-binary: expected 2 result columns, actual {:?}",
        statement.columns()
    );
    require_column(
        &statement,
        0,
        "id",
        &Type::INT8,
        "vector-indexed-stored-binary",
    )?;
    assert_vector_identity(
        "vector-indexed-stored-binary-vec-type",
        statement.columns()[1].type_(),
        vector_type,
    )?;

    let rows = h.lake.query(&statement, &[]).await.with_context(|| {
        format!("case=vector-indexed-stored-binary stage=execute SQL={sql}")
    })?;
    ensure!(
        rows.len() == 1,
        "case=vector-indexed-stored-binary: expected one row, actual {}",
        rows.len()
    );
    let id: i64 = rows[0].try_get(0)?;
    ensure!(
        id == 0,
        "case=vector-indexed-stored-binary: id: expected 0, actual {id}"
    );
    let decoded: Option<Vector> = rows[0]
        .try_get(1)
        .with_context(|| "case=vector-indexed-stored-binary: official pgvector decode")?;
    let actual = decoded.as_ref().map(|vector| vector.to_vec());
    ensure!(
        actual.as_ref() == Some(&expected),
        "case=vector-indexed-stored-binary: vector: expected {expected:?}, actual {actual:?}"
    );
    eprintln!(
        "PASS case=vector-indexed-stored-binary table={ANN_TABLE} id=0 \
         direct_column=true projection=none codec=official-pgvector"
    );

    let simple = compare::simple(&h.lake, &sql)
        .await
        .with_context(|| format!("case=vector-indexed-stored-text SQL={sql}"))?;
    compare::check_columns(&simple, &[("id", Type::INT8), ("vec", vector_type.clone())])?;
    let text = vector_literal(&expected);
    let expected_rows = common::rows(&[&[Some("0"), Some(text.as_str())]]);
    compare::compare_rows(&simple.rows, &expected_rows, true)?;
    eprintln!(
        "PASS case=vector-indexed-stored-text table={ANN_TABLE} id=0 exact_text=true \
         expected={text}"
    );
    Ok(())
}

fn distance_literals_sql() -> String {
    let a = vector_literal(&FIXED_A);
    let b = vector_literal(&FIXED_B);
    let opposite = vector_literal(&opposite_b());
    format!(
        "SELECT '{a}'::vector({DIM}) <-> '{b}'::vector({DIM}) AS l2, \
         '{a}'::vector({DIM}) <#> '{b}'::vector({DIM}) AS neg_ip, \
         '{a}'::vector({DIM}) <#> '{opposite}'::vector({DIM}) AS neg_ip_positive_dot, \
         '{a}'::vector({DIM}) <=> '{b}'::vector({DIM}) AS cosine, \
         '{a}'::vector({DIM}) <=> '{opposite}'::vector({DIM}) AS cosine_opposite"
    )
}

/// The three distance operators on fixed `vector(8)` literals: binary result
/// decoding with `FLOAT8` OIDs and names, and the simple-text path parsed back
/// into independent `f64` oracles.
async fn distance_codecs(h: &Harness) -> Result<()> {
    let sql = distance_literals_sql();
    let opposite = opposite_b();
    let l2_expected = l2_oracle(&FIXED_A, &FIXED_B);
    let neg_ip_expected = negative_inner_product_oracle(&FIXED_A, &FIXED_B);
    let neg_ip_positive_dot = negative_inner_product_oracle(&FIXED_A, &opposite);
    let cosine_expected = cosine_distance_oracle(&FIXED_A, &FIXED_B);
    let cosine_opposite = cosine_distance_oracle(&FIXED_A, &opposite);

    // The sign cases must be genuinely nontrivial: `<#>` is the negative inner
    // product, and the vectors have a nonzero norm.
    ensure!(
        l2_expected > 0.0,
        "distance fixture A/B must not be identical, oracle L2={l2_expected}"
    );
    ensure!(
        neg_ip_expected > 0.0 && neg_ip_positive_dot < 0.0,
        "negative-inner-product sign fixture is wrong: \
         dot(A,B)={}, dot(A,-B)={}",
        -neg_ip_expected,
        -neg_ip_positive_dot
    );
    ensure!(
        cosine_expected > 1.0 && cosine_opposite < 1.0,
        "cosine fixture must have a nontrivial norm and opposite-sign dot product: \
         cosine(A,B)={cosine_expected}, cosine(A,-B)={cosine_opposite}"
    );

    let expected: [(&str, f64, f64); 5] = [
        ("l2", l2_expected, L2_ABS_TOLERANCE),
        (
            "neg_ip",
            neg_ip_expected,
            NEGATIVE_INNER_PRODUCT_ABS_TOLERANCE,
        ),
        (
            "neg_ip_positive_dot",
            neg_ip_positive_dot,
            NEGATIVE_INNER_PRODUCT_ABS_TOLERANCE,
        ),
        ("cosine", cosine_expected, COSINE_DISTANCE_ABS_TOLERANCE),
        (
            "cosine_opposite",
            cosine_opposite,
            COSINE_DISTANCE_ABS_TOLERANCE,
        ),
    ];

    let statement =
        h.lake.prepare(&sql).await.with_context(|| {
            format!("case=vector-distance-binary stage=parse SQL={sql}")
        })?;
    ensure!(
        statement.columns().len() == expected.len(),
        "case=vector-distance-binary: expected {} columns, actual {:?}",
        expected.len(),
        statement.columns()
    );
    for (index, (name, _, _)) in expected.iter().enumerate() {
        require_column(
            &statement,
            index,
            name,
            &Type::FLOAT8,
            "vector-distance-binary",
        )?;
    }
    let rows = h.lake.query(&statement, &[]).await.with_context(|| {
        format!("case=vector-distance-binary stage=execute SQL={sql}")
    })?;
    ensure!(
        rows.len() == 1,
        "case=vector-distance-binary: expected one row, actual {}",
        rows.len()
    );
    for (index, (name, oracle, tolerance)) in expected.iter().enumerate() {
        let actual: Option<f64> = rows[0]
            .try_get(index)
            .with_context(|| format!("case=vector-distance-binary: column {name}"))?;
        let actual = actual.ok_or_else(|| {
            anyhow!("case=vector-distance-binary: column {name} is unexpectedly NULL")
        })?;
        assert_close(
            &format!("vector-distance-binary-{name}"),
            actual,
            *oracle,
            *tolerance,
        )?;
    }
    eprintln!(
        "PASS case=vector-distance-binary columns=[l2,neg_ip,neg_ip_positive_dot,cosine,\
         cosine_opposite] oid=FLOAT8({}) sign_checks=true",
        Type::FLOAT8.oid()
    );

    let simple = compare::simple(&h.lake, &sql)
        .await
        .with_context(|| format!("case=vector-distance-text SQL={sql}"))?;
    let columns: Vec<(&str, Type)> = expected
        .iter()
        .map(|(name, _, _)| (*name, Type::FLOAT8))
        .collect();
    compare::check_columns(&simple, &columns)?;
    ensure!(
        simple.rows.len() == 1,
        "case=vector-distance-text: expected one row, actual {}",
        simple.rows.len()
    );
    for (index, (name, oracle, tolerance)) in expected.iter().enumerate() {
        let actual = text_float(
            simple.rows[0][index].as_deref(),
            "vector-distance-text",
            name,
        )?;
        assert_close(
            &format!("vector-distance-text-{name}"),
            actual,
            *oracle,
            *tolerance,
        )?;
    }
    eprintln!("PASS case=vector-distance-text protocol=simple-text parsed_f64=true");
    Ok(())
}

fn null_operand_sql() -> String {
    let a = vector_literal(&FIXED_A);
    format!(
        "SELECT NULL <-> '{a}'::vector({DIM}) AS null_left_l2, \
         '{a}'::vector({DIM}) <-> NULL AS null_right_l2, \
         NULL <#> '{a}'::vector({DIM}) AS null_left_neg_ip, \
         '{a}'::vector({DIM}) <=> NULL AS null_right_cosine"
    )
}

/// SQL NULL on either side of each distance operator propagates NULL instead of
/// becoming a type/coercion error.
async fn null_operands(h: &Harness) -> Result<()> {
    let sql = null_operand_sql();
    let names = [
        "null_left_l2",
        "null_right_l2",
        "null_left_neg_ip",
        "null_right_cosine",
    ];
    let statement = h.lake.prepare(&sql).await.with_context(|| {
        format!("case=vector-null-literal-binary stage=parse SQL={sql}")
    })?;
    ensure!(
        statement.columns().len() == names.len(),
        "case=vector-null-literal-binary: expected {} columns, actual {:?}",
        names.len(),
        statement.columns()
    );
    for (index, name) in names.iter().enumerate() {
        require_column(
            &statement,
            index,
            name,
            &Type::FLOAT8,
            "vector-null-literal-binary",
        )?;
    }
    let rows = h.lake.query(&statement, &[]).await.with_context(|| {
        format!("case=vector-null-literal-binary stage=execute SQL={sql}")
    })?;
    ensure!(
        rows.len() == 1,
        "case=vector-null-literal-binary: expected one row, actual {}",
        rows.len()
    );
    for (index, name) in names.iter().enumerate() {
        let actual: Option<f64> = rows[0]
            .try_get(index)
            .with_context(|| format!("case=vector-null-literal-binary: column {name}"))?;
        ensure!(
            actual.is_none(),
            "case=vector-null-literal-binary: column {name} expected SQL NULL, actual {actual:?}"
        );
    }

    let simple = compare::simple(&h.lake, &sql)
        .await
        .with_context(|| format!("case=vector-null-literal-text SQL={sql}"))?;
    let columns: Vec<(&str, Type)> =
        names.iter().map(|name| (*name, Type::FLOAT8)).collect();
    compare::check_columns(&simple, &columns)?;
    let expected: compare::Rows = vec![vec![None; names.len()]];
    ensure!(
        simple.rows == expected,
        "case=vector-null-literal-text: expected one all-NULL row, actual {:?}",
        simple.rows
    );
    eprintln!(
        "PASS case=vector-null-literal protocols=extended-binary,simple-text \
         null_both_sides=true operators=[<->,<#>,<=>]"
    );
    Ok(())
}

fn stored_null_sql() -> String {
    let a = vector_literal(&FIXED_A);
    format!(
        "SELECT vec <-> '{a}'::vector({DIM}) AS l2, \
         vec <#> '{a}'::vector({DIM}) AS neg_ip, \
         vec <=> '{a}'::vector({DIM}) AS cosine \
         FROM public.{VALUES_TABLE} WHERE id = 3"
    )
}

/// A stored SQL NULL vector (`id = 3`) yields NULL from every distance
/// operator, in both protocols.
async fn stored_null_operand(h: &Harness) -> Result<()> {
    let sql = stored_null_sql();
    let names = ["l2", "neg_ip", "cosine"];
    let statement = h.lake.prepare(&sql).await.with_context(|| {
        format!("case=vector-null-stored-binary stage=parse SQL={sql}")
    })?;
    ensure!(
        statement.columns().len() == names.len(),
        "case=vector-null-stored-binary: expected {} columns, actual {:?}",
        names.len(),
        statement.columns()
    );
    for (index, name) in names.iter().enumerate() {
        require_column(
            &statement,
            index,
            name,
            &Type::FLOAT8,
            "vector-null-stored-binary",
        )?;
    }
    let rows = h.lake.query(&statement, &[]).await.with_context(|| {
        format!("case=vector-null-stored-binary stage=execute SQL={sql}")
    })?;
    ensure!(
        rows.len() == 1,
        "case=vector-null-stored-binary: expected one row, actual {}",
        rows.len()
    );
    for (index, name) in names.iter().enumerate() {
        let actual: Option<f64> = rows[0]
            .try_get(index)
            .with_context(|| format!("case=vector-null-stored-binary: column {name}"))?;
        ensure!(
            actual.is_none(),
            "case=vector-null-stored-binary: column {name} expected SQL NULL, actual {actual:?}"
        );
    }

    let simple = compare::simple(&h.lake, &sql)
        .await
        .with_context(|| format!("case=vector-null-stored-text SQL={sql}"))?;
    let columns: Vec<(&str, Type)> =
        names.iter().map(|name| (*name, Type::FLOAT8)).collect();
    compare::check_columns(&simple, &columns)?;
    let expected: compare::Rows = vec![vec![None; names.len()]];
    ensure!(
        simple.rows == expected,
        "case=vector-null-stored-text: expected one all-NULL row, actual {:?}",
        simple.rows
    );
    eprintln!(
        "PASS case=vector-null-stored protocols=extended-binary,simple-text \
         id=3 null_propagation=true"
    );
    Ok(())
}

fn explicit_parameter_sql() -> String {
    format!(
        "SELECT id, $1::vector({DIM}) AS echoed, vec::vector({DIM}) AS vec, \
         vec <-> $1::vector({DIM}) AS distance \
         FROM public.{VALUES_TABLE} ORDER BY id"
    )
}

fn bare_parameter_sql() -> String {
    format!(
        "SELECT id, vec::vector({DIM}) AS vec, vec <-> $1 AS distance \
         FROM public.{VALUES_TABLE} ORDER BY id"
    )
}

#[derive(Debug, PartialEq)]
struct ParameterRow {
    id: i64,
    echoed: Option<Vector>,
    vector: Option<Vector>,
    distance: Option<f64>,
}

async fn execute_parameter_rows(
    client: &Client,
    statement: &Statement,
    bound: Option<&Vector>,
    echo: bool,
    case: &str,
) -> Result<Vec<ParameterRow>> {
    let rows = match bound {
        Some(vector) => {
            let params: [&(dyn ToSql + Sync); 1] = [vector];
            client.query(statement, &params).await
        }
        None => {
            let null: Option<Vector> = None;
            let params: [&(dyn ToSql + Sync); 1] = [&null];
            client.query(statement, &params).await
        }
    }
    .with_context(|| format!("case={case} stage=execute bound={}", bound.is_some()))?;
    let expected = stored_vectors();
    ensure!(
        rows.len() == expected.len(),
        "case={case}: expected {} rows, actual {}",
        expected.len(),
        rows.len()
    );
    let (vector_index, distance_index) = if echo { (2, 3) } else { (1, 2) };
    let mut decoded_rows = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let expected_id = (index + 1) as i64;
        let id: i64 = row.try_get(0)?;
        ensure!(
            id == expected_id,
            "case={case}: row {index} id: expected {expected_id}, actual {id}"
        );
        let echoed = if echo {
            Some(row.try_get::<_, Option<Vector>>(1).with_context(|| {
                format!("case={case}: row {index} echoed vector decode")
            })?)
        } else {
            None
        };
        let vector: Option<Vector> = row
            .try_get(vector_index)
            .with_context(|| format!("case={case}: row {index} stored vector decode"))?;
        let distance: Option<f64> = row
            .try_get(distance_index)
            .with_context(|| format!("case={case}: row {index} distance decode"))?;

        ensure!(
            vector.as_ref().map(|vector| vector.to_vec()).as_ref()
                == expected[index].as_ref(),
            "case={case}: row {index} stored vector: expected {:?}, actual {:?}",
            expected[index],
            vector
        );
        if echo {
            let expected_echo = bound.map(|vector| vector.to_vec());
            ensure!(
                echoed
                    .as_ref()
                    .and_then(|echoed| echoed.as_ref())
                    .map(|vector| vector.to_vec())
                    == expected_echo,
                "case={case}: row {index} echoed bound vector: expected {:?}, actual {:?}",
                expected_echo,
                echoed
            );
        }
        match (bound, expected[index].as_ref()) {
            (Some(query), Some(stored)) => {
                let actual = distance.ok_or_else(|| {
                    anyhow!("case={case}: row {index} distance is unexpectedly NULL")
                })?;
                assert_close(
                    &format!("{case}-row{}", index + 1),
                    actual,
                    l2_oracle(stored, query.as_slice()),
                    L2_ABS_TOLERANCE,
                )?;
            }
            _ => ensure!(
                distance.is_none(),
                "case={case}: row {index} expected SQL NULL distance, actual {distance:?}"
            ),
        }
        decoded_rows.push(ParameterRow {
            id,
            echoed: echoed.flatten(),
            vector,
            distance,
        });
    }
    Ok(decoded_rows)
}

/// Prepare a statement whose parameter is the server's dynamic `vector` type,
/// then reuse that one `Statement` with two different vectors, SQL NULL and the
/// first vector again. `vector_values` has no index, so every distance is the
/// exact brute-force value.
async fn parameter_statement(
    h: &Harness,
    case: &str,
    sql: &str,
    echo: bool,
    vector_type: &Type,
) -> Result<()> {
    let statement = h
        .lake
        .prepare(sql)
        .await
        .with_context(|| format!("case={case} stage=parse SQL={sql}"))?;
    let names: Vec<_> = statement
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    if echo {
        ensure!(
            names.as_slice() == &["id", "echoed", "vec", "distance"][..],
            "case={case}: expected columns [id, echoed, vec, distance], actual {names:?}"
        );
        require_column(&statement, 0, "id", &Type::INT8, case)?;
        assert_vector_identity(
            &format!("{case}-echoed-type"),
            statement.columns()[1].type_(),
            vector_type,
        )?;
        assert_vector_identity(
            &format!("{case}-stored-type"),
            statement.columns()[2].type_(),
            vector_type,
        )?;
        require_column(&statement, 3, "distance", &Type::FLOAT8, case)?;
    } else {
        ensure!(
            names.as_slice() == &["id", "vec", "distance"][..],
            "case={case}: expected columns [id, vec, distance], actual {names:?}"
        );
        require_column(&statement, 0, "id", &Type::INT8, case)?;
        assert_vector_identity(
            &format!("{case}-stored-type"),
            statement.columns()[1].type_(),
            vector_type,
        )?;
        require_column(&statement, 2, "distance", &Type::FLOAT8, case)?;
    }
    ensure!(
        statement.params().len() == 1,
        "case={case}: expected one parameter, actual {:?}",
        statement.params()
    );
    assert_vector_identity(
        &format!("{case}-parameter-type"),
        &statement.params()[0],
        vector_type,
    )?;

    let first = Vector::from(FIXED_A.to_vec());
    let second = Vector::from(FIXED_B.to_vec());
    let first_rows =
        execute_parameter_rows(&h.lake, &statement, Some(&first), echo, case).await?;
    let second_rows =
        execute_parameter_rows(&h.lake, &statement, Some(&second), echo, case).await?;
    let first_distances: Vec<_> = first_rows.iter().map(|row| row.distance).collect();
    let second_distances: Vec<_> = second_rows.iter().map(|row| row.distance).collect();
    ensure!(
        first_distances != second_distances,
        "case={case}: rebinding a different vector must change the distances, \
         first={first_distances:?} second={second_distances:?}"
    );

    let null_rows = execute_parameter_rows(&h.lake, &statement, None, echo, case).await?;
    for row in &null_rows {
        ensure!(
            row.distance.is_none(),
            "case={case}: NULL parameter must produce NULL distance, actual {:?}",
            row.distance
        );
        if echo {
            ensure!(
                row.echoed.is_none(),
                "case={case}: NULL parameter must round-trip as SQL NULL, actual {:?}",
                row.echoed
            );
        }
    }

    let repeat_rows =
        execute_parameter_rows(&h.lake, &statement, Some(&first), echo, case).await?;
    ensure!(
        repeat_rows == first_rows,
        "case={case}: reusing the statement with the first vector must reproduce the \
         exact rows; first={first_rows:?} repeat={repeat_rows:?}"
    );
    eprintln!(
        "PASS case={case} statements=1 executions=4 bindings=[first,second,NULL,first] \
         parameter_type=vector null_shape=sql-null echo={echo}"
    );
    Ok(())
}

async fn recover(h: &Harness, case: &str) -> Result<()> {
    // The very first query after the server error proves the connection is
    // still usable; there is no retry and no reconnect.
    common::assert_ids(
        &h.lake,
        &format!("{case}-recovery-first-query"),
        &format!("SELECT id FROM public.{VALUES_TABLE} ORDER BY id"),
        &[1, 2, 3],
    )
    .await
}

fn assert_server_error(
    case: &str,
    stage: &str,
    error: &tokio_postgres::Error,
    expected_state: &str,
) -> Result<()> {
    let db = error.as_db_error().ok_or_else(|| {
        anyhow!(
            "case={case} stage={stage}: expected server SQLSTATE {expected_state}, \
             but the driver rejected the value locally: {error}"
        )
    })?;
    ensure!(
        db.code().code() == expected_state,
        "case={case} stage={stage}: SQLSTATE: expected {expected_state}, actual {} \
         message={:?}",
        db.code().code(),
        db.message()
    );
    eprintln!(
        "PASS case={case} stage={stage} sqlstate={expected_state} \
         server_error=true message={:?}",
        db.message()
    );
    Ok(())
}

async fn expect_parse_error(
    h: &Harness,
    case: &str,
    sql: &str,
    expected_state: &str,
) -> Result<()> {
    for extended in [false, true] {
        let stage = if extended {
            "extended-parse"
        } else {
            "simple-planning"
        };
        let result = if extended {
            h.lake.prepare(sql).await.map(drop)
        } else {
            h.lake.simple_query(sql).await.map(drop)
        };
        match result {
            Ok(()) => bail!(
                "case={case} stage={stage}: expected server rejection SQLSTATE {expected_state}, SQL={sql}"
            ),
            Err(error) => assert_server_error(case, stage, &error, expected_state)?,
        }
        recover(h, case).await?;
    }
    Ok(())
}

/// Every rejection is asserted at the stage the current server actually
/// rejects it, with the SQLSTATE from the source path, and the first following
/// query proves the connection survived.
async fn rejection_cases(h: &Harness, vector_type: &Type) -> Result<()> {
    // The literal declares `vector(8)` but carries two components: the
    // operand normalizer rejects this while building the logical plan (Parse).
    let case = "vector-reject-declared-dim-literal";
    let sql = format!(
        "SELECT vec <-> '[1,2]'::vector({DIM}) AS distance \
         FROM public.{VALUES_TABLE} ORDER BY id"
    );
    expect_parse_error(h, case, &sql, "XX000").await?;

    // A bare literal is checked against the dimension the other operand
    // declares; DataFusion would otherwise compare only shared components.
    let case = "vector-reject-bare-literal-dim";
    let full = vector_literal(&FIXED_A);
    let sql = format!("SELECT '[1,2,3]' <-> '{full}'::vector({DIM}) AS distance");
    expect_parse_error(h, case, &sql, "XX000").await?;

    // Malformed literals are not rewritten into vector lists; the distance
    // function's type coercion reports the unsupported UTF-8 argument (Parse).
    for (case, malformed) in [
        ("vector-reject-malformed-literal", "oops"),
        ("vector-reject-malformed-empty", "[]"),
    ] {
        let sql = format!(
            "SELECT vec <-> '{malformed}' AS distance \
             FROM public.{VALUES_TABLE} ORDER BY id"
        );
        expect_parse_error(h, case, &sql, "XX000").await?;
    }

    // A bound `pgvector::Vector` shorter than the declared `vector(8)` cast is
    // rejected by the parameter decoder at Bind time with invalid parameter
    // value (22023), not by the client's ToSql serializer.
    let case = "vector-reject-parameter-dim";
    let sql = format!(
        "SELECT id, vec <-> $1::vector({DIM}) AS distance \
         FROM public.{VALUES_TABLE} ORDER BY id"
    );
    let statement = h
        .lake
        .prepare(&sql)
        .await
        .with_context(|| format!("case={case} stage=parse SQL={sql}"))?;
    ensure!(
        statement.params().len() == 1,
        "case={case}: expected one parameter, actual {:?}",
        statement.params()
    );
    assert_vector_identity(
        &format!("{case}-parameter-type"),
        &statement.params()[0],
        vector_type,
    )?;
    let short = Vector::from(vec![0.0; DIM - 1]);
    let error = match h.lake.query(&statement, &[&short]).await {
        Ok(_) => bail!(
            "case={case} stage=bind: expected server rejection SQLSTATE 22023, \
             but executing a {}-component parameter succeeded SQL={sql}",
            DIM - 1
        ),
        Err(error) => error,
    };
    assert_server_error(case, "extended-bind/execute", &error, "22023")?;
    recover(h, case).await?;

    // A bare `$1` carries a variable-length list type (not dimension 8), so the
    // decoder accepts the value and DataFusion's distance execution rejects the
    // unequal component counts. This is an execute-stage check, not a
    // declared-dimension Parse check.
    let case = "vector-reject-bare-parameter-length";
    let sql = format!(
        "SELECT id, vec <-> $1 AS distance \
         FROM public.{VALUES_TABLE} ORDER BY id"
    );
    let statement = h
        .lake
        .prepare(&sql)
        .await
        .with_context(|| format!("case={case} stage=parse SQL={sql}"))?;
    ensure!(
        statement.params().len() == 1,
        "case={case}: expected one parameter, actual {:?}",
        statement.params()
    );
    assert_vector_identity(
        &format!("{case}-parameter-type"),
        &statement.params()[0],
        vector_type,
    )?;
    let short = Vector::from(vec![0.0; DIM - 1]);
    let error = match h.lake.query(&statement, &[&short]).await {
        Ok(_) => bail!(
            "case={case} stage=execute: expected the distance length check to reject a \
             {}-component bare parameter, but execution succeeded SQL={sql}",
            DIM - 1
        ),
        Err(error) => error,
    };
    assert_server_error(case, "extended-execute", &error, "XX000")?;
    recover(h, case).await
}
