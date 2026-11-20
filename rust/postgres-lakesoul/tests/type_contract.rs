// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL wire type contract for the types added in the type-coverage
//! branch: UUID, arrays, money, Decimal256, timestamp/time precision and
//! character lengths.

pub mod support;

use anyhow::{Context, Result, ensure};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use support::Harness;
use tokio_postgres::types::Type;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_type_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { run(h).await })).await
}

async fn run(h: &Harness) -> Result<()> {
    h.lake
        .simple_query(
            "CREATE TABLE type_contract (\
                id BIGINT PRIMARY KEY,\
                u UUID,\
                tags INT[],\
                money MONEY,\
                n NUMERIC(40,2),\
                ts TIMESTAMP(3),\
                t TIME(3),\
                v VARCHAR(5),\
                c CHAR(3))",
        )
        .await?;

    // Extended protocol parameters for the native Arrow types.
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    let time = NaiveTime::from_hms_micro_opt(12, 34, 56, 123_000).unwrap();
    let timestamp = NaiveDate::from_ymd_opt(2024, 1, 2)
        .unwrap()
        .and_hms_micro_opt(3, 4, 5, 123_000)
        .unwrap();
    h.lake
        .execute(
            "INSERT INTO type_contract (id, u, tags, ts, t, v, c) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &1i64,
                &uuid,
                &vec![1i32, 2, 3],
                &timestamp,
                &time,
                &"abc",
                &"a",
            ],
        )
        .await
        .context("parameterized insert for native types")?;

    // Money and Decimal256 via text literals; the wire type is asserted from
    // the row description below.
    h.lake
        .simple_query(
            "INSERT INTO type_contract (id, money, n) VALUES \
             (2, '$1,234.56', 12345678901234567890123456789012345678.90)",
        )
        .await
        .context("money/numeric insert")?;

    h.lake
        .simple_query(
            "UPDATE type_contract SET money = '$2,000.00', \
             n = 11111111111111111111111111111111111111.12 WHERE id = 2",
        )
        .await
        .context("money/numeric update")?;

    let rows = h
        .lake
        .query(
            "SELECT id, u, tags, money, n, ts, t, v, c FROM type_contract ORDER BY id",
            &[],
        )
        .await
        .context("select typed columns")?;
    ensure!(rows.len() == 2, "expected two rows, got {}", rows.len());
    let first = &rows[0];
    ensure!(first.columns()[1].type_() == &Type::UUID, "uuid OID");
    ensure!(first.columns()[2].type_() == &Type::INT4_ARRAY, "array OID");
    ensure!(first.columns()[3].type_() == &Type::MONEY, "money OID");
    ensure!(first.columns()[4].type_() == &Type::NUMERIC, "numeric OID");
    ensure!(
        first.columns()[5].type_() == &Type::TIMESTAMP,
        "timestamp OID"
    );
    ensure!(first.columns()[6].type_() == &Type::TIME, "time OID");
    ensure!(first.columns()[7].type_() == &Type::VARCHAR, "varchar OID");
    ensure!(first.columns()[8].type_() == &Type::BPCHAR, "char OID");

    let got_uuid: Uuid = first.get(1);
    ensure!(got_uuid == uuid, "UUID binary value: {got_uuid}");
    let got_tags: Vec<i32> = first.get(2);
    ensure!(got_tags == vec![1, 2, 3], "array value: {got_tags:?}");
    let got_time: NaiveTime = first.get(6);
    ensure!(got_time == time, "time value: {got_time}");
    let got_timestamp: NaiveDateTime = first.get(5);
    ensure!(
        got_timestamp == timestamp,
        "timestamp value: {got_timestamp}"
    );
    let got_v: String = first.get(7);
    ensure!(got_v == "abc", "varchar value: {got_v}");
    let got_c: String = first.get(8);
    ensure!(got_c.trim_end() == "a", "char value: {got_c:?}");

    // Money/Decimal256 are checked through their text casts so the test does
    // not need an arbitrary-precision client type.
    let values = h
        .lake
        .query(
            "SELECT money::text, n::text FROM type_contract WHERE id = 2",
            &[],
        )
        .await?;
    ensure!(values[0].get::<_, String>(0) == "2000.00");
    ensure!(values[0].get::<_, String>(1) == "11111111111111111111111111111111111111.12");

    // ALTER preserves the semantic metadata and only accepts safe widenings.
    h.lake
        .simple_query("ALTER TABLE type_contract ADD COLUMN u2 UUID")
        .await?;
    h.lake
        .simple_query("ALTER TABLE type_contract ADD COLUMN tags2 INT[]")
        .await?;
    h.lake
        .simple_query("ALTER TABLE type_contract ADD COLUMN ts0 TIMESTAMP(0)")
        .await?;
    h.lake
        .simple_query("ALTER TABLE type_contract ALTER COLUMN v TYPE VARCHAR(10)")
        .await?;
    h.lake
        .simple_query("ALTER TABLE type_contract ALTER COLUMN n TYPE NUMERIC(50,2)")
        .await?;
    h.lake
        .simple_query("ALTER TABLE type_contract ALTER COLUMN ts TYPE TIMESTAMP(6)")
        .await?;
    let narrowed = h
        .lake
        .simple_query("ALTER TABLE type_contract ALTER COLUMN v TYPE VARCHAR(3)")
        .await
        .expect_err("narrowing varchar must be refused");
    ensure!(sqlstate(&narrowed) == Some("0A000"), "{narrowed}");
    let change_uuid = h
        .lake
        .simple_query("ALTER TABLE type_contract ALTER COLUMN u TYPE TEXT")
        .await
        .expect_err("changing UUID to text must be refused");
    ensure!(sqlstate(&change_uuid) == Some("0A000"), "{change_uuid}");

    // Decimal256 up to the Arrow precision limit.
    h.lake
        .simple_query(
            "CREATE TABLE type_contract_76 (id BIGINT PRIMARY KEY, n NUMERIC(76,2))",
        )
        .await?;
    h.lake
        .simple_query(
            "INSERT INTO type_contract_76 VALUES \
             (1, '12345678901234567890123456789012345678901234567890123456789012345678901234.56')",
        )
        .await?;
    let wide = h
        .lake
        .query("SELECT n::text FROM type_contract_76", &[])
        .await?;
    ensure!(
        wide[0].get::<_, String>(0)
            == "12345678901234567890123456789012345678901234567890123456789012345678901234.56"
    );

    let regtypes = h
        .lake
        .query(
            "SELECT to_regtype('uuid'), to_regtype('money'), to_regtype('inet'), \
                    to_regtype('int4[]')",
            &[],
        )
        .await?;
    ensure!(regtypes[0].get::<_, i32>(0) == 2950, "uuid regtype");
    ensure!(regtypes[0].get::<_, i32>(1) == 790, "money regtype");
    ensure!(regtypes[0].get::<_, i32>(2) == 869, "inet regtype");
    ensure!(regtypes[0].get::<_, i32>(3) == 1007, "int4[] regtype");

    // Catalog typmods are visible to psql/SQLAlchemy reflection.
    let catalog = h
        .lake
        .query(
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = 'type_contract' AND a.attnum > 0 \
             ORDER BY a.attnum",
            &[],
        )
        .await?;
    let types: Vec<(String, String)> =
        catalog.iter().map(|row| (row.get(0), row.get(1))).collect();
    ensure!(
        types
            == vec![
                ("id".to_string(), "bigint".to_string()),
                ("u".to_string(), "uuid".to_string()),
                ("tags".to_string(), "integer[]".to_string()),
                ("money".to_string(), "money".to_string()),
                ("n".to_string(), "numeric(50,2)".to_string()),
                (
                    "ts".to_string(),
                    "timestamp(6) without time zone".to_string()
                ),
                ("t".to_string(), "time(3) without time zone".to_string()),
                ("v".to_string(), "character varying(10)".to_string()),
                ("c".to_string(), "character(3)".to_string()),
                ("u2".to_string(), "uuid".to_string()),
                ("tags2".to_string(), "integer[]".to_string()),
                (
                    "ts0".to_string(),
                    "timestamp(0) without time zone".to_string()
                ),
            ],
        "catalog types: {types:?}"
    );
    Ok(())
}

fn sqlstate(error: &tokio_postgres::Error) -> Option<&str> {
    error.as_db_error().map(|error| error.code().code())
}
