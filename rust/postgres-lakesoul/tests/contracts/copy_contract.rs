// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `COPY` contracts (both directions) against the real server.
//!
//! Every case runs through the real PostgreSQL wire protocol against a fresh
//! `postgres-lakesoul` child. `tokio-postgres` `copy_in` uses the extended
//! protocol (Bind/Execute, then CopyData/CopyDone/Sync), so these tests pin
//! the copy-in response and the write the server performs at CopyDone; the
//! simple-protocol path is covered by the client-compatibility suite
//! (`psycopg2`, `pgjdbc`).
//!
//! The cases cover both formats, framing that splits a row across CopyData
//! messages, escapes and quoted fields, custom delimiter/NULL options, an
//! omitted column list, a transaction block, and the refusal matrix. A failed
//! copy must leave no row behind: the duplicate-key and malformed-value cases
//! assert the target row count, and the refusal matrix compares the
//! authoritative metadata/warehouse snapshot around the failures.

use crate::support;

use std::pin::pin;

use crate::support::Harness;
use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use uuid::Uuid;

macro_rules! ensure_eq {
    ($left:expr, $right:expr $(, $($arg:tt)+)?) => {
        ensure!($left == $right $(, $($arg)+)?)
    };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn copy_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { copy_scenarios(h).await })).await
}

async fn copy_scenarios(h: &Harness) -> Result<()> {
    text_round_trip(h).await?;
    csv_round_trip(h).await?;
    custom_options(h).await?;
    whole_table_and_omitted_columns(h).await?;
    typed_columns(h).await?;
    semantic_arrays(h).await?;
    json_copy(h).await?;
    empty_copy_writes_nothing(h).await?;
    transaction_rollback(h).await?;
    failed_copy_leaves_no_rows(h).await?;
    copy_out_round_trip(h).await?;
    refusal_matrix_changes_nothing(h).await?;
    Ok(())
}

/// TEXT format: row framing across CopyData messages, `\N`, escapes and the
/// `COPY <rows>` tag.
async fn text_round_trip(h: &Harness) -> Result<()> {
    let mut sink = pin!(
        h.lake
            .copy_in("COPY empty_items (id, val, label) FROM STDIN")
            .await
            .context("start text COPY")?
    );
    sink.send(Bytes::from_static(b"1\t10\talice\n2\t\\N\t"))
        .await
        .context("send first text frame")?;
    // The row continues in the next CopyData message.
    sink.send(Bytes::from_static(b"bob\n3\t30\tline\\nnext\n"))
        .await
        .context("send second text frame")?;
    let rows = sink.finish().await.context("finish text COPY")?;
    ensure_eq!(rows, 3, "COPY tag row count");

    let first = h
        .lake
        .query_one("SELECT val, label FROM empty_items WHERE id = 1", &[])
        .await
        .context("read the first copied row")?;
    ensure_eq!(first.get::<_, Option<i32>>(0), Some(10), "val of row 1");
    ensure_eq!(first.get::<_, String>(1), "alice", "label of row 1");

    let second = h
        .lake
        .query_one("SELECT val, label FROM empty_items WHERE id = 2", &[])
        .await?;
    ensure_eq!(second.get::<_, Option<i32>>(0), None, "\\N becomes NULL");
    ensure_eq!(second.get::<_, String>(1), "bob", "label of row 2");

    let escaped: String = h
        .lake
        .query_one("SELECT label FROM empty_items WHERE id = 3", &[])
        .await?
        .get(0);
    ensure_eq!(escaped, "line\nnext", "\\n unescapes to a newline");
    eprintln!("PASS case=copy-text rows=3 null=ok escapes=ok");
    Ok(())
}

/// CSV format: header, quoted fields with commas and newlines, a quoted empty
/// string and an unquoted empty field (NULL).
async fn csv_round_trip(h: &Harness) -> Result<()> {
    let mut sink = pin!(
        h.lake
            .copy_in(
                "COPY empty_items (id, val, label) FROM STDIN \
                 WITH (FORMAT csv, HEADER true)",
            )
            .await
            .context("start CSV COPY")?
    );
    sink.send(Bytes::from_static(
        b"id,val,label\n10,100,\"a,b\"\n11,,\"line\nnext\"\n12,120,\"\"\n",
    ))
    .await
    .context("send CSV frames")?;
    let rows = sink.finish().await.context("finish CSV COPY")?;
    ensure_eq!(rows, 3, "CSV COPY tag row count");
    ensure_eq!(
        h.lake
            .query_one("SELECT count(*) FROM empty_items WHERE id = 10", &[])
            .await?
            .get::<_, i64>(0),
        1,
        "header row skipped, first row copied"
    );

    let label: String = h
        .lake
        .query_one("SELECT label FROM empty_items WHERE id = 10", &[])
        .await?
        .get(0);
    ensure_eq!(label, "a,b", "quoted comma");
    let label: String = h
        .lake
        .query_one("SELECT label FROM empty_items WHERE id = 11", &[])
        .await?
        .get(0);
    ensure_eq!(label, "line\nnext", "quoted newline");
    ensure_eq!(
        h.lake
            .query_one("SELECT val FROM empty_items WHERE id = 11", &[])
            .await?
            .get::<_, Option<i32>>(0),
        None,
        "unquoted empty CSV field is NULL"
    );
    ensure_eq!(
        h.lake
            .query_one("SELECT label FROM empty_items WHERE id = 12", &[])
            .await?
            .get::<_, String>(0),
        "",
        "quoted empty CSV field is an empty string"
    );
    eprintln!("PASS case=copy-csv rows=3 header=skipped quotes=ok null=ok");
    Ok(())
}

/// A custom delimiter must protect an escaped delimiter, and a custom NULL
/// marker must not be confused with data.
async fn custom_options(h: &Harness) -> Result<()> {
    let mut sink = pin!(
        h.lake
            .copy_in(
                "COPY empty_items (id, val, label) FROM STDIN \
                 WITH (FORMAT text, DELIMITER '|', NULL 'NULL')",
            )
            .await
            .context("start COPY with custom options")?
    );
    sink.send(Bytes::from_static(b"20|200|NULL\n21|210|a\\|b\n"))
        .await
        .context("send custom-option rows")?;
    ensure_eq!(
        sink.finish().await.context("finish custom-option COPY")?,
        2,
        "custom-option COPY tag"
    );
    ensure_eq!(
        h.lake
            .query_one("SELECT label FROM empty_items WHERE id = 20", &[])
            .await?
            .get::<_, Option<String>>(0),
        None,
        "custom NULL marker becomes NULL"
    );
    ensure_eq!(
        h.lake
            .query_one("SELECT label FROM empty_items WHERE id = 21", &[])
            .await?
            .get::<_, String>(0),
        "a|b",
        "escaped delimiter is data"
    );
    eprintln!("PASS case=copy-custom-options delimiter=ok null=ok");
    Ok(())
}

/// No column list copies every column in table order; an explicit list may
/// omit trailing columns, which are filled with NULL.
async fn whole_table_and_omitted_columns(h: &Harness) -> Result<()> {
    let mut sink = pin!(
        h.lake
            .copy_in("COPY empty_items FROM STDIN")
            .await
            .context("start whole-table COPY")?
    );
    sink.send(Bytes::from_static(b"30\t300\twhole\n"))
        .await
        .context("send whole-table row")?;
    ensure_eq!(
        sink.finish().await.context("finish whole-table COPY")?,
        1,
        "whole-table COPY tag"
    );

    h.lake
        .batch_execute(
            "CREATE TABLE copy_subset (\
               id BIGINT NOT NULL PRIMARY KEY, val INTEGER, label TEXT\
             )",
        )
        .await
        .context("create copy_subset")?;
    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_subset (id, label) FROM STDIN")
            .await
            .context("start COPY with an omitted column")?
    );
    sink.send(Bytes::from_static(b"1\tone\n2\t\\N\n"))
        .await
        .context("send omitted-column rows")?;
    ensure_eq!(
        sink.finish().await.context("finish omitted-column COPY")?,
        2,
        "omitted-column COPY tag"
    );
    ensure_eq!(
        h.lake
            .query_one("SELECT count(*) FROM copy_subset WHERE val IS NULL", &[])
            .await?
            .get::<_, i64>(0),
        2,
        "omitted columns are NULL"
    );
    ensure_eq!(
        h.lake
            .query_one("SELECT label FROM copy_subset WHERE id = 2", &[])
            .await?
            .get::<_, Option<String>>(0),
        None,
        "\\N is still NULL in a column-list copy"
    );
    eprintln!("PASS case=copy-columns whole-table=ok omitted=null");
    Ok(())
}

/// COPY decodes every type the LakeSoul DDL creates. The bytea value is the
/// interesting one: COPY TEXT escapes the backslash, so the stream carries
/// `\\xdead` and the field decoder must hand `\xdead` to bytea input.
async fn typed_columns(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE copy_types (\
               id BIGINT NOT NULL PRIMARY KEY, flag BOOLEAN, \
               amount NUMERIC(20,4), day DATE, at TIMESTAMPTZ, data BYTEA\
             )",
        )
        .await
        .context("create copy_types")?;
    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_types FROM STDIN")
            .await
            .context("start typed COPY")?
    );
    sink.send(Bytes::from_static(
        b"1\tt\t12.3400\t2024-01-02\t2024-01-02 03:04:05+00\t\\\\xdead\n",
    ))
    .await
    .context("send typed row")?;
    ensure_eq!(
        sink.finish().await.context("finish typed COPY")?,
        1,
        "typed COPY tag"
    );

    let row = h
        .lake
        .query_one(
            "SELECT flag, amount, day, at, data FROM copy_types WHERE id = 1",
            &[],
        )
        .await
        .context("read the typed row")?;
    ensure_eq!(row.get::<_, bool>(0), true, "boolean");
    ensure_eq!(
        row.get::<_, rust_decimal::Decimal>(1).to_string(),
        "12.3400",
        "numeric keeps its scale"
    );
    ensure_eq!(
        row.get::<_, chrono::NaiveDate>(2).to_string(),
        "2024-01-02",
        "date"
    );
    ensure_eq!(
        row.get::<_, chrono::DateTime<chrono::Utc>>(3).to_rfc3339(),
        "2024-01-02T03:04:05+00:00",
        "timestamptz"
    );
    ensure_eq!(row.get::<_, Vec<u8>>(4), vec![0xde, 0xad], "bytea");
    eprintln!("PASS case=copy-types bool=ok numeric=ok date=ok timestamptz=ok bytea=ok");
    Ok(())
}

/// COPY preserves semantic array element types: UUID, money, 40-digit numeric
/// and character length, including quoted elements and NULLs.
async fn semantic_arrays(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE copy_arrays (\
               id BIGINT NOT NULL PRIMARY KEY, us UUID[], m MONEY[], \
               n NUMERIC(40,2)[], v VARCHAR(5)[]\
             )",
        )
        .await
        .context("create copy_arrays")?;
    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_arrays FROM STDIN")
            .await
            .context("start array COPY")?
    );
    sink.send(Bytes::from_static(
        b"1\t{550e8400-e29b-41d4-a716-446655440000}\t{\"$1,234.56\"}\t{12345678901234567890123456789012345678.90}\t{\"a,b\",NULL}\n",
    ))
    .await
    .context("send array row")?;
    ensure_eq!(
        sink.finish().await.context("finish array COPY")?,
        1,
        "array COPY tag"
    );

    let row = h
        .lake
        .query_one(
            "SELECT us, m[1]::text, n[1]::text, v FROM copy_arrays WHERE id = 1",
            &[],
        )
        .await
        .context("read the array row")?;
    let us: Vec<Uuid> = row.get(0);
    ensure_eq!(us.len(), 1, "UUID[] length");
    ensure_eq!(
        us[0].to_string(),
        "550e8400-e29b-41d4-a716-446655440000",
        "UUID[] element"
    );
    ensure_eq!(row.get::<_, String>(1), "1234.56", "money array element");
    ensure_eq!(
        row.get::<_, String>(2),
        "12345678901234567890123456789012345678.90",
        "numeric array keeps every digit"
    );
    ensure_eq!(
        row.get::<_, Vec<Option<String>>>(3),
        vec![Some("a,b".to_string()), None],
        "quoted element and NULL"
    );

    // An over-long element is refused before the row is written.
    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_arrays FROM STDIN")
            .await
            .context("start refused array COPY")?
    );
    sink.send(Bytes::from_static(b"2\t{}\t{}\t{}\t{abcdef}\n"))
        .await
        .context("send over-long element")?;
    let error = sink.finish().await.expect_err("over-long array element");
    ensure_eq!(
        error.as_db_error().map(|error| error.code().code()),
        Some("22001"),
        "array element length SQLSTATE"
    );
    Ok(())
}

async fn json_copy(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE copy_json (id BIGINT, doc JSON, docb JSONB)")
        .await
        .context("CREATE TABLE copy_json")?;

    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_json FROM STDIN")
            .await
            .context("start JSON COPY")?
    );
    sink.send(Bytes::from_static(
        b"1\t{\"b\": 2, \"a\": 1}\t{\"b\": 2, \"a\": 1}\n",
    ))
    .await
    .context("send JSON COPY row")?;
    let rows = sink.finish().await.context("finish JSON COPY")?;
    ensure_eq!(rows, 1, "JSON COPY tag row count");

    let row = h
        .lake
        .query_one(
            "SELECT doc, docb, docb::text AS docb_text FROM copy_json WHERE id = 1",
            &[],
        )
        .await
        .context("read JSON COPY row")?;
    ensure_eq!(
        row.get::<_, serde_json::Value>(0),
        serde_json::json!({"b": 2, "a": 1}),
        "json keeps its document"
    );
    ensure_eq!(
        row.get::<_, serde_json::Value>(1),
        serde_json::json!({"a": 1, "b": 2}),
        "jsonb is normalized"
    );
    let text = row.get::<_, String>(2);
    ensure!(
        text == "{\"a\":1,\"b\":2}",
        "jsonb text form: expected canonical jsonb, actual {text:?}"
    );

    let mut sink = pin!(
        h.lake
            .copy_in::<_, Bytes>("COPY copy_json (id, docb) FROM STDIN")
            .await
            .context("start invalid JSON COPY")?
    );
    sink.send(Bytes::from_static(b"2\t{not json\n"))
        .await
        .context("send invalid JSON COPY row")?;
    let error = sink
        .finish()
        .await
        .expect_err("invalid jsonb COPY must fail");
    ensure_eq!(
        error.code(),
        Some(&tokio_postgres::error::SqlState::INVALID_TEXT_REPRESENTATION),
        "invalid jsonb COPY is 22P02"
    );

    h.lake
        .batch_execute("DROP TABLE copy_json")
        .await
        .context("DROP TABLE copy_json")?;
    eprintln!("PASS case=copy-json json=ok jsonb-normalized=ok invalid=22P02");
    Ok(())
}

async fn empty_copy_writes_nothing(h: &Harness) -> Result<()> {
    let sink = pin!(
        h.lake
            .copy_in::<_, Bytes>("COPY empty_items (id, val, label) FROM STDIN")
            .await
            .context("start empty COPY")?
    );
    ensure_eq!(
        sink.finish().await.context("finish empty COPY")?,
        0,
        "empty COPY tag"
    );
    ensure_eq!(
        h.lake
            .query_one("SELECT count(*) FROM empty_items WHERE id = 0", &[])
            .await?
            .get::<_, i64>(0),
        0,
        "empty COPY inserted nothing"
    );
    eprintln!("PASS case=copy-empty rows=0");
    Ok(())
}

/// A copy inside a transaction block is staged: no peer sees it before
/// `COMMIT`, and `ROLLBACK` removes it.
async fn transaction_rollback(h: &Harness) -> Result<()> {
    h.lake.batch_execute("BEGIN").await?;
    let mut sink = pin!(
        h.lake
            .copy_in("COPY empty_items (id, val, label) FROM STDIN")
            .await
            .context("start COPY inside a block")?
    );
    sink.send(Bytes::from_static(b"40\t400\tstaged\n"))
        .await
        .context("send staged row")?;
    ensure_eq!(
        sink.finish().await.context("finish staged COPY")?,
        1,
        "staged COPY tag"
    );
    let peer = h.connect_lake().await?;
    ensure_eq!(
        peer.query_one("SELECT count(*) FROM empty_items WHERE id = 40", &[],)
            .await?
            .get::<_, i64>(0),
        0,
        "a peer must not see a staged COPY"
    );
    h.lake.batch_execute("ROLLBACK").await?;
    ensure_eq!(
        h.lake
            .query_one("SELECT count(*) FROM empty_items WHERE id = 40", &[])
            .await?
            .get::<_, i64>(0),
        0,
        "ROLLBACK removes the staged COPY"
    );
    eprintln!("PASS case=copy-transaction rollback=ok staged=ok");
    Ok(())
}

/// `COPY ... TO STDOUT` streams the plan's rows as TEXT and CSV, from a
/// table and from a query, with the shared options.
async fn copy_out_round_trip(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE copy_export (\
               id BIGINT NOT NULL PRIMARY KEY, label TEXT, amount NUMERIC(20,4)\
             )",
        )
        .await
        .context("create copy_export")?;
    h.lake
        .batch_execute(
            "INSERT INTO copy_export (id, label, amount) VALUES \
             (1, 'a\\tb', 12.3400), (2, NULL, -0.5000)",
        )
        .await
        .context("insert copy_export rows")?;

    let text = copy_out_bytes(
        &h.lake,
        "COPY (SELECT id, label, amount FROM copy_export ORDER BY id) TO STDOUT",
    )
    .await?;
    ensure_eq!(
        text,
        b"1\ta\\\\tb\t12.3400\n2\t\\N\t-0.5000\n".to_vec(),
        "TEXT copy-out"
    );

    let csv = copy_out_bytes(
        &h.lake,
        "COPY (SELECT id, label FROM copy_export ORDER BY id) TO STDOUT \
         WITH (FORMAT csv, HEADER true)",
    )
    .await?;
    ensure_eq!(csv, b"id,label\n1,a\\tb\n2,\n".to_vec(), "CSV copy-out");

    let custom = copy_out_bytes(
        &h.lake,
        "COPY (SELECT id, label FROM copy_export ORDER BY id) TO STDOUT \
         WITH (DELIMITER '|', NULL 'NULL')",
    )
    .await?;
    ensure_eq!(
        custom,
        b"1|a\\\\tb\n2|NULL\n".to_vec(),
        "custom-option copy-out"
    );

    // A table copy streams the same rows; the storage order is not part of
    // the contract, so compare the set of lines.
    let table = copy_out_bytes(&h.lake, "COPY copy_export (id, label) TO STDOUT").await?;
    let mut lines = String::from_utf8(table)
        .context("table copy-out is UTF-8")?
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    lines.sort();
    ensure_eq!(
        lines,
        vec!["1\ta\\\\tb".to_string(), "2\t\\N".to_string()],
        "table copy-out lines"
    );

    // Empty tables stream no rows; the tag is checked by the client suite.
    let empty = copy_out_bytes(
        &h.lake,
        "COPY (SELECT id FROM copy_export WHERE id < 0) TO STDOUT",
    )
    .await?;
    ensure_eq!(empty, Vec::<u8>::new(), "empty copy-out");

    // A block's staged write is visible to its own COPY TO, and disappears
    // with the rollback.
    h.lake.batch_execute("BEGIN").await?;
    h.lake
        .batch_execute(
            "INSERT INTO copy_export (id, label, amount) VALUES (3, 'staged', 1.0)",
        )
        .await
        .context("stage a row for copy-out")?;
    let staged = copy_out_bytes(
        &h.lake,
        "COPY (SELECT id FROM copy_export WHERE id = 3) TO STDOUT",
    )
    .await?;
    ensure_eq!(staged, b"3\n".to_vec(), "staged copy-out");
    h.lake.batch_execute("ROLLBACK").await?;
    let after_rollback = copy_out_bytes(
        &h.lake,
        "COPY (SELECT id FROM copy_export WHERE id = 3) TO STDOUT",
    )
    .await?;
    ensure_eq!(after_rollback, Vec::<u8>::new(), "copy-out after rollback");

    eprintln!("PASS case=copy-out text=csv=query=table=empty=staged");
    Ok(())
}

async fn copy_out_bytes(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<u8>> {
    let stream = client
        .copy_out(sql)
        .await
        .with_context(|| format!("copy_out {sql}"))?;
    let mut stream = pin!(stream);
    let mut data = Vec::new();
    while let Some(chunk) = stream.next().await {
        data.extend_from_slice(&chunk.with_context(|| format!("read copy_out {sql}"))?);
    }
    Ok(data)
}

async fn copy_out_error(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<tokio_postgres::Error> {
    match client.copy_out(sql).await {
        Ok(_) => bail!("COPY statement `{sql}` started a copy-out"),
        Err(error) => Ok(error),
    }
}

/// A copy is one commit: a duplicate key or a malformed value publishes
/// nothing, not even the rows that preceded the failing one.
async fn failed_copy_leaves_no_rows(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE copy_pk (\
               id BIGINT NOT NULL PRIMARY KEY, val INTEGER, label TEXT\
             )",
        )
        .await
        .context("create copy_pk")?;

    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_pk FROM STDIN")
            .await
            .context("start duplicate-key COPY")?
    );
    sink.send(Bytes::from_static(b"1\t1\tfirst\n1\t2\tsecond\n"))
        .await
        .context("send duplicate-key rows")?;
    let error = sink
        .finish()
        .await
        .expect_err("a duplicate key must fail the COPY");
    ensure_sqlstate(&error, "23505", "duplicate key")?;
    ensure_eq!(
        h.lake
            .query_one("SELECT count(*) FROM copy_pk", &[])
            .await?
            .get::<_, i64>(0),
        0,
        "a failed COPY publishes no row"
    );

    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_pk (id, val) FROM STDIN")
            .await
            .context("start malformed-value COPY")?
    );
    sink.send(Bytes::from_static(b"2\tnot-a-number\n"))
        .await
        .context("send malformed value")?;
    let error = sink
        .finish()
        .await
        .expect_err("a malformed integer must fail the COPY");
    ensure_sqlstate(&error, "22P02", "malformed integer")?;

    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_pk (id, val) FROM STDIN")
            .await
            .context("start short-row COPY")?
    );
    sink.send(Bytes::from_static(b"3\n"))
        .await
        .context("send a short row")?;
    let error = sink
        .finish()
        .await
        .expect_err("a row with a missing column must fail the COPY");
    ensure_sqlstate(&error, "22P04", "missing column")?;

    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_pk (val) FROM STDIN")
            .await
            .context("start NULL-primary-key COPY")?
    );
    sink.send(Bytes::from_static(b"9\n"))
        .await
        .context("send a NULL primary key")?;
    let error = sink
        .finish()
        .await
        .expect_err("a NULL primary key must fail the COPY");
    ensure_sqlstate(&error, "23502", "NULL primary key")?;

    let mut sink = pin!(
        h.lake
            .copy_in("COPY copy_pk (id, val, label) FROM STDIN WITH (FORMAT csv)",)
            .await
            .context("start unterminated-CSV COPY")?
    );
    sink.send(Bytes::from_static(b"4,4,\"open\n"))
        .await
        .context("send an unterminated quoted field")?;
    let error = sink
        .finish()
        .await
        .expect_err("an unterminated CSV quote must fail the COPY");
    ensure_sqlstate(&error, "22P04", "unterminated CSV quote")?;

    ensure_eq!(
        h.lake
            .query_one("SELECT count(*) FROM copy_pk", &[])
            .await?
            .get::<_, i64>(0),
        0,
        "no failed COPY published a row"
    );
    eprintln!(
        "PASS case=copy-failures duplicate=23505 value=22P02 \
         arity=22P04 not-null=23502 csv=22P04 rows=0"
    );
    Ok(())
}

/// Unsupported copy forms are refused before any state changes.
async fn refusal_matrix_changes_nothing(h: &Harness) -> Result<()> {
    let before = h.authoritative_snapshot().await?;

    for (sql, code, case) in [
        (
            "COPY empty_items (id, val) FROM STDIN WITH (FORMAT binary)",
            "0A000",
            "binary format",
        ),
        (
            "COPY empty_items FROM '/tmp/items.csv'",
            "0A000",
            "file source",
        ),
        (
            "COPY empty_items FROM STDIN WITH (FORCE_NOT_NULL (val))",
            "0A000",
            "FORCE_NOT_NULL",
        ),
        (
            "COPY empty_items FROM STDIN WITH (FREEZE)",
            "0A000",
            "FREEZE",
        ),
        ("COPY missing_copy FROM STDIN", "42P01", "missing table"),
        (
            "COPY empty_items (missing) FROM STDIN",
            "42703",
            "missing column",
        ),
        (
            "COPY empty_items (id, id) FROM STDIN",
            "42701",
            "duplicate column",
        ),
        (
            "COPY empty_items (id) FROM STDIN WITH (HEADER true)",
            "42601",
            "HEADER in text mode",
        ),
    ] {
        let error = copy_in_error(&h.lake, sql).await?;
        ensure_sqlstate(&error, code, case)?;
    }

    for (sql, code, case) in [
        (
            "COPY empty_items TO STDOUT WITH (FORMAT binary)",
            "0A000",
            "binary copy-out",
        ),
        (
            "COPY empty_items TO '/tmp/items.csv'",
            "0A000",
            "copy-out to a file",
        ),
        (
            "COPY empty_items TO PROGRAM 'cat'",
            "0A000",
            "copy-out to a program",
        ),
        (
            "COPY missing_copy TO STDOUT",
            "42P01",
            "copy-out missing table",
        ),
        (
            "COPY empty_items (missing) TO STDOUT",
            "42703",
            "copy-out missing column",
        ),
    ] {
        let error = copy_out_error(&h.lake, sql).await?;
        ensure_sqlstate(&error, code, case)?;
    }

    let after = h.authoritative_snapshot().await?;
    ensure!(
        before == after,
        "refused COPY statements changed metadata or the warehouse"
    );
    eprintln!("PASS case=copy-refusals forms=10 snapshot=unchanged");
    Ok(())
}

async fn copy_in_error(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<tokio_postgres::Error> {
    match client.copy_in::<_, Bytes>(sql).await {
        Ok(_) => bail!("COPY statement `{sql}` was accepted"),
        Err(error) => Ok(error),
    }
}

fn ensure_sqlstate(
    error: &tokio_postgres::Error,
    expected: &str,
    case: &str,
) -> Result<()> {
    let database = error.as_db_error().with_context(|| {
        format!("case={case}: expected a server ErrorResponse, actual: {error}")
    })?;
    ensure!(
        database.code().code() == expected,
        "case={case}: expected SQLSTATE={expected}, actual {}",
        database.code().code()
    );
    eprintln!(
        "PASS case=copy-refusal {case} sqlstate={}",
        database.code().code()
    );
    Ok(())
}
