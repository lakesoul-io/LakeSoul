// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `CREATE TABLE` / `DROP TABLE` contracts against the real server.
//!
//! Every case is run through the PostgreSQL wire protocol against a fresh
//! `postgres-lakesoul` child; the expected metadata rows are read from the
//! metadata PostgreSQL database directly, so a table that the server reports
//! as created is proven to exist in `table_info`, `table_name_id` and
//! `table_path_id`, and a dropped table is proven to leave none behind.
//!
//! Cases are ordered so that each one starts from the warehouse the previous
//! one left: creation, duplicate/`IF NOT EXISTS`, drop/absent-drop, then the
//! refusal matrix (which must not create anything).

pub mod support;

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::Client;
use tokio_postgres::types::Type;

use support::Harness;

/// One `table_info` row reduced to the fields the DDL contract pins.
struct TableRow {
    table_id: String,
    table_path: String,
    partitions: String,
    hash_bucket_num: Option<String>,
    file_format: Option<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ddl_create_and_drop_tables() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            create_table_publishes_metadata(h).await?;
            duplicate_and_if_not_exists(h).await?;
            drop_table_removes_metadata(h).await?;
            drop_absent_and_if_exists(h).await?;
            options_and_column_types(h).await?;
            refusal_matrix_creates_nothing(h).await?;
            extended_protocol_creates_and_drops(h).await?;
            ddl_is_refused_inside_a_transaction(h).await?;
            Ok(())
        })
    })
    .await
}

/// `CREATE TABLE` publishes a readable, empty table plus its metadata.
async fn create_table_publishes_metadata(h: &Harness) -> Result<()> {
    let table = "ddl_orders";
    command(
        h,
        "CREATE TABLE ddl_orders (id BIGINT NOT NULL PRIMARY KEY, \
                label TEXT, amount NUMERIC(12, 2), created TIMESTAMPTZ)",
    )
    .await?;

    let row = metadata(h, table)
        .await?
        .with_context(|| context(table, "after CREATE TABLE", "metadata row"))?;
    ensure!(
        row.partitions == ";id",
        "{}",
        detail(
            table,
            "primary key partitions",
            &format!("expected \";id\", actual {:?}", row.partitions)
        )
    );
    ensure!(
        row.hash_bucket_num.as_deref() == Some("4"),
        "{}",
        detail(
            table,
            "hash bucket count",
            &format!("expected default 4, actual {:?}", row.hash_bucket_num)
        )
    );
    ensure!(
        row.file_format.as_deref() == Some("parquet"),
        "{}",
        detail(
            table,
            "file format",
            &format!("expected parquet, actual {:?}", row.file_format)
        )
    );
    let expected_suffix = format!("/{}/{table}", h.lake_namespace);
    ensure!(
        row.table_path.ends_with(&expected_suffix),
        "{}",
        detail(
            table,
            "table path",
            &format!(
                "expected suffix {expected_suffix:?}, actual {:?}",
                row.table_path
            )
        )
    );
    ensure!(
        std::path::Path::new(row.table_path.trim_start_matches("file://"))
            .starts_with(&h.warehouse_dir),
        "{}",
        detail(
            table,
            "table path",
            &format!(
                "expected a path under {:?}, actual {:?}",
                h.warehouse_dir, row.table_path
            )
        )
    );

    // Readable through the catalog carried on this connection.
    ensure!(
        pg_class_count(h, table).await? == 1,
        "{}",
        detail(table, "pg_class", "expected exactly one row")
    );
    let statement = h
        .lake
        .prepare("SELECT id, label, amount, created FROM ddl_orders")
        .await
        .with_context(|| context(table, "SELECT of the new table", "prepare"))?;
    let types: Vec<Type> = statement
        .columns()
        .iter()
        .map(|c| c.type_().clone())
        .collect();
    ensure!(
        types == [Type::INT8, Type::TEXT, Type::NUMERIC, Type::TIMESTAMPTZ],
        "{}",
        detail(table, "column types", &format!("actual {types:?}"))
    );
    let rows = h
        .lake
        .query(&statement, &[])
        .await
        .with_context(|| context(table, "SELECT of the new table", "execute"))?;
    ensure!(
        rows.is_empty(),
        "{}",
        detail(
            table,
            "row count",
            &format!("expected 0, actual {}", rows.len())
        )
    );

    // A table without a primary key is a plain, empty relation.
    command(h, "CREATE TABLE ddl_notes (body TEXT, seq INTEGER)").await?;
    let row = metadata(h, "ddl_notes")
        .await?
        .with_context(|| context("ddl_notes", "after CREATE TABLE", "metadata row"))?;
    ensure!(
        row.partitions == ";",
        "{}",
        detail(
            "ddl_notes",
            "partitions",
            &format!("expected \";\", actual {:?}", row.partitions)
        )
    );
    ensure!(
        row.hash_bucket_num.is_none(),
        "{}",
        detail(
            "ddl_notes",
            "hash bucket count",
            &format!(
                "a table without a merge key must not have one, actual {:?}",
                row.hash_bucket_num
            )
        )
    );

    // A fresh connection sees the table as well.
    let peer = h.connect_lake().await?;
    let count = scalar(&peer, "SELECT count(*) FROM ddl_notes").await?;
    ensure!(
        count == 0,
        "{}",
        detail(
            "ddl_notes",
            "row count on a new connection",
            &format!("expected 0, actual {count}")
        )
    );
    Ok(())
}

/// A second `CREATE TABLE` of the same name keeps the first table only.
async fn duplicate_and_if_not_exists(h: &Harness) -> Result<()> {
    let before = metadata(h, "ddl_notes").await?.with_context(|| {
        context("ddl_notes", "before duplicate CREATE", "metadata row")
    })?;
    let error = error_code(h, "CREATE TABLE ddl_notes (body TEXT, seq INTEGER)")
        .await
        .with_context(|| context("ddl_notes", "duplicate CREATE TABLE", "SQLSTATE"))?;
    ensure!(
        error == "42P07",
        "{}",
        detail(
            "ddl_notes",
            "duplicate CREATE TABLE",
            &format!("expected 42P07, actual {error}")
        )
    );

    command(
        h,
        "CREATE TABLE IF NOT EXISTS ddl_notes (body TEXT, seq INTEGER)",
    )
    .await?;
    let after = metadata(h, "ddl_notes")
        .await?
        .with_context(|| context("ddl_notes", "after IF NOT EXISTS", "metadata row"))?;
    ensure!(
        before.table_id == after.table_id,
        "{}",
        detail(
            "ddl_notes",
            "IF NOT EXISTS table identity",
            &format!("expected {}, actual {}", before.table_id, after.table_id)
        )
    );
    ensure!(
        metadata_counts(h, "ddl_notes").await? == [1, 1, 1, 0, 0],
        "{}",
        detail(
            "ddl_notes",
            "metadata rows after IF NOT EXISTS",
            "expected [table_info, table_name_id, table_path_id, partition_info, data_commit_info] = [1, 1, 1, 0, 0]"
        )
    );
    Ok(())
}

/// `DROP TABLE` removes the table and every metadata row that names it.
async fn drop_table_removes_metadata(h: &Harness) -> Result<()> {
    command(h, "DROP TABLE ddl_notes").await?;
    ensure!(
        metadata(h, "ddl_notes").await?.is_none(),
        "{}",
        detail(
            "ddl_notes",
            "after DROP TABLE",
            "table_info row must be gone"
        )
    );
    ensure!(
        metadata_counts(h, "ddl_notes").await? == [0, 0, 0, 0, 0],
        "{}",
        detail(
            "ddl_notes",
            "metadata rows after DROP TABLE",
            "expected every count to be 0"
        )
    );
    ensure!(
        pg_class_count(h, "ddl_notes").await? == 0,
        "{}",
        detail("ddl_notes", "pg_class after DROP TABLE", "expected no row")
    );
    // Catalog resolution across the whole table namespace is a documented
    // support boundary: DataFusion planning failures are reported as `XX000`
    // (see `tests/catalog_contract/isolation.rs`), so a dropped table is
    // proven gone by the message naming it, not by a PostgreSQL `42P01`.
    let (code, message) = error_detail(h, "SELECT body FROM ddl_notes").await?;
    ensure!(
        code == "XX000" && message.contains("ddl_notes"),
        "{}",
        detail(
            "ddl_notes",
            "SELECT after DROP TABLE",
            &format!("expected XX000 naming the table, actual {code} ({message})")
        )
    );
    let error = error_code(h, "DROP TABLE ddl_notes").await?;
    ensure!(
        error == "42P01",
        "{}",
        detail(
            "ddl_notes",
            "DROP TABLE of an absent table",
            &format!("expected 42P01, actual {error}")
        )
    );
    Ok(())
}

/// `DROP TABLE IF EXISTS` is idempotent and still reclaims a live table.
async fn drop_absent_and_if_exists(h: &Harness) -> Result<()> {
    command(h, "DROP TABLE IF EXISTS ddl_absent").await?;
    command(h, "CREATE TABLE ddl_absent (id INT)").await?;
    command(h, "DROP TABLE IF EXISTS ddl_absent, ddl_absent").await?;
    ensure!(
        metadata(h, "ddl_absent").await?.is_none(),
        "{}",
        detail(
            "ddl_absent",
            "after DROP TABLE IF EXISTS with two names",
            "table_info row must be gone"
        )
    );
    Ok(())
}

/// `WITH (hashbucketnum, file_format)` reaches the table properties.
async fn options_and_column_types(h: &Harness) -> Result<()> {
    command(
        h,
        "CREATE TABLE ddl_buckets (id BIGINT PRIMARY KEY, tag TEXT) \
         WITH (hashbucketnum = 8, file_format = 'parquet')",
    )
    .await?;
    let row = metadata(h, "ddl_buckets")
        .await?
        .with_context(|| context("ddl_buckets", "after CREATE TABLE", "metadata row"))?;
    ensure!(
        row.hash_bucket_num.as_deref() == Some("8"),
        "{}",
        detail(
            "ddl_buckets",
            "hash bucket count",
            &format!("expected 8, actual {:?}", row.hash_bucket_num)
        )
    );
    ensure!(
        row.file_format.as_deref() == Some("parquet"),
        "{}",
        detail(
            "ddl_buckets",
            "file format",
            &format!("expected parquet, actual {:?}", row.file_format)
        )
    );
    command(h, "DROP TABLE ddl_buckets").await?;
    Ok(())
}

/// Statements outside the supported subset fail with `0A000` and create
/// nothing; names that address a missing schema fail with `3F000`.
async fn refusal_matrix_creates_nothing(h: &Harness) -> Result<()> {
    let cases: &[(&str, &str, &str)] = &[
        (
            "ddl_ctas",
            "CREATE TABLE ddl_ctas AS SELECT id FROM items",
            "0A000",
        ),
        (
            "ddl_temp",
            "CREATE TEMPORARY TABLE ddl_temp (id INT)",
            "0A000",
        ),
        (
            "ddl_replace",
            "CREATE OR REPLACE TABLE ddl_replace (id INT)",
            "0A000",
        ),
        (
            "ddl_numeric",
            "CREATE TABLE ddl_numeric (amount NUMERIC)",
            "0A000",
        ),
        ("ddl_uuid", "CREATE TABLE ddl_uuid (id UUID)", "0A000"),
        ("ddl_array", "CREATE TABLE ddl_array (tags INT[])", "0A000"),
        (
            "ddl_default",
            "CREATE TABLE ddl_default (id INT DEFAULT 1)",
            "0A000",
        ),
        (
            "ddl_unique",
            "CREATE TABLE ddl_unique (id INT UNIQUE)",
            "0A000",
        ),
        (
            "ddl_check",
            "CREATE TABLE ddl_check (id INT, CHECK (id > 0))",
            "0A000",
        ),
        (
            "ddl_fk",
            "CREATE TABLE ddl_fk (id INT REFERENCES ddl_orders(id))",
            "0A000",
        ),
        (
            "ddl_option",
            "CREATE TABLE ddl_option (id INT) WITH (compression = 'zstd')",
            "0A000",
        ),
        (
            "ddl_buckets_zero",
            "CREATE TABLE ddl_buckets_zero (id INT) WITH (hashbucketnum = 0)",
            "22023",
        ),
        (
            "ddl_three_part",
            "CREATE TABLE other.public.ddl_three_part (id INT)",
            "0A000",
        ),
        (
            "ddl_missing_schema",
            "CREATE TABLE nosuchschema.ddl_missing_schema (id INT)",
            "3F000",
        ),
        ("ddl_create_schema", "CREATE SCHEMA ddl_schema", "0A000"),
        (
            "ddl_alter",
            "ALTER TABLE ddl_orders ADD COLUMN extra INT",
            "0A000",
        ),
        ("ddl_drop_view", "DROP VIEW ddl_view", "0A000"),
        (
            "ddl_drop_missing_schema",
            "DROP TABLE nosuchschema.ddl_drop_missing",
            "3F000",
        ),
    ];
    for (table, sql, expected) in cases {
        let actual = error_code(h, sql).await.with_context(|| {
            format!("case={table} SQL={sql} expected SQLSTATE={expected}")
        })?;
        ensure!(
            actual == *expected,
            "case={table} SQL={sql} expected SQLSTATE={expected} actual={actual}"
        );
        // The connection stays usable and nothing was published.
        let count = scalar(&h.lake, "SELECT count(*) FROM ddl_orders").await?;
        ensure!(
            count == 0,
            "case={table} SQL={sql}: the fixture table must stay readable"
        );
        ensure!(
            metadata(h, table).await?.is_none(),
            "case={table} SQL={sql}: a refused statement must not publish metadata"
        );
        ensure!(
            pg_class_count(h, table).await? == 0,
            "case={table} SQL={sql}: a refused statement must not appear in pg_class"
        );
    }
    Ok(())
}

/// The extended protocol reaches the same code path.
async fn extended_protocol_creates_and_drops(h: &Harness) -> Result<()> {
    let create = h
        .lake
        .prepare("CREATE TABLE ddl_extended (id BIGINT PRIMARY KEY, note TEXT)")
        .await
        .context("extended Parse of CREATE TABLE")?;
    h.lake
        .execute(&create, &[])
        .await
        .context("extended Execute of CREATE TABLE")?;
    ensure!(
        metadata(h, "ddl_extended").await?.is_some(),
        "extended CREATE TABLE must publish metadata"
    );
    let drop = h
        .lake
        .prepare("DROP TABLE ddl_extended")
        .await
        .context("extended Parse of DROP TABLE")?;
    h.lake
        .execute(&drop, &[])
        .await
        .context("extended Execute of DROP TABLE")?;
    ensure!(
        metadata(h, "ddl_extended").await?.is_none(),
        "extended DROP TABLE must remove metadata"
    );
    Ok(())
}

/// LakeSoul commits table metadata immediately, so DDL inside a transaction
/// block is refused instead of silently ignoring `ROLLBACK`.
async fn ddl_is_refused_inside_a_transaction(h: &Harness) -> Result<()> {
    h.lake.simple_query("BEGIN").await?;
    let code = error_code(h, "CREATE TABLE ddl_tx (id INT)").await?;
    ensure!(
        code == "0A000",
        "CREATE TABLE inside BEGIN: expected 0A000, actual {code}"
    );
    h.lake.simple_query("ROLLBACK").await?;
    ensure!(
        metadata(h, "ddl_tx").await?.is_none(),
        "CREATE TABLE inside BEGIN must not publish metadata"
    );
    let (code, message) = error_detail(h, "SELECT id FROM ddl_tx").await?;
    ensure!(
        code == "XX000" && message.contains("ddl_tx"),
        "SELECT after refused transactional CREATE: expected XX000 naming the \
         table (see tests/catalog_contract/isolation.rs), actual {code} ({message})"
    );
    Ok(())
}

/// Run one DDL statement and require success.
async fn command(h: &Harness, sql: &str) -> Result<()> {
    h.lake
        .simple_query(sql)
        .await
        .with_context(|| format!("SQL={sql} expected success"))?;
    Ok(())
}

/// SQLSTATE of a statement that must fail with a server-side error.
async fn error_code(h: &Harness, sql: &str) -> Result<String> {
    Ok(error_detail(h, sql).await?.0)
}

/// SQLSTATE and message of a statement that must fail server-side.
async fn error_detail(h: &Harness, sql: &str) -> Result<(String, String)> {
    let error = match h.lake.simple_query(sql).await {
        Ok(_) => bail!("SQL={sql}: expected rejection, actual success"),
        Err(error) => error,
    };
    let db = error.as_db_error().with_context(|| {
        format!("SQL={sql}: expected a server ErrorResponse, actual client-side rejection: {error}")
    })?;
    Ok((db.code().code().to_owned(), db.message().to_owned()))
}

/// The single `table_info` row of `table`, if it exists.
async fn metadata(h: &Harness, table: &str) -> Result<Option<TableRow>> {
    let rows = h
        .reference
        .query(
            "SELECT table_id, table_path, partitions, \
             properties->>'hashBucketNum', properties->>'file_format' \
             FROM public.table_info WHERE table_namespace = $1 AND table_name = $2",
            &[&h.lake_namespace, &table],
        )
        .await
        .with_context(|| format!("metadata read for {table}"))?;
    Ok(rows.first().map(|row| TableRow {
        table_id: row.get(0),
        table_path: row.get(1),
        partitions: row.get(2),
        hash_bucket_num: row.get(3),
        file_format: row.get(4),
    }))
}

/// `[table_info, table_name_id, table_path_id, partition_info, data_commit_info]`
/// counts for `table`, so a drop is proven to cascade.
async fn metadata_counts(h: &Harness, table: &str) -> Result<[i64; 5]> {
    let row = h
        .reference
        .query_one(
            "SELECT \
             (SELECT count(*) FROM public.table_info WHERE table_namespace = $1 AND table_name = $2), \
             (SELECT count(*) FROM public.table_name_id WHERE table_namespace = $1 AND table_name = $2), \
             (SELECT count(*) FROM public.table_path_id WHERE table_namespace = $1 AND table_path LIKE '%/' || $2), \
             (SELECT count(*) FROM public.partition_info WHERE table_id IN \
                (SELECT table_id FROM public.table_info WHERE table_namespace = $1 AND table_name = $2)), \
             (SELECT count(*) FROM public.data_commit_info WHERE table_id IN \
                (SELECT table_id FROM public.table_info WHERE table_namespace = $1 AND table_name = $2))",
            &[&h.lake_namespace, &table],
        )
        .await
        .with_context(|| format!("metadata counts for {table}"))?;
    Ok([row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)])
}

async fn pg_class_count(h: &Harness, table: &str) -> Result<i64> {
    let sql = format!("SELECT count(*) FROM pg_class WHERE relname = '{table}'");
    scalar(&h.lake, &sql).await
}

/// First column of the single row `sql` returns, as an `i64`.
async fn scalar(client: &Client, sql: &str) -> Result<i64> {
    let row = client
        .query_one(sql, &[])
        .await
        .with_context(|| format!("SQL={sql}"))?;
    row.try_get(0).with_context(|| format!("SQL={sql}"))
}

fn context(table: &str, phase: &str, what: &str) -> String {
    format!("table={table} phase={phase} expected {what}")
}

fn detail(table: &str, what: &str, actual: &str) -> String {
    format!("table={table} {what}: {actual}")
}
