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
    append_only: Option<String>,
    use_cdc: Option<String>,
    cdc_change_column: Option<String>,
    vortex_row_block_size: Option<String>,
    vortex_data_block_target_bytes: Option<String>,
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
            cdc_options_reach_properties(h).await?;
            refusal_matrix_creates_nothing(h).await?;
            extended_protocol_creates_and_drops(h).await?;
            partitioned_by_creates_range_partitions(h).await?;
            partition_ddl_rejections_leave_no_trace(h).await?;
            identifiers_are_folded_and_contained(h).await?;
            drop_table_is_all_or_nothing(h).await?;
            simple_query_batch_with_ddl_is_refused(h).await?;
            if_not_exists_keeps_the_existing_schema(h).await?;
            ddl_is_refused_inside_a_transaction(h).await?;
            same_connection_ddl_replans_prepared_statements(h).await?;
            cross_connection_drop_keeps_the_bound_plan(h).await?;
            cross_connection_drop_and_recreate_keeps_the_bound_plan(h).await?;
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
        row.file_format.as_deref() == Some("vortex-compact"),
        "{}",
        detail(
            table,
            "file format",
            &format!("expected vortex-compact, actual {:?}", row.file_format)
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
    // Capture the identity before the drop: counting through `table_info`
    // afterwards would return zero even if the dependent rows survived.
    let table_id = metadata(h, "ddl_notes")
        .await?
        .with_context(|| context("ddl_notes", "before DROP", "metadata row"))?
        .table_id;
    let before = counts_for_table_id(h, &table_id).await?;
    ensure!(
        before == [1, 1, 1, 0, 0],
        "{}",
        detail(
            "ddl_notes",
            "rows by table_id before DROP",
            &format!(
                "expected [table_info, table_name_id, table_path_id, partition_info, data_commit_info] = [1, 1, 1, 0, 0], actual {before:?}"
            )
        )
    );
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
    let after = counts_for_table_id(h, &table_id).await?;
    ensure!(
        after == [0, 0, 0, 0, 0],
        "{}",
        detail(
            "ddl_notes",
            "rows by the pre-drop table_id after DROP TABLE",
            &format!("expected every count to be 0, actual {after:?}")
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

/// `WITH (hash_bucket_num, file_format, append_only)` reaches the table
/// properties.
async fn options_and_column_types(h: &Harness) -> Result<()> {
    command(
        h,
        "CREATE TABLE ddl_buckets (id BIGINT PRIMARY KEY, tag TEXT) \
         WITH (hash_bucket_num = 8, file_format = 'parquet', append_only = true, \
               vortex_row_block_size = 4096, \
               vortex_data_block_target_bytes = 262144)",
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
    ensure!(
        row.append_only.as_deref() == Some("true"),
        "{}",
        detail(
            "ddl_buckets",
            "append only",
            &format!("expected true, actual {:?}", row.append_only)
        )
    );
    ensure!(
        row.vortex_row_block_size.as_deref() == Some("4096"),
        "{}",
        detail(
            "ddl_buckets",
            "vector row block size",
            &format!("expected 4096, actual {:?}", row.vortex_row_block_size)
        )
    );
    ensure!(
        row.vortex_data_block_target_bytes.as_deref() == Some("262144"),
        "{}",
        detail(
            "ddl_buckets",
            "vector data block target",
            &format!(
                "expected 262144, actual {:?}",
                row.vortex_data_block_target_bytes
            )
        )
    );
    command(h, "DROP TABLE ddl_buckets").await?;

    // The quoted form normalises case the same way.
    command(
        h,
        "CREATE TABLE ddl_append_false (id BIGINT PRIMARY KEY) \
         WITH (append_only = 'FALSE')",
    )
    .await?;
    let row = metadata(h, "ddl_append_false").await?.with_context(|| {
        context("ddl_append_false", "after CREATE TABLE", "metadata row")
    })?;
    ensure!(
        row.append_only.as_deref() == Some("false"),
        "{}",
        detail(
            "ddl_append_false",
            "append only",
            &format!("expected false, actual {:?}", row.append_only)
        )
    );
    command(h, "DROP TABLE ddl_append_false").await?;
    Ok(())
}

/// `WITH (use_cdc, lakesoul_cdc_change_column)` reaches the table properties;
/// `use_cdc = true` defaults the column to `rowKinds` when it is declared.
async fn cdc_options_reach_properties(h: &Harness) -> Result<()> {
    command(
        h,
        "CREATE TABLE ddl_cdc (id BIGINT PRIMARY KEY, op TEXT) \
         WITH (use_cdc = true, lakesoul_cdc_change_column = 'op')",
    )
    .await?;
    let row = metadata(h, "ddl_cdc")
        .await?
        .with_context(|| context("ddl_cdc", "after CREATE TABLE", "metadata row"))?;
    ensure!(
        row.use_cdc.as_deref() == Some("true"),
        "{}",
        detail(
            "ddl_cdc",
            "use_cdc",
            &format!("expected true, actual {:?}", row.use_cdc)
        )
    );
    ensure!(
        row.cdc_change_column.as_deref() == Some("op"),
        "{}",
        detail(
            "ddl_cdc",
            "change column",
            &format!("expected op, actual {:?}", row.cdc_change_column)
        )
    );
    command(h, "DROP TABLE ddl_cdc").await?;

    // `use_cdc = true` alone falls back to the declared `rowKinds`, folded to
    // this front end's lower-case column names.
    command(
        h,
        "CREATE TABLE ddl_cdc_default (id BIGINT PRIMARY KEY, rowKinds TEXT) \
         WITH (use_cdc = true)",
    )
    .await?;
    let row = metadata(h, "ddl_cdc_default").await?.with_context(|| {
        context("ddl_cdc_default", "after CREATE TABLE", "metadata row")
    })?;
    ensure!(
        row.cdc_change_column.as_deref() == Some("rowkinds"),
        "{}",
        detail(
            "ddl_cdc_default",
            "default change column",
            &format!("expected rowkinds, actual {:?}", row.cdc_change_column)
        )
    );
    command(h, "DROP TABLE ddl_cdc_default").await?;
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
            "ddl_old_bucket_option",
            "CREATE TABLE ddl_old_bucket_option (id BIGINT PRIMARY KEY) \
             WITH (hashbucketnum = 8)",
            "0A000",
        ),
        (
            "ddl_buckets_zero",
            "CREATE TABLE ddl_buckets_zero (id INT) WITH (hash_bucket_num = 0)",
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
        (
            "ddl_dup_column",
            "CREATE TABLE ddl_dup_column (id INT, id INT)",
            "42701",
        ),
        (
            "ddl_dup_key",
            "CREATE TABLE ddl_dup_key (id INT, PRIMARY KEY (id, id))",
            "42P16",
        ),
        (
            "ddl_missing_key",
            "CREATE TABLE ddl_missing_key (id INT, PRIMARY KEY (nope))",
            "42703",
        ),
        (
            "ddl_null_conflict",
            "CREATE TABLE ddl_null_conflict (id INT NOT NULL NULL)",
            "42601",
        ),
        (
            "ddl_null_key_conflict",
            "CREATE TABLE ddl_null_key_conflict (id INT NULL PRIMARY KEY)",
            "42601",
        ),
        (
            "ddl_deferrable",
            "CREATE TABLE ddl_deferrable (id INT PRIMARY KEY DEFERRABLE \
             INITIALLY DEFERRED)",
            "0A000",
        ),
        (
            "ddl_inherits",
            "CREATE TABLE ddl_inherits (extra INT) INHERITS (ddl_orders)",
            "0A000",
        ),
        (
            "ddl_partition_by",
            "CREATE TABLE ddl_partition_by (id INT) PARTITION BY RANGE (id)",
            "0A000",
        ),
        (
            "ddl_negative_buckets",
            "CREATE TABLE ddl_negative_buckets (id INT) \
             WITH (hash_bucket_num = -1)",
            "22023",
        ),
        (
            "ddl_append_only_invalid",
            "CREATE TABLE ddl_append_only_invalid (id INT) \
             WITH (append_only = 'yes')",
            "22023",
        ),
        (
            "ddl_cdc_bad_bool",
            "CREATE TABLE ddl_cdc_bad_bool (id INT) WITH (use_cdc = 'maybe')",
            "22023",
        ),
        (
            "ddl_cdc_unknown_column",
            "CREATE TABLE ddl_cdc_unknown_column (id INT) \
             WITH (lakesoul_cdc_change_column = 'nope')",
            "42703",
        ),
        (
            "ddl_cdc_missing_column",
            "CREATE TABLE ddl_cdc_missing_column (id INT) WITH (use_cdc = true)",
            "22023",
        ),
        (
            "ddl_float_zero",
            "CREATE TABLE ddl_float_zero (value FLOAT(0))",
            "0A000",
        ),
        (
            "ddl_timestamp_zero",
            "CREATE TABLE ddl_timestamp_zero (at TIMESTAMP(0))",
            "0A000",
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
        .prepare(
            "CREATE TABLE ddl_extended (id BIGINT PRIMARY KEY, note TEXT) \
             WITH (hash_bucket_num = 16, append_only = 'true', \
             use_cdc = true, lakesoul_cdc_change_column = 'note')",
        )
        .await
        .context("extended Parse of CREATE TABLE")?;
    h.lake
        .execute(&create, &[])
        .await
        .context("extended Execute of CREATE TABLE")?;
    let row = metadata(h, "ddl_extended")
        .await?
        .context("extended CREATE TABLE must publish metadata")?;
    ensure!(
        row.hash_bucket_num.as_deref() == Some("16"),
        "extended CREATE TABLE must preserve the requested bucket count, actual {:?}",
        row.hash_bucket_num
    );
    ensure!(
        row.append_only.as_deref() == Some("true"),
        "extended CREATE TABLE must preserve append_only, actual {:?}",
        row.append_only
    );
    ensure!(
        row.use_cdc.as_deref() == Some("true"),
        "extended CREATE TABLE must preserve use_cdc, actual {:?}",
        row.use_cdc
    );
    ensure!(
        row.cdc_change_column.as_deref() == Some("note"),
        "extended CREATE TABLE must preserve the change column, actual {:?}",
        row.cdc_change_column
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

    // The clauses analyzable from the AST are refused at Parse...
    for sql in [
        "CREATE TABLE ddl_ext_ctas AS SELECT id FROM items",
        "CREATE TABLE ddl_ext_bad (id UUID)",
        "CREATE TABLE ddl_ext_old_bucket_option (id BIGINT PRIMARY KEY) \
         WITH (hashbucketnum = 8)",
    ] {
        let error = h
            .lake
            .prepare(sql)
            .await
            .err()
            .with_context(|| format!("SQL={sql}: expected a Parse-time rejection"))?;
        let db = error
            .as_db_error()
            .with_context(|| format!("SQL={sql}: expected a server ErrorResponse"))?;
        ensure!(
            db.code().code() == "0A000",
            "SQL={sql}: expected 0A000, actual {}",
            db.code().code()
        );
    }
    // ...while a utility statement is analyzed at Execute, like PostgreSQL,
    // so the schema is reported there.
    let drop = h
        .lake
        .prepare("DROP TABLE nosuchschema.ddl_ext_missing")
        .await
        .context("extended Parse of DROP TABLE with an unknown schema")?;
    let error = h
        .lake
        .execute(&drop, &[])
        .await
        .err()
        .context("extended Execute of DROP TABLE with an unknown schema")?;
    let db = error
        .as_db_error()
        .context("extended Execute: expected a server ErrorResponse")?;
    ensure!(
        db.code().code() == "3F000",
        "extended DROP TABLE with an unknown schema: expected 3F000, actual {}",
        db.code().code()
    );
    Ok(())
}

/// Partition order is independent of schema order and hash bucketing;
/// references follow the same identifier rules in both wire protocols.
async fn partitioned_by_creates_range_partitions(h: &Harness) -> Result<()> {
    for (table, sql, partitions, buckets, extended) in [
        (
            "ddl_part_pk",
            "CREATE TABLE ddl_part_pk (Part INTEGER NOT NULL, id BIGINT \
             PRIMARY KEY, val TEXT) PARTITIONED BY (\"part\") \
             WITH (hash_bucket_num = 4)",
            "part;id",
            Some("4"),
            false,
        ),
        (
            "ddl_part_multi",
            "CREATE TABLE ddl_part_multi (part INTEGER NOT NULL, \
             region TEXT NOT NULL, id BIGINT PRIMARY KEY, val TEXT) \
             PARTITIONED BY (\"region\", PART) WITH (hash_bucket_num = 8)",
            "region,part;id",
            Some("8"),
            false,
        ),
        (
            "ddl_part_nopk",
            "CREATE TABLE ddl_part_nopk (part INTEGER NOT NULL, body TEXT) \
             PARTITIONED BY (part)",
            "part;",
            None,
            false,
        ),
        (
            "ddl_part_extended",
            "CREATE TABLE ddl_part_extended (part INTEGER NOT NULL, \
             id BIGINT PRIMARY KEY, val TEXT) PARTITIONED BY (part) \
             WITH (hash_bucket_num = 16)",
            "part;id",
            Some("16"),
            true,
        ),
        (
            "ddl_part_cdc",
            "CREATE TABLE ddl_part_cdc (part TEXT NOT NULL, \
             id BIGINT NOT NULL, \"rowKinds\" TEXT NOT NULL, \
             PRIMARY KEY (id)) PARTITIONED BY (part) \
             WITH (use_cdc = true, \
             lakesoul_cdc_change_column = 'rowKinds', hash_bucket_num = 2)",
            "part;id",
            Some("2"),
            false,
        ),
    ] {
        if extended {
            let create = h.lake.prepare(sql).await.context("partition DDL Parse")?;
            ensure!(
                metadata(h, table).await?.is_none(),
                "{table}: Parse must not publish a table"
            );
            h.lake
                .execute(&create, &[])
                .await
                .context("partition DDL Execute")?;
        } else {
            command(h, sql).await?;
        }
        let row = metadata(h, table)
            .await?
            .with_context(|| format!("{table}: partition DDL must publish metadata"))?;
        ensure!(
            row.partitions == partitions,
            "{table}: expected partitions {partitions:?}, actual {:?}",
            row.partitions
        );
        ensure!(
            row.hash_bucket_num.as_deref() == buckets,
            "{table}: expected partition-local buckets {buckets:?}, actual {:?}",
            row.hash_bucket_num
        );
        if table == "ddl_part_cdc" {
            ensure!(
                row.cdc_change_column.as_deref() == Some("rowKinds"),
                "{table}: expected the exact CDC column rowKinds, actual {:?}",
                row.cdc_change_column
            );
        }
        ensure!(
            metadata_counts(h, table).await? == [1, 1, 1, 0, 0],
            "{table}: CREATE must publish one table, not precreate child partitions"
        );
        let drop_sql = format!("DROP TABLE {table}");
        if extended {
            let drop = h.lake.prepare(&drop_sql).await?;
            h.lake.execute(&drop, &[]).await?;
        } else {
            command(h, &drop_sql).await?;
        }
        ensure!(
            metadata(h, table).await?.is_none(),
            "{table}: DROP must remove the partitioned table"
        );
    }
    Ok(())
}

/// Partition references resolve against the declared columns, and the
/// PostgreSQL declarative-partition syntax stays refused: both the simple and
/// the extended Parse paths reject before anything is published, leaving the
/// authoritative metadata and warehouse files byte-for-byte unchanged.
async fn partition_ddl_rejections_leave_no_trace(h: &Harness) -> Result<()> {
    let before = h
        .authoritative_snapshot()
        .await
        .context("partition rejection matrix: snapshot before")?;
    // Malformed column lists are refused by the upstream parser; its generic
    // error SQLSTATE is outside the DDL hook's semantic error contract.
    let cases: &[(&str, &str, Option<&str>)] = &[
        (
            "ddl_part_unknown",
            "CREATE TABLE ddl_part_unknown (part INTEGER NOT NULL, \
             id BIGINT NOT NULL, PRIMARY KEY (id)) PARTITIONED BY (nope)",
            Some("42703"),
        ),
        (
            "ddl_part_duplicate",
            "CREATE TABLE ddl_part_duplicate (part INTEGER NOT NULL) \
             PARTITIONED BY (part, PART)",
            Some("42701"),
        ),
        (
            "ddl_part_expression",
            "CREATE TABLE ddl_part_expression (part INTEGER NOT NULL) \
             PARTITIONED BY (part + 1)",
            None,
        ),
        (
            "ddl_part_empty",
            "CREATE TABLE ddl_part_empty (part INTEGER) PARTITIONED BY ()",
            None,
        ),
        (
            "ddl_part_typed",
            "CREATE TABLE ddl_part_typed (part INTEGER NOT NULL) \
             PARTITIONED BY (part INTEGER)",
            Some("0A000"),
        ),
        (
            "ddl_part_quoted_upper",
            "CREATE TABLE ddl_part_quoted_upper (part INTEGER NOT NULL) \
             PARTITIONED BY (\"Part\")",
            Some("0A000"),
        ),
        (
            "ddl_pg_range",
            "CREATE TABLE ddl_pg_range (id INT) PARTITION BY RANGE (id)",
            Some("0A000"),
        ),
        (
            "ddl_pg_list",
            "CREATE TABLE ddl_pg_list (id INT) PARTITION BY LIST (id)",
            Some("0A000"),
        ),
        (
            "ddl_pg_hash",
            "CREATE TABLE ddl_pg_hash (id INT) PARTITION BY HASH (id)",
            Some("0A000"),
        ),
        (
            "ddl_pg_child",
            "CREATE TABLE ddl_pg_child PARTITION OF ddl_pg_parent \
             FOR VALUES FROM (1) TO (10)",
            Some("0A000"),
        ),
    ];
    for (table, sql, expected) in cases {
        let (code, message) = error_detail(h, sql)
            .await
            .with_context(|| format!("case={table} simple SQL={sql}"))?;
        if let Some(expected) = expected {
            ensure!(
                code == *expected,
                "case={table} simple SQL={sql}: expected {expected}, actual \
                 {code} ({message})"
            );
        }
        // The extended Parse must refuse the statement before Execute, not
        // bind a plan that could publish on a later message.
        let error = h.lake.prepare(sql).await.err().with_context(|| {
            format!("case={table} extended Parse SQL={sql}: expected rejection")
        })?;
        let db = error.as_db_error().with_context(|| {
            format!("case={table}: extended Parse must return a server error")
        })?;
        if let Some(expected) = expected {
            ensure!(
                db.code().code() == *expected,
                "case={table}: expected {expected}, actual {}",
                db.code().code()
            );
        }
        ensure!(
            metadata(h, table).await?.is_none(),
            "case={table} SQL={sql}: a refused statement must not publish metadata"
        );
        ensure!(
            pg_class_count(h, table).await? == 0,
            "case={table} SQL={sql}: a refused statement must not appear in pg_class"
        );
        // A rejected extended Parse leaves the session usable.
        let count = scalar(&h.lake, "SELECT count(*) FROM ddl_orders").await?;
        ensure!(
            count == 0,
            "case={table} SQL={sql}: the fixture table must stay readable"
        );
    }
    let after = h
        .authoritative_snapshot()
        .await
        .context("partition rejection matrix: snapshot after")?;
    ensure!(
        before == after,
        "the partition rejection matrix must not change authoritative \
         metadata, rows, or warehouse file bytes"
    );
    Ok(())
}

/// Unquoted names fold to lower case, and a name that would change the data
/// path or the partition description is refused before anything is created.
async fn identifiers_are_folded_and_contained(h: &Harness) -> Result<()> {
    let before = namespace_table_count(h).await?;
    for (label, sql) in [
        ("path traversal", "CREATE TABLE \"../../outside\" (id INT)"),
        ("absolute path", "CREATE TABLE \"/tmp/outside\" (id INT)"),
        ("url metacharacter", "CREATE TABLE \"escaped?x=1\" (id INT)"),
        ("quoted upper case", "CREATE TABLE \"Orders\" (id INT)"),
        (
            "separator in a column",
            "CREATE TABLE ddl_name_check (\"a,b\" INT)",
        ),
    ] {
        let code = error_code(h, sql)
            .await
            .with_context(|| format!("case={label} SQL={sql}"))?;
        ensure!(
            code == "0A000",
            "case={label} SQL={sql}: expected 0A000, actual {code}"
        );
        ensure!(
            namespace_table_count(h).await? == before,
            "case={label} SQL={sql}: a refused name must not publish a table"
        );
    }

    // PostgreSQL folds the unquoted names, and so does the metadata: one
    // lower-case identity for the table, its path, its columns and its key.
    command(h, "CREATE TABLE Orders (ID BIGINT PRIMARY KEY, Label TEXT)").await?;
    let row = metadata(h, "orders").await?.with_context(|| {
        context("orders", "after folded CREATE TABLE", "metadata row")
    })?;
    ensure!(
        row.partitions == ";id",
        "{}",
        detail(
            "orders",
            "folded primary key",
            &format!("expected \";id\", actual {:?}", row.partitions)
        )
    );
    ensure!(
        row.table_path
            .ends_with(&format!("/{}/orders", h.lake_namespace)),
        "{}",
        detail(
            "orders",
            "folded table path",
            &format!("actual {:?}", row.table_path)
        )
    );
    ensure!(
        pg_class_count(h, "orders").await? == 1,
        "{}",
        detail("orders", "pg_class", "expected the folded name")
    );
    let statement = h
        .lake
        .prepare("SELECT id, label FROM orders")
        .await
        .context("SELECT of the folded names")?;
    let columns: Vec<&str> = statement
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    ensure!(
        columns == ["id", "label"],
        "{}",
        detail(
            "orders",
            "folded column names",
            &format!("actual {columns:?}")
        )
    );
    command(h, "DROP TABLE orders").await?;
    Ok(())
}

/// A multi-name `DROP TABLE` that fails leaves every named table in place.
async fn drop_table_is_all_or_nothing(h: &Harness) -> Result<()> {
    command(h, "CREATE TABLE ddl_multi (id INT)").await?;
    let table_id = metadata(h, "ddl_multi")
        .await?
        .with_context(|| context("ddl_multi", "after CREATE", "metadata row"))?
        .table_id;

    let code = error_code(h, "DROP TABLE ddl_multi, ddl_missing")
        .await
        .context("DROP TABLE with a missing second name")?;
    ensure!(
        code == "42P01",
        "{}",
        detail(
            "ddl_multi",
            "DROP TABLE with a missing second name",
            &format!("expected 42P01, actual {code}")
        )
    );
    ensure!(
        metadata(h, "ddl_multi").await?.is_some(),
        "{}",
        detail(
            "ddl_multi",
            "after a failed multi-name DROP",
            "the first table must still exist"
        )
    );
    ensure!(
        counts_for_table_id(h, &table_id).await? == [1, 1, 1, 0, 0],
        "{}",
        detail(
            "ddl_multi",
            "rows after a failed multi-name DROP",
            "expected [table_info, table_name_id, table_path_id, partition_info, data_commit_info] = [1, 1, 1, 0, 0]"
        )
    );

    let code = error_code(h, "DROP TABLE ddl_multi, nosuchschema.other")
        .await
        .context("DROP TABLE with a second name in an unknown schema")?;
    ensure!(
        code == "3F000",
        "{}",
        detail(
            "ddl_multi",
            "DROP TABLE with an unknown second schema",
            &format!("expected 3F000, actual {code}")
        )
    );
    ensure!(
        metadata(h, "ddl_multi").await?.is_some(),
        "{}",
        detail(
            "ddl_multi",
            "after a failed multi-name DROP",
            "the first table must still exist"
        )
    );

    command(h, "DROP TABLE ddl_multi").await?;
    ensure!(
        metadata(h, "ddl_multi").await?.is_none(),
        "{}",
        detail(
            "ddl_multi",
            "after DROP TABLE",
            "table_info row must be gone"
        )
    );
    Ok(())
}

/// PostgreSQL runs a multi-statement simple query in an implicit transaction,
/// so DDL cannot be mixed with other statements here: the metadata commit
/// cannot be rolled back.
async fn simple_query_batch_with_ddl_is_refused(h: &Harness) -> Result<()> {
    for sql in [
        "CREATE TABLE ddl_batch (id INT); SELECT 1 / 0",
        "SELECT 1; CREATE TABLE ddl_batch (id INT)",
        "CREATE TABLE ddl_batch (id INT); DROP TABLE ddl_batch",
        "CREATE TABLE ddl_batch (part INT NOT NULL) PARTITIONED BY (part); \
         SELECT 1",
        "SELECT 1; CREATE TABLE ddl_batch (part INT NOT NULL) \
         PARTITIONED BY (part)",
    ] {
        let (code, message) = error_detail(h, sql)
            .await
            .with_context(|| format!("SQL={sql}"))?;
        ensure!(
            code == "0A000" && message.contains("implicit transaction"),
            "SQL={sql}: expected 0A000 naming the implicit transaction, \
             actual {code} ({message})"
        );
        ensure!(
            metadata(h, "ddl_batch").await?.is_none(),
            "SQL={sql}: a refused batch must not publish a table"
        );
    }
    // The same statements work one message at a time.
    command(h, "CREATE TABLE ddl_batch (id INT)").await?;
    command(h, "DROP TABLE ddl_batch").await?;
    Ok(())
}

/// `IF NOT EXISTS` keeps the existing identity and schema, as PostgreSQL does.
async fn if_not_exists_keeps_the_existing_schema(h: &Harness) -> Result<()> {
    command(h, "CREATE TABLE ddl_keep (id INT)").await?;
    let before = metadata(h, "ddl_keep")
        .await?
        .with_context(|| context("ddl_keep", "after CREATE", "metadata row"))?;
    command(h, "CREATE TABLE IF NOT EXISTS ddl_keep (other TEXT)").await?;
    let after = metadata(h, "ddl_keep")
        .await?
        .with_context(|| context("ddl_keep", "after IF NOT EXISTS", "metadata row"))?;
    ensure!(
        before.table_id == after.table_id,
        "{}",
        detail(
            "ddl_keep",
            "IF NOT EXISTS identity",
            &format!("expected {}, actual {}", before.table_id, after.table_id)
        )
    );
    let statement = h
        .lake
        .prepare("SELECT * FROM ddl_keep")
        .await
        .context("SELECT * of the kept table")?;
    let columns: Vec<&str> = statement
        .columns()
        .iter()
        .map(|column| column.name())
        .collect();
    ensure!(
        columns == ["id"],
        "{}",
        detail(
            "ddl_keep",
            "columns after IF NOT EXISTS with a different declaration",
            &format!("expected [\"id\"], actual {columns:?}")
        )
    );
    command(h, "DROP TABLE ddl_keep").await?;
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

    // A partition declaration does not change the commit boundary.
    h.lake.simple_query("BEGIN").await?;
    let code = error_code(
        h,
        "CREATE TABLE ddl_tx_part (part INTEGER NOT NULL) PARTITIONED BY (part)",
    )
    .await?;
    ensure!(
        code == "0A000",
        "partitioned CREATE TABLE inside BEGIN: expected 0A000, actual {code}"
    );
    h.lake.simple_query("ROLLBACK").await?;
    ensure!(
        metadata(h, "ddl_tx_part").await?.is_none(),
        "partitioned CREATE TABLE inside BEGIN must not publish metadata"
    );
    Ok(())
}

/// A successful `CREATE`/`DROP` on this connection advances its settings
/// generation, so a statement parsed earlier is replanned at `Execute`: after
/// `DROP` it reports `42P01`, and after `DROP` + same-name `CREATE` it binds
/// the new table. A refused DDL statement must not invalidate anything.
async fn same_connection_ddl_replans_prepared_statements(h: &Harness) -> Result<()> {
    const CONTRACT: &str = "same-connection-ddl-plan-invalidation";
    command(
        h,
        "CREATE TABLE ddl_replan (id BIGINT NOT NULL PRIMARY KEY, label TEXT)",
    )
    .await?;
    h.lake
        .simple_query("INSERT INTO ddl_replan VALUES (1, 'old')")
        .await
        .with_context(|| format!("case={CONTRACT} seed INSERT"))?;
    let statement = h
        .lake
        .prepare("SELECT label FROM ddl_replan WHERE id = 1")
        .await
        .with_context(|| format!("case={CONTRACT} prepare"))?;
    let before: Option<String> = h
        .lake
        .query_one(&statement, &[])
        .await
        .with_context(|| format!("case={CONTRACT} first Execute"))?
        .get(0);
    ensure!(
        before.as_deref() == Some("old"),
        "case={CONTRACT}: expected the seeded row before DROP, actual {before:?}"
    );

    // A failed DDL statement has not changed the catalog: the plan stays
    // valid. The duplicate name is the easiest refusal to reach here.
    let duplicate = error_code(h, "CREATE TABLE ddl_replan (id BIGINT)")
        .await
        .with_context(|| format!("case={CONTRACT} refused CREATE"))?;
    ensure!(
        duplicate == "42P07",
        "case={CONTRACT}: duplicate CREATE expected 42P07, actual {duplicate}"
    );
    let still_valid: Option<String> = h
        .lake
        .query_one(&statement, &[])
        .await
        .with_context(|| format!("case={CONTRACT} Execute after refused DDL"))?
        .get(0);
    ensure!(
        still_valid.as_deref() == Some("old"),
        "case={CONTRACT}: a refused DDL must not break the bound plan, actual {still_valid:?}"
    );

    command(h, "DROP TABLE ddl_replan").await?;
    let error = h
        .lake
        .query(&statement, &[])
        .await
        .expect_err("Execute after same-connection DROP must fail");
    ensure_server_sqlstate(&error, "42P01", CONTRACT)?;

    // The generated plan is replaced, not reused: the same Statement now
    // binds the table re-created under the same name.
    command(
        h,
        "CREATE TABLE ddl_replan (id BIGINT NOT NULL PRIMARY KEY, label TEXT)",
    )
    .await?;
    h.lake
        .simple_query("INSERT INTO ddl_replan VALUES (1, 'new')")
        .await
        .with_context(|| format!("case={CONTRACT} seed re-created table"))?;
    let after: Option<String> = h
        .lake
        .query_one(&statement, &[])
        .await
        .with_context(|| format!("case={CONTRACT} Execute after DROP+CREATE"))?
        .get(0);
    ensure!(
        after.as_deref() == Some("new"),
        "case={CONTRACT}: DROP+CREATE must bind the new table, actual {after:?}"
    );
    eprintln!(
        "PASS case={CONTRACT} DROP=42P01 DROP+CREATE=new-table refused-DDL=keeps-plan"
    );
    command(h, "DROP TABLE ddl_replan").await?;
    Ok(())
}

/// A prepared statement binds catalog objects at Parse. A `DROP` from another
/// connection removes the metadata but does not invalidate this connection's
/// plan, so `Execute` still runs the old binding: its scan lists no committed
/// files and the aggregate returns zero rows instead of PostgreSQL's
/// `42P01`. Re-Prepare sees the deletion; its SQLSTATE is the documented
/// catalog-resolution boundary (`XX000`), not PostgreSQL equivalence.
async fn cross_connection_drop_keeps_the_bound_plan(h: &Harness) -> Result<()> {
    const CONTRACT: &str = "cross-connection-drop-bound-plan";
    const CLASSIFICATION: &str = "lakesoul-catalog-binding";
    let other = h.connect_lake().await?;
    other
        .simple_query("CREATE TABLE ddl_cross_drop (id BIGINT, label TEXT)")
        .await
        .context("cross-connection fixture CREATE")?;
    other
        .simple_query("INSERT INTO ddl_cross_drop VALUES (1, 'a'), (2, 'b')")
        .await
        .context("cross-connection fixture INSERT")?;
    let statement = h
        .lake
        .prepare("SELECT count(*) FROM ddl_cross_drop")
        .await
        .context("prepare before the cross-connection DROP")?;
    let before: i64 = h
        .lake
        .query_one(&statement, &[])
        .await
        .context("first Execute before the cross-connection DROP")?
        .get(0);
    ensure!(
        before == 2,
        "case={CONTRACT}: expected 2 rows before the peer DROP, actual {before}"
    );

    other
        .simple_query("DROP TABLE ddl_cross_drop")
        .await
        .context("peer DROP TABLE")?;
    ensure!(
        metadata(h, "ddl_cross_drop").await?.is_none(),
        "case={CONTRACT}: peer DROP must remove the table_info row"
    );

    // A refused DDL statement on this connection must not advance the
    // generation: if it did, the stale plan below would be replanned and
    // report the now-missing table instead of running its old binding.
    let refused = error_code(h, "CREATE TABLE items (id INT)")
        .await
        .with_context(|| format!("case={CONTRACT} refused DDL"))?;
    ensure!(
        refused == "42P07",
        "case={CONTRACT}: duplicate CREATE expected 42P07, actual {refused}"
    );

    // The old binding still runs: no error, no rows.
    let after: i64 = h
        .lake
        .query_one(&statement, &[])
        .await
        .with_context(|| {
            format!(
                "case={CONTRACT} classification={CLASSIFICATION}: the old \
                 binding must keep executing after a peer DROP and a refused \
                 local DDL"
            )
        })?
        .get(0);
    ensure!(
        after == 0,
        "case={CONTRACT} classification={CLASSIFICATION}: expected zero rows \
         from the old table_id, actual {after}"
    );

    let error = h
        .lake
        .prepare("SELECT count(*) FROM ddl_cross_drop")
        .await
        .expect_err("a fresh Prepare after the peer DROP must fail");
    ensure_server_sqlstate(&error, "XX000", "fresh Prepare after peer DROP")?;
    eprintln!(
        "PASS case={CONTRACT} classification={CLASSIFICATION} old-binding=zero-rows \
         re-prepare=XX000 metadata=absent refused-DDL=no-invalidation"
    );
    Ok(())
}

/// `DROP` + same-name `CREATE` from another connection leaves the first
/// connection's plan on the old `table_id`: the old binding returns no rows
/// (the old commits were dropped) while a fresh Prepare reads the new table.
/// This is the documented LakeSoul catalog-binding boundary, not PostgreSQL's
/// catalog invalidation.
async fn cross_connection_drop_and_recreate_keeps_the_bound_plan(
    h: &Harness,
) -> Result<()> {
    const CONTRACT: &str = "cross-connection-drop-recreate-bound-plan";
    const CLASSIFICATION: &str = "lakesoul-catalog-binding";
    let other = h.connect_lake().await?;
    other
        .simple_query("CREATE TABLE ddl_cross_recreate (id BIGINT, label TEXT)")
        .await
        .context("cross-connection recreate fixture CREATE")?;
    other
        .simple_query("INSERT INTO ddl_cross_recreate VALUES (1, 'old')")
        .await
        .context("cross-connection recreate fixture INSERT")?;
    let old_table_id = metadata(h, "ddl_cross_recreate")
        .await?
        .with_context(|| format!("case={CONTRACT}: metadata before DROP"))?
        .table_id;
    let statement = h
        .lake
        .prepare("SELECT label FROM ddl_cross_recreate WHERE id = 1")
        .await
        .context("prepare before the cross-connection DROP")?;
    let before: Option<String> = h
        .lake
        .query_one(&statement, &[])
        .await
        .context("first Execute before the cross-connection DROP")?
        .get(0);
    ensure!(
        before.as_deref() == Some("old"),
        "case={CONTRACT}: expected the old row before DROP, actual {before:?}"
    );

    other
        .simple_query("DROP TABLE ddl_cross_recreate")
        .await
        .context("peer DROP TABLE")?;
    other
        .simple_query("CREATE TABLE ddl_cross_recreate (id BIGINT, label TEXT)")
        .await
        .context("peer same-name CREATE TABLE")?;
    other
        .simple_query("INSERT INTO ddl_cross_recreate VALUES (1, 'new')")
        .await
        .context("peer INSERT into the re-created table")?;
    let new_table_id = metadata(h, "ddl_cross_recreate")
        .await?
        .with_context(|| format!("case={CONTRACT}: metadata after re-create"))?
        .table_id;
    ensure!(
        new_table_id != old_table_id,
        "case={CONTRACT}: re-create must publish a new table_id, both were \
         {old_table_id}"
    );

    let stale_rows = h.lake.query(&statement, &[]).await.with_context(|| {
        format!(
            "case={CONTRACT} classification={CLASSIFICATION}: the old \
                 binding must keep executing after DROP+CREATE"
        )
    })?;
    ensure!(
        stale_rows.is_empty(),
        "case={CONTRACT} classification={CLASSIFICATION}: the old table_id has \
         no commits after its DROP, expected no rows, actual {}",
        stale_rows.len()
    );

    let fresh = h
        .lake
        .prepare("SELECT label FROM ddl_cross_recreate WHERE id = 1")
        .await
        .context("fresh Prepare after DROP+CREATE")?;
    let fresh_value: Option<String> = h
        .lake
        .query_one(&fresh, &[])
        .await
        .context("Execute of the fresh Prepare")?
        .get(0);
    ensure!(
        fresh_value.as_deref() == Some("new"),
        "case={CONTRACT}: a fresh Prepare must bind the new table, actual \
         {fresh_value:?}"
    );
    eprintln!(
        "PASS case={CONTRACT} classification={CLASSIFICATION} \
         old-binding=no-rows fresh-prepare=new-table"
    );
    other
        .simple_query("DROP TABLE ddl_cross_recreate")
        .await
        .context("cleanup DROP of the re-created table")?;
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

/// Require a client error to be a server `ErrorResponse` with `expected`.
fn ensure_server_sqlstate(
    error: &tokio_postgres::Error,
    expected: &str,
    context: &str,
) -> Result<()> {
    let db = error.as_db_error().with_context(|| {
        format!(
            "{context}: expected a server ErrorResponse, actual client-side \
             rejection: {error}"
        )
    })?;
    ensure!(
        db.code().code() == expected,
        "{context}: expected SQLSTATE {expected}, actual {} ({})",
        db.code().code(),
        db.message()
    );
    Ok(())
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
             properties->>'hashBucketNum', properties->>'file_format', \
             properties->>'appendOnly', properties->>'use_cdc', \
             properties->>'lakesoul_cdc_change_column', \
             properties->>'vortex_row_block_size', \
             properties->>'vortex_data_block_target_bytes' \
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
        append_only: row.get(5),
        use_cdc: row.get(6),
        cdc_change_column: row.get(7),
        vortex_row_block_size: row.get(8),
        vortex_data_block_target_bytes: row.get(9),
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

/// Table rows the test namespace holds, used to prove a refused statement
/// published nothing even when its name is not a valid SQL identifier.
async fn namespace_table_count(h: &Harness) -> Result<i64> {
    let row = h
        .reference
        .query_one(
            "SELECT count(*) FROM public.table_info WHERE table_namespace = $1",
            &[&h.lake_namespace],
        )
        .await
        .context("count table_info rows in the test namespace")?;
    row.try_get(0)
        .context("count table_info rows in the test namespace")
}

/// `[table_info, table_name_id, table_path_id, partition_info, data_commit_info]`
/// counts keyed by `table_id`, so a drop is proven to cascade even after the
/// `table_info` row it used to be found through is gone.
async fn counts_for_table_id(h: &Harness, table_id: &str) -> Result<[i64; 5]> {
    let row = h
        .reference
        .query_one(
            "SELECT \
             (SELECT count(*) FROM public.table_info WHERE table_id = $1), \
             (SELECT count(*) FROM public.table_name_id WHERE table_id = $1), \
             (SELECT count(*) FROM public.table_path_id WHERE table_id = $1), \
             (SELECT count(*) FROM public.partition_info WHERE table_id = $1), \
             (SELECT count(*) FROM public.data_commit_info WHERE table_id = $1)",
            &[&table_id],
        )
        .await
        .with_context(|| format!("metadata counts for table_id={table_id}"))?;
    Ok([row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)])
}

async fn pg_class_count(h: &Harness, table: &str) -> Result<i64> {
    let row = h
        .lake
        .query_one(
            "SELECT count(*) FROM pg_class WHERE relname = $1",
            &[&table],
        )
        .await
        .with_context(|| format!("pg_class count for {table}"))?;
    row.try_get(0)
        .with_context(|| format!("pg_class count for {table}"))
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
