// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `DELETE` and `TRUNCATE` contracts against the real server.

use crate::support;

use crate::support::Harness;
use anyhow::{Context, Result, ensure};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_and_truncate_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { scenarios(h).await })).await
}

/// The rewrite path with two real Flight workers and local fallback disabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_delete_contract() -> Result<()> {
    support::run_distributed_insert_suite(|h| {
        Box::pin(async move { distributed_delete(h).await })
    })
    .await
}

async fn distributed_delete(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE del_dist (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("CREATE TABLE del_dist")?;
    h.lake
        .batch_execute(
            "INSERT INTO del_dist \
             SELECT i, i * 2 FROM generate_series(1, 1000) AS t(i)",
        )
        .await
        .context("distributed INSERT del_dist")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM del_dist").await? == 1000,
        "distributed insert must write 1000 rows"
    );

    let deleted = h
        .lake
        .execute("DELETE FROM del_dist WHERE id <= 500", &[])
        .await
        .context("distributed DELETE del_dist")?;
    ensure!(deleted == 500, "expected DELETE 500, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_dist").await? == 500,
        "the surviving rows must be 500"
    );
    ensure!(
        scalar(h, "SELECT min(id) FROM del_dist").await? == 501,
        "the first surviving key must be 501"
    );
    eprintln!("PASS case=distributed-delete rewrite=ok rows=500");
    Ok(())
}

async fn scenarios(h: &Harness) -> Result<()> {
    delete_without_primary_key(h).await?;
    delete_parquet(h).await?;
    delete_jsonb(h).await?;
    delete_with_primary_key(h).await?;
    delete_cdc_table(h).await?;
    delete_partition_clear(h).await?;
    delete_null_semantics(h).await?;
    prepared_delete_returning_identity(h).await?;
    append_only_and_lifecycle(h).await?;
    truncate_table(h).await?;
    truncate_partition(h).await?;
    truncate_protocol_paths(h).await?;
    garbage_boundaries(h).await?;
    Ok(())
}

async fn scalar(h: &Harness, sql: &str) -> Result<i64> {
    let row = h
        .lake
        .query_one(sql, &[])
        .await
        .with_context(|| format!("query `{sql}`"))?;
    Ok(row.get(0))
}

async fn delete_without_primary_key(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE del_nopk (id BIGINT, val BIGINT, label TEXT)")
        .await
        .context("CREATE TABLE del_nopk")?;
    h.lake
        .batch_execute(
            "INSERT INTO del_nopk VALUES (1, 10, 'a'), (1, 11, 'b'), (2, 20, 'c'), (3, 30, 'd')",
        )
        .await
        .context("INSERT del_nopk")?;

    let deleted = h
        .lake
        .execute("DELETE FROM del_nopk WHERE id = 1", &[])
        .await
        .context("DELETE del_nopk id=1")?;
    ensure!(deleted == 2, "expected DELETE 2, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_nopk").await? == 2,
        "two rows must remain"
    );
    ensure!(
        scalar(h, "SELECT count(*) FROM del_nopk WHERE id = 1").await? == 0,
        "deleted rows must be gone"
    );

    let version: i32 = h
        .reference
        .query_one(
            "SELECT version FROM public.partition_info p \
             JOIN public.table_info t ON t.table_id = p.table_id \
             WHERE t.table_name = 'del_nopk' AND t.table_namespace = $1 \
             ORDER BY p.version DESC LIMIT 1",
            &[&h.lake_namespace],
        )
        .await
        .context("read del_nopk partition version")?
        .get(0);
    ensure!(version >= 1, "DELETE must publish a new partition version");

    let deleted = h
        .lake
        .execute("DELETE FROM del_nopk", &[])
        .await
        .context("DELETE del_nopk all")?;
    ensure!(deleted == 2, "expected DELETE 2, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_nopk").await? == 0,
        "table must be empty"
    );

    h.lake
        .batch_execute("TRUNCATE TABLE del_nopk")
        .await
        .context("TRUNCATE del_nopk")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM del_nopk").await? == 0,
        "truncate must leave the table empty"
    );

    eprintln!("PASS case=delete-nopk rewrite=ok count=2 truncate=ok");
    Ok(())
}

async fn delete_parquet(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE del_parquet (id BIGINT, val BIGINT) \
             WITH (file_format = 'parquet')",
        )
        .await
        .context("CREATE TABLE del_parquet")?;
    h.lake
        .batch_execute("INSERT INTO del_parquet VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .context("INSERT del_parquet")?;
    let deleted = h
        .lake
        .execute("DELETE FROM del_parquet WHERE id = 2", &[])
        .await
        .context("DELETE del_parquet")?;
    ensure!(deleted == 1, "expected DELETE 1, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_parquet").await? == 2,
        "parquet rewrite must keep two rows"
    );
    h.lake
        .batch_execute("DROP TABLE del_parquet")
        .await
        .context("DROP del_parquet")?;
    eprintln!("PASS case=delete-parquet rewrite=ok");
    Ok(())
}

async fn delete_jsonb(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE del_jsonb (id BIGINT PRIMARY KEY, payload JSONB)")
        .await
        .context("CREATE TABLE del_jsonb")?;
    h.lake
        .batch_execute(
            "INSERT INTO del_jsonb VALUES \
             (1, '{\"b\": 2, \"a\": 1}'), (2, '{\"x\": [1, 2]}')",
        )
        .await
        .context("INSERT del_jsonb")?;
    let deleted = h
        .lake
        .execute("DELETE FROM del_jsonb WHERE id = 1", &[])
        .await
        .context("DELETE del_jsonb")?;
    ensure!(deleted == 1, "expected DELETE 1, actual {deleted}");

    let statement = h
        .lake
        .prepare("SELECT payload FROM del_jsonb WHERE id = 2")
        .await
        .context("prepare JSONB SELECT after DELETE")?;
    ensure!(
        statement.columns()[0].type_().oid() == 3802,
        "the rewrite must keep the jsonb result type"
    );
    let row = h
        .lake
        .query_one(&statement, &[])
        .await
        .context("read JSONB after DELETE")?;
    ensure!(
        row.get::<_, serde_json::Value>(0) == serde_json::json!({"x": [1, 2]}),
        "the rewritten JSONB value must survive"
    );

    h.lake
        .batch_execute("DROP TABLE del_jsonb")
        .await
        .context("DROP del_jsonb")?;
    eprintln!("PASS case=delete-jsonb rewrite=ok type=3802");
    Ok(())
}

async fn delete_with_primary_key(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE del_pk (id BIGINT PRIMARY KEY, val BIGINT, label TEXT)",
        )
        .await
        .context("CREATE TABLE del_pk")?;
    h.lake
        .batch_execute(
            "INSERT INTO del_pk VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')",
        )
        .await
        .context("INSERT del_pk")?;

    let deleted = h
        .lake
        .execute("DELETE FROM del_pk WHERE id = 2", &[])
        .await
        .context("DELETE del_pk id=2")?;
    ensure!(deleted == 1, "expected DELETE 1, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_pk").await? == 2,
        "two rows must remain"
    );
    ensure!(
        scalar(h, "SELECT count(*) FROM del_pk WHERE id = 2").await? == 0,
        "deleted key must be gone"
    );

    let rows = h
        .lake
        .query("DELETE FROM del_pk WHERE id = 3 RETURNING id, val", &[])
        .await
        .context("DELETE ... RETURNING")?;
    ensure!(rows.len() == 1, "RETURNING must return the deleted row");
    ensure!(rows[0].get::<_, i64>(0) == 3, "RETURNING id");
    ensure!(
        rows[0].get::<_, Option<i64>>(1) == Some(30),
        "RETURNING val"
    );

    let statement = h
        .lake
        .prepare("DELETE FROM del_pk WHERE id = $1")
        .await
        .context("prepare DELETE")?;
    let deleted = h
        .lake
        .execute(&statement, &[&1i64])
        .await
        .context("execute prepared DELETE")?;
    ensure!(deleted == 1, "prepared DELETE must delete one row");

    h.lake
        .batch_execute("INSERT INTO del_pk VALUES (4, 40, 'd')")
        .await
        .context("INSERT del_pk before alias DELETE")?;
    let deleted = h
        .lake
        .execute("DELETE FROM del_pk AS d WHERE d.id = 4", &[])
        .await
        .context("DELETE with alias")?;
    ensure!(deleted == 1, "alias DELETE must delete one row");

    h.lake
        .batch_execute("INSERT INTO del_pk VALUES (5, 50, 'e'), (6, 60, 'f')")
        .await
        .context("INSERT del_pk before subquery DELETE")?;
    let deleted = h
        .lake
        .execute(
            "DELETE FROM del_pk WHERE id IN (SELECT id FROM del_pk WHERE val = 50)",
            &[],
        )
        .await
        .context("DELETE with a subquery predicate")?;
    ensure!(deleted == 1, "subquery DELETE must delete one row");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_pk").await? == 1,
        "only the unrelated row must remain"
    );

    eprintln!(
        "PASS case=delete-pk rewrite=ok returning=ok prepared=ok alias=ok subquery=ok"
    );
    Ok(())
}

async fn delete_cdc_table(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE del_cdc (id BIGINT PRIMARY KEY, op TEXT, val BIGINT) \
             WITH (use_cdc = true, lakesoul_cdc_change_column = 'op')",
        )
        .await
        .context("CREATE TABLE del_cdc")?;
    h.lake
        .batch_execute(
            "INSERT INTO del_cdc (id, op, val) VALUES \
             (1, 'insert', 10), (2, 'insert', 20), (3, 'insert', 30)",
        )
        .await
        .context("INSERT del_cdc")?;

    let deleted = h
        .lake
        .execute("DELETE FROM del_cdc WHERE id = 2", &[])
        .await
        .context("DELETE del_cdc id=2")?;
    ensure!(deleted == 1, "expected DELETE 1, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_cdc").await? == 2,
        "the tombstoned key must be hidden"
    );
    ensure!(
        scalar(h, "SELECT count(*) FROM del_cdc WHERE id = 2").await? == 0,
        "the tombstoned key must not be readable"
    );

    h.lake
        .batch_execute(
            "INSERT INTO del_cdc (id, op, val) VALUES (2, 'insert', 21) \
             ON CONFLICT (id) DO UPDATE SET op = EXCLUDED.op, val = EXCLUDED.val",
        )
        .await
        .context("revive del_cdc id=2")?;
    ensure!(
        scalar(h, "SELECT val FROM del_cdc WHERE id = 2").await? == 21,
        "a later insert must revive the key"
    );

    h.lake.batch_execute("BEGIN").await.context("BEGIN")?;
    let deleted = h
        .lake
        .execute("DELETE FROM del_cdc WHERE id = 3", &[])
        .await
        .context("DELETE inside a block")?;
    ensure!(deleted == 1, "CDC DELETE in a block must report one row");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_cdc").await? == 2,
        "read-your-writes must hide the staged tombstone"
    );
    h.lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM del_cdc").await? == 3,
        "rollback must restore the staged tombstone"
    );

    h.lake
        .batch_execute("BEGIN")
        .await
        .context("BEGIN commit")?;
    h.lake
        .execute("DELETE FROM del_cdc WHERE id = 1", &[])
        .await
        .context("DELETE inside a committed block")?;
    h.lake.batch_execute("COMMIT").await.context("COMMIT")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM del_cdc").await? == 2,
        "COMMIT must publish the staged tombstone"
    );

    h.lake
        .batch_execute(
            "CREATE TABLE del_cdc_nopk (id BIGINT, op TEXT) \
             WITH (use_cdc = true, lakesoul_cdc_change_column = 'op')",
        )
        .await
        .context("CREATE TABLE del_cdc_nopk")?;
    let error = h
        .lake
        .execute("DELETE FROM del_cdc_nopk", &[])
        .await
        .expect_err("CDC DELETE without a primary key must be refused");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED),
        "CDC DELETE without a primary key must be 0A000, actual {:?}",
        error.code()
    );

    h.lake
        .batch_execute(
            "CREATE TABLE del_cdc_default (id BIGINT PRIMARY KEY, rowkinds TEXT, val BIGINT) \
             WITH (use_cdc = true)",
        )
        .await
        .context("CREATE TABLE del_cdc_default")?;
    h.lake
        .batch_execute(
            "INSERT INTO del_cdc_default (id, rowkinds, val) \
             VALUES (1, 'insert', 10), (2, 'insert', 20)",
        )
        .await
        .context("INSERT del_cdc_default")?;
    let deleted = h
        .lake
        .execute("DELETE FROM del_cdc_default WHERE id = 1", &[])
        .await
        .context("DELETE del_cdc_default")?;
    ensure!(
        deleted == 1,
        "default CDC change column must delete one row"
    );
    ensure!(
        scalar(h, "SELECT count(*) FROM del_cdc_default").await? == 1,
        "the default CDC tombstone must hide the key"
    );

    h.lake
        .batch_execute("DROP TABLE del_cdc, del_cdc_nopk, del_cdc_default")
        .await
        .context("DROP del_cdc tables")?;
    eprintln!(
        "PASS case=delete-cdc tombstone=ok revive=ok block=ok no-pk=0A000 default=ok"
    );
    Ok(())
}

async fn delete_partition_clear(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE del_range (part INT, id BIGINT, val BIGINT) \
             PARTITIONED BY (part)",
        )
        .await
        .context("CREATE TABLE del_range")?;
    h.lake
        .batch_execute(
            "INSERT INTO del_range VALUES \
             (1, 1, 10), (1, 2, 20), (2, 1, 30), (2, 2, 40)",
        )
        .await
        .context("INSERT del_range")?;

    let deleted = h
        .lake
        .execute("DELETE FROM del_range WHERE part = 1", &[])
        .await
        .context("DELETE del_range part=1")?;
    ensure!(deleted == 2, "expected DELETE 2, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_range").await? == 2,
        "only part=2 must remain"
    );
    let cleared: i64 = h
        .reference
        .query_one(
            "SELECT count(*) FROM public.partition_info p \
             JOIN public.table_info t ON t.table_id = p.table_id \
             WHERE t.table_name = 'del_range' AND t.table_namespace = $1 \
               AND p.partition_desc = 'part=1' AND cardinality(p.snapshot) = 0",
            &[&h.lake_namespace],
        )
        .await
        .context("read del_range cleared partition")?
        .get(0);
    ensure!(
        cleared >= 1,
        "partition clear must publish an empty snapshot"
    );

    let deleted = h
        .lake
        .execute("DELETE FROM del_range WHERE val >= 30", &[])
        .await
        .context("DELETE del_range general predicate")?;
    ensure!(deleted == 2, "expected DELETE 2, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_range").await? == 0,
        "general predicate must rewrite both remaining partitions"
    );

    h.lake
        .batch_execute("INSERT INTO del_range VALUES (1, 1, 10), (2, 2, 40), (2, 3, 50)")
        .await
        .context("INSERT del_range before mixed predicate")?;
    let before: i32 = h
        .reference
        .query_one(
            "SELECT version FROM public.partition_info p \
             JOIN public.table_info t ON t.table_id = p.table_id \
             WHERE t.table_name = 'del_range' AND t.table_namespace = $1 \
               AND p.partition_desc = 'part=1' \
             ORDER BY p.version DESC LIMIT 1",
            &[&h.lake_namespace],
        )
        .await
        .context("read del_range part=1 version")?
        .get(0);
    let deleted = h
        .lake
        .execute("DELETE FROM del_range WHERE part = 2 AND val = 40", &[])
        .await
        .context("DELETE mixed partition predicate")?;
    ensure!(deleted == 1, "expected DELETE 1, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_range WHERE part = 1").await? == 1,
        "part=1 must not be rewritten"
    );
    let after: i32 = h
        .reference
        .query_one(
            "SELECT version FROM public.partition_info p \
             JOIN public.table_info t ON t.table_id = p.table_id \
             WHERE t.table_name = 'del_range' AND t.table_namespace = $1 \
               AND p.partition_desc = 'part=1' \
             ORDER BY p.version DESC LIMIT 1",
            &[&h.lake_namespace],
        )
        .await
        .context("read del_range part=1 version after DELETE")?
        .get(0);
    ensure!(
        before == after,
        "a partition-only conjunct must keep other partitions untouched          (before={before}, after={after})"
    );

    h.lake
        .batch_execute("INSERT INTO del_range VALUES (2, 4, 60)")
        .await
        .context("INSERT del_range before IN")?;
    let deleted = h
        .lake
        .execute("DELETE FROM del_range WHERE part IN (1, 2)", &[])
        .await
        .context("DELETE del_range IN")?;
    ensure!(deleted == 3, "expected DELETE 3, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_range").await? == 0,
        "partition IN must clear every selected partition"
    );

    eprintln!("PASS case=delete-partition clear=ok rewrite=ok in=ok");
    Ok(())
}

async fn delete_null_semantics(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE del_null (id BIGINT, val BIGINT)")
        .await
        .context("CREATE TABLE del_null")?;
    h.lake
        .batch_execute("INSERT INTO del_null VALUES (1, NULL), (2, 10), (3, NULL)")
        .await
        .context("INSERT del_null")?;

    let deleted = h
        .lake
        .execute("DELETE FROM del_null WHERE val = NULL", &[])
        .await
        .context("DELETE val = NULL")?;
    ensure!(deleted == 0, "NULL comparison must delete nothing");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_null").await? == 3,
        "all rows must remain"
    );

    let deleted = h
        .lake
        .execute("DELETE FROM del_null WHERE val IS NULL", &[])
        .await
        .context("DELETE val IS NULL")?;
    ensure!(deleted == 2, "expected DELETE 2, actual {deleted}");
    ensure!(
        scalar(h, "SELECT count(*) FROM del_null").await? == 1,
        "only the non-NULL row must remain"
    );

    eprintln!("PASS case=delete-null three-valued=ok");
    Ok(())
}

async fn prepared_delete_returning_identity(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE del_prep (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("CREATE TABLE del_prep")?;
    h.lake
        .batch_execute("INSERT INTO del_prep VALUES (1, 10)")
        .await
        .context("INSERT del_prep")?;
    // Prepared on this connection while the table is still the old one.
    let statement = h
        .lake
        .prepare("DELETE FROM del_prep WHERE id = $1 RETURNING id")
        .await
        .context("prepare DELETE ... RETURNING")?;

    // A different connection drops and recreates the name. This connection's
    // prepared plan keeps the old table_id; PostgreSQL's old relation is gone,
    // so the Execute must not bind the replacement.
    let other = h.connect_lake().await?;
    other
        .batch_execute("DROP TABLE del_prep")
        .await
        .context("cross-connection DROP del_prep")?;
    other
        .batch_execute("CREATE TABLE del_prep (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("cross-connection re-create del_prep")?;
    other
        .batch_execute("INSERT INTO del_prep VALUES (1, 999)")
        .await
        .context("cross-connection INSERT recreated del_prep")?;

    let error = h
        .lake
        .query(&statement, &[&1i64])
        .await
        .expect_err("a prepared DELETE RETURNING must not write the recreated table");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE),
        "recreated target must be 42P01, actual {:?}",
        error.code()
    );
    ensure!(
        scalar(h, "SELECT count(*) FROM del_prep").await? == 1,
        "the recreated table must keep its row"
    );

    other
        .batch_execute("DROP TABLE del_prep")
        .await
        .context("DROP recreated del_prep")?;
    eprintln!("PASS case=delete-prepared-returning identity=42P01 row-kept");
    Ok(())
}

async fn append_only_and_lifecycle(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE del_append (id BIGINT, val BIGINT) \
             WITH (append_only = true)",
        )
        .await
        .context("CREATE TABLE del_append")?;
    h.lake
        .batch_execute("INSERT INTO del_append VALUES (1, 10), (2, 20)")
        .await
        .context("INSERT del_append")?;

    let error = h
        .lake
        .execute("DELETE FROM del_append WHERE id = 1", &[])
        .await
        .expect_err("DELETE on appendOnly must be refused");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED),
        "appendOnly DELETE must be 0A000, actual {:?}",
        error.code()
    );

    h.lake
        .batch_execute("TRUNCATE TABLE del_append")
        .await
        .context("TRUNCATE appendOnly")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM del_append").await? == 0,
        "TRUNCATE must reset an appendOnly table"
    );
    h.lake
        .batch_execute("INSERT INTO del_append VALUES (3, 30)")
        .await
        .context("INSERT after appendOnly TRUNCATE")?;

    h.lake
        .batch_execute("TRUNCATE TABLE IF EXISTS missing_truncate")
        .await
        .context("TRUNCATE IF EXISTS")?;
    let error = h
        .lake
        .batch_execute("TRUNCATE TABLE missing_truncate")
        .await
        .expect_err("TRUNCATE missing table must fail");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE),
        "missing table must be 42P01, actual {:?}",
        error.code()
    );

    eprintln!("PASS case=delete-append-only=0A000 truncate=ok if-exists=ok");
    Ok(())
}

async fn truncate_table(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE trunc_a (id BIGINT, val BIGINT)")
        .await
        .context("CREATE TABLE trunc_a")?;
    h.lake
        .batch_execute("CREATE TABLE trunc_b (id BIGINT, val BIGINT)")
        .await
        .context("CREATE TABLE trunc_b")?;
    h.lake
        .batch_execute("INSERT INTO trunc_a VALUES (1, 10), (2, 20)")
        .await
        .context("INSERT trunc_a")?;
    h.lake
        .batch_execute("INSERT INTO trunc_b VALUES (1, 10)")
        .await
        .context("INSERT trunc_b")?;

    h.lake
        .batch_execute("TRUNCATE TABLE trunc_a, trunc_b")
        .await
        .context("TRUNCATE two tables")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_a").await? == 0,
        "trunc_a must be empty"
    );
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_b").await? == 0,
        "trunc_b must be empty"
    );

    let empty_snapshots: i64 = h
        .reference
        .query_one(
            "SELECT count(*) FROM public.partition_info p \
             JOIN public.table_info t ON t.table_id = p.table_id \
             WHERE t.table_name = 'trunc_a' AND t.table_namespace = $1 \
               AND cardinality(p.snapshot) = 0",
            &[&h.lake_namespace],
        )
        .await
        .context("read trunc_a empty snapshots")?
        .get(0);
    ensure!(
        empty_snapshots >= 1,
        "TRUNCATE must publish an empty snapshot"
    );

    h.lake
        .batch_execute("INSERT INTO trunc_a VALUES (3, 30)")
        .await
        .context("INSERT after TRUNCATE")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_a").await? == 1,
        "writes after TRUNCATE must work"
    );

    eprintln!("PASS case=truncate table=ok multi-table=ok empty-snapshot=ok");
    Ok(())
}

async fn truncate_partition(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(
            "CREATE TABLE trunc_part (part INT, id BIGINT, val BIGINT) \
             PARTITIONED BY (part)",
        )
        .await
        .context("CREATE TABLE trunc_part")?;
    h.lake
        .batch_execute("INSERT INTO trunc_part VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)")
        .await
        .context("INSERT trunc_part")?;

    h.lake
        .batch_execute("TRUNCATE TABLE trunc_part PARTITION (part = 1)")
        .await
        .context("TRUNCATE PARTITION")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_part").await? == 1,
        "only part=1 must be cleared"
    );
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_part WHERE part = 2").await? == 1,
        "part=2 must remain"
    );

    h.lake
        .batch_execute(
            "CREATE TABLE trunc_multi (p1 INT, p2 INT, id BIGINT) \
             PARTITIONED BY (p1, p2)",
        )
        .await
        .context("CREATE TABLE trunc_multi")?;
    h.lake
        .batch_execute(
            "INSERT INTO trunc_multi VALUES (1, 1, 10), (1, 2, 20), (2, 1, 30)",
        )
        .await
        .context("INSERT trunc_multi")?;
    h.lake
        .batch_execute("TRUNCATE TABLE trunc_multi PARTITION (p1 = 1, p2 = 2)")
        .await
        .context("TRUNCATE multi-column partition")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_multi").await? == 2,
        "only (1,2) must be cleared"
    );
    ensure!(
        scalar(
            h,
            "SELECT count(*) FROM trunc_multi WHERE p1 = 1 AND p2 = 1"
        )
        .await?
            == 1,
        "(1,1) must remain"
    );

    eprintln!("PASS case=truncate-partition partition=1 remaining=1 multi-column=ok");
    Ok(())
}

async fn truncate_protocol_paths(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute("CREATE TABLE trunc_proto (id BIGINT, val BIGINT)")
        .await
        .context("CREATE TABLE trunc_proto")?;
    h.lake
        .batch_execute("INSERT INTO trunc_proto VALUES (1, 10)")
        .await
        .context("INSERT trunc_proto")?;

    let statement = h
        .lake
        .prepare("TRUNCATE TABLE trunc_proto")
        .await
        .context("prepare TRUNCATE")?;
    h.lake
        .execute(&statement, &[])
        .await
        .context("execute prepared TRUNCATE")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_proto").await? == 0,
        "prepared TRUNCATE must clear the table"
    );

    h.lake
        .batch_execute("INSERT INTO trunc_proto VALUES (2, 20)")
        .await
        .context("INSERT before DELETE batch")?;
    let error = h
        .lake
        .simple_query("SELECT 1; DELETE FROM trunc_proto")
        .await
        .expect_err("DELETE in a simple-query batch must be refused");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED),
        "DELETE in a batch must be 0A000, actual {:?}",
        error.code()
    );

    let error = h
        .lake
        .simple_query("SELECT 1; TRUNCATE TABLE trunc_proto")
        .await
        .expect_err("TRUNCATE in a simple-query batch must be refused");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED),
        "TRUNCATE in a batch must be 0A000, actual {:?}",
        error.code()
    );

    h.lake
        .batch_execute("TRUNCATE TABLE trunc_proto RESTART IDENTITY CASCADE")
        .await
        .context("TRUNCATE CASCADE RESTART IDENTITY")?;
    ensure!(
        scalar(h, "SELECT count(*) FROM trunc_proto").await? == 0,
        "TRUNCATE options must not stop the table reset"
    );

    eprintln!("PASS case=truncate-protocol prepared=ok batch=0A000 options=ok");
    Ok(())
}

async fn garbage_boundaries(h: &Harness) -> Result<()> {
    let error = h
        .lake
        .execute("DELETE FROM missing_table", &[])
        .await
        .expect_err("DELETE on a missing table must fail");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE),
        "missing table must be 42P01, actual {:?}",
        error.code()
    );

    let error = h
        .lake
        .execute(
            "DELETE FROM del_pk USING del_pk AS other WHERE del_pk.id = other.id",
            &[],
        )
        .await
        .expect_err("DELETE ... USING must be refused");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED),
        "DELETE ... USING must be 0A000, actual {:?}",
        error.code()
    );

    h.lake.batch_execute("BEGIN").await.context("BEGIN")?;
    let error = h
        .lake
        .execute("DELETE FROM del_nopk WHERE id = 1", &[])
        .await
        .expect_err("rewrite DELETE in a block must be refused");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED),
        "rewrite DELETE in a block must be 0A000, actual {:?}",
        error.code()
    );
    h.lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;

    h.lake
        .batch_execute("BEGIN")
        .await
        .context("BEGIN for TRUNCATE")?;
    let error = h
        .lake
        .batch_execute("TRUNCATE TABLE del_pk")
        .await
        .expect_err("TRUNCATE in a block must be refused");
    ensure!(
        error.code() == Some(&tokio_postgres::error::SqlState::FEATURE_NOT_SUPPORTED),
        "TRUNCATE in a block must be 0A000, actual {:?}",
        error.code()
    );
    h.lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;

    eprintln!(
        "PASS case=delete-truncate-boundaries missing=42P01 using=0A000 block=0A000"
    );
    Ok(())
}
