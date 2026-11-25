// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Client transaction identity locks, with an independent coordinator and
//! PostgreSQL lock-state barriers instead of timing-based collision attempts.

use crate::support::{self, Harness};
use anyhow::{Context, Result, bail, ensure};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_identity_locks() -> Result<()> {
    support::run_suite(|h| Box::pin(contract(h))).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_transaction_identity_locks() -> Result<()> {
    support::run_distributed_insert_suite(|h| Box::pin(contract(h))).await
}

async fn contract(h: &Harness) -> Result<()> {
    let peer = h.connect_peer_coordinator().await?;
    begin_is_lazy(h, &peer).await?;
    prepared_queries_without_scans_still_lock(h, &peer).await?;
    for simple in [true, false] {
        for ending in ["COMMIT", "ROLLBACK", "disconnect"] {
            read_lock(h, &peer, simple, ending).await?;
        }
        for ending in ["COMMIT", "ROLLBACK"] {
            write_lock(h, &peer, simple, ending).await?;
        }
    }
    // Exercise the same locks through one coordinator's shared metadata pool.
    read_lock(h, &h.lake, true, "COMMIT").await?;
    write_lock(h, &h.lake, false, "ROLLBACK").await?;
    alter_waits_for_reader(h, &peer).await?;
    stale_drop_preserves_replacement_indexes(h, &peer).await?;
    failed_statement_releases_locks(h, &peer).await?;
    ddl_and_data_publish_with_retained_locks(h, &peer).await?;
    rollback_to_savepoint_preserves_earlier_locks(h, &peer).await?;
    read_committed_allows_peer_append(h, &peer).await?;
    cancellation_releases_a_waiting_lock(h).await?;
    deleted_identity_is_not_rebound(h, &peer).await?;
    Ok(())
}

async fn command(client: &Client, simple: bool, sql: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        if simple {
            client.batch_execute(sql).await?;
        } else {
            client.execute(sql, &[]).await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| format!("command timed out: {sql}"))??;
    Ok(())
}

async fn create(h: &Harness, name: &str) -> Result<String> {
    h.lake
        .batch_execute(&format!("CREATE TABLE {name} (id INTEGER)"))
        .await?;
    Ok(h.reference.query_one(
        "SELECT table_id FROM public.table_info WHERE table_namespace=$1 AND table_name=$2",
        &[&h.lake_namespace, &name],
    ).await?.get(0))
}

async fn holder(h: &Harness, table_id: &str) -> Result<i32> {
    let label = format!("lakesoul-tx:{table_id}");
    Ok(h.reference.query_one(
        "SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND xact_start IS NOT NULL",
        &[&label],
    ).await.context("table access must retain a real metadata transaction")?.get(0))
}

async fn no_owned_metadata(h: &Harness, id: &str) -> Result<()> {
    for table in [
        "table_info",
        "table_name_id",
        "table_path_id",
        "partition_info",
        "data_commit_info",
    ] {
        let count: i64 = h
            .reference
            .query_one(
                &format!("SELECT count(*) FROM public.{table} WHERE table_id=$1"),
                &[&id],
            )
            .await?
            .get(0);
        ensure!(count == 0, "old id {id} left {count} rows in {table}");
    }
    Ok(())
}

async fn begin_is_lazy(h: &Harness, peer: &Client) -> Result<()> {
    let id = create(h, "tx_lazy").await?;
    let client = h.connect_lake().await?;
    client.batch_execute("BEGIN; SELECT 1").await?;
    let label = format!("lakesoul-tx:{id}");
    let count: i64 = h
        .reference
        .query_one(
            "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
            &[&label],
        )
        .await?
        .get(0);
    ensure!(
        count == 0,
        "BEGIN/SELECT 1 must not reserve a table transaction"
    );
    command(peer, true, "DROP TABLE tx_lazy").await?;
    client.batch_execute("ROLLBACK").await?;
    no_owned_metadata(h, &id).await?;
    eprintln!(
        "PASS transaction-lock lazy BEGIN permits peer DROP before first table access"
    );
    Ok(())
}

async fn read_lock(h: &Harness, peer: &Client, simple: bool, ending: &str) -> Result<()> {
    let table = format!("tx_read_{}_{}", usize::from(simple), ending.to_lowercase());
    let id = create(h, &table).await?;
    let mut client = Some(h.connect_lake().await?);
    let sql = format!("SELECT count(*) FROM {table}");
    // The prepared plan predates BEGIN; Execute must still acquire the lock.
    let prepared = client.as_ref().unwrap().prepare(&sql).await?;
    command(client.as_ref().unwrap(), simple, "BEGIN").await?;
    if simple {
        client.as_ref().unwrap().simple_query(&sql).await?;
    } else {
        client.as_ref().unwrap().query(&prepared, &[]).await?;
    }
    let pid = holder(h, &id).await?;
    let drop_sql = format!("DROP TABLE {table}");
    let drop = command(peer, simple, &drop_sql);
    tokio::pin!(drop);
    tokio::select! {
        result = &mut drop => bail!("DROP completed before the reader ended: {result:?}"),
        result = h.wait_for_metadata_blocker(pid) => result?,
    }
    if ending == "disconnect" {
        client.take();
    } else {
        command(client.as_ref().unwrap(), simple, ending).await?;
    }
    drop.await?;
    no_owned_metadata(h, &id).await?;
    eprintln!(
        "PASS transaction-lock read simple={simple} ending={ending} metadata_pid={pid}"
    );
    Ok(())
}

async fn write_lock(
    h: &Harness,
    peer: &Client,
    simple: bool,
    ending: &str,
) -> Result<()> {
    let table = format!("tx_write_{}_{}", usize::from(simple), ending.to_lowercase());
    let id = create(h, &table).await?;
    let client = h.connect_lake().await?;
    command(&client, simple, "BEGIN").await?;
    command(&client, simple, &format!("INSERT INTO {table} VALUES (17)")).await?;
    let pending = h.table_file_bytes(&table).await?;
    ensure!(
        !pending.is_empty(),
        "the transaction must own real staged files"
    );
    let pid = holder(h, &id).await?;
    let drop_sql = format!("DROP TABLE {table}");
    let drop = command(peer, simple, &drop_sql);
    tokio::pin!(drop);
    tokio::select! {
        result = &mut drop => bail!("DROP completed before the writer ended: {result:?}"),
        result = h.wait_for_metadata_blocker(pid) => result?,
    }
    command(&client, simple, ending).await?;
    drop.await?;
    no_owned_metadata(h, &id).await?;
    let files = h.table_file_bytes(&table).await?;
    if ending == "ROLLBACK" {
        ensure!(files.is_empty(), "ROLLBACK leaked staged files");
    } else {
        ensure!(
            files == pending,
            "DROP retains existing data files; COMMIT must not delete them"
        );
    }
    eprintln!(
        "PASS transaction-lock write simple={simple} ending={ending} metadata_pid={pid}"
    );
    Ok(())
}

async fn read_committed_allows_peer_append(h: &Harness, peer: &Client) -> Result<()> {
    create(h, "tx_fresh").await?;
    let client = h.connect_lake().await?;
    client.batch_execute("BEGIN").await?;
    let sql = "SELECT count(*) FROM tx_fresh";
    let before: i64 = client.query_one(sql, &[]).await?.get(0);
    ensure!(before == 0);
    command(peer, false, "INSERT INTO tx_fresh VALUES (1)").await?;
    let after: i64 = client.query_one(sql, &[]).await?.get(0);
    ensure!(
        after == 1,
        "READ COMMITTED must see a peer append after taking the table lock"
    );
    client.batch_execute("ROLLBACK").await?;
    eprintln!(
        "PASS transaction-lock compatible peer append remains visible to later reads"
    );
    Ok(())
}

async fn cancellation_releases_a_waiting_lock(h: &Harness) -> Result<()> {
    let id = create(h, "tx_cancel_lock").await?;
    let admin = h.connect_reference().await?;
    admin.batch_execute("BEGIN").await?;
    let admin_pid: i32 = admin
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    admin
        .query_one(
            "SELECT 1 FROM public.table_info WHERE table_id=$1 FOR UPDATE",
            &[&id],
        )
        .await?;
    let client = h.connect_lake().await?;
    client.batch_execute("BEGIN").await?;
    let read = client.query("SELECT count(*) FROM tx_cancel_lock", &[]);
    tokio::pin!(read);
    tokio::select! {
        result = &mut read => bail!("read completed while identity was exclusively locked: {result:?}"),
        result = h.wait_for_metadata_blocker(admin_pid) => result?,
    }
    client.cancel_token().cancel_query(NoTls).await?;
    let error = tokio::time::timeout(Duration::from_secs(8), &mut read)
        .await
        .context("cancelled frontend read did not finish")?
        .expect_err("cancelled lock request must fail");
    ensure!(
        error.code().is_some_and(|code| code.code() == "57014"),
        "expected cancellation, got {error:?}"
    );
    // The metadata query must be cancelled too: ROLLBACK cannot remain queued
    // behind it while the administrator still holds the conflicting lock.
    command(&client, true, "ROLLBACK").await?;
    let label = format!("lakesoul-tx:{id}");
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let count: i64 = h
                .reference
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
                    &[&label],
                )
                .await?
                .get(0);
            if count == 0 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("cancelled metadata connection leaked")??;
    admin.batch_execute("ROLLBACK").await?;
    client.batch_execute("SELECT 1").await?;
    eprintln!(
        "PASS transaction-lock cancellation releases blocked metadata query and connection"
    );
    Ok(())
}

async fn ddl_and_data_publish_with_retained_locks(
    h: &Harness,
    peer: &Client,
) -> Result<()> {
    let id = create(h, "tx_ddl_existing").await?;
    create(h, "tx_ddl_drop").await?;
    let client = h.connect_lake().await?;
    for sql in [
        "BEGIN",
        "SELECT count(*) FROM tx_ddl_existing",
        "ALTER TABLE tx_ddl_existing ADD COLUMN note TEXT",
        "INSERT INTO tx_ddl_existing VALUES (1, 'existing')",
        "CREATE TABLE tx_ddl_new (id INT)",
        "INSERT INTO tx_ddl_new VALUES (2)",
        "DROP TABLE tx_ddl_drop",
    ] {
        command(&client, true, sql).await?;
    }
    holder(h, &id).await?;
    let count: i64 = peer
        .query_one("SELECT count(*) FROM tx_ddl_existing", &[])
        .await?
        .get(0);
    ensure!(count == 0, "staged data became visible before COMMIT");
    command(&client, true, "COMMIT").await?;
    let note: String = client
        .query_one("SELECT note FROM tx_ddl_existing WHERE id = 1", &[])
        .await?
        .get(0);
    ensure!(
        note == "existing",
        "ALTER and the data staged against it must both publish"
    );
    let id: i32 = client
        .query_one("SELECT id FROM tx_ddl_new", &[])
        .await?
        .get(0);
    ensure!(id == 2, "CREATE and its staged data must both publish");
    let error = client
        .query("SELECT id FROM tx_ddl_drop", &[])
        .await
        .expect_err("the staged DROP must publish in the same block");
    ensure!(error.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE));
    Ok(())
}

async fn rollback_to_savepoint_preserves_earlier_locks(
    h: &Harness,
    peer: &Client,
) -> Result<()> {
    let id = create(h, "tx_save_before").await?;
    create(h, "tx_save_after").await?;
    let client = h.connect_lake().await?;
    for sql in [
        "BEGIN",
        "SELECT count(*) FROM tx_save_before",
        "SAVEPOINT s",
        "SELECT count(*) FROM tx_save_after",
    ] {
        command(&client, true, sql).await?;
    }
    let pid = holder(h, &id).await?;
    ensure!(client.query("SELECT 1 / 0", &[]).await.is_err());
    command(&client, true, "ROLLBACK TO SAVEPOINT s").await?;
    command(peer, true, "DROP TABLE tx_save_after").await?;
    let drop = command(peer, true, "DROP TABLE tx_save_before");
    tokio::pin!(drop);
    tokio::select! {
        result = &mut drop => bail!("ROLLBACK TO released a pre-savepoint lock: {result:?}"),
        result = h.wait_for_metadata_blocker(pid) => result?,
    }
    command(&client, true, "RELEASE SAVEPOINT s").await?;
    command(&client, true, "COMMIT").await?;
    drop.await?;
    no_owned_metadata(h, &id).await
}

async fn failed_statement_releases_locks(h: &Harness, peer: &Client) -> Result<()> {
    let id = create(h, "tx_error_release").await?;
    let client = h.connect_lake().await?;
    client
        .batch_execute("BEGIN; SELECT count(*) FROM tx_error_release")
        .await?;
    holder(h, &id).await?;
    ensure!(
        client.query("SELECT 1 / 0", &[]).await.is_err(),
        "statement must fail"
    );
    command(peer, true, "DROP TABLE tx_error_release").await?;
    client.batch_execute("COMMIT").await?;
    client.batch_execute("SELECT 1").await?;
    no_owned_metadata(h, &id).await?;
    eprintln!(
        "PASS transaction-lock failed statement aborts metadata before explicit ROLLBACK"
    );
    Ok(())
}

async fn stale_drop_preserves_replacement_indexes(
    h: &Harness,
    peer: &Client,
) -> Result<()> {
    let old = create(h, "tx_replaced").await?;
    let path: String = h
        .reference
        .query_one(
            "SELECT table_path FROM public.table_info WHERE table_id=$1",
            &[&old],
        )
        .await?
        .get(0);
    command(peer, true, "DROP TABLE tx_replaced").await?;
    let new = create(h, "tx_replaced").await?;
    ensure!(new != old);
    let prefix = format!(
        "{}/_vector_index/guard",
        lakesoul_metadata::index_catalog::normalize_index_prefix(&path)
    );
    let shard: i64 = h.reference.query_one("INSERT INTO public.index_shard(kind,index_prefix) VALUES ('vector',$1) RETURNING shard_id", &[&prefix]).await?.get(0);
    let meta = lakesoul_metadata::MetaDataClient::from_env().await?;
    ensure!(
        !meta
            .delete_table_by_table_id_cascade_if_exists(&old, &path)
            .await?,
        "old identity must report zero deletions"
    );
    ensure!(
        meta.delete_table_info_by_id_and_path(&old, &path).await? == 0,
        "legacy DAO must also report zero deletions"
    );
    let kept: i64 = h
        .reference
        .query_one(
            "SELECT count(*) FROM public.index_shard WHERE shard_id=$1",
            &[&shard],
        )
        .await?
        .get(0);
    ensure!(kept == 1, "zero-row old DROP removed a replacement's index");
    let commit = lakesoul_metadata_proto::entity::DataCommitInfo {
        table_id: old.clone(),
        partition_desc: "-5".to_owned(),
        commit_id: Some(lakesoul_metadata_proto::entity::Uuid { high: 11, low: 17 }),
        commit_op: lakesoul_metadata_proto::entity::CommitOp::AppendCommit as i32,
        ..Default::default()
    };
    ensure!(
        matches!(
            meta.commit_data_commit_infos(vec![commit]).await,
            Err(lakesoul_metadata::LakeSoulMetaDataError::NotFound(_))
        ),
        "independent staging must reject a dropped parent"
    );
    no_owned_metadata(h, &old).await?;
    command(peer, true, "DROP TABLE tx_replaced").await?;
    let removed: i64 = h
        .reference
        .query_one(
            "SELECT count(*) FROM public.index_shard WHERE shard_id=$1",
            &[&shard],
        )
        .await?
        .get(0);
    ensure!(
        removed == 0,
        "the replacement's own DROP must remove its index"
    );
    no_owned_metadata(h, &new).await?;
    eprintln!(
        "PASS transaction-lock stale DROP preserves replacement index; staging rejects old identity"
    );
    Ok(())
}

async fn alter_waits_for_reader(h: &Harness, peer: &Client) -> Result<()> {
    let id = create(h, "tx_alter").await?;
    let reader = h.connect_lake().await?;
    reader
        .batch_execute("BEGIN; SELECT count(*) FROM tx_alter")
        .await?;
    let pid = holder(h, &id).await?;
    let alter = command(peer, false, "ALTER TABLE tx_alter ADD COLUMN extra BIGINT");
    tokio::pin!(alter);
    tokio::select! {
        result = &mut alter => bail!("ALTER completed before the reader ended: {result:?}"),
        result = h.wait_for_metadata_blocker(pid) => result?,
    }
    command(&reader, true, "COMMIT").await?;
    alter.await?;
    reader.query("SELECT extra FROM tx_alter", &[]).await?;
    command(peer, true, "DROP TABLE tx_alter").await?;
    no_owned_metadata(h, &id).await?;
    eprintln!("PASS transaction-lock ALTER waits for the client's table-identity lock");
    Ok(())
}

async fn deleted_identity_is_not_rebound(h: &Harness, peer: &Client) -> Result<()> {
    let id = create(h, "tx_drop_wins").await?;
    let admin = h.connect_reference().await?;
    admin.batch_execute("BEGIN").await?;
    let pid: i32 = admin
        .query_one("SELECT pg_backend_pid()", &[])
        .await?
        .get(0);
    admin
        .query_one(
            "SELECT 1 FROM public.table_info WHERE table_id=$1 FOR UPDATE",
            &[&id],
        )
        .await?;
    let reader = h.connect_lake().await?;
    reader.batch_execute("BEGIN").await?;
    let read = reader.query("SELECT count(*) FROM tx_drop_wins", &[]);
    tokio::pin!(read);
    tokio::select! {
        result = &mut read => bail!("read completed before the exclusive owner: {result:?}"),
        result = h.wait_for_metadata_blocker(pid) => result?,
    }
    // Complete the metadata deletion while retaining its parent-row lock.
    // This controls the exact window in which the reader has resolved the
    // original identity but has not yet acquired its shared lock.
    for table in [
        "table_name_id",
        "table_path_id",
        "partition_info",
        "data_commit_info",
        "table_info",
    ] {
        admin
            .execute(
                &format!("DELETE FROM public.{table} WHERE table_id=$1"),
                &[&id],
            )
            .await?;
    }
    admin.batch_execute("COMMIT").await?;
    let error = tokio::time::timeout(Duration::from_secs(8), &mut read)
        .await?
        .expect_err("a deleted identity cannot be read");
    ensure!(
        error.code().is_some_and(|code| code.code() == "42P01"),
        "expected missing table after waiting: {error:?}"
    );
    reader.batch_execute("ROLLBACK").await?;
    let replacement = create(h, "tx_drop_wins").await?;
    ensure!(replacement != id);
    let count: i64 = reader
        .query_one("SELECT count(*) FROM tx_drop_wins", &[])
        .await?
        .get(0);
    ensure!(count == 0);
    command(peer, true, "DROP TABLE tx_drop_wins").await?;
    no_owned_metadata(h, &id).await?;
    no_owned_metadata(h, &replacement).await?;
    eprintln!(
        "PASS transaction-lock first access waits for deletion then returns 42P01 without rebinding"
    );
    Ok(())
}

async fn prepared_queries_without_scans_still_lock(
    h: &Harness,
    peer: &Client,
) -> Result<()> {
    for subquery in [false, true] {
        let table = format!("tx_no_scan_{}", usize::from(subquery));
        let id = create(h, &table).await?;
        let reader = h.connect_lake().await?;
        let sql = if subquery {
            format!("SELECT (SELECT count(*) FROM {table}) WHERE FALSE")
        } else {
            format!("SELECT id FROM {table} WHERE FALSE")
        };
        let plan = reader.prepare(&sql).await?;
        reader.batch_execute("BEGIN").await?;
        ensure!(reader.query(&plan, &[]).await?.is_empty());
        let pid = holder(h, &id).await.context("a prepared query must lock referenced tables even when optimization removes their scans")?;
        let sql = format!("DROP TABLE {table}");
        let drop = command(peer, true, &sql);
        tokio::pin!(drop);
        tokio::select! {
            result = &mut drop => bail!("DROP bypassed a prepared query's table reference: {result:?}"),
            result = h.wait_for_metadata_blocker(pid) => result?,
        }
        command(&reader, true, "COMMIT").await?;
        drop.await?;
        no_owned_metadata(h, &id).await?;
        eprintln!(
            "PASS transaction-lock prepared query retains identity despite removed scan subquery={subquery}"
        );
    }
    Ok(())
}
