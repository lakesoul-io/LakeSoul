// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Concurrency, failure and distributed-execution tests for SQL `UPDATE`.
//!
//! Every statement here targets a primary-key LakeSoul table and is planned by
//! the production planner `planner::update::plan_update_sql` with
//! `SnapshotMode::Capture` — the same function the PostgreSQL front end uses —
//! so the tested behaviour is the one a client gets. That planner is not
//! DataFusion's native `TableProvider::update`: it captures the primary
//! partition snapshot before it builds the old-row scan, installs it in the
//! session (via [`LakeSoulFixedSnapshot`], which every test session carries),
//! and produces the same `LakeSoulWriteExec`/`LakeSoulCommitExec` pair as an
//! `INSERT`, so the distributed planner splits it identically.
//!
//! The concurrency tests use real barriers instead of sleeps racing the
//! writers:
//!
//! - a **gated object store** parks each worker's first upload, which proves
//!   the statement has already read its old rows and computed its new ones
//!   (the write stage can only upload after the scan produced rows) while the
//!   gate is still closed;
//! - a **failing object store** rejects uploads into one range partition by
//!   destination path, so "some files were written, then the statement failed"
//!   is deterministic and not a timing accident.
//!
//! The invariants asserted after each interleaving are the storage ones:
//!
//! - a partition version only advances together with its commit row, in one
//!   metadata transaction; a losing statement leaves neither (`40001`,
//!   retryable, no rebase);
//! - an update appends new row versions, so the partition snapshot keeps its
//!   live files instead of being replaced;
//! - a conflicting range partition makes the whole statement publish nothing,
//!   never a prefix of its partitions;
//! - files are only deleted when this statement provably published nothing;
//!   files of already committed versions stay;
//! - a statement that lost the table identity (DROP + same-name rebuild) must
//!   not publish against the old `table_id` nor leave orphan metadata rows,
//!   which are checked by `table_id` directly, not through `table_info`.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::common::TableReference;
use datafusion::physical_plan::{ExecutionPlan, collect, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion_distributed::DistributedExec;
use futures::StreamExt;
use lakesoul_common::ser::arrow_java::schema_to_metadata_parts;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef, PooledClient, QueryType};
use lakesoul_metadata_proto::entity::TableInfo;
use tokio::sync::watch;

use crate::Result;
use crate::catalog::{LakeSoulTableProperty, format_table_info_partitions};
use crate::cli::CoreArgs;
use crate::lakesoul_table::LakeSoulTable;
use crate::planner::update::{PlannedUpdate, SnapshotMode, plan_update_sql};
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::distributed_tests::{
    distributed_factory_sized, run_distributed_test, spawn_workers,
};
use crate::tests::distributed_write_tests::{
    MetadataState, TableFixture, WRITE_WORKERS, WorkerWriteProbe,
    assert_disk_equals_active_files, assert_keyed_write_is_distributed,
    connect_observers, create_pk_table, data_files_on_disk, find_cause, inserted_rows,
    iv_batch, local_path, metadata_pool, metadata_state, piv_batch, pk_schema,
    read_iv_rows, read_piv_rows, remove_table_dir, seed_pk_rows,
    spawn_fail_partition_workers, spawn_gated_workers, split_data_file_name, wait_until,
    wait_until_worker_drained, write_plan_shape,
};
use crate::write_mode::{LakeSoulFixedSnapshot, WriteConflict};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Plans one `UPDATE` with the production planner.
///
/// `SnapshotMode::Capture` reads the target's partition versions from the
/// primary and installs them in the session before the scan is planned; the
/// returned value owns the installed snapshot and must outlive execution.
async fn plan_update(
    ctx: &SessionContext,
    table: &str,
    sql: &str,
) -> Result<PlannedUpdate> {
    Ok(
        plan_update_sql(ctx, sql, TableReference::bare(table), SnapshotMode::Capture)
            .await?,
    )
}

/// Plans an `UPDATE` and builds the physical plan that will be executed.
///
/// The physical plan is inspected *and* executed by the caller, so the
/// statement id asserted from the file names is the one that wrote them. The
/// [`PlannedUpdate`] must be kept alive until the execution finished.
async fn update_physical_plan(
    ctx: &SessionContext,
    table: &str,
    sql: &str,
) -> Result<(PlannedUpdate, Arc<dyn ExecutionPlan>)> {
    let planned = plan_update(ctx, table, sql).await?;
    let plan = ctx.state().create_physical_plan(planned.plan()).await?;
    Ok((planned, plan))
}

/// Plans and executes one `UPDATE`, keeping the snapshot guard alive for the
/// whole execution.
async fn run_update(
    ctx: &SessionContext,
    table: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let (planned, plan) = update_physical_plan(ctx, table, sql).await?;
    let batches = collect(plan, ctx.task_ctx()).await?;
    drop(planned);
    Ok(batches)
}

/// Plans and executes one non-UPDATE statement (`INSERT ...`).
async fn run_sql(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(collect(plan, ctx.task_ctx()).await?)
}

/// The session config every UPDATE session needs: the planner captures into
/// the per-statement snapshot handle and fails with `MissingSnapshotHandle`
/// without it. Each session gets its own handle, never a shared one.
fn update_session_template() -> Result<datafusion::prelude::SessionConfig> {
    Ok(crate::create_lakesoul_session_config()?
        .with_extension(Arc::new(LakeSoulFixedSnapshot::new())))
}

/// A single-node session over its own [`MetaDataClient`]: an independent
/// coordinator, not a second handle on the distributed one.
fn single_node_session(client: MetaDataClientRef) -> Result<Arc<SessionContext>> {
    let factory = LakeSoulSessionFactory::new(client, &CoreArgs::default())?
        .with_session_template(update_session_template()?);
    factory.create_session(&LakeSoulSessionOptions::default())
}

/// A distributed session (`fallback_to_local = false`) with its own snapshot
/// handle, so UPDATE planning and execution stay on the distributed path.
fn distributed_update_session(
    client: MetaDataClientRef,
    worker_urls: Vec<String>,
) -> Result<Arc<SessionContext>> {
    let factory = distributed_factory_sized(client, worker_urls, 6, Some(1), false)?
        .with_session_template(update_session_template()?);
    factory.create_session(&LakeSoulSessionOptions::default())
}

/// Seeds rows into a range-partitioned primary-key table.
async fn seed_piv_rows(table: &TableFixture, rows: &[(i32, i32, i32)]) -> Result<()> {
    LakeSoulTable::for_name(&table.name)
        .await?
        .execute_upsert(piv_batch(rows))
        .await
}

/// The partition rows and commit rows that belong to `table_id` alone.
///
/// A dropped table's rows are invisible through the snapshot APIs, which is
/// exactly why an orphan check must query the two tables by `table_id`.
async fn table_scoped_rows(pool: &PooledClient, table_id: &str) -> Result<(i64, i64)> {
    let table_id = table_id.to_string();
    let row = pool
        .query(
            "SELECT (SELECT count(*)::bigint FROM partition_info WHERE table_id = $1),
                    (SELECT count(*)::bigint FROM data_commit_info WHERE table_id = $1)",
            QueryType::RW,
            &[&table_id],
        )
        .await?;
    Ok((row[0].get::<_, i64>(0), row[0].get::<_, i64>(1)))
}

/// Rebuilds a table with an existing name and a fresh `table_id`, the way a
/// same-name DROP + CREATE does: the storage rows of the previous table are
/// gone and the name now points at a different identity.
async fn rebuild_table_identity(
    client: &MetaDataClient,
    table: &TableFixture,
) -> Result<String> {
    client.drop_table(&table.name, "default").await?;
    let (schema, schema_ipc, schema_ipc_hash) =
        schema_to_metadata_parts(pk_schema().as_ref());
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: table.name.clone(),
            table_path: format!("file://{}", table.root.display()),
            table_schema: schema,
            table_schema_arrow_ipc: schema_ipc,
            table_schema_arrow_ipc_json_hash: schema_ipc_hash,
            table_namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(String::from("4")),
                file_format: Some(String::from("parquet")),
                ..Default::default()
            })?,
            partitions: format_table_info_partitions(&[], &[String::from("id")]),
            domain: "public".to_string(),
        })
        .await?;
    Ok(table_id)
}

/// Only the files this statement wrote, identified by its statement id.
fn statement_files(files: &[String], write_id: &str) -> Result<Vec<PathBuf>> {
    let mut owned = Vec::new();
    for file in files {
        let path = local_path(file)?;
        if split_data_file_name(&path, write_id).is_some() {
            owned.push(path);
        }
    }
    Ok(owned)
}

/// The commit rows of one table, sorted: `(partition_desc, commits, committed)`.
fn commit_rows(state: &MetadataState) -> Vec<(String, i64, i64)> {
    let mut rows = state
        .commits
        .iter()
        .map(|(partition, commits, _, committed)| {
            (partition.clone(), *commits, *committed)
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

// ---------------------------------------------------------------------------
// Distributed execution
// ---------------------------------------------------------------------------

#[test]
fn update_writes_on_workers_and_publishes_one_commit_per_partition() {
    run_distributed_test(
        update_writes_on_workers_and_publishes_one_commit_per_partition_inner(),
    );
}

/// A distributed `UPDATE` keeps the commit node on the coordinator, runs the
/// write on real workers with `fallback_to_local = false`, and publishes one
/// appended version per touched range partition.
///
/// The files the metadata ends up with must be exactly the files the plan's
/// statement id wrote, spread over several write tasks and several worker
/// servers, so the write provably happened in parallel on the workers.
async fn update_writes_on_workers_and_publishes_one_commit_per_partition_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "update_distributed", true).await?;
    seed_piv_rows(&dest, &[(0, 1, 10), (1, 2, 20), (2, 3, 30)]).await?;
    let seeded = metadata_state(&pool, &client, &dest).await?;
    assert_eq!(
        seeded.versions,
        vec![
            ("part=0".to_string(), 0, 1),
            ("part=1".to_string(), 0, 1),
            ("part=2".to_string(), 0, 1),
        ],
        "{}: the seed must publish one file per range partition",
        dest.name
    );

    let (worker_urls, probes, mut workers) =
        spawn_fail_partition_workers(WRITE_WORKERS, "__no_upload_fault__").await?;
    let ctx = distributed_update_session(client.clone(), worker_urls)?;

    let sql = format!("UPDATE {} SET v = v + 1", dest.name);
    let (_planned, plan) = update_physical_plan(&ctx, &dest.name, &sql).await?;
    let shape = write_plan_shape(&plan);
    assert!(
        plan.is::<DistributedExec>(),
        "a distributed UPDATE must keep its plan distributed:\n{}",
        shape.explain
    );
    assert_keyed_write_is_distributed(&plan, &shape, "UPDATE");
    let write_tasks = shape.write_stage_tasks[0];
    assert!(
        write_tasks > 1,
        "the write of a three-partition UPDATE must fan out over several \
         tasks:\n{}",
        shape.explain
    );
    let write_id = shape
        .write_id
        .clone()
        .expect("the plan's write node carries a statement id");

    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    assert_eq!(
        inserted_rows(&batches),
        3,
        "UPDATE reports the rows it matched"
    );

    let read_ctx = single_node_session(client.clone())?;
    assert_eq!(
        read_piv_rows(&read_ctx, &dest.name).await?,
        vec![(0, 1, 11), (1, 2, 21), (2, 3, 31)],
        "every matched row must carry its incremented value"
    );

    // Which files did *this* statement write? Everything else in the table
    // belongs to the seed.
    let committed = client
        .get_data_files_by_table_name(&dest.name, "default")
        .await?;
    let update_files = statement_files(&committed, &write_id)?;
    assert!(
        !update_files.is_empty(),
        "the UPDATE must have committed files named after its statement id"
    );
    let mut task_ids = BTreeSet::new();
    for path in &update_files {
        let (task, _bucket) =
            split_data_file_name(path, &write_id).unwrap_or_else(|| {
                panic!(
                    "committed file {path:?} does not follow \
                 part-{write_id}T<task>_<bucket:04>.parquet"
                )
            });
        let task = task.unwrap_or_else(|| {
            panic!("a write stage of {write_tasks} tasks must stamp the task index into {path:?}")
        });
        assert!(
            task < write_tasks,
            "file {path:?} carries task {task}, but the stage has {write_tasks} tasks"
        );
        task_ids.insert(task);
    }
    assert!(
        task_ids.len() > 1,
        "the UPDATE's files came from a single write task ({task_ids:?}), so \
         the update did not run in parallel"
    );
    assert_eq!(
        probes.iter().map(WorkerWriteProbe::uploads).sum::<usize>(),
        update_files.len(),
        "each committed UPDATE file must be exactly one worker upload"
    );
    assert!(
        probes.iter().filter(|probe| probe.uploads() > 0).count() >= 2,
        "at least two worker servers must upload the UPDATE's files"
    );

    // One appended version per touched partition: the seed's live file is
    // still part of the snapshot, and the metadata names exactly the files on
    // disk.
    let after = metadata_state(&pool, &client, &dest).await?;
    assert_eq!(
        after.versions,
        vec![
            ("part=0".to_string(), 1, 2),
            ("part=1".to_string(), 1, 2),
            ("part=2".to_string(), 1, 2),
        ],
        "{}: an UPDATE appends one version and keeps the live files",
        dest.name
    );
    assert_eq!(
        commit_rows(&after),
        vec![
            ("part=0".to_string(), 2, 2),
            ("part=1".to_string(), 2, 2),
            ("part=2".to_string(), 2, 2),
        ],
        "{}: one committed commit row for the seed and one for the update",
        dest.name
    );
    assert_eq!(after.uncommitted, 0);
    assert_disk_equals_active_files(&client, &dest).await?;

    workers.abort_all();
    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

// ---------------------------------------------------------------------------
// Lost snapshot race
// ---------------------------------------------------------------------------

#[test]
fn update_losing_the_snapshot_race_is_retryable_and_increments_twice() {
    run_distributed_test(
        update_losing_the_snapshot_race_is_retryable_and_increments_twice_inner(),
    );
}

/// Two independent coordinators update the same row from the same old value.
///
/// The distributed statement is parked after it computed its new row, the
/// single-node statement commits, and the parked one must then fail with the
/// retryable `40001` instead of publishing the same increment twice. Retrying
/// the whole statement applies the second increment, so the final value is
/// the seed's value plus two.
async fn update_losing_the_snapshot_race_is_retryable_and_increments_twice_inner()
-> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "update_race", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;
    let seeded = metadata_state(&pool, &client_a, &dest).await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let ctx = distributed_update_session(client_a.clone(), worker_urls)?;

    let sql = format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name);
    let (_planned, plan) = update_physical_plan(&ctx, &dest.name, &sql).await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "parked UPDATE");

    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    // The write stage can only upload after the old rows were read and the
    // new ones computed, so a parked upload proves the snapshot read is done.
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "the UPDATE parked its first upload at the gate",
    )
    .await;

    // The independent coordinator wins the race with the same old value.
    let ctx_b = single_node_session(client_b.clone())?;
    let won = run_update(
        &ctx_b,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name),
    )
    .await?;
    assert_eq!(inserted_rows(&won), 1);
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 11)],
        "the competing UPDATE must commit before the gate opens"
    );

    gate_tx
        .send(true)
        .expect("the gated workers keep the gate receiver alive");
    let mut failure = None;
    while let Some(item) = stream.next().await {
        if let Err(error) = item {
            failure = Some(error);
            break;
        }
    }
    drop(stream);
    let error = failure.expect("the stale-snapshot UPDATE must fail");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(
        conflict.sqlstate(),
        "40001",
        "a lost snapshot race must be retryable, got: {error}"
    );

    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("raced UPDATE worker {index}"))
            .await?;
    }

    // The losing statement published nothing: nothing but the winner's
    // version exists, and no file it uploaded survives.
    let after_race = metadata_state(&pool, &client_b, &dest).await?;
    assert_eq!(
        after_race.versions,
        vec![(seeded.versions[0].0.clone(), 1, 2)],
        "the loser must not append a version"
    );
    assert_eq!(after_race.uncommitted, 0);
    assert_eq!(
        commit_rows(&after_race),
        vec![(seeded.commits[0].0.clone(), 2, 2)],
        "only the seed and the winner may have committed"
    );
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 11)],
        "the losing UPDATE must not apply its increment"
    );
    assert_disk_equals_active_files(&client_b, &dest).await?;

    // Retrying the whole statement re-reads the row and applies the second
    // increment, so no increment is lost.
    let retried = run_update(
        &ctx_b,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name),
    )
    .await?;
    assert_eq!(inserted_rows(&retried), 1);
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 12)],
        "the retry must increment the value the winner published"
    );
    let after_retry = metadata_state(&pool, &client_b, &dest).await?;
    assert_eq!(
        after_retry.versions,
        vec![(seeded.versions[0].0.clone(), 2, 3)],
        "the retry appends one more version to the seed and the winner"
    );
    assert_eq!(
        commit_rows(&after_retry),
        vec![(seeded.commits[0].0.clone(), 3, 3)]
    );
    assert_disk_equals_active_files(&client_b, &dest).await?;

    workers.abort_all();
    client_b.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_planned_before_a_concurrent_commit_never_publishes_a_stale_row()
-> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "update_stale_plan", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;

    let ctx_a = single_node_session(client_a.clone())?;
    let ctx_b = single_node_session(client_b.clone())?;

    // The production planner captures the target's partition versions from
    // the primary and installs them in the session *before* it builds the
    // old-row scan (`planner::update::plan_update`). Building the physical
    // plan therefore fixes both the file view and the versions the commit
    // will validate: this plan must be refused once a newer commit exists.
    let (_planned, plan) = update_physical_plan(
        &ctx_a,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name),
    )
    .await?;

    let won = run_update(
        &ctx_b,
        &dest.name,
        &format!("UPDATE {} SET v = 100 WHERE id = 1", dest.name),
    )
    .await?;
    assert_eq!(inserted_rows(&won), 1);
    assert_eq!(read_iv_rows(&ctx_b, &dest.name).await?, vec![(1, 100)]);

    // Executing it now must conflict: the version it captured is no longer
    // the head, so publishing would apply rows read from a replaced version.
    let error = collect(plan, ctx_a.task_ctx())
        .await
        .expect_err("a plan from before a concurrent commit must not publish");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(
        conflict.sqlstate(),
        "40001",
        "a plan from before a concurrent commit must be retryable, got: {error}"
    );
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 100)],
        "the refused statement must not publish anything; a refreshed read \
         (101) or a rebased stale row (11) both prove the snapshot was not \
         fixed when the plan was built"
    );
    let after = metadata_state(&pool, &client_b, &dest).await?;
    assert_eq!(after.uncommitted, 0);
    assert_eq!(
        commit_rows(&after),
        vec![(after.commits[0].0.clone(), 2, 2)],
        "only the seed and the winning commit may exist"
    );
    assert_disk_equals_active_files(&client_b, &dest).await?;

    client_a.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_conflicts_with_an_ordinary_replacing_writer() -> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "update_ordinary_writer", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;
    let seeded = metadata_state(&pool, &client_a, &dest).await?;

    let ctx_a = single_node_session(client_a.clone())?;
    let ctx_b = single_node_session(client_b.clone())?;

    let (_planned, plan) = update_physical_plan(
        &ctx_a,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name),
    )
    .await?;

    // An ordinary write path (`INSERT OR REPLACE` publishes through
    // `commit_data_files`, which rebases its own snapshot) rewrites the same
    // row after the UPDATE captured its snapshot.
    let replaced = run_sql(
        &ctx_b,
        &format!("INSERT OR REPLACE INTO {} VALUES (1, 99)", dest.name),
    )
    .await?;
    assert_eq!(inserted_rows(&replaced), 1);
    assert_eq!(read_iv_rows(&ctx_b, &dest.name).await?, vec![(1, 99)]);

    let error = collect(plan, ctx_a.task_ctx())
        .await
        .expect_err("a stale UPDATE must not overwrite a concurrent ordinary write");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(conflict.sqlstate(), "40001", "got: {error}");

    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 99)],
        "the ordinary writer's row must survive the refused UPDATE"
    );
    let after = metadata_state(&pool, &client_b, &dest).await?;
    assert_eq!(after.uncommitted, 0);
    assert_eq!(
        after.versions[0].1,
        seeded.versions[0].1 + 1,
        "only the ordinary write may have appended a version"
    );
    assert_disk_equals_active_files(&client_b, &dest).await?;

    // Retrying re-reads the row and applies the increment to the writer's
    // value, which is what a client does after `40001`.
    let retried = run_update(
        &ctx_b,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name),
    )
    .await?;
    assert_eq!(inserted_rows(&retried), 1);
    assert_eq!(read_iv_rows(&ctx_b, &dest.name).await?, vec![(1, 100)]);

    client_a.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

// ---------------------------------------------------------------------------
// Partition scope
// ---------------------------------------------------------------------------

#[test]
fn update_on_a_different_partition_does_not_conflict() {
    run_distributed_test(update_on_a_different_partition_does_not_conflict_inner());
}

/// A commit in one range partition must not invalidate an update that only
/// touches another partition: the version check is per partition.
async fn update_on_a_different_partition_does_not_conflict_inner() -> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "update_other_partition", true).await?;
    seed_piv_rows(&dest, &[(0, 1, 10), (1, 2, 20)]).await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let ctx = distributed_update_session(client_a.clone(), worker_urls)?;

    let (_planned, plan) = update_physical_plan(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE part = 0", dest.name),
    )
    .await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "partition-scoped UPDATE");

    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "the partition-scoped UPDATE parked its first upload at the gate",
    )
    .await;

    let ctx_b = single_node_session(client_b.clone())?;
    let won = run_update(
        &ctx_b,
        &dest.name,
        &format!("UPDATE {} SET v = v + 100 WHERE part = 1", dest.name),
    )
    .await?;
    assert_eq!(inserted_rows(&won), 1);

    gate_tx
        .send(true)
        .expect("the gated workers keep the gate receiver alive");
    let mut failure = None;
    while let Some(item) = stream.next().await {
        if let Err(error) = item {
            failure = Some(error);
            break;
        }
    }
    drop(stream);
    if let Some(error) = failure {
        panic!("an update of another partition must not conflict: {error}");
    }

    let read_ctx = single_node_session(client_b.clone())?;
    assert_eq!(
        read_piv_rows(&read_ctx, &dest.name).await?,
        vec![(0, 1, 11), (1, 2, 120)],
        "both updates must apply to their own partition"
    );
    let after = metadata_state(&pool, &client_b, &dest).await?;
    assert_eq!(
        after.versions,
        vec![("part=0".to_string(), 1, 2), ("part=1".to_string(), 1, 2)],
        "each partition must advance exactly once"
    );
    assert_eq!(after.uncommitted, 0);
    assert_disk_equals_active_files(&client_b, &dest).await?;

    workers.abort_all();
    client_b.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

#[test]
fn multi_partition_update_with_one_conflicting_partition_publishes_nothing() {
    run_distributed_test(
        multi_partition_update_with_one_conflicting_partition_publishes_nothing_inner(),
    );
}

/// A two-partition `UPDATE` whose second partition moved must fail as a whole:
/// the partition that did not move keeps its committed version and no prefix
/// of the statement becomes visible.
async fn multi_partition_update_with_one_conflicting_partition_publishes_nothing_inner()
-> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "update_partial", true).await?;
    seed_piv_rows(&dest, &[(0, 1, 10), (1, 2, 20)]).await?;
    let seeded = metadata_state(&pool, &client_a, &dest).await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let ctx = distributed_update_session(client_a.clone(), worker_urls)?;

    let (_planned, plan) = update_physical_plan(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1", dest.name),
    )
    .await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "multi-partition UPDATE");

    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "the multi-partition UPDATE parked its first upload at the gate",
    )
    .await;

    let ctx_b = single_node_session(client_b.clone())?;
    let won = run_update(
        &ctx_b,
        &dest.name,
        &format!("UPDATE {} SET v = 100 WHERE part = 1", dest.name),
    )
    .await?;
    assert_eq!(inserted_rows(&won), 1);

    gate_tx
        .send(true)
        .expect("the gated workers keep the gate receiver alive");
    let mut failure = None;
    while let Some(item) = stream.next().await {
        if let Err(error) = item {
            failure = Some(error);
            break;
        }
    }
    drop(stream);
    let error = failure.expect("a conflicting partition must fail the statement");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(conflict.sqlstate(), "40001", "got: {error}");

    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("partial UPDATE worker {index}"))
            .await?;
    }

    assert_eq!(
        read_piv_rows(&ctx_b, &dest.name).await?,
        vec![(0, 1, 10), (1, 2, 100)],
        "the untouched partition must keep its committed value"
    );
    let after = metadata_state(&pool, &client_b, &dest).await?;
    let part0 = seeded
        .versions
        .iter()
        .find(|(desc, _, _)| desc == "part=0")
        .expect("seeded part=0 version");
    assert_eq!(
        after
            .versions
            .iter()
            .find(|(desc, _, _)| desc == "part=0")
            .expect("part=0 version"),
        part0,
        "part=0 must not advance when the statement fails as a whole"
    );
    assert_eq!(
        after
            .versions
            .iter()
            .find(|(desc, _, _)| desc == "part=1")
            .expect("part=1 version"),
        &("part=1".to_string(), 1, 2),
        "only the winning statement may advance part=1"
    );
    assert_eq!(after.uncommitted, 0);
    assert_eq!(
        commit_rows(&after),
        vec![("part=0".to_string(), 1, 1), ("part=1".to_string(), 2, 2)],
        "the failed statement must not leave a commit row anywhere"
    );
    assert_disk_equals_active_files(&client_b, &dest).await?;

    workers.abort_all();
    client_b.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

// ---------------------------------------------------------------------------
// Late failures
// ---------------------------------------------------------------------------

#[test]
fn update_upload_failure_after_progress_publishes_nothing_and_keeps_files() {
    run_distributed_test(
        update_upload_failure_after_progress_publishes_nothing_and_keeps_files_inner(),
    );
}

/// One partition's upload fails after the other partition's file was written.
///
/// The statement must publish nothing, remove the file it did write, and leave
/// every already committed file untouched.
async fn update_upload_failure_after_progress_publishes_nothing_and_keeps_files_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "update_late_failure", true).await?;
    seed_piv_rows(&dest, &[(0, 1, 10), (1, 2, 20)]).await?;
    let seeded = metadata_state(&pool, &client, &dest).await?;
    let seeded_files = data_files_on_disk(&dest.root);
    assert!(!seeded_files.is_empty(), "the seed must write files");

    let (worker_urls, probes, mut workers) =
        spawn_fail_partition_workers(WRITE_WORKERS, "part=1").await?;
    let ctx = distributed_update_session(client.clone(), worker_urls)?;

    let error = run_update(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1", dest.name),
    )
    .await
    .expect_err("the failing partition's upload must fail the statement");
    assert!(
        probes.iter().map(WorkerWriteProbe::uploads).sum::<usize>() > 0,
        "the healthy partition must have uploaded before the statement failed: \
         {error}"
    );
    assert!(
        probes.iter().map(WorkerWriteProbe::failures).sum::<usize>() > 0,
        "the injected upload fault must have fired: {error}"
    );

    let read_ctx = single_node_session(client.clone())?;
    assert_eq!(
        read_piv_rows(&read_ctx, &dest.name).await?,
        vec![(0, 1, 10), (1, 2, 20)],
        "a failed UPDATE must not change any row"
    );
    let after = metadata_state(&pool, &client, &dest).await?;
    assert_eq!(
        after, seeded,
        "a failed UPDATE must not publish a version or a commit row"
    );
    assert_eq!(after.uncommitted, 0);
    // The failed statement's own files are gone; the committed seed files are
    // still the files on disk.
    assert_eq!(
        data_files_on_disk(&dest.root),
        seeded_files,
        "the failed statement's files must be removed and the committed files kept"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    workers.abort_all();
    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

#[test]
fn update_rejected_by_constraint_or_expression_publishes_nothing() {
    run_distributed_test(
        update_rejected_by_constraint_or_expression_publishes_nothing_inner(),
    );
}

/// A rejected assignment — NULL for a NOT NULL column, or an expression that
/// cannot be planned — fails the whole statement: no version, no commit row,
/// no new file, and the committed rows stay readable.
async fn update_rejected_by_constraint_or_expression_publishes_nothing_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "update_not_null", false).await?;
    seed_pk_rows(&dest, &[(1, 10), (2, 20)]).await?;
    let seeded = metadata_state(&pool, &client, &dest).await?;

    let (worker_urls, mut workers) = spawn_workers(WRITE_WORKERS).await;
    let ctx = distributed_update_session(client.clone(), worker_urls)?;

    run_update(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v = NULL WHERE id = 1", dest.name),
    )
    .await
    .expect_err("a NOT NULL column must reject a NULL assignment");

    // An assignment that cannot even be evaluated must fail the same way.
    run_update(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v = 'not-an-int' WHERE id = 1", dest.name),
    )
    .await
    .expect_err("an untypeable assignment must be rejected");

    let read_ctx = single_node_session(client.clone())?;
    assert_eq!(
        read_iv_rows(&read_ctx, &dest.name).await?,
        vec![(1, 10), (2, 20)],
        "a rejected UPDATE must not change any row"
    );
    assert_eq!(
        metadata_state(&pool, &client, &dest).await?,
        seeded,
        "a rejected UPDATE must not change the metadata"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    workers.abort_all();
    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

// ---------------------------------------------------------------------------
// Table identity
// ---------------------------------------------------------------------------

#[test]
fn update_after_drop_and_rebuild_does_not_publish_to_the_new_identity() {
    run_distributed_test(
        update_after_drop_and_rebuild_does_not_publish_to_the_new_identity_inner(),
    );
}

/// An `UPDATE` whose plan outlives a DROP + same-name rebuild must not publish
/// against the old `table_id` and must not touch the rebuilt table.
///
/// The parked statement has provably uploaded files when the rebuild happens,
/// so the failure also has to clean them up without deleting anything that was
/// already on disk. Orphan metadata is checked by `table_id`, because the
/// dropped identity is invisible to the snapshot APIs.
async fn update_after_drop_and_rebuild_does_not_publish_to_the_new_identity_inner()
-> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "update_rebuild", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;
    let old_table_id = dest.table_id.clone();

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let ctx = distributed_update_session(client_a.clone(), worker_urls)?;

    let (_planned, plan) = update_physical_plan(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name),
    )
    .await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "rebuild-racing UPDATE");

    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "the UPDATE parked its first upload at the gate",
    )
    .await;

    // A second, independent coordinator drops the table and creates a table
    // with the same name, which is a different `table_id` over the same path.
    let new_table_id = rebuild_table_identity(&client_b, &dest).await?;
    assert_ne!(new_table_id, old_table_id);
    let rebuilt = LakeSoulTable::for_name(&dest.name).await?;
    rebuilt.execute_upsert(iv_batch(&[(9, 90)])).await?;
    let ctx_b = single_node_session(client_b.clone())?;
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(9, 90)],
        "the rebuilt table must start empty apart from its own seed"
    );
    // Nothing is parked at the gate yet because the parked upload is blocked:
    // the file set right now is exactly what already existed.
    let baseline = data_files_on_disk(&dest.root);

    gate_tx
        .send(true)
        .expect("the gated workers keep the gate receiver alive");
    let mut failure = None;
    while let Some(item) = stream.next().await {
        if let Err(error) = item {
            failure = Some(error);
            break;
        }
    }
    drop(stream);
    let error = failure.expect("an UPDATE of a dropped table must fail");
    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("rebuild UPDATE worker {index}"))
            .await?;
    }
    assert!(
        probes.iter().map(WorkerWriteProbe::uploads).sum::<usize>() > 0,
        "the parked statement must have written files before it failed: {error}"
    );

    // No orphan metadata under the dropped identity, checked by table_id.
    assert_eq!(
        table_scoped_rows(&pool, &old_table_id).await?,
        (0, 0),
        "a failed UPDATE must not resurrect partition or commit rows for a \
         dropped table_id ({error})"
    );

    // The rebuilt table is untouched by the stale statement.
    let rebuilt_rows = client_b.get_all_partition_info(&new_table_id).await?;
    assert_eq!(rebuilt_rows.len(), 1, "the rebuilt table has one partition");
    assert_eq!(rebuilt_rows[0].version, 0);
    assert_eq!(
        rebuilt_rows[0].snapshot.len(),
        1,
        "only the rebuilt table's own seed commit may be active"
    );
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(9, 90)],
        "the stale statement must not change the rebuilt table"
    );

    // The stale statement removed only the files it wrote: the file set on
    // disk is exactly what existed before the gate opened.
    wait_until(
        || data_files_on_disk(&dest.root) == baseline,
        "the failed statement removed the files it uploaded",
    )
    .await;
    assert_eq!(
        data_files_on_disk(&dest.root),
        baseline,
        "the stale statement must clean up its own files and delete nothing else"
    );

    workers.abort_all();
    client_b.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_refuses_non_overwrite_merge_but_allows_explicit_use_last() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "update_merge_operator", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;
    let before = metadata_state(&pool, &client, &dest).await?;
    let info = Arc::new(
        client
            .get_table_info_by_table_id(&dest.table_id)
            .await?
            .expect("the seeded table exists"),
    );
    // A default memory catalog preserves the actual provider, unlike the
    // LakeSoul namespace, which reconstructs providers from table metadata.
    let ctx = SessionContext::new_with_config(
        update_session_template()?.with_create_default_catalog_and_schema(true),
    );
    for (operator, supported) in [("SumAll", false), ("UseLast", true)] {
        let config =
            crate::lakesoul_table::helpers::create_io_config_builder_from_table_info(
                info.clone(),
                Default::default(),
                Default::default(),
            )?
            .with_merge_op("v".to_string(), operator.to_string())
            .build();
        let provider = crate::datasource::table_provider::LakeSoulTableProvider::try_new(
            &ctx.state(),
            client.clone(),
            config,
            info.clone(),
        )
        .await?;
        ctx.register_table(dest.name.as_str(), Arc::new(provider))?;
        let sql = format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name);
        if supported {
            let batches = run_update(&ctx, &dest.name, &sql).await?;
            assert_eq!(inserted_rows(&batches), 1);
        } else {
            let error = plan_update_sql(
                &ctx,
                &sql,
                TableReference::bare(dest.name.clone()),
                SnapshotMode::Capture,
            )
            .await
            .expect_err("an additive merge cannot implement SQL assignment");
            assert!(matches!(
                error,
                crate::planner::update::UpdatePlanError::Table(
                    crate::write_mode::UpdateUnsupported::CustomMergeOperator
                )
            ));
            assert_eq!(error.sqlstate(), "0A000");
            assert_eq!(metadata_state(&pool, &client, &dest).await?, before);
            assert_disk_equals_active_files(&client, &dest).await?;
        }
        ctx.deregister_table(dest.name.as_str())?;
    }
    let read_ctx = single_node_session(client.clone())?;
    assert_eq!(read_iv_rows(&read_ctx, &dest.name).await?, vec![(1, 11)]);
    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_reads_snapshot_file_descriptors_from_primary_not_a_lagging_replica()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "update_replica", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;
    let schema = format!("update_replica_{}", uuid::Uuid::new_v4().simple());
    let connection = pool.get(QueryType::RW).await?;
    // The secondary sees current table/partition definitions but has not
    // replayed data commits. Its search_path shadows only that metadata table.
    connection
        .batch_execute(&format!(
            "create schema {schema}; \
             create table {schema}.data_commit_info \
             (like public.data_commit_info including all)"
        ))
        .await?;
    let config = lakesoul_metadata::pg_config_from_env(
        lakesoul_metadata::PRIMARY_URL_PROP_KEY,
        lakesoul_metadata::PRIMARY_URL_ENV_KEY,
    )?;
    let replica_client = Arc::new(
        MetaDataClient::from_config(
            config.clone(),
            Some(format!("{config} options='-c search_path={schema},public'")),
        )
        .await?,
    );
    let ctx = single_node_session(replica_client)?;
    let result = run_update(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v = v + 1 WHERE id = 1", dest.name),
    )
    .await;
    let primary = single_node_session(client.clone())?;
    let rows = read_iv_rows(&primary, &dest.name).await;
    connection
        .batch_execute(&format!("drop schema {schema} cascade"))
        .await?;
    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);

    assert_eq!(
        inserted_rows(&result?),
        1,
        "a primary snapshot must not lose rows when secondary commit metadata lags"
    );
    assert_eq!(rows?, vec![(1, 11)]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_exceeding_execution_memory_fails_without_publication() -> Result<()> {
    const ROWS: i32 = 4096;
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
    ]));
    let dest = crate::tests::distributed_write_tests::create_table_fixture(
        &client,
        "update_memory",
        schema.clone(),
        false,
        vec!["id".to_string()],
    )
    .await?;
    let tail = "x".repeat(504);
    let seed = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int32Array::from_iter_values(0..ROWS))
                as arrow::array::ArrayRef,
            Arc::new(arrow::array::Int32Array::from_iter_values(0..ROWS)),
            Arc::new(arrow::array::StringArray::from_iter_values(
                (0..ROWS).map(|id| format!("{id:08}{tail}")),
            )),
        ],
    )?;
    LakeSoulTable::for_name(&dest.name)
        .await?
        .execute_upsert(seed)
        .await?;
    let before = metadata_state(&pool, &client, &dest).await?;
    let factory = LakeSoulSessionFactory::new_with_memory_pool(
        client.clone(),
        &CoreArgs::default(),
        Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(
            64 * 1024,
        )),
    )?
    .with_session_template(update_session_template()?.with_batch_size(ROWS as usize));
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
    let result =
        run_update(&ctx, &dest.name, &format!("UPDATE {} SET v=v+1", dest.name)).await;
    let after = metadata_state(&pool, &client, &dest).await?;
    assert_disk_equals_active_files(&client, &dest).await?;
    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    assert_eq!(after, before, "memory exhaustion must not publish a prefix");
    let error = result.expect_err("the input exceeds the explicit execution budget");
    let error = error
        .downcast_report::<datafusion::error::DataFusionError>()
        .unwrap_or_else(|error| panic!("expected a typed DataFusion error: {error}"))
        .into_current_context();
    assert!(
        matches!(
            error.find_root(),
            datafusion::error::DataFusionError::ResourcesExhausted(_)
        ),
        "expected a typed execution-memory error, got: {error}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_scan_losing_captured_commit_descriptors_does_not_become_empty()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let dest = create_pk_table(&client, "update_snapshot_drop", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;
    let ctx = single_node_session(client.clone())?;
    let planned = plan_update(
        &ctx,
        &dest.name,
        &format!("UPDATE {} SET v=v+1 WHERE id=1", dest.name),
    )
    .await?;
    let provider = ctx
        .table_provider(TableReference::bare(dest.name.clone()))
        .await?;

    // Park at the scan's bound-provider boundary: DROP may win after target
    // resolution but before the captured commit descriptors are fetched.
    // Calling the production scan directly prevents a fresh DML target lookup
    // from rejecting DROP earlier and accidentally hiding this interleaving.
    client.drop_table(&dest.name, "default").await?;
    let result = provider.scan(&ctx.state(), None, &[], None).await;
    drop(planned);
    remove_table_dir(&dest);
    let error = result.expect_err("a lost snapshot must not become UPDATE 0");
    assert!(
        matches!(
            find_cause::<lakesoul_metadata::error::LakeSoulMetaDataError>(&error),
            Some(lakesoul_metadata::error::LakeSoulMetaDataError::CommitConflict(_))
        ),
        "expected a typed snapshot conflict, got: {error}"
    );
    Ok(())
}
