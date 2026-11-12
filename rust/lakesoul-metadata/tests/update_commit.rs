// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for UPDATE publication at the metadata layer.
//!
//! `UPDATE` reads old rows and publishes new row versions, so its commit is a
//! read-modify-write: the partition versions it captured must still be the
//! versions it publishes on top of. These tests pin that contract with real
//! PostgreSQL and never rely on timing:
//!
//! - concurrent updates from the same snapshot have exactly one winner; the
//!   loser gets the typed version conflict the SQL layer maps to `40001` and
//!   publishes neither a partition version nor a commit row;
//! - a retried statement appends its own version, so no increment is lost;
//! - an ordinary append landing between the snapshot and the commit conflicts
//!   with the update instead of being rebased over;
//! - an update of one range partition is not invalidated by a commit in
//!   another partition;
//! - a multi-partition update is all-or-nothing: one stale partition makes
//!   the whole statement publish nothing, never a prefix;
//! - the publication appends the new commit id to the existing snapshot, so
//!   the rows it did not rewrite stay live;
//! - an update whose table was dropped (or dropped and rebuilt under the same
//!   name) publishes nothing and leaves no orphan rows; orphans are checked by
//!   `table_id`, because the snapshot APIs cannot see a dropped table.
//!
//! The tests require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and use unique table names.

use std::collections::HashSet;
use std::sync::Arc;

use lakesoul_metadata::error::Result;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata::{
    LakeSoulMetaDataError, MetaDataClient, PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY,
    PooledClient, QueryType, pg_config_from_env,
};
use lakesoul_metadata_proto::entity::{PartitionInfo, TableInfo};
use tokio::sync::Barrier;

const NAMESPACE: &str = "default";

fn unique_name(tag: &str) -> String {
    format!("update_commit_{tag}_{}", uuid::Uuid::new_v4().simple())
}

async fn create_test_table(client: &MetaDataClient, name: &str) -> Result<String> {
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.to_string(),
            table_namespace: NAMESPACE.to_string(),
            table_path: format!("file:///tmp/{name}"),
            table_schema: "[]".to_string(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            domain: "public".to_string(),
            ..Default::default()
        })
        .await?;
    Ok(table_id)
}

/// A file op as an UPDATE reports it: a new version of rows that already
/// existed. The metadata layer never inspects row contents.
fn file(partition_desc: &str, name: &str) -> DataFileInfo {
    DataFileInfo {
        partition_desc: partition_desc.to_string(),
        path: format!("/tmp/{partition_desc}/{name}.parquet"),
        file_op: "add".to_string(),
        size: 1,
        file_exist_cols: "id,n".to_string(),
        ..Default::default()
    }
}

fn snapshot_ids(partition_info: &PartitionInfo) -> HashSet<(u64, u64)> {
    partition_info
        .snapshot
        .iter()
        .map(|id| (id.high, id.low))
        .collect()
}

async fn pool() -> Result<PooledClient> {
    PooledClient::try_new(
        pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?,
        None,
    )
    .await
}

/// Every `partition_info` and `data_commit_info` row that belongs to
/// `table_id`, sorted. Two equal states mean an attempt published nothing,
/// including no uncommitted or orphan row.
async fn table_rows(pool: &PooledClient, table_id: &str) -> Result<Vec<String>> {
    let table_id = table_id.to_string();
    Ok(pool
        .query(
            "SELECT 'data:' || row_to_json(d)::text AS state FROM data_commit_info d WHERE table_id = $1
             UNION ALL
             SELECT 'partition:' || row_to_json(p)::text AS state FROM partition_info p WHERE table_id = $1
             ORDER BY state",
            QueryType::RW,
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

async fn latest_partition_info(
    client: &MetaDataClient,
    table_id: &str,
    partition_desc: &str,
) -> Result<PartitionInfo> {
    client
        .get_partition_info_by_table_id_and_partition_list(
            table_id,
            &[partition_desc.to_string()],
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| {
            LakeSoulMetaDataError::NotFound(format!(
                "partition {partition_desc} not found"
            ))
        })
}

/// The typed storage conflicts a refused publication produces. Both map to
/// the retryable `40001` (`write_exec::metadata_error`); a raw PostgreSQL
/// error would not, so a unique-key race must not surface as one.
#[track_caller]
fn assert_version_conflict(error: LakeSoulMetaDataError) {
    assert!(
        matches!(
            error,
            LakeSoulMetaDataError::InsertConflict { .. }
                | LakeSoulMetaDataError::CommitConflict(_)
        ),
        "a lost version check must be a typed conflict, got: {error}"
    );
}

/// Seeds a table with one committed file in `partitions`.
async fn seed(
    client: &MetaDataClient,
    table: &TableInfo,
    partitions: &[&str],
) -> Result<()> {
    let files = partitions
        .iter()
        .map(|partition| file(partition, "seed"))
        .collect::<Vec<_>>();
    let published = client.commit_insert_only(table, &[], files).await?;
    assert_eq!(
        published.len(),
        partitions.len(),
        "the seed must publish one commit per partition"
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_updates_from_the_same_snapshot_have_one_winner() -> Result<()> {
    let first = Arc::new(MetaDataClient::from_env().await?);
    let second = Arc::new(MetaDataClient::from_env().await?);
    let pool = pool().await?;
    let name = unique_name("race");
    let table_id = create_test_table(&first, &name).await?;
    let table = first.get_table_info_by_table_id(&table_id).await?.unwrap();
    seed(&first, &table, &["p1"]).await?;

    // Both writers capture the same snapshot, exactly like two statements
    // that read the same old row.
    let first_snapshot = first.get_all_partition_info(&table_id).await?;
    let second_snapshot = second.get_all_partition_info(&table_id).await?;
    assert_eq!(
        first_snapshot, second_snapshot,
        "both updates must start from the same snapshot"
    );
    let base = first_snapshot[0].clone();
    assert_eq!(base.version, 0);
    assert_eq!(base.snapshot.len(), 1);

    let barrier = Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for (index, client) in [first.clone(), second.clone()].into_iter().enumerate() {
        let barrier = barrier.clone();
        let table = table.clone();
        let snapshot = if index == 0 {
            first_snapshot.clone()
        } else {
            second_snapshot.clone()
        };
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .commit_update(
                    &table,
                    &snapshot,
                    vec![file("p1", &format!("update_{index}"))],
                )
                .await
        }));
    }

    let mut winners = 0;
    let mut conflicts = 0;
    for task in tasks {
        match task.await.expect("writer panicked") {
            Ok(published) => {
                assert_eq!(published.len(), 1, "one commit for one partition");
                winners += 1;
            }
            Err(error) => {
                assert_version_conflict(error);
                conflicts += 1;
            }
        }
    }
    assert_eq!(winners, 1, "concurrent updates must not both publish");
    assert_eq!(conflicts, 1, "the loser must be refused, not merged");

    // The winner appended its version; nothing was replaced.
    let after_race = latest_partition_info(&first, &table_id, "p1").await?;
    assert_eq!(after_race.version, base.version + 1);
    assert_eq!(after_race.snapshot.len(), base.snapshot.len() + 1);
    assert_eq!(
        first
            .get_data_files_of_single_partition(&after_race)
            .await?
            .len(),
        2,
        "the live file and the updated row version must both remain"
    );

    // Retrying the whole statement reads the new snapshot and publishes the
    // second increment, so the two attempts add up.
    let retried = first
        .commit_update(
            &table,
            std::slice::from_ref(&after_race),
            vec![file("p1", "retry")],
        )
        .await?;
    assert_eq!(retried.len(), 1);
    let after_retry = latest_partition_info(&first, &table_id, "p1").await?;
    assert_eq!(after_retry.version, base.version + 2);
    assert_eq!(after_retry.snapshot.len(), base.snapshot.len() + 2);

    // Only three committed commit rows exist: the seed, the winner and the
    // retry. The losing update left no row at all.
    let rows = table_rows(&pool, &table_id).await?;
    assert_eq!(
        rows.iter().filter(|row| row.starts_with("data:")).count(),
        3,
        "unexpected commit rows: {rows:#?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.starts_with("partition:"))
            .count(),
        3,
        "seed, winner and retry must retain their partition versions: {rows:#?}"
    );

    first.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn update_conflicts_with_an_ordinary_writer_without_orphans() -> Result<()> {
    let updater = Arc::new(MetaDataClient::from_env().await?);
    let writer = Arc::new(MetaDataClient::from_env().await?);
    let pool = pool().await?;
    let name = unique_name("ordinary");
    let table_id = create_test_table(&updater, &name).await?;
    let table = updater
        .get_table_info_by_table_id(&table_id)
        .await?
        .unwrap();
    seed(&updater, &table, &["p1"]).await?;
    let snapshot = updater.get_all_partition_info(&table_id).await?;

    // A plain append lands after the UPDATE captured its snapshot.
    writer
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "append")])
        .await?;
    let before = table_rows(&pool, &table_id).await?;
    let head = latest_partition_info(&updater, &table_id, "p1").await?;
    assert_eq!(head.version, snapshot[0].version + 1);

    let error = updater
        .commit_update(&table, &snapshot, vec![file("p1", "stale_update")])
        .await
        .expect_err("a stale UPDATE must not rebase over an append");
    assert_version_conflict(error);
    assert_eq!(
        table_rows(&pool, &table_id).await?,
        before,
        "a refused update must not leave a commit row, a version, or an \
         uncommitted row"
    );

    // The retried statement publishes on the appended version and keeps the
    // appended file live.
    updater
        .commit_update(
            &table,
            std::slice::from_ref(&head),
            vec![file("p1", "retry")],
        )
        .await?;
    let after = latest_partition_info(&updater, &table_id, "p1").await?;
    assert_eq!(after.version, head.version + 1);
    assert_eq!(after.snapshot.len(), head.snapshot.len() + 1);
    let files = updater
        .get_data_files_of_single_partition(&after)
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    assert!(
        files.contains("/tmp/p1/append.parquet"),
        "the concurrently appended file must stay live: {files:?}"
    );
    assert!(
        files.contains("/tmp/p1/retry.parquet"),
        "the retried update must be live: {files:?}"
    );

    updater.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn updates_on_different_partitions_do_not_conflict() -> Result<()> {
    let first = Arc::new(MetaDataClient::from_env().await?);
    let second = Arc::new(MetaDataClient::from_env().await?);
    let pool = pool().await?;
    let name = unique_name("partitions");
    let table_id = create_test_table(&first, &name).await?;
    let table = first.get_table_info_by_table_id(&table_id).await?.unwrap();
    seed(&first, &table, &["p1", "p2"]).await?;
    let snapshot = first.get_all_partition_info(&table_id).await?;
    let p1 = snapshot
        .iter()
        .find(|partition| partition.partition_desc == "p1")
        .cloned()
        .expect("seeded p1");
    let p2 = snapshot
        .iter()
        .find(|partition| partition.partition_desc == "p2")
        .cloned()
        .expect("seeded p2");
    assert_eq!(p1.version, 0);
    assert_eq!(p2.version, 0);

    let barrier = Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for (client, expected, partition) in [
        (first.clone(), p1.clone(), "p1"),
        (second, p2.clone(), "p2"),
    ] {
        let barrier = barrier.clone();
        let table = table.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .commit_update(
                    &table,
                    std::slice::from_ref(&expected),
                    vec![file(partition, &format!("update_{partition}"))],
                )
                .await
        }));
    }
    // Both statements touch their own partition, so both must publish.
    for task in tasks {
        let published = task
            .await
            .expect("writer panicked")
            .expect("an update of another partition must not conflict");
        assert_eq!(published.len(), 1);
    }

    for (partition, expected) in [("p1", &p1), ("p2", &p2)] {
        let after = latest_partition_info(&first, &table_id, partition).await?;
        assert_eq!(
            after.version,
            expected.version + 1,
            "partition {partition} must advance exactly once"
        );
        assert_eq!(
            after.snapshot.len(),
            expected.snapshot.len() + 1,
            "partition {partition} must append one commit"
        );
    }
    let rows = table_rows(&pool, &table_id).await?;
    assert_eq!(
        rows.iter().filter(|row| row.starts_with("data:")).count(),
        4,
        "one seed, one update per partition: {rows:#?}"
    );

    first.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn multi_partition_update_is_all_or_nothing() -> Result<()> {
    let updater = Arc::new(MetaDataClient::from_env().await?);
    let writer = Arc::new(MetaDataClient::from_env().await?);
    let pool = pool().await?;
    let name = unique_name("atomic");
    let table_id = create_test_table(&updater, &name).await?;
    let table = updater
        .get_table_info_by_table_id(&table_id)
        .await?
        .unwrap();
    seed(&updater, &table, &["p1", "p2"]).await?;
    let snapshot = updater.get_all_partition_info(&table_id).await?;

    // One of the two partitions moves after the snapshot was captured.
    writer
        .commit_data_files(&name, NAMESPACE, vec![file("p2", "append")])
        .await?;
    let before = table_rows(&pool, &table_id).await?;

    // The statement covers both partitions; the stale one fails it as a whole.
    let error = updater
        .commit_update(
            &table,
            &snapshot,
            vec![file("p1", "partial_p1"), file("p2", "partial_p2")],
        )
        .await
        .expect_err("a stale partition must fail the whole update");
    assert_version_conflict(error);
    assert_eq!(
        table_rows(&pool, &table_id).await?,
        before,
        "the failed statement must not publish its other partition either"
    );
    let p1 = latest_partition_info(&updater, &table_id, "p1").await?;
    assert_eq!(
        p1.version, 0,
        "the untouched partition must keep its committed version"
    );

    // Retrying with the fresh snapshot publishes both partitions together.
    let fresh = updater.get_all_partition_info(&table_id).await?;
    let published = updater
        .commit_update(
            &table,
            &fresh,
            vec![file("p1", "retry_p1"), file("p2", "retry_p2")],
        )
        .await?;
    assert_eq!(
        published.len(),
        2,
        "one commit per partition, in one commit"
    );
    for partition in ["p1", "p2"] {
        let after = latest_partition_info(&updater, &table_id, partition).await?;
        let expected = fresh
            .iter()
            .find(|head| head.partition_desc == partition)
            .expect("fresh partition head");
        assert_eq!(after.version, expected.version + 1);
        assert_eq!(after.snapshot.len(), expected.snapshot.len() + 1);
    }
    let rows = table_rows(&pool, &table_id).await?;
    assert_eq!(
        rows.iter().filter(|row| row.starts_with("data:")).count(),
        5,
        "two seeds, one concurrent append and two retried commits: {rows:#?}"
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row.starts_with("partition:"))
            .count(),
        5
    );

    updater.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn update_appends_and_keeps_the_live_snapshot() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let name = unique_name("append");
    let table_id = create_test_table(&client, &name).await?;
    let table = client.get_table_info_by_table_id(&table_id).await?.unwrap();
    seed(&client, &table, &["p1"]).await?;
    let base = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(base.snapshot.len(), 1);

    let published = client
        .commit_update(
            &table,
            std::slice::from_ref(&base),
            vec![file("p1", "updated")],
        )
        .await?;
    assert_eq!(published.len(), 1);

    let after = latest_partition_info(&client, &table_id, "p1").await?;
    // The new snapshot is the old snapshot plus the statement's commit: the
    // rows this statement did not rewrite stay live. A whole-partition
    // replacement (`CommitOp::UpdateCommit` through `commit_data`) would have
    // dropped them.
    assert_eq!(
        after.snapshot.len(),
        base.snapshot.len() + 1,
        "an update must append, not replace, the partition snapshot"
    );
    assert!(
        snapshot_ids(&base).is_subset(&snapshot_ids(&after)),
        "every previously live commit must still be in the snapshot"
    );
    let files = client
        .get_data_files_of_single_partition(&after)
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(
        files,
        HashSet::from([
            "/tmp/p1/seed.parquet".to_string(),
            "/tmp/p1/updated.parquet".to_string(),
        ]),
        "the live file set must contain the old rows and the new version"
    );

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn update_without_files_publishes_nothing() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = pool().await?;
    let name = unique_name("no_rows");
    let table_id = create_test_table(&client, &name).await?;
    let table = client.get_table_info_by_table_id(&table_id).await?.unwrap();
    seed(&client, &table, &["p1"]).await?;
    let before = table_rows(&pool, &table_id).await?;
    let snapshot = client.get_all_partition_info(&table_id).await?;

    // `UPDATE 0`: no row matched, so there is nothing to publish.
    let published = client.commit_update(&table, &snapshot, Vec::new()).await?;
    assert!(published.is_empty(), "an empty update must publish nothing");
    assert_eq!(table_rows(&pool, &table_id).await?, before);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn update_of_a_dropped_table_leaves_no_orphans() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = pool().await?;
    let name = unique_name("dropped");
    let table_id = create_test_table(&client, &name).await?;
    let table = client.get_table_info_by_table_id(&table_id).await?.unwrap();
    seed(&client, &table, &["p1"]).await?;
    let snapshot = client.get_all_partition_info(&table_id).await?;

    // The table is dropped while the update holds its snapshot and identity.
    client.drop_table(&name, NAMESPACE).await?;
    assert!(
        client
            .get_table_info_by_table_id(&table_id)
            .await?
            .is_none()
    );

    let error = client
        .commit_update(&table, &snapshot, vec![file("p1", "orphan")])
        .await
        .expect_err("an update of a dropped table must not publish");
    assert!(
        matches!(error, LakeSoulMetaDataError::NotFound(_)),
        "the commit transaction must find no table row for the dropped \
         identity, got: {error}"
    );
    assert_eq!(
        table_rows(&pool, &table_id).await?,
        Vec::<String>::new(),
        "a refused update must not recreate partition or commit rows for a \
         dropped table_id"
    );
    Ok(())
}

#[tokio::test]
async fn update_after_same_name_rebuild_does_not_publish_to_the_new_identity()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let other = Arc::new(MetaDataClient::from_env().await?);
    let pool = pool().await?;
    let name = unique_name("rebuild");
    let old_table_id = create_test_table(&client, &name).await?;
    let old_table = client
        .get_table_info_by_table_id(&old_table_id)
        .await?
        .unwrap();
    seed(&client, &old_table, &["p1"]).await?;
    let old_snapshot = client.get_all_partition_info(&old_table_id).await?;

    // Same name, different identity: the name now points at a fresh table.
    other.drop_table(&name, NAMESPACE).await?;
    let new_name = name.clone();
    let new_table_id = create_test_table(&other, &new_name).await?;
    assert_ne!(new_table_id, old_table_id);
    let new_table = other
        .get_table_info_by_table_id(&new_table_id)
        .await?
        .unwrap();
    seed(&other, &new_table, &["p1"]).await?;
    let new_rows = table_rows(&pool, &new_table_id).await?;

    // The parked update still holds the old identity and must not publish.
    let error = client
        .commit_update(&old_table, &old_snapshot, vec![file("p1", "stale")])
        .await
        .expect_err("a stale identity must not publish");
    assert!(
        matches!(error, LakeSoulMetaDataError::NotFound(_)),
        "the rebuilt name must not satisfy the old table_id, got: {error}"
    );
    assert_eq!(
        table_rows(&pool, &old_table_id).await?,
        Vec::<String>::new(),
        "the dropped identity must not gain partition or commit rows"
    );
    assert_eq!(
        table_rows(&pool, &new_table_id).await?,
        new_rows,
        "the rebuilt table must be untouched"
    );
    let new_head = latest_partition_info(&other, &new_table_id, "p1").await?;
    assert_eq!(new_head.version, 0);
    assert_eq!(new_head.snapshot.len(), 1);

    other.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

/// Observe an actual blocked backend, rather than guessing when it reached
/// publication from a delay. The returned blockers identify the lock owner.
async fn blocked_backend(
    pool: &PooledClient,
    application: &str,
) -> Result<(i32, Vec<i32>)> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let rows = pool
                .query(
                    "select pid, pg_blocking_pids(pid) from pg_stat_activity \
                     where application_name = $1 and wait_event_type = 'Lock'",
                    QueryType::RW,
                    &[&application],
                )
                .await?;
            if let Some(row) = rows.first() {
                return Ok((row.get(0), row.get(1)));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("metadata operation never reached its lock barrier")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_waits_for_inflight_update_before_removing_any_metadata() -> Result<()> {
    let config = pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?;
    let name = unique_name("drop_publication");
    let update_application = format!("update_{}", uuid::Uuid::new_v4().simple());
    let drop_application = format!("drop_{}", uuid::Uuid::new_v4().simple());
    let updater = Arc::new(
        MetaDataClient::from_config(
            format!("{config} application_name={update_application}"),
            None,
        )
        .await?,
    );
    let dropper = MetaDataClient::from_config(
        format!("{config} application_name={drop_application}"),
        None,
    )
    .await?;
    let pool = pool().await?;
    let table_id = create_test_table(&updater, &name).await?;
    let table = updater
        .get_table_info_by_table_id(&table_id)
        .await?
        .unwrap();
    seed(&updater, &table, &["p1"]).await?;
    let snapshot = updater.get_all_partition_info(&table_id).await?;
    let before = table_rows(&pool, &table_id).await?;

    // Publication has acquired its table identity lock but cannot insert its
    // partition version until this transaction opens the gate.
    let mut connection = pool.get(QueryType::RW).await?;
    let gate = connection.transaction().await?;
    gate.batch_execute("lock table partition_info in share mode")
        .await?;
    let update_task = tokio::spawn(async move {
        updater
            .commit_update(&table, &snapshot, vec![file("p1", "update")])
            .await
    });
    let (update_pid, _) = blocked_backend(&pool, &update_application).await?;
    let drop_name = name.clone();
    let drop_task =
        tokio::spawn(async move { dropper.drop_table(&drop_name, NAMESPACE).await });
    let (_, blockers) = blocked_backend(&pool, &drop_application).await?;
    assert!(
        blockers.contains(&update_pid),
        "DROP must wait for the publisher's identity lock, not remove \
         name/path records and only then wait on partition metadata: {blockers:?}"
    );
    let mappings = pool
        .query(
            "select (select count(*) from table_name_id where table_id = $1), \
                    (select count(*) from table_path_id where table_id = $1)",
            QueryType::RW,
            &[&table_id],
        )
        .await?;
    assert_eq!(mappings[0].get::<_, i64>(0), 1);
    assert_eq!(mappings[0].get::<_, i64>(1), 1);
    assert_eq!(table_rows(&pool, &table_id).await?, before);

    gate.commit().await?;
    let published = tokio::time::timeout(std::time::Duration::from_secs(30), update_task)
        .await
        .expect("UPDATE did not finish after publication was unblocked")
        .expect("UPDATE task panicked")?;
    assert_eq!(published.len(), 1);
    tokio::time::timeout(std::time::Duration::from_secs(30), drop_task)
        .await
        .expect("DROP did not finish after the publisher released its identity")
        .expect("DROP task panicked")?;
    assert_eq!(table_rows(&pool, &table_id).await?, Vec::<String>::new());
    let remaining = pool
        .query(
            "select (select count(*) from table_info where table_id = $1) + \
                    (select count(*) from table_name_id where table_id = $1) + \
                    (select count(*) from table_path_id where table_id = $1)",
            QueryType::RW,
            &[&table_id],
        )
        .await?;
    assert_eq!(remaining[0].get::<_, i64>(0), 0);
    Ok(())
}
