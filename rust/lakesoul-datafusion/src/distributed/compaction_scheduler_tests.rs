// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::distributed::DistributedOptions;
use crate::session::LakeSoulSessionFactory;
use crate::tests::create_table_with_file_format;
use crate::tests::distributed_tests::{
    pk_schema, run_distributed_test, spawn_gated_workers, spawn_workers, upsert_pk,
    wait_until_all_reached,
};
use arrow::array::Array;
use lakesoul_io::{config::LakeSoulIOConfigBuilder, file_format::PhysicalFormat};
use lakesoul_metadata::MetaDataClient;
use lakesoul_metadata_proto::entity::{CommitOp, FileOp, PartitionInfo, TableInfo};
use std::time::Duration;
use tokio::time::Instant;

async fn sql(ctx: &SessionContext, statement: &str) -> crate::Result<()> {
    ctx.sql(statement).await?.collect().await?;
    Ok(())
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[test]
fn tick_waits_for_complete_snapshot_compacts_once_and_skips_stale_mirror() {
    run_distributed_test(serial_compaction());
}

async fn serial_compaction() -> crate::Result<()> {
    let source = Arc::new(MetaDataClient::from_env().await?.primary_only().await);
    let name = format!("tick_compact_{}", uuid::Uuid::new_v4().simple());
    create_table_with_file_format(
        source.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(pk_schema())
            .with_primary_keys(vec!["id".into()])
            .with_hash_bucket_num("1")
            .build(),
        PhysicalFormat::Parquet,
    )
    .await?;
    upsert_pk(&name, vec![1, 2], vec![10, 20]).await?;
    upsert_pk(&name, vec![2, 3], vec![200, 30]).await?;
    let table = source
        .get_table_info_by_table_name(&name, "default")
        .await?
        .unwrap();
    let read = source
        .get_all_partition_info(&table.table_id)
        .await?
        .pop()
        .unwrap();
    assert_eq!(read.snapshot.len(), 2);
    let inputs = source
        .get_data_file_infos_of_single_partition(&read)
        .await?;
    assert_eq!(inputs.len(), 2);

    // Publish the test mirror in two steps to deterministically reproduce CDC lag.
    let mirror = Arc::new(SessionContext::new());
    sql(&mirror, "CREATE SCHEMA lakesoul_meta").await?;
    sql(
        &mirror,
        &format!(
            "CREATE TABLE lakesoul_meta.table_info AS SELECT
        {} AS table_id, 'default' AS table_namespace, {} AS table_name, {} AS table_path,
        {} AS table_schema, CAST(NULL AS BYTEA) AS table_schema_arrow_ipc,
        {} AS table_schema_arrow_ipc_json_hash, {} AS properties, {} AS partitions,
        {} AS domain, '' AS creator",
            quote(&table.table_id),
            quote(&name),
            quote(&table.table_path),
            quote(&table.table_schema),
            quote(&table.table_schema_arrow_ipc_json_hash),
            quote(&table.properties),
            quote(&table.partitions),
            quote(&table.domain)
        ),
    )
    .await?;
    let ids: Vec<_> = read
        .snapshot
        .iter()
        .map(|u| uuid::Uuid::from_u64_pair(u.high, u.low).to_string())
        .collect();
    sql(
        &mirror,
        &format!(
            "CREATE TABLE lakesoul_meta.partition_info AS SELECT
        {} AS table_id, {} AS partition_desc, CAST({} AS INT) AS version,
        {} AS commit_op, CAST({} AS BIGINT) AS timestamp, {} AS snapshot,
        {} AS expression, {} AS domain, false AS pinned",
            quote(&read.table_id),
            quote(&read.partition_desc),
            read.version,
            quote(read.commit_op().as_str_name()),
            read.timestamp,
            quote(&serde_json::to_string(&ids)?),
            quote(&read.expression),
            quote(&read.domain)
        ),
    )
    .await?;
    sql(&mirror,"CREATE TABLE lakesoul_meta.data_commit_info (table_id VARCHAR, partition_desc VARCHAR,
        commit_id VARCHAR, committed BOOLEAN, timestamp BIGINT, file_ops VARCHAR)").await?;
    let mut inserts = Vec::new();
    for id in &ids {
        let c = source
            .get_single_data_commit_info(&table.table_id, &read.partition_desc, id)
            .await?
            .unwrap();
        let ops: Vec<_> = c.file_ops.iter().map(|op| serde_json::json!({
            "path": op.path, "size": op.size, "file_exist_cols": op.file_exist_cols,
            "file_op": FileOp::try_from(op.file_op).unwrap().as_str_name()
        })).collect();
        inserts.push(format!(
            "INSERT INTO lakesoul_meta.data_commit_info VALUES ({},{},{},{},{},{})",
            quote(&table.table_id),
            quote(&read.partition_desc),
            quote(id),
            c.committed,
            c.timestamp,
            quote(&serde_json::to_string(&ops)?)
        ));
    }
    sql(&mirror, &inserts[0]).await?;

    let (urls, mut workers) = spawn_workers(1).await;
    let factory = LakeSoulSessionFactory::new(source.clone(), &Default::default())?;
    let execution = factory
        .with_session_template(crate::create_lakesoul_session_config()?)
        .with_distributed(DistributedOptions::static_workers(urls))
        .create_session(&Default::default())?;
    let mut policy = PolicyConfig::default();
    policy.rolling_enabled = false;
    policy.policy_defaults.level0_file_num_limit = 2;
    let coordinator =
        CompactionCoordinator::new(mirror.clone(), DiscoveryConfig::default())
            .with_execution(
                execution,
                ExecutionConfig {
                    policy,
                    reader_options: Default::default(),
                    writer_options: Default::default(),
                    execution_timeout: Duration::from_secs(30),
                },
            );
    let mut cursor = None;
    let mut shutdown = Box::pin(std::future::pending::<()>());
    // An expired deadline must still allow the first candidate to make progress.
    assert!(
        !coordinator
            .tick(&source, Instant::now(), &mut cursor, shutdown.as_mut())
            .await?
    );
    assert_eq!(
        cursor.as_ref(),
        Some(&(read.table_id.clone(), read.partition_desc.clone()))
    );
    assert_eq!(
        source.get_all_partition_info(&table.table_id).await?,
        vec![read.clone()]
    );

    sql(&mirror, &inserts[1]).await?;
    assert!(
        !coordinator
            .tick(&source, Instant::now(), &mut cursor, shutdown.as_mut())
            .await?
    );
    let head = source
        .get_all_partition_info(&table.table_id)
        .await?
        .pop()
        .unwrap();
    assert_eq!(head.version, read.version + 1);
    assert_eq!(head.commit_op(), CommitOp::CompactionCommit);
    let files = source
        .get_data_file_infos_of_single_partition(&head)
        .await?;
    assert_eq!(files.len(), 1);
    assert!(inputs.iter().all(|f| f.path != files[0].path));
    let session = LakeSoulSessionFactory::new(source.clone(), &Default::default())?
        .create_local_session()?;
    let rows = session
        .sql(&format!("SELECT id,v FROM {name} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let mut actual = Vec::new();
    for batch in rows {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        actual
            .extend((0..batch.num_rows()).map(|row| (ids.value(row), values.value(row))));
    }
    assert_eq!(actual, [(1, 10), (2, 200), (3, 30)]);

    // The mirror still references the two original commits.
    assert!(
        !coordinator
            .tick(&source, Instant::now(), &mut cursor, shutdown.as_mut())
            .await?
    );
    assert_eq!(
        source.get_all_partition_info(&table.table_id).await?,
        vec![head]
    );
    workers.shutdown().await;
    source.drop_table(&name, "default").await?;
    Ok(())
}

async fn create_pk_source(
    name: &str,
) -> crate::Result<(Arc<MetaDataClient>, TableInfo, PartitionInfo)> {
    let source = Arc::new(MetaDataClient::from_env().await?.primary_only().await);
    create_table_with_file_format(
        source.clone(),
        name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(pk_schema())
            .with_primary_keys(vec!["id".into()])
            .with_hash_bucket_num("1")
            .build(),
        PhysicalFormat::Parquet,
    )
    .await?;
    upsert_pk(name, vec![1, 2], vec![10, 20]).await?;
    upsert_pk(name, vec![2, 3], vec![200, 30]).await?;
    let table = source
        .get_table_info_by_table_name(name, "default")
        .await?
        .unwrap();
    let read = source
        .get_all_partition_info(&table.table_id)
        .await?
        .pop()
        .unwrap();
    assert_eq!(read.snapshot.len(), 2);
    Ok((source, table, read))
}

/// Build a complete mirror snapshot for `read`, with every commit row already
/// present, so Discovery returns `Ready` and the compactors can run.
async fn mirror_full_snapshot(
    source: &MetaDataClient,
    table: &TableInfo,
    read: &PartitionInfo,
    name: &str,
) -> crate::Result<(Arc<SessionContext>, Vec<String>)> {
    let mirror = Arc::new(SessionContext::new());
    sql(&mirror, "CREATE SCHEMA lakesoul_meta").await?;
    sql(
        &mirror,
        &format!(
            "CREATE TABLE lakesoul_meta.table_info AS SELECT
        {} AS table_id, 'default' AS table_namespace, {} AS table_name, {} AS table_path,
        {} AS table_schema, CAST(NULL AS BYTEA) AS table_schema_arrow_ipc,
        {} AS table_schema_arrow_ipc_json_hash, {} AS properties, {} AS partitions,
        {} AS domain, '' AS creator",
            quote(&table.table_id),
            quote(name),
            quote(&table.table_path),
            quote(&table.table_schema),
            quote(&table.table_schema_arrow_ipc_json_hash),
            quote(&table.properties),
            quote(&table.partitions),
            quote(&table.domain)
        ),
    )
    .await?;
    let ids: Vec<_> = read
        .snapshot
        .iter()
        .map(|u| uuid::Uuid::from_u64_pair(u.high, u.low).to_string())
        .collect();
    sql(
        &mirror,
        &format!(
            "CREATE TABLE lakesoul_meta.partition_info AS SELECT
        {} AS table_id, {} AS partition_desc, CAST({} AS INT) AS version,
        {} AS commit_op, CAST({} AS BIGINT) AS timestamp, {} AS snapshot,
        {} AS expression, {} AS domain, false AS pinned",
            quote(&read.table_id),
            quote(&read.partition_desc),
            read.version,
            quote(read.commit_op().as_str_name()),
            read.timestamp,
            quote(&serde_json::to_string(&ids)?),
            quote(&read.expression),
            quote(&read.domain)
        ),
    )
    .await?;
    sql(
        &mirror,
        "CREATE TABLE lakesoul_meta.data_commit_info (table_id VARCHAR, partition_desc VARCHAR,
        commit_id VARCHAR, committed BOOLEAN, timestamp BIGINT, file_ops VARCHAR)",
    )
    .await?;
    let mut inserts = Vec::new();
    for id in &ids {
        let commit = source
            .get_single_data_commit_info(&table.table_id, &read.partition_desc, id)
            .await?
            .unwrap();
        let ops: Vec<_> = commit
            .file_ops
            .iter()
            .map(|op| {
                serde_json::json!({
                    "path": op.path,
                    "size": op.size,
                    "file_exist_cols": op.file_exist_cols,
                    "file_op": FileOp::try_from(op.file_op).unwrap().as_str_name()
                })
            })
            .collect();
        inserts.push(format!(
            "INSERT INTO lakesoul_meta.data_commit_info VALUES ({},{},{},{},{},{})",
            quote(&table.table_id),
            quote(&read.partition_desc),
            quote(id),
            commit.committed,
            commit.timestamp,
            quote(&serde_json::to_string(&ops)?)
        ));
    }
    Ok((mirror, inserts))
}

async fn gated_workers() -> (
    Vec<String>,
    Vec<Arc<std::sync::atomic::AtomicUsize>>,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinSet<()>,
) {
    let (gate, received) = tokio::sync::watch::channel(false);
    let (urls, arrived, _handles, workers) = spawn_gated_workers(1, received).await;
    (urls, arrived, gate, workers)
}

fn build_coordinator(
    source: Arc<MetaDataClient>,
    mirror: Arc<SessionContext>,
    workers: Vec<String>,
) -> crate::Result<CompactionCoordinator> {
    let factory = LakeSoulSessionFactory::new(source, &Default::default())?;
    let execution = factory
        .with_session_template(crate::create_lakesoul_session_config()?)
        .with_distributed(DistributedOptions::static_workers(workers))
        .create_session(&Default::default())?;
    let mut policy = PolicyConfig::default();
    policy.rolling_enabled = false;
    policy.policy_defaults.level0_file_num_limit = 2;
    Ok(
        CompactionCoordinator::new(mirror, DiscoveryConfig::default()).with_execution(
            execution,
            ExecutionConfig {
                policy,
                reader_options: Default::default(),
                writer_options: Default::default(),
                execution_timeout: Duration::from_secs(30),
            },
        ),
    )
}

fn count_local_parquet_files(table_path: &str) -> usize {
    fn walk(directory: &std::path::Path, count: &mut usize) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, count);
            } else if path.extension().and_then(|extension| extension.to_str())
                == Some("parquet")
            {
                *count += 1;
            }
        }
    }

    let root = url::Url::parse(table_path).unwrap().to_file_path().unwrap();
    let mut count = 0;
    walk(&root, &mut count);
    count
}

async fn read_pk_rows(
    source: Arc<MetaDataClient>,
    name: &str,
) -> crate::Result<Vec<(i32, i32)>> {
    let session = LakeSoulSessionFactory::new(source, &Default::default())?
        .create_local_session()?;
    let batches = session
        .sql(&format!("SELECT id,v FROM {name} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (ids.value(row), values.value(row))));
    }
    Ok(rows)
}

/// Run one `tick` while the gated compaction workers are parked on their first
/// input read. `concurrent` commits a source change and returns the source
/// head it observed; only then is the gate opened, so the publish sees that
/// exact change in its intervening-version window.
async fn run_tick_with_concurrent<F, Fut, T>(
    coordinator: &CompactionCoordinator,
    source: &MetaDataClient,
    arrived: &[Arc<std::sync::atomic::AtomicUsize>],
    gate: tokio::sync::watch::Sender<bool>,
    concurrent: F,
) -> crate::Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = crate::Result<T>>,
{
    let mut cursor = None;
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let change = async move {
        wait_until_all_reached(arrived, "compaction").await;
        let result = concurrent().await;
        let _ = gate.send(true);
        result
    };
    let (tick, change) = tokio::join!(
        coordinator.tick(source, Instant::now(), &mut cursor, shutdown.as_mut()),
        change,
    );
    let change = change?;
    assert!(!tick?);
    Ok(change)
}

/// A merge commit that lands after the mirrored read is not in the compaction
/// input, but publish must preserve it: head becomes `[K, merge]` and reads
/// return both the compacted rows and the concurrent write.
async fn empty_mirror() -> crate::Result<Arc<SessionContext>> {
    let mirror = Arc::new(SessionContext::new());
    sql(&mirror, "CREATE SCHEMA lakesoul_meta").await?;
    sql(
        &mirror,
        "CREATE TABLE lakesoul_meta.table_info (
            table_id VARCHAR, table_namespace VARCHAR, table_name VARCHAR)",
    )
    .await?;
    sql(
        &mirror,
        "CREATE TABLE lakesoul_meta.partition_info (
            table_id VARCHAR, partition_desc VARCHAR, version INT,
            commit_op VARCHAR, timestamp BIGINT, snapshot VARCHAR,
            expression VARCHAR, domain VARCHAR, pinned BOOLEAN)",
    )
    .await?;
    Ok(mirror)
}

fn empty_coordinator(
    mirror: Arc<SessionContext>,
) -> crate::Result<CompactionCoordinator> {
    Ok(
        CompactionCoordinator::new(mirror, DiscoveryConfig::default()).with_execution(
            Arc::new(SessionContext::new()),
            ExecutionConfig {
                policy: Default::default(),
                reader_options: Default::default(),
                writer_options: Default::default(),
                execution_timeout: Duration::from_secs(30),
            },
        ),
    )
}

/// `run` must sleep until the period expires, but shutdown has to interrupt
/// that sleep instead of waiting a whole period. The empty mirror keeps the
/// test on the scheduler loop only.
#[test]
fn run_sleeps_until_period_but_shutdown_interrupts_the_sleep() {
    run_distributed_test(async {
        let source = MetaDataClient::from_env().await?;
        let mut coordinator = empty_coordinator(empty_mirror().await?)?;

        let started = Instant::now();
        coordinator
            .run(&source, Duration::from_secs(30), async {
                tokio::time::sleep(Duration::from_millis(100)).await;
            })
            .await?;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "shutdown must interrupt the period sleep"
        );

        let started = Instant::now();
        coordinator
            .run(&source, Duration::from_secs(30), async {})
            .await?;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "ready shutdown must not wait for the period"
        );
        Ok(())
    });
}

#[test]
fn run_rejects_missing_execution_and_zero_period() {
    run_distributed_test(async {
        let source = MetaDataClient::from_env().await?;

        let mut without_execution =
            CompactionCoordinator::new(empty_mirror().await?, DiscoveryConfig::default());
        assert!(
            without_execution
                .run(
                    &source,
                    Duration::from_secs(1),
                    std::future::pending::<()>(),
                )
                .await
                .is_err()
        );

        let mut coordinator = empty_coordinator(empty_mirror().await?)?;
        assert!(
            coordinator
                .run(&source, Duration::ZERO, std::future::pending::<()>(),)
                .await
                .is_err()
        );
        Ok(())
    });
}

#[test]
fn tick_merges_concurrent_append_after_workers_start() {
    run_distributed_test(tick_merges_concurrent_append_after_workers_start_inner());
}

async fn tick_merges_concurrent_append_after_workers_start_inner() -> crate::Result<()> {
    let name = format!("tick_merge_append_{}", uuid::Uuid::new_v4().simple());
    let (source, table, read) = create_pk_source(&name).await?;
    let (mirror, inserts) = mirror_full_snapshot(&source, &table, &read, &name).await?;
    for insert in &inserts {
        sql(&mirror, insert).await?;
    }
    let (urls, arrived, gate, mut workers) = gated_workers().await;
    let coordinator = build_coordinator(source.clone(), mirror, urls)?;
    let concurrent_source = source.clone();
    let concurrent_name = name.clone();
    let concurrent_table_id = table.table_id.clone();
    let append_head = run_tick_with_concurrent(
        &coordinator,
        &source,
        &arrived,
        gate,
        move || async move {
            upsert_pk(&concurrent_name, vec![4], vec![40]).await?;
            Ok(concurrent_source
                .get_all_partition_info(&concurrent_table_id)
                .await?
                .pop()
                .unwrap())
        },
    )
    .await?;

    let head = source
        .get_all_partition_info(&table.table_id)
        .await?
        .pop()
        .unwrap();
    assert_eq!(head.version, append_head.version + 1);
    assert_eq!(head.commit_op(), CommitOp::CompactionCommit);
    assert_eq!(head.snapshot.len(), 2);
    let appended = append_head.snapshot.last().unwrap();
    assert_eq!(&head.snapshot[1], appended);
    assert_ne!(&head.snapshot[0], appended);
    assert_eq!(
        read_pk_rows(source.clone(), &name).await?,
        vec![(1, 10), (2, 200), (3, 30), (4, 40)]
    );

    workers.shutdown().await;
    source.drop_table(&name, "default").await?;
    Ok(())
}

/// Spark's compaction conflict window does not treat DeleteCommit as a
/// conflict: a delete that lands after the read leaves an empty head, and
/// publishing the compaction makes the partition observable again as `[K]`.
/// This test pins that (surprising) contract through the real tick path.
#[test]
fn tick_rebases_over_concurrent_delete_like_spark() {
    run_distributed_test(tick_rebases_over_concurrent_delete_like_spark_inner());
}

async fn tick_rebases_over_concurrent_delete_like_spark_inner() -> crate::Result<()> {
    let name = format!("tick_merge_delete_{}", uuid::Uuid::new_v4().simple());
    let (source, table, read) = create_pk_source(&name).await?;
    let (mirror, inserts) = mirror_full_snapshot(&source, &table, &read, &name).await?;
    for insert in &inserts {
        sql(&mirror, insert).await?;
    }
    let (urls, arrived, gate, mut workers) = gated_workers().await;
    let coordinator = build_coordinator(source.clone(), mirror, urls)?;
    let concurrent_source = source.clone();
    let concurrent_table = table.clone();
    let concurrent_table_id = table.table_id.clone();
    let delete_head = run_tick_with_concurrent(
        &coordinator,
        &source,
        &arrived,
        gate,
        move || async move {
            let head = concurrent_source
                .get_all_partition_info(&concurrent_table_id)
                .await?
                .pop()
                .unwrap();
            concurrent_source
                .commit_delete(
                    &concurrent_table,
                    std::slice::from_ref(&head),
                    vec![head.clone()],
                )
                .await?;
            Ok(concurrent_source
                .get_all_partition_info(&concurrent_table_id)
                .await?
                .pop()
                .unwrap())
        },
    )
    .await?;
    assert!(delete_head.snapshot.is_empty());

    let head = source
        .get_all_partition_info(&table.table_id)
        .await?
        .pop()
        .unwrap();
    assert_eq!(head.version, delete_head.version + 1);
    assert_eq!(head.commit_op(), CommitOp::CompactionCommit);
    assert_eq!(head.snapshot.len(), 1);
    assert!(!read.snapshot.contains(&head.snapshot[0]));
    assert_eq!(
        read_pk_rows(source.clone(), &name).await?,
        vec![(1, 10), (2, 200), (3, 30)]
    );

    workers.shutdown().await;
    source.drop_table(&name, "default").await?;
    Ok(())
}

/// An update in the window replaces the read set this compaction is based on;
/// publish must skip and keep the update head.
#[test]
fn tick_skips_concurrent_update_after_workers_start() {
    run_distributed_test(tick_skips_concurrent_update_after_workers_start_inner());
}

async fn tick_skips_concurrent_update_after_workers_start_inner() -> crate::Result<()> {
    let name = format!("tick_merge_update_{}", uuid::Uuid::new_v4().simple());
    let (source, table, read) = create_pk_source(&name).await?;
    let (mirror, inserts) = mirror_full_snapshot(&source, &table, &read, &name).await?;
    for insert in &inserts {
        sql(&mirror, insert).await?;
    }
    let (urls, arrived, gate, mut workers) = gated_workers().await;
    let coordinator = build_coordinator(source.clone(), mirror, urls)?;
    let concurrent_source = source.clone();
    let concurrent_name = name.clone();
    let concurrent_table_id = table.table_id.clone();
    let update_head = run_tick_with_concurrent(
        &coordinator,
        &source,
        &arrived,
        gate,
        move || async move {
            let head = concurrent_source
                .get_all_partition_info(&concurrent_table_id)
                .await?
                .pop()
                .unwrap();
            let files = concurrent_source
                .get_data_file_infos_of_single_partition(&head)
                .await?;
            concurrent_source
                .commit_update_parts(
                    &concurrent_name,
                    "default",
                    files,
                    vec![head],
                    vec![],
                )
                .await?;
            Ok(concurrent_source
                .get_all_partition_info(&concurrent_table_id)
                .await?
                .pop()
                .unwrap())
        },
    )
    .await?;

    let head = source
        .get_all_partition_info(&table.table_id)
        .await?
        .pop()
        .unwrap();
    assert_eq!(head, update_head);
    assert_eq!(head.commit_op(), CommitOp::UpdateCommit);
    assert_eq!(head.version, read.version + 1);
    assert_eq!(count_local_parquet_files(&table.table_path), 2);
    assert_eq!(
        read_pk_rows(source.clone(), &name).await?,
        vec![(1, 10), (2, 200), (3, 30)]
    );

    workers.shutdown().await;
    source.drop_table(&name, "default").await?;
    Ok(())
}
