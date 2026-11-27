// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for optimistic commit concurrency (OCC).
//!
//! They require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and use unique table names.

use std::collections::HashSet;
use std::sync::Arc;

use lakesoul_metadata::error::Result;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata::{
    CommitOutcome, LakeSoulMetaDataError, MetaDataClient, PRIMARY_URL_ENV_KEY,
    PRIMARY_URL_PROP_KEY, PooledClient, QueryType, pg_config_from_env,
};
use lakesoul_metadata_proto::entity::{
    CommitOp, DataCommitInfo, MetaInfo, PartitionInfo, TableInfo,
};
use tokio::sync::Barrier;

const NAMESPACE: &str = "default";

fn unique_name(tag: &str) -> String {
    format!("occ_{tag}_{}", uuid::Uuid::new_v4().simple())
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

fn file(partition_desc: &str, name: &str) -> DataFileInfo {
    DataFileInfo {
        partition_desc: partition_desc.to_string(),
        path: format!("/tmp/{partition_desc}/{name}.parquet"),
        file_op: "add".to_string(),
        size: 1,
        file_exist_cols: "id".to_string(),
        ..Default::default()
    }
}

fn new_uuid() -> ((u64, u64), lakesoul_metadata_proto::entity::Uuid) {
    let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
    (
        (high, low),
        lakesoul_metadata_proto::entity::Uuid { high, low },
    )
}

fn snapshot_ids(partition_info: &PartitionInfo) -> HashSet<(u64, u64)> {
    partition_info
        .snapshot
        .iter()
        .map(|id| (id.high, id.low))
        .collect()
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
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(format!(
                "partition {partition_desc} not found"
            ))
        })
}

async fn commit_with_read(
    client: &MetaDataClient,
    table_id: &str,
    partition_desc: &str,
    commit_op: CommitOp,
    snapshot: Vec<lakesoul_metadata_proto::entity::Uuid>,
    read: Option<&PartitionInfo>,
) -> Result<CommitOutcome> {
    let table_info = client
        .get_table_info_by_table_id(table_id)
        .await?
        .ok_or_else(|| {
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(format!(
                "table {table_id} not found"
            ))
        })?;
    // Real flows insert pending data commits before publication; publication
    // marks them committed in the same transaction. A replay arrives with
    // commit ids whose rows already exist, so staging skips them.
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    for commit_id in &snapshot {
        let commit_id_str =
            uuid::Uuid::from_u64_pair(commit_id.high, commit_id.low).to_string();
        if client
            .get_single_data_commit_info(table_id, partition_desc, &commit_id_str)
            .await?
            .is_none()
        {
            client
                .insert_data_commit_info(&DataCommitInfo {
                    table_id: table_id.to_string(),
                    partition_desc: partition_desc.to_string(),
                    pinned: false,
                    commit_id: Some(*commit_id),
                    file_ops: Vec::new(),
                    commit_op: commit_op.into(),
                    timestamp,
                    committed: false,
                    domain: "public".to_string(),
                })
                .await?;
        }
    }
    client
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: table_id.to_string(),
                    partition_desc: partition_desc.to_string(),
                    snapshot,
                    ..Default::default()
                }],
                read_partition_info: read.cloned().into_iter().collect(),
            },
            commit_op,
        )
        .await
}

#[tokio::test]
async fn concurrent_appends_keep_every_commit() -> Result<()> {
    let first = Arc::new(MetaDataClient::from_env().await?);
    let second = Arc::new(MetaDataClient::from_env().await?);
    let name = unique_name("appends");
    let table_id = create_test_table(&first, &name).await?;

    // With `MAX_COMMIT_ATTEMPTS = 5`, a writer loses at most `writers - 1`
    // times, so four concurrent writers always fit into the retry budget.
    let writers = 4;
    let barrier = Arc::new(Barrier::new(writers));
    let mut tasks = Vec::new();
    for index in 0..writers {
        let client = if index % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let name = name.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .commit_data_files(
                    &name,
                    NAMESPACE,
                    vec![file("p1", &format!("f{index}"))],
                )
                .await
        }));
    }
    for task in tasks {
        task.await.expect("writer panicked")?;
    }

    // Every writer must have published a version; a lost update would show up
    // as a lower version or a snapshot missing one file.
    let latest = latest_partition_info(&first, &table_id, "p1").await?;
    assert_eq!(latest.version, writers as i32 - 1);
    assert_eq!(latest.snapshot.len(), writers);

    let files = first
        .get_data_files_of_single_partition(&latest)
        .await?
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(files.len(), writers);
    for index in 0..writers {
        assert!(
            files.contains(&format!("/tmp/p1/f{index}.parquet")),
            "file f{index} was lost, active files: {files:?}"
        );
    }

    first.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn compaction_merges_concurrent_append() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("compact_merge");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let read = latest_partition_info(&client, &table_id, "p1").await?;

    // A concurrent append lands after the compaction read its snapshot.
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f2")])
        .await?;
    let after_append = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(after_append.version, read.version + 1);

    let concurrent_ids = snapshot_ids(&after_append)
        .difference(&snapshot_ids(&read))
        .copied()
        .collect::<HashSet<_>>();
    assert_eq!(concurrent_ids.len(), 1);

    let (compaction_id, compaction_uuid) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::CompactionCommit,
        vec![compaction_uuid],
        Some(&read),
    )
    .await?;

    // snapshot = compaction output + concurrent append, no lost file
    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, after_append.version + 1);
    assert_eq!(latest.commit_op(), CommitOp::CompactionCommit);
    let expected = concurrent_ids
        .into_iter()
        .chain(std::iter::once(compaction_id))
        .collect::<HashSet<_>>();
    assert_eq!(snapshot_ids(&latest), expected);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn merge_rebases_over_compatible_commits() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("merge_rebase");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "base")])
        .await?;
    let read = latest_partition_info(&client, &table_id, "p1").await?;

    // A plain append lands after the upsert read its snapshot.
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "other")])
        .await?;
    let after_append = latest_partition_info(&client, &table_id, "p1").await?;

    let (merge_id, merge_uuid) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::MergeCommit,
        vec![merge_uuid],
        Some(&read),
    )
    .await?;

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, after_append.version + 1);
    assert_eq!(latest.commit_op(), CommitOp::MergeCommit);
    // A merge extends the whole current head (base + concurrent append) with
    // its own commit: unlike an update it does not drop the read snapshot.
    let expected = snapshot_ids(&after_append)
        .into_iter()
        .chain(std::iter::once(merge_id))
        .collect::<HashSet<_>>();
    assert_eq!(snapshot_ids(&latest), expected);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn merge_conflicts_with_concurrent_update_or_delete() -> Result<()> {
    let client = MetaDataClient::from_env().await?;

    for rewrite_op in [CommitOp::UpdateCommit, CommitOp::DeleteCommit] {
        let name = unique_name("merge_conflict");
        let table_id = create_test_table(&client, &name).await?;

        client
            .commit_data_files(&name, NAMESPACE, vec![file("p1", "base")])
            .await?;
        let read = latest_partition_info(&client, &table_id, "p1").await?;

        // A rewrite the upsert's rows were not computed against lands first.
        commit_with_read(
            &client,
            &table_id,
            "p1",
            rewrite_op,
            vec![new_uuid().1],
            Some(&read),
        )
        .await?;

        // The upsert publication path rejects a rewrite it was not computed
        // against; other MergeCommit callers are allowed to rebase over it.
        let table_info = client.get_table_info_by_table_id(&table_id).await?.unwrap();
        let result = client
            .commit_merge_with_retry(
                &table_info,
                &[read.clone()],
                vec![file("p1", "upsert")],
            )
            .await;
        assert!(
            result.is_err(),
            "upsert merge must not rebase over a concurrent {rewrite_op:?}"
        );

        client.drop_table(&name, NAMESPACE).await?;
    }
    Ok(())
}

#[tokio::test]
async fn update_over_concurrent_append_merges() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("update_merge");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let read = latest_partition_info(&client, &table_id, "p1").await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f2")])
        .await?;
    let after_append = latest_partition_info(&client, &table_id, "p1").await?;
    let concurrent_ids = snapshot_ids(&after_append)
        .difference(&snapshot_ids(&read))
        .copied()
        .collect::<HashSet<_>>();

    let (update_id, update_uuid) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![update_uuid],
        Some(&read),
    )
    .await?;

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, after_append.version + 1);
    assert_eq!(latest.commit_op(), CommitOp::UpdateCommit);
    let expected = concurrent_ids
        .into_iter()
        .chain(std::iter::once(update_id))
        .collect::<HashSet<_>>();
    assert_eq!(snapshot_ids(&latest), expected);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn stale_update_conflicts_with_concurrent_update() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("update_conflict");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    let (_, first_update) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![first_update],
        Some(&v0),
    )
    .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;

    let (_, second_update) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![second_update],
        Some(&v1),
    )
    .await?;

    // a third update still holding v1 must be rejected, not silently dropped
    let (_, third_update) = new_uuid();
    let error = commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![third_update],
        Some(&v1),
    )
    .await
    .expect_err("stale update must conflict");
    assert!(
        error
            .to_string()
            .contains("conflicts with concurrent writes"),
        "unexpected error: {error}"
    );

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v1.version + 1);
    assert!(!snapshot_ids(&latest).contains(&(third_update.high, third_update.low)));

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn stale_compaction_is_skipped_when_update_landed() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("compact_skip");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    let (_, update) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![update],
        Some(&v0),
    )
    .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;

    // the compaction read v0 while an update already owns the partition: its
    // partition must be dropped from the commit, not merged on top
    let (_, stale_compaction) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::CompactionCommit,
        vec![stale_compaction],
        Some(&v0),
    )
    .await?;

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v1.version);
    assert_eq!(snapshot_ids(&latest), snapshot_ids(&v1));

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn stale_update_conflicts_with_compaction_carrying_appends() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("compact_conflict");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    // a compaction whose snapshot kept a concurrent append (snapshot[1..])
    let (_, compaction_output) = new_uuid();
    let (_, concurrent_append) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::CompactionCommit,
        vec![compaction_output, concurrent_append],
        Some(&v0),
    )
    .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(v1.snapshot.len(), 2);

    // an update based on v0 cannot be merged across such a compaction
    let (_, update) = new_uuid();
    let error = commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![update],
        Some(&v0),
    )
    .await
    .expect_err("update over compaction with appends must conflict");
    assert!(
        error.to_string().contains("conflicts with a compaction"),
        "unexpected error: {error}"
    );

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v1.version);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

async fn insert_only_metadata_state(table_id: &str) -> Result<Vec<String>> {
    use lakesoul_metadata::{
        PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY, PooledClient, QueryType,
        pg_config_from_env,
    };
    let pool = PooledClient::try_new(
        pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?,
        None,
    )
    .await?;
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
        .map(|row| row.get(0))
        .collect())
}

#[tokio::test]
async fn insert_only_commits_are_atomic_and_do_not_rebase_stale_reads() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("insert_only");
    let table_id = create_test_table(&client, &name).await?;
    let table = client.get_table_info_by_table_id(&table_id).await?.unwrap();
    let initial_ids = client
        .commit_insert_only(&table, &[], vec![file("p1", "initial")])
        .await?;
    let initial = client.get_all_partition_info(&table_id).await?;
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0].version, 0);
    let commit = client
        .get_single_data_commit_info(&table_id, "p1", &initial_ids[0])
        .await?;
    assert!(commit.unwrap().committed);

    // A legacy writer publishes after the INSERT's key scan snapshot.
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "concurrent")])
        .await?;
    let before = insert_only_metadata_state(&table_id).await?;
    let error = client
        .commit_insert_only(
            &table,
            &initial,
            // a_new sorts first, so its tentative rows must roll back too.
            vec![file("a_new", "must_roll_back"), file("p1", "stale")],
        )
        .await
        .expect_err("stale INSERT must not rebase across another writer");
    assert!(matches!(
        error,
        lakesoul_metadata::LakeSoulMetaDataError::InsertConflict { .. }
    ));
    assert_eq!(insert_only_metadata_state(&table_id).await?, before);
    let current = client.get_all_partition_info(&table_id).await?;
    client
        .commit_insert_only(
            &table,
            &current,
            vec![file("a_new", "valid_a"), file("p1", "valid_p1")],
        )
        .await?;
    let latest = client.get_all_partition_info(&table_id).await?;
    assert_eq!(latest.len(), 2);
    assert_eq!(
        latest
            .iter()
            .find(|p| p.partition_desc == "p1")
            .unwrap()
            .snapshot
            .len(),
        3
    );
    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn competing_insert_only_initial_commits_have_one_winner() -> Result<()> {
    let first = Arc::new(MetaDataClient::from_env().await?);
    let second = Arc::new(MetaDataClient::from_env().await?);
    let name = unique_name("insert_race");
    let table_id = create_test_table(&first, &name).await?;
    let table = first.get_table_info_by_table_id(&table_id).await?.unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let mut tasks = Vec::new();
    for (index, client) in [first.clone(), second].into_iter().enumerate() {
        let barrier = barrier.clone();
        let table = table.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client
                .commit_insert_only(
                    &table,
                    &[],
                    vec![file("p1", &format!("candidate_{index}"))],
                )
                .await
        }));
    }
    let mut successes = 0;
    for task in tasks {
        if task.await.unwrap().is_ok() {
            successes += 1;
        }
    }
    assert_eq!(successes, 1, "concurrent INSERTs must not silently merge");
    let latest = latest_partition_info(&first, &table_id, "p1").await?;
    assert_eq!(latest.version, 0);
    assert_eq!(latest.snapshot.len(), 1);
    let state = insert_only_metadata_state(&table_id).await?;
    assert_eq!(
        state.iter().filter(|row| row.starts_with("data:")).count(),
        1
    );
    first.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn replayed_commit_is_idempotent() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("replay");
    let table_id = create_test_table(&client, &name).await?;

    let (_, commit_uuid) = new_uuid();
    let commit_id_str =
        uuid::Uuid::from_u64_pair(commit_uuid.high, commit_uuid.low).to_string();
    assert_eq!(
        commit_with_read(
            &client,
            &table_id,
            "p1",
            CommitOp::AppendCommit,
            vec![commit_uuid],
            None,
        )
        .await?,
        CommitOutcome::Committed
    );
    let head = latest_partition_info(&client, &table_id, "p1").await?;
    assert!(
        client
            .get_single_data_commit_info(&table_id, "p1", &commit_id_str)
            .await?
            .expect("staged commit row")
            .committed
    );
    // Replaying the same append must not mint a second version: every staged
    // data commit is already published.
    assert_eq!(
        commit_with_read(
            &client,
            &table_id,
            "p1",
            CommitOp::AppendCommit,
            vec![commit_uuid],
            None,
        )
        .await?,
        CommitOutcome::AlreadyCommitted
    );
    let after_replay = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(after_replay.version, head.version);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn multi_partition_publication_marks_every_commit() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("multi_pub");
    let table_id = create_test_table(&client, &name).await?;

    let mut commits = Vec::new();
    for partition_desc in ["p1", "p2"] {
        let (_, commit_uuid) = new_uuid();
        client
            .insert_data_commit_info(&DataCommitInfo {
                table_id: table_id.to_string(),
                partition_desc: partition_desc.to_string(),
                pinned: false,
                commit_id: Some(commit_uuid),
                file_ops: Vec::new(),
                commit_op: CommitOp::AppendCommit.into(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
                committed: false,
                domain: "public".to_string(),
            })
            .await?;
        let commit_id_str =
            uuid::Uuid::from_u64_pair(commit_uuid.high, commit_uuid.low).to_string();
        commits.push((partition_desc, commit_id_str, commit_uuid));
    }

    let table_info = client
        .get_table_info_by_table_id(&table_id)
        .await?
        .ok_or_else(|| {
            lakesoul_metadata::LakeSoulMetaDataError::NotFound(format!(
                "table {table_id} not found"
            ))
        })?;
    // Both partitions are published in one call, one transaction.
    assert_eq!(
        client
            .commit_data(
                MetaInfo {
                    table_info: Some(table_info),
                    list_partition: commits
                        .iter()
                        .map(|(partition_desc, _, commit_uuid)| PartitionInfo {
                            table_id: table_id.clone(),
                            partition_desc: partition_desc.to_string(),
                            snapshot: vec![*commit_uuid],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                CommitOp::AppendCommit,
            )
            .await?,
        CommitOutcome::Committed
    );

    for (partition_desc, commit_id, commit_uuid) in &commits {
        let head = latest_partition_info(&client, &table_id, partition_desc).await?;
        assert_eq!(head.version, 0);
        assert!(snapshot_ids(&head).contains(&(commit_uuid.high, commit_uuid.low)));
        assert!(
            client
                .get_single_data_commit_info(
                    &table_id,
                    partition_desc,
                    &commit_id.to_string()
                )
                .await?
                .expect("staged commit row")
                .committed
        );
    }

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn multi_partition_append_shares_one_commit_id() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("shared_id");
    let table_id = create_test_table(&client, &name).await?;

    // Flink and Spark streaming stage every partition entry of a batch with
    // the same commit id; the publication must scope marks per partition.
    let ((high, low), commit_id) = new_uuid();
    for partition_desc in ["p1", "p2"] {
        client
            .insert_data_commit_info(&DataCommitInfo {
                table_id: table_id.clone(),
                partition_desc: partition_desc.to_string(),
                pinned: false,
                commit_id: Some(commit_id),
                file_ops: Vec::new(),
                commit_op: CommitOp::AppendCommit.into(),
                timestamp: 1_000,
                committed: false,
                domain: "public".to_string(),
            })
            .await?;
    }

    let table_info = client
        .get_table_info_by_table_id(&table_id)
        .await?
        .expect("table exists");
    let outcome = client
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![
                    PartitionInfo {
                        table_id: table_id.clone(),
                        partition_desc: "p1".to_string(),
                        snapshot: vec![commit_id],
                        ..Default::default()
                    },
                    PartitionInfo {
                        table_id: table_id.clone(),
                        partition_desc: "p2".to_string(),
                        snapshot: vec![commit_id],
                        ..Default::default()
                    },
                ],
                read_partition_info: Vec::new(),
            },
            CommitOp::AppendCommit,
        )
        .await?;
    assert_eq!(outcome, CommitOutcome::Committed);

    for partition_desc in ["p1", "p2"] {
        let latest = latest_partition_info(&client, &table_id, partition_desc).await?;
        assert_eq!(latest.version, 0);
        assert!(snapshot_ids(&latest).contains(&(high, low)));
        let commit = client
            .get_single_data_commit_info(
                &table_id,
                partition_desc,
                &uuid::Uuid::from_u64_pair(high, low).to_string(),
            )
            .await?
            .expect("shared-id data commit must be published");
        assert!(commit.committed);
    }

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn stale_update_reading_version_zero_conflicts() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("v0_conflict");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    let (_, first_update) = new_uuid();
    commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![first_update],
        Some(&v0),
    )
    .await?;
    let v1 = latest_partition_info(&client, &table_id, "p1").await?;

    // A second update that still read v0 must be rejected like any other
    // update-versus-update race, not merged into a double rewrite.
    let (_, second_update) = new_uuid();
    let error = commit_with_read(
        &client,
        &table_id,
        "p1",
        CommitOp::UpdateCommit,
        vec![second_update],
        Some(&v0),
    )
    .await
    .expect_err("update race at version zero must conflict");
    assert!(
        error
            .to_string()
            .contains("conflicts with concurrent writes"),
        "unexpected error: {error}"
    );

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v1.version);
    assert!(!snapshot_ids(&latest).contains(&(second_update.high, second_update.low)));

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn update_reassert_on_current_head_mints() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("reassert");
    let table_id = create_test_table(&client, &name).await?;

    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "f1")])
        .await?;
    let v0 = latest_partition_info(&client, &table_id, "p1").await?;

    // A rewrite re-asserting the ids the current head already holds reads the
    // current version on purpose and must mint a new version, not be treated
    // as a replay.
    let table_info = client
        .get_table_info_by_table_id(&table_id)
        .await?
        .expect("table exists");
    let outcome = client
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: table_id.clone(),
                    partition_desc: "p1".to_string(),
                    commit_op: CommitOp::UpdateCommit.into(),
                    snapshot: v0.snapshot.clone(),
                    ..Default::default()
                }],
                read_partition_info: vec![PartitionInfo {
                    table_id: table_id.clone(),
                    partition_desc: "p1".to_string(),
                    version: v0.version,
                    ..Default::default()
                }],
            },
            CommitOp::UpdateCommit,
        )
        .await?;
    assert_eq!(outcome, CommitOutcome::Committed);

    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, v0.version + 1);
    assert_eq!(latest.commit_op(), CommitOp::UpdateCommit);
    assert_eq!(snapshot_ids(&latest), snapshot_ids(&v0));

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn table_without_name_row_commits() -> Result<()> {
    // Tables created by path (`DBManager.createTable` with a blank name) have
    // no `table_name_id` row; the commit domain comes from `table_info`.
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("path_only");
    let table_id = create_test_table(&client, &name).await?;
    client.delete_table_name_id_by_table_id(&table_id).await?;
    let table = client.get_table_info_by_table_id(&table_id).await?.unwrap();

    let (_, commit_uuid) = new_uuid();
    assert_eq!(
        commit_with_read(
            &client,
            &table_id,
            "p1",
            CommitOp::AppendCommit,
            vec![commit_uuid],
            None,
        )
        .await?,
        CommitOutcome::Committed
    );
    let head = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(head.domain, table.domain);

    client
        .commit_insert_only(
            &table,
            std::slice::from_ref(&head),
            vec![file("p1", "insert")],
        )
        .await?;
    let latest = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(latest.version, head.version + 1);
    assert_eq!(latest.snapshot.len(), 2);
    assert_eq!(latest.domain, table.domain);

    client.delete_table_by_table_info_cascade(&table).await?;
    Ok(())
}

/// Staged and published `data_commit_info` rows carry their table's domain,
/// never the caller's string, so the RBAC row-level security policies keep
/// every commit visible to the owning domain's roles.
///
/// Both staging paths are covered: the batch publication the pg-wire writer
/// uses (which leaves the proto default empty domain) and the single-row
/// insert (whose callers have hard-coded `public` in the past).
#[tokio::test]
async fn data_commit_info_stored_with_table_domain() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let name = unique_name("commit_domain");
    let domain = format!("occ_domain_{}", uuid::Uuid::new_v4().simple());
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: name.clone(),
            table_namespace: NAMESPACE.to_string(),
            table_path: format!("file:///tmp/{name}"),
            table_schema: "[]".to_string(),
            properties: "{}".to_string(),
            partitions: ";".to_string(),
            domain: domain.clone(),
            ..Default::default()
        })
        .await?;

    // The batch path: the caller leaves the proto default empty domain.
    let (_, batch_commit) = new_uuid();
    client
        .commit_data_commit_infos(vec![DataCommitInfo {
            table_id: table_id.clone(),
            partition_desc: "p1".to_string(),
            pinned: false,
            commit_id: Some(batch_commit),
            file_ops: Vec::new(),
            commit_op: CommitOp::AppendCommit.into(),
            timestamp: 1_000,
            committed: false,
            domain: String::new(),
        }])
        .await?;
    let batch_commit_str =
        uuid::Uuid::from_u64_pair(batch_commit.high, batch_commit.low).to_string();
    let staged = client
        .get_single_data_commit_info(&table_id, "p1", &batch_commit_str)
        .await?
        .expect("batch commit row missing");
    assert_eq!(staged.domain, domain);
    let head = latest_partition_info(&client, &table_id, "p1").await?;
    assert_eq!(head.domain, domain);

    // The single-row path: a wrong value must not reach the database either.
    let (_, single_commit) = new_uuid();
    client
        .insert_data_commit_info(&DataCommitInfo {
            table_id: table_id.clone(),
            partition_desc: "p2".to_string(),
            pinned: false,
            commit_id: Some(single_commit),
            file_ops: Vec::new(),
            commit_op: CommitOp::AppendCommit.into(),
            timestamp: 1_000,
            committed: false,
            domain: "public".to_string(),
        })
        .await?;
    let single_commit_str =
        uuid::Uuid::from_u64_pair(single_commit.high, single_commit.low).to_string();
    let staged = client
        .get_single_data_commit_info(&table_id, "p2", &single_commit_str)
        .await?
        .expect("single commit row missing");
    assert_eq!(staged.domain, domain);

    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

fn staged_commit(table_id: &str, partition_desc: &str) -> DataCommitInfo {
    let (_, commit_id) = new_uuid();
    DataCommitInfo {
        table_id: table_id.to_string(),
        partition_desc: partition_desc.to_string(),
        pinned: false,
        commit_id: Some(commit_id),
        file_ops: Vec::new(),
        commit_op: CommitOp::AppendCommit.into(),
        timestamp: 1_000,
        committed: false,
        domain: "public".to_string(),
    }
}

async fn pool() -> Result<PooledClient> {
    PooledClient::try_new(
        pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?,
        None,
    )
    .await
}

async fn named_client(application: &str) -> Result<MetaDataClient> {
    let config = pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?;
    MetaDataClient::from_config(format!("{config} application_name={application}"), None)
        .await
}

/// Every metadata row for `table_id`, including uncommitted data commits.
/// An empty result means the id owns nothing.
async fn metadata_state(pool: &PooledClient, table_id: &str) -> Result<Vec<String>> {
    let table_id = table_id.to_string();
    Ok(pool
        .query(
            "SELECT 'data:' || row_to_json(d)::text AS state \
             FROM data_commit_info d WHERE table_id = $1
             UNION ALL
             SELECT 'partition:' || row_to_json(p)::text \
             FROM partition_info p WHERE table_id = $1
             UNION ALL
             SELECT 'info:' || row_to_json(t)::text \
             FROM table_info t WHERE table_id = $1
             UNION ALL
             SELECT 'name:' || row_to_json(n)::text \
             FROM table_name_id n WHERE table_id = $1
             UNION ALL
             SELECT 'path:' || row_to_json(p)::text \
             FROM table_path_id p WHERE table_id = $1
             ORDER BY state",
            QueryType::RW,
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

fn assert_missing_table(error: LakeSoulMetaDataError, table_id: &str) {
    match error {
        LakeSoulMetaDataError::NotFound(message) => {
            assert_eq!(
                message,
                format!("table {table_id} no longer exists"),
                "staging must report the absent table_id"
            );
        }
        other => panic!("expected NotFound, got {other}"),
    }
}

/// Observe an actual blocked backend, rather than guessing when it reached
/// its lock. The returned blockers identify the lock owner.
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

async fn finish<T>(task: tokio::task::JoinHandle<Result<T>>, what: &str) -> Result<T> {
    tokio::time::timeout(std::time::Duration::from_secs(30), task)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish after the gate opened"))
        .unwrap_or_else(|_| panic!("{what} task panicked"))
}

/// DROP holds `table_info` FOR UPDATE and is blocked on `table_path_id`.
/// Staging must wait on that DROP, then fail without inserting a row for the
/// dropped id. A same-name replacement keeps only its own commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_blocks_single_record_staging_and_leaves_no_orphan() -> Result<()> {
    let name = unique_name("stage_drop");
    let stage_application = format!("stage_{}", uuid::Uuid::new_v4().simple());
    let drop_application = format!("drop_{}", uuid::Uuid::new_v4().simple());
    let stager = named_client(&stage_application).await?;
    let dropper = named_client(&drop_application).await?;
    let pool = pool().await?;
    let table_id = create_test_table(&stager, &name).await?;

    let mut connection = pool.get(QueryType::RW).await?;
    let gate = connection.transaction().await?;
    assert!(
        gate.query_opt(
            "select table_id from table_path_id where table_id = $1::TEXT for update",
            &[&table_id],
        )
        .await?
        .is_some(),
        "the gate must lock the original table_path_id row"
    );
    let gate_pid: i32 = gate.query_one("select pg_backend_pid()", &[]).await?.get(0);

    let drop_name = name.clone();
    let drop_task =
        tokio::spawn(async move { dropper.drop_table(&drop_name, NAMESPACE).await });
    let (drop_pid, drop_blockers) = blocked_backend(&pool, &drop_application).await?;
    assert!(
        drop_blockers.contains(&gate_pid),
        "DROP must hold table_info FOR UPDATE and wait on table_path_id: \
         {drop_blockers:?}"
    );

    let stage_table_id = table_id.clone();
    let stage_client = stager.clone();
    let stage_task = tokio::spawn(async move {
        stage_client
            .insert_data_commit_info(&staged_commit(&stage_table_id, "p1"))
            .await
    });
    let (_, stage_blockers) = blocked_backend(&pool, &stage_application).await?;
    assert!(
        stage_blockers.contains(&drop_pid),
        "staging must wait on DROP's table_info lock, not insert an orphan: \
         {stage_blockers:?}"
    );

    gate.commit().await?;
    finish(drop_task, "DROP").await?;
    let staged = finish(stage_task, "staging").await;
    assert_missing_table(
        staged.expect_err("staging must not insert after DROP"),
        &table_id,
    );

    assert_eq!(
        metadata_state(&pool, &table_id).await?,
        Vec::<String>::new(),
        "the dropped table_id must own no metadata, including uncommitted commits"
    );
    let replacement_id = create_test_table(&stager, &name).await?;
    assert_ne!(replacement_id, table_id);
    stager
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "replacement")])
        .await?;
    let replacement_rows = metadata_state(&pool, &replacement_id).await?;
    assert!(
        replacement_rows.iter().any(|row| row.starts_with("data:")),
        "the replacement must have its own commit"
    );
    assert_eq!(
        metadata_state(&pool, &table_id).await?,
        Vec::<String>::new(),
        "the replacement commit must not land on the dropped table_id"
    );
    assert_eq!(
        metadata_state(&pool, &replacement_id).await?,
        replacement_rows,
        "the replacement table must be untouched"
    );
    stager.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

/// Staging holds `table_info` FOR SHARE and is blocked on its insert.
/// DROP must wait for that lock before deleting, then remove the staged row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_record_staging_blocks_drop_until_the_row_is_removed() -> Result<()> {
    let name = unique_name("stage_holds");
    let stage_application = format!("stage_{}", uuid::Uuid::new_v4().simple());
    let drop_application = format!("drop_{}", uuid::Uuid::new_v4().simple());
    let stager = named_client(&stage_application).await?;
    let dropper = named_client(&drop_application).await?;
    let pool = pool().await?;
    let table_id = create_test_table(&stager, &name).await?;
    let before = metadata_state(&pool, &table_id).await?;

    let mut connection = pool.get(QueryType::RW).await?;
    let gate = connection.transaction().await?;
    gate.batch_execute("lock table data_commit_info in share mode")
        .await?;

    let stage_table_id = table_id.clone();
    let stage_task = tokio::spawn(async move {
        stager
            .insert_data_commit_info(&staged_commit(&stage_table_id, "p1"))
            .await
    });
    let (stage_pid, _) = blocked_backend(&pool, &stage_application).await?;
    let drop_name = name.clone();
    let drop_task =
        tokio::spawn(async move { dropper.drop_table(&drop_name, NAMESPACE).await });
    let (_, drop_blockers) = blocked_backend(&pool, &drop_application).await?;
    assert!(
        drop_blockers.contains(&stage_pid),
        "DROP must wait for staging's table_info FOR SHARE before deleting: \
         {drop_blockers:?}"
    );
    assert_eq!(
        metadata_state(&pool, &table_id).await?,
        before,
        "DROP must not delete metadata while staging holds the parent row"
    );

    gate.commit().await?;
    finish(stage_task, "staging").await?;
    finish(drop_task, "DROP").await?;
    assert_eq!(
        metadata_state(&pool, &table_id).await?,
        Vec::<String>::new(),
        "DROP must remove the staged row and every other metadata row"
    );
    Ok(())
}

#[tokio::test]
async fn insert_data_commit_info_rejects_a_replaced_table_id() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let pool = pool().await?;
    let name = unique_name("replaced_id");
    let old_table_id = create_test_table(&client, &name).await?;
    client.drop_table(&name, NAMESPACE).await?;

    let new_table_id = create_test_table(&client, &name).await?;
    assert_ne!(new_table_id, old_table_id);
    client
        .commit_data_files(&name, NAMESPACE, vec![file("p1", "published")])
        .await?;
    client
        .insert_data_commit_info(&staged_commit(&new_table_id, "p2"))
        .await?;
    let replacement_rows = metadata_state(&pool, &new_table_id).await?;
    assert!(
        replacement_rows.iter().any(|row| row.starts_with("data:")),
        "the replacement must have staged and published rows"
    );

    let error = client
        .insert_data_commit_info(&staged_commit(&old_table_id, "p1"))
        .await
        .expect_err("the old table_id must not stage a commit");
    assert_missing_table(error, &old_table_id);
    assert_eq!(
        metadata_state(&pool, &old_table_id).await?,
        Vec::<String>::new(),
        "the old table_id must gain no data_commit_info row"
    );
    assert_eq!(
        metadata_state(&pool, &new_table_id).await?,
        replacement_rows,
        "the replacement table must be untouched"
    );
    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}

#[tokio::test]
async fn insert_data_commit_info_missing_table_is_not_found() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let pool = pool().await?;
    let table_id = format!("table_{}", uuid::Uuid::new_v4());
    let error = client
        .insert_data_commit_info(&staged_commit(&table_id, "p1"))
        .await
        .expect_err("a missing table_id must not stage a commit");
    assert_missing_table(error, &table_id);
    assert_eq!(
        metadata_state(&pool, &table_id).await?,
        Vec::<String>::new(),
        "the rolled-back insert must not leave a data_commit_info row"
    );
    Ok(())
}

#[tokio::test]
async fn insert_data_commit_info_duplicate_key_propagates_postgres_error() -> Result<()> {
    let client = MetaDataClient::from_env().await?;
    let pool = pool().await?;
    let name = unique_name("dup_stage");
    let table_id = create_test_table(&client, &name).await?;
    let commit = staged_commit(&table_id, "p1");
    client.insert_data_commit_info(&commit).await?;
    let before = metadata_state(&pool, &table_id).await?;

    let error = client
        .insert_data_commit_info(&commit)
        .await
        .expect_err("a duplicate primary key must fail");
    match &error {
        LakeSoulMetaDataError::PostgresError(postgres) => {
            assert_eq!(
                postgres.as_db_error().map(|db| db.code().code()),
                Some("23505"),
                "the duplicate must roll back and propagate the postgres error, \
                 got {error}"
            );
        }
        other => panic!("expected a postgres unique violation, got {other}"),
    }
    assert_eq!(
        metadata_state(&pool, &table_id).await?,
        before,
        "the duplicate insert must roll back and leave the first row"
    );
    client.drop_table(&name, NAMESPACE).await?;
    Ok(())
}
