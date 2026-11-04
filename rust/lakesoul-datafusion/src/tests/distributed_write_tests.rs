// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Distributed `INSERT ... SELECT` over real LakeSoul tables.
//!
//! Every test boots in-process gRPC workers sharing the coordinator's local
//! object store, inspects the statement's physical plan, and executes that
//! same plan, so the statement id asserted from the file names is the one
//! that wrote them. The assertions are consumer-visible:
//!
//! - The plan is a [`DistributedExec`] with the `LakeSoulCommitExec` in the
//!   coordinator's head stage and the `LakeSoulWriteExec` in a worker stage.
//! - The `INSERT` result's `count` column (rows affected) equals the rows the
//!   statement wrote, and a single-node read returns exactly the expected
//!   rows.
//! - File names are `part-{write_id}T{task_index}_{bucket:04}.parquet` for a
//!   multi-task write stage, all files share one statement id, several task
//!   indices are present, and a one-task statement stamps no task index.
//!   Files on disk are exactly the files the metadata names.
//! - A fresh destination per statement makes its `data_commit_info` history
//!   that statement's: one committed commit row and one partition version per
//!   touched range partition, no uncommitted row, no orphan file.
//! - Grouped and global aggregates and a global `ORDER BY ... LIMIT` keep
//!   their exact result through the write.
//! - A worker-side upload fault after other partitions uploaded fails the
//!   statement and leaves no commit, version, active file, or data file.
//! - Dropping the statement mid-upload (healthy workers, gated first upload)
//!   publishes nothing and leaves no data file; the workers are observed to
//!   drain before the assertions.
//! - Keyed `INSERT` validates on the workers against the broadcast existing
//!   key scan: duplicates are typed `23505` rejections that publish nothing,
//!   `Skip` drops only conflicting rows, keys are partition-local, `INSERT OR
//!   REPLACE` merges across statements, and a commit landing between the
//!   plan-time snapshot and publication yields the retryable `40001` instead
//!   of a silent rebase.
//!
//! Everything above uses real barriers — an object-store gate or fault — not
//! sleeps racing the writers.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{ExecutionPlan, collect, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion_distributed::grpc::{GetTaskProgressRequest, ObservabilityServiceClient};
use datafusion_distributed::{
    DistributedExec, NetworkBoundaryExt, Stage, display_plan_ascii,
};
use futures::StreamExt;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_metadata::{
    MetaDataClient, MetaDataClientRef, PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY,
    PooledClient, QueryType, pg_config_from_env,
};
use rootcause::report;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;

use crate::Result;
use crate::cli::CoreArgs;
use crate::datasource::file_format::{LakeSoulCommitExec, LakeSoulWriteExec};
use crate::distributed::{LakeSoulWorkerOptions, StaticWorkerResolver};
use crate::lakesoul_table::LakeSoulTable;
use crate::session::{
    ExecutionMemoryArgs, LakeSoulSessionFactory, LakeSoulSessionOptions,
    build_worker_runtime_env,
};
use crate::tests::create_table_with_file_format;
use crate::tests::distributed_tests::{
    distributed_factory_sized, run_distributed_test, spawn_workers,
};
use crate::write_mode::{ConflictMode, LakeSoulWriteMode, WriteConflict, WriteViolation};

/// Workers of the distributed writes, which is also the task count the write
/// stage gets for the multi-task tests: three range partitions fan out to one
/// task each.
const WRITE_WORKERS: usize = 3;

/// Source files written per range partition by the test fixture.
const SOURCE_CALLS: usize = 4;

/// Global suffix keeping per-test table names unique (the metadata store is
/// only cleaned once per test binary run).
static WRITE_TABLE_SUFFIX: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Table fixture
// ---------------------------------------------------------------------------

/// A `(part INT, b INT)` append-only table: the shape every test writes into
/// and selects from.
fn write_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]))
}

/// One source batch: `(part, p * 1000 + 100 * file + 1)` for each partition.
fn source_batch(file: usize) -> RecordBatch {
    let parts: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
    let values: Vec<i32> = (0..3)
        .map(|part| part * 1000 + 100 * file as i32 + 1)
        .collect();
    let values: ArrayRef = Arc::new(Int32Array::from(values));
    RecordBatch::try_new(write_schema(), vec![parts, values]).unwrap()
}

/// The rows [`seed_source`] writes, sorted.
fn expected_source_rows() -> Vec<(i32, i32)> {
    let mut rows: Vec<(i32, i32)> = (0..3)
        .flat_map(|part| {
            (0..SOURCE_CALLS).map(move |file| (part, part * 1000 + 100 * file as i32 + 1))
        })
        .collect();
    rows.sort();
    rows
}

/// A table created for one test, with its metadata id and local directory.
struct TableFixture {
    name: String,
    table_id: String,
    root: PathBuf,
}

/// Creates an append-only Parquet table of [`write_schema`], optionally
/// range-partitioned by `part`, and returns its fixture.
async fn create_append_table(
    client: &MetaDataClientRef,
    prefix: &str,
    range_partitioned: bool,
) -> Result<TableFixture> {
    create_table_fixture(
        client,
        prefix,
        write_schema(),
        range_partitioned,
        Vec::new(),
    )
    .await
}

/// Creates a Parquet table with an explicit schema, range partitioning and
/// primary keys, and resolves its metadata id and local directory.
async fn create_table_fixture(
    client: &MetaDataClientRef,
    prefix: &str,
    schema: SchemaRef,
    range_partitioned: bool,
    primary_keys: Vec<String>,
) -> Result<TableFixture> {
    let suffix = WRITE_TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_write_{prefix}_{suffix}");
    let mut config = LakeSoulIOConfigBuilder::new().with_schema(schema);
    if range_partitioned {
        config = config.with_range_partitions(vec!["part".to_string()]);
    }
    if !primary_keys.is_empty() {
        config = config.with_primary_keys(primary_keys);
    }
    create_table_with_file_format(
        client.clone(),
        &name,
        config.build(),
        PhysicalFormat::Parquet,
    )
    .await?;
    let info = client
        .get_table_info_by_table_name(&name, "default")
        .await?
        .ok_or_else(|| report!("table {name} was not registered in the metadata"))?;
    let root = local_path(&info.table_path)?;
    Ok(TableFixture {
        name,
        table_id: info.table_id,
        root,
    })
}

/// Writes [`SOURCE_CALLS`] files into `table` through the normal upsert path.
async fn seed_source(table: &TableFixture) -> Result<()> {
    let table = LakeSoulTable::for_name(&table.name).await?;
    for file in 0..SOURCE_CALLS {
        table.execute_upsert(source_batch(file)).await?;
    }
    Ok(())
}

/// Removes the fixture's local directory after the test is done with it.
fn remove_table_dir(table: &TableFixture) {
    let _ = std::fs::remove_dir_all(&table.root);
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

/// What a write statement's physical plan says about where the write runs.
struct WritePlanShape {
    explain: String,
    /// The coordinator's head stage contains the commit node.
    head_has_commit: bool,
    /// The coordinator's head stage contains the write node. A distributed
    /// write must keep this `false`.
    head_has_write: bool,
    /// Task counts of the worker stages whose plan contains a write node.
    write_stage_tasks: Vec<usize>,
    /// Statement id stamped into the file names of the plan's write node.
    write_id: Option<String>,
}

/// Reads the write structure out of a planned statement.
///
/// The head stage is the plan down to (excluding) the first network boundary;
/// worker stages are the stages behind the boundaries. [`stage_contains_write`]
/// stops at a nested boundary, so only the stage a write node actually runs in
/// is reported, not every stage above it.
fn write_plan_shape(plan: &Arc<dyn ExecutionPlan>) -> WritePlanShape {
    let explain = display_plan_ascii(plan.as_ref(), false);

    let mut head_has_commit = false;
    let mut head_has_write = false;
    visit_head(plan, &mut |node| {
        head_has_commit |= node.downcast_ref::<LakeSoulCommitExec>().is_some();
        head_has_write |= node.downcast_ref::<LakeSoulWriteExec>().is_some();
    });

    let mut write_stage_tasks = Vec::new();
    plan.apply(|node| {
        if let Some(boundary) = node.as_network_boundary()
            && let Stage::Local(stage) = boundary.input_stage()
            && stage_contains_write(&stage.plan)
        {
            write_stage_tasks.push(stage.tasks);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a physical plan cannot fail");

    let mut write_id = None;
    plan.apply(|node| {
        if let Some(exec) = node.downcast_ref::<LakeSoulWriteExec>() {
            write_id = Some(exec.write_id().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a physical plan cannot fail");

    WritePlanShape {
        explain,
        head_has_commit,
        head_has_write,
        write_stage_tasks,
        write_id,
    }
}

/// Visits the coordinator's head stage: every node of `plan` above the first
/// network boundary.
fn visit_head(
    plan: &Arc<dyn ExecutionPlan>,
    visit: &mut impl FnMut(&Arc<dyn ExecutionPlan>),
) {
    if plan.is_network_boundary() {
        return;
    }
    visit(plan);
    for child in plan.children() {
        visit_head(child, visit);
    }
}

/// Whether the stage plan below `plan` runs a write node, without crossing a
/// nested network boundary into a lower stage.
fn stage_contains_write(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.downcast_ref::<LakeSoulWriteExec>().is_some() {
        return true;
    }
    if plan.is_network_boundary() {
        return false;
    }
    plan.children()
        .iter()
        .any(|child| stage_contains_write(child))
}

// ---------------------------------------------------------------------------
// Results and file names
// ---------------------------------------------------------------------------

/// Rows the `INSERT` reports as affected: the `count` column the SQL front
/// end reads.
fn inserted_rows(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .map(|batch| {
            batch
                .column_by_name("count")
                .expect("INSERT result has a count column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("INSERT count is UInt64")
                .values()
                .iter()
                .sum::<u64>()
        })
        .sum()
}

/// Writes below a SQL `ORDER BY` so results can be compared exactly.
async fn read_write_rows(ctx: &SessionContext, table: &str) -> Result<Vec<(i32, i32)>> {
    let batches = ctx
        .sql(&format!("SELECT part, b FROM {table} ORDER BY part, b"))
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in batches {
        let part = batch
            .column_by_name("part")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| report!("part column is not Int32"))?;
        let value = batch
            .column_by_name("b")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .ok_or_else(|| report!("b column is not Int32"))?;
        for row in 0..batch.num_rows() {
            rows.push((part.value(row), value.value(row)));
        }
    }
    Ok(rows)
}

/// Splits a data file name written by `write_id` into its task index and
/// bucket: `part-{write_id}T{task}_{bucket:04}.parquet` (task suffix present
/// only when the write stage ran in several tasks) or
/// `part-{write_id}_{bucket:04}.parquet`.
fn split_data_file_name(path: &Path, write_id: &str) -> Option<(Option<usize>, usize)> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_prefix("part-")?.strip_suffix(".parquet")?;
    let rest = stem.strip_prefix(write_id)?;
    let (task, rest) = match rest.strip_prefix('T') {
        Some(after) => {
            let (task, after) = after.split_once('_')?;
            if task.is_empty() || !task.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            (Some(task.parse().ok()?), after)
        }
        None => (None, rest.strip_prefix('_')?),
    };
    if rest.len() != 4 || !rest.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((task, rest.parse().ok()?))
}

/// Converts a `file://` path from the metadata into a local path.
fn local_path(path: &str) -> Result<PathBuf> {
    let url = url::Url::parse(path).map_err(|error| report!(error))?;
    url.to_file_path()
        .map_err(|_| report!("metadata path is not a local file: {path}"))
}

/// Every Parquet file below `root`, sorted.
fn data_files_on_disk(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries {
            let entry = entry.expect("test table directory is readable");
            let path = entry.path();
            if entry
                .file_type()
                .expect("test table entry has a type")
                .is_dir()
            {
                directories.push(path);
            } else if path.extension().and_then(|extension| extension.to_str())
                == Some("parquet")
            {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// A pooled PostgreSQL client for the assertions that need the commit rows
/// themselves: snapshot APIs cannot show an uncommitted or orphan row.
async fn metadata_pool() -> Result<PooledClient> {
    Ok(PooledClient::try_new(
        pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?,
        None,
    )
    .await?)
}

/// Per-partition commit rows of `table_id`: `(partition_desc, commits, files
/// listed by those commits, committed commits)`, plus the number of
/// uncommitted rows.
async fn metadata_counts(
    pool: &PooledClient,
    table_id: &str,
) -> Result<(Vec<(String, i64, i64, i64)>, i64)> {
    let table_id = table_id.to_string();
    let rows = pool
        .query(
            "SELECT partition_desc,
                    count(*)::bigint,
                    coalesce(sum(cardinality(file_ops)), 0)::bigint,
                    sum(CASE WHEN committed THEN 1 ELSE 0 END)::bigint
             FROM data_commit_info
             WHERE table_id = $1
             GROUP BY partition_desc
             ORDER BY partition_desc",
            QueryType::RW,
            &[&table_id],
        )
        .await?;
    let partitions = rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, i64>(1),
                row.get::<_, i64>(2),
                row.get::<_, i64>(3),
            )
        })
        .collect();
    let uncommitted = pool
        .query(
            "SELECT count(*)::bigint FROM data_commit_info
             WHERE table_id = $1 AND NOT committed",
            QueryType::RW,
            &[&table_id],
        )
        .await?[0]
        .get::<_, i64>(0);
    Ok((partitions, uncommitted))
}

/// `(path, size, file_exist_cols)` of every file op the table committed.
async fn committed_file_ops(
    pool: &PooledClient,
    table_id: &str,
) -> Result<Vec<(String, i64, String)>> {
    let table_id = table_id.to_string();
    let rows = pool
        .query(
            "SELECT (op).path, (op).size, (op).file_exist_cols
             FROM data_commit_info d, unnest(d.file_ops) AS op
             WHERE d.table_id = $1
             ORDER BY (op).path",
            QueryType::RW,
            &[&table_id],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                row.get::<_, i64>(1),
                row.get::<_, String>(2),
            )
        })
        .collect())
}

/// Asserts the statement committed exactly once per `expected_partitions`,
/// that every partition has exactly one committed version, that each
/// partition's active files live in its own directory, and that the files
/// the metadata references are exactly the files on disk. Returns the
/// committed local paths.
async fn assert_committed_each_partition_once(
    pool: &PooledClient,
    client: &MetaDataClientRef,
    table: &TableFixture,
    expected_partitions: &[&str],
) -> Result<Vec<PathBuf>> {
    let (commits, uncommitted) = metadata_counts(pool, &table.table_id).await?;
    assert_eq!(
        uncommitted, 0,
        "{}: the statement left an uncommitted commit row",
        table.name
    );

    let mut found: Vec<String> = commits.iter().map(|row| row.0.clone()).collect();
    let mut expected: Vec<String> = expected_partitions
        .iter()
        .map(|name| name.to_string())
        .collect();
    found.sort();
    expected.sort();
    assert_eq!(
        found, expected,
        "{}: commits must cover exactly the touched range partitions",
        table.name
    );
    for (partition, commit_count, files, committed) in &commits {
        assert_eq!(
            *commit_count, 1,
            "{}: partition {partition} must have exactly one commit",
            table.name
        );
        assert_eq!(
            *committed, 1,
            "{}: partition {partition}'s only commit must be committed",
            table.name
        );
        assert!(
            *files > 0,
            "{}: partition {partition}'s commit lists no files",
            table.name
        );
    }

    let partitions = client.get_all_partition_info(&table.table_id).await?;
    assert_eq!(
        partitions.len(),
        expected_partitions.len(),
        "{}: one partition version per touched range partition",
        table.name
    );
    let mut committed_files = Vec::new();
    let mut listed_files = 0i64;
    for partition in &partitions {
        assert_eq!(
            partition.snapshot.len(),
            1,
            "{}: partition {} must have exactly one committed version",
            table.name,
            partition.partition_desc
        );
        let files = client.get_data_files_of_single_partition(partition).await?;
        assert!(
            !files.is_empty(),
            "{}: partition {} has no active file",
            table.name,
            partition.partition_desc
        );
        for file in files {
            let path = local_path(&file)?;
            assert_eq!(
                path.parent()
                    .and_then(Path::file_name)
                    .and_then(|name| name.to_str()),
                Some(partition.partition_desc.as_str()),
                "{}: file {file} is not in partition {}'s directory",
                table.name,
                partition.partition_desc
            );
            committed_files.push(path);
            listed_files += 1;
        }
    }
    let commit_listed_files: i64 = commits.iter().map(|row| row.2).sum();
    assert_eq!(
        commit_listed_files, listed_files,
        "{}: commit file lists must equal the partitions' active files",
        table.name
    );

    committed_files.sort();
    assert_eq!(
        committed_files,
        data_files_on_disk(&table.root),
        "{}: the committed file set must equal the files on disk",
        table.name
    );
    Ok(committed_files)
}

/// Asserts nothing was published for `table`: no commit row (committed or
/// not), no partition version, and no active file.
async fn assert_nothing_published(
    pool: &PooledClient,
    client: &MetaDataClientRef,
    table: &TableFixture,
) -> Result<()> {
    let (commits, uncommitted) = metadata_counts(pool, &table.table_id).await?;
    assert!(
        commits.is_empty(),
        "{}: a statement that must not publish left {} commit row(s)",
        table.name,
        commits.len()
    );
    assert_eq!(
        uncommitted, 0,
        "{}: the failed statement left an uncommitted commit row",
        table.name
    );
    let partitions = client.get_all_partition_info(&table.table_id).await?;
    assert!(
        partitions.is_empty(),
        "{}: a statement that must not publish created {} partition version(s)",
        table.name,
        partitions.len()
    );
    let files = client
        .get_data_files_by_table_name(&table.name, "default")
        .await?;
    assert!(
        files.is_empty(),
        "{}: a statement that must not publish left active files: {files:?}",
        table.name
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Report-only success
// ---------------------------------------------------------------------------

#[test]
fn test_distributed_append_insert_select_writes_on_remote_stages() {
    run_distributed_test(
        test_distributed_append_insert_select_writes_on_remote_stages_inner(),
    );
}

/// The core distributed-write guarantee: a plain `INSERT ... SELECT` into an
/// append-only range-partitioned table writes on the workers, reports the
/// exact rows, and publishes one commit per range partition.
async fn test_distributed_append_insert_select_writes_on_remote_stages_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let source = create_append_table(&client, "source", true).await?;
    seed_source(&source).await?;
    let dest = create_append_table(&client, "dest", true).await?;

    let (worker_urls, uploads, mut workers) =
        spawn_fail_partition_workers(WRITE_WORKERS, "__no_upload_fault__").await?;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let sql = format!(
        "INSERT INTO {} SELECT part, b FROM {}",
        dest.name, source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);

    assert!(
        plan.is::<DistributedExec>(),
        "the INSERT must be planned as a distributed statement:\n{}",
        shape.explain
    );
    assert!(
        shape.head_has_commit,
        "the commit node must stay on the coordinator:\n{}",
        shape.explain
    );
    assert!(
        !shape.head_has_write,
        "the write node must run in a worker stage, not in the coordinator \
         head stage:\n{}",
        shape.explain
    );
    assert_eq!(
        shape.write_stage_tasks.len(),
        1,
        "exactly one worker stage must run the write:\n{}",
        shape.explain
    );
    let write_tasks = shape.write_stage_tasks[0];
    assert!(
        write_tasks > 1,
        "the write stage must fan out over several tasks, got {write_tasks}:\n{}",
        shape.explain
    );
    let write_id = shape
        .write_id
        .clone()
        .expect("the plan's write node carries a statement id");

    // Execute the plan that was just inspected: re-planning would generate a
    // new statement id and the file-name assertions below would be checking
    // the wrong statement.
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let expected = expected_source_rows();
    assert_eq!(
        inserted_rows(&batches),
        expected.len() as u64,
        "rows affected must be the number of source rows"
    );

    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
    let read_ctx = single.create_session(&LakeSoulSessionOptions::default())?;
    assert_eq!(
        read_write_rows(&read_ctx, &dest.name).await?,
        expected,
        "the destination must contain every source row exactly once"
    );

    let committed = assert_committed_each_partition_once(
        &pool,
        &client,
        &dest,
        &["part=0", "part=1", "part=2"],
    )
    .await?;

    // Every file belongs to this statement and carries the task it was
    // written in. `write_id` is known from the executed plan, so a `T` inside
    // the id cannot be confused with the task separator.
    let mut task_ids = BTreeSet::new();
    for path in &committed {
        let (task, _bucket) =
            split_data_file_name(path, &write_id).unwrap_or_else(|| {
                panic!(
                    "committed file {path:?} does not follow \
                 part-{write_id}T<task>_<bucket:04>.parquet"
                )
            });
        let task = task.unwrap_or_else(|| {
            panic!(
                "a write stage of {write_tasks} tasks must stamp the task \
                 index into {path:?}"
            )
        });
        assert!(
            task < write_tasks,
            "file {path:?} carries task {task}, but the stage has {write_tasks} tasks"
        );
        task_ids.insert(task);
    }
    assert!(
        task_ids.len() > 1,
        "files came from a single write task ({task_ids:?}); the statement \
         did not write in parallel"
    );

    // What the worker-local stores actually did: every committed file came
    // from one completed upload on a worker, the uploads are spread over
    // several worker servers, and the no-fault marker rejected nothing.
    let uploaded: usize = uploads.iter().map(WorkerWriteProbe::uploads).sum();
    assert_eq!(
        uploaded,
        committed.len(),
        "each committed file must be one completed worker upload"
    );
    let writing_workers = uploads.iter().filter(|probe| probe.uploads() > 0).count();
    assert!(
        writing_workers >= 2,
        "at least two distinct worker servers must actually upload files, got \
         {writing_workers}: {uploads:?}"
    );
    assert_eq!(
        uploads
            .iter()
            .map(WorkerWriteProbe::failures)
            .sum::<usize>(),
        0,
        "the success statement must not hit an upload fault"
    );

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Aggregate / limit semantics
// ---------------------------------------------------------------------------

#[test]
fn test_distributed_insert_select_keeps_aggregate_and_limit_results() {
    run_distributed_test(
        test_distributed_insert_select_keeps_aggregate_and_limit_results_inner(),
    );
}

/// `INSERT SELECT` over a grouped aggregate, a global aggregate and a global
/// `ORDER BY ... LIMIT` must write exactly the source operator's result.
///
/// The destination table is fresh per statement, so the commit assertions
/// belong to that statement alone. The write stage may or may not get its own
/// worker stage here (the aggregate/limit may already put the write above a
/// network boundary), so only the rows and the single per-partition commit
/// are pinned.
async fn test_distributed_insert_select_keeps_aggregate_and_limit_results_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let source = create_append_table(&client, "agg_source", true).await?;
    seed_source(&source).await?;

    let (worker_urls, mut workers) = spawn_workers(WRITE_WORKERS).await;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let cases = vec![
        (
            "grouped aggregate",
            format!(
                "SELECT part, CAST(sum(b) AS INT) AS b FROM {} GROUP BY part",
                source.name
            ),
            vec![(0, 604), (1, 4604), (2, 8604)],
            vec!["part=0", "part=1", "part=2"],
        ),
        (
            "global aggregate",
            format!(
                "SELECT CAST(0 AS INT) AS part, CAST(count(*) AS INT) AS b FROM {}",
                source.name
            ),
            vec![(0, 12)],
            vec!["part=0"],
        ),
        (
            "global order by and limit",
            format!(
                "SELECT part, b FROM {} ORDER BY b DESC LIMIT 5",
                source.name
            ),
            vec![(1, 1301), (2, 2001), (2, 2101), (2, 2201), (2, 2301)],
            vec!["part=1", "part=2"],
        ),
    ];

    for (label, projection, expected, expected_partitions) in cases {
        let dest = create_append_table(&client, "agg_dest", true).await?;
        let sql = format!("INSERT INTO {} {projection}", dest.name);
        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        let shape = write_plan_shape(&plan);
        assert!(
            shape.head_has_commit,
            "{label}: the commit node must stay on the coordinator:\n{}",
            shape.explain
        );
        assert!(
            shape.write_id.is_some(),
            "{label}: the plan must contain the write node:\n{}",
            shape.explain
        );

        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_eq!(
            inserted_rows(&batches),
            expected.len() as u64,
            "{label}: rows affected"
        );

        let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
        let read_ctx = single.create_session(&LakeSoulSessionOptions::default())?;
        assert_eq!(
            read_write_rows(&read_ctx, &dest.name).await?,
            expected,
            "{label}: the destination must hold the exact SELECT result"
        );
        assert_committed_each_partition_once(&pool, &client, &dest, &expected_partitions)
            .await?;
        remove_table_dir(&dest);
    }

    workers.abort_all();
    remove_table_dir(&source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Single-task naming
// ---------------------------------------------------------------------------

#[test]
fn test_single_task_write_does_not_stamp_a_task_index() {
    run_distributed_test(test_single_task_write_does_not_stamp_a_task_index_inner());
}

/// A statement whose write stage runs in one task must name its files
/// `part-{write_id}_{bucket:04}.parquet`: the task suffix exists only to keep
/// the files of several parallel tasks apart.
///
/// The source below is two tiny files in an unpartitioned table, so with the
/// library's default bytes-per-partition the scan stays one task and the
/// distributed planner leaves the statement single-node. That is exactly the
/// path the file name must still describe correctly.
async fn test_single_task_write_does_not_stamp_a_task_index_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let source = create_append_table(&client, "single_source", false).await?;
    seed_source(&source).await?;
    let dest = create_append_table(&client, "single_dest", false).await?;

    let (worker_urls, mut workers) = spawn_workers(2).await;
    let factory = distributed_factory_sized(client.clone(), worker_urls, 4, None, false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let sql = format!(
        "INSERT INTO {} SELECT part, b FROM {}",
        dest.name, source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert!(
        shape.write_stage_tasks.iter().all(|tasks| *tasks <= 1),
        "this fixture must plan a one-task write, got {:?}:\n{}",
        shape.write_stage_tasks,
        shape.explain
    );
    let write_id = shape
        .write_id
        .clone()
        .expect("the plan's write node carries a statement id");

    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let expected = expected_source_rows();
    assert_eq!(inserted_rows(&batches), expected.len() as u64);

    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
    let read_ctx = single.create_session(&LakeSoulSessionOptions::default())?;
    assert_eq!(read_write_rows(&read_ctx, &dest.name).await?, expected);

    let (commits, uncommitted) = metadata_counts(&pool, &dest.table_id).await?;
    assert_eq!(uncommitted, 0);
    assert_eq!(
        commits.len(),
        1,
        "an unpartitioned table has one commit row: {commits:?}"
    );
    let (partition_desc, commit_count, listed_files, committed) = &commits[0];
    assert_eq!(partition_desc, "-5", "the default partition description");
    assert_eq!(*commit_count, 1);
    assert_eq!(*committed, 1);

    let partitions = client.get_all_partition_info(&dest.table_id).await?;
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].snapshot.len(), 1);
    let committed_paths = client
        .get_data_files_of_single_partition(&partitions[0])
        .await?;
    assert_eq!(
        committed_paths.len() as i64,
        *listed_files,
        "the commit's file list must equal the partition's active files"
    );
    // The writer knows each file's byte count and stored columns, and the
    // commit records them. This is also what keeps the statement single-node:
    // with the unknown-size sentinel, the scan could only be split one task
    // per file.
    let file_ops = committed_file_ops(&pool, &dest.table_id).await?;
    assert_eq!(
        file_ops.len() as i64,
        *listed_files,
        "every committed file carries an op row: {file_ops:?}"
    );
    for (path, size, exist_cols) in &file_ops {
        let local = path.strip_prefix("file://").unwrap_or(path);
        let on_disk = std::fs::metadata(local)
            .map(|metadata| metadata.len() as i64)
            .expect("the committed file exists on disk");
        assert_eq!(
            *size, on_disk,
            "the committed size of {path} must be the file's length"
        );
        assert!(*size > 0, "a committed file has a known size: {path}");
        assert!(
            exist_cols.contains("part") && exist_cols.contains('b'),
            "the file op records the stored columns, got {exist_cols:?} for {path}"
        );
    }
    let mut local_files = Vec::new();
    for path in &committed_paths {
        let path = local_path(path)?;
        let (task, bucket) = split_data_file_name(&path, &write_id)
            .unwrap_or_else(|| panic!("{path:?} is not a file of {write_id}"));
        assert_eq!(
            task, None,
            "a one-task statement must not stamp a task index into {path:?}"
        );
        // One task still writes one file per input partition (scan lane), so
        // the bucket suffix stays even though the statement is single-node.
        assert!(
            bucket < 4,
            "the bucket suffix of {path:?} must be a lane index, got {bucket}"
        );
        local_files.push(path);
    }
    local_files.sort();
    assert_eq!(
        local_files,
        data_files_on_disk(&dest.root),
        "the committed file set must equal the files on disk"
    );

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Failure after data progress
// ---------------------------------------------------------------------------

/// Per-worker accounting of the object-store operations a worker really
/// performed.
///
/// `uploads` counts only uploads the inner store accepted (a `put_opts` that
/// returned `Ok`, or a multipart upload whose `complete` succeeded), so a
/// test can prove which worker servers actually wrote data.
#[derive(Debug, Clone, Default)]
struct WorkerWriteProbe {
    arrived: Arc<AtomicUsize>,
    uploads: Arc<AtomicUsize>,
    failures: Arc<AtomicUsize>,
}

impl WorkerWriteProbe {
    fn arrived(&self) -> usize {
        self.arrived.load(Ordering::SeqCst)
    }

    fn uploads(&self) -> usize {
        self.uploads.load(Ordering::SeqCst)
    }

    fn failures(&self) -> usize {
        self.failures.load(Ordering::SeqCst)
    }

    fn record_upload(&self) {
        self.uploads.fetch_add(1, Ordering::SeqCst);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::SeqCst);
    }
}

/// Wraps a multipart upload so the probe counts it only when `complete`
/// succeeds: obtaining an uploader is not an upload.
#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn object_store::MultipartUpload>,
    probe: WorkerWriteProbe,
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for CountingUpload {
    fn put_part(&mut self, data: object_store::PutPayload) -> object_store::UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        let result = self.inner.complete().await;
        if result.is_ok() {
            self.probe.record_upload();
        }
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

/// An object store that fails every upload into one range partition and lets
/// the other partitions upload normally.
///
/// The fault is selected by the destination path, not by a count or a sleep:
/// the first partition's files are provably uploaded before any failing
/// upload can make the statement fail, and the failing partition provably
/// never reaches storage. With the `__no_upload_fault__` marker no path
/// matches, so the same store serves as a per-worker upload recorder for the
/// success test.
#[derive(Debug)]
struct FailPartitionStore {
    inner: Arc<dyn object_store::ObjectStore>,
    /// Directory segment (a partition description) whose uploads fail.
    fail_segment: String,
    probe: WorkerWriteProbe,
}

impl std::fmt::Display for FailPartitionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fail-partition-store")
    }
}

fn injected_error(store: &'static str, message: &str) -> object_store::Error {
    object_store::Error::Generic {
        store,
        source: message.into(),
    }
}

impl FailPartitionStore {
    fn fails(&self, location: &object_store::path::Path) -> bool {
        location
            .as_ref()
            .split('/')
            .any(|segment| segment == self.fail_segment)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FailPartitionStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if self.fails(location) {
            self.probe.record_failure();
            return Err(injected_error(
                "fail-partition-store",
                "injected upload failure",
            ));
        }
        let result = self.inner.put_opts(location, payload, options).await;
        if result.is_ok() {
            self.probe.record_upload();
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        if self.fails(location) {
            self.probe.record_failure();
            return Err(injected_error(
                "fail-partition-store",
                "injected upload failure",
            ));
        }
        let upload = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(CountingUpload {
            inner: upload,
            probe: self.probe.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Spawns LakeSoul workers whose `file://` store is a [`FailPartitionStore`]
/// and returns each worker's [`WorkerWriteProbe`].
async fn spawn_fail_partition_workers(
    workers: usize,
    fail_segment: &str,
) -> Result<(Vec<String>, Vec<WorkerWriteProbe>, JoinSet<()>)> {
    let mut urls = Vec::new();
    let mut probes = Vec::new();
    let mut join_set = JoinSet::new();
    for _ in 0..workers {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let probe = WorkerWriteProbe::default();
        let runtime = build_worker_runtime_env(
            &CoreArgs::default(),
            ExecutionMemoryArgs::default().create_pool(),
        )?;
        runtime.register_object_store(
            &url::Url::parse("file://").unwrap(),
            Arc::new(FailPartitionStore {
                inner: Arc::new(object_store::local::LocalFileSystem::new()),
                fail_segment: fail_segment.to_string(),
                probe: probe.clone(),
            }),
        );
        let options = LakeSoulWorkerOptions {
            core_args: CoreArgs::default(),
            memory_pool: ExecutionMemoryArgs::default().create_pool(),
        };
        let worker =
            crate::distributed::lakesoul_worker(&options)?.with_runtime_env(runtime);
        urls.push(url);
        probes.push(probe);
        join_set.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
    }
    Ok((urls, probes, join_set))
}

#[test]
fn test_distributed_insert_failure_after_progress_publishes_nothing() {
    run_distributed_test(
        test_distributed_insert_failure_after_progress_publishes_nothing_inner(),
    );
}

/// A worker-side upload failure after other partition files were written must
/// fail the statement without publishing anything, and the coordinator must
/// delete the files that were written.
async fn test_distributed_insert_failure_after_progress_publishes_nothing_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let source = create_append_table(&client, "fail_source", true).await?;
    seed_source(&source).await?;
    let dest = create_append_table(&client, "fail_dest", true).await?;

    let (worker_urls, probes, mut workers) =
        spawn_fail_partition_workers(WRITE_WORKERS, "part=1").await?;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let sql = format!(
        "INSERT INTO {} SELECT part, b FROM {}",
        dest.name, source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert!(
        plan.is::<DistributedExec>(),
        "the failing statement must be distributed, or the worker fault is \
         never reached:\n{}",
        shape.explain
    );
    assert!(
        shape.head_has_commit,
        "the failing statement must still go through the commit node:\n{}",
        shape.explain
    );
    assert!(
        !shape.head_has_write && !shape.write_stage_tasks.is_empty(),
        "the failing write must run in a worker stage, or the injected store \
         is never used:\n{}",
        shape.explain
    );

    let error = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect_err("the injected upload failure must fail the INSERT");

    let successes: usize = probes.iter().map(WorkerWriteProbe::uploads).sum();
    let failures: usize = probes.iter().map(WorkerWriteProbe::failures).sum();
    assert!(
        successes >= 1,
        "the fault must land after at least one file was uploaded; error: {error}"
    );
    assert!(
        failures >= 1,
        "the injected partition never attempted an upload; error: {error}"
    );

    assert_nothing_published(&pool, &client, &dest).await?;
    let leftovers = data_files_on_disk(&dest.root);
    assert!(
        leftovers.is_empty(),
        "a failed statement must delete the files it wrote, found {leftovers:?}"
    );

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

/// A store that parks each worker's first upload at a gate: the write is
/// provably mid-flight when the gate is still closed, so only dropping the
/// statement can stop it from being committed.
///
/// The same store records every completed upload and every acknowledged
/// delete. A test can therefore prove that the files a healthy writer
/// uploaded after the gate opened were removed through that worker's own
/// store, rather than merely observing an empty directory later.
#[derive(Debug)]
struct GatedWriteStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gate: watch::Receiver<bool>,
    /// Whether this worker store already reached the gate.
    reached_gate: AtomicBool,
    probe: WorkerWriteProbe,
}

impl std::fmt::Display for GatedWriteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gated-write-store")
    }
}

impl GatedWriteStore {
    async fn wait_for_gate(&self) -> object_store::Result<()> {
        if !self.reached_gate.swap(true, Ordering::SeqCst) {
            self.probe.arrived.fetch_add(1, Ordering::SeqCst);
            let mut gate = self.gate.clone();
            while !*gate.borrow_and_update() {
                gate.changed().await.map_err(|_| {
                    injected_error(
                        "gated-write-store",
                        "the gate was dropped before it opened",
                    )
                })?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for GatedWriteStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.wait_for_gate().await?;
        let result = self.inner.put_opts(location, payload, options).await;
        if result.is_ok() {
            self.probe.record_upload();
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.wait_for_gate().await?;
        let upload = self.inner.put_multipart_opts(location, options).await?;
        Ok(Box::new(CountingUpload {
            inner: upload,
            probe: self.probe.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Spawns workers with a gated `file://` store and the observability service,
/// so a test can observe the tasks leave the worker cache once the statement
/// is dropped. The returned probes record the worker-local uploads and
/// deletions.
async fn spawn_gated_workers(
    workers: usize,
    gate: watch::Receiver<bool>,
) -> Result<(Vec<String>, Vec<WorkerWriteProbe>, JoinSet<()>)> {
    let mut urls = Vec::new();
    let mut probes = Vec::new();
    let mut join_set = JoinSet::new();
    for _ in 0..workers {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let url = format!("http://127.0.0.1:{}", listener.local_addr().unwrap().port());
        let probe = WorkerWriteProbe::default();
        let runtime = build_worker_runtime_env(
            &CoreArgs::default(),
            ExecutionMemoryArgs::default().create_pool(),
        )?;
        runtime.register_object_store(
            &url::Url::parse("file://").unwrap(),
            Arc::new(GatedWriteStore {
                inner: Arc::new(object_store::local::LocalFileSystem::new()),
                gate: gate.clone(),
                reached_gate: AtomicBool::new(false),
                probe: probe.clone(),
            }),
        );
        let options = LakeSoulWorkerOptions {
            core_args: CoreArgs::default(),
            memory_pool: ExecutionMemoryArgs::default().create_pool(),
        };
        let worker =
            crate::distributed::lakesoul_worker(&options)?.with_runtime_env(runtime);
        let observability = worker.with_observability_service(Arc::new(
            StaticWorkerResolver::new(vec![url.clone()])
                .expect("worker url is well formed"),
        ));
        urls.push(url);
        probes.push(probe);
        join_set.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .add_service(observability)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
    }
    Ok((urls, probes, join_set))
}

/// Polls until `predicate` holds or panics at a 30 s deadline.
async fn wait_until(mut predicate: impl FnMut() -> bool, label: &str) {
    for _ in 0..600 {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{label} did not happen within 30 s");
}

/// Connects to the observability service of every worker in `urls`.
async fn connect_observers(
    urls: &[String],
) -> Result<Vec<ObservabilityServiceClient<tonic::transport::Channel>>> {
    let mut observers = Vec::with_capacity(urls.len());
    for url in urls {
        observers.push(ObservabilityServiceClient::connect(url.clone()).await?);
    }
    Ok(observers)
}

/// Waits until a worker holds no initialized task, so no further upload or
/// report can be produced by it.
async fn wait_until_worker_drained(
    observer: &mut ObservabilityServiceClient<tonic::transport::Channel>,
    label: &str,
) -> Result<()> {
    let remaining = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let tasks = observer
                .get_task_progress(GetTaskProgressRequest {})
                .await?
                .into_inner()
                .tasks
                .len();
            if tasks == 0 {
                return Ok::<usize, tonic::Status>(0);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| report!("{label} did not drain its initialized tasks within 10 s"))??;
    let _ = remaining;
    Ok(())
}

#[test]
fn test_distributed_insert_cancelled_before_commit_publishes_nothing() {
    run_distributed_test(
        test_distributed_insert_cancelled_before_commit_publishes_nothing_inner(),
    );
}

/// Dropping an `INSERT`'s stream while its write tasks are parked mid-upload
/// must not publish a commit and must not leave the files the healthy writers
/// produce after the gate opens.
///
/// The library routes the write stage's tasks over distinct workers for this
/// fixture, so the test first waits until *every* worker's first upload is
/// parked at the gate; at that point no writer can have written a file. The
/// stream is then dropped (how the PostgreSQL front end cancels), the gate
/// opens, and each worker is observed to drain its plan.
///
/// An empty task registry alone does not prove the detached writers finished.
/// The statement is cancelled through the coordinator's result stream, so the
/// commit node keeps draining the workers' reports; once every worker has
/// drained, all uploads have been reported. The commit node then observes the
/// cancellation flag and deletes the reported files through the coordinator's
/// store. The test waits for every routed worker to upload after the gate
/// opened, for the destination directory to become empty, and then for the
/// upload counters to stay unchanged, so a late upload cannot race the check.
/// Metadata and the on-disk file set are asserted after that terminal state.
async fn test_distributed_insert_cancelled_before_commit_publishes_nothing_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let source = create_append_table(&client, "cancel_source", true).await?;
    seed_source(&source).await?;
    let dest = create_append_table(&client, "cancel_dest", true).await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let sql = format!(
        "INSERT INTO {} SELECT part, b FROM {}",
        dest.name, source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert!(
        plan.is::<DistributedExec>(),
        "the cancelled statement must be distributed, or the gate is never \
         reached:\n{}",
        shape.explain
    );
    assert!(
        shape.head_has_commit,
        "the statement being cancelled must be an INSERT:\n{}",
        shape.explain
    );
    assert!(
        !shape.head_has_write && !shape.write_stage_tasks.is_empty(),
        "the cancelled write must run in a worker stage, or the gate is never \
         reached:\n{}",
        shape.explain
    );

    let stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    let write_tasks = shape.write_stage_tasks[0];
    // The library routes the stage's tasks over distinct workers, so waiting
    // until exactly the planned number of workers have parked an upload
    // proves every writer is blocked before the drop.
    wait_until(
        || probes.iter().filter(|probe| probe.arrived() > 0).count() == write_tasks,
        "every write task parked its first upload at the gate",
    )
    .await;

    // Cancellation: drop the statement while its writers are still blocked.
    drop(stream);
    gate_tx
        .send(true)
        .expect("the gated workers keep the gate receiver alive");

    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("cancelled write worker {index}"))
            .await?;
    }

    // The commit node drains every report before it checks the cancellation
    // flag, so once the workers have drained and the directory is empty, the
    // reported files are gone. A short stable window proves no upload is still
    // in flight.
    wait_until(
        || {
            probes
                .iter()
                .filter(|probe| probe.arrived() > 0)
                .all(|probe| probe.uploads() > 0)
        },
        "every healthy writer completed an upload after the gate opened",
    )
    .await;
    wait_until(
        || data_files_on_disk(&dest.root).is_empty(),
        "a cancelled statement removed the files its healthy writers uploaded",
    )
    .await;
    let uploads = probes.iter().map(WorkerWriteProbe::uploads).sum::<usize>();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        probes.iter().map(WorkerWriteProbe::uploads).sum::<usize>(),
        uploads,
        "an upload landed after the cancelled statement's cleanup"
    );
    assert!(
        data_files_on_disk(&dest.root).is_empty(),
        "a cancelled statement left data files behind"
    );
    assert_nothing_published(&pool, &client, &dest).await?;

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Primary keys
// ---------------------------------------------------------------------------

/// `(id INT NOT NULL, v INT NOT NULL)` with `id` as primary key.
fn pk_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

/// `(part INT NOT NULL, id INT NOT NULL, v INT NOT NULL)` with `id` as
/// primary key and `part` as the range partition column. LakeSoul keys are
/// partition-local, so the unique key of this table is `(part, id)`.
fn pk_part_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

fn iv_batch(rows: &[(i32, i32)]) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    ));
    let values: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(pk_schema(), vec![ids, values]).unwrap()
}

fn piv_batch(rows: &[(i32, i32, i32)]) -> RecordBatch {
    let parts: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|(part, _, _)| *part).collect::<Vec<_>>(),
    ));
    let ids: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|(_, id, _)| *id).collect::<Vec<_>>(),
    ));
    let values: ArrayRef = Arc::new(Int32Array::from(
        rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(pk_part_schema(), vec![parts, ids, values]).unwrap()
}

/// Creates a primary-key table, optionally range-partitioned by `part`.
async fn create_pk_table(
    client: &MetaDataClientRef,
    prefix: &str,
    range_partitioned: bool,
) -> Result<TableFixture> {
    if range_partitioned {
        create_table_fixture(
            client,
            prefix,
            pk_part_schema(),
            true,
            vec!["id".to_string()],
        )
        .await
    } else {
        create_table_fixture(client, prefix, pk_schema(), false, vec!["id".to_string()])
            .await
    }
}

/// Seeds committed keys through the normal upsert path.
async fn seed_pk_rows(table: &TableFixture, rows: &[(i32, i32)]) -> Result<()> {
    LakeSoulTable::for_name(&table.name)
        .await?
        .execute_upsert(iv_batch(rows))
        .await
}

/// An append-only `(id, v)` source table, one file per entry of `files`.
async fn create_iv_source(
    client: &MetaDataClientRef,
    prefix: &str,
    files: Vec<Vec<(i32, i32)>>,
) -> Result<TableFixture> {
    let table =
        create_table_fixture(client, prefix, pk_schema(), false, Vec::new()).await?;
    let handle = LakeSoulTable::for_name(&table.name).await?;
    for rows in files {
        handle.execute_upsert(iv_batch(&rows)).await?;
    }
    Ok(table)
}

/// An append-only `(part, id, v)` source table range-partitioned by `part`,
/// one call per entry of `files`.
async fn create_piv_source(
    client: &MetaDataClientRef,
    prefix: &str,
    files: Vec<Vec<(i32, i32, i32)>>,
) -> Result<TableFixture> {
    let table =
        create_table_fixture(client, prefix, pk_part_schema(), true, Vec::new()).await?;
    let handle = LakeSoulTable::for_name(&table.name).await?;
    for rows in files {
        handle.execute_upsert(piv_batch(&rows)).await?;
    }
    Ok(table)
}

fn int32_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
        .ok_or_else(|| report!("column {name} is missing or not Int32"))
}

async fn read_iv_rows(ctx: &SessionContext, table: &str) -> Result<Vec<(i32, i32)>> {
    let batches = ctx
        .sql(&format!("SELECT id, v FROM {table} ORDER BY id"))
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in batches {
        let ids = int32_column(&batch, "id")?;
        let values = int32_column(&batch, "v")?;
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    Ok(rows)
}

async fn read_piv_rows(
    ctx: &SessionContext,
    table: &str,
) -> Result<Vec<(i32, i32, i32)>> {
    let batches = ctx
        .sql(&format!(
            "SELECT part, id, v FROM {table} ORDER BY part, id"
        ))
        .await?
        .collect()
        .await?;
    let mut rows = Vec::new();
    for batch in batches {
        let parts = int32_column(&batch, "part")?;
        let ids = int32_column(&batch, "id")?;
        let values = int32_column(&batch, "v")?;
        for row in 0..batch.num_rows() {
            rows.push((parts.value(row), ids.value(row), values.value(row)));
        }
    }
    Ok(rows)
}

/// The metadata of a table at one instant: its commit rows per partition,
/// the number of uncommitted rows, and the latest version/snapshot length of
/// every partition. Two equal states mean a statement changed nothing.
#[derive(Debug, PartialEq, Eq)]
struct MetadataState {
    commits: Vec<(String, i64, i64, i64)>,
    uncommitted: i64,
    versions: Vec<(String, i32, usize)>,
}

async fn metadata_state(
    pool: &PooledClient,
    client: &MetaDataClientRef,
    table: &TableFixture,
) -> Result<MetadataState> {
    let (commits, uncommitted) = metadata_counts(pool, &table.table_id).await?;
    let partitions = client.get_all_partition_info(&table.table_id).await?;
    let mut versions: Vec<(String, i32, usize)> = partitions
        .iter()
        .map(|partition| {
            (
                partition.partition_desc.clone(),
                partition.version,
                partition.snapshot.len(),
            )
        })
        .collect();
    versions.sort();
    Ok(MetadataState {
        commits,
        uncommitted,
        versions,
    })
}

/// Asserts the files the metadata references are exactly the files on disk,
/// so a statement did not leave an orphan and did not lose a committed file.
async fn assert_disk_equals_active_files(
    client: &MetaDataClientRef,
    table: &TableFixture,
) -> Result<()> {
    let active = client
        .get_data_files_by_table_name(&table.name, "default")
        .await?;
    let mut paths = active
        .iter()
        .map(|path| local_path(path))
        .collect::<Result<Vec<_>>>()?;
    paths.sort();
    assert_eq!(
        paths,
        data_files_on_disk(&table.root),
        "{}: the active files and the files on disk must be the same set",
        table.name
    );
    Ok(())
}

/// Finds a typed cause inside a `DataFusionError`.
///
/// The write validation runs on a worker and comes back through the report
/// stream, so the error may be wrapped; the type is what preserves the
/// SQLSTATE, not the message.
fn find_cause<T>(error: &DataFusionError) -> Option<&T>
where
    T: std::error::Error + 'static,
{
    fn walk<'a, T>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a T>
    where
        T: std::error::Error + 'static,
    {
        if let Some(found) = error.downcast_ref::<T>() {
            return Some(found);
        }
        error.source().and_then(walk::<T>)
    }
    if let DataFusionError::External(inner) = error {
        let inner: &(dyn std::error::Error + 'static) = &**inner;
        if let Some(found) = inner.downcast_ref::<T>() {
            return Some(found);
        }
        if let Some(found) = inner.source().and_then(walk::<T>) {
            return Some(found);
        }
    }
    walk::<T>(error)
}

/// A keyed statement must keep the commit on the coordinator and the keyed
/// write in a worker stage; the keyed shuffle is what gives equal keys one
/// validator.
fn assert_keyed_write_is_distributed(
    plan: &Arc<dyn ExecutionPlan>,
    shape: &WritePlanShape,
    label: &str,
) {
    assert!(
        plan.is::<DistributedExec>(),
        "{label}: the keyed INSERT must be distributed:\n{}",
        shape.explain
    );
    assert!(
        shape.head_has_commit,
        "{label}: the commit node must stay on the coordinator:\n{}",
        shape.explain
    );
    assert!(
        !shape.head_has_write,
        "{label}: the keyed write must run in a worker stage:\n{}",
        shape.explain
    );
    assert!(
        !shape.write_stage_tasks.is_empty(),
        "{label}: the write must have a worker stage:\n{}",
        shape.explain
    );
    assert!(
        shape.write_id.is_some(),
        "{label}: the plan's write node carries a statement id:\n{}",
        shape.explain
    );
}

/// Builds a distributed session whose config carries a `LakeSoulWriteMode`
/// handle set to `mode`, the way the SQL front ends install it before a
/// statement.
fn session_with_write_mode(
    client: &MetaDataClientRef,
    worker_urls: Vec<String>,
    mode: ConflictMode,
) -> Result<(Arc<SessionContext>, Arc<LakeSoulWriteMode>)> {
    let write_mode = Arc::new(LakeSoulWriteMode::new());
    write_mode.set(mode);
    let template =
        crate::create_lakesoul_session_config()?.with_extension(write_mode.clone());
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?
            .with_session_template(template);
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
    Ok((ctx, write_mode))
}

#[test]
fn test_distributed_pk_insert_rejects_existing_and_duplicate_keys() {
    run_distributed_test(
        test_distributed_pk_insert_rejects_existing_and_duplicate_keys_inner(),
    );
}

/// A plain keyed `INSERT` rejects a key that is already committed and a key
/// that two rows of the same statement share, in both cases without
/// publishing a commit, a partition version, or a data file.
async fn test_distributed_pk_insert_rejects_existing_and_duplicate_keys_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "pk_reject_dest", false).await?;
    seed_pk_rows(&dest, &[(1, 10), (2, 20)]).await?;
    let existing_source = create_iv_source(
        &client,
        "pk_reject_existing",
        vec![vec![(1, 999)], vec![(3, 30)], vec![(4, 40)]],
    )
    .await?;
    let duplicate_source = create_iv_source(
        &client,
        "pk_reject_duplicate",
        vec![vec![(7, 1)], vec![(7, 2)], vec![(8, 80)]],
    )
    .await?;
    let fresh_dest = create_pk_table(&client, "pk_reject_fresh", false).await?;

    let (worker_urls, mut workers) = spawn_workers(WRITE_WORKERS).await;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    // A key that is already committed: the worker validation sees it through
    // the broadcast existing-key scan and rejects the whole statement.
    let before = metadata_state(&pool, &client, &dest).await?;
    let before_files = data_files_on_disk(&dest.root);
    let sql = format!(
        "INSERT INTO {} SELECT id, v FROM {}",
        dest.name, existing_source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "existing-key reject");
    let error = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect_err("a committed key must fail a plain INSERT");
    let violation = find_cause::<WriteViolation>(&error)
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert!(
        matches!(violation, WriteViolation::ExistingKey { .. }),
        "expected ExistingKey, got {violation:?}"
    );
    assert_eq!(violation.sqlstate(), "23505");
    assert_eq!(
        metadata_state(&pool, &client, &dest).await?,
        before,
        "a rejected statement must not change the metadata"
    );
    assert_eq!(
        data_files_on_disk(&dest.root),
        before_files,
        "a rejected statement must not add files"
    );

    // Two rows of the same statement sharing a key. They may arrive in
    // different source files, so this also proves the keyed shuffle gave
    // them one validator.
    let sql = format!(
        "INSERT INTO {} SELECT id, v FROM {}",
        fresh_dest.name, duplicate_source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "duplicate-key reject");
    let error = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect_err("a duplicate key inside the statement must fail a plain INSERT");
    let violation = find_cause::<WriteViolation>(&error)
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert!(
        matches!(violation, WriteViolation::DuplicateKey { .. }),
        "expected DuplicateKey, got {violation:?}"
    );
    assert_eq!(violation.sqlstate(), "23505");
    assert_nothing_published(&pool, &client, &fresh_dest).await?;
    assert!(
        data_files_on_disk(&fresh_dest.root).is_empty(),
        "a rejected statement must delete the files it wrote"
    );

    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
    let read_ctx = single.create_session(&LakeSoulSessionOptions::default())?;
    assert_eq!(
        read_iv_rows(&read_ctx, &dest.name).await?,
        vec![(1, 10), (2, 20)],
        "the rejected statements must leave the committed rows untouched"
    );

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&fresh_dest);
    remove_table_dir(&existing_source);
    remove_table_dir(&duplicate_source);
    Ok(())
}

#[test]
fn test_distributed_pk_insert_skip_drops_only_conflicting_rows() {
    run_distributed_test(
        test_distributed_pk_insert_skip_drops_only_conflicting_rows_inner(),
    );
}

/// `ON CONFLICT DO NOTHING` (the session's `Skip` mode) drops a row whose key
/// is already committed and reports only the rows actually inserted.
async fn test_distributed_pk_insert_skip_drops_only_conflicting_rows_inner() -> Result<()>
{
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "pk_skip_dest", false).await?;
    seed_pk_rows(&dest, &[(1, 10), (2, 20)]).await?;
    let source = create_iv_source(
        &client,
        "pk_skip_source",
        vec![vec![(1, 999)], vec![(3, 30)], vec![(4, 40)]],
    )
    .await?;

    let (worker_urls, mut workers) = spawn_workers(WRITE_WORKERS).await;
    let (ctx, _write_mode) =
        session_with_write_mode(&client, worker_urls, ConflictMode::Skip)?;

    let sql = format!(
        "INSERT INTO {} SELECT id, v FROM {}",
        dest.name, source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "skip mode");

    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    assert_eq!(
        inserted_rows(&batches),
        2,
        "only the two non-conflicting rows may be inserted"
    );

    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
    let read_ctx = single.create_session(&LakeSoulSessionOptions::default())?;
    assert_eq!(
        read_iv_rows(&read_ctx, &dest.name).await?,
        vec![(1, 10), (2, 20), (3, 30), (4, 40)],
        "the skipped key must keep its committed value"
    );

    let (commits, uncommitted) = metadata_counts(&pool, &dest.table_id).await?;
    assert_eq!(uncommitted, 0);
    assert_eq!(commits.len(), 1);
    let (_, commit_count, _, committed) = &commits[0];
    assert_eq!(
        *commit_count, 2,
        "the seed and the skip-mode statement must each commit once"
    );
    assert_eq!(*committed, 2);
    assert_eq!(
        client.get_all_partition_info(&dest.table_id).await?[0]
            .snapshot
            .len(),
        2
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

#[test]
fn test_distributed_pk_insert_or_replace_merges_across_commits() {
    run_distributed_test(
        test_distributed_pk_insert_or_replace_merges_across_commits_inner(),
    );
}

/// `INSERT OR REPLACE` updates committed keys in later commits; every read
/// merges the statements' files into one row per key.
async fn test_distributed_pk_insert_or_replace_merges_across_commits_inner() -> Result<()>
{
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "pk_replace_dest", false).await?;
    seed_pk_rows(&dest, &[(1, 10), (2, 20)]).await?;
    let first = create_iv_source(
        &client,
        "pk_replace_first",
        vec![vec![(2, 200)], vec![(3, 300)]],
    )
    .await?;
    let second = create_iv_source(
        &client,
        "pk_replace_second",
        vec![vec![(1, 100)], vec![(3, 333)]],
    )
    .await?;

    let (worker_urls, mut workers) = spawn_workers(WRITE_WORKERS).await;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
    let read_ctx = single.create_session(&LakeSoulSessionOptions::default())?;

    let cases = [
        (
            &first,
            vec![(1, 10), (2, 200), (3, 300)],
            "the first replace must update key 2 and insert key 3",
        ),
        (
            &second,
            vec![(1, 100), (2, 200), (3, 333)],
            "the second replace must update keys 1 and 3",
        ),
    ];
    for (source, expected, label) in cases {
        let sql = format!(
            "INSERT OR REPLACE INTO {} SELECT id, v FROM {}",
            dest.name, source.name
        );
        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        let shape = write_plan_shape(&plan);
        assert_keyed_write_is_distributed(&plan, &shape, label);

        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_eq!(inserted_rows(&batches), 2, "{label}: rows affected");
        assert_eq!(
            read_iv_rows(&read_ctx, &dest.name).await?,
            expected,
            "{label}"
        );
    }

    let (commits, uncommitted) = metadata_counts(&pool, &dest.table_id).await?;
    assert_eq!(uncommitted, 0);
    assert_eq!(commits.len(), 1, "an unpartitioned table has one partition");
    let (_, commit_count, _, committed) = &commits[0];
    assert_eq!(
        *commit_count, 3,
        "the seed and the two replace statements must each commit once"
    );
    assert_eq!(*committed, 3);
    assert_eq!(
        client.get_all_partition_info(&dest.table_id).await?[0]
            .snapshot
            .len(),
        3
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&first);
    remove_table_dir(&second);
    Ok(())
}

#[test]
fn test_distributed_pk_insert_keys_are_partition_local() {
    run_distributed_test(test_distributed_pk_insert_keys_are_partition_local_inner());
}

/// LakeSoul unique keys are partition-local: the same id in two range
/// partitions is two keys, while a duplicate inside one partition is
/// rejected. The range column is part of the validation key.
async fn test_distributed_pk_insert_keys_are_partition_local_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "pk_range_dest", true).await?;
    let allowed = create_piv_source(
        &client,
        "pk_range_allowed",
        vec![vec![(0, 1, 10)], vec![(1, 1, 20)]],
    )
    .await?;
    let same_partition =
        create_piv_source(&client, "pk_range_existing", vec![vec![(0, 1, 99)]]).await?;
    let duplicate_in_partition = create_piv_source(
        &client,
        "pk_range_duplicate",
        vec![vec![(2, 5, 1)], vec![(2, 5, 2)]],
    )
    .await?;

    let (worker_urls, mut workers) = spawn_workers(WRITE_WORKERS).await;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    // The same id in two range partitions is allowed.
    let sql = format!(
        "INSERT INTO {} SELECT part, id, v FROM {}",
        dest.name, allowed.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "partition-local allowed");
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    assert_eq!(inserted_rows(&batches), 2);

    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
    let read_ctx = single.create_session(&LakeSoulSessionOptions::default())?;
    assert_eq!(
        read_piv_rows(&read_ctx, &dest.name).await?,
        vec![(0, 1, 10), (1, 1, 20)]
    );

    // The same id within one partition conflicts with the committed row.
    let before = metadata_state(&pool, &client, &dest).await?;
    let sql = format!(
        "INSERT INTO {} SELECT part, id, v FROM {}",
        dest.name, same_partition.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "partition-local conflict");
    let error = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect_err("a duplicate key inside one partition must fail");
    let violation = find_cause::<WriteViolation>(&error)
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert!(matches!(violation, WriteViolation::ExistingKey { .. }));
    assert_eq!(violation.sqlstate(), "23505");
    assert_eq!(metadata_state(&pool, &client, &dest).await?, before);

    // A duplicate inside a partition that has no committed rows at all is
    // still a duplicate: it must not create that partition.
    let sql = format!(
        "INSERT INTO {} SELECT part, id, v FROM {}",
        dest.name, duplicate_in_partition.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "partition-local duplicate");
    let error = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect_err("a duplicate key inside the statement must fail");
    let violation = find_cause::<WriteViolation>(&error)
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert!(matches!(violation, WriteViolation::DuplicateKey { .. }));
    assert_eq!(violation.sqlstate(), "23505");
    assert_eq!(
        metadata_state(&pool, &client, &dest).await?,
        before,
        "the failed statements must not create the part=2 partition"
    );
    assert_eq!(
        read_piv_rows(&read_ctx, &dest.name).await?,
        vec![(0, 1, 10), (1, 1, 20)]
    );

    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&allowed);
    remove_table_dir(&same_partition);
    remove_table_dir(&duplicate_in_partition);
    Ok(())
}

#[test]
fn test_distributed_pk_insert_losing_the_snapshot_race_is_retryable() {
    run_distributed_test(
        test_distributed_pk_insert_losing_the_snapshot_race_is_retryable_inner(),
    );
}

/// A commit that lands between the insert-only plan's snapshot and its
/// publication must make the statement fail retryably (`40001`), never
/// rebase silently on the newer version.
///
/// The distributed statement's workers are parked mid-upload at a gate after
/// their existing-key validation passed; a single-node session then commits a
/// new key, moving the partition version. Opening the gate lets the first
/// statement publish, where its plan-time snapshot no longer matches.
async fn test_distributed_pk_insert_losing_the_snapshot_race_is_retryable_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "pk_race_dest", false).await?;
    seed_pk_rows(&dest, &[(1, 10)]).await?;
    let source = create_iv_source(
        &client,
        "pk_race_source",
        vec![vec![(4, 40)], vec![(6, 60)]],
    )
    .await?;
    let concurrent =
        create_iv_source(&client, "pk_race_concurrent", vec![vec![(5, 50)]]).await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let factory =
        distributed_factory_sized(client.clone(), worker_urls, 6, Some(1), false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let sql = format!(
        "INSERT INTO {} SELECT id, v FROM {}",
        dest.name, source.name
    );
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let shape = write_plan_shape(&plan);
    assert_keyed_write_is_distributed(&plan, &shape, "snapshot race");

    let mut stream = execute_stream(Arc::clone(&plan), ctx.task_ctx())?;
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "a keyed write upload parked at the gate",
    )
    .await;

    // The race: a second session commits to the same partition after the
    // first statement captured its snapshot and validated its keys, but
    // before it can publish.
    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?;
    let single_ctx = single.create_session(&LakeSoulSessionOptions::default())?;
    single_ctx
        .sql(&format!(
            "INSERT INTO {} SELECT id, v FROM {}",
            dest.name, concurrent.name
        ))
        .await?
        .collect()
        .await?;
    assert_eq!(
        read_iv_rows(&single_ctx, &dest.name).await?,
        vec![(1, 10), (5, 50)],
        "the concurrent statement must commit before the gate opens"
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
    let error = failure.expect("the stale-snapshot statement must fail");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(
        conflict.sqlstate(),
        "40001",
        "a lost snapshot race must be retryable, got: {error}"
    );

    assert_eq!(
        read_iv_rows(&single_ctx, &dest.name).await?,
        vec![(1, 10), (5, 50)],
        "the conflicting statement must not publish its keys"
    );
    assert_eq!(
        metadata_state(&pool, &client, &dest).await?.uncommitted,
        0,
        "the conflicting statement must leave no uncommitted commit row"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("raced write worker {index}"))
            .await?;
    }
    workers.abort_all();
    remove_table_dir(&dest);
    remove_table_dir(&source);
    remove_table_dir(&concurrent);
    Ok(())
}
