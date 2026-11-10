// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Executor-level regression tests for SQL `INSERT ... ON CONFLICT DO UPDATE`.
//!
//! Every statement is planned by the production planner
//! `planner::upsert::plan_upsert_sql` with `SnapshotMode::Capture` and executed
//! through the physical plan a client gets, so the tests cover the same
//! planner/executor/codec path as the PostgreSQL front end. The PostgreSQL
//! contract target (`tests/upsert_contract.rs`) owns the SQL-oracle matrix;
//! these tests pin the properties that need a native counter or a real
//! coordinator/worker barrier:
//!
//! - the source plan is executed exactly once (a counting `TableProvider`);
//! - a repeated source key is `21000` before the later row's `WHERE`, and
//!   explicit source order decides which row is first;
//! - empty and all-skipped sources publish nothing;
//! - more than 1024 output rows cross the state machine's output-batch
//!   boundary;
//! - an input larger than the execution-memory budget either spills and
//!   succeeds or fails with a typed `ResourcesExhausted` and no publication;
//! - two coordinators racing on the same (missing or existing) key cannot both
//!   publish; the loser is a retryable `40001` and the retried statement
//!   rebranches from the new snapshot.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::TableReference;
use datafusion::datasource::MemTable;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, collect, execute_stream,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef, PooledClient};
use tokio::sync::watch;

use crate::Result;
use crate::cli::CoreArgs;
use crate::lakesoul_table::LakeSoulTable;
use crate::planner::upsert::{PlannedUpsert, SnapshotMode, plan_upsert_sql};
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::distributed_tests::{distributed_factory_sized, run_distributed_test};
use crate::tests::distributed_write_tests::{
    MetadataState, TableFixture, WRITE_WORKERS, assert_disk_equals_active_files,
    connect_observers, create_pk_table, create_table_fixture, find_cause, inserted_rows,
    iv_batch, metadata_pool, metadata_state, pk_schema, read_iv_rows, remove_table_dir,
    spawn_gated_workers, wait_until, wait_until_worker_drained, write_plan_shape,
};
use crate::write_mode::{LakeSoulFixedSnapshot, WriteConflict, WriteViolation};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Plans one upsert with the production planner and keeps the captured
/// snapshot guard alive for the whole execution.
async fn plan_upsert(
    ctx: &SessionContext,
    table: &str,
    sql: &str,
) -> Result<PlannedUpsert> {
    Ok(
        plan_upsert_sql(ctx, sql, TableReference::bare(table), SnapshotMode::Capture)
            .await?,
    )
}

async fn upsert_physical_plan(
    ctx: &SessionContext,
    table: &str,
    sql: &str,
) -> Result<(PlannedUpsert, Arc<dyn ExecutionPlan>)> {
    let planned = plan_upsert(ctx, table, sql).await?;
    let plan = ctx.state().create_physical_plan(planned.plan()).await?;
    Ok((planned, plan))
}

async fn run_upsert(
    ctx: &SessionContext,
    table: &str,
    sql: &str,
) -> Result<Vec<RecordBatch>> {
    let (planned, plan) = upsert_physical_plan(ctx, table, sql).await?;
    let batches = collect(plan, ctx.task_ctx()).await?;
    drop(planned);
    Ok(batches)
}

/// Every SQL statement session carries the per-statement snapshot handle; the
/// planner fails with `MissingSnapshotHandle` without it.
fn upsert_session_template() -> Result<datafusion::prelude::SessionConfig> {
    Ok(crate::create_lakesoul_session_config()?
        .with_extension(Arc::new(LakeSoulFixedSnapshot::new())))
}

fn single_node_session(client: MetaDataClientRef) -> Result<Arc<SessionContext>> {
    let factory = LakeSoulSessionFactory::new(client, &CoreArgs::default())?
        .with_session_template(upsert_session_template()?);
    factory.create_session(&LakeSoulSessionOptions::default())
}

fn distributed_upsert_session(
    client: MetaDataClientRef,
    worker_urls: Vec<String>,
) -> Result<Arc<SessionContext>> {
    let factory = distributed_factory_sized(client, worker_urls, 6, Some(1), false)?
        .with_session_template(upsert_session_template()?);
    factory.create_session(&LakeSoulSessionOptions::default())
}

/// A `(id INT, v INT)` append-only source, seeded through the normal insert
/// path so repeated keys stay possible.
async fn create_iv_source(
    client: &MetaDataClientRef,
    prefix: &str,
    batches: Vec<Vec<(i32, i32)>>,
) -> Result<TableFixture> {
    let table =
        create_table_fixture(client, prefix, pk_schema(), false, Vec::new()).await?;
    let handle = LakeSoulTable::for_name(&table.name).await?;
    for rows in batches {
        handle.execute_upsert(iv_batch(&rows)).await?;
    }
    Ok(table)
}

/// Plans and executes one non-upsert statement (a plain `INSERT`).
async fn run_sql(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    Ok(collect(plan, ctx.task_ctx()).await?)
}

/// Status of one table's metadata, for zero-publication assertions.
async fn state_of(
    pool: &PooledClient,
    client: &MetaDataClientRef,
    table: &TableFixture,
) -> Result<MetadataState> {
    metadata_state(pool, client, table).await
}

// ---------------------------------------------------------------------------
// Single source evaluation
// ---------------------------------------------------------------------------

/// A table provider that counts every physical execution of its scan.
#[derive(Debug)]
struct CountingTable {
    inner: Arc<MemTable>,
    executions: Arc<AtomicUsize>,
}

#[async_trait]
impl TableProvider for CountingTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(CountingExec {
            inner,
            executions: Arc::clone(&self.executions),
        }))
    }
}

/// Wrapper that increments a counter when DataFusion starts executing the
/// source scan. Planning a scan does not count; only real evaluation does.
#[derive(Debug)]
struct CountingExec {
    inner: Arc<dyn ExecutionPlan>,
    executions: Arc<AtomicUsize>,
}

impl DisplayAs for CountingExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CountingExec")
    }
}

impl ExecutionPlan for CountingExec {
    fn name(&self) -> &str {
        "CountingExec"
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )
            -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [inner] = children.as_slice() else {
            return Err(datafusion::error::DataFusionError::Internal(
                "CountingExec expects exactly one child".into(),
            ));
        };
        Ok(Arc::new(Self {
            inner: Arc::clone(inner),
            executions: Arc::clone(&self.executions),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        self.inner.execute(partition, context)
    }
}

/// The proposed-row source must be evaluated exactly once: match, insert and
/// update all consume the same stream, and the old value must be visible in a
/// `SET` expression without a second scan of the source.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_source_is_executed_once() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let dest = create_pk_table(&client, "upsert_single_source", false).await?;
    crate::tests::distributed_write_tests::seed_pk_rows(&dest, &[(1, 10), (2, 20)])
        .await?;

    let ctx = single_node_session(client.clone())?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![5, 30])) as ArrayRef,
        ],
    )?;
    let executions = Arc::new(AtomicUsize::new(0));
    let mem = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
    // The LakeSoul catalog rejects a non-LakeSoul provider on
    // `register_table`, so expose the counting source through a plain memory
    // catalog instead.
    let counting_catalog = Arc::new(datafusion::catalog::MemoryCatalogProvider::new());
    let counting_schema = Arc::new(datafusion::catalog::MemorySchemaProvider::new());
    counting_schema.register_table(
        "source".to_string(),
        Arc::new(CountingTable {
            inner: mem,
            executions: Arc::clone(&executions),
        }),
    )?;
    counting_catalog.register_schema("public", counting_schema)?;
    ctx.register_catalog("counting", counting_catalog);

    let sql = format!(
        "INSERT INTO {table} (id, v) \
         SELECT id, v FROM counting.public.source \
         ON CONFLICT (id) DO UPDATE SET v = {table}.v + EXCLUDED.v",
        table = dest.name
    );
    let batches = run_upsert(&ctx, &dest.name, &sql).await?;
    assert_eq!(
        inserted_rows(&batches),
        2,
        "one conflict update and one insert must both be reported"
    );
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "the source scan must be executed exactly once, not once for a \
         duplicate pre-check and once for the write"
    );
    assert_eq!(
        read_iv_rows(&ctx, &dest.name).await?,
        vec![(1, 15), (2, 20), (3, 30)],
        "the existing key must be updated from its old value and the new key \
         inserted, both from the same single source stream"
    );

    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

// ---------------------------------------------------------------------------
// Duplicate-key state and output batches
// ---------------------------------------------------------------------------

/// Explicit source order decides which repeated key is first, exactly like
/// PostgreSQL: an update followed by a second hit is `21000` even when the
/// later row's `WHERE` is false; a skipped first row leaves the key
/// unmodified, so the second input may update it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_duplicate_keys_follow_source_order() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "upsert_duplicate_order", false).await?;
    crate::tests::distributed_write_tests::seed_pk_rows(&dest, &[(1, 10)]).await?;
    let source = create_iv_source(
        &client,
        "upsert_duplicate_order_src",
        vec![vec![(1, 6), (1, 5)]],
    )
    .await?;

    let ctx = single_node_session(client.clone())?;
    let before = state_of(&pool, &client, &dest).await?;
    let ascending = format!(
        "INSERT INTO {table} (id, v) \
         SELECT id, v FROM {source} ORDER BY v ASC \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v = 5",
        table = dest.name,
        source = source.name
    );
    let error = run_upsert(&ctx, &dest.name, &ascending)
        .await
        .expect_err("the second input key must hit this command's own update");
    let error = error
        .downcast_report::<datafusion::error::DataFusionError>()
        .unwrap_or_else(|error| panic!("expected a typed DataFusion error: {error}"))
        .into_current_context();
    let violation = find_cause::<WriteViolation>(error.find_root())
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert_eq!(
        violation.sqlstate(),
        "21000",
        "a repeated key that hits a row this command modified is 21000"
    );
    assert_eq!(
        read_iv_rows(&ctx, &dest.name).await?,
        vec![(1, 10)],
        "a 21000 source must publish nothing"
    );
    assert_eq!(
        state_of(&pool, &client, &dest).await?,
        before,
        "21000 must leave partition versions and commits untouched"
    );
    assert_disk_equals_active_files(&client, &dest).await?;
    assert_disk_equals_active_files(&client, &source).await?;

    // DESC puts (1,6) first: its WHERE is false, it is skipped without
    // modifying the key, so (1,5) may then update the same row.
    let descending = format!(
        "INSERT INTO {table} (id, v) \
         SELECT id, v FROM {source} ORDER BY v DESC \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.v = 5",
        table = dest.name,
        source = source.name
    );
    let batches = run_upsert(&ctx, &dest.name, &descending).await?;
    assert_eq!(inserted_rows(&batches), 1);
    assert_eq!(read_iv_rows(&ctx, &dest.name).await?, vec![(1, 5)]);
    assert_disk_equals_active_files(&client, &dest).await?;

    client.drop_table(&dest.name, "default").await?;
    client.drop_table(&source.name, "default").await?;
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

/// Small results and results larger than the executor's 1024-row output
/// buffer must both be complete: the state machine flushes incrementally and
/// cannot drop the final batch at EOF.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_output_crosses_batch_boundary() -> Result<()> {
    const ROWS: usize = 1500;
    let client = Arc::new(MetaDataClient::from_env().await?);
    let dest = create_pk_table(&client, "upsert_batch_boundary", false).await?;

    let source = create_iv_source(
        &client,
        "upsert_batch_boundary_src",
        vec![(0..ROWS).map(|row| (row as i32, row as i32 * 10)).collect()],
    )
    .await?;
    let ctx = single_node_session(client.clone())?;
    let sql = format!(
        "INSERT INTO {table} (id, v) SELECT id, v FROM {source} \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        table = dest.name,
        source = source.name
    );
    let batches = run_upsert(&ctx, &dest.name, &sql).await?;
    assert_eq!(
        inserted_rows(&batches),
        ROWS as u64,
        "every proposed row must be written once, including the final batch"
    );
    let rows = read_iv_rows(&ctx, &dest.name).await?;
    assert_eq!(rows.len(), ROWS, "the final output batch must not be lost");
    assert_eq!(rows[0], (0, 0));
    assert_eq!(
        rows[ROWS - 1],
        ((ROWS - 1) as i32, ((ROWS - 1) as i32) * 10)
    );
    let pool = metadata_pool().await?;
    let state = state_of(&pool, &client, &dest).await?;
    assert_eq!(state.uncommitted, 0);
    assert!(
        !state.commits.is_empty(),
        "the statement must publish a commit"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    client.drop_table(&dest.name, "default").await?;
    client.drop_table(&source.name, "default").await?;
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Zero publication
// ---------------------------------------------------------------------------

/// An empty source and a fully filtered conflict action publish nothing at
/// all, while the conflict `WHERE` never filters a genuinely new key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_empty_and_skipped_sources_publish_nothing() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "upsert_zero_publish", false).await?;
    crate::tests::distributed_write_tests::seed_pk_rows(&dest, &[(1, 10)]).await?;
    let ctx = single_node_session(client.clone())?;
    let seeded = state_of(&pool, &client, &dest).await?;

    for (sql, case) in [
        (
            format!(
                "INSERT INTO {table} (id, v) SELECT id, v FROM {table} WHERE false \
                 ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
                table = dest.name
            ),
            "empty source",
        ),
        (
            format!(
                "INSERT INTO {table} (id, v) VALUES (1, 5) \
                 ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE false",
                table = dest.name
            ),
            "conflict WHERE false",
        ),
        (
            format!(
                "INSERT INTO {table} (id, v) VALUES (1, 5) \
                 ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE NULL",
                table = dest.name
            ),
            "conflict WHERE NULL",
        ),
    ] {
        let batches = run_upsert(&ctx, &dest.name, &sql).await?;
        assert_eq!(inserted_rows(&batches), 0, "{case} must report 0");
        assert_eq!(
            read_iv_rows(&ctx, &dest.name).await?,
            vec![(1, 10)],
            "{case} must leave the live relation unchanged"
        );
        assert_eq!(
            state_of(&pool, &client, &dest).await?,
            seeded,
            "{case} must not create files, commits or partition versions"
        );
    }

    // The conflict WHERE filters only conflict rows. The new key 9 is inserted
    // and the existing key 1 keeps its old value.
    let batches = run_upsert(
        &ctx,
        &dest.name,
        &format!(
            "INSERT INTO {table} (id, v) VALUES (1, 5), (9, 90) \
             ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE false",
            table = dest.name
        ),
    )
    .await?;
    assert_eq!(inserted_rows(&batches), 1);
    assert_eq!(
        read_iv_rows(&ctx, &dest.name).await?,
        vec![(1, 10), (9, 90)]
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

// ---------------------------------------------------------------------------
// Source and predicate semantics
// ---------------------------------------------------------------------------

/// A `SELECT DISTINCT` source keeps its own types. Casting `1.1`/`1.2` to the
/// target `INT` before `DISTINCT` would collapse them into one row; PostgreSQL
/// deduplicates the decimals, then the assignment turns both into key 1, so
/// the second input is `21000` and nothing is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_distinct_source_keeps_source_types() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let dest = create_table_fixture(
        &client,
        "upsert_distinct_source",
        schema,
        false,
        vec!["id".to_string()],
    )
    .await?;
    let ctx = single_node_session(client.clone())?;
    let before = state_of(&pool, &client, &dest).await?;
    let sql = format!(
        "INSERT INTO {table} SELECT DISTINCT 1, v \
         FROM (VALUES (1.1), (1.2)) AS s(v) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        table = dest.name
    );
    let error = run_upsert(&ctx, &dest.name, &sql)
        .await
        .expect_err("both distinct decimals become key 1 after the assignment");
    let error = error
        .downcast_report::<datafusion::error::DataFusionError>()
        .unwrap_or_else(|error| panic!("expected a typed DataFusion error: {error}"))
        .into_current_context();
    let violation = find_cause::<WriteViolation>(error.find_root())
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert_eq!(
        violation.sqlstate(),
        "21000",
        "the deduplicated decimals must both become key 1"
    );
    assert_eq!(
        state_of(&pool, &client, &dest).await?,
        before,
        "21000 must not publish a row"
    );
    let remaining = run_sql(
        &ctx,
        &format!("SELECT count(*) AS remaining FROM {}", dest.name),
    )
    .await?;
    assert_eq!(
        datafusion::common::ScalarValue::try_from_array(remaining[0].column(0), 0)?,
        datafusion::common::ScalarValue::Int64(Some(0)),
        "the failed statement must leave the table empty"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

/// A `UNION` source also combines its branches in the original types. Casting
/// each branch to the target `INT` before the union would make the two
/// decimals identical, so `UNION` would deduplicate them into one row instead
/// of producing two inputs for key 1 (`21000`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_union_source_keeps_source_types() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int32, false),
    ]));
    let dest = create_table_fixture(
        &client,
        "upsert_union_source",
        schema,
        false,
        vec!["id".to_string()],
    )
    .await?;
    let ctx = single_node_session(client.clone())?;
    let before = state_of(&pool, &client, &dest).await?;
    let sql = format!(
        "INSERT INTO {table} \
         SELECT 1, v FROM (VALUES (1.1)) AS s(v) \
         UNION \
         SELECT 1, v FROM (VALUES (1.2)) AS s(v) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        table = dest.name
    );
    let error = run_upsert(&ctx, &dest.name, &sql)
        .await
        .expect_err("the union keeps two decimals that both become key 1");
    let error = error
        .downcast_report::<datafusion::error::DataFusionError>()
        .unwrap_or_else(|error| panic!("expected a typed DataFusion error: {error}"))
        .into_current_context();
    let violation = find_cause::<WriteViolation>(error.find_root())
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert_eq!(
        violation.sqlstate(),
        "21000",
        "the union must see distinct source values, not two pre-cast key 1 rows"
    );
    assert_eq!(
        state_of(&pool, &client, &dest).await?,
        before,
        "21000 must not publish a row"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

/// A numeric source is ordered as a number, and only then assigned to a
/// `TEXT` target. Casting the projection to `TEXT` before `ORDER BY` would
/// sort `'2'` above `'10'` and select the wrong row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_numeric_source_orders_before_text_assignment() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let dest = create_table_fixture(
        &client,
        "upsert_text_assignment",
        schema,
        false,
        vec!["id".to_string()],
    )
    .await?;
    let ctx = single_node_session(client.clone())?;
    let sql = format!(
        "INSERT INTO {table} (id, v) \
         SELECT id, v AS x FROM (VALUES (1, 2), (2, 10)) AS s(id, v) \
         ORDER BY x DESC LIMIT 1 \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        table = dest.name
    );
    let batches = run_upsert(&ctx, &dest.name, &sql).await?;
    assert_eq!(inserted_rows(&batches), 1);
    let batches = run_sql(
        &ctx,
        &format!("SELECT id, v FROM {} ORDER BY id", dest.name),
    )
    .await?;
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("the statement must insert one row");
    let id = batch
        .column_by_name("id")
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .expect("id is Int64");
    let value = batch
        .column_by_name("v")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .expect("v is Utf8");
    assert_eq!(id.value(0), 2, "the largest number must sort first");
    assert_eq!(
        value.value(0),
        "10",
        "the assignment to TEXT must happen after the numeric ORDER BY"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

/// A parenthesized `VALUES` source orders inside a nested query. Its pre-sort
/// row positions must not be pinned as the statement order: `DESC` visits
/// `(1,2)` first, updates it, and the later `(1,1)` is `21000`; `ASC` skips
/// `(1,1)` and then updates with `(1,2)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_nested_values_order_matches_source_order() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "upsert_nested_values_order", false).await?;
    crate::tests::distributed_write_tests::seed_pk_rows(&dest, &[(1, 0)]).await?;
    let ctx = single_node_session(client.clone())?;
    let before = state_of(&pool, &client, &dest).await?;

    let descending = format!(
        "INSERT INTO {table} (id, v) \
         (VALUES (1, 1), (1, 2) ORDER BY 2 DESC) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v \
         WHERE EXCLUDED.v = 2",
        table = dest.name
    );
    let error = run_upsert(&ctx, &dest.name, &descending)
        .await
        .expect_err("DESC updates with (1,2) before the later (1,1)");
    let error = error
        .downcast_report::<datafusion::error::DataFusionError>()
        .unwrap_or_else(|error| panic!("expected a typed DataFusion error: {error}"))
        .into_current_context();
    let violation = find_cause::<WriteViolation>(error.find_root())
        .unwrap_or_else(|| panic!("expected a typed write violation, got: {error}"));
    assert_eq!(violation.sqlstate(), "21000");
    assert_eq!(
        read_iv_rows(&ctx, &dest.name).await?,
        vec![(1, 0)],
        "the descending source must publish nothing"
    );
    assert_eq!(
        state_of(&pool, &client, &dest).await?,
        before,
        "the descending source must leave the partition state untouched"
    );

    let ascending = format!(
        "INSERT INTO {table} (id, v) \
         (VALUES (1, 1), (1, 2) ORDER BY 2 ASC) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v \
         WHERE EXCLUDED.v = 2",
        table = dest.name
    );
    let batches = run_upsert(&ctx, &dest.name, &ascending).await?;
    assert_eq!(inserted_rows(&batches), 1);
    assert_eq!(
        read_iv_rows(&ctx, &dest.name).await?,
        vec![(1, 2)],
        "ASC skips (1,1) and then updates the row with (1,2)"
    );
    assert_disk_equals_active_files(&client, &dest).await?;

    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

/// An untyped string predicate takes PostgreSQL's boolean context, while a
/// genuinely typed text value stays `42804` and an invalid spelling is
/// `22P02` before anything is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_untyped_string_predicate_is_boolean() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client, "upsert_string_predicate", false).await?;
    crate::tests::distributed_write_tests::seed_pk_rows(&dest, &[(1, 10)]).await?;
    let ctx = single_node_session(client.clone())?;

    let true_sql = format!(
        "INSERT INTO {table} (id, v) VALUES (1, 5) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE 'true'",
        table = dest.name
    );
    let batches = run_upsert(&ctx, &dest.name, &true_sql).await?;
    assert_eq!(inserted_rows(&batches), 1);
    assert_eq!(
        read_iv_rows(&ctx, &dest.name).await?,
        vec![(1, 5)],
        "'true' must resolve to boolean true"
    );

    let false_sql = format!(
        "INSERT INTO {table} (id, v) VALUES (1, 9) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE 'false'",
        table = dest.name
    );
    let batches = run_upsert(&ctx, &dest.name, &false_sql).await?;
    assert_eq!(inserted_rows(&batches), 0);
    assert_eq!(
        read_iv_rows(&ctx, &dest.name).await?,
        vec![(1, 5)],
        "'false' must skip the conflict row"
    );

    let before = state_of(&pool, &client, &dest).await?;
    let invalid_sql = format!(
        "INSERT INTO {table} (id, v) VALUES (1, 9) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE 'not-bool'",
        table = dest.name
    );
    let error = plan_upsert_sql(
        &ctx,
        &invalid_sql,
        TableReference::bare(dest.name.as_str()),
        SnapshotMode::Capture,
    )
    .await
    .expect_err("an invalid unknown string cannot become a boolean");
    assert_eq!(
        error.sqlstate(),
        "22P02",
        "an invalid untyped boolean spelling is a conversion error"
    );
    assert_eq!(
        state_of(&pool, &client, &dest).await?,
        before,
        "a rejected predicate must not publish"
    );

    let typed_sql = format!(
        "INSERT INTO {table} (id, v) VALUES (1, 9) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE 'true'::text",
        table = dest.name
    );
    let error = plan_upsert_sql(
        &ctx,
        &typed_sql,
        TableReference::bare(dest.name.as_str()),
        SnapshotMode::Capture,
    )
    .await
    .expect_err("typed text is not a boolean predicate");
    assert_eq!(
        error.sqlstate(),
        "42804",
        "a genuinely typed text predicate must stay a datatype mismatch"
    );
    assert_eq!(
        state_of(&pool, &client, &dest).await?,
        before,
        "a rejected predicate must not publish"
    );

    client.drop_table(&dest.name, "default").await?;
    remove_table_dir(&dest);
    Ok(())
}

// ---------------------------------------------------------------------------
// Execution memory
// ---------------------------------------------------------------------------

/// The input is far larger than the statement's execution budget. Sort/join
/// support spilling, so the statement must still succeed and publish exactly
/// once; success below the input size is only possible by spilling.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_spills_within_budget_and_publishes() -> Result<()> {
    const ROWS: i32 = 8192;
    // 32 KB per row: ~256 MB of proposed rows against a 64 MB execution
    // budget, so the sorted proposed stream must spill.
    const LABEL: usize = 32768;
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let dest = create_table_fixture(
        &client,
        "upsert_spill",
        schema.clone(),
        false,
        vec!["id".to_string()],
    )
    .await?;
    let source = create_table_fixture(
        &client,
        "upsert_spill_src",
        schema.clone(),
        false,
        Vec::new(),
    )
    .await?;
    let tail = "x".repeat(LABEL);
    let seed = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..ROWS)) as ArrayRef,
            Arc::new(Int32Array::from_iter_values(0..ROWS)) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|id| format!("{id:08}{tail}")),
            )) as ArrayRef,
        ],
    )?;
    LakeSoulTable::for_name(&source.name)
        .await?
        .execute_upsert(seed)
        .await?;
    // Every proposed key exists, so the conflict branch streams the tiny old
    // rows while the 64 MB proposed stream is what must spill.
    let old = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..ROWS)) as ArrayRef,
            Arc::new(Int32Array::from_iter_values(0..ROWS)) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|_| "s".to_string()),
            )) as ArrayRef,
        ],
    )?;
    LakeSoulTable::for_name(&dest.name)
        .await?
        .execute_upsert(old)
        .await?;

    let factory = LakeSoulSessionFactory::new_with_memory_pool(
        client.clone(),
        &CoreArgs::default(),
        Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(
            64 * 1024 * 1024,
        )),
    )?
    .with_session_template({
        let mut config = upsert_session_template()?.with_batch_size(128);
        // A sort-merge join keeps the matched snapshot tiny; the spilled
        // memory here is then the sorted proposed rows, not a hash table.
        config.options_mut().optimizer.prefer_hash_join = false;
        config
    });
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
    let sql = format!(
        "INSERT INTO {table} (id, v, label) SELECT id, v, label FROM {source} \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        table = dest.name,
        source = source.name
    );
    let (_planned, physical) = upsert_physical_plan(&ctx, &dest.name, &sql).await?;
    let batches = collect(Arc::clone(&physical), ctx.task_ctx()).await?;
    fn spilled_bytes(plan: &dyn ExecutionPlan) -> usize {
        plan.metrics()
            .and_then(|metrics| metrics.spilled_bytes())
            .unwrap_or(0)
            + plan
                .children()
                .iter()
                .map(|child| spilled_bytes(child.as_ref()))
                .sum::<usize>()
    }
    let spilled = spilled_bytes(physical.as_ref());
    assert!(
        spilled > 0,
        "the budgeted upsert must actually spill, not escape its pool"
    );
    eprintln!("upsert_spill_bytes={spilled}");
    assert_eq!(
        inserted_rows(&batches),
        ROWS as u64,
        "an input larger than the budget must spill and still insert every row"
    );
    let state = state_of(&pool, &client, &dest).await?;
    assert_eq!(state.uncommitted, 0);
    assert_eq!(
        state.commits.len(),
        1,
        "one partition desc row, holding the seed and the spilled statement"
    );
    assert_eq!(
        state.commits[0].3, 2,
        "the spilled statement must publish exactly one committed commit"
    );
    assert_disk_equals_active_files(&client, &dest).await?;
    let aggregate = ctx
        .sql(&format!(
            "SELECT count(*), CAST(min(id) AS BIGINT), CAST(max(id) AS BIGINT), \
             sum(CAST(v AS BIGINT)), sum(CAST(length(label) AS BIGINT)) FROM {}",
            dest.name,
        ))
        .await?
        .collect()
        .await?;
    for (index, expected) in [
        i64::from(ROWS),
        0,
        i64::from(ROWS - 1),
        i64::from(ROWS) * i64::from(ROWS - 1) / 2,
        i64::from(ROWS), // the untouched column keeps its one-byte label
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            datafusion::common::ScalarValue::try_from_array(
                aggregate[0].column(index),
                0
            )?,
            datafusion::common::ScalarValue::Int64(Some(expected)),
        );
    }

    client.drop_table(&dest.name, "default").await?;
    client.drop_table(&source.name, "default").await?;
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

/// The same shape with a budget too small to execute fails with a typed
/// resource error and publishes nothing; a partial prefix of rows is never
/// visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_exceeding_memory_fails_without_publication() -> Result<()> {
    const ROWS: i32 = 4096;
    let client = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let dest = create_table_fixture(
        &client,
        "upsert_memory",
        schema.clone(),
        false,
        vec!["id".to_string()],
    )
    .await?;
    let source = create_table_fixture(
        &client,
        "upsert_memory_src",
        schema.clone(),
        false,
        Vec::new(),
    )
    .await?;
    let tail = "x".repeat(504);
    let seed = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from_iter_values(0..ROWS)) as ArrayRef,
            Arc::new(Int32Array::from_iter_values(0..ROWS)) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                (0..ROWS).map(|id| format!("{id:08}{tail}")),
            )) as ArrayRef,
        ],
    )?;
    LakeSoulTable::for_name(&source.name)
        .await?
        .execute_upsert(seed)
        .await?;

    let factory = LakeSoulSessionFactory::new_with_memory_pool(
        client.clone(),
        &CoreArgs::default(),
        Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(
            64 * 1024,
        )),
    )?
    .with_session_template(upsert_session_template()?.with_batch_size(ROWS as usize));
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
    let sql = format!(
        "INSERT INTO {table} (id, v, label) SELECT id, v, label FROM {source} \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
        table = dest.name,
        source = source.name
    );
    let before = state_of(&pool, &client, &dest).await?;
    let result = run_upsert(&ctx, &dest.name, &sql).await;
    let after = state_of(&pool, &client, &dest).await?;
    assert_eq!(after, before, "memory exhaustion must not publish a prefix");
    assert_disk_equals_active_files(&client, &dest).await?;
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

    client.drop_table(&dest.name, "default").await?;
    client.drop_table(&source.name, "default").await?;
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

// ---------------------------------------------------------------------------
// Cross-coordinator races
// ---------------------------------------------------------------------------

/// Two coordinators race to insert the same key that is absent from the
/// captured snapshot. The loser must receive the retryable `40001` instead of
/// silently overwriting, and a whole-statement retry must branch from the new
/// snapshot.
#[test]
fn upsert_first_key_race_is_retryable_and_retries_the_new_snapshot() {
    run_distributed_test(
        upsert_first_key_race_is_retryable_and_retries_the_new_snapshot_inner(),
    );
}

async fn upsert_first_key_race_is_retryable_and_retries_the_new_snapshot_inner()
-> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "upsert_first_key_race", false).await?;
    let source =
        create_iv_source(&client_a, "upsert_first_key_race_src", vec![vec![(1, 10)]])
            .await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let ctx_a = distributed_upsert_session(client_a.clone(), worker_urls)?;
    let sql = format!(
        "INSERT INTO {table} (id, v) SELECT id, v FROM {source} \
         ON CONFLICT (id) DO UPDATE SET v = {table}.v + EXCLUDED.v",
        table = dest.name,
        source = source.name
    );
    let (_planned, plan) = upsert_physical_plan(&ctx_a, &dest.name, &sql).await?;
    let shape = write_plan_shape(&plan);
    assert!(
        shape.write_id.is_some(),
        "a distributed upsert must keep a write node:\n{}",
        shape.explain
    );
    let mut stream = execute_stream(Arc::clone(&plan), ctx_a.task_ctx())?;
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "the first-key upsert parked its first upload at the gate",
    )
    .await;

    let ctx_b = single_node_session(client_b.clone())?;
    let won = run_upsert(&ctx_b, &dest.name, &sql).await?;
    assert_eq!(inserted_rows(&won), 1);
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 10)],
        "the competing coordinator must insert the key before the gate opens"
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
    let error = failure.expect("the stale-snapshot upsert must fail");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(
        conflict.sqlstate(),
        "40001",
        "a lost first-key race must be retryable, got: {error}"
    );
    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("raced upsert worker {index}"))
            .await?;
    }

    let after_race = state_of(&pool, &client_b, &dest).await?;
    assert_eq!(
        after_race.commits.len(),
        1,
        "only the winner may have published during the race"
    );
    assert_eq!((after_race.commits[0].1, after_race.commits[0].3), (1, 1));
    assert_eq!(after_race.uncommitted, 0);
    assert_disk_equals_active_files(&client_b, &dest).await?;

    // The retry sees the winner's row and takes the update branch, so the
    // retry is not a duplicate insert.
    let retried = run_upsert(&ctx_b, &dest.name, &sql).await?;
    assert_eq!(inserted_rows(&retried), 1);
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 20)],
        "the retry must add to the row the winner published"
    );
    let after_retry = state_of(&pool, &client_b, &dest).await?;
    assert_eq!(after_retry.commits.len(), 1);
    assert_eq!((after_retry.commits[0].1, after_retry.commits[0].3), (2, 2));
    assert_eq!(after_retry.uncommitted, 0);
    assert_disk_equals_active_files(&client_b, &dest).await?;

    workers.abort_all();
    client_b.drop_table(&dest.name, "default").await?;
    client_b.drop_table(&source.name, "default").await?;
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

/// Two coordinators update the same existing key from the same old value; the
/// loser must not publish a lost increment. Retrying applies the second
/// update to the new value.
#[test]
fn upsert_existing_key_race_is_retryable_and_retries_the_new_snapshot() {
    run_distributed_test(
        upsert_existing_key_race_is_retryable_and_retries_the_new_snapshot_inner(),
    );
}

async fn upsert_existing_key_race_is_retryable_and_retries_the_new_snapshot_inner()
-> Result<()> {
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "upsert_existing_key_race", false).await?;
    crate::tests::distributed_write_tests::seed_pk_rows(&dest, &[(1, 10)]).await?;
    let source = create_iv_source(
        &client_a,
        "upsert_existing_key_race_src",
        vec![vec![(1, 5)]],
    )
    .await?;
    let seeded = state_of(&pool, &client_a, &dest).await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let ctx_a = distributed_upsert_session(client_a.clone(), worker_urls)?;
    let sql = format!(
        "INSERT INTO {table} (id, v) SELECT id, v FROM {source} \
         ON CONFLICT (id) DO UPDATE SET v = {table}.v + EXCLUDED.v",
        table = dest.name,
        source = source.name
    );
    let (_planned, plan) = upsert_physical_plan(&ctx_a, &dest.name, &sql).await?;
    let mut stream = execute_stream(Arc::clone(&plan), ctx_a.task_ctx())?;
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "the existing-key upsert parked its first upload at the gate",
    )
    .await;

    let ctx_b = single_node_session(client_b.clone())?;
    let won = run_upsert(&ctx_b, &dest.name, &sql).await?;
    assert_eq!(inserted_rows(&won), 1);
    assert_eq!(read_iv_rows(&ctx_b, &dest.name).await?, vec![(1, 15)]);

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
    let error = failure.expect("the stale-snapshot upsert must fail");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(conflict.sqlstate(), "40001");
    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("raced upsert worker {index}"))
            .await?;
    }

    let after_race = state_of(&pool, &client_b, &dest).await?;
    assert_eq!(
        after_race.versions,
        vec![(seeded.versions[0].0.clone(), 1, 2)],
        "the loser must not append a version"
    );
    assert_eq!(after_race.commits.len(), seeded.commits.len());
    assert_eq!(
        (after_race.commits[0].1, after_race.commits[0].3),
        (seeded.commits[0].1 + 1, seeded.commits[0].3 + 1),
    );
    assert_eq!(after_race.uncommitted, 0);
    assert_eq!(read_iv_rows(&ctx_b, &dest.name).await?, vec![(1, 15)]);
    assert_disk_equals_active_files(&client_b, &dest).await?;

    let retried = run_upsert(&ctx_b, &dest.name, &sql).await?;
    assert_eq!(inserted_rows(&retried), 1);
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 20)],
        "the retry must increment the winner's value, so both updates count"
    );
    let after_retry = state_of(&pool, &client_b, &dest).await?;
    assert_eq!(
        after_retry.versions,
        vec![(seeded.versions[0].0.clone(), 2, 3)]
    );
    assert_eq!(after_retry.uncommitted, 0);
    assert_disk_equals_active_files(&client_b, &dest).await?;

    workers.abort_all();
    client_b.drop_table(&dest.name, "default").await?;
    client_b.drop_table(&source.name, "default").await?;
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}

/// An ordinary insert of a different key is still a concurrent writer of the
/// same partition, so the parked upsert must fail honestly with `40001`
/// instead of publishing from its stale snapshot.
#[test]
fn upsert_conflicts_with_a_plain_writer_of_the_same_partition() {
    run_distributed_test(
        upsert_conflicts_with_a_plain_writer_of_the_same_partition_inner(),
    );
}

async fn upsert_conflicts_with_a_plain_writer_of_the_same_partition_inner() -> Result<()>
{
    let client_a = Arc::new(MetaDataClient::from_env().await?);
    let client_b = Arc::new(MetaDataClient::from_env().await?);
    let pool = metadata_pool().await?;
    let dest = create_pk_table(&client_a, "upsert_plain_writer_race", false).await?;
    crate::tests::distributed_write_tests::seed_pk_rows(&dest, &[(1, 10)]).await?;
    let source = create_iv_source(
        &client_a,
        "upsert_plain_writer_race_src",
        vec![vec![(1, 5)]],
    )
    .await?;

    let (gate_tx, gate_rx) = watch::channel(false);
    let (worker_urls, probes, mut workers) =
        spawn_gated_workers(WRITE_WORKERS, gate_rx).await?;
    let mut observers = connect_observers(&worker_urls).await?;
    let ctx_a = distributed_upsert_session(client_a.clone(), worker_urls)?;
    let sql = format!(
        "INSERT INTO {table} (id, v) SELECT id, v FROM {source} \
         ON CONFLICT (id) DO UPDATE SET v = {table}.v + EXCLUDED.v",
        table = dest.name,
        source = source.name
    );
    let (_planned, plan) = upsert_physical_plan(&ctx_a, &dest.name, &sql).await?;
    let mut stream = execute_stream(Arc::clone(&plan), ctx_a.task_ctx())?;
    wait_until(
        || probes.iter().any(|probe| probe.arrived() > 0),
        "the upsert parked its first upload at the gate",
    )
    .await;

    // A plain INSERT of a different key still advances the only partition's
    // version; the parked statement's snapshot is now stale.
    let ctx_b = single_node_session(client_b.clone())?;
    let inserted = run_sql(
        &ctx_b,
        &format!(
            "INSERT INTO {table} (id, v) VALUES (2, 20)",
            table = dest.name
        ),
    )
    .await?;
    assert_eq!(inserted_rows(&inserted), 1);

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
    let error = failure.expect("the stale-snapshot upsert must fail");
    let conflict = find_cause::<WriteConflict>(&error)
        .unwrap_or_else(|| panic!("expected a retryable write conflict, got: {error}"));
    assert_eq!(conflict.sqlstate(), "40001");
    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_worker_drained(observer, &format!("raced upsert worker {index}"))
            .await?;
    }
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 10), (2, 20)],
        "the loser must not apply its update"
    );
    let after = state_of(&pool, &client_b, &dest).await?;
    assert_eq!(after.uncommitted, 0);
    assert_disk_equals_active_files(&client_b, &dest).await?;

    let retried = run_upsert(&ctx_b, &dest.name, &sql).await?;
    assert_eq!(inserted_rows(&retried), 1);
    assert_eq!(
        read_iv_rows(&ctx_b, &dest.name).await?,
        vec![(1, 15), (2, 20)]
    );

    workers.abort_all();
    client_b.drop_table(&dest.name, "default").await?;
    client_b.drop_table(&source.name, "default").await?;
    remove_table_dir(&dest);
    remove_table_dir(&source);
    Ok(())
}
