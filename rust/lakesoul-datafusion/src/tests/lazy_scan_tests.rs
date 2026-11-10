// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `LakeSoulTableProvider::scan` must plan from metadata descriptors alone.
//!
//! Every test swaps the session's `file://` object store for a gate that
//! refuses (and counts) HEAD/GET/range/list traffic. Planning, optimization,
//! and calling `ExecutionPlan::execute` without polling must leave the gate
//! untouched; only consuming the first stream may read files. The exact rows
//! read afterwards pin the snapshot, version order, append-only splitting,
//! format changes, and schema evolution that the descriptor path must keep.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use futures::stream::BoxStream;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::lakesoul_scan::LakeSoulScanExec;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use tokio::runtime::Runtime;

use crate::Result;
use crate::cli::CoreArgs;
use crate::lakesoul_table::LakeSoulTable;
use crate::tests::create_table_with_file_format;
use crate::tests::mor_nullability_tests::write_storage_batch;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

fn batch(parts: &[i32], ids: &[i32], values: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(parts.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(values.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn unique_table(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// What the object store is allowed to answer. Planning flips it off; the
/// read phase flips it on.
#[derive(Debug, Default)]
struct IoGate {
    allow: AtomicBool,
    head: AtomicUsize,
    get: AtomicUsize,
    range: AtomicUsize,
    list: AtomicUsize,
}

impl IoGate {
    fn allow(&self) {
        self.allow.store(true, Ordering::SeqCst);
    }

    fn deny(&self) {
        self.allow.store(false, Ordering::SeqCst);
    }

    fn assert_untouched(&self) {
        assert_eq!(self.head.load(Ordering::SeqCst), 0, "unexpected HEAD");
        assert_eq!(self.get.load(Ordering::SeqCst), 0, "unexpected GET");
        assert_eq!(self.range.load(Ordering::SeqCst), 0, "unexpected range GET");
        assert_eq!(self.list.load(Ordering::SeqCst), 0, "unexpected LIST");
    }

    fn reads(&self) -> usize {
        self.head.load(Ordering::SeqCst)
            + self.get.load(Ordering::SeqCst)
            + self.range.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct GatedStore {
    inner: Arc<dyn ObjectStore>,
    gate: Arc<IoGate>,
}

impl GatedStore {
    fn error(operation: &'static str) -> object_store::Error {
        object_store::Error::Generic {
            store: "lazy-scan-gate",
            source: format!("metadata-only scan issued a {operation}").into(),
        }
    }

    fn check(&self, operation: &'static str) -> object_store::Result<()> {
        if self.gate.allow.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(Self::error(operation))
        }
    }
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lazy-scan-gate")
    }
}

#[async_trait::async_trait]
impl ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check("PUT")?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check("multipart PUT")?;
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            self.gate.head.fetch_add(1, Ordering::SeqCst);
            self.check("HEAD")?;
        } else if options.range.is_some() {
            self.gate.range.fetch_add(1, Ordering::SeqCst);
            self.check("range GET")?;
        } else {
            self.gate.get.fetch_add(1, Ordering::SeqCst);
            self.check("GET")?;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.gate.list.fetch_add(1, Ordering::SeqCst);
        if let Err(error) = self.check("LIST") {
            return futures::stream::once(async move { Err(error) }).boxed();
        }
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.gate.list.fetch_add(1, Ordering::SeqCst);
        self.check("LIST")?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check("COPY")?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.check("RENAME")?;
        self.inner.rename_opts(from, to, options).await
    }
}

async fn session_with_gate(
    client: MetaDataClientRef,
    gate: Arc<IoGate>,
) -> Result<Arc<SessionContext>> {
    let config = crate::create_lakesoul_session_config()?;
    let ctx = crate::create_lakesoul_session_ctx_with_config(
        client,
        &CoreArgs::default(),
        config,
    )?;
    ctx.register_object_store(
        &url::Url::parse("file://").unwrap(),
        Arc::new(GatedStore {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            gate,
        }),
    );
    Ok(ctx)
}

async fn seed_pk(
    client: MetaDataClientRef,
    name: &str,
    file_format: PhysicalFormat,
) -> Result<LakeSoulTable> {
    create_table_with_file_format(
        client,
        name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(schema())
            .with_primary_keys(vec!["id".to_string()])
            .with_range_partitions(vec!["part".to_string()])
            .build(),
        file_format,
    )
    .await?;
    let table = LakeSoulTable::for_name(name).await?;
    table
        .execute_upsert(batch(&[0, 1], &[1, 2], &[1, 2]))
        .await?;
    table
        .execute_upsert(batch(&[0, 1], &[1, 2], &[100, 200]))
        .await?;
    Ok(table)
}

async fn seed_append(
    client: MetaDataClientRef,
    name: &str,
    file_format: PhysicalFormat,
) -> Result<LakeSoulTable> {
    create_table_with_file_format(
        client,
        name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(schema())
            .with_range_partitions(vec!["part".to_string()])
            .build(),
        file_format,
    )
    .await?;
    let table = LakeSoulTable::for_name(name).await?;
    table.execute_upsert(batch(&[0], &[1], &[10])).await?;
    Ok(table)
}

async fn plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap()
}

fn has_lazy_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    plan.apply(|node| {
        found |= node.is::<LakeSoulScanExec>();
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

fn rows(batches: &[RecordBatch]) -> Vec<(i32, i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let parts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend(
            (0..batch.num_rows())
                .map(|row| (parts.value(row), ids.value(row), values.value(row))),
        );
    }
    rows.sort();
    rows
}

fn run(inner: impl Future<Output = Result<()>>) {
    Runtime::new().unwrap().block_on(inner).unwrap();
}

#[test]
fn lazy_scan_is_metadata_only_until_first_poll() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("lazy_scan");
        seed_pk(client.clone(), &table_name, PhysicalFormat::Parquet).await?;

        let gate = Arc::new(IoGate::default());
        gate.deny();
        let ctx = session_with_gate(client, gate.clone()).await?;

        let sql = format!("SELECT * FROM {table_name} ORDER BY id");
        let scan_plan = plan(&ctx, &sql).await;
        assert!(
            has_lazy_scan(&scan_plan),
            "scan did not stay lazy:\n{scan_plan:?}"
        );

        // `execute` must only build a lazy lane; no child is opened before the
        // stream is polled.
        for partition in 0..scan_plan.output_partitioning().partition_count() {
            let stream = scan_plan.execute(partition, ctx.task_ctx()).unwrap();
            drop(stream);
        }
        gate.assert_untouched();

        // A primary-key predicate takes the locator path. Its candidates are
        // metadata descriptors as well, so planning still touches nothing.
        let pk_plan =
            plan(&ctx, &format!("SELECT * FROM {table_name} WHERE id = 1")).await;
        gate.assert_untouched();

        gate.allow();
        assert_eq!(
            rows(&collect(scan_plan, ctx.task_ctx()).await.unwrap()),
            vec![(0, 1, 100), (1, 2, 200)]
        );
        assert!(gate.reads() > 0, "the read phase never opened a file");
        assert_eq!(
            gate.list.load(Ordering::SeqCst),
            0,
            "reads must come from descriptors, not object-store listing"
        );

        let before = gate.reads();
        assert_eq!(
            rows(&collect(pk_plan, ctx.task_ctx()).await.unwrap()),
            vec![(0, 1, 100)]
        );
        assert!(gate.reads() > before, "the locator read no candidates");
        Ok(())
    });
}

#[test]
fn lazy_scan_keeps_the_planned_snapshot() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("lazy_snapshot");
        create_table_with_file_format(
            client.clone(),
            &table_name,
            LakeSoulIOConfigBuilder::new()
                .with_schema(schema())
                .with_primary_keys(vec!["id".to_string()])
                .with_range_partitions(vec!["part".to_string()])
                .build(),
            PhysicalFormat::Parquet,
        )
        .await?;
        let table = LakeSoulTable::for_name(&table_name).await?;
        table
            .execute_upsert(batch(&[0, 1], &[1, 2], &[1, 2]))
            .await?;

        let gate = Arc::new(IoGate::default());
        gate.deny();
        let ctx = session_with_gate(client, gate.clone()).await?;
        let sql = format!("SELECT * FROM {table_name} ORDER BY id");
        let plan = plan(&ctx, &sql).await;
        gate.assert_untouched();

        // Commit a newer version after the plan froze its units. The plan
        // must still read the snapshot it selected at planning time.
        table
            .execute_upsert(batch(&[0, 1], &[1, 2], &[100, 200]))
            .await?;

        gate.allow();
        assert_eq!(
            rows(&collect(plan, ctx.task_ctx()).await.unwrap()),
            vec![(0, 1, 1), (1, 2, 2)]
        );
        Ok(())
    });
}

#[test]
fn lazy_scan_missing_data_file_fails_on_first_poll() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("lazy_missing");
        let table = seed_pk(client.clone(), &table_name, PhysicalFormat::Parquet).await?;

        let table_url = url::Url::parse(&table.table_info().table_path)?;
        let table_dir = table_url
            .to_file_path()
            .map_err(|_| rootcause::report!("table path is not a local file path"))?;
        let data_file =
            find_file(&table_dir, "parquet").expect("seeded table has a parquet file");
        std::fs::remove_file(&data_file)?;

        let gate = Arc::new(IoGate::default());
        gate.deny();
        let ctx = session_with_gate(client, gate.clone()).await?;
        let plan = plan(&ctx, &format!("SELECT * FROM {table_name}")).await;
        gate.assert_untouched();

        gate.allow();
        let error = collect(plan, ctx.task_ctx()).await.unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("not found")
                || error.to_string().to_lowercase().contains("no such file"),
            "unexpected missing-file error: {error}"
        );
        Ok(())
    });
}

#[test]
fn lazy_scan_empty_table_never_touches_object_store() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("lazy_empty");
        create_table_with_file_format(
            client.clone(),
            &table_name,
            LakeSoulIOConfigBuilder::new()
                .with_schema(schema())
                .with_primary_keys(vec!["id".to_string()])
                .with_range_partitions(vec!["part".to_string()])
                .build(),
            PhysicalFormat::Parquet,
        )
        .await?;

        let gate = Arc::new(IoGate::default());
        gate.deny();
        let ctx = session_with_gate(client, gate.clone()).await?;
        let sql = format!("SELECT * FROM {table_name}");
        let plan = plan(&ctx, &sql).await;
        for partition in 0..plan.output_partitioning().partition_count() {
            drop(plan.execute(partition, ctx.task_ctx()).unwrap());
        }
        assert!(collect(plan, ctx.task_ctx()).await.unwrap().is_empty());
        gate.assert_untouched();
        Ok(())
    });
}

#[test]
fn lazy_scan_append_only_mixed_formats_keep_rows() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("lazy_append_mixed");
        let table =
            seed_append(client.clone(), &table_name, PhysicalFormat::Parquet).await?;

        // A second commit with a different physical format must stay in
        // snapshot order; the lazy append-only units split only at format
        // boundaries, never inside a run.
        write_storage_batch(
            client.clone(),
            &table,
            PhysicalFormat::Vortex,
            batch(&[1], &[2], &[20]),
        )
        .await?;

        let gate = Arc::new(IoGate::default());
        gate.deny();
        let ctx = session_with_gate(client, gate.clone()).await?;
        let plan = plan(&ctx, &format!("SELECT * FROM {table_name} ORDER BY id")).await;
        gate.assert_untouched();

        gate.allow();
        assert_eq!(
            rows(&collect(plan, ctx.task_ctx()).await.unwrap()),
            vec![(0, 1, 10), (1, 2, 20)]
        );
        Ok(())
    });
}

#[test]
fn lazy_scan_evolved_column_reads_nulls_for_old_files() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("lazy_evolved");
        let evolved_schema = Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, false),
            Field::new("evolved", DataType::Int32, true),
        ]));
        create_table_with_file_format(
            client.clone(),
            &table_name,
            LakeSoulIOConfigBuilder::new()
                .with_schema(evolved_schema.clone())
                .with_primary_keys(vec!["id".to_string()])
                .with_range_partitions(vec!["part".to_string()])
                .build(),
            PhysicalFormat::Parquet,
        )
        .await?;
        let table = LakeSoulTable::for_name(&table_name).await?;
        // The first file predates `evolved`; the second supplies it.
        write_storage_batch(
            client.clone(),
            &table,
            PhysicalFormat::Parquet,
            batch(&[0], &[1], &[10]),
        )
        .await?;
        let with_evolved = RecordBatch::try_new(
            evolved_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![0])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
                Arc::new(Int32Array::from(vec![20])) as ArrayRef,
                Arc::new(Int32Array::from(vec![200])) as ArrayRef,
            ],
        )
        .unwrap();
        write_storage_batch(
            client.clone(),
            &table,
            PhysicalFormat::Parquet,
            with_evolved,
        )
        .await?;

        let gate = Arc::new(IoGate::default());
        gate.deny();
        let ctx = session_with_gate(client, gate.clone()).await?;
        let plan = plan(
            &ctx,
            &format!("SELECT id, evolved FROM {table_name} ORDER BY id"),
        )
        .await;
        gate.assert_untouched();

        gate.allow();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let evolved = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend((0..batch.num_rows()).map(|row| {
                (
                    ids.value(row),
                    (!evolved.is_null(row)).then(|| evolved.value(row)),
                )
            }));
        }
        assert_eq!(rows, vec![(1, None), (2, Some(200))]);
        Ok(())
    });
}

fn find_file(root: &Path, extension: &str) -> Option<PathBuf> {
    for entry in std::fs::read_dir(root).ok()? {
        let entry = entry.ok()?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_file(&path, extension) {
                return Some(found);
            }
        } else if path.extension().is_some_and(|ext| ext == extension) {
            return Some(path);
        }
    }
    None
}

/// The row cap the plan's lazy leaf carries, if any.
/// Every node of one scan applies the *query's* key-lookup threshold, not the
/// unit count its own node happens to carry.
#[test]
fn pk_scan_splits_the_lookup_threshold_over_its_units() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("locator_threshold");
        seed_pk(client.clone(), &table_name, PhysicalFormat::Parquet).await?;
        let gate = Arc::new(IoGate::default());
        gate.allow();
        let ctx = session_with_gate(client, gate.clone()).await?;

        let scan_plan = plan(&ctx, &format!("SELECT * FROM {table_name}")).await;
        let mut unit_count = 0;
        let mut thresholds = Vec::new();
        scan_plan
            .apply(|node| {
                if let Some(scan) = node.downcast_ref::<LakeSoulScanExec>() {
                    unit_count += scan.work_units().len();
                    thresholds.push(scan.scan_config().min_index_rows);
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("walking a physical plan cannot fail");

        assert!(!thresholds.is_empty(), "the plan has no metadata-only scan");
        assert!(
            unit_count > 1,
            "the fixture has to scan more than one work unit"
        );
        let expected = lakesoul_io::pk_locator::min_index_rows_per_unit(unit_count);
        assert!(
            expected < lakesoul_io::pk_locator::MIN_INDEX_ROWS,
            "a multi-unit scan splits the query-wide threshold"
        );
        assert_eq!(
            thresholds,
            vec![expected; thresholds.len()],
            "each node applies the query's share, not its own unit count"
        );
        gate.assert_untouched();
        Ok(())
    });
}

fn leaf_limit(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let mut limit = None;
    plan.apply(|node| {
        if let Some(scan) = node.downcast_ref::<LakeSoulScanExec>() {
            limit = scan.scan_config().limit;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a physical plan cannot fail");
    limit
}

/// A consumer's `LIMIT` reaches the append-only leaves, so a leaf stops
/// reading once the cap is met. `EXPLAIN` has to say so too: the cap is the
/// only difference between a scan that may stop early and one that may not.
#[test]
fn append_only_scan_forwards_the_limit_to_its_leaves() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("limit_append");
        let table =
            seed_append(client.clone(), &table_name, PhysicalFormat::Parquet).await?;
        // Three more rows, so a cap has something to cut.
        table
            .execute_upsert(batch(&[0, 0, 0], &[2, 3, 4], &[20, 30, 40]))
            .await?;

        let gate = Arc::new(IoGate::default());
        gate.deny();
        let ctx = session_with_gate(client, gate.clone()).await?;

        let sql = format!("SELECT * FROM {table_name} LIMIT 2");
        let scan_plan = plan(&ctx, &sql).await;
        assert!(
            has_lazy_scan(&scan_plan),
            "scan did not stay lazy:\n{scan_plan:?}"
        );
        assert_eq!(
            leaf_limit(&scan_plan),
            Some(2),
            "the consumer's LIMIT must reach the leaves"
        );
        let explained =
            datafusion_distributed::display_plan_ascii(scan_plan.as_ref(), false);
        assert!(
            explained.contains("limit=2"),
            "EXPLAIN must show the pushed cap:\n{explained}"
        );
        gate.assert_untouched();

        gate.allow();
        let batches = collect(scan_plan, ctx.task_ctx()).await?;
        assert_eq!(rows(&batches).len(), 2, "LIMIT 2 returns exactly 2 rows");

        // A predicate makes the scan drop rows itself, so a leaf must not stop
        // early: it could not be topped up above the scan.
        let sql = format!("SELECT * FROM {table_name} WHERE v > 0 LIMIT 2");
        let scan_plan = plan(&ctx, &sql).await;
        assert_eq!(
            leaf_limit(&scan_plan),
            None,
            "a filtered scan must not cap its leaves"
        );
        let batches = collect(scan_plan, ctx.task_ctx()).await?;
        assert_eq!(rows(&batches).len(), 2);
        Ok(())
    });
}

/// A merge has to see every version of a key, so its inputs may never stop
/// early, whatever the consumer asked for.
#[test]
fn merge_on_read_scan_never_caps_its_leaves() {
    run(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = unique_table("limit_merge");
        seed_pk(client.clone(), &table_name, PhysicalFormat::Parquet).await?;
        let gate = Arc::new(IoGate::default());
        let ctx = session_with_gate(client, gate.clone()).await?;

        let sql = format!("SELECT * FROM {table_name} LIMIT 1");
        let scan_plan = plan(&ctx, &sql).await;
        assert_eq!(
            leaf_limit(&scan_plan),
            None,
            "a merge must read every version, never a capped prefix"
        );
        gate.allow();
        let batches = collect(scan_plan, ctx.task_ctx()).await?;
        assert_eq!(rows(&batches).len(), 1);
        Ok(())
    });
}
