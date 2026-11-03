// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A metadata-only scan resolves file metadata only when a consumer polls it.
//!
//! Every test here drives the real node: real parquet files, a real object
//! store (wrapped to count requests), and batches read out of the stream. The
//! assertions are about observable behavior — which requests happened before
//! the first poll, which rows and schemas came out, which descriptors are
//! rejected — not about how the node is wired.

use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use futures::stream::BoxStream;
use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::{
    LakeSoulScanConfig, LakeSoulScanExec, MIN_PARQUET_RANGE_BYTES,
    PARQUET_READ_OPTION_PREFIX, ScanFile, ScanWorkUnit, capture_read_options,
    restore_read_options,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as StoreResult,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Object store that records every request, so "no I/O yet" is observable.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CountingStore {
    inner: LocalFileSystem,
    calls: Mutex<Vec<(&'static str, String)>>,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: LocalFileSystem::new(),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, operation: &'static str, location: &StorePath) {
        self.calls
            .lock()
            .expect("request log")
            .push((operation, location.to_string()));
    }

    fn calls(&self) -> Vec<(&'static str, String)> {
        self.calls.lock().expect("request log").clone()
    }

    fn touched(&self, path: &str) -> bool {
        self.calls().iter().any(|(_, location)| location == path)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("counting-store")
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> StoreResult<GetResult> {
        self.record(if options.head { "head" } else { "get" }, location);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<u64>],
    ) -> StoreResult<Vec<Bytes>> {
        self.record("get_ranges", location);
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&StorePath>,
        offset: &StorePath,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: RenameOptions,
    ) -> StoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

/// A task context whose object stores record their requests.
fn task_context(store: &Arc<CountingStore>) -> Arc<TaskContext> {
    let runtime = RuntimeEnv::default();
    runtime.register_object_store(
        &url::Url::parse("file://").expect("local file url"),
        Arc::clone(store) as Arc<dyn ObjectStore>,
    );
    Arc::new(TaskContext::new(
        None,
        "lazy-scan-tests".to_string(),
        SessionConfig::default(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(runtime),
    ))
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

fn write_parquet(
    dir: &Path,
    name: &str,
    batch: &RecordBatch,
    row_group_rows: usize,
) -> (String, u64) {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let properties = WriterProperties::builder()
        .set_max_row_group_size(row_group_rows)
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))
        .expect("parquet writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
    let size = std::fs::metadata(&path).expect("file metadata").len();
    let url = format!("file://{}", path.display());
    (url, size)
}

/// The store-relative path a full `file://` URL addresses.
fn store_path(url: &str) -> String {
    url.strip_prefix("file://")
        .expect("a file url")
        .trim_start_matches('/')
        .to_string()
}

fn scan_file(url: &str, size: Option<u64>, range: Option<Range<i64>>) -> ScanFile {
    ScanFile {
        path: url.to_string(),
        physical_format: PhysicalFormat::Parquet,
        size,
        range,
    }
}

fn fake_parquet(dir: &str, name: &str, size: u64) -> ScanFile {
    ScanFile {
        path: format!("file:///lakesoul-split-tests/{dir}/{name}"),
        physical_format: PhysicalFormat::Parquet,
        size: Some(size),
        range: None,
    }
}

fn unit(files: Vec<ScanFile>, lane: usize) -> ScanWorkUnit {
    ScanWorkUnit {
        files,
        default_column_value: HashMap::new(),
        lane,
    }
}

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn int_batch(schema: &SchemaRef, rows: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int64Array::from(rows.to_vec()))],
    )
    .expect("valid int fixture")
}

fn int32_batch(schema: &SchemaRef, rows: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int32Array::from(rows.to_vec()))],
    )
    .expect("valid int32 fixture")
}

fn append_only_config(
    table_schema: SchemaRef,
    partition_schema: SchemaRef,
) -> LakeSoulScanConfig {
    LakeSoulScanConfig {
        output_schema: Arc::clone(&table_schema),
        table_schema: Arc::clone(&table_schema),
        partition_schema,
        io_config: LakeSoulIOConfig::builder()
            .with_schema(table_schema)
            .build(),
        predicate: None,
        key_constraint: None,
        parquet_force_view_types: false,
        read_options: HashMap::new(),
        limit: None,
    }
}

/// A merge-on-read configuration for the sparse-version fixture.
fn merge_config(table_schema: SchemaRef) -> LakeSoulScanConfig {
    LakeSoulScanConfig {
        output_schema: Arc::clone(&table_schema),
        table_schema: Arc::clone(&table_schema),
        partition_schema: Arc::new(Schema::empty()),
        io_config: LakeSoulIOConfig::builder()
            .with_schema(table_schema)
            .with_primary_keys(vec!["id".to_string()])
            .with_merge_op("v".to_string(), "UseLast".to_string())
            .with_merge_op("n".to_string(), "UseLast".to_string())
            .with_merge_op("op".to_string(), "UseLast".to_string())
            .with_batch_size(2)
            .with_thread_num(1)
            .build(),
        predicate: None,
        key_constraint: None,
        parquet_force_view_types: false,
        read_options: HashMap::new(),
        limit: None,
    }
}

// ---------------------------------------------------------------------------
// Reading helpers.
// ---------------------------------------------------------------------------

async fn collect(
    plan: &LakeSoulScanExec,
    lane: usize,
    store: &Arc<CountingStore>,
) -> Vec<RecordBatch> {
    let mut stream = plan
        .execute(lane, task_context(store))
        .expect("execute the scan");
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.expect("the scan reads its unit"));
    }
    batches
}

async fn collect_all_lanes(
    plan: &LakeSoulScanExec,
    store: &Arc<CountingStore>,
) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    for lane in 0..plan.output_partitioning().partition_count() {
        batches.extend(collect(plan, lane, store).await);
    }
    batches
}

async fn first_error(
    plan: &LakeSoulScanExec,
    lane: usize,
    store: &Arc<CountingStore>,
) -> String {
    let mut stream = plan
        .execute(lane, task_context(store))
        .expect("execute returns a stream; the failure belongs to the first poll");
    loop {
        match stream.next().await {
            Some(Ok(_)) => continue,
            Some(Err(error)) => return error.to_string(),
            None => panic!("the scan ended without reporting an error"),
        }
    }
}

fn int64_rows(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
    let mut rows = Vec::new();
    for batch in batches {
        let values = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        for row in 0..values.len() {
            rows.push((!values.is_null(row)).then(|| values.value(row)));
        }
    }
    rows
}

fn string_value(values: &StringArray, row: usize) -> Option<String> {
    (!values.is_null(row)).then(|| values.value(row).to_string())
}

/// Merged rows as (id, v, n, op), ordered so multi-batch output compares.
type MergedRow = (i64, Option<String>, Option<i64>, Option<String>);

fn merged_rows(batches: &[RecordBatch]) -> Vec<MergedRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let v = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("v");
        let n = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("n");
        let op = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("op");
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                string_value(v, row),
                (!n.is_null(row)).then(|| n.value(row)),
                string_value(op, row),
            ));
        }
    }
    rows.sort();
    rows
}

/// Asserts that the tasks' pieces partition each file exactly once.
fn assert_coverage(tasks: &[Arc<LakeSoulScanExec>], file_count: usize, size: u64) {
    let mut spans: HashMap<String, Vec<(i64, i64)>> = HashMap::new();
    for task in tasks {
        for unit in task.work_units() {
            for file in &unit.files {
                let (start, end) = match &file.range {
                    Some(range) => (range.start, range.end),
                    None => (0, size as i64),
                };
                spans
                    .entry(file.path.clone())
                    .or_default()
                    .push((start, end));
            }
        }
    }
    assert_eq!(spans.len(), file_count, "every file is assigned to a task");
    for (path, mut file_spans) in spans {
        file_spans.sort_unstable();
        let mut cursor = 0;
        for (start, end) in file_spans {
            assert_eq!(
                start, cursor,
                "{path}: pieces must not overlap or leave gaps"
            );
            cursor = end;
        }
        assert_eq!(cursor, size as i64, "{path}: every byte is assigned once");
    }
}

macro_rules! assert_rejected {
    ($node:expr, $needle:literal) => {{
        let error = $node
            .err()
            .expect(concat!("descriptor must be rejected: ", $needle))
            .to_string();
        assert!(
            error.contains($needle),
            "expected {:?} in {:?}",
            $needle,
            error
        );
    }};
}

// ---------------------------------------------------------------------------
// Laziness: nothing before the first poll.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nothing_is_read_before_the_first_poll() {
    let dir = TempDir::new().expect("temp dir");
    let schema = id_schema();
    let (url, size) = write_parquet(
        dir.path(),
        "part-0.parquet",
        &int_batch(&schema, &[1, 2, 3]),
        1024,
    );
    let node = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty())),
        vec![unit(vec![scan_file(&url, Some(size), None)], 0)],
        1,
    )
    .expect("valid descriptor");
    let store = Arc::new(CountingStore::new());

    // Planning, optimization and execute-before-first-poll are metadata work.
    assert_eq!(node.work_units().len(), 1);
    assert!(!node.is_merge_on_read());
    assert_eq!(node.splittable_task_limit(), Some(1));
    assert_eq!(node.split_into_tasks(2).expect("split").len(), 2);
    let unread = node
        .execute(0, task_context(&store))
        .expect("execute the scan");
    assert!(
        store.calls().is_empty(),
        "nothing may be read before the first poll: {:?}",
        store.calls()
    );
    drop(unread);
    assert!(
        store.calls().is_empty(),
        "dropping an unpolled stream must not read either: {:?}",
        store.calls()
    );

    let batches = collect(&node, 0, &store).await;
    assert_eq!(int64_rows(&batches, 0), vec![Some(1), Some(2), Some(3)]);
    assert!(
        !store.calls().is_empty(),
        "the first poll reads the unit's file"
    );
}

#[tokio::test]
async fn an_unknown_size_is_resolved_once_on_the_first_poll() {
    let dir = TempDir::new().expect("temp dir");
    let schema = id_schema();
    let (url, _) = write_parquet(
        dir.path(),
        "part-0.parquet",
        &int_batch(&schema, &[4, 5]),
        1024,
    );
    let node = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty())),
        vec![unit(vec![scan_file(&url, None, None)], 0)],
        1,
    )
    .expect("an unknown size is a legal descriptor");
    let store = Arc::new(CountingStore::new());

    let stream = node
        .execute(0, task_context(&store))
        .expect("execute the scan");
    assert!(
        store.calls().is_empty(),
        "an unknown size is not resolved while planning: {:?}",
        store.calls()
    );
    let batches: Vec<RecordBatch> = {
        let mut stream = stream;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.expect("the head resolves the size on the first poll"));
        }
        batches
    };
    assert_eq!(int64_rows(&batches, 0), vec![Some(4), Some(5)]);
    let heads = store
        .calls()
        .into_iter()
        .filter(|(operation, _)| *operation == "head")
        .count();
    assert_eq!(heads, 1, "one head resolves the unit's unknown size");
}

#[tokio::test]
async fn a_missing_file_fails_the_first_poll() {
    let dir = TempDir::new().expect("temp dir");
    let schema = id_schema();
    let url = format!("file://{}/missing.parquet", dir.path().display());
    let node = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty())),
        vec![unit(vec![scan_file(&url, Some(1024), None)], 0)],
        1,
    )
    .expect("a descriptor never checks that a file exists");
    let store = Arc::new(CountingStore::new());

    let error = first_error(&node, 0, &store).await;
    let error = error.to_lowercase();
    assert!(
        error.contains("missing.parquet") || error.contains("not found"),
        "the first poll reports the unreadable file: {error}"
    );
}

#[tokio::test]
async fn a_lane_finishes_one_unit_before_initializing_the_next() {
    let dir = TempDir::new().expect("temp dir");
    let schema = id_schema();
    let (first_url, first_size) = write_parquet(
        dir.path(),
        "unit-0.parquet",
        &int_batch(&schema, &[10]),
        1024,
    );
    let (second_url, second_size) = write_parquet(
        dir.path(),
        "unit-1.parquet",
        &int_batch(&schema, &[20]),
        1024,
    );
    let node = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty())),
        vec![
            unit(vec![scan_file(&first_url, Some(first_size), None)], 0),
            unit(vec![scan_file(&second_url, Some(second_size), None)], 0),
        ],
        1,
    )
    .expect("valid descriptor");
    let store = Arc::new(CountingStore::new());
    let first = store_path(&first_url);
    let second = store_path(&second_url);

    let mut stream = node
        .execute(0, task_context(&store))
        .expect("execute the scan");
    let batch = stream
        .next()
        .await
        .expect("a first batch")
        .expect("the first unit reads");
    drop(batch);
    assert!(
        store.touched(&first),
        "the first unit is read: {:?}",
        store.calls()
    );
    assert!(
        !store.touched(&second),
        "the second unit must not be initialized before the first is released: {:?}",
        store.calls()
    );
    drop(stream);
    assert!(
        !store.touched(&second),
        "dropping the stream releases the lane instead of starting the next unit: {:?}",
        store.calls()
    );

    let batches = collect(&node, 0, &store).await;
    assert_eq!(int64_rows(&batches, 0), vec![Some(10), Some(20)]);
    let calls = store.calls();
    let last_of_first = calls
        .iter()
        .rposition(|(_, path)| *path == first)
        .expect("the first unit was read");
    let first_of_second = calls
        .iter()
        .position(|(_, path)| *path == second)
        .expect("the second unit was read");
    assert!(
        last_of_first < first_of_second,
        "a lane initializes its units one at a time, in order: {calls:?}"
    );
}

// ---------------------------------------------------------------------------
// Reading the units: sparse merge versions and partition values.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sparse_versions_keep_a_missing_column_apart_from_an_explicit_null() {
    let dir = TempDir::new().expect("temp dir");
    let table = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("n", DataType::Int64, true),
        Field::new("op", DataType::Utf8, true),
    ]));
    let version_one = RecordBatch::try_new(
        Arc::clone(&table),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("old"), Some("keep")])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            Arc::new(StringArray::from(vec![Some("insert"), Some("insert")])),
        ],
    )
    .expect("version one");
    // A version that does not carry `v` or `op`: those columns are absent, not
    // updated to NULL.
    let partial = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("n", DataType::Int64, true),
    ]));
    let version_two = RecordBatch::try_new(
        Arc::clone(&partial),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![Some(7)])),
        ],
    )
    .expect("version two");
    // A version that carries `v` with an explicit NULL for one row.
    let update = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, true),
    ]));
    let version_three = RecordBatch::try_new(
        Arc::clone(&update),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("delete")])),
        ],
    )
    .expect("version three");
    let (url_one, size_one) = write_parquet(dir.path(), "v1.parquet", &version_one, 1024);
    let (url_two, size_two) = write_parquet(dir.path(), "v2.parquet", &version_two, 1024);
    let (url_three, size_three) =
        write_parquet(dir.path(), "v3.parquet", &version_three, 1024);

    let node = LakeSoulScanExec::try_new(
        merge_config(Arc::clone(&table)),
        vec![unit(
            vec![
                scan_file(&url_one, Some(size_one), None),
                scan_file(&url_two, Some(size_two), None),
                scan_file(&url_three, Some(size_three), None),
            ],
            0,
        )],
        1,
    )
    .expect("valid merge descriptor");
    assert!(node.is_merge_on_read());
    assert_eq!(
        node.splittable_task_limit(),
        None,
        "a merge cannot be split: a key's versions must reach one merge"
    );
    assert_eq!(
        node.split_into_tasks(3).expect("split").len(),
        1,
        "a merge-on-read scan comes back as itself"
    );

    let store = Arc::new(CountingStore::new());
    let batches = collect(&node, 0, &store).await;
    for batch in &batches {
        assert_eq!(
            batch.schema(),
            node.schema(),
            "every merged batch keeps the frozen schema"
        );
    }
    assert_eq!(
        merged_rows(&batches),
        vec![
            (
                1,
                Some("old".to_string()),
                Some(7),
                Some("insert".to_string())
            ),
            (2, None, Some(2), Some("delete".to_string())),
        ],
        "an absent column keeps the older value; an explicit NULL replaces it"
    );
}

#[tokio::test]
async fn append_only_lanes_read_their_own_partition_values() {
    let dir = TempDir::new().expect("temp dir");
    let table = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("part", DataType::Utf8, false),
    ]));
    let partition_schema =
        Arc::new(Schema::new(vec![Field::new("part", DataType::Utf8, false)]));
    // Files hold data columns only; the partition column comes from the unit.
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let (first_url, first_size) = write_parquet(
        dir.path(),
        "part-p1.parquet",
        &int32_batch(&file_schema, &[1]),
        1024,
    );
    let (second_url, second_size) = write_parquet(
        dir.path(),
        "part-p2.parquet",
        &int32_batch(&file_schema, &[2]),
        1024,
    );
    let mut first_unit = unit(vec![scan_file(&first_url, Some(first_size), None)], 0);
    first_unit
        .default_column_value
        .insert("part".to_string(), "p1".to_string());
    let mut second_unit = unit(vec![scan_file(&second_url, Some(second_size), None)], 1);
    second_unit
        .default_column_value
        .insert("part".to_string(), "p2".to_string());

    let node = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&table), partition_schema),
        vec![first_unit, second_unit],
        2,
    )
    .expect("valid descriptor");
    let store = Arc::new(CountingStore::new());
    let batches = collect_all_lanes(&node, &store).await;
    let mut rows = Vec::new();
    for batch in &batches {
        assert_eq!(batch.schema(), node.schema());
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id");
        let parts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("part");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), string_value(parts, row)));
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![(1, Some("p1".to_string())), (2, Some("p2".to_string()))]
    );
}

// ---------------------------------------------------------------------------
// Splitting an append-only scan.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn splitting_covers_every_file_exactly_once() {
    let schema = id_schema();
    let size = 2 * MIN_PARQUET_RANGE_BYTES;
    let files: Vec<ScanFile> = (0..5)
        .map(|index| fake_parquet("table", &format!("part-{index}.parquet"), size))
        .collect();
    let node = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty())),
        vec![unit(files, 0)],
        1,
    )
    .expect("valid descriptor");
    assert_eq!(
        node.splittable_task_limit(),
        Some(10),
        "each file can be read as two pieces of at least the minimum range"
    );

    let tasks = node.split_into_tasks(4).expect("split into four tasks");
    assert_eq!(tasks.len(), 4);
    for task in &tasks {
        assert_eq!(task.schema(), node.schema(), "every task emits one schema");
        assert_eq!(
            task.output_partitioning().partition_count(),
            1,
            "a task reports the scan's partition count"
        );
        assert!(!task.is_merge_on_read());
    }
    assert_coverage(&tasks, 5, size);

    let padded = node.split_into_tasks(20).expect("split into twenty tasks");
    assert_eq!(padded.len(), 20);
    let busy = padded
        .iter()
        .filter(|task| task.work_units().iter().any(|unit| !unit.files.is_empty()))
        .count();
    assert_eq!(busy, 10, "tasks beyond the legal pieces carry no work");
    assert_coverage(&padded, 5, size);
}

#[tokio::test]
async fn only_parquet_files_are_cut_into_ranges() {
    let schema = id_schema();
    let parquet = fake_parquet("table", "big.parquet", 12 * MIN_PARQUET_RANGE_BYTES);
    let vortex = ScanFile {
        path: "file:///lakesoul-split-tests/table/big.vortex".to_string(),
        physical_format: PhysicalFormat::Vortex,
        size: Some(1000 * MIN_PARQUET_RANGE_BYTES),
        range: None,
    };
    let node = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty())),
        vec![unit(vec![parquet, vortex], 0)],
        1,
    )
    .expect("valid descriptor");
    assert_eq!(node.splittable_task_limit(), Some(13));

    let tasks = node.split_into_tasks(4).expect("split");
    let mut parquet_ranges = Vec::new();
    let mut vortex_pieces = Vec::new();
    for task in &tasks {
        for unit in task.work_units() {
            for file in &unit.files {
                if file.path.ends_with(".vortex") {
                    vortex_pieces.push(file.clone());
                } else {
                    parquet_ranges
                        .push(file.range.clone().expect("a piece carries its range"));
                }
            }
        }
    }
    assert_eq!(
        vortex_pieces.len(),
        1,
        "a vortex file is never split: only parquet supports legal ranges"
    );
    assert_eq!(vortex_pieces[0].range, None);
    parquet_ranges.sort_by_key(|range| range.start);
    assert_eq!(parquet_ranges.len(), 3);
    let mut cursor = 0;
    for range in parquet_ranges {
        assert_eq!(range.start, cursor);
        cursor = range.end;
    }
    assert_eq!(cursor, (12 * MIN_PARQUET_RANGE_BYTES) as i64);
}

#[tokio::test]
async fn disjoint_parquet_ranges_read_every_row_once() {
    let dir = TempDir::new().expect("temp dir");
    let schema = id_schema();
    // Three row groups, so a range boundary can fall between them.
    let (url, size) = write_parquet(
        dir.path(),
        "row-groups.parquet",
        &int_batch(&schema, &[1, 2, 3, 4, 5]),
        2,
    );
    let middle = (size / 2) as i64;
    assert!(middle > 0, "the fixture file is not empty");
    let config = append_only_config(Arc::clone(&schema), Arc::new(Schema::empty()));
    let lower = LakeSoulScanExec::try_new(
        config.clone(),
        vec![unit(vec![scan_file(&url, Some(size), Some(0..middle))], 0)],
        1,
    )
    .expect("a lower piece is a valid descriptor");
    let upper = LakeSoulScanExec::try_new(
        config,
        vec![unit(
            vec![scan_file(&url, Some(size), Some(middle..size as i64))],
            0,
        )],
        1,
    )
    .expect("an upper piece is a valid descriptor");
    let store = Arc::new(CountingStore::new());
    let mut rows = int64_rows(&collect(&lower, 0, &store).await, 0);
    rows.extend(int64_rows(&collect(&upper, 0, &store).await, 0));
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
        "a row group belongs to exactly one piece"
    );

    // Splitting the same scan must not duplicate or drop a row.
    let whole = LakeSoulScanExec::try_new(
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty())),
        vec![unit(vec![scan_file(&url, Some(size), None)], 0)],
        1,
    )
    .expect("valid descriptor");
    let mut split_rows = Vec::new();
    for task in whole.split_into_tasks(2).expect("split") {
        split_rows.extend(int64_rows(&collect(&task, 0, &store).await, 0));
    }
    split_rows.sort_unstable();
    assert_eq!(
        split_rows,
        vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
        "splitting the scan keeps every row exactly once"
    );
}

// ---------------------------------------------------------------------------
// Descriptor validation.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn overlapping_or_repeated_pieces_are_rejected() {
    let schema = id_schema();
    let config = append_only_config(Arc::clone(&schema), Arc::new(Schema::empty()));
    let url = "file:///lakesoul-split-tests/table/part-0.parquet";

    assert_rejected!(
        LakeSoulScanExec::try_new(
            config.clone(),
            vec![
                unit(vec![scan_file(url, Some(1000), Some(0..500))], 0),
                unit(vec![scan_file(url, Some(1000), Some(0..500))], 0),
            ],
            1,
        ),
        "read twice"
    );
    assert_rejected!(
        LakeSoulScanExec::try_new(
            config.clone(),
            vec![unit(
                vec![
                    scan_file(url, Some(1000), Some(0..600)),
                    scan_file(url, Some(1000), Some(500..1000)),
                ],
                0,
            )],
            1,
        ),
        "read twice"
    );
    assert_rejected!(
        LakeSoulScanExec::try_new(
            config,
            vec![unit(
                vec![
                    scan_file(url, Some(1000), None),
                    scan_file(url, Some(1000), Some(0..10)),
                ],
                0,
            )],
            1,
        ),
        "read twice"
    );
    // A merge reads whole version files; a piece of one version would merge
    // part of a key's history.
    assert_rejected!(
        LakeSoulScanExec::try_new(
            LakeSoulScanConfig {
                partition_schema: Arc::new(Schema::empty()),
                ..merge_config(Arc::clone(&schema))
            },
            vec![unit(vec![scan_file(url, Some(1000), Some(0..10))], 0)],
            1,
        ),
        "merge-on-read"
    );
}

#[tokio::test]
async fn descriptors_are_validated_without_io() {
    let schema = id_schema();
    let config = append_only_config(Arc::clone(&schema), Arc::new(Schema::empty()));
    let url = "file:///lakesoul-split-tests/table/part-0.parquet";

    assert_rejected!(
        LakeSoulScanExec::try_new(
            config.clone(),
            vec![unit(vec![scan_file(url, Some(10), None)], 2)],
            2,
        ),
        "lanes"
    );
    assert_rejected!(
        LakeSoulScanExec::try_new(
            config.clone(),
            vec![unit(
                vec![ScanFile {
                    path: "file:///lakesoul-split-tests/table/part-0.vortex".to_string(),
                    physical_format: PhysicalFormat::Vortex,
                    size: Some(10),
                    range: Some(0..5),
                }],
                0,
            )],
            1,
        ),
        "only parquet"
    );
    assert_rejected!(
        LakeSoulScanExec::try_new(
            config.clone(),
            vec![unit(vec![scan_file(url, Some(10), Some(0..20))], 0)],
            1,
        ),
        "past its size"
    );
    let mut unknown_column =
        append_only_config(Arc::clone(&schema), Arc::new(Schema::empty()));
    unknown_column.output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("absent", DataType::Int64, true),
    ]));
    assert_rejected!(
        LakeSoulScanExec::try_new(
            unknown_column,
            vec![unit(vec![scan_file(url, Some(10), None)], 0)],
            1,
        ),
        "not a column of the table"
    );
    let partitioned_table = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("part", DataType::Utf8, false),
    ]));
    let mut partitioned = append_only_config(
        Arc::clone(&partitioned_table),
        Arc::new(Schema::new(vec![Field::new("part", DataType::Utf8, false)])),
    );
    partitioned.output_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("part", DataType::Utf8, true),
    ]));
    assert_rejected!(
        LakeSoulScanExec::try_new(
            partitioned,
            vec![unit(vec![scan_file(url, Some(10), None)], 0)],
            1,
        ),
        "no value for partition column"
    );
    let mut dropped_key = merge_config(Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ])));
    dropped_key.output_schema =
        Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
    assert_rejected!(
        LakeSoulScanExec::try_new(
            dropped_key,
            vec![unit(vec![scan_file(url, Some(10), None)], 0)],
            1,
        ),
        "omits primary key"
    );
}

#[tokio::test]
async fn conservative_nullability_is_frozen_on_every_unit() {
    let table = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("part", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let partition_schema =
        Arc::new(Schema::new(vec![Field::new("part", DataType::Utf8, false)]));
    let config = LakeSoulScanConfig {
        output_schema: Arc::clone(&table),
        table_schema: Arc::clone(&table),
        partition_schema,
        io_config: LakeSoulIOConfig::builder()
            .with_schema(Arc::clone(&table))
            .with_primary_keys(vec!["id".to_string()])
            .build(),
        predicate: None,
        key_constraint: None,
        parquet_force_view_types: false,
        read_options: HashMap::new(),
        limit: None,
    };
    let empty = LakeSoulScanExec::try_new(config.clone(), Vec::new(), 1)
        .expect("a scan without units still describes its schema");
    let mut partitioned_unit = unit(vec![fake_parquet("table", "part-0.parquet", 10)], 0);
    partitioned_unit
        .default_column_value
        .insert("part".to_string(), "p1".to_string());
    let populated = LakeSoulScanExec::try_new(config, vec![partitioned_unit], 1)
        .expect("valid descriptor");

    for node in [&empty, &populated] {
        let schema = node.schema();
        assert!(
            !schema.field_with_name("id").expect("id").is_nullable(),
            "a primary key is known for every merged row"
        );
        assert!(
            !schema.field_with_name("part").expect("part").is_nullable(),
            "a partition value is known for every row of a unit"
        );
        assert!(
            schema.field_with_name("v").expect("v").is_nullable(),
            "a value column may be absent from a file, so it is emitted nullable"
        );
    }
    assert_eq!(
        empty.schema(),
        populated.schema(),
        "every task of a scan exposes one schema"
    );
}

// ---------------------------------------------------------------------------
// Reader options.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_options_round_trip_into_the_task() {
    let mut planner = SessionConfig::default();
    planner
        .options_mut()
        .set("datafusion.execution.parquet.pushdown_filters", "true")
        .expect("known parquet option");
    planner
        .options_mut()
        .set("datafusion.execution.batch_size", "16")
        .expect("known execution option");

    let captured = capture_read_options(planner.options());
    assert!(
        captured.contains_key("datafusion.execution.parquet.pushdown_filters"),
        "reader semantics travel with the plan: {captured:?}"
    );
    assert!(
        !captured.contains_key("datafusion.execution.batch_size"),
        "the consumer task keeps its own batch size: {captured:?}"
    );
    assert!(
        captured
            .keys()
            .all(|key| key.starts_with(PARQUET_READ_OPTION_PREFIX))
    );

    let restored =
        restore_read_options(SessionConfig::default(), &captured).expect("restore");
    assert_eq!(
        capture_read_options(restored.options()),
        captured,
        "the restored configuration reproduces the planner's reader options"
    );

    let foreign = HashMap::from([(
        "datafusion.execution.batch_size".to_string(),
        "16".to_string(),
    )]);
    assert!(
        restore_read_options(SessionConfig::default(), &foreign).is_err(),
        "a non-reader option is refused instead of silently ignored"
    );

    // A scan carrying those options reads the same rows.
    let dir = TempDir::new().expect("temp dir");
    let schema = id_schema();
    let (url, size) = write_parquet(
        dir.path(),
        "part-0.parquet",
        &int_batch(&schema, &[7, 8]),
        1024,
    );
    let mut config = append_only_config(Arc::clone(&schema), Arc::new(Schema::empty()));
    config.read_options = captured;
    let node = LakeSoulScanExec::try_new(
        config,
        vec![unit(vec![scan_file(&url, Some(size), None)], 0)],
        1,
    )
    .expect("valid descriptor");
    let store = Arc::new(CountingStore::new());
    assert_eq!(
        int64_rows(&collect(&node, 0, &store).await, 0),
        vec![Some(7), Some(8)]
    );
}
