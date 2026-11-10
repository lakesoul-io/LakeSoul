// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Regression tests for the process-wide metadata fetch budget.
//!
//! Flattening a work unit reads every file's parquet footer to infer its schema
//! and statistics. The reads are bounded by one budget shared by the whole
//! process, whatever session, flatten route or number of file groups the
//! planning call has: before, the per-group stream and the per-file stream each
//! buffered `meta_fetch_concurrency`, so one call ran up to its square, and a
//! second concurrent call (another session, or the generic route next to the
//! raw parquet route) ran a second independent square.
//!
//! These tests observe the bound from the object store, not from a limiter:
//! every ranged read enters a gate, records how many reads are in flight and
//! blocks until the test lets it go. No read can finish on its own, so the
//! number of reads the store has admitted is exactly the number of metadata
//! fetches the planner let through, and a scheduling delay can only make the
//! test see *fewer* reads than the implementation permits, never more.
//!
//! The budget is read once per process from
//! `DATAFUSION_EXECUTION_META_FETCH_CONCURRENCY`, so each scenario runs in a
//! child process of this same test binary with the variable set explicitly;
//! mutating the environment in a process with parallel tests would race. The
//! child processes are the `#[ignore]` `probe_*` tests below.

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::stats::Precision;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::stream::BoxStream;
use lakesoul_io::file_format::{
    flatten_file_scan_config, flatten_file_scan_config_for_format,
};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as StoreResult,
};
use parquet::arrow::ArrowWriter;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use url::Url;

/// The environment variable a child process sets to pin the budget.
const CAPACITY_ENV: &str = "DATAFUSION_EXECUTION_META_FETCH_CONCURRENCY";

/// How long to wait for reads to stop arriving when nothing is released.
///
/// With the budget in force the gate accepts exactly the budget's worth of
/// reads and then stalls: no read can complete without a release, so no further
/// read can be admitted. An implementation that admits more keeps sending
/// starts and is caught on the next event; a correct one stays silent, and this
/// is the watchdog that ends the observation.
const SETTLE: Duration = Duration::from_millis(1_500);

/// Deadlock watchdog for every other await.
const WATCHDOG: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Parent tests: re-run one probe in a child process with a private budget.
// ---------------------------------------------------------------------------

#[test]
fn process_budget_is_shared_across_sessions_and_flatten_routes() {
    run_child(
        "probe_budget_is_shared_across_sessions_and_flatten_routes",
        2,
    );
}

#[test]
fn flatten_keeps_input_order_when_reads_complete_out_of_order() {
    run_child(
        "probe_flatten_keeps_input_order_when_reads_complete_out_of_order",
        4,
    );
}

#[test]
fn budget_is_released_after_metadata_errors_and_cancelled_planning() {
    run_child(
        "probe_budget_is_released_after_metadata_errors_and_cancelled_planning",
        2,
    );
}

/// Runs the ignored probe test `name` in a child process with `capacity` as
/// the process metadata budget.
fn run_child(name: &str, capacity: usize) {
    let exe = std::env::current_exe().expect("current test executable");
    let output = Command::new(&exe)
        .args([
            name,
            "--exact",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(CAPACITY_ENV, capacity.to_string())
        .output()
        .expect("run the metadata budget probe subprocess");
    assert!(
        output.status.success(),
        "probe {name} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// The budget this process was started with, as production reads it.
fn env_capacity() -> usize {
    let capacity: usize = SessionConfig::from_env()
        .expect("session config from the environment")
        .options()
        .execution
        .meta_fetch_concurrency
        .into();
    capacity
}

// ---------------------------------------------------------------------------
// Probe 1: one process budget across two sessions and two flatten routes.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess probe target; re-invoked by process_budget_is_shared_across_sessions_and_flatten_routes"]
async fn probe_budget_is_shared_across_sessions_and_flatten_routes() {
    const CAPACITY: usize = 2;
    assert_eq!(
        env_capacity(),
        CAPACITY,
        "the probe must run with its explicit budget"
    );

    let root = tempfile::tempdir().expect("tempdir");
    let raw_dir = root.path().join("raw");
    let generic_dir = root.path().join("generic");
    std::fs::create_dir_all(&raw_dir).expect("create raw dir");
    std::fs::create_dir_all(&generic_dir).expect("create generic dir");

    // Two file groups per call, three files each: enough files that the bound,
    // not the input size, decides how many reads run at once.
    let (raw_metas, raw_expected) = write_groups(
        &raw_dir,
        &[
            &[
                ("raw-0.parquet", &[1, 2][..], false),
                ("raw-1.parquet", &[3][..], false),
                ("raw-2.parquet", &[4, 5, 6][..], false),
            ],
            &[
                ("raw-3.parquet", &[7][..], false),
                ("raw-4.parquet", &[8, 9][..], true),
                ("raw-5.parquet", &[10][..], false),
            ],
        ],
    );
    let (generic_metas, generic_expected) = write_groups(
        &generic_dir,
        &[
            &[
                ("generic-0.parquet", &[11][..], false),
                ("generic-1.parquet", &[12, 13][..], false),
                ("generic-2.parquet", &[14][..], true),
            ],
            &[
                ("generic-3.parquet", &[15, 16, 17][..], false),
                ("generic-4.parquet", &[18][..], false),
                ("generic-5.parquet", &[19, 20][..], false),
            ],
        ],
    );

    let (probe, mut starts, _completions) = Probe::new(true);

    let raw_url = Url::parse("budget://raw").expect("raw url");
    let raw_store_url = ObjectStoreUrl::parse("budget://raw").expect("raw store url");
    let raw_format = ParquetFormat::default();
    let raw_conf = scan_config(&raw_store_url, &raw_format, wide_schema(), raw_metas);
    let raw_state = session_with_store(&raw_url, local_store(&raw_dir, &probe));

    let generic_url = Url::parse("budget://generic").expect("generic url");
    let generic_store_url =
        ObjectStoreUrl::parse("budget://generic").expect("generic store url");
    let generic_format: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let generic_conf = scan_config(
        &generic_store_url,
        &ParquetFormat::default(),
        wide_schema(),
        generic_metas,
    );
    let generic_state =
        session_with_store(&generic_url, local_store(&generic_dir, &probe));

    let raw_task: JoinHandle<_> = tokio::spawn(async move {
        flatten_file_scan_config(
            &raw_state,
            raw_format.into(),
            raw_conf,
            &[],
            "",
            Arc::new(Schema::empty()),
            wide_schema(),
        )
        .await
    });
    let generic_task: JoinHandle<_> = tokio::spawn(async move {
        flatten_file_scan_config_for_format(
            &generic_state,
            generic_format,
            generic_conf,
            &[],
            "",
            Arc::new(Schema::empty()),
            wide_schema(),
        )
        .await
    });

    assert_bounded_starts(
        &probe,
        &mut starts,
        CAPACITY,
        "raw and generic flatten in two independent sessions",
    )
    .await;

    probe.global_release.add_permits(1_000);

    let raw_configs = timeout(raw_task, "raw flatten after releasing the budget")
        .await
        .expect("raw flatten failed")
        .expect("raw flatten task panicked");
    let generic_configs =
        timeout(generic_task, "generic flatten after releasing the budget")
            .await
            .expect("generic flatten failed")
            .expect("generic flatten task panicked");

    assert_output_configs(&raw_configs, &raw_expected, "raw flatten");
    assert_output_configs(&generic_configs, &generic_expected, "generic flatten");
}

// ---------------------------------------------------------------------------
// Probe 2: output order survives out-of-order metadata completion.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess probe target; re-invoked by flatten_keeps_input_order_when_reads_complete_out_of_order"]
async fn probe_flatten_keeps_input_order_when_reads_complete_out_of_order() {
    const CAPACITY: usize = 4;
    assert_eq!(
        env_capacity(),
        CAPACITY,
        "the probe must run with its explicit budget"
    );

    let root = tempfile::tempdir().expect("tempdir");
    let dir = root.path().join("order");
    std::fs::create_dir_all(&dir).expect("create order dir");

    // File names are deliberately not in sort order: the flattened output must
    // follow the input groups and files, not any path or completion order.
    let (metas, expected) = write_groups(
        &dir,
        &[
            &[
                ("part-z2.parquet", &[1, 2, 3][..], false),
                ("part-a1.parquet", &[4, 5][..], false),
            ],
            &[
                ("part-m1.parquet", &[6, 7, 8, 9][..], true),
                ("part-b0.parquet", &[10, 11][..], false),
                ("part-c3.parquet", &[12, 13, 14][..], false),
            ],
        ],
    );

    let (probe, mut starts, mut completions) = Probe::new(true);
    // Every file's reads wait for the test's own release, so completion order
    // is exactly the release order.
    for file in &expected {
        probe.path_gate(file.name);
    }

    let url = Url::parse("budget://order").expect("order url");
    let store_url = ObjectStoreUrl::parse("budget://order").expect("order store url");
    let format = ParquetFormat::default();
    let conf = scan_config(&store_url, &format, wide_schema(), metas);
    let runtime = Arc::new(RuntimeEnv::default());
    runtime.register_object_store(&url, local_store(&dir, &probe));
    let mut config = SessionConfig::new();
    config.options_mut().execution.meta_fetch_concurrency =
        datafusion::common::config::ConfigNonZeroUsize::try_new(CAPACITY).unwrap();
    let state = SessionContext::new_with_config_rt(config, runtime).state();
    let task = tokio::spawn(async move {
        flatten_file_scan_config(
            &state,
            format.into(),
            conf,
            &[],
            "",
            Arc::new(Schema::empty()),
            wide_schema(),
        )
        .await
    });

    let started = wait_for_starts(&mut starts, CAPACITY, "out-of-order probe").await;
    let started: HashSet<String> = started.into_iter().collect();
    let expected_names: HashSet<String> = expected[..CAPACITY]
        .iter()
        .map(|file| file.name.to_string())
        .collect();
    assert_eq!(started, expected_names);
    assert_eq!(
        probe.max_active.load(Ordering::SeqCst),
        CAPACITY,
        "the first four files read their metadata at once"
    );

    // Completing a later file must admit the fifth while the first stays
    // blocked. An ordered buffer would leave that capacity unused.
    let first_completed = expected[CAPACITY - 1].name;
    probe.path_gate(first_completed).add_permits(1_000);
    loop {
        let name = timeout(starts.recv(), "refilling a completed metadata slot")
            .await
            .expect("metadata read event stream closed");
        if name == expected[CAPACITY].name {
            break;
        }
        assert_eq!(name, first_completed);
    }

    let release_order = [3, 4, 2, 1, 0];
    let mut released: HashSet<String> = HashSet::from([first_completed.to_string()]);
    let mut seen: HashSet<String> = HashSet::new();
    let mut completion_order: Vec<String> = Vec::new();
    for index in release_order {
        let file = &expected[index];
        let name = file.name.to_string();
        released.insert(name.clone());
        probe.path_gate(&name).add_permits(1_000);
        loop {
            let completed = timeout(
                completions.recv(),
                "out-of-order probe waiting for a metadata read to complete",
            )
            .await
            .expect("metadata read event stream closed");
            assert!(
                released.contains(&completed),
                "metadata read for {completed} completed before it was released"
            );
            if seen.insert(completed.clone()) {
                completion_order.push(completed.clone());
            }
            if completed == name {
                break;
            }
        }
    }
    let expected_completion: Vec<String> = release_order
        .iter()
        .map(|index| expected[*index].name.to_string())
        .collect();
    assert_eq!(
        completion_order, expected_completion,
        "metadata completion order differs from merge input order"
    );

    let configs = timeout(task, "out-of-order flatten")
        .await
        .expect("out-of-order flatten failed")
        .expect("out-of-order flatten task panicked");
    assert_output_configs(&configs, &expected, "out-of-order flatten");
}

// ---------------------------------------------------------------------------
// Probe 3: errors and cancellation return their permits.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "subprocess probe target; re-invoked by budget_is_released_after_metadata_errors_and_cancelled_planning"]
async fn probe_budget_is_released_after_metadata_errors_and_cancelled_planning() {
    const CAPACITY: usize = 2;
    assert_eq!(
        env_capacity(),
        CAPACITY,
        "the probe must run with its explicit budget"
    );

    let root = tempfile::tempdir().expect("tempdir");

    // A failed metadata read must return its permit: a healthy flatten right
    // after it still gets the whole budget.
    let error_dir = root.path().join("error");
    std::fs::create_dir_all(&error_dir).expect("create error dir");
    let (error_metas, _error_expected) = write_groups(
        &error_dir,
        &[&[
            ("error-0.parquet", &[1][..], false),
            ("error-bad.parquet", &[2][..], false),
            ("error-1.parquet", &[3, 4][..], false),
            ("error-2.parquet", &[5][..], false),
        ]],
    );
    let error_url = Url::parse("budget://error").expect("error url");
    let error_store_url =
        ObjectStoreUrl::parse("budget://error").expect("error store url");
    let (error_probe, _error_starts, _error_completions) = Probe::new(false);
    error_probe.fail("error-bad.parquet");
    let error_format = ParquetFormat::default();
    let error_conf =
        scan_config(&error_store_url, &error_format, wide_schema(), error_metas);
    let error_state =
        session_with_store(&error_url, local_store(&error_dir, &error_probe));
    let error = timeout(
        flatten_file_scan_config(
            &error_state,
            Arc::new(error_format),
            error_conf,
            &[],
            "",
            Arc::new(Schema::empty()),
            wide_schema(),
        ),
        "flatten over a failing metadata read",
    )
    .await
    .expect_err("flatten must surface the injected metadata read failure");
    assert!(
        error.to_string().contains("injected metadata read failure"),
        "unexpected flatten error: {error}"
    );

    let healthy_dir = root.path().join("healthy-after-error");
    std::fs::create_dir_all(&healthy_dir).expect("create healthy dir");
    let (healthy_metas, healthy_expected) = write_groups(
        &healthy_dir,
        &[&[
            ("healthy-0.parquet", &[6][..], false),
            ("healthy-1.parquet", &[7, 8][..], false),
            ("healthy-2.parquet", &[9][..], true),
            ("healthy-3.parquet", &[10, 11][..], false),
        ]],
    );
    healthy_flatten(
        "healthy-after-error",
        healthy_dir,
        healthy_metas,
        &healthy_expected,
        CAPACITY,
        "healthy flatten after a metadata error",
    )
    .await;

    // Dropping a planning future that is waiting for the budget must return
    // the permits its in-flight file futures hold.
    let cancel_dir = root.path().join("cancel");
    std::fs::create_dir_all(&cancel_dir).expect("create cancel dir");
    let (cancel_metas, _cancel_expected) = write_groups(
        &cancel_dir,
        &[
            &[
                ("cancel-0.parquet", &[12][..], false),
                ("cancel-1.parquet", &[13][..], false),
                ("cancel-2.parquet", &[14][..], false),
            ],
            &[
                ("cancel-3.parquet", &[15][..], false),
                ("cancel-4.parquet", &[16][..], false),
                ("cancel-5.parquet", &[17][..], false),
            ],
        ],
    );
    let cancel_url = Url::parse("budget://cancel").expect("cancel url");
    let cancel_store_url =
        ObjectStoreUrl::parse("budget://cancel").expect("cancel store url");
    let (cancel_probe, mut cancel_starts, _cancel_completions) = Probe::new(true);
    let cancel_format = ParquetFormat::default();
    let cancel_conf = scan_config(
        &cancel_store_url,
        &cancel_format,
        wide_schema(),
        cancel_metas,
    );
    let cancel_state =
        session_with_store(&cancel_url, local_store(&cancel_dir, &cancel_probe));
    let cancelled = tokio::spawn(async move {
        flatten_file_scan_config(
            &cancel_state,
            cancel_format.into(),
            cancel_conf,
            &[],
            "",
            Arc::new(Schema::empty()),
            wide_schema(),
        )
        .await
    });
    wait_for_starts(
        &mut cancel_starts,
        CAPACITY,
        "flatten to cancel while it holds the budget",
    )
    .await;
    assert_eq!(
        cancel_probe.active.load(Ordering::SeqCst),
        CAPACITY,
        "the cancelled planning holds the whole budget before it is dropped"
    );
    cancelled.abort();
    assert!(
        cancelled
            .await
            .expect_err("an aborted planning must not complete")
            .is_cancelled(),
        "the planning task must be cancelled, not fail"
    );

    let after_cancel_dir = root.path().join("healthy-after-cancel");
    std::fs::create_dir_all(&after_cancel_dir).expect("create after-cancel dir");
    let (after_cancel_metas, after_cancel_expected) = write_groups(
        &after_cancel_dir,
        &[&[
            ("after-cancel-0.parquet", &[18][..], false),
            ("after-cancel-1.parquet", &[19, 20][..], false),
            ("after-cancel-2.parquet", &[21][..], false),
            ("after-cancel-3.parquet", &[22, 23][..], true),
        ]],
    );
    healthy_flatten(
        "healthy-after-cancel",
        after_cancel_dir,
        after_cancel_metas,
        &after_cancel_expected,
        CAPACITY,
        "healthy flatten after a cancelled planning",
    )
    .await;
}

/// Runs a blocking flatten and requires it to be able to occupy the whole
/// budget, then releases the reads and checks the returned configs.
async fn healthy_flatten(
    name: &str,
    dir: std::path::PathBuf,
    metas: Vec<Vec<ObjectMeta>>,
    expected: &[ExpectedFile],
    capacity: usize,
    context: &str,
) {
    let url = Url::parse(&format!("budget://{name}")).expect("healthy url");
    let store_url =
        ObjectStoreUrl::parse(&format!("budget://{name}")).expect("healthy store url");
    let (probe, mut starts, _completions) = Probe::new(true);
    let format = ParquetFormat::default();
    let conf = scan_config(&store_url, &format, wide_schema(), metas);
    let state = session_with_store(&url, local_store(&dir, &probe));
    let task = tokio::spawn(async move {
        flatten_file_scan_config(
            &state,
            format.into(),
            conf,
            &[],
            "",
            Arc::new(Schema::empty()),
            wide_schema(),
        )
        .await
    });

    wait_for_starts(&mut starts, capacity, context).await;
    assert_eq!(
        probe.max_active.load(Ordering::SeqCst),
        capacity,
        "{context}: only {} of the {capacity} permits were usable",
        probe.max_active.load(Ordering::SeqCst)
    );

    probe.global_release.add_permits(1_000);
    let configs = timeout(task, context)
        .await
        .unwrap_or_else(|error| panic!("{context}: planning task failed: {error:?}"))
        .expect(context);
    assert_output_configs(&configs, expected, context);
}

// ---------------------------------------------------------------------------
// Observation of the shared budget.
// ---------------------------------------------------------------------------

/// Consumes metadata read starts until the gate goes quiet and requires that
/// none was admitted past `capacity`.
///
/// Nothing is released while this runs, so every admitted read stays in
/// flight: the gate cannot admit a read the budget did not let through, and a
/// read the budget did let through cannot leave. With the budget implemented
/// the gate admits `capacity` reads and then stalls; an implementation that
/// multiplies the streams (or keeps a budget per call) keeps admitting reads
/// and the in-flight count crosses `capacity`.
async fn assert_bounded_starts(
    probe: &Probe,
    starts: &mut mpsc::UnboundedReceiver<String>,
    capacity: usize,
    context: &str,
) -> usize {
    let mut started = 0usize;
    loop {
        match tokio::time::timeout(SETTLE, starts.recv()).await {
            Ok(Some(_)) => {
                started += 1;
                let active = probe.active.load(Ordering::SeqCst);
                let max_active = probe.max_active.load(Ordering::SeqCst);
                assert!(
                    active <= capacity && max_active <= capacity,
                    "{context}: {active} metadata reads in flight (max {max_active}), \
                     but the process budget is {capacity}",
                );
            }
            // The senders live as long as the stores; a closed channel means
            // the setup dropped them before any read, which the count below
            // reports.
            Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(
        started >= capacity,
        "{context}: only {started} metadata reads started, so the budget of \
         {capacity} was not usable",
    );
    started
}

/// Waits until at least `n` metadata reads have started, returning them.
async fn wait_for_starts(
    starts: &mut mpsc::UnboundedReceiver<String>,
    n: usize,
    context: &str,
) -> Vec<String> {
    let mut started = Vec::with_capacity(n);
    while started.len() < n {
        let path = timeout(
            starts.recv(),
            &format!(
                "{context}: waiting for metadata read {} of {n}",
                started.len() + 1
            ),
        )
        .await
        .expect("metadata read event stream closed");
        started.push(path);
    }
    started
}

/// Awaits `future` with the deadlock watchdog and a message that names it.
async fn timeout<F: Future>(future: F, context: &str) -> F::Output {
    tokio::time::timeout(WATCHDOG, future)
        .await
        .unwrap_or_else(|_| panic!("{context}: timed out after {WATCHDOG:?}"))
}

// ---------------------------------------------------------------------------
// The object store gate.
// ---------------------------------------------------------------------------

/// Counts ranged reads and holds each one until the test releases it.
struct Probe {
    blocking: bool,
    /// Reads admitted and not yet released.
    active: AtomicUsize,
    max_active: AtomicUsize,
    /// Permits released by the test for every gated read.
    global_release: Arc<Semaphore>,
    /// Per-file release gates, so a test can order completions.
    path_release: Mutex<HashMap<String, Arc<Semaphore>>>,
    /// Paths whose metadata read must fail.
    failing: Mutex<HashSet<String>>,
    started: mpsc::UnboundedSender<String>,
    completed: mpsc::UnboundedSender<String>,
}

impl Probe {
    fn new(
        blocking: bool,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<String>,
        mpsc::UnboundedReceiver<String>,
    ) {
        let (started, starts) = mpsc::unbounded_channel();
        let (completed, completions) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                blocking,
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                global_release: Arc::new(Semaphore::new(0)),
                path_release: Mutex::new(HashMap::new()),
                failing: Mutex::new(HashSet::new()),
                started,
                completed,
            }),
            starts,
            completions,
        )
    }

    /// The release gate for one file's reads, created on first use.
    fn path_gate(&self, path: &str) -> Arc<Semaphore> {
        Arc::clone(
            self.path_release
                .lock()
                .expect("path release gates")
                .entry(path.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(0))),
        )
    }

    fn fail(&self, path: &str) {
        self.failing
            .lock()
            .expect("failing paths")
            .insert(path.to_string());
    }

    fn fails(&self, path: &str) -> bool {
        self.failing.lock().expect("failing paths").contains(path)
    }

    /// Registers a metadata read and waits for the test to release it.
    async fn admit(&self, path: &str) {
        if !self.blocking {
            return;
        }
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let _ = self.started.send(path.to_string());
        let release = self
            .path_release
            .lock()
            .expect("path release gates")
            .get(path)
            .map(Arc::clone)
            .unwrap_or_else(|| Arc::clone(&self.global_release));
        let _permit = release
            .acquire()
            .await
            .expect("the test never closes a release gate");
        self.active.fetch_sub(1, Ordering::SeqCst);
        let _ = self.completed.send(path.to_string());
    }
}

struct ProbeStore {
    inner: Arc<dyn ObjectStore>,
    probe: Arc<Probe>,
}

impl ProbeStore {
    fn new(inner: Arc<dyn ObjectStore>, probe: Arc<Probe>) -> Self {
        Self { inner, probe }
    }
}

impl Debug for ProbeStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl Display for ProbeStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProbeStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ProbeStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> StoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    /// The metadata fetch path: every parquet footer read goes through here.
    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> StoreResult<Vec<Bytes>> {
        let path = location.to_string();
        if self.probe.fails(&path) {
            return Err(object_store::Error::Generic {
                store: "probe",
                source: "injected metadata read failure".into(),
            });
        }
        self.probe.admit(&path).await;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<ObjectPath>>,
    ) -> BoxStream<'static, StoreResult<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn local_store(dir: &Path, probe: &Arc<Probe>) -> Arc<dyn ObjectStore> {
    let inner = LocalFileSystem::new_with_prefix(dir).expect("local object store");
    Arc::new(ProbeStore::new(Arc::new(inner), Arc::clone(probe)))
}

/// A session with its own store registry, so two sessions in one process have
/// no shared metadata cache: every session really has to read its files.
///
/// Every session explicitly asks for a wider metadata concurrency than the
/// child process budget has files. The session may buffer that many file
/// futures, so the tests observe the process budget — not DataFusion's default,
/// and not the first caller's session setting, which must not raise it.
fn session_with_store(url: &Url, store: Arc<dyn ObjectStore>) -> SessionState {
    let runtime = Arc::new(RuntimeEnv::default());
    runtime.register_object_store(url, store);
    let mut config = SessionConfig::new();
    config.options_mut().execution.meta_fetch_concurrency =
        datafusion::common::config::ConfigNonZeroUsize::try_new(8)
            .expect("a session asks for more metadata concurrency than the budget");
    SessionContext::new_with_config_rt(config, runtime).state()
}

// ---------------------------------------------------------------------------
// Parquet fixtures and expectations.
// ---------------------------------------------------------------------------

/// One written file: where it is, how many rows it holds and whether it has
/// the column added after the base schema.
struct ExpectedFile {
    name: &'static str,
    rows: usize,
    wide: bool,
}

impl ExpectedFile {
    fn field_names(&self) -> Vec<&'static str> {
        if self.wide {
            vec!["id", "payload", "evolved"]
        } else {
            vec!["id", "payload"]
        }
    }
}

fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, true),
        Field::new("evolved", DataType::Int64, true),
    ]))
}

/// Writes each group's files and returns the file metadata per group, plus the
/// concatenated expectation in input order.
fn write_groups(
    dir: &Path,
    groups: &[&[(&'static str, &[i32], bool)]],
) -> (Vec<Vec<ObjectMeta>>, Vec<ExpectedFile>) {
    let mut metas = Vec::with_capacity(groups.len());
    let mut expected = Vec::new();
    for group in groups {
        let mut group_metas = Vec::with_capacity(group.len());
        for &(name, ids, wide) in *group {
            group_metas.push(write_parquet(dir, name, ids, wide));
            expected.push(ExpectedFile {
                name,
                rows: ids.len(),
                wide,
            });
        }
        metas.push(group_metas);
    }
    (metas, expected)
}

/// Writes one real parquet file with `ids` and a payload derived from them.
fn write_parquet(dir: &Path, name: &str, ids: &[i32], wide: bool) -> ObjectMeta {
    let schema = if wide { wide_schema() } else { base_schema() };
    let payload: Vec<String> = ids.iter().map(|id| format!("v{id}")).collect();
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(ids.to_vec())),
        Arc::new(StringArray::from(payload)),
    ];
    if wide {
        let evolved: Vec<i64> = ids.iter().map(|id| i64::from(*id) * 10).collect();
        columns.push(Arc::new(Int64Array::from(evolved)));
    }
    let batch = RecordBatch::try_new(schema.clone(), columns).expect("record batch");
    let path = dir.join(name);
    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");

    ObjectMeta {
        location: ObjectPath::from(name),
        last_modified: chrono::DateTime::<chrono::Utc>::from(
            std::time::SystemTime::UNIX_EPOCH,
        ),
        size: std::fs::metadata(&path).expect("file metadata").len(),
        e_tag: None,
        version: None,
    }
}

fn scan_config(
    url: &ObjectStoreUrl,
    format: &ParquetFormat,
    target_schema: SchemaRef,
    groups: Vec<Vec<ObjectMeta>>,
) -> FileScanConfig {
    let table_schema = TableSchema::new(target_schema, Vec::new());
    let file_groups = groups
        .into_iter()
        .map(|metas| {
            FileGroup::new(
                metas
                    .into_iter()
                    .map(PartitionedFile::new_from_meta)
                    .collect(),
            )
        })
        .collect();
    FileScanConfigBuilder::new(url.clone(), format.file_source(table_schema))
        .with_file_groups(file_groups)
        .build()
}

fn assert_output_configs(
    configs: &[FileScanConfig],
    expected: &[ExpectedFile],
    context: &str,
) {
    assert_eq!(
        configs.len(),
        expected.len(),
        "{context}: one flattened config per input file"
    );
    for (config, expected) in configs.iter().zip(expected) {
        assert_eq!(
            config.file_groups.len(),
            1,
            "{context}: {} is its own file group",
            expected.name
        );
        let files = config.file_groups[0].files();
        assert_eq!(
            files.len(),
            1,
            "{context}: {} is alone in its file group",
            expected.name
        );
        assert_eq!(
            files[0].object_meta.location.to_string(),
            expected.name,
            "{context}: flattened configs follow the input group and file order"
        );
        assert_eq!(
            config.statistics().num_rows,
            Precision::Exact(expected.rows),
            "{context}: {} keeps its real row count",
            expected.name
        );
        let field_names: Vec<&str> = config
            .file_schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(
            field_names,
            expected.field_names(),
            "{context}: {} keeps its real schema",
            expected.name
        );
    }
}
