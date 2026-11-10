// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for the split write path.
//!
//! The module is included from `write_exec.rs`, so the report format, the
//! commit guard, and the metric recording it exercises are the production
//! ones rather than a re-implementation.

use std::sync::Mutex as StdMutex;
use std::time::Duration;

use super::*;
use crate::tests::create_table_with_vector_index;
use crate::vector_index::{VectorIndexParams, VectorIndexTableConfig};
use datafusion::physical_plan::empty::EmptyExec;
use futures::TryStreamExt;
use lakesoul_common::ser::arrow_java::schema_to_metadata_str;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_metadata::MetaDataClient;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::subscriber::with_default;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// Emits the report stream a write node produces.
///
/// `delay` stands in for the time the file writers take: the reports only
/// exist once every file has been written, so the commit node must not start
/// its commit measurement before it has drained them.
#[derive(Debug)]
struct ReportExec {
    reports: Vec<WriteFileReport>,
    delay: Option<Duration>,
    properties: Arc<PlanProperties>,
}

impl DisplayAs for ReportExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ReportExec")
    }
}

impl ReportExec {
    fn new(reports: Vec<WriteFileReport>, delay: Option<Duration>) -> Self {
        Self {
            reports,
            delay,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(report_schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }
}

impl ExecutionPlan for ReportExec {
    fn name(&self) -> &str {
        "ReportExec"
    }

    fn schema(&self) -> SchemaRef {
        report_schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let batch = report_batch(&self.reports)?;
        let schema = report_schema();
        let delay = self.delay;
        let stream = futures::stream::once(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// The writers' NOT NULL validation must also reject null values of a nested
/// column whose parent struct is null, and a sparse input that omits a stored
/// column the table requires.
#[test]
fn nested_not_null_checks_reject_null_ancestors_and_missing_structs() {
    use arrow::array::{Int32Array, StructArray};
    use arrow::buffer::NullBuffer;

    let stored_children: arrow::datatypes::Fields =
        vec![Field::new("v", DataType::Int32, false)].into();
    let source_children: arrow::datatypes::Fields =
        vec![Field::new("v", DataType::Int32, true)].into();
    let table = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("s", DataType::Struct(stored_children), true),
    ]);
    let input = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("s", DataType::Struct(source_children.clone()), true),
    ]));
    let checks = write_checks(&table, &input, &[], &["id".into()]).unwrap();
    for (value, parent_valid, accepted) in [
        (Some(10), true, true),
        (None, true, false),
        (Some(10), false, false),
    ] {
        let nested = StructArray::new(
            source_children.clone(),
            vec![Arc::new(Int32Array::from(vec![value]))],
            Some(NullBuffer::from(vec![parent_valid])),
        );
        let batch = RecordBatch::try_new(
            input.clone(),
            vec![Arc::new(Int32Array::from(vec![1])), Arc::new(nested)],
        )
        .unwrap();
        let result = checks.validate(&batch);
        assert_eq!(result.is_ok(), accepted);
    }
    let key_only = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    assert!(write_checks(&table, &key_only, &[], &["id".into()]).is_err());
}

/// The recorder is thread-local, so the commit has to run on this thread.
fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

fn client(runtime: &tokio::runtime::Runtime) -> MetaDataClientRef {
    runtime.block_on(async { Arc::new(MetaDataClient::from_env().await.unwrap()) })
}

/// Value of the `prefix{labels}` sample in Prometheus text exposition.
fn sample(text: &str, prefix: &str, labels: &[&str]) -> Option<f64> {
    text.lines()
        .find(|line| {
            line.starts_with(prefix) && labels.iter().all(|label| line.contains(label))
        })
        .and_then(|line| line.rsplit(' ').next()?.parse().ok())
}

/// A commit node that publishes into `default.<table_name>`.
fn commit_exec(
    input: Arc<dyn ExecutionPlan>,
    table_name: &str,
    client: MetaDataClientRef,
) -> LakeSoulCommitExec {
    LakeSoulCommitExec::new(
        input,
        Arc::new(TableInfo {
            table_namespace: "default".to_string(),
            table_name: table_name.to_string(),
            properties: "{}".to_string(),
            ..Default::default()
        }),
        client,
        vec!["pk".to_string()],
        SecretMap::new(),
        None,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicBool::new(false)),
    )
}

/// Runs a commit node and returns the row count it reports to the front end.
fn run_commit(
    runtime: &tokio::runtime::Runtime,
    commit: LakeSoulCommitExec,
) -> DFResult<u64> {
    runtime.block_on(async {
        let stream = commit.execute(0, Arc::new(TaskContext::default()))?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let batch = batches
            .into_iter()
            .next()
            .expect("the commit returns one batch");
        Ok(batch
            .column_by_name("count")
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .map(|counts| counts.value(0))
            .expect("the commit batch carries the row count"))
    })
}

/// The commit metric covers the metadata commit alone. The reports only exist
/// after the file writers are done, so their time must not show up as commit
/// latency.
#[test]
fn commit_latency_excludes_the_report_drain() {
    let runtime = current_thread_runtime();
    let client = client(&runtime);
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let count = metrics::with_local_recorder(&recorder, || {
        run_commit(
            &runtime,
            commit_exec(
                Arc::new(ReportExec::new(Vec::new(), Some(Duration::from_secs(1)))),
                "tmp_commit_metrics_absent",
                client.clone(),
            ),
        )
    })
    .expect("committing without files succeeds");
    assert_eq!(count, 0, "an empty report inserts no rows");

    let rendered = handle.render();
    let latency = sample(
        &rendered,
        &format!("{COMMIT_DURATION_SECONDS}_sum"),
        &["outcome=\"success\""],
    )
    .expect("a successful commit is recorded");
    assert!(
        latency < 0.5,
        "commit latency {latency}s must not include the 1s report drain:\n{rendered}"
    );
}

/// The commit metric covers the metadata commit alone: a failed index build
/// after a successful commit fails the write but is recorded by the index
/// metric, not as a failed commit.
#[test]
fn index_failure_is_not_reported_as_a_commit_failure() {
    let runtime = current_thread_runtime();
    let client = client(&runtime);
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();

    let table = "tmp_commit_metrics_index_failure";
    let schema = Arc::new(Schema::new(vec![
        Field::new("pk", DataType::Int64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                4,
            ),
            true,
        ),
    ]));
    let config = LakeSoulIOConfigBuilder::new()
        .with_schema(schema)
        .with_primary_keys(vec!["pk".to_string()])
        .build();
    // Stored index metadata parses, but the build it describes must fail:
    // the native builder rejects this metric.
    let vector_index_configs = vec![VectorIndexTableConfig {
        column: "vec".to_string(),
        params: VectorIndexParams {
            dim: 4,
            nlist: 4,
            total_bits: 7,
            metric: "not-a-metric".to_string(),
            rotator_type: "FhtKac".to_string(),
            seed: 42,
            use_faster_config: true,
        },
        management: Default::default(),
    }];

    let result = metrics::with_local_recorder(&recorder, || {
        runtime.block_on(async {
            create_table_with_vector_index(
                client.clone(),
                table,
                config,
                &vector_index_configs,
            )
            .await
            .expect("create the table");
            let mut partitions = PartitionedFile::new();
            partitions.insert(
                "hash=0".to_string(),
                (
                    vec![CommittedFile {
                        path: "file:///tmp/tmp_commit_metrics_index_failure.parquet"
                            .to_string(),
                        size: 1024,
                        file_exist_cols: "pk,vec".to_string(),
                    }],
                    1,
                ),
            );
            wait_for_commit(
                client.clone(),
                format!("default.{table}"),
                vec!["pk".to_string()],
                SecretMap::new(),
                partitions,
                None,
                None,
            )
            .await
        })
    });
    assert!(
        result.is_err(),
        "the index maintenance failure must still fail the write: {result:?}"
    );

    let rendered = handle.render();
    let commit_outcomes = |outcome: &str| {
        sample(
            &rendered,
            &format!("{COMMIT_DURATION_SECONDS}_count"),
            &[&format!("outcome=\"{outcome}\"")],
        )
    };
    assert_eq!(
        commit_outcomes("success"),
        Some(1.0),
        "the metadata commit succeeded:\n{rendered}"
    );
    assert_eq!(
        commit_outcomes("error"),
        None,
        "a failed index build is not a failed commit:\n{rendered}"
    );
    assert_eq!(
        sample(&rendered, COMMIT_PARTITIONS_TOTAL, &[]),
        Some(1.0),
        "the committed partition is counted:\n{rendered}"
    );
    assert_eq!(
        sample(
            &rendered,
            &format!("{INDEX_BUILD_DURATION_SECONDS}_count"),
            &["index_type=\"vector\"", "outcome=\"error\""],
        ),
        Some(1.0),
        "the index outcome is recorded by the index metric:\n{rendered}"
    );
}

/// The report stream is the only channel between a worker's file writer and
/// the coordinator's commit node. The size and column list the writer already
/// has must survive it: without them the commit records a file of unknown size
/// and every reader has to treat the file as one un-splittable piece.
#[test]
fn report_carries_the_written_size_and_column_list() {
    let reports = vec![
        WriteFileReport {
            partition_desc: "-5".to_string(),
            bucket: 2,
            path: "file:///tmp/part-abc_0002.parquet".to_string(),
            row_count: 7,
            size: 3 * 1024 * 1024,
            file_exist_cols: "pk,v".to_string(),
        },
        WriteFileReport {
            partition_desc: "-5".to_string(),
            bucket: 0,
            path: "file:///tmp/part-def_0000.parquet".to_string(),
            row_count: 1,
            // Not a realistic file, but a size a `u32` or an `as i32` cast
            // would silently truncate.
            size: i64::MAX - 1,
            file_exist_cols: "pk".to_string(),
        },
    ];
    let batch = report_batch(&reports).expect("the reports are a batch");
    let mut partitioned = PartitionedFile::new();
    let rows = collect_reports(&batch, &mut partitioned).expect("the batch is readable");
    assert_eq!(rows, 8, "the commit counts the reported rows");
    let (files, row_count) = partitioned.get("-5").expect("one partition");
    assert_eq!(*row_count, 8, "the partition sums the reported rows");
    assert_eq!(
        files,
        &vec![
            CommittedFile {
                path: "file:///tmp/part-abc_0002.parquet".to_string(),
                size: 3 * 1024 * 1024,
                file_exist_cols: "pk,v".to_string(),
            },
            CommittedFile {
                path: "file:///tmp/part-def_0000.parquet".to_string(),
                size: i64::MAX - 1,
                file_exist_cols: "pk".to_string(),
            },
        ],
        "the report must keep each file's size and stored columns"
    );
}

/// `(span name, parent span name)` of every span created through this layer.
type CreatedSpans = Arc<StdMutex<Vec<(String, Option<String>)>>>;

#[derive(Clone, Default)]
struct CapturedSpans(CreatedSpans);

impl CapturedSpans {
    fn parent_of(&self, name: &str) -> Option<Option<String>> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|(span, _)| span == name)
            .map(|(_, parent)| parent.clone())
    }

    fn names(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(span, _)| span.clone())
            .collect()
    }
}

impl<S> Layer<S> for CapturedSpans
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: LayerContext<'_, S>,
    ) {
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent().map(|parent| parent.name().to_string()));
        self.0
            .lock()
            .unwrap()
            .push((attrs.metadata().name().to_string(), parent));
    }
}

/// A write node writing an empty table: the plan needs no store and no
/// metadata client, and its write tasks still have to be spawned.
fn empty_write_exec() -> LakeSoulWriteExec {
    LakeSoulWriteExec::new(
        Arc::new(EmptyExec::new(Arc::new(Schema::empty()))),
        None,
        Arc::new(TableInfo {
            partitions: ";".to_string(),
            properties: "{}".to_string(),
            table_schema: schema_to_metadata_str(&Schema::empty()),
            ..Default::default()
        }),
    )
    .expect("an empty table can be written")
}

/// The write node spawns its per-partition writer with `tokio::spawn`. A
/// spawned future does not inherit a scoped subscriber, and an `#[instrument]`
/// async fn builds its span on the first poll, so the node has to carry its
/// span and dispatcher into the task for the writer to stay in the statement
/// trace.
#[test]
fn spawned_write_tasks_join_the_write_span() {
    let captured = CapturedSpans::default();
    let subscriber = Registry::default().with(captured.clone());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    with_default(subscriber, || {
        runtime.block_on(async {
            let write = empty_write_exec();
            let statement = tracing::info_span!("statement_execute");
            let stream = {
                let _entered = statement.enter();
                write.execute(0, Arc::new(TaskContext::default())).unwrap()
            };
            // The report stream completes once every write task is joined, so
            // the spawned spans exist by then.
            let _: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        });
    });

    assert_eq!(
        captured.parent_of("lake_soul_write_execute"),
        Some(Some("statement_execute".to_string())),
        "the write span must hang off the statement span: {:?}",
        captured.names()
    );
    assert_eq!(
        captured.parent_of("table_write_partition"),
        Some(Some("lake_soul_write_execute".to_string())),
        "the production write spawn must join the write trace: {:?}",
        captured.names()
    );
}

/// The commit node spawns the metadata commit with `tokio::spawn`; its span is
/// created before the spawn so the commit stays in the statement trace instead
/// of taking whichever span the executor thread happens to have.
#[test]
fn spawned_commit_task_joins_the_commit_span() {
    let runtime = current_thread_runtime();
    let client = client(&runtime);
    let captured = CapturedSpans::default();
    let subscriber = Registry::default().with(captured.clone());

    with_default(subscriber, || {
        runtime.block_on(async {
            let commit = commit_exec(
                Arc::new(ReportExec::new(Vec::new(), None)),
                "tmp_span_commit_execute",
                client,
            );
            let statement = tracing::info_span!("statement_execute");
            let stream = {
                let _entered = statement.enter();
                commit.execute(0, Arc::new(TaskContext::default())).unwrap()
            };
            let _: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        });
    });

    assert_eq!(
        captured.parent_of("lake_soul_commit_execute"),
        Some(Some("statement_execute".to_string())),
        "the commit span must hang off the statement span: {:?}",
        captured.names()
    );
}

/// `execute` captures the caller's dispatcher, and every span it creates has to
/// be created through that dispatcher. `Span::current()` and `info_span!` each
/// read the thread-local dispatcher, so resolving the parent under one registry
/// and creating the child under another makes `Registry::new_span` clone an id
/// it does not own, which panics with "no span exists with that ID".
#[test]
fn node_spans_use_the_captured_dispatcher() {
    let owner_spans = CapturedSpans::default();
    let owner = tracing::Dispatch::new(Registry::default().with(owner_spans.clone()));
    let foreign_spans = CapturedSpans::default();
    let foreign = tracing::Dispatch::new(Registry::default().with(foreign_spans.clone()));

    // The caller's statement span belongs to `owner` and is current on this
    // thread, while the ambient dispatcher below is `foreign`.
    let statement = tracing::dispatcher::with_default(&owner, || {
        tracing::info_span!("statement_execute")
    });
    let entered = statement.enter();
    let (write, commit) = tracing::dispatcher::with_default(&foreign, || {
        (
            LakeSoulWriteExec::create_exec_span(&owner, &TableInfo::default(), "write"),
            LakeSoulCommitExec::create_commit_span(&owner, &TableInfo::default()),
        )
    });
    drop(entered);
    drop(write);
    drop(commit);

    assert_eq!(
        owner_spans.parent_of("lake_soul_write_execute"),
        Some(Some("statement_execute".to_string())),
        "the write span must hang off the caller's statement span"
    );
    assert_eq!(
        owner_spans.parent_of("lake_soul_commit_execute"),
        Some(Some("statement_execute".to_string())),
        "the commit span must hang off the caller's statement span"
    );
    assert!(
        foreign_spans.0.lock().unwrap().is_empty(),
        "the node spans must not be created in the ambient registry: {:?}",
        foreign_spans.names()
    );

    // The shape this guards against: the child created under the ambient
    // dispatcher while the parent lives in another registry.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tracing::dispatcher::with_default(&foreign, || {
            let _ = tracing::info_span!(parent: &statement, "unscoped_child");
        });
    }));
    assert!(
        panicked.is_err(),
        "cloning a parent owned by another registry is a hard panic"
    );
}
