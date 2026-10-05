// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The metrics emitted by the SQL entry and the refresh engine.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTableOptions, PhysicalFormat, sum_count_mv_schema_for,
};
use metrics::{
    Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// The metrics captured by the test recorder, keyed by `name{label=value}`.
#[derive(Default)]
struct Metrics {
    counters: Mutex<HashMap<String, u64>>,
    histograms: Mutex<HashMap<String, u64>>,
}

impl Metrics {
    fn counter(&self, key: &str) -> u64 {
        *self.counters.lock().unwrap().get(key).unwrap_or(&0)
    }

    fn histogram(&self, key: &str) -> u64 {
        *self.histograms.lock().unwrap().get(key).unwrap_or(&0)
    }
}

/// A canonical `name{label=value,...}` key with the labels sorted.
fn metric_key(key: &Key) -> String {
    let mut labels = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect::<Vec<_>>();
    labels.sort();
    format!("{}{{{}}}", key.name(), labels.join(","))
}

struct TestCounter {
    metrics: Arc<Metrics>,
    key: String,
}

impl metrics::CounterFn for TestCounter {
    fn increment(&self, value: u64) {
        *self
            .metrics
            .counters
            .lock()
            .unwrap()
            .entry(self.key.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        self.metrics
            .counters
            .lock()
            .unwrap()
            .insert(self.key.clone(), value);
    }
}

struct TestHistogram {
    metrics: Arc<Metrics>,
    key: String,
}

impl metrics::HistogramFn for TestHistogram {
    fn record(&self, _value: f64) {
        *self
            .metrics
            .histograms
            .lock()
            .unwrap()
            .entry(self.key.clone())
            .or_default() += 1;
    }
}

struct TestRecorder {
    metrics: Arc<Metrics>,
}

impl Recorder for TestRecorder {
    fn describe_counter(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(TestCounter {
            metrics: self.metrics.clone(),
            key: metric_key(key),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(TestHistogram {
            metrics: self.metrics.clone(),
            key: metric_key(key),
        }))
    }
}

#[test_log::test]
fn executor_and_engine_record_metrics() {
    let state = Arc::new(Metrics::default());
    let recorder = TestRecorder {
        metrics: state.clone(),
    };
    metrics::with_local_recorder(&recorder, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let runtime = IvmRuntime::from_env().await.unwrap();
            runtime.init_schema().await.unwrap();
            let dir = tempdir().unwrap();
            let suffix = uuid::Uuid::new_v4().simple();
            let source = runtime
                .create_table(
                    IvmTableOptions::new(
                        format!("ivm_metrics_src_{suffix}"),
                        format!("file://{}", dir.path().join("src").display()),
                        schema(),
                    )
                    .with_primary_keys(vec!["k".to_string()])
                    .with_cdc_column(CHANGE_COLUMN)
                    .with_file_format(PhysicalFormat::Vortex),
                )
                .await
                .unwrap();
            let mv = runtime
                .create_table(
                    IvmTableOptions::new(
                        format!("ivm_metrics_mv_{suffix}"),
                        format!("file://{}", dir.path().join("mv").display()),
                        sum_count_mv_schema_for(&schema(), &["g".to_string()], Some("v"))
                            .unwrap(),
                    )
                    .with_primary_keys(vec!["g".to_string()])
                    .with_file_format(PhysicalFormat::Vortex),
                )
                .await
                .unwrap();
            let context = SessionContext::new();
            context
                .register_table(
                    source.table_name.as_str(),
                    Arc::new(runtime.table_provider(&source)),
                )
                .unwrap();
            let executor = IvmSqlExecutor::new(runtime).with_session(context);
            let sql = format!(
                "INSERT INTO {} SELECT g, SUM(v) FROM {} GROUP BY g",
                mv.table_name, source.table_name
            );
            // The first execution bootstraps the (empty) view.
            executor.execute(&sql).await.unwrap();
            // One row and an incremental refresh.
            source
                .append_batch(
                    executor.runtime().client(),
                    batch(&[(1, "a", 10, "insert")]),
                )
                .await
                .unwrap();
            executor.execute(&sql).await.unwrap();
            // No new data: the statement refreshes an empty window.
            executor.execute(&sql).await.unwrap();
            // A changed definition rebuilds the view.
            executor
                .execute(&format!(
                    "INSERT INTO {} SELECT g, SUM(v) FROM {} WHERE v > 0 GROUP BY g",
                    mv.table_name, source.table_name
                ))
                .await
                .unwrap();
        });
    });

    assert_eq!(
        state.counter("lakesoul_ivm_statements_total{action=bootstrap}"),
        1
    );
    assert_eq!(
        state.counter("lakesoul_ivm_statements_total{action=incremental}"),
        2
    );
    assert_eq!(
        state.counter("lakesoul_ivm_statements_total{action=rebuild}"),
        1
    );
    assert_eq!(
        state.counter("lakesoul_ivm_refreshes_total{kind=sum_count,result=applied}"),
        1
    );
    assert_eq!(
        state.counter("lakesoul_ivm_refreshes_total{kind=sum_count,result=noop}"),
        1
    );
    assert_eq!(
        state.counter("lakesoul_ivm_rebuilds_total{kind=sum_count}"),
        2
    );
    assert_eq!(
        state.counter("lakesoul_ivm_epochs_total{kind=sum_count}"),
        3
    );
    assert_eq!(
        state.histogram("lakesoul_ivm_statement_duration_seconds{action=bootstrap}"),
        1
    );
    assert_eq!(
        state.histogram("lakesoul_ivm_refresh_duration_seconds{kind=sum_count}"),
        2
    );
    assert_eq!(
        state.histogram("lakesoul_ivm_rebuild_duration_seconds{kind=sum_count}"),
        2
    );
}
