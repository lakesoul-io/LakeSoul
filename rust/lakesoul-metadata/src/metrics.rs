// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Metrics for metadata operations.
//!
//! The `metadata_*` tracing spans already carry per-request detail; the
//! metrics here mirror them for aggregation so dashboards and alerts do not
//! depend on trace sampling. `dao_type` comes from the finite [`DaoType`]
//! enum, so it is safe to use as a label.
//!
//! [`DaoType`]: crate::DaoType

use std::sync::Once;
use std::time::Instant;

const REQUESTS_TOTAL: &str = "lakesoul_metadata_requests_total";
const REQUEST_DURATION_SECONDS: &str = "lakesoul_metadata_request_duration_seconds";
const ROWS_TOTAL: &str = "lakesoul_metadata_rows_total";
const POOL_CONNECTIONS: &str = "lakesoul_metadata_pool_connections";

static DESCRIBE_METRICS: Once = Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            REQUESTS_TOTAL,
            "Metadata requests grouped by operation, DAO type and outcome"
        );
        metrics::describe_histogram!(
            REQUEST_DURATION_SECONDS,
            metrics::Unit::Seconds,
            "Metadata request latency grouped by operation, DAO type and outcome"
        );
        metrics::describe_counter!(
            ROWS_TOTAL,
            "Rows returned or affected by metadata operations"
        );
        metrics::describe_gauge!(
            POOL_CONNECTIONS,
            "Metadata PostgreSQL connection pool connections by pool and state"
        );
    });
}

/// Records one metadata request from creation until it succeeds or is dropped.
///
/// Call [`MetadataRequestMetrics::success`] on the success path. Dropping the
/// guard without that call records the request as `error`, so early `?`
/// and explicit `return Err` paths stay accounted for.
pub(crate) struct MetadataRequestMetrics {
    operation: &'static str,
    dao_type: String,
    started: Instant,
    rows: u64,
    finished: bool,
}

impl MetadataRequestMetrics {
    pub(crate) fn start(operation: &'static str, dao_type: String) -> Self {
        describe_metrics();
        Self {
            operation,
            dao_type,
            started: Instant::now(),
            rows: 0,
            finished: false,
        }
    }

    /// Attaches a row count to the request; recorded only on success.
    pub(crate) fn record_rows(&mut self, rows: u64) {
        self.rows = rows;
    }

    pub(crate) fn success(mut self) {
        self.finish("success");
    }

    fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;

        metrics::counter!(
            REQUESTS_TOTAL,
            "operation" => self.operation,
            "dao_type" => self.dao_type.clone(),
            "outcome" => outcome,
        )
        .increment(1);
        metrics::histogram!(
            REQUEST_DURATION_SECONDS,
            "operation" => self.operation,
            "dao_type" => self.dao_type.clone(),
            "outcome" => outcome,
        )
        .record(self.started.elapsed().as_secs_f64());
        if self.rows > 0 {
            metrics::counter!(
                ROWS_TOTAL,
                "operation" => self.operation,
                "dao_type" => self.dao_type.clone(),
            )
            .increment(self.rows);
        }
    }
}

impl Drop for MetadataRequestMetrics {
    fn drop(&mut self) {
        self.finish("error");
    }
}

/// Publishes connection-pool gauges for one physical pool.
pub(crate) fn record_pool_metrics(pool: &'static str, state: &bb8_postgres::bb8::State) {
    let in_use = state.connections.saturating_sub(state.idle_connections);
    metrics::gauge!(POOL_CONNECTIONS, "pool" => pool, "state" => "total")
        .set(state.connections as f64);
    metrics::gauge!(POOL_CONNECTIONS, "pool" => pool, "state" => "idle")
        .set(state.idle_connections as f64);
    metrics::gauge!(POOL_CONNECTIONS, "pool" => pool, "state" => "in_use")
        .set(in_use as f64);
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    #[test]
    fn successful_request_records_metrics_once() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            let mut request =
                MetadataRequestMetrics::start("query", "TestDao".to_string());
            request.record_rows(3);
            request.success();
        });

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "lakesoul_metadata_requests_total{operation=\"query\",dao_type=\"TestDao\",outcome=\"success\"} 1"
            ),
            "requests metric missing in:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "lakesoul_metadata_rows_total{operation=\"query\",dao_type=\"TestDao\"} 3"
            ),
            "rows metric missing in:\n{rendered}"
        );
    }

    #[test]
    fn dropped_request_records_error_outcome() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        metrics::with_local_recorder(&recorder, || {
            let _request = MetadataRequestMetrics::start("update", "TestDao".to_string());
        });

        let rendered = handle.render();
        assert!(rendered.contains("outcome=\"error\"} 1"));
    }
}
