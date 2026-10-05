// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the incremental maintenance entry.
//!
//! The counters and histograms follow the `lakesoul_*` naming used by the IO
//! layer; callers install a recorder (for example the Prometheus exporter) to
//! export them. The labels stay low cardinality: the view kind and the action,
//! never a view or table id.

use std::sync::Once;
use std::time::Duration;

const STATEMENTS_TOTAL: &str = "lakesoul_ivm_statements_total";
const STATEMENT_DURATION: &str = "lakesoul_ivm_statement_duration_seconds";
const REFRESHES_TOTAL: &str = "lakesoul_ivm_refreshes_total";
const REFRESH_DURATION: &str = "lakesoul_ivm_refresh_duration_seconds";
const REBUILDS_TOTAL: &str = "lakesoul_ivm_rebuilds_total";
const REBUILD_DURATION: &str = "lakesoul_ivm_rebuild_duration_seconds";
const EPOCHS_TOTAL: &str = "lakesoul_ivm_epochs_total";

static DESCRIBE_METRICS: Once = Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            STATEMENTS_TOTAL,
            "Total number of incremental maintenance statements by action"
        );
        metrics::describe_histogram!(
            STATEMENT_DURATION,
            metrics::Unit::Seconds,
            "Incremental maintenance statement duration by action"
        );
        metrics::describe_counter!(
            REFRESHES_TOTAL,
            "Total number of view refreshes by view kind and result"
        );
        metrics::describe_histogram!(
            REFRESH_DURATION,
            metrics::Unit::Seconds,
            "View refresh duration by view kind"
        );
        metrics::describe_counter!(
            REBUILDS_TOTAL,
            "Total number of view rebuilds by view kind"
        );
        metrics::describe_histogram!(
            REBUILD_DURATION,
            metrics::Unit::Seconds,
            "View rebuild duration by view kind"
        );
        metrics::describe_counter!(
            EPOCHS_TOTAL,
            "Total number of committed epochs by view kind"
        );
    });
}

/// One `INSERT INTO <mv> SELECT ...` execution; `action` is the action label
/// (`bootstrap`, `incremental`, `rebuild`, `overwrite` or `error`).
pub(crate) fn record_statement(action: &str, duration: Duration) {
    describe_metrics();
    metrics::counter!(STATEMENTS_TOTAL, "action" => action.to_string()).increment(1);
    metrics::histogram!(STATEMENT_DURATION, "action" => action.to_string())
        .record(duration.as_secs_f64());
}

/// One view refresh: `applied` is true when a window was consumed and an epoch
/// committed, false for an empty window.
pub(crate) fn record_refresh(kind: &str, applied: bool, duration: Duration) {
    describe_metrics();
    let result = if applied { "applied" } else { "noop" };
    metrics::counter!(
        REFRESHES_TOTAL,
        "kind" => kind.to_string(),
        "result" => result.to_string(),
    )
    .increment(1);
    metrics::histogram!(REFRESH_DURATION, "kind" => kind.to_string())
        .record(duration.as_secs_f64());
    if applied {
        metrics::counter!(EPOCHS_TOTAL, "kind" => kind.to_string()).increment(1);
    }
}

/// One view rebuild from the full source state.
pub(crate) fn record_rebuild(kind: &str, duration: Duration) {
    describe_metrics();
    metrics::counter!(REBUILDS_TOTAL, "kind" => kind.to_string()).increment(1);
    metrics::histogram!(REBUILD_DURATION, "kind" => kind.to_string())
        .record(duration.as_secs_f64());
    metrics::counter!(EPOCHS_TOTAL, "kind" => kind.to_string()).increment(1);
}
