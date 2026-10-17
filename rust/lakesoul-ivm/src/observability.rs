// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Metrics for the incremental maintenance entry.
//!
//! The counters and histograms follow the `lakesoul_*` naming used by the IO
//! layer; callers install a recorder (for example the Prometheus exporter) to
//! export them. The labels stay low cardinality: the view kind and the action,
//! never a view or table id.

use std::cell::RefCell;
use std::future::Future;
use std::sync::Once;
use std::time::Duration;

const STATEMENTS_TOTAL: &str = "lakesoul_ivm_statements_total";
const STATEMENT_DURATION: &str = "lakesoul_ivm_statement_duration_seconds";
const REFRESHES_TOTAL: &str = "lakesoul_ivm_refreshes_total";
const REFRESH_DURATION: &str = "lakesoul_ivm_refresh_duration_seconds";
const REBUILDS_TOTAL: &str = "lakesoul_ivm_rebuilds_total";
const REBUILD_DURATION: &str = "lakesoul_ivm_rebuild_duration_seconds";
const EPOCHS_TOTAL: &str = "lakesoul_ivm_epochs_total";
const FALLBACKS_TOTAL: &str = "lakesoul_ivm_fallbacks_total";
const FALLBACK_PARTITIONS: &str = "lakesoul_ivm_fallback_partitions_total";
const FALLBACK_GUARD: &str = "lakesoul_ivm_fallback_guard_total";
const FALLBACK_ROWS: &str = "lakesoul_ivm_fallback_rows";
const REFRESH_FILES_READ: &str = "lakesoul_ivm_refresh_files_read";
const REFRESH_ROWS_SCANNED: &str = "lakesoul_ivm_refresh_rows_scanned";
const MV_ROWS_WRITTEN: &str = "lakesoul_ivm_mv_rows_written";
const MV_BYTES_WRITTEN: &str = "lakesoul_ivm_mv_bytes_written";

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
            FALLBACK_PARTITIONS,
            "Total number of partitions a pruned Tier-2 fallback refresh recomputed"
        );
        metrics::describe_counter!(
            FALLBACK_GUARD,
            "Total number of pruned Tier-2 fallback refreshes that recomputed the whole view"
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
        metrics::describe_counter!(
            FALLBACKS_TOTAL,
            "Total number of Tier-2 fallback recomputes (whole-statement refreshes)"
        );
        metrics::describe_counter!(
            FALLBACK_ROWS,
            "Rows written by Tier-2 fallback recomputes (a cost proxy)"
        );
        metrics::describe_counter!(
            REFRESH_FILES_READ,
            "Data files read by one view refresh, by view kind"
        );
        metrics::describe_counter!(
            REFRESH_ROWS_SCANNED,
            "Rows read by one view refresh, by view kind"
        );
        metrics::describe_counter!(
            MV_ROWS_WRITTEN,
            "Materialized-view rows written by one view refresh, by view kind"
        );
        metrics::describe_counter!(
            MV_BYTES_WRITTEN,
            "Materialized-view bytes written by one view refresh, by view kind"
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

/// One Tier-2 fallback recompute: the whole statement ran because its shape
/// is not maintained incrementally.  `rows` counts the rows it wrote, so a
/// cost spike is visible without a view id label.
pub(crate) fn record_fallback(rows: u64) {
    describe_metrics();
    metrics::counter!(FALLBACKS_TOTAL).increment(1);
    metrics::counter!(FALLBACK_ROWS).increment(rows);
}

/// The partitions a pruned fallback refresh recomputed.
pub(crate) fn record_fallback_partitions(partitions: u64) {
    describe_metrics();
    metrics::counter!(FALLBACK_PARTITIONS).increment(partitions);
}

/// A pruned fallback refresh whose rows left the restriction: the refresh
/// recomputed the whole view instead (an analysis bug, not a data bug).
pub(crate) fn record_fallback_guard() {
    describe_metrics();
    metrics::counter!(FALLBACK_GUARD).increment(1);
}

/// One view rebuild from the full source state.
pub(crate) fn record_rebuild(kind: &str, duration: Duration) {
    describe_metrics();
    metrics::counter!(REBUILDS_TOTAL, "kind" => kind.to_string()).increment(1);
    metrics::histogram!(REBUILD_DURATION, "kind" => kind.to_string())
        .record(duration.as_secs_f64());
    metrics::counter!(EPOCHS_TOTAL, "kind" => kind.to_string()).increment(1);
}

/// The IO one refresh consumed, attributed to its task.
#[derive(Clone, Copy, Default)]
pub(crate) struct RefreshIo {
    files_read: u64,
    rows_scanned: u64,
    rows_written: u64,
    bytes_written: u64,
}

tokio::task_local! {
    static REFRESH_IO: RefCell<RefreshIo>;
}

/// Run one view refresh (or rebuild) with its IO counters scoped to the task.
///
/// The counters are recorded when the future completes; a read outside a
/// refresh scope is not attributed to any view.
pub(crate) async fn scope_refresh_io<F, T>(kind: &str, future: F) -> T
where
    F: Future<Output = T>,
{
    REFRESH_IO
        .scope(RefCell::new(RefreshIo::default()), async {
            let value = future.await;
            let io = REFRESH_IO.with(|cell| *cell.borrow());
            record_refresh_io(kind, io);
            value
        })
        .await
}

/// One data file read by the current refresh.
pub(crate) fn add_files_read(files: u64) {
    let _ = REFRESH_IO.try_with(|cell| cell.borrow_mut().files_read += files);
}

/// Rows returned by the reader of the current refresh.
pub(crate) fn add_rows_scanned(rows: u64) {
    let _ = REFRESH_IO.try_with(|cell| cell.borrow_mut().rows_scanned += rows);
}

/// Rows appended to a materialized view by the current refresh.
pub(crate) fn add_rows_written(rows: u64) {
    let _ = REFRESH_IO.try_with(|cell| cell.borrow_mut().rows_written += rows);
}

/// Bytes of the files a materialized-view append wrote.
pub(crate) fn add_bytes_written(bytes: u64) {
    let _ = REFRESH_IO.try_with(|cell| cell.borrow_mut().bytes_written += bytes);
}

fn record_refresh_io(kind: &str, io: RefreshIo) {
    describe_metrics();
    if io.files_read > 0 {
        metrics::counter!(REFRESH_FILES_READ, "kind" => kind.to_string())
            .increment(io.files_read);
    }
    if io.rows_scanned > 0 {
        metrics::counter!(REFRESH_ROWS_SCANNED, "kind" => kind.to_string())
            .increment(io.rows_scanned);
    }
    if io.rows_written > 0 {
        metrics::counter!(MV_ROWS_WRITTEN, "kind" => kind.to_string())
            .increment(io.rows_written);
    }
    if io.bytes_written > 0 {
        metrics::counter!(MV_BYTES_WRITTEN, "kind" => kind.to_string())
            .increment(io.bytes_written);
    }
}
