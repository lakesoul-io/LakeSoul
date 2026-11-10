// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Resource limits for the PostgreSQL endpoint, and the accounting they need.
//!
//! The plan asks for governance the upstream service does not have: a bound on
//! connections, on the statements one user - and the whole server - may run at
//! once, and on the rows a statement may return, all of them observable. They
//! are enforced here, before the work they bound starts, and released by RAII
//! guards, so a disconnected client or a panicking task cannot leak a slot.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError};
use parking_lot::Mutex;

/// `53300 too_many_connections`, what PostgreSQL reports when it is out of
/// connection slots.
pub const TOO_MANY_CONNECTIONS: &str = "53300";

/// `53400 configuration_limit_exceeded`, used for the limits PostgreSQL has no
/// state for (statements per user, rows per result).
pub const CONFIGURATION_LIMIT_EXCEEDED: &str = "53400";

/// Statements running right now, across every user.
const ACTIVE_QUERIES: &str = "lakesoul_active_queries";
/// Connections and statements a limit refused, grouped by the limit.
const ADMISSION_REJECTED_TOTAL: &str = "lakesoul_admission_rejected_total";

/// The limits one server enforces. Zero means unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_connections: usize,
    pub max_queries_per_user: usize,
    /// Statements the whole server may run at once, 0 for unlimited.
    ///
    /// The connection limit does not bound this - a connection holds a slot
    /// per suspended portal - and the per-user limit grows with the number of
    /// users, while every statement draws on one shared memory pool.
    pub max_concurrent_queries: usize,
    pub max_result_rows: usize,
    /// Bytes of encoded rows a statement may send, 0 for unlimited.
    pub max_result_bytes: usize,
}

/// The defaults a server starts from, and what `Limits::unlimited` relaxes.
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_queries_per_user: 8,
            max_concurrent_queries: 0,
            max_result_rows: 0,
            max_result_bytes: 0,
        }
    }
}

impl Limits {
    /// Every limit off: for tests that are not about a limit.
    #[cfg(test)]
    pub fn unlimited() -> Self {
        Self {
            max_connections: 0,
            max_queries_per_user: 0,
            max_concurrent_queries: 0,
            max_result_rows: 0,
            max_result_bytes: 0,
        }
    }

    /// Whether one user may take every server-wide statement slot, leaving
    /// none for anybody else: the per-user limit is unset or not below the
    /// server-wide one.
    pub fn one_user_can_fill_the_server(&self) -> bool {
        self.max_concurrent_queries > 0
            && (self.max_queries_per_user == 0
                || self.max_queries_per_user >= self.max_concurrent_queries)
    }
}

/// Why a limit refused something.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LimitError {
    #[error("limit {max}")]
    TooManyConnections { max: usize },
    #[error("\"{user}\" (limit {max})")]
    TooManyQueries { user: String, max: usize },
    #[error("limit {max}")]
    ServerAtCapacity { max: usize },
}

impl LimitError {
    /// The `limit` label the refusal is counted under.
    fn label(&self) -> &'static str {
        match self {
            Self::TooManyConnections { .. } => "connections",
            Self::TooManyQueries { .. } => "queries_per_user",
            Self::ServerAtCapacity { .. } => "concurrent_queries",
        }
    }

    /// The error sent to a client during startup, which ends the connection.
    pub fn into_fatal(self) -> PgWireError {
        let (code, message) = match &self {
            Self::TooManyConnections { .. } => (
                TOO_MANY_CONNECTIONS,
                format!("sorry, too many clients already ({self})"),
            ),
            Self::TooManyQueries { .. } => (
                TOO_MANY_CONNECTIONS,
                format!("too many concurrent queries for user {self}"),
            ),
            Self::ServerAtCapacity { .. } => (
                TOO_MANY_CONNECTIONS,
                format!("too many concurrent queries on this server ({self})"),
            ),
        };
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "FATAL".to_string(),
            code.to_string(),
            message,
        )))
    }

    /// The error sent for a statement that is refused while connected.
    pub fn into_statement(self) -> PgWireError {
        let (code, message) = match &self {
            Self::TooManyConnections { .. } => (
                CONFIGURATION_LIMIT_EXCEEDED,
                format!("connection limit reached ({self})"),
            ),
            Self::TooManyQueries { .. } => (
                CONFIGURATION_LIMIT_EXCEEDED,
                format!("too many concurrent queries for user {self}"),
            ),
            Self::ServerAtCapacity { .. } => (
                CONFIGURATION_LIMIT_EXCEEDED,
                format!("too many concurrent queries on this server ({self})"),
            ),
        };
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            code.to_string(),
            message,
        )))
    }
}

impl From<LimitError> for PgWireError {
    fn from(value: LimitError) -> Self {
        value.into_fatal()
    }
}

/// The error for a result that exceeded `max_result_rows`.
pub fn result_limit_error(rows: usize) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        CONFIGURATION_LIMIT_EXCEEDED.to_string(),
        format!("result exceeds the configured maximum of {rows} rows"),
    )))
}

/// The error for a result that exceeded `max_result_bytes`.
pub fn result_bytes_limit_error(bytes: usize) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        CONFIGURATION_LIMIT_EXCEEDED.to_string(),
        format!("result exceeds the configured maximum of {bytes} bytes"),
    )))
}

static DESCRIBE_METRICS: std::sync::Once = std::sync::Once::new();

/// Exports the admission metrics at zero, so the first refusal after startup
/// shows in `increase()`. Call once at startup, after installing the metrics
/// recorder.
pub fn register_limit_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_gauge!(
            ACTIVE_QUERIES,
            "Statements holding an admission slot, across every user"
        );
        metrics::describe_counter!(
            ADMISSION_REJECTED_TOTAL,
            "Connections and statements refused by a limit, grouped by limit"
        );
    });
    metrics::gauge!(ACTIVE_QUERIES).set(0.0);
    for limit in ["connections", "queries_per_user", "concurrent_queries"] {
        metrics::counter!(ADMISSION_REJECTED_TOTAL, "limit" => limit).increment(0);
    }
}

/// Counts a refusal and hands it back.
fn rejected(error: LimitError) -> LimitError {
    metrics::counter!(ADMISSION_REJECTED_TOTAL, "limit" => error.label()).increment(1);
    error
}

/// The statements running now: per user, and in total.
///
/// One lock covers both, so a statement takes - or is refused - both slots at
/// once, and the gauge, set under it, never reports a stale total.
#[derive(Debug, Default)]
struct StatementCounts {
    per_user: HashMap<String, usize>,
    total: usize,
}

/// The live counts behind [`Limits`], shared by every connection of a server.
#[derive(Debug, Default)]
pub struct LimitsState {
    active_connections: AtomicUsize,
    statements: Mutex<StatementCounts>,
}

impl LimitsState {
    /// How many connections are open. Diagnostics and tests.
    #[cfg(test)]
    pub fn connection_count(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    /// How many statements `user` is running. Diagnostics and tests.
    #[cfg(test)]
    pub fn query_count(&self, user: &str) -> usize {
        self.statements
            .lock()
            .per_user
            .get(user)
            .copied()
            .unwrap_or_default()
    }

    /// How many statements are running across every user. Diagnostics and
    /// tests.
    #[cfg(test)]
    pub fn total_query_count(&self) -> usize {
        self.statements.lock().total
    }
}

/// Enforces [`Limits`] against a shared [`LimitsState`].
#[derive(Debug)]
pub struct ServerLimits {
    limits: Limits,
    state: LimitsState,
}

impl ServerLimits {
    pub fn new(limits: Limits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: LimitsState::default(),
        })
    }

    #[cfg(test)]
    pub fn state(&self) -> &LimitsState {
        &self.state
    }

    /// Takes a connection slot, or refuses the connection.
    ///
    /// The check and the increment are one atomic operation: two startups that
    /// both saw a free slot must not both take it, or a burst could exceed the
    /// limit the server exists to enforce.
    pub fn admit_connection(self: &Arc<Self>) -> Result<ConnectionPermit, LimitError> {
        let max = self.limits.max_connections;
        let mut current = self.state.active_connections.load(Ordering::Acquire);
        loop {
            if max > 0 && current >= max {
                return Err(rejected(LimitError::TooManyConnections { max }));
            }
            match self.state.active_connections.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ConnectionPermit {
                        server: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Takes a statement slot for `user`, or refuses the statement.
    ///
    /// The per-user limit is checked first: a user at their own limit is told
    /// so, which they can act on, rather than that the server is busy. Both
    /// checks and both increments happen under one lock, so a refused statement
    /// never holds half a slot.
    pub fn admit_statement(
        self: &Arc<Self>,
        user: &str,
    ) -> Result<StatementPermit, LimitError> {
        let per_user = self.limits.max_queries_per_user;
        let global = self.limits.max_concurrent_queries;
        let mut counts = self.state.statements.lock();
        let running = counts.per_user.get(user).copied().unwrap_or_default();
        if per_user > 0 && running >= per_user {
            return Err(rejected(LimitError::TooManyQueries {
                user: user.to_string(),
                max: per_user,
            }));
        }
        if global > 0 && counts.total >= global {
            return Err(rejected(LimitError::ServerAtCapacity { max: global }));
        }
        *counts.per_user.entry(user.to_string()).or_default() += 1;
        counts.total += 1;
        metrics::gauge!(ACTIVE_QUERIES).set(counts.total as f64);
        Ok(StatementPermit {
            server: Arc::clone(self),
            user: user.to_string(),
        })
    }

    /// Whether a result of `rows` rows and `bytes` encoded bytes may be sent.
    ///
    /// One row can be arbitrarily large, so a row count alone is not a bound.
    pub fn check_result_rows(
        &self,
        rows: usize,
        bytes: usize,
    ) -> Result<(), PgWireError> {
        let max_rows = self.limits.max_result_rows;
        if max_rows > 0 && rows > max_rows {
            return Err(result_limit_error(max_rows));
        }
        let max_bytes = self.limits.max_result_bytes;
        if max_bytes > 0 && bytes > max_bytes {
            return Err(result_bytes_limit_error(max_bytes));
        }
        Ok(())
    }
}

/// A held connection slot, released when the connection ends.
#[derive(Debug)]
pub struct ConnectionPermit {
    server: Arc<ServerLimits>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.server
            .state
            .active_connections
            .fetch_sub(1, Ordering::AcqRel);
    }
}

/// A held statement slot, released when the statement ends.
#[derive(Debug)]
pub struct StatementPermit {
    server: Arc<ServerLimits>,
    user: String,
}

impl Drop for StatementPermit {
    fn drop(&mut self) {
        let mut counts = self.server.state.statements.lock();
        if let Some(running) = counts.per_user.get_mut(&self.user) {
            *running = running.saturating_sub(1);
            if *running == 0 {
                counts.per_user.remove(&self.user);
            }
        }
        counts.total = counts.total.saturating_sub(1);
        metrics::gauge!(ACTIVE_QUERIES).set(counts.total as f64);
    }
}

/// Records how a statement ended, for the log line it produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementOutcome {
    Completed = 0,
    Canceled = 1,
    TimedOut = 2,
    ResultLimit = 3,
    Failed = 4,
    /// The client went away, or closed the portal, before the result ended.
    Abandoned = 5,
}

impl StatementOutcome {
    /// Every outcome, so each `outcome` label can be exported before its
    /// first statement.
    pub const ALL: [Self; 6] = [
        Self::Completed,
        Self::Canceled,
        Self::TimedOut,
        Self::ResultLimit,
        Self::Failed,
        Self::Abandoned,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Canceled => "canceled",
            Self::TimedOut => "timed_out",
            Self::ResultLimit => "result_limit",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(
        max_connections: usize,
        max_queries: usize,
        max_rows: usize,
    ) -> Arc<ServerLimits> {
        ServerLimits::new(Limits {
            max_connections,
            max_queries_per_user: max_queries,
            max_concurrent_queries: 0,
            max_result_rows: max_rows,
            max_result_bytes: 0,
        })
    }

    #[test]
    fn connections_are_bounded_and_a_disconnect_gives_the_slot_back() {
        let server = limits(2, 0, 0);
        let first = server.admit_connection().expect("first");
        let _second = server.admit_connection().expect("second");
        assert_eq!(server.state().connection_count(), 2);

        let refused = server.admit_connection().expect_err("third");
        assert_eq!(refused, LimitError::TooManyConnections { max: 2 });
        match refused.into_fatal() {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, TOO_MANY_CONNECTIONS);
                assert_eq!(info.severity, "FATAL");
                // PostgreSQL's own wording, which clients match on, with the
                // limit that refused the connection.
                assert_eq!(info.message, "sorry, too many clients already (limit 2)");
            }
            other => panic!("unexpected: {other:?}"),
        }

        drop(first);
        assert_eq!(server.state().connection_count(), 1);
        _ = server.admit_connection().expect("the freed slot is usable");
    }

    #[test]
    fn statements_are_bounded_per_user() {
        let server = limits(0, 2, 0);
        let first = server.admit_statement("user_a").expect("first");
        let _second = server.admit_statement("user_a").expect("second");
        assert!(
            server.admit_statement("user_b").is_ok(),
            "per user, not global"
        );

        let refused = server.admit_statement("user_a").expect_err("third");
        match refused {
            LimitError::TooManyQueries { user, max } => {
                assert_eq!(user, "user_a");
                assert_eq!(max, 2);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(server.state().query_count("user_a"), 2);

        drop(first);
        assert_eq!(server.state().query_count("user_a"), 1);
        assert!(server.admit_statement("user_a").is_ok());
    }

    #[test]
    fn a_user_without_statements_is_forgotten() {
        let server = limits(0, 4, 0);
        let permit = server.admit_statement("user_a").expect("first");
        drop(permit);
        assert_eq!(server.state().query_count("user_a"), 0);
    }

    #[test]
    fn results_are_bounded_when_a_maximum_is_set() {
        let server = limits(0, 0, 100);
        assert!(server.check_result_rows(100, 0).is_ok());
        let error = server
            .check_result_rows(101, 0)
            .expect_err("over the limit");
        assert!(matches!(
            error,
            PgWireError::UserError(info) if info.code == CONFIGURATION_LIMIT_EXCEEDED
                && info.message.contains("100")
        ));

        let unlimited = limits(0, 0, 0);
        assert!(unlimited.check_result_rows(usize::MAX, usize::MAX).is_ok());
    }

    #[test]
    fn result_bytes_are_bounded_when_a_maximum_is_set() {
        let server = ServerLimits::new(Limits {
            max_result_bytes: 1_000,
            ..Limits::unlimited()
        });
        assert!(server.check_result_rows(1, 1_000).is_ok());
        let error = server
            .check_result_rows(1, 1_001)
            .expect_err("one row can still be too large");
        assert!(matches!(
            error,
            PgWireError::UserError(info) if info.code == CONFIGURATION_LIMIT_EXCEEDED
                && info.message.contains("bytes")
        ));
    }

    #[test]
    fn only_one_of_many_racing_connections_gets_the_last_slot() {
        let server = limits(1, 0, 0);
        let granted = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let server = Arc::clone(&server);
                let granted = Arc::clone(&granted);
                scope.spawn(move || {
                    if let Ok(permit) = server.admit_connection() {
                        granted.fetch_add(1, Ordering::Relaxed);
                        // Held until the thread is joined, like a live
                        // connection.
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        drop(permit);
                    }
                });
            }
        });
        assert_eq!(
            granted.load(Ordering::Relaxed),
            1,
            "a burst must not exceed the connection limit"
        );
    }

    #[test]
    fn zero_means_unlimited() {
        let server = limits(0, 0, 0);
        let held: Vec<_> = (0..1_000)
            .map(|_| server.admit_statement("user_a").expect("unlimited"))
            .collect();
        assert_eq!(server.state().query_count("user_a"), 1_000);
        drop(held);
        assert_eq!(server.state().query_count("user_a"), 0);
    }

    fn concurrent(max_queries: usize, max_concurrent: usize) -> Arc<ServerLimits> {
        ServerLimits::new(Limits {
            max_queries_per_user: max_queries,
            max_concurrent_queries: max_concurrent,
            ..Limits::unlimited()
        })
    }

    #[test]
    fn statements_are_bounded_across_users() {
        let server = concurrent(0, 2);
        let first = server.admit_statement("user_a").expect("first");
        let _second = server.admit_statement("user_b").expect("second");
        assert_eq!(server.state().total_query_count(), 2);

        let refused = server.admit_statement("user_c").expect_err("third");
        assert_eq!(refused, LimitError::ServerAtCapacity { max: 2 });
        match refused.into_statement() {
            PgWireError::UserError(info) => {
                assert_eq!(info.code, CONFIGURATION_LIMIT_EXCEEDED);
                assert_eq!(info.severity, "ERROR");
                assert!(info.message.contains("on this server"), "{}", info.message);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(
            server.state().query_count("user_c"),
            0,
            "a refused statement holds no per-user slot"
        );

        drop(first);
        assert_eq!(server.state().total_query_count(), 1);
        _ = server
            .admit_statement("user_c")
            .expect("the freed slot is usable");
    }

    #[test]
    fn a_user_at_their_own_limit_is_told_so_before_the_server_limit() {
        let server = concurrent(1, 1);
        let _held = server.admit_statement("user_a").expect("first");
        assert!(matches!(
            server.admit_statement("user_a"),
            Err(LimitError::TooManyQueries { .. })
        ));
        assert!(matches!(
            server.admit_statement("user_b"),
            Err(LimitError::ServerAtCapacity { max: 1 })
        ));
        assert_eq!(server.state().total_query_count(), 1);
    }

    #[test]
    fn only_one_of_many_racing_statements_gets_the_last_server_slot() {
        let server = concurrent(0, 1);
        let granted = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for user in 0..16 {
                let server = Arc::clone(&server);
                let granted = Arc::clone(&granted);
                scope.spawn(move || {
                    if let Ok(permit) = server.admit_statement(&format!("user_{user}")) {
                        granted.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        drop(permit);
                    }
                });
            }
        });
        assert_eq!(granted.load(Ordering::Relaxed), 1);
        assert_eq!(server.state().total_query_count(), 0);
    }

    #[test]
    fn one_user_filling_the_server_is_detected() {
        assert!(!Limits::unlimited().one_user_can_fill_the_server());
        assert!(!concurrent(4, 0).limits.one_user_can_fill_the_server());
        assert!(!concurrent(4, 16).limits.one_user_can_fill_the_server());
        assert!(concurrent(0, 16).limits.one_user_can_fill_the_server());
        assert!(concurrent(16, 16).limits.one_user_can_fill_the_server());
    }

    #[test]
    fn admission_metrics_track_active_statements_and_refusals() {
        let recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        // Rendered while `first` still holds its slot: the gauge is the number
        // of statements running at the moment of the scrape.
        let rendered = metrics::with_local_recorder(&recorder, || {
            register_limit_metrics();
            let server = concurrent(0, 2);
            let first = server.admit_statement("user_a").expect("first");
            let second = server.admit_statement("user_b").expect("second");
            assert!(server.admit_statement("user_c").is_err());
            drop(second);
            let rendered = handle.render();
            drop(first);
            rendered
        });
        for series in [
            "lakesoul_active_queries 1",
            "lakesoul_admission_rejected_total{limit=\"concurrent_queries\"} 1",
            "lakesoul_admission_rejected_total{limit=\"queries_per_user\"} 0",
            "lakesoul_admission_rejected_total{limit=\"connections\"} 0",
        ] {
            assert!(
                rendered.lines().any(|line| line == series),
                "{series} missing in:\n{rendered}"
            );
        }
    }
}
