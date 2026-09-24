// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Resource limits for the PostgreSQL endpoint, and the accounting they need.
//!
//! The plan asks for governance the upstream service does not have: a bound on
//! connections, on the statements one user may run at once, and on the rows a
//! statement may return, all of them observable. They are enforced here, before
//! the work they bound starts, and released by RAII guards, so a disconnected
//! client or a panicking task cannot leak a slot.

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

/// The limits one server enforces. Zero means unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_connections: usize,
    pub max_queries_per_user: usize,
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
            max_result_rows: 0,
            max_result_bytes: 0,
        }
    }
}

/// Why a limit refused something.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LimitError {
    #[error("limit {max}")]
    TooManyConnections { max: usize },
    #[error("\"{user}\" (limit {max})")]
    TooManyQueries { user: String, max: usize },
}

impl LimitError {
    /// The error sent to a client during startup, which ends the connection.
    pub fn into_fatal(self) -> PgWireError {
        let (code, message) = match &self {
            Self::TooManyConnections { .. } => (
                TOO_MANY_CONNECTIONS,
                "sorry, too many clients already (self)".to_string(),
            ),
            Self::TooManyQueries { .. } => (
                TOO_MANY_CONNECTIONS,
                format!("too many concurrent queries for user {self}"),
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

/// The live counts behind [`Limits`], shared by every connection of a server.
#[derive(Debug, Default)]
pub struct LimitsState {
    active_connections: AtomicUsize,
    queries_per_user: Mutex<HashMap<String, usize>>,
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
        self.queries_per_user
            .lock()
            .get(user)
            .copied()
            .unwrap_or_default()
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
                return Err(LimitError::TooManyConnections { max });
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
    pub fn admit_statement(
        self: &Arc<Self>,
        user: &str,
    ) -> Result<StatementPermit, LimitError> {
        let max = self.limits.max_queries_per_user;
        let mut queries = self.state.queries_per_user.lock();
        let running = queries.entry(user.to_string()).or_default();
        if max > 0 && *running >= max {
            return Err(LimitError::TooManyQueries {
                user: user.to_string(),
                max,
            });
        }
        *running += 1;
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
        let mut queries = self.server.state.queries_per_user.lock();
        if let Some(running) = queries.get_mut(&self.user) {
            *running = running.saturating_sub(1);
            if *running == 0 {
                queries.remove(&self.user);
            }
        }
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
                assert!(
                    info.message.contains("too many clients"),
                    "{}",
                    info.message
                );
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
}
