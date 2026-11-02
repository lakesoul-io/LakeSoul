// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-statement write semantics shared by a SQL front end and the sink.
//!
//! The mode lives in the session config as an extension, so a front end can
//! select the conflict behaviour of the statement it is about to execute
//! without changing the table or the plan. Writers that do not know about the
//! extension get [`ConflictMode::Reject`], which is the historical behaviour:
//! a plain `INSERT` into a primary-key table fails on duplicate keys.

use std::fmt;
use std::sync::Arc;

/// Re-exported so SQL front ends that depend on this crate can map a NOT NULL
/// violation without depending on the writer crate directly.
pub use lakesoul_io::writer::async_writer::NotNullViolation;
use std::sync::atomic::{AtomicU8, Ordering};

/// What an insert does when a row's key is already present.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConflictMode {
    /// Reject the statement (SQL `INSERT`, LakeSoul "insert-only").
    #[default]
    Reject,
    /// Drop the conflicting row (`ON CONFLICT DO NOTHING`).
    Skip,
}

impl ConflictMode {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Skip,
            _ => Self::Reject,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Reject => 0,
            Self::Skip => 1,
        }
    }
}

/// Handle to the conflict mode of the statement currently being executed.
///
/// Cloning shares the value. A session installs one handle in its config; the
/// front end sets it before planning a statement and the sink reads it while
/// planning the write.
#[derive(Debug, Clone, Default)]
pub struct LakeSoulWriteMode {
    mode: Arc<AtomicU8>,
}

impl LakeSoulWriteMode {
    pub fn new() -> Self {
        Self::default()
    }

    /// The mode installed in `session`, or the default when the session has no
    /// handle (every non-SQL front end).
    pub fn from_session(session: &dyn datafusion::catalog::Session) -> ConflictMode {
        session
            .config()
            .get_extension::<Self>()
            .map(|mode| mode.get())
            .unwrap_or_default()
    }

    pub fn get(&self) -> ConflictMode {
        ConflictMode::from_u8(self.mode.load(Ordering::Acquire))
    }

    pub fn set(&self, mode: ConflictMode) {
        self.mode.store(mode.as_u8(), Ordering::Release);
    }
}

impl fmt::Display for LakeSoulWriteMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.get())
    }
}

/// A write violation that a SQL front end maps to its own error class.
///
/// The writer already knows why the statement must fail; carrying the reason
/// as a typed error lets the PostgreSQL front end answer with the SQLSTATE a
/// client expects (`23505` for a unique violation) instead of a generic
/// internal error. The message stays user-facing and unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteViolation {
    /// Two rows of the same statement use the same key.
    DuplicateKey { table: String },
    /// A row's key is already committed in the table.
    ExistingKey { table: String },
    /// A NOT NULL column received NULL.
    NotNull { column: String },
}

impl WriteViolation {
    /// PostgreSQL SQLSTATE for this violation.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::DuplicateKey { .. } | Self::ExistingKey { .. } => "23505",
            Self::NotNull { .. } => "23502",
        }
    }
}

impl fmt::Display for WriteViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKey { table } => write!(
                f,
                "INSERT contains a duplicate primary key in table '{table}'; \
                 use INSERT OR REPLACE for Upsert"
            ),
            Self::ExistingKey { table } => write!(
                f,
                "INSERT conflicts with an existing primary key in table '{table}'; \
                 use INSERT OR REPLACE for Upsert"
            ),
            Self::NotNull { column } => {
                write!(
                    f,
                    "null value in column \"{column}\" violates not-null constraint"
                )
            }
        }
    }
}

impl std::error::Error for WriteViolation {}

/// A write that lost a race with a concurrent writer.
///
/// LakeSoul validates a primary-key scan against a partition snapshot and then
/// publishes with a version comparison; if another writer moved the partition
/// in between, the statement is rolled back and must be retried. PostgreSQL's
/// closest class is `40001` (serialization failure), which clients already
/// treat as retryable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteConflict {
    /// Human-readable detail: table and partition that moved.
    pub detail: String,
}

impl WriteConflict {
    pub fn sqlstate(&self) -> &'static str {
        "40001"
    }
}

impl fmt::Display for WriteConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "could not serialize access due to a concurrent write: {}; retry the statement",
            self.detail
        )
    }
}

impl std::error::Error for WriteConflict {}
