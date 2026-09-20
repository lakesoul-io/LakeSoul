// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul incremental materialized view runtime.
//!
//! This crate builds the IVM layer on top of the LakeSoul metadata primitives:
//! an `ivm` PostgreSQL schema for view specs and cursors, internal LakeSoul
//! tables for materialized views and state, and a refresh engine that consumes
//! the source changelog by partition version.
//!
//! The first supported view shapes are `SUM`/`COUNT` over an append-only or
//! primary-key (upsert) source ([`runtime::SumCountView`]) and an inner
//! equi-join of two append-only sources ([`runtime::JoinView`]). Refreshes are incremental and
//! replay safe: every window is recorded in `ivm.epochs` (see `EPOCH.md`), so
//! a retry skips data that is already written. A view whose source history
//! cannot be consumed incrementally (updates/deletes in the window, or an
//! unaligned cursor rewind) can be rebuilt from the full source state with
//! [`runtime::IvmRuntime::rebuild_sum_count`] /
//! [`runtime::IvmRuntime::rebuild_join`].
//!
//! # Retention
//!
//! A refresh can only consume a source commit window while its history is
//! retained: cursors are persisted as partition versions, and the changelog
//! read resolves those versions through `partition_info` / `data_commit_info`.
//! LakeSoul does not delete history by default, but the opt-in cleanup
//! facilities do:
//!
//! * `partition.ttl` / `compaction.ttl` (Flink async clean job, Spark
//!   `CleanExpiredData`) delete old `partition_info` versions and their files;
//! * `cleanOldCompaction` deletes the files of older compaction versions;
//! * `dataExpiredTime` drives the async discard/compaction cleanup.
//!
//! V1 therefore requires that tables consumed by an IVM view keep the default
//! retention and are not configured with those TTLs. Cursor-aware garbage
//! collection (checking `min(ivm.cursors.last_timestamp)` before deleting) is a
//! follow-up.

pub mod error;
pub mod metadata;
pub mod runtime;
pub mod table;

pub use error::Result;
pub use metadata::{
    BeginEpoch, Cursor, EpochRecord, EpochStatus, IvmMetadata, PartitionVersion,
    SourceVersionRange,
};
pub use runtime::{
    IVM_COUNT_COLUMN, IVM_SUM_COLUMN, IvmRuntime, JoinView, SumCountView, ViewSpec,
    join_view_schema, sum_count_mv_schema, window_key,
};
pub use table::{
    IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN, IvmTable, IvmTableOptions, create_ivm_table,
};

pub(crate) fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as i64
}
