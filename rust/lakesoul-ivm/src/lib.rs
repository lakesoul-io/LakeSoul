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
//! The first supported view shape is `SUM`/`COUNT` over an append-only source;
//! see [`runtime::SumCountView`].

pub mod error;
pub mod metadata;
pub mod runtime;
pub mod table;

pub use error::Result;
pub use metadata::{Cursor, IvmMetadata};
pub use runtime::{
    IVM_COUNT_COLUMN, IVM_SUM_COLUMN, IvmRuntime, SumCountView, ViewSpec,
    sum_count_mv_schema,
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
