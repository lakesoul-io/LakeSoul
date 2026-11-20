// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL time travel: `lakesoul_table_scan(table, mode, value)`.
//!
//! The provider layer already knows how to read a pinned view of a table
//! ([`LakeSoulTableProvider::snapshot_view`]) and the metadata layer can
//! resolve partition versions by timestamp, snapshot id or tag. This module
//! is the thin SQL entry point: parse three string literals, resolve the table
//! through the session catalog (so ACL/RBAC/domain visibility still applies),
//! and hand a [`TableReadSnapshot`] to `snapshot_view`.
//!
//! The result is a per-query pinned scan. It does not change any session
//! state: reading an old version and writing it to another table or partition
//! is an ordinary query, not an error.

use std::any::Any;
use std::sync::{Arc, Weak};

use chrono::DateTime;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use lakesoul_datafusion::datasource::table_provider::{
    LakeSoulTableProvider, TableReadSnapshot,
};
use tokio::runtime::{Handle, RuntimeFlavor};

pub(crate) const FUNCTION_NAME: &str = "lakesoul_table_scan";

/// A pinned read of a single table, resolved at query planning time.
///
/// Holds a `Weak<SessionContext>` instead of an `Arc` so the function registry
/// does not own the context that owns the registry. The context is always
/// alive while a statement is being planned.
pub struct TableScanFunction {
    context: Weak<SessionContext>,
}

impl TableScanFunction {
    pub fn new(context: &Arc<SessionContext>) -> Self {
        Self {
            context: Arc::downgrade(context),
        }
    }
}

impl std::fmt::Debug for TableScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TableScanFunction")
    }
}

impl TableFunctionImpl for TableScanFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.len() != 3 {
            return Err(invalid(format!(
                "expected 3 arguments (table, mode, value), got {}",
                exprs.len()
            )));
        }
        let [table_arg, mode_arg, value_arg] = exprs else {
            return Err(invalid("expected 3 arguments (table, mode, value)"));
        };
        let table = string_literal(table_arg, "table")?;
        let mode = string_literal(mode_arg, "mode")?;
        let value = string_literal(value_arg, "value")?;
        let snapshot = parse_snapshot(&mode, &value)?;

        let context = self.context.upgrade().ok_or_else(|| {
            invalid(
                "the session has already been closed; a pinned read needs a live session",
            )
        })?;
        // `TableFunctionImpl::call_with_args` is synchronous, while catalog
        // resolution is async. Wait on the current multi-thread runtime the
        // same way the metadata write path does (catalog::snapshot::wait_on_runtime).
        let context = Arc::clone(&context);
        let lookup = table.clone();
        let provider = wait_on_runtime(
            async move { context.table_provider(lookup).await },
        )
        .map_err(|error| {
            invalid(format!(
                "table '{table}' not found or not visible in this session: {error}"
            ))
        })?;
        let provider: &dyn Any = provider.as_ref();
        let provider = provider
            .downcast_ref::<LakeSoulTableProvider>()
            .ok_or_else(|| {
                invalid(format!(
                    "table '{table}' is not a LakeSoul table; \
                     {FUNCTION_NAME} only supports LakeSoul tables"
                ))
            })?;
        Ok(Arc::new(provider.snapshot_view(snapshot)))
    }
}

/// Validate `(mode, value)` into the provider's snapshot selector.
fn parse_snapshot(mode: &str, value: &str) -> DFResult<TableReadSnapshot> {
    match mode.to_ascii_lowercase().as_str() {
        "timestamp" => {
            let parsed = DateTime::parse_from_rfc3339(value).map_err(|error| {
                invalid(format!(
                    "timestamp must be an RFC3339 string with an explicit offset \
                     (e.g. 2026-11-20T10:00:00+08:00): {error}"
                ))
            })?;
            Ok(TableReadSnapshot::AsOfMs(parsed.timestamp_millis()))
        }
        "tag" => {
            if value.trim().is_empty() {
                return Err(invalid("tag must not be empty"));
            }
            Ok(TableReadSnapshot::Tag(value.to_string()))
        }
        "snapshot_id" | "version" => {
            let id = value.parse::<i64>().map_err(|error| {
                invalid(format!("snapshot_id must be an integer: {error}"))
            })?;
            Ok(TableReadSnapshot::SnapshotId(id))
        }
        other => Err(invalid(format!(
            "unknown mode '{other}'; expected 'timestamp', 'tag' or \
             'snapshot_id' (alias 'version')"
        ))),
    }
}

/// Extract one string literal argument; non-literals would be planner
/// expressions we cannot evaluate here and are refused explicitly.
fn string_literal(expr: &Expr, name: &str) -> DFResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Ok(value.clone()),
        _ => Err(invalid(format!(
            "{name} must be a string literal, got '{expr}'"
        ))),
    }
}

/// Drive an async catalog lookup from a synchronous table-function call.
///
/// `Handle::current()` panics without a runtime and plain `block_on` deadlocks
/// a runtime worker; `block_in_place` lets the multi-thread runtime move other
/// tasks away while this thread waits. A current-thread runtime is refused
/// with a planner error rather than risking a hang.
fn wait_on_runtime<T: Send + 'static>(
    future: impl std::future::Future<Output = DFResult<T>> + Send + 'static,
) -> DFResult<T> {
    let handle = Handle::try_current().map_err(|error| {
        invalid(format!(
            "a tokio runtime is required for a pinned read: {error}"
        ))
    })?;
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        return Err(invalid(
            "a pinned read requires a multi-thread tokio runtime",
        ));
    }
    tokio::task::block_in_place(|| handle.block_on(future))
}

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(format!("{FUNCTION_NAME}: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(value: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), None)
    }

    #[test]
    fn string_literals_are_required() {
        assert_eq!(string_literal(&lit("v1"), "value").expect("literal"), "v1");
        // A column reference is not a literal: the planner cannot substitute
        // one here, so it must be refused rather than resolved as a table.
        let column = Expr::Column(datafusion::common::Column::from_name("value"));
        assert!(string_literal(&column, "value").is_err());
    }

    #[test]
    fn timestamp_requires_an_explicit_offset() {
        let with_offset = parse_snapshot("timestamp", "2026-11-20T10:00:00+08:00")
            .expect("RFC3339 with offset");
        assert!(matches!(with_offset, TableReadSnapshot::AsOfMs(ms) if ms > 0));
        assert!(parse_snapshot("timestamp", "2026-11-20T10:00:00").is_err());
        assert!(parse_snapshot("timestamp", "not-a-time").is_err());
    }

    #[test]
    fn tag_and_snapshot_id_are_validated() {
        assert!(matches!(
            parse_snapshot("tag", "release-1").expect("tag"),
            TableReadSnapshot::Tag(tag) if tag == "release-1"
        ));
        assert!(parse_snapshot("tag", "  ").is_err());
        assert!(matches!(
            parse_snapshot("snapshot_id", "42").expect("snapshot id"),
            TableReadSnapshot::SnapshotId(42)
        ));
        assert!(matches!(
            parse_snapshot("version", "42").expect("version alias"),
            TableReadSnapshot::SnapshotId(42)
        ));
        assert!(parse_snapshot("snapshot_id", "abc").is_err());
        assert!(parse_snapshot("unknown", "x").is_err());
    }
}
