// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Logical optimizer rule that turns `text_match(column, query)` filters on
//! LakeSoul tables into a text-index candidate scan.
//!
//! The rule recognizes
//!
//! ```text
//! Limit(fetch=k)
//! └── [Projection]*
//!     └── Filter(P)          with a text_match(column, query) term in P
//!         └── [Projection / Filter]*
//!             └── TableScan
//! ```
//!
//! and appends the internal search marker to `TableScan.filters`; the
//! provider's `scan` turns that into a text-index candidate read.  The
//! predicate itself is left untouched: the physical planner rewrites it to
//! the analyzer-configured form (it can resolve the table provider, this
//! rule cannot), so it is evaluated exactly above the scan and removes stale
//! index candidates.
//!
//! Without a finite `LIMIT` the rule does not rewrite (the index path
//! returns only the top candidates); such queries fall back to an exact full
//! scan filtered by the `text_match` UDF.

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::TableSource;
use datafusion::logical_expr::{Expr, LogicalPlan, TableScan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::udf::text_search_marker::{find_user_text_match, is_marker_expr, marker_expr};

/// Logical optimizer rule for the text-search pushdown (see module docs).
#[derive(Debug, Default)]
pub struct TextSearchPushdownRule;

impl OptimizerRule for TextSearchPushdownRule {
    fn name(&self) -> &str {
        "lakesoul_text_search_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let Some((column, query, top_k, scan_source)) = detect(&plan) else {
            return Ok(Transformed::no(plan));
        };
        let Some(with_marker) =
            rewrite_scan(plan.clone(), &scan_source, &column, &query, top_k)
        else {
            return Ok(Transformed::no(plan));
        };
        Ok(Transformed::yes(with_marker))
    }
}

/// Match the plan shape: `Limit → ... → Filter(text_match) → ... → TableScan`.
///
/// Returns the search parameters and the target scan's source.
fn detect(plan: &LogicalPlan) -> Option<(String, String, usize, Arc<dyn TableSource>)> {
    let LogicalPlan::Limit(limit) = plan else {
        return None;
    };
    let top_k = fetch_value(limit.fetch.as_deref())?;
    if top_k == 0 {
        return None;
    }

    let mut current = Arc::clone(&limit.input);
    let (column, query) = loop {
        match current.as_ref() {
            LogicalPlan::Projection(projection) => {
                current = Arc::clone(&projection.input);
            }
            LogicalPlan::Filter(filter) => {
                if let Some(found) = find_user_text_match(&filter.predicate) {
                    break found;
                }
                current = Arc::clone(&filter.input);
            }
            _ => return None,
        }
    };

    loop {
        match current.as_ref() {
            LogicalPlan::Projection(projection) => {
                current = Arc::clone(&projection.input);
            }
            LogicalPlan::Filter(filter) => {
                current = Arc::clone(&filter.input);
            }
            LogicalPlan::TableScan(scan) => {
                if scan.filters.iter().any(is_marker_expr) {
                    return None;
                }
                return Some((column, query, top_k, Arc::clone(&scan.source)));
            }
            _ => return None,
        }
    }
}

/// Evaluate a literal `LIMIT` fetch expression.
fn fetch_value(fetch: Option<&Expr>) -> Option<usize> {
    match fetch? {
        Expr::Literal(datafusion::common::ScalarValue::Int64(Some(v)), _) => {
            Some((*v).max(0) as usize)
        }
        Expr::Literal(datafusion::common::ScalarValue::UInt64(Some(v)), _) => {
            Some(*v as usize)
        }
        Expr::Literal(datafusion::common::ScalarValue::Int32(Some(v)), _) => {
            Some((*v).max(0) as usize)
        }
        Expr::Literal(datafusion::common::ScalarValue::UInt32(Some(v)), _) => {
            Some(*v as usize)
        }
        _ => None,
    }
}

/// Rebuild the plan with the marker appended to the target scan.
fn rewrite_scan(
    plan: LogicalPlan,
    source: &Arc<dyn TableSource>,
    column: &str,
    query: &str,
    top_k: usize,
) -> Option<LogicalPlan> {
    if let LogicalPlan::TableScan(scan) = &plan
        && Arc::ptr_eq(&scan.source, source)
    {
        let mut filters = scan.filters.clone();
        filters.push(marker_expr(column, query, top_k));
        return Some(LogicalPlan::TableScan(TableScan {
            table_name: scan.table_name.clone(),
            source: Arc::clone(&scan.source),
            projection: scan.projection.clone(),
            projected_schema: scan.projected_schema.clone(),
            filters,
            fetch: scan.fetch,
            statistics_requests: scan.statistics_requests.clone(),
        }));
    }
    let mut changed = false;
    let rewritten = plan
        .map_children(|child| {
            let unchanged = child.clone();
            match rewrite_scan(child, source, column, query, top_k) {
                Some(rewritten) => {
                    changed = true;
                    Ok(Transformed::yes(rewritten))
                }
                None => Ok(Transformed::no(unchanged)),
            }
        })
        .ok()?;
    changed.then_some(rewritten.data)
}
