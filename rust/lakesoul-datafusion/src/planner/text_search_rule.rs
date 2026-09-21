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
//!     └── [Sort]             ORDER BY text_score(column, query) DESC
//!         └── [Projection]*
//!             └── Filter(P)  with a text_match(column, query) term in P
//!                 └── [Projection / Filter]*
//!                     └── TableScan
//! ```
//!
//! and appends the internal search marker to `TableScan.filters`; the
//! provider's `scan` turns that into a text-index candidate read.
//!
//! * Without a relevance `Sort`, the predicate is left untouched: the
//!   physical planner rewrites it to the analyzer-configured form and it is
//!   evaluated exactly above the scan, removing stale index candidates.
//! * With `ORDER BY text_score(column, query) DESC`, the rule removes the
//!   `Sort` and marks the search as ordered; the scan then returns the
//!   global top-`k` by BM25 score.  `text_score` cannot be projected into
//!   the result yet (the score column is not part of the table schema).
//!
//! Without a finite `LIMIT` the rule does not rewrite (the index path
//! returns only the top candidates); such queries fall back to an exact full
//! scan filtered by the `text_match` UDF.

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::TableSource;
use datafusion::logical_expr::{Expr, LogicalPlan, Sort, TableScan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::udf::text_search_marker::{
    TEXT_SCORE_FUNCTION, find_user_text_match, is_marker_expr, marker_expr,
    parse_text_score_call,
};

/// Logical optimizer rule for the text-search pushdown (see module docs).
#[derive(Debug, Default)]
pub struct TextSearchPushdownRule;

/// The pushdown parameters detected on the plan path.
struct Detected {
    column: String,
    query: String,
    top_k: usize,
    /// `ORDER BY text_score(column, query) DESC` was present.
    order_by: bool,
    source: Arc<dyn TableSource>,
}

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
        let Some(found) = detect(&plan) else {
            return Ok(Transformed::no(plan));
        };
        let Some(with_marker) = rewrite_plan(plan.clone(), &found, true) else {
            return Ok(Transformed::no(plan));
        };
        Ok(Transformed::yes(with_marker))
    }
}

/// Match the plan shape described in the module docs and collect the search
/// parameters.
fn detect(plan: &LogicalPlan) -> Option<Detected> {
    let LogicalPlan::Limit(limit) = plan else {
        return None;
    };
    let top_k = fetch_value(limit.fetch.as_deref())?;
    if top_k == 0 {
        return None;
    }

    let mut current = Arc::clone(&limit.input);
    let mut order_by = false;
    let mut sort_key: Option<(String, String)> = None;
    let mut filter_key: Option<(String, String)> = None;
    loop {
        match current.as_ref() {
            LogicalPlan::Projection(projection) => {
                current = Arc::clone(&projection.input);
            }
            LogicalPlan::Sort(sort) => {
                // A relevance sort must be the only one and sit above the
                // filter; any other sort is kept and merely passed through.
                if sort_contains_text_score(sort) {
                    if order_by || filter_key.is_some() {
                        return None;
                    }
                    let key = text_score_sort_key(sort)?;
                    sort_key = Some(key);
                    order_by = true;
                }
                current = Arc::clone(&sort.input);
            }
            LogicalPlan::Filter(filter) => {
                if filter_key.is_none() {
                    filter_key = find_user_text_match(&filter.predicate);
                }
                current = Arc::clone(&filter.input);
            }
            LogicalPlan::TableScan(scan) => {
                if scan.filters.iter().any(is_marker_expr) {
                    return None;
                }
                let (column, query) = filter_key?;
                if let Some(sort_key) = &sort_key
                    && *sort_key != (column.clone(), query.clone())
                {
                    return None;
                }
                return Some(Detected {
                    column,
                    query,
                    top_k,
                    order_by,
                    source: Arc::clone(&scan.source),
                });
            }
            _ => return None,
        }
    }
}

/// The sort key of `ORDER BY text_score(column, query) DESC`, when the sort
/// has exactly that one descending expression.
fn text_score_sort_key(sort: &Sort) -> Option<(String, String)> {
    if sort.expr.len() != 1 || sort.expr[0].asc {
        return None;
    }
    parse_text_score_call(&sort.expr[0].expr)
}

/// Whether a sort expression tree references `text_score`.
fn sort_contains_text_score(sort: &Sort) -> bool {
    sort.expr
        .iter()
        .any(|sort_expr| expr_contains_text_score(&sort_expr.expr))
}

fn expr_contains_text_score(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarFunction(call) => {
            call.func.name() == TEXT_SCORE_FUNCTION
                || call.args.iter().any(expr_contains_text_score)
        }
        Expr::BinaryExpr(binary) => {
            expr_contains_text_score(&binary.left)
                || expr_contains_text_score(&binary.right)
        }
        Expr::Cast(cast) => expr_contains_text_score(&cast.expr),
        Expr::Alias(alias) => expr_contains_text_score(&alias.expr),
        _ => false,
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

/// Rebuild the plan with the marker appended to the target scan and the
/// relevance `Sort` (when present) removed.
///
/// `remove_sort` guards against removing more than the one matching sort on
/// the path.
fn rewrite_plan(
    plan: LogicalPlan,
    found: &Detected,
    remove_sort: bool,
) -> Option<LogicalPlan> {
    if let LogicalPlan::TableScan(scan) = &plan
        && Arc::ptr_eq(&scan.source, &found.source)
    {
        let mut filters = scan.filters.clone();
        filters.push(marker_expr(
            &found.column,
            &found.query,
            found.top_k,
            found.order_by,
        ));
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
    if remove_sort
        && found.order_by
        && let LogicalPlan::Sort(sort) = &plan
        && text_score_sort_key(sort) == Some((found.column.clone(), found.query.clone()))
    {
        return rewrite_plan((*sort.input).clone(), found, false);
    }
    let mut changed = false;
    let rewritten = plan
        .map_children(|child| {
            let unchanged = child.clone();
            match rewrite_plan(child, found, remove_sort) {
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
