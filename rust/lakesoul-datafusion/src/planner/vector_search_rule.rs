// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Logical optimizer rule that rewrites
//! `ORDER BY <distance>(vec, q) LIMIT k` into a LakeSoul vector-index
//! search.
//!
//! The rule recognizes the pattern
//!
//! ```text
//! Limit(fetch=k)
//! └── Sort([array_distance(vec, q) ASC] | [inner_product(vec, q) DESC])
//!     └── [Projection / Filter]*
//!         └── TableScan
//! ```
//!
//! and appends an internal marker call to the `TableScan.filters` list.
//! The scan provider then switches the table read to the vector-index
//! candidate path while the physical `Sort` + `Limit` stay in place for the
//! exact re-rank of the (small) candidate set.  Any plan shape that does
//! not match — including `cosine_distance`, which the LakeSoul index does
//! not support — is left untouched and executes as a regular full scan.
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Cast, Expr, LogicalPlan, SortExpr, TableScan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use crate::udf::vector_search_marker::{VECTOR_SEARCH_MARKER, marker_expr};

/// Logical optimizer rule for the vector-search pushdown (see module docs).
#[derive(Debug, Default)]
pub struct VectorSearchPushdownRule;

/// Detected vector-search specification extracted from a matching plan.
#[derive(Debug, Clone)]
pub(crate) struct VectorSearchSpec {
    pub vec_column: String,
    pub query: Vec<f32>,
    pub top_k: usize,
    /// `"L2"` or `"IP"`.
    pub metric: String,
}

fn is_marker(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call) if call.func.name() == VECTOR_SEARCH_MARKER)
}

impl OptimizerRule for VectorSearchPushdownRule {
    fn name(&self) -> &str {
        "lakesoul_vector_search_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        match detect(&plan) {
            Some((spec, target)) => {
                let rewritten = rewrite_chain(plan, &spec, &target)
                    .expect("matching plan must be rewritable");
                Ok(Transformed::yes(rewritten))
            }
            None => Ok(Transformed::no(plan)),
        }
    }
}

/// Match the `Limit → Sort(distance) → [Projection/Filter]* → TableScan`
/// pattern.  Returns the spec and the `Arc` of the target `TableScan` (the
/// same Arc stored inside the plan tree).
fn detect(plan: &LogicalPlan) -> Option<(VectorSearchSpec, Arc<LogicalPlan>)> {
    let limit = match plan {
        LogicalPlan::Limit(limit) => limit,
        _ => return None,
    };
    let sort = match limit.input.as_ref() {
        LogicalPlan::Sort(sort) => sort,
        _ => return None,
    };
    // The optimizer may merge the limit into the Sort node as a fetch.
    let top_k = sort.fetch.or_else(|| fetch_value(limit.fetch.as_deref()))?;
    if top_k == 0 {
        return None;
    }
    if sort.expr.len() != 1 {
        return None;
    }
    let sort_expr = &sort.expr[0];
    let metric = distance_metric(&sort_expr.expr)?;
    let expected_asc = metric == "L2";
    if sort_expr.asc != expected_asc {
        return None;
    }

    let mut current = Arc::clone(&sort.input);
    loop {
        match current.as_ref() {
            LogicalPlan::Projection(projection) => {
                current = Arc::clone(&projection.input);
            }
            LogicalPlan::Filter(filter) => {
                current = Arc::clone(&filter.input);
            }
            LogicalPlan::TableScan(ts) => {
                if ts.filters.iter().any(is_marker) {
                    return None;
                }
                let (vec_column, query) = extract_distance_args(sort_expr, ts)?;
                return Some((
                    VectorSearchSpec {
                        vec_column,
                        query,
                        top_k,
                        metric: metric.to_string(),
                    },
                    current,
                ));
            }
            _ => return None,
        }
    }
}

/// Evaluate a literal `LIMIT` fetch expression to a usize.
fn fetch_value(fetch: Option<&Expr>) -> Option<usize> {
    match fetch? {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some((*v).max(0) as usize),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v as usize),
        Expr::Literal(ScalarValue::UInt32(Some(v)), _) => Some(*v as usize),
        Expr::Literal(ScalarValue::Int32(Some(v)), _) => Some((*v).max(0) as usize),
        _ => None,
    }
}

/// The distance metric implied by a function call, or None for functions
/// the LakeSoul index cannot serve (e.g. `cosine_distance`).
fn distance_metric(expr: &Expr) -> Option<&'static str> {
    let call = as_scalar_function(unwrap_cast(expr))?;
    match call.func.name() {
        "array_distance" => Some("L2"),
        "inner_product" | "dot_product" => Some("IP"),
        _ => None,
    }
}

fn as_scalar_function(expr: &Expr) -> Option<&ScalarFunction> {
    match expr {
        Expr::ScalarFunction(call) => Some(call),
        _ => None,
    }
}

/// Extract (vector column name, query vector) from the distance call args,
/// validating the column type against the scan schema.
fn extract_distance_args(
    sort_expr: &SortExpr,
    ts: &TableScan,
) -> Option<(String, Vec<f32>)> {
    let call = as_scalar_function(unwrap_cast(&sort_expr.expr))?;
    if call.args.len() != 2 {
        return None;
    }
    let col = match unwrap_cast(&call.args[0]) {
        Expr::Column(col) => col,
        _ => return None,
    };
    if !ts
        .projected_schema
        .field_with_unqualified_name(&col.name)
        .is_ok()
    {
        return None;
    }
    let field = ts
        .projected_schema
        .field_with_unqualified_name(&col.name)
        .ok()?;
    if !is_vector_type(field.data_type()) {
        return None;
    }
    let query = extract_query(unwrap_cast(&call.args[1]))?;
    if query.is_empty() {
        return None;
    }
    Some((col.name.clone(), query))
}

/// True for list-of-numbers types accepted by the vector index.
fn is_vector_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) | DataType::LargeList(field) => {
            matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        }
        DataType::FixedSizeList(field, _) => {
            matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        }
        _ => false,
    }
}

/// Extract the query vector from a constant expression: a `make_array(...)`
/// call of numeric literals, or a list/fixed-size-list literal.
fn extract_query(expr: &Expr) -> Option<Vec<f32>> {
    match expr {
        Expr::ScalarFunction(call) if call.func.name() == "make_array" => call
            .args
            .iter()
            .map(|arg| match arg {
                Expr::Literal(ScalarValue::Float32(Some(v)), _) => Some(*v),
                Expr::Literal(ScalarValue::Float64(Some(v)), _) => Some(*v as f32),
                Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(*v as f32),
                Expr::Literal(ScalarValue::Int32(Some(v)), _) => Some(*v as f32),
                _ => None,
            })
            .collect(),
        Expr::Literal(ScalarValue::List(array), _) => {
            let array = array.as_ref();
            let values = array.value(0);
            match values.data_type() {
                DataType::Float64 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()?;
                    Some(
                        values
                            .iter()
                            .map(|v| v.map(|v| v as f32).unwrap_or_default())
                            .collect(),
                    )
                }
                DataType::Float32 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()?;
                    Some(values.iter().map(|v| v.unwrap_or_default()).collect())
                }
                _ => None,
            }
        }
        Expr::Literal(ScalarValue::FixedSizeList(array), _) => {
            let array = array.as_ref();
            let values = array.value(0);
            match values.data_type() {
                DataType::Float64 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()?;
                    Some(
                        values
                            .iter()
                            .map(|v| v.map(|v| v as f32).unwrap_or_default())
                            .collect(),
                    )
                }
                DataType::Float32 => {
                    let values = values
                        .as_any()
                        .downcast_ref::<arrow::array::Float32Array>()?;
                    Some(values.iter().map(|v| v.unwrap_or_default()).collect())
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Strip surrounding `Cast` / `Alias` wrappers.
fn unwrap_cast(expr: &Expr) -> &Expr {
    let mut current = expr;
    loop {
        current = match current {
            Expr::Cast(cast) => {
                let Cast { expr, .. } = cast;
                expr
            }
            Expr::Alias(alias) => &alias.expr,
            other => return other,
        };
    }
}

/// Rebuild the plan chain with the marker appended to the target scan.
///
/// Rebuilds every node between the root and the target `TableScan` via the
/// `TreeNode` machinery (the node structs are non-exhaustive); the target
/// scan gets the marker filter appended.
fn rewrite_chain(
    plan: LogicalPlan,
    spec: &VectorSearchSpec,
    target: &Arc<LogicalPlan>,
) -> Option<LogicalPlan> {
    if let LogicalPlan::TableScan(ts) = &plan
        && let LogicalPlan::TableScan(target_ts) = target.as_ref()
        && Arc::ptr_eq(&ts.source, &target_ts.source)
    {
        return Some(rewrite_scan(&plan, spec));
    }
    let mut changed = false;
    let rewritten = plan
        .map_children(|child| {
            let unchanged = child.clone();
            match rewrite_chain(child, spec, target) {
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

/// Append the marker filter to a TableScan, preserving everything else.
fn rewrite_scan(scan: &LogicalPlan, spec: &VectorSearchSpec) -> LogicalPlan {
    let LogicalPlan::TableScan(ts) = scan else {
        unreachable!("target must be a TableScan")
    };
    let mut filters = ts.filters.clone();
    filters.push(marker_expr(
        &spec.vec_column,
        &spec.query,
        spec.top_k,
        &spec.metric,
    ));
    LogicalPlan::TableScan(TableScan {
        table_name: ts.table_name.clone(),
        source: Arc::clone(&ts.source),
        projection: ts.projection.clone(),
        projected_schema: ts.projected_schema.clone(),
        filters,
        fetch: ts.fetch,
    })
}
