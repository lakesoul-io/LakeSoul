// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Analyzer rule that rewrites a literal query vector into a handle call.
//!
//! `ORDER BY array_distance(vec, ARRAY[…]) LIMIT k` reaches the analyzer as one
//! expression node per dimension.  Measured on the GloVe index-only table, the
//! 200-element array costs ~1.1 ms of the ~2.4 ms a single-query statement
//! spends planning: the analyzer's element-wise coercion, and then the first
//! optimizer pass walking the raw plan (re-optimizing the already rewritten
//! plan costs 0.03 ms).  Folding an all-literal array into a single `List`
//! literal before type coercion keeps every value and removes the
//! per-dimension nodes, so no later rule has to walk them.
//!
//! The same rule also rewrites the call into an internal distance function that
//! carries the vector by handle, because the optimizer's pre-rule work still
//! scales with the size of a literal that is left in the plan.  Only literal
//! arrays handed to a distance function are touched, and the handle function
//! evaluates by delegating to the built-in one, so no query changes meaning.

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::analyzer::AnalyzerRule;

/// Distance functions whose second argument is a query vector.
const DISTANCE_FUNCTIONS: &[&str] = &["array_distance", "inner_product", "dot_product"];

/// Folds `ARRAY[…literal…]` arguments of the distance functions.
#[derive(Debug, Default)]
pub struct FoldVectorQueryLiterals;

impl AnalyzerRule for FoldVectorQueryLiterals {
    fn name(&self) -> &str {
        "lakesoul_fold_vector_query_literals"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|node| node.map_expressions(|expr| expr.transform_up(fold)))
            .data()
    }
}

/// Whether the query vector travels by handle (`LAKESOUL_DISTANCE_HANDLE=0`
/// keeps it inline as one list literal for A/B runs).
fn distance_handles_enabled() -> bool {
    !std::env::var("LAKESOUL_DISTANCE_HANDLE").is_ok_and(|value| value == "0")
}

/// Fold the query argument of one distance call, when it is all literals.
fn fold(expr: Expr) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(mut call) = expr else {
        return Ok(Transformed::no(expr));
    };
    let is_distance = DISTANCE_FUNCTIONS
        .iter()
        .any(|name| call.func.name().eq_ignore_ascii_case(name));
    if !is_distance || call.args.len() != 2 {
        return Ok(Transformed::no(Expr::ScalarFunction(call)));
    }
    let Some((query, element_type)) = literal_query_vector(&call.args[1]) else {
        return Ok(Transformed::no(Expr::ScalarFunction(call)));
    };
    if !distance_handles_enabled() {
        // Fallback (`LAKESOUL_DISTANCE_HANDLE=0`): keep the vector inline as one
        // list literal instead of a handle, for A/B runs.
        let values = query
            .iter()
            .map(|value| match element_type {
                datafusion::arrow::datatypes::DataType::Float32 => {
                    ScalarValue::Float32(Some(*value))
                }
                datafusion::arrow::datatypes::DataType::Int64 => {
                    ScalarValue::Int64(Some(*value as i64))
                }
                datafusion::arrow::datatypes::DataType::Int32 => {
                    ScalarValue::Int32(Some(*value as i32))
                }
                _ => ScalarValue::Float64(Some(*value as f64)),
            })
            .collect::<Vec<_>>();
        let list = ScalarValue::new_list(&values, &element_type, true);
        call.args[1] = Expr::Literal(ScalarValue::List(list), None);
        return Ok(Transformed::yes(Expr::ScalarFunction(call)));
    }
    // Carry the vector by handle: the analyzed plan then holds eight bytes
    // instead of one node per dimension, so the optimizer's work before the
    // pushdown rewrite no longer scales with the vector dimension.
    let metric = if matches!(call.func.name(), "inner_product" | "dot_product") {
        "IP"
    } else {
        "L2"
    };
    let handle = crate::udf::vector_distance_handle::register_query_vector(&query);
    Ok(Transformed::yes(
        crate::udf::vector_distance_handle::distance_handle_expr(
            &call.args[0],
            &handle,
            metric,
        ),
    ))
}

/// The values of a query vector already folded into one list literal.
fn literal_list_vector(
    list: &datafusion::arrow::array::GenericListArray<i32>,
) -> Option<(Vec<f32>, datafusion::arrow::datatypes::DataType)> {
    use datafusion::arrow::array::{Array, Float32Array, Float64Array};
    let values = list.values();
    match values.data_type() {
        datafusion::arrow::datatypes::DataType::Float32 => {
            let values = values.as_any().downcast_ref::<Float32Array>()?;
            Some((
                (0..values.len()).map(|row| values.value(row)).collect(),
                datafusion::arrow::datatypes::DataType::Float32,
            ))
        }
        datafusion::arrow::datatypes::DataType::Float64 => {
            let values = values.as_any().downcast_ref::<Float64Array>()?;
            Some((
                (0..values.len())
                    .map(|row| values.value(row) as f32)
                    .collect(),
                datafusion::arrow::datatypes::DataType::Float64,
            ))
        }
        _ => None,
    }
}

/// The literal values of an all-literal query vector, with their element type.
fn literal_query_vector(
    expr: &Expr,
) -> Option<(Vec<f32>, datafusion::arrow::datatypes::DataType)> {
    // The SQL planner spells `ARRAY[…]` as a `make_array(…)` call; the
    // expression API produces the same shape.
    let elements: &[Expr] = match expr {
        Expr::ScalarFunction(call) if call.func.name() == "make_array" => {
            call.args.as_slice()
        }
        // pgvector-compatible operators (`<->`, `<#>`, `<=>`) reach the
        // analyzer with the query already parsed into one list literal, so
        // there are no per-dimension nodes to fold — but the vector still has
        // to leave the plan (a 200-element list literal otherwise costs the
        // analyzer and the pre-rewrite optimizer passes ~1.3 ms per
        // statement, the same reason the `ARRAY[…]` form is folded).
        Expr::Literal(ScalarValue::List(values), _) => {
            // `LAKESOUL_FOLD_LIST_LITERALS=0` keeps the pgvector form's list
            // literal inline for A/B runs.
            if std::env::var("LAKESOUL_FOLD_LIST_LITERALS")
                .is_ok_and(|value| value == "0")
            {
                return None;
            }
            return literal_list_vector(values.as_ref());
        }
        _ => return None,
    };
    if elements.is_empty() {
        return None;
    }
    let mut vector = Vec::with_capacity(elements.len());
    let mut element_type: Option<datafusion::arrow::datatypes::DataType> = None;
    for element in elements {
        let Expr::Literal(value, _) = element else {
            return None;
        };
        let current = value.data_type();
        if let Some(expected) = &element_type {
            if *expected != current {
                return None;
            }
        } else {
            element_type = Some(current);
        }
        match value {
            ScalarValue::Float64(Some(value)) => vector.push(*value as f32),
            ScalarValue::Float32(Some(value)) => vector.push(*value),
            ScalarValue::Int64(Some(value)) => vector.push(*value as f32),
            ScalarValue::Int32(Some(value)) => vector.push(*value as f32),
            _ => return None,
        }
    }
    Some((vector, element_type?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::expr::ScalarFunction;

    fn literal(value: f64) -> Expr {
        Expr::Literal(ScalarValue::Float64(Some(value)), None)
    }

    fn make_array(values: &[f64]) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion::functions_nested::make_array::make_array_udf(),
            values.iter().map(|value| literal(*value)).collect(),
        ))
    }

    fn distance(query: Expr) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion::functions_nested::distance::array_distance_udf(),
            vec![
                Expr::Column(datafusion::common::Column::new_unqualified(
                    "vec".to_string(),
                )),
                query,
            ],
        ))
    }

    #[test]
    fn literal_query_becomes_a_handle_call() {
        let rewritten = fold(distance(make_array(&[0.1, 0.2, 0.3]))).unwrap();
        let Expr::ScalarFunction(call) = rewritten.data else {
            panic!("distance call expected");
        };
        assert_eq!(
            call.func.name(),
            crate::udf::vector_distance_handle::VECTOR_DISTANCE_HANDLE
        );
        assert_eq!(call.args.len(), 3, "column, handle, metric");
        let query = crate::udf::vector_distance_handle::handle_query(&call)
            .and_then(crate::udf::vector_distance_handle::resolve_query_vector)
            .expect("handle resolves");
        assert_eq!(query.as_ref(), &[0.1f32, 0.2, 0.3]);
    }

    #[test]
    fn inner_product_keeps_its_metric() {
        let expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion::functions_nested::inner_product::inner_product_udf(),
            vec![
                Expr::Column(datafusion::common::Column::new_unqualified(
                    "vec".to_string(),
                )),
                make_array(&[1.0, 2.0]),
            ],
        ));
        let rewritten = fold(expr).unwrap();
        let Expr::ScalarFunction(call) = rewritten.data else {
            panic!("distance call expected");
        };
        assert_eq!(
            crate::udf::vector_distance_handle::handle_metric(&call),
            Some("IP")
        );
    }

    #[test]
    fn non_literal_query_is_left_alone() {
        let expr = distance(Expr::Column(datafusion::common::Column::new_unqualified(
            "other".to_string(),
        )));
        let folded = fold(expr.clone()).unwrap();
        assert!(!folded.transformed);
        assert_eq!(folded.data, expr);
    }
}
