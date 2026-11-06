// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Analyzer rule that folds a literal query vector into one list literal.
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
//! Only literal arrays handed to a distance function are folded, and the
//! element type is kept exactly, so no query changes meaning.

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
    let Some(literal) = fold_query_array(&call.args[1]) else {
        return Ok(Transformed::no(Expr::ScalarFunction(call)));
    };
    call.args[1] = literal;
    Ok(Transformed::yes(Expr::ScalarFunction(call)))
}

/// Turn an all-literal array expression into a single list literal.
///
/// Accepts the `ARRAY[…]` form the SQL planner produces and the `make_array(…)`
/// form the expression API produces.  Mixed or non-literal elements are left
/// alone: folding them would need the analyzer's coercion machinery.
fn fold_query_array(expr: &Expr) -> Option<Expr> {
    // The SQL planner spells `ARRAY[…]` as a `make_array(…)` call; the
    // expression API produces the same shape.
    let elements: &[Expr] = match expr {
        Expr::ScalarFunction(call) if call.func.name() == "make_array" => {
            call.args.as_slice()
        }
        _ => return None,
    };
    if elements.is_empty() {
        return None;
    }
    let values = elements
        .iter()
        .map(|element| match element {
            Expr::Literal(value, _)
                if matches!(
                    value,
                    ScalarValue::Float64(Some(_))
                        | ScalarValue::Float32(Some(_))
                        | ScalarValue::Int64(Some(_))
                        | ScalarValue::Int32(Some(_))
                ) =>
            {
                Some(value.clone())
            }
            _ => None,
        })
        .collect::<Option<Vec<ScalarValue>>>()?;
    // Keep the element type the literals already have, so the list is exactly
    // the value the array denoted and any column-type coercion happens on the
    // single literal instead of on every element.
    let element_type = values.first()?.data_type();
    if values.iter().any(|value| value.data_type() != element_type) {
        return None;
    }
    let list = ScalarValue::new_list(&values, &element_type, true);
    Some(Expr::Literal(ScalarValue::List(list), None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
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
    fn literal_query_becomes_one_list_literal() {
        let folded = fold(distance(make_array(&[0.1, 0.2, 0.3]))).unwrap();
        let Expr::ScalarFunction(call) = folded.data else {
            panic!("distance call expected");
        };
        let Expr::Literal(ScalarValue::List(list), _) = &call.args[1] else {
            panic!("list literal expected, got {:?}", call.args[1]);
        };
        assert_eq!(list.len(), 1, "one list element");
        assert_eq!(list.value_length(0), 3, "three values");
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
