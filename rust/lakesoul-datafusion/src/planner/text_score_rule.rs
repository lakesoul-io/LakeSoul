// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Physical rule backing `SELECT text_score(column, query)`.
//!
//! The score column is not part of the table schema — a DataFusion logical
//! `TableScan` cannot carry a field the provider does not declare — so the
//! projection is rewritten here, after physical planning.  For every
//! `ProjectionExec` that calls the `text_score` UDF, the text scan in its
//! input subtree is rebuilt with the internal BM25 score column appended
//! (the copy requests scores from the reader, which also enables exact
//! verification), and the UDF call is replaced by a reference to that
//! column.  The projection may sit above the `Limit`, so the scan is looked
//! up through the whole input subtree rather than only the direct child.

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use lakesoul_io::text::verify::TEXT_SCORE_FIELD;

use crate::datasource::file_format::LakeSoulTextSearchExec;
use crate::udf::text_search_marker::TEXT_SCORE_FUNCTION;

/// Replaces projected `text_score` calls with the scan's score column.
#[derive(Debug, Default)]
pub struct TextScoreProjectionRule;

impl PhysicalOptimizerRule for TextScoreProjectionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node: Arc<dyn ExecutionPlan>| {
            let Some(projection) = node.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            // Find the text scan below and the search it serves.
            let mut search_key = None;
            let _ = projection.input().apply(|node| {
                if let Some(search) = node.downcast_ref::<LakeSoulTextSearchExec>() {
                    search_key = Some((
                        search.text_search().column.clone(),
                        search.text_search().query.clone(),
                    ));
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            });
            let Some(search_key) = search_key else {
                return Ok(Transformed::no(node));
            };
            // Every projected score must refer to that same search.
            let mut scored = false;
            for projection_expr in projection.expr() {
                if is_text_score_expr(&projection_expr.expr) {
                    if text_score_call(&projection_expr.expr) != Some(search_key.clone())
                    {
                        return Ok(Transformed::no(node));
                    }
                    scored = true;
                }
            }
            if !scored {
                return Ok(Transformed::no(node));
            }

            // Rebuild the scan with the score column, then read it here.
            let input = projection
                .input()
                .clone()
                .transform_down(|node: Arc<dyn ExecutionPlan>| {
                    if let Some(search) = node.downcast_ref::<LakeSoulTextSearchExec>() {
                        return Ok(Transformed::yes(search.with_exposed_score()?));
                    }
                    Ok(Transformed::no(node))
                })?
                .data;
            let score_index = input.schema().index_of(TEXT_SCORE_FIELD)?;
            let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = projection
                .expr()
                .iter()
                .map(|projection_expr| {
                    if is_text_score_expr(&projection_expr.expr) {
                        (
                            Arc::new(Column::new(TEXT_SCORE_FIELD, score_index))
                                as Arc<dyn PhysicalExpr>,
                            projection_expr.alias.clone(),
                        )
                    } else {
                        (
                            Arc::clone(&projection_expr.expr),
                            projection_expr.alias.clone(),
                        )
                    }
                })
                .collect();
            Ok(Transformed::yes(Arc::new(ProjectionExec::try_new(
                exprs, input,
            )?)))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "lakesoul_text_score_projection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether an expression is a `text_score(column, query)` UDF call.
fn is_text_score_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<ScalarFunctionExpr>()
        .is_some_and(|function| function.name() == TEXT_SCORE_FUNCTION)
}

/// The `(column, query)` arguments of a `text_score` call.
fn text_score_call(expr: &Arc<dyn PhysicalExpr>) -> Option<(String, String)> {
    let function = expr.downcast_ref::<ScalarFunctionExpr>()?;
    let [column, query] = function.args() else {
        return None;
    };
    let column = column.downcast_ref::<Column>()?.name().to_string();
    let literal = query.downcast_ref::<Literal>()?;
    let query = match literal.value() {
        datafusion::common::ScalarValue::Utf8(Some(query)) => query.clone(),
        _ => return None,
    };
    Some((column, query))
}
