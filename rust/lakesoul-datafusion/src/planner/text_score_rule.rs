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
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
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

            // Rebuild the scan with the score column, then read it here.  The
            // score must be exposed directly above the index scan: a
            // PostgreSQL plan keeps a `FilterExec`/`CooperativeExec` between
            // this projection and the scan, and those nodes only preserve
            // their input schema.  Wrapping the scan in a projection that
            // appends the score makes every ancestor carry it.
            let input = expose_score(projection.input().clone())?;
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

/// Make the text scan's internal score column visible to every ancestor.
///
/// `FilterExec`, `GlobalLimitExec` and `CooperativeExec` only forward their
/// input schema, so appending the score at the scan itself is not enough when
/// the score projection sits above them.  Wrap the scan in a projection that
/// keeps all visible columns and appends the score; ancestors then rebuild
/// with the wider schema.
fn expose_score(plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
    if plan.schema().index_of(TEXT_SCORE_FIELD).is_ok() {
        return Ok(plan);
    }
    if let Some(search) = plan.downcast_ref::<LakeSoulTextSearchExec>() {
        let exposed = search.with_exposed_score()?;
        let schema = exposed.schema();
        let score_index = schema.index_of(TEXT_SCORE_FIELD)?;
        let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            if field.name() == TEXT_SCORE_FIELD {
                continue;
            }
            exprs.push((
                Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            ));
        }
        exprs.push((
            Arc::new(Column::new(TEXT_SCORE_FIELD, score_index)) as Arc<dyn PhysicalExpr>,
            TEXT_SCORE_FIELD.to_string(),
        ));
        return Ok(Arc::new(ProjectionExec::try_new(exprs, exposed)?));
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let rewritten = children
        .into_iter()
        .map(|child| expose_score(Arc::clone(child)))
        .collect::<DFResult<Vec<_>>>()?;
    let rewritten = plan.with_new_children(rewritten)?;
    // `FilterExec` can carry a projection that drops the score column.  Keep
    // its existing output columns and append the score when its input now
    // exposes it, so the projection above the filter can read it.
    if let Some(filter) = rewritten.downcast_ref::<FilterExec>()
        && filter.schema().index_of(TEXT_SCORE_FIELD).is_err()
        && let Ok(score_index) = filter.input().schema().index_of(TEXT_SCORE_FIELD)
    {
        let mut projection = filter
            .projection()
            .as_ref()
            .map(|projection| projection.to_vec())
            .unwrap_or_else(|| (0..filter.input().schema().fields().len()).collect());
        projection.push(score_index);
        let new_filter = FilterExecBuilder::new(
            Arc::clone(filter.predicate()),
            Arc::clone(filter.input()),
        )
        .apply_projection(Some(projection))?
        .build()?;
        return Ok(Arc::new(new_filter));
    }
    Ok(rewritten)
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
