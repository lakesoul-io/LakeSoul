// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::common::alias::AliasGenerator;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// Reuses DataFusion's join rewrite for provably single-row, uncorrelated
/// scalar subqueries. Other runtime scalar subqueries retain their cardinality
/// checks and are placed on the coordinator by the physical planner.
#[derive(Debug, Default)]
pub(crate) struct DistributedScalarSubqueryToJoin(ScalarSubqueryToJoin);

impl OptimizerRule for DistributedScalarSubqueryToJoin {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.0.apply_order()
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let exprs = match &plan {
            LogicalPlan::Filter(filter) => std::slice::from_ref(&filter.predicate),
            LogicalPlan::Projection(projection) => projection.expr.as_slice(),
            _ => return Ok(Transformed::no(plan)),
        };
        let mut saw_global_aggregate = false;
        let mut saw_correlated = false;
        for expr in exprs {
            // The upstream switch applies to all uncorrelated scalars in this
            // node. If even one needs the runtime cardinality check, keep it
            // enabled for the whole node (which cannot ship as a worker stage).
            if expr.exists(|expr| {
                let Expr::ScalarSubquery(subquery) = expr else {
                    return Ok(false);
                };
                if !subquery.outer_ref_columns.is_empty() {
                    saw_correlated = true;
                    return Ok(false);
                }
                let single_row = is_global_aggregate(&subquery.subquery);
                saw_global_aggregate |= single_row;
                Ok(!single_row)
            })? {
                return self.0.rewrite(plan, config);
            }
        }
        if saw_global_aggregate {
            self.0.rewrite(plan, &JoinRewriteConfig(config))
        } else if saw_correlated {
            self.0.rewrite(plan, config)
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

fn is_global_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => is_global_aggregate(&projection.input),
        LogicalPlan::SubqueryAlias(alias) => is_global_aggregate(&alias.input),
        // Exactly one row even on empty input; a grouped aggregate can return
        // several. Do not use estimates or infer uniqueness from scan stats.
        LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.is_empty(),
        _ => false,
    }
}

/// Turn on the upstream join rewrite for this node only, retaining the
/// statement's alias generator so aliases cannot collide across nodes.
struct JoinRewriteConfig<'a>(&'a dyn OptimizerConfig);

impl OptimizerConfig for JoinRewriteConfig<'_> {
    fn query_execution_start_time(&self) -> Option<DateTime<Utc>> {
        self.0.query_execution_start_time()
    }

    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        self.0.alias_generator()
    }

    fn options(&self) -> Arc<ConfigOptions> {
        let mut options = self.0.options();
        Arc::make_mut(&mut options)
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery = false;
        options
    }

    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        self.0.function_registry()
    }
}
