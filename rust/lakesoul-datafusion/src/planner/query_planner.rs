// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;

use crate::planner::physical_planner::LakeSoulPhysicalPlanner;

use async_trait::async_trait;

const QUERY_PLANS_TOTAL: &str = "lakesoul_query_plans_total";
const QUERY_PLANNING_DURATION_SECONDS: &str = "lakesoul_query_planning_duration_seconds";

static DESCRIBE_METRICS: std::sync::Once = std::sync::Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            QUERY_PLANS_TOTAL,
            "Physical plans created by the LakeSoul query planner grouped by outcome"
        );
        metrics::describe_histogram!(
            QUERY_PLANNING_DURATION_SECONDS,
            metrics::Unit::Seconds,
            "Physical planning duration grouped by outcome"
        );
    });
}

/// The wrapper of the [`QueryPlanner`] for LakeSoul table.
#[derive(Debug)]
pub struct LakeSoulQueryPlanner {}

impl LakeSoulQueryPlanner {
    pub fn new_ref() -> Arc<dyn QueryPlanner + Send + Sync> {
        Arc::new(Self {})
    }
}

/// Fast physical plan for the index-only vector-search statement shape.
///
/// The logical optimizer has already rewritten the statement into
/// `Limit(fetch) -> TableScan(projection, filters=[marker], ...)`, so this
/// shape needs no per-node plan conversion and none of the physical optimizer
/// rewrites: the scan itself builds the vector-search exec, and the only node
/// above it is the row limit.  Every mismatch returns `Ok(None)` and the
/// statement is planned by the default planner, so only this exact shape takes
/// the shortcut.
///
/// `LAKESOUL_VECTOR_FAST_PLAN=0` disables the shortcut for A/B runs.
async fn build_fast_plan(
    logical_plan: &LogicalPlan,
    session_state: &dyn Session,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if !fast_plan_enabled() {
        return Ok(None);
    }
    // Shape: Limit(fetch) on the table scan.
    let LogicalPlan::Limit(limit) = logical_plan else {
        return Ok(None);
    };
    let Some(fetch) = limit.fetch.as_deref().and_then(limit_literal) else {
        return Ok(None);
    };
    let LogicalPlan::TableScan(scan) = limit.input.as_ref() else {
        return Ok(None);
    };
    // Only the index-only ordering can drop the sort: the scan returns the
    // candidates already ordered by their index distance.
    let Some(request) =
        crate::udf::vector_search_marker::parse_vector_search_request(&scan.filters)
    else {
        return Ok(None);
    };
    if !request.order_by {
        return Ok(None);
    }
    let Ok(source) = datafusion::datasource::source_as_provider(&scan.source) else {
        return Ok(None);
    };
    let Some(provider) =
        source.downcast_ref::<crate::datasource::table_provider::LakeSoulTableProvider>()
    else {
        return Ok(None);
    };
    let exec = provider
        .scan(
            session_state,
            scan.projection.as_ref(),
            &scan.filters,
            Some(fetch),
        )
        .await?;
    // Anything but the vector scan (a fallback full scan) keeps the default
    // planner.
    if !contains_vector_search_exec(exec.as_ref()) {
        return Ok(None);
    }
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::limit::GlobalLimitExec::new(exec, 0, Some(fetch)),
    );
    // Hand over only when the shortcut plan agrees with the logical plan's
    // output schema.
    let expected = logical_plan.schema().as_arrow().clone();
    let plan_schema = plan.schema();
    if plan_schema.as_ref() != &expected {
        return Ok(None);
    }
    Ok(Some(plan))
}

/// The `fetch` value of a literal limit, when it is a non-negative integer.
fn limit_literal(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) if *value >= 0 => {
            Some(*value as usize)
        }
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => {
            Some(usize::try_from(*value).ok()?)
        }
        _ => None,
    }
}

fn fast_plan_enabled() -> bool {
    !std::env::var("LAKESOUL_VECTOR_FAST_PLAN").is_ok_and(|value| value == "0")
}

/// Whether the plan tree contains the vector-search scan.
fn contains_vector_search_exec(plan: &dyn ExecutionPlan) -> bool {
    plan.is::<crate::datasource::file_format::LakeSoulVectorSearchExec>()
        || plan
            .children()
            .iter()
            .any(|child| contains_vector_search_exec(child.as_ref()))
}

#[async_trait]
impl QueryPlanner for LakeSoulQueryPlanner {
    // Given a `LogicalPlan`, create an [`ExecutionPlan`] suitable for execution
    #[tracing::instrument(
        name = "physical_plan",
        level = "info",
        skip_all,
        fields(
            output_field_count = logical_plan.schema().fields().len(),
            physical_plan_root = tracing::field::Empty,
        ),
        err
    )]
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        describe_metrics();
        let started = std::time::Instant::now();
        let plan = build_fast_plan(logical_plan, session_state).await;
        let fast_path = plan.is_ok();
        let t_default = std::time::Instant::now();
        let plan = match plan {
            Ok(Some(plan)) => Ok(plan),
            _ => {
                let planner = LakeSoulPhysicalPlanner::new();
                planner
                    .create_physical_plan(logical_plan, session_state)
                    .await
            }
        };
        if std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok() {
            tracing::debug!(
                fast_path,
                default_planner_ms = t_default.elapsed().as_secs_f64() * 1000.0,
                total_ms = started.elapsed().as_secs_f64() * 1000.0,
                "lakesoul physical planner profile"
            );
        }
        let outcome = if plan.is_ok() { "success" } else { "error" };
        metrics::counter!(QUERY_PLANS_TOTAL, "outcome" => outcome).increment(1);
        metrics::histogram!(
            QUERY_PLANNING_DURATION_SECONDS,
            "outcome" => outcome,
        )
        .record(started.elapsed().as_secs_f64());
        let plan = plan?;
        tracing::Span::current().record("physical_plan_root", plan.name());
        tracing::debug!(root = plan.name(), "physical plan created");
        Ok(plan)
    }
}
