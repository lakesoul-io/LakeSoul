// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::compute::SortOptions;
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, create_physical_expr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
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

/// Ends the OpenTelemetry span of the currently-entered tracing span right now,
/// so it is exported even when the tracing registry keeps the span alive past
/// this function: an unclosed descendant span holds the registry's reference
/// count above zero and would otherwise suppress the export. This overrides
/// only the export lifetime; it does not fix the underlying reference-count
/// leak.
///
/// Shared with [`crate::distributed::planner`], which ends its own
/// `distributed_plan` span the same way.
pub(crate) fn end_current_otel_span() {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let span = tracing::Span::current();
    let cx = span.context();
    let span_ref = cx.span();
    let sc = span_ref.span_context().clone();
    let recording_before = span_ref.is_recording();
    tracing::info!(
        span_name = span.metadata().map(|m| m.name()).unwrap_or("<none>"),
        span_id = ?span.id(),
        otel_has_active_span = cx.has_active_span(),
        otel_span_valid = sc.is_valid(),
        otel_remote = sc.is_remote(),
        otel_trace_id = %sc.trace_id(),
        otel_span_id = %sc.span_id(),
        otel_sampled = sc.trace_flags().is_sampled(),
        otel_is_recording_before = recording_before,
        "ending current OTel span"
    );
    span_ref.end();
    let recording_after = span_ref.is_recording();
    tracing::info!(
        span_name = span.metadata().map(|m| m.name()).unwrap_or("<none>"),
        otel_is_recording_after = recording_after,
        "OTel span end dispatched"
    );
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
    if let Some(plan) = build_fast_plan_index_only(logical_plan, session_state).await? {
        return Ok(Some(plan));
    }
    build_fast_plan_exact(logical_plan, session_state).await
}

/// Fast physical plan for the index-only statement shape.
async fn build_fast_plan_index_only(
    logical_plan: &LogicalPlan,
    session_state: &dyn Session,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
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

/// Fast physical plan for the exact re-rank statement shape.
///
/// The optimized logical plan is
/// `[Limit] -> [Projection]* -> Sort(distance, fetch) -> TableScan(marker)`:
/// the limit is folded into the sort's fetch and the select list is a plain
/// column projection above the sort.  The physical optimizer cannot improve
/// this shape (the scan already builds the vector-search exec), so building
/// `scan -> SortExec(fetch) -> ProjectionExec` directly avoids the default
/// planner and its optimizer.  Every mismatch falls back to the default
/// planner.
async fn build_fast_plan_exact(
    logical_plan: &LogicalPlan,
    session_state: &dyn Session,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let mut node = logical_plan;
    let mut limit_fetch = None;
    if let LogicalPlan::Limit(limit) = node {
        if let Some(skip) = limit.skip.as_deref()
            && limit_literal(skip) != Some(0)
        {
            return Ok(None);
        }
        if let Some(expr) = limit.fetch.as_deref() {
            match limit_literal(expr) {
                Some(value) => limit_fetch = Some(value),
                None => return Ok(None),
            }
        }
        node = limit.input.as_ref();
    }
    // Plain column projections above the sort.
    let mut projections: Vec<&datafusion::logical_expr::Projection> = Vec::new();
    loop {
        match node {
            LogicalPlan::Projection(projection) => {
                if projection
                    .expr
                    .iter()
                    .any(|expr| !matches!(expr, Expr::Column(_)))
                {
                    return Ok(None);
                }
                projections.push(projection);
                node = projection.input.as_ref();
            }
            _ => break,
        }
    }
    let LogicalPlan::Sort(sort) = node else {
        return Ok(None);
    };
    let Some(fetch) = sort.fetch.or(limit_fetch) else {
        return Ok(None);
    };
    if fetch == 0 || sort.expr.is_empty() {
        return Ok(None);
    }
    let LogicalPlan::TableScan(scan) = sort.input.as_ref() else {
        return Ok(None);
    };
    let Some(request) =
        crate::udf::vector_search_marker::parse_vector_search_request(&scan.filters)
    else {
        return Ok(None);
    };
    // The index-only ordering is served by that shortcut above.
    if request.order_by {
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
    let mut plan: Arc<dyn ExecutionPlan> = provider
        .scan(
            session_state,
            scan.projection.as_ref(),
            &scan.filters,
            Some(fetch),
        )
        .await?;
    if !contains_vector_search_exec(plan.as_ref()) {
        return Ok(None);
    }
    // The exact re-rank: the scan returns the candidate rows and the sort
    // keeps the global top-k.
    let input_schema = sort.input.schema();
    let mut sort_exprs = Vec::with_capacity(sort.expr.len());
    for sort_expr in &sort.expr {
        let physical = create_physical_expr(
            sort_expr.expr.as_ref(),
            input_schema,
            session_state.execution_props(),
            &PhysicalPlanningContext::default(),
        )?;
        sort_exprs.push(PhysicalSortExpr {
            expr: physical,
            options: SortOptions {
                descending: !sort_expr.asc,
                nulls_first: sort_expr.nulls_first,
            },
        });
    }
    // The vector-search exec returns exact mode rows already ordered by their
    // exact distance (the same f32 accumulation the sort expression above
    // evaluates), so this statement shape needs no Sort at all: the scan is
    // the ordered source and only the limit remains.
    if !fused_exact_order_enabled() {
        let Some(ordering) = LexOrdering::new(sort_exprs) else {
            return Ok(None);
        };
        plan = Arc::new(SortExec::new(ordering, plan).with_fetch(Some(fetch)));
    }
    // The projections run above the sort, innermost first.
    for projection in projections.into_iter().rev() {
        let input_schema = projection.input.schema();
        let mut exprs = Vec::with_capacity(projection.expr.len());
        for expr in &projection.expr {
            let Expr::Column(column) = expr else {
                return Ok(None);
            };
            let physical = create_physical_expr(
                expr,
                input_schema,
                session_state.execution_props(),
                &PhysicalPlanningContext::default(),
            )?;
            exprs.push((physical, column.name.clone()));
        }
        plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
    }
    // Without the Sort the scan's fetch is not enforced by a top-k node, so
    // the limit always keeps only the first `fetch` ordered rows.
    plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(fetch)));
    // Hand over only when the shortcut plan agrees with the logical plan's
    // output schema.
    let expected = logical_plan.schema().as_arrow().clone();
    if plan.schema().as_ref() != &expected {
        return Ok(None);
    }
    Ok(Some(plan))
}

/// Whether the exact statement can rely on the vector-search exec's ordering
/// instead of a `SortExec` above it (`LAKESOUL_VECTOR_FUSED_EXACT_ORDER=0`
/// restores the sort for A/B runs).
fn fused_exact_order_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        !std::env::var("LAKESOUL_VECTOR_FUSED_EXACT_ORDER")
            .is_ok_and(|value| value == "0")
    })
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
        tracing::info!(
            wall_ms = started.elapsed().as_secs_f64() * 1000.0,
            success = plan.is_ok(),
            "scan planning physical planner summary"
        );
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
        end_current_otel_span();
        Ok(plan)
    }
}
