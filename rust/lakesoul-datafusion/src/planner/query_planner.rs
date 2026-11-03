// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::error::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
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
        let planner = LakeSoulPhysicalPlanner::new();
        let plan = planner
            .create_physical_plan(logical_plan, session_state)
            .await;
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
