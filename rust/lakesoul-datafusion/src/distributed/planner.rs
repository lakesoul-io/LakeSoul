// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Policy gate in front of the distributed planner.
//!
//! The gate enforces the availability policy **before** the distributed
//! planner touches a plan:
//!
//! - workers available → delegate to the wrapped distributed planner;
//! - no worker, `fallback_to_local = true` (development) → fall back to the
//!   plain LakeSoul planner, executing single-node on the coordinator;
//! - no worker, `fallback_to_local = false` (production) → fail fast with an
//!   explicit error. There is never a silent downgrade to coordinator-only
//!   execution: plans that would produce no distributed stage are refused too,
//!   instead of running unnoticed on a single machine.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::WorkerResolver;
use url::Url;

use crate::planner::LakeSoulQueryPlanner;

/// Wraps the distributed [`QueryPlanner`] installed by
/// `SessionStateBuilderExt::with_distributed_planner` and decides per query
/// whether distributed execution is possible.
pub struct LakeSoulDistributedQueryPlanner {
    /// The distributed planner to delegate to when workers are available.
    distributed: Arc<dyn QueryPlanner + Send + Sync>,
    /// The plain LakeSoul planner used for the single-node fallback.
    local: Arc<dyn QueryPlanner + Send + Sync>,
    resolver: Arc<dyn WorkerResolver>,
    fallback_to_local: bool,
}

impl std::fmt::Debug for LakeSoulDistributedQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSoulDistributedQueryPlanner")
            .field("fallback_to_local", &self.fallback_to_local)
            .finish_non_exhaustive()
    }
}

impl LakeSoulDistributedQueryPlanner {
    /// Builds the gate. `distributed` must be the planner produced by
    /// `with_distributed_planner()`.
    pub fn new(
        distributed: Arc<dyn QueryPlanner + Send + Sync>,
        resolver: Arc<dyn WorkerResolver>,
        fallback_to_local: bool,
    ) -> Self {
        Self {
            distributed,
            local: LakeSoulQueryPlanner::new_ref(),
            resolver,
            fallback_to_local,
        }
    }

    /// The resolved worker list, or an error carrying the availability
    /// policy verdict.
    fn check_availability(&self) -> DFResult<Vec<Url>> {
        match self.resolver.get_urls() {
            Ok(urls) if !urls.is_empty() => Ok(urls),
            Ok(_) if self.fallback_to_local => Ok(Vec::new()),
            Ok(_) => Err(datafusion::error::DataFusionError::Execution(
                "no ready LakeSoul workers available and single-node fallback is \
                 disabled (production mode); refusing to execute the query on the \
                 coordinator"
                    .to_string(),
            )),
            Err(err) if self.fallback_to_local => {
                warn!(
                    "LakeSoul worker resolution failed ({err}); falling back to \
                     single-node execution (development mode)"
                );
                Ok(Vec::new())
            }
            Err(err) => Err(datafusion::error::DataFusionError::Context(
                "no ready LakeSoul workers available (production mode)".to_string(),
                Box::new(err),
            )),
        }
    }
}

#[async_trait::async_trait]
impl QueryPlanner for LakeSoulDistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let workers = self.check_availability()?;
        if workers.is_empty() {
            return self.local.create_physical_plan(logical_plan, session).await;
        }
        self.distributed
            .create_physical_plan(logical_plan, session)
            .await
    }
}
