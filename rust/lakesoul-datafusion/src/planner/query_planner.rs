// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;

use crate::planner::physical_planner::LakeSoulPhysicalPlanner;

use async_trait::async_trait;

use lakesoul_io::datafusion;

pub struct LakeSoulQueryPlanner {}

impl LakeSoulQueryPlanner {
    pub fn new_ref() -> Arc<dyn QueryPlanner + Send + Sync> {
        Arc::new(Self {})
    }
}

#[async_trait]
impl QueryPlanner for LakeSoulQueryPlanner {
    // Given a `LogicalPlan`, create an [`ExecutionPlan`] suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = LakeSoulPhysicalPlanner::new();
        planner.create_physical_plan(logical_plan, session_state).await
    }
}
