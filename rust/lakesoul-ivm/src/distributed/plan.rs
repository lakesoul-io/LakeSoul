// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Task-splitting hooks for the distributed planner.
//!
//! `lakesoul-datafusion` registers the planner handlers, but the scan node is
//! private to this crate.  These helpers keep the node opaque and expose only
//! the two operations the planner needs.

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;

/// How many independent task variants an IVM scan can offer.
///
/// `None` means the plan is not an IVM scan.  The count is the number of
/// already bucket-split file groups; one group is the smallest unit that
/// preserves merge-on-read semantics.
pub fn stream_scan_task_count(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    plan.downcast_ref::<crate::provider::IvmStreamExec>()
        .map(|exec| exec.group_count())
}

/// How many tasks a keyed state lookup can use.
///
/// The lookup owns one state-bucket range per task, so it must never request
/// more tasks than the table has buckets: a larger consumer task count would
/// ask the range shuffle to split a bucket, which merge-on-read cannot do.
pub fn lookup_task_count(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    plan.downcast_ref::<crate::runtime::keyed_plan::IvmPkLookupExec>()
        .map(|exec| exec.bucket_count())
}

/// Split an IVM scan over `task_count` variants, or `None` for other plans.
pub fn split_stream_scan(
    plan: &Arc<dyn ExecutionPlan>,
    task_count: usize,
) -> DFResult<Option<Vec<Arc<dyn ExecutionPlan>>>> {
    let Some(exec) = plan.downcast_ref::<crate::provider::IvmStreamExec>() else {
        return Ok(None);
    };
    exec.split_for_tasks(task_count).map(Some)
}
