// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Bridge from DataFusion's synchronous table-function hook to async metadata.

use datafusion::common::{DataFusionError, Result as DFResult};
use tokio::runtime::{Handle, RuntimeFlavor};

/// Drive an async catalog/metadata lookup from a synchronous table-function
/// call.
///
/// `Handle::current()` panics without a runtime and plain `block_on` deadlocks
/// a runtime worker; `block_in_place` lets the multi-thread runtime move other
/// tasks away while this thread waits. A current-thread runtime is refused
/// with a planner error rather than risking a hang. This mirrors the metadata
/// write path's `catalog::snapshot::wait_on_runtime`.
pub(crate) fn wait_on_runtime<T: Send + 'static>(
    future: impl std::future::Future<Output = DFResult<T>> + Send + 'static,
) -> DFResult<T> {
    let handle = Handle::try_current().map_err(|error| {
        invalid(format!(
            "a tokio runtime is required for a pinned read: {error}"
        ))
    })?;
    if handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        return Err(invalid(
            "a pinned read requires a multi-thread tokio runtime",
        ));
    }
    tokio::task::block_in_place(|| handle.block_on(future))
}

fn invalid(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
