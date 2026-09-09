// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Distributed query execution for LakeSoul tables, built on
//! [`datafusion_distributed`].
//!
//! The coordinator session wraps the LakeSoul [`QueryPlanner`] with the
//! distributed planner (see [`session::LakeSoulSessionFactory`]); standalone
//! workers are created with [`lakesoul_worker`] and served over gRPC.
//!
//! Protocol compatibility between a coordinator and its workers is pinned by
//! [`DISTRIBUTED_PROTOCOL_VERSION`]: workers advertise it through their
//! `GetWorkerInfo` gRPC endpoint and discovery only admits matching workers.
//! The version must be bumped whenever the physical-plan encoding changes
//! (e.g. the [`codec::LakeSoulCodec`] wire format or the grouping of scan
//! leaves), so that a mixed cluster fails fast instead of misinterpreting
//! plans.

pub mod codec;
pub mod planner;
pub mod resolver;
pub mod worker;

pub use codec::LakeSoulCodec;
pub use planner::LakeSoulDistributedQueryPlanner;
pub use resolver::{
    KubernetesDiscovery, StaticWorkerResolver, WorkerDiscovery, WorkerSnapshot,
};

/// Protocol version exchanged between coordinators and workers.
///
/// Discovery filters workers whose reported version does not match, and
/// workers with a different build reject encoded plans loudly at decode time.
pub const DISTRIBUTED_PROTOCOL_VERSION: &str = "lakesoul-distributed/1";

pub use worker::{
    LakeSoulWorkerOptions, LakeSoulWorkerSessionBuilder, lakesoul_worker,
    spawn_lakesoul_worker,
};

#[derive(Debug, Clone)]
pub struct DistributedOptions {
    /// How worker URLs are discovered.
    pub discovery: WorkerDiscovery,
    /// Development-mode fallback: when no ready worker is available, execute
    /// the query single-node on the coordinator instead of failing.
    ///
    /// Production deployments must leave this `false`: an empty worker
    /// snapshot then fails every distributed query fast, never silently
    /// degrading to coordinator-only execution.
    pub fallback_to_local: bool,
    /// `target_partitions` applied to distributed sessions.
    ///
    /// The physical planner only inserts the hash `RepartitionExec` nodes that
    /// become distributed stage boundaries when `target_partitions > 1`, so
    /// distributed sessions must not run with `1`.
    pub target_partitions: usize,
    /// Overrides the distributed planner's bytes-per-partition scan estimate.
    ///
    /// Lower values fan the scan stage out over more tasks (and therefore more
    /// workers). `None` keeps the library default (16 MiB).
    pub bytes_per_partition: Option<usize>,
}

impl Default for DistributedOptions {
    fn default() -> Self {
        Self {
            discovery: WorkerDiscovery::Static(Vec::new()),
            fallback_to_local: false,
            target_partitions: 4,
            bytes_per_partition: None,
        }
    }
}

impl DistributedOptions {
    /// Static worker list (local development / bare metal).
    pub fn static_workers(urls: Vec<String>) -> Self {
        Self {
            discovery: WorkerDiscovery::Static(urls),
            ..Self::default()
        }
    }
}
