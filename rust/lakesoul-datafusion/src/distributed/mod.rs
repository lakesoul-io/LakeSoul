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
//! [`DISTRIBUTED_PROTOCOL_VERSION`]: workers advertise it through their
//! `GetWorkerInfo` gRPC endpoint. Both discovery backends consult it —
//! Kubernetes discovery admits only matching workers, and static worker
//! lists are probed (and mismatches dropped) when a resolver is built or
//! updated — so that a mixed cluster fails fast instead of misinterpreting
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
///
/// # Generations
///
/// The version identifies a *protocol generation*, not just this crate's
/// release: bump it whenever the physical-plan encoding changes. Introducing
/// the explicit [`codec::CODEC_VERSION`] field was such a change — the
/// previous generation advertised `lakesoul-distributed/1` with an unversioned
/// `MergeParquetExecProto`, and protobuf decoders ignore unknown fields rather
/// than rejecting them, so a plan written by the versioned encoder would have
/// been silently accepted by a `/1` worker. Advertising `/2` filters those
/// workers out at discovery and closes the other direction at decode time.
pub const DISTRIBUTED_PROTOCOL_VERSION: &str = "lakesoul-distributed/2";

pub use worker::{
    LakeSoulWorkerOptions, LakeSoulWorkerSessionBuilder, lakesoul_worker,
    spawn_lakesoul_worker,
};

#[derive(Debug, Clone)]
pub struct DistributedOptions {
    /// How worker URLs are discovered.
    pub discovery: WorkerDiscovery,
    /// Development-mode fallback: when the distributed planner fails to plan
    /// a query, or plans one whose worker stages cannot be serialized (a scan
    /// leaf without a wire form, e.g. vortex), plan it with the plain LakeSoul
    /// planner and execute it single-node on the coordinator instead of
    /// failing.
    ///
    /// Production deployments must leave this `false`: such a failure then
    /// fails the query instead of silently degrading to coordinator-only
    /// execution. Only planning failures are covered — a query the distributed
    /// planner can plan by itself (including every query while no worker is
    /// ready, which such a planner plans single-node, and any plan it decides
    /// to keep on the coordinator) is executed as planned in either mode.
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
