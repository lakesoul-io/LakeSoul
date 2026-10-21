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
//!
//! # Task placement
//!
//! The distributed planner splits a file scan over the tasks of its stage.
//! The desired-task-count handlers the session registers keep two kinds of
//! stage in one task instead: a stage holding a merge-on-read work unit,
//! which must see every version of its keys, and a stage in which an
//! operator requires a distribution of an input that reaches a scan without
//! leaving the stage — a `Single` aggregate, a global sort or limit planned
//! on a one-partition append-only work unit — which must see the whole scan.
//! Such a stage stays a stage of its own next to a stage of several tasks;
//! the planner drops a boundary between two one-task stages, and a plan left
//! without any boundary runs on the coordinator as the single-node plan.
//!
//! DataFusion splits file scans into partitions by a rule of its own, before
//! the distributed planner runs: `EnforceDistribution` byte-splits a scan of
//! at least `repartition_file_min_size` bytes, while the distributed planner
//! splits a scan of more than `bytes_per_partition * target_partitions`
//! bytes. A distributed session caps the former at the latter, so where a
//! source supports byte ranges, a scan the distributed planner splits is
//! already planned in several partitions — a `Partial` aggregate on the scan,
//! its `Final` half behind a stage boundary — and its stage keeps its tasks.
//! The cap only shapes plans: a scan DataFusion still keeps whole (Parquet
//! files, which LakeSoul labels compressed, or exact statistics of at most
//! one batch) relies on the one-task rule above for correctness.
//! Merge-on-read inputs declare their single file as their partitioning and
//! their primary-key order, so DataFusion never byte-splits one and, with
//! the merge's stage in one task, the merge reads each input as one sorted
//! stream.
//!
//! # Configuration and invariants
//!
//! The cap is an initialization-time heuristic, not a unified partitioning
//! policy. The thresholds control different decisions: whether DataFusion
//! introduces local scan partitions, and how many distributed tasks share
//! them. Source capabilities, row statistics, declared partitioning and
//! order-preserving file groups can prevent the first decision independently
//! of byte size; changing either setting after session creation also bypasses
//! the alignment. Memory sources do not consult the file-size threshold.
//!
//! Keep the settings independent and the one-task guards authoritative. A
//! longer-term distributed planner should place already-planned partitions
//! and indivisible work units without invalidating a parent's distribution
//! or ordering requirements, rather than independently re-splitting every
//! scan by bytes. The regression suite checks both levels: DataFusion's scan
//! partitioning and aggregate modes before task splitting, and input
//! distribution/ordering plus finalization cardinality after splitting.

pub mod codec;
pub mod headers;
pub mod planner;
pub mod resolver;
mod trace_context;
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
/// release: bump it whenever the physical-plan encoding changes.
///
/// `/1` advertised an unversioned `MergeParquetExecProto`: protobuf decoders
/// ignore unknown fields rather than rejecting them, so a plan written by this
/// build would have been silently accepted by a `/1` worker.
///
/// `/2` is the current generation and the first one versioned on the wire: it
/// carries the explicit [`codec::CODEC_VERSION`] field *and* registers the
/// vortex scan codec at position 2 of [`codec::user_codecs`]. Both changes are
/// one generation rather than two because neither format has ever been
/// released — a `/2` worker from this tree resolves every codec position a
/// coordinator from it can stamp. Split them the moment one of the two ships on
/// its own: extension payloads name the *position* of the codec that wrote
/// them, so a coordinator that can stamp position 2 needs workers that resolve
/// it.
///
/// Advertising `/2` filters `/1` workers out at discovery and closes the other
/// direction at decode time.
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
    /// leaf without a wire form), plan it with the plain LakeSoul planner and
    /// execute it single-node on the coordinator instead of failing.
    ///
    /// Production deployments must leave this `false`: such a failure then
    /// fails the query instead of silently degrading to coordinator-only
    /// execution. Only planning failures are covered: a plan the distributed
    /// planner keeps on the coordinator (a stage capped at one task, or every
    /// query while a static worker list is empty) runs as planned in either
    /// mode, and a worker resolver error (Kubernetes discovery without an
    /// eligible worker, or a stale snapshot) fails the query in either mode.
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
    /// workers). `None` keeps the library default (16 MiB). The session also
    /// caps `datafusion.optimizer.repartition_file_min_size` at
    /// `bytes_per_partition * target_partitions`, so DataFusion partitions the
    /// scans this splits wherever their source supports it (see
    /// [Task placement](self#task-placement)).
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
