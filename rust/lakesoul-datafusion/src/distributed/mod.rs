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
//! A primary-key scan groups its files into atomic `(range partition, hash
//! bucket)` work units, preserving version order within each unit. Whole units
//! are packed across the scan's task budget, so even one range partition (or
//! a table without range columns) can use several workers when it has several
//! populated buckets. Each task's merge runs at most `target_partitions` local
//! lanes, executing further units sequentially. If any file has a missing or
//! out-of-range bucket ID, its whole range stays one indivisible unit instead.
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

pub use planner::LakeSoulDistributedQueryPlanner;
pub use resolver::{
    KubernetesDiscovery, StaticWorkerResolver, WorkerDiscovery, WorkerSnapshot,
};

/// Protocol version exchanged between coordinators and workers.
///
/// Discovery filters workers whose reported version does not match, and
/// workers whose [`codec::CODEC_VERSION`] differs reject encoded plans loudly
/// at decode time.
///
/// # Generations
///
/// The version identifies a *protocol generation*, not just this crate's
/// release. Bump it for incompatible changes to a released protocol.
///
/// `/1` advertised an unversioned `MergeParquetExecProto`: protobuf decoders
/// ignore unknown fields rather than rejecting them, so a plan written by this
/// build would have been silently accepted by a `/1` worker.
///
/// The protocol is still unreleased, so the generation stays at `/2`
/// (`/2-ivm` with the `ivm` feature) while the codec list grows. It carries
/// the explicit [`codec::CODEC_VERSION`] field and registers the vortex scan
/// codec at position 1, the metadata-only scan codec at position 2, the write
/// codec at position 3, the federation codec at position 4, the IVM refresh
/// codec at position 5 when the `ivm` feature is enabled, then the
/// external-file scan and write codecs and the ordered per-key upsert state
/// machine.
///
/// The numeric part tracks [`codec::CODEC_VERSION`]; the `-ivm` suffix keeps
/// the two codec lists apart during discovery. Because the same number covers
/// every development build, every coordinator and worker must run the same
/// build: a payload from another build can name a codec position that
/// dispatches to a different codec instead of failing loudly.
///
/// Do not bump the number for an unreleased change: append codecs and keep
/// `/2`. Discovery is the only gate between two development builds that share
/// this number; [`StaticWorkerResolver`] probes only when it is built or
/// updated and keeps workers whose probe timed out, so rebuild the resolver
/// when a static worker changes build.
#[cfg(feature = "ivm")]
pub const DISTRIBUTED_PROTOCOL_VERSION: &str = "lakesoul-distributed/2-ivm";
#[cfg(not(feature = "ivm"))]
pub const DISTRIBUTED_PROTOCOL_VERSION: &str = "lakesoul-distributed/2";

#[cfg(feature = "ivm")]
pub use worker::set_ivm_worker_options;
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
    /// Maximum tasks per distributed stage, or 0 for the library default
    /// (the number of workers).
    ///
    /// A worker has several execution threads; keeping the cap at the worker
    /// count leaves all but one of them idle.  Raising it lets a stage fan a
    /// scan or keyed merge out over several tasks per worker.
    pub max_tasks_per_stage: usize,
}

impl Default for DistributedOptions {
    fn default() -> Self {
        Self {
            discovery: WorkerDiscovery::Static(Vec::new()),
            fallback_to_local: false,
            target_partitions: 4,
            bytes_per_partition: None,
            max_tasks_per_stage: 0,
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
