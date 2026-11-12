// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cache-locality placement for append-only scans.
//!
//! Every worker keeps its own disk cache of the files it has read. Without a
//! stable choice of worker, the same file lands on a random worker on every
//! query and each worker ends up caching the same hot files. This module gives
//! every piece of a scan a preferred worker by rendezvous (highest random
//! weight) hashing of an affinity key, and bounds how far that preference may
//! overload one worker.
//!
//! Two steps follow the same placement:
//!
//! 1. **Pieces to tasks**, when the distributed planner scales a scan leaf
//!    out ([`split_scan_by_locality`]): pieces preferred by one worker are
//!    grouped into that worker's tasks.
//! 2. **Tasks to workers**, when the coordinator routes the stage
//!    ([`locality_route_tasks`]): the placement is recomputed against the
//!    current worker snapshot, and each task goes to the worker that owns most
//!    of its bytes. Recomputing instead of carrying a hint in the plan keeps
//!    the wire protocol unchanged, and a worker that disappeared between
//!    planning and execution only moves the pieces it owned.
//!
//! The placement is a pure function of the pieces, the worker set, the task
//! count and the scan's table, so planning and routing agree without sharing
//! state. Merge-on-read scans are not placed yet: their stage is a
//! children-isolator union whose task-to-child mapping is not public.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{
    DistributedGetterExt, DistributedLeafExec, NetworkBoundaryExt, RouteTasksEvent,
    RouteTasksEventResponse,
};
use lakesoul_io::cache::LAKESOUL_CACHE_PAGE_BYTES;
use lakesoul_io::physical_plan::{LakeSoulScanExec, ScanFile};
use url::Url;

/// Counter of routed scan tasks, labelled by whether every piece of the task
/// was placed on the worker it was routed to.
pub const LOCALITY_TASKS_TOTAL: &str = "lakesoul_locality_tasks_total";
/// Gauge of the most loaded worker's routed bytes over the mean, for the last
/// routed scan stage.
pub const LOCALITY_LOAD_IMBALANCE: &str = "lakesoul_locality_load_imbalance_ratio";

/// Cache-locality task placement settings.
///
/// Stored in the coordinator session's config as an extension; its presence
/// turns locality placement on.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LocalityOptions {
    /// How far a worker's share of the scan's bytes may exceed its fair
    /// share before pieces spill to their next preferred worker: each of the
    /// `W` workers a scan runs on takes at most `(1 + epsilon) / W` of its
    /// bytes, unless a single piece is larger than that bound.
    ///
    /// `0` balances strictly and loses the most locality; large values
    /// approach plain rendezvous hashing, which never moves a piece but can
    /// put a skewed share on one worker.
    pub epsilon: f64,
}

impl LocalityOptions {
    /// The default load-bound slack.
    pub const DEFAULT_EPSILON: f64 = 0.25;

    /// Options with the given slack, rejecting a negative or non-finite one.
    pub fn new(epsilon: f64) -> Result<Self, String> {
        if !epsilon.is_finite() || epsilon < 0.0 {
            return Err(format!(
                "locality epsilon must be a finite number >= 0, got {epsilon}"
            ));
        }
        Ok(Self { epsilon })
    }
}

impl Default for LocalityOptions {
    fn default() -> Self {
        Self {
            epsilon: Self::DEFAULT_EPSILON,
        }
    }
}

/// One piece of work to place: its affinity key and its size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalityUnit {
    /// The bytes the preferred worker is derived from.
    pub key: Vec<u8>,
    /// The bytes the piece reads; used for the load bound and task balance.
    pub weight: u64,
}

/// Where every unit and every task goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The unit indexes each task reads, ascending.
    pub tasks: Vec<Vec<usize>>,
    /// The worker (index into the placed worker list) each task is meant for.
    pub task_workers: Vec<usize>,
    /// The worker each unit was placed on.
    pub unit_workers: Vec<usize>,
}

/// The rendezvous score of `key` on `worker`; the highest score wins.
///
/// Stable across processes and releases: never seeded from `RandomState`.
pub fn hrw_score(key: &[u8], worker: &Url) -> u64 {
    score(fnv1a(key), worker_hash(worker))
}

/// Sorts and deduplicates a worker list, the form [`place`] expects.
pub fn normalize_workers(mut workers: Vec<Url>) -> Vec<Url> {
    workers.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
    workers.dedup();
    workers
}

/// Places `units` over `task_count` tasks on `workers`.
///
/// `workers` must be normalized ([`normalize_workers`]). `scope` names the
/// table: when there are fewer tasks than workers, the tasks go to the
/// `task_count` workers ranked highest for `scope`, so one table keeps using
/// the same subset of workers. With at least as many tasks as workers, every
/// worker gets `task_count / W` tasks, and the workers ranked highest for
/// `scope` one more until the count is reached.
///
/// Units are taken largest first. Each goes to the highest-scoring worker
/// that has tasks and whose load stays within `(1 + epsilon)` times its fair
/// share; if none can take it, to the worker it overloads least. Inside a
/// worker, units are spread over its tasks by bytes, largest first.
///
/// The result depends only on the units (not their order, unless two have
/// the same key and weight), the worker set, the task count, `epsilon` and
/// `scope`. Returns `None` when there are no workers or no tasks.
pub fn place(
    units: &[LocalityUnit],
    workers: &[Url],
    task_count: usize,
    epsilon: f64,
    scope: &[u8],
) -> Option<Placement> {
    if workers.is_empty() || task_count == 0 {
        return None;
    }
    let worker_hashes: Vec<u64> = workers.iter().map(worker_hash).collect();

    // Task slots per worker, decided by the table's ranking of the workers.
    let scope_hash = fnv1a(scope);
    let ranked = rank_workers(scope_hash, &worker_hashes);
    let (base, extra) = (task_count / workers.len(), task_count % workers.len());
    let mut slots = vec![0usize; workers.len()];
    for (rank, &worker) in ranked.iter().enumerate() {
        slots[worker] = base + usize::from(rank < extra);
    }
    let mut worker_tasks: Vec<Vec<usize>> = vec![Vec::new(); workers.len()];
    let mut task_workers = Vec::with_capacity(task_count);
    for &worker in &ranked {
        for _ in 0..slots[worker] {
            worker_tasks[worker].push(task_workers.len());
            task_workers.push(worker);
        }
    }
    let candidates: Vec<usize> = (0..workers.len()).filter(|&w| slots[w] > 0).collect();

    // Units to workers, largest first, under the load bound.
    let key_hashes: Vec<u64> = units.iter().map(|unit| fnv1a(&unit.key)).collect();
    let mut order: Vec<usize> = (0..units.len()).collect();
    order.sort_unstable_by(|&a, &b| {
        (Reverse(units[a].weight), key_hashes[a], &units[a].key, a).cmp(&(
            Reverse(units[b].weight),
            key_hashes[b],
            &units[b].key,
            b,
        ))
    });
    // Workers, not tasks, are the resource: a worker with more tasks splits
    // the same fair share into smaller tasks rather than drawing keys away
    // from the others, which would move them on every change of task count.
    let total: f64 = units.iter().map(|unit| unit.weight as f64).sum();
    let capacity = (1.0 + epsilon) * total / candidates.len() as f64;
    let mut loads = vec![0u64; workers.len()];
    let mut unit_workers = vec![0usize; units.len()];
    let mut preference: Vec<(u64, usize)> = Vec::with_capacity(candidates.len());
    for &unit in &order {
        let weight = units[unit].weight;
        preference.clear();
        preference.extend(
            candidates
                .iter()
                .map(|&w| (score(key_hashes[unit], worker_hashes[w]), w)),
        );
        preference.sort_unstable_by_key(|&(s, w)| (Reverse(s), w));
        let chosen = preference
            .iter()
            .map(|&(_, w)| w)
            .find(|&w| loads[w].saturating_add(weight) as f64 <= capacity)
            .unwrap_or_else(|| {
                // Nothing fits: overload the least loaded worker, preferring
                // the better-ranked one on a tie.
                preference
                    .iter()
                    .map(|&(_, w)| w)
                    .min_by_key(|&w| loads[w])
                    .expect("a candidate worker")
            });
        loads[chosen] = loads[chosen].saturating_add(weight);
        unit_workers[unit] = chosen;
    }

    // Units of a worker to its tasks, largest first onto the lightest task.
    let mut tasks: Vec<Vec<usize>> = vec![Vec::new(); task_count];
    let mut task_loads = vec![0u64; task_count];
    for &unit in &order {
        let task = *worker_tasks[unit_workers[unit]]
            .iter()
            .min_by_key(|&&t| (task_loads[t], tasks[t].len(), t))
            .expect("a placed worker has a task");
        task_loads[task] = task_loads[task].saturating_add(units[unit].weight);
        tasks[task].push(unit);
    }
    for task in &mut tasks {
        task.sort_unstable();
    }
    Some(Placement {
        tasks,
        task_workers,
        unit_workers,
    })
}

/// The affinity key of a scan piece: its path, plus the cache page its range
/// starts on when that is not the first.
///
/// A piece read from the start of the file shares its key with the whole
/// file, so a file read whole by one query and cut into ranges by the next
/// keeps its first range on the worker that cached it.
pub fn piece_key(file: &ScanFile) -> Vec<u8> {
    let page = file.range.as_ref().map_or(0, |range| {
        u64::try_from(range.start).unwrap_or(0) / LAKESOUL_CACHE_PAGE_BYTES as u64
    });
    let mut key = file.path.as_bytes().to_vec();
    if page > 0 {
        key.push(0);
        key.extend_from_slice(&page.to_le_bytes());
    }
    key
}

/// The placement units of scan pieces.
///
/// A piece of unknown size weighs the mean of the known ones (or 1 when none
/// is known), so a scan without sizes still balances by piece count.
pub fn scan_units<'a>(
    files: impl IntoIterator<Item = &'a ScanFile>,
) -> Vec<LocalityUnit> {
    let sized: Vec<(Vec<u8>, Option<u64>)> = files
        .into_iter()
        .map(|file| (piece_key(file), piece_bytes(file)))
        .collect();
    let known: Vec<u64> = sized.iter().filter_map(|(_, bytes)| *bytes).collect();
    let fallback = if known.is_empty() {
        1
    } else {
        (known.iter().map(|&b| u128::from(b)).sum::<u128>() / known.len() as u128) as u64
    };
    sized
        .into_iter()
        .map(|(key, bytes)| LocalityUnit {
            key,
            weight: bytes.unwrap_or(fallback),
        })
        .collect()
}

/// The table a scan reads, used to pick its worker subset.
fn scan_scope(scan: &LakeSoulScanExec) -> &[u8] {
    scan.scan_config().io_config.prefix().as_bytes()
}

/// The worker snapshot locality placement uses, or `None` when placement
/// does not apply: locality is off, no resolver is registered, it fails, or
/// it knows no worker. The default split and routing then apply.
fn locality_workers(config: &SessionConfig) -> Option<(LocalityOptions, Vec<Url>)> {
    let options = *config.get_extension::<LocalityOptions>()?;
    let resolver = config.get_distributed_worker_resolver().ok()?;
    let workers = normalize_workers(resolver.get_urls().ok()?);
    (!workers.is_empty()).then_some((options, workers))
}

/// Splits an append-only scan over `task_count` tasks so that each task's
/// pieces share one preferred worker.
///
/// Returns `None` when locality placement does not apply (see
/// [`LocalityOptions`]); the caller then uses the default split.
pub fn split_scan_by_locality(
    scan: &LakeSoulScanExec,
    task_count: usize,
    config: &SessionConfig,
) -> Option<DataFusionResult<Vec<Arc<LakeSoulScanExec>>>> {
    if scan.is_merge_on_read() {
        return None;
    }
    let (options, workers) = locality_workers(config)?;
    let scope = scan_scope(scan);
    Some(scan.split_into_tasks_by(task_count, |pieces| {
        let units = scan_units(pieces);
        Ok(place(&units, &workers, task_count, options.epsilon, scope)
            .expect("workers and tasks are non-empty")
            .tasks)
    }))
}

/// Routes the tasks of a stage that scans a LakeSoul table append-only to the
/// workers its pieces were placed on.
///
/// Answers `None`, deferring to the default routing, when locality is off,
/// the stage has a single task (which the defaults co-locate with the
/// coordinator or its input), or no split LakeSoul scan reads in this stage.
pub fn locality_route_tasks(
    ev: RouteTasksEvent<'_>,
) -> Option<DataFusionResult<RouteTasksEventResponse>> {
    if ev.task_count <= 1 {
        return None;
    }
    let config = ev.task_ctx.session_config();
    config.get_extension::<LocalityOptions>()?;
    let variants = stage_scan_variants(ev.plan)?;
    if variants.len() != ev.task_count {
        return None;
    }
    let (options, workers) = locality_workers(config)?;
    let urls = route_variants(&variants, &workers, options.epsilon);
    Some(Ok(RouteTasksEventResponse::new(urls)))
}

/// The task variants of the largest split append-only LakeSoul scan this
/// stage reads, without looking into the stages below it.
fn stage_scan_variants(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<&LakeSoulScanExec>> {
    let mut best: Option<(u64, Vec<&LakeSoulScanExec>)> = None;
    plan.apply(|node| {
        if node.is_network_boundary() {
            return Ok(TreeNodeRecursion::Jump);
        }
        let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        let Some(scan) = leaf.original().downcast_ref::<LakeSoulScanExec>() else {
            return Ok(TreeNodeRecursion::Jump);
        };
        if scan.is_merge_on_read() {
            return Ok(TreeNodeRecursion::Jump);
        }
        let variants: Option<Vec<&LakeSoulScanExec>> = leaf
            .variants()
            .iter()
            .map(|variant| variant.downcast_ref::<LakeSoulScanExec>())
            .collect();
        if let Some(variants) = variants {
            let bytes: u64 = scan_units(variant_files(&variants).map(|(_, file)| file))
                .iter()
                .map(|unit| unit.weight)
                .sum();
            if best.as_ref().is_none_or(|(most, _)| bytes > *most) {
                best = Some((bytes, variants));
            }
        }
        Ok(TreeNodeRecursion::Jump)
    })
    .ok()?;
    best.map(|(_, variants)| variants)
}

/// Every piece of every variant, with the index of the task that reads it.
fn variant_files<'a>(
    variants: &'a [&'a LakeSoulScanExec],
) -> impl Iterator<Item = (usize, &'a ScanFile)> + 'a {
    variants.iter().enumerate().flat_map(|(task, variant)| {
        variant
            .work_units()
            .iter()
            .flat_map(move |unit| unit.files.iter().map(move |file| (task, file)))
    })
}

/// One worker per variant: the worker its bytes were placed on by the same
/// placement the split used, recomputed against `workers`.
fn route_variants(
    variants: &[&LakeSoulScanExec],
    workers: &[Url],
    epsilon: f64,
) -> Vec<Url> {
    let task_count = variants.len();
    let (task_of, files): (Vec<usize>, Vec<&ScanFile>) = variant_files(variants).unzip();
    let units = scan_units(files);
    let scope = variants.first().map_or(&[][..], |scan| scan_scope(scan));
    let placement = place(&units, workers, task_count, epsilon, scope)
        .expect("workers and tasks are non-empty");

    // Bytes of each task per worker; a zero-byte piece still counts once.
    let mut votes: Vec<HashMap<usize, u64>> = vec![HashMap::new(); task_count];
    for (unit, &task) in task_of.iter().enumerate() {
        *votes[task].entry(placement.unit_workers[unit]).or_insert(0) +=
            units[unit].weight.max(1);
    }
    let mut routed: Vec<Option<usize>> = votes
        .iter()
        .map(|task| {
            task.iter()
                .max_by_key(|&(&worker, &bytes)| (bytes, Reverse(worker)))
                .map(|(&worker, _)| worker)
        })
        .collect();

    // A task without pieces goes to the placed worker with the fewest tasks.
    let mut task_counts = vec![0usize; workers.len()];
    for worker in routed.iter().flatten() {
        task_counts[*worker] += 1;
    }
    let mut placed: Vec<usize> = placement.task_workers.clone();
    placed.sort_unstable();
    placed.dedup();
    for slot in routed.iter_mut().filter(|slot| slot.is_none()) {
        let worker = *placed
            .iter()
            .min_by_key(|&&w| (task_counts[w], w))
            .expect("a placement has a worker");
        task_counts[worker] += 1;
        *slot = Some(worker);
    }
    let routed: Vec<usize> = routed.into_iter().map(|w| w.expect("routed")).collect();

    record_route_metrics(&votes, &routed, &units, &task_of, workers.len());
    routed.into_iter().map(|w| workers[w].clone()).collect()
}

fn record_route_metrics(
    votes: &[HashMap<usize, u64>],
    routed: &[usize],
    units: &[LocalityUnit],
    task_of: &[usize],
    worker_count: usize,
) {
    for (task, worker) in votes.iter().zip(routed) {
        if task.is_empty() {
            continue;
        }
        let outcome = if task.len() == 1 && task.contains_key(worker) {
            "preferred"
        } else {
            "fallback"
        };
        metrics::counter!(LOCALITY_TASKS_TOTAL, "outcome" => outcome).increment(1);
    }
    let mut bytes = vec![0u64; worker_count];
    for (unit, &task) in task_of.iter().enumerate() {
        bytes[routed[task]] = bytes[routed[task]].saturating_add(units[unit].weight);
    }
    let busy: Vec<u64> = bytes.into_iter().filter(|&b| b > 0).collect();
    if !busy.is_empty() {
        let mean = busy.iter().map(|&b| b as f64).sum::<f64>() / busy.len() as f64;
        let max = busy.iter().copied().max().unwrap_or(0) as f64;
        metrics::gauge!(LOCALITY_LOAD_IMBALANCE).set(max / mean);
    }
}

/// The bytes a piece reads, or `None` when its size is unknown.
fn piece_bytes(file: &ScanFile) -> Option<u64> {
    match &file.range {
        Some(range) => Some(u64::try_from(range.end - range.start).unwrap_or(0)),
        None => file.size,
    }
}

/// The workers in descending rendezvous order for `scope`.
fn rank_workers(scope_hash: u64, worker_hashes: &[u64]) -> Vec<usize> {
    let mut ranked: Vec<usize> = (0..worker_hashes.len()).collect();
    ranked.sort_unstable_by_key(|&w| (Reverse(score(scope_hash, worker_hashes[w])), w));
    ranked
}

fn worker_hash(worker: &Url) -> u64 {
    splitmix64(fnv1a(worker.as_str().as_bytes()))
}

fn score(key_hash: u64, worker_hash: u64) -> u64 {
    splitmix64(key_hash ^ worker_hash)
}

/// 64-bit FNV-1a: a fixed, dependency-free hash of the key bytes.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The splitmix64 finalizer: spreads every input bit over the output, so
/// scores of one key on different workers are independent.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn workers(count: usize) -> Vec<Url> {
        normalize_workers(
            (0..count)
                .map(|i| Url::parse(&format!("http://worker-{i}:50051")).unwrap())
                .collect(),
        )
    }

    fn uniform(count: usize, weight: u64) -> Vec<LocalityUnit> {
        (0..count)
            .map(|i| LocalityUnit {
                key: format!("s3://bucket/table/part-{i:05}.parquet").into_bytes(),
                weight,
            })
            .collect()
    }

    /// Each unit's worker, by key.
    fn owners(
        units: &[LocalityUnit],
        workers: &[Url],
        placement: &Placement,
    ) -> HashMap<Vec<u8>, Url> {
        units
            .iter()
            .zip(&placement.unit_workers)
            .map(|(unit, &w)| (unit.key.clone(), workers[w].clone()))
            .collect()
    }

    fn assert_covers(placement: &Placement, units: usize, tasks: usize) {
        assert_eq!(placement.tasks.len(), tasks);
        assert_eq!(placement.task_workers.len(), tasks);
        let mut seen: Vec<usize> = placement.tasks.iter().flatten().copied().collect();
        seen.sort_unstable();
        assert_eq!(
            seen,
            (0..units).collect::<Vec<_>>(),
            "every unit exactly once"
        );
        for (task, units) in placement.tasks.iter().enumerate() {
            for &unit in units {
                assert_eq!(
                    placement.unit_workers[unit], placement.task_workers[task],
                    "a task only holds units placed on its worker"
                );
            }
        }
    }

    #[test]
    fn scores_are_stable_constants() {
        // Pinned so a refactor cannot silently reshuffle every cache.
        let worker = Url::parse("http://worker-0:50051").unwrap();
        assert_eq!(
            hrw_score(b"s3://bucket/a.parquet", &worker),
            hrw_score(b"s3://bucket/a.parquet", &worker)
        );
        assert_ne!(
            hrw_score(b"s3://bucket/a.parquet", &worker),
            hrw_score(b"s3://bucket/b.parquet", &worker)
        );
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
    }

    #[test]
    fn placement_is_deterministic_and_order_independent() {
        let ws = workers(5);
        let units = uniform(200, 1 << 20);
        let first = place(&units, &ws, 5, 0.25, b"t").unwrap();
        assert_eq!(first, place(&units, &ws, 5, 0.25, b"t").unwrap());
        assert_covers(&first, units.len(), 5);

        let mut reversed = units.clone();
        reversed.reverse();
        let mut shuffled_workers = ws.clone();
        shuffled_workers.reverse();
        let second = place(
            &reversed,
            &normalize_workers(shuffled_workers),
            5,
            0.25,
            b"t",
        )
        .unwrap();
        assert_eq!(owners(&units, &ws, &first), owners(&reversed, &ws, &second));
    }

    #[test]
    fn removing_a_worker_moves_about_its_share() {
        let ws = workers(8);
        let units = uniform(4000, 1 << 20);
        let before = owners(&units, &ws, &place(&units, &ws, 8, 0.25, b"t").unwrap());
        let removed = ws[3].clone();
        let fewer: Vec<Url> = ws.iter().filter(|w| **w != removed).cloned().collect();
        let after = owners(
            &units,
            &fewer,
            &place(&units, &fewer, 7, 0.25, b"t").unwrap(),
        );

        let moved_elsewhere = units
            .iter()
            .filter(|u| before[&u.key] != removed && before[&u.key] != after[&u.key])
            .count();
        let owned = units.iter().filter(|u| before[&u.key] == removed).count();
        assert!(owned > 0);
        // The removed worker's ~1/8 must move; the load bound may shift a
        // little more, but far less than a reshuffle (~7/8).
        assert!(
            moved_elsewhere < units.len() / 8,
            "{moved_elsewhere} of {} units left a surviving worker",
            units.len()
        );
    }

    #[test]
    fn without_a_load_bound_removal_moves_only_the_removed_workers_units() {
        let ws = workers(6);
        let units = uniform(1000, 1);
        let loose = 1e9;
        let before = owners(&units, &ws, &place(&units, &ws, 6, loose, b"t").unwrap());
        let fewer: Vec<Url> = ws[..5].to_vec();
        let after = owners(
            &units,
            &fewer,
            &place(&units, &fewer, 5, loose, b"t").unwrap(),
        );
        for unit in &units {
            if before[&unit.key] != ws[5] {
                assert_eq!(before[&unit.key], after[&unit.key]);
            }
        }
    }

    #[test]
    fn adding_a_file_moves_only_that_file() {
        let ws = workers(4);
        let units = uniform(400, 1);
        let before = owners(&units, &ws, &place(&units, &ws, 4, 1e9, b"t").unwrap());
        let mut more = units.clone();
        more.push(LocalityUnit {
            key: b"s3://bucket/table/new.parquet".to_vec(),
            weight: 1,
        });
        let after = owners(&more, &ws, &place(&more, &ws, 4, 1e9, b"t").unwrap());
        for unit in &units {
            assert_eq!(before[&unit.key], after[&unit.key]);
        }
    }

    #[test]
    fn the_load_bound_holds_for_small_units() {
        let ws = workers(6);
        let units = uniform(3000, 1000);
        let total: u64 = units.iter().map(|u| u.weight).sum();
        for epsilon in [0.0, 0.1, 0.25, 1.0] {
            let placement = place(&units, &ws, 6, epsilon, b"t").unwrap();
            let mut loads = vec![0u64; ws.len()];
            for (unit, &w) in placement.unit_workers.iter().enumerate() {
                loads[w] += units[unit].weight;
            }
            let bound = (1.0 + epsilon) * total as f64 / 6.0;
            for load in loads {
                assert!(
                    load as f64 <= bound,
                    "{load} > {bound} at epsilon {epsilon}"
                );
            }
        }
    }

    #[test]
    fn a_unit_larger_than_the_bound_still_gets_placed() {
        let ws = workers(3);
        let mut units = uniform(10, 1);
        units.push(LocalityUnit {
            key: b"huge".to_vec(),
            weight: 1_000_000,
        });
        let placement = place(&units, &ws, 3, 0.25, b"t").unwrap();
        assert_covers(&placement, units.len(), 3);
    }

    #[test]
    fn fewer_tasks_than_workers_use_a_stable_subset_per_table() {
        let ws = workers(8);
        let units = uniform(100, 10);
        let a = place(&units, &ws, 3, 0.25, b"table-a").unwrap();
        assert_covers(&a, units.len(), 3);
        let used: HashSet<usize> = a.task_workers.iter().copied().collect();
        assert_eq!(used.len(), 3, "one task per worker");
        assert!(a.unit_workers.iter().all(|w| used.contains(w)));
        assert_eq!(a, place(&units, &ws, 3, 0.25, b"table-a").unwrap());
        // Another table may use another subset; across many tables every
        // worker gets used.
        let mut all = HashSet::new();
        for table in 0..32 {
            let p =
                place(&units, &ws, 3, 0.25, format!("table-{table}").as_bytes()).unwrap();
            all.extend(p.task_workers);
        }
        assert_eq!(all.len(), 8);
    }

    #[test]
    fn more_tasks_than_workers_split_each_workers_share() {
        let ws = workers(3);
        let units = uniform(90, 7);
        let placement = place(&units, &ws, 7, 0.25, b"t").unwrap();
        assert_covers(&placement, units.len(), 7);
        let mut per_worker = vec![0; 3];
        for &w in &placement.task_workers {
            per_worker[w] += 1;
        }
        per_worker.sort_unstable();
        assert_eq!(per_worker, vec![2, 2, 3]);
        assert!(placement.tasks.iter().all(|task| !task.is_empty()));

        // Each worker carries about a third of the bytes whatever its task
        // count; its tasks split that share evenly.
        let units = uniform(6000, 1);
        let placement = place(&units, &ws, 7, 0.25, b"t").unwrap();
        let mut worker_loads = vec![0usize; 3];
        for &w in &placement.unit_workers {
            worker_loads[w] += 1;
        }
        assert!(worker_loads.iter().all(|&l| l <= 2500), "{worker_loads:?}");
        for w in 0..3 {
            let loads: Vec<usize> = (0..7)
                .filter(|&t| placement.task_workers[t] == w)
                .map(|t| placement.tasks[t].len())
                .collect();
            let (min, max) = (*loads.iter().min().unwrap(), *loads.iter().max().unwrap());
            assert!(max - min <= 1, "tasks of worker {w}: {loads:?}");
        }
    }

    #[test]
    fn edge_cases() {
        assert!(place(&uniform(3, 1), &[], 2, 0.25, b"t").is_none());
        assert!(place(&uniform(3, 1), &workers(2), 0, 0.25, b"t").is_none());

        let empty = place(&[], &workers(2), 2, 0.25, b"t").unwrap();
        assert_covers(&empty, 0, 2);

        let zero = uniform(5, 0);
        let placement = place(&zero, &workers(2), 2, 0.25, b"t").unwrap();
        assert_covers(&placement, 5, 2);
    }

    #[test]
    fn piece_keys_follow_cache_pages() {
        let file = |range: Option<std::ops::Range<i64>>| ScanFile {
            path: "s3://b/t/f.parquet".to_string(),
            physical_format: lakesoul_io::file_format::PhysicalFormat::Parquet,
            size: Some(64 << 20),
            range,
        };
        let page = LAKESOUL_CACHE_PAGE_BYTES as i64;
        assert_eq!(piece_key(&file(None)), piece_key(&file(Some(0..page))));
        assert_eq!(
            piece_key(&file(Some(0..page))),
            piece_key(&file(Some(10..page)))
        );
        assert_ne!(
            piece_key(&file(None)),
            piece_key(&file(Some(page..2 * page)))
        );
        assert_eq!(
            piece_key(&file(Some(page..2 * page))),
            piece_key(&file(Some(page + 5..2 * page)))
        );
    }

    // -----------------------------------------------------------------------
    // Scale-up and routing on real scan descriptors.
    // -----------------------------------------------------------------------

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_distributed::{DistributedExt, WorkerResolver};
    use lakesoul_io::config::LakeSoulIOConfig;
    use lakesoul_io::file_format::PhysicalFormat;
    use lakesoul_io::physical_plan::{LakeSoulScanConfig, ScanWorkUnit};

    struct FixedWorkers(Vec<Url>);

    impl WorkerResolver for FixedWorkers {
        fn get_urls(&self) -> DataFusionResult<Vec<Url>> {
            Ok(self.0.clone())
        }
    }

    fn scan(primary_keys: &[&str], files: usize) -> LakeSoulScanExec {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let config = LakeSoulScanConfig {
            output_schema: Arc::clone(&schema),
            table_schema: Arc::clone(&schema),
            partition_schema: Arc::new(Schema::empty()),
            io_config: LakeSoulIOConfig::builder()
                .with_schema(schema)
                .with_prefix("s3://bucket/table".to_string())
                .with_primary_keys(primary_keys.iter().map(|k| k.to_string()).collect())
                .build(),
            predicate: None,
            key_constraint: None,
            parquet_force_view_types: false,
            read_options: HashMap::new(),
            limit: None,
            min_index_rows: lakesoul_io::pk_locator::MIN_INDEX_ROWS,
        };
        let files = (0..files)
            .map(|i| ScanFile {
                path: format!("s3://bucket/table/part-{i:04}.parquet"),
                physical_format: PhysicalFormat::Parquet,
                size: Some(1 << 20),
                range: None,
            })
            .collect();
        let unit = ScanWorkUnit {
            files,
            default_column_value: HashMap::new(),
            lane: 0,
        };
        LakeSoulScanExec::try_new(config, vec![unit], 1).expect("valid scan")
    }

    fn session(locality: bool, workers: &[Url]) -> SessionConfig {
        let mut config = SessionConfig::new()
            .with_distributed_worker_resolver(FixedWorkers(workers.to_vec()));
        if locality {
            config = config.with_extension(Arc::new(LocalityOptions::default()));
        }
        config
    }

    fn leaf(
        scan: &LakeSoulScanExec,
        variants: &[Arc<LakeSoulScanExec>],
    ) -> Arc<dyn ExecutionPlan> {
        let original: Arc<dyn ExecutionPlan> = Arc::new(scan.clone());
        Arc::new(
            DistributedLeafExec::try_new(
                original,
                variants
                    .iter()
                    .map(|v| Arc::clone(v) as Arc<dyn ExecutionPlan>),
            )
            .expect("valid leaf"),
        )
    }

    fn route(
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        config: SessionConfig,
    ) -> Option<Vec<Url>> {
        let task_ctx = Arc::new(TaskContext::default().with_session_config(config));
        locality_route_tasks(RouteTasksEvent {
            task_ctx,
            plan,
            task_count,
        })
        .map(|response| response.expect("routing succeeds").urls)
    }

    fn task_paths(variants: &[Arc<LakeSoulScanExec>]) -> Vec<Vec<String>> {
        variants
            .iter()
            .map(|v| {
                v.work_units()
                    .iter()
                    .flat_map(|u| u.files.iter().map(|f| f.path.clone()))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn locality_is_off_without_the_extension() {
        let ws = workers(3);
        let node = scan(&[], 12);
        assert!(split_scan_by_locality(&node, 3, &session(false, &ws)).is_none());
        let variants = node.split_into_tasks(3).unwrap();
        assert!(route(&leaf(&node, &variants), 3, session(false, &ws)).is_none());
    }

    #[test]
    fn merge_on_read_scans_are_not_placed() {
        let ws = workers(3);
        let node = scan(&["id"], 4);
        assert!(split_scan_by_locality(&node, 3, &session(true, &ws)).is_none());
    }

    #[test]
    fn routing_sends_each_task_to_the_worker_its_pieces_were_placed_on() {
        let ws = workers(4);
        let node = scan(&[], 64);
        let config = session(true, &ws);
        let variants = split_scan_by_locality(&node, 4, &config).unwrap().unwrap();
        assert_eq!(variants.len(), 4);
        let urls = route(&leaf(&node, &variants), 4, config).expect("routed");
        assert_eq!(urls.len(), 4);
        assert_eq!(
            urls.iter().collect::<HashSet<_>>().len(),
            4,
            "one task per worker"
        );

        // Every file is routed to the worker the placement prefers for it.
        let units = scan_units(node.work_units().iter().flat_map(|u| u.files.iter()));
        let placement = place(
            &units,
            &ws,
            4,
            LocalityOptions::DEFAULT_EPSILON,
            b"s3://bucket/table",
        )
        .unwrap();
        let preferred = owners(&units, &ws, &placement);
        for (task, paths) in task_paths(&variants).iter().enumerate() {
            for path in paths {
                assert_eq!(preferred[path.as_bytes()], urls[task], "{path}");
            }
        }

        // The same scan planned again lands every file on the same worker.
        let config = session(true, &ws);
        let again = split_scan_by_locality(&node, 4, &config).unwrap().unwrap();
        let urls_again = route(&leaf(&node, &again), 4, config).unwrap();
        let by_worker = |variants: &[Arc<LakeSoulScanExec>], urls: &[Url]| {
            let mut map = HashMap::new();
            for (paths, url) in task_paths(variants).into_iter().zip(urls) {
                for path in paths {
                    map.insert(path, url.clone());
                }
            }
            map
        };
        assert_eq!(by_worker(&variants, &urls), by_worker(&again, &urls_again));
    }

    #[test]
    fn a_worker_lost_before_routing_only_moves_its_tasks() {
        let ws = workers(4);
        let node = scan(&[], 64);
        let config = session(true, &ws);
        let variants = split_scan_by_locality(&node, 4, &config).unwrap().unwrap();
        let plan = leaf(&node, &variants);
        let before = route(&plan, 4, config).unwrap();

        let lost = before[2].clone();
        let survivors: Vec<Url> = ws.iter().filter(|w| **w != lost).cloned().collect();
        let after = route(&plan, 4, session(true, &survivors)).unwrap();
        assert_eq!(after.len(), 4, "one url per task");
        assert!(after.iter().all(|url| survivors.contains(url)));
        for task in [0, 1, 3] {
            assert_eq!(before[task], after[task], "task {task} keeps its worker");
        }
    }

    #[test]
    fn routing_defers_when_it_does_not_apply() {
        let ws = workers(3);
        let node = scan(&[], 12);
        let variants = node.split_into_tasks(3).unwrap();
        let plan = leaf(&node, &variants);
        assert!(route(&plan, 1, session(true, &ws)).is_none(), "single task");
        assert!(
            route(&plan, 2, session(true, &ws)).is_none(),
            "task count mismatch"
        );
        assert!(route(&plan, 3, session(true, &[])).is_none(), "no workers");

        let other: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        assert!(
            route(&other, 3, session(true, &ws)).is_none(),
            "no LakeSoul scan"
        );
    }

    #[test]
    fn tasks_without_pieces_still_get_a_worker() {
        let ws = workers(4);
        let node = scan(&[], 2);
        let config = session(true, &ws);
        let variants = split_scan_by_locality(&node, 4, &config).unwrap().unwrap();
        let busy = variants
            .iter()
            .filter(|v| !v.work_units().is_empty())
            .count();
        assert_eq!(busy, 2);
        let urls = route(&leaf(&node, &variants), 4, config).unwrap();
        assert_eq!(urls.len(), 4);
    }

    #[test]
    fn unknown_sizes_weigh_the_mean_of_known_ones() {
        let file = |name: &str, size: Option<u64>| ScanFile {
            path: name.to_string(),
            physical_format: lakesoul_io::file_format::PhysicalFormat::Parquet,
            size,
            range: None,
        };
        let files = [file("a", Some(10)), file("b", Some(30)), file("c", None)];
        let weights: Vec<u64> = scan_units(&files).iter().map(|u| u.weight).collect();
        assert_eq!(weights, vec![10, 30, 20]);
        let unknown = [file("a", None), file("b", None)];
        let weights: Vec<u64> = scan_units(&unknown).iter().map(|u| u.weight).collect();
        assert_eq!(weights, vec![1, 1]);
    }
}
