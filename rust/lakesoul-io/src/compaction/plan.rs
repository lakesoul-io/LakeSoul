// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Compaction mode selection.
//!
//! The planner reads the primary keys of every input (never payload columns),
//! runs an ordered merge with UseLast semantics, and decides between:
//!
//! * [`CompactionMode::FileLevel`] - drop files whose rows are all superseded
//!   and keep the remaining files as runs (a metadata-only trivial move when
//!   nothing has to be rewritten);
//! * [`CompactionMode::BlockLevel`] - merge the surviving files by copying
//!   complete encoded blocks where possible;
//! * [`CompactionMode::Fallback`] - keep the existing full read-merge-write
//!   path (hash-like keys, heavy interleaving, unsupported format).

use crate::Result;
use crate::compaction::analyze::{FileFormat, FileSummary, InputSummary, PkKind};
use crate::compaction::pk_merge::{WinnerRow, merge_pk_winners};
use crate::config::LakeSoulIOConfig;

/// Minimum fraction of surviving rows that must fall in complete source blocks
/// before the planner selects block-level copy.
pub const DEFAULT_BLOCK_COPY_MIN_RATIO: f64 = 0.5;

/// Fraction of files whose min/max cover the observed key extremes above which
/// a fixed-hex primary key is treated as hash-like.
const HASH_LIKE_WIDE_RANGE_RATIO: f64 = 0.8;

#[derive(Debug, Clone)]
pub struct CompactionOptions {
    pub primary_keys: Vec<String>,
    /// Select block-level copy when at least this fraction of surviving rows
    /// sits in complete source blocks.
    pub block_copy_min_ratio: f64,
    /// When true, the caller requires one output file.  File-level keep/discard
    /// is then only used when a single file survives; otherwise the surviving
    /// files must be merged.
    pub require_single_file: bool,
    /// Only allow modes that do not rewrite payload data.
    pub metadata_only: bool,
}

impl CompactionOptions {
    pub fn new(primary_keys: Vec<String>) -> Self {
        Self {
            primary_keys,
            block_copy_min_ratio: DEFAULT_BLOCK_COPY_MIN_RATIO,
            require_single_file: false,
            metadata_only: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionMode {
    /// Already one file (or nothing to do).
    KeepOnly,
    /// Keep the surviving files, discard fully superseded files.
    FileLevel,
    /// Merge surviving files by copying complete encoded blocks.
    BlockLevel,
    /// Existing full read-merge-write path.
    Fallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileAction {
    Keep,
    Discard,
    Rewrite,
}

#[derive(Debug, Clone)]
pub struct FilePlan {
    pub index: usize,
    pub path: String,
    pub action: FileAction,
    pub winner_rows: u64,
}

/// One contiguous source row range to copy, in output order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange {
    /// Source file index in the input list.
    pub file: usize,
    /// First source row (inclusive), aligned to a source block boundary.
    pub start_row: u64,
    /// Last source row (exclusive), aligned to a source block boundary.
    pub end_row: u64,
}

/// A fully copyable block-level plan.
///
/// Phase 1 only emits plans whose surviving rows are entirely covered by
/// complete source blocks.  The ranges are already in output key order and may
/// alternate between files when block-aligned updates allow it.  Mixed
/// copy/re-encode plans are a later phase.
#[derive(Debug, Clone)]
pub struct BlockPlan {
    pub ranges: Vec<BlockRange>,
    /// Rows that can be copied without decoding.
    pub copy_rows: u64,
    /// Total surviving rows.
    pub total_rows: u64,
}

#[derive(Debug, Clone, Default)]
pub struct PlanStats {
    pub total_rows: u64,
    pub winner_rows: u64,
    pub copy_rows: u64,
    pub duplicate_ratio: f64,
    pub copy_ratio: f64,
    pub nested_ratio: f64,
    pub disjoint_ratio: f64,
    pub overlap_ratio: f64,
    pub wide_range_ratio: f64,
    pub newest_contains_all: bool,
    pub hash_like: bool,
    pub files: usize,
    pub keep_files: usize,
    pub discard_files: usize,
    pub rewrite_files: usize,
}

#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub mode: CompactionMode,
    pub files: Vec<FilePlan>,
    pub block_plan: Option<BlockPlan>,
    pub stats: PlanStats,
    pub reason: String,
}

impl CompactionPlan {
    pub fn file_level(&self) -> bool {
        self.mode == CompactionMode::FileLevel
    }

    pub fn block_level(&self) -> bool {
        self.mode == CompactionMode::BlockLevel
    }

    pub fn fallback(&self) -> bool {
        self.mode == CompactionMode::Fallback
    }
}

/// Plan a compaction from file summaries read by
/// [`crate::compaction::analyze::summarize_files`].
///
/// The summaries must be in LakeSoul version order (oldest first).  The
/// planner reads primary keys only and never reads a payload column.
pub async fn plan_compaction(
    summary: &InputSummary,
    options: &CompactionOptions,
) -> Result<CompactionPlan> {
    plan_compaction_with_config(summary, options, None).await
}

/// Like [`plan_compaction`], but reuses `reader_config` for the primary-key
/// merge: object-store credentials, partition defaults, range partitions and
/// native-reader options all come from it.  The planner replaces its file
/// list, primary keys and target schema.
pub async fn plan_compaction_with_config(
    summary: &InputSummary,
    options: &CompactionOptions,
    reader_config: Option<&LakeSoulIOConfig>,
) -> Result<CompactionPlan> {
    let files = &summary.files;
    if files.is_empty() {
        return Ok(plan(
            CompactionMode::KeepOnly,
            files,
            None,
            PlanStats::default(),
            "no input files",
        ));
    }
    if files.len() == 1 {
        let mut stats = stats_from_summaries(files);
        stats.total_rows = files[0].rows;
        stats.winner_rows = files[0].rows;
        stats.copy_rows = files[0].rows;
        stats.copy_ratio = 1.0;
        return Ok(plan(
            CompactionMode::KeepOnly,
            files,
            None,
            stats,
            "single input file",
        ));
    }
    if summary.pk_types.is_none() {
        return Ok(plan(
            CompactionMode::Fallback,
            files,
            None,
            stats_from_summaries(files),
            "primary key types differ across input files",
        ));
    }

    let metrics = range_metrics(files);
    let mut stats = PlanStats {
        nested_ratio: metrics.nested_ratio,
        disjoint_ratio: metrics.disjoint_ratio,
        overlap_ratio: metrics.overlap_ratio,
        wide_range_ratio: metrics.wide_range_ratio,
        newest_contains_all: metrics.newest_contains_all,
        hash_like: summary.pk_kind == Some(PkKind::FixedHex)
            && metrics.wide_range_ratio >= HASH_LIKE_WIDE_RANGE_RATIO
            && !metrics.newest_contains_all,
        files: files.len(),
        ..Default::default()
    };

    // A cheap metadata guard for hash-like keys: almost every file spans
    // nearly the whole observed key range and no single newest file contains
    // the others.  Random/hash keys cannot have fully superseded files, and
    // their blocks are fully interleaved, so the key pass cannot change the
    // fallback decision.  A false positive only loses a possible optimization;
    // it never copies or drops data incorrectly.
    if stats.hash_like && !stats.newest_contains_all {
        stats.total_rows = files.iter().map(|file| file.rows).sum();
        stats.winner_rows = stats.total_rows;
        stats.copy_ratio = 0.0;
        stats.keep_files = files.len();
        stats.rewrite_files = files.len();
        return Ok(plan(
            CompactionMode::Fallback,
            files,
            None,
            stats,
            "hash-like key ranges: every file spans the key space, use the existing merge path",
        ));
    }

    // `hash_like` is otherwise only diagnostic: two ordered files with a small
    // overlap also produce overlapping non-nested ranges, and only the key
    // merge can tell whether blocks are copyable.  The key-only pass is cheap
    // compared with reading payload.
    //
    // Run the real merge-on-read pipeline over the key columns only: the
    // reader decides which row survives for every key, and the source tag
    // appended by `merge_pk_winners` tells the planner where that row lives.
    let merge = merge_pk_winners(summary, &options.primary_keys, reader_config).await?;
    stats.total_rows = merge.total_rows;
    stats.winner_rows = merge.winners.len() as u64;
    stats.duplicate_ratio = if merge.total_rows == 0 {
        0.0
    } else {
        1.0 - merge.winners.len() as f64 / merge.total_rows as f64
    };
    let runs = winner_runs(&merge.winners);
    let mut block_ranges = Vec::new();
    let mut copy_rows = 0u64;
    for run in &runs {
        if let Some((start, end)) =
            complete_block_range(&files[run.file], run.start, run.end)
        {
            copy_rows += end - start;
            block_ranges.push(BlockRange {
                file: run.file,
                start_row: start,
                end_row: end,
            });
        }
    }
    stats.copy_rows = copy_rows;
    stats.copy_ratio = if stats.winner_rows == 0 {
        0.0
    } else {
        copy_rows as f64 / stats.winner_rows as f64
    };

    let mut winner_counts = vec![0u64; files.len()];
    for winner in &merge.winners {
        winner_counts[winner.file] += 1;
    }
    let discard: Vec<usize> = (0..files.len())
        .filter(|&index| winner_counts[index] == 0)
        .collect();
    let keep: Vec<usize> = (0..files.len())
        .filter(|&index| winner_counts[index] > 0)
        .collect();
    stats.keep_files = keep.len();
    stats.discard_files = discard.len();

    let mut file_plans = files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let action = if winner_counts[index] == 0 {
                FileAction::Discard
            } else {
                FileAction::Keep
            };
            FilePlan {
                index,
                path: file.path.clone(),
                action,
                winner_rows: winner_counts[index],
            }
        })
        .collect::<Vec<_>>();

    // File level: at least one fully superseded file, and the caller accepts
    // multiple output runs.  No payload is rewritten.
    if !discard.is_empty() && !options.require_single_file {
        stats.rewrite_files = 0;
        return Ok(CompactionPlan {
            mode: CompactionMode::FileLevel,
            files: file_plans,
            block_plan: None,
            stats,
            reason: format!(
                "{} of {} files are fully superseded; keep the rest as runs",
                discard.len(),
                files.len()
            ),
        });
    }

    // Block level: every surviving row is covered by complete source blocks.
    // The ranges already follow the merge output order, so block-aligned
    // updates can alternate between source files.
    if !options.metadata_only
        && stats.copy_ratio >= options.block_copy_min_ratio
        && stats.copy_rows == stats.winner_rows
        && files.iter().all(|file| file.format == FileFormat::Vortex)
    {
        stats.rewrite_files = keep.len();
        for file in &mut file_plans {
            if file.action == FileAction::Keep {
                file.action = FileAction::Rewrite;
            }
        }
        return Ok(CompactionPlan {
            mode: CompactionMode::BlockLevel,
            files: file_plans,
            block_plan: Some(BlockPlan {
                ranges: block_ranges,
                copy_rows: stats.copy_rows,
                total_rows: stats.winner_rows,
            }),
            stats,
            reason: "all surviving rows sit in complete blocks; copy encoded blocks"
                .to_string(),
        });
    }

    // File-level keep/discard is still useful when the caller requires a single
    // file only if exactly one file survives.
    if discard.len() + 1 == files.len() && keep.len() == 1 {
        stats.rewrite_files = 0;
        return Ok(CompactionPlan {
            mode: CompactionMode::FileLevel,
            files: file_plans,
            block_plan: None,
            stats,
            reason: "one file survives; discard the rest".to_string(),
        });
    }

    if options.metadata_only {
        stats.rewrite_files = keep.len();
        return Ok(CompactionPlan {
            mode: CompactionMode::Fallback,
            files: file_plans,
            block_plan: None,
            stats,
            reason: "metadata-only mode requires a single surviving file".to_string(),
        });
    }

    stats.rewrite_files = keep.len();
    Ok(CompactionPlan {
        mode: CompactionMode::Fallback,
        files: file_plans,
        block_plan: None,
        stats,
        reason: "surviving rows are interleaved; use the existing merge path".to_string(),
    })
}

fn plan(
    mode: CompactionMode,
    files: &[FileSummary],
    block_plan: Option<BlockPlan>,
    stats: PlanStats,
    reason: impl Into<String>,
) -> CompactionPlan {
    CompactionPlan {
        mode,
        files: files
            .iter()
            .enumerate()
            .map(|(index, file)| FilePlan {
                index,
                path: file.path.clone(),
                action: FileAction::Keep,
                winner_rows: file.rows,
            })
            .collect(),
        block_plan,
        stats,
        reason: reason.into(),
    }
}

fn stats_from_summaries(files: &[FileSummary]) -> PlanStats {
    PlanStats {
        total_rows: files.iter().map(|file| file.rows).sum(),
        files: files.len(),
        ..Default::default()
    }
}

struct RangeMetrics {
    nested_ratio: f64,
    disjoint_ratio: f64,
    overlap_ratio: f64,
    wide_range_ratio: f64,
    newest_contains_all: bool,
}

fn range_metrics(files: &[FileSummary]) -> RangeMetrics {
    let mut nested = 0usize;
    let mut disjoint = 0usize;
    let mut overlap = 0usize;
    let mut pairs = 0usize;
    for left in 0..files.len() {
        for right in left + 1..files.len() {
            pairs += 1;
            let (a, b) = (&files[left], &files[right]);
            if a.pk_last < b.pk_first || b.pk_last < a.pk_first {
                disjoint += 1;
            } else if contains(a, b) || contains(b, a) {
                nested += 1;
            } else {
                overlap += 1;
            }
        }
    }
    let newest_contains_all = files.last().is_some_and(|newest| {
        files[..files.len() - 1]
            .iter()
            .all(|older| contains(newest, older))
    });
    // Cheap hash-key signal: most files' first and last keys share the first
    // encoded byte of the global extremes.  Random keys cover the whole key
    // space; ordered/range-clustered files do not.
    let global_first = files
        .iter()
        .filter(|file| !file.pk_first.is_empty())
        .map(|file| &file.pk_first)
        .min();
    let global_last = files
        .iter()
        .filter(|file| !file.pk_last.is_empty())
        .map(|file| &file.pk_last)
        .max();
    let wide = files
        .iter()
        .filter(|file| {
            !file.pk_first.is_empty()
                && global_first
                    .and_then(|key| key.first())
                    .is_some_and(|byte| file.pk_first.first() == Some(byte))
                && global_last
                    .and_then(|key| key.first())
                    .is_some_and(|byte| file.pk_last.first() == Some(byte))
        })
        .count();
    RangeMetrics {
        nested_ratio: ratio(nested, pairs),
        disjoint_ratio: ratio(disjoint, pairs),
        overlap_ratio: ratio(overlap, pairs),
        wide_range_ratio: ratio(wide, files.len()),
        newest_contains_all,
    }
}

fn ratio(value: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        value as f64 / total as f64
    }
}

fn contains(outer: &FileSummary, inner: &FileSummary) -> bool {
    outer.pk_first <= inner.pk_first && outer.pk_last >= inner.pk_last
}

#[derive(Debug)]
struct WinnerRun {
    file: usize,
    start: u64,
    end: u64,
}

fn winner_runs(winners: &[WinnerRow]) -> Vec<WinnerRun> {
    let mut runs: Vec<WinnerRun> = Vec::new();
    for winner in winners {
        match runs.last_mut() {
            Some(run) if run.file == winner.file && run.end == winner.row => {
                run.end += 1;
            }
            _ => runs.push(WinnerRun {
                file: winner.file,
                start: winner.row,
                end: winner.row + 1,
            }),
        }
    }
    runs
}

/// The complete-block sub-range of `[start, end)`, or `None` when no whole
/// block is covered.
fn complete_block_range(file: &FileSummary, start: u64, end: u64) -> Option<(u64, u64)> {
    if file.blocks.len() < 2 {
        return None;
    }
    // The block containing `start` is partial; complete blocks start at the
    // first boundary >= start.
    let first = file.blocks.partition_point(|offset| *offset < start);
    // The first boundary > end; all complete blocks end at or before
    // `blocks[last - 1]`.
    let last = file.blocks.partition_point(|offset| *offset <= end);
    if last <= first + 1 {
        return None;
    }
    Some((file.blocks[first], file.blocks[last - 1]))
}
