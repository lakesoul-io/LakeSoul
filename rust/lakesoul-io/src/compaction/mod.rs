// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Compaction planning and copy-through execution.
//!
//! The planner reads metadata and primary keys only, then selects one of:
//! file-level keep/discard, block-level copy, or the existing fallback path.
//! See `plan.rs` for the decision rules, `pk_merge.rs` for the reader-based
//! primary-key merge, and `raw_copy.rs` for the encoded-segment copy.

pub mod analyze;
pub mod compact;
pub mod copy;
pub mod pk_merge;
pub mod plan;
pub mod raw_copy;

pub use analyze::{
    FileFormat, FileSummary, InputSummary, PkKey, PkKind, read_file_schema,
    summarize_file, summarize_files,
};
pub use compact::{CompactToOneOptions, CompactToOneResult, compact_to_one_file};
pub use copy::{copy_files, copy_vortex_files, copy_vortex_ranges};
pub use pk_merge::{
    PROVENANCE_FILE_COLUMN, PROVENANCE_ROW_COLUMN, PkMergeResult, WinnerRow,
    merge_pk_winners,
};
pub use plan::{
    BlockPlan, BlockRange, CompactionMode, CompactionOptions, CompactionPlan, FileAction,
    FilePlan, PlanStats, plan_compaction, plan_compaction_with_config,
};
pub use raw_copy::copy_vortex_ranges_raw;
