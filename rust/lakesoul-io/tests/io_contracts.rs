// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Single integration-test binary for `lakesoul-io`.
//!
//! Each suite used to be its own non-cacheable test executable. Declaring
//! them as modules links once and keeps the shared dependency build small.

#[path = "io_contracts/append_only_leaf.rs"]
mod append_only_leaf;

#[path = "io_contracts/bucket_prefix.rs"]
mod bucket_prefix;

#[path = "io_contracts/cdc_delete_test.rs"]
mod cdc_delete_test;

#[path = "io_contracts/compaction_plan.rs"]
mod compaction_plan;

#[path = "io_contracts/lazy_scan.rs"]
mod lazy_scan;

#[path = "io_contracts/merge_on_read_vortex.rs"]
mod merge_on_read_vortex;

#[path = "io_contracts/metadata_fetch_budget.rs"]
mod metadata_fetch_budget;

#[path = "io_contracts/mor_filter_pushdown_test.rs"]
mod mor_filter_pushdown_test;

#[path = "io_contracts/mor_nullability.rs"]
mod mor_nullability;

#[path = "io_contracts/read_test.rs"]
mod read_test;

#[path = "io_contracts/session_adapter.rs"]
mod session_adapter;

#[path = "io_contracts/text_e2e_test.rs"]
mod text_e2e_test;

#[path = "io_contracts/vector_e2e_test.rs"]
mod vector_e2e_test;

#[path = "io_contracts/vortex_chunk_probe.rs"]
mod vortex_chunk_probe;

#[path = "io_contracts/vortex_pk_probe.rs"]
mod vortex_pk_probe;

#[path = "io_contracts/vortex_row_block_bench.rs"]
mod vortex_row_block_bench;

#[path = "io_contracts/vortex_row_blocks.rs"]
mod vortex_row_blocks;

#[path = "io_contracts/writer_spill_test.rs"]
mod writer_spill_test;
