// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Single integration-test binary for `rust/lakesoul-vector`.
//!
//! Each suite used to be its own non-cacheable test executable.
//! Declaring them as modules links once and keeps the build cacheable.

#[path = "vector_contracts/segment_merge_alignment.rs"]
mod segment_merge_alignment;

#[cfg(feature = "catalog")]
#[path = "vector_contracts/vector_commit_catalog.rs"]
mod vector_commit_catalog;
