// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Text index integration: shard build, search and exact verification.
//!
//! The kind-specific pieces live in the `lakesoul-text` crate (splits,
//! Tantivy search, verification); this module wires them into the shared
//! index framework (shard prefixes, reader options, candidate injection,
//! opaque [`ResolvedIndex`](crate::index::commit::ResolvedIndex) transport).

pub mod builder;
pub mod reader;
pub mod search;
pub mod verify;
