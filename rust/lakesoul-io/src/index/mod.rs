// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Kind-agnostic index framework.
//!
//! Secondary indexes (vector, text, ...) share the same storage and reader
//! mechanics: shard prefixes are derived from data file paths, shard commits
//! are resolved by the caller from the metadata catalog and transported
//! opaquely ([`commit::ResolvedIndex`]), loaded indexes are cached per
//! commit, and search results are injected into the reader's filters as
//! candidate primary keys.
//!
//! Kind-specific code lives next to the kind (`crate::vector`,
//! `crate::text`) and only implements the artifact writer/reader, the query
//! parser and the scoring.

pub mod cache;
pub mod candidate;
pub mod commit;
pub mod options;
pub mod prefix;
pub mod reader;

/// A lease held by a reader while it loads or searches a resolved index.
///
/// The concrete guard (typically a PostgreSQL lease handle) releases itself
/// when dropped; erasing the type keeps this crate independent of the
/// metadata layer.
pub struct IndexLease {
    _guard: Box<dyn std::any::Any + Send + Sync>,
}

impl IndexLease {
    pub fn new<T: std::any::Any + Send + Sync>(guard: T) -> Self {
        Self {
            _guard: Box::new(guard),
        }
    }
}

impl std::fmt::Debug for IndexLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexLease").finish()
    }
}

/// A candidate row produced by an index search.
///
/// `id` is the primary key of the row (the index stores ids as `u64`, like
/// the vector index does); `score` is the kind-specific relevance score
/// (higher is better) when the index computes one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candidate {
    pub id: u64,
    pub score: Option<f32>,
}

impl Candidate {
    /// A candidate without a relevance score.
    pub fn new(id: u64) -> Self {
        Self { id, score: None }
    }

    /// A candidate with a relevance score.
    pub fn scored(id: u64, score: f32) -> Self {
        Self {
            id,
            score: Some(score),
        }
    }
}
