// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Transport type for an index commit resolved by the caller.
//!
//! The caller (the layer with metadata access) resolves a commit from the
//! catalog and hands it to the reader/build path.  The segment list is kept
//! serialized so this crate stays independent of the metadata layer and of
//! the concrete segment payload of each index kind; kind-specific code turns
//! it back into its own type with [`ResolvedIndex::segments_as`].

use lakesoul_common::IndexKind;
use serde::de::DeserializeOwned;

use crate::Result;

/// An index commit resolved by the caller from the metadata catalog.
#[derive(Debug, Clone)]
pub struct ResolvedIndex {
    pub kind: IndexKind,
    pub index_prefix: String,
    pub commit_id: i64,
    pub generation: u64,
    pub version: u64,
    /// Opaque, kind-specific header (e.g. the serialized vector index
    /// header).
    pub header: Vec<u8>,
    /// Kind-specific segment list, serialized as JSON.
    ///
    /// The catalog stores the same JSON in `index_commit.segments`.
    pub segments: serde_json::Value,
}

impl ResolvedIndex {
    /// Deserialize the segment list into the kind's segment entry type.
    pub fn segments_as<S: DeserializeOwned>(&self) -> Result<Vec<S>> {
        serde_json::from_value(self.segments.clone()).map_err(|error| {
            rootcause::report!(
                "invalid {} index segment list at '{}': {}",
                self.kind,
                self.index_prefix,
                error
            )
        })
    }

    /// Whether this commit belongs to the given index kind.
    pub fn is_kind(&self, kind: IndexKind) -> bool {
        self.kind == kind
    }
}
