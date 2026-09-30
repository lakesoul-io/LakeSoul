// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Opaque, variable-length external key carried by every indexed vector.
//!
//! The core never interprets the bytes.  Callers encode the table's
//! primary-key columns with the same `arrow-row` layout the row-level
//! primary-key locator uses for its per-file key indexes, so a key returned
//! by a search can be looked up there without decoding.

use smallvec::SmallVec;

/// Inline capacity: a single 8-byte integer field (null mask + payload) and
/// most short keys fit without a heap allocation.
const INLINE_KEY_BYTES: usize = 16;

/// An external vector key: an opaque, variable-length byte string.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Default, PartialOrd, Ord)]
pub struct IndexKey(SmallVec<[u8; INLINE_KEY_BYTES]>);

impl IndexKey {
    /// Wrap raw key bytes.
    pub fn new(bytes: impl AsRef<[u8]>) -> Self {
        Self(SmallVec::from_slice(bytes.as_ref()))
    }

    /// The raw key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Key length in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// True for an empty key.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Heap footprint of this key (0 when the bytes are stored inline).
    pub fn heap_bytes(&self) -> usize {
        if self.0.spilled() {
            self.0.capacity()
        } else {
            0
        }
    }
}

impl From<Vec<u8>> for IndexKey {
    fn from(bytes: Vec<u8>) -> Self {
        Self(SmallVec::from_vec(bytes))
    }
}

impl From<&[u8]> for IndexKey {
    fn from(bytes: &[u8]) -> Self {
        Self(SmallVec::from_slice(bytes))
    }
}

impl AsRef<[u8]> for IndexKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
