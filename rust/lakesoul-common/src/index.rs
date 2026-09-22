// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Descriptor of the secondary index kinds supported by LakeSoul.
//!
//! The kind is the single source of truth for the places where an index
//! kind shows up outside of its own implementation: the catalog's `kind`
//! column, the `_<kind>_index` object-store directory, the `{prefix}_*`
//! reader options, and the table property key declaring index configs.

use std::collections::HashSet;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A secondary index kind attached to table columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexKind {
    /// IVF + RaBitQ vector index.
    Vector,
    /// Full-text (Tantivy) index.
    Text,
}

impl IndexKind {
    /// Every kind, for maintenance loops such as drop-table cleanup.
    pub const ALL: [IndexKind; 2] = [IndexKind::Vector, IndexKind::Text];

    /// Stable identifier stored in the catalog's `kind` column.
    pub const fn as_str(self) -> &'static str {
        match self {
            IndexKind::Vector => "vector",
            IndexKind::Text => "text",
        }
    }

    /// Directory, under the table directory, holding this kind's index data.
    pub const fn directory(self) -> &'static str {
        match self {
            IndexKind::Vector => "_vector_index",
            IndexKind::Text => "_text_index",
        }
    }

    /// Prefix of the reader options selecting this index
    /// (`{prefix}_column`, `{prefix}_query`, `{prefix}_top_k`, ...).
    pub const fn option_prefix(self) -> &'static str {
        match self {
            IndexKind::Vector => "vector_search",
            IndexKind::Text => "text_search",
        }
    }

    /// Table property key declaring this kind's index configurations (JSON).
    pub const fn property_key(self) -> &'static str {
        match self {
            IndexKind::Vector => "vector_index_columns",
            IndexKind::Text => "text_index_columns",
        }
    }
}

impl std::fmt::Display for IndexKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One artifact referenced by an index commit.
///
/// The metadata catalog is generic over this payload: the vector index
/// stores IVF segment entries, the text index stores split entries.  The
/// catalog only relies on the file name (dedup and GC retention) and a
/// stable sort key (deterministic commit content).
pub trait CatalogSegment:
    Serialize + DeserializeOwned + Clone + Send + Sync + 'static
{
    /// File name of the artifact, relative to the shard prefix.
    fn filename(&self) -> &str;

    /// Deterministic ordering key of the artifact inside a commit.
    fn sort_key(&self) -> (u64, u64);

    /// Data files whose rows this artifact indexed, when the build recorded
    /// them.
    ///
    /// Empty for artifacts written before coverage tracking; callers must
    /// treat that as "unknown" rather than "covers nothing".
    fn data_files(&self) -> &[String] {
        &[]
    }
}

/// Active files of one shard that the current index commit does not cover.
///
/// Files are compared by name because the metadata listing and the recorded
/// build inputs may spell the same object differently (scheme or prefix).
/// An empty result also means "unknown": commits written before coverage
/// tracking carry no file lists, and the caller cannot distinguish a fully
/// indexed shard from an untracked one, so it must not index or scan again.
pub fn pending_shard_files<S: CatalogSegment>(
    active_files: &[String],
    segments: &[S],
) -> Vec<String> {
    let covered: HashSet<&str> = segments
        .iter()
        .flat_map(|segment| segment.data_files().iter())
        .map(|file| file_name(file))
        .collect();
    if covered.is_empty() {
        return Vec::new();
    }
    active_files
        .iter()
        .filter(|file| !covered.contains(file_name(file)))
        .cloned()
        .collect()
}

/// Name of an object path (`s3://bucket/dir/file` → `file`).
fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FakeSegment {
        filename: String,
        #[serde(default)]
        data_files: Vec<String>,
    }

    impl CatalogSegment for FakeSegment {
        fn filename(&self) -> &str {
            &self.filename
        }

        fn sort_key(&self) -> (u64, u64) {
            (0, 0)
        }

        fn data_files(&self) -> &[String] {
            &self.data_files
        }
    }

    #[test]
    fn pending_shard_files_uses_recorded_coverage() {
        let active = vec![
            "file:///t/part-a_0000.parquet".to_string(),
            "file:///t/part-b_0000.parquet".to_string(),
            "s3://bucket/t/part-c_0000.parquet".to_string(),
        ];
        let segments = vec![FakeSegment {
            filename: "s.split".to_string(),
            // A different scheme must still match by file name.
            data_files: vec!["/data/part-a_0000.parquet".to_string()],
        }];
        let pending = pending_shard_files(&active, &segments);
        assert_eq!(
            pending,
            vec![
                "file:///t/part-b_0000.parquet".to_string(),
                "s3://bucket/t/part-c_0000.parquet".to_string(),
            ]
        );

        // No recorded coverage means "unknown", never "unindexed".
        let legacy = vec![FakeSegment {
            filename: "s.split".to_string(),
            data_files: Vec::new(),
        }];
        assert!(pending_shard_files(&active, &legacy).is_empty());

        // Full coverage leaves nothing pending.
        let full = vec![FakeSegment {
            filename: "s.split".to_string(),
            data_files: active.clone(),
        }];
        assert!(pending_shard_files(&active, &full).is_empty());
    }

    #[test]
    fn descriptors_match_the_vector_wire_format() {
        let vector = IndexKind::Vector;
        assert_eq!(vector.as_str(), "vector");
        assert_eq!(vector.directory(), "_vector_index");
        assert_eq!(vector.option_prefix(), "vector_search");
        assert_eq!(vector.property_key(), "vector_index_columns");
        assert_eq!(vector.to_string(), "vector");

        let text = IndexKind::Text;
        assert_eq!(text.as_str(), "text");
        assert_eq!(text.directory(), "_text_index");
        assert_eq!(text.option_prefix(), "text_search");
        assert_eq!(text.property_key(), "text_index_columns");
    }

    #[test]
    fn kinds_serialize_as_lowercase_names() {
        assert_eq!(
            serde_json::to_string(&IndexKind::Vector).unwrap(),
            "\"vector\""
        );
        assert_eq!(
            serde_json::from_str::<IndexKind>("\"text\"").unwrap(),
            IndexKind::Text
        );
    }
}
