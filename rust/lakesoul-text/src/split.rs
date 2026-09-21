// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Text index splits: local build, bundle upload, cached open.
//!
//! A split is one self-contained Tantivy index built from a batch of rows.
//! It is stored as a single immutable object
//! `{index_prefix}/{split_id}.split` whose payload bundles every Tantivy
//! file plus a JSON footer with offsets and CRCs.  A single object per
//! split keeps the shared catalog/GC contract (one unique file name per
//! segment) and makes opening a split a single download.
//!
//! Object layout:
//!
//! ```text
//! {table dir}/_text_index/{column}/{partition_desc}/{bucket_id}/{split_id}.split
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use lakesoul_common::CatalogSegment;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};
use tantivy::schema::TantivyDocument;
use tantivy::{Index, IndexWriter, Term};
use tracing::{debug, info};

use crate::TextError;
use crate::config::TextIndexConfig;
use crate::error::Result;
use crate::schema::TextSchema;
use crate::tokenizer::register_tokenizers;

/// Format version of the split bundle.
pub const SPLIT_FORMAT_VERSION: u32 = 1;

/// Bundle file magic.
const BUNDLE_MAGIC: &[u8; 4] = b"LSTX";
/// Header length: magic (4) + format version (4).
const BUNDLE_HEADER_LEN: usize = 8;
/// Trailer holding the footer JSON length.
const BUNDLE_FOOTER_LEN_BYTES: usize = 4;

/// Default writer memory budget while building a split.
pub const DEFAULT_WRITER_MEMORY_BUDGET: usize = 128 * 1024 * 1024;

/// Environment variable overriding the local split cache directory.
pub const ENV_SPLIT_CACHE_DIR: &str = "LAKESOUL_TEXT_SPLIT_CACHE_DIR";

/// One text index split referenced by a catalog commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextSplitEntry {
    /// Unique id of the split.
    pub split_id: String,
    /// Object name of the split bundle, relative to the shard prefix.
    pub filename: String,
    /// Number of documents in the split.
    pub num_docs: u64,
    /// Size of the split bundle in bytes.
    pub file_size: u64,
    /// Bundle format version.
    pub format_version: u32,
}

impl CatalogSegment for TextSplitEntry {
    fn filename(&self) -> &str {
        &self.filename
    }

    fn sort_key(&self) -> (u64, u64) {
        // Splits have no natural numeric order; the stable sort keeps the
        // commit insertion order.
        (0, 0)
    }
}

/// Whether a shard's delta history outweighs its compacted base.
///
/// A rebuild publishes a single split (the generation's base); every delta
/// build appends one split.  Superseded and deleted rows stay in the base
/// and earlier delta splits until the next rebuild, so the accumulated delta
/// documents are the (upper-bounded) stale part.  Comparing them to the base
/// amortises the rebuild cost: writing about as many documents as the base
/// holds triggers the next compaction.
///
/// `splits` must be in commit order (as stored by the catalog), so the first
/// entry is the base of the current generation.
pub fn drift_exceeds_threshold(splits: &[TextSplitEntry], max_delta_ratio: f32) -> bool {
    let Some(base) = splits.first().map(|split| split.num_docs) else {
        return false;
    };
    let total: u64 = splits.iter().map(|split| split.num_docs).sum();
    let delta = total.saturating_sub(base);
    base > 0 && delta as f32 / base as f32 > max_delta_ratio
}

/// One file inside a split bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundleFile {
    name: String,
    offset: u64,
    len: u64,
    crc32: u32,
}

/// Footer of a split bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundleFooter {
    format_version: u32,
    num_docs: u64,
    files: Vec<BundleFile>,
}

/// Build one split from `(primary key, text)` rows and upload its bundle.
///
/// The Tantivy index is built in a local temporary directory, force-merged
/// into a single segment (so opening a split only touches one segment), then
/// bundled and uploaded.  Returns the catalog entry of the new split.
pub async fn write_split(
    store: &Arc<dyn ObjectStore>,
    index_prefix: &str,
    config: &TextIndexConfig,
    docs: &[(u64, String)],
    memory_budget: usize,
) -> Result<TextSplitEntry> {
    let split_id = uuid::Uuid::new_v4().simple().to_string();
    let filename = format!("{split_id}.split");
    info!(
        index_prefix,
        split_id,
        docs = docs.len(),
        "building text index split"
    );

    let temp_dir = tempfile::tempdir()?;
    let text_schema = TextSchema::build(config);
    let index = Index::create_in_dir(temp_dir.path(), text_schema.schema.clone())?;
    register_tokenizers(&index);

    let budget = memory_budget.max(15_000_000);
    // A single indexing thread produces a single segment per commit for
    // batches that fit the memory budget.
    let mut writer: IndexWriter = index.writer_with_num_threads(1, budget)?;
    for (id, text) in docs {
        // Upsert semantics: the last document of a primary key wins.  A
        // rebuilt shard reads every active data file, so an updated row
        // appears once per version; without this the split would index all
        // of them and only the exact verification pass would drop the stale
        // copies.
        writer.delete_term(Term::from_field_u64(text_schema.pk_field, *id));
        let mut document = TantivyDocument::new();
        document.add_u64(text_schema.pk_field, *id);
        document.add_text(text_schema.text_field, text.as_str());
        writer.add_document(document)?;
    }
    writer.commit()?;
    // Large batches may flush more than one segment; merge them into one so
    // an opened split has a single segment and no merge-time overhead.
    let segment_ids = index.searchable_segment_ids()?;
    if segment_ids.len() > 1 {
        writer.merge(&segment_ids).wait()?;
        writer.commit()?;
    }
    writer.wait_merging_threads()?;
    // Live documents only: `delete_term` tombstones the superseded copies.
    let num_docs = index.reader()?.searcher().num_docs();
    drop(index);

    let files = collect_index_files(temp_dir.path())?;
    if files.is_empty() {
        return Err(TextError::Invalid(
            "text index split produced no files".to_string(),
        ));
    }
    let bundle = build_bundle(&files, num_docs)?;
    let file_size = bundle.len() as u64;
    let object_path = ObjectPath::from(format!(
        "{}/{}",
        index_prefix.trim_end_matches('/'),
        filename
    ));
    store.put(&object_path, bundle.into()).await?;
    debug!(
        index_prefix,
        filename,
        bytes = file_size,
        files = files.len(),
        "uploaded text index split"
    );

    Ok(TextSplitEntry {
        split_id,
        filename,
        num_docs,
        file_size,
        format_version: SPLIT_FORMAT_VERSION,
    })
}

/// Collect the data files of a Tantivy index directory.
///
/// Lock/managed files (`*.lock`, dot files) are skipped; `meta.json` and
/// every segment file are kept.
fn collect_index_files(dir: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.ends_with(".lock") {
            continue;
        }
        files.push((name, std::fs::read(entry.path())?));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(files)
}

/// Bundle Tantivy files into one payload with a JSON footer.
fn build_bundle(files: &[(String, Vec<u8>)], num_docs: u64) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(BUNDLE_MAGIC);
    out.extend_from_slice(&SPLIT_FORMAT_VERSION.to_le_bytes());

    let mut footer_files = Vec::with_capacity(files.len());
    for (name, bytes) in files {
        let offset = out.len() as u64;
        out.extend_from_slice(bytes);
        footer_files.push(BundleFile {
            name: name.clone(),
            offset,
            len: bytes.len() as u64,
            crc32: crc32fast::hash(bytes),
        });
    }
    let footer = BundleFooter {
        format_version: SPLIT_FORMAT_VERSION,
        num_docs,
        files: footer_files,
    };
    let footer_bytes = serde_json::to_vec(&footer)?;
    out.extend_from_slice(&footer_bytes);
    out.extend_from_slice(&(footer_bytes.len() as u32).to_le_bytes());
    Ok(out)
}

/// Unpack a split bundle into `dest`, verifying magic and CRCs.
fn unpack_bundle(bytes: &[u8], dest: &Path) -> Result<BundleFooter> {
    if bytes.len() < BUNDLE_HEADER_LEN + BUNDLE_FOOTER_LEN_BYTES {
        return Err(TextError::Invalid("text index split too small".to_string()));
    }
    if &bytes[..4] != BUNDLE_MAGIC {
        return Err(TextError::Invalid(
            "not a LakeSoul text index split (bad magic)".to_string(),
        ));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != SPLIT_FORMAT_VERSION {
        return Err(TextError::Invalid(format!(
            "unsupported text index split format version {version}"
        )));
    }

    let footer_len_offset = bytes.len() - BUNDLE_FOOTER_LEN_BYTES;
    let footer_len =
        u32::from_le_bytes(bytes[footer_len_offset..].try_into().unwrap()) as usize;
    if footer_len == 0 || footer_len > footer_len_offset {
        return Err(TextError::Invalid(
            "corrupted text index split footer".to_string(),
        ));
    }
    let footer_start = footer_len_offset - footer_len;
    let footer: BundleFooter =
        serde_json::from_slice(&bytes[footer_start..footer_len_offset])?;
    if footer.format_version != SPLIT_FORMAT_VERSION {
        return Err(TextError::Invalid(format!(
            "unsupported split footer version {}",
            footer.format_version
        )));
    }

    std::fs::create_dir_all(dest)?;
    for file in &footer.files {
        let start = file.offset as usize;
        let end = start + file.len as usize;
        if end > footer_start {
            return Err(TextError::Invalid(format!(
                "corrupted text index split entry '{}'",
                file.name
            )));
        }
        let data = &bytes[start..end];
        if crc32fast::hash(data) != file.crc32 {
            return Err(TextError::Invalid(format!(
                "text index split checksum mismatch for '{}'",
                file.name
            )));
        }
        std::fs::write(dest.join(&file.name), data)?;
    }
    Ok(footer)
}

/// Local disk cache that materializes splits and opens them read-only.
///
/// Splits are immutable, so a materialized directory is reused until the
/// process removes it; the cache root defaults to
/// `$LAKESOUL_TEXT_SPLIT_CACHE_DIR` or the system temp directory.
pub struct SplitCache {
    root: PathBuf,
}

impl SplitCache {
    /// Create a cache rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Create a cache rooted at the configured/default directory.
    pub fn from_env() -> Self {
        let root = std::env::var(ENV_SPLIT_CACHE_DIR)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("lakesoul_text_split_cache"));
        Self::new(root)
    }

    /// The cache root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Local directory a split materializes into.
    pub fn split_dir(&self, index_prefix: &str, entry: &TextSplitEntry) -> PathBuf {
        let mut name = String::with_capacity(index_prefix.len() + 3);
        for byte in index_prefix.bytes() {
            if byte.is_ascii_alphanumeric() {
                name.push(byte as char);
            } else {
                name.push('_');
            }
        }
        self.root.join(name).join(&entry.split_id)
    }

    /// Download (if needed) and open a split.
    ///
    /// The returned [`Index`] holds memory maps into the cache directory, so
    /// the directory must not be removed while the index is in use.
    pub async fn open(
        &self,
        store: &Arc<dyn ObjectStore>,
        index_prefix: &str,
        entry: &TextSplitEntry,
    ) -> Result<Index> {
        let dir = self.split_dir(index_prefix, entry);
        if !dir.join("meta.json").exists() {
            self.materialize(store, index_prefix, entry, &dir).await?;
        }
        let index = Index::open_in_dir(&dir)?;
        register_tokenizers(&index);
        Ok(index)
    }

    async fn materialize(
        &self,
        store: &Arc<dyn ObjectStore>,
        index_prefix: &str,
        entry: &TextSplitEntry,
        dir: &Path,
    ) -> Result<()> {
        let object_path = ObjectPath::from(format!(
            "{}/{}",
            index_prefix.trim_end_matches('/'),
            entry.filename
        ));
        let bytes = store.get(&object_path).await?.bytes().await?;
        if bytes.len() as u64 != entry.file_size {
            return Err(TextError::Invalid(format!(
                "text index split '{}' size mismatch: expected {}, got {}",
                entry.filename,
                entry.file_size,
                bytes.len()
            )));
        }

        std::fs::create_dir_all(&self.root)?;
        let staging = tempfile::Builder::new()
            .prefix("split-")
            .tempdir_in(&self.root)?;
        unpack_bundle(&bytes, staging.path())?;

        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        let staged = staging.into_path();
        if let Err(error) = std::fs::rename(&staged, dir) {
            // Cross-device fallback (cache root changed between calls).
            copy_dir_all(&staged, dir)?;
            let _ = std::fs::remove_dir_all(&staged);
            debug!(?error, "split cache rename fell back to copy");
        }
        Ok(())
    }
}

fn copy_dir_all(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_all(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files() -> Vec<(String, Vec<u8>)> {
        vec![
            ("meta.json".to_string(), b"{\"segments\":[]}".to_vec()),
            ("seg.term".to_string(), vec![1, 2, 3, 4, 5]),
        ]
    }

    #[test]
    fn bundle_roundtrips_files() {
        let bundle = build_bundle(&files(), 7).unwrap();
        let dest = tempfile::tempdir().unwrap();
        let footer = unpack_bundle(&bundle, dest.path()).unwrap();
        assert_eq!(footer.num_docs, 7);
        assert_eq!(footer.files.len(), 2);
        assert_eq!(
            std::fs::read(dest.path().join("meta.json")).unwrap(),
            files()[0].1
        );
        assert_eq!(
            std::fs::read(dest.path().join("seg.term")).unwrap(),
            files()[1].1
        );
    }

    #[test]
    fn corrupted_bundle_is_rejected() {
        let mut bundle = build_bundle(&files(), 1).unwrap();
        // Flip a byte inside the first file payload (after the header).
        bundle[BUNDLE_HEADER_LEN] ^= 0xff;
        let dest = tempfile::tempdir().unwrap();
        let error = unpack_bundle(&bundle, dest.path()).unwrap_err();
        assert!(error.to_string().contains("checksum"), "{error}");
    }

    #[test]
    fn bad_magic_is_rejected() {
        let mut bundle = build_bundle(&files(), 1).unwrap();
        bundle[0] = b'X';
        let dest = tempfile::tempdir().unwrap();
        let error = unpack_bundle(&bundle, dest.path()).unwrap_err();
        assert!(error.to_string().contains("magic"), "{error}");
    }

    #[test]
    fn split_entry_uses_the_bundle_name() {
        let entry = TextSplitEntry {
            split_id: "abc".to_string(),
            filename: "abc.split".to_string(),
            num_docs: 1,
            file_size: 2,
            format_version: SPLIT_FORMAT_VERSION,
        };
        assert_eq!(entry.filename(), "abc.split");
        assert_eq!(entry.sort_key(), (0, 0));
    }
}
