// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shard text index builder.
//!
//! Reads a shard's data files through the shared framework reader and writes
//! one Tantivy split per build.  Splits are immutable and additive: a
//! rebuild from the shard's full file list produces the replacement
//! generation, an incremental build appends a split for the newly written
//! files.

use std::collections::HashMap;
use std::sync::Arc;

use lakesoul_common::IndexKind;
use lakesoul_text::{
    DEFAULT_WRITER_MEMORY_BUDGET, TextIndexConfig, TextSplitEntry, write_split,
};
use object_store::ObjectStore;
use rootcause::report;
use tracing::info;

use crate::Result;
use crate::index::commit::ResolvedIndex;
use crate::index::prefix::shard_index_prefix;
use crate::index::reader::read_shard_batches;
use crate::text::reader::collect_text_documents;

/// Splits produced by a build, for the caller to commit.
#[derive(Debug, Clone)]
pub struct TextBuildOutcome {
    pub index_prefix: String,
    /// Serialized [`TextIndexConfig`], stored as the catalog header.
    pub header: Vec<u8>,
    /// New splits only: the delta split of an incremental build, or the
    /// replacement splits of a rebuild.
    pub new_splits: Vec<TextSplitEntry>,
}

impl TextBuildOutcome {
    /// Serialize the new splits into the catalog segment payload.
    pub fn segments_json(&self) -> Result<serde_json::Value> {
        serde_json::to_value(&self.new_splits)
            .map_err(|error| report!("failed to serialize text index splits: {}", error))
    }
}

/// Builds text index splits from a shard's data files.
pub struct TextShardIndexBuilder {
    store: Arc<dyn ObjectStore>,
    config: TextIndexConfig,
    file_paths: Vec<String>,
    pk_column: String,
    object_store_options: HashMap<String, String>,
    default_fs: Option<String>,
    base: Option<ResolvedIndex>,
}

impl TextShardIndexBuilder {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: TextIndexConfig,
        file_paths: Vec<String>,
        pk_column: String,
        object_store_options: HashMap<String, String>,
        default_fs: Option<String>,
    ) -> Self {
        Self {
            store,
            config,
            file_paths,
            pk_column,
            object_store_options,
            default_fs,
            base: None,
        }
    }

    /// Run an incremental build on top of the resolved base commit.
    pub fn with_base(mut self, base: ResolvedIndex) -> Self {
        self.base = Some(base);
        self
    }

    /// Index prefix of this shard.
    pub fn index_prefix(&self) -> String {
        self.base
            .as_ref()
            .map(|base| base.index_prefix.clone())
            .unwrap_or_else(|| {
                shard_index_prefix(
                    &self.file_paths,
                    IndexKind::Text,
                    &self.config.column_name,
                )
            })
    }

    /// Read the shard files and build one split from the current rows.
    ///
    /// The caller decides the file set: all active files for a fresh build
    /// or a rebuild, only the newly written files for an incremental build.
    pub async fn build(self) -> Result<TextBuildOutcome> {
        let index_prefix = self.index_prefix();
        let header = serde_json::to_vec(&self.config).map_err(|error| {
            report!("failed to serialize text index config: {}", error)
        })?;

        info!(
            index_prefix,
            column = %self.config.column_name,
            files = self.file_paths.len(),
            "building text index split"
        );
        let documents = self.read_documents().await?;
        if documents.is_empty() {
            return Ok(TextBuildOutcome {
                index_prefix,
                header,
                new_splits: Vec::new(),
            });
        }

        let mut split = write_split(
            &self.store,
            &index_prefix,
            &self.config,
            &documents,
            DEFAULT_WRITER_MEMORY_BUDGET,
        )
        .await
        .map_err(|error| {
            report!("failed to write text index split at '{index_prefix}': {error}")
        })?;
        // Coverage bookkeeping: the split indexes exactly these data files.
        split.data_files = self.file_paths.clone();
        info!(
            index_prefix,
            docs = split.num_docs,
            bytes = split.file_size,
            "text index split built"
        );
        Ok(TextBuildOutcome {
            index_prefix,
            header,
            new_splits: vec![split],
        })
    }

    async fn read_documents(&self) -> Result<Vec<(u64, String)>> {
        let batches = read_shard_batches(
            &self.file_paths,
            &self.pk_column,
            std::slice::from_ref(&self.config.column_name),
            &self.object_store_options,
            self.default_fs.as_deref(),
        )
        .await?;

        let mut documents = Vec::new();
        for batch in &batches {
            documents.extend(collect_text_documents(
                batch,
                &self.pk_column,
                &self.config.column_name,
            )?);
        }
        Ok(documents)
    }
}
