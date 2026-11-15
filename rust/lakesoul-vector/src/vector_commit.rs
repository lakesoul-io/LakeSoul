// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Durable catalog publication for vector index updates.
//!
//! A vector commit may carry a mutation sidecar: the keys inserted by a
//! delta build (upsert) or deleted by a delete-only commit.  Logical
//! validity is decided by the catalog revision of a segment
//! (`birth_version`) against the newest mutation revision of its key, so
//! this module owns the revision protocol shared by the DataFusion write
//! path and the Python bindings:
//!
//! 1. resolve the shard's current commit;
//! 2. stamp every new segment's `birth_version` with the revision it will
//!    be published at;
//! 3. write the immutable mutation object (delta, or a merged checkpoint
//!    once the chain grows too long);
//! 4. CAS-publish segments and mutation head in one catalog transaction.
//!
//! A conflict rewrites only the small sidecar object and retries; segment
//! files are never rewritten.

use std::sync::Arc;

use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::index_catalog::{
    CommitMode, IndexCommitView, VectorCatalog, VectorSegmentEntry,
};
use object_store::ObjectStore;
use rootcause::{Report, report};
use tracing::{info, warn};
use uuid::Uuid;

use crate::IndexKey;
use crate::rabitq::mutation::{
    MutationBatch, MutationCheckpoint, checkpoint_filename, delta_filename, load_state,
    write_checkpoint, write_delta,
};
use crate::rabitq::segment::IndexStore;

/// Mutation deltas are merged into a checkpoint once the chain reaches
/// this many records.  A checkpoint bounds loading cost without shrinking
/// the key set, so a long-lived index still needs a rebuild eventually.
pub const MAX_MUTATION_RECORDS: u32 = 32;

/// How often a publication is retried after losing a CAS race.  A retry
/// only rewrites the mutation sidecar, never a segment file.
const COMMIT_ATTEMPTS: usize = 3;

type Result<T> = std::result::Result<T, Report>;

/// The catalog commit a publication produced.
#[derive(Debug, Clone)]
pub struct VectorCommitOutcome {
    pub commit_id: i64,
    pub generation: u64,
    pub version: u64,
    pub mutation_head: Option<String>,
    pub mutation_records: u32,
}

impl From<IndexCommitView<VectorSegmentEntry>> for VectorCommitOutcome {
    fn from(view: IndexCommitView<VectorSegmentEntry>) -> Self {
        Self {
            commit_id: view.commit_id,
            generation: view.generation,
            version: view.version,
            mutation_head: view.mutation_head,
            mutation_records: view.mutation_records,
        }
    }
}

/// Publish a built shard.
///
/// `segments` are the files a build just wrote (`birth_version` is a
/// placeholder).  `upserted_keys` are the keys a delta build inserted: they
/// are recorded in the mutation sidecar so older copies of the same key are
/// masked on the next load.  Rebuilds start a new generation and drop the
/// mutation chain, so their key list is ignored.
///
/// Returns `Ok(None)` when there is nothing to publish (no segments).
pub async fn commit_vector_build(
    store: &Arc<dyn ObjectStore>,
    catalog: &VectorCatalog,
    index_prefix: &str,
    header: &[u8],
    mut segments: Vec<VectorSegmentEntry>,
    upserted_keys: Vec<IndexKey>,
    mode: CommitMode,
) -> Result<Option<VectorCommitOutcome>> {
    if segments.is_empty() {
        return Ok(None);
    }
    if catalog.is_dropped(index_prefix).await? {
        warn!(
            prefix = index_prefix,
            "vector index shard was dropped while a build was running; \
             discarding the build result instead of recreating it"
        );
        return Ok(None);
    }
    match mode {
        CommitMode::Rebuild => {
            commit_rebuild(catalog, index_prefix, header, segments).await
        }
        CommitMode::Delta => {
            for _ in 0..COMMIT_ATTEMPTS {
                let Some(current) = resolve(catalog, index_prefix).await? else {
                    // The base commit disappeared while building; publish
                    // the freshly built segments as a new generation.
                    return commit_rebuild(catalog, index_prefix, header, segments).await;
                };
                let revision = current.version + 1;
                for segment in &mut segments {
                    segment.birth_version = revision;
                }
                let (head, records) = if upserted_keys.is_empty() {
                    (current.mutation_head.clone(), current.mutation_records)
                } else {
                    write_mutation(
                        store,
                        index_prefix,
                        &current,
                        revision,
                        &upserted_keys,
                    )
                    .await?
                };
                match catalog
                    .commit_with_mutations(
                        index_prefix,
                        header,
                        &segments,
                        CommitMode::Delta,
                        (current.generation, current.version),
                        head,
                        records,
                    )
                    .await
                {
                    Ok(view) => return Ok(Some(view.into())),
                    Err(error) if is_conflict(&error) => {
                        warn!(
                            prefix = index_prefix,
                            "vector delta commit lost a race; rewriting the \
                             mutation sidecar and retrying"
                        );
                    }
                    Err(error) if is_dropped(&error) => {
                        warn!(
                            prefix = index_prefix,
                            "vector index shard was dropped during the build; \
                             discarding the build result"
                        );
                        return Ok(None);
                    }
                    Err(error) => {
                        return Err(report!(
                            "failed to commit vector index at \
                             '{index_prefix}': {error}"
                        ));
                    }
                }
            }
            Err(report!(
                "vector index commit at '{index_prefix}' kept conflicting \
                 with concurrent commits"
            ))
        }
    }
}

/// Publish a rebuilt shard as a new generation (mutation chain cleared).
async fn commit_rebuild(
    catalog: &VectorCatalog,
    index_prefix: &str,
    header: &[u8],
    mut segments: Vec<VectorSegmentEntry>,
) -> Result<Option<VectorCommitOutcome>> {
    for segment in &mut segments {
        segment.birth_version = 1;
    }
    for _ in 0..COMMIT_ATTEMPTS {
        let current = resolve(catalog, index_prefix).await?;
        let expected = current
            .as_ref()
            .map(|view| (view.generation, view.version))
            .unwrap_or((0, 0));
        match catalog
            .commit_with_mutations(
                index_prefix,
                header,
                &segments,
                CommitMode::Rebuild,
                expected,
                None,
                0,
            )
            .await
        {
            Ok(view) => return Ok(Some(view.into())),
            Err(error) if is_conflict(&error) => {
                warn!(
                    prefix = index_prefix,
                    "vector rebuild commit lost a race; retrying"
                );
            }
            Err(error) if is_dropped(&error) => {
                warn!(
                    prefix = index_prefix,
                    "vector index shard was dropped during the rebuild; \
                     discarding the rebuild result"
                );
                return Ok(None);
            }
            Err(error) => {
                return Err(report!(
                    "failed to commit vector index rebuild at \
                     '{index_prefix}': {error}"
                ));
            }
        }
    }
    Err(report!(
        "vector index rebuild at '{index_prefix}' kept conflicting with \
         concurrent commits"
    ))
}

/// Durably delete `keys` from a shard without rewriting any segment.
///
/// The delete is a catalog commit with an empty segment list: the next
/// search masks every physical copy of the keys.  Deleting a key that is
/// not present is not an error.  Returns `Ok(None)` when the shard has no
/// commit, or when `keys` is empty.
pub async fn delete_vector_keys(
    store: &Arc<dyn ObjectStore>,
    catalog: &VectorCatalog,
    index_prefix: &str,
    keys: &[IndexKey],
) -> Result<Option<VectorCommitOutcome>> {
    if keys.is_empty() {
        return Ok(None);
    }
    let no_segments: Vec<VectorSegmentEntry> = Vec::new();
    for _ in 0..COMMIT_ATTEMPTS {
        let Some(current) = resolve(catalog, index_prefix).await? else {
            warn!(
                prefix = index_prefix,
                "delete requested for a shard without an index; no-op"
            );
            return Ok(None);
        };
        let revision = current.version + 1;
        let (head, records) =
            write_mutation(store, index_prefix, &current, revision, keys).await?;
        match catalog
            .commit_with_mutations(
                index_prefix,
                &current.header,
                &no_segments,
                CommitMode::Delta,
                (current.generation, current.version),
                head,
                records,
            )
            .await
        {
            Ok(view) => return Ok(Some(view.into())),
            Err(error) if is_conflict(&error) => {
                warn!(
                    prefix = index_prefix,
                    "vector delete commit lost a race; rewriting the mutation \
                     sidecar and retrying"
                );
            }
            Err(error) => {
                return Err(report!(
                    "failed to commit vector index delete at \
                     '{index_prefix}': {error}"
                ));
            }
        }
    }
    Err(report!(
        "vector index delete at '{index_prefix}' kept conflicting with \
         concurrent commits"
    ))
}

async fn resolve(
    catalog: &VectorCatalog,
    index_prefix: &str,
) -> Result<Option<IndexCommitView<VectorSegmentEntry>>> {
    catalog.resolve(index_prefix).await.map_err(|error| {
        report!("failed to resolve vector index at '{index_prefix}': {error}")
    })
}

fn is_conflict(error: &LakeSoulMetaDataError) -> bool {
    matches!(error, LakeSoulMetaDataError::IndexVersionConflict { .. })
}

/// A concurrent `drop_vector_index` won the race.  Index maintenance is
/// best-effort and must never fail the data commit that triggered it.
fn is_dropped(error: &LakeSoulMetaDataError) -> bool {
    matches!(error, LakeSoulMetaDataError::IndexDropped { .. })
}

/// Write the object that records `keys` at `revision`.
///
/// Returns the new mutation head and the chain length it represents.
async fn write_mutation(
    store: &Arc<dyn ObjectStore>,
    index_prefix: &str,
    current: &IndexCommitView<VectorSegmentEntry>,
    revision: u64,
    keys: &[IndexKey],
) -> Result<(Option<String>, u32)> {
    let istore = IndexStore::new(
        store.clone(),
        index_prefix.trim_end_matches('/').to_string(),
    );
    let txid = Uuid::new_v4().simple().to_string();
    let merge_into_checkpoint = current.mutation_head.is_some()
        && current.mutation_records >= MAX_MUTATION_RECORDS;
    if merge_into_checkpoint {
        let head = current.mutation_head.as_deref().unwrap_or_default();
        let mut state = load_state(
            &istore,
            Some(head),
            current.mutation_records,
            current.version,
        )
        .await
        .map_err(|error| {
            report!(
                "failed to load the vector mutation chain at \
                         '{index_prefix}': {error}"
            )
        })?;
        state.apply_delta(revision, keys);
        let checkpoint = MutationCheckpoint::new(revision, state.entries().to_vec());
        let key = checkpoint_filename(current.generation, &txid);
        write_checkpoint(&istore, &key, &checkpoint)
            .await
            .map_err(|error| {
                report!(
                    "failed to write the vector mutation checkpoint at \
                     '{index_prefix}': {error}"
                )
            })?;
        info!(
            prefix = index_prefix,
            revision,
            entries = checkpoint.entries.len(),
            "wrote a merged vector mutation checkpoint"
        );
        Ok((Some(key), 1))
    } else {
        let batch =
            MutationBatch::new(revision, current.mutation_head.clone(), keys.to_vec());
        let key = delta_filename(current.generation, &txid);
        write_delta(&istore, &key, &batch).await.map_err(|error| {
            report!(
                "failed to write the vector mutation delta at \
                     '{index_prefix}': {error}"
            )
        })?;
        Ok((Some(key), current.mutation_records.saturating_add(1)))
    }
}
