//! Compact delete/upsert mutation records keyed by the opaque [`IndexKey`].
//!
//! A delta stores a sorted set of keys plus one global revision; a checkpoint
//! stores the merged `(key, revision)` table and bounds the parent chain
//! length.  Mutation objects contain no cluster/lane information, so a
//! durable delete never has to read or rewrite index segments.

use std::collections::{BTreeMap, HashSet};
use std::io::{Cursor, Read, Write};

use crc32fast::Hasher;
use object_store::ObjectStoreExt;

use crate::rabitq::RabitqError;
use crate::rabitq::key::IndexKey;
use crate::rabitq::segment::IndexStore;

/// Directory under the index prefix that holds mutation sidecars.
pub const MUTATIONS_PREFIX: &str = "_mutations";

const DELTA_MAGIC: [u8; 4] = *b"LSMD";
const CHECKPOINT_MAGIC: [u8; 4] = *b"LSMC";
const MUTATION_VERSION: u32 = 1;

/// One batch of deleted/upserted keys. `parent` links to an older mutable
/// record, forming an immutable chain.
#[derive(Debug, Clone)]
pub struct MutationBatch {
    pub revision: u64,
    pub parent: Option<String>,
    pub keys: Vec<IndexKey>,
}

impl MutationBatch {
    pub fn new(revision: u64, parent: Option<String>, mut keys: Vec<IndexKey>) -> Self {
        keys.sort_unstable();
        keys.dedup();
        Self {
            revision,
            parent,
            keys,
        }
    }
}

/// Merged mutation checkpoint: the newest revision of every mutated key.
#[derive(Debug, Clone)]
pub struct MutationCheckpoint {
    pub revision: u64,
    pub entries: Vec<(IndexKey, u64)>,
}

impl MutationCheckpoint {
    pub fn new(revision: u64, mut entries: Vec<(IndexKey, u64)>) -> Self {
        // Highest revision first within each key, then keep the first entry.
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
        entries.dedup_by(|a, b| a.0 == b.0);
        Self { revision, entries }
    }
}

/// Loaded mutation state: sorted `(key, latest revision)` entries.
#[derive(Debug, Clone, Default)]
pub struct MutationState {
    entries: Vec<(IndexKey, u64)>,
    revision: u64,
    records: u32,
}

impl MutationState {
    /// Create a state from possibly unsorted entries; duplicate keys keep the
    /// maximum revision.
    pub fn new(revision: u64, records: u32, mut entries: Vec<(IndexKey, u64)>) -> Self {
        // Highest revision first within each key, then keep the first entry.
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
        entries.dedup_by(|a, b| a.0 == b.0);
        Self {
            entries,
            revision,
            records,
        }
    }

    /// Revision the state was published at.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Number of mutation objects in the loaded chain.
    pub fn records(&self) -> u32 {
        self.records
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of distinct mutated keys.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Latest delete/upsert revision of `key`, if any.
    pub fn latest_revision(&self, key: &IndexKey) -> Option<u64> {
        self.entries
            .binary_search_by(|(entry_key, _)| entry_key.cmp(key))
            .ok()
            .map(|index| self.entries[index].1)
    }

    /// A physical record is live when it was born at or after the key's most
    /// recent mutation revision.
    pub fn is_valid(&self, key: &IndexKey, birth_revision: u64) -> bool {
        self.latest_revision(key)
            .is_none_or(|revision| birth_revision >= revision)
    }

    pub fn entries(&self) -> &[(IndexKey, u64)] {
        &self.entries
    }

    /// Merge a delta into this state, replacing older revisions per key.
    pub fn apply_delta(&mut self, revision: u64, keys: &[IndexKey]) {
        for key in keys {
            match self
                .entries
                .binary_search_by(|(entry_key, _)| entry_key.cmp(key))
            {
                Ok(index) => self.entries[index].1 = revision,
                Err(index) => self.entries.insert(index, (key.clone(), revision)),
            }
        }
        self.revision = self.revision.max(revision);
    }
}

/// Relative object key for one mutation delta.
pub fn delta_filename(generation: u64, txid: &str) -> String {
    format!("{MUTATIONS_PREFIX}/g{generation:08}/{txid}.delta")
}

/// Relative object key for one mutation checkpoint.
pub fn checkpoint_filename(generation: u64, txid: &str) -> String {
    format!("{MUTATIONS_PREFIX}/g{generation:08}/{txid}.checkpoint")
}

/// Serialize and write an immutable delta.
pub async fn write_delta(
    istore: &IndexStore,
    key: &str,
    batch: &MutationBatch,
) -> Result<(), RabitqError> {
    let mut body = Vec::new();
    body.write_all(&DELTA_MAGIC)?;
    body.write_all(&MUTATION_VERSION.to_le_bytes())?;
    body.write_all(&batch.revision.to_le_bytes())?;
    let parent = batch.parent.as_deref().unwrap_or("");
    body.write_all(&(parent.len() as u32).to_le_bytes())?;
    body.write_all(parent.as_bytes())?;
    body.write_all(&(batch.keys.len() as u64).to_le_bytes())?;
    for key in &batch.keys {
        write_key(&mut body, key)?;
    }
    let crc = checksum(&body);
    body.write_all(&crc.to_le_bytes())?;
    istore
        .store
        .put(&istore.full_path(key), body.into())
        .await
        .map_err(RabitqError::from)?;
    Ok(())
}

/// Serialize and write a merged mutation checkpoint.
pub async fn write_checkpoint(
    istore: &IndexStore,
    key: &str,
    checkpoint: &MutationCheckpoint,
) -> Result<(), RabitqError> {
    let mut body = Vec::new();
    body.write_all(&CHECKPOINT_MAGIC)?;
    body.write_all(&MUTATION_VERSION.to_le_bytes())?;
    body.write_all(&checkpoint.revision.to_le_bytes())?;
    body.write_all(&(checkpoint.entries.len() as u64).to_le_bytes())?;
    for (key, revision) in &checkpoint.entries {
        write_key(&mut body, key)?;
        write_varint(&mut body, *revision);
    }
    let crc = checksum(&body);
    body.write_all(&crc.to_le_bytes())?;
    istore
        .store
        .put(&istore.full_path(key), body.into())
        .await
        .map_err(RabitqError::from)?;
    Ok(())
}

/// Read and verify one mutation record.
async fn read_record(
    istore: &IndexStore,
    key: &str,
) -> Result<MutationRecord, RabitqError> {
    let bytes = istore
        .store
        .get(&istore.full_path(key))
        .await
        .map_err(RabitqError::from)?
        .bytes()
        .await
        .map_err(|error| RabitqError::Io(error.to_string()))?;
    if bytes.len() < 8 {
        return Err(RabitqError::InvalidPersistence("mutation record too short"));
    }
    let payload = &bytes[..bytes.len() - 4];
    let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
    if checksum(payload) != stored {
        return Err(RabitqError::InvalidPersistence(
            "mutation checksum mismatch",
        ));
    }
    let mut reader = Cursor::new(payload);
    let mut magic = [0u8; 4];
    reader.read_exact(&mut magic)?;
    let version = read_u32(&mut reader)?;
    if version != MUTATION_VERSION {
        return Err(RabitqError::InvalidPersistence(
            "unsupported mutation version",
        ));
    }
    match magic {
        DELTA_MAGIC => decode_delta(&mut reader),
        CHECKPOINT_MAGIC => decode_checkpoint(&mut reader),
        _ => Err(RabitqError::InvalidPersistence(
            "unknown mutation record magic",
        )),
    }
}

#[derive(Debug, Clone)]
enum MutationRecord {
    Delta(MutationBatch),
    Checkpoint(MutationCheckpoint),
}

fn decode_delta(reader: &mut Cursor<&[u8]>) -> Result<MutationRecord, RabitqError> {
    let revision = read_u64(reader)?;
    let parent_len = read_u32(reader)? as usize;
    let mut parent = vec![0u8; parent_len];
    reader.read_exact(&mut parent)?;
    let parent =
        if parent.is_empty() {
            None
        } else {
            Some(String::from_utf8(parent).map_err(|_| {
                RabitqError::InvalidPersistence("invalid mutation parent")
            })?)
        };
    let count = usize::try_from(read_u64(reader)?)
        .map_err(|_| RabitqError::InvalidPersistence("mutation key count too large"))?;
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        keys.push(read_key(reader)?);
    }
    Ok(MutationRecord::Delta(MutationBatch {
        revision,
        parent,
        keys,
    }))
}

fn decode_checkpoint(reader: &mut Cursor<&[u8]>) -> Result<MutationRecord, RabitqError> {
    let revision = read_u64(reader)?;
    let count = usize::try_from(read_u64(reader)?).map_err(|_| {
        RabitqError::InvalidPersistence("checkpoint entry count too large")
    })?;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key = read_key(reader)?;
        let revision = read_varint(reader)?;
        entries.push((key, revision));
    }
    Ok(MutationRecord::Checkpoint(MutationCheckpoint {
        revision,
        entries,
    }))
}

/// Load and merge a mutation chain. `records` bounds the chain length, and
/// `revision` is the catalog revision of the resolved view.
pub async fn load_state(
    istore: &IndexStore,
    head: Option<&str>,
    records: u32,
    revision: u64,
) -> Result<MutationState, RabitqError> {
    let Some(head) = head else {
        return Ok(MutationState {
            entries: Vec::new(),
            revision,
            records: 0,
        });
    };
    let mut chain = Vec::new();
    let mut cursor = Some(head.to_string());
    let mut seen = HashSet::new();
    while let Some(key) = cursor {
        if !seen.insert(key.clone()) {
            return Err(RabitqError::InvalidPersistence("mutation chain cycle"));
        }
        let record = read_record(istore, &key).await?;
        cursor = match &record {
            MutationRecord::Delta(batch) => batch.parent.clone(),
            MutationRecord::Checkpoint(_) => None,
        };
        chain.push(record);
        if chain.len() > records as usize + 1 || chain.len() > 4096 {
            return Err(RabitqError::InvalidPersistence(
                "mutation chain is too long",
            ));
        }
    }
    let mut merged: BTreeMap<IndexKey, u64> = BTreeMap::new();
    for record in chain.into_iter().rev() {
        match record {
            MutationRecord::Delta(batch) => {
                for key in batch.keys {
                    merged.insert(key, batch.revision);
                }
            }
            MutationRecord::Checkpoint(checkpoint) => {
                for (key, revision) in checkpoint.entries {
                    merged.insert(key, revision);
                }
            }
        }
    }
    Ok(MutationState {
        entries: merged.into_iter().collect(),
        revision,
        records,
    })
}

/// Object keys referenced by the current mutation chain, for GC retention.
pub async fn chain_keys(
    istore: &IndexStore,
    head: Option<&str>,
) -> Result<Vec<String>, RabitqError> {
    let mut keys = Vec::new();
    let mut cursor = head.map(str::to_string);
    let mut seen = HashSet::new();
    while let Some(key) = cursor {
        if !seen.insert(key.clone()) {
            return Err(RabitqError::InvalidPersistence("mutation chain cycle"));
        }
        let record = read_record(istore, &key).await?;
        cursor = match &record {
            MutationRecord::Delta(batch) => batch.parent.clone(),
            MutationRecord::Checkpoint(_) => None,
        };
        keys.push(key);
        if keys.len() > 4096 {
            return Err(RabitqError::InvalidPersistence(
                "mutation chain is too long",
            ));
        }
    }
    Ok(keys)
}

fn checksum(body: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(body);
    hasher.finalize()
}

fn write_key(out: &mut Vec<u8>, key: &IndexKey) -> Result<(), RabitqError> {
    let bytes = key.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| RabitqError::InvalidPersistence("mutation key too large"))?;
    out.write_all(&len.to_le_bytes())?;
    out.write_all(bytes)?;
    Ok(())
}

fn read_key(reader: &mut Cursor<&[u8]>) -> Result<IndexKey, RabitqError> {
    let len = read_u32(reader)? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    Ok(IndexKey::new(bytes))
}

fn read_u32(reader: &mut Cursor<&[u8]>) -> Result<u32, RabitqError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut Cursor<&[u8]>) -> Result<u64, RabitqError> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint(reader: &mut Cursor<&[u8]>) -> Result<u64, RabitqError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        value |= ((byte[0] & 0x7f) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(RabitqError::InvalidPersistence("varint overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn key(value: u64) -> IndexKey {
        IndexKey::new(value.to_le_bytes())
    }

    #[tokio::test]
    async fn delta_and_checkpoint_roundtrip() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let istore = IndexStore::new(store, "idx".to_string());
        let batch = MutationBatch::new(7, None, vec![key(42), key(0), key(42)]);
        let delta_key = delta_filename(1, "delta");
        write_delta(&istore, &delta_key, &batch).await.unwrap();
        let state = load_state(&istore, Some(&delta_key), 1, 7).await.unwrap();
        assert_eq!(state.latest_revision(&key(42)), Some(7));
        assert_eq!(state.entries().len(), 2);

        let checkpoint = MutationCheckpoint::new(9, vec![(key(0), 3), (key(42), 9)]);
        let checkpoint_key = checkpoint_filename(1, "checkpoint");
        write_checkpoint(&istore, &checkpoint_key, &checkpoint)
            .await
            .unwrap();
        let state = load_state(&istore, Some(&checkpoint_key), 1, 9)
            .await
            .unwrap();
        assert_eq!(state.entries(), checkpoint.entries.as_slice());
    }

    #[tokio::test]
    async fn mutation_chain_keeps_latest_revision() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let istore = IndexStore::new(store, "idx".to_string());
        let first_key = delta_filename(1, "first");
        write_delta(
            &istore,
            &first_key,
            &MutationBatch::new(1, None, vec![key(7)]),
        )
        .await
        .unwrap();
        let second_key = delta_filename(1, "second");
        write_delta(
            &istore,
            &second_key,
            &MutationBatch::new(2, Some(first_key), vec![key(7), key(8)]),
        )
        .await
        .unwrap();
        let state = load_state(&istore, Some(&second_key), 2, 2).await.unwrap();
        assert_eq!(state.latest_revision(&key(7)), Some(2));
        assert_eq!(state.latest_revision(&key(8)), Some(2));
        assert!(!state.is_valid(&key(7), 1));
        assert!(state.is_valid(&key(7), 2));
    }

    #[test]
    fn newer_revision_wins_on_duplicate_keys() {
        let state = MutationState::new(5, 1, vec![(key(1), 2), (key(1), 5), (key(2), 3)]);
        assert_eq!(state.latest_revision(&key(1)), Some(5));
        assert!(state.is_valid(&key(1), 5));
        assert!(!state.is_valid(&key(1), 4));
        assert!(state.is_valid(&key(9), 0));
    }
}
