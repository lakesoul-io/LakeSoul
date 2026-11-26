// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shard index builder.
//!
//! Reads a shard's data files through [`LakeSoulReader`] and writes the
//! resulting segment files; commit bookkeeping (which segments form the
//! current index) belongs to the caller, which has metadata access.  The
//! caller resolves the shard's current commit and passes it via
//! [`VectorShardIndexBuilder::with_base`]: without a base the builder
//! trains a fresh index, with one it appends a delta.  The returned
//! [`ShardBuildOutcome`] carries the segment entries to publish.

use futures::StreamExt;
use futures::future::LocalBoxFuture;
use lakesoul_common::misc::SecretMap;
use std::sync::{Arc, Mutex};

use lakesoul_common::IndexKind;
use lakesoul_vector::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, VectorIndexConfig};
use object_store::ObjectStore;
use tracing::{info, warn};

use crate::index::commit::ResolvedIndex;
use crate::index::key::{KeyCodec, KeyLayout};
use crate::index::reader::{read_shard_batches, stream_shard_batches};
use crate::vector::reader::extract_vector_batch;

/// Header and segment files produced by a build, for the caller to commit.
#[derive(Debug, Clone)]
pub struct ShardBuildOutcome {
    pub index_prefix: String,
    /// Serialized [`IndexHeader`].
    pub header: Vec<u8>,
    /// New segments only: the delta segments of an incremental build, or
    /// every base segment of a fresh build / rebuild.
    pub new_segments: Vec<SegmentEntry>,
    /// Keys an incremental build inserted.  They must be recorded in the
    /// mutation delta so older copies of the same key are masked.  Empty for
    /// fresh builds and rebuilds, which start from a clean generation.
    pub upserted_keys: Vec<lakesoul_vector::IndexKey>,
}

pub struct VectorShardIndexBuilder {
    store: Arc<dyn ObjectStore>,
    config: VectorIndexConfig,
    file_paths: Vec<String>,
    pk_columns: Vec<String>,
    object_store_options: SecretMap,
    default_fs: Option<String>,
    base: Option<ResolvedIndex>,
}

/// Whether fresh/rebuild builds use the two-pass streaming reader.
///
/// Set `LAKESOUL_VECTOR_STREAMING_BUILD=0` to force the legacy materialising
/// path for A/B or debugging.
fn streaming_build_enabled() -> bool {
    std::env::var("LAKESOUL_VECTOR_STREAMING_BUILD")
        .map(|value| value != "0")
        .unwrap_or(true)
}

impl VectorShardIndexBuilder {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: VectorIndexConfig,
        file_paths: Vec<String>,
        pk_columns: Vec<String>,
        object_store_options: SecretMap,
        default_fs: Option<String>,
    ) -> Self {
        Self {
            store,
            config,
            file_paths,
            pk_columns,
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

    pub fn index_prefix(&self) -> String {
        self.base
            .as_ref()
            .map(|base| base.index_prefix.clone())
            .unwrap_or_else(|| {
                crate::index::prefix::shard_index_prefix(
                    &self.file_paths,
                    IndexKind::Vector,
                    &self.config.column_name,
                )
            })
    }

    /// Build the shard: fresh when no base was supplied, incremental otherwise.
    ///
    /// A base index is only updated incrementally while its stored key layout
    /// matches the current primary-key columns; after a schema change the
    /// shard is rebuilt from scratch so the index keys stay comparable with
    /// the data files' key layout.
    pub fn build(
        self,
    ) -> LocalBoxFuture<'static, Result<ShardBuildOutcome, lakesoul_vector::RabitqError>>
    {
        let future: LocalBoxFuture<
            'static,
            Result<ShardBuildOutcome, lakesoul_vector::RabitqError>,
        > = Box::pin(Self::build_impl(self));
        future
    }

    async fn build_impl(self) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        use lakesoul_vector::RabitqError;

        let index_prefix = self.index_prefix();
        if self.base.is_none()
            && streaming_build_enabled()
            && let Some(outcome) =
                self.build_fresh_streaming(index_prefix.clone()).await?
        {
            return Ok(outcome);
        }
        let (codec, all_batches) = self
            .read_batches()
            .await
            .map_err(|e| RabitqError::Io(format!("Failed to read: {}", e)))?;
        let key_kind = codec.as_ref().map(KeyCodec::kind_hash).unwrap_or(0);

        if all_batches.is_empty() {
            return match &self.base {
                Some(base) => Ok(ShardBuildOutcome {
                    index_prefix,
                    header: base.header.clone(),
                    new_segments: Vec::new(),
                    upserted_keys: Vec::new(),
                }),
                None => Err(RabitqError::InvalidPersistence(
                    "no vectors found in data files",
                )),
            };
        }

        let Some(base) = self.base.clone() else {
            return self.build_fresh(index_prefix, key_kind, all_batches).await;
        };
        let header = IndexHeader::deserialize(&base.header)?;
        if header.key_kind != key_kind {
            warn!(
                "vector index key layout changed (index kind {:#x}, data kind \
                 {:#x}); rebuilding the shard",
                header.key_kind, key_kind
            );
            return self.build_fresh(index_prefix, key_kind, all_batches).await;
        }
        self.build_incremental(base, index_prefix, header, all_batches)
            .await
    }

    /// Force a full rebuild of the shard index from scratch.
    ///
    /// Unlike [`build`](Self::build) — which appends the new batch into the
    /// existing clusters as a delta segment — a rebuild re-reads **all**
    /// data files of the shard, re-trains the IVF centroids on the full
    /// dataset, and returns a complete new generation.  Callers must pass
    /// the complete shard file list (base + previous deltas + the new
    /// batch), e.g. when the accumulated delta/base ratio has drifted past
    /// the configured threshold or on an explicit user request.
    pub fn rebuild(
        self,
    ) -> LocalBoxFuture<'static, Result<ShardBuildOutcome, lakesoul_vector::RabitqError>>
    {
        let future: LocalBoxFuture<
            'static,
            Result<ShardBuildOutcome, lakesoul_vector::RabitqError>,
        > = Box::pin(Self::rebuild_impl(self));
        future
    }

    async fn rebuild_impl(
        self,
    ) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        use lakesoul_vector::RabitqError;

        let index_prefix = self.index_prefix();
        if streaming_build_enabled()
            && let Some(outcome) =
                self.build_fresh_streaming(index_prefix.clone()).await?
        {
            return Ok(outcome);
        }
        let (codec, all_batches) = self
            .read_batches()
            .await
            .map_err(|e| RabitqError::Io(format!("Failed to read: {}", e)))?;
        if all_batches.is_empty() {
            return Err(RabitqError::InvalidPersistence(
                "no vectors found in data files",
            ));
        }
        let key_kind = codec.as_ref().map(KeyCodec::kind_hash).unwrap_or(0);
        self.build_fresh(index_prefix, key_kind, all_batches).await
    }

    /// Two-pass bounded-memory build: pass 1 streams the shard into the
    /// builder's reservoir, pass 2 streams it again for quantisation.  Raw
    /// vectors are never materialised as one `Vec`.
    ///
    /// Returns `Ok(None)` when the shard is too small to train `nlist`
    /// clusters; the caller then falls back to the legacy in-memory path.
    fn build_fresh_streaming<'a>(
        &'a self,
        index_prefix: String,
    ) -> LocalBoxFuture<'a, Result<Option<ShardBuildOutcome>, lakesoul_vector::RabitqError>>
    {
        let future: LocalBoxFuture<
            'a,
            Result<Option<ShardBuildOutcome>, lakesoul_vector::RabitqError>,
        > = Box::pin(Self::build_fresh_streaming_impl(self, index_prefix));
        future
    }

    async fn build_fresh_streaming_impl(
        &self,
        index_prefix: String,
    ) -> Result<Option<ShardBuildOutcome>, lakesoul_vector::RabitqError> {
        use lakesoul_vector::RabitqError;

        let dim = self.config.dim;
        let column = self.config.column_name.clone();
        let nlist = self.config.nlist;
        let mut builder: Option<IvfRabitqBuilder> = None;
        let mut codec: Option<KeyCodec> = None;
        let mut total = 0usize;
        let mut pass1 = Box::pin(stream_shard_batches(
            self.file_paths.clone(),
            self.pk_columns.clone(),
            vec![column.clone()],
            self.object_store_options
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            self.default_fs.clone(),
        ));
        while let Some(batch) = pass1.next().await {
            let batch =
                batch.map_err(|e| RabitqError::Io(format!("stream read: {e}")))?;
            if batch.num_rows() == 0 {
                continue;
            }
            if builder.is_none() {
                let layout =
                    KeyLayout::from_schema(batch.schema().as_ref(), &self.pk_columns)
                        .map_err(|e| RabitqError::Io(format!("key layout: {e}")))?;
                let key_codec = KeyCodec::new(layout)
                    .map_err(|e| RabitqError::Io(format!("key codec: {e}")))?;
                builder = Some(IvfRabitqBuilder::new(
                    dim,
                    nlist,
                    self.config.total_bits,
                    self.config.metric,
                    self.config.rotator_type,
                    self.config.seed,
                    self.config.use_faster_config,
                    key_codec.kind_hash(),
                ));
                codec = Some(key_codec);
            }
            let extracted = extract_vector_batch(
                &batch,
                codec.as_ref().expect("codec initialized with builder"),
                &column,
                dim,
            )
            .map_err(|e| RabitqError::Io(format!("extract vectors: {e}")))?;
            total += extracted.ids.len();
            builder
                .as_mut()
                .expect("builder initialized")
                .insert_batch(extracted)?;
        }
        let Some(builder) = builder else {
            return Err(RabitqError::InvalidPersistence(
                "no vectors found in data files",
            ));
        };
        let Some(codec) = codec else {
            return Err(RabitqError::InvalidPersistence(
                "no vectors found in data files",
            ));
        };
        if total == 0 {
            return Err(RabitqError::InvalidPersistence(
                "no vectors found in data files",
            ));
        }
        if total < nlist {
            info!(
                "streaming build skipped: {} vectors < nlist={}; falling back",
                total, nlist
            );
            return Ok(None);
        }
        let files = self.file_paths.clone();
        let pks = self.pk_columns.clone();
        let options = self
            .object_store_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<std::collections::HashMap<_, _>>();
        let default_fs = self.default_fs.clone();
        let col2 = column.clone();
        let raw = stream_shard_batches(files, pks, vec![column], options, default_fs);
        let pass2_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let error_slot = Arc::clone(&pass2_error);
        let pass2 = Box::pin(async_stream::stream! {
            let mut raw = Box::pin(raw);
            while let Some(batch) = raw.next().await {
                match batch {
                    Ok(batch) => {
                        if batch.num_rows() == 0 { continue; }
                        match extract_vector_batch(&batch, &codec, &col2, dim) {
                            Ok(extracted) if !extracted.ids.is_empty() => yield extracted,
                            Ok(_) => {}
                            Err(error) => {
                                *error_slot.lock().expect("pass2 error lock") =
                                    Some(format!("extract vectors: {error}"));
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        *error_slot.lock().expect("pass2 error lock") =
                            Some(format!("stream read: {error}"));
                        break;
                    }
                }
            }
        });
        let mut pass2_slot = Some(pass2);
        let make_stream =
            move || pass2_slot.take().expect("pass 2 stream is consumed once");
        let istore = IndexStore::new(self.store.clone(), index_prefix.clone());
        let chunk_bytes = std::env::var("LAKESOUL_VECTOR_BUILD_CHUNK_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(64 * 1024 * 1024);
        let (index, mut new_segments) = builder
            .build_with_chunk_flush(make_stream, istore, chunk_bytes)
            .await?;
        if let Some(error) = pass2_error.lock().expect("pass2 error lock").take() {
            return Err(RabitqError::Io(error));
        }
        let header = index.index_header();
        for segment in &mut new_segments {
            segment.data_files = self.file_paths.clone();
        }
        info!(
            "Fresh index built successfully (streaming input, {} segments, chunk={} MiB)",
            new_segments.len(),
            chunk_bytes / 1024 / 1024
        );
        Ok(Some(ShardBuildOutcome {
            index_prefix,
            header: header.serialize(),
            new_segments,
            upserted_keys: Vec::new(),
        }))
    }

    async fn build_fresh(
        self,
        index_prefix: String,
        key_kind: u64,
        all_batches: Vec<IdAndVecBatch>,
    ) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        use lakesoul_vector::RabitqError;

        info!(
            "Building fresh vector index for column '{}' at '{}' ({} files)",
            self.config.column_name,
            index_prefix,
            self.file_paths.len()
        );

        let total: usize = all_batches.iter().map(|b| b.ids.len()).sum();
        info!("Read {} vectors from {} batches", total, all_batches.len());

        if total == 0 {
            return Err(RabitqError::InvalidPersistence(
                "no vectors found in data files",
            ));
        }

        // Clamp nlist so we never create more clusters than there are vectors;
        // rabitq panics on empty clusters / nlist > vector count.
        let nlist = self.config.nlist.clamp(1, total);
        info!(
            "Building with nlist={} (requested {}) for {} vectors",
            nlist, self.config.nlist, total
        );

        let mut builder = IvfRabitqBuilder::new(
            self.config.dim,
            nlist,
            self.config.total_bits,
            self.config.metric,
            self.config.rotator_type,
            self.config.seed,
            self.config.use_faster_config,
            key_kind,
        );
        for batch in &all_batches {
            builder.insert_batch(batch.clone())?;
        }

        // Pass 2: feed the same data again for K-Means + quantize
        info!("Pass 2: K-Means + streaming quantization");
        let batches = all_batches;
        let make_stream = move || {
            let iter = batches.clone().into_iter();
            Box::pin(futures::stream::iter(iter))
        };
        let index = builder.build(make_stream).await?;

        info!("Writing index segments...");
        let istore = IndexStore::new(self.store.clone(), index_prefix.clone());
        let (header, mut new_segments) =
            index.write_base_segments(&istore).await.map_err(|e| {
                warn!("Failed to write index segments: {:?}", e);
                e
            })?;
        // Record the data files this build read, so a later search can tell
        // which active files the commit covers (and exact-scan the rest).
        for segment in &mut new_segments {
            segment.data_files = self.file_paths.clone();
        }
        info!("Fresh index built successfully");
        Ok(ShardBuildOutcome {
            index_prefix,
            header: header.serialize(),
            new_segments,
            upserted_keys: Vec::new(),
        })
    }

    async fn build_incremental(
        self,
        base: ResolvedIndex,
        index_prefix: String,
        header: IndexHeader,
        all_batches: Vec<IdAndVecBatch>,
    ) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        use lakesoul_vector::RabitqError;

        let base_segments: Vec<SegmentEntry> = base
            .segments_as()
            .map_err(|e| RabitqError::Io(format!("invalid base segments: {}", e)))?;
        let istore = IndexStore::new(self.store.clone(), index_prefix.clone());

        info!(
            "Incrementally updating vector index for column '{}' at '{}' ({} files)",
            self.config.column_name,
            index_prefix,
            self.file_paths.len()
        );

        let mut builder =
            IvfRabitqBuilder::load(&istore, &header, &base_segments).await?;

        let mut total = 0usize;
        for batch in &all_batches {
            total += batch.ids.len();
            builder.insert_batch(batch.clone())?;
        }
        info!("Inserted {} vectors", total);

        let (header, mut new_segments, upserted_keys) =
            builder.flush_with_mutations(&istore).await?;
        // The delta covers exactly the files this build read; the base
        // segments keep the coverage they were committed with.
        for segment in &mut new_segments {
            segment.data_files = self.file_paths.clone();
        }
        info!(
            keys = upserted_keys.len(),
            "Incremental index update complete"
        );
        Ok(ShardBuildOutcome {
            index_prefix,
            header: header.serialize(),
            new_segments,
            upserted_keys,
        })
    }

    /// Read the shard's rows and build the key codec from the data schema.
    ///
    /// Returns `(None, [])` when the shard's data files hold no rows.
    fn read_batches<'a>(
        &'a self,
    ) -> LocalBoxFuture<'a, crate::Result<(Option<KeyCodec>, Vec<IdAndVecBatch>)>> {
        let future: LocalBoxFuture<
            'a,
            crate::Result<(Option<KeyCodec>, Vec<IdAndVecBatch>)>,
        > = Box::pin(Self::read_batches_impl(self));
        future
    }

    async fn read_batches_impl(
        &self,
    ) -> crate::Result<(Option<KeyCodec>, Vec<IdAndVecBatch>)> {
        let vec_col = self.config.column_name.clone();
        let dim = self.config.dim;

        let batches = read_shard_batches(
            &self.file_paths,
            &self.pk_columns,
            std::slice::from_ref(&vec_col),
            &self.object_store_options,
            self.default_fs.as_deref(),
        )
        .await?;

        let Some(schema) = batches.first().map(|batch| batch.schema()) else {
            return Ok((None, Vec::new()));
        };
        let codec =
            KeyCodec::new(KeyLayout::from_schema(schema.as_ref(), &self.pk_columns)?)?;
        let batches = batches
            .iter()
            .map(|batch| extract_vector_batch(batch, &codec, &vec_col, dim))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok((Some(codec), batches))
    }

    /// Read all rows via LakeSoulReader (handles merge-on-read, CDC, etc.)
    pub async fn read_all_batches(&self) -> crate::Result<Vec<IdAndVecBatch>> {
        Ok(self.read_batches().await?.1)
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use lakesoul_vector::{Metric, RotatorType};
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::config::LakeSoulIOConfigBuilder;
    use crate::file_format::PhysicalFormat;
    use crate::writer::create_writer_with_io_config;

    #[tokio::test]
    async fn read_vector_batches_from_vortex_compact() -> crate::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let prefix = temp_dir.path().to_string_lossy().into_owned();
        let dim = 2;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim,
                ),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
                Arc::new(FixedSizeListArray::new(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim,
                    Arc::new(Float32Array::from(vec![
                        1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5,
                    ])),
                    None,
                )),
            ],
        )?;

        let writer_config = LakeSoulIOConfigBuilder::new()
            .with_prefix(prefix)
            .with_schema(schema)
            .with_physical_format(PhysicalFormat::VortexCompact)
            .build();
        let mut writer = create_writer_with_io_config(writer_config).await?;
        writer.write_record_batch(batch).await?;
        let outputs = writer.flush_and_close().await?;

        let builder = VectorShardIndexBuilder::new(
            Arc::new(LocalFileSystem::new()),
            VectorIndexConfig {
                column_name: "vec".to_string(),
                dim: dim as usize,
                nlist: 2,
                total_bits: 7,
                metric: Metric::L2,
                rotator_type: RotatorType::FhtKacRotator,
                seed: 42,
                use_faster_config: true,
                rebuild_mode: "auto".to_string(),
                max_delta_ratio: 1.0,
            },
            outputs.into_iter().map(|output| output.file_path).collect(),
            vec!["id".to_string()],
            SecretMap::new(),
            None,
        );

        let batches = builder.read_all_batches().await?;
        assert_eq!(
            batches.iter().map(|batch| batch.ids.len()).sum::<usize>(),
            4
        );
        assert!(
            batches
                .iter()
                .all(|batch| batch.vectors.len() == batch.ids.len() * dim as usize)
        );
        Ok(())
    }
}
