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

use std::collections::HashMap;
use std::sync::Arc;

use lakesoul_common::IndexKind;
use lakesoul_vector::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, VectorIndexConfig};
use object_store::ObjectStore;
use tracing::{info, warn};

use crate::index::commit::ResolvedIndex;
use crate::index::reader::read_shard_batches;
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
}

pub struct VectorShardIndexBuilder {
    store: Arc<dyn ObjectStore>,
    config: VectorIndexConfig,
    file_paths: Vec<String>,
    pk_column: String,
    object_store_options: HashMap<String, String>,
    default_fs: Option<String>,
    base: Option<ResolvedIndex>,
}

impl VectorShardIndexBuilder {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: VectorIndexConfig,
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
    pub async fn build(self) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        if self.base.is_some() {
            self.build_incremental().await
        } else {
            self.build_fresh().await
        }
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
    pub async fn rebuild(
        self,
    ) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        self.build_fresh().await
    }

    async fn build_fresh(
        self,
    ) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        use lakesoul_vector::RabitqError;

        let index_prefix = self.index_prefix();
        info!(
            "Building fresh vector index for column '{}' at '{}' ({} files)",
            self.config.column_name,
            index_prefix,
            self.file_paths.len()
        );

        // Pass 1: read all vectors via LakeSoulReader (handles merge-on-read)
        info!("Pass 1: reading via LakeSoulReader for reservoir sampling");
        let all_batches = self
            .read_all_batches()
            .await
            .map_err(|e| RabitqError::Io(format!("Failed to read: {}", e)))?;
        let total: usize = all_batches.iter().map(|b| b.ids.len()).sum();
        info!(
            "Pass 1 done: {} vectors from {} batches",
            total,
            all_batches.len()
        );

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
        let (header, new_segments) =
            index.write_base_segments(&istore).await.map_err(|e| {
                warn!("Failed to write index segments: {:?}", e);
                e
            })?;
        info!("Fresh index built successfully");
        Ok(ShardBuildOutcome {
            index_prefix,
            header: header.serialize(),
            new_segments,
        })
    }

    async fn build_incremental(
        self,
    ) -> Result<ShardBuildOutcome, lakesoul_vector::RabitqError> {
        use lakesoul_vector::RabitqError;

        let base = self.base.clone().expect("checked by caller");
        let index_prefix = base.index_prefix.clone();
        let header = IndexHeader::deserialize(&base.header)?;
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

        let all_batches = self
            .read_all_batches()
            .await
            .map_err(|e| RabitqError::Io(format!("Failed to read: {}", e)))?;
        let mut total = 0usize;
        for batch in &all_batches {
            total += batch.ids.len();
            builder.insert_batch(batch.clone())?;
        }
        info!("Inserted {} vectors", total);
        if total == 0 {
            return Ok(ShardBuildOutcome {
                index_prefix,
                header: base.header,
                new_segments: Vec::new(),
            });
        }

        let (header, new_segments) = builder.flush(&istore).await?;
        info!("Incremental index update complete");
        Ok(ShardBuildOutcome {
            index_prefix,
            header: header.serialize(),
            new_segments,
        })
    }

    /// Read all rows via LakeSoulReader (handles merge-on-read, CDC, etc.)
    pub async fn read_all_batches(&self) -> crate::Result<Vec<IdAndVecBatch>> {
        let vec_col = self.config.column_name.clone();
        let pk_col = self.pk_column.clone();
        let dim = self.config.dim;

        let batches = read_shard_batches(
            &self.file_paths,
            &pk_col,
            std::slice::from_ref(&vec_col),
            &self.object_store_options,
            self.default_fs.as_deref(),
        )
        .await?;

        batches
            .iter()
            .map(|batch| extract_vector_batch(batch, &pk_col, &vec_col, dim))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

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
            "id".to_string(),
            HashMap::new(),
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
