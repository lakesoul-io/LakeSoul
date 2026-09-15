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

use arrow_schema::Schema;
use lakesoul_vector::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, VectorIndexConfig};
use object_store::ObjectStore;
use tracing::{info, warn};

use crate::config::LakeSoulIOConfigBuilder;
use crate::session::LakeSoulIOSession;
use crate::vector::reader::extract_vector_batch;

/// Derive the vector index store prefix for the shard containing `file_paths`.
///
/// All files of a shard share the same partition directory, so the first
/// file determines the shard's `_vector_index/{column}/...` prefix (matching
/// how the search path locates the index).
pub fn shard_index_prefix(file_paths: &[String], column: &str) -> String {
    let prefix = file_paths
        .first()
        .and_then(|u| {
            let u = u
                .trim_start_matches("file://")
                .trim_start_matches("s3://")
                .trim_start_matches("s3a://");
            std::path::Path::new(u.trim_end_matches('/'))
                .parent()?
                .to_str()
                .map(|s| s.to_string())
        })
        .unwrap_or_default();
    crate::vector::search::derive_index_prefixes(file_paths, &prefix, column)
        .first()
        .map(|(p, _)| p.clone())
        .unwrap_or_else(|| format!("_vector_index/{column}/-5/0/"))
}

/// An index commit resolved by the caller from the metadata catalog.
///
/// The header is the opaque [`IndexHeader`] serialization; the segments are
/// every file of the resolved view.  Types are deliberately plain so that
/// this crate stays independent of the metadata layer.
#[derive(Debug, Clone)]
pub struct ResolvedIndexShard {
    pub index_prefix: String,
    pub commit_id: i64,
    pub generation: u64,
    pub version: u64,
    pub header: Vec<u8>,
    pub segments: Vec<SegmentEntry>,
}

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
    base: Option<ResolvedIndexShard>,
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
    pub fn with_base(mut self, base: ResolvedIndexShard) -> Self {
        self.base = Some(base);
        self
    }

    pub fn index_prefix(&self) -> String {
        self.base
            .as_ref()
            .map(|base| base.index_prefix.clone())
            .unwrap_or_else(|| {
                shard_index_prefix(&self.file_paths, &self.config.column_name)
            })
    }

    fn table_prefix(&self) -> String {
        self.file_paths
            .first()
            .and_then(|u| {
                let (scheme, rest) = if let Some(r) = u.strip_prefix("file://") {
                    ("file://", r)
                } else if let Some(r) = u.strip_prefix("s3://") {
                    ("s3://", r)
                } else if let Some(r) = u.strip_prefix("s3a://") {
                    ("s3a://", r)
                } else {
                    ("", u.as_str())
                };
                std::path::Path::new(rest.trim_end_matches('/'))
                    .parent()?
                    .to_str()
                    .map(|s| format!("{}{}", scheme, s))
            })
            .unwrap_or_default()
    }

    fn reader_config_builder(&self) -> LakeSoulIOConfigBuilder {
        let mut config_builder = LakeSoulIOConfigBuilder::new()
            .with_files(self.file_paths.clone())
            .with_prefix(self.table_prefix())
            .with_primary_keys(vec![self.pk_column.clone()]);

        // Pass through object-store configuration.  The simplified keys used
        // by create_object_store (access_key_id, endpoint, …) are harmless
        // here — the reader only acts on the fs.s3a.* keys it recognises.
        for (key, value) in &self.object_store_options {
            if key != "type" {
                config_builder =
                    config_builder.with_object_store_option(key.clone(), value.clone());
            }
        }
        if let Some(default_fs) = &self.default_fs {
            config_builder = config_builder
                .with_object_store_option("fs.defaultFS".to_string(), default_fs.clone());
        }

        config_builder
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
        let istore = IndexStore::new(self.store.clone(), index_prefix.clone());

        info!(
            "Incrementally updating vector index for column '{}' at '{}' ({} files)",
            self.config.column_name,
            index_prefix,
            self.file_paths.len()
        );

        let mut builder =
            IvfRabitqBuilder::load(&istore, &header, &base.segments).await?;

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
        let mut results = Vec::new();
        let vec_col = self.config.column_name.clone();
        let pk_col = self.pk_column.clone();
        let dim = self.config.dim;

        if self.file_paths.is_empty() {
            return Ok(results);
        }

        // Infer through LakeSoul's format registry so Parquet, Vortex, and remote
        // object stores all use the same schema path as the actual reader.
        let inference_config = self
            .reader_config_builder()
            .set_inferring_schema(true)
            .build();
        let inference_session =
            LakeSoulIOSession::try_new(inference_config).map_err(|e| {
                rootcause::report!("failed to create schema inference session: {}", e)
            })?;
        let inferred_schema = inference_session
            .get_table_schema()
            .await
            .map_err(|e| rootcause::report!("failed to infer data file schema: {}", e))?;
        let file_schema = inferred_schema.file_schema();
        let schema = Arc::new(Schema::new(vec![
            file_schema
                .field_with_name(&pk_col)
                .map_err(|e| {
                    rootcause::report!("PK column '{}' not found: {}", pk_col, e)
                })?
                .clone(),
            file_schema
                .field_with_name(&vec_col)
                .map_err(|e| {
                    rootcause::report!("vector column '{}' not found: {}", vec_col, e)
                })?
                .clone(),
        ]));

        let io_config = self.reader_config_builder().with_schema(schema).build();
        let mut reader = crate::reader::LakeSoulReader::new(io_config)
            .map_err(|e| rootcause::report!("failed to create reader: {}", e))?;
        reader
            .start()
            .await
            .map_err(|e| rootcause::report!("failed to start reader: {}", e))?;

        while let Some(batch_result) = reader.next_rb().await {
            let batch =
                batch_result.map_err(|e| rootcause::report!("read error: {}", e))?;
            if batch.num_rows() == 0 {
                continue;
            }
            results.push(extract_vector_batch(&batch, &pk_col, &vec_col, dim)?);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field};
    use lakesoul_vector::{Metric, RotatorType};
    use object_store::local::LocalFileSystem;

    use super::*;
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
