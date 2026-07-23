// Integrate with the reader's vector_search injection later
// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-shard vector index builder: reads parquet via LakeSoulReader, builds IVF+RaBitQ index.

use std::sync::Arc;

use arrow_schema::Schema;
use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, ManifestStore, RabitqError};
use object_store::ObjectStore;
use tracing::{info, warn};

use crate::config::LakeSoulIOConfigBuilder;
use crate::reader::LakeSoulReader;
use crate::session::LakeSoulIOSession;

use crate::vector::reader::extract_vector_batch;
use lakesoul_vector::VectorIndexConfig;

pub struct VectorShardIndexBuilder {
    store: Arc<dyn ObjectStore>,
    config: VectorIndexConfig,
    file_paths: Vec<String>,
    pk_column: String,
    object_store_options: std::collections::HashMap<String, String>,
    default_fs: Option<String>,
}

impl VectorShardIndexBuilder {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: VectorIndexConfig,
        file_paths: Vec<String>,
        pk_column: String,
        object_store_options: std::collections::HashMap<String, String>,
        default_fs: Option<String>,
    ) -> Self {
        Self {
            store,
            config,
            file_paths,
            pk_column,
            object_store_options,
            default_fs,
        }
    }

    fn index_prefix(&self) -> String {
        // Derive table prefix from the first file's parent directory
        let prefix = self
            .file_paths
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
        let prefixes = crate::vector::search::derive_index_prefixes(
            &self.file_paths,
            &prefix,
            &self.config.column_name,
        );
        prefixes
            .first()
            .map(|(p, _)| p.clone())
            .unwrap_or_else(|| format!("_vector_index/{}/-5/0/", self.config.column_name))
    }

    fn table_prefix(&self) -> String {
        self.file_paths
            .first()
            .and_then(|u| {
                let u = u
                    .trim_start_matches("file://")
                    .trim_start_matches("s3://")
                    .trim_start_matches("s3a://");
                std::path::Path::new(u.trim_end_matches('/'))
                    .parent()?
                    .to_str()
                    .map(|s| format!("file://{}", s))
            })
            .unwrap_or_default()
    }

    fn reader_config_builder(&self) -> LakeSoulIOConfigBuilder {
        let mut config_builder = LakeSoulIOConfigBuilder::new()
            .with_files(self.file_paths.clone())
            .with_prefix(self.table_prefix())
            .with_primary_keys(vec![self.pk_column.clone()]);

        for (key, value) in &self.object_store_options {
            config_builder =
                config_builder.with_object_store_option(key.clone(), value.clone());
        }
        if let Some(default_fs) = &self.default_fs {
            config_builder = config_builder
                .with_object_store_option("fs.defaultFS".to_string(), default_fs.clone());
        }

        config_builder
    }

    pub async fn build(self) -> Result<(), RabitqError> {
        let mstore = ManifestStore::new(self.store.clone(), self.index_prefix());
        let need_fresh =
            !lakesoul_vector::rabitq::manifest::manifest_exists(&mstore).await;
        if need_fresh {
            self.build_fresh(mstore).await
        } else {
            self.build_loaded(mstore).await
        }
    }

    async fn build_fresh(self, mstore: ManifestStore) -> Result<(), RabitqError> {
        let mut builder = IvfRabitqBuilder::new(
            self.config.dim,
            self.config.nlist,
            self.config.total_bits,
            self.config.metric,
            self.config.rotator_type,
            self.config.seed,
            self.config.use_faster_config,
        );

        // Pass 1: read all vectors via LakeSoulReader (handles merge-on-read)
        info!("Pass 1: reading via LakeSoulReader for reservoir sampling");
        let all_batches = self
            .read_all_batches()
            .await
            .map_err(|e| RabitqError::Io(format!("Failed to read: {}", e)))?;
        let mut total = 0usize;
        for batch in &all_batches {
            total += batch.ids.len();
            builder.insert_batch(batch.clone())?;
        }
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

        // Pass 2: feed the same data again for K-Means + quantize
        info!("Pass 2: K-Means + streaming quantization");
        let batches = all_batches;
        let make_stream = move || {
            let iter = batches.clone().into_iter();
            Box::pin(futures::stream::iter(iter))
        };
        let index = builder.build(make_stream).await?;
        info!("Saving index to object store...");
        index.save_to_v4(&mstore).await.map_err(|e| {
            warn!("Failed to save index: {:?}", e);
            e
        })?;
        info!("Fresh index built successfully");
        Ok(())
    }

    async fn build_loaded(self, mstore: ManifestStore) -> Result<(), RabitqError> {
        let mut builder = IvfRabitqBuilder::load(
            &mstore,
            self.config.dim,
            self.config.nlist,
            self.config.total_bits,
            self.config.metric,
            self.config.rotator_type,
            self.config.seed,
            self.config.use_faster_config,
        )
        .await?;

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
            return Ok(());
        }

        builder.flush(&mstore).await?;
        info!("Incremental index update complete");
        Ok(())
    }

    /// Read all rows via LakeSoulReader (handles merge-on-read, CDC, etc.)
    async fn read_all_batches(&self) -> crate::Result<Vec<IdAndVecBatch>> {
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
        let mut reader = LakeSoulReader::new(io_config)
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
