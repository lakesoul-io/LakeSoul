// Integrate with the reader's vector_search injection later
// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-shard vector index builder: reads parquet via LakeSoulReader, builds IVF+RaBitQ index.

use std::sync::Arc;

use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, ManifestStore, RabitqError};
use object_store::ObjectStore;
use tracing::{info, warn};

use crate::config::LakeSoulIOConfigBuilder;
use crate::reader::LakeSoulReader;

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

        let prefix = self.table_prefix();

        let mut config_builder = LakeSoulIOConfigBuilder::new()
            .with_files(self.file_paths.clone())
            .with_prefix(prefix)
            .with_primary_keys(vec![pk_col.clone()])
            .with_batch_size(8192)
            .with_thread_num(1);

        // Apply object store options
        for (k, v) in &self.object_store_options {
            config_builder =
                config_builder.with_object_store_option(k.clone(), v.clone());
        }
        if let Some(ref default_fs) = self.default_fs {
            config_builder = config_builder
                .with_object_store_option("fs.defaultFS".to_string(), default_fs.clone());
        }

        let io_config = config_builder.build();
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
