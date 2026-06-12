// Integrate with the reader's vector_search injection later
// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-shard vector index builder: reads parquet, builds IVF+RaBitQ index.

use std::sync::Arc;

use futures::StreamExt;
use object_store::ObjectStore;
use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, ManifestStore, RabitqError};
use tracing::{info, warn};

use crate::config::LakeSoulIOConfigBuilder;

use lakesoul_vector::VectorIndexConfig;
use crate::vector::reader::extract_vector_batch;

pub struct VectorShardIndexBuilder {
    store: Arc<dyn ObjectStore>,
    config: VectorIndexConfig,
    file_paths: Vec<String>,
    pk_column: String,
    index_prefix: String,
    object_store_options: std::collections::HashMap<String, String>,
    default_fs: Option<String>,
}

impl VectorShardIndexBuilder {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: VectorIndexConfig,
        file_paths: Vec<String>,
        pk_column: String,
        index_prefix: String,
        object_store_options: std::collections::HashMap<String, String>,
        default_fs: Option<String>,
    ) -> Self {
        Self { store, config, file_paths, pk_column, index_prefix, object_store_options, default_fs }
    }

    pub async fn build(self) -> Result<(), RabitqError> {
        let mstore = ManifestStore::new(self.store.clone(), self.index_prefix.clone());
        let need_fresh = !lakesoul_vector::rabitq::manifest::manifest_exists(&mstore).await;
        if need_fresh {
            self.build_fresh(mstore).await
        } else {
            self.build_loaded(mstore).await
        }
    }

    async fn build_fresh(self, mstore: ManifestStore) -> Result<(), RabitqError> {
        let mut builder = IvfRabitqBuilder::new(
            self.config.dim, self.config.nlist, self.config.total_bits,
            self.config.metric, self.config.rotator_type,
            self.config.seed, self.config.use_faster_config,
        );

        // Pass 1: read all vectors, reservoir-sample into builder
        info!("Pass 1: reading parquet files for reservoir sampling");
        let all_batches = self.read_all_batches().await.map_err(|e| {
            RabitqError::Io(format!("Failed to read parquet: {}", e))
        })?;
        let mut total = 0usize;
        for batch in &all_batches {
            total += batch.ids.len();
            builder.insert_batch(batch.clone())?;
        }
        info!("Pass 1 done: {} vectors from {} batches", total, all_batches.len());

        if total == 0 {
            return Err(RabitqError::InvalidPersistence("no vectors found in data files"));
        }

        // Pass 2: feed the same data again for K-Means + quantize
        info!("Pass 2: K-Means + streaming quantization");
        let batches = all_batches;
        let make_stream = move || {
            let iter = batches.clone().into_iter().map(|b| b);
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
            &mstore, self.config.dim, self.config.nlist, self.config.total_bits,
            self.config.metric, self.config.rotator_type,
            self.config.seed, self.config.use_faster_config,
        ).await?;

        let all_batches = self.read_all_batches().await.map_err(|e| {
            RabitqError::Io(format!("Failed to read parquet: {}", e))
        })?;
        let mut total = 0usize;
        for batch in &all_batches {
            total += batch.ids.len();
            builder.insert_batch(batch.clone())?;
        }
        info!("Inserted {} vectors", total);
        if total == 0 { return Ok(()); }

        builder.flush(&mstore).await?;
        info!("Incremental index update complete");
        Ok(())
    }

    async fn read_all_batches(&self) -> crate::Result<Vec<IdAndVecBatch>> {
        let mut results = Vec::new();
        let vec_col = self.config.column_name.clone();
        let pk_col = self.pk_column.clone();
        let dim = self.config.dim;

        for file_path in &self.file_paths {
            // Read parquet directly (LakeSoulReader requires full table setup)
            let path = file_path.trim_start_matches("file://");
            let file = std::fs::File::open(path).map_err(|e| {
                rootcause::report!("cannot open {}: {}", path, e)
            })?;
            let parquet_reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| rootcause::report!("parquet error: {}", e))?
                .with_batch_size(8192)
                .build()
                .map_err(|e| rootcause::report!("parquet build error: {}", e))?;
            for batch_result in parquet_reader {
                let batch = batch_result.map_err(|e| rootcause::report!("parquet read error: {}", e))?;
                if batch.num_rows() == 0 { continue; }
                results.push(extract_vector_batch(&batch, &pk_col, &vec_col, dim)?);
            }
        }
        Ok(results)
    }
}
